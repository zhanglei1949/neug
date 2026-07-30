/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/transaction/version_manager.h"

#include <glog/logging.h>
#include <limits>
#include <mutex>
#include <ostream>
#include <thread>

#include "neug/utils/bitset.h"
#include "neug/utils/concurrency_test_hooks.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/types.h"

namespace neug {

// VersionManager implementation

VersionManager::VersionManager() : runtime_wait_(&NativeRuntimeWait) {}

void VersionManager::init_ts(uint32_t ts, int thread_num) {
  if (ts == std::numeric_limits<uint32_t>::max()) {
    THROW_RUNTIME_ERROR(
        "Transaction timestamp space exhausted; checkpoint/reset the timeline "
        "before reopening the database");
  }
  write_ts_.store(ts + 1, std::memory_order_relaxed);
  read_ts_.store(ts, std::memory_order_relaxed);
  operation_gate_state_.store(
      PackOperationGateState(
          {OperationGatePhase::kNormal, 0 /* readers */, 0 /* inserters */}),
      std::memory_order_relaxed);

  // The recovery frontier doubles as the initial view generation: the
  // snapshot store's initial slot is tagged with the same value, so the
  // first published read view matches it (see NeugDB::openGraphAndIngestWals).
  view_generation_.store(ts, std::memory_order_relaxed);
  published_read_view_.store(PackPublishedReadView({ts, ts}),
                             std::memory_order_relaxed);

  ts_window_.init();
  thread_num_ = thread_num;
}

bool VersionManager::try_set_runtime_wait_if_quiescent(
    RuntimeWaitFn runtime_wait) noexcept {
  if (runtime_wait == nullptr) {
    return false;
  }
  std::unique_lock lock(lock_, std::try_to_lock);
  if (!lock.owns_lock()) {
    return false;
  }

  uint64_t expected = PackOperationGateState(
      {OperationGatePhase::kNormal, 0 /* readers */, 0 /* inserters */});
  const uint64_t blocked = PackOperationGateState(
      {OperationGatePhase::kExclusive, 0 /* readers */, 0 /* inserters */});
  if (!operation_gate_state_.compare_exchange_strong(
          expected, blocked, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    return false;
  }

  // The packed CAS checks phase and both admission counts at one
  // linearization point, then temporarily closes admission while replacing
  // the policy. Service lifecycle code prevents new transaction attempts;
  // the closed phase additionally makes an accidental racing entrant wait.
  runtime_wait_.store(runtime_wait, std::memory_order_release);
  operation_gate_state_.store(
      PackOperationGateState(
          {OperationGatePhase::kNormal, 0 /* readers */, 0 /* inserters */}),
      std::memory_order_release);
  return true;
}

uint64_t VersionManager::PackOperationGateState(OperationGateState state) {
  CHECK_LE(state.readers, kMaxAdmissionCount);
  CHECK_LE(state.inserters, kMaxAdmissionCount);
  return (static_cast<uint64_t>(state.phase) << 62) |
         (static_cast<uint64_t>(state.readers) << 31) | state.inserters;
}

VersionManager::OperationGateState VersionManager::UnpackOperationGateState(
    uint64_t packed) {
  return {static_cast<OperationGatePhase>(packed >> 62),
          static_cast<uint32_t>((packed >> 31) & kMaxAdmissionCount),
          static_cast<uint32_t>(packed & kMaxAdmissionCount)};
}

bool VersionManager::IsReaderAllowed(OperationGatePhase phase) {
  return phase == OperationGatePhase::kNormal ||
         phase == OperationGatePhase::kUpdateExecution;
}

void VersionManager::wait_for_admission_change(uint64_t observed) const {
  AdaptiveBackoff wait(runtime_wait());
  while (operation_gate_state_.load(std::memory_order_acquire) == observed) {
    wait();
  }
}

RuntimeWaitFn VersionManager::runtime_wait() const noexcept {
  return runtime_wait_.load(std::memory_order_acquire);
}

uint32_t VersionManager::allocate_write_timestamp() {
  uint32_t current = write_ts_.load(std::memory_order_acquire);
  while (true) {
    if (current == std::numeric_limits<uint32_t>::max()) {
      THROW_RUNTIME_ERROR(
          "Transaction timestamp space exhausted; checkpoint/reset the "
          "timeline before accepting more writes");
    }
    if (write_ts_.compare_exchange_weak(current, current + 1,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
      return current;
    }
  }
}

void VersionManager::acquire_read_admission() {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    const OperationGateState state = UnpackOperationGateState(observed);
    if (!IsReaderAllowed(state.phase)) {
      wait_for_admission_change(observed);
      observed = operation_gate_state_.load(std::memory_order_acquire);
      continue;
    }
    if (state.readers == kMaxAdmissionCount) {
      THROW_RUNTIME_ERROR("Reader admission counter exhausted");
    }
    OperationGateState desired = state;
    ++desired.readers;
    const uint64_t packed_desired = PackOperationGateState(desired);
    if (operation_gate_state_.compare_exchange_weak(
            observed, packed_desired, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      RunConcurrencyHook(g_concurrency_test_hooks.after_read_admission);
      return;
    }
  }
}

PublishedReadView VersionManager::load_published_read_view() const {
  const PublishedReadView view = UnpackPublishedReadView(
      published_read_view_.load(std::memory_order_acquire));
  RunConcurrencyHook(g_concurrency_test_hooks.after_read_view_load);
  return view;
}

void VersionManager::release_read_timestamp() {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    const OperationGateState state = UnpackOperationGateState(observed);
    if (state.readers == 0) {
      THROW_INTERNAL_EXCEPTION("release_read_timestamp without admission");
    }
    OperationGateState desired = state;
    --desired.readers;
    if (operation_gate_state_.compare_exchange_weak(
            observed, PackOperationGateState(desired),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      return;
    }
  }
}

uint32_t VersionManager::acquire_insert_timestamp() {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    const OperationGateState state = UnpackOperationGateState(observed);
    if (state.phase != OperationGatePhase::kNormal) {
      wait_for_admission_change(observed);
      observed = operation_gate_state_.load(std::memory_order_acquire);
      continue;
    }
    if (state.inserters == kMaxAdmissionCount) {
      THROW_RUNTIME_ERROR("Inserter admission counter exhausted");
    }
    OperationGateState desired = state;
    ++desired.inserters;
    if (operation_gate_state_.compare_exchange_weak(
            observed, PackOperationGateState(desired),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      try {
        return allocate_write_timestamp();
      } catch (...) {
        release_insert_timestamp(INVALID_TIMESTAMP);
        throw;
      }
    }
  }
}

void VersionManager::release_insert_timestamp(uint32_t ts) {
  if (ts != INVALID_TIMESTAMP) {
    complete_write_timestamp(ts);
  }
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    const OperationGateState state = UnpackOperationGateState(observed);
    if (state.inserters == 0) {
      THROW_INTERNAL_EXCEPTION("release_insert_timestamp without admission");
    }
    OperationGateState desired = state;
    --desired.inserters;
    if (operation_gate_state_.compare_exchange_weak(
            observed, PackOperationGateState(desired),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      return;
    }
  }
}

void VersionManager::complete_write_timestamp(uint32_t ts) {
  RunConcurrencyHook(g_concurrency_test_hooks.before_read_frontier_publish);
  // Mark completion (lock-free atomic operation)
  ts_window_.mark_completed(ts);

  // Check under lock: only advance if ts == read_ts + 1
  std::unique_lock lock(lock_, std::defer_lock);
  if (!lock.try_lock()) {
    AdaptiveBackoff wait(runtime_wait());
    do {
      wait();
    } while (!lock.try_lock());
  }
  uint32_t current_read_ts = read_ts_.load(std::memory_order_relaxed);
  if (ts == current_read_ts + 1) {
    // May need to advance, safe under lock protection
    advance_read_ts_locked();
    // Publish the advanced frontier together with the current view
    // generation as ONE atomic value. This is the visibility linearization
    // point: for a snapshot commit the new slot is already current, so an
    // acquire load of this pair also observes the initialized slot.
    published_read_view_.store(
        PackPublishedReadView(
            {read_ts_.load(std::memory_order_relaxed),
             view_generation_.load(std::memory_order_relaxed)}),
        std::memory_order_release);
  }
}

void VersionManager::advance_read_ts_locked() {
  uint32_t current = read_ts_.load(std::memory_order_relaxed);

  // Advance read_ts
  while (true) {
    uint32_t next_ts = current + 1;

    if (!ts_window_.is_completed(next_ts)) {
      break;  // Next timestamp not completed
    }

    // Clear the advanced bit
    ts_window_.clear(next_ts);
    current = next_ts;
    read_ts_.store(current, std::memory_order_release);
  }

  // Sliding window maintenance
  ts_window_.slide_window(current);
}

uint32_t VersionManager::acquire_write_timestamp(WriteIntent intent) {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    const OperationGateState state = UnpackOperationGateState(observed);
    if (state.phase != OperationGatePhase::kNormal) {
      wait_for_admission_change(observed);
      observed = operation_gate_state_.load(std::memory_order_acquire);
      continue;
    }
    OperationGateState desired = state;
    desired.phase = intent == WriteIntent::kUpdate
                        ? OperationGatePhase::kUpdateExecution
                        : OperationGatePhase::kExclusive;
    if (operation_gate_state_.compare_exchange_weak(
            observed, PackOperationGateState(desired),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      break;
    }
  }

  if (intent == WriteIntent::kCompact) {
    // Compact mutations are exclusive from admission through completion.
    drain_exclusive_admission();
  } else {
    // Inserters admitted before NORMAL -> UPDATE_EXECUTION are represented in
    // the same word and cannot be missed by this wait.
    while (UnpackOperationGateState(
               operation_gate_state_.load(std::memory_order_acquire))
               .inserters != 0) {
      wait_for_admission_change(
          operation_gate_state_.load(std::memory_order_relaxed));
    }
  }
  try {
    return allocate_write_timestamp();
  } catch (...) {
    set_phase(intent == WriteIntent::kUpdate
                  ? OperationGatePhase::kUpdateExecution
                  : OperationGatePhase::kExclusive,
              OperationGatePhase::kNormal);
    throw;
  }
}

void VersionManager::begin_write_commit(uint32_t ts,
                                        WriteCompletion completion) {
  (void) ts;
  switch (completion) {
  case WriteCompletion::kSnapshot:
    set_phase(OperationGatePhase::kUpdateExecution,
              OperationGatePhase::kPublishing);
    return;
  case WriteCompletion::kInPlace:
    set_phase(OperationGatePhase::kUpdateExecution,
              OperationGatePhase::kExclusive);
    drain_exclusive_admission();
    return;
  case WriteCompletion::kNoSnapshot:
    THROW_INTERNAL_EXCEPTION(
        "A no-snapshot write must complete without entering commit");
  }
}

void VersionManager::set_phase(OperationGatePhase expected,
                               OperationGatePhase desired_phase) {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    const OperationGateState state = UnpackOperationGateState(observed);
    if (state.phase != expected) {
      THROW_INTERNAL_EXCEPTION(
          "Invalid transaction admission phase transition");
    }
    OperationGateState desired = state;
    desired.phase = desired_phase;
    if (operation_gate_state_.compare_exchange_weak(
            observed, PackOperationGateState(desired),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      return;
    }
  }
}

void VersionManager::drain_exclusive_admission() {
  const OperationGateState initial = UnpackOperationGateState(
      operation_gate_state_.load(std::memory_order_acquire));
  if (initial.phase != OperationGatePhase::kExclusive) {
    THROW_INTERNAL_EXCEPTION(
        "exclusive admission drain requested outside exclusive state");
  }
  while (true) {
    const uint64_t observed =
        operation_gate_state_.load(std::memory_order_acquire);
    const OperationGateState state = UnpackOperationGateState(observed);
    if (state.phase != OperationGatePhase::kExclusive) {
      THROW_INTERNAL_EXCEPTION("Exclusive admission released while draining");
    }
    if (state.readers == 0 && state.inserters == 0) {
      return;
    }
    wait_for_admission_change(observed);
  }
}

void VersionManager::complete_write(uint32_t ts, WriteCompletion completion,
                                    uint32_t new_view_generation) {
  const OperationGateState state = UnpackOperationGateState(
      operation_gate_state_.load(std::memory_order_acquire));
  switch (completion) {
  case WriteCompletion::kSnapshot:
    if (state.phase != OperationGatePhase::kPublishing) {
      THROW_INTERNAL_EXCEPTION("snapshot completion outside publishing state");
    }
    // The COW snapshot carrying this generation has already been installed
    // as current.  Record it before publishing the read frontier so the
    // packed view never pairs a new frontier with a stale generation.
    view_generation_.store(new_view_generation, std::memory_order_relaxed);
    complete_write_timestamp(ts);
    set_phase(OperationGatePhase::kPublishing, OperationGatePhase::kNormal);
    return;
  case WriteCompletion::kInPlace:
    if (state.phase != OperationGatePhase::kExclusive || state.readers != 0 ||
        state.inserters != 0) {
      THROW_INTERNAL_EXCEPTION(
          "in-place completion requires drained exclusive admission");
    }
    view_generation_.store(new_view_generation, std::memory_order_relaxed);
    complete_write_timestamp(ts);
    set_phase(OperationGatePhase::kExclusive, OperationGatePhase::kNormal);
    return;
  case WriteCompletion::kNoSnapshot:
    if (state.phase != OperationGatePhase::kUpdateExecution &&
        state.phase != OperationGatePhase::kPublishing &&
        state.phase != OperationGatePhase::kExclusive) {
      THROW_INTERNAL_EXCEPTION(
          "no-snapshot completion outside write admission");
    }
    // Abort / no-op: resolve the timestamp gap while retaining the
    // previously published graph generation.
    complete_write_timestamp(ts);
    set_phase(state.phase, OperationGatePhase::kNormal);
    return;
  }
}

}  // namespace neug
