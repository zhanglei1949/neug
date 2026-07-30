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

#include "neug/storages/graph_snapshot_store.h"

#include <glog/logging.h>
#include <limits>
#include <utility>

#include "neug/generated/proto/plan/error.pb.h"
#include "neug/utils/concurrency_test_hooks.h"

namespace neug {

// Slot-state sentinel: reader_count_ holds a large negative value while the
// slot is unavailable to readers (writer claim in progress or cleanup in
// progress). Non-positive counts are never modified by a pin attempt.
static constexpr int kSlotUnavailable = -(1 << 20);

GraphSnapshotStore::GraphSnapshotStore(
    int slot_num, std::shared_ptr<PropertyGraph> initial_pg,
    uint32_t initial_view_generation, uint32_t initial_schema_generation)
    : slot_num_(slot_num), slots_(slot_num) {
  // Publish initial PG into slot 0.
  //
  // Invariant: while a slot is current, reader_count_ >= 1 (held by the
  // "cur-pin"). InstallPreparedSnapshot transfers the cur-pin from the old slot
  // to the new slot atomically around the cur_slot_index_ switch.
  slots_[0].storage_ = std::move(initial_pg);
  slots_[0].view_ = GraphView(*slots_[0].storage_);
  slots_[0].view_generation_ = initial_view_generation;
  slots_[0].schema_generation_ = initial_schema_generation;
  slots_[0].reader_count_.store(1, std::memory_order_relaxed);  // cur-pin
  cur_slot_index_.store(0, std::memory_order_release);

  initFreeList();
}

GraphSnapshotStore::PreparedSnapshot::PreparedSnapshot(
    PreparedSnapshot&& other) noexcept
    : store_(other.store_), slot_index_(other.slot_index_) {
  other.store_ = nullptr;
  other.slot_index_ = -1;
}

GraphSnapshotStore::PreparedSnapshot&
GraphSnapshotStore::PreparedSnapshot::operator=(
    PreparedSnapshot&& other) noexcept {
  if (this != &other) {
    reset();
    store_ = other.store_;
    slot_index_ = other.slot_index_;
    other.store_ = nullptr;
    other.slot_index_ = -1;
  }
  return *this;
}

void GraphSnapshotStore::PreparedSnapshot::reset() noexcept {
  if (store_ != nullptr) {
    store_->releasePreparedSlot(slot_index_);
    store_ = nullptr;
    slot_index_ = -1;
  }
}

void GraphSnapshotStore::initFreeList() {
  // Slots 1 to slot_num_-1 are initially free
  for (int i = 1; i < slot_num_; ++i) {
    free_list_.push_back(i);
  }
}

int GraphSnapshotStore::getFreeSlot() {
  std::lock_guard<std::mutex> lock(free_list_mutex_);
  if (free_list_.empty()) {
    return -1;  // No free slot
  }
  int slot_index = free_list_.back();
  free_list_.pop_back();
  return slot_index;
}

void GraphSnapshotStore::returnFreeSlot(int slot_index) {
  std::lock_guard<std::mutex> lock(free_list_mutex_);
  free_list_.push_back(slot_index);
}

void GraphSnapshotStore::cleanupSlot(int slot_index) {
  if (slot_index < 0 || slot_index >= slot_num_) {
    return;
  }
  slots_[slot_index].storage_.reset();
  slots_[slot_index].view_ = GraphView();
  slots_[slot_index].reader_count_.fetch_add(-kSlotUnavailable,
                                             std::memory_order_release);
  returnFreeSlot(slot_index);
}

void GraphSnapshotStore::releasePreparedSlot(int slot_index) noexcept {
  if (slot_index < 0 || slot_index >= slot_num_) {
    return;
  }
  auto& slot = slots_[slot_index];
  slot.storage_.reset();
  slot.view_ = GraphView();
  slot.view_generation_ = 0;
  slot.schema_generation_ = 0;
  // A prepared slot is exclusively held at kSlotUnavailable. Returning it to
  // zero before publishing it in the free list makes a stale pin retry rather
  // than observe partially initialized payload.
  slot.reader_count_.store(0, std::memory_order_release);
  returnFreeSlot(slot_index);
}

GraphSnapshotStore::SnapshotSlot&
GraphSnapshotStore::PinCurrentSnapshot() noexcept {
  RunConcurrencyHook(g_concurrency_test_hooks.before_snapshot_pin);
  while (true) {
    int slot_index = cur_slot_index_.load(std::memory_order_acquire);
    RunConcurrencyHook(g_concurrency_test_hooks.after_cur_slot_load);
    auto& slot = slots_[slot_index];

    // Positive-only CAS pin. Invariant: while a slot is current,
    // reader_count_ >= 1 (cur-pin). count <= 0 means the slot is in a
    // writer claim (kSlotUnavailable) or transitioning to free (count == 0);
    // reloading cur and retrying is correct in both cases. A failed CAS
    // never mutates the slot, so a delayed pin can neither resurrect a freed
    // slot (0 -> 1) nor roll a stale decrement into a new incarnation — no
    // compensating rollback exists in this algorithm.
    int count = slot.reader_count_.load(std::memory_order_acquire);
    if (count <= 0) {
      continue;
    }
    if (!slot.reader_count_.compare_exchange_weak(count, count + 1,
                                                  std::memory_order_acquire,
                                                  std::memory_order_relaxed)) {
      continue;
    }

    if (cur_slot_index_.load(std::memory_order_acquire) == slot_index) {
      RunConcurrencyHook(g_concurrency_test_hooks.after_snapshot_pin);
      return slot;
    }

    // The slot stopped being current between the index load and the CAS; the
    // pin itself was valid, so release it normally before retrying.
    UnpinSnapshotByIndex(slot_index);
  }
}

void GraphSnapshotStore::UnpinSnapshot(const SnapshotSlot& slot) noexcept {
  int slot_index = static_cast<int>(&slot - slots_.data());
  UnpinSnapshotByIndex(slot_index);
}

void GraphSnapshotStore::UnpinSnapshotByIndex(int slot_index) noexcept {
  if (slot_index < 0 || slot_index >= slot_num_) {
    LOG(ERROR) << "Invalid slot index in UnpinSnapshot: " << slot_index;
    return;
  }

  int prev_count =
      slots_[slot_index].reader_count_.fetch_sub(1, std::memory_order_acq_rel);
  if (prev_count <= 0) {
    LOG(ERROR) << "UnpinSnapshot called on slot with reader_count <= 0";
    return;
  }

  // If this was the last reader and slot is no longer current, clean it up.
  // Use CAS on reader_count (0 → unavailable) as a cleanup lock. With the
  // positive-only pin CAS, no delayed pin can resurrect a zero-count slot,
  // so this CAS can only fail if cleanup is already claimed; either way we
  // skip cleanup.
  if (prev_count == 1) {
    int current = cur_slot_index_.load(std::memory_order_acquire);
    if (slot_index != current && slots_[slot_index].storage_) {
      int expected = 0;
      if (slots_[slot_index].reader_count_.compare_exchange_strong(
              expected, kSlotUnavailable, std::memory_order_acq_rel)) {
        cleanupSlot(slot_index);
      }
    }
  }
}

const PropertyGraph& GraphSnapshotStore::CurrentSnapshot() const {
  int slot_index = cur_slot_index_.load(std::memory_order_acquire);
  CHECK(slots_[slot_index].storage_ != nullptr);
  return *slots_[slot_index].storage_;
}

uint32_t GraphSnapshotStore::current_schema_generation() const {
  return slots_[cur_slot_index_.load(std::memory_order_acquire)]
      .schema_generation_;
}

void GraphSnapshotStore::AdvanceCurrentSlotSchemaGeneration() {
  auto& generation = slots_[cur_slot_index_.load(std::memory_order_acquire)]
                         .schema_generation_;
  if (generation == std::numeric_limits<uint32_t>::max()) {
    THROW_RUNTIME_ERROR("Schema generation space exhausted");
  }
  ++generation;
}

uint32_t GraphSnapshotStore::NextCurrentSchemaGeneration() const {
  const uint32_t current = current_schema_generation();
  if (current == std::numeric_limits<uint32_t>::max()) {
    THROW_RUNTIME_ERROR("Schema generation space exhausted");
  }
  return current + 1;
}

void GraphSnapshotStore::SetCurrentViewGeneration(uint32_t generation) {
  if (generation == 0) {
    THROW_INTERNAL_EXCEPTION("View generation 0 is reserved after startup");
  }
  auto& slot = slots_[cur_slot_index_.load(std::memory_order_acquire)];
  if (generation <= slot.view_generation_) {
    THROW_INTERNAL_EXCEPTION("View generation must advance monotonically");
  }
  slot.view_generation_ = generation;
}

Status GraphSnapshotStore::PrepareSnapshot(
    const std::shared_ptr<PropertyGraph>& new_pg, uint32_t view_generation,
    uint32_t schema_generation, PreparedSnapshot& prepared) {
  if (prepared.valid()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "PreparedSnapshot output is already active");
  }
  if (new_pg == nullptr || view_generation == 0) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Snapshot and non-zero view generation are required");
  }
  int slot_index = getFreeSlot();
  if (slot_index < 0) {
    return Status(StatusCode::ERR_POOL_EXHAUSTED,
                  "GraphSnapshotStore slot exhausted");
  }
  auto& slot = slots_[slot_index];

  // Claim the free slot with an exact 0 -> unavailable CAS. A free slot
  // always has reader_count_ == 0, and only the serialized writer pops slots
  // from the free list, so the claim cannot fail. While claimed, any
  // concurrent pin that races onto this slot observes count < 0 and retries
  // against cur.
  int expected = 0;
  CHECK(slot.reader_count_.compare_exchange_strong(expected, kSlotUnavailable,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_relaxed));

  try {
    slot.storage_ = new_pg;
    slot.view_ = GraphView(*new_pg);
    slot.view_generation_ = view_generation;
    slot.schema_generation_ = schema_generation;
  } catch (const std::exception& e) {
    releasePreparedSlot(slot_index);
    return Status(StatusCode::ERR_INTERNAL_ERROR,
                  std::string("Failed to prepare graph snapshot: ") + e.what());
  } catch (...) {
    releasePreparedSlot(slot_index);
    return Status(StatusCode::ERR_INTERNAL_ERROR,
                  "Failed to prepare graph snapshot");
  }

  prepared = PreparedSnapshot(this, slot_index);
  return Status::OK();
}

void GraphSnapshotStore::InstallPreparedSnapshot(
    PreparedSnapshot&& prepared) noexcept {
  CHECK(prepared.store_ == this);
  const int slot_index = prepared.slot_index_;
  CHECK_GE(slot_index, 0);
  CHECK_LT(slot_index, slot_num_);
  auto& slot = slots_[slot_index];
  CHECK_EQ(slot.reader_count_.load(std::memory_order_acquire),
           kSlotUnavailable);

  // Publish storage, view, and generations together with the cur-pin: a
  // reader whose acquire CAS observes count >= 1 also observes all fields
  // written above.
  slot.reader_count_.store(1, std::memory_order_release);

  // The old cur slot's count >= 1 (its cur-pin) protects it from premature
  // cleanup across the switch: UnpinSnapshotByIndex only triggers cleanup
  // when prev_count == 1, which cannot happen while the cur-pin is held.
  int old_slot_index = cur_slot_index_.load(std::memory_order_acquire);

  // Switch cur to the new slot.  The new slot already has its cur-pin (= 1)
  // from the store above, so readers that observe the new index will pin
  // successfully.
  RunConcurrencyHook(g_concurrency_test_hooks.before_slot_switch);
  cur_slot_index_.store(slot_index, std::memory_order_release);
  RunConcurrencyHook(g_concurrency_test_hooks.after_slot_switch);

  // Release the old slot's cur-pin now that the new slot is current.
  // If no readers are holding the old slot, prev_count == 1, count drops to
  // 0, and cleanup fires via the CAS inside UnpinSnapshotByIndex.
  // If readers still hold it, cleanup is deferred to the last reader release.
  UnpinSnapshotByIndex(old_slot_index);

  // The new slot's pin becomes its cur-pin — do NOT release it here.

  prepared.store_ = nullptr;
  prepared.slot_index_ = -1;
}

}  // namespace neug
