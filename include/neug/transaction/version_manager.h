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
#pragma once

#include <stdint.h>
#include <atomic>

#include "neug/transaction/runtime_wait.h"
#include "neug/transaction/timestamp_window.h"
#include "neug/utils/spinlock.h"

namespace neug {

/**
 * @brief The authoritative read-visible frontier: visibility timestamp +
 * graph view generation, published and loaded as ONE 64-bit atomic value.
 *
 * Readers must never load the two fields from separate atomics. Encoding is
 * centralized in Pack/UnpackPublishedReadView.
 */
struct PublishedReadView {
  uint32_t visibility_ts;
  uint32_t view_generation;
};

/**
 * @brief The admission acquired for a write before it reaches commit.
 *
 * Update admission permits concurrent reads while the COW graph is built.
 * Compact admission is exclusive from the start.  The distinction is a
 * caller-visible workload choice; the admission phases used to implement it
 * remain private to VersionManager.
 */
enum class WriteIntent : uint8_t {
  kUpdate,
  kCompact,
};

/** @brief The externally observable outcome of a write admission. */
enum class WriteCompletion : uint8_t {
  kSnapshot,
  kInPlace,
  kNoSnapshot,
};

inline uint64_t PackPublishedReadView(const PublishedReadView& view) {
  return (static_cast<uint64_t>(view.view_generation) << 32) |
         view.visibility_ts;
}

inline PublishedReadView UnpackPublishedReadView(uint64_t packed) {
  return PublishedReadView{static_cast<uint32_t>(packed & 0xffffffffu),
                           static_cast<uint32_t>(packed >> 32)};
}

/**
 * @brief Unified interface for transaction timestamp and concurrency control.
 *
 * IVersionManager defines the contract for managing timestamp acquisition,
 * release, and inter-transaction synchronization. Each transaction type
 * (Read, Insert, Update, Compact) interacts with this interface to obtain
 * a timestamp and to coordinate exclusive/shared access with other
 * transaction types.
 *
 * The current implementation is VersionManager, which uses one packed
 * OperationGateState atomic for phase and admission counters, replacing the
 * earlier independent-counter design.
 *
 * Read-view publication:
 * - published_read_view_ is the ONLY read-visible timestamp authority.
 *   Readers register via acquire_read_admission() and obtain their
 *   visibility timestamp exclusively from load_published_read_view(); they
 *   never read a bare read frontier.
 * - Writers express an intent, then a completion outcome.  VersionManager
 *   owns all admission-phase transitions and reader draining; callers never
 *   manipulate those implementation phases directly.
 *
 * @see VersionManager for the concrete implementation and its
 *      concurrency matrix.
 */
class IVersionManager {
 public:
  virtual void init_ts(uint32_t ts, int thread_num) = 0;
  // Lifecycle-only operation. The implementation closes admission while
  // checking quiescence, but the caller must still prevent new transaction
  // attempts so no already-waiting caller retains the previous runtime wait.
  virtual bool try_set_runtime_wait_if_quiescent(
      RuntimeWaitFn runtime_wait) noexcept = 0;
  virtual void acquire_read_admission() = 0;
  virtual void release_read_timestamp() = 0;
  virtual PublishedReadView load_published_read_view() const = 0;
  virtual uint32_t acquire_insert_timestamp() = 0;
  virtual void release_insert_timestamp(uint32_t ts) = 0;
  virtual uint32_t acquire_write_timestamp(WriteIntent intent) = 0;
  virtual void begin_write_commit(uint32_t ts, WriteCompletion completion) = 0;
  virtual void complete_write(uint32_t ts, WriteCompletion completion,
                              uint32_t new_view_generation = 0) = 0;

  virtual ~IVersionManager() {}
};

/**
 * @brief VersionManager — concurrency control via atomic state machine.
 *
 * OperationGateState is one 64-bit CAS word:
 *   phase: 2 bits | active_readers: 31 bits | active_inserters: 31 bits.
 * A reader/inserter admission and a writer gate transition therefore share
 * one modification order; a transition cannot miss an already-admitted
 * operation on weak-memory machines.
 *
 * Concurrency (new acquisitions; in-flight ops are not interrupted):
 *
 *   |               | Read | Insert | Update-exec | Update-commit | Compact |
 *   | Read          | yes  | yes    | yes         |   no*         |   no    |
 *   | Insert        | yes  | yes    |   no        |   no          |   no    |
 *   | Update-exec   | yes  |  no    |   no        |    -          |   no    |
 *   | Update-commit |  no* |  no    |   -         |   no          |   no    |
 *   | Compact       |  no  |  no    |   no        |   no          |   no    |
 *   *New reads wait with the configured adaptive backoff; already-acquired
 *   reads continue.
 *
 * Mechanism:
 * - write_ts_: next available write timestamp (monotonically increasing).
 *   Storage compaction may reset per-record visibility timestamps to zero, but
 *   transaction/WAL timestamps must never be reset within a WAL timeline.
 * - read_ts_: highest timestamp fully committed and visible to all readers.
 * - operation_gate_state_: 0=normal, 1=update-exec (inserters drained),
 *   2=publishing (new reads/inserts block; existing reads continue),
 *   3=exclusive (readers+inserters drained before mutation).
 * - SpinLock lock_: serializes read_ts advancement (check-and-advance
 *   in complete_write_timestamp).
 * - TimestampWindow ts_window_: tracks completed timestamps for read_ts
 * reclamation.
 */
class VersionManager : public IVersionManager {
 public:
  VersionManager();
  ~VersionManager() override = default;

  void init_ts(uint32_t ts, int thread_num) override;
  bool try_set_runtime_wait_if_quiescent(
      RuntimeWaitFn runtime_wait) noexcept override;

  void acquire_read_admission() override;
  void release_read_timestamp() override;
  PublishedReadView load_published_read_view() const override;
  uint32_t acquire_insert_timestamp() override;
  void release_insert_timestamp(uint32_t ts) override;
  uint32_t acquire_write_timestamp(WriteIntent intent) override;
  void begin_write_commit(uint32_t ts, WriteCompletion completion) override;
  void complete_write(uint32_t ts, WriteCompletion completion,
                      uint32_t new_view_generation = 0) override;

 private:
  friend struct VersionManagerTestPeer;

  int thread_num_;
  enum class OperationGatePhase : uint8_t {
    kNormal = 0,
    kUpdateExecution = 1,
    kPublishing = 2,
    kExclusive = 3,
  };

  struct OperationGateState {
    OperationGatePhase phase;
    uint32_t readers;
    uint32_t inserters;
  };

  static constexpr uint32_t kMaxAdmissionCount = 0x7fffffffu;
  static uint64_t PackOperationGateState(OperationGateState state);
  static OperationGateState UnpackOperationGateState(uint64_t packed);
  static bool IsReaderAllowed(OperationGatePhase phase);
  void wait_for_admission_change(uint64_t observed) const;
  uint32_t allocate_write_timestamp();
  void set_phase(OperationGatePhase expected, OperationGatePhase desired);
  void drain_exclusive_admission();
  RuntimeWaitFn runtime_wait() const noexcept;
  void complete_write_timestamp(uint32_t ts);
  void advance_read_ts_locked();

  std::atomic<uint32_t> write_ts_{1};
  // Internal migration aid: the contiguous committed frontier. Readers must
  // NOT use it; the authoritative pair lives in published_read_view_.
  std::atomic<uint32_t> read_ts_{1};
  // The single authoritative read view: {visibility_ts, view_generation}.
  std::atomic<uint64_t> published_read_view_{0};
  // Latest published view generation. Written only by the serialized update
  // committer before complete_write_timestamp; read under lock_ or in the
  // same commit path.
  std::atomic<uint32_t> view_generation_{0};

  std::atomic<uint64_t> operation_gate_state_{0};

  TimestampWindow ts_window_;

  SpinLock lock_;
  std::atomic<RuntimeWaitFn> runtime_wait_;
};

}  // namespace neug
