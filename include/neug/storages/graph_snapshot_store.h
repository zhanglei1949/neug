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

#include <stddef.h>
#include <stdint.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/result.h"

namespace neug {

class IVersionManager;
class SnapshotGuard;

/**
 * @brief Fixed-size slot pool for MVCC PropertyGraph snapshots.
 *
 * Maintains `slot_num` slots. `cur_slot_index_` marks the active slot.
 * Readers pin through SnapshotGuard (refcounted). Stale slots are recycled
 * when the last guard releases.
 *
 * Transaction usage:
 * - Read/Insert: SnapshotGuard -> view/graph access -> release().
 *   InsertTransaction mutates the live slot in-place (timestamp-filtered).
 * - Update: CurrentSnapshot().Clone() -> mutate COW copy ->
 * PrepareSnapshot() / InstallPreparedSnapshot().
 *
 * Concurrency:
 * - Lock-free internal pinning via positive-only CAS loop: a failed pin
 *   never mutates the slot, so a delayed pin can neither resurrect a freed
 *   slot nor roll a stale decrement into a new incarnation.
 * - Concurrent installs are NOT safe — VersionManager serializes writers
 *   through typed write admission, ensuring only one update/compact can be
 *   in progress at a time.
 * - InstallPreparedSnapshot publishes the new slot BEFORE VersionManager
 * advances the visible read frontier, so readers never see "new ts + old slot".
 *
 * Generations:
 * - Each slot incarnation carries a view generation (and a schema
 *   generation), written by PrepareSnapshot before the slot becomes
 *   pinnable and stable until the incarnation is reclaimed.
 * - Slot index reuse does NOT imply snapshot identity; readers validate
 *   their pinned slot's generation against the published read view (see
 *   ReadSnapshotLease::Acquire) instead of comparing slot indexes.
 * - The schema generation advances ONLY on schema/catalog changes and is
 *   the query-cache correctness key component (read-view publication
 *   protocol, Phase 5). It advances via either:
 *   - PrepareSnapshot with a bumped generation (TP COW update commit whose
 *     WAL changed the schema); or
 *   - AdvanceCurrentSlotSchemaGeneration (AP in-place schema mutations
 *     that publish no snapshot: direct kSchema execution, temporary-schema
 *     cleanup), under an exclusive admission with readers drained.
 *   The current slot's schema generation is therefore monotonically
 *   non-decreasing across the database's lifetime in one process.
 */
class GraphSnapshotStore {
 public:
  class PreparedSnapshot {
   public:
    PreparedSnapshot() = default;
    ~PreparedSnapshot() noexcept { reset(); }
    PreparedSnapshot(const PreparedSnapshot&) = delete;
    PreparedSnapshot& operator=(const PreparedSnapshot&) = delete;
    PreparedSnapshot(PreparedSnapshot&& other) noexcept;
    PreparedSnapshot& operator=(PreparedSnapshot&& other) noexcept;

    bool valid() const { return store_ != nullptr; }
    void reset() noexcept;

   private:
    friend class GraphSnapshotStore;
    PreparedSnapshot(GraphSnapshotStore* store, int slot_index) noexcept
        : store_(store), slot_index_(slot_index) {}
    GraphSnapshotStore* store_{nullptr};
    int slot_index_{-1};
  };

 private:
  /// A slot holding a PropertyGraph, its GraphView, and a pin count. Slot
  /// identity and pin state are implementation details; clients use
  /// SnapshotGuard instead.
  class SnapshotSlot {
   public:
    SnapshotSlot() = default;
    ~SnapshotSlot() = default;

    // Non-copyable, non-movable: slots live in a fixed-size vector and are
    // accessed exclusively by pointer/reference. The atomic reader_count_
    // also prevents implicit copy/move, but we state it explicitly for
    // clarity.
    SnapshotSlot(const SnapshotSlot&) = delete;
    SnapshotSlot& operator=(const SnapshotSlot&) = delete;
    SnapshotSlot(SnapshotSlot&&) = delete;
    SnapshotSlot& operator=(SnapshotSlot&&) = delete;

   private:
    friend class GraphSnapshotStore;
    friend class SnapshotGuard;
    std::shared_ptr<PropertyGraph> storage_;
    GraphView view_;
    uint32_t view_generation_{0};
    uint32_t schema_generation_{0};
    std::atomic<int> reader_count_{0};
  };

 public:
  /// @param slot_num  Pool capacity (default 128).
  /// @param initial_pg Published into slot 0.
  /// @param initial_view_generation Generation tagged on slot 0; must equal
  /// the initial published read view's generation (recovery frontier).
  /// @param initial_schema_generation Schema generation tagged on slot 0.
  explicit GraphSnapshotStore(int slot_num,
                              std::shared_ptr<PropertyGraph> initial_pg,
                              uint32_t initial_view_generation = 0,
                              uint32_t initial_schema_generation = 0);

  ~GraphSnapshotStore() = default;

  /// Current PropertyGraph (for UpdateTransaction to Clone).
  /// No lock — VersionManager guarantees exclusive update access
  /// (operation gate in update-execution phase, all inserters drained).
  const PropertyGraph& CurrentSnapshot() const;

  /// Schema generation of the CURRENT slot: the latest committed schema
  /// generation. Callers must hold the admission that keeps the current
  /// slot stable (update-exec, exclusive write with drained readers, or
  /// the database mutex).
  uint32_t current_schema_generation() const;

  /// EXCLUSIVE-only in-place schema generation bump for AP schema
  /// mutations that publish no snapshot (direct kSchema execution,
  /// temporary-schema cleanup). The caller must hold an exclusive update
  /// admission with readers drained, so no reader can observe the field
  /// mid-write; readers pinning afterwards synchronize via reader_count_
  /// and observe the new value.
  void AdvanceCurrentSlotSchemaGeneration();

  /// Returns the next schema generation without modifying the slot. Callers
  /// use this before any fallible/Durable operation so overflow cannot leave
  /// an already-mutated graph without a publishable generation.
  uint32_t NextCurrentSchemaGeneration() const;

  /// Reserve and fully initialize a slot before WAL append. On success the
  /// returned RAII object owns an unavailable slot; reset() rolls it back.
  /// InstallPreparedSnapshot is noexcept and is the only post-WAL operation.
  Status PrepareSnapshot(const std::shared_ptr<PropertyGraph>& new_pg,
                         uint32_t view_generation, uint32_t schema_generation,
                         PreparedSnapshot& prepared);

  /// Installs a prepared slot as current. No allocation, validation, or
  /// normal error path is permitted here; unexpected invariant breakage is
  /// fail-stop because WAL has already been appended.
  void InstallPreparedSnapshot(PreparedSnapshot&& prepared) noexcept;

 private:
  friend class SnapshotGuard;
  friend void CompleteInPlaceCommit(IVersionManager& version_manager,
                                    GraphSnapshotStore& snapshot_store,
                                    uint32_t timestamp,
                                    uint32_t view_generation);

  /// Pin the current slot via lock-free positive-only CAS loop. SnapshotGuard
  /// is the only public owner of this raw pin.
  SnapshotSlot& PinCurrentSnapshot() noexcept;
  void UnpinSnapshot(const SnapshotSlot& slot) noexcept;
  void SetCurrentViewGeneration(uint32_t generation);

  int slot_num_;
  std::vector<SnapshotSlot> slots_;
  std::atomic<int> cur_slot_index_{0};
  std::vector<int> free_list_;
  mutable std::mutex free_list_mutex_;

  void initFreeList();
  int getFreeSlot();
  void returnFreeSlot(int slot_index);
  void UnpinSnapshotByIndex(int slot_index) noexcept;
  void cleanupSlot(int slot_index);
  void releasePreparedSlot(int slot_index) noexcept;
};

/**
 * @brief RAII guard for an internal GraphSnapshotStore pin.
 *
 * Ensures the pinned slot is always released, even on exception paths.
 * Call release() to explicitly unpin early; the destructor is a no-op
 * after release().
 */
class SnapshotGuard {
 public:
  explicit SnapshotGuard(GraphSnapshotStore& store) noexcept
      : store_(&store), slot_(&store.PinCurrentSnapshot()) {}

  ~SnapshotGuard() noexcept {
    if (slot_) {
      store_->UnpinSnapshot(*slot_);
    }
  }

  SnapshotGuard(const SnapshotGuard&) = delete;
  SnapshotGuard& operator=(const SnapshotGuard&) = delete;

  SnapshotGuard(SnapshotGuard&& other) noexcept
      : store_(other.store_), slot_(other.slot_) {
    other.slot_ = nullptr;
  }

  SnapshotGuard& operator=(SnapshotGuard&& other) noexcept {
    if (this != &other) {
      if (slot_) {
        store_->UnpinSnapshot(*slot_);
      }
      store_ = other.store_;
      slot_ = other.slot_;
      other.slot_ = nullptr;
    }
    return *this;
  }

  /// Generation of the pinned slot incarnation (stable while pinned).
  uint32_t view_generation() const { return slot_->view_generation_; }
  uint32_t schema_generation() const { return slot_->schema_generation_; }

  const GraphView& view() const { return slot_->view_; }
  const PropertyGraph* graph() const { return slot_->storage_.get(); }
  GraphView& mutable_view() { return slot_->view_; }
  PropertyGraph* mutable_graph() { return slot_->storage_.get(); }

  bool valid() const { return slot_ != nullptr; }

  void release() noexcept {
    if (slot_) {
      store_->UnpinSnapshot(*slot_);
      slot_ = nullptr;
    }
  }

 private:
  GraphSnapshotStore* store_;
  GraphSnapshotStore::SnapshotSlot* slot_;
};

}  // namespace neug
