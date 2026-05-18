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
#include <shared_mutex>
#include <vector>

#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/result.h"

namespace neug {

/**
 * @brief SnapshotStore manages multiple versions of PropertyGraph for MVCC.
 *
 * SnapshotStore is the core component for snapshot isolation in NeuG. It
 * maintains a ring buffer of PropertyGraph slots (default 128), allowing:
 * - Read/Insert transactions to acquire a snapshot StorageSlot with a cached
 * GraphView
 * - Update transactions to install new snapshots after commit
 * - Old slots to be released when no readers are holding them
 *
 * **Key Design:**
 * - acquireSnapshot(): Pins the current slot and returns a reference to it.
 *   The slot holds a pre-built GraphView — no per-call construction.
 * - releaseSnapshot(slot): Decrements reader count, may trigger slot cleanup.
 *   Derives slot_index via pointer arithmetic (&slot - slots_.data()).
 * - installSnapshot(): Writes new PG + GraphView to a slot, switches cur.
 * - currentSnapshot(): Returns current PG for UpdateTransaction to Fork.
 *
 * **Concurrency Model:**
 * - commit_lock_: Shared for acquireSnapshot, Exclusive for installSnapshot
 * - Multiple reads can acquire snapshot concurrently
 * - installSnapshot blocks new reads briefly (only during slot switch)
 * - Existing reads on old slots continue unaffected
 *
 * @since v0.1.0
 */
class SnapshotStore {
 public:
  /**
   * @brief A slot in the snapshot ring buffer.
   *
   * Each slot holds a PropertyGraph, a pre-built GraphView over it, and an
   * atomic reader count for pin-based lifetime management.
   *
   * Callers obtain a StorageSlot& from acquireSnapshot() and access the
   * graph via view() / pg().
   */
  class StorageSlot {
   public:
    StorageSlot() = default;
    ~StorageSlot() = default;

    // Non-copyable, non-movable: slots live in a fixed-size vector and are
    // accessed exclusively by pointer/reference. The atomic reader_count_
    // also prevents implicit copy/move, but we state it explicitly for
    // clarity.
    StorageSlot(const StorageSlot&) = delete;
    StorageSlot& operator=(const StorageSlot&) = delete;
    StorageSlot(StorageSlot&&) = delete;
    StorageSlot& operator=(StorageSlot&&) = delete;

    /// Read-only view accessor.
    const GraphView& view() const { return view_; }
    /// Mutable view accessor (for InsertTransaction / AP write path).
    GraphView& mutable_view() { return view_; }
    /// Mutable PropertyGraph pointer (storage_.get() yields T* regardless
    /// of shared_ptr constness, so this works through const StorageSlot& too).
    PropertyGraph* pg() const { return storage_.get(); }

    /// Rebuild the cached GraphView from the current PropertyGraph state.
    /// Must be called after in-place PG mutations (DDL, AP writes) that
    /// change the schema or add vertex/edge types, so that subsequent
    /// acquireSnapshot() callers see the updated view.
    void refreshView() {
      if (storage_) {
        view_ = GraphView(*storage_);
      }
    }

   private:
    friend class SnapshotStore;
    std::shared_ptr<PropertyGraph> storage_;
    GraphView view_;
    std::atomic<int> reader_count_{0};
  };

  /**
   * @brief Construct SnapshotStore with specified slot count.
   *
   * @param slot_num Number of slots in ring buffer (default 128)
   * @param initial_pg Initial PropertyGraph to install in slot 0
   * @param initial_ts Initial timestamp for the first snapshot
   */
  explicit SnapshotStore(int slot_num,
                         std::shared_ptr<PropertyGraph> initial_pg,
                         timestamp_t initial_ts);

  ~SnapshotStore();

  /**
   * @brief Acquire a snapshot for any transaction type.
   *
   * Pins the current slot (reader_count++) and returns a reference to it.
   * The slot's pre-built GraphView is available via slot.view(). The caller
   * supplies the read timestamp to individual view methods as needed.
   *
   * Caller must call releaseSnapshot(slot) when done.
   *
   * @return StorageSlot& reference to the pinned slot.
   *
   * @note This method acquires commit_lock_ shared lock briefly.
   */
  StorageSlot& acquireSnapshot();

  /**
   * @brief Release a snapshot slot after transaction ends.
   *
   * Decrements reader_count. If count reaches 0 and slot is not current,
   * the slot is cleaned up and returned to free_list.
   *
   * @param slot The StorageSlot reference returned by acquireSnapshot().
   *             slot_index is derived via &slot - slots_.data().
   */
  void releaseSnapshot(const StorageSlot& slot);

  /**
   * @brief Get current PropertyGraph for UpdateTransaction to Fork.
   *
   * Returns reference to the PropertyGraph in current slot. Update transaction
   * should call this to get the base for COW Fork.
   *
   * @return const PropertyGraph& Reference to current PG
   *
   * @note No lock needed - Update holds write_mutex_ exclusively
   */
  const PropertyGraph& currentSnapshot() const;

  /**
   * @brief Install a new snapshot from UpdateTransaction commit.
   *
   * Called by UpdateTransaction::Commit() after WAL flush.
   * This method:
   * 1. Acquires a free slot from free_list. On exhaustion returns an error
   *    WITHOUT touching @p new_pg, so the caller can retry/abort safely.
   * 2. Writes the new PG and builds a GraphView into the slot.
   * 3. Switches cur_slot_index_ atomically.
   *
   * It does NOT eagerly try to clean up the old slot. Old slots are recycled
   * solely by the last reader's releaseSnapshot() call, eliminating the
   * race between install-side and release-side cleanup.
   *
   * @param new_pg The COW PropertyGraph from UpdateTransaction. Taken by
   *               const-ref and only moved internally after the free slot has
   *               been reserved.
   * @param commit_ts Commit timestamp for the new snapshot.
   * @return Status OK on success, error if slot exhausted (caller's @p new_pg
   *         remains valid).
   */
  Status installSnapshot(const std::shared_ptr<PropertyGraph>& new_pg,
                         timestamp_t commit_ts);

  /**
   * @brief Get current slot index.
   *
   * @return int Current slot index
   */
  int currentSlotIndex() const { return cur_slot_index_.load(); }

  /**
   * @brief Get number of slots.
   *
   * @return int Number of slots
   */
  int slotNum() const { return slot_num_; }

 private:
  int slot_num_;
  std::vector<StorageSlot> slots_;
  std::atomic<int> cur_slot_index_{0};
  std::vector<int> free_list_;
  std::mutex free_list_mutex_;

  mutable std::shared_mutex commit_lock_;

  // Initialize free_list with all slots except slot 0
  void initFreeList();

  // Get a free slot index from free_list
  int getFreeSlot();

  // Return a slot to free_list
  void returnFreeSlot(int slot_index);

  // Release by slot_index (internal use by installSnapshot phantom-pin).
  void releaseSnapshotByIndex(int slot_index);

  // Clean up a slot (clear storage + view). Caller must hold logical exclusive
  // ownership of the slot (e.g., it just transitioned reader_count to 0 and
  // is no longer the current slot).
  void cleanupSlot(int slot_index);
};

}  // namespace neug