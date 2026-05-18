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

#include "neug/storages/snapshot_store.h"

#include <glog/logging.h>

#include "neug/generated/proto/plan/error.pb.h"

namespace neug {

SnapshotStore::SnapshotStore(int slot_num,
                             std::shared_ptr<PropertyGraph> initial_pg,
                             timestamp_t initial_ts)
    : slot_num_(slot_num), slots_(slot_num) {
  // Install initial PG into slot 0
  slots_[0].storage_ = std::move(initial_pg);
  slots_[0].view_ = GraphView(*slots_[0].storage_);
  slots_[0].reader_count_ = 0;
  cur_slot_index_ = 0;

  initFreeList();
}

SnapshotStore::~SnapshotStore() {
  for (auto& slot : slots_) {
    slot.storage_.reset();
    slot.view_ = GraphView();
  }
}

void SnapshotStore::initFreeList() {
  // Slots 1 to slot_num_-1 are initially free
  for (int i = 1; i < slot_num_; ++i) {
    free_list_.push_back(i);
  }
}

int SnapshotStore::getFreeSlot() {
  std::lock_guard<std::mutex> lock(free_list_mutex_);
  if (free_list_.empty()) {
    return -1;  // No free slot
  }
  int slot_index = free_list_.back();
  free_list_.pop_back();
  return slot_index;
}

void SnapshotStore::returnFreeSlot(int slot_index) {
  std::lock_guard<std::mutex> lock(free_list_mutex_);
  free_list_.push_back(slot_index);
}

void SnapshotStore::cleanupSlot(int slot_index) {
  if (slot_index < 0 || slot_index >= slot_num_) {
    return;
  }
  slots_[slot_index].storage_.reset();
  slots_[slot_index].view_ = GraphView();
  returnFreeSlot(slot_index);
}

SnapshotStore::StorageSlot& SnapshotStore::acquireSnapshot() {
  // Hold commit_lock_ shared to read cur_slot_index_ and pin the slot.
  // The slot already holds a pre-built GraphView — no per-call construction.
  std::shared_lock<std::shared_mutex> lock(commit_lock_);
  int slot_index = cur_slot_index_.load();
  slots_[slot_index].reader_count_.fetch_add(1);
  return slots_[slot_index];
}

void SnapshotStore::releaseSnapshot(const StorageSlot& slot) {
  int slot_index = static_cast<int>(&slot - slots_.data());
  releaseSnapshotByIndex(slot_index);
}

void SnapshotStore::releaseSnapshotByIndex(int slot_index) {
  if (slot_index < 0 || slot_index >= slot_num_) {
    LOG(ERROR) << "Invalid slot index in releaseSnapshot: " << slot_index;
    return;
  }

  int prev_count = slots_[slot_index].reader_count_.fetch_sub(1);
  if (prev_count <= 0) {
    LOG(ERROR) << "releaseSnapshot called on slot with reader_count <= 0";
    return;
  }

  // If this was the last reader and slot is no longer current, clean it up
  int current = cur_slot_index_.load();
  if (prev_count == 1 && slot_index != current && slots_[slot_index].storage_) {
    cleanupSlot(slot_index);
  }
}

const PropertyGraph& SnapshotStore::currentSnapshot() const {
  int slot_index = cur_slot_index_.load();
  CHECK(slots_[slot_index].storage_ != nullptr);
  return *slots_[slot_index].storage_;
}

Status SnapshotStore::installSnapshot(
    const std::shared_ptr<PropertyGraph>& new_pg, timestamp_t commit_ts) {
  // 1. Reserve a free slot first. On failure, return early WITHOUT touching
  //    @p new_pg so the caller can retry or abort the transaction.
  int slot_index = getFreeSlot();
  if (slot_index < 0) {
    return Status(StatusCode::ERR_POOL_EXHAUSTED,
                  "SnapshotStore slot exhausted");
  }

  // 2. Write new PG to the reserved slot. The slot was just popped from the
  //    free list and is not visible to readers yet, so plain writes are safe.
  slots_[slot_index].storage_ = new_pg;
  slots_[slot_index].view_ = GraphView(*new_pg);
  slots_[slot_index].reader_count_ = 0;

  // 3. Switch cur_slot_index_ atomically while phantom-pinning the old slot.
  //    commit_lock_ ensures acquireSnapshot sees a consistent (cur_slot,
  //    reader_count++) pairing across the switch.
  std::unique_lock<std::shared_mutex> lock(commit_lock_);
  int old_slot_index = cur_slot_index_.load();
  slots_[old_slot_index].reader_count_.fetch_add(1);
  cur_slot_index_.store(slot_index);
  lock.unlock();

  releaseSnapshotByIndex(old_slot_index);
  return Status::OK();
}

}  // namespace neug