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
#include <condition_variable>
#include <mutex>
#include <shared_mutex>

#include "neug/utils/bitset.h"
#include "neug/utils/spinlock.h"

namespace neug {

class IVersionManager {
 public:
  virtual void init_ts(uint32_t ts, int thread_num) = 0;
  virtual uint32_t acquire_read_timestamp() = 0;
  virtual void release_read_timestamp() = 0;
  virtual uint32_t acquire_insert_timestamp() = 0;
  virtual void release_insert_timestamp(uint32_t ts) = 0;
  virtual uint32_t acquire_update_timestamp() = 0;
  virtual void release_update_timestamp(uint32_t ts) = 0;
  virtual uint32_t acquire_compact_timestamp() = 0;
  virtual void release_compact_timestamp(uint32_t ts) = 0;
  virtual void clear() = 0;
  virtual ~IVersionManager() {}
};

/**
 * @brief TPVersionManager — concurrency control for the COW design.
 *
 * Concurrency matrix (post-COW refactor):
 *
 *   |          | Read   | Insert | Update | Compact |
 *   | Read     |  yes   |  yes   |  yes   |   no    |
 *   | Insert   |  yes   |  yes   |  no    |   no    |
 *   | Update   |  yes   |  no    |  no    |   no    |
 *   | Compact  |  no    |  no    |  no    |   no    |
 *
 * Implementation:
 *  - `rw_mutex_` (std::shared_mutex):
 *      Insert holds shared, Update holds exclusive. This serializes Update vs
 *      all Insert/Update, while letting Insert run concurrently among
 *      themselves.
 *  - Read takes neither side of `rw_mutex_`. Snapshot isolation is enforced
 *    by SnapshotStore + per-slot timestamps; Read does NOT block Update or
 *    Insert and is not blocked by them.
 *  - `pending_reqs_` continues to track in-flight reads/inserts for use by
 *    timestamp reclamation only — it is NOT used to fence Update against
 *    concurrent reads anymore.
 */
class TPVersionManager : public IVersionManager {
 public:
  TPVersionManager();
  ~TPVersionManager();

  void init_ts(uint32_t ts, int thread_num) override;

  void clear() override;
  uint32_t acquire_read_timestamp() override;
  void release_read_timestamp() override;
  uint32_t acquire_insert_timestamp() override;
  void release_insert_timestamp(uint32_t ts) override;
  uint32_t acquire_update_timestamp() override;
  void release_update_timestamp(uint32_t ts) override;
  uint32_t acquire_compact_timestamp() override;
  void release_compact_timestamp(uint32_t ts) override;

 private:
  int thread_num_;
  std::atomic<uint32_t> write_ts_{1};
  std::atomic<uint32_t> read_ts_{0};

  // In-flight read/insert tracker.
  // Positive: number of active reads/inserts.
  // Negative: compact has subtracted thread_num_ to block new readers.
  std::atomic<int> pending_reqs_{0};
  static constexpr int kCompactBias = 1 << 20;

  Bitset buf_;
  SpinLock lock_;

  // Insert/Update mutual exclusion. Insert acquires shared lock; Update
  // acquires exclusive. Read does not touch this mutex.
  std::shared_mutex rw_mutex_;
};

}  // namespace neug
