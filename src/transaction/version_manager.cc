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
#include <ostream>
#include <thread>

#include "neug/utils/bitset.h"
#include "neug/utils/likely.h"

namespace neug {

constexpr static uint32_t ring_buf_size = 1024 * 1024;
constexpr static uint32_t ring_index_mask = ring_buf_size - 1;

// TPVersionManager implementation

TPVersionManager::TPVersionManager() {
  buf_.resize(ring_buf_size);
  buf_.reset_all();
}

TPVersionManager::~TPVersionManager() {}

void TPVersionManager::init_ts(uint32_t ts, int thread_num) {
  write_ts_.store(ts + 1);
  read_ts_.store(ts);
  thread_num_ = thread_num;
}

void TPVersionManager::clear() {
  write_ts_.store(1);
  read_ts_.store(0);
  pending_reqs_.store(0);
  buf_.reset_all();
}

uint32_t TPVersionManager::acquire_read_timestamp() {
  int pr = pending_reqs_.fetch_add(1);
  if (NEUG_LIKELY(pr >= 0)) {
    return read_ts_.load();
  } else {
    --pending_reqs_;
    while (true) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      if (pending_reqs_.load(std::memory_order_acquire) >= 0) {
        pr = pending_reqs_.fetch_add(1);
        if (pr >= 0) {
          return read_ts_.load();
        } else {
          --pending_reqs_;
        }
      }
    }
  }
}

void TPVersionManager::release_read_timestamp() { pending_reqs_.fetch_sub(1); }

uint32_t TPVersionManager::acquire_insert_timestamp() {
  rw_mutex_.lock_shared();
  pending_reqs_.fetch_add(1);
  return write_ts_.fetch_add(1);
}

void TPVersionManager::release_insert_timestamp(uint32_t ts) {
  lock_.lock();
  if (ts == read_ts_.load() + 1) {
    while (buf_.atomic_reset_with_ret((ts + 1) & ring_index_mask)) {
      ++ts;
    }
    read_ts_.store(ts);
  } else {
    buf_.atomic_set(ts & ring_index_mask);
  }
  lock_.unlock();

  pending_reqs_.fetch_sub(1);
  rw_mutex_.unlock_shared();
}

uint32_t TPVersionManager::acquire_update_timestamp() {
  rw_mutex_.lock();
  return write_ts_.fetch_add(1);
}

void TPVersionManager::release_update_timestamp(uint32_t ts) {
  lock_.lock();
  if (ts == read_ts_.load() + 1) {
    read_ts_.store(ts);
  } else {
    LOG(ERROR) << "read ts is expected to be " << ts - 1 << ", while it is "
               << read_ts_.load();
    buf_.atomic_set(ts & ring_index_mask);
  }
  lock_.unlock();

  rw_mutex_.unlock();
}

uint32_t TPVersionManager::acquire_compact_timestamp() {
  rw_mutex_.lock();
  pending_reqs_.fetch_sub(thread_num_);
  while (pending_reqs_.load(std::memory_order_acquire) != -thread_num_) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  return write_ts_.fetch_add(1);
}

void TPVersionManager::release_compact_timestamp(uint32_t ts) {
  lock_.lock();
  if (ts == read_ts_.load() + 1) {
    read_ts_.store(ts);
  } else {
    LOG(ERROR) << "read ts is expected to be " << ts - 1 << ", while it is "
               << read_ts_.load();
    buf_.atomic_set(ts & ring_index_mask);
  }
  lock_.unlock();

  pending_reqs_.fetch_add(thread_num_);
  rw_mutex_.unlock();
}

}  // namespace neug
