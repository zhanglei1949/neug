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

#include <functional>

namespace neug {

/**
 * @brief Deterministic test hooks for the transaction/snapshot concurrency
 * protocol.
 *
 * Production code invokes these at protocol boundaries. They are empty by
 * default (one branch cost) and are intended for tests that script specific
 * interleavings with latches/promises instead of sleeps, e.g. the issue-#793
 * old-timestamp/new-snapshot regression test.
 *
 * NOT thread-safe to install or reset: set hooks before starting worker
 * threads and Reset() only after joining them.
 */
struct ConcurrencyTestHooks {
  /// VersionManager: reader admission registered, about to return.
  std::function<void()> after_read_admission;
  /// VersionManager: published read view loaded, about to return.
  std::function<void()> after_read_view_load;
  /// VersionManager: about to advance/publish the visible read frontier.
  std::function<void()> before_read_frontier_publish;
  /// GraphSnapshotStore: PinCurrentSnapshot entered / about to return.
  std::function<void()> before_snapshot_pin;
  std::function<void()> after_snapshot_pin;
  /// GraphSnapshotStore: cur slot index loaded inside the pin loop. Tests use
  /// this to park a reader between the index load and the pin CAS
  /// (delayed-pin interleavings).
  std::function<void()> after_cur_slot_load;
  /// GraphSnapshotStore: prepared snapshot installation around the cur-slot
  /// switch.
  std::function<void()> before_slot_switch;
  std::function<void()> after_slot_switch;

  void Reset() { *this = ConcurrencyTestHooks(); }
};

inline ConcurrencyTestHooks g_concurrency_test_hooks;

inline void RunConcurrencyHook(const std::function<void()>& hook) {
  if (hook) {
    hook();
  }
}

}  // namespace neug
