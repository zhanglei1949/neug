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

#include "neug/transaction/read_snapshot_lease.h"

#include <thread>

namespace neug {

ReadSnapshotLease ReadSnapshotLease::Acquire(
    IVersionManager& version_manager, GraphSnapshotStore& snapshot_store) {
  for (;;) {
    version_manager.acquire_read_admission();
    const PublishedReadView published =
        version_manager.load_published_read_view();
    SnapshotGuard snapshot(snapshot_store);

    if (snapshot.view_generation() == published.view_generation) {
      return ReadSnapshotLease(version_manager, std::move(snapshot), published);
    }

    // A COW commit changed the current slot after the published-view load.
    // Release in protocol order before retrying so an exclusive writer is not
    // delayed by a reader that cannot return a coherent view.
    snapshot.release();
    version_manager.release_read_timestamp();
    std::this_thread::yield();
  }
}

}  // namespace neug
