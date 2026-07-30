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
#include <utility>

#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/version_manager.h"
#include "neug/utils/property/types.h"

namespace neug {

/**
 * @brief Move-only RAII handle owning ONE coherent read view.
 *
 * A ReadSnapshotLease bundles the three components that must never be
 * acquired independently (read-view publication protocol, see
 * doc/source/transaction/read_view_publication_protocol.md):
 *
 *   ReadView = visibility timestamp + view generation + pinned snapshot
 *
 * It owns the reader admission token and the pinned snapshot together.
 * Acquire() is the only way to construct one, and validates that the pinned
 * slot generation matches the published view generation.
 *
 * Release order is part of the contract (protocol invariant 9): the snapshot
 * pin is released BEFORE the reader admission token. release() is idempotent
 * and noexcept; the destructor, explicit Commit/Abort, and exception paths
 * all funnel through it. Member declaration order mirrors this: snapshot_ is
 * declared last so automatic destruction also unpins first.
 *
 * Future explicit read-only transactions (#779) retain one lease across
 * statements for repeatable read; an auto-commit read is the one-statement
 * special case.
 */
class ReadSnapshotLease {
 public:
  /// Acquire one coherent read view. Retries internally when a COW commit
  /// changes the current snapshot between published-view load and pin.
  static ReadSnapshotLease Acquire(IVersionManager& version_manager,
                                   GraphSnapshotStore& snapshot_store);

  ReadSnapshotLease(ReadSnapshotLease&& other) noexcept
      : vm_(other.vm_),
        published_view_(other.published_view_),
        active_(other.active_),
        snapshot_(std::move(other.snapshot_)) {
    other.vm_ = nullptr;
    other.active_ = false;
  }

  ReadSnapshotLease& operator=(ReadSnapshotLease&& other) noexcept {
    if (this != &other) {
      release();
      vm_ = other.vm_;
      published_view_ = other.published_view_;
      active_ = other.active_;
      snapshot_ = std::move(other.snapshot_);
      other.vm_ = nullptr;
      other.active_ = false;
    }
    return *this;
  }

  ReadSnapshotLease(const ReadSnapshotLease&) = delete;
  ReadSnapshotLease& operator=(const ReadSnapshotLease&) = delete;

  ~ReadSnapshotLease() noexcept { release(); }

  /// Idempotent release: unpin the snapshot first, then unregister the
  /// reader admission (protocol invariant 9).
  void release() noexcept {
    if (!active_) {
      return;
    }
    active_ = false;
    snapshot_.release();
    vm_->release_read_timestamp();
  }

  /// Visibility timestamp of this read view. Records with larger timestamps
  /// are not visible.
  timestamp_t timestamp() const { return published_view_.visibility_ts; }
  /// Graph view generation this lease was validated against.
  uint32_t view_generation() const { return published_view_.view_generation; }
  /// Schema generation of the pinned snapshot (stable while pinned). Used as
  /// the query-cache correctness key component.
  uint32_t schema_generation() const { return snapshot_.schema_generation(); }

  const GraphView& view() const { return snapshot_.view(); }
  const PropertyGraph* graph() const { return snapshot_.graph(); }

  bool valid() const { return active_; }

 private:
  ReadSnapshotLease(IVersionManager& version_manager, SnapshotGuard snapshot,
                    const PublishedReadView& published_view) noexcept
      : vm_(&version_manager),
        published_view_(published_view),
        active_(true),
        snapshot_(std::move(snapshot)) {}

  IVersionManager* vm_;
  PublishedReadView published_view_;
  bool active_;
  // Declared last: automatic destruction unpins the snapshot before the
  // admission bookkeeping above is destroyed (invariant 9).
  SnapshotGuard snapshot_;
};

}  // namespace neug
