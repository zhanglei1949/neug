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

// Concurrency tests for GraphSnapshotStore at the unit level (no NeugDB
// execution-slot layer). Validates the lock-window invariants documented in
// graph_snapshot_store.h: phantom-pin, last-reader-cleans rule, pool exhaustion
// safety, and the guard/install interlock.
//
// Run under thread sanitizer to surface data races:
//   cmake -DENABLE_THREAD_SANITIZER=ON -DBUILD_TEST=ON ..
//   make graph_snapshot_store_test
//   TSAN_OPTIONS="halt_on_error=1" ./tests/storage/graph_snapshot_store_test

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <limits>
#include <random>
#include <thread>
#include <vector>

#include "neug/generated/proto/plan/error.pb.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/utils/concurrency_test_hooks.h"
#include "neug/utils/result.h"
#include "unittest/utils.h"

namespace neug {

namespace {

Status InstallPreparedSnapshotForTest(
    GraphSnapshotStore& store, const std::shared_ptr<PropertyGraph>& graph,
    uint32_t view_generation, uint32_t schema_generation) {
  GraphSnapshotStore::PreparedSnapshot prepared;
  auto status = store.PrepareSnapshot(graph, view_generation, schema_generation,
                                      prepared);
  if (!status.ok()) {
    return status;
  }
  store.InstallPreparedSnapshot(std::move(prepared));
  return Status::OK();
}

}  // namespace

class GraphSnapshotStoreConcurrencyTest : public ::testing::Test {
 protected:
  static constexpr int kSlotNum = 128;
  std::string work_dir_;
  CheckpointManager checkpoint_mgr_;
  std::shared_ptr<PropertyGraph> initial_pg_;
  std::unique_ptr<GraphSnapshotStore> store_;

  void SetUp() override {
    work_dir_ = std::string("/tmp/test_graph_snapshot_store_concurrency_") +
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
    std::filesystem::create_directories(work_dir_);

    checkpoint_mgr_.Open(work_dir_);
    auto ckp = make_checkpoint(checkpoint_mgr_);

    initial_pg_ = std::make_shared<PropertyGraph>();
    initial_pg_->Open(ckp, MemoryLevel::kInMemory);

    CreateVertexTypeParamBuilder person_builder;
    auto status = initial_pg_->CreateVertexType(
        person_builder.VertexLabel("person")
            .AddProperty("id", neug::Value::INT64(0))
            .AddPrimaryKeyName("id")
            .Build());
    ASSERT_TRUE(status.ok());

    store_ = std::make_unique<GraphSnapshotStore>(kSlotNum, initial_pg_);
  }

  void TearDown() override {
    g_concurrency_test_hooks.Reset();
    store_.reset();
    initial_pg_.reset();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
  }

  // Build a fresh PG suitable for prepared installation. Clones from
  // initial_pg_ (held alive by the fixture) rather than from
  // store_->CurrentSnapshot() to avoid racing on a slot that may be freed under
  // concurrent publishes.
  std::shared_ptr<PropertyGraph> make_new_pg() const {
    return initial_pg_->Clone();
  }

  // Publish with a monotonically increasing view generation, mirroring the
  // TP commit convention (view generation = update timestamp).
  Status publish(std::shared_ptr<PropertyGraph> pg) {
    return InstallPreparedSnapshotForTest(*store_, pg, ++last_view_generation_,
                                          /*schema_generation=*/0);
  }

  uint32_t last_view_generation_{0};
};

// 1. Heavy concurrent guard acquisition vs publish. Verifies SnapshotGuard
// always returns a handle whose view points to a live PG with the seeded
// schema — i.e., readers never observe a freed or partially-published slot.
// TSan catches the data races; this assertion catches the high-level break.
TEST_F(GraphSnapshotStoreConcurrencyTest,
       GuardAcquisitionIsConsistentUnderInstallRace) {
  constexpr int kReaders = 16;
  constexpr int kWriters = 4;
  constexpr auto kDuration = std::chrono::milliseconds(500);

  std::atomic<bool> stop{false};
  std::atomic<int64_t> pins_done{0};
  std::atomic<int64_t> publishes_done{0};

  std::mutex publish_mutex;
  std::vector<std::thread> threads;
  for (int i = 0; i < kReaders; ++i) {
    threads.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        SnapshotGuard guard(*store_);
        ASSERT_GE(guard.view().schema().vertex_label_num(), 1u);
        pins_done.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (int i = 0; i < kWriters; ++i) {
    threads.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        auto pg = make_new_pg();
        // Prepared installation is NOT safe for concurrent calls — caller must
        // serialize externally (see GraphSnapshotStore class doc).
        std::lock_guard<std::mutex> lock(publish_mutex);
        auto status = publish(pg);
        if (status.ok()) {
          publishes_done.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  std::this_thread::sleep_for(kDuration);
  stop.store(true);
  for (auto& t : threads)
    t.join();

  EXPECT_GT(pins_done.load(), 0);
  EXPECT_GT(publishes_done.load(), 0);

  // Final bookkeeping: a fresh guard works cleanly.
  SnapshotGuard guard(*store_);
  EXPECT_GE(guard.view().schema().vertex_label_num(), 1u);
}

// 2. Hold N reader pins on the original cur slot, publish N new snapshots,
// then release all readers. Verify the original slot is reclaimed by the
// last-reader-cleans rule and the pool fully recovers.
TEST_F(GraphSnapshotStoreConcurrencyTest, PhantomPinPreventsCleanupLeak) {
  constexpr int kReaders = 32;

  std::vector<SnapshotGuard> pinned_slots;
  pinned_slots.reserve(kReaders);
  for (int i = 0; i < kReaders; ++i) {
    pinned_slots.emplace_back(*store_);
  }

  // Publish kReaders new snapshots while readers continue holding slot 0.
  for (int i = 0; i < kReaders; ++i) {
    auto status = publish(make_new_pg());
    ASSERT_TRUE(status.ok()) << "publish " << i << " failed";
  }

  // Release all reader pins on slot 0 in reverse order. The last release
  // should trigger slot 0 cleanup.
  for (int i = kReaders - 1; i >= 0; --i) {
    pinned_slots[i].release();
  }

  // Verify no slot leak: we should be able to do (kSlotNum - 2) more
  // publishes back-to-back without exhausting the pool. (1 slot is the live
  // cur; all others must have been recycled.)
  for (int i = 0; i < kSlotNum - 2; ++i) {
    auto status = publish(make_new_pg());
    ASSERT_TRUE(status.ok())
        << "publish " << i << " failed — pool leak suspected";
  }
}

// 3. Last-reader-cleans rule under randomized release order. Variant of test
// 2 with explicit shuffle to exercise interleaved release ordering.
TEST_F(GraphSnapshotStoreConcurrencyTest, LastReaderCleansRule) {
  std::vector<SnapshotGuard> pins;
  for (int i = 0; i < 3; ++i) {
    pins.emplace_back(*store_);
  }

  // Publish slot B → phantom-pin on slot 0, then release. Slot 0 still has 3
  // real reader pins, so it survives.
  ASSERT_TRUE(publish(make_new_pg()).ok());

  std::mt19937 gen(12345);
  std::shuffle(pins.begin(), pins.end(), gen);
  for (auto& guard : pins) {
    guard.release();
  }

  // Slot 0 must now be in the free pool. Verify by exhausting publishes.
  for (int i = 0; i < kSlotNum - 2; ++i) {
    auto status = publish(make_new_pg());
    ASSERT_TRUE(status.ok())
        << "publish " << i << " failed — slot 0 leak suspected";
  }
}

// 4. Pool exhaustion must return ERR_POOL_EXHAUSTED without consuming the
// new_pg shared_ptr passed in (caller can retry/abort).
TEST_F(GraphSnapshotStoreConcurrencyTest, PoolExhaustionDoesNotConsumeNewPg) {
  // Pin all kSlotNum slots: pin cur, publish, repeat. Each iteration
  // pins the about-to-become-non-cur slot before swapping cur.
  std::vector<SnapshotGuard> pinned;
  pinned.emplace_back(*store_);
  for (int i = 0; i < kSlotNum - 1; ++i) {
    ASSERT_TRUE(publish(make_new_pg()).ok())
        << "setup publish " << i << " failed";
    pinned.emplace_back(*store_);
  }

  // All slots pinned; one more publish must fail with ERR_POOL_EXHAUSTED.
  auto extra_pg = make_new_pg();
  long use_count_before = extra_pg.use_count();
  auto status = publish(extra_pg);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), StatusCode::ERR_POOL_EXHAUSTED);
  EXPECT_EQ(extra_pg.use_count(), use_count_before)
      << "PrepareSnapshot must not consume new_pg on failure";

  // Release all pins so TearDown can run cleanly.
  for (auto& guard : pinned) {
    guard.release();
  }
}

// 5. Reader throughput stays positive under heavy concurrent publish
// traffic; no deadlock between commit_lock_ shared and exclusive holders.
TEST_F(GraphSnapshotStoreConcurrencyTest, PublishExclusivityVsPinShared) {
  constexpr int kReaders = 8;
  constexpr int kPublishes = 500;
  constexpr auto kReaderDuration = std::chrono::milliseconds(200);

  std::atomic<bool> readers_stop{false};
  std::atomic<int64_t> reads_done{0};
  std::vector<std::thread> readers;
  for (int i = 0; i < kReaders; ++i) {
    readers.emplace_back([&] {
      while (!readers_stop.load(std::memory_order_relaxed)) {
        SnapshotGuard guard(*store_);
        reads_done.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  std::thread writer([&] {
    for (int i = 0; i < kPublishes; ++i) {
      (void) publish(make_new_pg());
    }
  });

  std::this_thread::sleep_for(kReaderDuration);
  readers_stop.store(true);
  writer.join();
  for (auto& t : readers)
    t.join();

  EXPECT_GT(reads_done.load(), 0);
}

// 6. SnapshotGuard keeps the pinned PG alive across a concurrent install
// that publishes a new cur slot.
TEST_F(GraphSnapshotStoreConcurrencyTest, PinnedSnapshotSurvivesPublish) {
  SnapshotGuard guard(*store_);
  ASSERT_NE(guard.graph(), nullptr);

  ASSERT_TRUE(publish(make_new_pg()).ok());

  // graph pointer is still safely dereferenceable: storage held alive by our
  // pin.
  EXPECT_GE(guard.graph()->schema().vertex_label_num(), 1u);
}

// 7. COW publish → pin sees new snapshot (E2E visibility).
// Simulates the UpdateTransaction commit path: clone PG, mutate, publish,
// then verify a new reader observes the mutation.
TEST_F(GraphSnapshotStoreConcurrencyTest, CowPublishIsVisibleToNewReaders) {
  // Clone and add a new vertex type to the COW copy.
  auto cow_pg = initial_pg_->Clone();
  CreateVertexTypeParamBuilder builder;
  auto status =
      cow_pg->CreateVertexType(builder.VertexLabel("company")
                                   .AddProperty("name", neug::Value::STRING(""))
                                   .AddPrimaryKeyName("name")
                                   .Build());
  ASSERT_TRUE(status.ok());

  // Publish the mutated snapshot.
  ASSERT_TRUE(publish(cow_pg).ok());

  // A new reader must observe the "company" vertex type.
  SnapshotGuard guard(*store_);
  EXPECT_TRUE(guard.graph()->schema().is_vertex_label_valid("company"));
  EXPECT_TRUE(guard.graph()->schema().is_vertex_label_valid("person"));
  EXPECT_EQ(guard.graph()->schema().vertex_label_num(), 2u);
}

// 8. Abort-style release recycles the slot properly.
// Simulates an UpdateTransaction that clones, publishes, then the caller
// releases the old pin (as Abort would). Verifies no slot leak.
TEST_F(GraphSnapshotStoreConcurrencyTest, AbortReleasesSlotWithoutLeak) {
  // Pin the current slot (simulates a transaction-owned SnapshotGuard).
  SnapshotGuard old_slot(*store_);

  // Publish a new snapshot (simulates successful COW commit path).
  ASSERT_TRUE(publish(make_new_pg()).ok());

  // Release the old pin (simulates Abort/release_pin).
  old_slot.release();

  // Verify no slot leak: we should still be able to exhaust (kSlotNum - 2)
  // publishes without hitting pool exhaustion.
  for (int i = 0; i < kSlotNum - 2; ++i) {
    auto status = publish(make_new_pg());
    ASSERT_TRUE(status.ok())
        << "publish " << i << " failed — abort slot leak suspected";
  }
}

// 9. COW isolation after Clone → Mutate → Publish cycle.
//
// Validates that after a COW clone is mutated and published as the new
// snapshot, subsequent Clone()s from that snapshot still produce
// correct COW copies with proper isolation. The shallow shared_ptr copy in
// Clone() combined with detach-on-write must ensure that
// mutations in cow2 don't leak back to the published cow1.
TEST_F(GraphSnapshotStoreConcurrencyTest, CowIsolationAfterCloneMutatePublish) {
  // Phase 1: seed the initial snapshot with a vertex.
  vid_t vid0 = 0;
  auto status = initial_pg_->AddVertex(0, neug::Value::INT64(1), {}, vid0, 1);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(initial_pg_->VertexNum(0, MAX_TIMESTAMP), 1u);

  // Phase 2: Clone → Mutate → Publish (simulates UpdateTxn
  // commit). Explicitly detach shared modules before mutation, mirroring
  // UpdateTransaction::detachVertexTableForInsert.
  auto cow1 = initial_pg_->Clone();
  auto& vt1 = cow1->get_vertex_table(0);
  vt1.DetachIndexer();
  vt1.DetachVertexTimestamp();
  vt1.get_table().DetachAllColumns(*cow1->checkpoint_ptr(),
                                   cow1->memory_level());
  vid_t vid1 = 0;
  status = cow1->AddVertex(0, neug::Value::INT64(2), {}, vid1, 2);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(cow1->VertexNum(0, MAX_TIMESTAMP), 2u);

  // Publish the mutated COW clone as the new snapshot.
  ASSERT_TRUE(publish(cow1).ok());

  // Phase 3: clone again from the published snapshot. Explicitly detach
  // shared modules so cow2's mutations don't leak back to cow1.
  auto cow2 = cow1->Clone();
  auto& vt2 = cow2->get_vertex_table(0);
  vt2.DetachIndexer();
  vt2.DetachVertexTimestamp();
  vt2.get_table().DetachAllColumns(*cow2->checkpoint_ptr(),
                                   cow2->memory_level());
  vid_t vid2 = 0;
  status = cow2->AddVertex(0, neug::Value::INT64(3), {}, vid2, 3);
  ASSERT_TRUE(status.ok());

  // cow2 sees all three vertices.
  EXPECT_EQ(cow2->VertexNum(0, MAX_TIMESTAMP), 3u);

  // The installed snapshot (cow1) must still see only two vertices —
  // COW isolation guarantees that cow2's mutation doesn't leak back.
  EXPECT_EQ(cow1->VertexNum(0, MAX_TIMESTAMP), 2u);

  // Original PG still sees only one.
  EXPECT_EQ(initial_pg_->VertexNum(0, MAX_TIMESTAMP), 1u);

  // Publish cow2 and verify a reader sees all three.
  ASSERT_TRUE(publish(cow2).ok());
  SnapshotGuard guard(*store_);
  EXPECT_EQ(guard.graph()->VertexNum(0, MAX_TIMESTAMP), 3u);
}

// 10. Prepared installation tags the slot with the given view generation; a
// pinned incarnation keeps its generation stable across later publishes.
TEST_F(GraphSnapshotStoreConcurrencyTest,
       GenerationTaggedOnPublishAndStableWhilePinned) {
  ASSERT_TRUE(publish(make_new_pg()).ok());
  const uint32_t pinned_gen = last_view_generation_;

  SnapshotGuard guard(*store_);
  EXPECT_EQ(guard.view_generation(), pinned_gen);

  ASSERT_TRUE(publish(make_new_pg()).ok());
  EXPECT_EQ(guard.view_generation(), pinned_gen)
      << "a pinned incarnation's generation must be stable";

  SnapshotGuard newer(*store_);
  EXPECT_EQ(newer.view_generation(), last_view_generation_);
  EXPECT_NE(newer.view_generation(), guard.view_generation());
}

// 11. Slot index reuse (A-B-A) must not imply snapshot identity: a reused
// slot carries the new generation, so generation comparison rejects the
// stale pairing. Uses a 2-slot pool to force reuse.
TEST_F(GraphSnapshotStoreConcurrencyTest, SlotReuseABAChangesGeneration) {
  GraphSnapshotStore store(2, initial_pg_, /*initial_view_generation=*/10);

  uint32_t first_gen;
  {
    SnapshotGuard first(store);
    first_gen = first.view_generation();
    ASSERT_EQ(first_gen, 10u);
    // cur moves to slot 1 / gen 11 while `first` retains slot 0 / gen 10.
    ASSERT_TRUE(
        InstallPreparedSnapshotForTest(store, make_new_pg(), 11, 0).ok());
  }  // release `first`: slot 0 is cleaned up and returned to the free list.

  // Reuses slot 0 with a new generation.
  ASSERT_TRUE(InstallPreparedSnapshotForTest(store, make_new_pg(), 12, 0).ok());
  SnapshotGuard current(store);
  EXPECT_EQ(current.view_generation(), 12u);
  EXPECT_NE(current.view_generation(), first_gen)
      << "generation comparison must distinguish reused slot incarnations";
}

// 12. Delayed-pin regression: a reader parked between the cur-index load and
// its pin CAS must not resurrect a slot that was fully cleaned up meanwhile
// (the old fetch_add algorithm temporarily bounced 0 -> 1 -> 0 and could
// make cleanup miss the slot). Deterministic via the after_cur_slot_load
// hook.
TEST_F(GraphSnapshotStoreConcurrencyTest,
       DelayedPinCannotResurrectCleanedSlot) {
  std::promise<void> pin_loaded_cur;
  std::promise<void> cleanup_done;
  auto cleanup_done_future = cleanup_done.get_future();
  std::atomic<bool> parked{false};
  std::atomic<const PropertyGraph*> pinned_graph{nullptr};

  g_concurrency_test_hooks.after_cur_slot_load = [&] {
    if (!parked.exchange(true)) {
      pin_loaded_cur.set_value();
      // Park the reader between the cur-index load and the pin CAS.
      cleanup_done_future.wait();
    }
  };

  std::thread reader([&] {
    SnapshotGuard guard(*store_);
    pinned_graph.store(guard.graph());
  });

  pin_loaded_cur.get_future().wait();
  // The reader has loaded cur = slot 0 but not yet CASed. Publishing moves
  // cur away; slot 0 holds only its cur-pin, so releasing it cleans the slot
  // up synchronously and returns it to the free list.
  auto pg2 = make_new_pg();
  ASSERT_TRUE(publish(pg2).ok());
  cleanup_done.set_value();
  reader.join();

  // The delayed pin attempt must have retried onto the new cur instead of
  // resurrecting slot 0.
  EXPECT_EQ(pinned_graph.load(), pg2.get());

  // Slot 0 must have been recycled: exhaust the rest of the pool.
  for (int i = 0; i < kSlotNum - 2; ++i) {
    ASSERT_TRUE(publish(make_new_pg()).ok())
        << "publish " << i << " failed — delayed pin leaked a slot";
  }
}

// 13. Schema generation (read-view publication protocol, Phase 5):
// Prepared installation tags the slot with the given schema generation;
// current_schema_generation() follows the current slot; a pinned old
// incarnation keeps its own generation.
TEST_F(GraphSnapshotStoreConcurrencyTest,
       SchemaGenerationTaggedOnPublishAndTrackedPerSlot) {
  // The fixture store starts with initial_schema_generation = 0.
  EXPECT_EQ(store_->current_schema_generation(), 0u);

  // DML-only publish: the schema generation is carried over unchanged.
  ASSERT_TRUE(
      InstallPreparedSnapshotForTest(*store_, make_new_pg(), 1, 0).ok());
  EXPECT_EQ(store_->current_schema_generation(), 0u);

  // DDL publish: the schema generation advances by one.
  ASSERT_TRUE(
      InstallPreparedSnapshotForTest(*store_, make_new_pg(), 2, 1).ok());
  EXPECT_EQ(store_->current_schema_generation(), 1u);

  // A reader pinning the new current slot observes the bumped generation.
  SnapshotGuard guard(*store_);
  EXPECT_EQ(guard.schema_generation(), 1u);
  EXPECT_EQ(guard.view_generation(), 2u);
}

// 14. AdvanceCurrentSlotSchemaGeneration (EXCLUSIVE-only in-place bump for
// AP schema mutations that publish no snapshot) advances the current
// slot's schema generation without touching the view generation.
TEST_F(GraphSnapshotStoreConcurrencyTest,
       AdvanceCurrentSlotSchemaGenerationBumpsInPlace) {
  EXPECT_EQ(store_->current_schema_generation(), 0u);

  store_->AdvanceCurrentSlotSchemaGeneration();
  EXPECT_EQ(store_->current_schema_generation(), 1u);

  // New readers pinning the same (still current) slot observe the bump.
  SnapshotGuard guard(*store_);
  EXPECT_EQ(guard.schema_generation(), 1u);
  EXPECT_EQ(guard.view_generation(), 0u)
      << "an in-place schema bump must not touch the view generation";
}

TEST_F(GraphSnapshotStoreConcurrencyTest,
       PreparedSnapshotIsInvisibleUntilInstalledAndRollsBackOnRelease) {
  auto candidate = make_new_pg();
  GraphSnapshotStore::PreparedSnapshot prepared;
  ASSERT_TRUE(store_
                  ->PrepareSnapshot(candidate, 91 /* view_generation */,
                                    0 /* schema_generation */, prepared)
                  .ok());
  ASSERT_TRUE(prepared.valid());

  // Reserving/building the slot must not alter the reader-visible current
  // snapshot; this is the pre-WAL failure boundary.
  SnapshotGuard before_install(*store_);
  EXPECT_EQ(before_install.graph(), initial_pg_.get());
  before_install.release();

  prepared.reset();
  ASSERT_TRUE(publish(candidate).ok())
      << "abandoned preparation must return its slot to the free pool";
}

TEST_F(GraphSnapshotStoreConcurrencyTest,
       PreparedSnapshotInstallsWithoutFurtherConstruction) {
  auto candidate = make_new_pg();
  GraphSnapshotStore::PreparedSnapshot prepared;
  ASSERT_TRUE(store_->PrepareSnapshot(candidate, 92, 3, prepared).ok());
  store_->InstallPreparedSnapshot(std::move(prepared));
  EXPECT_FALSE(prepared.valid());

  SnapshotGuard current(*store_);
  EXPECT_EQ(current.graph(), candidate.get());
  EXPECT_EQ(current.view_generation(), 92u);
  EXPECT_EQ(current.schema_generation(), 3u);
}

TEST_F(GraphSnapshotStoreConcurrencyTest, GenerationOverflowFailsClosed) {
  GraphSnapshotStore schema_max_store(2, initial_pg_, 0,
                                      std::numeric_limits<uint32_t>::max());
  EXPECT_THROW((void) schema_max_store.NextCurrentSchemaGeneration(),
               std::exception);
}

}  // namespace neug
