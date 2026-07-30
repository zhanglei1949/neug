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

// Deterministic regression harness for issue #793 (read-view publication
// protocol, Phase 3): a read view used to be acquired as two independent
// steps (read timestamp, then snapshot pin), so a reader paused between them
// could pair an old visibility timestamp with a newly published snapshot.
//
// Phase 3 makes the split detectable at the raw layer (the packed published
// view's generation no longer matches the pinned slot's generation) and
// unconstructible at the service layer (ReadSnapshotLease::Acquire validates
// and retries).
//
// All interleavings are scripted with latches/promises via
// ConcurrencyTestHooks — no sleeps are used as a correctness oracle.

#include <gtest/gtest.h>
#include <atomic>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/read_snapshot_lease.h"
#include "neug/transaction/version_manager.h"
#include "neug/utils/concurrency_test_hooks.h"
#include "unittest/utils.h"

namespace neug {
namespace {

class ReadViewPublicationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    work_dir_ = std::string("/tmp/test_read_view_publication_") +
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::filesystem::remove_all(work_dir_);
    std::filesystem::create_directories(work_dir_);

    checkpoint_mgr_.Open(work_dir_);
    initial_pg_ = std::make_shared<PropertyGraph>();
    initial_pg_->Open(make_checkpoint(checkpoint_mgr_), MemoryLevel::kInMemory);

    CreateVertexTypeParamBuilder person_builder;
    ASSERT_TRUE(initial_pg_
                    ->CreateVertexType(person_builder.VertexLabel("person")
                                           .AddProperty("id", Value::INT64(0))
                                           .AddPrimaryKeyName("id")
                                           .Build())
                    .ok());

    // Slot 0's initial generation (1) must match the VersionManager's
    // initial published view generation (init_ts(1) -> {1, 1}).
    store_ = std::make_unique<GraphSnapshotStore>(
        4, initial_pg_, 1 /* initial_view_generation */,
        0 /* initial_schema_generation */);
    vm_ = std::make_unique<VersionManager>();
    vm_->init_ts(1, 4);
    g_concurrency_test_hooks.Reset();
  }

  void TearDown() override {
    g_concurrency_test_hooks.Reset();
    vm_.reset();
    store_.reset();
    initial_pg_.reset();
    std::filesystem::remove_all(work_dir_);
  }

  std::string work_dir_;
  CheckpointManager checkpoint_mgr_;
  std::shared_ptr<PropertyGraph> initial_pg_;
  std::unique_ptr<GraphSnapshotStore> store_;
  std::unique_ptr<VersionManager> vm_;
};

// Raw-layer view of issue #793 after Phase 3: a reader parked between the
// published-view load and the snapshot pin observes a GENERATION MISMATCH
// (old view generation vs the newly published slot's generation). The split
// acquisition can no longer produce an undetectable (old ts, new snapshot)
// pair — the mismatch is exactly what ReadSnapshotLease::Acquire validates.
TEST_F(ReadViewPublicationTest,
       Issue793SplitAcquisitionSurfacesAsGenerationMismatch) {
  std::promise<void> view_loaded;
  std::promise<void> writer_committed;
  auto view_loaded_future = view_loaded.get_future();
  auto writer_committed_future = writer_committed.get_future();

  g_concurrency_test_hooks.after_read_view_load = [&] {
    view_loaded.set_value();
    // Hold the reader between the published-view load and the snapshot pin.
    writer_committed_future.wait();
  };

  std::atomic<uint32_t> reader_view_generation{0};
  std::atomic<uint32_t> reader_view_ts{0};
  std::atomic<uint32_t> reader_slot_generation{0};
  PropertyGraph* published_graph = nullptr;

  std::thread reader([&] {
    vm_->acquire_read_admission();
    const auto view = vm_->load_published_read_view();
    reader_view_ts.store(view.visibility_ts);
    reader_view_generation.store(view.view_generation);
    {
      SnapshotGuard guard(*store_);
      reader_slot_generation.store(guard.view_generation());
    }
    vm_->release_read_timestamp();
  });

  std::thread writer([&] {
    view_loaded_future.wait();
    const uint32_t write_ts =
        vm_->acquire_write_timestamp(WriteIntent::kUpdate);
    vm_->begin_write_commit(write_ts, WriteCompletion::kSnapshot);
    auto cow_pg = initial_pg_->Clone();
    published_graph = cow_pg.get();
    GraphSnapshotStore::PreparedSnapshot prepared;
    ASSERT_TRUE(store_
                    ->PrepareSnapshot(cow_pg, write_ts /* view_generation */,
                                      0 /* schema_generation */, prepared)
                    .ok());
    store_->InstallPreparedSnapshot(std::move(prepared));
    vm_->complete_write(write_ts, WriteCompletion::kSnapshot, write_ts);
    writer_committed.set_value();
  });

  writer.join();
  reader.join();

  // The reader loaded the pre-commit view {ts=1, gen=1} but pinned the
  // snapshot published by the commit at timestamp 2 (gen=2). Pre-Phase-3
  // this pair was silently returned; now the generations disagree, which is
  // precisely the validation failure ReadSnapshotLease::Acquire retries on.
  EXPECT_EQ(reader_view_ts.load(), 1u);
  EXPECT_EQ(reader_view_generation.load(), 1u);
  EXPECT_EQ(reader_slot_generation.load(), 2u)
      << "the pinned slot must carry the commit's generation";
  ASSERT_NE(published_graph, nullptr);
}

// The same interleaving through ReadSnapshotLease::Acquire: the generation
// mismatch is detected and the acquisition retries until it
// returns a coherent (new ts, new snapshot) view.
TEST_F(ReadViewPublicationTest, AcquireRetriesToCoherentView) {
  std::promise<void> view_loaded;
  std::promise<void> writer_committed;
  auto view_loaded_future = view_loaded.get_future();
  auto writer_committed_future = writer_committed.get_future();

  // Park only the reader's FIRST published-view load; the retry's load must
  // pass through.
  std::atomic<bool> parked{false};
  g_concurrency_test_hooks.after_read_view_load = [&] {
    if (!parked.exchange(true)) {
      view_loaded.set_value();
      writer_committed_future.wait();
    }
  };
  std::atomic<int> admission_count{0};
  g_concurrency_test_hooks.after_read_admission = [&] {
    admission_count.fetch_add(1);
  };

  PropertyGraph* published_graph = nullptr;

  std::thread reader([&] {
    auto lease = ReadSnapshotLease::Acquire(*vm_, *store_);

    EXPECT_TRUE(lease.valid());
    // The retried acquisition observes the commit's published view: the
    // coherent (ts=2, gen=2) pair, never (ts=1, gen=2).
    EXPECT_EQ(lease.timestamp(), 2u);
    EXPECT_EQ(lease.view_generation(), 2u);
    EXPECT_EQ(lease.view_generation(), 2u);
    EXPECT_EQ(lease.graph(), published_graph);
  });

  std::thread writer([&] {
    view_loaded_future.wait();
    const uint32_t write_ts =
        vm_->acquire_write_timestamp(WriteIntent::kUpdate);
    vm_->begin_write_commit(write_ts, WriteCompletion::kSnapshot);
    auto cow_pg = initial_pg_->Clone();
    published_graph = cow_pg.get();
    GraphSnapshotStore::PreparedSnapshot prepared;
    ASSERT_TRUE(store_
                    ->PrepareSnapshot(cow_pg, write_ts /* view_generation */,
                                      0 /* schema_generation */, prepared)
                    .ok());
    store_->InstallPreparedSnapshot(std::move(prepared));
    vm_->complete_write(write_ts, WriteCompletion::kSnapshot, write_ts);
    writer_committed.set_value();
  });

  writer.join();
  reader.join();

  EXPECT_GE(admission_count.load(), 2)
      << "the mismatched acquisition must release admission and retry";
}

// Control case: without an interleaved commit, a reader pins the snapshot
// that matches its published view.
TEST_F(ReadViewPublicationTest, ReaderWithoutCommitSeesInitialSnapshot) {
  vm_->acquire_read_admission();
  const auto view = vm_->load_published_read_view();
  {
    SnapshotGuard guard(*store_);
    EXPECT_EQ(guard.graph(), initial_pg_.get());
    EXPECT_EQ(guard.view_generation(), view.view_generation);
  }
  vm_->release_read_timestamp();
  EXPECT_EQ(view.visibility_ts, 1u);
  EXPECT_EQ(view.view_generation, 1u);
}

}  // namespace
}  // namespace neug
