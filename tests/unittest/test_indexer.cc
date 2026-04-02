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

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "neug/common/extra_type_info.h"
#include "neug/storages/container/file_header.h"
#include "neug/storages/file_names.h"
#include "neug/storages/workspace.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/property.h"

namespace neug {

class LFIndexerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ =
        "/tmp/lf_indexer_test_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  static void CreateEmptyIndicesFile(const std::string& base_path) {
    std::filesystem::create_directories(
        std::filesystem::path(base_path).parent_path());
    FileHeader header{};
    std::ofstream fout(base_path + ".indices", std::ios::binary);
    fout.write(reinterpret_cast<const char*>(&header), sizeof(header));
  }

  template <typename INDEX_T>
  static void ExpectInt64Values(const LFIndexer<INDEX_T>& indexer,
                                const std::vector<int64_t>& values) {
    ASSERT_EQ(indexer.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      INDEX_T lid;
      ASSERT_TRUE(indexer.get_index(Property::from_int64(values[i]), lid));
      EXPECT_EQ(lid, static_cast<INDEX_T>(i));

      auto key = indexer.get_key(static_cast<INDEX_T>(i));
      EXPECT_EQ(key.type(), DataTypeId::kInt64);
      EXPECT_EQ(key.as_int64(), values[i])
          << " at index " << i << " with lid " << static_cast<INDEX_T>(i)
          << " and key " << key.as_int64();
    }
  }

  template <typename INDEX_T>
  static void ExpectStringValues(const LFIndexer<INDEX_T>& indexer,
                                 const std::vector<std::string>& values) {
    ASSERT_EQ(indexer.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      INDEX_T lid;
      ASSERT_TRUE(
          indexer.get_index(Property::from_string_view(values[i]), lid));
      EXPECT_EQ(lid, static_cast<INDEX_T>(i));

      auto key = indexer.get_key(static_cast<INDEX_T>(i));
      EXPECT_EQ(key.type(), DataTypeId::kVarchar);
      EXPECT_EQ(key.as_string_view(), values[i]);
    }
  }

  std::string test_dir_;
};

TEST_F(LFIndexerTest, SupportsCoreMutableInterfacesInMemory) {
  EXPECT_EQ(LFIndexer<uint32_t>::prefix(), "indexer");

  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  LFIndexer<uint32_t> indexer;
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kInt64)));
    indexer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }
  EXPECT_EQ(indexer.get_type(), DataTypeId::kInt64);
  EXPECT_EQ(indexer.get_keys().type(), DataTypeId::kInt64);
  EXPECT_EQ(indexer.size(), 0U);
  EXPECT_EQ(indexer.capacity(), 0U);

  indexer.reserve(8);
  EXPECT_GE(indexer.capacity(), 8U);

  std::vector<int64_t> values = {7, 11, 13, 17, 19, 23, 29, 31, 37, 41};
  EXPECT_EQ(indexer.insert(Property::from_int64(values[0])), 0U);
  for (size_t i = 1; i < values.size(); ++i) {
    EXPECT_EQ(indexer.insert_safe(Property::from_int64(values[i])),
              static_cast<uint32_t>(i));
  }

  EXPECT_EQ(indexer.size(), values.size());
  EXPECT_GE(indexer.capacity(), values.size());
  ExpectInt64Values(indexer, values);

  uint32_t lid = std::numeric_limits<uint32_t>::max();
  EXPECT_TRUE(indexer.get_index(Property::from_int64(23), lid));
  EXPECT_EQ(lid, 5U);
  EXPECT_FALSE(indexer.get_index(Property::from_int32(23), lid));
  EXPECT_TRUE(indexer.contains(Property::from_int64(37)));
  EXPECT_FALSE(indexer.contains(Property::from_int64(1001)));
  EXPECT_EQ(indexer.get_index(Property::from_int64(1001)),
            std::numeric_limits<uint32_t>::max());

  indexer.rehash(64);
  EXPECT_GE(indexer.capacity(), 64U);
  ExpectInt64Values(indexer, values);

  indexer.Close();
}

TEST_F(LFIndexerTest, DumpsAndOpensAcrossBackends) {
  const std::string name = "persisted_index";

  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  ModuleDescriptor desc;
  {
    LFIndexer<uint32_t> writable;
    {
      ModuleDescriptor fresh_desc;
      fresh_desc.module_type = "lf_indexer";
      fresh_desc.set("type",
                     std::to_string(static_cast<uint8_t>(DataTypeId::kInt64)));
      writable.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
    }

    std::vector<int64_t> values = {5, 10, 15, 20};
    for (const auto& value : values) {
      writable.insert_safe(Property::from_int64(value));
    }

    desc = writable.Dump(ckp);
  }
  std::vector<int64_t> values = {5, 10, 15, 20};
  {
    LFIndexer<uint32_t> readonly;
    readonly.Open(ckp, desc, MemoryLevel::kInMemory);
    ExpectInt64Values(readonly, values);
    EXPECT_EQ(readonly.get_keys().type(), DataTypeId::kInt64);
    readonly.Close();

    // Verify that sub-module paths were recorded in the descriptor
    EXPECT_FALSE(desc.sub_modules().at("keys").path.empty());
    EXPECT_TRUE(
        std::filesystem::exists(desc.sub_modules().at("keys").path) ||
        std::filesystem::exists(desc.sub_modules().at("keys").path + ".items"));
    EXPECT_FALSE(desc.sub_modules().at("indices").path.empty());
    EXPECT_TRUE(std::filesystem::exists(desc.sub_modules().at("indices").path));
  }

  // Verify the data can also be opened using hugepages and sync-to-file modes
  {
    LFIndexer<uint32_t> hugepage_idx;
    hugepage_idx.Open(ckp, desc, MemoryLevel::kHugePagePrefered);
    ExpectInt64Values(hugepage_idx, values);
    hugepage_idx.Close();
  }
  {
    LFIndexer<uint32_t> sync_idx;
    sync_idx.Open(ckp, desc, MemoryLevel::kSyncToFile);
    ExpectInt64Values(sync_idx, values);
    sync_idx.Close();
  }
}

TEST_F(LFIndexerTest, SupportsBuildEmptySwapAndVarcharKeys) {
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  // Verify that an empty indexer can be initialised and dumped to disk.
  LFIndexer<uint32_t> empty_indexer;
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kInt64)));
    empty_indexer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }
  ModuleDescriptor empty_dump = empty_indexer.Dump(ckp);
  EXPECT_FALSE(empty_dump.sub_modules().at("indices").path.empty());
  EXPECT_TRUE(
      std::filesystem::exists(empty_dump.sub_modules().at("indices").path));
  empty_indexer.Close();

  const std::string lhs_base = test_dir_ + "/lhs_varchar";
  const std::string rhs_base = test_dir_ + "/rhs_varchar";

  LFIndexer<uint32_t> lhs;
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
    lhs.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }
  lhs.reserve(4);

  LFIndexer<uint32_t> rhs;
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
    rhs.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }
  rhs.reserve(4);

  std::vector<std::string> lhs_values = {"alice", "bob"};
  std::vector<std::string> rhs_values = {"carol", "dave", "erin"};
  for (const auto& value : lhs_values) {
    lhs.insert_safe(Property::from_string_view(value));
  }
  for (const auto& value : rhs_values) {
    rhs.insert_safe(Property::from_string_view(value));
  }

  EXPECT_EQ(lhs.get_type(), DataTypeId::kVarchar);
  EXPECT_EQ(lhs.get_keys().type(), DataTypeId::kVarchar);
  ExpectStringValues(lhs, lhs_values);
  ExpectStringValues(rhs, rhs_values);

  lhs.swap(rhs);
  ExpectStringValues(lhs, rhs_values);
  ExpectStringValues(rhs, lhs_values);

  rhs.drop();
  lhs.close();
}

}  // namespace neug
