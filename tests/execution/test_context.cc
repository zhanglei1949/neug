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

#include <memory>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/storages/loader/loader_utils.h"

namespace neug {
namespace execution {
namespace test {

namespace {

std::shared_ptr<IContextColumn> MakeI64Column(
    const std::vector<int64_t>& values) {
  ValueColumnBuilder<int64_t> builder;
  builder.reserve(values.size());
  for (auto value : values) {
    builder.push_back_opt(value);
  }
  return builder.finish();
}

std::shared_ptr<DataChunk> MakeChunk(const std::vector<int64_t>& col0,
                                     const std::vector<int64_t>& col1,
                                     const std::vector<int64_t>& col2) {
  auto chunk = std::make_shared<DataChunk>();
  chunk->set(0, MakeI64Column(col0));
  chunk->set(1, MakeI64Column(col1));
  chunk->set(2, MakeI64Column(col2));
  return chunk;
}

class CountingChunkSupplier final : public IDataChunkSupplier {
 public:
  CountingChunkSupplier(std::vector<std::shared_ptr<DataChunk>> chunks,
                        std::shared_ptr<int> get_next_count)
      : chunks_(std::move(chunks)),
        get_next_count_(std::move(get_next_count)) {}

  std::shared_ptr<DataChunk> GetNextChunk() override {
    ++(*get_next_count_);
    if (index_ >= chunks_.size()) {
      return nullptr;
    }
    return chunks_[index_++];
  }

  int64_t RowNum() const override {
    int64_t total = 0;
    for (const auto& chunk : chunks_) {
      total += static_cast<int64_t>(chunk->row_num());
    }
    return total;
  }

 private:
  std::vector<std::shared_ptr<DataChunk>> chunks_;
  std::shared_ptr<int> get_next_count_;
  size_t index_ = 0;
};

class CountingChunkSource final : public IDataChunkSource {
 public:
  explicit CountingChunkSource(std::vector<std::shared_ptr<DataChunk>> chunks)
      : chunks_(std::move(chunks)),
        open_count_(std::make_shared<int>(0)),
        get_next_count_(std::make_shared<int>(0)) {}

  std::shared_ptr<IDataChunkSupplier> Open() const override {
    ++(*open_count_);
    return std::make_shared<CountingChunkSupplier>(chunks_, get_next_count_);
  }

  bool rewindable() const override { return true; }

  int open_count() const { return *open_count_; }
  int get_next_count() const { return *get_next_count_; }

 private:
  std::vector<std::shared_ptr<DataChunk>> chunks_;
  std::shared_ptr<int> open_count_;
  std::shared_ptr<int> get_next_count_;
};

template <typename T>
std::shared_ptr<ValueColumn<T>> AsValueColumn(
    const std::shared_ptr<IContextColumn>& column) {
  auto typed = std::dynamic_pointer_cast<ValueColumn<T>>(column);
  EXPECT_NE(typed, nullptr);
  return typed;
}

}  // namespace

TEST(ContextStreamingTest, MakeSupplierProjectsWithoutMaterializing) {
  auto source = std::make_shared<CountingChunkSource>(
      std::vector<std::shared_ptr<DataChunk>>{
          MakeChunk({1, 2}, {10, 20}, {100, 200}),
          MakeChunk({3}, {30}, {300}),
      });

  Context ctx;
  ctx.set_streaming_source(source);
  ASSERT_TRUE(ctx.is_streaming());
  EXPECT_EQ(source->open_count(), 0);
  EXPECT_EQ(source->get_next_count(), 0);

  auto supplier = ctx.make_supplier({2, 0});
  EXPECT_TRUE(ctx.is_streaming());
  EXPECT_EQ(source->open_count(), 1);
  EXPECT_EQ(source->get_next_count(), 0);

  auto first = supplier->GetNextChunk();
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->row_num(), 2);
  ASSERT_EQ(first->col_num(), 2);
  auto projected_col0 = AsValueColumn<int64_t>(first->get(0));
  auto projected_col1 = AsValueColumn<int64_t>(first->get(1));
  ASSERT_NE(projected_col0, nullptr);
  ASSERT_NE(projected_col1, nullptr);
  EXPECT_EQ(projected_col0->get_value(0), 100);
  EXPECT_EQ(projected_col0->get_value(1), 200);
  EXPECT_EQ(projected_col1->get_value(0), 1);
  EXPECT_EQ(projected_col1->get_value(1), 2);
  EXPECT_TRUE(ctx.is_streaming());

  auto second = supplier->GetNextChunk();
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(AsValueColumn<int64_t>(second->get(0))->get_value(0), 300);
  EXPECT_EQ(supplier->GetNextChunk(), nullptr);
  EXPECT_EQ(source->get_next_count(), 3);
  EXPECT_TRUE(ctx.is_streaming());

  EXPECT_EQ(ctx.row_num(), 3);
  EXPECT_FALSE(ctx.is_streaming());
  EXPECT_EQ(source->open_count(), 2);
  EXPECT_EQ(ctx.chunk_num(), 2);
  EXPECT_EQ(ctx.chunk(0).chunk().get(0)->size(), 2);
  EXPECT_EQ(ctx.chunk(1).chunk().get(0)->size(), 1);
}

TEST(ContextStreamingTest, CopyKeepsIndependentMaterializedPayload) {
  Context ctx;
  ctx.append_chunk(std::move(*MakeChunk({1, 2}, {10, 20}, {100, 200})));

  Context copy = ctx;
  auto result =
      ctx.apply_chunks([](ContextChunk&& chunk) -> neug::result<ContextChunk> {
        return std::move(chunk);
      });
  ASSERT_TRUE(result);

  EXPECT_EQ(result.value().row_num(), 2);
  EXPECT_EQ(copy.row_num(), 2);
  EXPECT_EQ(copy.chunk_num(), 1);
  EXPECT_EQ(AsValueColumn<int64_t>(copy.chunk(0).chunk().get(0))->get_value(1),
            2);
}

TEST(ContextStreamingTest, RowNumIfMaterializedDoesNotOpenStream) {
  auto source = std::make_shared<CountingChunkSource>(
      std::vector<std::shared_ptr<DataChunk>>{
          MakeChunk({1, 2}, {10, 20}, {100, 200}),
      });

  Context ctx;
  ctx.set_streaming_source(source);
  auto row_num = ctx.row_num_if_materialized();

  EXPECT_FALSE(row_num.has_value());
  EXPECT_TRUE(ctx.is_streaming());
  EXPECT_EQ(source->open_count(), 0);
  EXPECT_EQ(source->get_next_count(), 0);
}

TEST(ContextStreamingTest, MaterializeDoesNotMoveFromSourceChunks) {
  auto source_chunk = MakeChunk({1, 2}, {10, 20}, {100, 200});
  auto source = std::make_shared<CountingChunkSource>(
      std::vector<std::shared_ptr<DataChunk>>{source_chunk});

  Context ctx;
  ctx.set_streaming_source(source);

  EXPECT_EQ(ctx.row_num(), 2);
  EXPECT_FALSE(ctx.is_streaming());
  EXPECT_EQ(source_chunk->row_num(), 2);
  EXPECT_EQ(AsValueColumn<int64_t>(source_chunk->get(0))->get_value(1), 2);
}

}  // namespace test
}  // namespace execution
}  // namespace neug
