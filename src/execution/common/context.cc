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

#include "neug/execution/common/context.h"

#include <glog/logging.h>

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"

namespace neug {

namespace execution {

class IContextData {
 public:
  virtual ~IContextData() = default;
  virtual bool is_streaming() const = 0;
  virtual std::shared_ptr<IDataChunkSource> source() const = 0;
  virtual std::vector<ContextChunk>& chunks() = 0;
  virtual std::shared_ptr<IContextData> Clone() const = 0;
};

namespace {

class MaterializedContextData final : public IContextData {
 public:
  MaterializedContextData() = default;
  explicit MaterializedContextData(std::vector<ContextChunk> chunks)
      : chunks_(std::move(chunks)) {}

  bool is_streaming() const override { return false; }
  std::shared_ptr<IDataChunkSource> source() const override { return nullptr; }
  std::vector<ContextChunk>& chunks() override { return chunks_; }
  std::shared_ptr<IContextData> Clone() const override {
    return std::make_shared<MaterializedContextData>(chunks_);
  }

 private:
  std::vector<ContextChunk> chunks_;
};

class StreamingContextData final : public IContextData {
 public:
  explicit StreamingContextData(std::shared_ptr<IDataChunkSource> source)
      : source_(std::move(source)) {}

  bool is_streaming() const override { return source_ != nullptr; }
  std::shared_ptr<IDataChunkSource> source() const override { return source_; }

  std::vector<ContextChunk>& chunks() override {
    THROW_INTERNAL_EXCEPTION(
        "StreamingContextData must be materialized before chunk access.");
  }
  std::shared_ptr<IContextData> Clone() const override {
    return std::make_shared<StreamingContextData>(source_);
  }

 private:
  std::shared_ptr<IDataChunkSource> source_;
};

class StaticChunkSupplier final : public IDataChunkSupplier {
 public:
  explicit StaticChunkSupplier(std::vector<std::shared_ptr<DataChunk>> chunks)
      : chunks_(std::move(chunks)) {}

  std::shared_ptr<DataChunk> GetNextChunk() override {
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

  std::optional<ChunkSupplierStats> GetStats() const override {
    ChunkSupplierStats stats;
    stats.produced_chunks = static_cast<int64_t>(chunks_.size());
    stats.consumed_chunks = static_cast<int64_t>(index_);
    for (size_t i = 0; i < chunks_.size(); ++i) {
      auto rows = static_cast<int64_t>(chunks_[i]->row_num());
      stats.produced_rows += rows;
      if (i < index_) {
        stats.consumed_rows += rows;
      }
    }
    return stats;
  }

 private:
  std::vector<std::shared_ptr<DataChunk>> chunks_;
  size_t index_ = 0;
};

class StaticChunkSource final : public IDataChunkSource {
 public:
  explicit StaticChunkSource(std::vector<std::shared_ptr<DataChunk>> chunks)
      : chunks_(std::move(chunks)) {}

  std::shared_ptr<IDataChunkSupplier> Open() const override {
    return std::make_shared<StaticChunkSupplier>(chunks_);
  }

  bool rewindable() const override { return true; }

 private:
  std::vector<std::shared_ptr<DataChunk>> chunks_;
};

class ProjectingChunkSupplier final : public IDataChunkSupplier {
 public:
  ProjectingChunkSupplier(std::shared_ptr<IDataChunkSupplier> input,
                          std::vector<int32_t> aliases)
      : input_(std::move(input)), aliases_(std::move(aliases)) {}

  std::shared_ptr<DataChunk> GetNextChunk() override {
    auto in = input_->GetNextChunk();
    if (!in) {
      return nullptr;
    }
    auto out = std::make_shared<DataChunk>();
    for (size_t i = 0; i < aliases_.size(); ++i) {
      auto column = in->get(aliases_[i]);
      if (column == nullptr) {
        THROW_INTERNAL_EXCEPTION("Column not found for tag id: " +
                                 std::to_string(aliases_[i]));
      }
      out->set(static_cast<int>(i), column);
    }
    return out;
  }

  int64_t RowNum() const override { return input_->RowNum(); }

  std::optional<ChunkSupplierStats> GetStats() const override {
    return input_->GetStats();
  }

 private:
  std::shared_ptr<IDataChunkSupplier> input_;
  std::vector<int32_t> aliases_;
};

class ProjectingChunkSource final : public IDataChunkSource {
 public:
  ProjectingChunkSource(std::shared_ptr<IDataChunkSource> input,
                        std::vector<int32_t> aliases)
      : input_(std::move(input)), aliases_(std::move(aliases)) {}

  std::shared_ptr<IDataChunkSupplier> Open() const override {
    return std::make_shared<ProjectingChunkSupplier>(input_->Open(), aliases_);
  }

  std::shared_ptr<IDataChunkSupplier> Open(
      const ChunkSourceOptions& options) const override {
    return std::make_shared<ProjectingChunkSupplier>(input_->Open(options),
                                                     aliases_);
  }

  bool rewindable() const override { return input_->rewindable(); }

  int64_t EstimatedBytes() const override { return input_->EstimatedBytes(); }

 private:
  std::shared_ptr<IDataChunkSource> input_;
  std::vector<int32_t> aliases_;
};

std::shared_ptr<IContextData> clone_context_data(
    const std::shared_ptr<IContextData>& data) {
  if (data == nullptr) {
    return std::make_shared<MaterializedContextData>();
  }
  return data->Clone();
}

}  // namespace

Context::Context() : data_(std::make_shared<MaterializedContextData>()) {}

Context::Context(const Context& other)
    : tag_ids(other.tag_ids), data_(clone_context_data(other.data_)) {}

Context& Context::operator=(const Context& other) {
  if (this == &other) {
    return *this;
  }
  tag_ids = other.tag_ids;
  data_ = clone_context_data(other.data_);
  return *this;
}

void Context::clear() {
  data_ = std::make_shared<MaterializedContextData>();
  tag_ids.clear();
}

bool Context::is_streaming() const { return data_->is_streaming(); }

void Context::set_streaming_source(std::shared_ptr<IDataChunkSource> source) {
  if (source == nullptr) {
    data_ = std::make_shared<MaterializedContextData>();
    return;
  }
  data_ = std::make_shared<StreamingContextData>(std::move(source));
}

std::shared_ptr<IDataChunkSource> Context::make_source(
    const std::vector<int32_t>& aliases) const {
  if (data_->is_streaming()) {
    return std::make_shared<ProjectingChunkSource>(data_->source(), aliases);
  }
  std::vector<std::shared_ptr<DataChunk>> projected_chunks;
  const auto& chunks = chunks_ref();
  projected_chunks.reserve(chunks.size());
  for (const auto& context_chunk : chunks) {
    const auto& chunk = context_chunk.chunk();
    auto out_chunk = std::make_shared<DataChunk>();
    for (size_t i = 0; i < aliases.size(); ++i) {
      auto column = chunk.get(aliases[i]);
      if (column == nullptr) {
        THROW_INTERNAL_EXCEPTION("Column not found for tag id: " +
                                 std::to_string(aliases[i]));
      }
      out_chunk->set(static_cast<int>(i), column);
    }
    projected_chunks.push_back(std::move(out_chunk));
  }
  return std::make_shared<StaticChunkSource>(std::move(projected_chunks));
}

std::shared_ptr<IDataChunkSupplier> Context::make_supplier(
    const std::vector<int32_t>& aliases) const {
  return make_source(aliases)->Open();
}

void Context::ensure_materialized() const {
  if (!data_->is_streaming()) {
    return;
  }

  std::vector<ContextChunk> chunks;
  if (auto source = data_->source()) {
    auto supplier = source->Open();
    while (auto chunk = supplier->GetNextChunk()) {
      auto chunk_copy = *chunk;
      chunks.emplace_back(std::move(chunk_copy));
    }
  }
  data_ = std::make_shared<MaterializedContextData>(std::move(chunks));
}

std::vector<ContextChunk>& Context::chunks_mut() {
  ensure_materialized();
  return data_->chunks();
}

const std::vector<ContextChunk>& Context::chunks_ref() const {
  ensure_materialized();
  return data_->chunks();
}

size_t Context::chunk_num() const { return chunks_ref().size(); }

ContextChunk& Context::chunk(size_t idx) { return chunks_mut()[idx]; }

const ContextChunk& Context::chunk(size_t idx) const {
  return chunks_ref()[idx];
}

std::vector<ContextChunk>& Context::chunks() { return chunks_mut(); }

const std::vector<ContextChunk>& Context::chunks() const {
  return chunks_ref();
}

void Context::append_chunk(DataChunk&& chunk) {
  chunks_mut().push_back(ContextChunk(std::move(chunk)));
}

void Context::append_chunk(DataChunk&& chunk,
                           std::shared_ptr<IContextColumn> head) {
  chunks_mut().push_back(ContextChunk(std::move(chunk), std::move(head)));
}

void Context::append_chunk(ContextChunk&& chunk) {
  chunks_mut().push_back(std::move(chunk));
}

void Context::flatten() {
  auto& chunks = chunks_mut();
  if (chunks.size() <= 1) {
    return;
  }
  ContextChunk merged = std::move(chunks[0]);
  for (size_t i = 1; i < chunks.size(); ++i) {
    merged = merged.union_with(chunks[i]);
  }
  chunks.clear();
  chunks.push_back(std::move(merged));
}

void Context::ensure_single_chunk(const char* caller) {
  auto& chunks = chunks_mut();
  if (chunks.size() <= 1)
    return;
  LOG(WARNING) << caller << ": expected single-chunk Context, got "
               << chunks.size() << " chunks; flattening";
  flatten();
}

size_t Context::col_num() const {
  const auto& chunks = chunks_ref();
  return chunks.empty() ? 0 : chunks[0].columns().size();
}

size_t Context::row_num() const {
  size_t total = 0;
  for (const auto& cc : chunks_ref()) {
    total += cc.row_num();
  }
  return total;
}

std::optional<size_t> Context::row_num_if_materialized() const {
  if (data_ == nullptr || data_->is_streaming()) {
    return std::nullopt;
  }
  size_t total = 0;
  for (const auto& cc : data_->chunks()) {
    total += cc.row_num();
  }
  return total;
}

}  // namespace execution

}  // namespace neug
