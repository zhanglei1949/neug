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

#include "neug/storages/graph/vertex_table.h"

#include "neug/execution/common/columns/value_columns.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/loader/chunk_pipeline_utils.h"
#include "neug/storages/module/module_broker.h"
#include "neug/storages/module/module_factory.h"
#include "neug/storages/module_descriptor.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/likely.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <mutex>
#include <stdexcept>

namespace neug {

namespace {

class VertexBulkBuildFallback : public std::runtime_error {
 public:
  explicit VertexBulkBuildFallback(const std::string& reason)
      : std::runtime_error(reason) {}
};

struct VertexBulkScanStats {
  size_t row_count = 0;
  std::vector<size_t> varchar_bytes;
  int64_t pass1_ms = 0;
};

int64_t elapsed_ms(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now() - start)
      .count();
}

VertexBulkScanStats make_vertex_bulk_scan_stats(size_t property_count) {
  VertexBulkScanStats stats;
  stats.varchar_bytes.assign(property_count, 0);
  return stats;
}

void finish_vertex_bulk_scan(
    VertexBulkScanStats& stats, std::chrono::steady_clock::time_point start,
    const std::shared_ptr<IDataChunkSupplier>& supplier) {
  stats.pass1_ms = elapsed_ms(start);
  log_chunk_supplier_stats(supplier, "BatchBuildVertices pass1", true);
}

void add_row_count(VertexBulkScanStats& stats, size_t rows) {
  if (NEUG_UNLIKELY(rows >
                    std::numeric_limits<size_t>::max() - stats.row_count)) {
    THROW_RUNTIME_ERROR("VertexBulkBuild row count overflow");
  }
  stats.row_count += rows;
}

void merge_vertex_bulk_scan_stats(VertexBulkScanStats& dst,
                                  const VertexBulkScanStats& src) {
  add_row_count(dst, src.row_count);
  CHECK_EQ(dst.varchar_bytes.size(), src.varchar_bytes.size());
  for (size_t i = 0; i < dst.varchar_bytes.size(); ++i) {
    if (NEUG_UNLIKELY(src.varchar_bytes[i] >
                      std::numeric_limits<size_t>::max() -
                          dst.varchar_bytes[i])) {
      THROW_RUNTIME_ERROR("VertexBulkBuild varchar byte count overflow");
    }
    dst.varchar_bytes[i] += src.varchar_bytes[i];
  }
}

bool is_supported_vertex_bulk_property_type(DataTypeId type) {
  switch (type) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return true;
    FOR_EACH_DATA_TYPE_PRIMITIVE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kDate:
  case DataTypeId::kTimestampMs:
  case DataTypeId::kInterval:
  case DataTypeId::kVarchar:
    return true;
  default:
    return false;
  }
}

bool is_supported_vertex_bulk_pk_type(DataTypeId type) {
  switch (type) {
  case DataTypeId::kInt64:
  case DataTypeId::kInt32:
  case DataTypeId::kUInt32:
  case DataTypeId::kUInt64:
    return true;
  default:
    return false;
  }
}

void validate_vertex_bulk_schema(const VertexSchema& schema,
                                 DataTypeId pk_type) {
  if (!is_supported_vertex_bulk_pk_type(pk_type)) {
    throw VertexBulkBuildFallback(
        "VertexBulkBuild v1 only supports numeric primary keys, got " +
        DataType(pk_type).ToString());
  }
  for (const auto& type : schema.property_types) {
    if (!is_supported_vertex_bulk_property_type(type.id())) {
      throw VertexBulkBuildFallback(
          "VertexBulkBuild unsupported property type: " + type.ToString());
    }
  }
}

void validate_vertex_bulk_chunk(const VertexSchema& schema,
                                const execution::DataChunk& chunk,
                                DataTypeId pk_type, const Table& table,
                                std::vector<size_t>* varchar_bytes) {
  const auto& columns = chunk.columns;
  if (columns.size() != schema.property_names.size() + 1) {
    throw VertexBulkBuildFallback(
        "chunk column count does not match vertex schema");
  }
  size_t pk_ind = std::get<2>(schema.primary_keys[0]);
  if (pk_ind >= columns.size()) {
    throw VertexBulkBuildFallback("primary key column index is out of range");
  }
  auto pk_col = columns[pk_ind];
  if (pk_col == nullptr || pk_col->is_optional() ||
      pk_col->elem_type().id() != pk_type) {
    throw VertexBulkBuildFallback(
        "primary key column is optional or type mismatched");
  }

  size_t prop_idx = 0;
  for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
    if (col_idx == pk_ind) {
      continue;
    }
    auto ctx_col = columns[col_idx];
    if (ctx_col == nullptr || ctx_col->is_optional()) {
      throw VertexBulkBuildFallback(
          "optional/null property column is not supported by VertexBulkBuild");
    }
    const auto& prop_type = schema.property_types[prop_idx];
    if (ctx_col->elem_type().id() != prop_type.id()) {
      throw VertexBulkBuildFallback(
          "property column type does not match vertex schema");
    }
    if (ctx_col->size() != chunk.row_num()) {
      throw VertexBulkBuildFallback(
          "property column row count does not match chunk row count");
    }
    if (prop_type.id() == DataTypeId::kVarchar && varchar_bytes != nullptr) {
      auto value_col =
          std::dynamic_pointer_cast<execution::ValueColumn<std::string>>(
              ctx_col);
      auto string_col =
          dynamic_cast<const StringColumn*>(table.get_column_by_id(prop_idx));
      if (value_col == nullptr || string_col == nullptr) {
        throw VertexBulkBuildFallback(
            "varchar property column type does not match storage column");
      }
      size_t bytes = 0;
      for (const auto& value : value_col->data()) {
        size_t truncated = string_col->truncated_byte_size(value);
        if (NEUG_UNLIKELY(truncated >
                          std::numeric_limits<size_t>::max() - bytes)) {
          THROW_RUNTIME_ERROR("VertexBulkBuild varchar byte count overflow");
        }
        bytes += truncated;
      }
      if (NEUG_UNLIKELY(bytes > std::numeric_limits<size_t>::max() -
                                    (*varchar_bytes)[prop_idx])) {
        THROW_RUNTIME_ERROR("VertexBulkBuild varchar byte count overflow");
      }
      (*varchar_bytes)[prop_idx] += bytes;
    }
    ++prop_idx;
  }
}

void accumulate_vertex_bulk_scan_chunk(const VertexSchema& schema,
                                       const execution::DataChunk& chunk,
                                       DataTypeId pk_type, const Table& table,
                                       VertexBulkScanStats& stats) {
  validate_vertex_bulk_chunk(schema, chunk, pk_type, table,
                             &stats.varchar_bytes);
  add_row_count(stats, chunk.row_num());
}

VertexBulkScanStats scan_vertex_bulk_source(
    const std::shared_ptr<IDataChunkSource>& source, const VertexSchema& schema,
    DataTypeId pk_type, const Table& table) {
  auto start = std::chrono::steady_clock::now();
  int64_t source_bytes = source->EstimatedBytes();
  auto options =
      resolve_copy_chunk_source_options(CopyLoadTarget::kVertex, source_bytes);
  auto supplier = source->Open(options);
  if (supplier == nullptr) {
    throw VertexBulkBuildFallback("source returned null supplier in pass1");
  }

  size_t property_count = schema.property_types.size();
  auto stats = make_vertex_bulk_scan_stats(property_count);
  bool use_parallel = can_use_parallel_supplier(supplier, options);
  int32_t consumer_count = resolve_copy_consumer_count(
      options.worker_count, use_parallel, CopyLoadTarget::kVertex);
  if (consumer_count <= 1) {
    while (auto chunk = supplier->GetNextChunk()) {
      accumulate_vertex_bulk_scan_chunk(schema, *chunk, pk_type, table, stats);
    }
    finish_vertex_bulk_scan(stats, start, supplier);
    return stats;
  }

  LOG(INFO) << "BatchBuildVertices pass1 parallel scan label="
            << schema.label_name << ", consumers=" << consumer_count
            << ", csv_workers=" << options.worker_count
            << ", source_bytes=" << source_bytes;

  std::vector<VertexBulkScanStats> local_stats(
      static_cast<size_t>(consumer_count),
      make_vertex_bulk_scan_stats(property_count));
  run_chunk_consumers_with_local_state<VertexBulkScanStats>(
      supplier, consumer_count, local_stats,
      [&](const std::shared_ptr<execution::DataChunk>& chunk,
          VertexBulkScanStats& local) {
        accumulate_vertex_bulk_scan_chunk(schema, *chunk, pk_type, table,
                                          local);
      },
      [&](const VertexBulkScanStats& local) {
        merge_vertex_bulk_scan_stats(stats, local);
      });
  finish_vertex_bulk_scan(stats, start, supplier);
  return stats;
}

template <typename PK_T>
void fill_vertex_chunk_typed(IndexerType& indexer, Table& table,
                             VertexTimestamp& v_ts, const VertexSchema& schema,
                             const std::shared_ptr<execution::DataChunk>& chunk,
                             std::atomic<int64_t>& duplicate_count,
                             std::mutex& indexer_mutex) {
  CHECK(chunk != nullptr);
  validate_vertex_bulk_chunk(schema, *chunk,
                             execution::ValueConverter<PK_T>::type().id(),
                             table, nullptr);
  size_t pk_ind = std::get<2>(schema.primary_keys[0]);
  auto pk_col = std::dynamic_pointer_cast<execution::ValueColumn<PK_T>>(
      chunk->get(static_cast<int>(pk_ind)));
  if (pk_col == nullptr || pk_col->is_optional()) {
    throw VertexBulkBuildFallback(
        "primary key column is not a non-optional typed value column");
  }

  const auto& pks = pk_col->data();
  std::vector<vid_t> vids(pks.size(), std::numeric_limits<vid_t>::max());
  auto access = indexer.template typed_access<PK_T>();
  {
    std::lock_guard<std::mutex> lock(indexer_mutex);
    for (size_t i = 0; i < pks.size(); ++i) {
      auto [vid, inserted] = access.insert_or_get(pks[i], false);
      if (inserted) {
        vids[i] = vid;
        v_ts.InsertVertex(vid, 0);
      } else {
        duplicate_count.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  size_t prop_idx = 0;
  for (size_t col_idx = 0; col_idx < chunk->columns.size(); ++col_idx) {
    if (col_idx == pk_ind) {
      continue;
    }
    auto* col = table.get_column_by_id(prop_idx);
    set_properties_from_context_column(col, chunk->columns[col_idx], vids);
    ++prop_idx;
  }
}

template <typename PK_T>
void fill_vertices_serial(const std::shared_ptr<IDataChunkSupplier>& supplier,
                          IndexerType& indexer, Table& table,
                          VertexTimestamp& v_ts, const VertexSchema& schema,
                          std::atomic<int64_t>& duplicate_count,
                          std::mutex& indexer_mutex) {
  CHECK(supplier != nullptr);
  while (auto chunk = supplier->GetNextChunk()) {
    fill_vertex_chunk_typed<PK_T>(indexer, table, v_ts, schema, chunk,
                                  duplicate_count, indexer_mutex);
  }
}

void fallback_batch_add_vertices(
    VertexTable& table, const std::shared_ptr<IDataChunkSource>& source,
    const std::string& reason) {
  LOG(INFO) << "BatchBuildVertices fallback for label "
            << table.get_vertex_schema_ptr()->label_name << ": " << reason;
  ChunkSourceOptions options;
  options.parallel_enabled = false;
  options.collect_stats = true;
  auto supplier = source->Open(options);
  CHECK(supplier != nullptr);
  table.insert_vertices(supplier);
  log_chunk_supplier_stats(supplier, "BatchBuildVertices fallback", true);
}

}  // namespace

void VertexTable::Init(std::shared_ptr<Checkpoint> ckp, MemoryLevel level) {
  CHECK(vertex_schema_ != nullptr) << "VertexTable::Init requires schema";
  CHECK(indexer_ != nullptr) << "VertexTable::Init requires indexer slot";
  CHECK(v_ts_ != nullptr) << "VertexTable::Init requires vertex_timestamp slot";
  CHECK(pk_type_.id() != DataTypeId::kUnknown)
      << "VertexTable::Init: pk_type must be set; was the schema-aware "
         "constructor used?";
  ckp_ = std::move(ckp);
  memory_level_ = level;
  auto keys = CreateColumn(pk_type_);
  keys->Open(*ckp_, ModuleDescriptor{}, level);
  auto indices = std::make_unique<TypedColumn<vid_t>>();
  indices->Open(*ckp_, ModuleDescriptor{}, level);
  indexer_->Open(*ckp_, ModuleDescriptor{}, level, std::move(keys),
                 std::move(indices));
  table_ = std::make_unique<Table>(vertex_schema_->property_names,
                                   vertex_schema_->property_types);
  table_->Init(*ckp_, level);
  v_ts_->Open(*ckp_, ModuleDescriptor{}, level);
}

void VertexTable::insert_vertices(
    std::shared_ptr<IDataChunkSupplier> supplier) {
  auto pk_type_id = pk_type_.id();
  if (pk_type_id == DataTypeId::kInt64) {
    insert_vertices_impl<int64_t>(supplier);
  } else if (pk_type_id == DataTypeId::kInt32) {
    insert_vertices_impl<int32_t>(supplier);
  } else if (pk_type_id == DataTypeId::kUInt32) {
    insert_vertices_impl<uint32_t>(supplier);
  } else if (pk_type_id == DataTypeId::kUInt64) {
    insert_vertices_impl<uint64_t>(supplier);
  } else if (pk_type_id == DataTypeId::kVarchar) {
    insert_vertices_impl<std::string_view>(supplier);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported primary key type for vertex, type: " +
        pk_type_.ToString() + ", label: " + vertex_schema_->label_name);
  }
}

void VertexTable::BatchBuildVertices(std::shared_ptr<IDataChunkSource> source) {
  if (source == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "BatchBuildVertices requires a non-null data chunk source");
  }
  if (!source->rewindable()) {
    fallback_batch_add_vertices(*this, source, "source is not rewindable");
    return;
  }
  if (Size() != 0 || VertexNum() != 0) {
    fallback_batch_add_vertices(*this, source, "vertex table is not empty");
    return;
  }

  auto run_typed = [&]<typename PK_T>() {
    auto total_start = std::chrono::steady_clock::now();
    try {
      validate_vertex_bulk_schema(*vertex_schema_, pk_type_.id());
      auto scan = scan_vertex_bulk_source(source, *vertex_schema_,
                                          pk_type_.id(), *table_);
      LOG(INFO) << "BatchBuildVertices pass1 label="
                << vertex_schema_->label_name << ", rows=" << scan.row_count
                << ", pass1_ms=" << scan.pass1_ms;

      VertexTable fresh(vertex_schema_);
      fresh.Init(ckp_, memory_level_);
      fresh.indexer_->reserve(scan.row_count);
      for (size_t i = 0; i < vertex_schema_->property_types.size(); ++i) {
        auto* col = fresh.table_->get_column_by_id(i);
        CHECK(col != nullptr);
        if (vertex_schema_->property_types[i].id() == DataTypeId::kVarchar) {
          auto* string_col = dynamic_cast<StringColumn*>(col);
          CHECK(string_col != nullptr);
          string_col->bulk_resize(scan.row_count, scan.varchar_bytes[i]);
        } else {
          col->resize(scan.row_count);
        }
      }
      fresh.v_ts_->Reserve(scan.row_count);

      std::atomic<int64_t> duplicate_count{0};
      std::mutex indexer_mutex;
      int64_t pass2_ms = 0;
      int32_t consumer_count = 1;
      if (scan.row_count > 0) {
        auto fill_plan =
            open_copy_bulk_fill_supplier(source, CopyLoadTarget::kVertex);
        auto supplier = fill_plan.supplier;
        consumer_count = fill_plan.initial_consumer_count;
        auto pass2_start = std::chrono::steady_clock::now();
        if (consumer_count <= 1) {
          fill_vertices_serial<PK_T>(supplier, *fresh.indexer_, *fresh.table_,
                                     *fresh.v_ts_, *vertex_schema_,
                                     duplicate_count, indexer_mutex);
        } else {
          LOG(INFO) << "BatchBuildVertices pass2 adaptive fill label="
                    << vertex_schema_->label_name
                    << ", initial_consumers=" << consumer_count
                    << ", max_consumers=" << fill_plan.max_consumer_count
                    << ", csv_workers=" << fill_plan.options.worker_count;
          run_adaptive_chunk_consumers(
              supplier, consumer_count, fill_plan.max_consumer_count, "vertex",
              [&](const std::shared_ptr<execution::DataChunk>& chunk) {
                fill_vertex_chunk_typed<PK_T>(
                    *fresh.indexer_, *fresh.table_, *fresh.v_ts_,
                    *vertex_schema_, chunk, duplicate_count, indexer_mutex);
              });
        }
        pass2_ms = elapsed_ms(pass2_start);
        log_chunk_supplier_stats(supplier, "BatchBuildVertices pass2", true);
      }

      int64_t duplicates = duplicate_count.load(std::memory_order_relaxed);
      if (duplicates > 0) {
        LOG(INFO) << "BatchBuildVertices duplicate primary keys detected for "
                     "label="
                  << vertex_schema_->label_name
                  << ", duplicate_rows=" << duplicates
                  << ", fallback to BatchAddVertices";
        fallback_batch_add_vertices(*this, source,
                                    "duplicate primary keys detected");
        return;
      }

      Swap(fresh);
      size_t varchar_total = 0;
      for (auto bytes : scan.varchar_bytes) {
        varchar_total += bytes;
      }
      LOG(INFO) << "BatchBuildVertices done label="
                << vertex_schema_->label_name << ", rows=" << scan.row_count
                << ", consumers=" << consumer_count
                << ", duplicate_rows=" << duplicates
                << ", varchar_bytes=" << varchar_total
                << ", pass1_ms=" << scan.pass1_ms << ", pass2_ms=" << pass2_ms
                << ", total_ms=" << elapsed_ms(total_start);
    } catch (const VertexBulkBuildFallback& fallback) {
      fallback_batch_add_vertices(*this, source, fallback.what());
    }
  };

  switch (pk_type_.id()) {
  case DataTypeId::kInt64:
    run_typed.template operator()<int64_t>();
    break;
  case DataTypeId::kInt32:
    run_typed.template operator()<int32_t>();
    break;
  case DataTypeId::kUInt32:
    run_typed.template operator()<uint32_t>();
    break;
  case DataTypeId::kUInt64:
    run_typed.template operator()<uint64_t>();
    break;
  default:
    fallback_batch_add_vertices(*this, source,
                                "primary key type is not supported by "
                                "VertexBulkBuild: " +
                                    pk_type_.ToString());
    break;
  }
}

void VertexTable::Close() {
  indexer_.reset();
  if (table_) {
    table_->close();
  }
  v_ts_.reset();
}

void VertexTable::SetVertexSchema(
    std::shared_ptr<const VertexSchema> vertex_schema) {
  // First ensure the primary key is same with the existing one
  if (vertex_schema->primary_keys.size() != 1) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Vertex schema must have exactly one primary key.");
  }
  if (!VertexSchema::is_pk_same(*vertex_schema_, *vertex_schema)) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "New vertex schema's primary key is different from the existing "
        "one.");
  }

  vertex_schema_ = vertex_schema;
}

bool VertexTable::get_index(const execution::Value& oid, vid_t& lid,
                            timestamp_t ts) const {
  auto res = indexer_->get_index(oid, lid);
  if (NEUG_UNLIKELY(res && !v_ts_->IsVertexValid(lid, ts))) {
    return false;
  }
  return res;
}

size_t VertexTable::VertexNum(timestamp_t ts) const {
  return v_ts_->ValidVertexNum(ts, indexer_->size());
}

size_t VertexTable::LidNum() const { return indexer_->size(); }

vid_t internal::insert_vertex_pk_internal(IndexerType& indexer,
                                          VertexTimestamp& v_ts,
                                          const execution::Value& id,
                                          timestamp_t ts, bool insert_safe) {
  vid_t vid;
  if (NEUG_UNLIKELY(indexer.get_index(id, vid))) {
    if (NEUG_UNLIKELY(v_ts.IsVertexValid(vid, ts))) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex with id " + id.to_string() +
                                       " already exists with lid " +
                                       std::to_string(vid));
    }
  } else {
    vid = indexer.insert(id, insert_safe);
  }
  v_ts.InsertVertex(vid, ts);
  return vid;
}

bool VertexTable::AddVertex(const execution::Value& id,
                            const std::vector<execution::Value>& props,
                            vid_t& vid, timestamp_t ts, bool insert_safe) {
  if (indexer_->capacity() <= indexer_->size()) {
    return false;
  }
  vid = internal::insert_vertex_pk_internal(*indexer_, *v_ts_, id, ts,
                                            insert_safe);
  assert([&]() {
    if (table_->col_num() > 0) {
      return vid < table_->get_column_by_id(0)->size();
    } else {
      return true;
    }
  }());
  table_->insert(vid, props, insert_safe);
  return true;
}

bool VertexTable::UpdateProperty(vid_t vid, int32_t prop_id,
                                 const execution::Value& value,
                                 timestamp_t ts) {
  if (NEUG_UNLIKELY(vid >= indexer_->size())) {
    LOG(ERROR) << "Lid " << vid << " is out of range.";
    return false;
  }
  if (NEUG_UNLIKELY(!v_ts_->IsVertexValid(vid, ts))) {
    LOG(ERROR) << "Vertex with lid " << vid << " is not valid at timestamp "
               << ts << ".";
    return false;
  }
  if (prop_id < 0 || prop_id >= static_cast<int32_t>(table_->col_num())) {
    LOG(ERROR) << "Property id " << prop_id << " is out of range.";
    return false;
  }
  table_->get_column_by_id(prop_id)->set_any(vid, value, true);
  return true;
}

execution::Value VertexTable::GetOid(vid_t lid, timestamp_t ts) const {
  if (NEUG_UNLIKELY(lid >= indexer_->size())) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Lid " + std::to_string(lid) +
                                     " is out of range.");
  }
  if (NEUG_UNLIKELY(!v_ts_->IsVertexValid(lid, ts))) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Lid " + std::to_string(lid) +
                                     " has been deleted.");
  }
  return indexer_->get_key(lid);
}

bool VertexTable::IsValidLid(vid_t lid, timestamp_t ts) const {
  return lid < indexer_->size() && v_ts_->IsVertexValid(lid, ts);
}

size_t VertexTable::EnsureCapacity(size_t capacity) {
  if (capacity <= indexer_->capacity()) {
    return indexer_->capacity();
  }
  capacity = std::max(capacity, 4096UL);
  if (capacity > indexer_->capacity()) {
    indexer_->reserve(capacity);
  }
  if (table_ && table_->size() < capacity) {
    table_->resize(capacity, vertex_schema_->get_default_property_values());
  }
  v_ts_->Reserve(capacity);
  return indexer_->capacity();
}

void VertexTable::BatchDeleteVertices(const std::vector<vid_t>& vids) {
  size_t delete_cnt = 0;
  for (auto v : vids) {
    if (v < indexer_->size() && v_ts_->IsVertexValid(v, MAX_TIMESTAMP)) {
      v_ts_->RemoveVertex(v);
      delete_cnt++;
    }
  }
  VLOG(10) << "Deleted " << delete_cnt << " vertices in batch.";
}

void VertexTable::DeleteVertex(const execution::Value& id, timestamp_t ts) {
  vid_t vid;
  if (!get_index(id, vid, ts)) {
    LOG(WARNING) << "Vertex with id " << id.to_string() << " not found.";
    return;
  }
  return DeleteVertex(vid, ts);
}

void VertexTable::DeleteVertex(vid_t lid, timestamp_t ts) {
  if (lid >= indexer_->size()) {
    LOG(WARNING) << "Lid " << lid << " is out of range.";
    return;
  }
  if (v_ts_->IsVertexValid(lid, ts)) {
    v_ts_->RemoveVertex(lid);
  } else {
    LOG(WARNING) << "Vertex with lid " << lid << " has been deleted.";
  }
}

void VertexTable::RevertDeleteVertex(vid_t lid, timestamp_t ts) {
  assert(lid < indexer_->size());
  if (v_ts_->IsRemoved(lid)) {
    v_ts_->RevertRemoveVertex(lid, ts);
  } else {
    LOG(WARNING) << "Vertex with lid " << lid << " is not deleted.";
  }
}

void VertexTable::DeleteProperties(const std::vector<std::string>& properties) {
  for (const auto& prop : properties) {
    table_->delete_column(prop);
  }
}

void VertexTable::AddProperties(
    Checkpoint& ckp, const std::vector<std::string>& properties,
    const std::vector<DataType>& types,
    const std::vector<execution::Value>& default_values) {
  table_->add_columns(ckp, properties, types, default_values,
                      indexer_->capacity(), memory_level_);
}

void VertexTable::RenameProperties(const std::vector<std::string>& old_names,
                                   const std::vector<std::string>& new_names) {
  CHECK(old_names.size() == new_names.size());
  for (size_t i = 0; i < old_names.size(); ++i) {
    table_->rename_column(old_names[i], new_names[i]);
  }
}

void VertexTable::Compact(timestamp_t ts) {
  v_ts_->Compact();
  // TODO(zhanglei): Support compact unused lid in indexer_ and table
}

vid_t VertexTable::insert_vertex_pk(const execution::Value& id, timestamp_t ts,
                                    bool insert_safe) {
  return internal::insert_vertex_pk_internal(*indexer_, *v_ts_, id, ts,
                                             insert_safe);
}

// --- Static key builders ---

std::string VertexTable::KeyKeys(const std::string& label) {
  return "vertex_" + label + "_keys";
}

std::string VertexTable::KeyIndices(const std::string& label) {
  return "vertex_" + label + "_indices";
}

std::string VertexTable::KeyIndexer(const std::string& label) {
  return "vertex_" + label + "_indexer";
}

std::string VertexTable::KeyVertexTimestamp(const std::string& label) {
  return "vertex_" + label + "_v_ts";
}

std::string VertexTable::KeyProperty(const std::string& label, size_t index) {
  return "vertex_" + label + "_prop_" + std::to_string(index);
}

// --- Snapshot orchestration ---

VertexTable VertexTable::OpenFrom(std::shared_ptr<Checkpoint> ckp,
                                  std::shared_ptr<const VertexSchema> vs,
                                  ModuleBroker& store,
                                  const CheckpointManifest& meta,
                                  MemoryLevel level) {
  VertexTable vt(vs);
  vt.ckp_ = ckp;
  vt.SetMemoryLevel(level);
  const auto& lbl = vs->label_name;

  if (!store.Contains(KeyKeys(lbl))) {
    vt.Init(ckp, level);
    return vt;
  }

  // Restore indexer via LFIndexer::Open
  auto& idx = vt.get_indexer();
  auto indexer_desc = meta.module(KeyIndexer(lbl));
  CHECK(indexer_desc.has_value())
      << "missing indexer meta entry for vertex " << lbl;
  idx.Open(*ckp, indexer_desc.value(), level,
           store.TakeModule<ColumnBase>(KeyKeys(lbl)),
           store.TakeModule<TypedColumn<vid_t>>(KeyIndices(lbl)));

  auto table = std::make_unique<Table>(vs->property_names, vs->property_types);
  for (size_t i = 0; i < vs->property_types.size(); ++i) {
    table->SetColumn(static_cast<int>(i),
                     store.TakeModule<ColumnBase>(KeyProperty(lbl, i)));
  }
  vt.SetTable(std::move(table));
  vt.SetVertexTimestamp(
      store.TakeModule<VertexTimestamp>(KeyVertexTimestamp(lbl)));
  return vt;
}

void VertexTable::DisassembleTo(ModuleBroker& store, CheckpointManifest& meta,
                                Checkpoint& ckp) {
  const auto& lbl = vertex_schema_->label_name;
  auto& idx = get_indexer();

  // Persist indexer via LFIndexer::Dump.  The returned descriptor carries the
  // indexer's three scalars; store it under KeyIndexer so store.Dump's later
  // pass (which writes the columns' own descriptors to KeyKeys / KeyIndices)
  // does not clobber it.
  std::unique_ptr<ColumnBase> keys_out;
  std::unique_ptr<TypedColumn<vid_t>> indices_out;
  meta.set_module(KeyIndexer(lbl), idx.Dump(ckp, keys_out, indices_out));
  store.SetModule(KeyKeys(lbl), std::move(keys_out));
  store.SetModule(KeyIndices(lbl), std::move(indices_out));

  auto table = TakeTable();
  for (size_t i = 0; i < table->col_num(); ++i) {
    table->get_column_by_id(i)->Dump(ckp, meta, KeyProperty(lbl, i));
  }
  store.SetModule(KeyVertexTimestamp(lbl), TakeVertexTimestamp());
}

VertexTable VertexTable::Clone() const {
  CHECK(ckp_ != nullptr) << "VertexTable::Clone requires a valid checkpoint";
  VertexTable cow_clone;
  cow_clone.ckp_ = ckp_;
  cow_clone.indexer_ = indexer_->Clone();
  cow_clone.table_ = table_->Clone();
  cow_clone.vertex_schema_ = vertex_schema_;
  cow_clone.v_ts_ = std::unique_ptr<VertexTimestamp>(
      dynamic_cast<VertexTimestamp*>(v_ts_->Clone().release()));
  cow_clone.pk_type_ = pk_type_;
  cow_clone.memory_level_ = memory_level_;
  return cow_clone;
}

void VertexTable::DetachIndexer() {
  CHECK(ckp_ != nullptr) << "Checkpoint is null, cannot detach indexer";
  indexer_->Detach(*ckp_, memory_level_);
}

void VertexTable::DetachVertexTimestamp() {
  CHECK(ckp_ != nullptr)
      << "Checkpoint is null, cannot detach vertex timestamp";
  v_ts_->Detach(*ckp_, memory_level_);
}

}  // namespace neug
