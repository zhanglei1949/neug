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

#include <glog/logging.h>
#include <stddef.h>

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "neug/execution/common/data_chunk.h"
#include "neug/storages/loader/loading_config.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/csv/csv_read_config.h"
#include "neug/utils/string_utils.h"

namespace neug {

class ColumnBase;

struct CsvSupplierRuntime;

void printDiskRemaining(const std::string& path);

void put_boolean_option(CsvReadConfig& config);

void put_delimiter_option(const std::string& delimiter_str,
                          CsvReadConfig& config);

std::string process_header_row_token(const std::string& token, bool is_quoting,
                                     char quote_char, bool is_escaping,
                                     char escape_char);

std::vector<std::string> read_header(const std::string& file_name,
                                     const CsvReadConfig& config);

std::vector<std::string> columnMappingsToSelectedCols(
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings);

void put_column_names_option(bool header_row, const std::string& file_path,
                             CsvReadConfig& config, size_t len);

void set_column_types_on_config(const std::vector<DataType>& column_types,
                                const std::vector<std::string>& column_names,
                                CsvReadConfig& config);

/// Build a CsvReadConfig from batch-import style CSV option strings.
CsvReadConfig build_csv_read_config(
    const std::string& file_path,
    const std::unordered_map<std::string, std::string>& csv_options,
    const std::vector<DataType>& column_types);

struct ChunkSourceOptions {
  bool parallel_enabled = true;
  int32_t worker_count = 0;
  int32_t queue_capacity = 0;
  int64_t min_partition_file_bytes = -1;
  bool collect_stats = false;
};

struct ChunkSupplierStats {
  bool parallel = false;
  int32_t worker_count = 0;
  int64_t produced_chunks = 0;
  int64_t produced_rows = 0;
  int64_t consumed_chunks = 0;
  int64_t consumed_rows = 0;
  int64_t bytes_read = 0;
  int64_t producer_wait_ms = 0;
  int64_t consumer_wait_ms = 0;
  int64_t max_queue_size = 0;
  std::string fallback_reason;
};

ChunkSourceOptions resolve_default_chunk_source_options();

class IDataChunkSupplier {
 public:
  virtual ~IDataChunkSupplier() = default;
  virtual std::shared_ptr<execution::DataChunk> GetNextChunk() = 0;
  virtual int64_t RowNum() const = 0;
  virtual std::optional<ChunkSupplierStats> GetStats() const {
    return std::nullopt;
  }
};

class IDataChunkSource {
 public:
  virtual ~IDataChunkSource() = default;
  virtual std::shared_ptr<IDataChunkSupplier> Open() const = 0;
  virtual std::shared_ptr<IDataChunkSupplier> Open(
      const ChunkSourceOptions& options) const {
    (void) options;
    return Open();
  }
  virtual bool rewindable() const = 0;
};

inline constexpr int64_t kUnknownRowNum = -1;

enum class CsvRowCountMode {
  kCountOnOpen,
  kUnknown,
};

/// csv-parser based supplier. Reads CSV in chunks and yields ValueColumns.
class CSVChunkSupplier : public IDataChunkSupplier {
 public:
  CSVChunkSupplier(
      const std::string& file_path, CsvReadConfig config,
      CsvRowCountMode row_count_mode = CsvRowCountMode::kCountOnOpen,
      std::string fallback_reason = "");

  ~CSVChunkSupplier() override;

  std::shared_ptr<execution::DataChunk> GetNextChunk() override;

  int64_t RowNum() const override;

  std::optional<ChunkSupplierStats> GetStats() const override;

 private:
  std::string file_path_;
  std::string fallback_reason_;
  std::unique_ptr<CsvSupplierRuntime> runtime_;
};

class CSVChunkSource : public IDataChunkSource {
 public:
  CSVChunkSource(std::vector<std::string> file_paths, CsvReadConfig config);

  std::shared_ptr<IDataChunkSupplier> Open() const override;
  std::shared_ptr<IDataChunkSupplier> Open(
      const ChunkSourceOptions& options) const override;
  bool rewindable() const override { return true; }

 private:
  std::vector<std::string> file_paths_;
  CsvReadConfig config_;
};

using CSVStreamChunkSupplier = CSVChunkSupplier;
using CSVTableChunkSupplier = CSVChunkSupplier;

void fillVertexReaderMeta(label_t v_label, const std::string& v_label_name,
                          const std::string& v_file,
                          const LoadingConfig& loading_config,
                          const std::vector<std::string>& vertex_property_names,
                          const std::vector<DataTypeId>& vertex_property_types,
                          DataTypeId pk_type, const std::string& pk_name,
                          size_t pk_ind, CsvReadConfig& config);

void fillEdgeReaderMeta(label_t src_label_id, label_t dst_label_id,
                        label_t label_id, const std::string& edge_label_name,
                        const std::string& e_file,
                        const LoadingConfig& loading_config,
                        const std::vector<std::string>& edge_property_names,
                        const std::vector<DataTypeId>& edge_property_types,
                        DataTypeId src_pk_type, DataTypeId dst_pk_type,
                        CsvReadConfig& config);

void set_properties_from_context_column(
    ColumnBase* col, const std::shared_ptr<execution::IContextColumn>& ctx_col,
    const std::vector<vid_t>& vids, std::shared_mutex& mutex);

}  // namespace neug
