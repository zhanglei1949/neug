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

#include "neug/storages/loader/loader_utils.h"

#include <glog/logging.h>
#include <stdint.h>
#include <sys/statvfs.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <sstream>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_set>

#include "csv.hpp"
#include "neug/execution/common/columns/columns_utils.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/types/value.h"
#include "neug/utils/datetime_parsers.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/column.h"
#include "neug/utils/string_utils.h"

namespace neug {

namespace {

constexpr size_t kDefaultCsvChunkRows = 4096;
constexpr size_t kMaxCsvChunkRows = 65536;
constexpr int64_t kDefaultMinPartitionFileBytes = 256LL << 20;
constexpr int32_t kDefaultCsvWorkerCap = 8;

int64_t ns_to_ms(int64_t ns) { return ns / 1000000; }

bool parse_env_bool(const char* value, bool default_value) {
  if (value == nullptr) {
    return default_value;
  }
  std::string normalized(value);
  std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                 [](unsigned char ch) { return std::tolower(ch); });
  if (normalized == "1" || normalized == "true" || normalized == "on" ||
      normalized == "yes") {
    return true;
  }
  if (normalized == "0" || normalized == "false" || normalized == "off" ||
      normalized == "no") {
    return false;
  }
  return default_value;
}

int32_t parse_env_i32(const char* name, int32_t default_value) {
  const char* value = std::getenv(name);
  if (value == nullptr) {
    return default_value;
  }
  try {
    return std::stoi(value);
  } catch (const std::exception&) {
    LOG(WARNING) << "Invalid integer env " << name << "=" << value
                 << ", using default " << default_value;
    return default_value;
  }
}

int64_t parse_env_i64(const char* name, int64_t default_value) {
  const char* value = std::getenv(name);
  if (value == nullptr) {
    return default_value;
  }
  try {
    return std::stoll(value);
  } catch (const std::exception&) {
    LOG(WARNING) << "Invalid int64 env " << name << "=" << value
                 << ", using default " << default_value;
    return default_value;
  }
}

int32_t default_csv_worker_count() {
  auto hw = static_cast<int32_t>(std::thread::hardware_concurrency());
  if (hw <= 0) {
    hw = 1;
  }
  return std::max<int32_t>(1, std::min<int32_t>(hw, kDefaultCsvWorkerCap));
}

template <typename T>
class BoundedBlockingQueue {
 public:
  explicit BoundedBlockingQueue(size_t capacity)
      : capacity_(std::max<size_t>(1, capacity)) {}

  bool Push(T value, std::atomic<int64_t>* wait_ns,
            std::atomic<int64_t>* max_size) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!closed_ && queue_.size() >= capacity_) {
      auto start = std::chrono::steady_clock::now();
      not_full_.wait(lock,
                     [&]() { return closed_ || queue_.size() < capacity_; });
      if (wait_ns != nullptr) {
        auto elapsed = std::chrono::steady_clock::now() - start;
        wait_ns->fetch_add(
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed)
                .count(),
            std::memory_order_relaxed);
      }
    }
    if (closed_) {
      return false;
    }
    queue_.push_back(std::move(value));
    if (max_size != nullptr) {
      int64_t observed = static_cast<int64_t>(queue_.size());
      int64_t current = max_size->load(std::memory_order_relaxed);
      while (observed > current &&
             !max_size->compare_exchange_weak(current, observed,
                                              std::memory_order_relaxed)) {}
    }
    not_empty_.notify_one();
    return true;
  }

  bool Pop(T& value, std::atomic<int64_t>* wait_ns) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!closed_ && queue_.empty()) {
      auto start = std::chrono::steady_clock::now();
      not_empty_.wait(lock, [&]() { return closed_ || !queue_.empty(); });
      if (wait_ns != nullptr) {
        auto elapsed = std::chrono::steady_clock::now() - start;
        wait_ns->fetch_add(
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed)
                .count(),
            std::memory_order_relaxed);
      }
    }
    if (queue_.empty()) {
      return false;
    }
    value = std::move(queue_.front());
    queue_.pop_front();
    not_full_.notify_one();
    return true;
  }

  void Close() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      closed_ = true;
    }
    not_empty_.notify_all();
    not_full_.notify_all();
  }

 private:
  size_t capacity_;
  std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  std::deque<T> queue_;
  bool closed_ = false;
};

size_t resolve_chunk_size(const CsvReadConfig& config) {
  if (config.chunk_size <= 0) {
    return kDefaultCsvChunkRows;
  }
  return static_cast<size_t>(
      std::clamp<int64_t>(config.chunk_size, 1, kMaxCsvChunkRows));
}

csv::CSVFormat build_csv_format(const CsvReadConfig& config) {
  csv::CSVFormat csv_format;
  csv_format.delimiter(config.delimiter);
  if (config.quoting) {
    csv_format.quote(config.quote_char);
    if (config.escaping) {
      LOG_FIRST_N(WARNING, 1)
          << "Both escaping and quoting are enabled for CSV parsing. "
             "Escape characters inside quoted fields may not be handled "
             "correctly by the underlying CSV parser.";
    }
  } else {
    csv_format.quote(false);
  }
  csv_format.variable_columns(csv::VariableColumnPolicy::KEEP);
  csv_format.no_header();
  csv_format.column_names(config.column_names);
  return csv_format;
}

std::unordered_set<std::string> to_lookup_set(
    const std::vector<std::string>& values) {
  std::unordered_set<std::string> lookup_set;
  lookup_set.reserve(values.size());
  for (const auto& value : values) {
    lookup_set.insert(value);
  }
  return lookup_set;
}

std::vector<std::string> resolve_selected_column_names(
    const CsvReadConfig& config) {
  if (!config.include_columns.empty()) {
    return config.include_columns;
  }
  return config.column_names;
}

std::vector<size_t> resolve_selected_column_indices(
    const std::vector<std::string>& selected_column_names,
    const std::vector<std::string>& all_column_names) {
  std::unordered_map<std::string, size_t> name_to_index;
  name_to_index.reserve(all_column_names.size());
  for (size_t index = 0; index < all_column_names.size(); ++index) {
    name_to_index[all_column_names[index]] = index;
  }

  std::vector<size_t> selected_column_indices;
  selected_column_indices.reserve(selected_column_names.size());
  for (const auto& column_name : selected_column_names) {
    auto iter = name_to_index.find(column_name);
    if (iter == name_to_index.end()) {
      THROW_SCHEMA_MISMATCH("Column not found in CSV schema: " + column_name);
    }
    selected_column_indices.push_back(iter->second);
  }
  return selected_column_indices;
}

std::vector<DataType> resolve_selected_column_types(
    const std::vector<std::string>& selected_column_names,
    const CsvReadConfig& config) {
  std::vector<DataType> selected_column_types;
  selected_column_types.reserve(selected_column_names.size());
  for (const auto& column_name : selected_column_names) {
    auto iter = config.column_types.find(column_name);
    if (iter == config.column_types.end()) {
      selected_column_types.emplace_back(DataTypeId::kVarchar);
      continue;
    }
    selected_column_types.push_back(iter->second);
  }
  return selected_column_types;
}

bool parse_timestamp_ms(const std::string& token, int64_t* out_ms) {
  return utils::parse_timestamp_ms(token.data(), token.size(), out_ms);
}

bool parse_epoch_timestamp_ms(const std::string& token, int64_t* out_ms) {
  return utils::parse_epoch_timestamp_ms(token.data(), token.size(), out_ms);
}

std::string canonicalize_bool_token(
    const std::string& token,
    const std::unordered_set<std::string>& true_values,
    const std::unordered_set<std::string>& false_values) {
  if (token == "1" || token == "0") {
    return token;
  }
  if (true_values.contains(token)) {
    return "true";
  }
  if (false_values.contains(token)) {
    return "false";
  }

  auto lowered = to_lower_copy(token);
  if (lowered == "true" || lowered == "false") {
    return lowered;
  }
  THROW_CONVERSION_EXCEPTION("Invalid boolean value: " + token);
}

template <typename T>
execution::Value parse_typed_value(const std::string& token) {
  return execution::Value::CreateValue<T>(
      execution::ValueConverter<T>::typed_from_string(token));
}

execution::Value parse_date_value(const std::string& token) {
  int64_t millis = 0;
  if (parse_timestamp_ms(token, &millis) ||
      parse_epoch_timestamp_ms(token, &millis)) {
    Date d;
    d.from_timestamp(millis);
    return execution::Value::CreateValue<Date>(d);
  }
  return parse_typed_value<execution::date_t>(token);
}

execution::Value parse_timestamp_value(const std::string& token) {
  int64_t millis = 0;
  if (parse_timestamp_ms(token, &millis) ||
      parse_epoch_timestamp_ms(token, &millis)) {
    return execution::Value::CreateValue<execution::timestamp_ms_t>(
        DateTime(millis));
  }
  return parse_typed_value<execution::timestamp_ms_t>(token);
}

execution::Value parse_value_by_type(
    const std::string& token, const DataType& data_type,
    const std::unordered_set<std::string>& true_values,
    const std::unordered_set<std::string>& false_values) {
  switch (data_type.id()) {
  case DataTypeId::kInt32:
    return parse_typed_value<int32_t>(token);
  case DataTypeId::kInt64:
    return parse_typed_value<int64_t>(token);
  case DataTypeId::kUInt32:
    return parse_typed_value<uint32_t>(token);
  case DataTypeId::kUInt64:
    return parse_typed_value<uint64_t>(token);
  case DataTypeId::kFloat:
    return parse_typed_value<float>(token);
  case DataTypeId::kDouble:
    return parse_typed_value<double>(token);
  case DataTypeId::kBoolean:
    return parse_typed_value<bool>(
        canonicalize_bool_token(token, true_values, false_values));
  case DataTypeId::kDate:
    return parse_date_value(token);
  case DataTypeId::kTimestampMs:
    return parse_timestamp_value(token);
  case DataTypeId::kInterval:
    return parse_typed_value<execution::interval_t>(token);
  case DataTypeId::kVarchar:
    return parse_typed_value<std::string>(token);
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported data type in CSV parser: " +
                                  data_type.ToString());
  }
}

std::string unescape_token(const std::string& token, char escape_char) {
  std::string res;
  res.reserve(token.size());
  for (size_t i = 0; i < token.size(); ++i) {
    if (token[i] == escape_char && i + 1 < token.size()) {
      res.push_back(token[i + 1]);
      ++i;
    } else {
      res.push_back(token[i]);
    }
  }
  return res;
}

void append_csv_token_to_builder(
    std::shared_ptr<execution::IContextColumnBuilder>& builder,
    const DataType& data_type, std::string token,
    const std::unordered_set<std::string>& null_values,
    const std::unordered_set<std::string>& true_values,
    const std::unordered_set<std::string>& false_values,
    const std::string& file_path, int64_t row_number,
    const std::string& column_name, bool escaping, char escape_char) {
  if (null_values.contains(token)) {
    builder->push_back_null();
    return;
  }

  if (escaping) {
    token = unescape_token(token, escape_char);
  }

  try {
    builder->push_back_elem(
        parse_value_by_type(token, data_type, true_values, false_values));
  } catch (const std::exception& error) {
    THROW_CONVERSION_EXCEPTION("Failed to parse CSV field, file=" + file_path +
                               ", row=" + std::to_string(row_number) +
                               ", column=" + column_name +
                               ", type=" + data_type.ToString() + ", value='" +
                               token + "', reason=" + error.what());
  }
}

void append_csv_field_to_builder(
    std::shared_ptr<execution::IContextColumnBuilder>& builder,
    const DataType& data_type, csv::CSVField field,
    const std::unordered_set<std::string>& null_values,
    const std::unordered_set<std::string>& true_values,
    const std::unordered_set<std::string>& false_values,
    const std::string& file_path, int64_t row_number,
    const std::string& column_name, bool escaping, char escape_char) {
  if (field.is_null()) {
    builder->push_back_null();
    return;
  }
  append_csv_token_to_builder(builder, data_type, field.get<std::string>(),
                              null_values, true_values, false_values, file_path,
                              row_number, column_name, escaping, escape_char);
}

}  // namespace

ChunkSourceOptions resolve_default_chunk_source_options() {
  ChunkSourceOptions options;
  if (const char* mode = std::getenv("NEUG_COPY_PARALLEL_CSV")) {
    std::string normalized(mode);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char ch) { return std::tolower(ch); });
    if (normalized == "0" || normalized == "false" || normalized == "off" ||
        normalized == "no") {
      options.parallel_enabled = false;
    } else if (normalized == "1" || normalized == "true" ||
               normalized == "on" || normalized == "yes") {
      options.parallel_enabled = true;
      options.min_partition_file_bytes = 0;
    }
  }
  options.worker_count =
      parse_env_i32("NEUG_COPY_CSV_WORKERS", options.worker_count);
  options.queue_capacity =
      parse_env_i32("NEUG_COPY_CSV_QUEUE_CAPACITY", options.queue_capacity);
  options.min_partition_file_bytes = parse_env_i64(
      "NEUG_COPY_CSV_MIN_FILE_BYTES", options.min_partition_file_bytes);
  options.collect_stats =
      parse_env_bool(std::getenv("NEUG_COPY_CSV_STATS"), options.collect_stats);
  return options;
}

struct CsvSupplierRuntime {
  explicit CsvSupplierRuntime(const std::string& file_path,
                              const CsvReadConfig& config,
                              CsvRowCountMode row_count_mode)
      : file_path_(file_path),
        csv_format_(build_csv_format(config)),
        selected_column_names_(resolve_selected_column_names(config)),
        selected_column_indices_(resolve_selected_column_indices(
            selected_column_names_, config.column_names)),
        selected_column_types_(
            resolve_selected_column_types(selected_column_names_, config)),
        null_values_(to_lookup_set(config.null_values)),
        true_values_(to_lookup_set(config.true_values)),
        false_values_(to_lookup_set(config.false_values)),
        rows_to_skip_(std::max<int64_t>(0, config.skip_rows)),
        chunk_size_(resolve_chunk_size(config)),
        escaping_(config.escaping),
        escape_char_(config.escape_char) {
    if (selected_column_indices_.empty()) {
      THROW_SCHEMA_MISMATCH("No columns selected for CSV file: " + file_path_);
    }
    row_num_ = row_count_mode == CsvRowCountMode::kCountOnOpen ? count_rows()
                                                               : kUnknownRowNum;
    reset_reader();
  }

  std::shared_ptr<execution::DataChunk> get_next_chunk() {
    if (!reader_) {
      return nullptr;
    }

    std::vector<std::shared_ptr<execution::IContextColumnBuilder>> builders;
    builders.reserve(selected_column_types_.size());
    for (const auto& column_type : selected_column_types_) {
      auto builder = execution::ColumnsUtils::create_builder(column_type);
      builder->reserve(chunk_size_);
      builders.emplace_back(std::move(builder));
    }

    csv::CSVRow row;
    size_t output_rows = 0;
    while (output_rows < chunk_size_ && reader_->read_row(row)) {
      if (row.empty()) {
        continue;
      }
      current_row_number_ += 1;

      for (size_t column_index = 0; column_index < builders.size();
           ++column_index) {
        auto physical_index = selected_column_indices_[column_index];
        if (physical_index >= row.size()) {
          builders[column_index]->push_back_null();
          continue;
        }
        append_csv_field_to_builder(
            builders[column_index], selected_column_types_[column_index],
            row[physical_index], null_values_, true_values_, false_values_,
            file_path_, current_row_number_,
            selected_column_names_[column_index], escaping_, escape_char_);
      }
      output_rows += 1;
    }

    if (output_rows == 0) {
      return nullptr;
    }

    auto chunk = std::make_shared<execution::DataChunk>();
    for (size_t column_index = 0; column_index < builders.size();
         ++column_index) {
      chunk->set(static_cast<int>(column_index),
                 builders[column_index]->finish());
    }
    produced_chunks_ += 1;
    produced_rows_ += static_cast<int64_t>(output_rows);
    return chunk;
  }

  int64_t row_num() const { return row_num_; }
  int64_t produced_chunks() const { return produced_chunks_; }
  int64_t produced_rows() const { return produced_rows_; }

 private:
  void reset_reader() {
    try {
      reader_ = std::make_unique<csv::CSVReader>(file_path_, csv_format_);
      skip_rows(*reader_);
      current_row_number_ = rows_to_skip_;
    } catch (const std::exception& error) {
      THROW_IO_EXCEPTION("Failed to initialize CSV reader for file: " +
                         file_path_ + ", reason=" + error.what());
    }
  }

  void skip_rows(csv::CSVReader& reader) const {
    csv::CSVRow skipped_row;
    for (int64_t index = 0; index < rows_to_skip_; ++index) {
      if (!reader.read_row(skipped_row)) {
        break;
      }
    }
  }

  int64_t count_rows() const {
    try {
      csv::CSVReader counter(file_path_, csv_format_);
      skip_rows(counter);
      int64_t count = 0;
      csv::CSVRow row;
      while (counter.read_row(row)) {
        if (row.empty()) {
          continue;
        }
        count += 1;
      }
      return count;
    } catch (const std::exception& error) {
      THROW_IO_EXCEPTION("Failed to count rows for file: " + file_path_ +
                         ", reason=" + error.what());
    }
  }

 private:
  std::string file_path_;
  csv::CSVFormat csv_format_;
  std::vector<std::string> selected_column_names_;
  std::vector<size_t> selected_column_indices_;
  std::vector<DataType> selected_column_types_;
  std::unordered_set<std::string> null_values_;
  std::unordered_set<std::string> true_values_;
  std::unordered_set<std::string> false_values_;
  int64_t rows_to_skip_ = 0;
  size_t chunk_size_ = kDefaultCsvChunkRows;
  bool escaping_ = false;
  char escape_char_ = '\\';
  int64_t row_num_ = 0;
  int64_t current_row_number_ = 0;
  int64_t produced_chunks_ = 0;
  int64_t produced_rows_ = 0;
  std::unique_ptr<csv::CSVReader> reader_;
};

static bool put_skip_rows_option(const LoadingConfig& loading_config,
                                 CsvReadConfig& config) {
  bool header_row = loading_config.GetHasHeaderRow();
  config.skip_rows = header_row ? 1 : 0;
  return header_row;
}

static void put_escape_char_option(const LoadingConfig& loading_config,
                                   CsvReadConfig& config) {
  config.escaping = loading_config.GetIsEscaping();
  if (config.escaping) {
    config.escape_char = loading_config.GetEscapeChar();
  }
}

static void put_block_size_option(const LoadingConfig& loading_config,
                                  CsvReadConfig& config) {
  auto batch_size = loading_config.GetBatchSize();
  if (batch_size <= 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Block size should be positive");
  }
  config.chunk_size = batch_size;
}

static void put_quote_char_option(const LoadingConfig& loading_config,
                                  CsvReadConfig& config) {
  config.quoting = loading_config.GetIsQuoting();
  if (config.quoting) {
    config.quote_char = loading_config.GetQuotingChar();
  }
  config.double_quote = loading_config.GetIsDoubleQuoting();
}

static void put_null_values(const LoadingConfig& loading_config,
                            CsvReadConfig& config) {
  config.null_values = loading_config.GetNullValues();
}

void printDiskRemaining(const std::string& path) {
  struct statvfs buf;
  if (statvfs(path.c_str(), &buf) == 0) {
    LOG(INFO) << "Disk remaining: " << buf.f_bsize * buf.f_bavail / 1024 / 1024
              << "MB";
  }
}

void put_delimiter_option(const std::string& delimiter_str,
                          CsvReadConfig& config) {
  if (delimiter_str.size() != 1 && delimiter_str[0] != '\\') {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Delimiter should be a single character, or a escape "
        "character, like '\\t'");
  }
  if (delimiter_str[0] == '\\') {
    if (delimiter_str.size() != 2) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Delimiter should be a single character");
    }
    switch (delimiter_str[1]) {
    case 't':
      config.delimiter = '\t';
      break;
    default:
      THROW_INVALID_ARGUMENT_EXCEPTION("Unsupported escape character: " +
                                       std::string(1, delimiter_str[1]));
    }
  } else {
    config.delimiter = delimiter_str[0];
  }
}

void put_boolean_option(CsvReadConfig& config) {
  config.true_values = {"True", "true", "TRUE"};
  config.false_values = {"False", "false", "FALSE"};
}

std::string process_header_row_token(const std::string& token, bool is_quoting,
                                     char quote_char, bool is_escaping,
                                     char escape_char) {
  std::string new_token = token;
  // trim the quote char at the beginning and end of the token
  if (is_quoting) {
    if (token.size() >= 2 && token[0] == quote_char &&
        token[token.size() - 1] == quote_char) {
      new_token = token.substr(1, token.size() - 2);
    }
  }
  // unescape the token
  if (is_escaping) {
    std::string res;
    for (size_t i = 0; i < new_token.size(); ++i) {
      if (new_token[i] == escape_char) {
        if (i + 1 < new_token.size()) {
          res.push_back(new_token[i + 1]);
          i++;
        }
      } else {
        res.push_back(new_token[i]);
      }
    }
    new_token = res;
  }
  return new_token;
}

std::vector<std::string> read_header_manual(const std::string& file_name,
                                            char delimiter, bool is_quoting,
                                            char quote_char, bool is_escaping,
                                            char escape_char) {
  std::vector<std::string> res_vec;
  std::ifstream file(file_name);
  std::string line;
  if (file.is_open()) {
    if (std::getline(file, line)) {
      std::stringstream ss(line);
      std::string token;
      while (std::getline(ss, token, delimiter)) {
        size_t endpos = token.find_last_not_of(" \n\r\t");
        if (endpos == std::string::npos) {
          token.clear();
        } else {
          token.erase(endpos + 1);
        }
        token = process_header_row_token(token, is_quoting, quote_char,
                                         is_escaping, escape_char);
        res_vec.push_back(token);
      }
    } else {
      file.close();
      THROW_IO_EXCEPTION("Fail to read header line of file: " + file_name);
    }
    file.close();
  } else {
    THROW_IO_EXCEPTION("Fail to open file: " + file_name);
  }
  return res_vec;
}

std::vector<std::string> read_header(const std::string& file_name,
                                     const CsvReadConfig& config) {
  if (config.escaping) {
    return read_header_manual(file_name, config.delimiter, config.quoting,
                              config.quote_char, config.escaping,
                              config.escape_char);
  }
  try {
    csv::CSVFormat csv_format;
    csv_format.delimiter(config.delimiter);
    if (config.quoting) {
      csv_format.quote(config.quote_char);
    } else {
      csv_format.quote(false);
    }
    csv_format.no_header();
    csv::CSVReader reader(file_name, csv_format);
    csv::CSVRow row;
    if (!reader.read_row(row)) {
      THROW_IO_EXCEPTION("Fail to read header line of file: " + file_name);
    }
    std::vector<std::string> res_vec;
    res_vec.reserve(row.size());
    for (size_t i = 0; i < row.size(); ++i) {
      res_vec.push_back(row[i].get<std::string>());
    }
    return res_vec;
  } catch (const std::exception& error) {
    THROW_IO_EXCEPTION("Fail to read header line of file: " + file_name +
                       ", reason=" + error.what());
  }
}

void deduplicate_column_names(std::vector<std::string>& all_column_names) {
  std::unordered_map<std::string, int> name_count;
  for (auto& name : all_column_names) {
    if (name_count.find(name) == name_count.end()) {
      name_count[name] = 1;
    } else {
      name_count[name]++;
    }
  }
  for (size_t i = 0; i < all_column_names.size(); ++i) {
    auto& name = all_column_names[i];
    if (name_count[name] > 1) {
      auto cur_cnt = name_count[name];
      name_count[name] -= 1;
      all_column_names[i] = name + "_" + std::to_string(cur_cnt);
    }
  }
}

void put_column_names_option(bool header_row, const std::string& file_path,
                             CsvReadConfig& config, size_t len) {
  if (header_row) {
    config.column_names = read_header(file_path, config);
    deduplicate_column_names(config.column_names);
  } else {
    config.column_names.resize(len);
    for (size_t i = 0; i < config.column_names.size(); ++i) {
      config.column_names[i] = std::string("f") + std::to_string(i);
    }
  }
  VLOG(10) << "Got all column names: " << config.column_names.size()
           << neug::to_string(config.column_names);
}

void set_column_types_on_config(const std::vector<DataType>& column_types,
                                const std::vector<std::string>& column_names,
                                CsvReadConfig& config) {
  if (column_types.size() != column_names.size()) {
    THROW_RUNTIME_ERROR("Column types size does not match column names size: " +
                        std::to_string(column_types.size()) + " vs " +
                        std::to_string(column_names.size()));
  }
  for (size_t i = 0; i < column_types.size(); ++i) {
    const auto& col_name = column_names[i];
    if (config.column_types.find(col_name) != config.column_types.end()) {
      THROW_RUNTIME_ERROR("Duplicate column name found: " + col_name);
    }
    config.column_types.insert({col_name, column_types[i]});
  }
}

CsvReadConfig build_csv_read_config(
    const std::string& file_path,
    const std::unordered_map<std::string, std::string>& csv_options,
    const std::vector<DataType>& column_types) {
  CsvReadConfig config;
  put_boolean_option(config);

  static constexpr const char* kDefaultDelimiter = "|";
  if (csv_options.count("DELIMITER")) {
    put_delimiter_option(csv_options.at("DELIMITER"), config);
  } else if (csv_options.count("DELIM")) {
    put_delimiter_option(csv_options.at("DELIM"), config);
  } else {
    put_delimiter_option(kDefaultDelimiter, config);
  }

  if (csv_options.count("ESCAPE")) {
    if (csv_options.at("ESCAPE").size() == 1) {
      config.escaping = true;
      config.escape_char = csv_options.at("ESCAPE")[0];
    } else {
      config.escaping = false;
    }
  }

  if (csv_options.count("QUOTE")) {
    if (csv_options.at("QUOTE").size() == 1) {
      config.quoting = true;
      config.double_quote = false;
      config.quote_char = csv_options.at("QUOTE")[0];
    } else {
      config.quoting = false;
    }
  }

  if (csv_options.count("DOUBLE_QUOTE")) {
    if (!config.quoting) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "CSV quoting must be enabled for double quotes");
    }
    auto value = csv_options.at("DOUBLE_QUOTE");
    config.double_quote = (value == "true" || value == "1" || value == "TRUE");
  }

  if (csv_options.count("PARALLEL")) {
    auto value = to_lower_copy(csv_options.at("PARALLEL"));
    config.parallel.enabled =
        !(value == "0" || value == "false" || value == "off" || value == "no");
  }

  bool header_row = true;
  if (csv_options.count("HEADER")) {
    auto val = to_lower_copy(csv_options.at("HEADER"));
    if (val == "false" || val == "0") {
      header_row = false;
    }
  }

  put_column_names_option(header_row, file_path, config, column_types.size());
  if (config.column_names.size() != column_types.size()) {
    THROW_SCHEMA_MISMATCH("Schema mismatch: column names size (" +
                          std::to_string(config.column_names.size()) +
                          ") does not match column types size (" +
                          std::to_string(column_types.size()) + ")");
  }
  set_column_types_on_config(column_types, config.column_names, config);
  config.include_columns = config.column_names;
  if (header_row) {
    config.skip_rows = 1;
  }
  return config;
}

std::vector<std::string> columnMappingsToSelectedCols(
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings) {
  std::vector<std::string> selected_cols;
  for (auto& column_mapping : column_mappings) {
    selected_cols.push_back(std::get<1>(column_mapping));
  }
  return selected_cols;
}

CSVChunkSupplier::CSVChunkSupplier(const std::string& file_path,
                                   CsvReadConfig config,
                                   CsvRowCountMode row_count_mode,
                                   std::string fallback_reason)
    : file_path_(file_path), fallback_reason_(std::move(fallback_reason)) {
  runtime_ =
      std::make_unique<CsvSupplierRuntime>(file_path, config, row_count_mode);
  VLOG(10) << "Finish init CSVChunkSupplier for file: " << file_path_;
}

CSVChunkSupplier::~CSVChunkSupplier() = default;

std::shared_ptr<execution::DataChunk> CSVChunkSupplier::GetNextChunk() {
  if (!runtime_) {
    THROW_IO_EXCEPTION("CSV runtime is null for file: " + file_path_);
  }
  return runtime_->get_next_chunk();
}

int64_t CSVChunkSupplier::RowNum() const {
  if (!runtime_) {
    return kUnknownRowNum;
  }
  return runtime_->row_num();
}

std::optional<ChunkSupplierStats> CSVChunkSupplier::GetStats() const {
  if (!runtime_ && fallback_reason_.empty()) {
    return std::nullopt;
  }
  ChunkSupplierStats stats;
  if (runtime_) {
    stats.produced_chunks = runtime_->produced_chunks();
    stats.produced_rows = runtime_->produced_rows();
    stats.consumed_chunks = stats.produced_chunks;
    stats.consumed_rows = stats.produced_rows;
  }
  stats.fallback_reason = fallback_reason_;
  return stats;
}

namespace {

class PartitionedCSVChunkSupplier final : public IDataChunkSupplier {
 public:
  PartitionedCSVChunkSupplier(const std::string& file_path,
                              CsvReadConfig config, ChunkSourceOptions options)
      : file_path_(file_path),
        config_(std::move(config)),
        selected_column_names_(resolve_selected_column_names(config_)),
        selected_column_indices_(resolve_selected_column_indices(
            selected_column_names_, config_.column_names)),
        selected_column_types_(
            resolve_selected_column_types(selected_column_names_, config_)),
        null_values_(to_lookup_set(config_.null_values)),
        true_values_(to_lookup_set(config_.true_values)),
        false_values_(to_lookup_set(config_.false_values)),
        rows_to_skip_(std::max<int64_t>(0, config_.skip_rows)),
        chunk_size_(resolve_chunk_size(config_)),
        escaping_(config_.escaping),
        escape_char_(config_.escape_char),
        worker_count_(resolve_worker_count(config_, options)),
        queue_capacity_(resolve_queue_capacity(config_, options)),
        queue_(queue_capacity_) {
    if (selected_column_indices_.empty()) {
      THROW_SCHEMA_MISMATCH("No columns selected for CSV file: " + file_path_);
    }
    physical_to_output_.assign(config_.column_names.size(), -1);
    for (size_t output_index = 0;
         output_index < selected_column_indices_.size(); ++output_index) {
      auto physical_index = selected_column_indices_[output_index];
      if (physical_index < physical_to_output_.size()) {
        physical_to_output_[physical_index] = static_cast<int>(output_index);
      }
    }
    file_size_ = static_cast<int64_t>(std::filesystem::file_size(file_path_));
    if (file_size_ > 0) {
      worker_count_ = std::min<int32_t>(
          worker_count_, static_cast<int32_t>(std::min<int64_t>(
                             file_size_, std::numeric_limits<int32_t>::max())));
    }
    LOG(INFO) << "PartitionedCSVChunkSupplier opened: file=" << file_path_
              << ", file_size=" << file_size_ << ", workers=" << worker_count_
              << ", queue_capacity=" << queue_capacity_
              << ", chunk_size=" << chunk_size_
              << ", skip_rows=" << rows_to_skip_
              << ", physical_columns=" << config_.column_names.size()
              << ", selected_columns=" << selected_column_names_.size()
              << ", delimiter='" << config_.delimiter << "'";
    start_workers();
  }

  ~PartitionedCSVChunkSupplier() override {
    stop_.store(true, std::memory_order_relaxed);
    queue_.Close();
    for (auto& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    log_final_stats_once("closed");
  }

  std::shared_ptr<execution::DataChunk> GetNextChunk() override {
    std::shared_ptr<execution::DataChunk> chunk;
    if (queue_.Pop(chunk, &consumer_wait_ns_)) {
      consumed_chunks_.fetch_add(1, std::memory_order_relaxed);
      consumed_rows_.fetch_add(static_cast<int64_t>(chunk->row_num()),
                               std::memory_order_relaxed);
      return chunk;
    }
    rethrow_error_if_any();
    log_final_stats_once("completed");
    return nullptr;
  }

  int64_t RowNum() const override { return kUnknownRowNum; }

  std::optional<ChunkSupplierStats> GetStats() const override {
    ChunkSupplierStats stats;
    stats.parallel = true;
    stats.worker_count = worker_count_;
    stats.produced_chunks = produced_chunks_.load(std::memory_order_relaxed);
    stats.produced_rows = produced_rows_.load(std::memory_order_relaxed);
    stats.consumed_chunks = consumed_chunks_.load(std::memory_order_relaxed);
    stats.consumed_rows = consumed_rows_.load(std::memory_order_relaxed);
    stats.bytes_read = bytes_read_.load(std::memory_order_relaxed);
    stats.producer_wait_ms =
        ns_to_ms(producer_wait_ns_.load(std::memory_order_relaxed));
    stats.consumer_wait_ms =
        ns_to_ms(consumer_wait_ns_.load(std::memory_order_relaxed));
    stats.max_queue_size = max_queue_size_.load(std::memory_order_relaxed);
    return stats;
  }

 private:
  static int32_t resolve_worker_count(const CsvReadConfig& config,
                                      const ChunkSourceOptions& options) {
    int32_t workers = options.worker_count > 0 ? options.worker_count
                                               : config.parallel.worker_count;
    if (workers <= 0) {
      workers = default_csv_worker_count();
    }
    return std::max<int32_t>(1, workers);
  }

  static int32_t resolve_queue_capacity(const CsvReadConfig& config,
                                        const ChunkSourceOptions& options) {
    int32_t capacity = options.queue_capacity > 0
                           ? options.queue_capacity
                           : config.parallel.queue_capacity;
    if (capacity <= 0) {
      auto workers = resolve_worker_count(config, options);
      capacity = std::max<int32_t>(2, workers * 2);
    }
    return capacity;
  }

  std::vector<std::shared_ptr<execution::IContextColumnBuilder>>
  create_builders() const {
    std::vector<std::shared_ptr<execution::IContextColumnBuilder>> builders;
    builders.reserve(selected_column_types_.size());
    for (const auto& column_type : selected_column_types_) {
      auto builder = execution::ColumnsUtils::create_builder(column_type);
      builder->reserve(chunk_size_);
      builders.emplace_back(std::move(builder));
    }
    return builders;
  }

  std::shared_ptr<execution::DataChunk> finish_chunk(
      std::vector<std::shared_ptr<execution::IContextColumnBuilder>>& builders)
      const {
    auto chunk = std::make_shared<execution::DataChunk>();
    for (size_t column_index = 0; column_index < builders.size();
         ++column_index) {
      chunk->set(static_cast<int>(column_index),
                 builders[column_index]->finish());
    }
    return chunk;
  }

  void append_line(
      const std::string& line, int64_t row_number,
      std::vector<std::shared_ptr<execution::IContextColumnBuilder>>&
          builders) {
    std::vector<std::string_view> selected_tokens(builders.size());
    std::vector<bool> found(builders.size(), false);

    size_t field_index = 0;
    size_t field_start = 0;
    while (field_start <= line.size()) {
      auto field_end = line.find(config_.delimiter, field_start);
      if (field_end == std::string::npos) {
        field_end = line.size();
      }
      if (field_index < physical_to_output_.size()) {
        int output_index = physical_to_output_[field_index];
        if (output_index >= 0) {
          selected_tokens[output_index] = std::string_view(
              line.data() + field_start, field_end - field_start);
          found[output_index] = true;
        }
      }
      if (field_end == line.size()) {
        break;
      }
      field_start = field_end + 1;
      ++field_index;
    }

    for (size_t column_index = 0; column_index < builders.size();
         ++column_index) {
      if (!found[column_index]) {
        builders[column_index]->push_back_null();
        continue;
      }
      append_csv_token_to_builder(
          builders[column_index], selected_column_types_[column_index],
          std::string(selected_tokens[column_index]), null_values_,
          true_values_, false_values_, file_path_, row_number,
          selected_column_names_[column_index], escaping_, escape_char_);
    }
  }

  bool push_chunk(
      std::vector<std::shared_ptr<execution::IContextColumnBuilder>>& builders,
      size_t output_rows) {
    if (output_rows == 0) {
      return false;
    }
    auto chunk = finish_chunk(builders);
    if (!queue_.Push(std::move(chunk), &producer_wait_ns_, &max_queue_size_)) {
      return false;
    }
    produced_chunks_.fetch_add(1, std::memory_order_relaxed);
    produced_rows_.fetch_add(static_cast<int64_t>(output_rows),
                             std::memory_order_relaxed);
    return true;
  }

  void start_workers() {
    LOG(INFO) << "PartitionedCSVChunkSupplier starting workers: file="
              << file_path_ << ", workers=" << worker_count_;
    active_workers_.store(worker_count_, std::memory_order_relaxed);
    workers_.reserve(worker_count_);
    for (int32_t worker_id = 0; worker_id < worker_count_; ++worker_id) {
      workers_.emplace_back([this, worker_id]() { worker_main(worker_id); });
    }
  }

  void worker_main(int32_t worker_id) {
    try {
      read_partition(worker_id);
    } catch (const std::exception& e) {
      LOG(ERROR) << "PartitionedCSVChunkSupplier worker failed: file="
                 << file_path_ << ", worker=" << worker_id
                 << ", error=" << e.what();
      set_error(std::current_exception());
    } catch (...) {
      LOG(ERROR) << "PartitionedCSVChunkSupplier worker failed: file="
                 << file_path_ << ", worker=" << worker_id
                 << ", error=<unknown>";
      set_error(std::current_exception());
    }
    if (active_workers_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      queue_.Close();
    }
  }

  void read_partition(int32_t worker_id) {
    if (file_size_ <= 0) {
      return;
    }
    int64_t start = file_size_ * worker_id / worker_count_;
    int64_t end = file_size_ * (worker_id + 1) / worker_count_;
    LOG(INFO) << "PartitionedCSVChunkSupplier worker started: file="
              << file_path_ << ", worker=" << worker_id << "/" << worker_count_
              << ", byte_range=[" << start << ", " << end << ")";

    std::ifstream file(file_path_, std::ios::binary);
    if (!file.is_open()) {
      THROW_IO_EXCEPTION("Failed to open CSV file: " + file_path_);
    }

    std::string line;
    int64_t row_number = 0;
    if (worker_id == 0) {
      for (int64_t skipped = 0; skipped < rows_to_skip_; ++skipped) {
        if (!std::getline(file, line)) {
          return;
        }
      }
      row_number = rows_to_skip_;
    } else {
      if (start <= 0) {
        file.seekg(0);
      } else {
        file.seekg(start - 1);
        char previous = '\0';
        file.get(previous);
        if (previous == '\n') {
          file.seekg(start);
        } else {
          if (!std::getline(file, line)) {
            return;
          }
        }
      }
    }

    auto builders = create_builders();
    size_t output_rows = 0;
    int64_t local_rows = 0;
    int64_t local_chunks = 0;
    int64_t local_bytes = 0;
    while (!stop_.load(std::memory_order_relaxed)) {
      auto pos = file.tellg();
      if (pos == std::streampos(-1)) {
        break;
      }
      if (static_cast<int64_t>(pos) >= end && worker_id != worker_count_ - 1) {
        break;
      }
      if (!std::getline(file, line)) {
        break;
      }
      local_bytes += static_cast<int64_t>(line.size() + 1);
      bytes_read_.fetch_add(static_cast<int64_t>(line.size() + 1),
                            std::memory_order_relaxed);
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }
      if (line.empty()) {
        continue;
      }
      ++row_number;
      append_line(line, row_number, builders);
      ++output_rows;
      ++local_rows;
      if (output_rows >= chunk_size_) {
        if (push_chunk(builders, output_rows)) {
          ++local_chunks;
        } else {
          break;
        }
        builders = create_builders();
        output_rows = 0;
      }
    }
    if (push_chunk(builders, output_rows)) {
      ++local_chunks;
    }
    LOG(INFO) << "PartitionedCSVChunkSupplier worker finished: file="
              << file_path_ << ", worker=" << worker_id << ", byte_range=["
              << start << ", " << end << ")"
              << ", rows=" << local_rows << ", chunks=" << local_chunks
              << ", bytes_read=" << local_bytes
              << ", stopped=" << stop_.load(std::memory_order_relaxed);
  }

  void set_error(std::exception_ptr error) {
    bool expected = false;
    if (has_error_.compare_exchange_strong(expected, true,
                                           std::memory_order_acq_rel)) {
      {
        std::lock_guard<std::mutex> lock(error_mutex_);
        first_error_ = std::move(error);
      }
      stop_.store(true, std::memory_order_relaxed);
      queue_.Close();
    }
  }

  void rethrow_error_if_any() const {
    if (!has_error_.load(std::memory_order_acquire)) {
      return;
    }
    std::exception_ptr error;
    {
      std::lock_guard<std::mutex> lock(error_mutex_);
      error = first_error_;
    }
    if (error) {
      std::rethrow_exception(error);
    }
  }

  void log_final_stats_once(const char* state) {
    bool expected = false;
    if (!final_stats_logged_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
      return;
    }
    auto stats = GetStats();
    CHECK(stats.has_value());
    LOG(INFO) << "PartitionedCSVChunkSupplier " << state
              << ": file=" << file_path_
              << ", produced_chunks=" << stats->produced_chunks
              << ", produced_rows=" << stats->produced_rows
              << ", consumed_chunks=" << stats->consumed_chunks
              << ", consumed_rows=" << stats->consumed_rows
              << ", bytes_read=" << stats->bytes_read
              << ", producer_wait_ms=" << stats->producer_wait_ms
              << ", consumer_wait_ms=" << stats->consumer_wait_ms
              << ", max_queue_size=" << stats->max_queue_size
              << ", workers=" << stats->worker_count
              << ", has_error=" << has_error_.load(std::memory_order_relaxed);
  }

 private:
  std::string file_path_;
  CsvReadConfig config_;
  std::vector<std::string> selected_column_names_;
  std::vector<size_t> selected_column_indices_;
  std::vector<DataType> selected_column_types_;
  std::vector<int> physical_to_output_;
  std::unordered_set<std::string> null_values_;
  std::unordered_set<std::string> true_values_;
  std::unordered_set<std::string> false_values_;
  int64_t rows_to_skip_ = 0;
  size_t chunk_size_ = kDefaultCsvChunkRows;
  bool escaping_ = false;
  char escape_char_ = '\\';
  int32_t worker_count_ = 1;
  int32_t queue_capacity_ = 1;
  int64_t file_size_ = 0;
  BoundedBlockingQueue<std::shared_ptr<execution::DataChunk>> queue_;
  std::vector<std::thread> workers_;
  std::atomic<int32_t> active_workers_{0};
  std::atomic<bool> stop_{false};
  std::atomic<bool> has_error_{false};
  mutable std::mutex error_mutex_;
  std::exception_ptr first_error_;
  std::atomic<int64_t> produced_chunks_{0};
  std::atomic<int64_t> produced_rows_{0};
  std::atomic<int64_t> consumed_chunks_{0};
  std::atomic<int64_t> consumed_rows_{0};
  std::atomic<int64_t> bytes_read_{0};
  std::atomic<int64_t> producer_wait_ns_{0};
  std::atomic<int64_t> consumer_wait_ns_{0};
  std::atomic<int64_t> max_queue_size_{0};
  std::atomic<bool> final_stats_logged_{false};
};

class ChainedChunkSupplier final : public IDataChunkSupplier {
 public:
  ChainedChunkSupplier(std::vector<std::string> file_paths,
                       CsvReadConfig config)
      : file_paths_(std::move(file_paths)), config_(std::move(config)) {}

  std::shared_ptr<execution::DataChunk> GetNextChunk() override {
    while (index_ < file_paths_.size()) {
      if (current_ == nullptr) {
        current_ = std::make_shared<CSVChunkSupplier>(
            file_paths_[index_], config_, CsvRowCountMode::kUnknown);
      }
      auto chunk = current_->GetNextChunk();
      if (chunk) {
        return chunk;
      }
      current_.reset();
      ++index_;
    }
    return nullptr;
  }

  int64_t RowNum() const override { return kUnknownRowNum; }

 private:
  std::vector<std::string> file_paths_;
  CsvReadConfig config_;
  std::shared_ptr<IDataChunkSupplier> current_;
  size_t index_ = 0;
};

struct EffectiveCsvParallelOptions {
  bool enabled = true;
  int32_t worker_count = 0;
  int32_t queue_capacity = 0;
  int64_t min_partition_file_bytes = kDefaultMinPartitionFileBytes;
  bool collect_stats = false;
};

EffectiveCsvParallelOptions merge_parallel_options(
    const CsvReadConfig& config, const ChunkSourceOptions& options) {
  EffectiveCsvParallelOptions effective;
  effective.enabled = config.parallel.enabled && options.parallel_enabled;
  effective.worker_count = options.worker_count > 0
                               ? options.worker_count
                               : config.parallel.worker_count;
  if (effective.worker_count <= 0) {
    effective.worker_count = default_csv_worker_count();
  }
  effective.queue_capacity = options.queue_capacity > 0
                                 ? options.queue_capacity
                                 : config.parallel.queue_capacity;
  effective.min_partition_file_bytes =
      options.min_partition_file_bytes >= 0
          ? options.min_partition_file_bytes
          : config.parallel.min_partition_file_bytes;
  effective.collect_stats =
      config.parallel.collect_stats || options.collect_stats;
  return effective;
}

std::string partitioned_csv_fallback_reason(
    const std::vector<std::string>& file_paths, const CsvReadConfig& config,
    const EffectiveCsvParallelOptions& options, int64_t* out_file_size) {
  if (!options.enabled) {
    return "parallel csv disabled";
  }
  if (file_paths.size() != 1) {
    return "parallel csv supports single file only";
  }
  if (options.worker_count <= 1) {
    return "parallel csv worker count <= 1";
  }
  if (config.quoting) {
    return "parallel csv disabled for quoting";
  }
  if (config.escaping) {
    return "parallel csv disabled for escaping";
  }
  if (config.column_names.empty()) {
    return "parallel csv requires known column names";
  }
  if (config.skip_rows < 0) {
    return "parallel csv does not support negative skip_rows";
  }
  std::error_code ec;
  auto size = std::filesystem::file_size(file_paths.front(), ec);
  if (ec) {
    return "failed to stat csv file: " + ec.message();
  }
  *out_file_size = static_cast<int64_t>(size);
  if (*out_file_size < options.min_partition_file_bytes) {
    return "csv file smaller than partition threshold";
  }
  return "";
}

}  // namespace

CSVChunkSource::CSVChunkSource(std::vector<std::string> file_paths,
                               CsvReadConfig config)
    : file_paths_(std::move(file_paths)), config_(std::move(config)) {}

std::shared_ptr<IDataChunkSupplier> CSVChunkSource::Open() const {
  return Open(resolve_default_chunk_source_options());
}

std::shared_ptr<IDataChunkSupplier> CSVChunkSource::Open(
    const ChunkSourceOptions& options) const {
  auto effective = merge_parallel_options(config_, options);
  int64_t file_size = 0;
  auto fallback_reason = partitioned_csv_fallback_reason(file_paths_, config_,
                                                         effective, &file_size);
  if (fallback_reason.empty()) {
    ChunkSourceOptions partition_options = options;
    partition_options.worker_count = effective.worker_count;
    partition_options.queue_capacity = effective.queue_capacity;
    partition_options.collect_stats = effective.collect_stats;
    partition_options.min_partition_file_bytes =
        effective.min_partition_file_bytes;
    LOG(INFO) << "Using partitioned CSV supplier for file "
              << file_paths_.front() << ", size=" << file_size
              << ", workers=" << effective.worker_count << ", queue_capacity="
              << (effective.queue_capacity > 0
                      ? effective.queue_capacity
                      : std::max<int32_t>(2, effective.worker_count * 2));
    return std::make_shared<PartitionedCSVChunkSupplier>(
        file_paths_.front(), config_, partition_options);
  }
  if (effective.collect_stats || options.collect_stats) {
    LOG(INFO) << "Using serial CSV supplier: " << fallback_reason;
  }
  if (file_paths_.size() == 1) {
    return std::make_shared<CSVChunkSupplier>(file_paths_.front(), config_,
                                              CsvRowCountMode::kUnknown,
                                              std::move(fallback_reason));
  }
  return std::make_shared<ChainedChunkSupplier>(file_paths_, config_);
}

void fillVertexReaderMeta(
    label_t v_label, const std::string& v_label_name, const std::string& v_file,
    const LoadingConfig& loading_config,
    const std::vector<std::string>& vertex_property_names,
    const std::vector<DataTypeId>& vertex_edge_property_types,
    DataTypeId pk_type, const std::string& pk_name, size_t pk_ind,
    CsvReadConfig& config) {
  CHECK(vertex_edge_property_types.size() == vertex_property_names.size());
  put_boolean_option(config);

  put_delimiter_option(loading_config.GetDelimiter(), config);
  bool header_row = put_skip_rows_option(loading_config, config);
  put_escape_char_option(loading_config, config);
  put_quote_char_option(loading_config, config);
  put_block_size_option(loading_config, config);
  put_null_values(loading_config, config);
  config.parallel.worker_count = loading_config.GetParallelism();
  put_column_names_option(header_row, v_file, config,
                          vertex_property_names.size() + 1);

  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;

  auto cur_label_col_mapping = loading_config.GetVertexColumnMappings(v_label);

  if (cur_label_col_mapping.size() == 0) {
    std::vector<std::string> vertex_property_names_copy = vertex_property_names;
    CHECK(vertex_property_names.size() + 1 == config.column_names.size())
        << " size in schema: " << vertex_property_names.size()
        << ", size in file: " << config.column_names.size() << ","
        << neug::to_string(vertex_property_names)
        << ", read options: " << neug::to_string(config.column_names);
    vertex_property_names_copy.insert(
        vertex_property_names_copy.begin() + pk_ind, pk_name);

    for (size_t i = 0; i < config.column_names.size(); ++i) {
      included_col_names.emplace_back(config.column_names[i]);
      mapped_property_names.emplace_back(vertex_property_names_copy[i]);
    }
  } else {
    for (size_t i = 0; i < cur_label_col_mapping.size(); ++i) {
      auto& [col_id, col_name, property_name] = cur_label_col_mapping[i];
      if (col_name.empty()) {
        if (col_id >= config.column_names.size() || col_id < 0) {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "The specified column index: " + std::to_string(col_id) +
              " is out of range, please check your configuration");
        }
        col_name = config.column_names[col_id];
      }
      if (col_id >= 0 &&
          col_id < static_cast<int64_t>(config.column_names.size())) {
        if (col_name != config.column_names[col_id]) {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "The specified column name: " + col_name +
              " does not match the column name in the file: " +
              config.column_names[col_id]);
        }
      }
      included_col_names.emplace_back(col_name);
      mapped_property_names.emplace_back(property_name);
    }
  }

  VLOG(10) << "Include columns: " << included_col_names.size();
  config.include_columns = included_col_names;

  for (size_t i = 0; i < vertex_edge_property_types.size(); ++i) {
    auto property_type = vertex_edge_property_types[i];
    auto property_name = vertex_property_names[i];
    size_t ind = mapped_property_names.size();
    for (size_t j = 0; j < mapped_property_names.size(); ++j) {
      if (mapped_property_names[j] == property_name) {
        ind = j;
        break;
      }
    }
    if (ind == mapped_property_names.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "The specified property name: " + property_name +
          " does not exist in the vertex column mapping for "
          "vertex label: " +
          v_label_name +
          " please "
          "check your configuration");
    }
    VLOG(10) << "vertex_label: " << v_label_name
             << " property_name: " << property_name
             << " property_type: " << property_type << " ind: " << ind;
    config.column_types.insert(
        {included_col_names[ind], DataType(property_type)});
  }
  {
    size_t ind = mapped_property_names.size();
    for (size_t i = 0; i < mapped_property_names.size(); ++i) {
      if (mapped_property_names[i] == pk_name) {
        ind = i;
        break;
      }
    }
    if (ind == mapped_property_names.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "The specified property name: " + pk_name +
          " does not exist in the vertex column mapping, please "
          "check your configuration");
    }
    config.column_types.insert({included_col_names[ind], DataType(pk_type)});
  }
}

void fillEdgeReaderMeta(label_t src_label_id, label_t dst_label_id,
                        label_t label_id, const std::string& edge_label_name,
                        const std::string& e_file,
                        const LoadingConfig& loading_config,
                        const std::vector<std::string>& edge_property_names,
                        const std::vector<DataTypeId>& edge_property_types,
                        DataTypeId src_pk_type, DataTypeId dst_pk_type,
                        CsvReadConfig& config) {
  CHECK(edge_property_types.size() == edge_property_names.size());
  put_boolean_option(config);

  put_delimiter_option(loading_config.GetDelimiter(), config);
  bool header_row = put_skip_rows_option(loading_config, config);
  put_escape_char_option(loading_config, config);
  put_quote_char_option(loading_config, config);
  put_block_size_option(loading_config, config);
  put_null_values(loading_config, config);
  config.parallel.worker_count = loading_config.GetParallelism();
  put_column_names_option(header_row, e_file, config,
                          edge_property_names.size() + 2);

  auto src_dst_cols =
      loading_config.GetEdgeSrcDstCol(src_label_id, dst_label_id, label_id);

  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;

  {
    CHECK(src_dst_cols.first.size() == 1 && src_dst_cols.second.size() == 1);
    auto src_col_ind = src_dst_cols.first[0].second;
    auto dst_col_ind = src_dst_cols.second[0].second;
    CHECK(src_col_ind >= 0 && src_col_ind < config.column_names.size())
        << " src_col_ind: " << src_col_ind
        << ", column_names.size(): " << config.column_names.size();
    CHECK(dst_col_ind >= 0 && dst_col_ind < config.column_names.size())
        << " dst_col_ind: " << dst_col_ind
        << ", column_names.size(): " << config.column_names.size();

    included_col_names.emplace_back(config.column_names[src_col_ind]);
    included_col_names.emplace_back(config.column_names[dst_col_ind]);
  }

  auto cur_label_col_mapping = loading_config.GetEdgeColumnMappings(
      src_label_id, dst_label_id, label_id);
  if (cur_label_col_mapping.empty()) {
    for (size_t i = 0; i < edge_property_names.size(); ++i) {
      auto property_name = edge_property_names[i];
      if (loading_config.GetHasHeaderRow()) {
        included_col_names.emplace_back(property_name);
      } else {
        included_col_names.emplace_back(config.column_names[i + 2]);
      }
      mapped_property_names.emplace_back(property_name);
    }
  } else {
    for (size_t i = 0; i < cur_label_col_mapping.size(); ++i) {
      auto& [col_id, col_name, property_name] = cur_label_col_mapping[i];
      if (col_name.empty()) {
        if (col_id >= config.column_names.size() || col_id < 0) {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "The specified column index: " + std::to_string(col_id) +
              " is out of range, please check your configuration");
        }
        col_name = config.column_names[col_id];
      }
      if (col_id >= 0 &&
          col_id < static_cast<int64_t>(config.column_names.size())) {
        if (col_name != config.column_names[col_id]) {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "The specified column name: " + col_name +
              " does not match the column name in the file: " +
              config.column_names[col_id]);
        }
      }
      if (loading_config.GetHasHeaderRow()) {
        included_col_names.emplace_back(col_name);
      } else {
        included_col_names.emplace_back(config.column_names[col_id]);
      }
      mapped_property_names.emplace_back(property_name);
    }
  }

  VLOG(10) << "Include Edge columns: " << neug::to_string(included_col_names);
  config.include_columns = included_col_names;

  for (size_t i = 0; i < edge_property_types.size(); ++i) {
    auto property_type = edge_property_types[i];
    auto property_name = edge_property_names[i];
    size_t ind = mapped_property_names.size();
    for (size_t j = 0; j < mapped_property_names.size(); ++j) {
      if (mapped_property_names[j] == property_name) {
        ind = j;
        break;
      }
    }
    if (ind == mapped_property_names.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "The specified property name: " + property_name +
          " does not exist in the vertex column mapping, please "
          "check your configuration");
    }
    VLOG(10) << "edge_label: " << edge_label_name
             << " property_name: " << property_name
             << " property_type: " << property_type << " ind: " << ind;
    config.column_types.insert(
        {included_col_names[ind + 2], DataType(property_type)});
  }
  {
    auto src_dst_cols =
        loading_config.GetEdgeSrcDstCol(src_label_id, dst_label_id, label_id);
    CHECK(src_dst_cols.first.size() == 1 && src_dst_cols.second.size() == 1);
    auto src_col_ind = src_dst_cols.first[0].second;
    auto dst_col_ind = src_dst_cols.second[0].second;
    CHECK(src_col_ind >= 0 &&
          src_col_ind < static_cast<int64_t>(config.column_names.size()));
    CHECK(dst_col_ind >= 0 &&
          dst_col_ind < static_cast<int64_t>(config.column_names.size()));
    config.column_types.insert(
        {config.column_names[src_col_ind], DataType(src_pk_type)});
    config.column_types.insert(
        {config.column_names[dst_col_ind], DataType(dst_pk_type)});

    VLOG(10) << "Column types: ";
    for (const auto& iter : config.column_types) {
      VLOG(10) << iter.first << " : " << iter.second.ToString();
    }
  }
}

namespace {

template <typename SRC_T, typename COL_T = SRC_T>
void set_column_from_value_column(
    ColumnBase* col, const std::shared_ptr<execution::IContextColumn>& ctx_col,
    const std::vector<vid_t>& vids, std::shared_mutex* mutex = nullptr) {
  auto* typed = dynamic_cast<TypedColumn<COL_T>*>(col);
  auto value_col =
      std::dynamic_pointer_cast<execution::ValueColumn<SRC_T>>(ctx_col);

  CHECK(typed != nullptr)
      << "Storage column type does not match expected type.";
  CHECK(value_col != nullptr)
      << "Context column type does not match expected type.";
  auto data = value_col->data();

  auto set_value = [&](size_t k) {
    if constexpr (std::is_same_v<COL_T, std::string_view>) {
      CHECK(mutex != nullptr) << "String column write requires a shared mutex.";
      const auto& s = data[k];
      std::shared_lock<std::shared_mutex> lock(*mutex);
      if (typed->available_space() <= s.size()) {
        lock.unlock();
        std::unique_lock<std::shared_mutex> w_lock(*mutex);
        typed->resize(typed->size());
        w_lock.unlock();
        lock.lock();
      }
      typed->set_value(vids[k], std::string_view(s));
    } else {
      typed->set_value(vids[k], data[k]);
    }
  };

  for (size_t k = 0; k < vids.size(); ++k) {
    if (vids[k] >= std::numeric_limits<vid_t>::max()) {
      continue;
    }
    set_value(k);
  }
}

}  // namespace

void set_properties_from_context_column(
    neug::ColumnBase* col,
    const std::shared_ptr<execution::IContextColumn>& ctx_col,
    const std::vector<vid_t>& vids, std::shared_mutex& mutex) {
  auto col_type = col->type();
  switch (col_type) {
#define SET_TYPED_VALUE(enum_val, ctype)                     \
  case DataTypeId::enum_val: {                               \
    set_column_from_value_column<ctype>(col, ctx_col, vids); \
    break;                                                   \
  }
    FOR_EACH_DATA_TYPE_PRIMITIVE(SET_TYPED_VALUE)
#undef SET_TYPED_VALUE
  case DataTypeId::kDate: {
    set_column_from_value_column<Date>(col, ctx_col, vids);
    break;
  }
  case DataTypeId::kTimestampMs: {
    set_column_from_value_column<DateTime>(col, ctx_col, vids);
    break;
  }
  case DataTypeId::kInterval: {
    set_column_from_value_column<Interval>(col, ctx_col, vids);
    break;
  }
  case DataTypeId::kVarchar: {
    set_column_from_value_column<std::string, std::string_view>(col, ctx_col,
                                                                vids, &mutex);
    break;
  }
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "set_properties_from_context_column: unsupported type " +
        DataType(col_type).ToString());
  }
}

}  // namespace neug
