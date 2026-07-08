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

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "neug/storages/loader/loader_utils.h"

namespace neug {

namespace copy_load_env {

inline constexpr const char* kStreaming = "NEUG_COPY_STREAMING";
inline constexpr const char* kParallelCsv = "NEUG_COPY_PARALLEL_CSV";
inline constexpr const char* kCsvWorkers = "NEUG_COPY_CSV_WORKERS";
inline constexpr const char* kCsvQueueCapacity = "NEUG_COPY_CSV_QUEUE_CAPACITY";
inline constexpr const char* kCsvMinFileBytes = "NEUG_COPY_CSV_MIN_FILE_BYTES";
inline constexpr const char* kCsvStats = "NEUG_COPY_CSV_STATS";

inline constexpr const char* kBulkConsumers = "NEUG_COPY_BULK_CONSUMERS";
inline constexpr const char* kBulkMaxConsumers = "NEUG_COPY_BULK_MAX_CONSUMERS";

inline constexpr const char* kVertexBulkBuild = "NEUG_COPY_VERTEX_BULK_BUILD";
inline constexpr const char* kVertexBulkParallel =
    "NEUG_COPY_VERTEX_BULK_PARALLEL";
inline constexpr const char* kVertexBulkParallelMinBytes =
    "NEUG_COPY_VERTEX_BULK_PARALLEL_MIN_BYTES";
inline constexpr const char* kVertexBulkConsumers =
    "NEUG_COPY_VERTEX_BULK_CONSUMERS";
inline constexpr const char* kVertexBulkMaxConsumers =
    "NEUG_COPY_VERTEX_BULK_MAX_CONSUMERS";

inline constexpr const char* kEdgeBulkBuild = "NEUG_COPY_EDGE_BULK_BUILD";
inline constexpr const char* kLegacyEdgeBulkBuild = "NEUG_EDGE_BULK_BUILD";
inline constexpr const char* kEdgeBulkConsumers =
    "NEUG_COPY_EDGE_BULK_CONSUMERS";
inline constexpr const char* kEdgeBulkMaxConsumers =
    "NEUG_COPY_EDGE_BULK_MAX_CONSUMERS";

}  // namespace copy_load_env

enum class CopyLoadTarget {
  kVertex,
  kEdge,
};

struct CopyBulkBuildEnv {
  const char* build_enabled_env = nullptr;
  const char* legacy_build_enabled_env = nullptr;
  const char* consumers_env = nullptr;
  const char* max_consumers_env = nullptr;
};

inline CopyBulkBuildEnv get_copy_bulk_build_env(CopyLoadTarget target) {
  switch (target) {
  case CopyLoadTarget::kVertex:
    return {copy_load_env::kVertexBulkBuild, nullptr,
            copy_load_env::kVertexBulkConsumers,
            copy_load_env::kVertexBulkMaxConsumers};
  case CopyLoadTarget::kEdge:
    return {copy_load_env::kEdgeBulkBuild, copy_load_env::kLegacyEdgeBulkBuild,
            copy_load_env::kEdgeBulkConsumers,
            copy_load_env::kEdgeBulkMaxConsumers};
  }
  return {};
}

inline std::string normalize_env_value(std::string_view value) {
  std::string normalized(value);
  std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                 [](unsigned char ch) { return std::tolower(ch); });
  return normalized;
}

inline bool is_explicitly_disabled_env_value(std::string_view value) {
  auto normalized = normalize_env_value(value);
  return normalized == "0" || normalized == "false" || normalized == "off" ||
         normalized == "no";
}

inline bool is_explicitly_enabled_env_value(std::string_view value) {
  auto normalized = normalize_env_value(value);
  return normalized == "1" || normalized == "true" || normalized == "on" ||
         normalized == "yes";
}

inline bool is_explicitly_disabled_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr) {
    return false;
  }
  return is_explicitly_disabled_env_value(value);
}

inline bool is_explicitly_disabled_env(const char* primary_name,
                                       const char* legacy_name) {
  const char* primary = std::getenv(primary_name);
  if (primary != nullptr) {
    return is_explicitly_disabled_env_value(primary);
  }
  if (legacy_name == nullptr) {
    return false;
  }
  return is_explicitly_disabled_env(legacy_name);
}

inline bool is_copy_streaming_env_enabled() {
  return !is_explicitly_disabled_env(copy_load_env::kStreaming);
}

inline bool is_copy_bulk_build_env_enabled(CopyLoadTarget target) {
  auto env = get_copy_bulk_build_env(target);
  return !is_explicitly_disabled_env(env.build_enabled_env,
                                     env.legacy_build_enabled_env);
}

inline int32_t default_chunk_copy_worker_count() {
  auto hw = static_cast<int32_t>(std::thread::hardware_concurrency());
  if (hw <= 0) {
    hw = 1;
  }
  return std::max<int32_t>(1, std::min<int32_t>(hw, 8));
}

inline int64_t read_nonnegative_env_i64(const char* name,
                                        int64_t default_value) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    return default_value;
  }
  char* end = nullptr;
  long long parsed = std::strtoll(value, &end, 10);
  if (end == value || parsed < 0) {
    LOG(WARNING) << "Ignore invalid " << name << "=" << value;
    return default_value;
  }
  return static_cast<int64_t>(
      std::min<long long>(parsed, std::numeric_limits<int64_t>::max()));
}

inline int32_t read_positive_env_int(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    return 0;
  }
  char* end = nullptr;
  long parsed = std::strtol(value, &end, 10);
  if (end == value || parsed <= 0) {
    LOG(WARNING) << "Ignore invalid " << name << "=" << value;
    return 0;
  }
  return static_cast<int32_t>(
      std::min<long>(parsed, std::numeric_limits<int32_t>::max()));
}

inline int32_t read_positive_env_int(const char* primary_name,
                                     const char* fallback_name) {
  int32_t value = read_positive_env_int(primary_name);
  if (value > 0 || fallback_name == nullptr) {
    return value;
  }
  return read_positive_env_int(fallback_name);
}

inline int32_t resolve_copy_consumer_count(int32_t worker_count,
                                           bool use_parallel,
                                           CopyLoadTarget target) {
  if (!use_parallel) {
    return 1;
  }
  auto env = get_copy_bulk_build_env(target);
  int32_t override_consumers =
      read_positive_env_int(env.consumers_env, copy_load_env::kBulkConsumers);
  if (override_consumers > 0) {
    return override_consumers;
  }
  return std::max<int32_t>(1, worker_count / 2);
}

inline int32_t resolve_max_copy_consumer_count(int32_t initial_count,
                                               CopyLoadTarget target) {
  auto env = get_copy_bulk_build_env(target);
  int32_t override_consumers = read_positive_env_int(
      env.max_consumers_env, copy_load_env::kBulkMaxConsumers);
  if (override_consumers > 0) {
    return std::max(initial_count, override_consumers);
  }
  auto hw = static_cast<int32_t>(std::thread::hardware_concurrency());
  if (hw <= 0) {
    hw = initial_count;
  }
  return std::max(initial_count, hw);
}

struct CopyBulkFillPlan {
  ChunkSourceOptions options;
  std::shared_ptr<IDataChunkSupplier> supplier;
  int32_t initial_consumer_count = 1;
  int32_t max_consumer_count = 1;
  bool use_parallel = false;
};

inline constexpr int64_t kDefaultVertexBulkParallelMinBytes =
    10LL * 1024 * 1024 * 1024;

inline bool is_global_parallel_csv_forced_on() {
  const char* mode = std::getenv(copy_load_env::kParallelCsv);
  return mode != nullptr && is_explicitly_enabled_env_value(mode);
}

inline void apply_vertex_bulk_parallel_threshold(ChunkSourceOptions& options,
                                                 int64_t source_bytes) {
  if (!options.parallel_enabled) {
    return;
  }
  const char* mode = std::getenv(copy_load_env::kVertexBulkParallel);
  if (mode != nullptr) {
    if (is_explicitly_disabled_env_value(mode)) {
      options.parallel_enabled = false;
      return;
    }
    if (is_explicitly_enabled_env_value(mode)) {
      options.parallel_enabled = true;
      options.min_partition_file_bytes = 0;
      return;
    }
  }
  if (is_global_parallel_csv_forced_on()) {
    return;
  }

  int64_t min_bytes =
      read_nonnegative_env_i64(copy_load_env::kVertexBulkParallelMinBytes,
                               kDefaultVertexBulkParallelMinBytes);
  if (source_bytes == kUnknownSourceBytes || source_bytes < min_bytes) {
    options.parallel_enabled = false;
  }
}

inline ChunkSourceOptions resolve_copy_chunk_source_options(
    CopyLoadTarget target, int64_t source_bytes) {
  auto options = resolve_default_chunk_source_options();
  options.collect_stats = true;
  if (options.worker_count <= 0) {
    options.worker_count = default_chunk_copy_worker_count();
  }
  if (target == CopyLoadTarget::kVertex) {
    apply_vertex_bulk_parallel_threshold(options, source_bytes);
  }
  return options;
}

inline bool can_use_parallel_supplier(
    const std::shared_ptr<IDataChunkSupplier>& supplier,
    const ChunkSourceOptions& options) {
  if (!options.parallel_enabled || supplier == nullptr) {
    return false;
  }
  auto stats = supplier->GetStats();
  return stats && stats->parallel && stats->fallback_reason.empty();
}

inline CopyBulkFillPlan open_copy_bulk_fill_supplier(
    const std::shared_ptr<IDataChunkSource>& source, CopyLoadTarget target) {
  CHECK(source != nullptr);
  CopyBulkFillPlan plan;
  plan.options =
      resolve_copy_chunk_source_options(target, source->EstimatedBytes());
  plan.supplier = source->Open(plan.options);
  CHECK(plan.supplier != nullptr);
  plan.use_parallel = can_use_parallel_supplier(plan.supplier, plan.options);
  plan.initial_consumer_count = resolve_copy_consumer_count(
      plan.options.worker_count, plan.use_parallel, target);
  plan.max_consumer_count =
      resolve_max_copy_consumer_count(plan.initial_consumer_count, target);
  return plan;
}

// Shared error guard for chunk consumer pools. Captures the first exception
// from any worker thread and provides a safe way to rethrow it after all
// threads have been joined.
struct ChunkConsumerErrorGuard {
  std::atomic<bool> has_error{false};
  std::mutex error_mutex;
  std::exception_ptr first_error;

  // Must be called from within a catch block.
  void capture() {
    bool expected = false;
    if (has_error.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel)) {
      std::lock_guard<std::mutex> lock(error_mutex);
      first_error = std::current_exception();
    }
  }

  bool should_stop() const { return has_error.load(std::memory_order_acquire); }

  void rethrow_if_failed() {
    if (has_error.load(std::memory_order_acquire)) {
      std::exception_ptr error;
      {
        std::lock_guard<std::mutex> lock(error_mutex);
        error = first_error;
      }
      if (error) {
        std::rethrow_exception(error);
      }
    }
  }
};

template <typename ProcessChunkFn>
inline void run_adaptive_chunk_consumers(
    const std::shared_ptr<IDataChunkSupplier>& supplier,
    int32_t initial_consumer_count, int32_t max_consumer_count,
    std::string_view fill_name, ProcessChunkFn&& process_chunk) {
  CHECK(supplier != nullptr);
  CHECK_GE(initial_consumer_count, 1);
  CHECK_GE(max_consumer_count, initial_consumer_count);

  ChunkConsumerErrorGuard error_guard;

  std::atomic<int32_t> active_consumers{0};
  auto worker_fn = [&]() {
    active_consumers.fetch_add(1, std::memory_order_relaxed);
    try {
      while (!error_guard.should_stop()) {
        auto chunk = supplier->GetNextChunk();
        if (!chunk) {
          break;
        }
        process_chunk(chunk);
      }
    } catch (...) { error_guard.capture(); }
    active_consumers.fetch_sub(1, std::memory_order_relaxed);
  };

  std::vector<std::thread> consumers;
  consumers.reserve(static_cast<size_t>(max_consumer_count));
  for (int32_t i = 0; i < initial_consumer_count; ++i) {
    consumers.emplace_back(worker_fn);
  }

  constexpr int64_t kSpawnThresholdMs = 500;
  constexpr auto kMonitorInterval = std::chrono::milliseconds(100);
  while (active_consumers.load(std::memory_order_acquire) > 0) {
    std::this_thread::sleep_for(kMonitorInterval);

    if (error_guard.should_stop()) {
      break;
    }

    auto stats = supplier->GetStats();
    if (!stats || stats->worker_count == 0) {
      continue;
    }

    int32_t current_count = static_cast<int32_t>(consumers.size());
    if (current_count >= max_consumer_count) {
      continue;
    }

    if (active_consumers.load(std::memory_order_acquire) == 0) {
      break;
    }

    double per_producer_wait =
        static_cast<double>(stats->producer_wait_ms) / stats->worker_count;
    double per_consumer_wait = static_cast<double>(stats->consumer_wait_ms) /
                               std::max(1, static_cast<int>(consumers.size()));

    if (per_producer_wait > per_consumer_wait + kSpawnThresholdMs) {
      int32_t to_spawn = std::min<int32_t>(
          std::max<int32_t>(
              1, static_cast<int32_t>((per_producer_wait - per_consumer_wait) /
                                      kSpawnThresholdMs)),
          max_consumer_count - current_count);
      VLOG(1) << "Adaptive " << fill_name << " fill: spawning " << to_spawn
              << " more consumers (total=" << current_count + to_spawn
              << ", per_producer_wait="
              << static_cast<int64_t>(per_producer_wait) << "ms"
              << ", per_consumer_wait="
              << static_cast<int64_t>(per_consumer_wait) << "ms)";
      for (int32_t i = 0; i < to_spawn; ++i) {
        consumers.emplace_back(worker_fn);
      }
    }
  }

  for (auto& consumer : consumers) {
    if (consumer.joinable()) {
      consumer.join();
    }
  }

  error_guard.rethrow_if_failed();
}

// Fixed-size consumer pool with per-worker local state. Unlike
// run_adaptive_chunk_consumers, this variant does not dynamically spawn
// additional workers because the pass1 scan is lightweight (validation +
// counting) and does not benefit from adaptive scaling.
template <typename LocalState, typename ProcessChunkFn, typename MergeLocalFn>
inline void run_chunk_consumers_with_local_state(
    const std::shared_ptr<IDataChunkSupplier>& supplier, int32_t consumer_count,
    std::vector<LocalState>& local_states, ProcessChunkFn process_chunk,
    MergeLocalFn merge_local) {
  CHECK(supplier != nullptr);
  CHECK_GT(consumer_count, 0);
  CHECK_EQ(local_states.size(), static_cast<size_t>(consumer_count));

  ChunkConsumerErrorGuard error_guard;

  auto worker_fn = [&](int32_t worker_index) {
    try {
      auto& local = local_states[static_cast<size_t>(worker_index)];
      while (!error_guard.should_stop()) {
        auto chunk = supplier->GetNextChunk();
        if (!chunk) {
          break;
        }
        process_chunk(chunk, local);
      }
    } catch (...) { error_guard.capture(); }
  };

  std::vector<std::thread> consumers;
  consumers.reserve(static_cast<size_t>(consumer_count));
  for (int32_t i = 0; i < consumer_count; ++i) {
    consumers.emplace_back(worker_fn, i);
  }
  for (auto& consumer : consumers) {
    if (consumer.joinable()) {
      consumer.join();
    }
  }

  error_guard.rethrow_if_failed();

  for (const auto& local : local_states) {
    merge_local(local);
  }
}

inline void log_chunk_supplier_stats(
    const std::shared_ptr<IDataChunkSupplier>& supplier, const char* phase,
    bool info_log = false) {
  if (supplier == nullptr) {
    return;
  }
  auto stats = supplier->GetStats();
  if (!stats) {
    return;
  }
  std::ostringstream os;
  os << phase << " stats: produced_chunks=" << stats->produced_chunks
     << ", parallel=" << stats->parallel
     << ", worker_count=" << stats->worker_count
     << ", produced_rows=" << stats->produced_rows
     << ", consumed_chunks=" << stats->consumed_chunks
     << ", consumed_rows=" << stats->consumed_rows
     << ", bytes_read=" << stats->bytes_read
     << ", producer_wait_ms=" << stats->producer_wait_ms
     << ", consumer_wait_ms=" << stats->consumer_wait_ms
     << ", max_queue_size=" << stats->max_queue_size
     << ", fallback_reason=" << stats->fallback_reason;
  if (info_log) {
    LOG(INFO) << os.str();
  } else {
    VLOG(1) << os.str();
  }
}

}  // namespace neug
