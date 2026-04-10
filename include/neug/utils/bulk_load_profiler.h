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

// Header-only lightweight profiler for the bulk-loading pipeline.
//
// Usage:
//   BLPROF_SCOPE("Phase/loadEdges");          // string literal key
//   BLPROF_SCOPE_STR("addEdges[" + name + "]"); // computed string key
//
// Both macros create a RAII ScopeTimer that records elapsed milliseconds
// for the given key when it goes out of scope.  Thread-safe.
//
// At the end of bulk loading, call:
//   neug::BulkLoadProfiler::Get().PrintReport("/path/to/output.txt");

#pragma once

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

namespace neug {

class BulkLoadProfiler {
 public:
  // -------------------------------------------------------------------------
  // Aggregate statistics for one named operation.
  // -------------------------------------------------------------------------
  struct Entry {
    size_t count = 0;
    double total_ms = 0.0;
    double min_ms = std::numeric_limits<double>::max();
    double max_ms = 0.0;

    void record(double ms) {
      ++count;
      total_ms += ms;
      if (ms < min_ms)
        min_ms = ms;
      if (ms > max_ms)
        max_ms = ms;
    }
  };

  // -------------------------------------------------------------------------
  // Global singleton.
  // -------------------------------------------------------------------------
  static BulkLoadProfiler& Get() {
    static BulkLoadProfiler instance;
    return instance;
  }

  // Thread-safe recording.
  void Record(const std::string& key, double ms) {
    std::lock_guard<std::mutex> lk(mutex_);
    entries_[key].record(ms);
  }

  // -------------------------------------------------------------------------
  // RAII scope timer.  Constructed with a key; records elapsed ms on dtor.
  // -------------------------------------------------------------------------
  struct ScopeTimer {
    std::string key;
    std::chrono::high_resolution_clock::time_point t0;

    explicit ScopeTimer(std::string k)
        : key(std::move(k)), t0(std::chrono::high_resolution_clock::now()) {}

    ~ScopeTimer() {
      double ms =
          std::chrono::duration<double, std::milli>(std::chrono::high_resolution_clock::now() - t0)
              .count();
      BulkLoadProfiler::Get().Record(key, ms);
    }

    // Non-copyable, non-movable – purely stack-allocated.
    ScopeTimer(const ScopeTimer&) = delete;
    ScopeTimer& operator=(const ScopeTimer&) = delete;
  };

  // -------------------------------------------------------------------------
  // Print sorted report to LOG(INFO) and optionally to a file.
  // Entries with total_ms < threshold_ms are omitted (default: 0 = show all).
  // -------------------------------------------------------------------------
  void PrintReport(const std::string& file_path, double threshold_ms = 0.0) {
    // Snapshot and sort by total_ms descending.
    std::vector<std::pair<std::string, Entry>> sorted;
    {
      std::lock_guard<std::mutex> lk(mutex_);
      for (auto& kv : entries_) {
        if (kv.second.total_ms >= threshold_ms) {
          sorted.emplace_back(kv.first, kv.second);
        }
      }
    }
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.second.total_ms > b.second.total_ms; });

    // Format as markdown table.
    std::ostringstream oss;
    oss << "# Bulk Load Profiling Report\n\n";
    oss << "| Operation | Calls | Total(s) | Avg(ms) | Min(ms) | Max(ms) |\n";
    oss << "|-----------|-------|----------|---------|---------|----------|\n";

    for (auto& [key, e] : sorted) {
      double avg = e.count > 0 ? e.total_ms / static_cast<double>(e.count) : 0.0;
      double min_ms = e.count > 0 ? e.min_ms : 0.0;

      oss << "| " << key << " | " << e.count << " | " << std::fixed << std::setprecision(3)
          << e.total_ms / 1000.0 << " | " << std::fixed << std::setprecision(3) << avg << " | "
          << std::fixed << std::setprecision(3) << min_ms << " | " << std::fixed
          << std::setprecision(3) << e.max_ms << " |\n";
    }
    oss << "\n---\n*Report generated at "
        << std::chrono::system_clock::now().time_since_epoch().count() << "*\n";

    std::string report = oss.str();

    // Emit to glog.
    LOG(INFO) << report;

    // Write to file if requested.
    if (!file_path.empty()) {
      std::ofstream f(file_path);
      if (f.is_open()) {
        f << report;
        f.flush();
        // Extract just the filename from the path
        auto last_slash = file_path.find_last_of("/\\");
        std::string file_name =
            (last_slash != std::string::npos) ? file_path.substr(last_slash + 1) : file_path;
        LOG(INFO) << "Bulk load profiling report written to: " << file_name;
      } else {
        auto last_slash = file_path.find_last_of("/\\");
        std::string file_name =
            (last_slash != std::string::npos) ? file_path.substr(last_slash + 1) : file_path;
        LOG(WARNING) << "Failed to open profiling report file: " << file_name;
      }
    }
  }

 private:
  BulkLoadProfiler() = default;
  std::mutex mutex_;
  std::unordered_map<std::string, Entry> entries_;
};

}  // namespace neug

// ---------------------------------------------------------------------------
// Convenience macros.
//
// BLPROF_SCOPE("literal key")  – key is a string literal or variable
// BLPROF_SCOPE_STR(expr)       – key is a computed std::string expression
//
// __COUNTER__ is expanded via a two-level helper so the pasted token is the
// numeric value (GCC/Clang/MSVC all support __COUNTER__).
// ---------------------------------------------------------------------------
#define BLPROF_CAT2_(a, b) a##b
#define BLPROF_CAT_(a, b) BLPROF_CAT2_(a, b)

#define BLPROF_SCOPE(key) \
  neug::BulkLoadProfiler::ScopeTimer BLPROF_CAT_(_blprof_, __COUNTER__) { key }

#define BLPROF_SCOPE_STR(expr) \
  neug::BulkLoadProfiler::ScopeTimer BLPROF_CAT_(_blprofs_, __COUNTER__) { expr }
