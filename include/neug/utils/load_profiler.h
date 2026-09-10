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

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>
#include <sstream>
#include <string>

namespace neug {
namespace profiling {

/// Process-wide load profiler.
///
/// Accumulates wall-clock time and call counts per *logical code phase* (e.g.
/// "edge.resolve_endpoint_ids"), aggregated across ALL labels. Phases are keyed
/// by the code stage they measure, never by vertex/edge label, so the summary
/// answers "which step of the load pipeline is slow" rather than "which label
/// is slow".
///
/// Opt-in via the environment variable NEUG_LOAD_PROFILE (any value other than
/// the empty string or "0" enables it). When disabled, instrumentation is a
/// no-op and costs a single cached bool check per scope.
///
/// The summary is printed once at process exit (registered via atexit) to both
/// stderr and LOG(INFO), so it is visible whether the run tails the glog file
/// or captures stderr.
class LoadProfiler {
 public:
  /// Leak-on-purpose singleton: the instance lives until the process dies, so
  /// the atexit summary never touches a destroyed object (avoids the static
  /// destruction-order fiasco).
  static LoadProfiler& Instance() {
    static LoadProfiler* instance = []() {
      auto* p = new LoadProfiler();
      std::atexit([]() { Instance().DumpSummary(); });
      return p;
    }();
    return *instance;
  }

  /// Cached one-shot read of NEUG_LOAD_PROFILE.
  static bool Enabled() {
    static const bool enabled = []() {
      const char* v = std::getenv("NEUG_LOAD_PROFILE");
      return v != nullptr && v[0] != '\0' && v[0] != '0';
    }();
    return enabled;
  }

  /// Accumulate @p seconds of wall-clock time into @p phase (thread-safe).
  void Record(const char* phase, double seconds) {
    if (!Enabled()) {
      return;
    }
    std::lock_guard<std::mutex> lk(mu_);
    auto& s = stats_[phase];
    s.total_seconds += seconds;
    s.calls += 1;
  }

  /// Print the accumulated breakdown. Safe to call more than once; the atexit
  /// hook calls it exactly once at process termination.
  void DumpSummary() {
    std::lock_guard<std::mutex> lk(mu_);
    if (stats_.empty()) {
      return;
    }
    std::ostringstream oss;
    oss << "\n==== NeuG Load Profile Summary (NEUG_LOAD_PROFILE) ====\n";
    // std::map keeps phases sorted by name, which groups them by their
    // "vertex." / "edge." / "opr." logical prefix.
    for (const auto& kv : stats_) {
      oss << "  " << kv.first;
      // Pad the phase name so the columns line up.
      if (kv.first.size() < 36) {
        oss << std::string(36 - kv.first.size(), ' ');
      } else {
        oss << ' ';
      }
      oss << kv.second.total_seconds << " s  (" << kv.second.calls
          << " calls)\n";
    }
    oss << "=======================================================\n";
    const std::string out = oss.str();
    // stderr is unbuffered and the most reliable channel at exit.
    std::fwrite(out.data(), 1, out.size(), stderr);
    LOG(INFO) << out;
  }

 private:
  struct Stats {
    double total_seconds = 0.0;
    uint64_t calls = 0;
  };

  LoadProfiler() = default;

  std::mutex mu_;
  std::map<std::string, Stats> stats_;
};

/// RAII scoped timer: records the wall-clock duration of the enclosing scope
/// into the named logical phase on destruction. Cheap no-op when profiling is
/// disabled.
class ScopedLoadTimer {
 public:
  explicit ScopedLoadTimer(const char* phase)
      : phase_(phase), active_(LoadProfiler::Enabled()) {
    if (active_) {
      start_ = std::chrono::steady_clock::now();
    }
  }

  ~ScopedLoadTimer() {
    if (active_) {
      auto end = std::chrono::steady_clock::now();
      LoadProfiler::Instance().Record(
          phase_, std::chrono::duration<double>(end - start_).count());
    }
  }

  ScopedLoadTimer(const ScopedLoadTimer&) = delete;
  ScopedLoadTimer& operator=(const ScopedLoadTimer&) = delete;

 private:
  const char* phase_;
  bool active_;
  std::chrono::steady_clock::time_point start_;
};

}  // namespace profiling
}  // namespace neug
