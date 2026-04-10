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

// Performance benchmark for FileSharedMMap::Sync() and Dump().
//
// Usage:  ./test_file_shared_mmap_perf
//
// The test creates a backing file whose *payload* is FILE_SIZE bytes (default
// 20 GiB), fills it with pseudo-random data, then measures:
//   1. Sync() on a clean (already-synced) mapping  – should be near-zero.
//   2. Sync() after dirtying one byte               – should trigger msync.
//   3. Dump() to a different path                   – copy + close.

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "neug/storages/container/file_header.h"
#include "neug/storages/container/file_mmap_container.h"

namespace {

// Payload size (bytes).  Override at compile time via -DFILE_SIZE=...
#ifndef FILE_SIZE
static constexpr size_t kFileSize =
    static_cast<size_t>(20) * 1024 * 1024 * 1024;  // 20 GiB
#else
static constexpr size_t kFileSize = static_cast<size_t>(FILE_SIZE);
#endif

// Pure in-memory MD5 benchmark payload size. Override at compile time via
// -DMD5_STRING_DATA_SIZE=... or at runtime via NEUG_TEST_MD5_BYTES.
#ifndef MD5_STRING_DATA_SIZE
static constexpr size_t kMD5StringDataSize =
    static_cast<size_t>(256) * 1024 * 1024;  // 256 MiB
#else
static constexpr size_t kMD5StringDataSize =
    static_cast<size_t>(MD5_STRING_DATA_SIZE);
#endif

#ifndef MD5_STRING_ITERATIONS
static constexpr size_t kMD5StringIterations = 1;
#else
static constexpr size_t kMD5StringIterations =
    static_cast<size_t>(MD5_STRING_ITERATIONS);
#endif

using Clock = std::chrono::high_resolution_clock;
using Ms = std::chrono::duration<double, std::milli>;

static double elapsed_ms(Clock::time_point t0) {
  return Ms(Clock::now() - t0).count();
}

static size_t ReadSizeEnvOrDefault(const char* name, size_t default_value) {
  const char* value = std::getenv(name);
  if (value == nullptr || value[0] == '\0') {
    return default_value;
  }
  char* end = nullptr;
  unsigned long long parsed = std::strtoull(value, &end, 10);
  if (end == value || *end != '\0') {
    return default_value;
  }
  return static_cast<size_t>(parsed);
}

static std::string MakeRandomStringData(size_t size) {
  std::string data(size, '\0');
  std::mt19937_64 generator(std::random_device{}());
  std::uniform_int_distribution<int> distribution(0, 255);
  for (size_t index = 0; index < size; ++index) {
    data[index] = static_cast<char>(distribution(generator));
  }
  return data;
}

// ---------------------------------------------------------------------------
// Helper: create a sparse file of (payload_size + sizeof(FileHeader)) bytes.
// Uses ftruncate to allocate the size without writing every page (fast even
// for 20 GiB).  Writes a non-zero sentinel at the first and last payload
// words so the file is not trivially all-zeros from the MD5 perspective.
// The MD5/msync benchmarks themselves will force the page-ins at measurement
// time.
// ---------------------------------------------------------------------------
static std::string CreateBackingFile(const std::string& path,
                                     size_t payload_size) {
  size_t total = payload_size + sizeof(neug::FileHeader);

  int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
  EXPECT_NE(fd, -1) << "open failed: " << strerror(errno);

  // Extend to desired size (sparse – no physical I/O for zero pages).
  EXPECT_EQ(ftruncate(fd, static_cast<off_t>(total)), 0)
      << "ftruncate failed: " << strerror(errno);

  // Write sentinels so the payload is not pure zeros.
  off_t first = static_cast<off_t>(sizeof(neug::FileHeader));
  off_t last = static_cast<off_t>(total) - static_cast<off_t>(sizeof(uint64_t));
  uint64_t sentinel = 0xDEADBEEFCAFEBABEULL;
  EXPECT_EQ(pwrite(fd, &sentinel, sizeof(sentinel), first),
            static_cast<ssize_t>(sizeof(sentinel)));
  EXPECT_EQ(pwrite(fd, &sentinel, sizeof(sentinel), last),
            static_cast<ssize_t>(sizeof(sentinel)));
  close(fd);
  return path;
}

}  // namespace

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------
class FileSharedMMapPerfTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Allow the test directory to be overridden so large files can be placed
    // on a partition with sufficient free space.
    const char* base = std::getenv("NEUG_TEST_TMPDIR");
    tmp_dir_ =
        std::string(base ? base : "/data/tmp") + "/test_file_shared_mmap_perf";
    std::filesystem::create_directories(tmp_dir_);
    src_path_ = tmp_dir_ + "/source.bin";
    dst_path_ = tmp_dir_ + "/dest.bin";

    std::cout << "[setup] Creating " << kFileSize / (1024.0 * 1024 * 1024)
              << " GiB backing file at " << src_path_ << " ...\n";
    auto t0 = Clock::now();
    CreateBackingFile(src_path_, kFileSize);
    std::cout << "[setup] done in " << elapsed_ms(t0) << " ms\n";
  }

  void TearDown() override { std::filesystem::remove_all(tmp_dir_); }

  std::string tmp_dir_;
  std::string src_path_;
  std::string dst_path_;
};

// ---------------------------------------------------------------------------
// Test 1 – cost of Sync() when the mapping is already clean
//   After Open() the FileHeader.data_md5 was written by whatever created the
//   file.  We call Sync() once to populate the MD5 field, then call it again;
//   the second call should detect "not dirty" after computing the MD5 and skip
//   msync.
// ---------------------------------------------------------------------------
TEST_F(FileSharedMMapPerfTest, SyncClean) {
  neug::FileSharedMMap mm;
  mm.Open(src_path_);

  // First Sync: updates the MD5 in the header (file was created without a
  // valid MD5).
  {
    auto t0 = Clock::now();
    mm.Sync();
    double ms = elapsed_ms(t0);
    std::cout << "[Sync #1 – dirty->clean] " << ms << " ms  "
              << "(triggers MD5 compute + msync)\n";
  }

  // Second Sync: mapping is clean, MD5 matches; only MD5 recomputation should
  // happen; msync is skipped.
  {
    auto t0 = Clock::now();
    mm.Sync();
    double ms = elapsed_ms(t0);
    std::cout << "[Sync #2 – already clean] " << ms << " ms  "
              << "(MD5 compute only, no msync)\n";
    // No hard time assertion – just report.
  }

  mm.Close();
}

// ---------------------------------------------------------------------------
// Test 2 – cost of Sync() after dirtying one byte
//   We open the mapping, force it clean (Sync #1), then write one byte to the
//   payload and call Sync again to measure the full MD5+msync path.
// ---------------------------------------------------------------------------
TEST_F(FileSharedMMapPerfTest, SyncAfterDirty) {
  neug::FileSharedMMap mm;
  mm.Open(src_path_);

  // Warm up: make the mapping clean.
  mm.Sync();

  // Dirty one byte.
  char* payload = static_cast<char*>(mm.GetData());
  ASSERT_NE(payload, nullptr);
  payload[0] ^= 0xFF;

  auto t0 = Clock::now();
  mm.Sync();
  double ms = elapsed_ms(t0);
  std::cout << "[Sync after 1-byte write] " << ms << " ms  "
            << "(MD5 compute + msync over "
            << kFileSize / (1024.0 * 1024 * 1024) << " GiB)\n";

  mm.Close();
}

// ---------------------------------------------------------------------------
// Test 3 – cost of Dump() to a different file
//   Measures the full Dump(dst) path: Sync() + copy_file + Close().
// ---------------------------------------------------------------------------
TEST_F(FileSharedMMapPerfTest, DumpToAnotherFile) {
  // Remove destination if it was left from a previous run.
  std::filesystem::remove(dst_path_);

  neug::FileSharedMMap mm;
  mm.Open(src_path_);

  // Warm up: make the mapping clean so the Sync() inside Dump() is fast.
  mm.Sync();

  auto t0 = Clock::now();
  mm.Dump(dst_path_);
  double dump_ms = elapsed_ms(t0);

  std::cout << "[Dump to different file] " << dump_ms << " ms  ("
            << kFileSize / (1024.0 * 1024 * 1024) << " GiB)\n";

  // Verify the destination exists and has the right size.
  ASSERT_TRUE(std::filesystem::exists(dst_path_));
  size_t expected_total = kFileSize + sizeof(neug::FileHeader);
  EXPECT_EQ(std::filesystem::file_size(dst_path_), expected_total)
      << "Dumped file size mismatch";

  // mm is already closed by Dump(); no Close() needed.
}

// ---------------------------------------------------------------------------
// Test 4 – cost of Dump() to the same file (should be Sync() + early return)
// ---------------------------------------------------------------------------
TEST_F(FileSharedMMapPerfTest, DumpToSameFile) {
  neug::FileSharedMMap mm;
  mm.Open(src_path_);

  // First Sync to make it clean.
  mm.Sync();

  auto t0 = Clock::now();
  mm.Dump(src_path_);  // same path → Sync() + return, no copy
  double ms = elapsed_ms(t0);

  std::cout << "[Dump to same file] " << ms << " ms  "
            << "(Sync + early return, no copy)\n";

  // mapping is NOT closed for same-path Dump.
  mm.Close();
}

// ---------------------------------------------------------------------------
// Test 5 - cost of calculating the MD5 sum for an in-memory string data array
//   This isolates the digest computation from mmap/file-system overhead.
// ---------------------------------------------------------------------------
TEST(FileSharedMMapPerfStandaloneTest, MD5StringDataArray) {
  size_t data_size =
      ReadSizeEnvOrDefault("NEUG_TEST_MD5_BYTES", kMD5StringDataSize);
  size_t iterations =
      ReadSizeEnvOrDefault("NEUG_TEST_MD5_ITERATIONS", kMD5StringIterations);
  ASSERT_GT(data_size, 0U);
  ASSERT_GT(iterations, 0U);

  std::string payload = MakeRandomStringData(data_size);
  std::array<unsigned char, MD5_DIGEST_LENGTH> digest{};

  auto t0 = Clock::now();
  for (size_t iteration = 0; iteration < iterations; ++iteration) {
    unsigned char* result =
        MD5(reinterpret_cast<const unsigned char*>(payload.data()),
            payload.size(), digest.data());
    ASSERT_NE(result, nullptr);
  }
  double total_ms = elapsed_ms(t0);

  std::array<unsigned char, MD5_DIGEST_LENGTH> zero_digest{};
  EXPECT_NE(std::memcmp(digest.data(), zero_digest.data(), digest.size()), 0);

  std::cout << "[MD5 string data array] " << total_ms << " ms total, "
            << (total_ms / static_cast<double>(iterations))
            << " ms avg per iteration (" << data_size / (1024.0 * 1024.0)
            << " MiB, iterations=" << iterations << ")\n";
}
