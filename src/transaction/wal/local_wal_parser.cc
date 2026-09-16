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

#include "neug/transaction/wal/local_wal_parser.h"
#include <fcntl.h>
#include <glog/logging.h>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <sys/mman.h>
#include <unistd.h>
#endif
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include "neug/utils/exception/exception.h"
namespace neug {
namespace {
std::vector<std::string> WalPaths(const std::string& uri) {
  std::vector<std::string> paths;
  const auto dir = get_wal_uri_path(uri);
  std::error_code error;
  if (dir.empty() || !std::filesystem::exists(dir, error)) {
    if (error)
      throw exception::WalRecoveryException(
          WalRecoveryErrorKind::kIoError,
          "Failed to inspect WAL directory " + dir + ": " + error.message());
    return paths;
  }
  std::filesystem::directory_iterator iterator(dir, error), end;
  while (!error && iterator != end) {
    const auto& entry = *iterator;
    const bool regular = entry.is_regular_file(error);
    if (!error && regular && entry.path().extension() == ".wal")
      paths.push_back(entry.path().string());
    iterator.increment(error);
  }
  if (error)
    throw exception::WalRecoveryException(
        WalRecoveryErrorKind::kIoError,
        "Failed to enumerate WAL directory " + dir + ": " + error.message());
  std::sort(paths.begin(), paths.end());
  return paths;
}
bool IsZeroFile(const std::string& path) {
  std::array<char, 65536> buffer{};
  std::ifstream input(path, std::ios::binary);
  if (!input)
    throw exception::WalRecoveryException(WalRecoveryErrorKind::kIoError,
                                          "Failed to read WAL: " + path);
  while (input) {
    input.read(buffer.data(), buffer.size());
    if (std::any_of(buffer.begin(), buffer.begin() + input.gcount(),
                    [](char c) { return c != 0; }))
      return false;
  }
  if (!input.eof())
    throw exception::WalRecoveryException(WalRecoveryErrorKind::kIoError,
                                          "Failed to read WAL: " + path);
  return true;
}
std::string Location(const std::string& path, uint64_t off) {
  return path + " at offset " + std::to_string(off);
}
}  // namespace
void ValidateLegacyWalEpochEmpty(const std::string& uri) {
  for (const auto& path : WalPaths(uri)) {
    if (!IsZeroFile(path))
      throw exception::WalRecoveryException(
          WalRecoveryErrorKind::kUnsupportedFormat,
          Location(path, 0) +
              ": legacy/experimental WAL has records; recover and checkpoint "
              "with the old binary, close it, then upgrade");
  }
}

struct LocalWalParser::MappedFile {
  std::string path;
  size_t size{0};
#ifdef _WIN32
  HANDLE file{INVALID_HANDLE_VALUE};
  HANDLE mapping{nullptr};
  void* data{nullptr};
#else
  int fd{-1};
  void* data{MAP_FAILED};
#endif
  explicit MappedFile(const std::string& p) : path(p) {
    std::error_code error;
    size = std::filesystem::file_size(path, error);
    if (error)
      throw exception::WalRecoveryException(
          WalRecoveryErrorKind::kIoError,
          "Failed to inspect WAL " + path + ": " + error.message());
#ifdef _WIN32
    file = ::CreateFileA(path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr,
                         OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file == INVALID_HANDLE_VALUE)
      throw exception::WalRecoveryException(WalRecoveryErrorKind::kIoError,
                                            "Failed to open WAL: " + path);
    mapping = ::CreateFileMappingA(file, nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (!mapping) {
      ::CloseHandle(file);
      file = INVALID_HANDLE_VALUE;
      throw exception::WalRecoveryException(WalRecoveryErrorKind::kIoError,
                                            "Failed to map WAL: " + path);
    }
    data = ::MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, 0);
    if (!data) {
      ::CloseHandle(mapping);
      ::CloseHandle(file);
      mapping = nullptr;
      file = INVALID_HANDLE_VALUE;
      throw exception::WalRecoveryException(WalRecoveryErrorKind::kIoError,
                                            "Failed to map WAL: " + path);
    }
#else
    fd = ::open(path.c_str(), O_RDONLY);
    if (fd == -1)
      throw exception::WalRecoveryException(
          WalRecoveryErrorKind::kIoError,
          "Failed to open WAL: " + path + ": " + strerror(errno));
    data = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
      ::close(fd);
      fd = -1;
      throw exception::WalRecoveryException(
          WalRecoveryErrorKind::kIoError,
          "Failed to mmap WAL: " + path + ": " + strerror(errno));
    }
#endif
  }
  ~MappedFile() {
#ifdef _WIN32
    if (data)
      ::UnmapViewOfFile(data);
    if (mapping)
      ::CloseHandle(mapping);
    if (file != INVALID_HANDLE_VALUE)
      ::CloseHandle(file);
#else
    if (data != MAP_FAILED)
      ::munmap(data, size);
    if (fd != -1)
      ::close(fd);
#endif
  }
};
LocalWalParser::LocalWalParser(const std::string& uri, uint64_t id) {
  open(uri, id);
}
LocalWalParser::~LocalWalParser() { close(); }
std::string_view LocalWalParser::source_path(size_t i) const {
  return files_.at(i)->path;
}
void LocalWalParser::close() {
  replay_units_.clear();
  files_.clear();
  last_ts_ = 0;
}
void LocalWalParser::open(const std::string& uri, uint64_t id) {
  close();
  std::vector<std::unique_ptr<MappedFile>> files;
  std::vector<WalReplayUnit> units;
  for (const auto& path : WalPaths(uri)) {
    std::error_code file_error;
    const auto file_size = std::filesystem::file_size(path, file_error);
    if (file_error)
      throw exception::WalRecoveryException(
          WalRecoveryErrorKind::kIoError,
          "Failed to inspect WAL " + path + ": " + file_error.message());
    if (file_size == 0)
      continue;
    // Check a possible legacy zero-filled preallocation before mmap so a huge
    // empty file cannot consume address space during compatibility handling.
    std::ifstream prefix(path, std::ios::binary);
    char first;
    if (!prefix.get(first))
      throw exception::WalRecoveryException(WalRecoveryErrorKind::kIoError,
                                            "Failed to read WAL: " + path);
    if (first == 0 && IsZeroFile(path))
      continue;
    auto file = std::make_unique<MappedFile>(path);
    auto* bytes = static_cast<const uint8_t*>(file->data);
    size_t offset = 0;
    uint32_t timestamp = 0;
    try {
      if (file->size < kWalFileHeaderSize) {
        const auto expected = EncodeWalFileHeader(id);
        if (!std::equal(bytes, bytes + file->size, expected.begin()))
          throw exception::WalRecoveryException(
              WalRecoveryErrorKind::kUnsupportedFormat,
              "invalid partial file header");
        LOG(WARNING) << "Ignoring incomplete WAL file header: " << path;
        continue;
      }
      ValidateWalFileHeader(bytes, file->size, id);
      offset = kWalFileHeaderSize;
      while (offset < file->size) {
        const size_t remaining = file->size - offset;
        if (remaining < kWalFrameHeaderSize) {
          LOG(WARNING) << "Ignoring incomplete WAL header: "
                       << Location(path, offset);
          break;
        }
        timestamp = 0;
        const auto frame = DecodeWalFrameHeader(bytes + offset, remaining);
        timestamp = frame.timestamp;
        if (frame.payload_length > remaining - kWalFrameHeaderSize) {
          LOG(WARNING) << "Ignoring incomplete WAL payload: "
                       << Location(path, offset)
                       << ", timestamp=" << frame.timestamp;
          break;
        }
        const char* payload =
            reinterpret_cast<const char*>(bytes + offset + kWalFrameHeaderSize);
        ValidateWalFrame(bytes + offset, payload, frame);
        units.push_back({frame.timestamp, frame.kind,
                         std::string_view(payload, frame.payload_length),
                         files.size(), offset});
        offset +=
            kWalFrameHeaderSize + static_cast<size_t>(frame.payload_length);
      }
    } catch (const exception::WalRecoveryException& e) {
      throw exception::WalRecoveryException(
          e.kind(),
          Location(path, offset) +
              (timestamp ? ", timestamp=" + std::to_string(timestamp) : "") +
              ": " + e.what());
    }
    files.push_back(std::move(file));
  }
  std::sort(units.begin(), units.end(), [](const auto& a, const auto& b) {
    return a.timestamp < b.timestamp;
  });
  for (size_t i = 1; i < units.size(); ++i)
    if (units[i - 1].timestamp == units[i].timestamp)
      throw exception::WalRecoveryException(
          WalRecoveryErrorKind::kDuplicateTimestamp,
          "timestamp=" + std::to_string(units[i].timestamp) + " in " +
              Location(files[units[i - 1].file_index]->path,
                       units[i - 1].source_offset) +
              " and " +
              Location(files[units[i].file_index]->path,
                       units[i].source_offset));
  if (!units.empty())
    last_ts_ = units.back().timestamp;
  files_ = std::move(files);
  replay_units_ = std::move(units);
}
const bool LocalWalParser::registered_ =
    WalParserFactory::RegisterWalParser("file", &LocalWalParser::Make);
}  // namespace neug
