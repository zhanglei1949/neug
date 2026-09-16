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

#include "neug/transaction/wal/local_wal_writer.h"
#include <fcntl.h>
#include <glog/logging.h>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#ifdef _WIN32
#include <io.h>
#include <sys/stat.h>
#else
#include <unistd.h>
#endif
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"
namespace neug {
std::unique_ptr<IWalWriter> LocalWalWriter::Make(const std::string& uri,
                                                 int slot) {
  return std::make_unique<LocalWalWriter>(uri, slot);
}
LocalWalWriter::~LocalWalWriter() noexcept {
  try {
    close();
  } catch (const std::exception& e) { LOG(ERROR) << e.what(); }
}
void LocalWalWriter::open(const std::string& uri, uint64_t id) {
  close();
  wal_uri_ = uri;
  checkpoint_id_ = id;
  opened_ = true;
}
void LocalWalWriter::close() {
  opened_ = false;
  durable_offset_ = 0;
  directory_syncs_.clear();
  path_.clear();
  if (fd_ != -1) {
    const int fd = fd_;
    fd_ = -1;
#ifdef _WIN32
    if (::_close(fd) != 0)
#else
    if (::close(fd) != 0)
#endif
      THROW_IO_EXCEPTION("Failed to close WAL: " +
                         std::string(strerror(errno)));
  }
}
void LocalWalWriter::write_all(const void* data, size_t length) {
  const char* p = static_cast<const char*>(data);
  while (length) {
    const size_t chunk =
        std::min(length, static_cast<size_t>(std::numeric_limits<int>::max()));
    ptrdiff_t n;
    if (write_hook_)
      n = write_hook_(fd_, p, chunk);
    else {
#ifdef _WIN32
      n = ::_write(fd_, p, static_cast<unsigned int>(chunk));
#else
      n = ::write(fd_, p, chunk);
#endif
    }
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0 || static_cast<size_t>(n) > chunk)
      THROW_IO_EXCEPTION("Failed or zero-progress WAL write: " + path_);
    p += n;
    length -= static_cast<size_t>(n);
  }
}
void LocalWalWriter::sync_file() {
  int ret;
  do {
    if (sync_hook_)
      ret = sync_hook_(fd_);
    else {
#ifdef _WIN32
      ret = ::_commit(fd_);
#elif defined(F_FULLFSYNC)
      ret = ::fcntl(fd_, F_FULLFSYNC);
#else
      ret = ::fdatasync(fd_);
#endif
    }
  } while (ret != 0 && errno == EINTR);
  if (ret != 0)
    THROW_IO_EXCEPTION("Failed to sync WAL: " + path_ + ": " + strerror(errno));
}
void LocalWalWriter::create_file() {
  namespace fs = std::filesystem;
  fs::path dir = fs::absolute(get_wal_uri_path(wal_uri_));
  for (auto p = dir; !fs::exists(p); p = p.parent_path())
    directory_syncs_.push_back(p.parent_path().string());
  fs::create_directories(dir);
  for (int version = 0; version < 65536; ++version) {
    path_ = (dir / ("thread_" + std::to_string(slot_id_) + "_" +
                    std::to_string(version) + ".wal"))
                .string();
#ifdef _WIN32
    fd_ = ::_open(path_.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_BINARY,
                  _S_IREAD | _S_IWRITE);
#else
    fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
#endif
    if (fd_ != -1)
      break;
    if (errno != EEXIST)
      THROW_IO_EXCEPTION("Failed to create WAL: " + path_ + ": " +
                         strerror(errno));
  }
  if (fd_ == -1)
    THROW_IO_EXCEPTION("WAL file versions exhausted: " + wal_uri_);
  directory_syncs_.insert(directory_syncs_.begin(), dir.string());
  const auto header = EncodeWalFileHeader(checkpoint_id_);
  write_all(header.data(), header.size());
}
bool LocalWalWriter::append_frame(uint32_t ts, WalRecordKind kind,
                                  const char* payload, size_t length) {
  // Validation and checksum computation precede all file mutations.
  const auto header = EncodeWalFrameHeader(ts, kind, payload, length);
  if (!opened_)
    return false;
  try {
    const bool first = fd_ == -1;
    if (first)
      create_file();
    write_all(header.data(), header.size());
    if (length)
      write_all(payload, length);
    sync_file();
    for (const auto& dir : directory_syncs_) {
      bool synced;
      do {
        errno = 0;
        synced = directory_sync_hook_ ? directory_sync_hook_(dir)
                                      : file_utils::fsync_directory(dir);
      } while (!synced && errno == EINTR);
      if (!synced)
        THROW_IO_EXCEPTION("Failed to sync WAL directory: " + dir);
    }
    directory_syncs_.clear();
    durable_offset_ +=
        (first ? kWalFileHeaderSize : 0) + header.size() + length;
    return true;
  } catch (const std::exception& e) {
    LOG(FATAL) << "WAL append durability unknown: " << e.what();
  } catch (...) { LOG(FATAL) << "WAL append durability unknown"; }
  return false;
}
const bool LocalWalWriter::registered_ =
    WalWriterFactory::RegisterWalWriter("file", &LocalWalWriter::Make);
}  // namespace neug
