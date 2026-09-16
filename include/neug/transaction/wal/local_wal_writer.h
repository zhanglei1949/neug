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
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "neug/transaction/wal/wal.h"
namespace neug {
class WalWriterTestPeer;
/// Slot-owned writer. First append creates the file; success includes file
/// and directory durability. I/O failure after append begins is fail-stop.
class LocalWalWriter : public IWalWriter {
 public:
  static std::unique_ptr<IWalWriter> Make(const std::string& uri, int slot_id);
  LocalWalWriter(const std::string& uri, int slot_id)
      : wal_uri_(uri), slot_id_(slot_id) {}
  ~LocalWalWriter() noexcept override;
  void open(const std::string& uri, uint64_t checkpoint_id) override;
  void close() override;
  bool append_frame(uint32_t timestamp, WalRecordKind kind, const char* payload,
                    size_t length) override;
  std::string type() const override { return "file"; }

 private:
  friend class WalWriterTestPeer;
  void create_file();
  void write_all(const void* data, size_t length);
  void sync_file();
  std::string wal_uri_, path_;
  int slot_id_, fd_{-1};
  uint64_t checkpoint_id_{0}, durable_offset_{0};
  bool opened_{false};
  std::vector<std::string> directory_syncs_;
  // Private I/O substitution for fault and subprocess tests.
  std::function<ptrdiff_t(int, const void*, size_t)> write_hook_;
  std::function<int(int)> sync_hook_;
  std::function<bool(const std::string&)> directory_sync_hook_;
  static const bool registered_;
};
}  // namespace neug
