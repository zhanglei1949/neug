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
#include <memory>
#include <string>
#include <vector>
#include "neug/transaction/wal/wal.h"
namespace neug {
/// Only zero-byte/all-zero legacy files are safe for a checkpoint upgrade.
NEUG_API void ValidateLegacyWalEpochEmpty(const std::string& wal_uri);
class LocalWalParser : public IWalParser {
 public:
  static std::unique_ptr<IWalParser> Make(const std::string& uri, uint64_t id) {
    return std::make_unique<LocalWalParser>(uri, id);
  }
  LocalWalParser(const std::string& uri, uint64_t checkpoint_id);
  ~LocalWalParser() override;
  void open(const std::string& uri, uint64_t checkpoint_id) override;
  void close() override;
  std::string_view source_path(size_t file_index) const override;
  uint32_t last_ts() const override { return last_ts_; }
  const std::vector<WalReplayUnit>& replay_units() const override {
    return replay_units_;
  }

 private:
  struct MappedFile;
  std::vector<std::unique_ptr<MappedFile>> files_;
  std::vector<WalReplayUnit> replay_units_;
  uint32_t last_ts_{0};
  static const bool registered_;
};
}  // namespace neug
