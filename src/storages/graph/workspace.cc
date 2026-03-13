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

#include "neug/storages/workspace.h"

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <string>

#include <glog/logging.h>

namespace neug {

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

Checkpoint::Checkpoint(std::string path, uint32_t id)
    : path_(std::move(path)), id_(id) {}

void Checkpoint::create_dirs() const {
  assert(!IsEmpty());
  std::error_code ec;
  std::filesystem::create_directories(snapshot_dir(), ec);
  if (ec) {
    LOG(ERROR) << "Checkpoint::create_dirs: failed to create " << snapshot_dir()
               << ": " << ec.message();
  }
  std::filesystem::create_directories(runtime_dir(), ec);
  if (ec) {
    LOG(ERROR) << "Checkpoint::create_dirs: failed to create " << runtime_dir()
               << ": " << ec.message();
  }
  std::filesystem::create_directories(wal_dir(), ec);
  if (ec) {
    LOG(ERROR) << "Checkpoint::create_dirs: failed to create " << wal_dir()
               << ": " << ec.message();
  }
}

SnapshotMeta Checkpoint::LoadMeta() const {
  assert(!IsEmpty());
  SnapshotMeta meta;
  ModuleDescriptor dummy;
  meta.Open(*this, dummy, MemoryLevel::kInMemory);
  return meta;
}

void Checkpoint::SaveMeta(SnapshotMeta meta) const {
  assert(!IsEmpty());
  meta.Dump(*this);
}

// ---------------------------------------------------------------------------
// Workspace
// ---------------------------------------------------------------------------

Workspace::Workspace(std::string db_dir) : db_dir_(std::move(db_dir)) {}

std::string Workspace::FormatId(uint32_t id) {
  std::ostringstream oss;
  oss << std::setw(kCheckpointIdWidth) << std::setfill('0') << id;
  return oss.str();
}

std::string Workspace::CheckpointPath(uint32_t id) const {
  return db_dir_ + "/" + kCheckpointPrefix + FormatId(id);
}

std::vector<uint32_t> Workspace::ListCheckpointIds() const {
  std::vector<uint32_t> ids;
  if (!std::filesystem::exists(db_dir_)) {
    return ids;
  }
  const std::string prefix(kCheckpointPrefix);
  for (const auto& entry : std::filesystem::directory_iterator(db_dir_)) {
    if (!entry.is_directory()) {
      continue;
    }
    std::string name = entry.path().filename().string();
    if (name.substr(0, prefix.size()) != prefix) {
      continue;
    }
    try {
      uint32_t id =
          static_cast<uint32_t>(std::stoul(name.substr(prefix.size())));
      ids.push_back(id);
    } catch (...) {
      // Ignore entries with non-numeric suffixes
    }
  }
  std::sort(ids.begin(), ids.end());
  return ids;
}

std::optional<Checkpoint> Workspace::LatestCheckpoint() const {
  auto ids = ListCheckpointIds();
  if (ids.empty()) {
    return std::nullopt;
  }
  return GetCheckpoint(ids.back());
}

Checkpoint Workspace::GetCheckpoint(uint32_t id) const {
  return Checkpoint(CheckpointPath(id), id);
}

Checkpoint Workspace::CreateCheckpoint() const {
  auto ids = ListCheckpointIds();
  uint32_t next_id = ids.empty() ? 1u : ids.back() + 1u;
  Checkpoint cp(CheckpointPath(next_id), next_id);
  cp.create_dirs();
  return cp;
}

}  // namespace neug
