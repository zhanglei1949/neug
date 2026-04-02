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

#include "neug/storages/container/container_utils.h"

#include <glog/logging.h>

namespace neug {

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

bool parse_checkpoint_path(const std::string& path, int& id) {
  if (!std::filesystem::is_directory(path)) {
    return false;
  }
  std::string name = std::filesystem::path(path).filename().string();
  if (sscanf(name.c_str(), "checkpoint-%d", &id) != 1) {
    return false;
  }
  return true;
}

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

std::unique_ptr<IDataContainer> Checkpoint::CreateRuntimeContainer(
    size_t size, MemoryLevel level) {
  assert(!IsEmpty());
  std::string path = runtime_dir() + "/" + create_runtime_object();
  auto ret = OpenContainer("", path, level);
  ret->Resize(size);
  return ret;
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

std::string Checkpoint::create_runtime_object() {
  std::lock_guard<std::mutex> lock(mutex_);
  while (true) {
    std::string uuid = UUIDGenerator().generate();
    if (runtime_objects_.count(uuid) == 0 &&
        uncommitted_runtime_objects_.count(uuid) == 0) {
      uncommitted_runtime_objects_.insert(uuid);
      return uuid;
    }
  }
}

// ---------------------------------------------------------------------------
// Workspace
// ---------------------------------------------------------------------------

Workspace::Workspace() {}

Workspace::~Workspace() {}

void Workspace::Open(const std::string& db_dir) {
  if (!db_dir.empty()) {
    Close();
  }
  std::lock_guard<std::mutex> lock(mutex_);
  db_dir_ = std::filesystem::absolute(db_dir).string();
  if (!std::filesystem::is_directory(db_dir_)) {
    std::filesystem::create_directory(db_dir_);
  }
  try {
    for (const auto& entry : std::filesystem::directory_iterator(db_dir_)) {
      if (entry.is_directory()) {
        int id;
        if (parse_checkpoint_path(entry.path().string(), id)) {
          checkpoints_[id] =
              std::make_unique<Checkpoint>(entry.path().string());
        }
      }
    }
  } catch (const std::filesystem::filesystem_error& e) {
    LOG(ERROR) << "Workspace::Open: failed to read directory " << db_dir_
               << ": " << e.what();
  }
}

void Workspace::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  db_dir_.clear();
  checkpoints_.clear();
}

size_t Workspace::NumCheckpoints() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return checkpoints_.size();
}

int Workspace::HeadId() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (checkpoints_.empty()) {
    return -1;
  }
  return checkpoints_.rbegin()->first;
}

int Workspace::CreateCheckpoint() {
  int id = HeadId() + 1;
  std::string path = db_dir_ + "/checkpoint-" + std::to_string(id);
  std::filesystem::create_directory(path);
  std::lock_guard<std::mutex> lock(mutex_);
  checkpoints_[id] = std::make_unique<Checkpoint>(path);
  return id;
}

Checkpoint& Workspace::GetCheckpoint(int id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return *checkpoints_.at(id);
}

Checkpoint& Workspace::GetCheckpoint(int id) {
  std::lock_guard<std::mutex> lock(mutex_);
  return *checkpoints_.at(id);
}

}  // namespace neug
