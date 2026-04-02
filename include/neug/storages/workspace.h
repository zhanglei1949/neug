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

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <mutex>
#include "neug/storages/container/container_utils.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/module_descriptor.h"
#include "neug/storages/snapshot_meta.h"
#include "neug/utils/uuid.h"

namespace neug {

/**
 * @brief Generate a random UUID string by reading from the kernel entropy pool.
 *
 * On Linux, /proc/sys/kernel/random/uuid provides a fresh RFC-4122 UUID on
 * each read.  The returned string is in canonical 8-4-4-4-12 form, e.g.:
 *   "a3b8c1d2-e4f5-6789-abcd-ef0123456789"
 */
inline std::string generate_uuid() {
  static UUIDGenerator generator;
  return generator.generate();
}

/**
 * @brief Represents a single numbered checkpoint directory.
 *
 * A checkpoint lives at `db_dir/checkpoint-NNNNN/` and contains:
 *
 * ```
 * checkpoint-NNNNN/
 * ├── meta        ← SnapshotMeta JSON
 * ├── snapshot/   ← immutable data files
 * ├── runtime/    ← mutable working files (allocator, tmp, …)
 * └── wal/        ← write-ahead log files
 * ```
 */
class Checkpoint {
 public:
  Checkpoint() = default;
  Checkpoint(std::string path, uint32_t id);

  // ---------------------------------------------------------------------------
  // Directory helpers
  // ---------------------------------------------------------------------------

  /// Root path of this checkpoint: `db_dir/checkpoint-NNNNN`.
  const std::string& path() const { return path_; }

  /// Numeric checkpoint identifier.
  uint32_t id() const { return id_; }

  /// `path/snapshot/`
  std::string snapshot_dir() const {
    assert(!IsEmpty());
    return path_ + "/snapshot";
  }

  /// `path/runtime/`
  std::string runtime_dir() const {
    assert(!IsEmpty());
    return path_ + "/runtime";
  }

  /// `path/wal/`
  std::string wal_dir() const {
    assert(!IsEmpty());
    return path_ + "/wal";
  }

  /// `path/meta`
  std::string meta_path() const {
    assert(!IsEmpty());
    return path_ + "/meta";
  }

  /**
   * @brief Open a data container for @p file_path using the appropriate
   * strategy for @p level.  The container is created fresh and assigned to
   * @p buffer.
   *
   * - kSyncToFile: if the file exists it is opened MAP_SHARED in-place;
   *   otherwise a new file is created in runtime_dir().
   * - kInMemory / kHugePagePrefered: if the file exists it is loaded via
   *   MAP_PRIVATE; otherwise an anonymous mapping is returned.
   */
  std::unique_ptr<IDataContainer> OpenFile(const std::string& file_path,
                                           MemoryLevel level);

  std::unique_ptr<IDataContainer> CreateRuntimeContainer(size_t size,
                                                         MemoryLevel level);

  /**
   * @brief Commit a data container to a persistent snapshot file and return
   * a ModuleDescriptor whose `path` points to the written file.
   *
   * For MAP_SHARED containers (kSyncToFile) the backing file is already on
   * disk – we just Sync() and record its path.  For all other container
   * types the data is written to a freshly generated UUID file under
   * runtime_dir().
   */
  ModuleDescriptor Commit(IDataContainer& buffer);

  /**
   * @brief Create the checkpoint's sub-directories if they do not exist.
   * Creates: snapshot/, runtime/, wal/ under path_.
   */
  void create_dirs() const;

  // ---------------------------------------------------------------------------
  // Meta helpers
  // ---------------------------------------------------------------------------

  /**
   * @brief Load and return the SnapshotMeta stored in this checkpoint.
   * Reads the JSON file at meta_path().
   */
  SnapshotMeta LoadMeta() const;

  /**
   * @brief Persist @p meta to this checkpoint's meta file.
   * Writes the JSON file to meta_path().
   */
  void SaveMeta(SnapshotMeta meta) const;

  bool IsEmpty() const { return path_.empty() || id_ == 0; }

  std::string create_runtime_object();

  void CommitRuntimeObject(const std::string& uuid) {
    std::lock_guard<std::mutex> lock(mutex_);
    uncommitted_runtime_objects_.erase(uuid);
    runtime_objects_.insert(uuid);
  }

 private:
  std::string path_;
  uint32_t id_;
  mutable std::mutex mutex_;
  std::set<std::string> runtime_objects_;
  std::set<std::string> uncommitted_runtime_objects_;
};

// ---------------------------------------------------------------------------

/**
 * @brief Manages a database directory that holds multiple numbered checkpoints.
 *
 * Directory layout:
 * ```
 * db_dir/
 * ├── checkpoint-00001/
 * │   ├── meta
 * │   ├── snapshot/
 * │   ├── runtime/
 * │   └── wal/
 * ├── checkpoint-00002/
 * └── ...
 * ```
 *
 * `Workspace` does **not** inherit `Module`; it is a directory-level
 * manager, not a data module itself.
 */
class Workspace {
 public:
  Workspace();
  ~Workspace();

  /**
   * @brief Open a database directory.
   * @param db_dir Path to the database directory
   */
  void Open(const std::string& db_dir);

  /**
   * @brief Close the workspace and release resources.
   */
  void Close();

  /**
   * @brief Get the number of checkpoints in the workspace.
   */
  size_t NumCheckpoints() const;

  /**
   * @brief Get the ID of the most recent checkpoint.
   * @return -1 if no checkpoints exist
   */
  int HeadId() const;

  /**
   * @brief Create a new checkpoint.
   * @return The ID of the new checkpoint
   */
  int CreateCheckpoint();

  /**
   * @brief Get a checkpoint by ID (const version).
   */
  Checkpoint& GetCheckpoint(int id) const;

  /**
   * @brief Get a checkpoint by ID.
   */
  Checkpoint& GetCheckpoint(int id);

  Checkpoint& GetLatestCheckpoint() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (checkpoints_.empty()) {
      throw std::runtime_error("No checkpoints available");
    }
    return *checkpoints_.rbegin()->second;
  }

 private:
  std::string db_dir_;
  std::map<int, std::unique_ptr<Checkpoint>> checkpoints_;
  mutable std::mutex mutex_;
};

inline std::unique_ptr<IDataContainer> Checkpoint::OpenFile(
    const std::string& file_path, MemoryLevel level) {
  std::string new_path = "";
  if (!file_path.empty() && std::filesystem::exists(file_path)) {
    new_path = runtime_dir() + "/" + create_runtime_object();
  }
  return OpenContainer(file_path, new_path, level);
}

inline ModuleDescriptor Checkpoint::Commit(IDataContainer& buffer) {
  ModuleDescriptor desc;
  if (buffer.GetContainerType() == ContainerType::kFileSharedMMap) {
    // MAP_SHARED container: data is already persisted in its backing file.
    // Just sync dirty pages and record the existing path.
    buffer.Sync();
    desc.path = buffer.GetPath();
    buffer.Close();
  } else {
    // Anonymous or MAP_PRIVATE container: write data out to a new file.
    auto dest = runtime_dir() + "/" + create_runtime_object();
    buffer.Dump(dest);
    buffer.Close();
    desc.path = dest;
  }
  return desc;
}

}  // namespace neug
