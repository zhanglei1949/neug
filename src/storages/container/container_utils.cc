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

#include "neug/storages/container_utils.h"

#include <cstdio>
#include <filesystem>
#include <functional>
#include <memory>
#include <utility>

#include "neug/storages/container/file_header.h"
#include "neug/storages/container/i_container.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"

namespace neug {

void prepare_container_file(const std::string& snapshot_file,
                            const std::string& tmp_file) {
  // Ensure the target directory exists
  auto parent_dir = std::filesystem::path(tmp_file).parent_path();
  if (!parent_dir.empty()) {
    std::filesystem::create_directories(parent_dir);
  }

  if (std::filesystem::exists(snapshot_file)) {
    file_utils::copy_file(snapshot_file, tmp_file, true);
  } else {
    CreateEmptyContainerFile(tmp_file);
  }
}

std::unique_ptr<IDataContainer> prepare_and_open_container(
    const std::string& snapshot_file, const std::string& tmp_file,
    MemoryLevel memory_level) {
  if (memory_level == MemoryLevel::kSyncToFile) {
    if (tmp_file.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Temporary file path is required for disk-backed containers");
    }
    // For disk-backed containers, prepare the file first
    prepare_container_file(snapshot_file, tmp_file);
    return OpenDataContainer(memory_level, tmp_file);
  } else {
    // For in-memory or hugepage containers, use snapshot file directly
    return OpenDataContainer(memory_level, snapshot_file);
  }
}

void write_nbr_file(
    const std::string& path, size_t num_segs,
    const std::function<std::pair<const void*, size_t>(size_t)>& seg_fn) {
  FILE* fp = fopen(path.c_str(), "wb");
  if (fp == nullptr) {
    THROW_IO_EXCEPTION("Failed to open file for writing: " + path);
  }
  FileHeader header{};
  if (fwrite(&header, sizeof(FileHeader), 1, fp) != 1) {
    fclose(fp);
    THROW_IO_EXCEPTION("Failed to write header to: " + path);
  }
  MD5_CTX ctx;
  MD5_Init(&ctx);
  for (size_t i = 0; i < num_segs; ++i) {
    auto [data, len] = seg_fn(i);
    if (len == 0 || data == nullptr) {
      continue;
    }
    if (fwrite(data, 1, len, fp) != len) {
      fclose(fp);
      THROW_IO_EXCEPTION("Failed to write segment " + std::to_string(i) +
                         " to: " + path);
    }
    MD5_Update(&ctx, data, len);
  }
  MD5_Final(header.data_md5, &ctx);
  if (fseek(fp, 0, SEEK_SET) != 0) {
    fclose(fp);
    THROW_IO_EXCEPTION("Failed to seek in: " + path);
  }
  if (fwrite(&header, sizeof(FileHeader), 1, fp) != 1) {
    fclose(fp);
    THROW_IO_EXCEPTION("Failed to rewrite header in: " + path);
  }
  if (fclose(fp) != 0) {
    THROW_IO_EXCEPTION("Failed to close file: " + path);
  }
}

}  // namespace neug
