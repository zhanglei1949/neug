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

#include "neug/storages/csr/mutable_csr.h"

#include "neug/storages/module/module_factory.h"

#include <errno.h>
#include <stdint.h>
#include <unistd.h>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <numeric>
#include <thread>
#include <utility>
#include <vector>

#include "neug/storages/container/container_utils.h"
#include "neug/storages/container/file_mmap_container.h"
#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/property/types.h"
#include "neug/utils/spinlock.h"

namespace neug {

template <typename NbrType>
void initialize_adj_lists(NbrType** adj_lists_ptr,
                          std::atomic<int>* adj_list_size_ptr,
                          int* adj_list_cap_ptr, NbrType* nbr_list_ptr,
                          const int* degree_list_ptr, const int* cap_list_ptr,
                          size_t num_vertices) {
  for (size_t i = 0; i < num_vertices; ++i) {
    int deg = degree_list_ptr[i];
    int cap = cap_list_ptr[i];
    adj_lists_ptr[i] = nbr_list_ptr;
    adj_list_size_ptr[i].store(deg);
    adj_list_cap_ptr[i] = cap;
    nbr_list_ptr += cap;
  }
}

std::pair<std::shared_ptr<IDataContainer>, std::shared_ptr<IDataContainer>>
load_degree_and_capacity(const std::string& prefix) {
  auto degree_file_name = prefix + ".deg";
  auto cap_file_name = prefix + ".cap";

  std::shared_ptr<IDataContainer> degree_list =
      std::make_shared<FilePrivateMMap>();
  if (std::filesystem::exists(degree_file_name)) {
    degree_list->Open(degree_file_name);
  }

  std::shared_ptr<IDataContainer> cap_list = degree_list;
  if (std::filesystem::exists(cap_file_name)) {
    cap_list = std::make_shared<FilePrivateMMap>();
    cap_list->Open(cap_file_name);
  }
  return {degree_list, cap_list};
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::open_internal(const std::string& snapshot_prefix,
                                        const std::string& tmp_prefix,
                                        MemoryLevel mem_level) {
  Close();
  load_meta(snapshot_prefix);
  auto [degree_list, cap_list] = load_degree_and_capacity(snapshot_prefix);

  // For nbr_list: kSyncToFile copies snapshot to tmp and opens with
  // FileSharedMMap; kInMemory/kHugePagePrefered opens snapshot directly with
  // the corresponding anonymous/private mapping (same as the original code).
  nbr_list_ = prepare_and_open_container(snapshot_prefix + ".nbr",
                                         tmp_prefix + ".nbr", mem_level);
  auto v_cap = degree_list->GetDataSize() / sizeof(int);
  if (mem_level == MemoryLevel::kSyncToFile) {
    adj_list_buffer_ =
        prepare_and_open_container("", tmp_prefix + ".buf", mem_level);
    adj_list_size_ =
        prepare_and_open_container("", tmp_prefix + ".size", mem_level);
    adj_list_capacity_ =
        prepare_and_open_container("", tmp_prefix + ".cap", mem_level);
  } else {
    adj_list_buffer_ = prepare_and_open_container("", "", mem_level);
    adj_list_size_ = prepare_and_open_container("", "", mem_level);
    adj_list_capacity_ = prepare_and_open_container("", "", mem_level);
  }
  adj_list_buffer_->Resize(v_cap * sizeof(nbr_t*));
  adj_list_size_->Resize(v_cap * sizeof(std::atomic<int>));
  adj_list_capacity_->Resize(v_cap * sizeof(int));
  locks_ = new SpinLock[v_cap];

  auto degree_ptr = reinterpret_cast<const int*>(degree_list->GetData());
  auto cap_ptr = reinterpret_cast<const int*>(cap_list->GetData());
  initialize_adj_lists(
      reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData()),
      reinterpret_cast<std::atomic<int>*>(adj_list_size_->GetData()),
      reinterpret_cast<int*>(adj_list_capacity_->GetData()),
      reinterpret_cast<nbr_t*>(nbr_list_->GetData()), degree_ptr, cap_ptr,
      v_cap);
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::Open(const Checkpoint& ckp,
                               const ModuleDescriptor& descriptor,
                               MemoryLevel memory_level) {
  Close();
  std::unique_ptr<IDataContainer> degree_list;
  std::unique_ptr<IDataContainer> cap_list_owned;
  IDataContainer* cap_list_ptr = nullptr;

  if (!descriptor.module_type.empty()) {
    degree_list = ckp.OpenFile(descriptor.get_sub_module("degree_list").path,
                               memory_level);
    if (descriptor.has_sub_module("capacity_list")) {
      cap_list_owned = ckp.OpenFile(
          descriptor.get_sub_module("capacity_list").path, memory_level);
      cap_list_ptr = cap_list_owned.get();
    } else {
      cap_list_ptr = degree_list.get();
    }
    load_meta(descriptor.path);
    ckp.OpenFile(descriptor.get_sub_module("nbr_list").path, memory_level,
                 nbr_list_);
  }
  auto tmp_prefix = ckp.runtime_dir() + "/" + generate_uuid();
  auto v_cap = degree_list->GetDataSize() / sizeof(int);
  if (mem_level == MemoryLevel::kSyncToFile) {
    adj_list_buffer_ = OpenContainer("", tmp_prefix + ".buf", mem_level);
    adj_list_size_ = OpenContainer("", tmp_prefix + ".size", mem_level);
    adj_list_capacity_ = OpenContainer("", tmp_prefix + ".cap", mem_level);
  } else {
    adj_list_buffer_ = OpenContainer("", "", mem_level);
    adj_list_size_ = OpenContainer("", "", mem_level);
    adj_list_capacity_ = OpenContainer("", "", mem_level);
  }
  adj_list_buffer_->Resize(v_cap * sizeof(nbr_t*));
  adj_list_size_->Resize(v_cap * sizeof(std::atomic<int>));
  adj_list_capacity_->Resize(v_cap * sizeof(int));
  locks_ = new SpinLock[v_cap];

  const auto* degree_ptr = reinterpret_cast<const int*>(degree_list->GetData());
  const auto* cap_ptr = reinterpret_cast<const int*>(cap_list->GetData());
  auto* adj_lists_ptr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* adj_list_size_ptr =
      reinterpret_cast<std::atomic<int>*>(adj_list_size_->GetData());
  auto* adj_list_cap_ptr =
      reinterpret_cast<int*>(adj_list_capacity_->GetData());
  auto* nbr_list_ptr = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  for (size_t i = 0; i < v_cap; ++i) {
    int deg = degree_ptr[i];
    int cap = cap_ptr[i];
    adj_lists_ptr[i] = nbr_list_ptr;
    adj_list_size_ptr[i].store(deg);
    adj_list_cap_ptr[i] = cap;
    nbr_list_ptr += cap;
  }
  locks_ = new SpinLock[degree_list.size()];

  nbr_t* ptr = nbr_list_.data();
  for (size_t i = 0; i < degree_list.size(); ++i) {
    int degree = degree_list[i];
    int cap = (*cap_list)[i];
    adj_list_buffer_[i] = ptr;
    adj_list_capacity_[i] = cap;
    adj_list_size_[i] = degree;
    ptr += cap;
  }
  if (cap_list != &degree_list) {
    delete cap_list;
  }
}
template <typename EDATA_T>
ModuleDescriptor MutableCsr<EDATA_T>::Dump(const Checkpoint& ckp) {
  ModuleDescriptor descriptor;
  descriptor.module_type = ModuleTypeName();
  size_t vnum =
      adj_list_buffer_ ? adj_list_buffer_->GetDataSize() / sizeof(nbr_t*) : 0;
  {
    auto name = generate_uuid();
    auto new_snapshot_dir = ckp.runtime_dir();
    dump_meta(new_snapshot_dir + "/" + name);
    ModuleDescriptor meta;
    meta.path = new_snapshot_dir + "/" + name;
    descriptor.set_sub_module("meta", meta);
  }

  auto degree_list = OpenContainer("", "", MemoryLevel::kInMemory);
  degree_list->Resize(vnum * sizeof(int));
  auto cap_list = OpenContainer("", "", MemoryLevel::kInMemory);
  cap_list->Resize(vnum * sizeof(int));
  auto degree_ptr = reinterpret_cast<int*>(degree_list->GetData());
  auto cap_ptr = reinterpret_cast<int*>(cap_list->GetData());
  const auto* sz_arr =
      reinterpret_cast<const std::atomic<int>*>(adj_list_size_->GetData());
  const int* caps = reinterpret_cast<const int*>(adj_list_capacity_->GetData());
  for (size_t i = 0; i < vnum; ++i) {
    degree_ptr[i] = sz_arr[i].load(std::memory_order_relaxed);
    cap_ptr[i] = caps[i];
  }
  descriptor.set_sub_module("cap_list", ckp.Commit(*cap_list));
  descriptor.set_sub_module("degree_list", ckp.Commit(*degree_list));

  // auto cap_file = new_snapshot_dir + "/" + name + ".cap";
  // if (need_cap_list) {
  //   cap_list->Dump(cap_file);
  // } else if (std::filesystem::exists(cap_file)) {
  //   std::filesystem::remove(cap_file);
  // }

  auto name = generate_uuid();
  auto new_snapshot_dir = ckp.runtime_dir();
  ModuleDescriptor nbr_desc;
  nbr_desc.path = new_snapshot_dir + "/" + name + ".nbr";
  const nbr_t* const* lists =
      reinterpret_cast<const nbr_t* const*>(adj_list_buffer_->GetData());
  std::unique_ptr<FILE, decltype(&fclose)> fp(
      fopen(nbr_desc.path.c_str(), "wb"), &fclose);
  if (fp == nullptr) {
    THROW_IO_EXCEPTION("Failed to open file for writing: " + nbr_desc.path);
  }
  FileHeader header{};
  if (fwrite(&header, sizeof(FileHeader), 1, fp.get()) != 1) {
    THROW_IO_EXCEPTION("Failed to write header to: " + nbr_desc.path);
  }
  MD5_CTX ctx;
  MD5_Init(&ctx);
  for (size_t i = 0; i < vnum; ++i) {
    const void* data = lists[i];
    size_t len = static_cast<size_t>(caps[i]) * sizeof(nbr_t);
    if (len == 0 || data == nullptr) {
      continue;
    }
    if (fwrite(data, 1, len, fp.get()) != len) {
      THROW_IO_EXCEPTION("Failed to write segment " + std::to_string(i) +
                         " to: " + nbr_desc.path);
    }
    MD5_Update(&ctx, data, len);
  }
  MD5_Final(header.data_md5, &ctx);
  if (fseek(fp.get(), 0, SEEK_SET) != 0) {
    THROW_IO_EXCEPTION("Failed to seek in: " + nbr_desc.path);
  }
  if (fwrite(&header, sizeof(FileHeader), 1, fp.get()) != 1) {
    THROW_IO_EXCEPTION("Failed to rewrite header in: " + nbr_desc.path);
  }
  if (fclose(fp.release()) != 0) {
    THROW_IO_EXCEPTION("Failed to close file: " + nbr_desc.path);
  }
  descriptor.set_sub_module("nbr_list", nbr_desc);
  return descriptor;
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::reset_timestamp() {
  size_t vnum = vertex_capacity();
  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* sz_arr = reinterpret_cast<std::atomic<int>*>(adj_list_size_->GetData());
  for (size_t i = 0; i != vnum; ++i) {
    nbr_t* nbrs = buf_arr[i];
    if (nbrs == nullptr) {
      continue;
    }
    size_t deg = sz_arr[i].load(std::memory_order_relaxed);
    for (size_t j = 0; j != deg; ++j) {
      if (nbrs[j].timestamp != INVALID_TIMESTAMP) {
        nbrs[j].timestamp.store(0, std::memory_order_relaxed);
      }
    }
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::compact() {
  // We don't shrink the capacity of each adjacency list, but just remove the
  // deleted edges.
  size_t vnum = vertex_capacity();
  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* sz_arr = reinterpret_cast<std::atomic<int>*>(adj_list_size_->GetData());
  for (size_t i = 0; i != vnum; ++i) {
    int sz = sz_arr[i];
    nbr_t* read_ptr = buf_arr[i];
    if (read_ptr == nullptr) {
      continue;
    }
    nbr_t* read_end = read_ptr + sz;
    nbr_t* write_ptr = buf_arr[i];
    int removed = 0;
    while (read_ptr != read_end) {
      if (read_ptr->timestamp != INVALID_TIMESTAMP) {
        if (removed) {
          *write_ptr = *read_ptr;
        }
        ++write_ptr;
      } else {
        ++removed;
      }
      ++read_ptr;
    }
    sz_arr[i] -= removed;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::resize(vid_t vnum) {
  if (adj_list_buffer_ == nullptr || adj_list_size_ == nullptr ||
      adj_list_capacity_ == nullptr) {
    LOG(ERROR) << "Containers not initialized, cannot resize";
    THROW_RUNTIME_ERROR("Containers not initialized");
  }
  auto old_vnum = vertex_capacity();
  if (vnum > old_vnum) {
    adj_list_buffer_->Resize(vnum * sizeof(nbr_t*));
    adj_list_size_->Resize(vnum * sizeof(std::atomic<int>));
    adj_list_capacity_->Resize(vnum * sizeof(int));
    auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
    auto* sz_arr =
        reinterpret_cast<std::atomic<int>*>(adj_list_size_->GetData());
    auto* cap_arr = reinterpret_cast<int*>(adj_list_capacity_->GetData());
    for (vid_t i = old_vnum; i < vnum; ++i) {
      buf_arr[i] = nullptr;
      sz_arr[i].store(0, std::memory_order_relaxed);
      cap_arr[i] = 0;
    }
    delete[] locks_;
    locks_ = new SpinLock[vnum];
  } else {
    adj_list_buffer_->Resize(vnum * sizeof(nbr_t*));
    adj_list_size_->Resize(vnum * sizeof(std::atomic<int>));
    adj_list_capacity_->Resize(vnum * sizeof(int));
  }
}

template <typename EDATA_T>
size_t MutableCsr<EDATA_T>::capacity() const {
  // We assume the capacity of each csr is INFINITE.
  return CsrBase::INFINITE_CAPACITY;
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::Close() {
  if (locks_ != nullptr) {
    delete[] locks_;
    locks_ = nullptr;
  }
  if (adj_list_buffer_) {
    adj_list_buffer_->Close();
  }
  if (adj_list_size_) {
    adj_list_size_->Close();
  }
  if (adj_list_capacity_) {
    adj_list_capacity_->Close();
  }
  if (nbr_list_) {
    nbr_list_->Close();
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {
  if (adj_list_buffer_ != nullptr) {
    size_t vnum = vertex_capacity();
    auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
    auto* sz_arr =
        reinterpret_cast<std::atomic<int>*>(adj_list_size_->GetData());
    for (size_t i = 0; i != vnum; ++i) {
      nbr_t* begin = buf_arr[i];
      if (begin == nullptr) {
        continue;
      }
      std::sort(begin, begin + sz_arr[i].load(std::memory_order_relaxed),
                [](const nbr_t& lhs, const nbr_t& rhs) {
                  return lhs.data < rhs.data;
                });
    }
  }
  unsorted_since_ = ts;
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* sz_arr = reinterpret_cast<std::atomic<int>*>(adj_list_size_->GetData());
  for (vid_t src : src_set) {
    if (src < vnum) {
      sz_arr[src].store(0, std::memory_order_relaxed);
    }
  }
  for (vid_t src = 0; src < vnum; ++src) {
    if (sz_arr[src].load(std::memory_order_relaxed) == 0) {
      continue;
    }
    const nbr_t* read_ptr = buf_arr[src];
    if (read_ptr == nullptr) {
      continue;
    }
    const nbr_t* read_end =
        read_ptr + sz_arr[src].load(std::memory_order_relaxed);
    nbr_t* write_ptr = buf_arr[src];
    int removed = 0;
    while (read_ptr != read_end) {
      vid_t nbr = read_ptr->neighbor;
      if (dst_set.find(nbr) == dst_set.end()) {
        if (removed) {
          *write_ptr = *read_ptr;
        }
        ++write_ptr;
      } else {
        ++removed;
      }
      ++read_ptr;
    }
    sz_arr[src] -= removed;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list) {
  std::map<vid_t, std::set<vid_t>> src_dst_map;
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    src_dst_map[src].insert(dst_list[i]);
  }
  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* sz_arr = reinterpret_cast<std::atomic<int>*>(adj_list_size_->GetData());
  for (const auto& pair : src_dst_map) {
    vid_t src = pair.first;
    nbr_t* write_ptr = buf_arr[src];
    if (write_ptr == nullptr) {
      continue;
    }
    const nbr_t* read_end =
        write_ptr + sz_arr[src].load(std::memory_order_relaxed);
    while (write_ptr != read_end) {
      if (pair.second.find(write_ptr->neighbor) != pair.second.end()) {
        write_ptr->timestamp.store(std::numeric_limits<timestamp_t>::max());
      }
      ++write_ptr;
    }
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<std::pair<vid_t, int32_t>>& edges) {
  std::map<vid_t, std::set<int32_t>> src_offset_map;
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  const auto* sz_arr =
      reinterpret_cast<const std::atomic<int>*>(adj_list_size_->GetData());
  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  for (const auto& edge : edges) {
    if (edge.first >= vnum ||
        edge.second >= sz_arr[edge.first].load(std::memory_order_relaxed)) {
      continue;
    }
    src_offset_map[edge.first].insert(edge.second);
  }
  for (const auto& pair : src_offset_map) {
    vid_t src = pair.first;
    nbr_t* write_ptr = buf_arr[src];
    if (write_ptr == nullptr) {
      continue;
    }
    for (auto offset : pair.second) {
      write_ptr[offset].timestamp.store(
          std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::delete_edge(vid_t src, int32_t offset,
                                      timestamp_t ts) {
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  const auto* sz_arr =
      reinterpret_cast<const std::atomic<int>*>(adj_list_size_->GetData());
  if (src >= vnum || offset >= sz_arr[src].load(std::memory_order_relaxed)) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData())[src];
  if (nbrs == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("adjacency buffer is null");
  }
  auto old_ts = nbrs[offset].timestamp.load();
  if (old_ts <= ts) {
    nbrs[offset].timestamp.store(std::numeric_limits<timestamp_t>::max());
  } else if (old_ts == std::numeric_limits<timestamp_t>::max()) {
    LOG(ERROR) << "Attempting to delete already deleted edge.";
  } else {
    LOG(ERROR) << "Attempting to delete edge with timestamp " << old_ts
               << " using older timestamp " << ts;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::revert_delete_edge(vid_t src, vid_t nbr,
                                             int32_t offset, timestamp_t ts) {
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  const auto* sz_arr =
      reinterpret_cast<const std::atomic<int>*>(adj_list_size_->GetData());
  if (src >= vnum || offset >= sz_arr[src].load(std::memory_order_relaxed)) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* nbrs = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData())[src];
  if (nbrs == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("adjacency buffer is null");
  }
  if (nbrs[offset].neighbor != nbr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("neighbor id not match");
  }
  auto old_ts = nbrs[offset].timestamp.load();
  if (old_ts == std::numeric_limits<timestamp_t>::max()) {
    assert(nbrs[offset].neighbor == nbr);
    nbrs[offset].timestamp.store(ts);
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Attempting to revert delete on edge that is not deleted.");
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::batch_put_edges(const std::vector<vid_t>& src_list,
                                          const std::vector<vid_t>& dst_list,
                                          const std::vector<EDATA_T>& data_list,
                                          timestamp_t ts) {
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  if (vnum == 0) {
    return;
  }
  std::vector<int> degree(vnum, 0);
  for (auto src : src_list) {
    if (src < vnum) {
      degree[src]++;
    }
  }

  auto** buf_arr = reinterpret_cast<nbr_t**>(adj_list_buffer_->GetData());
  auto* sz_arr = reinterpret_cast<std::atomic<int>*>(adj_list_size_->GetData());
  auto* cap_arr = reinterpret_cast<int*>(adj_list_capacity_->GetData());
  size_t total_to_move = 0;
  size_t total_to_allocate = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    int old_deg = sz_arr[i].load(std::memory_order_relaxed);
    total_to_move += old_deg;
    int new_degree = degree[i] + old_deg;
    int new_cap = std::ceil(new_degree * NeugDBConfig::DEFAULT_RESERVE_RATIO);
    cap_arr[i] = new_cap;
    total_to_allocate += new_cap;
  }

  std::vector<nbr_t> new_nbr_list(total_to_move);
  size_t offset = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    int old_deg = sz_arr[i].load(std::memory_order_relaxed);
    if (old_deg > 0 && buf_arr[i] != nullptr) {
      memcpy(new_nbr_list.data() + offset, buf_arr[i], sizeof(nbr_t) * old_deg);
    }
    offset += old_deg;
  }
  nbr_list_->Resize(total_to_allocate * sizeof(nbr_t));
  auto base_ptr = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  offset = 0;
  size_t new_offset = 0;
  for (vid_t i = 0; i < vnum; ++i) {
    nbr_t* new_buffer = base_ptr != nullptr ? base_ptr + offset : nullptr;
    int old_deg = sz_arr[i].load(std::memory_order_relaxed);
    if (old_deg > 0 && new_buffer != nullptr) {
      memcpy(new_buffer, new_nbr_list.data() + new_offset,
             sizeof(nbr_t) * old_deg);
    }
    new_offset += old_deg;
    offset += cap_arr[i];
    buf_arr[i] = new_buffer;
    sz_arr[i].store(old_deg, std::memory_order_relaxed);
  }

  for (size_t i = 0; i < src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    vid_t dst = dst_list[i];
    const EDATA_T& data = data_list[i];
    auto& nbr =
        buf_arr[src][sz_arr[src].fetch_add(1, std::memory_order_relaxed)];
    nbr.neighbor = dst;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::load_meta(const std::string& prefix) {
  std::string meta_file_path = prefix + ".meta";
  if (std::filesystem::exists(meta_file_path)) {
    read_file(meta_file_path, &unsorted_since_, sizeof(timestamp_t), 1);
  } else {
    unsorted_since_ = 0;
  }
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::dump_meta(const std::string& prefix) const {
  std::string meta_file_path = prefix + ".meta";
  write_file(meta_file_path, &unsorted_since_, sizeof(timestamp_t), 1);
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::Open(const Checkpoint& ckp,
                                     const ModuleDescriptor& descriptor,
                                     MemoryLevel level) {
  if (!descriptor.module_type.empty() &&
      descriptor.has_sub_module("nbr_list")) {
    nbr_list_ = ckp.OpenFile(descriptor.get_sub_module("nbr_list").path, level);
  }
}

template <typename EDATA_T>
ModuleDescriptor SingleMutableCsr<EDATA_T>::Dump(const Checkpoint& ckp) {
  ModuleDescriptor descriptor;
  descriptor.module_type = ModuleTypeName();
  descriptor.set_sub_module("nbr_list", ckp.Commit(*nbr_list_));
  return descriptor;
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::reset_timestamp() {
  if (!nbr_list_) {
    return;
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  size_t vnum = vertex_capacity();
  for (size_t i = 0; i != vnum; ++i) {
    if (data[i].timestamp != INVALID_TIMESTAMP) {
      data[i].timestamp.store(0, std::memory_order_relaxed);
    }
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::compact() {}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::resize(vid_t vnum) {
  size_t old_vnum = vertex_capacity();
  nbr_list_->Resize(vnum * sizeof(nbr_t));
  if (vnum > old_vnum) {
    auto* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
    for (vid_t i = old_vnum; i < vnum; ++i) {
      data[i].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
size_t SingleMutableCsr<EDATA_T>::capacity() const {
  return vertex_capacity();
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::Close() {
  if (nbr_list_) {
    nbr_list_->Close();
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_sort_by_edge_data(timestamp_t ts) {}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_delete_vertices(
    const std::set<vid_t>& src_set, const std::set<vid_t>& dst_set) {
  if (!nbr_list_) {
    return;
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  for (auto src : src_set) {
    if (src < vnum) {
      data[src].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
  for (vid_t v = 0; v < vnum; ++v) {
    auto& nbr = data[v];
    if (dst_set.find(nbr.neighbor) != dst_set.end()) {
      nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list) {
  if (!nbr_list_) {
    return;
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  for (size_t i = 0; i != src_list.size(); ++i) {
    vid_t src = src_list[i];
    vid_t dst = dst_list[i];
    if (src >= vnum) {
      continue;
    }
    auto& nbr = data[src];
    if (nbr.neighbor == dst) {
      nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_delete_edges(
    const std::vector<std::pair<vid_t, int32_t>>& edge_list) {
  if (!nbr_list_) {
    return;
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  for (const auto& edge : edge_list) {
    vid_t src = edge.first;
    if (src >= vnum) {
      continue;
    }
    auto& nbr = data[src];
    assert(edge.second == 0);
    nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::delete_edge(vid_t src, int32_t offset,
                                            timestamp_t ts) {
  if (!nbr_list_) {
    return;
  }
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  if (src >= vnum) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "src out of bound: " + std::to_string(src) +
        " >= " + std::to_string(vnum));
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  auto& nbr = data[src];
  assert(offset == 0);
  if (nbr.timestamp.load() <= ts) {
    nbr.timestamp.store(std::numeric_limits<timestamp_t>::max());
  } else if (nbr.timestamp.load() == std::numeric_limits<timestamp_t>::max()) {
    LOG(ERROR) << "Fail to delete edge, already deleted.";
  } else {
    LOG(ERROR) << "Fail to delete edge, timestamp not satisfied.";
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::revert_delete_edge(vid_t src, vid_t nbr_vid,
                                                   int32_t offset,
                                                   timestamp_t ts) {
  if (!nbr_list_) {
    return;
  }
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  if (src >= vnum || offset != 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("src out of bound or offset out of bound");
  }
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  auto& nbr = data[src];
  if (nbr.neighbor != nbr_vid) {
    THROW_INVALID_ARGUMENT_EXCEPTION("neighbor id not match");
  }
  if (nbr.timestamp.load() == std::numeric_limits<timestamp_t>::max()) {
    nbr.timestamp.store(ts);
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Attempting to revert delete on edge that is not deleted.");
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::batch_put_edges(
    const std::vector<vid_t>& src_list, const std::vector<vid_t>& dst_list,
    const std::vector<EDATA_T>& data_list, timestamp_t ts) {
  if (!nbr_list_) {
    return;
  }
  vid_t vnum = static_cast<vid_t>(vertex_capacity());
  nbr_t* data = reinterpret_cast<nbr_t*>(nbr_list_->GetData());
  for (size_t i = 0; i != src_list.size(); ++i) {
    vid_t src = src_list[i];
    if (src >= vnum) {
      continue;
    }
    auto& nbr = data[src];
    nbr.neighbor = dst_list[i];
    nbr.data = data_list[i];
    nbr.timestamp.store(ts);
  }
}

template class MutableCsr<EmptyType>;
template class MutableCsr<int32_t>;
template class MutableCsr<uint32_t>;
template class MutableCsr<Date>;
template class MutableCsr<int64_t>;
template class MutableCsr<uint64_t>;
template class MutableCsr<double>;
template class MutableCsr<float>;
template class MutableCsr<DateTime>;
template class MutableCsr<Interval>;
template class MutableCsr<bool>;

template class SingleMutableCsr<float>;
template class SingleMutableCsr<double>;
template class SingleMutableCsr<uint64_t>;
template class SingleMutableCsr<int64_t>;
template class SingleMutableCsr<Date>;
template class SingleMutableCsr<uint32_t>;
template class SingleMutableCsr<int32_t>;
template class SingleMutableCsr<EmptyType>;
template class SingleMutableCsr<DateTime>;
template class SingleMutableCsr<Interval>;
template class SingleMutableCsr<bool>;

// ---------------------------------------------------------------------------
// ModuleFactory registrations
// ---------------------------------------------------------------------------
using MutableCsr_empty = MutableCsr<EmptyType>;
using MutableCsr_bool = MutableCsr<bool>;
using MutableCsr_int32 = MutableCsr<int32_t>;
using MutableCsr_uint32 = MutableCsr<uint32_t>;
using MutableCsr_int64 = MutableCsr<int64_t>;
using MutableCsr_uint64 = MutableCsr<uint64_t>;
using MutableCsr_float = MutableCsr<float>;
using MutableCsr_double = MutableCsr<double>;
using MutableCsr_date = MutableCsr<Date>;
using MutableCsr_datetime = MutableCsr<DateTime>;
using MutableCsr_interval = MutableCsr<Interval>;

NEUG_REGISTER_MODULE("mutable_csr_empty", MutableCsr_empty);
NEUG_REGISTER_MODULE("mutable_csr_bool", MutableCsr_bool);
NEUG_REGISTER_MODULE("mutable_csr_int32", MutableCsr_int32);
NEUG_REGISTER_MODULE("mutable_csr_uint32", MutableCsr_uint32);
NEUG_REGISTER_MODULE("mutable_csr_int64", MutableCsr_int64);
NEUG_REGISTER_MODULE("mutable_csr_uint64", MutableCsr_uint64);
NEUG_REGISTER_MODULE("mutable_csr_float", MutableCsr_float);
NEUG_REGISTER_MODULE("mutable_csr_double", MutableCsr_double);
NEUG_REGISTER_MODULE("mutable_csr_date", MutableCsr_date);
NEUG_REGISTER_MODULE("mutable_csr_datetime", MutableCsr_datetime);
NEUG_REGISTER_MODULE("mutable_csr_interval", MutableCsr_interval);

using SingleMutableCsr_empty = SingleMutableCsr<EmptyType>;
using SingleMutableCsr_bool = SingleMutableCsr<bool>;
using SingleMutableCsr_int32 = SingleMutableCsr<int32_t>;
using SingleMutableCsr_uint32 = SingleMutableCsr<uint32_t>;
using SingleMutableCsr_int64 = SingleMutableCsr<int64_t>;
using SingleMutableCsr_uint64 = SingleMutableCsr<uint64_t>;
using SingleMutableCsr_float = SingleMutableCsr<float>;
using SingleMutableCsr_double = SingleMutableCsr<double>;
using SingleMutableCsr_date = SingleMutableCsr<Date>;
using SingleMutableCsr_datetime = SingleMutableCsr<DateTime>;
using SingleMutableCsr_interval = SingleMutableCsr<Interval>;

NEUG_REGISTER_MODULE("single_mutable_csr_empty", SingleMutableCsr_empty);
NEUG_REGISTER_MODULE("single_mutable_csr_bool", SingleMutableCsr_bool);
NEUG_REGISTER_MODULE("single_mutable_csr_int32", SingleMutableCsr_int32);
NEUG_REGISTER_MODULE("single_mutable_csr_uint32", SingleMutableCsr_uint32);
NEUG_REGISTER_MODULE("single_mutable_csr_int64", SingleMutableCsr_int64);
NEUG_REGISTER_MODULE("single_mutable_csr_uint64", SingleMutableCsr_uint64);
NEUG_REGISTER_MODULE("single_mutable_csr_float", SingleMutableCsr_float);
NEUG_REGISTER_MODULE("single_mutable_csr_double", SingleMutableCsr_double);
NEUG_REGISTER_MODULE("single_mutable_csr_date", SingleMutableCsr_date);
NEUG_REGISTER_MODULE("single_mutable_csr_datetime", SingleMutableCsr_datetime);
NEUG_REGISTER_MODULE("single_mutable_csr_interval", SingleMutableCsr_interval);

using EmptyCsr_empty = EmptyCsr<EmptyType>;
using EmptyCsr_bool = EmptyCsr<bool>;
using EmptyCsr_int32 = EmptyCsr<int32_t>;
using EmptyCsr_uint32 = EmptyCsr<uint32_t>;
using EmptyCsr_int64 = EmptyCsr<int64_t>;
using EmptyCsr_uint64 = EmptyCsr<uint64_t>;
using EmptyCsr_float = EmptyCsr<float>;
using EmptyCsr_double = EmptyCsr<double>;
using EmptyCsr_date = EmptyCsr<Date>;
using EmptyCsr_datetime = EmptyCsr<DateTime>;
using EmptyCsr_interval = EmptyCsr<Interval>;

NEUG_REGISTER_MODULE("empty_csr_empty", EmptyCsr_empty);
NEUG_REGISTER_MODULE("empty_csr_bool", EmptyCsr_bool);
NEUG_REGISTER_MODULE("empty_csr_int32", EmptyCsr_int32);
NEUG_REGISTER_MODULE("empty_csr_uint32", EmptyCsr_uint32);
NEUG_REGISTER_MODULE("empty_csr_int64", EmptyCsr_int64);
NEUG_REGISTER_MODULE("empty_csr_uint64", EmptyCsr_uint64);
NEUG_REGISTER_MODULE("empty_csr_float", EmptyCsr_float);
NEUG_REGISTER_MODULE("empty_csr_double", EmptyCsr_double);
NEUG_REGISTER_MODULE("empty_csr_date", EmptyCsr_date);
NEUG_REGISTER_MODULE("empty_csr_datetime", EmptyCsr_datetime);
NEUG_REGISTER_MODULE("empty_csr_interval", EmptyCsr_interval);

}  // namespace neug
