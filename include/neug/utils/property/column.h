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

#include <glog/logging.h>
#include <stddef.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/likely.h"
#include "neug/utils/mmap_array.h"
#include "neug/utils/property/list_view.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {
class Table;

std::string_view truncate_utf8(std::string_view str, size_t length);

class ColumnBase {
 public:
  virtual ~ColumnBase() {}

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void open_in_memory(const std::string& name) = 0;

  virtual void open_with_hugepages(const std::string& name) = 0;

  virtual void close() = 0;

  virtual void dump(const std::string& filename) = 0;

  virtual size_t size() const = 0;

  virtual void resize(size_t size) = 0;
  virtual void resize(size_t size, const Property& default_value) = 0;

  virtual DataTypeId type() const = 0;

  // insert_safe is true when the column needs to be resized to accommodate the
  // new value, which can happen when the value is not fixed length. If the
  // value is fixed length, we should already have enough space allocated, so
  // insert_safe can be false.
  virtual void set_any(size_t index, const Property& value,
                       bool insert_safe = false) = 0;

  virtual Property get_prop(size_t index) const = 0;

  virtual void set_prop(size_t index, const Property& prop) {
    LOG(FATAL) << "Not implemented";
  }

  virtual void ingest(uint32_t index, OutArchive& arc) = 0;

  virtual void ensure_writable(const std::string& work_dir) = 0;
};

template <typename T>
class TypedColumn : public ColumnBase {
 public:
  explicit TypedColumn() : size_(0) {}
  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    std::string basic_path = snapshot_dir + "/" + name;
    if (std::filesystem::exists(basic_path)) {
      buffer_.open(basic_path, false, false);
      size_ = buffer_.size();
    } else {
      if (work_dir == "") {
        size_ = 0;
      } else {
        buffer_.open(work_dir + "/" + name, true);
        size_ = buffer_.size();
      }
    }
  }

  void open_in_memory(const std::string& name) override {
    if (!name.empty() && std::filesystem::exists(name)) {
      buffer_.open(name, false);
      size_ = buffer_.size();
    } else {
      buffer_.reset();
      size_ = 0;
    }
  }

  void open_with_hugepages(const std::string& name) override {
    if (!name.empty() && std::filesystem::exists(name)) {
      buffer_.open_with_hugepages(name);
      size_ = buffer_.size();
    } else {
      buffer_.reset();
      buffer_.set_hugepage_prefered(true);
      size_ = 0;
    }
  }

  void close() override { buffer_.reset(); }

  void dump(const std::string& filename) override { buffer_.dump(filename); }

  size_t size() const override { return size_; }

  void resize(size_t size) override {
    size_ = size;
    buffer_.resize(size_);
  }

  // Assume it is safe to insert the default value even if it is reserving,
  // since user could always override
  void resize(size_t size, const Property& default_value) override {
    if (default_value.type() != type()) {
      THROW_RUNTIME_ERROR("Default value type does not match column type");
    }
    size_t old_size = size_;
    size_ = size;
    buffer_.resize(size_);
    auto default_typed_value = PropUtils<T>::to_typed(default_value);
    for (size_t i = old_size; i < size_; ++i) {
      set_value(i, default_typed_value);
    }
  }

  DataTypeId type() const override { return PropUtils<T>::prop_type(); }

  void set_value(size_t index, const T& val) {
    if (index < size_) {
      buffer_.set(index, val);
    } else {
      THROW_RUNTIME_ERROR("Index out of range");
    }
  }

  void set_any(size_t index, const Property& value, bool insert_safe) override {
    // allow resize is ignored for fixed-length types
    set_value(index, PropUtils<T>::to_typed(value));
  }

  inline T get_view(size_t index) const {
    assert(index < size_);
    return buffer_.get(index);
  }

  Property get_prop(size_t index) const override {
    return PropUtils<T>::to_prop(get_view(index));
  }

  void set_prop(size_t index, const Property& prop) override {
    set_value(index, PropUtils<T>::to_typed(prop));
  }

  void ingest(uint32_t index, OutArchive& arc) override {
    T val;
    arc >> val;
    set_value(index, val);
  }

  const mmap_array<T>& buffer() const { return buffer_; }
  size_t buffer_size() const { return size_; }

  void ensure_writable(const std::string& work_dir) override {
    buffer_.ensure_writable(work_dir);
  }

 private:
  mmap_array<T> buffer_;
  size_t size_;
};

using BoolColumn = TypedColumn<bool>;
using UInt8Column = TypedColumn<uint8_t>;
using UInt16Column = TypedColumn<uint16_t>;
using IntColumn = TypedColumn<int32_t>;
using UIntColumn = TypedColumn<uint32_t>;
using LongColumn = TypedColumn<int64_t>;
using ULongColumn = TypedColumn<uint64_t>;
using DateColumn = TypedColumn<Date>;
using DoubleColumn = TypedColumn<double>;
using FloatColumn = TypedColumn<float>;
using DateTimeColumn = TypedColumn<DateTime>;
using IntervalColumn = TypedColumn<Interval>;

template <>
class TypedColumn<EmptyType> : public ColumnBase {
 public:
  explicit TypedColumn() {}
  ~TypedColumn() {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}
  void open_in_memory(const std::string& name) override {}
  void open_with_hugepages(const std::string& name) override {}
  void dump(const std::string& filename) override {}
  void close() override {}
  size_t size() const override { return 0; }
  void resize(size_t size) override {}
  void resize(size_t size, const Property& default_value) override {}

  DataTypeId type() const override { return DataTypeId::kEmpty; }

  void set_any(size_t index, const Property& value, bool insert_safe) override {
  }

  void set_value(size_t index, const EmptyType& value) {}

  Property get_prop(size_t index) const override { return Property::empty(); }

  void set_prop(size_t index, const Property& prop) override {}

  EmptyType get_view(size_t index) const { return EmptyType(); }

  void ingest(uint32_t index, OutArchive& arc) override {}

  void ensure_writable(const std::string& work_dir) override {}
};

template <>
class TypedColumn<std::string_view> : public ColumnBase {
 public:
  TypedColumn(uint16_t width)
      : size_(0), pos_(0), width_(width), type_(DataTypeId::kVarchar) {}
  explicit TypedColumn()
      : size_(0),
        pos_(0),
        width_(STRING_DEFAULT_MAX_LENGTH),
        type_(DataTypeId::kVarchar) {}
  TypedColumn(TypedColumn<std::string_view>&& rhs) {
    buffer_.swap(rhs.buffer_);
    size_ = rhs.size_;
    pos_ = rhs.pos_.load();
    width_ = rhs.width_;
    type_ = rhs.type_;
  }

  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    std::string basic_path = snapshot_dir + "/" + name;
    if (std::filesystem::exists(basic_path + ".items")) {
      buffer_.open(basic_path, false, false);
      size_ = buffer_.size();
      init_pos(basic_path + ".pos");
    } else {
      if (work_dir == "") {
        size_ = 0;
        pos_.store(0);
      } else {
        buffer_.open(work_dir + "/" + name, true);
        size_ = buffer_.size();
        init_pos(work_dir + "/" + name + ".pos");
      }
    }
  }

  void open_in_memory(const std::string& prefix) override {
    buffer_.open(prefix, false);
    size_ = buffer_.size();
    init_pos(prefix + ".pos");
  }

  void open_with_hugepages(const std::string& prefix) override {
    buffer_.open_with_hugepages(prefix);
    size_ = buffer_.size();
    init_pos(prefix + ".pos");
  }

  void close() override { buffer_.reset(); }

  void dump(const std::string& filename) override {
    size_t pos_val = pos_.load();
    write_file(filename + ".pos", &pos_val, sizeof(pos_val), 1);
    buffer_.dump(filename);
  }

  size_t size() const override { return size_; }

  void resize(size_t size) override {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    size_ = size;
    if (buffer_.size() != 0) {
      size_t avg_width =
          buffer_.avg_size();  // calculate average width of existing strings
      buffer_.resize(
          size_,
          std::max(size_ * (avg_width > 0 ? avg_width : width_), pos_.load()));
    } else {
      buffer_.resize(size_, std::max(size_ * width_, pos_.load()));
    }
  }

  void resize(size_t size, const Property& default_value) override {
    if (default_value.type() != type()) {
      THROW_RUNTIME_ERROR("Default value type does not match column type");
    }
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    size_t old_size = size_;
    size_ = size;
    auto default_str = PropUtils<std::string_view>::to_typed(default_value);
    default_str = truncate_utf8(default_str, width_);
    if (buffer_.size() != 0) {
      size_t avg_width =
          buffer_.avg_size();  // calculate average width of existing strings
      buffer_.resize(size_,
                     std::max(size_ * (avg_width > 0 ? avg_width : width_),
                              pos_.load() + width_));
    } else {
      buffer_.resize(size_, std::max(size_ * width_, pos_.load()));
    }
    if (default_str.size() == 0) {
      return;
    }

    if (old_size < size_) {
      set_value(old_size, default_str);
      auto string_item = buffer_.get_string_item(old_size);
      for (size_t i = old_size + 1; i < size_; ++i) {
        buffer_.set_string_item(i, string_item);
      }
    }
  }

  DataTypeId type() const override { return type_; }

  void set_value(size_t idx, const std::string_view& val) {
    auto copied_val = val;
    if (copied_val.size() >= width_) {
      VLOG(1) << "String length" << copied_val.size()
              << " exceeds the maximum length: " << width_ << ", cut off.";
      copied_val = truncate_utf8(copied_val, width_);
    }
    if (idx < size_ && pos_.load() + copied_val.size() <= buffer_.data_size()) {
      // NOTE: Even if idx has been set before, we always append the new value
      // to the end of buffer_. The previous value is not reclaimed, and should
      // be handled by garbage collection or compaction.
      size_t offset = pos_.fetch_add(copied_val.size());
      buffer_.set(idx, offset, copied_val);
    } else {
      THROW_RUNTIME_ERROR("Index out of range or not enough space in buffer");
    }
  }

  void set_any(size_t idx, const Property& value, bool insert_safe) override {
    if (insert_safe) {
      set_value_safe(idx, PropUtils<std::string_view>::to_typed(value));
    } else {
      set_value(idx, value.as_string_view());
    }
  }

  void set_value_safe(size_t idx, const std::string_view& value);

  inline std::string_view get_view(size_t idx) const {
    return buffer_.get(idx);
  }

  Property get_prop(size_t index) const override {
    return PropUtils<std::string_view>::to_prop(get_view(index));
  }

  void set_prop(size_t index, const Property& prop) override {
    set_value(index, PropUtils<std::string_view>::to_typed(prop));
  }

  void ingest(uint32_t index, OutArchive& arc) override {
    std::string_view val;
    arc >> val;
    set_value(index, val);
  }

  const mmap_array<std::string_view>& buffer() const { return buffer_; }

  size_t buffer_size() const { return size_; }

  void ensure_writable(const std::string& work_dir) override {
    buffer_.ensure_writable(work_dir);
  }

 private:
  inline void init_pos(const std::string& file_path) {
    if (std::filesystem::exists(file_path)) {
      size_t pos_val = 0;
      read_file(file_path, &pos_val, sizeof(pos_val), 1);
      pos_.store(pos_val);
    } else {
      pos_.store(0);
    }
  }

  mmap_array<std::string_view> buffer_;
  size_t size_;
  std::atomic<size_t> pos_;
  std::shared_mutex rw_mutex_;
  uint16_t width_;
  DataTypeId type_;
};

using StringColumn = TypedColumn<std::string_view>;

// ---------------------------------------------------------------------------
// list_storage_item
// ---------------------------------------------------------------------------
// Index entry used by ListColumn.  Wider than string_item (which has a 16-bit
// length field) to accommodate large list blobs.
struct list_storage_item {
  uint64_t offset;  // byte offset in the ListColumn data buffer
  uint32_t length;  // byte length of the serialized blob
  uint32_t padding{0};
};
static_assert(sizeof(list_storage_item) == 16,
              "list_storage_item size must be 16 bytes");

// ---------------------------------------------------------------------------
// ListColumn
// ---------------------------------------------------------------------------
// Stores a column of list-typed property values.  Each entry is a serialized
// binary blob produced by ListViewBuilder::finish_pod<T>() or
// ListViewBuilder::finish_varlen().
//
// Storage layout on disk (prefix = column name):
//   <prefix>.items  -- mmap_array<list_storage_item>: offset+length per entry
//   <prefix>.data   -- mmap_array<char>: packed blob storage
//   <prefix>.pos    -- uint64_t: committed write frontier in data buffer
//
// Reading:
//   ListView lv = col.get_view(idx);
//   // access via lv.GetElem<T>() / lv.GetChildStringView() etc.
//
// Writing:
//   ListViewBuilder b;
//   b.append_pod(val);          // or b.append_blob(sv);
//   Property p = Property::from_list_data(b.finish_pod<int32_t>());
//   col.set_any(idx, p, /*insert_safe=*/true);
class ListColumn : public ColumnBase {
 public:
  explicit ListColumn(const DataType& list_type)
      : list_type_(list_type), size_(0), pos_(0) {}
  ~ListColumn() override { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    std::string basic = snapshot_dir + "/" + name;
    if (std::filesystem::exists(basic + ".items")) {
      items_.open(basic + ".items", false, false);
      data_.open(basic + ".data", false, false);
      size_ = items_.size();
      init_pos(basic + ".pos");
    } else if (!work_dir.empty()) {
      std::string work = work_dir + "/" + name;
      items_.open(work + ".items", true);
      data_.open(work + ".data", true);
      size_ = items_.size();
      init_pos(work + ".pos");
    } else {
      size_ = 0;
      pos_.store(0);
    }
  }

  void open_in_memory(const std::string& prefix) override {
    if (!prefix.empty()) {
      items_.open(prefix + ".items", false);
      data_.open(prefix + ".data", false);
      size_ = items_.size();
      init_pos(prefix + ".pos");
    } else {
      size_ = 0;
      pos_.store(0);
    }
  }

  void open_with_hugepages(const std::string& prefix) override {
    if (!prefix.empty()) {
      items_.open_with_hugepages(prefix + ".items");
      data_.open_with_hugepages(prefix + ".data");
      size_ = items_.size();
      init_pos(prefix + ".pos");
    } else {
      size_ = 0;
      pos_.store(0);
    }
  }

  void close() override {
    items_.reset();
    data_.reset();
  }

  void dump(const std::string& filename) override {
    size_t pos_val = pos_.load();
    write_file(filename + ".pos", &pos_val, sizeof(pos_val), 1);
    items_.dump(filename + ".items");
    data_.dump(filename + ".data");
  }

  size_t size() const override { return size_; }

  void resize(size_t size) override {
    std::unique_lock<std::shared_mutex> lk(rw_mutex_);
    items_.resize(size);
    // Keep at least as much data space as already committed.
    size_t needed = std::max(data_.size(), pos_.load());
    data_.resize(std::max(needed, size * 64));  // 64B heuristic per list
    size_ = size;
  }

  void resize(size_t size, const Property& default_value) override {
    if (default_value.type() != DataTypeId::kList &&
        default_value.type() != DataTypeId::kEmpty) {
      THROW_RUNTIME_ERROR("Default value type does not match list column");
    }
    resize(size);
    // Leave entries zero-initialized (empty lists) for new slots.
  }

  DataTypeId type() const override { return DataTypeId::kList; }

  // Return the full DataType::List(...) of this column.
  const DataType& list_type() const { return list_type_; }

  // Store a pre-built blob (from ListViewBuilder::finish_*) at index idx.
  // The blob bytes are copied into the internal data buffer.
  void set_value(size_t idx, std::string_view blob) {
    if (idx >= size_) {
      THROW_RUNTIME_ERROR("Index out of range in ListColumn::set_value");
    }
    size_t offset = pos_.fetch_add(blob.size());
    if (offset + blob.size() > data_.size()) {
      std::unique_lock<std::shared_mutex> lk(rw_mutex_);
      if (offset + blob.size() > data_.size()) {
        data_.resize(
            std::max(data_.size() * 2, offset + blob.size() + blob.size()));
      }
    }
    if (!blob.empty()) {
      std::memcpy(data_.data() + offset, blob.data(), blob.size());
    }
    items_.set(idx, {static_cast<uint64_t>(offset),
                     static_cast<uint32_t>(blob.size())});
  }

  void set_any(size_t idx, const Property& value, bool insert_safe) override {
    set_value(idx, value.as_list_data());
  }

  ListView get_view(size_t idx) const {
    assert(idx < size_);
    const auto& item = items_.get(idx);
    return ListView(list_type_,
                    std::string_view(data_.data() + item.offset, item.length));
  }

  Property get_prop(size_t idx) const override {
    const auto& item = items_.get(idx);
    return Property::from_list_data(
        std::string_view(data_.data() + item.offset, item.length));
  }

  void set_prop(size_t idx, const Property& prop) override {
    set_value(idx, prop.as_list_data());
  }

  void ingest(uint32_t idx, OutArchive& arc) override {
    std::string_view sv;
    arc >> sv;
    set_value(idx, sv);
  }

  void ensure_writable(const std::string& work_dir) override {
    items_.ensure_writable(work_dir);
    data_.ensure_writable(work_dir);
  }

 private:
  void init_pos(const std::string& pos_path) {
    if (std::filesystem::exists(pos_path)) {
      size_t v = 0;
      read_file(pos_path, &v, sizeof(v), 1);
      pos_.store(v);
    } else {
      size_t total = 0;
      for (size_t i = 0; i < items_.size(); ++i) {
        const auto& it = items_.get(i);
        total = std::max(total, static_cast<size_t>(it.offset) + it.length);
      }
      pos_.store(total);
    }
  }

  DataType list_type_;
  mmap_array<list_storage_item> items_;
  mmap_array<char> data_;
  size_t size_;
  std::atomic<size_t> pos_;
  mutable std::shared_mutex rw_mutex_;
};

std::shared_ptr<ColumnBase> CreateColumn(DataType type);

/// Create RefColumn for ease of usage for hqps
class RefColumnBase {
 public:
  enum class ColType {
    kInternal,
    kExternal,
  };
  virtual ~RefColumnBase() {}
  virtual Property get(size_t index) const = 0;
  virtual DataTypeId type() const = 0;
  virtual ColType col_type() const = 0;
};

// Different from TypedColumn, RefColumn is a wrapper of mmap_array
template <typename T>
class TypedRefColumn : public RefColumnBase {
 public:
  using value_type = T;

  explicit TypedRefColumn(const TypedColumn<T>& column)
      : basic_buffer(column.buffer()), basic_size(column.buffer_size()) {}
  ~TypedRefColumn() {}

  inline T get_view(size_t index) const {
    assert(index < basic_size);
    return basic_buffer.get(index);
  }

  Property get(size_t index) const override {
    return PropUtils<T>::to_prop(get_view(index));
  }

  DataTypeId type() const override { return PropUtils<T>::prop_type(); }

  ColType col_type() const override { return ColType::kInternal; }

 private:
  const mmap_array<T>& basic_buffer;
  size_t basic_size;
};

// Create a reference column from a ColumnBase that contains a const reference
// to the actual column storage, offering a column-based store interface for
// vertex properties.
std::shared_ptr<RefColumnBase> CreateRefColumn(const ColumnBase& column);

}  // namespace neug
