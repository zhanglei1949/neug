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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>
#include <limits>
#include <type_traits>

#include "neug/storages/allocators.h"
#include "neug/storages/csr/nbr.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/spinlock.h"

namespace neug {

// CsrType lives in the view header so CsrBaseView can hold it without pulling
// in csr_base.h (avoiding a csr_base <-> csr_base_view include cycle).
enum class CsrType {
  kImmutable,
  kMutable,
  kSingleMutable,
  kSingleImmutable,
  kEmpty,
};

// ============================================================================
// NbrIterConfig / NbrIterator / NbrList — migrated from generic_view.h
// ============================================================================

struct NbrIterConfig {
  int stride : 16;      ///< Byte stride between neighbor entries
  int ts_offset : 8;    ///< Byte offset to timestamp field (0 if immutable)
  int data_offset : 8;  ///< Byte offset to edge data field
};

struct NbrIterator {
  NbrIterator() = default;
  ~NbrIterator() = default;

  void init(const void* ptr, const void* end, NbrIterConfig cfg,
            timestamp_t timestamp) {
    cur = ptr;
    this->end = end;
    this->cfg = cfg;
    this->timestamp = timestamp;
    int stride = cfg.stride;
    while (cur != end && get_timestamp() > timestamp) {
      cur = static_cast<const char*>(cur) + stride;
    }
  }

  inline vid_t operator*() const { return get_vertex(); }

  __attribute__((always_inline)) NbrIterator& operator++() {
    cur = static_cast<const char*>(cur) + cfg.stride;
    while (cur != end && get_timestamp() > timestamp) {
      cur = static_cast<const char*>(cur) + cfg.stride;
    }
    return *this;
  }

  __attribute__((always_inline)) NbrIterator& operator+=(size_t n) {
    for (size_t i = 0; i < n; ++i) {
      ++(*this);
    }
    return *this;
  }

  __attribute__((always_inline)) bool operator==(const NbrIterator& rhs) const {
    return (cur == rhs.cur);
  }

  __attribute__((always_inline)) bool operator!=(const NbrIterator& rhs) const {
    return (cur != rhs.cur);
  }

  inline timestamp_t get_timestamp() const {
    return *(reinterpret_cast<const timestamp_t*>(
        static_cast<const char*>(cur) + cfg.ts_offset));
  }

  inline vid_t get_vertex() const {
    return *reinterpret_cast<const vid_t*>(cur);
  }

  inline const void* get_data_ptr() const {
    return static_cast<const char*>(cur) + cfg.data_offset;
  }

  inline const void* get_nbr_ptr() const { return cur; }

  inline const timestamp_t* get_timestamp_ptr() const {
    return cfg.ts_offset == 0
               ? nullptr
               : reinterpret_cast<const timestamp_t*>(
                     static_cast<const char*>(cur) + cfg.ts_offset);
  }

  const void* cur;
  const void* end;
  NbrIterConfig cfg;
  timestamp_t timestamp;
};

static_assert(std::is_pod<NbrIterator>::value, "NbrIterator should be POD");

struct NbrList {
  NbrList() = default;
  ~NbrList() = default;

  __attribute__((always_inline)) NbrIterator begin() const {
    NbrIterator it;
    it.init(start_ptr, end_ptr, cfg, timestamp);
    return it;
  }

  __attribute__((always_inline)) NbrIterator end() const {
    NbrIterator it;
    it.init(end_ptr, end_ptr, cfg, timestamp);
    return it;
  }

  bool empty() const { return start_ptr == end_ptr; }

  const void* start_ptr;
  const void* end_ptr;
  NbrIterConfig cfg;
  timestamp_t timestamp;
};

static_assert(std::is_pod<NbrList>::value, "NbrList should be POD");

// ============================================================================
// EdgeDataAccessor — migrated from generic_view.h
// ============================================================================

struct EdgeDataAccessor {
  EdgeDataAccessor() : data_type_(DataTypeId::kEmpty), data_column_(nullptr) {}

  EdgeDataAccessor(DataTypeId data_type, ColumnBase* data_column)
      : data_type_(data_type), data_column_(data_column) {}
  EdgeDataAccessor(const EdgeDataAccessor& other)
      : data_type_(other.data_type_), data_column_(other.data_column_) {}

  bool is_bundled() const { return data_column_ == nullptr; }

  inline Property get_data(const NbrIterator& it) const {
    return data_column_ == nullptr
               ? get_generic_bundled_data_from_ptr(it.get_data_ptr())
               : get_generic_column_data(
                     *reinterpret_cast<const size_t*>(it.get_data_ptr()));
  }

  template <typename T>
  inline T get_typed_data(const NbrIterator& it) const {
    if constexpr (std::is_same<T, EmptyType>::value) {
      return EmptyType();
    } else {
      return data_column_ == nullptr
                 ? get_bundled_data_from_ptr<T>(it.get_data_ptr())
                 : get_column_data<T>(
                       *reinterpret_cast<const size_t*>(it.get_data_ptr()));
    }
  }

  template <typename T>
  inline T get_typed_data_from_ptr(const void* data_ptr) const {
    if constexpr (std::is_same<T, EmptyType>::value) {
      return EmptyType();
    } else {
      auto ret =
          data_column_ == nullptr
              ? *reinterpret_cast<const T*>(data_ptr)
              : get_column_data<T>(*reinterpret_cast<const size_t*>(data_ptr));
      return ret;
    }
  }

  inline Property get_data_from_ptr(const void* data_ptr) const {
    return data_column_ == nullptr
               ? get_generic_bundled_data_from_ptr(data_ptr)
               : get_generic_column_data(
                     *reinterpret_cast<const size_t*>(data_ptr));
  }

  inline void set_data(const NbrIterator& it, const Property& prop,
                       timestamp_t ts) {
    if (it.cfg.ts_offset != 0) {
      *const_cast<timestamp_t*>(it.get_timestamp_ptr()) = ts;
    }
    if (data_column_ != nullptr) {
      size_t idx = get_bundled_data_from_ptr<size_t>(it.get_data_ptr());
      data_column_->set_prop(idx, prop);
    } else {
      if (data_type_ == DataTypeId::kEmpty) {
        return;
      }
      switch (data_type_) {
#define TYPE_DISPATCHER(enum_val, type)                              \
  case DataTypeId::enum_val: {                                       \
    *reinterpret_cast<type*>(const_cast<void*>(it.get_data_ptr())) = \
        PropUtils<type>::to_typed(prop);                             \
    break;                                                           \
  }
        FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
      default:
        THROW_RUNTIME_ERROR("Could not set bundled data for type " +
                            std::to_string(data_type_));
        break;
      }
    }
  }

 private:
  template <typename T>
  inline T get_bundled_data_from_ptr(const void* data_ptr) const {
    return *reinterpret_cast<const T*>(data_ptr);
  }

  template <typename T>
  inline T get_column_data(size_t idx) const {
    return reinterpret_cast<const TypedColumn<T>*>(data_column_)->get_view(idx);
  }

  inline Property get_generic_bundled_data_from_ptr(
      const void* data_ptr) const {
    if (data_type_ == DataTypeId::kEmpty) {
      return Property::empty();
    }
    switch (data_type_) {
#define TYPE_DISPATCHER(enum_val, type)             \
  case DataTypeId::enum_val: {                      \
    return PropUtils<type>::to_prop(                \
        get_bundled_data_from_ptr<type>(data_ptr)); \
  }
      FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
    default:
      THROW_RUNTIME_ERROR("Could not get bundled data for type " +
                          std::to_string(data_type_));
      return Property::empty();
    }
  }

  inline Property get_generic_column_data(size_t idx) const {
    return data_column_->get_prop(idx);
  }

  DataTypeId data_type_;
  ColumnBase* data_column_;
};

namespace impl {
// Type-erased typed write at @p dst. Dispatches on edge-data type id.
inline void write_typed_data(char* dst, const Property& data, DataTypeId type) {
  switch (type) {
  case DataTypeId::kEmpty:
    return;
#define WRITE_CASE(enum_val, cpp_t)                                    \
  case DataTypeId::enum_val: {                                         \
    *reinterpret_cast<cpp_t*>(dst) = PropUtils<cpp_t>::to_typed(data); \
    return;                                                            \
  }
    FOR_EACH_DATA_TYPE_NO_STRING(WRITE_CASE)
#undef WRITE_CASE
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported edge-data type for put_generic_edge: " +
        std::to_string(type));
  }
}
}  // namespace impl

// ============================================================================
// CsrViewType enum — migrated from generic_view.h
// ============================================================================

enum class CsrViewType {
  kSingleMutable,
  kMultipleMutable,
  kSingleImmutable,
  kMultipleImmutable,
};

// Forward declaration for typed views
template <typename T, CsrViewType TYPE>
struct TypedCsrView;

// ============================================================================
// CsrBaseView — replaces GenericView
// ============================================================================

/**
 * @brief Runtime-erased view of a CSR, fully wired at construction.
 *
 * Returned by CsrBase::get_generic_view(ts). No enable_insert, no
 * CsrInsertContext, no COW fork logic. Caller must ensure adj-list
 * exclusivity before writing via put_generic_edge.
 */
struct CsrBaseView {
  CsrBaseView()
      : csr_type_(CsrType::kEmpty),
        adjlists_(nullptr),
        degrees_(nullptr),
        cfg_({0, 0, 0}),
        timestamp_(0),
        unsorted_since_(0),
        edge_num_ptr_(nullptr),
        caps_(nullptr),
        locks_(nullptr),
        data_type_id_(DataTypeId::kEmpty) {}

  /// Multi-edge constructor (for MutableCsr / ImmutableCsr).
  /// Insert fields (edge_num_ptr, caps, locks) are nullptr for immutable CSRs.
  CsrBaseView(CsrType csr_type, const char* adjlists, const int* degrees,
              NbrIterConfig cfg, timestamp_t timestamp,
              timestamp_t unsorted_since, std::atomic<uint64_t>* edge_num_ptr,
              int* caps, SpinLock* locks, DataTypeId data_type_id)
      : csr_type_(csr_type),
        adjlists_(adjlists),
        degrees_(degrees),
        cfg_(cfg),
        timestamp_(timestamp),
        unsorted_since_(unsorted_since),
        edge_num_ptr_(edge_num_ptr),
        caps_(caps),
        locks_(locks),
        data_type_id_(data_type_id) {}

  /// Single-edge constructor (for SingleMutableCsr / SingleImmutableCsr).
  CsrBaseView(CsrType csr_type, const char* adjlists, NbrIterConfig cfg,
              timestamp_t timestamp, timestamp_t unsorted_since,
              std::atomic<uint64_t>* edge_num_ptr, DataTypeId data_type_id)
      : csr_type_(csr_type),
        adjlists_(adjlists),
        degrees_(nullptr),
        cfg_(cfg),
        timestamp_(timestamp),
        unsorted_since_(unsorted_since),
        edge_num_ptr_(edge_num_ptr),
        caps_(nullptr),
        locks_(nullptr),
        data_type_id_(data_type_id) {}

  CsrType csr_type() const { return csr_type_; }

  /// Derive view type from layout fields (same logic as old
  /// GenericView::type()).
  CsrViewType type() const {
    if (degrees_ == nullptr) {
      if (cfg_.ts_offset != 0) {
        return CsrViewType::kSingleMutable;
      } else {
        return CsrViewType::kSingleImmutable;
      }
    } else {
      if (cfg_.ts_offset != 0) {
        return CsrViewType::kMultipleMutable;
      } else {
        return CsrViewType::kMultipleImmutable;
      }
    }
  }

  __attribute__((always_inline)) NbrList get_edges(vid_t v) const {
    NbrList ret;
    if (degrees_ == nullptr) {
      const char* start_ptr = adjlists_ + v * cfg_.stride;
      ret.start_ptr = start_ptr;
      ret.end_ptr = start_ptr + cfg_.stride;
    } else {
      const char* start_ptr = reinterpret_cast<const char*>(
          reinterpret_cast<const int64_t*>(adjlists_)[v]);
      if (start_ptr == nullptr) {
        ret.start_ptr = nullptr;
        ret.end_ptr = nullptr;
      } else {
        ret.start_ptr = start_ptr;
        ret.end_ptr = start_ptr + degrees_[v] * cfg_.stride;
      }
    }
    ret.cfg = cfg_;
    ret.timestamp = timestamp_;
    return ret;
  }

  /**
   * @brief Single-point insert. No COW fork — caller ensures exclusivity.
   *
   * Multi-mutable: lock -> grow-if-full -> append -> unlock.
   * Single-mutable: direct slot write.
   * Immutable: LOG(FATAL).
   */
  int32_t put_generic_edge(vid_t src, vid_t dst, const Property& data,
                           timestamp_t ts, Allocator& alloc) {
    if (edge_num_ptr_ == nullptr) {
      LOG(FATAL) << "CsrBaseView is not writable (immutable CSR)";
      return -1;
    }

    // Single-mutable CSR: flat nbr array, fixed-slot write.
    if (degrees_ == nullptr) {
      char* slot = const_cast<char*>(adjlists_) + src * cfg_.stride;
      *reinterpret_cast<vid_t*>(slot) = dst;
      impl::write_typed_data(slot + cfg_.data_offset, data, data_type_id_);
      if (cfg_.ts_offset != 0) {
        reinterpret_cast<std::atomic<timestamp_t>*>(slot + cfg_.ts_offset)
            ->store(ts);
      }
      edge_num_ptr_->fetch_add(1, std::memory_order_relaxed);
      return 0;
    }

    // Multi-mutable CSR: lock -> grow-if-full -> append -> unlock.
    // No COW fork
    auto** buffers = reinterpret_cast<char**>(const_cast<char*>(adjlists_));
    auto* sizes = const_cast<int*>(degrees_);
    locks_[src].lock();

    int sz = sizes[src];
    int cap = caps_[src];
    if (sz == cap) {
      cap += (cap >> 1);
      cap = std::max(cap, 8);
      void* new_buf = alloc.allocate(cap * cfg_.stride);
      if (sz > 0) {
        std::memcpy(new_buf, buffers[src], sz * cfg_.stride);
      }
      buffers[src] = static_cast<char*>(new_buf);
      caps_[src] = cap;
    }

    int32_t prev_size = sizes[src]++;
    char* slot = buffers[src] + prev_size * cfg_.stride;
    *reinterpret_cast<vid_t*>(slot) = dst;
    impl::write_typed_data(slot + cfg_.data_offset, data, data_type_id_);
    reinterpret_cast<std::atomic<timestamp_t>*>(slot + cfg_.ts_offset)
        ->store(ts);
    edge_num_ptr_->fetch_add(1);

    locks_[src].unlock();
    return prev_size;
  }

  /// Same template interface as old GenericView::get_typed_view<T, TYPE>().
  template <typename T, CsrViewType TYPE>
  TypedCsrView<T, TYPE> get_typed_view() const;

 private:
  CsrType csr_type_;
  const char* adjlists_;
  const int* degrees_;
  NbrIterConfig cfg_;
  timestamp_t timestamp_;
  timestamp_t unsorted_since_;
  // Insert fields (nullptr for immutable CSRs)
  std::atomic<uint64_t>* edge_num_ptr_;
  int* caps_;
  SpinLock* locks_;
  DataTypeId data_type_id_;
};

// ============================================================================
// Typed CSR Views — replace TypedView<T, CsrViewType>
// ============================================================================

/// Primary template (empty, for unsupported combinations).
template <typename T, CsrViewType TYPE>
struct TypedCsrView {
  TypedCsrView() = default;
  ~TypedCsrView() = default;
};

/// Multi-edge mutable typed view. Has put_edge, foreach_nbr_gt/lt.
template <typename T>
struct TypedCsrView<T, CsrViewType::kMultipleMutable> {
  using nbr_t = MutableNbr<T>;

  TypedCsrView() = default;
  ~TypedCsrView() = default;

  TypedCsrView(const MutableNbr<T>** adjlists, const int* degrees,
               timestamp_t timestamp, timestamp_t unsorted_since,
               std::atomic<uint64_t>* edge_num_ptr, int* caps, SpinLock* locks)
      : adjlists_(adjlists),
        degrees_(degrees),
        timestamp_(timestamp),
        unsorted_since_(unsorted_since),
        edge_num_ptr_(edge_num_ptr),
        caps_(caps),
        locks_(locks) {}

  /// Typed put_edge. No COW fork — caller ensures exclusivity.
  int32_t put_edge(vid_t src, vid_t dst, const T& data, timestamp_t ts,
                   Allocator& alloc) {
    auto** buffers =
        reinterpret_cast<nbr_t**>(const_cast<MutableNbr<T>**>(adjlists_));
    auto* sizes = const_cast<int*>(degrees_);
    locks_[src].lock();
    int sz = sizes[src];
    int cap = caps_[src];
    if (sz == cap) {
      cap += (cap >> 1);
      cap = std::max(cap, 8);
      void* new_buf = alloc.allocate(cap * sizeof(nbr_t));
      if (sz > 0) {
        std::memcpy(new_buf, buffers[src], sz * sizeof(nbr_t));
      }
      buffers[src] = static_cast<nbr_t*>(new_buf);
      caps_[src] = cap;
    }
    int32_t prev_size = sizes[src]++;
    auto& nbr = buffers[src][prev_size];
    nbr.neighbor = dst;
    nbr.data = data;
    nbr.timestamp.store(ts);
    edge_num_ptr_->fetch_add(1);
    locks_[src].unlock();
    return prev_size;
  }

  template <typename FUNC_T>
  void foreach_nbr_gt(vid_t v, const T& threshold, const FUNC_T& func) const {
    const nbr_t* ptr = adjlists_[v] + degrees_[v] - 1;
    const nbr_t* end = adjlists_[v] - 1;
    while (ptr != end) {
      if (ptr->timestamp > timestamp_) {
        --ptr;
        continue;
      }
      if (ptr->timestamp < unsorted_since_) {
        break;
      }
      if (threshold < ptr->data) {
        func(ptr->neighbor, ptr->data);
      }
      --ptr;
    }
    while (ptr != end) {
      if (threshold < ptr->data) {
        func(ptr->neighbor, ptr->data);
      } else {
        break;
      }
      --ptr;
    }
  }

  template <typename FUNC_T>
  void foreach_nbr_lt(vid_t v, const T& threshold, const FUNC_T& func) const {
    const nbr_t* ptr = adjlists_[v] + degrees_[v] - 1;
    const nbr_t* end = adjlists_[v] - 1;
    while (ptr != end) {
      if (ptr->timestamp > timestamp_) {
        --ptr;
        continue;
      }
      if (ptr->timestamp < unsorted_since_) {
        break;
      }
      if (threshold > ptr->data) {
        func(ptr->neighbor, ptr->data);
      }
      --ptr;
    }
    if (ptr == end) {
      return;
    }
    ptr = std::lower_bound(
              adjlists_[v], ptr + 1, threshold,
              [](const nbr_t& b, const T& a) { return b.data < a; }) -
          1;
    while (ptr != end) {
      func(ptr->neighbor, ptr->data);
      --ptr;
    }
  }

 private:
  const MutableNbr<T>** adjlists_{nullptr};
  const int* degrees_{nullptr};
  timestamp_t timestamp_{0};
  timestamp_t unsorted_since_{0};
  std::atomic<uint64_t>* edge_num_ptr_{nullptr};
  int* caps_{nullptr};
  SpinLock* locks_{nullptr};
};

/// Single-edge mutable typed view (minimal).
template <typename T>
struct TypedCsrView<T, CsrViewType::kSingleMutable> {
  TypedCsrView() = default;
  ~TypedCsrView() = default;
};

/// Multi-edge immutable typed view (minimal).
template <typename T>
struct TypedCsrView<T, CsrViewType::kMultipleImmutable> {
  TypedCsrView() = default;
  ~TypedCsrView() = default;
};

/// Single-edge immutable typed view (minimal).
template <typename T>
struct TypedCsrView<T, CsrViewType::kSingleImmutable> {
  TypedCsrView() = default;
  ~TypedCsrView() = default;
};

// Convenience aliases
template <typename T>
using MutableCsrView = TypedCsrView<T, CsrViewType::kMultipleMutable>;

template <typename T>
using SingleMutableCsrView = TypedCsrView<T, CsrViewType::kSingleMutable>;

template <typename T>
using ImmutableCsrView = TypedCsrView<T, CsrViewType::kMultipleImmutable>;

template <typename T>
using SingleImmutableCsrView = TypedCsrView<T, CsrViewType::kSingleImmutable>;

// ============================================================================
// CsrBaseView::get_typed_view implementation
// ============================================================================

template <typename T, CsrViewType TYPE>
TypedCsrView<T, TYPE> CsrBaseView::get_typed_view() const {
  if constexpr (TYPE == CsrViewType::kMultipleMutable) {
    assert(cfg_.ts_offset != 0);
    assert(cfg_.stride == sizeof(MutableNbr<T>));
    int64_t val = reinterpret_cast<int64_t>(adjlists_);
    const MutableNbr<T>** lists = reinterpret_cast<const MutableNbr<T>**>(val);
    return TypedCsrView<T, CsrViewType::kMultipleMutable>(
        lists, degrees_, timestamp_, unsorted_since_, edge_num_ptr_, caps_,
        locks_);
  } else {
    LOG(FATAL) << "get_typed_view not implemented for this CsrViewType";
    return TypedCsrView<T, TYPE>();
  }
}

}  // namespace neug
