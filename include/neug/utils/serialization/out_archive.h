/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#pragma once

#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include "neug/utils/exception/exception.h"

#include "neug/utils/property/types.h"

namespace neug {

class OutArchive {
 public:
  OutArchive() : begin_(NULL), end_(NULL) {}
  explicit OutArchive(size_t size)
      : buffer_(size), begin_(buffer_.data()), end_(begin_ + size) {}
  OutArchive(OutArchive&& oa) {
    buffer_ = std::move(oa.buffer_);
    begin_ = oa.begin_;
    end_ = oa.end_;

    oa.begin_ = NULL;
    oa.end_ = NULL;
  }

  ~OutArchive() {}

  OutArchive& operator=(OutArchive&& oa) {
    buffer_ = std::move(oa.buffer_);
    begin_ = oa.begin_;
    end_ = oa.end_;

    oa.begin_ = NULL;
    oa.end_ = NULL;
    return *this;
  }

  inline void Clear() {
    buffer_.clear();
    begin_ = NULL;
    end_ = NULL;
  }

  inline void Allocate(size_t size) {
    buffer_.resize(size);
    begin_ = buffer_.data();
    end_ = begin_ + static_cast<ptrdiff_t>(size);
  }

  inline void Rewind() { begin_ = buffer_.data(); }

  inline void SetSlice(char* buffer, size_t size) {
    buffer_.clear();
    begin_ = buffer;
    if (!buffer && size)
      THROW_INVALID_ARGUMENT_EXCEPTION("Null archive slice");
    end_ = size ? begin_ + size : begin_;
  }

  inline void SetSlice(const char* buffer, size_t size) {
    SetSlice(const_cast<char*>(buffer), size);
  }

  inline char* GetBuffer() { return begin_; }

  inline const char* GetBuffer() const { return begin_; }

  inline size_t GetSize() const {
    return begin_ ? static_cast<size_t>(end_ - begin_) : 0;
  }

  inline bool Empty() const { return begin_ == end_; }

  inline void* GetBytes(size_t size) {
    RequireBytes(size);
    char* ret = begin_;
    if (size)
      begin_ += size;
    return ret;
  }

  template <typename T>
  inline void Peek(T& value) {
    char* old_begin = begin_;
    *this >> value;
    begin_ = old_begin;
  }

  void RequireBytes(size_t size) const {
    if (size > GetSize())
      THROW_IO_EXCEPTION("Truncated archive payload");
  }
  void RequireCount(size_t count, size_t minimum_bytes = 1) const {
    if (minimum_bytes == 0 || count > GetSize() / minimum_bytes)
      THROW_IO_EXCEPTION("Invalid archive container count");
  }

 private:
  std::vector<char> buffer_;
  char* begin_;
  char* end_;
};

template <typename T,
          typename std::enable_if<std::is_pod<T>::value, T>::type* = nullptr>
inline OutArchive& operator>>(OutArchive& out_archive, T& u) {
  if constexpr (std::is_same_v<T, bool>) {
    unsigned char byte;
    std::memcpy(&byte, out_archive.GetBytes(1), 1);
    if (byte > 1)
      THROW_IO_EXCEPTION("Invalid archive boolean");
    u = byte != 0;
  } else {
    std::memcpy(&u, out_archive.GetBytes(sizeof(T)), sizeof(T));
  }
  return out_archive;
}

inline OutArchive& operator>>(OutArchive& out_archive, EmptyType&) {
  return out_archive;
}

inline OutArchive& operator>>(OutArchive& out_archive, std::string& s) {
  size_t size;
  out_archive >> size;
  out_archive.RequireBytes(size);
  s.resize(size);
  if (size)
    memcpy(s.data(), out_archive.GetBytes(size), size);
  return out_archive;
}

inline OutArchive& operator>>(OutArchive& out_archive, std::string_view& s) {
  size_t size;
  out_archive >> size;
  const char* data = reinterpret_cast<const char*>(out_archive.GetBytes(size));
  s = std::string_view(data, size);
  return out_archive;
}

template <typename T,
          typename std::enable_if<std::is_pod<T>::value, T>::type* = nullptr>
inline OutArchive& operator>>(OutArchive& out_archive, std::vector<T>& vec) {
  size_t size;
  out_archive >> size;
  out_archive.RequireCount(size, sizeof(T));
  vec.resize(size);
  if (size > 0) {
    if constexpr (std::is_same_v<T, bool>) {
      // Special handling for vector<bool>
      for (size_t i = 0; i < size; ++i) {
        bool val;
        out_archive >> val;
        vec[i] = val;
      }
      return out_archive;
    } else {
      memcpy(&vec[0], out_archive.GetBytes(sizeof(T) * size), sizeof(T) * size);
    }
  }
  return out_archive;
}

template <typename T,
          typename std::enable_if<!std::is_pod<T>::value, T>::type* = nullptr>
inline OutArchive& operator>>(OutArchive& out_archive, std::vector<T>& vec) {
  size_t size;
  out_archive >> size;
  out_archive.RequireCount(size);
  // Variable-size elements can be much larger in memory than their shortest
  // serialized form. Decode incrementally so a malformed count cannot reserve
  // the whole container before its first element is checked.
  std::vector<T> decoded;
  for (size_t i = 0; i < size; ++i) {
    T value;
    out_archive >> value;
    decoded.push_back(std::move(value));
  }
  vec = std::move(decoded);
  return out_archive;
}

template <typename... Args>
inline OutArchive& operator>>(OutArchive& out_archive,
                              std::tuple<Args...>& tup) {
  std::apply([&out_archive](Args&... args) { (out_archive >> ... >> args); },
             tup);
  return out_archive;
}

}  // namespace neug
