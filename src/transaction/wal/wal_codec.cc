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

#include "neug/transaction/wal/wal_codec.h"
#include <bit>
#include <cstring>
#include "absl/crc/crc32c.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"
namespace neug {
namespace {
static_assert(sizeof(size_t) == 8 && sizeof(int) == 4 && sizeof(bool) == 1,
              "WAL payload ABI 1 requires a 64-bit ABI");
static_assert(std::endian::native == std::endian::little,
              "WAL payload ABI 1 requires little endian");
void Put(uint8_t* p, uint64_t v, size_t n) {
  for (size_t i = 0; i < n; ++i)
    p[i] = static_cast<uint8_t>(v >> (8 * i));
}
uint64_t Get(const uint8_t* p, size_t n) {
  uint64_t v = 0;
  for (size_t i = 0; i < n; ++i)
    v |= uint64_t{p[i]} << (8 * i);
  return v;
}
uint32_t Crc(const void* data, size_t n) {
  return static_cast<uint32_t>(absl::ComputeCrc32c(
      absl::string_view(static_cast<const char*>(data), n)));
}
uint32_t FrameCrc(const uint8_t* header, const char* payload, size_t n) {
  auto crc = absl::crc32c_t{Crc(header, 9)};
  if (n)
    crc = absl::ExtendCrc32c(crc, absl::string_view(payload, n));
  return static_cast<uint32_t>(crc);
}
}  // namespace
std::string WalRecoveryErrorKindName(WalRecoveryErrorKind k) {
  switch (k) {
  case WalRecoveryErrorKind::kIoError:
    return "io_error";
  case WalRecoveryErrorKind::kUnsupportedFormat:
    return "unsupported_format";
  case WalRecoveryErrorKind::kCorruptedFrame:
    return "corrupted_frame";
  case WalRecoveryErrorKind::kDuplicateTimestamp:
    return "duplicate_timestamp";
  case WalRecoveryErrorKind::kUnknownRecordKind:
    return "unknown_record_kind";
  }
  return "corrupted_frame";
}
std::array<uint8_t, kWalFileHeaderSize> EncodeWalFileHeader(uint64_t id) {
  std::array<uint8_t, kWalFileHeaderSize> b{};
  std::memcpy(b.data(), "NEUGWAL", 8);
  Put(b.data() + 8, kWalFormatVersion, 2);
  Put(b.data() + 10, kWalPayloadAbi, 2);
  Put(b.data() + 12, id, 8);
  Put(b.data() + 20, Crc(b.data(), 20), 4);
  return b;
}
void ValidateWalFileHeader(const uint8_t* b, size_t n, uint64_t id) {
  if (n < kWalFileHeaderSize || std::memcmp(b, "NEUGWAL", 8) != 0 ||
      Get(b + 8, 2) != kWalFormatVersion || Get(b + 10, 2) != kWalPayloadAbi)
    throw exception::WalRecoveryException(
        WalRecoveryErrorKind::kUnsupportedFormat,
        "unsupported WAL format/ABI; recover and checkpoint with the old "
        "binary, close it, then upgrade");
  if (Get(b + 20, 4) != Crc(b, 20) || Get(b + 12, 8) != id)
    throw exception::WalRecoveryException(
        WalRecoveryErrorKind::kCorruptedFrame,
        "file header checksum or checkpoint epoch mismatch");
}
void ValidateWalFrameArguments(uint32_t ts, WalRecordKind k,
                               const char* payload, size_t n) {
  if (ts == 0 || ts >= MAX_TIMESTAMP || n > kWalMaxPayloadLength ||
      (n && !payload) || static_cast<uint8_t>(k) > 2 ||
      ((k == WalRecordKind::kCompact) != (n == 0)))
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Invalid WAL frame timestamp, kind, payload or length");
}
std::array<uint8_t, kWalFrameHeaderSize> EncodeWalFrameHeader(uint32_t ts,
                                                              WalRecordKind k,
                                                              const char* p,
                                                              size_t n) {
  ValidateWalFrameArguments(ts, k, p, n);
  std::array<uint8_t, kWalFrameHeaderSize> b{};
  b[0] = static_cast<uint8_t>(k);
  Put(b.data() + 1, n, 4);
  Put(b.data() + 5, ts, 4);
  Put(b.data() + 9, Crc(b.data(), 9), 4);
  Put(b.data() + 13, FrameCrc(b.data(), p, n), 4);
  return b;
}
WalFrameHeader DecodeWalFrameHeader(const uint8_t* b, size_t n) {
  if (n < kWalFrameHeaderSize || Get(b + 9, 4) != Crc(b, 9))
    throw exception::WalRecoveryException(WalRecoveryErrorKind::kCorruptedFrame,
                                          "frame header checksum mismatch");
  if (b[0] > 2)
    throw exception::WalRecoveryException(
        WalRecoveryErrorKind::kUnknownRecordKind, "unknown record kind");
  WalFrameHeader f{static_cast<WalRecordKind>(b[0]),
                   static_cast<uint32_t>(Get(b + 1, 4)),
                   static_cast<uint32_t>(Get(b + 5, 4)),
                   static_cast<uint32_t>(Get(b + 13, 4))};
  if (f.timestamp == 0 || f.timestamp >= MAX_TIMESTAMP ||
      f.payload_length > kWalMaxPayloadLength ||
      ((f.kind == WalRecordKind::kCompact) != (f.payload_length == 0)))
    throw exception::WalRecoveryException(
        WalRecoveryErrorKind::kCorruptedFrame,
        "invalid frame timestamp or payload length");
  return f;
}
void ValidateWalFrame(const uint8_t* b, const char* p,
                      const WalFrameHeader& f) {
  if (f.checksum != FrameCrc(b, p, f.payload_length))
    throw exception::WalRecoveryException(WalRecoveryErrorKind::kCorruptedFrame,
                                          "frame checksum mismatch");
}
}  // namespace neug
