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
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include "neug/utils/api.h"
namespace neug {
constexpr size_t kWalFileHeaderSize = 24;
constexpr size_t kWalFrameHeaderSize = 17;
constexpr uint16_t kWalFormatVersion = 1;
constexpr uint16_t kWalPayloadAbi = 1;
constexpr size_t kWalMaxPayloadLength = (size_t{1} << 30) - 1;
enum class WalRecordKind : uint8_t { kInsert = 0, kCowRedo = 1, kCompact = 2 };
enum class WalRecoveryErrorKind {
  kIoError,
  kUnsupportedFormat,
  kCorruptedFrame,
  kDuplicateTimestamp,
  kUnknownRecordKind
};
struct WalFrameHeader {
  WalRecordKind kind;
  uint32_t payload_length;
  uint32_t timestamp;
  uint32_t checksum;
};
NEUG_API std::array<uint8_t, kWalFileHeaderSize> EncodeWalFileHeader(
    uint64_t checkpoint_id);
NEUG_API void ValidateWalFileHeader(const uint8_t* data, size_t size,
                                    uint64_t checkpoint_id);
NEUG_API void ValidateWalFrameArguments(uint32_t timestamp, WalRecordKind kind,
                                        const char* payload, size_t length);
NEUG_API std::array<uint8_t, kWalFrameHeaderSize> EncodeWalFrameHeader(
    uint32_t timestamp, WalRecordKind kind, const char* payload, size_t length);
NEUG_API WalFrameHeader DecodeWalFrameHeader(const uint8_t* data, size_t size);
NEUG_API void ValidateWalFrame(const uint8_t* header, const char* payload,
                               const WalFrameHeader& frame);
NEUG_API std::string WalRecoveryErrorKindName(WalRecoveryErrorKind kind);
}  // namespace neug
