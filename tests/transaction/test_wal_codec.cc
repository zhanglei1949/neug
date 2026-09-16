/** Copyright 2026 Alibaba Group Holding Limited.
 * Licensed under the Apache License, Version 2.0.
 */
#include <fcntl.h>
#ifdef _WIN32
#include <io.h>
#include <process.h>
#else
#include <sys/wait.h>
#include <unistd.h>
#endif
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <vector>
#include "gtest/gtest.h"
#include "neug/common/types/value.h"
#include "neug/transaction/wal/local_wal_parser.h"
#include "neug/transaction/wal/local_wal_writer.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"
namespace neug {
class WalWriterTestPeer {
 public:
  static void Write(LocalWalWriter& w,
                    std::function<ptrdiff_t(int, const void*, size_t)> f) {
    w.write_hook_ = std::move(f);
  }
  static void Sync(LocalWalWriter& w, std::function<int(int)> f) {
    w.sync_hook_ = std::move(f);
  }
  static void DirectorySync(LocalWalWriter& w,
                            std::function<bool(const std::string&)> f) {
    w.directory_sync_hook_ = std::move(f);
  }
};
namespace {
using Bytes = std::vector<uint8_t>;
#ifdef _WIN32
int ProcessId() { return ::_getpid(); }
ptrdiff_t WriteFd(int fd, const void* data, size_t size) {
  return ::_write(fd, data, static_cast<unsigned int>(size));
}
#else
int ProcessId() { return ::getpid(); }
ptrdiff_t WriteFd(int fd, const void* data, size_t size) {
  return ::write(fd, data, size);
}
#endif
// Bitwise reference implementation keeps malformed-header fixtures independent
// of the production framing encoder.
void RefreshHeaderCrc(Bytes& bytes) {
  uint32_t crc = ~uint32_t{0};
  for (size_t i = kWalFileHeaderSize; i < kWalFileHeaderSize + 9; ++i) {
    crc ^= bytes[i];
    for (int bit = 0; bit < 8; ++bit)
      crc = (crc >> 1) ^ ((crc & 1) ? 0x82f63b78u : 0);
  }
  crc = ~crc;
  for (size_t i = 0; i < 4; ++i)
    bytes[kWalFileHeaderSize + 9 + i] = static_cast<uint8_t>(crc >> (8 * i));
}
Bytes Frame(uint32_t ts, WalRecordKind k = WalRecordKind::kInsert,
            const std::string& p = "abc") {
  const auto h = EncodeWalFrameHeader(ts, k, p.data(), p.size());
  Bytes b(h.begin(), h.end());
  b.insert(b.end(), p.begin(), p.end());
  return b;
}
Bytes File(const std::vector<Bytes>& frames, uint64_t id = 7) {
  auto h = EncodeWalFileHeader(id);
  Bytes b(h.begin(), h.end());
  for (const auto& f : frames)
    b.insert(b.end(), f.begin(), f.end());
  return b;
}
class WalProtocolTest : public ::testing::Test {
 protected:
  std::string dir;
  void SetUp() override {
    const auto* t = ::testing::UnitTest::GetInstance()->current_test_info();
    dir = (std::filesystem::temp_directory_path() /
           ("neug_protocol_" + std::to_string(ProcessId()) + "_" + t->name()))
              .string();
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
  }
  void TearDown() override { std::filesystem::remove_all(dir); }
  void Put(const Bytes& b, const std::string& name = "a.wal") {
    std::ofstream o(dir + "/" + name, std::ios::binary);
    o.write(reinterpret_cast<const char*>(b.data()), b.size());
    ASSERT_TRUE(o.good());
  }
  void Error(WalRecoveryErrorKind kind) {
    try {
      LocalWalParser p(dir, 7);
      FAIL() << "Accepted invalid WAL";
    } catch (const exception::WalRecoveryException& e) {
      EXPECT_EQ(e.kind(), kind);
      EXPECT_NE(std::string(e.what()).find(dir), std::string::npos);
    }
  }
};
TEST_F(WalProtocolTest, GoldenBytesAndAllKinds) {
  const std::array<uint8_t, 24> expected_file = {
      78, 69, 85, 71, 87, 65, 76, 0, 1,   0,   1,  0,
      7,  0,  0,  0,  0,  0,  0,  0, 112, 199, 51, 39};
  const std::array<uint8_t, 17> expected_frame = {
      0, 1, 0, 0, 0, 1, 0, 0, 0, 60, 191, 156, 47, 100, 45, 56, 251};
  EXPECT_EQ(EncodeWalFileHeader(7), expected_file);
  EXPECT_EQ(EncodeWalFrameHeader(1, WalRecordKind::kInsert, "\7", 1),
            expected_frame);
  Put(File({Frame(1), Frame(2, WalRecordKind::kCowRedo),
            Frame(3, WalRecordKind::kCompact, "")}));
  LocalWalParser p(dir, 7);
  ASSERT_EQ(p.replay_units().size(), 3);
  EXPECT_EQ(p.last_ts(), 3);
  EXPECT_EQ(p.replay_units()[2].kind, WalRecordKind::kCompact);
}
TEST_F(WalProtocolTest, EveryIncompleteTailPreservesPrefixAndReopens) {
  const auto next = Frame(9);
  for (size_t cut = 0; cut < next.size(); ++cut) {
    auto b = File({Frame(1)});
    b.insert(b.end(), next.begin(), next.begin() + cut);
    Put(b);
    LocalWalParser p(dir, 7);
    ASSERT_EQ(p.replay_units().size(), 1) << cut;
    EXPECT_EQ(p.last_ts(), 1);
    p.close();
    p.open(dir, 7);
    ASSERT_EQ(p.replay_units().size(), 1);
  }
  LocalWalWriter w(dir, 0);
  w.open(dir, 7);
  ASSERT_TRUE(w.append_frame(2, WalRecordKind::kInsert, "def", 3));
  w.close();
  LocalWalParser p(dir, 7);
  EXPECT_EQ(p.replay_units().size(), 2);
}
TEST_F(WalProtocolTest, IncompleteFileHeaderOnlyAcceptsExpectedPrefix) {
  const auto h = EncodeWalFileHeader(7);
  for (size_t cut = 0; cut < h.size(); ++cut) {
    Put(Bytes(h.begin(), h.begin() + cut));
    LocalWalParser p(dir, 7);
    EXPECT_TRUE(p.replay_units().empty());
  }
  Put(Bytes{1, 2});
  Error(WalRecoveryErrorKind::kUnsupportedFormat);
}
TEST_F(WalProtocolTest, LengthTamperingCannotHideCommittedFrameAsTail) {
  auto b = File({Frame(1)});
  b[kWalFileHeaderSize + 1] = 255;
  Put(b);
  Error(WalRecoveryErrorKind::kCorruptedFrame);
}
TEST_F(WalProtocolTest, EveryFileHeaderByteIsProtected) {
  const auto original = File({Frame(1)});
  for (size_t i = 0; i < kWalFileHeaderSize; ++i) {
    auto b = original;
    b[i] ^= 0x40;
    Put(b);
    EXPECT_THROW(LocalWalParser(dir, 7), exception::WalRecoveryException) << i;
  }
}
TEST_F(WalProtocolTest, EveryFullFrameByteIsProtected) {
  const auto original = File({Frame(1)});
  for (size_t i = kWalFileHeaderSize; i < original.size(); ++i) {
    auto b = original;
    b[i] ^= 0x40;
    Put(b);
    Error(WalRecoveryErrorKind::kCorruptedFrame);
  }
}
TEST_F(WalProtocolTest, UnknownFormatsAndEpochMismatchAreHardErrors) {
  auto b = File({Frame(1)});
  b[8] = 99;
  Put(b);
  Error(WalRecoveryErrorKind::kUnsupportedFormat);
  b = File({Frame(1)});
  b[10] = 99;
  Put(b);
  Error(WalRecoveryErrorKind::kUnsupportedFormat);
  Put(File({Frame(1)}, 8));
  Error(WalRecoveryErrorKind::kCorruptedFrame);
  Put(Bytes{0x4e, 0x45, 0x55, 0x57, 1, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0});
  Error(WalRecoveryErrorKind::kUnsupportedFormat);
}
TEST_F(WalProtocolTest, ValidChecksumDoesNotPermitInvalidHeaderFields) {
  auto b = File({Frame(1)});
  b[kWalFileHeaderSize] = 3;
  RefreshHeaderCrc(b);
  Put(b);
  Error(WalRecoveryErrorKind::kUnknownRecordKind);
  b = File({Frame(1)});
  b[kWalFileHeaderSize + 5] = 0;
  RefreshHeaderCrc(b);
  Put(b);
  Error(WalRecoveryErrorKind::kCorruptedFrame);
  b = File({Frame(1)});
  b[kWalFileHeaderSize + 4] = 0x40;
  RefreshHeaderCrc(b);
  Put(b);
  Error(WalRecoveryErrorKind::kCorruptedFrame);
  b = File({Frame(1)});
  b[kWalFileHeaderSize] = static_cast<uint8_t>(WalRecordKind::kCompact);
  RefreshHeaderCrc(b);
  Put(b);
  Error(WalRecoveryErrorKind::kCorruptedFrame);
}
TEST_F(WalProtocolTest, InvalidArgumentsFailBeforeFileCreation) {
  LocalWalWriter w(dir, 0);
  w.open(dir, 7);
  EXPECT_THROW(w.append_frame(0, WalRecordKind::kInsert, "a", 1),
               exception::Exception);
  EXPECT_THROW(
      w.append_frame(INVALID_TIMESTAMP, WalRecordKind::kInsert, "a", 1),
      exception::Exception);
  EXPECT_THROW(w.append_frame(1, WalRecordKind::kInsert, nullptr, 0),
               exception::Exception);
  EXPECT_THROW(w.append_frame(1, WalRecordKind::kCompact, "a", 1),
               exception::Exception);
  EXPECT_THROW(w.append_frame(1, static_cast<WalRecordKind>(9), "a", 1),
               exception::Exception);
  EXPECT_THROW(
      w.append_frame(1, WalRecordKind::kInsert, "a", kWalMaxPayloadLength + 1),
      exception::Exception);
  EXPECT_TRUE(std::filesystem::is_empty(dir));
  w.close();
  EXPECT_FALSE(w.append_frame(1, WalRecordKind::kInsert, "a", 1));
}
TEST_F(WalProtocolTest, CrossWriterOrderingGapsAndDuplicateKinds) {
  Put(File({Frame(7), Frame(12, WalRecordKind::kCompact, "")}), "z.wal");
  Put(File({Frame(2), Frame(9, WalRecordKind::kCowRedo)}));
  {
    LocalWalParser p(dir, 7);
    ASSERT_EQ(p.replay_units().size(), 4);
    EXPECT_EQ(p.replay_units()[0].timestamp, 2);
    EXPECT_EQ(p.last_ts(), 12);
  }
  for (const auto kind : {WalRecordKind::kInsert, WalRecordKind::kCowRedo,
                          WalRecordKind::kCompact}) {
    Put(File({Frame(7, kind, kind == WalRecordKind::kCompact ? "" : "x")}));
    Error(WalRecoveryErrorKind::kDuplicateTimestamp);
  }
}
TEST_F(WalProtocolTest, FailedParserOpenClearsViewsAndMappings) {
  Put(File({Frame(1)}));
  LocalWalParser p(dir, 7);
  ASSERT_EQ(p.replay_units().size(), 1);
  Put(File({Frame(1)}), "b.wal");
  EXPECT_THROW(p.open(dir, 7), exception::WalRecoveryException);
  EXPECT_TRUE(p.replay_units().empty());
  EXPECT_EQ(p.last_ts(), 0);
  std::filesystem::remove(dir + "/b.wal");
  p.open(dir, 7);
  EXPECT_EQ(p.replay_units().size(), 1);
}
TEST_F(WalProtocolTest, AbsentDirectoryAndLegacyEmptyFiles) {
  LocalWalParser p(dir + "/absent", 7);
  EXPECT_FALSE(std::filesystem::exists(dir + "/absent"));
  Put(Bytes(128, 0));
  EXPECT_NO_THROW(ValidateLegacyWalEpochEmpty(dir));
  p.open(dir, 7);
  EXPECT_TRUE(p.replay_units().empty());
  Put(Bytes(0));
  EXPECT_NO_THROW(ValidateLegacyWalEpochEmpty(dir));
  Put(Bytes(128, 0));
  std::fstream o(dir + "/a.wal",
                 std::ios::in | std::ios::out | std::ios::binary);
  o.seekp(100);
  o.put(1);
  o.close();
  EXPECT_THROW(ValidateLegacyWalEpochEmpty(dir),
               exception::WalRecoveryException);
}
TEST_F(WalProtocolTest, ShortWritesAndEintrCompleteWithoutPadding) {
  LocalWalWriter w(dir, 0);
  w.open(dir, 7);
  size_t calls = 0;
  WalWriterTestPeer::Write(w,
                           [&](int fd, const void* p, size_t n) -> ptrdiff_t {
                             if (calls++ % 4 == 0) {
                               errno = EINTR;
                               return -1;
                             }
                             return WriteFd(fd, p, std::min(n, size_t{2}));
                           });
  ASSERT_TRUE(w.append_frame(1, WalRecordKind::kInsert, "abc", 3));
  w.close();
  EXPECT_EQ(std::filesystem::file_size(dir + "/thread_0_0.wal"), 24 + 17 + 3);
  LocalWalParser p(dir, 7);
  EXPECT_EQ(p.replay_units()[0].payload, "abc");
}
TEST_F(WalProtocolTest, ZeroProgressWriteFailsStop) {
  EXPECT_DEATH(
      {
        LocalWalWriter w(dir, 0);
        w.open(dir, 7);
        WalWriterTestPeer::Write(
            w, [](int, const void*, size_t) -> ptrdiff_t { return 0; });
        w.append_frame(1, WalRecordKind::kInsert, "a", 1);
      },
      "durability unknown");
}
TEST_F(WalProtocolTest, InterruptedSyncsAreRetried) {
  LocalWalWriter w(dir, 0);
  w.open(dir, 7);
  int file_syncs = 0;
  WalWriterTestPeer::Sync(w, [&](int) {
    if (file_syncs++ == 0) {
      errno = EINTR;
      return -1;
    }
    return 0;
  });
  int directory_syncs = 0;
  WalWriterTestPeer::DirectorySync(w, [&](const std::string&) {
    if (directory_syncs++ == 0) {
      errno = EINTR;
      return false;
    }
    return true;
  });
  EXPECT_TRUE(w.append_frame(1, WalRecordKind::kInsert, "a", 1));
  EXPECT_EQ(file_syncs, 2);
  EXPECT_GE(directory_syncs, 2);
}
TEST_F(WalProtocolTest, FileAndDirectorySyncFailuresFailStop) {
  EXPECT_DEATH(
      {
        LocalWalWriter w(dir, 0);
        w.open(dir, 7);
        WalWriterTestPeer::Sync(w, [](int) {
          errno = EIO;
          return -1;
        });
        w.append_frame(1, WalRecordKind::kInsert, "a", 1);
      },
      "durability unknown");
  EXPECT_DEATH(
      {
        LocalWalWriter w(dir, 1);
        w.open(dir, 7);
        WalWriterTestPeer::DirectorySync(
            w, [](const std::string&) { return false; });
        w.append_frame(2, WalRecordKind::kInsert, "a", 1);
      },
      "durability unknown");
}
#ifndef _WIN32
TEST_F(WalProtocolTest, ActualPartialWritesExitWithoutRollback) {
  for (size_t cut = 0; cut < kWalFrameHeaderSize + 3; ++cut) {
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    pid_t child = fork();
    ASSERT_NE(child, -1);
    if (child == 0) {
      LocalWalWriter w(dir, 0);
      w.open(dir, 7);
      w.append_frame(1, WalRecordKind::kInsert, "abc", 3);
      size_t left = cut;
      WalWriterTestPeer::Write(
          w, [&](int fd, const void* p, size_t n) -> ptrdiff_t {
            if (left == 0)
              ::_exit(0);
            size_t amount = std::min(left, n);
            auto written = ::write(fd, p, amount);
            left -= written;
            return written;
          });
      w.append_frame(2, WalRecordKind::kInsert, "def", 3);
      ::_exit(2);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    LocalWalParser p(dir, 7);
    ASSERT_EQ(p.replay_units().size(), 1) << cut;
    EXPECT_EQ(p.last_ts(), 1);
  }
}
TEST_F(WalProtocolTest, ActualPartialFileHeaderAndExitAfterSync) {
  for (size_t cut = 0; cut < 24; ++cut) {
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    pid_t child = fork();
    ASSERT_NE(child, -1);
    if (child == 0) {
      LocalWalWriter w(dir, 0);
      w.open(dir, 7);
      size_t left = cut;
      WalWriterTestPeer::Write(
          w, [&](int fd, const void* p, size_t n) -> ptrdiff_t {
            if (!left)
              ::_exit(0);
            size_t amount = std::min(left, n);
            auto wrote = ::write(fd, p, amount);
            left -= wrote;
            return wrote;
          });
      w.append_frame(1, WalRecordKind::kInsert, "a", 1);
      ::_exit(2);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_EQ(WEXITSTATUS(status), 0);
    LocalWalParser p(dir, 7);
    EXPECT_TRUE(p.replay_units().empty());
  }
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  pid_t child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    LocalWalWriter w(dir, 0);
    w.open(dir, 7);
    w.append_frame(1, WalRecordKind::kInsert, "a", 1);
    ::_exit(0);
  }
  int status;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  LocalWalParser p(dir, 7);
  EXPECT_EQ(p.last_ts(), 1);
}
#endif
TEST_F(WalProtocolTest, BoundedArchiveRejectsCountsBooleansAndNesting) {
  InArchive encoded;
  encoded << std::numeric_limits<size_t>::max();
  OutArchive a;
  a.SetSlice(encoded.GetBuffer(), encoded.GetSize());
  std::string s;
  EXPECT_THROW(a >> s, exception::Exception);
  a.SetSlice(encoded.GetBuffer(), encoded.GetSize());
  std::vector<Value> values;
  EXPECT_THROW(a >> values, exception::Exception);
  a.SetSlice(encoded.GetBuffer(), encoded.GetSize());
  std::vector<uint64_t> pod;
  EXPECT_THROW(a >> pod, exception::Exception);
  char bad = 2;
  a.SetSlice(&bad, 1);
  bool b;
  EXPECT_THROW(a >> b, exception::Exception);
  // Nested LIST DataTypes, not nested heap allocations.
  encoded.Clear();
  for (int i = 0; i < 65; ++i)
    encoded << DataTypeId::kList << char{1};
  encoded << DataTypeId::kInt64 << char{0};
  a.SetSlice(encoded.GetBuffer(), encoded.GetSize());
  DataType t;
  EXPECT_THROW(a >> t, exception::Exception);
  encoded.Clear();
  encoded << DataTypeId::kArray << DataType::Array(DataType::INT64, 2)
          << uint32_t{1} << Value::INT64(1);
  a.SetSlice(encoded.GetBuffer(), encoded.GetSize());
  Value v;
  EXPECT_THROW(a >> v, exception::Exception);

  DataType supported = DataType::INT64;
  for (int i = 0; i < 63; ++i)
    supported = DataType::List(supported);
  InArchive supported_bytes;
  EXPECT_NO_THROW(supported_bytes << supported);
  DataType too_deep = DataType::List(supported);
  InArchive rejected_bytes;
  EXPECT_THROW(rejected_bytes << too_deep, exception::Exception);

  // Keep the declared child type shallow so this independently exercises the
  // Value recursion limit rather than the DataType recursion limit.
  Value nested_value = Value::INT64(1);
  for (int i = 0; i < 63; ++i) {
    std::vector<Value> child;
    child.push_back(std::move(nested_value));
    nested_value = Value::LIST(DataType::INT64, std::move(child));
  }
  InArchive supported_value_bytes;
  EXPECT_NO_THROW(supported_value_bytes << nested_value);
  std::vector<Value> child;
  child.push_back(std::move(nested_value));
  Value too_deep_value = Value::LIST(DataType::INT64, std::move(child));
  InArchive rejected_value_bytes;
  EXPECT_THROW(rejected_value_bytes << too_deep_value, exception::Exception);

  InArchive encoded_nested_value;
  const auto list_of_int64 = DataType::List(DataType::INT64);
  for (int i = 0; i < 64; ++i) {
    encoded_nested_value << DataTypeId::kList << list_of_int64 << uint32_t{1};
  }
  encoded_nested_value << DataTypeId::kInt64 << int64_t{1};
  a.SetSlice(encoded_nested_value.GetBuffer(), encoded_nested_value.GetSize());
  EXPECT_THROW(a >> v, exception::Exception);
}

TEST_F(WalProtocolTest, InsertRedoRejectsHugePropertyCountsBeforeAllocation) {
  InArchive encoded;
  encoded << std::string{} << Value{} << std::numeric_limits<uint32_t>::max();
  OutArchive archive;
  archive.SetSlice(encoded.GetBuffer(), encoded.GetSize());
  InsertVertexRedo vertex_redo;
  EXPECT_THROW(InsertVertexRedo::Deserialize(archive, vertex_redo),
               exception::IOException);

  encoded.Clear();
  encoded << std::string{} << Value{} << std::string{} << Value{}
          << std::string{} << std::numeric_limits<uint32_t>::max();
  archive.SetSlice(encoded.GetBuffer(), encoded.GetSize());
  InsertEdgeRedo edge_redo;
  EXPECT_THROW(InsertEdgeRedo::Deserialize(archive, edge_redo),
               exception::IOException);
}
}  // namespace
}  // namespace neug
