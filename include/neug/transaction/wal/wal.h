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

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include "neug/transaction/wal/wal_codec.h"

#include "neug/common/types/value.h"
#include "neug/storages/graph/graph_entry.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/storages/index/storage_index.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

/// Payload views remain valid until parser close/destruction.
struct WalReplayUnit {
  uint32_t timestamp;
  WalRecordKind kind;
  std::string_view payload;
  size_t file_index;
  uint64_t source_offset;
};

std::string get_wal_uri_scheme(const std::string& uri);
std::string get_wal_uri_path(const std::string& uri);

/**
 * The interface of wal writer.
 *
 * Implementations own their resources and must release them without throwing
 * from their destructors. close() remains available for callers that need
 * explicit error reporting.
 */
class IWalWriter {
 public:
  virtual ~IWalWriter() noexcept = default;

  virtual std::string type() const = 0;
  /**
   * Open a WAL file. In service mode, each logical execution slot owns one
   * writer. The slot may move between pthread workers and must retain the same
   * writer for the full transaction.
   * The uri could be a file_path or a remote connection string.
   */
  virtual void open(const std::string& wal_uri, uint64_t checkpoint_id) = 0;

  /**
   * Close the wal writer. If a remote connection is hold by the wal writer,
   * it should be closed.
   */
  virtual void close() = 0;

  /**
   * Append one checksummed transaction frame to the WAL.
   */
  virtual bool append_frame(uint32_t timestamp, WalRecordKind kind,
                            const char* payload, size_t length) = 0;
};

/**
 * The interface of wal parser.
 */
class IWalParser {
 public:
  virtual ~IWalParser() {}

  /**
   * Open wals from a uri and parse the wal files.
   */
  virtual void open(const std::string& wal_uri, uint64_t checkpoint_id) = 0;

  virtual void close() = 0;

  virtual uint32_t last_ts() const = 0;
  virtual std::string_view source_path(size_t file_index) const = 0;

  /*
   * Validated units in global timestamp order.
   */
  virtual const std::vector<WalReplayUnit>& replay_units() const = 0;
};

class WalWriterFactory {
 public:
  using wal_writer_initializer_t = std::unique_ptr<IWalWriter> (*)(
      const std::string& wal_uri, int32_t slot_id);

  static void Init();

  static void Finalize();

  static std::unique_ptr<IWalWriter> CreateDummyWalWriter();

  static std::unique_ptr<IWalWriter> CreateWalWriter(const std::string& wal_uri,
                                                     int32_t slot_id);

  static bool RegisterWalWriter(const std::string& wal_writer_type,
                                wal_writer_initializer_t initializer);

 private:
  static std::unordered_map<std::string, wal_writer_initializer_t>&
  getKnownWalWriters();
};

class WalParserFactory {
 public:
  using wal_writer_initializer_t = std::unique_ptr<IWalWriter> (*)();
  using wal_parser_initializer_t = std::unique_ptr<IWalParser> (*)(
      const std::string& wal_dir, uint64_t checkpoint_id);

  static void Init();

  static void Finalize();

  static std::unique_ptr<IWalParser> CreateWalParser(const std::string& wal_uri,
                                                     uint64_t checkpoint_id);

  static bool RegisterWalParser(const std::string& wal_parser_type,
                                wal_parser_initializer_t initializer);

 private:
  static std::unordered_map<std::string, wal_parser_initializer_t>&
  getKnownWalParsers();
};

struct CreateVertexTypeRedo {
  static void Serialize(InArchive& arc, const CreateVertexTypeParam& config);
  static CreateVertexTypeParam Deserialize(OutArchive& arc);
};

struct CreateEdgeTypeRedo {
  static void Serialize(InArchive& arc, const CreateEdgeTypeParam& config);
  static CreateEdgeTypeParam Deserialize(OutArchive& arc);
};

struct AddVertexPropertiesRedo {
  std::string vertex_type;
  AddVertexPropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& vertex_type,
                        const AddVertexPropertiesParam& config);
  static AddVertexPropertiesRedo Deserialize(OutArchive& arc);
};

struct AddEdgePropertiesRedo {
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  AddEdgePropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type,
                        const AddEdgePropertiesParam& config);
  static AddEdgePropertiesRedo Deserialize(OutArchive& arc);
};

struct RenameVertexPropertiesRedo {
  std::string vertex_type;
  RenameVertexPropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& vertex_type,
                        const RenameVertexPropertiesParam& config);
  static RenameVertexPropertiesRedo Deserialize(OutArchive& arc);
};

struct RenameEdgePropertiesRedo {
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  RenameEdgePropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type,
                        const RenameEdgePropertiesParam& config);
  static RenameEdgePropertiesRedo Deserialize(OutArchive& arc);
};

struct DeleteVertexPropertiesRedo {
  std::string vertex_type;
  DeleteVertexPropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& vertex_type,
                        const DeleteVertexPropertiesParam& config);
  static DeleteVertexPropertiesRedo Deserialize(OutArchive& arc);
};

struct DeleteEdgePropertiesRedo {
  std::string src_type;
  std::string dst_type;
  std::string edge_type;
  DeleteEdgePropertiesParam config;

  static void Serialize(InArchive& arc, const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type,
                        const DeleteEdgePropertiesParam& config);
  static DeleteEdgePropertiesRedo Deserialize(OutArchive& arc);
};

struct DeleteVertexTypeRedo {
  std::string vertex_type;

  static void Serialize(InArchive& arc, const std::string& vertex_type);
  static void Deserialize(OutArchive& arc, DeleteVertexTypeRedo& redo);
};

struct DeleteEdgeTypeRedo {
  std::string src_type, dst_type, edge_type;

  static void Serialize(InArchive& arc, const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type);
  static void Deserialize(OutArchive& arc, DeleteEdgeTypeRedo& redo);
};

struct CreateIndexRedo {
  static void Serialize(InArchive& arc, const IndexMeta& meta);
  static IndexMeta Deserialize(OutArchive& arc);
};

struct DropIndexRedo {
  static void Serialize(InArchive& arc, const std::string& name);
  static std::string Deserialize(OutArchive& arc);
};

struct ActivateIndexesRedo {
  static void Serialize(InArchive& arc);
};

struct AddGraphEntryRedo {
  std::string name;
  ProjectedGraphEntry entry;

  static void Serialize(InArchive& arc, const std::string& name,
                        const ProjectedGraphEntry& entry);
  static AddGraphEntryRedo Deserialize(OutArchive& arc);
};

struct DropGraphEntryRedo {
  std::string name;

  static void Serialize(InArchive& arc, const std::string& name);
  static DropGraphEntryRedo Deserialize(OutArchive& arc);
};

struct InsertVertexRedo {
  std::string vertex_type;
  Value oid;
  std::vector<Value> props;

  static void Serialize(InArchive& arc, const std::string& vertex_type,
                        const Value& oid, const std::vector<Value>& props);
  static void Deserialize(OutArchive& arc, InsertVertexRedo& redo);
};

struct InsertEdgeRedo {
  std::string src_type;
  Value src;
  std::string dst_type;
  Value dst;
  std::string edge_type;
  std::vector<Value> properties;

  static void Serialize(InArchive& arc, const std::string& src_type,
                        const Value& src, const std::string& dst_type,
                        const Value& dst, const std::string& edge_type,
                        const std::vector<Value>& properties);
  static void Deserialize(OutArchive& arc, InsertEdgeRedo& redo);
};

struct UpdateVertexPropRedo {
  std::string vertex_type;
  Value oid;
  int prop_id;
  Value value;

  static void Serialize(InArchive& arc, const std::string& vertex_type,
                        const Value& oid, int prop_id, const Value& value);
  static void Deserialize(OutArchive& arc, UpdateVertexPropRedo& redo);
};

struct UpdateEdgePropRedo {
  std::string src_type;
  Value src;
  std::string dst_type;
  Value dst;
  std::string edge_type;
  int32_t oe_offset, ie_offset;
  int prop_id;
  Value value;

  static void Serialize(InArchive& arc, const std::string& src_type,
                        const Value& src, const std::string& dst_type,
                        const Value& dst, const std::string& edge_type,
                        int32_t oe_offset, int32_t ie_offset, int prop_id,
                        const Value& value);
  static void Deserialize(OutArchive& arc, UpdateEdgePropRedo& redo);
};

struct RemoveVertexRedo {
  std::string vertex_type;
  Value oid;

  static void Serialize(InArchive& arc, const std::string& vertex_type,
                        const Value& oid);
  static void Deserialize(OutArchive& arc, RemoveVertexRedo& redo);
};

struct RemoveEdgeRedo {
  std::string src_type;
  Value src;
  std::string dst_type;
  Value dst;
  std::string edge_type;
  int32_t oe_offset, ie_offset;

  static void Serialize(InArchive& arc, const std::string& src_type,
                        const Value& src, const std::string& dst_type,
                        const Value& dst, const std::string& edge_type,
                        int32_t oe_offset, int32_t ie_offset);
  static void Deserialize(OutArchive& arc, RemoveEdgeRedo& redo);
};

InArchive& operator<<(InArchive& in_archive, const DeleteVertexTypeRedo& value);
InArchive& operator<<(InArchive& in_archive, const DeleteEdgeTypeRedo& value);
InArchive& operator<<(InArchive& in_archive, const InsertVertexRedo& value);
InArchive& operator<<(InArchive& in_archive, const InsertEdgeRedo& value);
InArchive& operator<<(InArchive& in_archive, const UpdateVertexPropRedo& value);
InArchive& operator<<(InArchive& in_archive, const UpdateEdgePropRedo& value);
InArchive& operator<<(InArchive& in_archive, const RemoveVertexRedo& value);
InArchive& operator<<(InArchive& in_archive, const RemoveEdgeRedo& value);

OutArchive& operator>>(OutArchive& out_archive, DeleteVertexTypeRedo& value);
OutArchive& operator>>(OutArchive& out_archive, DeleteEdgeTypeRedo& value);
OutArchive& operator>>(OutArchive& out_archive, InsertVertexRedo& value);
OutArchive& operator>>(OutArchive& out_archive, InsertEdgeRedo& value);
OutArchive& operator>>(OutArchive& out_archive, UpdateVertexPropRedo& value);
OutArchive& operator>>(OutArchive& out_archive, UpdateEdgePropRedo& value);
OutArchive& operator>>(OutArchive& out_archive, RemoveVertexRedo& value);
OutArchive& operator>>(OutArchive& out_archive, RemoveEdgeRedo& value);

}  // namespace neug
