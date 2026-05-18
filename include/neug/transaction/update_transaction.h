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
#include <limits>
#include <map>
#include <memory>
#include <stack>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"
#include "neug/common/extra_type_info.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/storages/allocators.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/snapshot_store.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"

namespace neug {

class PropertyGraph;
class IWalWriter;
class IVersionManager;
class Schema;
template <typename INDEX_T>
class IdIndexerBase;

/**
 * @brief Transaction for updating existing graph elements (vertices and edges).
 *
 * UpdateTransaction handles modifications to existing graph data with ACID
 * guarantees using Copy-on-Write (COW) for snapshot isolation.
 *
 * **COW Design:**
 * - Holds a shared_ptr to a forked PropertyGraph (COW copy)
 * - All DDL/DML modifications happen immediately on the COW copy
 * - Commit returns the COW copy for SnapshotStore::installSnapshot()
 * - Abort discards the COW copy (no effect on original)
 *
 * **Key Features:**
 * - Update vertex and edge properties
 * - DDL operations (create/delete types, add/delete properties)
 * - Write-Ahead Logging for durability
 * - MVCC support with timestamp management
 * - Lazy fork for efficient COW
 *
 * @since v0.1.0
 */
class UpdateTransaction {
 public:
  /**
   * @brief Construct an UpdateTransaction with a COW PropertyGraph.
   *
   * @param cow_storage Forked PropertyGraph (COW copy)
   * @param alloc Reference to memory allocator
   * @param logger Reference to WAL writer
   * @param vm Reference to version manager
   * @param snapshot_store Reference to SnapshotStore for commit
   * @param cache Reference to query cache
   * @param timestamp Transaction timestamp
   *
   * @note NeugDB is responsible for creating the COW copy via Fork()
   */
  UpdateTransaction(std::shared_ptr<PropertyGraph> cow_storage,
                    Allocator& alloc, IWalWriter& logger, IVersionManager& vm,
                    SnapshotStore& snapshot_store,
                    execution::LocalQueryCache& cache, timestamp_t timestamp);

  /**
   * @brief Destructor that calls Abort().
   */
  ~UpdateTransaction();

  /**
   * @brief Get the transaction timestamp.
   */
  timestamp_t timestamp() const;

  /**
   * @brief Get read-only access to the graph schema.
   */
  const Schema& schema() const { return cow_storage_->schema(); }

  /**
   * @brief Commit the transaction.
   *
   * Flushes WAL and installs the COW copy into SnapshotStore.
   *
   * @return true if committed successfully (or no-op), false if failed
   */
  bool Commit();

  void Abort();

  Status CreateVertexType(const CreateVertexTypeParam& config);

  Status CreateEdgeType(const CreateEdgeTypeParam& config);

  Status AddVertexProperties(const AddVertexPropertiesParam& config);

  Status AddEdgeProperties(const AddEdgePropertiesParam& config);

  Status RenameVertexProperties(const RenameVertexPropertiesParam& config);

  Status RenameEdgeProperties(const RenameEdgePropertiesParam& config);

  Status DeleteVertexProperties(const DeleteVertexPropertiesParam& config);

  Status DeleteEdgeProperties(const DeleteEdgePropertiesParam& config);

  Status DeleteVertexType(const std::string& vertex_type_name);

  Status DeleteEdgeType(const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type);

  bool AddVertex(label_t label, const Property& oid,
                 const std::vector<Property>& props, vid_t& vid);

  /**
   * @brief Delete vertex and its associated edges.
   * @param label Vertex label id.
   * @param lid Local vertex id.
   * @return true if deletion is successful, false otherwise.
   * @note We always delete vertex in detach mode.
   */
  bool DeleteVertex(label_t label, vid_t lid);

  bool AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
               label_t edge_label, const std::vector<Property>& properties);

  bool DeleteEdges(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                   label_t edge_label);

  bool DeleteEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                  label_t edge_label, int32_t oe_offset, int32_t ie_offset);

  std::shared_ptr<RefColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const {
    auto col = cow_storage_->GetVertexPropertyColumn(label, col_name);
    if (!col) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex property [" + col_name +
                                       "] not found for label " +
                                       std::to_string(label));
    }
    return col;
  }

  Property GetVertexProperty(label_t label, vid_t lid, int col_id) const;

  bool UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                            const Property& value);

  bool UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label,
                          vid_t dst, label_t edge_label, int32_t col_id,
                          const Property& value);

  bool UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label,
                          vid_t dst, label_t edge_label, int32_t oe_offset,
                          int32_t ie_offset, int32_t col_id,
                          const Property& value);

  CsrBaseView GetGenericOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return cow_storage_->GetGenericOutgoingGraphView(v_label, neighbor_label,
                                                     edge_label, timestamp_);
  }

  CsrBaseView GetGenericIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return cow_storage_->GetGenericIncomingGraphView(v_label, neighbor_label,
                                                     edge_label, timestamp_);
  }

  static void IngestWal(PropertyGraph& graph, uint32_t timestamp, char* data,
                        size_t length, Allocator& alloc);
  Property GetVertexId(label_t label, vid_t lid) const;

  bool GetVertexIndex(label_t label, const Property& id, vid_t& index) const;

  PropertyGraph& graph() const { return *cow_storage_; }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    return cow_storage_->GetEdgeDataAccessor(src_label, dst_label, edge_label,
                                             prop_id);
  }

  void CreateCheckpoint();

  /**
   * Batch add vertices with given label, ids and property table.
   * We will not generate wal log for this operation. Assume the ids and
   * table are valid.
   * @param v_label_id The label id of the vertices to be added.
   * @param ids The ids of the vertices to be added.
   * @param table The property table of the vertices to be added.
   * @return Status
   */
  // TODO(zhanglei): Remove batch method from UpdateTransaction after
  // refactoring GraphInterface.
  inline Status BatchAddVertices(
      label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) {
    return cow_storage_->BatchAddVertices(v_label_id, supplier);
  }

  // TODO(zhanglei): Remove batch method from UpdateTransaction after
  // refactoring GraphInterface.
  inline Status BatchAddEdges(label_t src_label, label_t dst_label,
                              label_t edge_label,
                              std::shared_ptr<IRecordBatchSupplier> supplier) {
    return cow_storage_->BatchAddEdges(src_label, dst_label, edge_label,
                                       std::move(supplier));
  }

  // TODO(zhanglei): Remove batch method from UpdateTransaction after
  // refactoring GraphInterface.
  inline Status BatchDeleteVertices(label_t v_label_id,
                                    const std::vector<vid_t>& vids) {
    return cow_storage_->BatchDeleteVertices(v_label_id, vids);
  }

 private:
  bool IsValidLid(label_t label, vid_t lid) const;

  void release();

  // COW storage - the forked PropertyGraph
  std::shared_ptr<PropertyGraph> cow_storage_;

  Allocator& alloc_;
  IWalWriter& logger_;
  IVersionManager& vm_;
  SnapshotStore& snapshot_store_;
  execution::LocalQueryCache& pipeline_cache_;
  timestamp_t timestamp_;

  // Fork context (from cow_storage_)
  Checkpoint* ckp_;
  MemoryLevel memory_level_;
  InArchive arc_;
  int op_num_;

  // Set by any successful DDL method. When true, Commit calls
  // pipeline_cache_.clearGlobalCache(...) after installSnapshot to bump the
  // GlobalQueryCache version + refresh planner meta. Pure DML transactions
  // leave this false to skip cache invalidation.
  bool schema_mutated_{false};
};

class StorageTPUpdateInterface : public StorageUpdateInterface {
 public:
  explicit StorageTPUpdateInterface(UpdateTransaction& txn)
      : StorageUpdateInterface(txn.graph(), txn.timestamp()), txn_(txn) {}
  ~StorageTPUpdateInterface() {}

  // --- Read method overrides ---
  // Methods that delegate to the COW storage via txn_.

  std::shared_ptr<RefColumnBase> GetVertexPropColumn(
      label_t label, const std::string& prop_name) const override {
    return txn_.get_vertex_property_column(label, prop_name);
  }

  bool GetVertexIndex(label_t label, const Property& id,
                      vid_t& index) const override {
    return txn_.GetVertexIndex(label, id, index);
  }

  Property GetVertexId(label_t label, vid_t index) const override {
    return txn_.GetVertexId(label, index);
  }

  Property GetVertexProperty(label_t label, vid_t index,
                             int prop_id) const override {
    return txn_.GetVertexProperty(label, index, prop_id);
  }

  CsrBaseView GetGenericOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const override {
    return txn_.GetGenericOutgoingGraphView(v_label, neighbor_label,
                                            edge_label);
  }

  CsrBaseView GetGenericIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const override {
    return txn_.GetGenericIncomingGraphView(v_label, neighbor_label,
                                            edge_label);
  }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label,
                                       int prop_id) const override {
    return txn_.GetEdgeDataAccessor(src_label, dst_label, edge_label, prop_id);
  }

  EdgeDataAccessor GetEdgeDataAccessor(
      label_t src_label, label_t dst_label, label_t edge_label,
      const std::string& prop_name) const override {
    int prop_id = txn_.schema()
                              .get_edge_schema(src_label, dst_label, edge_label)
                              ->property_names.size() > 0
                      ? 0
                      : -1;
    return txn_.GetEdgeDataAccessor(src_label, dst_label, edge_label, prop_id);
  }

  // --- Write methods ---
  inline void UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                                   const Property& value) override {
    txn_.UpdateVertexProperty(label, lid, col_id, value);
  }

  inline void UpdateEdgeProperty(label_t src_label, vid_t src,
                                 label_t dst_label, vid_t dst,
                                 label_t edge_label, int32_t oe_offset,
                                 int32_t ie_offset, int32_t col_id,
                                 const Property& value) override {
    txn_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                            oe_offset, ie_offset, col_id, value);
  }

  inline bool AddVertex(label_t label, const Property& id,
                        const std::vector<Property>& props,
                        vid_t& vid) override {
    return txn_.AddVertex(label, id, props, vid);
  }

  inline bool AddEdge(label_t src_label, vid_t src, label_t dst_label,
                      vid_t dst, label_t edge_label,
                      const std::vector<Property>& properties) override {
    return txn_.AddEdge(src_label, src, dst_label, dst, edge_label, properties);
  }
  void CreateCheckpoint() override;
  inline UpdateTransaction& GetTransaction() { return txn_; }
  Status BatchAddVertices(
      label_t v_label_id,
      std::shared_ptr<IRecordBatchSupplier> supplier) override;
  Status BatchAddEdges(label_t src_label, label_t dst_label, label_t edge_label,
                       std::shared_ptr<IRecordBatchSupplier> supplier) override;
  Status BatchDeleteVertices(label_t v_label_id,
                             const std::vector<vid_t>& vids) override;
  Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) override;
  Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) override;
  Status CreateVertexType(const CreateVertexTypeParam& config) override;
  Status CreateEdgeType(const CreateEdgeTypeParam& config) override;
  Status AddVertexProperties(const AddVertexPropertiesParam& config) override;
  Status AddEdgeProperties(const AddEdgePropertiesParam& config) override;
  Status RenameVertexProperties(
      const RenameVertexPropertiesParam& config) override;
  Status RenameEdgeProperties(const RenameEdgePropertiesParam& config) override;
  Status DeleteVertexProperties(
      const DeleteVertexPropertiesParam& config) override;
  Status DeleteEdgeProperties(const DeleteEdgePropertiesParam& config) override;
  Status DeleteVertexType(const std::string& vertex_type_name) override;
  Status DeleteEdgeType(const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type) override;

 private:
  UpdateTransaction& txn_;
};

}  // namespace neug
