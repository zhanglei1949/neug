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

#include "neug/transaction/update_transaction.h"

#include <glog/logging.h>
#include <cstdint>

#include <algorithm>
#include <filesystem>
#include <limits>
#include <ostream>
#include <string_view>

#include <flat_hash_map.hpp>
#include "neug/common/extra_type_info.h"
#include "neug/storages/allocators.h"
#include "neug/storages/csr/csr_base.h"
#include "neug/storages/csr/generic_view_utils.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

static Status resolveVertexLabel(const Schema& schema,
                                 const std::string& vertex_type_name,
                                 label_t& label_id) {
  if (!schema.contains_vertex_label(vertex_type_name)) {
    LOG(ERROR) << "Vertex type " << vertex_type_name << " does not exist.";
    return Status(StatusCode::ERR_SCHEMA_MISMATCH,
                  "Vertex type " + vertex_type_name + " does not exist.");
  }
  label_id = schema.get_vertex_label_id(vertex_type_name);
  return Status::OK();
}

static Status resolveEdgeTriplet(const Schema& schema,
                                 const std::string& src_type,
                                 const std::string& dst_type,
                                 const std::string& edge_type,
                                 label_t& src_label_id, label_t& dst_label_id,
                                 label_t& edge_label_id) {
  if (!schema.exist(src_type, dst_type, edge_type)) {
    LOG(ERROR) << "Edge type " << edge_type << " does not exist between "
               << src_type << " and " << dst_type << ".";
    return Status(StatusCode::ERR_SCHEMA_MISMATCH,
                  "Edge type " + edge_type + " does not exist between " +
                      src_type + " and " + dst_type + ".");
  }
  src_label_id = schema.get_vertex_label_id(src_type);
  dst_label_id = schema.get_vertex_label_id(dst_type);
  edge_label_id = schema.get_edge_label_id(edge_type);
  return Status::OK();
}

std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>>
fetch_edges_related_to_vertex_from_view(const std::vector<DataType>& props,
                                        const CsrBaseView& oe,
                                        const CsrBaseView& ie, vid_t lid,
                                        bool is_src, timestamp_t ts) {
  std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>> related_edges;
  if (is_src) {
    NbrList nbr_list = oe.get_edges(lid);
    auto stride = nbr_list.cfg.stride;
    auto start_ptr = static_cast<const char*>(nbr_list.start_ptr);
    for (auto it = nbr_list.begin(); it != nbr_list.end(); ++it) {
      if (it.get_timestamp() > ts) {
        continue;
      }
      auto oe_offset =
          (static_cast<const char*>(it.get_nbr_ptr()) - start_ptr) / stride;
      vid_t dst_lid = it.get_vertex();
      int32_t ie_offset = neug::search_other_offset_with_cur_offset(
          oe, ie, lid, dst_lid, oe_offset, props);
      related_edges.emplace_back(lid, dst_lid, oe_offset, ie_offset);
    }
  } else {
    NbrList nbr_list = ie.get_edges(lid);
    auto stride = nbr_list.cfg.stride;
    auto start_ptr = static_cast<const char*>(nbr_list.start_ptr);
    for (auto it = nbr_list.begin(); it != nbr_list.end(); ++it) {
      if (it.get_timestamp() > ts) {
        continue;
      }
      auto ie_offset =
          (static_cast<const char*>(it.get_nbr_ptr()) - start_ptr) / stride;
      vid_t src_lid = it.get_vertex();
      int32_t oe_offset = neug::search_other_offset_with_cur_offset(
          ie, oe, lid, src_lid, ie_offset, props);
      related_edges.emplace_back(src_lid, lid, oe_offset, ie_offset);
    }
  }
  return related_edges;
}

std::unordered_map<uint32_t,
                   std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>>>
fetch_edges_related_to_vertex(UpdateTransaction& txn, label_t v_label,
                              vid_t lid, timestamp_t ts) {
  std::unordered_map<uint32_t,
                     std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>>>
      related_edges;  // edge_triplet_id: <src, dst, oe_offset, ie_offset>
  auto v_label_num = txn.schema().vertex_label_frontier();
  auto e_label_num = txn.schema().edge_label_frontier();
  auto& schema = txn.schema();
  for (auto other_label_id = 0; other_label_id < v_label_num;
       ++other_label_id) {
    if (!schema.vertex_label_valid(other_label_id)) {
      continue;
    }
    for (auto e_label_id = 0; e_label_id < e_label_num; ++e_label_id) {
      if (!schema.edge_label_valid(e_label_id)) {
        continue;
      }
      if (schema.exist(v_label, other_label_id, e_label_id)) {
        auto props =
            schema.get_edge_properties(v_label, other_label_id, e_label_id);
        auto edge_triplet_id =
            schema.generate_edge_label(v_label, other_label_id, e_label_id);
        auto oe_view = txn.GetGenericOutgoingGraphView(v_label, other_label_id,
                                                       e_label_id);
        auto ie_view = txn.GetGenericIncomingGraphView(other_label_id, v_label,
                                                       e_label_id);

        related_edges[edge_triplet_id] =
            fetch_edges_related_to_vertex_from_view(props, oe_view, ie_view,
                                                    lid, true, ts);
      }
      if (other_label_id != v_label &&
          schema.exist(other_label_id, v_label, e_label_id)) {
        auto props =
            schema.get_edge_properties(other_label_id, v_label, e_label_id);
        auto edge_triplet_id =
            schema.generate_edge_label(other_label_id, v_label, e_label_id);
        auto oe_view = txn.GetGenericOutgoingGraphView(other_label_id, v_label,
                                                       e_label_id);
        auto ie_view = txn.GetGenericIncomingGraphView(v_label, other_label_id,
                                                       e_label_id);
        related_edges[edge_triplet_id] =
            fetch_edges_related_to_vertex_from_view(props, oe_view, ie_view,
                                                    lid, false, ts);
      }
    }
  }
  return related_edges;
}

UpdateTransaction::UpdateTransaction(std::shared_ptr<PropertyGraph> cow_storage,
                                     Allocator& alloc, IWalWriter& logger,
                                     IVersionManager& vm,
                                     SnapshotStore& snapshot_store,
                                     execution::LocalQueryCache& cache,
                                     timestamp_t timestamp)
    : cow_storage_(std::move(cow_storage)),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      snapshot_store_(snapshot_store),
      pipeline_cache_(cache),
      timestamp_(timestamp),
      ckp_(&cow_storage_->checkpoint()),
      memory_level_(cow_storage_->memory_level()),
      op_num_(0) {
  arc_.Resize(sizeof(WalHeader));
}

UpdateTransaction::~UpdateTransaction() { Abort(); }

timestamp_t UpdateTransaction::timestamp() const { return timestamp_; }

bool UpdateTransaction::Commit() {
  if (timestamp_ == INVALID_TIMESTAMP) {
    return true;  // Already committed or aborted
  }
  if (op_num_ == 0) {
    release();
    return true;  // No-op commit is success
  }

  auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
  header->length = arc_.GetSize() - sizeof(WalHeader);
  header->type = 1;
  header->timestamp = timestamp_;
  if (!logger_.append(arc_.GetBuffer(), arc_.GetSize())) {
    LOG(ERROR) << "Failed to append wal log";
    Abort();
    return false;
  }

  // Install the COW copy into SnapshotStore. All DDL/DML have already been
  // applied directly to cow_storage_ during the transaction.
  // Pass by const-ref: on slot exhaustion cow_storage_ stays valid so the
  // caller can retry or surface the error without losing transaction state.
  auto status = snapshot_store_.installSnapshot(cow_storage_, timestamp_);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to install snapshot: " << status.ToString();
    Abort();
    return false;
  }

  // If any DDL ran in this transaction, bump the global query cache version
  // and refresh the planner's compiler meta. This must happen BEFORE we
  // release cow_storage_, since we need its schema yaml. Stats are NOT
  // refreshed here — DDL doesn't change data distribution.
  if (schema_mutated_) {
    pipeline_cache_.clearGlobalCache(cow_storage_->schema().to_yaml().value());
  }

  // The COW PG is now owned by the new slot. Drop our reference so this
  // transaction does not appear to still hold the snapshot.
  cow_storage_.reset();
  release();
  return true;
}

void UpdateTransaction::Abort() {
  // COW: Simply discard the copy - modifications stay on the COW copy
  // No need to revert changes since original is unaffected
  cow_storage_.reset();
  release();
}

Status UpdateTransaction::CreateVertexType(
    const CreateVertexTypeParam& config) {
  const auto& name = config.GetVertexLabel();
  if (cow_storage_->schema().contains_vertex_label(name)) {
    LOG(ERROR) << "Vertex type " << name << " already exists.";
    return Status(StatusCode::ERR_SCHEMA_MISMATCH,
                  "Vertex type " + name + " already exists.");
  }
  CreateVertexTypeRedo::Serialize(arc_, config);
  op_num_ += 1;
  auto status = cow_storage_->CreateVertexType(config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to create vertex type " << name << ": "
               << status.ToString();
    return status;
  }
  schema_mutated_ = true;
  return status;
}

Status UpdateTransaction::CreateEdgeType(const CreateEdgeTypeParam& config) {
  const auto& src_type = config.GetSrcLabel();
  const auto& dst_type = config.GetDstLabel();
  const auto& edge_type = config.GetEdgeLabel();
  if (cow_storage_->schema().exist(src_type, dst_type, edge_type)) {
    LOG(ERROR) << "Edge type " << edge_type << " already exists between "
               << src_type << " and " << dst_type << ".";
    return Status(StatusCode::ERR_SCHEMA_MISMATCH,
                  "Edge type " + edge_type + " already exists between " +
                      src_type + " and " + dst_type + ".");
  }
  CreateEdgeTypeRedo::Serialize(arc_, config);
  op_num_ += 1;
  auto status = cow_storage_->CreateEdgeType(config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to create edge type " << edge_type << " between "
               << src_type << " and " << dst_type << ": " << status.ToString();
    return status;
  }
  schema_mutated_ = true;
  return status;
}

Status UpdateTransaction::AddVertexProperties(
    const AddVertexPropertiesParam& config) {
  const auto& vertex_type_name = config.GetVertexLabel();
  const auto& add_properties = config.GetProperties();
  label_t v_label;
  RETURN_IF_NOT_OK(
      resolveVertexLabel(graph_.schema(), vertex_type_name, v_label));
  AddVertexPropertiesRedo::Serialize(arc_, config);
  op_num_ += 1;
  auto status = cow_storage_->AddVertexProperties(config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add properties to vertex type "
               << config.GetVertexLabel() << ": " << status.ToString();
    return status;
  }
  schema_mutated_ = true;
  return status;
}

Status UpdateTransaction::AddEdgeProperties(
    const AddEdgePropertiesParam& config) {
  const auto& src_type = config.GetSrcLabel();
  const auto& dst_type = config.GetDstLabel();
  const auto& edge_type = config.GetEdgeLabel();
  const auto& add_properties = config.GetProperties();
  label_t src_label_id, dst_label_id, edge_label_id;
  RETURN_IF_NOT_OK(resolveEdgeTriplet(graph_.schema(), src_type, dst_type,
                                      edge_type, src_label_id, dst_label_id,
                                      edge_label_id));
  AddEdgePropertiesRedo::Serialize(arc_, config);
  op_num_ += 1;
  auto status = cow_storage_->AddEdgeProperties(config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add properties to edge type "
               << config.GetEdgeLabel() << " between " << config.GetSrcLabel()
               << " and " << config.GetDstLabel() << ": " << status.ToString();
    return status;
  }
  schema_mutated_ = true;
  return status;
}

Status UpdateTransaction::RenameVertexProperties(
    const RenameVertexPropertiesParam& config) {
  const auto& vertex_type_name = config.GetVertexLabel();
  const auto& rename_properties = config.GetRenameProperties();
  label_t v_label;
  RETURN_IF_NOT_OK(
      resolveVertexLabel(graph_.schema(), vertex_type_name, v_label));
  RenameVertexPropertiesRedo::Serialize(arc_, config);
  op_num_ += 1;
  auto status = cow_storage_->RenameVertexProperties(config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to rename properties of vertex type "
               << config.GetVertexLabel() << ": " << status.ToString();
    return status;
  }
  schema_mutated_ = true;
  return status;
}

Status UpdateTransaction::RenameEdgeProperties(
    const RenameEdgePropertiesParam& config) {
  const auto& src_type = config.GetSrcLabel();
  const auto& dst_type = config.GetDstLabel();
  const auto& edge_type = config.GetEdgeLabel();
  const auto& rename_properties = config.GetRenameProperties();
  label_t src_label_id, dst_label_id, edge_label_id;
  RETURN_IF_NOT_OK(resolveEdgeTriplet(graph_.schema(), src_type, dst_type,
                                      edge_type, src_label_id, dst_label_id,
                                      edge_label_id));
  RenameEdgePropertiesRedo::Serialize(arc_, config);
  op_num_ += 1;
  auto status = cow_storage_->RenameEdgeProperties(config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to rename properties of edge type "
               << config.GetEdgeLabel() << " between " << config.GetSrcLabel()
               << " and " << config.GetDstLabel() << ": " << status.ToString();
    return status;
  }
  schema_mutated_ = true;
  return status;
}

Status UpdateTransaction::DeleteVertexProperties(
    const DeleteVertexPropertiesParam& config) {
  const auto& vertex_type_name = config.GetVertexLabel();
  const auto& delete_properties = config.GetDeleteProperties();
  label_t v_label;
  RETURN_IF_NOT_OK(
      resolveVertexLabel(graph_.schema(), vertex_type_name, v_label));
  for (const auto& prop_name : config.GetDeleteProperties()) {
    if (!cow_storage_->schema().vertex_has_property(config.GetVertexLabel(),
                                                    prop_name)) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Property [" + prop_name + "] does not exist in vertex [" +
                        config.GetVertexLabel() + "].");
    }
  }
  DeleteVertexPropertiesRedo::Serialize(arc_, config);
  op_num_ += 1;
  auto status = cow_storage_->DeleteVertexProperties(config);
  if (status.ok()) {
    schema_mutated_ = true;
  }
  return status;
}

Status UpdateTransaction::DeleteEdgeProperties(
    const DeleteEdgePropertiesParam& config) {
  const auto& src_type = config.GetSrcLabel();
  const auto& dst_type = config.GetDstLabel();
  const auto& edge_type = config.GetEdgeLabel();
  const auto& delete_properties = config.GetDeleteProperties();
  label_t src_label_id, dst_label_id, edge_label_id;
  RETURN_IF_NOT_OK(resolveEdgeTriplet(graph_.schema(), src_type, dst_type,
                                      edge_type, src_label_id, dst_label_id,
                                      edge_label_id));
  for (const auto& prop_name : config.GetDeleteProperties()) {
    if (!cow_storage_->schema().edge_has_property(
            config.GetSrcLabel(), config.GetDstLabel(), config.GetEdgeLabel(),
            prop_name)) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Property [" + prop_name + "] does not exist in edge [" +
                        config.GetEdgeLabel() + "] between [" +
                        config.GetSrcLabel() + "] and [" +
                        config.GetDstLabel() + "].");
    }
  }
  DeleteEdgePropertiesRedo::Serialize(arc_, config);
  op_num_ += 1;
  auto status = cow_storage_->DeleteEdgeProperties(config);
  if (status.ok()) {
    schema_mutated_ = true;
  }
  return status;
}

Status UpdateTransaction::DeleteVertexType(
    const std::string& vertex_type_name) {
  label_t v_label;
  RETURN_IF_NOT_OK(
      resolveVertexLabel(graph_.schema(), vertex_type_name, v_label));
  DeleteVertexTypeRedo::Serialize(arc_, vertex_type_name);
  op_num_ += 1;
  auto status = cow_storage_->DeleteVertexType(vk.label);
  if (status.ok()) {
    schema_mutated_ = true;
  }
  return status;
}

Status UpdateTransaction::DeleteEdgeType(const std::string& src_type,
                                         const std::string& dst_type,
                                         const std::string& edge_type) {
  label_t src_label_id, dst_label_id, edge_label_id;
  RETURN_IF_NOT_OK(resolveEdgeTriplet(graph_.schema(), src_type, dst_type,
                                      edge_type, src_label_id, dst_label_id,
                                      edge_label_id));
  DeleteEdgeTypeRedo::Serialize(arc_, src_type, dst_type, edge_type);
  op_num_ += 1;
  auto status =
      cow_storage_->DeleteEdgeType(src_label_id, dst_label_id, edge_label_id);
  if (status.ok()) {
    schema_mutated_ = true;
  }
  return status;
}

bool UpdateTransaction::AddVertex(label_t label, const Property& oid,
                                  const std::vector<Property>& props,
                                  vid_t& vid) {
  std::vector<DataType> types =
      cow_storage_->schema().get_vertex_properties(label);
  if (types.size() != props.size()) {
    return false;
  }
  int col_num = types.size();
  for (int col_i = 0; col_i != col_num; ++col_i) {
    if (props[col_i].type() != types[col_i].id()) {
      return false;
    }
  }

  const auto& v_table = cow_storage_->get_vertex_table(label);
  if (v_table.Size() >= v_table.Capacity()) {
    size_t new_capacity =
        v_table.Size() < 4096 ? 4096 : v_table.Size() + v_table.Size() / 4;
    auto status = cow_storage_->EnsureCapacity(label, new_capacity);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to ensure space for vertex of label "
                 << cow_storage_->schema().get_vertex_label_name(label) << ": "
                 << status.ToString();
      return false;
    }
  }

  InsertVertexRedo::Serialize(arc_, label, oid, props);
  op_num_ += 1;
  auto status =
      cow_storage_->AddVertex(label, oid, props, vid, timestamp_, true);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add vertex of label "
               << cow_storage_->schema().get_vertex_label_name(label) << ": "
               << status.ToString();
    return false;
  }
  return true;
}

bool UpdateTransaction::DeleteVertex(label_t label, vid_t lid) {
  if (!cow_storage_->IsValidLid(label, lid, timestamp_)) {
    THROW_RUNTIME_ERROR("Vertex id is out of range or already deleted");
  }
  auto oid = cow_storage_->GetOid(label, lid, timestamp_);
  RemoveVertexRedo::Serialize(arc_, label, oid);
  op_num_ += 1;
  // edge_triplet_id: < src, dst, oe_offset, ie_offset>
  std::unordered_map<uint32_t,
                     std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>>>
      related_edges =
          fetch_edges_related_to_vertex(*this, label, lid, timestamp_);
  auto status = cow_storage_->DeleteVertex(label, lid, timestamp_, alloc_);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to delete vertex of label "
               << cow_storage_->schema().get_vertex_label_name(label) << ": "
               << status.ToString();
    return false;
  }
  return true;
}

// TODO(zhanglei): Return NbrIterator when refactoring the GraphInterface.
bool UpdateTransaction::AddEdge(label_t src_label, vid_t src_lid,
                                label_t dst_label, vid_t dst_lid,
                                label_t edge_label,
                                const std::vector<Property>& properties) {
  const auto& edge_table =
      cow_storage_->get_edge_table(src_label, dst_label, edge_label);
  if (edge_table.PropTableSize() >= edge_table.Capacity()) {
    auto new_capacity =
        edge_table.PropTableSize() < 4096
            ? 4096
            : edge_table.PropTableSize() + edge_table.PropTableSize() / 4;
    auto status = cow_storage_->EnsureCapacity(src_label, dst_label, edge_label,
                                               new_capacity);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to ensure space before insert edge: "
                 << status.ToString();
      return false;
    }
  }
  InsertEdgeRedo::Serialize(arc_, src_label, GetVertexId(src_label, src_lid),
                            dst_label, GetVertexId(dst_label, dst_lid),
                            edge_label, properties);
  op_num_ += 1;
  cow_storage_->AddEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                        properties, timestamp_, alloc_, true);
  return true;
}

bool UpdateTransaction::DeleteEdges(label_t src_label, vid_t src_lid,
                                    label_t dst_label, vid_t dst_lid,
                                    label_t edge_label) {
  if (!cow_storage_->IsValidLid(src_label, src_lid, timestamp_) ||
      !cow_storage_->IsValidLid(dst_label, dst_lid, timestamp_)) {
    THROW_RUNTIME_ERROR(
        "Source or destination vertex id is out of range or "
        "already deleted");
  }

  auto oe_edges = GetGenericOutgoingGraphView(src_label, dst_label, edge_label)
                      .get_edges(src_lid);
  auto ie_edges = GetGenericIncomingGraphView(dst_label, src_label, edge_label)
                      .get_edges(dst_lid);
  auto search_edge_prop_type =
      determine_search_prop_type(cow_storage_->schema().get_edge_properties(
          src_label, dst_label, edge_label));
  int32_t oe_offset = 0;
  for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
    if (it.get_vertex() == dst_lid) {
      auto ie_offset = fuzzy_search_offset_from_nbr_list(
          ie_edges, src_lid, it.get_data_ptr(), search_edge_prop_type);
      RemoveEdgeRedo::Serialize(
          arc_, src_label, GetVertexId(src_label, src_lid), dst_label,
          GetVertexId(dst_label, dst_lid), edge_label, oe_offset, ie_offset);
      op_num_ += 1;
      auto status = cow_storage_->DeleteEdge(src_label, src_lid, dst_label,
                                             dst_lid, edge_label, oe_offset,
                                             ie_offset, timestamp_, alloc_);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to delete edge: " << status.ToString();
        return false;
      }
    }
    oe_offset++;
  }

  return true;
}

bool UpdateTransaction::DeleteEdge(label_t src_label, vid_t src_lid,
                                   label_t dst_label, vid_t dst_lid,
                                   label_t edge_label, int32_t oe_offset,
                                   int32_t ie_offset) {
  if (!cow_storage_->IsValidLid(src_label, src_lid, timestamp_) ||
      !cow_storage_->IsValidLid(dst_label, dst_lid, timestamp_)) {
    THROW_RUNTIME_ERROR(
        "Source or destination vertex id is out of range or "
        "already deleted");
  }

  RemoveEdgeRedo::Serialize(arc_, src_label, GetVertexId(src_label, src_lid),
                            dst_label, GetVertexId(dst_label, dst_lid),
                            edge_label, oe_offset, ie_offset);
  op_num_ += 1;

  auto status = cow_storage_->DeleteEdge(src_label, src_lid, dst_label, dst_lid,
                                         edge_label, oe_offset, ie_offset,
                                         timestamp_, alloc_);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to delete edge: " << status.ToString();
    return false;
  }
  return true;
}

Property UpdateTransaction::GetVertexProperty(label_t label, vid_t lid,
                                              int col_id) const {
  auto col = cow_storage_->GetVertexPropertyColumn(label, col_id);
  if (!cow_storage_->IsValidLid(label, lid, timestamp_)) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Vertex lid is not valid in this transaction");
  }
  if (col == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Fail to find property column");
  }
  return col->get(lid);
}

Property UpdateTransaction::GetVertexId(label_t label, vid_t lid) const {
  return cow_storage_->GetOid(label, lid, timestamp_);
}

bool UpdateTransaction::GetVertexIndex(label_t label, const Property& id,
                                       vid_t& index) const {
  return cow_storage_->get_lid(label, id, index, timestamp_);
}

bool UpdateTransaction::UpdateVertexProperty(label_t label, vid_t lid,
                                             int col_id,
                                             const Property& value) {
  if (!cow_storage_->IsValidLid(label, lid, timestamp_)) {
    LOG(ERROR) << "Vertex lid " << lid << " of label "
               << cow_storage_->schema().get_vertex_label_name(label)
               << " is not valid in this transaction.";
    return false;
  }
  std::vector<DataType> types =
      cow_storage_->schema().get_vertex_properties(label);
  if (static_cast<size_t>(col_id) >= types.size()) {
    return false;
  }
  if (types[col_id].id() != value.type()) {
    return false;
  }
  UpdateVertexPropRedo::Serialize(arc_, label, GetVertexId(label, lid), col_id,
                                  value);

  auto status =
      cow_storage_->UpdateVertexProperty(label, lid, col_id, value, timestamp_);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to update vertex property: " << status.ToString();
    return false;
  }
  op_num_ += 1;
  return true;
}

bool UpdateTransaction::UpdateEdgeProperty(label_t src_label, vid_t src,
                                           label_t dst_label, vid_t dst,
                                           label_t edge_label, int32_t col_id,
                                           const Property& value) {
  if (!cow_storage_->IsValidLid(src_label, src, timestamp_) ||
      !cow_storage_->IsValidLid(dst_label, dst, timestamp_)) {
    THROW_RUNTIME_ERROR(
        "Source or destination vertex id is out of range or "
        "already deleted");
  }
  auto oe_edges = GetGenericOutgoingGraphView(src_label, dst_label, edge_label)
                      .get_edges(src);
  auto ie_edges = GetGenericIncomingGraphView(dst_label, src_label, edge_label)
                      .get_edges(dst);
  auto edge_prop_types = cow_storage_->schema().get_edge_properties(
      src_label, dst_label, edge_label);
  assert(col_id >= 0 && static_cast<size_t>(col_id) < edge_prop_types.size());
  auto search_prop_type = determine_search_prop_type(edge_prop_types);
  int32_t oe_offset = 0;
  for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
    if (it.get_vertex() == dst) {
      auto ie_offset = fuzzy_search_offset_from_nbr_list(
          ie_edges, src, it.get_data_ptr(), search_prop_type);
      UpdateEdgePropRedo::Serialize(arc_, src_label,
                                    GetVertexId(src_label, src), dst_label,
                                    GetVertexId(dst_label, dst), edge_label,
                                    oe_offset, ie_offset, col_id, value);
      op_num_ += 1;
      auto status = cow_storage_->UpdateEdgeProperty(
          src_label, src, dst_label, dst, edge_label, oe_offset, ie_offset,
          col_id, value, timestamp_, alloc_);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to update edge property: " << status.ToString();
        return false;
      }
    }
    oe_offset += 1;
  }

  return true;
}

bool UpdateTransaction::UpdateEdgeProperty(label_t src_label, vid_t src,
                                           label_t dst_label, vid_t dst,
                                           label_t edge_label,
                                           int32_t oe_offset, int32_t ie_offset,
                                           int32_t col_id,
                                           const Property& value) {
  if (!cow_storage_->IsValidLid(src_label, src, timestamp_) ||
      !cow_storage_->IsValidLid(dst_label, dst, timestamp_)) {
    THROW_RUNTIME_ERROR(
        "Source or destination vertex id is out of range or "
        "already deleted");
  }
  UpdateEdgePropRedo::Serialize(arc_, src_label, GetVertexId(src_label, src),
                                dst_label, GetVertexId(dst_label, dst),
                                edge_label, oe_offset, ie_offset, col_id,
                                value);
  op_num_ += 1;
  auto status = cow_storage_->UpdateEdgeProperty(
      src_label, src, dst_label, dst, edge_label, oe_offset, ie_offset, col_id,
      value, timestamp_, alloc_);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to update edge property: " << status.ToString();
    return false;
  }
  return true;
}

void UpdateTransaction::IngestWal(PropertyGraph& graph, uint32_t timestamp,
                                  char* data, size_t length, Allocator& alloc) {
  OutArchive arc;
  arc.SetSlice(data, length);
  while (!arc.Empty()) {
    OpType op_type;
    arc >> op_type;
    if (op_type == OpType::kCreateVertexType) {
      CreateVertexTypeParam redo = CreateVertexTypeRedo::Deserialize(arc);
      graph.CreateVertexType(redo);
    } else if (op_type == OpType::kCreateEdgeType) {
      const auto& redo = CreateEdgeTypeRedo::Deserialize(arc);
      graph.CreateEdgeType(redo);
    } else if (op_type == OpType::kInsertVertex) {
      InsertVertexRedo redo;
      arc >> redo;
      vid_t vid;
      auto& v_table = graph.get_vertex_table(redo.label);
      if (!graph.get_lid(redo.label, redo.oid, vid, timestamp) ||
          !graph.IsValidLid(redo.label, vid, timestamp)) {
        if (v_table.Size() >= v_table.Capacity()) {
          auto new_capacity = v_table.Size() < 4096
                                  ? 4096
                                  : v_table.Size() + v_table.Size() / 4;
          graph.EnsureCapacity(redo.label, new_capacity);
        }
        auto ret = graph.AddVertex(redo.label, redo.oid, redo.props, vid,
                                   timestamp, true);
        if (!ret.ok()) {
          THROW_STORAGE_EXCEPTION(
              "Failed to add vertex during WAL ingestion: " + ret.ToString());
        }
      }
    } else if (op_type == OpType::kInsertEdge) {
      InsertEdgeRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      graph.AddEdge(redo.src_label, src_vid, redo.dst_label, dst_vid,
                    redo.edge_label, redo.properties, timestamp, alloc, true);
    } else if (op_type == OpType::kUpdateVertexProp) {
      UpdateVertexPropRedo redo;
      arc >> redo;
      vid_t vid;
      CHECK(graph.get_lid(redo.label, redo.oid, vid, timestamp));
      graph.get_vertex_table(redo.label)
          .UpdateProperty(vid, redo.prop_id, redo.value, timestamp);
    } else if (op_type == OpType::kUpdateEdgeProp) {
      UpdateEdgePropRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      graph.UpdateEdgeProperty(redo.src_label, src_vid, redo.dst_label, dst_vid,
                               redo.edge_label, redo.oe_offset, redo.ie_offset,
                               redo.prop_id, redo.value, timestamp, alloc);
    } else if (op_type == OpType::kRemoveVertex) {
      RemoveVertexRedo redo;
      arc >> redo;
      vid_t vid;
      CHECK(graph.get_lid(redo.label, redo.oid, vid, timestamp));
      graph.DeleteVertex(redo.label, vid, timestamp, alloc);
    } else if (op_type == OpType::kRemoveEdge) {
      RemoveEdgeRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      graph.DeleteEdge(redo.src_label, src_vid, redo.dst_label, dst_vid,
                       redo.edge_label, redo.oe_offset, redo.ie_offset,
                       timestamp, alloc);
    } else if (op_type == OpType::kAddVertexProp) {
      auto config = AddVertexPropertiesRedo::Deserialize(arc);
      graph.AddVertexProperties(config);
    } else if (op_type == OpType::kAddEdgeProp) {
      auto config = AddEdgePropertiesRedo::Deserialize(arc);
      graph.AddEdgeProperties(config);
    } else if (op_type == OpType::kRenameVertexProp) {
      auto config = RenameVertexPropertiesRedo::Deserialize(arc);
      graph.RenameVertexProperties(config);
    } else if (op_type == OpType::kRenameEdgeProp) {
      auto config = RenameEdgePropertiesRedo::Deserialize(arc);
      graph.RenameEdgeProperties(config);
    } else if (op_type == OpType::kDeleteVertexProp) {
      auto config = DeleteVertexPropertiesRedo::Deserialize(arc);
      graph.DeleteVertexProperties(config);
    } else if (op_type == OpType::kDeleteEdgeProp) {
      auto config = DeleteEdgePropertiesRedo::Deserialize(arc);
      graph.DeleteEdgeProperties(config);
    } else if (op_type == OpType::kDeleteVertexType) {
      DeleteVertexTypeRedo redo;
      arc >> redo;
      graph.DeleteVertexType(redo.vertex_type);
    } else if (op_type == OpType::kDeleteEdgeType) {
      DeleteEdgeTypeRedo redo;
      arc >> redo;
      graph.DeleteEdgeType(redo.src_type, redo.dst_type, redo.edge_type);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION("Unexpected op_type: " +
                                    std::to_string(static_cast<int>(op_type)));
    }
  }
}

void UpdateTransaction::CreateCheckpoint() {
  // Create a checkpoint for the current graph. Expect no changes are made to
  // this transaction
  if (op_num_ != 0) {
    THROW_INTERNAL_EXCEPTION(
        "Checkpoint should be created in a update "
        "transaction without any updates");
  }
  cow_storage_->Dump();
}

bool UpdateTransaction::IsValidLid(label_t label, vid_t lid) const {
  return cow_storage_->IsValidLid(label, lid, timestamp_);
}

void UpdateTransaction::release() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    arc_.Clear();
    vm_.release_update_timestamp(timestamp_);
    timestamp_ = INVALID_TIMESTAMP;
    op_num_ = 0;
  }
}

void StorageTPUpdateInterface::CreateCheckpoint() { txn_.CreateCheckpoint(); }

Status StorageTPUpdateInterface::BatchAddVertices(
    label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) {
  LOG(ERROR) << "BatchAddVertices is not supported in TP mode currently.";
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "BatchAddVertices is not supported in TP mode currently.");
}

Status StorageTPUpdateInterface::BatchAddEdges(
    label_t src_label, label_t dst_label, label_t edge_label,
    std::shared_ptr<IRecordBatchSupplier> supplier) {
  LOG(ERROR) << "BatchAddEdges is not supported in TP mode currently.";
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "BatchAddEdges is not supported in TP mode currently.");
}

Status StorageTPUpdateInterface::BatchDeleteVertices(
    label_t v_label_id, const std::vector<vid_t>& vids) {
  for (vid_t lid : vids) {
    if (!txn_.DeleteVertex(v_label_id, lid)) {
      LOG(ERROR) << "Failed to delete vertex " << lid << " of label "
                 << v_label_id << " in batch request.";
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Failed to delete vertex " + std::to_string(lid));
    }
  }
  return Status::OK();
}

Status StorageTPUpdateInterface::BatchDeleteEdges(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::tuple<vid_t, vid_t>>& edges) {
  for (const auto& edge : edges) {
    vid_t src_lid = std::get<0>(edge);
    vid_t dst_lid = std::get<1>(edge);
    if (!txn_.DeleteEdges(src_v_label_id, src_lid, dst_v_label_id, dst_lid,
                          edge_label_id)) {
      LOG(ERROR) << "Failed to delete edge from vertex " << src_lid
                 << " to vertex " << dst_lid << " in batch request.";
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Failed to delete edge from vertex " +
                        std::to_string(src_lid) + " to vertex " +
                        std::to_string(dst_lid));
    }
  }
  return Status::OK();
}

Status StorageTPUpdateInterface::BatchDeleteEdges(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
    const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
  assert(oe_edges.size() == ie_edges.size());
  for (size_t i = 0; i < oe_edges.size(); ++i) {
    vid_t src_lid = oe_edges[i].first;
    vid_t dst_lid = ie_edges[i].first;
    int32_t oe_offset = oe_edges[i].second;
    int32_t ie_offset = ie_edges[i].second;
    if (!txn_.DeleteEdge(src_v_label_id, src_lid, dst_v_label_id, dst_lid,
                         edge_label_id, oe_offset, ie_offset)) {
      LOG(ERROR) << "Failed to delete edge from vertex " << src_lid
                 << " to vertex " << dst_lid << " in batch request.";
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Failed to delete edge from vertex " +
                        std::to_string(src_lid) + " to vertex " +
                        std::to_string(dst_lid));
    }
  }
  return Status::OK();
}

Status StorageTPUpdateInterface::CreateVertexType(
    const CreateVertexTypeParam& config) {
  return txn_.CreateVertexType(config);
}

Status StorageTPUpdateInterface::CreateEdgeType(
    const CreateEdgeTypeParam& config) {
  return txn_.CreateEdgeType(config);
}

Status StorageTPUpdateInterface::AddVertexProperties(
    const AddVertexPropertiesParam& config) {
  return txn_.AddVertexProperties(config);
}

Status StorageTPUpdateInterface::AddEdgeProperties(
    const AddEdgePropertiesParam& config) {
  return txn_.AddEdgeProperties(config);
}

Status StorageTPUpdateInterface::RenameVertexProperties(
    const RenameVertexPropertiesParam& config) {
  return txn_.RenameVertexProperties(config);
}
Status StorageTPUpdateInterface::RenameEdgeProperties(
    const RenameEdgePropertiesParam& config) {
  return txn_.RenameEdgeProperties(config);
}

Status StorageTPUpdateInterface::DeleteVertexProperties(
    const DeleteVertexPropertiesParam& config) {
  return txn_.DeleteVertexProperties(config);
}

Status StorageTPUpdateInterface::DeleteEdgeProperties(
    const DeleteEdgePropertiesParam& config) {
  return txn_.DeleteEdgeProperties(config);
}

Status StorageTPUpdateInterface::DeleteVertexType(
    const std::string& vertex_type_name) {
  return txn_.DeleteVertexType(vertex_type_name);
}

Status StorageTPUpdateInterface::DeleteEdgeType(const std::string& src_type,
                                                const std::string& dst_type,
                                                const std::string& edge_type) {
  return txn_.DeleteEdgeType(src_type, dst_type, edge_type);
}

}  // namespace neug
