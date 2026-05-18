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

#include "neug/transaction/insert_transaction.h"

#include "neug/utils/exception/exception.h"

#include <glog/logging.h>

#include <ostream>
#include <unordered_map>

#include "neug/storages/allocators.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

InsertTransaction::InsertTransaction(SnapshotStore::StorageSlot& slot,
                                     SnapshotStore& snapshot_store,
                                     Allocator& alloc, IWalWriter& logger,
                                     IVersionManager& vm,
                                     timestamp_t timestamp)
    : slot_(&slot),
      snapshot_store_(snapshot_store),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      timestamp_(timestamp) {
  arc_.Resize(sizeof(WalHeader));
}

InsertTransaction::~InsertTransaction() { Abort(); }

bool InsertTransaction::GetVertexIndex(label_t label, const Property& id,
                                       vid_t& index) const {
  const auto& vertex_view = slot_->view().get_vertex_view(label);
  if (vertex_view.get_index(id, index, timestamp_)) {
    return true;
  }
  if (added_vertices_.size() > label && added_vertices_[label] != nullptr &&
      added_vertices_[label]->get_index(id, index)) {
    index += added_vertices_base_[label];
    return true;
  }
  return false;
}

Property InsertTransaction::GetVertexId(label_t label, vid_t lid) const {
  if (added_vertices_.size() <= label || added_vertices_[label] == nullptr) {
    const auto& vertex_view = slot_->view().get_vertex_view(label);
    return vertex_view.GetOid(lid);
  }
  vid_t base = added_vertices_base_[label];
  if (lid >= base) {
    Property ret;
    CHECK(added_vertices_[label]->get_key(lid - base, ret));
    return ret;
  } else {
    const auto& vertex_view = slot_->view().get_vertex_view(label);
    return vertex_view.GetOid(lid);
  }
}

bool InsertTransaction::AddVertex(label_t label, const Property& id,
                                  const std::vector<Property>& props,
                                  vid_t& vid) {
  std::vector<DataType> types = slot_->view().schema().get_vertex_properties(label);
  if (types.size() != props.size()) {
    std::string label_name = slot_->view().schema().get_vertex_label_name(label);
    LOG(ERROR) << "Vertex [" << label_name
               << "] properties size not match, expected " << types.size()
               << ", but got " << props.size();
    return false;
  }
  int col_num = props.size();
  for (int col_i = 0; col_i != col_num; ++col_i) {
    auto& prop = props[col_i];
    if (prop.type() != types[col_i].id()) {
      std::string label_name = slot_->view().schema().get_vertex_label_name(label);
      LOG(ERROR) << "Vertex [" << label_name << "][" << col_i
                 << "] property type not match, expected "
                 << types[col_i].ToString() << ", but got "
                 << std::to_string(prop.type());
      return false;
    }
  }
  create_id_indexer_if_not_exists(label);
  if (!GetVertexIndex(label, id, vid)) {
    added_vertices_[label]->_add(id);
    vid = vertex_nums_[label] + added_vertices_base_[label];
    vertex_nums_[label]++;
    InsertVertexRedo::Serialize(arc_, label, id, props);
  }
  return true;
}

bool InsertTransaction::AddEdge(label_t src_label, vid_t src_vid,
                                label_t dst_label, vid_t dst_vid,
                                label_t edge_label,
                                const std::vector<Property>& properties) {
  const auto& src = GetVertexId(src_label, src_vid);
  const auto& dst = GetVertexId(dst_label, dst_vid);
  const auto& types =
      slot_->view().schema().get_edge_properties(src_label, dst_label, edge_label);
  if (properties.size() != types.size()) {
    std::string label_name = slot_->view().schema().get_edge_label_name(edge_label);
    LOG(ERROR) << "Edge property size not match for edge " << label_name
               << ", expected " << types.size() << ", got "
               << properties.size();
    return false;
  }
  for (size_t i = 0; i < properties.size(); ++i) {
    if (properties[i].type() != types[i].id()) {
      std::string label_name = slot_->view().schema().get_edge_label_name(edge_label);
      LOG(ERROR) << "Edge property " << label_name
                 << " type not match, expected " << types[i].ToString()
                 << ", got " << std::to_string(properties[i].type());
      return false;
    }
  }
  InsertEdgeRedo::Serialize(arc_, src_label, src, dst_label, dst, edge_label,
                            properties);
  return true;
}

bool InsertTransaction::Commit() {
  if (timestamp_ == INVALID_TIMESTAMP) {
    return true;
  }
  if (arc_.GetSize() == sizeof(WalHeader)) {
    vm_.release_insert_timestamp(timestamp_);
    clear();
    return true;
  }
  auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
  header->length = arc_.GetSize() - sizeof(WalHeader);
  header->type = 0;
  header->timestamp = timestamp_;

  if (!logger_.append(arc_.GetBuffer(), arc_.GetSize())) {
    LOG(ERROR) << "Failed to append wal log";
    Abort();
    return false;
  }
  // Apply WAL operations through the writable view. Capacity is assumed
  // to be sufficient; the strict insert path will throw if exhausted.
  IngestWal(slot_->mutable_view(), timestamp_,
            arc_.GetBuffer() + sizeof(WalHeader), header->length, alloc_);

  vm_.release_insert_timestamp(timestamp_);
  snapshot_store_.releaseSnapshot(*slot_);
  slot_ = nullptr;
  clear();
  return true;
}

void InsertTransaction::Abort() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    LOG(ERROR) << "aborting " << timestamp_ << "-th transaction (insert)";
    vm_.release_insert_timestamp(timestamp_);
    snapshot_store_.releaseSnapshot(*slot_);
    slot_ = nullptr;
    clear();
  }
}

timestamp_t InsertTransaction::timestamp() const { return timestamp_; }

void InsertTransaction::IngestWal(GraphView& view, uint32_t timestamp,
                                  char* data, size_t length, Allocator& alloc) {
  OutArchive arc;
  arc.SetSlice(data, length);
  while (!arc.Empty()) {
    OpType op_type;
    arc >> op_type;
    if (op_type == OpType::kInsertVertex) {
      InsertVertexRedo redo;
      arc >> redo;
      vid_t vid;
      if (!view.AddVertex(redo.label, redo.oid, redo.props, vid, timestamp)) {
        THROW_STORAGE_EXCEPTION("Failed to add vertex during WAL ingestion");
      }
    } else if (op_type == OpType::kInsertEdge) {
      InsertEdgeRedo redo;
      arc >> redo;
      vid_t src_lid = 0;
      vid_t dst_lid = 0;
      // Resolve src/dst lid via the view. The view's read_ts == timestamp
      // here — for the commit path that's the txn's own timestamp; for the
      // recovery path, the caller bumps timestamp per-WAL-unit which still
      // satisfies "vertices inserted earlier in this unit are visible".
      const auto& src_view = view.get_vertex_view(redo.src_label);
      const auto& dst_view = view.get_vertex_view(redo.dst_label);
      CHECK(src_view.get_index(redo.src, src_lid, timestamp));
      CHECK(dst_view.get_index(redo.dst, dst_lid, timestamp));
      view.AddEdge(redo.src_label, src_lid, redo.dst_label, dst_lid,
                   redo.edge_label, redo.properties, timestamp, alloc);
    } else {
      THROW_INTERNAL_EXCEPTION("Unexpected op-" +
                               std::to_string(static_cast<int>(op_type)));
    }
  }
}

void InsertTransaction::clear() {
  arc_.Clear();
  arc_.Resize(sizeof(WalHeader));
  added_vertices_.clear();
  added_vertices_base_.clear();
  vertex_nums_.clear();

  timestamp_ = INVALID_TIMESTAMP;
}

const Schema& InsertTransaction::schema() const { return slot_->view().schema(); }

void InsertTransaction::create_id_indexer_if_not_exists(label_t label) {
  if (label >= added_vertices_.size()) {
    added_vertices_base_.resize(label + 1, 0);
    vertex_nums_.resize(label + 1, 0);
    added_vertices_.resize(label + 1);
  }
  if (added_vertices_[label] == nullptr) {
    const auto& pks = slot_->view().schema().get_vertex_primary_key(label);
    DataTypeId type = std::get<0>(pks[0]).id();
    if (type == DataTypeId::kInt64) {
      added_vertices_[label] = std::make_unique<IdIndexer<int64_t, vid_t>>();
    } else if (type == DataTypeId::kUInt64) {
      added_vertices_[label] = std::make_unique<IdIndexer<uint64_t, vid_t>>();
    } else if (type == DataTypeId::kInt32) {
      added_vertices_[label] = std::make_unique<IdIndexer<int32_t, vid_t>>();
    } else if (type == DataTypeId::kUInt32) {
      added_vertices_[label] = std::make_unique<IdIndexer<uint32_t, vid_t>>();
    } else if (type == DataTypeId::kVarchar) {
      added_vertices_[label] =
          std::make_unique<IdIndexer<std::string_view, vid_t>>();
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "Only (u)int64/32 and string_view types for pk are supported, but "
          "got: " +
          std::to_string(type));
    }
    added_vertices_base_[label] = slot_->view().get_vertex_view(label).LidNum();
  }
}

Status StorageTPInsertInterface::BatchAddVertices(
    label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) {
  LOG(ERROR) << "BatchAddVertices is not supported in TP mode currently.";
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "BatchAddVertices is not supported in TP mode currently.");
}

Status StorageTPInsertInterface::BatchAddEdges(
    label_t src_label, label_t dst_label, label_t edge_label,
    std::shared_ptr<IRecordBatchSupplier> supplier) {
  LOG(ERROR) << "BatchAddEdges is not supported in TP mode currently.";
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "BatchAddEdges is not supported in TP mode currently.");
}

}  // namespace neug
