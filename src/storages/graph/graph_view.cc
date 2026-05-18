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

#include "neug/storages/graph/graph_view.h"

namespace neug {

GraphView::GraphView(PropertyGraph& storage)
    : schema_(&storage.schema()) {
  // Sub-views are eagerly built. The view is timestamp-agnostic; timestamp
  // is passed per-call to methods that need MVCC visibility.
  size_t vertex_label_num = storage.schema().vertex_label_num();
  vertex_views_.reserve(vertex_label_num);
  for (label_t label = 0; label < vertex_label_num; ++label) {
    auto& vertex_table = storage.get_vertex_table(label);
    vertex_views_.emplace_back(vertex_table);
  }

  const auto& e_schemas = storage.schema().get_all_edge_schemas();
  for (const auto& [index, edge_schema] : e_schemas) {
    label_t src_label =
        storage.schema().get_vertex_label_id(edge_schema->src_label_name);
    label_t dst_label =
        storage.schema().get_vertex_label_id(edge_schema->dst_label_name);
    label_t edge_label =
        storage.schema().get_edge_label_id(edge_schema->edge_label_name);
    auto& edge_table = storage.get_edge_table(src_label, dst_label, edge_label);
    edge_views_.emplace(index, EdgeTableView(edge_table));
  }
}

GraphView::GraphView(const PropertyGraph& storage)
    : schema_(&storage.schema()) {
  // Read-only construction. The sub-view constructors take non-const refs
  // but only capture raw pointers for reading; const_cast is safe here
  // because write paths are never invoked in a read-only GraphView.
  auto& mutable_storage = const_cast<PropertyGraph&>(storage);
  size_t vertex_label_num = storage.schema().vertex_label_num();
  vertex_views_.reserve(vertex_label_num);
  for (label_t label = 0; label < vertex_label_num; ++label) {
    auto& vertex_table = mutable_storage.get_vertex_table(label);
    vertex_views_.emplace_back(vertex_table);
  }

  const auto& e_schemas = storage.schema().get_all_edge_schemas();
  for (const auto& [index, edge_schema] : e_schemas) {
    label_t src_label =
        storage.schema().get_vertex_label_id(edge_schema->src_label_name);
    label_t dst_label =
        storage.schema().get_vertex_label_id(edge_schema->dst_label_name);
    label_t edge_label =
        storage.schema().get_edge_label_id(edge_schema->edge_label_name);
    auto& edge_table =
        mutable_storage.get_edge_table(src_label, dst_label, edge_label);
    edge_views_.emplace(index, EdgeTableView(edge_table));
  }
}

const VertexTableView& GraphView::get_vertex_view(label_t label) const {
  if (label >= vertex_views_.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid vertex label: " +
                                     std::to_string(label));
  }
  return vertex_views_[label];
}

const EdgeTableView& GraphView::get_edge_view(label_t src_label,
                                              label_t dst_label,
                                              label_t edge_label) const {
  uint32_t index =
      schema_->generate_edge_label(src_label, dst_label, edge_label);
  auto it = edge_views_.find(index);
  if (it == edge_views_.end()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Edge table for edge label triplet not found");
  }
  return it->second;
}

bool GraphView::AddVertex(label_t label, const Property& id,
                          const std::vector<Property>& props, vid_t& vid,
                          timestamp_t ts) {
  if (label >= vertex_views_.size()) {
    return false;
  }
  return vertex_views_[label].AddVertex(id, props, vid, ts);
}

int32_t GraphView::AddEdge(label_t src_label, vid_t src, label_t dst_label,
                           vid_t dst, label_t edge_label,
                           const std::vector<Property>& properties,
                           timestamp_t ts, Allocator& alloc) {
  uint32_t index =
      schema_->generate_edge_label(src_label, dst_label, edge_label);
  auto it = edge_views_.find(index);
  if (it == edge_views_.end()) {
    return -1;
  }
  return it->second.AddEdge(src, dst, properties, ts, alloc);
}

CsrBaseView GraphView::GetGenericOutgoingView(label_t src_label,
                                              label_t dst_label,
                                              label_t edge_label,
                                              timestamp_t ts) const {
  return get_edge_view(src_label, dst_label, edge_label).get_outgoing_view(ts);
}

CsrBaseView GraphView::GetGenericIncomingView(label_t src_label,
                                              label_t dst_label,
                                              label_t edge_label,
                                              timestamp_t ts) const {
  return get_edge_view(src_label, dst_label, edge_label).get_incoming_view(ts);
}

EdgeDataAccessor GraphView::GetEdgeDataAccessor(label_t src_label,
                                                label_t dst_label,
                                                label_t edge_label,
                                                int prop_id) const {
  return get_edge_view(src_label, dst_label, edge_label)
      .get_edge_data_accessor(prop_id);
}

EdgeDataAccessor GraphView::GetEdgeDataAccessor(
    label_t src_label, label_t dst_label, label_t edge_label,
    const std::string& prop_name) const {
  return get_edge_view(src_label, dst_label, edge_label)
      .get_edge_data_accessor(prop_name);
}

}  // namespace neug
