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

#include <unordered_map>
#include <vector>

#include "neug/storages/allocators.h"
#include "neug/storages/graph/edge_table_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_table_view.h"
#include "neug/utils/property/property.h"

namespace neug {

/**
 * @brief View of a PropertyGraph supporting both read and (strict) insert.
 *
 * GraphView is the top-level view in the View layer, mirroring PropertyGraph.
 * It holds pre-constructed VertexTableView per vertex label, EdgeTableView
 * per edge triplet, and a schema pointer. There is no read-only vs writable
 * distinction at this level: every GraphView is potentially-writable. Read
 * paths enforce read-only access at the smart-pointer level (the slot's
 * cached view is held as `shared_ptr<const GraphView>`).
 *
 * Insert is strict (no insert_safe). Allocator is supplied per AddEdge call
 * rather than stored on the view, decoupling view (snapshot-scoped) from
 * allocator (transaction-scoped).
 */
class GraphView {
 public:
  /**
   * @brief Construct a GraphView.
   *
   * Sub-views (VertexTableView, EdgeTableView) are eagerly constructed.
   * The view is timestamp-agnostic; timestamp is passed per-call to methods
   * that need MVCC visibility.
   *
   * @param storage The source PropertyGraph
   */
  explicit GraphView(PropertyGraph& storage);
  explicit GraphView(const PropertyGraph& storage);

  GraphView() = default;
  ~GraphView() = default;

  GraphView(const GraphView&) = default;
  GraphView(GraphView&&) = default;
  GraphView& operator=(const GraphView&) = default;
  GraphView& operator=(GraphView&&) = default;

  // --- Read methods ---

  /**
   * @brief Get vertex view by label.
   */
  const VertexTableView& get_vertex_view(label_t label) const;

  /**
   * @brief Get edge view by triplet (src_label, dst_label, edge_label).
   */
  const EdgeTableView& get_edge_view(label_t src_label, label_t dst_label,
                                      label_t edge_label) const;

  /**
   * @brief Get schema.
   */
  const Schema& schema() const { return *schema_; }

  /**
   * @brief Get number of vertex labels.
   */
  size_t vertex_label_num() const { return vertex_views_.size(); }

  /**
   * @brief Get number of edge triplet entries.
   */
  size_t edge_view_num() const { return edge_views_.size(); }

  // --- Insert methods (append-only, strict) ---

  /**
   * @brief Mutable vertex view accessor (for capacity reservation).
   */
  VertexTableView& get_vertex_view_mut(label_t label);

  /**
   * @brief Mutable edge view accessor (for capacity reservation).
   */
  EdgeTableView& get_edge_view_mut(label_t src_label, label_t dst_label,
                                   label_t edge_label);

  /**
   * @brief Strict add-vertex (no insert_safe). Caller must EnsureCapacity
   *        on the relevant vertex view beforehand; throws on overflow.
   */
  bool AddVertex(label_t label, const Property& id,
                 const std::vector<Property>& props, vid_t& vid,
                 timestamp_t ts);

  /**
   * @brief Strict add-edge (no insert_safe). Caller must EnsureCapacity on
   *        the relevant edge view beforehand; throws on overflow.
   *
   * @param alloc Per-call allocator for adjacency-list growth.
   */
  int32_t AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                  label_t edge_label, const std::vector<Property>& properties,
                  timestamp_t ts, Allocator& alloc);

  // --- Convenience methods matching PropertyGraph API ---

  /**
   * @brief Get outgoing edge view.
   */
  CsrBaseView GetGenericOutgoingView(label_t src_label, label_t dst_label,
                                      label_t edge_label,
                                      timestamp_t ts) const;

  /**
   * @brief Get incoming edge view.
   */
  CsrBaseView GetGenericIncomingView(label_t src_label, label_t dst_label,
                                      label_t edge_label,
                                      timestamp_t ts) const;

  /**
   * @brief Get edge data accessor.
   */
  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                        label_t edge_label, int prop_id) const;

  /**
   * @brief Get edge data accessor by property name.
   */
  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                        label_t edge_label,
                                        const std::string& prop_name) const;

 private:
  const Schema* schema_{nullptr};
  std::vector<VertexTableView> vertex_views_;
  std::unordered_map<uint32_t, EdgeTableView> edge_views_;
};

}  // namespace neug