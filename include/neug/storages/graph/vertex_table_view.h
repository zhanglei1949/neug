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

#include <memory>
#include <string>
#include <vector>

#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/storages/graph/vertex_timestamp.h"
#include "neug/utils/indexers.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/table_view.h"

namespace neug {

/**
 * @brief View of a VertexTable supporting both read and (in-place) insert.
 *
 * Holds raw pointers to VertexTable's sub-components: indexer, v_ts,
 * vertex_schema, and a TableView wrapping the property table. There is no
 * back-pointer to VertexTable itself; insert is implemented in-place by
 * directly calling the underlying components.
 *
 * View does not own COW or capacity concerns. The caller is responsible for
 * ensuring the underlying buffers are exclusive (via PG-level operations
 * like EnsureCapacity which still trigger ensureInsertReady) before the
 * view starts mutating them.
 */
class VertexTableView {
 public:
  explicit VertexTableView(VertexTable& vt);

  VertexTableView() = default;
  ~VertexTableView() = default;

  VertexTableView(const VertexTableView&) = default;
  VertexTableView(VertexTableView&&) = default;
  VertexTableView& operator=(const VertexTableView&) = default;
  VertexTableView& operator=(VertexTableView&&) = default;

  // --- Read methods ---

  VertexSet GetVertexSet(timestamp_t ts) const;

  RefColumnBase* GetPropertyColumn(const std::string& prop) const;
  RefColumnBase* GetPropertyColumn(int col_id) const;
  std::shared_ptr<RefColumnBase> GetPropertyColumnShared(
      const std::string& prop) const;

  const IndexerType* indexer() const { return indexer_; }
  const VertexTimestamp* vertex_timestamp() const { return v_ts_; }

  bool get_index(const Property& oid, vid_t& lid, timestamp_t ts) const;
  Property GetOid(vid_t lid) const;
  bool IsValidLid(vid_t lid, timestamp_t ts) const;

  size_t LidNum() const;
  size_t Capacity() const;

  const VertexSchema* vertex_schema() const { return vertex_schema_; }
  const TableView& table_view() const { return table_view_; }

  // --- Insert (strict, in-place) ---

  /**
   * @brief Strict add-vertex. Throws if there isn't enough reserved capacity
   *        in indexer / v_ts / property columns. Caller must reserve at the
   *        owning VertexTable level before invoking. Handles re-insertion of
   *        a previously-deleted vertex by reusing the existing vid.
   */
  bool AddVertex(const Property& id, const std::vector<Property>& props,
                 vid_t& vid, timestamp_t ts);

 private:
  IndexerType* indexer_{nullptr};
  VertexTimestamp* v_ts_{nullptr};
  const VertexSchema* vertex_schema_{nullptr};
  TableView table_view_;
};

}  // namespace neug
