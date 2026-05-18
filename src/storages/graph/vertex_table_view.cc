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

#include "neug/storages/graph/vertex_table_view.h"

#include "neug/utils/exception/exception.h"

namespace neug {

VertexTableView::VertexTableView(VertexTable& vt)
    : indexer_(&vt.get_indexer()),
      v_ts_(vt.v_ts_.get()),
      vertex_schema_(vt.vertex_schema_.get()),
      table_view_(vt.table_ ? TableView(*vt.table_) : TableView()) {}

VertexSet VertexTableView::GetVertexSet(timestamp_t ts) const {
  return VertexSet(LidNum(), *v_ts_, ts);
}

RefColumnBase* VertexTableView::GetPropertyColumn(const std::string& prop) const {
  if (vertex_schema_) {
    auto pk = vertex_schema_->primary_keys[0];
    if (prop == std::get<1>(pk)) {
      return CreateRefColumn(indexer_->get_keys()).get();
    }
  }
  return table_view_.GetColumn(prop);
}

RefColumnBase* VertexTableView::GetPropertyColumn(int col_id) const {
  return table_view_.GetColumn(col_id);
}

std::shared_ptr<RefColumnBase> VertexTableView::GetPropertyColumnShared(
    const std::string& prop) const {
  if (vertex_schema_) {
    auto pk = vertex_schema_->primary_keys[0];
    if (prop == std::get<1>(pk)) {
      return CreateRefColumn(indexer_->get_keys());
    }
  }
  int col_id = table_view_.get_column_id(prop);
  if (col_id < 0) {
    return nullptr;
  }
  auto col = table_view_.GetColumn(col_id);
  if (!col) {
    return nullptr;
  }
  return std::shared_ptr<RefColumnBase>(std::shared_ptr<void>(), col);
}

bool VertexTableView::get_index(const Property& oid, vid_t& lid,
                                timestamp_t ts) const {
  if (!indexer_) {
    return false;
  }
  vid_t tmp_lid;
  if (!indexer_->get_index(oid, tmp_lid)) {
    return false;
  }
  if (v_ts_ && !v_ts_->IsVertexValid(tmp_lid, ts)) {
    return false;
  }
  lid = tmp_lid;
  return true;
}

Property VertexTableView::GetOid(vid_t lid) const {
  return indexer_ ? indexer_->get_key(lid) : Property();
}

bool VertexTableView::IsValidLid(vid_t lid, timestamp_t ts) const {
  if (!indexer_ || lid >= indexer_->size()) {
    return false;
  }
  return v_ts_ ? v_ts_->IsVertexValid(lid, ts) : true;
}

size_t VertexTableView::LidNum() const { return indexer_ ? indexer_->size() : 0; }

size_t VertexTableView::Capacity() const {
  return indexer_ ? indexer_->capacity() : 0;
}

bool VertexTableView::AddVertex(const Property& id,
                                const std::vector<Property>& props, vid_t& vid,
                                timestamp_t ts) {
  if (!indexer_ || !v_ts_) {
    THROW_INTERNAL_EXCEPTION("VertexTableView is not initialized");
  }
  // Existing-id path: reject if currently valid; revive if previously deleted.
  vid_t existing_vid;
  if (NEUG_UNLIKELY(indexer_->get_index(id, existing_vid))) {
    if (NEUG_UNLIKELY(v_ts_->IsVertexValid(existing_vid, ts))) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex with id " + id.to_string() +
                                       " already exists with lid " +
                                       std::to_string(existing_vid));
    }
    vid = existing_vid;
  } else {
    // LFIndexer::insert with insert_safe=false throws on capacity exhaustion
    // (caller is expected to have reserved capacity at the VertexTable level).
    vid = indexer_->insert(id, /*insert_safe=*/false);
  }
  v_ts_->InsertVertex(vid, ts);
  table_view_.insert(vid, props);
  return true;
}

}  // namespace neug
