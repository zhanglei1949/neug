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

#include "neug/storages/graph/edge_table_view.h"

#include "neug/utils/exception/exception.h"

namespace neug {

EdgeTableView::EdgeTableView(EdgeTable& et)
    : table_view_(et.table_ ? TableView(*et.table_) : TableView()),
      edge_schema_(et.meta_.get()),
      table_ptr_(et.table_.get()),
      out_csr_(et.out_csr_.get()),
      in_csr_(et.in_csr_.get()),
      table_idx_(&et.table_idx_),
      table_capacity_(&et.capacity_) {
}

CsrBaseView EdgeTableView::get_outgoing_view(timestamp_t ts) const {
  return out_csr_ ? out_csr_->get_generic_view(ts) : CsrBaseView();
}

CsrBaseView EdgeTableView::get_incoming_view(timestamp_t ts) const {
  return in_csr_ ? in_csr_->get_generic_view(ts) : CsrBaseView();
}

RefColumnBase* EdgeTableView::get_edge_property(int col_id) const {
  return table_view_.GetColumn(col_id);
}

EdgeDataAccessor EdgeTableView::get_edge_data_accessor(int col_id) const {
  if (!edge_schema_) {
    return EdgeDataAccessor();
  }
  if (col_id < 0 ||
      static_cast<size_t>(col_id) >= edge_schema_->properties.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Edge property column id out of range: " + std::to_string(col_id) +
        " (edge has " + std::to_string(edge_schema_->properties.size()) +
        " properties)");
  }
  if (!edge_schema_->is_bundled()) {
    if (table_ptr_) {
      auto col = table_ptr_->get_column_by_id(col_id);
      if (col) {
        return EdgeDataAccessor(edge_schema_->properties[col_id].id(),
                                col.get());
      }
    }
    return EdgeDataAccessor();
  } else {
    return EdgeDataAccessor(edge_schema_->properties[0].id(), nullptr);
  }
}

EdgeDataAccessor EdgeTableView::get_edge_data_accessor(
    const std::string& prop_name) const {
  if (!edge_schema_) {
    return EdgeDataAccessor();
  }
  auto prop_ind = edge_schema_->get_property_index(prop_name);
  if (prop_ind == -1) {
    THROW_INVALID_ARGUMENT_EXCEPTION("property " + prop_name +
                                     " not found in edge table, or deleted");
  }
  return get_edge_data_accessor(static_cast<int>(prop_ind));
}

size_t EdgeTableView::EdgeNum() const {
  return out_csr_ ? out_csr_->edge_num() : 0;
}

size_t EdgeTableView::PropTableSize() const {
  return table_idx_ ? table_idx_->load() : 0;
}

size_t EdgeTableView::Capacity() const {
  if (edge_schema_ && edge_schema_->is_bundled()) {
    return out_csr_ ? out_csr_->capacity() : 0;
  }
  return table_capacity_ ? table_capacity_->load() : 0;
}

int32_t EdgeTableView::AddEdge(vid_t src, vid_t dst,
                               const std::vector<Property>& properties,
                               timestamp_t ts, Allocator& alloc) {
  // Construct CsrBaseViews on-the-fly for the write timestamp.
  auto ov = get_outgoing_view(ts);
  auto iv = get_incoming_view(ts);

  const bool bundled = edge_schema_ && edge_schema_->is_bundled();
  if (bundled) {
    if (properties.size() > 1) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Bundled edge expects at most one inline property, got " +
          std::to_string(properties.size()));
    }
    Property data = properties.empty() ? Property() : properties[0];
    iv.put_generic_edge(dst, src, data, ts, alloc);
    return ov.put_generic_edge(src, dst, data, ts, alloc);
  }

  if (edge_schema_ && properties.size() != edge_schema_->properties.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Edge property count mismatch: expected " +
        std::to_string(edge_schema_->properties.size()) + ", got " +
        std::to_string(properties.size()));
  }

  if (!table_idx_ || !table_capacity_) {
    THROW_INTERNAL_EXCEPTION("EdgeTableView property-table state missing");
  }
  // Strict row-id allocation: bump the table_idx_ atomic and verify capacity.
  size_t row_id = table_idx_->fetch_add(1);
  if (row_id >= table_capacity_->load()) {
    THROW_STORAGE_EXCEPTION(
        "EdgeTableView property table out of reserved capacity (row " +
        std::to_string(row_id) + " >= cap " +
        std::to_string(table_capacity_->load()) +
        "); reserve at the EdgeTable level beforehand");
  }
  Property row_id_prop;
  row_id_prop.set_uint64(row_id);
  iv.put_generic_edge(dst, src, row_id_prop, ts, alloc);
  int32_t oe_offset = ov.put_generic_edge(src, dst, row_id_prop, ts, alloc);
  table_view_.insert(row_id, properties);
  return oe_offset;
}

}  // namespace neug
