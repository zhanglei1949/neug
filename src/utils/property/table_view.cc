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

#include "neug/utils/property/table_view.h"

#include "neug/utils/exception/exception.h"
#include "neug/utils/property/table.h"

namespace neug {

TableView::TableView(const Table& table) {
  col_names_ = table.column_names();
  col_id_map_ = {};
  for (int i = 0; i < static_cast<int>(col_names_.size()); ++i) {
    col_id_map_[col_names_[i]] = i;
  }

  // Extract column views at construction time
  size_t col_count = table.col_num();
  column_views_.reserve(col_count);
  for (size_t i = 0; i < col_count; ++i) {
    auto col = table.get_column_by_id(i);
    if (col) {
      column_views_.push_back(CreateRefColumn(*col));
    } else {
      column_views_.push_back(nullptr);
    }
  }

  size_ = table.size();
}

RefColumnBase* TableView::GetColumn(int col_id) const {
  if (col_id < 0 || col_id >= static_cast<int>(column_views_.size())) {
    return nullptr;
  }
  return column_views_[col_id].get();
}

RefColumnBase* TableView::GetColumn(const std::string& name) const {
  int col_id = get_column_id(name);
  if (col_id < 0) {
    return nullptr;
  }
  return GetColumn(col_id);
}

int TableView::get_column_id(const std::string& name) const {
  auto it = col_id_map_.find(name);
  if (it == col_id_map_.end()) {
    return -1;
  }
  return it->second;
}

void TableView::insert(size_t row_id, const std::vector<Property>& values) {
  if (values.size() != column_views_.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "TableView::insert values size mismatch: expected " +
        std::to_string(column_views_.size()) + ", got " +
        std::to_string(values.size()));
  }
  for (size_t i = 0; i < column_views_.size(); ++i) {
    if (column_views_[i]) {
      column_views_[i]->set(row_id, values[i]);
    }
  }
}

}  // namespace neug