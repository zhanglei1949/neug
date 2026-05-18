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
#include <unordered_map>
#include <vector>

#include "neug/utils/property/column.h"

namespace neug {

class Table;

/**
 * @brief Read+insert view of a Table, holding pre-constructed column views.
 *
 * TableView is the symmetric mirror of Table in the View layer. It does NOT
 * hold a reference to the underlying Table; instead, it holds:
 * - Column names and id map (copied)
 * - Pre-constructed RefColumnBase pointers (extracted at construction)
 *
 * This design allows TableView to be used independently after construction.
 * Insert is strict: no insert_safe option, throws if there isn't enough
 * reserved space — caller must EnsureCapacity through the owning storage.
 */
class TableView {
 public:
  /**
   * @brief Construct a TableView from a Table.
   *
   * Extracts all column views at construction time. After construction,
   * the view can be used without accessing the original Table.
   *
   * @param table The source Table to create view from
   */
  explicit TableView(const Table& table);

  TableView() = default;
  ~TableView() = default;

  TableView(const TableView&) = default;
  TableView(TableView&&) = default;
  TableView& operator=(const TableView&) = default;
  TableView& operator=(TableView&&) = default;

  /**
   * @brief Get a column view by column ID.
   *
   * @param col_id Column index (0-based)
   * @return Pointer to RefColumnBase, nullptr if col_id is invalid
   */
  RefColumnBase* GetColumn(int col_id) const;

  /**
   * @brief Get a column view by column name.
   *
   * @param name Column name
   * @return Pointer to RefColumnBase, nullptr if name not found
   */
  RefColumnBase* GetColumn(const std::string& name) const;

  /**
   * @brief Get number of columns.
   */
  size_t col_num() const { return column_views_.size(); }

  /**
   * @brief Get number of rows (size of first column).
   */
  size_t size() const { return size_; }

  /**
   * @brief Get column names.
   */
  const std::vector<std::string>& column_names() const { return col_names_; }

  /**
   * @brief Get column ID by name.
   *
   * @param name Column name
   * @return Column ID, -1 if not found
   */
  int get_column_id(const std::string& name) const;

  /**
   * @brief Strict insert: write @p values into row @p row_id by calling each
   *        column view's set(). Throws if any column lacks reserved space
   *        — capacity must be ensured beforehand at the storage level.
   *
   * @param row_id Target row index (must be < column size)
   * @param values One Property per column, in column-id order
   */
  void insert(size_t row_id, const std::vector<Property>& values);

 private:
  std::vector<std::string> col_names_;
  std::unordered_map<std::string, int> col_id_map_;
  std::vector<std::shared_ptr<RefColumnBase>> column_views_;
  size_t size_{0};
};

}  // namespace neug