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

#include <atomic>
#include <string>
#include <vector>

#include "neug/storages/allocators.h"
#include "neug/storages/csr/csr_base_view.h"
#include "neug/storages/graph/edge_table.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/table_view.h"

namespace neug {

/**
 * @brief View of an EdgeTable supporting both read and (in-place) insert.
 *
 * Holds GenericView × 2 (outgoing/incoming) for CSR access, a TableView for
 * unbundled property columns, plus raw pointers to ancillary state
 * (edge_schema, table_idx_, capacity_, table_ptr_, out_csr_). No back-pointer
 * to EdgeTable itself.
 *
 * Insert is in-place: composes via outgoing_view_.put_edge +
 * incoming_view_.put_edge + (unbundled-only) table_view_.insert. View does
 * not handle COW or capacity reservation; caller is responsible for ensuring
 * the underlying buffers are exclusive and sized in advance.
 */
class EdgeTableView {
 public:
  explicit EdgeTableView(EdgeTable& et);

  EdgeTableView() = default;
  ~EdgeTableView() = default;

  EdgeTableView(const EdgeTableView&) = default;
  EdgeTableView(EdgeTableView&&) = default;
  EdgeTableView& operator=(const EdgeTableView&) = default;
  EdgeTableView& operator=(EdgeTableView&&) = default;

  // --- Read methods ---

  CsrBaseView get_outgoing_view(timestamp_t ts) const;
  CsrBaseView get_incoming_view(timestamp_t ts) const;

  RefColumnBase* get_edge_property(int col_id) const;

  EdgeDataAccessor get_edge_data_accessor(int col_id) const;
  EdgeDataAccessor get_edge_data_accessor(const std::string& prop_name) const;

  size_t EdgeNum() const;

  /// Next-row index into the unbundled property table (0 for bundled).
  size_t PropTableSize() const;
  /// Reserved capacity of the unbundled property table (0 for bundled).
  size_t Capacity() const;

  const EdgeSchema* edge_schema() const { return edge_schema_; }
  const TableView& table_view() const { return table_view_; }

  // --- Insert (strict, in-place) ---

  /**
   * @brief Strict add-edge. Throws if the unbundled property table is out
   *        of reserved capacity. Caller must reserve via the underlying
   *        EdgeTable beforehand.
   */
  int32_t AddEdge(vid_t src, vid_t dst,
                  const std::vector<Property>& properties, timestamp_t ts,
                  Allocator& alloc);

 private:
  TableView table_view_;
  const EdgeSchema* edge_schema_{nullptr};
  const Table* table_ptr_{nullptr};   // For EdgeDataAccessor (unbundled column lookups)
  const CsrBase* out_csr_{nullptr};   // For EdgeNum / get_outgoing_view
  const CsrBase* in_csr_{nullptr};    // For get_incoming_view

  // Atomics owned by the underlying EdgeTable; we hold raw pointers to bump
  // / read them without keeping a back-pointer to EdgeTable.
  std::atomic<uint64_t>* table_idx_{nullptr};
  std::atomic<uint64_t>* table_capacity_{nullptr};
};

}  // namespace neug
