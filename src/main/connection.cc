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

#include "neug/main/connection.h"

#include <exception>

#include "neug/main/execution_slot.h"
#include "neug/utils/access_mode.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/load_profiler.h"
#include "neug/utils/yaml_utils.h"

namespace neug {

Connection::Connection(std::unique_ptr<ExecutionSlot> execution_slot,
                       CloseCallback on_close)
    : execution_slot_(std::move(execution_slot)),
      on_close_(std::move(on_close)) {
  CHECK(execution_slot_ != nullptr);
}

Connection::~Connection() { Close(); }

std::string Connection::GetSchema() const {
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot get schema.";
    THROW_RUNTIME_ERROR("Connection is closed, cannot get schema.");
  }
  if (transaction_context_.IsRollbackOnly()) {
    if (transaction_context_.IsBulkLoad()) {
      THROW_TX_STATE_CONFLICT(
          "Bulk-load session failed; RollbackBulkLoad() is required before "
          "GetSchema.");
    }
    THROW_TX_STATE_CONFLICT(
        "Transaction is rollback-only; Rollback() is required before "
        "GetSchema.");
  }
  if (transaction_context_.IsActive()) {
    auto yaml = transaction_context_.schema().to_yaml();
    return get_json_string_from_yaml(yaml.value()).value();
  }
  return execution_slot_->GetSchema();
}

void Connection::Close() {
  if (is_closed_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Connection is already closed.";
    return;
  }
  LOG(INFO) << "Closing connection.";

  if (transaction_context_.IsBulkLoad()) {
    FinishBulkLoadProfile();
  }
  transaction_context_.Rollback();

  // Clean up all temporary schemas created through embedded execution.
  // This is safe to do globally because LOAD AS is only supported in
  // READ_WRITE mode, and ConnectionManager enforces that at most ONE
  // read-write connection exists at a time. Therefore, all temporary
  // labels in the schema must belong to this connection.
  execution_slot_->ClearTemporarySchema();
  execution_slot_.reset();
  is_closed_.store(true, std::memory_order_release);

  auto on_close = std::move(on_close_);
  if (on_close) {
    on_close(this);
  }
}

Status Connection::BeginTransaction(TransactionMode mode) {
  if (IsClosed()) {
    return Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed.");
  }
  if (transaction_context_.HasOwner()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "A transaction or bulk-load session is already active.");
  }

  try {
    switch (mode) {
    case TransactionMode::kReadOnly:
      transaction_context_.Begin(
          execution_slot_->BeginSnapshotReadTransaction());
      return Status::OK();
    case TransactionMode::kReadWrite: {
      auto transaction = execution_slot_->BeginCurrentCowWriteTransaction();
      if (!transaction) {
        return transaction.error();
      }
      transaction_context_.Begin(std::move(transaction).value());
      return Status::OK();
    }
    }
  } catch (const std::exception& e) {
    return Status::InternalError(
        std::string("Failed to begin explicit transaction: ") + e.what());
  } catch (...) {
    return Status::InternalError("Failed to begin explicit transaction");
  }
  return Status(StatusCode::ERR_INVALID_ARGUMENT,
                "Unsupported explicit transaction mode.");
}

Status Connection::BeginBulkLoad() {
  if (IsClosed()) {
    return Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed.");
  }
  if (transaction_context_.HasOwner()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "A transaction or bulk-load session is already active.");
  }

  bulk_load_started_at_ = std::chrono::steady_clock::now();
  try {
    auto transaction = execution_slot_->BeginCurrentCowWriteTransaction();
    if (!transaction) {
      bulk_load_started_at_.reset();
      if (transaction.error().error_code() == StatusCode::ERR_NOT_SUPPORTED ||
          transaction.error().error_code() ==
              StatusCode::ERR_INVALID_ARGUMENT) {
        return Status(transaction.error().error_code(),
                      "Bulk load is supported only in embedded read-write "
                      "mode.");
      }
      return transaction.error();
    }
    transaction_context_.BeginBulkLoad(std::move(transaction).value());
    return Status::OK();
  } catch (const std::exception& e) {
    bulk_load_started_at_.reset();
    return Status::InternalError(std::string("Failed to begin bulk load: ") +
                                 e.what());
  } catch (...) {
    bulk_load_started_at_.reset();
    return Status::InternalError("Failed to begin bulk load");
  }
}

result<QueryResult> Connection::ExecuteBulkLoadQuery(
    const std::string& query_string, const std::string& access_mode,
    const rapidjson::Value& parameters) {
  if (IsClosed()) {
    RETURN_ERROR(
        Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed."));
  }
  if (!transaction_context_.IsBulkLoad()) {
    RETURN_ERROR(Status(StatusCode::ERR_TX_STATE_CONFLICT,
                        "No bulk-load session is active."));
  }
  if (transaction_context_.IsRollbackOnly()) {
    RETURN_ERROR(Status(StatusCode::ERR_TX_STATE_CONFLICT,
                        "Bulk-load session failed; RollbackBulkLoad() is "
                        "required."));
  }

  AccessMode requested_mode;
  try {
    requested_mode = ParseAccessMode(access_mode);
  } catch (...) {
    transaction_context_.AbortAndMarkRollbackOnly();
    throw;
  }
  return execution_slot_->ExecuteBulkLoadQuery(query_string, requested_mode,
                                               parameters, /*num_threads=*/0,
                                               transaction_context_);
}

Status Connection::CommitBulkLoad() {
  if (IsClosed()) {
    return Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed.");
  }
  if (!transaction_context_.IsBulkLoad()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "No bulk-load session is active.");
  }
  if (transaction_context_.IsRollbackOnly()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "Bulk-load session failed; RollbackBulkLoad() is required.");
  }
  auto status = execution_slot_->CommitBulkLoad(transaction_context_);
  if (status.ok()) {
    FinishBulkLoadProfile();
  }
  return status;
}

Status Connection::RollbackBulkLoad() {
  if (IsClosed()) {
    return Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed.");
  }
  if (!transaction_context_.IsBulkLoad()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "No bulk-load session is active.");
  }
  transaction_context_.Rollback();
  FinishBulkLoadProfile();
  return Status::OK();
}

Status Connection::Commit() {
  if (IsClosed()) {
    return Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed.");
  }
  if (transaction_context_.IsBulkLoad()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  transaction_context_.IsRollbackOnly()
                      ? "Bulk-load session failed; RollbackBulkLoad() is "
                        "required."
                      : "Bulk-load sessions must use CommitBulkLoad().");
  }
  if (transaction_context_.IsRollbackOnly()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "Transaction is rollback-only; Rollback() is required.");
  }
  if (!transaction_context_.IsActive()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "No explicit transaction is active.");
  }
  return transaction_context_.Commit();
}

Status Connection::Rollback() {
  if (IsClosed()) {
    return Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed.");
  }
  if (!transaction_context_.HasOwner()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "No explicit transaction is active.");
  }
  if (transaction_context_.IsBulkLoad()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "Bulk-load sessions must use RollbackBulkLoad().");
  }
  transaction_context_.Rollback();
  return Status::OK();
}

result<QueryResult> Connection::Query(const std::string& query_string,
                                      const std::string& access_mode,
                                      const rapidjson::Value& parameters) {
  VLOG(1) << "Query: " << query_string;
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot execute query.";
    RETURN_ERROR(
        Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed."));
  }
  if (transaction_context_.IsRollbackOnly()) {
    if (transaction_context_.IsBulkLoad()) {
      RETURN_ERROR(Status(StatusCode::ERR_TX_STATE_CONFLICT,
                          "Bulk-load session failed; RollbackBulkLoad() is "
                          "required."));
    }
    RETURN_ERROR(
        Status(StatusCode::ERR_TX_STATE_CONFLICT,
               "Transaction is rollback-only; Rollback() is required."));
  }
  if (transaction_context_.IsActive()) {
    if (transaction_context_.IsBulkLoad()) {
      RETURN_ERROR(
          Status(StatusCode::ERR_TX_STATE_CONFLICT,
                 "Bulk-load sessions must use ExecuteBulkLoadQuery()."));
    }
    AccessMode requested_mode;
    try {
      requested_mode = ParseAccessMode(access_mode);
    } catch (...) {
      transaction_context_.AbortAndMarkRollbackOnly();
      throw;
    }
    return execution_slot_->ExecuteQueryInTransaction(
        query_string, requested_mode, parameters, /*num_threads=*/0,
        transaction_context_);
  }
  return execution_slot_->ExecuteQuery(query_string, access_mode, parameters);
}

void Connection::FinishBulkLoadProfile() noexcept {
  if (!bulk_load_started_at_) {
    return;
  }
  const auto elapsed = std::chrono::duration<double>(
      std::chrono::steady_clock::now() - *bulk_load_started_at_);
  try {
    profiling::LoadProfiler::Instance().Record("bulk_load.session.total",
                                               elapsed.count());
  } catch (...) {
    // Profiling must not interfere with commit, rollback, or connection close.
  }
  bulk_load_started_at_.reset();
}

}  // namespace neug
