/** Copyright 2020 Alibaba Group Holding Limited. */

#include "neug/c_api/neug.h"

#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/utils/result.h"
#include "rapidjson/document.h"

struct neug_database {
  std::unique_ptr<neug::NeugDB> database;
};

struct neug_connection {
  std::shared_ptr<neug::Connection> connection;
};

namespace {

constexpr int32_t kOk = 0;
constexpr int32_t kInvalidArgument = -1;
constexpr int32_t kInternalError = -2;

// NeuG currently uses process-global embedded runtime state during database
// open, query execution, and close. Keep the narrow C ABI safe for callers that
// manage more than one project database until that state is made per-instance.
std::mutex& c_api_mutex() {
  static std::mutex mutex;
  return mutex;
}

void set_error(neug_error_t* error, int32_t code, const std::string& message) {
  if (error == nullptr) {
    return;
  }
  error->code = code;
  error->message = static_cast<char*>(std::malloc(message.size() + 1));
  if (error->message != nullptr) {
    std::memcpy(error->message, message.data(), message.size());
    error->message[message.size()] = '\0';
  }
}

int32_t fail(neug_error_t* error, int32_t code, const std::string& message) {
  set_error(error, code, message);
  return code;
}

int32_t fail(neug_error_t* error, const neug::Status& status) {
  const auto code = static_cast<int32_t>(status.error_code());
  return fail(error, code == kOk ? kInternalError : code,
              status.error_message());
}

void initialize_outputs(neug_error_t* error) {
  if (error != nullptr) {
    error->code = kOk;
    error->message = nullptr;
  }
}

template <typename Operation>
int32_t protect(neug_error_t* error, Operation&& operation) noexcept {
  initialize_outputs(error);
  try {
    operation();
    return kOk;
  } catch (const std::exception& exception) {
    return fail(error, kInternalError, exception.what());
  } catch (...) {
    return fail(error, kInternalError, "Unknown NeuG C API failure");
  }
}

bool valid_connection(const neug_connection_t* connection) {
  return connection != nullptr && connection->connection != nullptr &&
         !connection->connection->IsClosed();
}

int32_t transaction_status(neug_error_t* error, const neug::Status& status) {
  if (status.ok()) {
    return kOk;
  }
  return fail(error, status);
}

}  // namespace

extern "C" {

uint32_t neug_c_api_version(void) { return 1; }

int32_t neug_database_open(const char* path, int32_t max_threads,
                           int32_t read_only, int32_t checkpoint_on_close,
                           neug_database_t** out_database,
                           neug_error_t* out_error) {
  if (out_database != nullptr) {
    *out_database = nullptr;
  }
  if (path == nullptr || path[0] == '\0' || out_database == nullptr ||
      max_threads < 0) {
    initialize_outputs(out_error);
    return fail(
        out_error, kInvalidArgument,
        "path, non-negative max_threads, and out_database are required");
  }
  std::lock_guard<std::mutex> lock(c_api_mutex());
  return protect(out_error, [&] {
    auto handle = std::make_unique<neug_database>();
    handle->database = std::make_unique<neug::NeugDB>();
    const auto mode =
        read_only != 0 ? neug::DBMode::READ_ONLY : neug::DBMode::READ_WRITE;
    if (!handle->database->Open(path, max_threads, mode, "gopt",
                                checkpoint_on_close != 0)) {
      throw std::runtime_error("NeuG database open returned false");
    }
    *out_database = handle.release();
  });
}

int32_t neug_database_close(neug_database_t* database,
                            neug_error_t* out_error) {
  if (database == nullptr || database->database == nullptr) {
    initialize_outputs(out_error);
    return fail(out_error, kInvalidArgument, "database handle is required");
  }
  std::lock_guard<std::mutex> lock(c_api_mutex());
  return protect(out_error, [&] {
    database->database->Close();
    delete database;
  });
}

int32_t neug_database_connect(neug_database_t* database,
                              neug_connection_t** out_connection,
                              neug_error_t* out_error) {
  if (out_connection != nullptr) {
    *out_connection = nullptr;
  }
  if (database == nullptr || database->database == nullptr ||
      out_connection == nullptr) {
    initialize_outputs(out_error);
    return fail(out_error, kInvalidArgument,
                "database and out_connection are required");
  }
  std::lock_guard<std::mutex> lock(c_api_mutex());
  return protect(out_error, [&] {
    auto handle = std::make_unique<neug_connection>();
    handle->connection = database->database->Connect();
    if (handle->connection == nullptr) {
      throw std::runtime_error("NeuG connection creation returned null");
    }
    *out_connection = handle.release();
  });
}

int32_t neug_connection_close(neug_connection_t* connection,
                              neug_error_t* out_error) {
  if (connection == nullptr || connection->connection == nullptr) {
    initialize_outputs(out_error);
    return fail(out_error, kInvalidArgument, "connection handle is required");
  }
  std::lock_guard<std::mutex> lock(c_api_mutex());
  return protect(out_error, [&] {
    connection->connection->Close();
    delete connection;
  });
}

int32_t neug_connection_execute(neug_connection_t* connection,
                                const char* query, const char* access_mode,
                                const char* parameters_json,
                                neug_buffer_t* out_result,
                                neug_error_t* out_error) {
  if (out_result != nullptr) {
    out_result->data = nullptr;
    out_result->len = 0;
  }
  if (!valid_connection(connection) || query == nullptr ||
      out_result == nullptr) {
    initialize_outputs(out_error);
    return fail(out_error, kInvalidArgument,
                "open connection, query, and out_result are required");
  }
  std::lock_guard<std::mutex> lock(c_api_mutex());
  initialize_outputs(out_error);
  try {
    rapidjson::Document parameters(rapidjson::kObjectType);
    if (parameters_json != nullptr && parameters_json[0] != '\0') {
      parameters.Parse(parameters_json);
      if (parameters.HasParseError() || !parameters.IsObject()) {
        return fail(out_error, kInvalidArgument,
                    "parameters_json must be a JSON object");
      }
    }
    auto result = connection->connection->Query(
        query, access_mode == nullptr ? "" : access_mode, parameters);
    if (!result) {
      return fail(out_error, result.error());
    }
    std::string serialized = result.value().Serialize();
    if (!serialized.empty()) {
      out_result->data = static_cast<uint8_t*>(std::malloc(serialized.size()));
      if (out_result->data == nullptr) {
        return fail(out_error, kInternalError,
                    "Failed to allocate query result buffer");
      }
      std::memcpy(out_result->data, serialized.data(), serialized.size());
      out_result->len = serialized.size();
    }
    return kOk;
  } catch (const std::exception& exception) {
    neug_buffer_free(out_result);
    return fail(out_error, kInternalError, exception.what());
  } catch (...) {
    neug_buffer_free(out_result);
    return fail(out_error, kInternalError, "Unknown NeuG query failure");
  }
}

int32_t neug_connection_begin(neug_connection_t* connection, int32_t read_only,
                              neug_error_t* out_error) {
  std::lock_guard<std::mutex> lock(c_api_mutex());
  initialize_outputs(out_error);
  if (!valid_connection(connection)) {
    return fail(out_error, kInvalidArgument, "open connection is required");
  }
  try {
    return transaction_status(
        out_error, connection->connection->BeginTransaction(
                       read_only != 0 ? neug::TransactionMode::kReadOnly
                                      : neug::TransactionMode::kReadWrite));
  } catch (const std::exception& exception) {
    return fail(out_error, kInternalError, exception.what());
  }
}

int32_t neug_connection_commit(neug_connection_t* connection,
                               neug_error_t* out_error) {
  std::lock_guard<std::mutex> lock(c_api_mutex());
  initialize_outputs(out_error);
  if (!valid_connection(connection)) {
    return fail(out_error, kInvalidArgument, "open connection is required");
  }
  try {
    return transaction_status(out_error, connection->connection->Commit());
  } catch (const std::exception& exception) {
    return fail(out_error, kInternalError, exception.what());
  }
}

int32_t neug_connection_rollback(neug_connection_t* connection,
                                 neug_error_t* out_error) {
  std::lock_guard<std::mutex> lock(c_api_mutex());
  initialize_outputs(out_error);
  if (!valid_connection(connection)) {
    return fail(out_error, kInvalidArgument, "open connection is required");
  }
  try {
    return transaction_status(out_error, connection->connection->Rollback());
  } catch (const std::exception& exception) {
    return fail(out_error, kInternalError, exception.what());
  }
}

void neug_buffer_free(neug_buffer_t* buffer) {
  if (buffer == nullptr) {
    return;
  }
  std::free(buffer->data);
  buffer->data = nullptr;
  buffer->len = 0;
}

void neug_error_free(neug_error_t* error) {
  if (error == nullptr) {
    return;
  }
  std::free(error->message);
  error->message = nullptr;
  error->code = kOk;
}

}  // extern "C"
