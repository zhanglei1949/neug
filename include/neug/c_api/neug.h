/** Copyright 2020 Alibaba Group Holding Limited. */
#pragma once

#include <stddef.h>
#include <stdint.h>

#include "neug/utils/api.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct neug_database neug_database_t;
typedef struct neug_connection neug_connection_t;

typedef struct neug_error {
  int32_t code;
  char* message;
} neug_error_t;

typedef struct neug_buffer {
  uint8_t* data;
  size_t len;
} neug_buffer_t;

/**
 * ABI version for consumers that dynamically load libneug.
 */
NEUG_API uint32_t neug_c_api_version(void);

/**
 * Open a database. On failure, *out_database is NULL and out_error is set.
 * The returned handle owns the database and must be closed exactly once.
 */
NEUG_API int32_t neug_database_open(const char* path, int32_t max_threads,
                                    int32_t read_only,
                                    int32_t checkpoint_on_close,
                                    neug_database_t** out_database,
                                    neug_error_t* out_error);

/**
 * Close and free a database handle. If close fails, the handle remains valid
 * so the caller can correct the failure and retry.
 */
NEUG_API int32_t neug_database_close(neug_database_t* database,
                                     neug_error_t* out_error);

NEUG_API int32_t neug_database_connect(neug_database_t* database,
                                       neug_connection_t** out_connection,
                                       neug_error_t* out_error);

NEUG_API int32_t neug_connection_close(neug_connection_t* connection,
                                       neug_error_t* out_error);

/**
 * Execute a query and return QueryResult::Serialize() bytes. parameters_json
 * must be a JSON object or NULL. The caller owns out_result->data and releases
 * it with neug_buffer_free.
 */
NEUG_API int32_t neug_connection_execute(neug_connection_t* connection,
                                         const char* query,
                                         const char* access_mode,
                                         const char* parameters_json,
                                         neug_buffer_t* out_result,
                                         neug_error_t* out_error);

NEUG_API int32_t neug_connection_begin(neug_connection_t* connection,
                                       int32_t read_only,
                                       neug_error_t* out_error);
NEUG_API int32_t neug_connection_commit(neug_connection_t* connection,
                                        neug_error_t* out_error);
NEUG_API int32_t neug_connection_rollback(neug_connection_t* connection,
                                          neug_error_t* out_error);

NEUG_API void neug_buffer_free(neug_buffer_t* buffer);
NEUG_API void neug_error_free(neug_error_t* error);

#ifdef __cplusplus
}
#endif
