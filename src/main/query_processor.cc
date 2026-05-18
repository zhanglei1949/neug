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

#include "neug/main/query_processor.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/main/neug_db.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/pb_utils.h"

namespace neug {

result<std::pair<AccessMode, std::shared_ptr<execution::CacheValue>>>
QueryProcessor::check_and_retrieve_pipeline(const PropertyGraph& pg,
                                            const std::string& query_string,
                                            const std::string& user_access_mode,
                                            int32_t num_threads) {
  if (num_threads == 0) {
    num_threads = max_num_threads_;
  }
  if (num_threads > max_num_threads_) {
    num_threads = max_num_threads_;
  }
  if (num_threads < 1) {
    RETURN_ERROR(neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                              "Number of threads must be greater than 0"));
  }

  auto access_mode = user_access_mode.empty()
                         ? planner_->analyzeMode(query_string)
                         : ParseAccessMode(user_access_mode);
  GS_AUTO(cache_value, global_query_cache_->Get(pg.schema(), query_string));
  assert(cache_value);
  const auto& flags = cache_value->flags;
  if (is_read_only_) {
    if (flags.insert() || flags.update() || flags.schema() || flags.batch() ||
        flags.create_temp_table() || flags.checkpoint() ||
        flags.procedure_call()) {
      RETURN_ERROR(
          neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                       "Write queries are not supported in read-only mode"));
    }
  }
  return std::make_pair(access_mode, cache_value);
}

result<QueryResult> QueryProcessor::execute(
    const std::string& query_string, const std::string& user_access_mode,
    const execution::ParamsMap& parameters, int32_t num_threads) {
  auto& slot = snapshot_store_.acquireSnapshot();
  auto pipeline_res = check_and_retrieve_pipeline(
      *slot.pg(), query_string, user_access_mode, num_threads);
  if (!pipeline_res) {
    snapshot_store_.releaseSnapshot(slot);
    return tl::unexpected(pipeline_res.error());
  }
  auto& [access_mode, cache_value] = pipeline_res.value();
  if (need_exclusive_lock(access_mode)) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return execute_internal(slot, query_string, cache_value, access_mode,
                            parameters, num_threads);
  } else {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return execute_internal(slot, query_string, cache_value, access_mode,
                            parameters, num_threads);
  }
}

result<QueryResult> QueryProcessor::execute(const std::string& query_string,
                                            const std::string& user_access_mode,
                                            const rapidjson::Value& parameters,
                                            int32_t num_threads) {
  auto& slot = snapshot_store_.acquireSnapshot();
  auto pipeline_res = check_and_retrieve_pipeline(
      *slot.pg(), query_string, user_access_mode, num_threads);
  if (!pipeline_res) {
    snapshot_store_.releaseSnapshot(slot);
    return tl::unexpected(pipeline_res.error());
  }
  auto& [access_mode, cache_value] = pipeline_res.value();
  const auto& param_types = cache_value->params_type;
  execution::ParamsMap params_map;
  if (parameters.IsObject()) {
    for (const auto& member : parameters.GetObject()) {
      std::string key = member.name.GetString();
      auto iter = param_types.find(key);
      if (iter == param_types.end()) {
        snapshot_store_.releaseSnapshot(slot);
        RETURN_ERROR(neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                                  "Unexpected parameter: " + key));
      }
      params_map.emplace(
          key, execution::Value::FromJson(member.value, iter->second));
    }
  }
  if (need_exclusive_lock(access_mode)) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return execute_internal(slot, query_string, cache_value, access_mode,
                            params_map, num_threads);
  } else {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return execute_internal(slot, query_string, cache_value, access_mode,
                            params_map, num_threads);
  }
}

// The concurrency control is done outside this function.
result<QueryResult> QueryProcessor::execute_internal(
    SnapshotStore::StorageSlot& slot, const std::string& query_string,
    std::shared_ptr<execution::CacheValue> cache_value, AccessMode access_mode,
    const execution::ParamsMap& parameters, int32_t num_threads) {
  auto& pg = *slot.pg();
  StorageAPUpdateInterface graph(pg, 0, allocator_);
  std::unique_ptr<execution::OprTimer> timer_ptr = nullptr;
  auto ctx_res = cache_value->pipeline.Execute(graph, execution::Context(),
                                               parameters, timer_ptr.get());
  if (!ctx_res) {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", error code: " << ctx_res.error().error_code()
               << ", message: " << ctx_res.error().error_message();
    snapshot_store_.releaseSnapshot(slot);
    RETURN_ERROR(ctx_res.error());
  }

  google::protobuf::Arena arena;
  // Create a QueryResponse message on the arena to hold the results.
  neug::QueryResponse* response =
      google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);
  neug::execution::Sink::sink_results(ctx_res.value(), graph, response);
  response->mutable_schema()->CopyFrom(cache_value->result_schema);
  QueryResult ret = QueryResult::From(response->SerializeAsString());

  update_compiler_meta_if_needed(pg, cache_value->flags, access_mode);

  // Refresh the cached GraphView if the query mutated the PG in-place (DDL,
  // AP batch writes, etc.), so subsequent acquireSnapshot() callers see the
  // updated view.
  if (access_mode != AccessMode::kRead) {
    slot.refreshView();
  }
  snapshot_store_.releaseSnapshot(slot);
  return ret;
}

bool QueryProcessor::need_exclusive_lock(AccessMode access_mode) {
  if (access_mode == AccessMode::kRead) {
    return false;
  }
  return true;  // For Insert and Update operations
}

void QueryProcessor::update_compiler_meta_if_needed(
    const PropertyGraph& pg, const physical::ExecutionFlag& flags,
    AccessMode mode) {
  YAML::Node schema_yaml;
  std::string statistics_json;
  bool need_update = false;
  if (flags.schema() || flags.create_temp_table() ||
      mode == AccessMode::kSchema) {
    schema_yaml = pg.schema().to_yaml().value();
    need_update = true;
  }
  if (flags.batch() || flags.insert() || flags.update()) {
    statistics_json = pg.get_statistics_json();
    need_update = true;
  }
  if (need_update) {
    global_query_cache_->clear(schema_yaml, statistics_json);
  }
}

}  // namespace neug
