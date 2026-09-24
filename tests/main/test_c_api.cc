/** Copyright 2020 Alibaba Group Holding Limited. */

#include "neug/c_api/neug.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "neug/main/query_result.h"

namespace {

class OwnedError {
 public:
  ~OwnedError() { neug_error_free(&value); }
  neug_error_t value{0, nullptr};
};

class OwnedBuffer {
 public:
  ~OwnedBuffer() { neug_buffer_free(&value); }
  neug_buffer_t value{nullptr, 0};
};

TEST(NeugCApi, LifecycleQueryParametersAndTransactions) {
  EXPECT_EQ(neug_c_api_version(), 1U);
  const auto unique_id =
      std::chrono::steady_clock::now().time_since_epoch().count();
  auto path = std::filesystem::temp_directory_path() /
              ("neug-c-api-lifecycle-" + std::to_string(unique_id));
  std::filesystem::remove_all(path);

  neug_database_t* database = nullptr;
  OwnedError error;
  ASSERT_EQ(neug_database_open(path.string().c_str(), 1, 0, 1, &database,
                               &error.value),
            0)
      << (error.value.message == nullptr ? "" : error.value.message);
  neug_connection_t* connection = nullptr;
  ASSERT_EQ(neug_database_connect(database, &connection, &error.value), 0);

  auto execute = [&](const char* query, const char* parameters = "{}") {
    OwnedBuffer buffer;
    EXPECT_EQ(neug_connection_execute(connection, query, "", parameters,
                                      &buffer.value, &error.value),
              0)
        << (error.value.message == nullptr ? "" : error.value.message);
    return neug::QueryResult::From(std::string(
        reinterpret_cast<char*>(buffer.value.data), buffer.value.len));
  };

  execute("CREATE NODE TABLE Person(id UINT32 PRIMARY KEY, name STRING)");
  ASSERT_EQ(neug_connection_begin(connection, 0, &error.value), 0);
  execute("CREATE (:Person {id: 1, name: 'Ada'})");
  ASSERT_EQ(neug_connection_rollback(connection, &error.value), 0);
  auto rolled_back = execute("MATCH (n:Person) RETURN count(n)");
  ASSERT_TRUE(rolled_back.hasNext());
  EXPECT_EQ(rolled_back.GetInt64(0), 0);

  ASSERT_EQ(neug_connection_begin(connection, 0, &error.value), 0);
  execute("CREATE (:Person {id: 1, name: 'Ada'})");
  ASSERT_EQ(neug_connection_commit(connection, &error.value), 0);
  auto result =
      execute("MATCH (n:Person) WHERE n.id = $id RETURN n.name", "{\"id\":1}");
  ASSERT_TRUE(result.hasNext());
  EXPECT_EQ(result.GetString(0), "Ada");

  EXPECT_EQ(neug_connection_close(connection, &error.value), 0);
  EXPECT_EQ(neug_database_close(database, &error.value), 0);
  std::filesystem::remove_all(path);
}

TEST(NeugCApi, SerializesMultipleEmbeddedDatabases) {
  const auto unique_id =
      std::chrono::steady_clock::now().time_since_epoch().count();
  auto root = std::filesystem::temp_directory_path() /
              ("neug-c-api-multiple-" + std::to_string(unique_id));
  std::filesystem::remove_all(root);
  std::atomic<int> successes{0};
  std::vector<std::thread> threads;
  for (int index = 0; index < 2; ++index) {
    threads.emplace_back([&, index] {
      const auto path = root / std::to_string(index);
      neug_database_t* database = nullptr;
      OwnedError error;
      if (neug_database_open(path.string().c_str(), 1, 0, 1, &database,
                             &error.value) != 0) {
        return;
      }
      neug_connection_t* connection = nullptr;
      if (neug_database_connect(database, &connection, &error.value) != 0) {
        neug_database_close(database, &error.value);
        return;
      }
      OwnedBuffer buffer;
      if (neug_connection_execute(
              connection, "CREATE NODE TABLE Item(id UINT32 PRIMARY KEY)", "",
              "{}", &buffer.value, &error.value) == 0) {
        ++successes;
      }
      neug_connection_close(connection, &error.value);
      neug_database_close(database, &error.value);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(successes.load(), 2);
  std::filesystem::remove_all(root);
}

}  // namespace
