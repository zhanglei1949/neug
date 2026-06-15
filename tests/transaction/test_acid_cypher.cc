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

/**
 * LDBC ACID tests expressed in Cypher (port of the "Bolt" driver in
 * github.com/ldbc/ldbc_acid).
 *
 * Like the upstream Bolt harness, this suite uses a SINGLE long-lived database
 * instance: one unified schema is created once, and every test starts by
 * nuking the data (MATCH (n) DETACH DELETE n equivalent) before its own Init
 * recreates the nodes/edges it needs.
 *
 * Query execution goes through the transaction-agnostic utility
 * execution::EvalQueryOnStorage(GlobalQueryCache&, storage, query, ...), which
 * compiles + runs a pipeline against a caller-owned transaction WITHOUT
 * committing. The caller (these tests) owns the transaction lifecycle
 * (GetReadTransaction / GetUpdateTransaction + Commit / Abort), which gives the
 * interactive, multi-statement control the isolation tests require
 * (read-sleep-read in one snapshot, conditional abort on an intermediate read).
 *
 * Because NeuG is schema-first (unlike the schema-less Bolt targets), the
 * shared schema is a superset Person table plus Post and the relationship
 * tables; tests set only the columns they care about (others take their
 * declared DEFAULTs).
 *
 * Adaptations vs. the upstream Bolt/Cypher queries (NeuG has no list-concat,
 * size(), extract(), or correlated comma-MATCH):
 *   - Atomicity: the `emails` list is replaced by a scalar `numEmails` counter
 *     (append -> +1, sum(size(emails)) -> sum(numEmails)). numNames is dropped
 *     because NeuG stores unset string properties as "" rather than NULL.
 *   - FR/OTV: read the 4-cycle via an explicit 4-hop pattern returning scalar
 *     versions instead of extract(p IN nodes(path) | p.version).
 *   - WS: pairs are linked with an explicit [:Pair] edge so wsR matches via the
 *     edge instead of the unsupported correlated `(p2 {id: p1.id+1})`.
 *   - G0 (Dirty Write) is intentionally omitted: it needs an ordered,
 *     appendable version-history list, which cannot be expressed without
 *     list-concat.
 */

#include <gtest/gtest.h>
#include <google/protobuf/arena.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "neug/execution/common/types/value.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/main/neug_db.h"
#include "neug/main/query_result.h"
#include "neug/server/neug_db_service.h"
#include "neug/server/neug_db_session.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"

namespace fs = std::filesystem;

namespace {

using neug::NeugDBSession;
using neug::QueryResult;
using neug::StorageReadInterface;
using neug::StorageTPUpdateInterface;
using neug::execution::EvalQueryOnStorage;
using neug::execution::GlobalQueryCache;
using neug::execution::ParamsMap;
using neug::execution::Value;

constexpr int kSleepMs = 1;
constexpr int kThreadNum = 8;

// All state shared across the whole suite (a single DB instance, like the Bolt
// harness). Created once in SetUpTestSuite, torn down in TearDownTestSuite.
// Member destruction order (reverse of declaration) tears down the service
// before the database, which is required since the service references the db.
struct SuiteCtx {
  std::unique_ptr<neug::NeugDB> db;
  std::shared_ptr<neug::NeugDBService> svc;
  GlobalQueryCache* cache = nullptr;  // owned by `db`
  std::string work_dir;
};
SuiteCtx* g_ctx = nullptr;

Value I64(int64_t v) { return Value::INT64(v); }

std::string RandString(int length) {
  static const char alphanum[] = "abcdefghijklmnopqrstuvwxyz";
  std::string ret;
  static thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<> dist(0, sizeof(alphanum) - 2);
  for (int i = 0; i < length; ++i)
    ret += alphanum[dist(gen)];
  return ret;
}

// --------------------------------------------------------------------------
// Small Cypher-execution helpers built on EvalQueryOnStorage + the global
// query cache. The caller owns the transaction; these never commit/abort.
// --------------------------------------------------------------------------

// Execute a query against an already-open transaction's storage interface and
// return the (copied) result.
QueryResult EvalQR(StorageReadInterface& storage, const std::string& query,
                   const ParamsMap& params = {}) {
  google::protobuf::Arena arena;
  auto* resp =
      google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);
  auto r = EvalQueryOnStorage(*g_ctx->cache, storage, query, params, resp);
  CHECK(r.has_value()) << "query failed: " << query;
  return QueryResult(*resp);
}

// Run a read-only query in its own read transaction.
QueryResult RunRead(NeugDBSession& sess, const std::string& query,
                    const ParamsMap& params = {}) {
  auto txn = sess.GetReadTransaction();
  StorageReadInterface gri(txn.view(), txn.timestamp());
  return EvalQR(gri, query, params);
}

// Run a single write statement in its own update transaction and commit.
// Returns true if the transaction committed successfully.
bool UpdateCommit(NeugDBSession& sess, const std::string& query,
                  const ParamsMap& params = {}) {
  auto txn = sess.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto r = EvalQueryOnStorage(*g_ctx->cache, gui, query, params);
  if (!r.has_value()) {
    txn.Abort();
    return false;
  }
  return txn.Commit();
}

// Initialize data: run several CREATE statements in one committed transaction.
void InitData(NeugDBSession& sess, const std::vector<std::string>& creates) {
  auto txn = sess.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  for (const auto& q : creates) {
    auto r = EvalQueryOnStorage(*g_ctx->cache, gui, q);
    CHECK(r.has_value()) << "init failed: " << q;
  }
  CHECK(txn.Commit());
}

// --------------------------------------------------------------------------
// Parallel drivers (mirror the helpers in test_acid.cc, kept local).
// --------------------------------------------------------------------------

template <typename FUNC_T>
void ParallelTxn(std::shared_ptr<neug::NeugDBService> svc, const FUNC_T& func,
                 int txn_num) {
  int thread_num = svc->SessionNum();
  std::vector<std::thread> threads;
  std::atomic<int> counter(0);
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([&]() {
      auto guard = svc->AcquireSession();
      NeugDBSession& sess = *guard.get();
      while (true) {
        int id = counter.fetch_add(1);
        if (id >= txn_num)
          break;
        func(sess, id);
      }
    });
  }
  for (auto& t : threads)
    t.join();
}

template <typename FUNC_T>
void ParallelClient(std::shared_ptr<neug::NeugDBService> svc,
                    const FUNC_T& func) {
  int thread_num = svc->SessionNum();
  std::vector<std::thread> threads;
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back(
        [&](int tid) {
          auto guard = svc->AcquireSession();
          NeugDBSession& sess = *guard.get();
          func(sess, tid);
        },
        i);
  }
  for (auto& t : threads)
    t.join();
}

}  // namespace

// ==========================================================================
// Shared-instance fixture: one NeugDB + one unified schema for all tests.
// ==========================================================================

class AcidCypherTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    g_ctx = new SuiteCtx();
    g_ctx->work_dir = "/tmp/neug_acid_cypher/" + RandString(8);
    fs::remove_all(g_ctx->work_dir);
    fs::create_directories(g_ctx->work_dir);

    g_ctx->db = std::make_unique<neug::NeugDB>();
    g_ctx->db->Open(g_ctx->work_dir, kThreadNum);
    g_ctx->svc = std::make_shared<neug::NeugDBService>(*g_ctx->db);
    g_ctx->cache = g_ctx->db->GetQueryCache().get();

    // One unified, schema-first superset used by every test. Non-PK columns
    // declare DEFAULTs so tests can CREATE nodes that set only a subset.
    auto conn = g_ctx->db->Connect();
    ASSERT_TRUE(conn->Query(
        "CREATE NODE TABLE Person (id INT64, name STRING DEFAULT '', "
        "numEmails INT64 DEFAULT 0, version INT64 DEFAULT 0, "
        "value INT64 DEFAULT 0, numFriends INT64 DEFAULT 0, "
        "PRIMARY KEY(id));"));
    ASSERT_TRUE(conn->Query("CREATE NODE TABLE Post (id INT64, PRIMARY KEY(id));"));
    ASSERT_TRUE(conn->Query(
        "CREATE REL TABLE Knows (FROM Person TO Person, since INT64 DEFAULT 0);"));
    ASSERT_TRUE(conn->Query("CREATE REL TABLE Likes (FROM Person TO Post);"));
    ASSERT_TRUE(conn->Query("CREATE REL TABLE Pair (FROM Person TO Person);"));
  }

  static void TearDownTestSuite() {
    std::string work_dir = g_ctx->work_dir;
    delete g_ctx;  // destroys svc before db (reverse declaration order)
    g_ctx = nullptr;
    fs::remove_all(work_dir);
  }

  void SetUp() override { NukeDatabase(); }

  // Equivalent of the Bolt driver's nukeDatabase(): wipe all data (and, via
  // DETACH, all edges) while keeping the shared schema.
  static void NukeDatabase() {
    auto guard = g_ctx->svc->AcquireSession();
    NeugDBSession& sess = *guard.get();
    auto txn = sess.GetUpdateTransaction();
    StorageTPUpdateInterface gui(txn);
    (void)EvalQueryOnStorage(*g_ctx->cache, gui,
                             "MATCH (n:Person) DETACH DELETE n");
    (void)EvalQueryOnStorage(*g_ctx->cache, gui,
                             "MATCH (n:Post) DETACH DELETE n");
    CHECK(txn.Commit());
  }
};

// ==========================================================================
// Atomicity
// ==========================================================================

TEST_F(AcidCypherTest, AtomicityC) {
  {
    auto guard = g_ctx->svc->AcquireSession();
    InitData(*guard.get(),
             {"CREATE (:Person {id: 1, numEmails: 1}), "
              "(:Person {id: 2, numEmails: 2})"});
  }

  // numPersons=2, numEmails=3 initially.
  auto guard = g_ctx->svc->AcquireSession();
  NeugDBSession& sess = *guard.get();
  // One committed atomicityC: +1 person, +1 email.
  bool ok = UpdateCommit(
      sess,
      "MATCH (p1:Person {id: $p1id}) "
      "CREATE (p1)-[k:Knows {since: $since}]->"
      "(p2:Person {id: $p2id, numEmails: 0}) "
      "SET p1.numEmails = p1.numEmails + 1",
      {{"p1id", I64(1)}, {"p2id", I64(3)}, {"since", I64(2020)}});
  ASSERT_TRUE(ok);

  auto qr = RunRead(sess, "MATCH (p:Person) RETURN count(p), sum(p.numEmails)");
  ASSERT_TRUE(qr.hasNext());
  EXPECT_EQ(qr.GetInt64(0), 3);  // numPersons
  EXPECT_EQ(qr.GetInt64(1), 4);  // numEmails
}

TEST_F(AcidCypherTest, AtomicityRB) {
  {
    auto guard = g_ctx->svc->AcquireSession();
    InitData(*guard.get(),
             {"CREATE (:Person {id: 1, numEmails: 1}), "
              "(:Person {id: 2, numEmails: 2})"});
  }

  auto guard = g_ctx->svc->AcquireSession();
  NeugDBSession& sess = *guard.get();

  // atomicityRB: append email to p1, then if p2 exists -> abort, else create.
  // person2Id=2 already exists, so the whole transaction must roll back.
  {
    auto txn = sess.GetUpdateTransaction();
    StorageTPUpdateInterface gui(txn);
    auto r1 = EvalQueryOnStorage(
        *g_ctx->cache, gui,
        "MATCH (p1:Person {id: $p1id}) SET p1.numEmails = p1.numEmails + 1",
        {{"p1id", I64(1)}});
    ASSERT_TRUE(r1.has_value());
    auto qr = EvalQR(gui, "MATCH (p2:Person {id: $p2id}) RETURN p2.id",
                     {{"p2id", I64(2)}});
    if (qr.hasNext()) {
      txn.Abort();
    } else {
      auto r2 = EvalQueryOnStorage(*g_ctx->cache, gui,
                                   "CREATE (:Person {id: $p2id, numEmails: 0})",
                                   {{"p2id", I64(2)}});
      ASSERT_TRUE(r2.has_value());
      ASSERT_TRUE(txn.Commit());
    }
  }

  auto qr = RunRead(sess, "MATCH (p:Person) RETURN count(p), sum(p.numEmails)");
  ASSERT_TRUE(qr.hasNext());
  EXPECT_EQ(qr.GetInt64(0), 2);  // numPersons unchanged
  EXPECT_EQ(qr.GetInt64(1), 3);  // numEmails unchanged (append rolled back)
}

// ==========================================================================
// G1A (Aborted Reads)
// ==========================================================================

TEST_F(AcidCypherTest, G1A) {
  {
    auto guard = g_ctx->svc->AcquireSession();
    InitData(*guard.get(), {"CREATE (:Person {id: 1, version: 1})"});
  }

  std::atomic<int64_t> num_incorrect(0);
  int rc = kThreadNum / 2;
  ParallelClient(g_ctx->svc, [&](NeugDBSession& sess, int client_id) {
    if (client_id < rc) {
      // reader: must always observe the committed version (1), never the
      // aborted writer's value (2).
      for (int i = 0; i < 100; ++i) {
        auto qr = RunRead(sess, "MATCH (p:Person {id: 1}) RETURN p.version");
        if (!qr.hasNext() || qr.GetInt64(0) != 1)
          num_incorrect.fetch_add(1);
      }
    } else {
      // writer: set version=2 inside a transaction, then abort.
      for (int i = 0; i < 100; ++i) {
        auto txn = sess.GetUpdateTransaction();
        StorageTPUpdateInterface gui(txn);
        std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
        (void)EvalQueryOnStorage(*g_ctx->cache, gui,
                                 "MATCH (p:Person {id: 1}) SET p.version = 2");
        std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
        txn.Abort();
      }
    }
  });
  EXPECT_EQ(num_incorrect.load(), 0);
}

// ==========================================================================
// G1B (Intermediate Reads)
// ==========================================================================

TEST_F(AcidCypherTest, G1B) {
  {
    auto guard = g_ctx->svc->AcquireSession();
    InitData(*guard.get(), {"CREATE (:Person {id: 1, version: 99})"});
  }

  std::atomic<int64_t> num_incorrect(0);
  int rc = kThreadNum / 2;
  ParallelClient(g_ctx->svc, [&](NeugDBSession& sess, int client_id) {
    if (client_id < rc) {
      // reader: committed versions are always odd (99, then 1); the
      // intermediate even value (0) must never be visible.
      for (int i = 0; i < 100; ++i) {
        auto qr = RunRead(sess, "MATCH (p:Person {id: 1}) RETURN p.version");
        if (!qr.hasNext() || (qr.GetInt64(0) % 2) != 1)
          num_incorrect.fetch_add(1);
      }
    } else {
      // writer: even -> sleep -> odd, in one transaction.
      for (int i = 0; i < 100; ++i) {
        auto txn = sess.GetUpdateTransaction();
        StorageTPUpdateInterface gui(txn);
        (void)EvalQueryOnStorage(*g_ctx->cache, gui,
                                 "MATCH (p:Person {id: 1}) SET p.version = $v",
                                 {{"v", I64(0)}});
        std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
        (void)EvalQueryOnStorage(*g_ctx->cache, gui,
                                 "MATCH (p:Person {id: 1}) SET p.version = $v",
                                 {{"v", I64(1)}});
        txn.Commit();
      }
    }
  });
  EXPECT_EQ(num_incorrect.load(), 0);
}

// ==========================================================================
// G1C (Circular Information Flow)
// ==========================================================================

TEST_F(AcidCypherTest, G1C) {
  {
    auto guard = g_ctx->svc->AcquireSession();
    InitData(*guard.get(),
             {"CREATE (:Person {id: 1, version: 0}), "
              "(:Person {id: 2, version: 0})"});
  }

  const int c = 100;
  std::vector<std::optional<int64_t>> results(c);
  ParallelTxn(
      g_ctx->svc,
      [&](NeugDBSession& sess, int id) {
        int64_t txn_id = id + 1;
        bool order = (txn_id % 2 == 0);
        int64_t p1id = order ? 1 : 2;
        int64_t p2id = order ? 2 : 1;

        auto txn = sess.GetUpdateTransaction();
        StorageTPUpdateInterface gui(txn);
        google::protobuf::Arena arena;
        auto* resp =
            google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);
        auto r = EvalQueryOnStorage(
            *g_ctx->cache, gui,
            "MATCH (p1:Person {id: $p1id}) SET p1.version = $txnId "
            "WITH count(*) AS dummy "
            "MATCH (p2:Person {id: $p2id}) RETURN p2.version",
            {{"p1id", I64(p1id)},
             {"p2id", I64(p2id)},
             {"txnId", I64(txn_id)}},
            resp);
        if (!r.has_value()) {
          txn.Abort();
          return;
        }
        QueryResult qr(*resp);
        int64_t p2v = qr.hasNext() ? qr.GetInt64(0) : 0;
        if (txn.Commit())
          results[id] = p2v;
      },
      c);

  int64_t num_incorrect = 0;
  for (int i = 1; i <= c; ++i) {
    auto& r1 = results[i - 1];
    if (!r1.has_value())
      continue;
    int64_t p2v1 = r1.value();
    if (p2v1 == 0)
      continue;
    auto& r2 = results[p2v1 - 1];
    if (!r2.has_value()) {
      // transaction i read a version written by transaction p2v1, but p2v1
      // aborted -> reading data of an aborted transaction is an anomaly.
      ++num_incorrect;
      continue;
    }
    int64_t p2v2 = r2.value();
    if (p2v2 == i)
      ++num_incorrect;
  }
  EXPECT_EQ(num_incorrect, 0);
}

// ==========================================================================
// IMP (Item-Many-Preceders)
// ==========================================================================

TEST_F(AcidCypherTest, IMP) {
  {
    auto guard = g_ctx->svc->AcquireSession();
    InitData(*guard.get(), {"CREATE (:Person {id: 1, version: 1})"});
  }

  std::atomic<int64_t> num_incorrect(0);
  int rc = kThreadNum / 2;
  ParallelClient(g_ctx->svc, [&](NeugDBSession& sess, int client_id) {
    if (client_id < rc) {
      // reader: two reads in one snapshot must agree.
      for (int i = 0; i < 100; ++i) {
        auto txn = sess.GetReadTransaction();
        StorageReadInterface gri(txn.view(), txn.timestamp());
        auto q1 = EvalQR(gri, "MATCH (p:Person {id: 1}) RETURN p.version");
        int64_t first = q1.hasNext() ? q1.GetInt64(0) : -1;
        std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
        auto q2 = EvalQR(gri, "MATCH (p:Person {id: 1}) RETURN p.version");
        int64_t second = q2.hasNext() ? q2.GetInt64(0) : -1;
        if (first != second)
          num_incorrect.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i) {
        UpdateCommit(sess,
                     "MATCH (p:Person {id: 1}) SET p.version = p.version + 1");
      }
    }
  });
  EXPECT_EQ(num_incorrect.load(), 0);
}

// ==========================================================================
// PMP (Predicate-Many-Preceders)
// ==========================================================================

TEST_F(AcidCypherTest, PMP) {
  {
    auto guard = g_ctx->svc->AcquireSession();
    InitData(*guard.get(),
             {"CREATE (:Person {id: 1})", "CREATE (:Post {id: 1})"});
  }

  std::atomic<int64_t> num_incorrect(0);
  int rc = kThreadNum / 2;
  ParallelClient(g_ctx->svc, [&](NeugDBSession& sess, int client_id) {
    if (client_id < rc) {
      // reader: count of LIKES edges to the post must be stable in one snapshot.
      for (int i = 0; i < 100; ++i) {
        auto txn = sess.GetReadTransaction();
        StorageReadInterface gri(txn.view(), txn.timestamp());
        auto q1 = EvalQR(
            gri,
            "MATCH (po:Post {id: 1})<-[:Likes]-(pe:Person) RETURN count(pe)");
        int64_t first = q1.hasNext() ? q1.GetInt64(0) : -1;
        std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
        auto q2 = EvalQR(
            gri,
            "MATCH (po:Post {id: 1})<-[:Likes]-(pe:Person) RETURN count(pe)");
        int64_t second = q2.hasNext() ? q2.GetInt64(0) : -1;
        if (first != second)
          num_incorrect.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i) {
        UpdateCommit(sess,
                     "MATCH (pe:Person {id: 1}), (po:Post {id: 1}) "
                     "CREATE (pe)-[:Likes]->(po)");
      }
    }
  });
  EXPECT_EQ(num_incorrect.load(), 0);
}

// ==========================================================================
// OTV (Observed Transaction Vanishes) and FR (Fractured Reads)
// ==========================================================================

namespace {

void InitCycle(std::shared_ptr<neug::NeugDBService> svc) {
  auto guard = svc->AcquireSession();
  InitData(
      *guard.get(),
      {"CREATE (:Person {id: 1, version: 0}), (:Person {id: 2, version: 0}), "
       "(:Person {id: 3, version: 0}), (:Person {id: 4, version: 0})",
       "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}), "
       "(p3:Person {id: 3}), (p4:Person {id: 4}) "
       "CREATE (p1)-[:Knows]->(p2), (p2)-[:Knows]->(p3), "
       "(p3)-[:Knows]->(p4), (p4)-[:Knows]->(p1)"});
}

// Read the four versions around the 4-cycle starting at `start_id`.
std::array<int64_t, 4> ReadCycle(StorageReadInterface& gri, int64_t start_id) {
  auto qr = EvalQR(gri,
                   "MATCH (p1:Person {id: $id})-[:Knows]->(p2:Person)"
                   "-[:Knows]->(p3:Person)-[:Knows]->(p4:Person)"
                   "-[:Knows]->(p1) "
                   "RETURN p1.version, p2.version, p3.version, p4.version",
                   {{"id", I64(start_id)}});
  CHECK(qr.hasNext());
  return {qr.GetInt64(0), qr.GetInt64(1), qr.GetInt64(2), qr.GetInt64(3)};
}

void CycleWrite(NeugDBSession& sess, int64_t start_id) {
  UpdateCommit(sess,
               "MATCH (p1:Person {id: $id})-[:Knows]->(p2:Person)"
               "-[:Knows]->(p3:Person)-[:Knows]->(p4:Person)-[:Knows]->(p1) "
               "SET p1.version = p1.version + 1, "
               "p2.version = p2.version + 1, "
               "p3.version = p3.version + 1, "
               "p4.version = p4.version + 1",
               {{"id", I64(start_id)}});
}

}  // namespace

TEST_F(AcidCypherTest, OTV) {
  InitCycle(g_ctx->svc);

  std::atomic<int64_t> num_incorrect(0);
  int rc = kThreadNum / 2;
  ParallelClient(g_ctx->svc, [&](NeugDBSession& sess, int client_id) {
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<int> dist(1, 4);
    if (client_id < rc) {
      for (int i = 0; i < 100; ++i) {
        auto txn = sess.GetReadTransaction();
        StorageReadInterface gri(txn.view(), txn.timestamp());
        auto first = ReadCycle(gri, dist(gen));
        std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
        auto second = ReadCycle(gri, dist(gen));
        int64_t fmax = *std::max_element(first.begin(), first.end());
        int64_t smin = *std::min_element(second.begin(), second.end());
        if (fmax > smin)
          num_incorrect.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i)
        CycleWrite(sess, dist(gen));
    }
  });
  EXPECT_EQ(num_incorrect.load(), 0);
}

TEST_F(AcidCypherTest, FR) {
  InitCycle(g_ctx->svc);

  std::atomic<int64_t> num_incorrect(0);
  int rc = kThreadNum / 2;
  ParallelClient(g_ctx->svc, [&](NeugDBSession& sess, int client_id) {
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<int> dist(1, 4);
    if (client_id < rc) {
      // reader: the two reads in one snapshot must be identical.
      for (int i = 0; i < 100; ++i) {
        auto txn = sess.GetReadTransaction();
        StorageReadInterface gri(txn.view(), txn.timestamp());
        auto first = ReadCycle(gri, dist(gen));
        std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
        auto second = ReadCycle(gri, dist(gen));
        if (first != second)
          num_incorrect.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i)
        CycleWrite(sess, dist(gen));
    }
  });
  EXPECT_EQ(num_incorrect.load(), 0);
}

// ==========================================================================
// LU (Lost Update)
// ==========================================================================

TEST_F(AcidCypherTest, LU) {
  {
    auto guard = g_ctx->svc->AcquireSession();
    InitData(*guard.get(), {"CREATE (:Person {id: 1, numFriends: 0})"});
  }

  const int n = 200;
  std::atomic<int> committed(0);
  ParallelTxn(
      g_ctx->svc,
      [&](NeugDBSession& sess, int id) {
        // friendId is unique per transaction to avoid PK collisions; the
        // contention is on Person(id=1).numFriends.
        int64_t friend_id = id + 2;
        bool ok = UpdateCommit(
            sess,
            "MATCH (p1:Person {id: 1}) "
            "CREATE (p1)-[:Knows]->(p2:Person {id: $fid, numFriends: 0}) "
            "SET p1.numFriends = p1.numFriends + 1",
            {{"fid", I64(friend_id)}});
        if (ok)
          committed.fetch_add(1);
      },
      n);

  auto guard = g_ctx->svc->AcquireSession();
  auto qr = RunRead(*guard.get(),
                    "MATCH (p:Person {id: 1}) "
                    "OPTIONAL MATCH (p)-[k:Knows]->() "
                    "WITH p, count(k) AS numKnowsEdges "
                    "RETURN numKnowsEdges, p.numFriends");
  ASSERT_TRUE(qr.hasNext());
  int64_t num_knows_edges = qr.GetInt64(0);
  int64_t num_friends = qr.GetInt64(1);
  EXPECT_EQ(num_friends, committed.load());
  EXPECT_EQ(num_knows_edges, committed.load());
}

// ==========================================================================
// WS (Write Skew)
// ==========================================================================

TEST_F(AcidCypherTest, WS) {
  {
    auto guard = g_ctx->svc->AcquireSession();
    std::vector<std::string> creates;
    for (int i = 1; i <= 10; ++i) {
      creates.push_back("CREATE (:Person {id: " + std::to_string(2 * i - 1) +
                        ", value: 70})-[:Pair]->(:Person {id: " +
                        std::to_string(2 * i) + ", value: 80})");
    }
    InitData(*guard.get(), creates);
  }

  ParallelClient(g_ctx->svc, [&](NeugDBSession& sess, int client_id) {
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<int> pair_dist(0, 9);
    std::uniform_int_distribution<int> coin(0, 1);
    for (int i = 0; i < 100; ++i) {
      int64_t p1id = pair_dist(gen) * 2 + 1;
      int64_t p2id = p1id + 1;

      auto txn = sess.GetUpdateTransaction();
      StorageTPUpdateInterface gui(txn);
      // read the pair; only proceed if their sum can absorb a -100.
      auto qr = EvalQR(
          gui,
          "MATCH (p1:Person {id: $p1id}), (p2:Person {id: $p2id}) "
          "WHERE p1.value + p2.value >= 100 "
          "RETURN p1.value, p2.value",
          {{"p1id", I64(p1id)}, {"p2id", I64(p2id)}});
      if (!qr.hasNext()) {
        txn.Abort();
        continue;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
      int64_t pick = coin(gen) ? p1id : p2id;
      (void)EvalQueryOnStorage(
          *g_ctx->cache, gui,
          "MATCH (p:Person {id: $pid}) SET p.value = p.value - 100",
          {{"pid", I64(pick)}});
      txn.Commit();
    }
  });

  // No pair may end up with a negative combined balance.
  auto guard = g_ctx->svc->AcquireSession();
  auto qr = RunRead(*guard.get(),
                    "MATCH (p1:Person)-[:Pair]->(p2:Person) "
                    "WHERE p1.value + p2.value <= 0 "
                    "RETURN p1.id, p2.id");
  EXPECT_FALSE(qr.hasNext());
}
