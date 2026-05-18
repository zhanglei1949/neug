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

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#include "neug/config.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "neug/server/neug_db_session.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"

#define SLEEP_TIME_MILLI_SEC 1

namespace fs = std::filesystem;
using namespace neug;
using neug::EdgeStrategy;
using neug::MemoryLevel;
using neug::NeugDB;
using neug::NeugDBSession;
using oid_t = int64_t;
using neug::DataTypeId;
using neug::Schema;
using neug::vid_t;

// Utility: Generate unique id (thread-safe)
static std::atomic<int64_t> neug_current_id(0);
Property neug_generate_id() {
  return neug::Property::From(neug_current_id.fetch_add(1));
}

std::string neug_generate_random_string(int length) {
  static const char alphanum[] = "abcdefghijklmnopqrstuvwxyz";
  std::string ret;
  static thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<> dist(0, sizeof(alphanum) - 2);
  for (int i = 0; i < length; ++i) {
    ret += alphanum[dist(gen)];
  }
  return ret;
}

std::string neug_generate_work_dir(const std::string& prefix) {
  while (true) {
    std::string dir = prefix + neug_generate_random_string(8);
    if (fs::exists(dir))
      continue;
    fs::create_directories(dir);
    return dir;
  }
}

// Test fixture for neug ACID tests
class NeugDBACIDTest : public ::testing::Test {
 protected:
  void SetUp() override {
    work_dir_ = neug_generate_work_dir("/tmp/neug_acid/");
    thread_num_ = 16;
  }
  void TearDown() override { fs::remove_all(work_dir_); }
  std::string work_dir_;
  int32_t thread_num_;
};

// Parallel helpers
template <typename FUNC_T>
void neug_parallel_transaction(std::shared_ptr<neug::NeugDBService> svc,
                               const FUNC_T& func, int txn_num) {
  std::vector<int> txn_ids(txn_num);
  std::iota(txn_ids.begin(), txn_ids.end(), 0);
  std::shuffle(txn_ids.begin(), txn_ids.end(),
               std::mt19937(std::random_device()()));
  int thread_num = svc->SessionNum();
  std::vector<std::thread> threads;
  std::atomic<int> txn_counter(0);
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back(
        [&](int tid) {
          auto guard = svc->AcquireSession();
          neug::NeugDBSession& session = *guard.get();
          while (true) {
            int txn_id = txn_counter.fetch_add(1);
            if (txn_id >= txn_num)
              break;
            func(session, txn_ids[txn_id]);
          }
        },
        i);
  }
  for (auto& t : threads)
    t.join();
}

template <typename FUNC_T>
void neug_parallel_client(std::shared_ptr<neug::NeugDBService> svc,
                          const FUNC_T& func) {
  int thread_num = svc->SessionNum();
  std::vector<std::thread> threads;
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back(
        [&](int tid) {
          auto guard = svc->AcquireSession();
          neug::NeugDBSession& session = *guard.get();
          func(session, tid);
        },
        i);
  }
  for (auto& t : threads)
    t.join();
}

// Helper: get random vertex iterator
bool neug_get_random_vertex(const StorageReadInterface& txn, label_t label_id,
                            vid_t& vid) {
  auto vertex_set = txn.GetVertexSet(label_id);
  int vnum = 0;
  for ([[maybe_unused]] auto v : vertex_set) {
    ++vnum;
  }
  if (vnum == 0)
    return false;
  std::random_device rand_dev;
  std::mt19937 gen(rand_dev());
  std::uniform_int_distribution<vid_t> dist(0, vnum - 1);
  int picked = dist(gen);
  auto v1 = txn.GetVertexSet(label_id);
  for (auto v : vertex_set) {
    if (picked == 0) {
      vid = v;
      return true;
    }
    --picked;
  }
  return false;
}

auto neug_get_random_vertex(StorageTPUpdateInterface& gi, label_t label_id) {
  auto vertex_set = gi.GetVertexSet(label_id);
  int num = 0;
  neug::vid_t vid = 0;
  for ([[maybe_unused]] auto v : vertex_set) {
    ++num;
  }

  if (num == 0)
    return vid;
  std::random_device rand_dev;
  std::mt19937 gen(rand_dev());
  std::uniform_int_distribution<int> dist(0, num - 1);
  int picked = dist(gen);
  auto v1 = gi.GetVertexSet(label_id);
  for (auto v : vertex_set) {
    if (picked == 0) {
      return v;
    }
    --picked;
  }
  return vid;
}

// Helper: append string to field
void neug_append_string_to_field(StorageTPUpdateInterface& gui, label_t label,
                                 neug::vid_t vit, int col_id,
                                 const std::string& str) {
  std::string cur_str =
      std::string(gui.GetVertexProperty(label, vit, col_id).as_string_view());
  if (cur_str.empty())
    cur_str = str;
  else {
    cur_str += ";";
    cur_str += str;
  }
  gui.UpdateVertexProperty(label, vit, col_id, Property::From(cur_str));
}

// Atomicity helpers and tests
std::shared_ptr<neug::NeugDBService> neug_AtomicityInit(
    NeugDB& db, const std::string& work_dir, int thread_num) {
  db.Open(work_dir, thread_num);
  auto service = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id2 INT64, name STRING, "
        "emails STRING, PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query(
        "CREATE REL TABLE KNOWS(FROM PERSON TO PERSON, since INT64);"));
  }

  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto sess = service->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  int64_t id1 = 1;
  std::string name1 = "Alice";
  std::string email1 = "alice@aol.com";
  int64_t id2 = 2;
  std::string name2 = "Bob";
  std::string email2 = "bob@hotmail.com;bobby@yahoo.com";
  vid_t vid;
  EXPECT_TRUE(gii.AddVertex(
      person_label_id, neug_generate_id(),
      {Property::from_int64(id1), Property::from_string_view(name1),
       Property::from_string_view(email1)},
      vid));
  EXPECT_TRUE(gii.AddVertex(
      person_label_id, neug_generate_id(),
      {Property::from_int64(id2), Property::from_string_view(name2),
       Property::from_string_view(email2)},
      vid));
  txn.Commit();

  return service;
}

bool neug_AtomicityC(neug::NeugDBSession& db, int64_t person2_id,
                     const std::string& new_email, int64_t since) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");
  auto vit = neug_get_random_vertex(gui, person_label_id);
  neug_append_string_to_field(gui, person_label_id, vit, 2, new_email);
  auto p2_id = neug_generate_id();
  std::string name = "", email = "";
  vid_t vid;

  if (!gui.AddVertex(
          person_label_id, p2_id,
          {Property::from_int64(person2_id), Property::from_string_view(name),
           Property::from_string_view(email)},
          vid)) {
    txn.Abort();
    return false;
  }
  if (!gui.AddEdge(person_label_id, vit, person_label_id, vid, knows_label_id,
                   {Property::from_int64(since)})) {
    txn.Abort();
    return false;
  }
  txn.Commit();
  return true;
}

bool neug_AtomicityRB(neug::NeugDBSession& db, int64_t person2_id,
                      const std::string& new_email, int64_t since) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vit1 = neug_get_random_vertex(gui, person_label_id);
  neug_append_string_to_field(gui, person_label_id, vit1, 2, new_email);
  neug::vid_t vit2;
  if (gui.GetVertexIndex(person_label_id,
                         neug::Property::from_int64(person2_id), vit2)) {
    txn.Abort();
    return false;
  }
  auto p2_id = neug_generate_id();
  std::string name = "", email = "";
  vid_t vid;
  EXPECT_TRUE(gui.AddVertex(
      person_label_id, p2_id,
      {Property::from_int64(person2_id), Property::from_string_view(name),
       Property::from_string_view(email)},
      vid));
  EXPECT_TRUE(txn.Commit());
  return true;
}

int64_t neug_count_email_num(const std::string_view& sv) {
  if (sv.empty())
    return 0;
  int64_t ret = 1;
  for (auto c : sv)
    if (c == ';')
      ++ret;
  return ret;
}

std::pair<int64_t, int64_t> neug_AtomicityCheck(
    std::shared_ptr<neug::NeugDBService> svc) {
  auto sess = svc->AcquireSession();
  auto txn = sess->GetReadTransaction();
  const auto& db = svc->db();
  StorageReadInterface gi(txn.view(), txn.timestamp());
  int64_t num_persons = 0, num_emails = 0;
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vprop_accessor = std::dynamic_pointer_cast<
      StorageReadInterface::vertex_column_t<std::string_view>>(
      gi.GetVertexPropColumn(person_label_id, "emails"));
  auto vset = gi.GetVertexSet(person_label_id);
  for (vid_t lid : vset) {
    ++num_persons;
    num_emails += neug_count_email_num(vprop_accessor->get_view(lid));
  }
  return {num_persons, num_emails};
}

// Dirty Writes

std::shared_ptr<neug::NeugDBService> G0Init(NeugDB& db,
                                            const std::string& work_dir,
                                            int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE PERSON (id1 INT64, id2 INT64, "
                    "versionHistory STRING, "
                    "PRIMARY KEY(id1));"));
    EXPECT_TRUE(conn->Query(
        "CREATE REL TABLE KNOWS(FROM PERSON TO PERSON, versionHistory "
        "STRING);"));
  }

  const auto& schema = db.schema();
  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto knows_label_id = schema.get_edge_label_id("KNOWS");

  // EnsureCapacity is a write operation, use const_cast for test setup
  auto& mutable_graph = const_cast<neug::PropertyGraph&>(db.graph());
  mutable_graph.EnsureCapacity(person_label_id, 1000);
  mutable_graph.EnsureCapacity(person_label_id, person_label_id, knows_label_id,
                               1000);

  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);

  std::string value = "0";
  for (int i = 0; i < 100; ++i) {
    auto p1_id = neug_generate_id();
    int64_t p1_id_property = 2 * i + 1;
    vid_t vid0, vid1;
    CHECK(gii.AddVertex(person_label_id, p1_id,
                        {Property::from_int64(p1_id_property),
                         Property::from_string_view(value)},
                        vid0));
    auto p2_id = neug_generate_id();
    int64_t p2_id_property = 2 * i + 2;
    CHECK(gii.AddVertex(person_label_id, p2_id,
                        {Property::from_int64(p2_id_property),
                         Property::from_string_view(value)},
                        vid1));
    CHECK(gii.AddEdge(person_label_id, vid0, person_label_id, vid1,
                      knows_label_id, {Property::from_string_view(value)}));
  }
  txn.Commit();
  return svc;
}

void G0(neug::NeugDBSession& db, int64_t person1_id, int64_t person2_id,
        int64_t txn_id) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");

  neug::vid_t vit1;
  const auto& vertex_set = gui.GetVertexSet(person_label_id);
  bool flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person1_id) {
      vit1 = v;
      flag = true;
      break;
    }
  }
  CHECK(flag);
  neug_append_string_to_field(gui, person_label_id, vit1, 1,
                              std::to_string(txn_id));

  neug::vid_t vit2;
  flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person2_id) {
      vit2 = v;
      flag = true;
      break;
    }
  }
  CHECK(flag);
  neug_append_string_to_field(gui, person_label_id, vit2, 1,
                              std::to_string(txn_id));

  auto oe_view = gui.GetGenericOutgoingGraphView(
      person_label_id, person_label_id, knows_label_id);
  auto oe_edge = oe_view.get_edges(vit1);
  auto oeit = oe_edge.begin();
  auto oeit_end = oe_edge.end();
  for (; oeit != oeit_end; ++oeit) {
    if (oeit.get_vertex() == vit2) {
      break;
    }
  }
  auto ed_accessor = gui.GetEdgeDataAccessor(person_label_id, knows_label_id,
                                             person_label_id, 0);
  CHECK(oeit != oeit_end);

  Property cur = ed_accessor.get_data(oeit);
  std::string cur_str(cur.as_string_view());
  if (cur_str.empty()) {
    cur_str = std::to_string(txn_id);
  } else {
    cur_str += ";";
    cur_str += std::to_string(txn_id);
  }
  Property new_value;
  new_value.set_string_view(cur_str);

  ed_accessor.set_data(oeit, new_value, txn.timestamp());

  txn.Commit();
}

std::tuple<std::string, std::string, std::string> G0Check(
    NeugDB& db, std::shared_ptr<neug::NeugDBService> svc, int64_t person1_id,
    int64_t person2_id) {
  auto sess = svc->AcquireSession();
  auto txn = sess->GetReadTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto prop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "id2"));

  auto name_col = std::dynamic_pointer_cast<
      StorageReadInterface::vertex_column_t<std::string_view>>(
      gi.GetVertexPropColumn(person_label_id, "versionHistory"));

  std::string p1_version_history;
  vid_t vit1_index = 0;
  auto vertex_set = gi.GetVertexSet(person_label_id);
  for (vid_t lid : vertex_set) {
    if (prop_col->get(lid).as_int64() == person1_id) {
      vit1_index = lid;
      p1_version_history = std::string(name_col->get(lid).as_string_view());
      break;
    }
  }

  vid_t vit2_index = 0;
  std::string p2_version_history;

  for (vid_t lid : vertex_set) {
    if (prop_col->get(lid).as_int64() == person2_id) {
      vit2_index = lid;
      p2_version_history = std::string(name_col->get(lid).as_string_view());
      break;
    }
  }

  auto view = gi.GetGenericOutgoingGraphView(person_label_id, person_label_id,
                                             knows_label_id);
  auto oeit = view.get_edges(vit1_index);
  NbrIterator iter = oeit.begin();
  auto end = oeit.end();
  for (; iter != end; ++iter) {
    if ((*iter) == vit2_index) {
      break;
    }
  }
  auto ed_accessor = gi.GetEdgeDataAccessor(person_label_id, knows_label_id,
                                            person_label_id, 0);

  CHECK(iter != end);
  Property k_version_history_field = ed_accessor.get_data(iter);
  CHECK(k_version_history_field.type() == neug::DataTypeId::kVarchar);
  std::string k_version_history(k_version_history_field.as_string_view());

  return std::make_tuple(p1_version_history, p2_version_history,
                         k_version_history);
}

// Intermediate Reads

std::shared_ptr<neug::NeugDBService> G1BInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, version INT64, "
        "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  int64_t value = 99;
  for (int i = 0; i < 100; ++i) {
    int64_t vertex_id_property = i + 1;
    vid_t vid;
    CHECK(gii.AddVertex(
        person_label_id, neug_generate_id(),
        {Property::from_int64(vertex_id_property), Property::from_int64(value)},
        vid));
  }
  txn.Commit();
  return svc;
}

void G1B1(neug::NeugDBSession& db, int64_t even, int64_t odd) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vit = neug_get_random_vertex(gui, person_label_id);
  gui.UpdateVertexProperty(person_label_id, vit, 1, Property::From(even));
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  gui.UpdateVertexProperty(person_label_id, vit, 1, Property::From(odd));
  txn.Commit();
}

int64_t G1B2(neug::NeugDBSession& db) {
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());

  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  vid_t vid;
  CHECK(neug_get_random_vertex(gi, person_label_id, vid));
  auto vprop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "version"));
  CHECK(vprop_col != nullptr);
  return vprop_col->get(vid).as_int64();
}

// Circular Information Flow

std::shared_ptr<neug::NeugDBService> G1CInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, version INT64, "
        "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();

  StorageTPInsertInterface gii(txn);

  int64_t version_property = 0;
  for (int i = 0; i < 100; ++i) {
    int64_t id_property = i + 1;
    vid_t vid;
    CHECK(gii.AddVertex(person_label_id, neug_generate_id(),
                        {Property::from_int64(id_property),
                         Property::from_int64(version_property)},
                        vid));
  }
  txn.Commit();
  return svc;
}

int64_t G1C(neug::NeugDBSession& db, int64_t person1_id, int64_t person2_id,
            int64_t txn_id) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  neug::vid_t person1_vid;
  bool flag = false;
  const auto& vertex_set = gui.GetVertexSet(person_label_id);
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person2_id) {
      person1_vid = v;
      flag = true;
      break;
    }
  }
  gui.UpdateVertexProperty(person_label_id, person1_vid, 1,
                           Property::From(txn_id));

  CHECK(flag);
  neug::vid_t person2_vid;
  flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person1_id) {
      person2_vid = v;
      flag = true;
      break;
    }
  }
  int64_t ret =
      gui.GetVertexProperty(person_label_id, person2_vid, 1).as_int64();

  txn.Commit();

  return ret;
}

// Aborted Reads

std::shared_ptr<neug::NeugDBService> G1AInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, version INT64, "
        "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  int64_t vertex_data = 1;
  for (int i = 0; i < 100; ++i) {
    int64_t vertex_id_property = i + 1;
    vid_t vid;
    CHECK(gii.AddVertex(person_label_id, neug_generate_id(),
                        {Property::from_int64(vertex_id_property),
                         Property::from_int64(vertex_data)},
                        vid));
  }
  txn.Commit();
  return svc;
}

void G1A1(neug::NeugDBSession& db) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  // select a random person
  auto vit = neug_get_random_vertex(gui, person_label_id);

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  // attempt to set version = 2
  gui.UpdateVertexProperty(person_label_id, vit, 1, Property::From<int64_t>(2));
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  txn.Abort();
}

int64_t G1A2(neug::NeugDBSession& db) {
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());

  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  vid_t vid;
  CHECK(neug_get_random_vertex(gi, person_label_id, vid));
  auto vprop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "version"));
  CHECK(vprop_col != nullptr);
  return vprop_col->get(vid).as_int64();
}

// Item-Many-Preceders

std::shared_ptr<neug::NeugDBService> IMPInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, version INT64, "
        "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  int64_t version_property = 1;
  for (int i = 0; i < 100; ++i) {
    int64_t id_property = i + 1;
    vid_t vid;
    CHECK(txn.AddVertex(person_label_id, neug_generate_id(),
                        {Property::from_int64(id_property),
                         Property::from_int64(version_property)},
                        vid));
  }
  txn.Commit();
  return svc;
}

void IMP1(neug::NeugDBSession& db) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  StorageTPUpdateInterface gui(txn);
  auto vit = neug_get_random_vertex(gui, person_label_id);
  int64_t old_version =
      gui.GetVertexProperty(person_label_id, vit, 1).as_int64();
  gui.UpdateVertexProperty(person_label_id, vit, 1,
                           Property::From(old_version + 1));
  txn.Commit();
}

std::tuple<int64_t, int64_t> IMP2(neug::NeugDBSession& db, int64_t person1_id) {
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  vid_t vit0_index = 0;
  auto v_prop_col0 =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "id_prop"));
  auto v_prop_col1 =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "version"));
  CHECK(v_prop_col0 != nullptr);
  auto vertex_set = gi.GetVertexSet(person_label_id);
  bool found = false;
  for (vid_t lid : vertex_set) {
    if (v_prop_col0->get(lid).as_int64() == person1_id) {
      vit0_index = lid;
      found = true;
      break;
    }
  }
  CHECK(found);

  int64_t v1 = v_prop_col1->get(vit0_index).as_int64();

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  vid_t vit1_index = 0;
  for (vid_t lid : vertex_set) {
    if (v_prop_col0->get(lid).as_int64() == person1_id) {
      vit1_index = lid;
      break;
    }
  }

  int64_t v2 = v_prop_col1->get(vit1_index).as_int64();

  return std::make_tuple(v1, v2);
}

// Predicate-Many-Preceders

std::shared_ptr<neug::NeugDBService> PMPInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE PERSON (id INT64, id_prop INT64, "
                    "PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE POST (id INT64, id_prop INT64, PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query("CREATE REL TABLE LIKES(FROM PERSON TO POST);"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto post_label_id = schema.get_vertex_label_id("POST");

  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  for (int i = 0; i < 100; ++i) {
    int64_t value = i + 1;
    vid_t vid;
    CHECK(gii.AddVertex(person_label_id, neug_generate_id(),
                        {Property::from_int64(value)}, vid));
    CHECK(gii.AddVertex(post_label_id, neug_generate_id(),
                        {Property::from_int64(value)}, vid));
  }
  txn.Commit();
  return svc;
}

bool PMP1(neug::NeugDBSession& db, int64_t person_id, int64_t post_id) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto post_label_id = db.schema().get_vertex_label_id("POST");
  auto likes_label_id = db.schema().get_edge_label_id("LIKES");
  neug::vid_t person_vid;
  bool found = false;
  auto vertex_set = gui.GetVertexSet(person_label_id);
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person_id) {
      person_vid = v;
      found = true;
      break;
    }
  }
  CHECK(found);
  neug::vid_t post_vid;
  found = false;
  auto post_vertex_set = gui.GetVertexSet(post_label_id);
  for (auto v : post_vertex_set) {
    int64_t v_id = gui.GetVertexProperty(post_label_id, v, 0).as_int64();
    if (v_id == post_id) {
      post_vid = v;
      found = true;
      break;
    }
  }
  CHECK(found);
  if (!txn.AddEdge(person_label_id, person_vid, post_label_id, post_vid,
                   likes_label_id, {})) {
    txn.Abort();
    return false;
  }
  txn.Commit();
  return true;
}

std::tuple<int64_t, int64_t> PMP2(neug::NeugDBSession& db, int64_t post_id) {
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto post_label_id = db.schema().get_vertex_label_id("POST");
  auto likes_label_id = db.schema().get_edge_label_id("LIKES");

  vid_t vit0_index = 0;
  auto v_prop_col0 =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(post_label_id, "id_prop"));
  CHECK(v_prop_col0 != nullptr);
  auto vertex_set = gi.GetVertexSet(post_label_id);
  for (vid_t lid : vertex_set) {
    if (v_prop_col0->get(lid).as_int64() == post_id) {
      vit0_index = lid;
      break;
    }
  }
  int64_t c1 = 0;
  auto view = gi.GetGenericIncomingGraphView(post_label_id, likes_label_id,
                                             person_label_id);
  auto ieit = view.get_edges(vit0_index);
  for (auto iter = ieit.begin(); iter != ieit.end(); ++iter) {
    c1++;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  auto v_prop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(post_label_id, "id_prop"));
  CHECK(v_prop_col != nullptr);
  vid_t vit1_index = 0;
  auto vertex_set1 = gi.GetVertexSet(post_label_id);
  for (vid_t lid : vertex_set1) {
    if (v_prop_col->get(lid).as_int64() == post_id) {
      vit1_index = lid;
      break;
    }
  }
  int64_t c2 = 0;
  auto view2 = gi.GetGenericIncomingGraphView(post_label_id, likes_label_id,
                                              person_label_id);
  auto ieit2 = view2.get_edges(vit1_index);
  for (auto iter = ieit2.begin(); iter != ieit2.end(); ++iter) {
    c2++;
  }
  return std::make_tuple(c1, c2);
}

// Observed Transaction Vanishes

std::shared_ptr<neug::NeugDBService> OTVInit(NeugDB& db,
                                             const std::string& work_dir,
                                             int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, name STRING, "
        "version INT64, PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query("CREATE REL TABLE KNOWS(FROM PERSON TO PERSON);"));
  }
  const auto& schema = db.schema();

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto knows_label_id = schema.get_edge_label_id("KNOWS");

  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  int64_t value = 0;
  std::vector<std::string> string_props;
  for (int j = 1; j <= 100; j++) {
    std::vector<vid_t> vids;
    for (int i = 1; i <= 4; i++) {
      auto id = neug_generate_id();
      int64_t id_property = j * 4 + i;
      vid_t vid;
      string_props.push_back(std::to_string(j));
      CHECK(gii.AddVertex(person_label_id, id,
                          {Property::from_int64(id_property),
                           Property::from_string_view(string_props.back()),
                           Property::from_int64(value)},
                          vid));
      vids.push_back(vid);
    }
    for (int i = 0; i < 4; i++) {
      CHECK(gii.AddEdge(person_label_id, vids[i], person_label_id,
                        vids[(i + 1) % 4], knows_label_id, {}));
    }
  }
  txn.Commit();
  return svc;
}

void OTV1(neug::NeugDBSession& db, int64_t person_id) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");
  vid_t vid1;

  bool found = false;
  auto vertex_set = gui.GetVertexSet(person_label_id);
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person_id) {
      vid1 = v;
      found = true;
      break;
    }
  }
  CHECK(found);
  auto oe_view = gui.GetGenericOutgoingGraphView(
      person_label_id, person_label_id, knows_label_id);
  auto vid1_edges = oe_view.get_edges(vid1);

  for (auto eit1 = vid1_edges.begin(); eit1 != vid1_edges.end(); ++eit1) {
    vid_t vid2 = eit1.get_vertex();
    auto vid2_edges = oe_view.get_edges(vid2);
    for (auto eit2 = vid2_edges.begin(); eit2 != vid2_edges.end(); ++eit2) {
      vid_t vid3 = eit2.get_vertex();
      auto vid3_edges = oe_view.get_edges(vid3);
      for (auto eit3 = vid3_edges.begin(); eit3 != vid3_edges.end(); ++eit3) {
        vid_t vid4 = eit3.get_vertex();
        auto vid4_edges = oe_view.get_edges(vid4);
        for (auto eit4 = vid4_edges.begin(); eit4 != vid4_edges.end(); ++eit4) {
          if (eit4.get_vertex() == vid1) {
            txn.UpdateVertexProperty(
                person_label_id, vid1, 2,
                Property::From(
                    txn.GetVertexProperty(person_label_id, vid1, 2).as_int64() +
                    1));

            gui.UpdateVertexProperty(
                person_label_id, vid2, 2,
                Property::From(
                    gui.GetVertexProperty(person_label_id, vid2, 2).as_int64() +
                    1));

            gui.UpdateVertexProperty(
                person_label_id, vid3, 2,
                Property::From(
                    gui.GetVertexProperty(person_label_id, vid3, 2).as_int64() +
                    1));

            gui.UpdateVertexProperty(
                person_label_id, vid4, 2,
                Property::From(
                    gui.GetVertexProperty(person_label_id, vid4, 2).as_int64() +
                    1));

            txn.Commit();
            return;
          }
        }
      }
    }
  }
}

std::tuple<std::tuple<int64_t, int64_t, int64_t, int64_t>,
           std::tuple<int64_t, int64_t, int64_t, int64_t>>
OTV2(neug::NeugDBSession& db, int64_t person_id) {
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");

  auto view = gi.GetGenericOutgoingGraphView(person_label_id, person_label_id,
                                             knows_label_id);
  auto prop0_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "id_prop"));
  auto vprop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "version"));

  auto get_versions = [&]() -> std::tuple<int64_t, int64_t, int64_t, int64_t> {
    auto vertex_set = gi.GetVertexSet(person_label_id);
    for (vid_t lid : vertex_set) {
      auto edges1 = view.get_edges(lid);
      for (auto it = edges1.begin(); it != edges1.end(); ++it) {
        vid_t vid2 = it.get_vertex();
        auto edges2 = view.get_edges(vid2);
        for (auto it2 = edges2.begin(); it2 != edges2.end(); ++it2) {
          vid_t vid3 = it2.get_vertex();
          auto edges3 = view.get_edges(vid3);
          for (auto it3 = edges3.begin(); it3 != edges3.end(); ++it3) {
            vid_t vid4 = it3.get_vertex();
            auto edges4 = view.get_edges(vid4);
            for (auto it4 = edges4.begin(); it4 != edges4.end(); ++it4) {
              if (it4.get_vertex() == lid) {
                int64_t v1_version = vprop_col->get(lid).as_int64();
                int64_t v2_version = vprop_col->get(vid2).as_int64();
                int64_t v3_version = vprop_col->get(vid3).as_int64();
                int64_t v4_version = vprop_col->get(vid4).as_int64();
                return std::make_tuple(v1_version, v2_version, v3_version,
                                       v4_version);
              }
            }
          }
        }
      }
    }
    return std::make_tuple(0, 0, 0, 0);
  };
  {
    auto vertex_set = gi.GetVertexSet(person_label_id);
    bool found = false;

    for (vid_t lid : vertex_set) {
      if (prop0_col->get(lid).as_int64() == person_id) {
        found = true;
        break;
      }
    }

    CHECK(found);
  }
  auto tup1 = get_versions();

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  {
    auto vertex_set = gi.GetVertexSet(person_label_id);
    bool found = false;
    for (vid_t lid : vertex_set) {
      if (prop0_col->get(lid).as_int64() == person_id) {
        found = true;
        break;
      }
    }
    CHECK(found);
  }
  auto tup2 = get_versions();

  return std::make_tuple(tup1, tup2);
}

// Fractured Reads

std::shared_ptr<neug::NeugDBService> FRInit(NeugDB& db,
                                            const std::string& work_dir,
                                            int thread_num) {
  return OTVInit(db, work_dir, thread_num);
}

void FR1(neug::NeugDBSession& db, int64_t person_id) { OTV1(db, person_id); }

std::tuple<std::tuple<int64_t, int64_t, int64_t, int64_t>,
           std::tuple<int64_t, int64_t, int64_t, int64_t>>
FR2(neug::NeugDBSession& db, int64_t person_id) {
  return OTV2(db, person_id);
}

// Lost Updates

std::shared_ptr<neug::NeugDBService> LUInit(NeugDB& db,
                                            const std::string& work_dir,
                                            int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE PERSON (id INT64, id_prop INT64, "
                    "num_friends INT64, "
                    "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();
  auto person_label_id = schema.get_vertex_label_id("PERSON");

  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gii(txn);
  int64_t num_property = 0;
  for (int i = 0; i < 100; ++i) {
    int64_t id_property = i + 1;
    vid_t vid;
    CHECK(gii.AddVertex(
        person_label_id, neug_generate_id(),
        {Property::from_int64(id_property), Property::from_int64(num_property)},
        vid));
  }

  txn.Commit();
  return svc;
}

bool LU1(neug::NeugDBSession& db, int64_t person_id) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");

  neug::vid_t person_vid;
  const auto& vertex_set = gui.GetVertexSet(person_label_id);
  bool flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person_id) {
      person_vid = v;
      flag = true;
      break;
    }
  }
  CHECK(flag);

  int64_t num_friends =
      gui.GetVertexProperty(person_label_id, person_vid, 1).as_int64();
  gui.UpdateVertexProperty(person_label_id, person_vid, 1,
                           Property::From(num_friends + 1));

  txn.Commit();
  return true;
}

std::map<int64_t, int64_t> LU2(neug::NeugDBSession& db) {
  std::map<int64_t, int64_t> numFriends;
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto prop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "id_prop"));
  auto num_friends_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "num_friends"));
  auto vertex_set = gi.GetVertexSet(person_label_id);
  for (vid_t lid : vertex_set) {
    int64_t person_id = prop_col->get(lid).as_int64();
    int64_t num_friends = num_friends_col->get(lid).as_int64();
    numFriends.emplace(person_id, num_friends);
  }

  return numFriends;
}

// Write Skews

std::shared_ptr<neug::NeugDBService> WSInit(NeugDB& db,
                                            const std::string& work_dir,
                                            int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id_prop INT64, version INT64, "
        "PRIMARY KEY(id));"));
  }
  const auto& schema = db.schema();
  auto person_label_id = schema.get_vertex_label_id("PERSON");

  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  StorageTPInsertInterface gi(txn);

  for (int i = 1; i <= 100; i++) {
    int64_t id1 = 2 * i - 1;
    int64_t version1 = 70;
    vid_t vid;
    CHECK(gi.AddVertex(
        person_label_id, neug_generate_id(),
        {Property::from_int64(id1), Property::from_int64(version1)}, vid));
    int64_t id2 = 2 * i;
    int64_t version2 = 80;
    CHECK(gi.AddVertex(
        person_label_id, neug_generate_id(),
        {Property::from_int64(id2), Property::from_int64(version2)}, vid));
  }
  txn.Commit();
  return svc;
}

void WS1(neug::NeugDBSession& db, int64_t person1_id, int64_t person2_id,
         std::mt19937& gen) {
  auto txn = db.GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");

  vid_t person1_vid;
  const auto& vertex_set = gui.GetVertexSet(person_label_id);
  bool flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person1_id) {
      person1_vid = v;
      flag = true;
      break;
    }
  }
  CHECK(flag);
  int64_t p1_value =
      gui.GetVertexProperty(person_label_id, person1_vid, 1).as_int64();
  vid_t person2_vid;
  flag = false;
  for (auto v : vertex_set) {
    int64_t v_id = gui.GetVertexProperty(person_label_id, v, 0).as_int64();
    if (v_id == person2_id) {
      person2_vid = v;
      flag = true;
      break;
    }
  }
  CHECK(flag);
  int64_t p2_value =
      gui.GetVertexProperty(person_label_id, person2_vid, 1).as_int64();

  if (p1_value + p2_value - 100 < 0) {
    txn.Abort();
    return;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  std::uniform_int_distribution<> dist(0, 1);

  // pick randomly between person1 and person2 and decrement the value
  // property
  if (dist(gen)) {
    gui.UpdateVertexProperty(person_label_id, person1_vid, 1,
                             neug::Property::From(p1_value - 100));
  } else {
    gui.UpdateVertexProperty(person_label_id, person2_vid, 1,
                             neug::Property::From(p2_value - 100));
  }
  txn.Commit();
}

std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> WS2(
    neug::NeugDBSession& db) {
  std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> results;
  auto txn = db.GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto person_prop_col =
      std::dynamic_pointer_cast<StorageReadInterface::vertex_column_t<int64_t>>(
          gi.GetVertexPropColumn(person_label_id, "id_prop"));
  auto vertex_set = gi.GetVertexSet(person_label_id);

  for (vid_t lid : vertex_set) {
    auto person1_id = person_prop_col->get(lid).as_int64();
    if (person1_id % 2 != 1) {
      continue;
    }
    int64_t p1_value = person_prop_col->get(lid).as_int64();
    auto person2_id = person1_id + 1;
    vid_t lid2;
    for (vid_t lid : vertex_set) {
      if (person_prop_col->get(lid).as_int64() == person2_id) {
        lid2 = lid;
        break;
      }
    }
    int64_t p2_value = person_prop_col->get(lid2).as_int64();
    if (p1_value + p2_value <= 0) {
      results.emplace_back(person1_id, p1_value, person2_id, p2_value);
    }
  }

  return results;
}

TEST_F(NeugDBACIDTest, AtomicityC) {
  std::string dir = work_dir_ + "/AtomicityC";
  NeugDB db;
  std::shared_ptr<neug::NeugDBService> svc =
      neug_AtomicityInit(db, dir, thread_num_);
  auto committed = neug_AtomicityCheck(svc);
  std::atomic<int> num_aborted_txns(0), num_committed_txns(0);
  neug_parallel_transaction(
      svc,
      [&](neug::NeugDBSession& session, int txn_id) {
        bool successful =
            neug_AtomicityC(session, 3 + txn_id, "alice@otherdomain.net", 2020);
        if (successful)
          num_committed_txns.fetch_add(1);
        else
          num_aborted_txns.fetch_add(1);
      },
      50);
  committed.first += num_committed_txns.load();
  committed.second += num_committed_txns.load();
  auto finalstate = neug_AtomicityCheck(svc);
  ASSERT_EQ(committed, finalstate);
}

TEST_F(NeugDBACIDTest, AtomicityRB) {
  std::string dir = work_dir_ + "/AtomicityRB";
  NeugDB db;
  auto svc = neug_AtomicityInit(db, dir, thread_num_);
  auto committed = neug_AtomicityCheck(svc);
  std::atomic<int> num_aborted_txns(0), num_committed_txns(0);
  neug_parallel_transaction(
      svc,
      [&](neug::NeugDBSession& session, int txn_id) {
        bool successful;
        if (txn_id % 2 == 0) {
          successful =
              neug_AtomicityRB(session, 2, "alice@otherdomain.net", 2020);
        } else {
          successful = neug_AtomicityRB(session, 3 + txn_id,
                                        "alice@otherdomain.net", 2020);
        }
        if (successful) {
          num_committed_txns.fetch_add(1);
        } else {
          num_aborted_txns.fetch_add(1);
        }
      },
      50);
  committed.first += num_committed_txns.load();
  committed.second += num_committed_txns.load();
  auto finalstate = neug_AtomicityCheck(svc);
  ASSERT_EQ(committed, finalstate);
}

// --- G0 (Dirty Writes) ---
TEST_F(NeugDBACIDTest, G0) {
  std::string dir = work_dir_ + "/G0";
  NeugDB db;
  auto svc = G0Init(db, dir, thread_num_);
  neug_parallel_transaction(
      svc,
      [&](neug::NeugDBSession& db, int txn_id) {
        std::random_device rand_dev;
        std::mt19937 gen(rand_dev());
        std::uniform_int_distribution<int> dist(1, 100);
        int picked = dist(gen) * 2 - 1;
        G0(db, picked, picked + 1, txn_id + 1);
      },
      200);
  std::string p1_version_history, p2_version_history, k_version_history;
  std::tie(p1_version_history, p2_version_history, k_version_history) =
      G0Check(db, svc, 1, 2);
  ASSERT_EQ(p1_version_history, p2_version_history);
  ASSERT_EQ(p2_version_history, k_version_history);
}

// --- G1A (Aborted Reads) ---
TEST_F(NeugDBACIDTest, G1A) {
  std::string dir = work_dir_ + "/G1A";
  NeugDB db;
  auto svc = G1AInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& db, int client_id) {
    if (client_id < rc) {
      for (int i = 0; i < 100; ++i) {
        auto p_version = G1A2(db);
        if (p_version != 1)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i) {
        G1A1(db);
      }
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- G1B (Intermediate Reads) ---
TEST_F(NeugDBACIDTest, G1B) {
  std::string dir = work_dir_ + "/G1B";
  NeugDB db;
  auto svc = G1BInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    if (client_id < rc) {
      for (int i = 0; i < 100; ++i) {
        auto p_version = G1B2(session);
        if (p_version % 2 != 1)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i) {
        G1B1(session, 0, 1);
      }
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- G1C (Circular Information Flow) ---
TEST_F(NeugDBACIDTest, G1C) {
  std::string dir = work_dir_ + "/G1C";
  NeugDB db;
  auto svc = G1CInit(db, dir, thread_num_);
  int64_t c = 1000;
  std::vector<int64_t> results(c);
  neug_parallel_transaction(
      svc,
      [&](neug::NeugDBSession& session, int txn_id) {
        std::random_device rand_dev;
        std::mt19937 gen(rand_dev());
        std::uniform_int_distribution<int> dist(1, 100);
        int64_t person1_id = dist(gen);
        int64_t person2_id;
        do {
          person2_id = dist(gen);
        } while (person1_id == person2_id);
        results[txn_id] = G1C(session, person1_id, person2_id, txn_id + 1);
      },
      c);
  int64_t num_incorrect_checks = 0;
  for (int64_t i = 1; i <= c; i++) {
    auto v1 = results[i - 1];
    if (v1 == 0)
      continue;
    auto v2 = results[v1 - 1];
    if (v2 == -1 || i == v2)
      num_incorrect_checks++;
  }
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- IMP (Item-Many-Preceders) ---
TEST_F(NeugDBACIDTest, IMP) {
  std::string dir = work_dir_ + "/IMP";
  NeugDB db;
  auto svc = IMPInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    if (client_id < rc) {
      std::random_device rand_dev;
      std::mt19937 gen(rand_dev());
      std::uniform_int_distribution<int> dist(1, 100);
      for (int i = 0; i < 100; ++i) {
        int picked = dist(gen);
        int64_t v1, v2;
        std::tie(v1, v2) = IMP2(session, picked);
        if (v1 != v2)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i)
        IMP1(session);
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- PMP (Predicate-Many-Preceders) ---
TEST_F(NeugDBACIDTest, PMP) {
  std::string dir = work_dir_ + "/PMP";
  NeugDB db;
  auto svc = PMPInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  std::atomic<int64_t> num_aborted_txns(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    if (client_id < rc) {
      for (int i = 0; i < 100; ++i) {
        int64_t v1, v2;
        int post_id = dist(gen);
        std::tie(v1, v2) = PMP2(session, post_id);
        if (v1 != v2)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i) {
        int person_id = dist(gen);
        int post_id = dist(gen);
        if (!PMP1(session, person_id, post_id))
          num_aborted_txns.fetch_add(1);
      }
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- OTV (Observed Transaction Vanishes) ---
TEST_F(NeugDBACIDTest, OTV) {
  std::string dir = work_dir_ + "/OTV";
  NeugDB db;
  auto svc = OTVInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    if (client_id < rc) {
      for (int i = 0; i < 100; ++i) {
        std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
        std::tie(tup1, tup2) = OTV2(session, dist(gen) * 4 + 1);
        int64_t v1_max = std::max({std::get<0>(tup1), std::get<1>(tup1),
                                   std::get<2>(tup1), std::get<3>(tup1)});
        int64_t v2_min = std::min({std::get<0>(tup2), std::get<1>(tup2),
                                   std::get<2>(tup2), std::get<3>(tup2)});
        if (v1_max > v2_min)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i)
        OTV1(session, dist(gen) * 4 + 1);
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- FR (Fractured Reads) ---
TEST_F(NeugDBACIDTest, FR) {
  std::string dir = work_dir_ + "/FR";
  NeugDB db;
  auto svc = FRInit(db, dir, thread_num_);
  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num_ / 2;
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    if (client_id < rc) {
      for (int i = 0; i < 100; ++i) {
        std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
        std::tie(tup1, tup2) = FR2(session, dist(gen) * 4 + 1);
        if (tup1 != tup2)
          num_incorrect_checks.fetch_add(1);
      }
    } else {
      for (int i = 0; i < 100; ++i)
        FR1(session, dist(gen) * 4 + 1);
    }
  });
  ASSERT_EQ(num_incorrect_checks, 0);
}

// --- LU (Lost Updates) ---
TEST_F(NeugDBACIDTest, LU) {
  std::string dir = work_dir_ + "/LU";
  NeugDB db;
  auto svc = LUInit(db, dir, thread_num_);
  std::map<int64_t, int64_t> expNumFriends;
  std::mutex mtx;
  std::atomic<int64_t> num_aborted_txns(0);
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    std::map<int64_t, int64_t> localExpNumFriends;
    for (int i = 0; i < 100; ++i) {
      int64_t person_id = dist(gen);
      if (LU1(session, person_id))
        ++localExpNumFriends[person_id];
      else
        num_aborted_txns.fetch_add(1);
    }
    std::lock_guard<std::mutex> lock(mtx);
    for (auto& pair : localExpNumFriends)
      expNumFriends[pair.first] += pair.second;
  });
  auto sess = svc->AcquireSession();
  std::map<int64_t, int64_t> numFriends = LU2(*sess.get());
  ASSERT_EQ(numFriends, expNumFriends);
}

// --- WS (Write Skews) ---
TEST_F(NeugDBACIDTest, WS) {
  std::string dir = work_dir_ + "/WS";
  NeugDB db;
  auto svc = WSInit(db, dir, thread_num_);
  neug_parallel_client(svc, [&](neug::NeugDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    for (int i = 0; i < 100; ++i) {
      int64_t person1_id = dist(gen) * 2 - 1;
      int64_t person2_id = person1_id + 1;
      WS1(session, person1_id, person2_id, gen);
    }
  });
  auto sess = svc->AcquireSession();
  auto results = WS2(*sess.get());
  ASSERT_TRUE(results.empty());
}

// ============================================================================
// Concurrency / Snapshot Isolation Tests
//
// Validate the SnapshotStore-based MVCC contract:
//   1) Read/Insert/Update lock granularity matches design (reads parallel,
//      inserts in-place, only commit briefly exclusive)
//   2) Snapshot isolation: readers see consistent point-in-time state
//   3) UpdateTransaction COW isolation + rollback correctness
//   4) Commit-window atomicity: reads always see fully-pre or fully-post commit
//
// To run under thread sanitizer:
//   cmake -DENABLE_THREAD_SANITIZER=ON -DBUILD_TEST=ON ..
//   make transaction_test
//   TSAN_OPTIONS="halt_on_error=1" ./tests/transaction/transaction_test \
//       --gtest_filter='NeugDBACIDTest.*Concurrent*:NeugDBACIDTest.*Reader*:\
//                       NeugDBACIDTest.*Update*:NeugDBACIDTest.*Commit*:\
//                       NeugDBACIDTest.*Reads*'
// ============================================================================

namespace concurrency_test {

constexpr int kSeedVertices = 100;
constexpr int kSeedEdges = 500;

// Schema: person(id INT64 PK, name STRING, age INT64) + knows(weight DOUBLE)
// Seeded with kSeedVertices vertices (id 1..N, age 20+i) and kSeedEdges edges.
std::shared_ptr<NeugDBService> cc_init(NeugDB& db, const std::string& work_dir,
                                       int thread_num) {
  db.Open(work_dir, thread_num);
  auto svc = std::make_shared<NeugDBService>(db);
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE person (id INT64, name STRING, age INT64, "
        "PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);"));
  }
  auto person_label = db.schema().get_vertex_label_id("person");
  auto knows_label = db.schema().get_edge_label_id("knows");

  auto& mutable_pg = const_cast<PropertyGraph&>(db.graph());
  mutable_pg.EnsureCapacity(person_label, kSeedVertices * 4);
  mutable_pg.EnsureCapacity(person_label, person_label, knows_label,
                            kSeedEdges * 4);

  auto sess = svc->AcquireSession();
  auto txn = sess->GetInsertTransaction();
  std::vector<vid_t> vids;
  vids.reserve(kSeedVertices);
  for (int i = 1; i <= kSeedVertices; ++i) {
    std::string name = "person_" + std::to_string(i);
    int64_t age = 20 + i;
    vid_t vid;
    CHECK(txn.AddVertex(person_label, Property::from_int64(i),
                        {Property::from_string_view(name),
                         Property::from_int64(age)},
                        vid));
    vids.push_back(vid);
  }
  std::mt19937 gen(42);
  std::uniform_int_distribution<int> vdist(0, kSeedVertices - 1);
  for (int e = 0; e < kSeedEdges; ++e) {
    int s = vdist(gen);
    int d = vdist(gen);
    if (s == d) d = (d + 1) % kSeedVertices;
    CHECK(txn.AddEdge(person_label, vids[s], person_label, vids[d], knows_label,
                      {Property::from_double(0.1 * e)}));
  }
  txn.Commit();
  return svc;
}

// Read age via a fresh ReadTxn from a freshly-acquired session.
int64_t cc_read_age(NeugDBService& svc, int64_t person_id) {
  auto sess = svc.AcquireSession();
  auto txn = sess->GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto person_label = svc.db().schema().get_vertex_label_id("person");
  vid_t vid;
  if (!gi.GetVertexIndex(person_label, Property::from_int64(person_id), vid)) {
    return -1;
  }
  return gi.GetVertexProperty(person_label, vid, 1).as_int64();
}

// Read age via a fresh ReadTxn on a caller-owned session (no contention on
// session pool acquisition).
int64_t cc_read_age(NeugDBSession& sess, NeugDB& db, int64_t person_id) {
  auto txn = sess.GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto person_label = db.schema().get_vertex_label_id("person");
  vid_t vid;
  if (!gi.GetVertexIndex(person_label, Property::from_int64(person_id), vid)) {
    return -1;
  }
  return gi.GetVertexProperty(person_label, vid, 1).as_int64();
}

// Read age via caller-owned session, returning {age, elapsed_ns}.
// Used by the two-phase latency measurement test.
std::pair<int64_t, int64_t> cc_read_age_timed(NeugDBSession& sess, NeugDB& db,
                                               int64_t person_id) {
  auto t0 = std::chrono::high_resolution_clock::now();
  auto txn = sess.GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto person_label = db.schema().get_vertex_label_id("person");
  vid_t vid;
  int64_t age = -1;
  if (gi.GetVertexIndex(person_label, Property::from_int64(person_id), vid)) {
    age = gi.GetVertexProperty(person_label, vid, 1).as_int64();
  }
  auto t1 = std::chrono::high_resolution_clock::now();
  int64_t ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
  return {age, ns};
}

// Read age via an existing ReadTransaction's captured snapshot — used to
// verify that a held snapshot remains coherent across concurrent commits.
int64_t cc_read_age_via(const ReadTransaction& txn, NeugDB& db,
                        int64_t person_id) {
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto person_label = db.schema().get_vertex_label_id("person");
  vid_t vid;
  if (!gi.GetVertexIndex(person_label, Property::from_int64(person_id), vid)) {
    return -1;
  }
  return gi.GetVertexProperty(person_label, vid, 1).as_int64();
}

// Update age via a fresh UpdateTxn + Commit. Returns true on success.
bool cc_update_age(NeugDBService& svc, int64_t person_id, int64_t new_age) {
  auto sess = svc.AcquireSession();
  auto txn = sess->GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);
  auto person_label = svc.db().schema().get_vertex_label_id("person");
  vid_t vid;
  if (!gui.GetVertexIndex(person_label, Property::from_int64(person_id), vid)) {
    txn.Abort();
    return false;
  }
  gui.UpdateVertexProperty(person_label, vid, 1, Property::From(new_age));
  return txn.Commit();
}

// Count visible person vertices.
size_t cc_count_persons(NeugDBService& svc) {
  auto sess = svc.AcquireSession();
  auto txn = sess->GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto person_label = svc.db().schema().get_vertex_label_id("person");
  size_t n = 0;
  for ([[maybe_unused]] auto v : gi.GetVertexSet(person_label)) ++n;
  return n;
}

// ===== Helpers for COW snapshot isolation tests (DML + DDL) =====
//
// Each "*_via" helper reads through a held ReadTransaction's frozen GraphView
// (`txn.view()`), so the observation reflects the snapshot pinned in the
// SnapshotStore slot — not the live PropertyGraph (which a concurrent writer
// may have replaced via installSnapshot).

// Count visible vertices for a label via a held read snapshot.
size_t cc_count_vertices_via(const ReadTransaction& txn, label_t label) {
  StorageReadInterface gi(txn.view(), txn.timestamp());
  size_t n = 0;
  for ([[maybe_unused]] auto v : gi.GetVertexSet(label)) ++n;
  return n;
}

// Count outgoing edges from src_vid for (src,dst,edge) via held snapshot.
size_t cc_count_oe_from_via(const ReadTransaction& txn, label_t src_label,
                            label_t dst_label, label_t edge_label,
                            vid_t src_vid) {
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto view = gi.GetGenericOutgoingGraphView(src_label, dst_label, edge_label);
  size_t n = 0;
  auto edges = view.get_edges(src_vid);
  for (auto it = edges.begin(); it != edges.end(); ++it) ++n;
  return n;
}

// Count outgoing edges across every src vertex via held snapshot.
size_t cc_count_all_oe_via(const ReadTransaction& txn, label_t src_label,
                           label_t dst_label, label_t edge_label) {
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto view = gi.GetGenericOutgoingGraphView(src_label, dst_label, edge_label);
  size_t n = 0;
  for (auto v : gi.GetVertexSet(src_label)) {
    auto edges = view.get_edges(v);
    for (auto it = edges.begin(); it != edges.end(); ++it) ++n;
  }
  return n;
}

// Same as cc_count_all_oe_via but on a fresh ReadTransaction (live snapshot).
size_t cc_count_all_oe(NeugDBService& svc, const char* src, const char* dst,
                       const char* edge) {
  auto sess = svc.AcquireSession();
  auto txn = sess->GetReadTransaction();
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto sl = gi.schema().get_vertex_label_id(src);
  auto dl = gi.schema().get_vertex_label_id(dst);
  auto el = gi.schema().get_edge_label_id(edge);
  auto view = gi.GetGenericOutgoingGraphView(sl, dl, el);
  size_t n = 0;
  for (auto v : gi.GetVertexSet(sl)) {
    auto edges = view.get_edges(v);
    for (auto it = edges.begin(); it != edges.end(); ++it) ++n;
  }
  return n;
}

// Read knows.weight on the edge from src_oid → dst_oid via held snapshot.
// Returns NaN if the edge is not visible.
double cc_read_knows_weight_via(const ReadTransaction& txn, NeugDB& db,
                                int64_t src_oid, int64_t dst_oid) {
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto p_label = gi.schema().get_vertex_label_id("person");
  auto e_label = gi.schema().get_edge_label_id("knows");
  vid_t src_vid, dst_vid;
  if (!gi.GetVertexIndex(p_label, Property::from_int64(src_oid), src_vid))
    return std::nan("");
  if (!gi.GetVertexIndex(p_label, Property::from_int64(dst_oid), dst_vid))
    return std::nan("");
  auto view = gi.GetGenericOutgoingGraphView(p_label, p_label, e_label);
  auto accessor = gi.GetEdgeDataAccessor(p_label, p_label, e_label, 0);
  auto edges = view.get_edges(src_vid);
  for (auto it = edges.begin(); it != edges.end(); ++it) {
    if (it.get_vertex() == dst_vid) {
      return accessor.get_data(it).as_double();
    }
  }
  (void)db;
  return std::nan("");
}

// Create person→software "created" edge type with 2 properties (weight DOUBLE,
// since INT64) — the ≥2-property path is unbundled (separate property table).
// Also adds one software vertex (id=1) and one created edge person 1→software 1.
void cc_setup_unbundled_created(NeugDBService& svc) {
  auto sess = svc.AcquireSession();
  auto txn = sess->GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn);

  CreateVertexTypeParamBuilder sb;
  ASSERT_TRUE(gui.CreateVertexType(
                     sb.VertexLabel("software")
                         .AddProperty(DataTypeId::kInt64, "id",
                                      Property::from_int64(0))
                         .AddProperty(DataTypeId::kVarchar, "name",
                                      Property::from_string_view(""))
                         .AddPrimaryKeyName("id")
                         .Build())
                  .ok());

  CreateEdgeTypeParamBuilder eb;
  ASSERT_TRUE(gui.CreateEdgeType(
                     eb.SrcLabel("person")
                         .DstLabel("software")
                         .EdgeLabel("created")
                         .AddProperty(DataTypeId::kDouble, "weight",
                                      Property::from_double(0.0))
                         .AddProperty(DataTypeId::kInt64, "since",
                                      Property::from_int64(0))
                         .Build())
                  .ok());

  auto sw_label = gui.schema().get_vertex_label_id("software");
  auto p_label = gui.schema().get_vertex_label_id("person");
  auto e_label = gui.schema().get_edge_label_id("created");

  vid_t sw_vid;
  ASSERT_TRUE(gui.AddVertex(sw_label, Property::from_int64(1),
                            {Property::from_string_view("NeugDB")}, sw_vid));

  vid_t p1_vid;
  ASSERT_TRUE(gui.GetVertexIndex(p_label, Property::from_int64(1), p1_vid));
  ASSERT_TRUE(gui.AddEdge(p_label, p1_vid, sw_label, sw_vid, e_label,
                          {Property::from_double(0.5),
                           Property::from_int64(2020)}));

  ASSERT_TRUE(txn.Commit());
}

// Read the `since` property on the unbundled created edge person 1→software 1
// via a held snapshot. Returns -1 if not visible.
int64_t cc_read_created_since_via(const ReadTransaction& txn) {
  StorageReadInterface gi(txn.view(), txn.timestamp());
  auto p_label = gi.schema().get_vertex_label_id("person");
  auto sw_label = gi.schema().get_vertex_label_id("software");
  auto e_label = gi.schema().get_edge_label_id("created");
  vid_t p1_vid, sw_vid;
  if (!gi.GetVertexIndex(p_label, Property::from_int64(1), p1_vid)) return -1;
  if (!gi.GetVertexIndex(sw_label, Property::from_int64(1), sw_vid)) return -1;
  auto view = gi.GetGenericOutgoingGraphView(p_label, sw_label, e_label);
  auto accessor = gi.GetEdgeDataAccessor(p_label, sw_label, e_label, 1);
  auto edges = view.get_edges(p1_vid);
  for (auto it = edges.begin(); it != edges.end(); ++it) {
    if (it.get_vertex() == sw_vid) {
      return accessor.get_data(it).as_int64();
    }
  }
  return -1;
}

}  // namespace concurrency_test

using namespace concurrency_test;

// ============================================================================
// Category 1 — Read/Insert/Update concurrency safety + granularity
// ============================================================================

TEST_F(NeugDBACIDTest, ConcurrentReadsConsistent) {
  std::string dir = work_dir_ + "/ConcReadsConsistent";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  std::atomic<int64_t> bad_reads{0};
  neug_parallel_client(svc, [&](NeugDBSession& sess, int) {
    for (int i = 0; i < 1000; ++i) {
      int pid = (i % kSeedVertices) + 1;
      int64_t expected = 20 + pid;
      if (cc_read_age(sess, db, pid) != expected) {
        bad_reads.fetch_add(1);
      }
    }
  });
  EXPECT_EQ(bad_reads.load(), 0);
}

TEST_F(NeugDBACIDTest, ConcurrentInsertsCommitInOrder) {
  std::string dir = work_dir_ + "/ConcInsertsOrder";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  // Pre-allocate space for the parallel inserts.
  auto& mutable_pg = const_cast<PropertyGraph&>(db.graph());
  mutable_pg.EnsureCapacity(db.schema().get_vertex_label_id("person"),
                            kSeedVertices + 16 * 50 * 4);

  constexpr int kPerThread = 50;
  size_t before = cc_count_persons(*svc);

  neug_parallel_client(svc, [&](NeugDBSession& sess, int tid) {
    int64_t base = 100000 + tid * kPerThread;
    auto person_label = db.schema().get_vertex_label_id("person");
    for (int i = 0; i < kPerThread; ++i) {
      auto txn = sess.GetInsertTransaction();
      vid_t vid;
      ASSERT_TRUE(txn.AddVertex(
          person_label, Property::from_int64(base + i),
          {Property::from_string_view("inserted"), Property::from_int64(99)},
          vid));
      ASSERT_TRUE(txn.Commit());
    }
  });

  size_t after = cc_count_persons(*svc);
  EXPECT_EQ(after, before + svc->SessionNum() * kPerThread);

  for (size_t tid = 0; tid < svc->SessionNum(); ++tid) {
    int64_t base = 100000 + static_cast<int64_t>(tid) * kPerThread;
    for (int i = 0; i < kPerThread; ++i) {
      EXPECT_EQ(cc_read_age(*svc, base + i), 99)
          << "tid=" << tid << " i=" << i;
    }
  }
}

TEST_F(NeugDBACIDTest, ConcurrentReadsAndInsertsDoNotInterfere) {
  std::string dir = work_dir_ + "/ConcReadsInserts";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto& mutable_pg = const_cast<PropertyGraph&>(db.graph());
  mutable_pg.EnsureCapacity(db.schema().get_vertex_label_id("person"),
                            kSeedVertices * 8);

  std::atomic<bool> stop{false};
  std::atomic<int64_t> reader_observations{0};
  std::atomic<int64_t> reader_anomalies{0};
  std::atomic<int64_t> insert_count{0};
  std::atomic<int64_t> next_id{200000};

  std::vector<std::thread> threads;
  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&] {
      auto guard = svc->AcquireSession();
      while (!stop.load()) {
        // Person 5 has age 25 in the seed and is never updated.
        int64_t got = cc_read_age(*guard.get(), db, 5);
        if (got != 25) reader_anomalies.fetch_add(1);
        reader_observations.fetch_add(1);
      }
    });
  }
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([&] {
      auto guard = svc->AcquireSession();
      auto person_label = db.schema().get_vertex_label_id("person");
      while (!stop.load()) {
        int64_t id = next_id.fetch_add(1);
        auto txn = guard->GetInsertTransaction();
        vid_t vid;
        if (txn.AddVertex(
                person_label, Property::from_int64(id),
                {Property::from_string_view("x"), Property::from_int64(99)},
                vid) &&
            txn.Commit()) {
          insert_count.fetch_add(1);
        }
      }
    });
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  stop.store(true);
  for (auto& t : threads) t.join();

  EXPECT_EQ(reader_anomalies.load(), 0)
      << "Reader saw inconsistent state under concurrent insert";
  EXPECT_GT(reader_observations.load(), 0);
  EXPECT_GT(insert_count.load(), 0);
}

TEST_F(NeugDBACIDTest, ConcurrentUpdatesAreSerializedAtCommit) {
  std::string dir = work_dir_ + "/ConcUpdatesSerial";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  constexpr int kThreads = 8;
  std::vector<std::thread> threads;
  std::atomic<int> commit_count{0};
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      if (cc_update_age(*svc, i + 1, 1000 + i)) {
        commit_count.fetch_add(1);
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(commit_count.load(), kThreads);
  for (int i = 0; i < kThreads; ++i) {
    EXPECT_EQ(cc_read_age(*svc, i + 1), 1000 + i) << "vertex " << (i + 1);
  }
}

// ============================================================================
// Category 2 — SnapshotStore correctness for concurrent queries + isolation
// ============================================================================

TEST_F(NeugDBACIDTest, ReaderHoldingOldSnapshotIsUnaffectedByCommit) {
  std::string dir = work_dir_ + "/ReaderUnaffected";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  EXPECT_EQ(cc_read_age_via(txn_r, db, 5), 25);

  // Concurrent update commits a new snapshot.
  EXPECT_TRUE(cc_update_age(*svc, 5, 999));

  // R still sees pre-commit state at its captured timestamp.
  EXPECT_EQ(cc_read_age_via(txn_r, db, 5), 25);

  // A fresh ReadTxn sees post-commit state.
  EXPECT_EQ(cc_read_age(*svc, 5), 999);
}

TEST_F(NeugDBACIDTest, InserterTimestampVisibility) {
  std::string dir = work_dir_ + "/InsertTsVis";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();

  // Pre-insert: id=9999 absent.
  EXPECT_EQ(cc_read_age_via(txn_r, db, 9999), -1);

  // Concurrent insert (in-place into R's slot's PG).
  {
    auto sess_w = svc->AcquireSession();
    auto txn_w = sess_w->GetInsertTransaction();
    auto person_label = db.schema().get_vertex_label_id("person");
    vid_t vid;
    ASSERT_TRUE(txn_w.AddVertex(
        person_label, Property::from_int64(9999),
        {Property::from_string_view("late"), Property::from_int64(50)}, vid));
    ASSERT_TRUE(txn_w.Commit());
  }

  // R's snapshot must filter the new vertex (insert ts > read ts).
  EXPECT_EQ(cc_read_age_via(txn_r, db, 9999), -1)
      << "Reader at ts0 must not see vertex inserted at ts1 > ts0";

  // Fresh ReadTxn sees it.
  EXPECT_EQ(cc_read_age(*svc, 9999), 50);
}

// ============================================================================
// Category 3 — UpdateTransaction COW isolation + rollback correctness
// ============================================================================

TEST_F(NeugDBACIDTest, UpdateForkDoesNotAffectActiveReaders) {
  std::string dir = work_dir_ + "/UpdateForkIso";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  EXPECT_EQ(cc_read_age_via(txn_r, db, 5), 25);

  // Open U and mutate without committing.
  auto sess_u = svc->AcquireSession();
  auto txn_u = sess_u->GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn_u);
  auto person_label = db.schema().get_vertex_label_id("person");
  vid_t vid_u;
  ASSERT_TRUE(gui.GetVertexIndex(person_label, Property::from_int64(5), vid_u));
  gui.UpdateVertexProperty(person_label, vid_u, 1,
                           Property::From<int64_t>(7777));

  // R still sees pre-fork state (no concurrent commit).
  EXPECT_EQ(cc_read_age_via(txn_r, db, 5), 25);
  // Fresh ReadTxn also sees pre-fork state (U hasn't committed).
  EXPECT_EQ(cc_read_age(*svc, 5), 25);

  txn_u.Abort();
  EXPECT_EQ(cc_read_age(*svc, 5), 25);
}

TEST_F(NeugDBACIDTest, UpdateRollbackOfDMLLeavesOriginalIntact) {
  std::string dir = work_dir_ + "/UpdateRollbackDML";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  EXPECT_EQ(cc_read_age(*svc, 5), 25);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    StorageTPUpdateInterface gui(txn);
    auto person_label = db.schema().get_vertex_label_id("person");
    vid_t vid;
    ASSERT_TRUE(gui.GetVertexIndex(person_label, Property::from_int64(5), vid));
    gui.UpdateVertexProperty(person_label, vid, 1,
                             Property::From<int64_t>(125));
    txn.Abort();
  }

  EXPECT_EQ(cc_read_age(*svc, 5), 25);
}

TEST_F(NeugDBACIDTest, UpdateRollbackOfDDLLeavesOriginalIntact) {
  std::string dir = work_dir_ + "/UpdateRollbackDDL";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  size_t labels_pre = svc->db().schema().vertex_label_num();

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    CreateVertexTypeParamBuilder b;
    auto status = txn.CreateVertexType(
        b.VertexLabel("foo")
            .AddProperty(DataTypeId::kInt64, "x", Property::from_int64(0))
            .AddPrimaryKeyName("x")
            .Build());
    ASSERT_TRUE(status.ok()) << "CreateVertexType setup failed";
    txn.Abort();
  }

  // After abort, schema label count is unchanged.
  EXPECT_EQ(svc->db().schema().vertex_label_num(), labels_pre);
  EXPECT_THROW(svc->db().schema().get_vertex_label_id("foo"), std::exception);
}

TEST_F(NeugDBACIDTest, UpdateRollbackOfEdgePropertyDeleteLeavesOriginalIntact) {
  std::string dir = work_dir_ + "/UpdateRollbackEdgeProp";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  // Capture pre-state edge count + a sample weight.
  auto person_label = db.schema().get_vertex_label_id("person");
  auto knows_label = db.schema().get_edge_label_id("knows");

  auto count_edges = [&]() {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    auto view =
        gi.GetGenericOutgoingGraphView(person_label, person_label, knows_label);
    size_t count = 0;
    for (auto v : gi.GetVertexSet(person_label)) {
      auto edges = view.get_edges(v);
      for (auto it = edges.begin(); it != edges.end(); ++it) ++count;
    }
    return count;
  };

  size_t edge_count_pre = count_edges();
  EXPECT_GT(edge_count_pre, 0u);

  // Open U, drop the weight property, then abort.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    DeleteEdgePropertiesParamBuilder b;
    auto status = txn.DeleteEdgeProperties(b.SrcLabel("person")
                                               .DstLabel("person")
                                               .EdgeLabel("knows")
                                               .AddDeleteProperty("weight")
                                               .Build());
    ASSERT_TRUE(status.ok()) << "DeleteEdgeProperties setup failed";
    txn.Abort();
  }

  // Original CSR + weight column intact.
  EXPECT_EQ(count_edges(), edge_count_pre);
}

// ============================================================================
// Category 3a — UpdateTransaction COW isolation across single-row DML.
//
// Pattern: open ReadTxn R, observe pre-state via R's frozen GraphView; open a
// separate UpdateTxn U on another session, mutate + Commit; R must continue to
// observe pre-state (its snapshot is pinned in its StorageStore slot, never
// mutated in place); a fresh ReadTxn must observe post-state.
// ============================================================================

TEST_F(NeugDBACIDTest, AddVertexCommitDoesNotAffectHeldReader) {
  std::string dir = work_dir_ + "/AddVertexCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  auto p_label = db.schema().get_vertex_label_id("person");
  size_t n_pre = cc_count_vertices_via(txn_r, p_label);
  EXPECT_EQ(n_pre, static_cast<size_t>(kSeedVertices));

  // Concurrent UpdateTxn commits a new vertex.
  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    StorageTPUpdateInterface gui(txn_u);
    vid_t vid;
    ASSERT_TRUE(gui.AddVertex(
        p_label, Property::from_int64(900001),
        {Property::from_string_view("late"), Property::from_int64(77)}, vid));
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: count unchanged, new pk not resolvable.
  EXPECT_EQ(cc_count_vertices_via(txn_r, p_label), n_pre);
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    vid_t v;
    EXPECT_FALSE(gi.GetVertexIndex(p_label, Property::from_int64(900001), v));
  }

  // Fresh reader: count + 1.
  EXPECT_EQ(cc_count_persons(*svc), n_pre + 1);
  EXPECT_EQ(cc_read_age(*svc, 900001), 77);
}

TEST_F(NeugDBACIDTest, DeleteVertexCommitDoesNotAffectHeldReader) {
  std::string dir = work_dir_ + "/DeleteVertexCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto p_label = db.schema().get_vertex_label_id("person");
  auto e_label = db.schema().get_edge_label_id("knows");

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  size_t n_pre = cc_count_vertices_via(txn_r, p_label);
  size_t e_pre = cc_count_all_oe_via(txn_r, p_label, p_label, e_label);
  EXPECT_GT(e_pre, 0u);
  EXPECT_EQ(cc_read_age_via(txn_r, db, 5), 25);

  // Writer deletes vertex 5 (and cascades its incident edges via EdgeTable::
  // DeleteVertex → DeleteEdge → ensureDeleteReady — the regression path for
  // the ensureDeleteReady signature fix).
  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    vid_t v5;
    ASSERT_TRUE(txn_u.GetVertexIndex(p_label, Property::from_int64(5), v5));
    EXPECT_TRUE(txn_u.DeleteVertex(p_label, v5));
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: vertex 5 still resolvable, vertex count + edge count unchanged.
  EXPECT_EQ(cc_read_age_via(txn_r, db, 5), 25);
  EXPECT_EQ(cc_count_vertices_via(txn_r, p_label), n_pre);
  EXPECT_EQ(cc_count_all_oe_via(txn_r, p_label, p_label, e_label), e_pre);

  // Fresh reader: vertex 5 unresolvable, count decremented.
  EXPECT_EQ(cc_read_age(*svc, 5), -1);
  EXPECT_EQ(cc_count_persons(*svc), n_pre - 1);
}

TEST_F(NeugDBACIDTest, AddEdgeCommitDoesNotAffectHeldReader) {
  std::string dir = work_dir_ + "/AddEdgeCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto p_label = db.schema().get_vertex_label_id("person");
  auto e_label = db.schema().get_edge_label_id("knows");

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  vid_t r_v1;
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    ASSERT_TRUE(gi.GetVertexIndex(p_label, Property::from_int64(1), r_v1));
  }
  size_t oe_pre = cc_count_oe_from_via(txn_r, p_label, p_label, e_label, r_v1);
  size_t total_pre = cc_count_all_oe_via(txn_r, p_label, p_label, e_label);

  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    StorageTPUpdateInterface gui(txn_u);
    vid_t s, d;
    ASSERT_TRUE(gui.GetVertexIndex(p_label, Property::from_int64(1), s));
    ASSERT_TRUE(gui.GetVertexIndex(p_label, Property::from_int64(2), d));
    EXPECT_TRUE(gui.AddEdge(p_label, s, p_label, d, e_label,
                            {Property::from_double(0.55)}));
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: counts unchanged.
  EXPECT_EQ(cc_count_oe_from_via(txn_r, p_label, p_label, e_label, r_v1),
            oe_pre);
  EXPECT_EQ(cc_count_all_oe_via(txn_r, p_label, p_label, e_label), total_pre);

  // Fresh reader: total + 1.
  EXPECT_EQ(cc_count_all_oe(*svc, "person", "person", "knows"), total_pre + 1);
}

TEST_F(NeugDBACIDTest, DeleteEdgeCommitDoesNotAffectHeldReader) {
  std::string dir = work_dir_ + "/DeleteEdgeCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto p_label = db.schema().get_vertex_label_id("person");
  auto e_label = db.schema().get_edge_label_id("knows");

  // First add a known edge 1→2 so we have a deterministic target to delete.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    StorageTPUpdateInterface gui(txn);
    vid_t s, d;
    ASSERT_TRUE(gui.GetVertexIndex(p_label, Property::from_int64(1), s));
    ASSERT_TRUE(gui.GetVertexIndex(p_label, Property::from_int64(2), d));
    EXPECT_TRUE(gui.AddEdge(p_label, s, p_label, d, e_label,
                            {Property::from_double(0.42)}));
    EXPECT_TRUE(txn.Commit());
  }

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  size_t total_pre = cc_count_all_oe_via(txn_r, p_label, p_label, e_label);
  EXPECT_GT(total_pre, 0u);

  // Writer deletes ALL edges from 1→2 (DeleteEdges fans out via DeleteEdge →
  // ensureDeleteReady — the regression path for the signature fix).
  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    vid_t s, d;
    ASSERT_TRUE(txn_u.GetVertexIndex(p_label, Property::from_int64(1), s));
    ASSERT_TRUE(txn_u.GetVertexIndex(p_label, Property::from_int64(2), d));
    EXPECT_TRUE(txn_u.DeleteEdges(p_label, s, p_label, d, e_label));
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: total unchanged.
  EXPECT_EQ(cc_count_all_oe_via(txn_r, p_label, p_label, e_label), total_pre);

  // Fresh reader: strictly fewer (depends on how many 1→2 edges existed).
  size_t total_post = cc_count_all_oe(*svc, "person", "person", "knows");
  EXPECT_LT(total_post, total_pre);
}

// ============================================================================
// Category 3b — Edge property update isolation (bundled + unbundled paths).
// ============================================================================

TEST_F(NeugDBACIDTest, UpdateEdgePropertyCommitDoesNotAffectHeldReader_Bundled) {
  std::string dir = work_dir_ + "/UpdateEdgePropBundled";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto p_label = db.schema().get_vertex_label_id("person");
  auto e_label = db.schema().get_edge_label_id("knows");
  ASSERT_TRUE(db.schema().get_edge_schema(p_label, p_label, e_label)
                  ->is_bundled())
      << "knows has a single non-varchar property and must be bundled";

  // Set a deterministic baseline: weight 0.42 on 1→2.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    StorageTPUpdateInterface gui(txn);
    vid_t s, d;
    ASSERT_TRUE(gui.GetVertexIndex(p_label, Property::from_int64(1), s));
    ASSERT_TRUE(gui.GetVertexIndex(p_label, Property::from_int64(2), d));
    EXPECT_TRUE(gui.AddEdge(p_label, s, p_label, d, e_label,
                            {Property::from_double(0.42)}));
    EXPECT_TRUE(txn.Commit());
  }

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  double w_pre = cc_read_knows_weight_via(txn_r, db, 1, 2);
  EXPECT_EQ(w_pre, 0.42);

  // Writer updates that edge to 0.99.
  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    vid_t s, d;
    ASSERT_TRUE(txn_u.GetVertexIndex(p_label, Property::from_int64(1), s));
    ASSERT_TRUE(txn_u.GetVertexIndex(p_label, Property::from_int64(2), d));
    // Find the matching outgoing edge to set its weight to 0.99.
    auto oe_view =
        txn_u.GetGenericOutgoingGraphView(p_label, p_label, e_label);
    auto edges = oe_view.get_edges(s);
    int32_t oe_off = 0;
    bool found = false;
    for (auto it = edges.begin(); it != edges.end(); ++it, ++oe_off) {
      if (it.get_vertex() == d) {
        // Bundled edge → ie_offset unused (matched via inline property).
        txn_u.UpdateEdgeProperty(p_label, s, p_label, d, e_label, oe_off, 0, 0,
                                 Property::from_double(0.99));
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found);
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: still 0.42.
  EXPECT_EQ(cc_read_knows_weight_via(txn_r, db, 1, 2), 0.42);
  // Fresh reader: 0.99 visible on at least one of the edges.
  bool saw_99 = false;
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    auto view = gi.GetGenericOutgoingGraphView(p_label, p_label, e_label);
    auto accessor = gi.GetEdgeDataAccessor(p_label, p_label, e_label, 0);
    vid_t s, d;
    ASSERT_TRUE(gi.GetVertexIndex(p_label, Property::from_int64(1), s));
    ASSERT_TRUE(gi.GetVertexIndex(p_label, Property::from_int64(2), d));
    auto edges = view.get_edges(s);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      if (it.get_vertex() == d && accessor.get_data(it).as_double() == 0.99) {
        saw_99 = true;
        break;
      }
    }
  }
  EXPECT_TRUE(saw_99);
}

TEST_F(NeugDBACIDTest,
       UpdateEdgePropertyCommitDoesNotAffectHeldReader_Unbundled) {
  std::string dir = work_dir_ + "/UpdateEdgePropUnbundled";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  cc_setup_unbundled_created(*svc);

  // Confirm the created edge type is unbundled (≥2 properties).
  ASSERT_FALSE(db.schema()
                   .get_edge_schema(db.schema().get_vertex_label_id("person"),
                                    db.schema().get_vertex_label_id("software"),
                                    db.schema().get_edge_label_id("created"))
                   ->is_bundled())
      << "created has 2 properties and must be unbundled";

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  EXPECT_EQ(cc_read_created_since_via(txn_r), 2020);

  // Writer updates `since` to 2099 on the created edge person 1 → software 1.
  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    auto p_label = txn_u.schema().get_vertex_label_id("person");
    auto sw_label = txn_u.schema().get_vertex_label_id("software");
    auto e_label = txn_u.schema().get_edge_label_id("created");
    vid_t p1, sw1;
    ASSERT_TRUE(txn_u.GetVertexIndex(p_label, Property::from_int64(1), p1));
    ASSERT_TRUE(txn_u.GetVertexIndex(sw_label, Property::from_int64(1), sw1));
    txn_u.UpdateEdgeProperty(p_label, p1, sw_label, sw1, e_label, 0, 0, 1,
                             Property::from_int64(2099));
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: still sees 2020 via its snapshot.
  EXPECT_EQ(cc_read_created_since_via(txn_r), 2020);

  // Fresh reader: sees 2099.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    EXPECT_EQ(cc_read_created_since_via(txn), 2099);
  }
}

// ============================================================================
// Category 3c — Vertex schema DDL isolation (Add/Delete/Rename properties).
// ============================================================================

TEST_F(NeugDBACIDTest, AddVertexPropertiesCommitDoesNotAffectHeldReaderSchema) {
  std::string dir = work_dir_ + "/AddVertexPropsCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto p_label = db.schema().get_vertex_label_id("person");

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "name"), nullptr);
    EXPECT_EQ(gi.GetVertexPropColumn(p_label, "email"), nullptr);
    EXPECT_EQ(gi.GetVertexPropColumn(p_label, "height"), nullptr);
  }

  // Writer adds email + height.
  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    AddVertexPropertiesParamBuilder b;
    EXPECT_TRUE(txn_u.AddVertexProperties(
                       b.VertexLabel("person")
                           .AddProperty(DataTypeId::kVarchar, "email",
                                        Property::from_string_view(""))
                           .AddProperty(DataTypeId::kDouble, "height",
                                        Property::from_double(0.0))
                           .Build())
                    .ok());
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: schema unchanged, no new columns visible.
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "name"), nullptr);
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "age"), nullptr);
    EXPECT_EQ(gi.GetVertexPropColumn(p_label, "email"), nullptr);
    EXPECT_EQ(gi.GetVertexPropColumn(p_label, "height"), nullptr);
    // Pre-existing reads still work.
    EXPECT_EQ(cc_read_age_via(txn_r, db, 5), 25);
  }

  // Fresh reader: new columns present.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "email"), nullptr);
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "height"), nullptr);
  }
}

TEST_F(NeugDBACIDTest,
       DeleteVertexPropertiesCommitDoesNotAffectHeldReaderSchema) {
  std::string dir = work_dir_ + "/DeleteVertexPropsCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto p_label = db.schema().get_vertex_label_id("person");

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  EXPECT_EQ(cc_read_age_via(txn_r, db, 5), 25);
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "age"), nullptr);
  }

  // Writer drops `age`.
  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    DeleteVertexPropertiesParamBuilder b;
    EXPECT_TRUE(txn_u.DeleteVertexProperties(
                       b.VertexLabel("person").AddDeleteProperty("age").Build())
                    .ok());
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: column still readable.
  EXPECT_EQ(cc_read_age_via(txn_r, db, 5), 25);
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "age"), nullptr);
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "name"), nullptr);
  }

  // Fresh reader: age gone, name intact.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    EXPECT_EQ(gi.GetVertexPropColumn(p_label, "age"), nullptr);
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "name"), nullptr);
  }
}

TEST_F(NeugDBACIDTest,
       RenameVertexPropertiesCommitDoesNotAffectHeldReaderSchema) {
  std::string dir = work_dir_ + "/RenameVertexPropsCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto p_label = db.schema().get_vertex_label_id("person");

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "age"), nullptr);
    EXPECT_EQ(gi.GetVertexPropColumn(p_label, "years"), nullptr);
  }

  // Writer renames age → years.
  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    RenameVertexPropertiesParamBuilder b;
    EXPECT_TRUE(
        txn_u.RenameVertexProperties(b.VertexLabel("person")
                                         .AddRenameProperty("age", "years")
                                         .Build())
            .ok());
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: snapshot still sees `age`, not `years`.
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "age"), nullptr);
    EXPECT_EQ(gi.GetVertexPropColumn(p_label, "years"), nullptr);
    EXPECT_EQ(cc_read_age_via(txn_r, db, 5), 25);
  }

  // Fresh reader: sees `years`, not `age`.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    EXPECT_EQ(gi.GetVertexPropColumn(p_label, "age"), nullptr);
    EXPECT_NE(gi.GetVertexPropColumn(p_label, "years"), nullptr);
  }
}

// ============================================================================
// Category 3d — Edge schema DDL isolation (Add/Delete/Rename properties).
// ============================================================================

TEST_F(NeugDBACIDTest, AddEdgePropertiesCommitDoesNotAffectHeldReaderSchema) {
  std::string dir = work_dir_ + "/AddEdgePropsCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto p_label = db.schema().get_vertex_label_id("person");
  auto e_label = db.schema().get_edge_label_id("knows");

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    auto es = gi.schema().get_edge_schema(p_label, p_label, e_label);
    EXPECT_GE(es->get_property_index("weight"), 0);
    EXPECT_EQ(es->get_property_index("license"), -1);
  }

  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    AddEdgePropertiesParamBuilder b;
    EXPECT_TRUE(txn_u.AddEdgeProperties(
                       b.SrcLabel("person")
                           .DstLabel("person")
                           .EdgeLabel("knows")
                           .AddProperty(DataTypeId::kVarchar, "license",
                                        Property::from_string_view(""))
                           .Build())
                    .ok());
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: snapshot edge schema still has no `license`.
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    auto es = gi.schema().get_edge_schema(p_label, p_label, e_label);
    EXPECT_GE(es->get_property_index("weight"), 0);
    EXPECT_EQ(es->get_property_index("license"), -1);
  }

  // Fresh reader: has `license`.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    auto es = gi.schema().get_edge_schema(p_label, p_label, e_label);
    EXPECT_GE(es->get_property_index("license"), 0);
  }
}

TEST_F(NeugDBACIDTest,
       DeleteEdgePropertiesCommitDoesNotAffectHeldReaderSchema) {
  // Tests the unbundled path: `created(weight, since)` → drop `since`.
  // Held reader's EdgeDataAccessor for `since` must still resolve.
  std::string dir = work_dir_ + "/DeleteEdgePropsCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);
  cc_setup_unbundled_created(*svc);

  auto p_label = db.schema().get_vertex_label_id("person");
  auto sw_label = db.schema().get_vertex_label_id("software");
  auto e_label = db.schema().get_edge_label_id("created");

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  EXPECT_EQ(cc_read_created_since_via(txn_r), 2020);
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    auto es = gi.schema().get_edge_schema(p_label, sw_label, e_label);
    EXPECT_GE(es->get_property_index("since"), 0);
    EXPECT_GE(es->get_property_index("weight"), 0);
  }

  // Writer drops `since`.
  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    DeleteEdgePropertiesParamBuilder b;
    EXPECT_TRUE(txn_u.DeleteEdgeProperties(b.SrcLabel("person")
                                               .DstLabel("software")
                                               .EdgeLabel("created")
                                               .AddDeleteProperty("since")
                                               .Build())
                    .ok());
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: still reads `since` via captured accessor on captured Table.
  EXPECT_EQ(cc_read_created_since_via(txn_r), 2020);
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    auto es = gi.schema().get_edge_schema(p_label, sw_label, e_label);
    EXPECT_GE(es->get_property_index("since"), 0);
    EXPECT_GE(es->get_property_index("weight"), 0);
  }

  // Fresh reader: `since` is gone (accessor throws); `weight` still works.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    auto es = gi.schema().get_edge_schema(p_label, sw_label, e_label);
    EXPECT_EQ(es->get_property_index("since"), -1);
    EXPECT_GE(es->get_property_index("weight"), 0);
    EXPECT_THROW(gi.GetEdgeDataAccessor(p_label, sw_label, e_label, "since"),
                 std::exception);
  }
}

TEST_F(NeugDBACIDTest,
       RenameEdgePropertiesCommitDoesNotAffectHeldReaderSchema) {
  std::string dir = work_dir_ + "/RenameEdgePropsCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto p_label = db.schema().get_vertex_label_id("person");
  auto e_label = db.schema().get_edge_label_id("knows");

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    auto es = gi.schema().get_edge_schema(p_label, p_label, e_label);
    EXPECT_GE(es->get_property_index("weight"), 0);
    EXPECT_EQ(es->get_property_index("importance"), -1);
  }

  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    RenameEdgePropertiesParamBuilder b;
    EXPECT_TRUE(
        txn_u.RenameEdgeProperties(b.SrcLabel("person")
                                       .DstLabel("person")
                                       .EdgeLabel("knows")
                                       .AddRenameProperty("weight", "importance")
                                       .Build())
            .ok());
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: still sees `weight`, not `importance`.
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    auto es = gi.schema().get_edge_schema(p_label, p_label, e_label);
    EXPECT_GE(es->get_property_index("weight"), 0);
    EXPECT_EQ(es->get_property_index("importance"), -1);
  }

  // Fresh reader: sees `importance`, not `weight`.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    auto es = gi.schema().get_edge_schema(p_label, p_label, e_label);
    EXPECT_EQ(es->get_property_index("weight"), -1);
    EXPECT_GE(es->get_property_index("importance"), 0);
  }
}

// ============================================================================
// Category 3e — Schema-level DDL isolation (Create/Delete vertex / edge type).
// ============================================================================

TEST_F(NeugDBACIDTest, CreateVertexTypeCommitDoesNotAffectHeldReaderSchema) {
  std::string dir = work_dir_ + "/CreateVTypeCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_FALSE(gi.schema().contains_vertex_label("company"));
  }

  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    CreateVertexTypeParamBuilder b;
    EXPECT_TRUE(
        txn_u.CreateVertexType(
                 b.VertexLabel("company")
                     .AddProperty(DataTypeId::kInt64, "id",
                                  Property::from_int64(0))
                     .AddProperty(DataTypeId::kVarchar, "name",
                                  Property::from_string_view(""))
                     .AddPrimaryKeyName("id")
                     .Build())
            .ok());
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: still does not have `company`.
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_FALSE(gi.schema().contains_vertex_label("company"));
  }
  // Fresh reader: has it.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    EXPECT_TRUE(gi.schema().contains_vertex_label("company"));
  }
}

TEST_F(NeugDBACIDTest, DeleteVertexTypeCommitDoesNotAffectHeldReaderSchema) {
  std::string dir = work_dir_ + "/DeleteVTypeCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  // Cache the label IDs from the live schema BEFORE the writer's DDL —
  // after DeleteVertexType("person"), db.schema().get_vertex_label_id("person")
  // throws, so anything reading via the snapshot must use the cached label
  // (the snapshot's PG still has the label, just db.schema() doesn't).
  auto p_label = db.schema().get_vertex_label_id("person");
  auto age_col_id = db.schema()
                        .get_vertex_schema(p_label)
                        ->get_property_index("age");

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  size_t n_pre = cc_count_vertices_via(txn_r, p_label);
  EXPECT_EQ(n_pre, static_cast<size_t>(kSeedVertices));

  // Read person 5's age via the snapshot using cached label/col — never via
  // db.schema() (which becomes invalid for "person" after the writer commits).
  auto read_age_via_snapshot = [&](int64_t oid) -> int64_t {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    vid_t v;
    if (!gi.GetVertexIndex(p_label, Property::from_int64(oid), v)) return -1;
    return gi.GetVertexProperty(p_label, v, age_col_id).as_int64();
  };
  EXPECT_EQ(read_age_via_snapshot(5), 25);

  // Writer drops the knows edge type and then the person vertex type.
  // VertexTable::Drop only resets the COW PG's local shared_ptrs; the original
  // PG still owns the indexer/v_ts/columns, so the held reader's view stays
  // intact.
  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    EXPECT_TRUE(txn_u.DeleteEdgeType("person", "person", "knows").ok());
    EXPECT_TRUE(txn_u.DeleteVertexType("person").ok());
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: still able to iterate persons and read their properties.
  EXPECT_EQ(cc_count_vertices_via(txn_r, p_label), n_pre);
  EXPECT_EQ(read_age_via_snapshot(5), 25);
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_TRUE(gi.schema().contains_vertex_label("person"));
    EXPECT_TRUE(gi.schema().contains_edge_label("knows"));
  }

  // Fresh reader: cannot resolve `person`.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    EXPECT_FALSE(gi.schema().contains_vertex_label("person"));
    EXPECT_FALSE(gi.schema().contains_edge_label("knows"));
  }
}

TEST_F(NeugDBACIDTest, CreateEdgeTypeCommitDoesNotAffectHeldReaderSchema) {
  std::string dir = work_dir_ + "/CreateETypeCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  // Need a second vertex type for the new edge to point at.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    CreateVertexTypeParamBuilder b;
    EXPECT_TRUE(
        txn.CreateVertexType(b.VertexLabel("company")
                                 .AddProperty(DataTypeId::kInt64, "id",
                                              Property::from_int64(0))
                                 .AddPrimaryKeyName("id")
                                 .Build())
            .ok());
    EXPECT_TRUE(txn.Commit());
  }

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_FALSE(gi.schema().contains_edge_label("employed_by"));
  }

  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    CreateEdgeTypeParamBuilder b;
    EXPECT_TRUE(txn_u.CreateEdgeType(b.SrcLabel("person")
                                         .DstLabel("company")
                                         .EdgeLabel("employed_by")
                                         .Build())
                    .ok());
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: still does not have `employed_by`.
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_FALSE(gi.schema().contains_edge_label("employed_by"));
  }
  // Fresh reader: has it.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    EXPECT_TRUE(gi.schema().contains_edge_label("employed_by"));
  }
}

TEST_F(NeugDBACIDTest, DeleteEdgeTypeCommitDoesNotAffectHeldReaderSchema) {
  std::string dir = work_dir_ + "/DeleteETypeCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto p_label = db.schema().get_vertex_label_id("person");
  auto e_label = db.schema().get_edge_label_id("knows");

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  size_t e_pre = cc_count_all_oe_via(txn_r, p_label, p_label, e_label);
  EXPECT_GT(e_pre, 0u);

  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    EXPECT_TRUE(txn_u.DeleteEdgeType("person", "person", "knows").ok());
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: still iterates knows edges via captured CSR.
  EXPECT_EQ(cc_count_all_oe_via(txn_r, p_label, p_label, e_label), e_pre);
  {
    StorageReadInterface gi(txn_r.view(), txn_r.timestamp());
    EXPECT_TRUE(gi.schema().contains_edge_label("knows"));
  }

  // Fresh reader: knows edge type is gone; the view accessor must throw.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    EXPECT_FALSE(gi.schema().contains_edge_label("knows"));
    EXPECT_THROW(
        gi.GetGenericOutgoingGraphView(p_label, p_label, e_label),
        std::exception);
  }
}

// ============================================================================
// Category 3f — Corner cases: multi-commit serial chain + varchar column COW.
// ============================================================================

TEST_F(NeugDBACIDTest, MultipleSequentialCommitsEachSnapshotIsolated) {
  // Pin a snapshot, commit, pin another, commit, … — each Ri must observe
  // exactly the value that was visible at its acquire time, never a later
  // commit's value. Holds 4 readers concurrently (well below the 128-slot
  // SnapshotStore default). 4 readers are unrolled by hand because
  // ReadTransaction holds reference members and is therefore neither copyable
  // nor movable, so it can't live in a standard container after value-init.
  std::string dir = work_dir_ + "/MultiCommitSnapshots";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto s0 = svc->AcquireSession();
  auto r0 = s0->GetReadTransaction();
  int64_t e0 = cc_read_age_via(r0, db, 5);
  ASSERT_TRUE(cc_update_age(*svc, 5, 5001));

  auto s1 = svc->AcquireSession();
  auto r1 = s1->GetReadTransaction();
  int64_t e1 = cc_read_age_via(r1, db, 5);
  ASSERT_TRUE(cc_update_age(*svc, 5, 5002));

  auto s2 = svc->AcquireSession();
  auto r2 = s2->GetReadTransaction();
  int64_t e2 = cc_read_age_via(r2, db, 5);
  ASSERT_TRUE(cc_update_age(*svc, 5, 5003));

  auto s3 = svc->AcquireSession();
  auto r3 = s3->GetReadTransaction();
  int64_t e3 = cc_read_age_via(r3, db, 5);
  ASSERT_TRUE(cc_update_age(*svc, 5, 5004));

  // All 4 readers held; each still sees its own captured pre-commit value.
  EXPECT_EQ(cc_read_age_via(r0, db, 5), e0);
  EXPECT_EQ(cc_read_age_via(r1, db, 5), e1);
  EXPECT_EQ(cc_read_age_via(r2, db, 5), e2);
  EXPECT_EQ(cc_read_age_via(r3, db, 5), e3);

  // Seed age is 20 + id (cc_init); each subsequent reader sees the previous
  // commit.
  EXPECT_EQ(e0, 25);
  EXPECT_EQ(e1, 5001);
  EXPECT_EQ(e2, 5002);
  EXPECT_EQ(e3, 5003);

  // Fresh reader sees the final commit.
  EXPECT_EQ(cc_read_age(*svc, 5), 5004);
}

TEST_F(NeugDBACIDTest, UpdateStringPropertyCommitDoesNotAffectHeldReader) {
  // Exercises the varchar column COW path inside Table::ensure_column_mutable.
  std::string dir = work_dir_ + "/UpdateStringCOW";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto p_label = db.schema().get_vertex_label_id("person");

  auto read_name_via = [&](const ReadTransaction& txn, int64_t oid) {
    StorageReadInterface gi(txn.view(), txn.timestamp());
    vid_t v;
    if (!gi.GetVertexIndex(p_label, Property::from_int64(oid), v)) return std::string();
    return std::string(gi.GetVertexProperty(p_label, v, 0).as_string_view());
  };

  auto sess_r = svc->AcquireSession();
  auto txn_r = sess_r->GetReadTransaction();
  std::string name_pre = read_name_via(txn_r, 5);
  EXPECT_EQ(name_pre, "person_5");

  // Writer updates person 5's name to a new string.
  {
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    vid_t v;
    ASSERT_TRUE(txn_u.GetVertexIndex(p_label, Property::from_int64(5), v));
    EXPECT_TRUE(txn_u.UpdateVertexProperty(
        p_label, v, 0, Property::from_string_view("renamed_5")));
    EXPECT_TRUE(txn_u.Commit());
  }

  // Held reader: still sees old name.
  EXPECT_EQ(read_name_via(txn_r, 5), "person_5");

  // Fresh reader: sees new name.
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    EXPECT_EQ(read_name_via(txn, 5), "renamed_5");
  }
}

// Design validation: GetUpdateTransaction acquires version_manager_'s
// rw_mutex_ exclusively (see TPVersionManager::acquire_update_timestamp),
// so only one UpdateTxn can be open at a time. A second concurrent
// GetUpdateTransaction must block until the first commits or aborts.
//
// This is the granularity contract for write isolation: COW happens on a
// single forked PG, never on two forked PGs in parallel — which means we
// don't need to test cross-COW write conflicts.
TEST_F(NeugDBACIDTest, UpdateTransactionsAreExclusivelySerialized) {
  std::string dir = work_dir_ + "/UpdateExclusive";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto sess1 = svc->AcquireSession();
  auto sess2 = svc->AcquireSession();

  std::atomic<bool> u1_acquired{false};
  std::atomic<bool> u2_acquired{false};
  std::atomic<bool> u1_committed{false};

  std::thread t1([&] {
    auto txn = sess1->GetUpdateTransaction();
    u1_acquired.store(true);
    // Hold the update slot for 100ms so T2 has time to attempt acquisition.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(u2_acquired.load())
        << "Second UpdateTxn must NOT acquire while first is still open";
    StorageTPUpdateInterface gui(txn);
    auto person_label = db.schema().get_vertex_label_id("person");
    vid_t vid;
    ASSERT_TRUE(gui.GetVertexIndex(person_label, Property::from_int64(1), vid));
    gui.UpdateVertexProperty(person_label, vid, 1,
                             Property::From<int64_t>(100));
    EXPECT_TRUE(txn.Commit());
    u1_committed.store(true);
  });

  std::thread t2([&] {
    // Wait for T1 to acquire so we don't race for the lock first.
    while (!u1_acquired.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto txn = sess2->GetUpdateTransaction();
    u2_acquired.store(true);
    EXPECT_TRUE(u1_committed.load())
        << "Second UpdateTxn must only acquire after first commits";
    StorageTPUpdateInterface gui(txn);
    auto person_label = db.schema().get_vertex_label_id("person");
    vid_t vid;
    ASSERT_TRUE(gui.GetVertexIndex(person_label, Property::from_int64(2), vid));
    gui.UpdateVertexProperty(person_label, vid, 1,
                             Property::From<int64_t>(200));
    EXPECT_TRUE(txn.Commit());
  });

  t1.join();
  t2.join();
  EXPECT_TRUE(u1_acquired.load());
  EXPECT_TRUE(u2_acquired.load());
  EXPECT_EQ(cc_read_age(*svc, 1), 100);
  EXPECT_EQ(cc_read_age(*svc, 2), 200);
}

// Validates the design: rw_mutex_ exclusive (held by Update) blocks
// rw_mutex_ shared (acquired by Insert). See acquire_insert_timestamp /
// acquire_update_timestamp in version_manager.cc.
TEST_F(NeugDBACIDTest, InsertBlockedByOpenUpdateTransaction) {
  std::string dir = work_dir_ + "/InsertBlockedByUpdate";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto sess1 = svc->AcquireSession();
  auto sess2 = svc->AcquireSession();

  std::atomic<bool> update_acquired{false};
  std::atomic<bool> insert_acquired{false};
  std::atomic<bool> update_committed{false};

  // Thread A opens UpdateTxn, sleeps 100ms, commits.
  std::thread t_update([&] {
    auto txn = sess1->GetUpdateTransaction();
    update_acquired.store(true);
    // Hold the update slot for 100ms so the insert thread has time to attempt.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(insert_acquired.load())
        << "InsertTxn must NOT acquire while UpdateTxn is still open";
    StorageTPUpdateInterface gui(txn);
    auto person_label = db.schema().get_vertex_label_id("person");
    vid_t vid;
    ASSERT_TRUE(gui.GetVertexIndex(person_label, Property::from_int64(1), vid));
    gui.UpdateVertexProperty(person_label, vid, 1,
                             Property::From<int64_t>(777));
    EXPECT_TRUE(txn.Commit());
    update_committed.store(true);
  });

  // Thread B waits for A to acquire, then attempts InsertTxn.
  std::thread t_insert([&] {
    // Wait for the update to acquire so we don't race for the lock first.
    while (!update_acquired.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto txn = sess2->GetInsertTransaction();
    insert_acquired.store(true);
    EXPECT_TRUE(update_committed.load())
        << "InsertTxn must only acquire after UpdateTxn commits";
    auto person_label = db.schema().get_vertex_label_id("person");
    vid_t vid;
    ASSERT_TRUE(txn.AddVertex(
        person_label, Property::from_int64(999999),
        {Property::from_string_view("blocked"), Property::from_int64(42)},
        vid));
    EXPECT_TRUE(txn.Commit());
  });

  t_update.join();
  t_insert.join();
  EXPECT_TRUE(update_acquired.load());
  EXPECT_TRUE(insert_acquired.load());
  // Both writes are visible.
  EXPECT_EQ(cc_read_age(*svc, 1), 777);
  EXPECT_EQ(cc_read_age(*svc, 999999), 42);
}

// ============================================================================
// Category 4 — Commit-window semantics
// ============================================================================

TEST_F(NeugDBACIDTest, ReadsBeforeCommitSeeOldSnapshot) {
  std::string dir = work_dir_ + "/ReadsBeforeCommit";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  auto sess_u = svc->AcquireSession();
  auto txn_u = sess_u->GetUpdateTransaction();
  StorageTPUpdateInterface gui(txn_u);
  auto person_label = db.schema().get_vertex_label_id("person");
  vid_t vid_u;
  ASSERT_TRUE(gui.GetVertexIndex(person_label, Property::from_int64(5), vid_u));
  gui.UpdateVertexProperty(person_label, vid_u, 1,
                           Property::From<int64_t>(9999));

  std::vector<std::thread> readers;
  std::atomic<int64_t> bad{0};
  for (int i = 0; i < 8; ++i) {
    readers.emplace_back([&] {
      for (int j = 0; j < 100; ++j) {
        if (cc_read_age(*svc, 5) != 25) bad.fetch_add(1);
      }
    });
  }
  for (auto& t : readers) t.join();
  EXPECT_EQ(bad.load(), 0);

  txn_u.Abort();
}

TEST_F(NeugDBACIDTest, ReadsAfterCommitSeeNewSnapshot) {
  std::string dir = work_dir_ + "/ReadsAfterCommit";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  EXPECT_TRUE(cc_update_age(*svc, 5, 1234));

  std::vector<std::thread> readers;
  std::atomic<int64_t> bad{0};
  for (int i = 0; i < 8; ++i) {
    readers.emplace_back([&] {
      for (int j = 0; j < 100; ++j) {
        if (cc_read_age(*svc, 5) != 1234) bad.fetch_add(1);
      }
    });
  }
  for (auto& t : readers) t.join();
  EXPECT_EQ(bad.load(), 0);
}

// Validates that under concurrent commit + read, every reader observation is
// exactly one of two consistent values (pre or post). The commit window
// itself is brief (commit_lock_ exclusive), but visibility is determined by
// the per-row timestamp filter: a reader's observation depends on whether
// its read_ts (allocated at GetReadTransaction time) is < or >= the writer's
// write_ts (allocated at GetUpdateTransaction time, published at
// release_update_timestamp time).
//
// Empirically the reader nearly always wins, because the writer's path from
// barrier release to release_update_timestamp includes WAL append +
// installSnapshot, while the reader's path is just acquire_read_timestamp
// (an atomic load). The pre-domination is a property of the path lengths,
// not a bug. Both outcomes are CORRECT; we only assert no garbage.
TEST_F(NeugDBACIDTest, ConcurrentReadsAndCommitsObserveConsistentValues) {
  std::string dir = work_dir_ + "/CommitWindowRace";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  // The invariant (reader sees pre OR post, never anything else) is what
  // matters; more iterations expose more interleavings. 1000 is enough to
  // cover both outcomes in a typical run; raise to 10000 with --gtest_repeat
  // for stress runs.
  constexpr int kIters = 1000;
  int64_t pre_count = 0;
  int64_t post_count = 0;

  for (int iter = 0; iter < kIters; ++iter) {
    int64_t pre_value = 25 + iter;
    int64_t post_value = pre_value + 1;

    // Set the baseline (visible pre-race value).
    ASSERT_TRUE(cc_update_age(*svc, 5, pre_value));

    // Open U with the post-race value staged in cow_storage.
    auto sess_u = svc->AcquireSession();
    auto txn_u = sess_u->GetUpdateTransaction();
    StorageTPUpdateInterface gui(txn_u);
    auto person_label = db.schema().get_vertex_label_id("person");
    vid_t vid_u;
    ASSERT_TRUE(
        gui.GetVertexIndex(person_label, Property::from_int64(5), vid_u));
    gui.UpdateVertexProperty(person_label, vid_u, 1,
                             Property::From(post_value));

    // Tight 2-thread barrier: each thread bumps ready_count then spins until
    // both have arrived. Avoids std::barrier (libc++ requires macOS 11.0
    // deployment target which this build doesn't set).
    std::atomic<int> ready{0};
    std::atomic<int64_t> reader_value{-1};
    std::thread reader([&] {
      ready.fetch_add(1);
      while (ready.load() < 2) {
      }
      reader_value.store(cc_read_age(*svc, 5));
    });
    std::thread writer([&] {
      ready.fetch_add(1);
      while (ready.load() < 2) {
      }
      txn_u.Commit();
    });
    reader.join();
    writer.join();

    int64_t v = reader_value.load();
    if (v == pre_value) {
      ++pre_count;
    } else if (v == post_value) {
      ++post_count;
    } else {
      FAIL() << "Reader observed unexpected value " << v << " (expected "
             << pre_value << " or " << post_value << ") at iter " << iter;
    }
  }

  // Correctness invariant: every observation is one of the two valid values.
  // We do NOT require both outcomes to occur — in practice the reader nearly
  // always wins the commit_lock_ race because the writer's Commit path does
  // WAL append + slot reservation BEFORE contending for the exclusive lock,
  // while the reader's path goes straight to commit_lock_ shared. The fact
  // that pre dominates is a system property (writer has more pre-lock work),
  // not a violation. The post-commit visibility is independently covered by
  // ReadsAfterCommitSeeNewSnapshot.
  EXPECT_EQ(pre_count + post_count, kIters);
  std::cerr << "[ConcurrentReadsAndCommits] pre=" << pre_count
            << " post=" << post_count << " total=" << kIters << "\n";
}

// Two-phase latency measurement:
//   Phase A: 8 readers for 1s, no commits. Record per-call latencies.
//   Phase B: 8 readers + 1 commit thread for 1s. Record per-call latencies.
//
// Each reader holds its own session for the duration (avoids session-pool
// contention noise).
//
// Assertions:
//   - Phase B total reads >= 30% of Phase A total reads
//     (proves no starvation; some throughput loss expected).
//   - Phase B p99 latency < 100ms absolute ceiling — bounded.
//
// Logs p50, p95, p99 for both phases.
TEST_F(NeugDBACIDTest, CommitsBrieflyBlockReadsButDoNotStarveThem) {
  std::string dir = work_dir_ + "/CommitLatency";
  NeugDB db;
  auto svc = cc_init(db, dir, thread_num_);

  constexpr int kReaders = 8;
  constexpr auto kPhaseDuration = std::chrono::seconds(1);

  // Helper: compute percentile from sorted vector.
  auto percentile = [](const std::vector<int64_t>& sorted, double p) -> int64_t {
    if (sorted.empty()) return 0;
    size_t idx = static_cast<size_t>(p * static_cast<double>(sorted.size() - 1));
    return sorted[idx];
  };

  // ---- Phase A: readers only ----
  std::vector<std::vector<int64_t>> phase_a_latencies(kReaders);
  {
    std::atomic<bool> stop{false};
    std::vector<std::thread> readers;
    for (int i = 0; i < kReaders; ++i) {
      readers.emplace_back([&, i] {
        auto sess = svc->AcquireSession();
        while (!stop.load(std::memory_order_relaxed)) {
          auto [age, ns] = cc_read_age_timed(*sess.get(), db, 5);
          (void)age;
          phase_a_latencies[i].push_back(ns);
        }
      });
    }
    std::this_thread::sleep_for(kPhaseDuration);
    stop.store(true);
    for (auto& t : readers) t.join();
  }

  // ---- Phase B: readers + 1 commit thread ----
  std::vector<std::vector<int64_t>> phase_b_latencies(kReaders);
  {
    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    for (int i = 0; i < kReaders; ++i) {
      threads.emplace_back([&, i] {
        auto sess = svc->AcquireSession();
        while (!stop.load(std::memory_order_relaxed)) {
          auto [age, ns] = cc_read_age_timed(*sess.get(), db, 5);
          (void)age;
          phase_b_latencies[i].push_back(ns);
        }
      });
    }
    threads.emplace_back([&] {
      int64_t v = 1000;
      while (!stop.load(std::memory_order_relaxed)) {
        cc_update_age(*svc, 5, v++);
      }
    });
    std::this_thread::sleep_for(kPhaseDuration);
    stop.store(true);
    for (auto& t : threads) t.join();
  }

  // Merge and sort latencies.
  std::vector<int64_t> a_all, b_all;
  for (auto& v : phase_a_latencies) a_all.insert(a_all.end(), v.begin(), v.end());
  for (auto& v : phase_b_latencies) b_all.insert(b_all.end(), v.begin(), v.end());
  std::sort(a_all.begin(), a_all.end());
  std::sort(b_all.begin(), b_all.end());

  int64_t a_p50 = percentile(a_all, 0.50);
  int64_t a_p95 = percentile(a_all, 0.95);
  int64_t a_p99 = percentile(a_all, 0.99);
  int64_t b_p50 = percentile(b_all, 0.50);
  int64_t b_p95 = percentile(b_all, 0.95);
  int64_t b_p99 = percentile(b_all, 0.99);

  std::cerr << "[CommitLatency] Phase A: n=" << a_all.size()
            << " p50=" << a_p50 / 1000 << "us"
            << " p95=" << a_p95 / 1000 << "us"
            << " p99=" << a_p99 / 1000 << "us\n";
  std::cerr << "[CommitLatency] Phase B: n=" << b_all.size()
            << " p50=" << b_p50 / 1000 << "us"
            << " p95=" << b_p95 / 1000 << "us"
            << " p99=" << b_p99 / 1000 << "us\n";

  // Phase B total reads >= 30% of Phase A (no starvation).
  EXPECT_GE(static_cast<int64_t>(b_all.size()),
            static_cast<int64_t>(a_all.size()) * 3 / 10)
      << "Phase B reads starved to < 30% of Phase A throughput";

  // Phase B p99 < 100ms absolute ceiling.
  EXPECT_LT(b_p99, 100'000'000)
      << "Phase B p99 latency exceeds 100ms ceiling";
}