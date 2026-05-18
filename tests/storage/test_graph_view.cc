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
#include <filesystem>

#include "neug/storages/allocators.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/workspace.h"

namespace neug {

class GraphViewTest : public ::testing::Test {
 protected:
  std::string work_dir_;
  std::unique_ptr<PropertyGraph> graph_;
  Workspace ws_;
  // Owns the buffers backing the SetUp edges' adjacency lists. Must outlive
  // graph_; tests that need to mutate the graph reuse this allocator so the
  // SetUp buffers stay live alongside any new ones they add.
  std::unique_ptr<Allocator> alloc_;

  void SetUp() override {
    work_dir_ = std::string("/tmp/test_graph_view") +
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
    std::filesystem::create_directories(work_dir_);
    graph_ = std::make_unique<PropertyGraph>();
    ws_.Open(work_dir_);
    auto ckp_id = ws_.CreateCheckpoint();
    auto& ckp = ws_.GetCheckpoint(ckp_id);
    graph_->Open(ckp, MemoryLevel::kInMemory);

    // Create vertex type: person with id as primary key and name as property
    CreateVertexTypeParamBuilder person_builder;
    ASSERT_TRUE(
        graph_
            ->CreateVertexType(person_builder.VertexLabel("person")
                                   .AddProperty(DataTypeId::kInt64, "id",
                                                Property::from_int64(0))
                                   .AddProperty(DataTypeId::kVarchar, "name",
                                                Property::from_string_view(""))
                                   .AddPrimaryKeyName("id")
                                   .Build())
            .ok());

    // Create edge type: knows
    CreateEdgeTypeParamBuilder knows_builder;
    ASSERT_TRUE(
        graph_
            ->CreateEdgeType(knows_builder.SrcLabel("person")
                                 .DstLabel("person")
                                 .EdgeLabel("knows")
                                 .AddProperty(DataTypeId::kDouble, "weight",
                                              Property::from_double(0.0))
                                 .Build())
            .ok());

    // Add vertices
    label_t person_label = graph_->schema().get_vertex_label_id("person");
    vid_t vid1, vid2, vid3;
    ASSERT_TRUE(graph_->AddVertex(person_label, Property::from_int64(1),
                                   {Property::from_string_view("Alice")}, vid1, 0, false).ok());
    ASSERT_TRUE(graph_->AddVertex(person_label, Property::from_int64(2),
                                   {Property::from_string_view("Bob")}, vid2, 0, false).ok());
    ASSERT_TRUE(graph_->AddVertex(person_label, Property::from_int64(3),
                                   {Property::from_string_view("Charlie")}, vid3, 0, false).ok());

    // Add edges
    alloc_ = std::make_unique<Allocator>(MemoryLevel::kInMemory, work_dir_);
    graph_->AddEdge(person_label, vid1, person_label, vid2,
                    graph_->schema().get_edge_label_id("knows"),
                    {Property::from_double(0.5)}, 0, *alloc_, false);
    graph_->AddEdge(person_label, vid2, person_label, vid3,
                    graph_->schema().get_edge_label_id("knows"),
                    {Property::from_double(0.7)}, 0, *alloc_, false);
  }

  void TearDown() override {
    graph_.reset();
    alloc_.reset();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
  }
};

TEST_F(GraphViewTest, Construction) {
  GraphView view(*graph_);

  EXPECT_EQ(view.vertex_label_num(), 1);
}

TEST_F(GraphViewTest, GetVertexView) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");

  const auto& vertex_view = view.get_vertex_view(person_label);
  EXPECT_EQ(vertex_view.LidNum(), 3);
}

TEST_F(GraphViewTest, VertexViewGetIndex) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");

  const auto& vertex_view = view.get_vertex_view(person_label);

  vid_t lid;
  EXPECT_TRUE(vertex_view.get_index(Property::from_int64(1), lid, 0));
  EXPECT_TRUE(vertex_view.get_index(Property::from_int64(2), lid, 0));
  EXPECT_TRUE(vertex_view.get_index(Property::from_int64(3), lid, 0));
  EXPECT_FALSE(vertex_view.get_index(Property::from_int64(999), lid, 0));
}

TEST_F(GraphViewTest, VertexViewGetOid) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");

  const auto& vertex_view = view.get_vertex_view(person_label);

  auto oid0 = vertex_view.GetOid(0);
  EXPECT_EQ(oid0.type(), DataTypeId::kInt64);
  EXPECT_EQ(oid0.as_int64(), 1);

  auto oid1 = vertex_view.GetOid(1);
  EXPECT_EQ(oid1.as_int64(), 2);

  auto oid2 = vertex_view.GetOid(2);
  EXPECT_EQ(oid2.as_int64(), 3);
}

TEST_F(GraphViewTest, GetEdgeView) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");
  label_t knows_label = view.schema().get_edge_label_id("knows");

  const auto& edge_view = view.get_edge_view(person_label, person_label, knows_label);
  EXPECT_EQ(edge_view.EdgeNum(), 2);
}

TEST_F(GraphViewTest, SchemaAccess) {
  GraphView view(*graph_);

  // GraphView provides schema access without exposing PropertyGraph reference
  EXPECT_EQ(view.schema().vertex_label_num(), 1);
  EXPECT_EQ(view.schema().edge_label_num(), 1);
}

TEST_F(GraphViewTest, IsValidLid) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");

  const auto& vertex_view = view.get_vertex_view(person_label);

  EXPECT_TRUE(vertex_view.IsValidLid(0, 0));
  EXPECT_TRUE(vertex_view.IsValidLid(1, 0));
  EXPECT_TRUE(vertex_view.IsValidLid(2, 0));
}

TEST_F(GraphViewTest, MutableViewConstruction) {
  GraphView view(*graph_);

  // GraphView is always potentially writable; no can_insert() check needed.
  EXPECT_GT(view.vertex_label_num(), 0u);
}

TEST_F(GraphViewTest, AddVertexThroughView) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");

  vid_t new_vid;
  EXPECT_TRUE(view.AddVertex(person_label, Property::from_int64(4),
                              {Property::from_string_view("David")}, new_vid, 0));

  // Verify vertex was added
  const auto& vertex_view = view.get_vertex_view(person_label);
  EXPECT_EQ(vertex_view.LidNum(), 4);

  vid_t lid;
  EXPECT_TRUE(vertex_view.get_index(Property::from_int64(4), lid, 0));
}

TEST_F(GraphViewTest, AddEdgeThroughView) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");
  label_t knows_label = view.schema().get_edge_label_id("knows");

  int32_t offset = view.AddEdge(person_label, 0, person_label, 2, knows_label,
                                 {Property::from_double(0.9)}, 0, *alloc_);
  EXPECT_GE(offset, 0);
}

}  // namespace neug