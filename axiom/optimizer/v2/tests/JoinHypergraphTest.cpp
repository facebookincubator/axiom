/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "axiom/optimizer/v2/JoinHypergraph.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/tests/UnitTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

class JoinHypergraphTest : public UnitTestBase {
 protected:
  NodeCP makeLeaf() {
    optimizer::ColumnVector columns{makeColumn("a", velox::BIGINT())};
    return builder_->makeEmptyValues(std::move(columns));
  }

  ExprCP makePredicate() {
    return builder_->makeBoolean(true);
  }
};

// Builds a 3-relation hypergraph and verifies that checkConsistency
// rejects it until enough edges connect every relation.
TEST_F(JoinHypergraphTest, connectivity) {
  JoinHypergraph graph;
  const int8_t aId = graph.addRelation(makeLeaf(), 100, {});
  const int8_t bId = graph.addRelation(makeLeaf(), 200, {});
  const int8_t cId = graph.addRelation(makeLeaf(), 300, {});

  VELOX_ASSERT_THROW(graph.checkConsistency(), "not connected");

  RelationSet aSet;
  aSet.add(aId);
  RelationSet bSet;
  bSet.add(bId);
  RelationSet abSet{aSet};
  abSet.unionSet(bSet);
  graph.addEdge(
      JoinEdge{
          aSet,
          bSet,
          ExprVector{makePredicate()},
          ExprVector{makePredicate()},
          ExprVector{},
          velox::core::JoinType::kInner,
          /*nullAware=*/false,
          /*nullAsValue=*/false},
      abSet);
  VELOX_ASSERT_THROW(graph.checkConsistency(), "not connected");

  RelationSet cSet;
  cSet.add(cId);
  RelationSet bcSet{bSet};
  bcSet.unionSet(cSet);
  graph.addEdge(
      JoinEdge{
          bSet,
          cSet,
          ExprVector{makePredicate()},
          ExprVector{makePredicate()},
          ExprVector{},
          velox::core::JoinType::kInner,
          /*nullAware=*/false,
          /*nullAsValue=*/false},
      bcSet);
  ASSERT_NO_THROW(graph.checkConsistency());
}

// Relation ids are bounded by `RelationSet::kMaxRelations`. The
// 65th addRelation must fail.
TEST_F(JoinHypergraphTest, maxRelations) {
  JoinHypergraph graph;
  for (int i = 0; i < RelationSet::kMaxRelations; ++i) {
    graph.addRelation(makeLeaf(), 100, {});
  }
  VELOX_ASSERT_THROW(
      graph.addRelation(makeLeaf(), 100, {}),
      "exceeds the per-hypergraph relation cap");
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
