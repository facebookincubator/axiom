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

#include <array>

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/EstimateProvider.h"
#include "axiom/optimizer/v2/HypergraphBuilder.h"
#include "axiom/optimizer/v2/tests/UnitTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

class JoinHypergraphTest : public UnitTestBase {
 protected:
  NodeCP makeLeaf() {
    return makeLeaf("a");
  }

  ExprCP makePredicate() {
    return builder_->makeBoolean(true);
  }

  NodeCP makeLeaf(std::string_view name) {
    optimizer::ColumnVector columns{makeColumn(name, velox::BIGINT())};
    return builder_->makeEmptyValues(std::move(columns));
  }

  JoinCP
  makeEquiJoin(NodeCP left, NodeCP right, velox::core::JoinType joinType) {
    using velox::core::JoinType;
    ColumnVector outputColumns;
    switch (joinType) {
      case JoinType::kRightSemiFilter:
        outputColumns = right->outputColumns();
        break;
      case JoinType::kRightSemiProject:
        outputColumns = right->outputColumns();
        outputColumns.push_back(makeColumn("mark", velox::BOOLEAN()));
        break;
      default:
        outputColumns = left->outputColumns();
        outputColumns.insert(
            outputColumns.end(),
            right->outputColumns().begin(),
            right->outputColumns().end());
    }
    return builder_->make<Join>(Join::Key{
        .left = left,
        .right = right,
        .joinType = joinType,
        .leftKeys = {left->outputColumns().front()},
        .rightKeys = {right->outputColumns().front()},
        .outputColumns = std::move(outputColumns),
    });
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
  graph.addEdge(
      JoinEdge{
          aSet,
          bSet,
          aSet,
          bSet,
          ExprVector{makePredicate()},
          ExprVector{makePredicate()},
          ExprVector{},
          velox::core::JoinType::kInner,
          /*nullAware=*/false,
          /*nullAsValue=*/false});
  VELOX_ASSERT_THROW(graph.checkConsistency(), "not connected");

  RelationSet cSet;
  cSet.add(cId);
  graph.addEdge(
      JoinEdge{
          bSet,
          cSet,
          bSet,
          cSet,
          ExprVector{makePredicate()},
          ExprVector{makePredicate()},
          ExprVector{},
          velox::core::JoinType::kInner,
          /*nullAware=*/false,
          /*nullAsValue=*/false});
  ASSERT_NO_THROW(graph.checkConsistency());
}

// A non-inner join remains pinned to its complete normalized operands even
// when its keys reference only one relation on each side.
TEST_F(JoinHypergraphTest, nonInnerEligibilityUsesNormalizedInputSides) {
  using velox::core::JoinType;
  struct TestCase {
    JoinType inputType;
    JoinType normalizedType;
    bool rightForm;
  };
  const std::array<TestCase, 4> testCases{{
      {JoinType::kLeft, JoinType::kLeft, false},
      {JoinType::kRight, JoinType::kLeft, true},
      {JoinType::kRightSemiFilter, JoinType::kLeftSemiFilter, true},
      {JoinType::kRightSemiProject, JoinType::kLeftSemiProject, true},
  }};

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(velox::core::JoinTypeName::toName(testCase.inputType));
    NodeCP a = makeLeaf("a");
    NodeCP b = makeLeaf("b");
    NodeCP x = makeLeaf("x");
    JoinCP bx = makeEquiJoin(b, x, JoinType::kInner);
    JoinCP root = testCase.rightForm ? makeEquiJoin(bx, a, testCase.inputType)
                                     : makeEquiJoin(a, bx, testCase.inputType);

    JoinCluster cluster{
        .root = root,
        .leaves = {a, b, x},
        .joins = {root, bx},
    };
    EstimateProvider estimateProvider;
    const JoinHypergraph graph =
        HypergraphBuilder::build(cluster, cluster.leaves, estimateProvider);

    const JoinEdge* rootEdge = nullptr;
    for (const auto& edge : graph.edges()) {
      if (edge.joinType() == testCase.normalizedType) {
        rootEdge = &edge;
        break;
      }
    }
    ASSERT_NE(rootEdge, nullptr);

    const RelationSet aSet = RelationSet::singleton(0);
    const RelationSet bSet = RelationSet::singleton(1);
    RelationSet bxSet{bSet};
    bxSet.add(2);
    EXPECT_EQ(rootEdge->leftEndpoints(), aSet);
    EXPECT_EQ(rootEdge->rightEndpoints(), bSet);
    EXPECT_EQ(rootEdge->leftEligibility(), aSet);
    EXPECT_EQ(rootEdge->rightEligibility(), bxSet);
  }
}

// A relation required only for eligibility still belongs to the edge's
// connected component.
TEST_F(JoinHypergraphTest, connectivityIncludesEligibilityOnlyRelations) {
  JoinHypergraph graph;
  const RelationSet aSet =
      RelationSet::singleton(graph.addRelation(makeLeaf(), 100, {}));
  const RelationSet bSet =
      RelationSet::singleton(graph.addRelation(makeLeaf(), 200, {}));
  const RelationSet xSet =
      RelationSet::singleton(graph.addRelation(makeLeaf(), 300, {}));
  RelationSet rightEligibility{bSet};
  rightEligibility.unionSet(xSet);
  graph.addEdge(
      JoinEdge{
          aSet,
          bSet,
          aSet,
          rightEligibility,
          ExprVector{makePredicate()},
          ExprVector{makePredicate()},
          ExprVector{},
          velox::core::JoinType::kInner,
          /*nullAware=*/false,
          /*nullAsValue=*/false});

  RelationSet allRelations{aSet};
  allRelations.unionSet(rightEligibility);
  EXPECT_EQ(graph.connectedComponent(aSet, allRelations), allRelations);
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
