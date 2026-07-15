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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/tests/UnitTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

class NodeTransformTest : public UnitTestBase {
 protected:
  // `makeEmptyValues` hash-conses on columns, so distinct leaves need
  // disjoint columns.
  NodeCP leaf(std::string_view name) {
    optimizer::ColumnVector cols{makeColumn(name, velox::BIGINT())};
    return builder_->makeEmptyValues(std::move(cols));
  }

  ExprCP truePredicate() {
    return builder_->makeBoolean(true);
  }
};

TEST_F(NodeTransformTest, valuesLeafRejectsNonEmptyInputs) {
  NodeCP values = leaf("a");
  EXPECT_EQ(values->withInputs({}, *builder_), values);
  VELOX_ASSERT_THROW(
      values->withInputs({values}, *builder_),
      "Leaf node cannot have inputs: Values");
}

TEST_F(NodeTransformTest, withInputsRoundtripReturnsSamePointer) {
  NodeCP values = leaf("a");
  auto* filter = builder_->make<Filter>({values, ExprVector{truePredicate()}});

  NodeCP roundtrip = filter->withInputs({values}, *builder_);
  EXPECT_EQ(roundtrip, filter);
  EXPECT_EQ(roundtrip->as<Filter>()->predicates(), filter->predicates());
}

TEST_F(NodeTransformTest, withInputsRebuildsFilterOnNewInput) {
  NodeCP leafA = leaf("a");
  NodeCP leafB = leaf("b");
  auto* filter = builder_->make<Filter>({leafA, ExprVector{truePredicate()}});

  NodeCP rebuilt = filter->withInputs({leafB}, *builder_);
  EXPECT_NE(rebuilt, filter);
  EXPECT_EQ(rebuilt->as<Filter>()->input(), leafB);
  EXPECT_EQ(rebuilt->as<Filter>()->predicates(), filter->predicates());
}

TEST_F(NodeTransformTest, withInputsRebuildsJoinOnEitherSide) {
  NodeCP leftA = leaf("l_a");
  NodeCP rightA = leaf("r_a");
  NodeCP leftB = leaf("l_b");
  NodeCP rightB = leaf("r_b");

  auto* join = builder_->make<Join>(
      {leftA,
       rightA,
       velox::core::JoinType::kInner,
       ExprVector{},
       ExprVector{},
       ExprVector{},
       /*nullAware=*/false,
       /*nullAsValue=*/false,
       optimizer::ColumnVector{
           leftA->outputColumns()[0], rightA->outputColumns()[0]}});

  NodeCP unchanged = join->withInputs({leftA, rightA}, *builder_);
  EXPECT_EQ(unchanged, join);

  NodeCP rebuilt = join->withInputs({leftB, rightB}, *builder_);
  EXPECT_NE(rebuilt, join);
  EXPECT_EQ(rebuilt->as<Join>()->left(), leftB);
  EXPECT_EQ(rebuilt->as<Join>()->right(), rightB);
  EXPECT_EQ(rebuilt->as<Join>()->joinType(), join->joinType());
}

TEST_F(NodeTransformTest, withInputsUnionAllUnchangedRoundtripAndSizeMismatch) {
  NodeCP legA = leaf("u_a");
  NodeCP legB = leaf("u_b");

  QGVector<optimizer::ColumnVector> legColumns;
  legColumns.push_back(optimizer::ColumnVector{legA->outputColumns()[0]});
  legColumns.push_back(optimizer::ColumnVector{legB->outputColumns()[0]});

  auto* unionAll = builder_->make<UnionAll>(
      {NodeVector{legA, legB},
       std::move(legColumns),
       optimizer::ColumnVector{makeColumn("u_out", velox::BIGINT())}});

  NodeCP unchanged = unionAll->withInputs(NodeVector{legA, legB}, *builder_);
  EXPECT_EQ(unchanged, unionAll);

  VELOX_ASSERT_THROW(unionAll->withInputs(NodeVector{legA}, *builder_), "");
}

TEST_F(NodeTransformTest, rewriteRebuildsJoinWhenOnlyOneSideChanges) {
  NodeCP leftA = leaf("l_a");
  NodeCP rightA = leaf("r_a");
  NodeCP leftB = leaf("l_b");

  auto* join = builder_->make<Join>(
      {leftA,
       rightA,
       velox::core::JoinType::kInner,
       ExprVector{},
       ExprVector{},
       ExprVector{},
       /*nullAware=*/false,
       /*nullAsValue=*/false,
       optimizer::ColumnVector{
           leftA->outputColumns()[0], rightA->outputColumns()[0]}});

  NodeCP result = join->rewrite(
      *builder_, [&](NodeCP n) { return n == leftA ? leftB : n; });

  ASSERT_NE(result, join);
  EXPECT_EQ(result->as<Join>()->left(), leftB);
  EXPECT_EQ(result->as<Join>()->right(), rightA);
}

TEST_F(NodeTransformTest, rewriteWithIdentityReturnsSameRoot) {
  NodeCP values = leaf("a");
  auto* filter = builder_->make<Filter>({values, ExprVector{truePredicate()}});

  NodeCP result = filter->rewrite(*builder_, [](NodeCP n) { return n; });
  EXPECT_EQ(result, filter);
}

TEST_F(NodeTransformTest, rewriteSubstitutingLeafRebuildsParent) {
  NodeCP leafA = leaf("a");
  NodeCP leafB = leaf("b");
  auto* filter = builder_->make<Filter>({leafA, ExprVector{truePredicate()}});

  NodeCP result = filter->rewrite(
      *builder_, [&](NodeCP n) { return n == leafA ? leafB : n; });

  ASSERT_NE(result, filter);
  ASSERT_EQ(result->nodeType(), NodeType::kFilter);
  EXPECT_EQ(result->as<Filter>()->input(), leafB);
  EXPECT_EQ(result->as<Filter>()->predicates(), filter->predicates());
}

// A node returned by the callback is terminal: `rewrite` does not descend
// into its children and does not reapply the callback to it.
TEST_F(NodeTransformTest, rewriteDoesNotDescendIntoSubstitute) {
  NodeCP leafA = leaf("a");
  auto* inner = builder_->make<Filter>({leafA, ExprVector{truePredicate()}});
  auto* outer = builder_->make<Filter>({inner, ExprVector{truePredicate()}});

  NodeCP substituteLeaf = leaf("sub_leaf");
  auto* substitute =
      builder_->make<Filter>({substituteLeaf, ExprVector{truePredicate()}});

  std::vector<NodeCP> visited;
  NodeCP result = outer->rewrite(*builder_, [&](NodeCP n) -> NodeCP {
    visited.push_back(n);
    return n == inner ? static_cast<NodeCP>(substitute) : n;
  });

  EXPECT_EQ(result->as<Filter>()->input(), substitute);
  EXPECT_THAT(visited, testing::Not(testing::Contains(substituteLeaf)));
  EXPECT_THAT(visited, testing::Not(testing::Contains(substitute)));
}

TEST_F(NodeTransformTest, rewritePropagatesRebuildUpToRoot) {
  NodeCP leafA = leaf("a");
  NodeCP leafB = leaf("b");
  auto* f1 = builder_->make<Filter>({leafA, ExprVector{truePredicate()}});
  auto* f2 = builder_->make<Filter>({f1, ExprVector{truePredicate()}});
  auto* f3 = builder_->make<Filter>({f2, ExprVector{truePredicate()}});

  NodeCP result =
      f3->rewrite(*builder_, [&](NodeCP n) { return n == leafA ? leafB : n; });

  ASSERT_NE(result, f3);
  const auto* r3 = result->as<Filter>();
  const auto* r2 = r3->input()->as<Filter>();
  const auto* r1 = r2->input()->as<Filter>();
  EXPECT_EQ(r1->input(), leafB);
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
