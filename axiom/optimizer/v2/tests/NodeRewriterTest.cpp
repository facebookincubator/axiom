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
#include "axiom/optimizer/v2/NodeRewriter.h"
#include "axiom/optimizer/v2/tests/UnitTestBase.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

class NodeRewriterTest : public UnitTestBase {
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

// Substitutes one `Values` leaf for another by pointer equality. The `from`
// and `to` leaves must share output-column identity so ancestor rebuilds
// through `withInputs` stay well-formed.
class LeafSubstituter : public NodeRewriter<> {
 public:
  LeafSubstituter(Builder& builder, NodeCP from, NodeCP to)
      : NodeRewriter<>{builder}, from_{from}, to_{to} {}

 protected:
  NodeCP rewriteValues(const Values* node, NoContext& /*context*/) override {
    return node == from_ ? to_ : node;
  }

 private:
  const NodeCP from_;
  const NodeCP to_;
};

TEST_F(NodeRewriterTest, passthroughReturnsSamePointer) {
  NodeCP values = leaf("a");
  NodeCP filter = builder_->make<Filter>({values, ExprVector{truePredicate()}});
  NodeCP outer = builder_->make<Filter>({filter, ExprVector{truePredicate()}});

  NodeRewriter<> rewriter{*builder_};
  EXPECT_EQ(rewriter.rewrite(outer), outer);
}

TEST_F(NodeRewriterTest, substitutionRebuildsParentChain) {
  NodeCP leafA = leaf("a");
  // Reuse leafA's Column so ancestor Filters rebuild through `withInputs`
  // without violating column-identity contracts.
  NodeCP leafB = builder_->makeEmptyValues(
      optimizer::ColumnVector{leafA->outputColumns()[0]});
  NodeCP filter1 = builder_->make<Filter>({leafA, ExprVector{truePredicate()}});
  NodeCP filter2 =
      builder_->make<Filter>({filter1, ExprVector{truePredicate()}});

  LeafSubstituter rewriter{*builder_, leafA, leafB};
  NodeCP result = rewriter.rewrite(filter2);

  ASSERT_NE(result, filter2);
  const auto* r2 = result->as<Filter>();
  const auto* r1 = r2->input()->as<Filter>();
  EXPECT_EQ(r1->input(), leafB);
  EXPECT_EQ(r2->predicates(), filter2->as<Filter>()->predicates());
  EXPECT_EQ(r1->predicates(), filter1->as<Filter>()->predicates());
}

TEST_F(NodeRewriterTest, uninvolvedSiblingKeepsIdentity) {
  NodeCP leftA = leaf("l_a");
  NodeCP leftB = builder_->makeEmptyValues(
      optimizer::ColumnVector{leftA->outputColumns()[0]});
  NodeCP right = leaf("r");

  NodeCP join = builder_->make<Join>(
      {leftA,
       right,
       velox::core::JoinType::kInner,
       ExprVector{},
       ExprVector{},
       ExprVector{},
       /*nullAware=*/false,
       /*nullAsValue=*/false,
       optimizer::ColumnVector{
           leftA->outputColumns()[0], right->outputColumns()[0]}});

  LeafSubstituter rewriter{*builder_, leftA, leftB};
  NodeCP result = rewriter.rewrite(join);

  ASSERT_NE(result, join);
  const auto* r = result->as<Join>();
  EXPECT_EQ(r->left(), leftB);
  EXPECT_EQ(r->right(), right);
}

// Exercises the back-fill branch in `rewriteChildren`: when the first
// changed child sits at `i > 0`, the unchanged prefix `inputs[0..i)` must
// be copied into `newInputs` before the rewritten child is pushed, and
// the unchanged suffix must be appended verbatim.
TEST_F(NodeRewriterTest, midInputChangeBackFillsPrefixAndKeepsSuffix) {
  NodeCP legA = leaf("u_a");
  NodeCP legB = leaf("u_b");
  NodeCP legB2 = builder_->makeEmptyValues(
      optimizer::ColumnVector{legB->outputColumns()[0]});
  NodeCP legC = leaf("u_c");

  QGVector<optimizer::ColumnVector> legColumns;
  legColumns.push_back(optimizer::ColumnVector{legA->outputColumns()[0]});
  legColumns.push_back(optimizer::ColumnVector{legB->outputColumns()[0]});
  legColumns.push_back(optimizer::ColumnVector{legC->outputColumns()[0]});

  NodeCP unionAll = builder_->make<UnionAll>(
      {NodeVector{legA, legB, legC},
       std::move(legColumns),
       optimizer::ColumnVector{makeColumn("u_out", velox::BIGINT())}});

  LeafSubstituter rewriter{*builder_, legB, legB2};
  NodeCP result = rewriter.rewrite(unionAll);

  ASSERT_NE(result, unionAll);
  EXPECT_THAT(result->inputs(), testing::ElementsAre(legA, legB2, legC));
  EXPECT_EQ(
      result->as<UnionAll>()->legColumns(),
      unionAll->as<UnionAll>()->legColumns());
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
