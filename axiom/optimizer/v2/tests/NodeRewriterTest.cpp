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

class NodeSubstituter : public NodeRewriter<> {
 public:
  NodeSubstituter(Builder& builder, NodeCP from, NodeCP to)
      : NodeRewriter<>{builder}, from_{from}, to_{to} {}

  const std::vector<NodeCP>& visited() const {
    return visited_;
  }

 protected:
  NodeCP rewriteNode(NodeCP node, NoContext& context) override {
    visited_.push_back(node);
    return node == from_ ? to_ : NodeRewriter::rewriteNode(node, context);
  }

 private:
  const NodeCP from_;
  const NodeCP to_;
  std::vector<NodeCP> visited_;
};

struct NodeVisitCount {
  int count{0};
};

class CountingRewriter : public NodeRewriter<NodeVisitCount> {
 public:
  using NodeRewriter<NodeVisitCount>::NodeRewriter;

 protected:
  NodeCP rewriteNode(NodeCP node, NodeVisitCount& context) override {
    ++context.count;
    return NodeRewriter::rewriteNode(node, context);
  }
};

TEST_F(NodeRewriterTest, passthroughReturnsSamePointer) {
  NodeCP values = leaf("a");
  NodeCP filter = builder_->make<Filter>({values, ExprVector{truePredicate()}});
  NodeCP outer = builder_->make<Filter>({filter, ExprVector{truePredicate()}});

  NodeRewriter<> rewriter{*builder_};
  EXPECT_EQ(rewriter.rewrite(outer), outer);
}

TEST_F(NodeRewriterTest, multiInputPassthroughReturnsSamePointer) {
  NodeCP legA = leaf("m_a");
  NodeCP legB = leaf("m_b");
  QGVector<optimizer::ColumnVector> legColumns;
  legColumns.push_back(optimizer::ColumnVector{legA->outputColumns()[0]});
  legColumns.push_back(optimizer::ColumnVector{legB->outputColumns()[0]});
  NodeCP unionAll = builder_->make<UnionAll>(
      {NodeVector{legA, legB},
       std::move(legColumns),
       optimizer::ColumnVector{makeColumn("m_out", velox::BIGINT())}});

  NodeRewriter<> rewriter{*builder_};
  EXPECT_EQ(rewriter.rewrite(unionAll), unionAll);
}

TEST_F(NodeRewriterTest, contextThreadsThroughSubtree) {
  NodeCP values = leaf("a");
  NodeCP filter = builder_->make<Filter>({values, ExprVector{truePredicate()}});
  NodeCP outer = builder_->make<Filter>({filter, ExprVector{truePredicate()}});

  CountingRewriter rewriter{*builder_};
  NodeVisitCount context;
  EXPECT_EQ(rewriter.rewrite(outer, context), outer);
  EXPECT_EQ(context.count, 3);
}

TEST_F(NodeRewriterTest, substitutionRebuildsParentChain) {
  NodeCP leafA = leaf("a");
  // `replacement` reuses leafA's Column so the ancestor Filters can rebuild
  // through `withInputs`.
  NodeCP replacement =
      builder_->make<Filter>({leafA, ExprVector{truePredicate()}});
  NodeCP innerFilter =
      builder_->make<Filter>({leafA, ExprVector{truePredicate()}});
  NodeCP outerFilter =
      builder_->make<Filter>({innerFilter, ExprVector{truePredicate()}});

  NodeSubstituter rewriter{*builder_, leafA, replacement};
  NodeCP result = rewriter.rewrite(outerFilter);

  ASSERT_NE(result, outerFilter);
  const auto* rewrittenOuter = result->as<Filter>();
  const auto* rewrittenInner = rewrittenOuter->input()->as<Filter>();
  EXPECT_EQ(rewrittenInner->input(), replacement);
  EXPECT_EQ(
      rewrittenOuter->predicates(), outerFilter->as<Filter>()->predicates());
  EXPECT_EQ(
      rewrittenInner->predicates(), innerFilter->as<Filter>()->predicates());
}

TEST_F(NodeRewriterTest, substitutionIsTerminal) {
  NodeCP leafA = leaf("a");
  NodeCP intercepted =
      builder_->make<Filter>({leafA, ExprVector{truePredicate()}});
  NodeCP outer =
      builder_->make<Filter>({intercepted, ExprVector{truePredicate()}});
  NodeCP substitute = builder_->makeEmptyValues(
      optimizer::ColumnVector{intercepted->outputColumns()[0]});

  NodeSubstituter rewriter{*builder_, intercepted, substitute};
  NodeCP result = rewriter.rewrite(outer);

  ASSERT_NE(result, outer);
  EXPECT_EQ(result->as<Filter>()->input(), substitute);
  EXPECT_THAT(rewriter.visited(), testing::Not(testing::Contains(leafA)));
}

TEST_F(NodeRewriterTest, uninvolvedSiblingKeepsIdentity) {
  NodeCP leftA = leaf("l_a");
  NodeCP leftReplacement =
      builder_->make<Filter>({leftA, ExprVector{truePredicate()}});
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

  NodeSubstituter rewriter{*builder_, leftA, leftReplacement};
  NodeCP result = rewriter.rewrite(join);

  ASSERT_NE(result, join);
  const auto* rewrittenJoin = result->as<Join>();
  EXPECT_EQ(rewrittenJoin->left(), leftReplacement);
  EXPECT_EQ(rewrittenJoin->right(), right);
}

TEST_F(NodeRewriterTest, midInputChangeBackFillsPrefixAndKeepsSuffix) {
  NodeCP legA = leaf("u_a");
  NodeCP legB = leaf("u_b");
  NodeCP legBReplacement =
      builder_->make<Filter>({legB, ExprVector{truePredicate()}});
  NodeCP legC = leaf("u_c");

  QGVector<optimizer::ColumnVector> legColumns;
  legColumns.push_back(optimizer::ColumnVector{legA->outputColumns()[0]});
  legColumns.push_back(optimizer::ColumnVector{legB->outputColumns()[0]});
  legColumns.push_back(optimizer::ColumnVector{legC->outputColumns()[0]});

  NodeCP unionAll = builder_->make<UnionAll>(
      {NodeVector{legA, legB, legC},
       std::move(legColumns),
       optimizer::ColumnVector{makeColumn("u_out", velox::BIGINT())}});

  NodeSubstituter rewriter{*builder_, legB, legBReplacement};
  NodeCP result = rewriter.rewrite(unionAll);

  ASSERT_NE(result, unionAll);
  EXPECT_THAT(
      result->inputs(), testing::ElementsAre(legA, legBReplacement, legC));
  EXPECT_EQ(
      result->as<UnionAll>()->legColumns(),
      unionAll->as<UnionAll>()->legColumns());
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
