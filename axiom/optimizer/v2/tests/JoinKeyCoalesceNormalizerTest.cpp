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

#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/JoinKeyCoalesceNormalizer.h"
#include "axiom/optimizer/v2/tests/UnitTestBase.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

using testing::ElementsAre;

class JoinKeyCoalesceNormalizerTest : public UnitTestBase {
 protected:
  NodeCP rewrite(NodeCP root) {
    return JoinKeyCoalesceNormalizer::normalize(root, *builder_, *evaluator_);
  }
};

TEST_F(JoinKeyCoalesceNormalizerTest, invalidatesReboundColumn) {
  const auto* leftKey = makeColumn("a", velox::BIGINT());
  const auto* rightKey = makeColumn("b", velox::BIGINT());
  const auto* result = makeColumn("c", velox::BIGINT());
  const auto* join = builder_->make<Join>({
      .left = builder_->makeEmptyValues({leftKey}),
      .right = builder_->makeEmptyValues({rightKey}),
      .joinType = velox::core::JoinType::kLeft,
      .leftKeys = {leftKey},
      .rightKeys = {rightKey},
      .outputColumns = {leftKey, rightKey},
  });
  const auto* rebound = builder_->make<Project>({
      .input = join,
      .exprs =
          {
              builder_->makeLiteral(
                  velox::Variant(int64_t{7}), leftKey->value().type),
              rightKey,
          },
      .outputColumns = {leftKey, rightKey},
  });
  ExprCP coalesce = ExprFactory{*builder_}.makeCoalesce(leftKey, rightKey);
  const auto* root = builder_->make<Project>({
      .input = rebound,
      .exprs = {coalesce},
      .outputColumns = {result},
  });

  const auto* rewritten = rewrite(root)->as<Project>();

  EXPECT_EQ(rewritten->exprs().front(), coalesce);
}

TEST_F(JoinKeyCoalesceNormalizerTest, unchangedExpression) {
  const auto* leftKey = makeColumn("a", velox::BIGINT());
  const auto* rightKey = makeColumn("b", velox::BIGINT());
  const auto* predicate = makeColumn("p", velox::BOOLEAN());
  const auto* result = makeColumn("c", velox::BOOLEAN());
  const auto* join = builder_->make<Join>({
      .left = builder_->makeEmptyValues({leftKey, predicate}),
      .right = builder_->makeEmptyValues({rightKey}),
      .joinType = velox::core::JoinType::kLeft,
      .leftKeys = {leftKey},
      .rightKeys = {rightKey},
      .outputColumns = {leftKey, predicate},
  });
  ExprFactory exprs{*builder_};
  ExprCP conjunction = exprs.makeAnd(builder_->makeBoolean(true), predicate);
  const auto* root = builder_->make<Project>({
      .input = join,
      .exprs = {conjunction},
      .outputColumns = {result},
  });

  EXPECT_EQ(rewrite(root), root);
}

TEST_F(JoinKeyCoalesceNormalizerTest, fullJoinCanonicalizesArgumentOrder) {
  const auto* rightKey = makeColumn("b", velox::BIGINT());
  const auto* leftKey = makeColumn("a", velox::BIGINT());
  const auto* firstResult = makeColumn("c", velox::BIGINT());
  const auto* secondResult = makeColumn("d", velox::BIGINT());
  const auto* join = builder_->make<Join>({
      .left = builder_->makeEmptyValues({leftKey}),
      .right = builder_->makeEmptyValues({rightKey}),
      .joinType = velox::core::JoinType::kFull,
      .leftKeys = {leftKey},
      .rightKeys = {rightKey},
      .outputColumns = {leftKey, rightKey},
  });
  ExprFactory exprs{*builder_};
  ExprCP canonical = exprs.makeCoalesce(leftKey, rightKey);
  const auto* root = builder_->make<Project>({
      .input = join,
      .exprs = {canonical, exprs.makeCoalesce(rightKey, leftKey)},
      .outputColumns = {firstResult, secondResult},
  });

  const auto* rewritten = rewrite(root)->as<Project>();

  EXPECT_THAT(rewritten->exprs(), ElementsAre(canonical, canonical));
}

TEST_F(JoinKeyCoalesceNormalizerTest, floatingPointKey) {
  const auto* leftKey = makeColumn("a", velox::DOUBLE());
  const auto* rightKey = makeColumn("b", velox::DOUBLE());
  const auto* firstResult = makeColumn("c", velox::DOUBLE());
  const auto* secondResult = makeColumn("d", velox::DOUBLE());
  const auto* join = builder_->make<Join>({
      .left = builder_->makeEmptyValues({leftKey}),
      .right = builder_->makeEmptyValues({rightKey}),
      .joinType = velox::core::JoinType::kFull,
      .leftKeys = {leftKey},
      .rightKeys = {rightKey},
      .outputColumns = {leftKey, rightKey},
  });
  ExprFactory exprs{*builder_};
  ExprCP forward = exprs.makeCoalesce(leftKey, rightKey);
  ExprCP reverse = exprs.makeCoalesce(rightKey, leftKey);
  const auto* root = builder_->make<Project>({
      .input = join,
      .exprs = {forward, reverse},
      .outputColumns = {firstResult, secondResult},
  });

  const auto* rewritten = rewrite(root)->as<Project>();

  EXPECT_THAT(rewritten->exprs(), ElementsAre(forward, reverse));
}

TEST_F(JoinKeyCoalesceNormalizerTest, conflictingBranchFacts) {
  const auto* leftKey = makeColumn("left_key", velox::BIGINT());
  const auto* rightKey = makeColumn("right_key", velox::BIGINT());
  const auto* leftParentKey = makeColumn("left_parent_key", velox::BIGINT());
  const auto* rightParentKey = makeColumn("right_parent_key", velox::BIGINT());
  const auto* leftValue = makeColumn("left_value", velox::BIGINT());
  const auto* rightValue = makeColumn("right_value", velox::BIGINT());
  const auto* result = makeColumn("result", velox::BIGINT());
  const auto* leftInput = builder_->makeEmptyValues({leftKey, leftParentKey});
  const auto* rightInput =
      builder_->makeEmptyValues({rightKey, rightParentKey});
  const auto* leftJoin = builder_->make<Join>({
      .left = leftInput,
      .right = rightInput,
      .joinType = velox::core::JoinType::kLeft,
      .leftKeys = {leftKey},
      .rightKeys = {rightKey},
      .outputColumns = {leftKey, leftParentKey},
  });
  const auto* rightJoin = builder_->make<Join>({
      .left = leftInput,
      .right = rightInput,
      .joinType = velox::core::JoinType::kRight,
      .leftKeys = {leftKey},
      .rightKeys = {rightKey},
      .outputColumns = {rightKey, rightParentKey},
  });
  ExprCP coalesce = ExprFactory{*builder_}.makeCoalesce(leftKey, rightKey);
  const auto* leftProject = builder_->make<Project>({
      .input = leftJoin,
      .exprs = {leftKey, leftParentKey, coalesce},
      .outputColumns = {leftKey, leftParentKey, leftValue},
  });
  const auto* rightProject = builder_->make<Project>({
      .input = rightJoin,
      .exprs = {rightKey, rightParentKey, coalesce},
      .outputColumns = {rightKey, rightParentKey, rightValue},
  });
  const auto* parentJoin = builder_->make<Join>({
      .left = leftProject,
      .right = rightProject,
      .joinType = velox::core::JoinType::kInner,
      .leftKeys = {leftParentKey},
      .rightKeys = {rightParentKey},
      .outputColumns = {leftKey, rightKey, leftValue, rightValue},
  });
  const auto* root = builder_->make<Project>({
      .input = parentJoin,
      .exprs = {coalesce},
      .outputColumns = {result},
  });

  const auto* rewritten = rewrite(root)->as<Project>();
  const auto* rewrittenParentJoin = rewritten->input()->as<Join>();

  EXPECT_EQ(rewritten->exprs().front(), coalesce);
  EXPECT_THAT(
      rewrittenParentJoin->left()->as<Project>()->exprs(),
      ElementsAre(leftKey, leftParentKey, leftKey));
  EXPECT_THAT(
      rewrittenParentJoin->right()->as<Project>()->exprs(),
      ElementsAre(rightKey, rightParentKey, rightKey));
}

TEST_F(JoinKeyCoalesceNormalizerTest, fixedPointBranches) {
  const auto* state = makeColumn("state", velox::BOOLEAN());
  const auto* stepOutput = makeColumn("step", velox::BOOLEAN());
  const auto* converged = makeColumn("converged", velox::BOOLEAN());
  const auto stateName = toName("r");
  ExprFactory exprs{*builder_};

  auto makeBranch =
      [&](NodeCP left, ColumnCP leftKey, ColumnCP rightKey, ColumnCP output) {
        const auto* join = builder_->make<Join>({
            .left = left,
            .right = builder_->makeEmptyValues({rightKey}),
            .joinType = velox::core::JoinType::kLeft,
            .leftKeys = {leftKey},
            .rightKeys = {rightKey},
            .outputColumns = {leftKey, rightKey},
        });
        return builder_->make<Project>({
            .input = join,
            .exprs = {exprs.makeCoalesce(leftKey, rightKey)},
            .outputColumns = {output},
        });
      };

  const auto* anchorKey = makeColumn("anchor_key", velox::BOOLEAN());
  const auto* anchorRightKey = makeColumn("anchor_right_key", velox::BOOLEAN());
  const auto* anchor = makeBranch(
      builder_->makeEmptyValues({anchorKey}), anchorKey, anchorRightKey, state);
  const auto* workingTable = builder_->make<WorkingTable>({
      .name = stateName,
      .outputColumns = {state},
      .readMode = WorkingTableReadMode::kLatestDelta,
  });
  const auto* stepRightKey = makeColumn("step_right_key", velox::BOOLEAN());
  const auto* step = makeBranch(workingTable, state, stepRightKey, stepOutput);
  const auto* convergenceRightKey =
      makeColumn("convergence_right_key", velox::BOOLEAN());
  const auto* convergence =
      makeBranch(workingTable, state, convergenceRightKey, converged);
  const auto* fixedPoint = builder_->make<FixedPoint>({
      .anchor = anchor,
      .step = step,
      .convergence = convergence,
      .name = stateName,
      .outputColumns = {state},
      .maxIterations = 3,
      .recursiveNumDrivers = std::nullopt,
  });

  // Each FixedPoint branch owns an independent join-key fact. Rebuilding the
  // node must rewrite the coalesce in all three branches.
  const auto* rewritten = rewrite(fixedPoint)->as<FixedPoint>();

  EXPECT_THAT(
      rewritten->anchor()->as<Project>()->exprs(), ElementsAre(anchorKey));
  EXPECT_THAT(rewritten->step()->as<Project>()->exprs(), ElementsAre(state));
  EXPECT_THAT(
      rewritten->convergence()->as<Project>()->exprs(), ElementsAre(state));
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
