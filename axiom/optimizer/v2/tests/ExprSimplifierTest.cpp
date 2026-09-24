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
#include "axiom/optimizer/v2/ExprSimplifier.h"
#include "axiom/optimizer/v2/tests/UnitTestBase.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

using testing::ElementsAre;

class ExprSimplifierTest : public UnitTestBase {};

TEST_F(ExprSimplifierTest, identicalCoalesceArguments) {
  const auto* column = makeColumn("a", velox::BIGINT());
  ExprFactory exprs{*builder_};
  ExprSimplifier simplifier{*builder_, *evaluator_};

  {
    SCOPED_TRACE("Deterministic");
    ExprCP coalesce = exprs.makeCoalesce(column, column);

    EXPECT_EQ(simplifier.simplify(coalesce), column);
  }

  {
    SCOPED_TRACE("Non-deterministic");
    ExprCP nondeterministic = builder_->makeCall(
        toName("nondeterministic"),
        column->value(),
        {column},
        FunctionSet{} | FunctionSet::kNonDeterministic);
    ExprCP coalesce = exprs.makeCoalesce(nondeterministic, nondeterministic);

    EXPECT_EQ(simplifier.simplify(coalesce), coalesce);
  }
}

TEST_F(ExprSimplifierTest, simplifyTree) {
  ExprFactory exprs{*builder_};
  ExprSimplifier simplifier{*builder_, *evaluator_};

  {
    SCOPED_TRACE("Field");
    const auto* row = makeColumn("r", velox::ROW("a", velox::BIGINT()));
    ExprCP coalesce = exprs.makeCoalesce(row, row);
    const auto* field =
        make<Field>(toType(velox::BIGINT()), coalesce, /*index=*/0);

    ExprCP simplified = simplifier.simplifyTree(field);

    ASSERT_TRUE(simplified->is(PlanType::kFieldExpr));
    EXPECT_EQ(simplified->as<Field>()->base(), row);
    EXPECT_EQ(simplified->as<Field>()->index(), 0);
  }

  {
    SCOPED_TRACE("Lambda");
    const auto* argument = makeColumn("a", velox::BIGINT());
    ExprCP coalesce = exprs.makeCoalesce(argument, argument);
    const auto* lambda =
        make<Lambda>(ColumnVector{argument}, toType(velox::BIGINT()), coalesce);

    ExprCP simplified = simplifier.simplifyTree(lambda);

    ASSERT_TRUE(simplified->is(PlanType::kLambdaExpr));
    EXPECT_THAT(simplified->as<Lambda>()->args(), ElementsAre(argument));
    EXPECT_EQ(simplified->as<Lambda>()->body(), argument);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
