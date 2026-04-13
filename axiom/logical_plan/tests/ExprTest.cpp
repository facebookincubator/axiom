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

#include "axiom/logical_plan/Expr.h"
#include <gtest/gtest.h>
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/ExprResolver.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/type/Type.h"

using namespace facebook::velox;

namespace facebook::axiom::logical_plan {
namespace {

class ExprTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::aggregate::prestosql::registerAllAggregateFunctions();
  }

  static ExprResolver::InputNameResolver inputResolver(
      const RowTypePtr& schema) {
    return [schema](
               const std::optional<std::string>&,
               const std::string& name) -> ExprPtr {
      return std::make_shared<InputReferenceExpr>(
          schema->findChild(name), name);
    };
  }

  void testLooksConstant(const ExprApi& expr, bool expected) {
    auto resolved =
        ExprResolver(nullptr, false)
            .resolveScalarTypes(expr.expr(), inputResolver(schema_));
    EXPECT_EQ(resolved->looksConstant(), expected);
  }

  RowTypePtr schema_ =
      ROW({"a", "x", "y", "z"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
};

TEST_F(ExprTest, looksConstant) {
  testLooksConstant(Lit(1LL), true);
  testLooksConstant(Col("a"), false);

  testLooksConstant(Call("plus", Lit(1LL), Lit(2LL)), true);
  testLooksConstant(Call("plus", Col("a"), Lit(1LL)), false);

  testLooksConstant(Cast(DOUBLE(), Lit(1LL)), true);
  testLooksConstant(Cast(DOUBLE(), Col("a")), false);
}

TEST_F(ExprTest, aggregateExprDistinctOrderBy) {
  auto makeAggregate =
      [this](const ExprApi& expr, const std::vector<SortKey>& ordering = {}) {
        return ExprResolver(nullptr, false)
            .resolveAggregateTypes(
                expr.expr(), inputResolver(schema_), nullptr, ordering, false);
      };

  auto makeDistinctAggregate =
      [this](const ExprApi& expr, const std::vector<SortKey>& ordering = {}) {
        return ExprResolver(nullptr, false)
            .resolveAggregateTypes(
                expr.expr(), inputResolver(schema_), nullptr, ordering, true);
      };

  // DISTINCT without ORDER BY is valid.
  EXPECT_NO_THROW(makeDistinctAggregate(Call("array_agg", Col("x"))));

  // Non-DISTINCT with ORDER BY is valid.
  EXPECT_NO_THROW(makeAggregate(
      Call("array_agg", Col("x")), {SortKey(Col("y"), ASC_NULLS_FIRST)}));

  // DISTINCT with ORDER BY key on single argument is valid.
  EXPECT_NO_THROW(makeDistinctAggregate(
      Call("array_agg", Col("x")), {SortKey(Col("x"), ASC_NULLS_FIRST)}));

  // DISTINCT with multiple arguments and ORDER BY key in arguments is valid.
  EXPECT_NO_THROW(makeDistinctAggregate(
      Call("min_by", Col("x"), Col("y")),
      {SortKey(Col("y"), ASC_NULLS_FIRST)}));

  // DISTINCT with multiple ORDER BY keys, all in arguments, is valid.
  EXPECT_NO_THROW(makeDistinctAggregate(
      Call("min_by", Col("x"), Col("y")),
      {SortKey(Col("x"), ASC_NULLS_FIRST),
       SortKey(Col("y"), ASC_NULLS_FIRST)}));

  // DISTINCT with ORDER BY keys as a subset of arguments in different order is
  // valid. min_by naturally takes 3 arguments: min_by(value, comparison, n).
  EXPECT_NO_THROW(makeDistinctAggregate(
      Call("min_by", Col("x"), Col("y"), Col("z")),
      {SortKey(Col("y"), ASC_NULLS_FIRST),
       SortKey(Col("x"), ASC_NULLS_FIRST)}));

  // DISTINCT with ORDER BY key NOT in arguments is not allowed.
  VELOX_ASSERT_THROW(
      makeDistinctAggregate(
          Call("array_agg", Col("x")), {SortKey(Col("y"), ASC_NULLS_FIRST)}),
      "For DISTINCT aggregations, ORDER BY keys must appear in aggregation arguments");

  // DISTINCT with some ORDER BY key not in arguments is not allowed.
  VELOX_ASSERT_THROW(
      makeDistinctAggregate(
          Call("array_agg", Col("x")),
          {SortKey(Col("x"), ASC_NULLS_FIRST),
           SortKey(Col("y"), ASC_NULLS_FIRST)}),
      "For DISTINCT aggregations, ORDER BY keys must appear in aggregation arguments");
}

TEST_F(ExprTest, aggregateWithLambda) {
  auto makeAggregate = [this](const ExprApi& expr) {
    return ExprResolver(nullptr, false)
        .resolveAggregateTypes(
            expr.expr(), inputResolver(schema_), nullptr, {}, false);
  };

  // reduce_agg(x, 0, (s, x) -> s + x, (s1, s2) -> s1 + s2).
  auto reduceAgg = makeAggregate(Call(
      "reduce_agg",
      Col("x"),
      Lit(0LL),
      Lambda({"s", "x"}, Call("plus", Col("s"), Col("x"))),
      Lambda({"s1", "s2"}, Call("plus", Col("s1"), Col("s2")))));

  EXPECT_EQ(*reduceAgg->type(), *BIGINT());
  EXPECT_EQ(reduceAgg->inputs().size(), 4);
  EXPECT_TRUE(reduceAgg->inputAt(2)->isLambda());
  EXPECT_TRUE(reduceAgg->inputAt(3)->isLambda());
}

TEST_F(ExprTest, aggregateWithLambdaEquality) {
  auto makeAggregate = [this](const ExprApi& expr) {
    return ExprResolver(nullptr, false)
        .resolveAggregateTypes(
            expr.expr(), inputResolver(schema_), nullptr, {}, false);
  };

  auto reduceAgg = makeAggregate(Call(
      "reduce_agg",
      Col("x"),
      Lit(0LL),
      Lambda({"s", "x"}, Call("plus", Col("s"), Col("x"))),
      Lambda({"s1", "s2"}, Call("plus", Col("s1"), Col("s2")))));

  auto reduceAggCopy = makeAggregate(Call(
      "reduce_agg",
      Col("x"),
      Lit(0LL),
      Lambda({"s", "x"}, Call("plus", Col("s"), Col("x"))),
      Lambda({"s1", "s2"}, Call("plus", Col("s1"), Col("s2")))));

  EXPECT_EQ(*reduceAgg, *reduceAggCopy);

  // Different lambda body.
  auto reduceAggDifferent = makeAggregate(Call(
      "reduce_agg",
      Col("x"),
      Lit(0LL),
      Lambda({"s", "x"}, Call("minus", Col("s"), Col("x"))),
      Lambda({"s1", "s2"}, Call("plus", Col("s1"), Col("s2")))));

  EXPECT_NE(*reduceAgg, *reduceAggDifferent);
}

TEST_F(ExprTest, aggregateWithLambdaCoercion) {
  auto schema = ROW({"a", "b"}, {BIGINT(), SMALLINT()});

  auto makeAggregate = [&](const ExprApi& expr) {
    return ExprResolver(nullptr, true)
        .resolveAggregateTypes(
            expr.expr(), inputResolver(schema), nullptr, {}, false);
  };

  // reduce_agg with coercions enabled and mixed types (SMALLINT input,
  // BIGINT initial state). Since reduce_agg has independent type variables
  // (T, S), no inter-argument coercion is needed. This exercises the
  // coercion-enabled code path but not the lambda re-resolution sub-path
  // (no known aggregate function with lambdas currently triggers it).
  auto reduceAgg = makeAggregate(Call(
      "reduce_agg",
      Col("b"),
      Lit(0LL),
      Lambda({"s", "x"}, Call("plus", Col("s"), Col("x"))),
      Lambda({"s1", "s2"}, Call("plus", Col("s1"), Col("s2")))));

  EXPECT_EQ(*reduceAgg->type(), *BIGINT());
  EXPECT_EQ(reduceAgg->inputs().size(), 4);
  EXPECT_EQ(*reduceAgg->inputAt(0)->type(), *SMALLINT());
  EXPECT_TRUE(reduceAgg->inputAt(2)->isLambda());
  EXPECT_TRUE(reduceAgg->inputAt(3)->isLambda());
}

TEST_F(ExprTest, aggregateWithLambdaUnknownFunction) {
  auto makeAggregate = [this](const ExprApi& expr) {
    return ExprResolver(nullptr, false)
        .resolveAggregateTypes(
            expr.expr(), inputResolver(schema_), nullptr, {}, false);
  };

  VELOX_ASSERT_THROW(
      makeAggregate(Call(
          "nonexistent_agg",
          Col("x"),
          Lambda({"s", "x"}, Call("plus", Col("s"), Col("x"))))),
      "Cannot resolve Aggregate with lambda arguments: nonexistent_agg");
}

} // namespace
} // namespace facebook::axiom::logical_plan
