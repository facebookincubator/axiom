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
#include <array>
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/ExprResolver.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/types/TimeWithTimezoneType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/type/Type.h"

using namespace facebook::velox;

namespace facebook::axiom::logical_plan {
namespace {

class ExprTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::aggregate::prestosql::registerAllAggregateFunctions();
    velox::window::prestosql::registerAllWindowFunctions();
  }

  static ExprResolver::InputNameResolver inputResolver(
      const RowTypePtr& schema) {
    return [schema](
               const std::optional<std::string>& alias,
               const std::string& name) -> ExprPtr {
      // The schema has no table aliases, so 's.a' names field 'a' of 's'.
      // Returning nullptr lets the resolver dereference it.
      if (alias.has_value()) {
        return nullptr;
      }
      return std::make_shared<InputReferenceExpr>(
          schema->findChild(name), name);
    };
  }

  void testLooksConstant(const ExprApi& expr, bool expected) {
    auto resolved =
        ExprResolver(nullptr, nullptr)
            .resolveScalarTypes(expr.expr(), inputResolver(schema_));
    EXPECT_EQ(resolved->looksConstant(), expected);
  }

  ExprPtr resolveScalar(const std::string& sql) {
    return resolveScalar(sql, schema_);
  }

  ExprPtr resolveScalar(const std::string& sql, const RowTypePtr& schema) {
    return ExprResolver(nullptr, &velox::TypeCoercer::defaults())
        .resolveScalarTypes(Sql(sql).expr(), inputResolver(schema));
  }

  AggregateExprPtr resolveAggregate(const std::string& sql) {
    return ExprResolver(nullptr, &velox::TypeCoercer::defaults())
        .resolveAggregateTypes(
            Sql(sql).expr(), inputResolver(schema_), nullptr, {}, false);
  }

  static RowTypePtr atTimeZoneSchema() {
    return ROW(
        {"timestamp",
         "timestamp_with_time_zone",
         "date",
         "time",
         "timestamp_utc",
         "time_micro_utc"},
        {TIMESTAMP(),
         TIMESTAMP_WITH_TIME_ZONE(),
         DATE(),
         TIME(),
         TIMESTAMP_UTC(),
         TIME_MICRO_UTC()});
  }

  static ExprApi atTimeZone(const ExprApi& value, const ExprApi& timeZone) {
    return ExprApi{std::make_shared<velox::core::AtTimeZoneExpr>(
        value.expr(),
        timeZone.expr(),
        velox::core::AtTimeZoneExpr::OperandTypes{
            .timestamp = TIMESTAMP(),
            .timestampWithTimeZone = TIMESTAMP_WITH_TIME_ZONE(),
            .time = TIME(),
            .timeWithTimeZone = TIME_WITH_TIME_ZONE(),
        },
        std::nullopt)};
  }

  static ExprPtr resolveAtTimeZone(const ExprApi& expr) {
    return ExprResolver(nullptr, &velox::TypeCoercer::defaults())
        .resolveScalarTypes(expr.expr(), inputResolver(atTimeZoneSchema()));
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

  // A placeholder has no value until the plan is optimized.
  testLooksConstant(Placeholder(BIGINT(), 1), false);
  testLooksConstant(Call("plus", Placeholder(BIGINT(), 1), Lit(1LL)), false);
}

TEST_F(ExprTest, conjunctInputTypes) {
  auto schema = ROW({"p", "a"}, {BOOLEAN(), BIGINT()});
  auto resolve = [&](const ExprApi& expr, const velox::TypeCoercer* coercer) {
    return ExprResolver(nullptr, coercer)
        .resolveScalarTypes(expr.expr(), inputResolver(schema));
  };

  auto nullLiteral = Lit(Variant{}, UNKNOWN());
  const auto* coercer = &velox::TypeCoercer::defaults();

  // A null literal input types as UNKNOWN and coerces to boolean.
  auto expr = resolve(Col("p") && nullLiteral, coercer);
  EXPECT_EQ(*expr->type(), *BOOLEAN());
  EXPECT_EQ(*expr->inputAt(1)->type(), *BOOLEAN());

  EXPECT_EQ(*resolve(nullLiteral || Col("p"), coercer)->type(), *BOOLEAN());

  // Without a coercer, an UNKNOWN input is kept as is. Velox evaluates the
  // conjunct only if it doesn't short-circuit first.
  EXPECT_EQ(
      *resolve(Col("p") && nullLiteral, nullptr)->inputAt(1)->type(),
      *UNKNOWN());

  // An input of a type that doesn't coerce to boolean is always rejected.
  VELOX_ASSERT_THROW(
      resolve(Col("p") && Col("a"), coercer),
      "Conjunct expression argument is not coercible to BOOLEAN");
  VELOX_ASSERT_THROW(
      resolve(Col("p") && Col("a"), nullptr),
      "All inputs to AND and OR must be boolean");
}

TEST_F(ExprTest, timestampAtTimeZone) {
  auto timestamp = resolveAtTimeZone(
      atTimeZone(Col("timestamp"), Lit("America/Los_Angeles")));
  VELOX_EXPECT_EQ_TYPES(timestamp->type(), TIMESTAMP_WITH_TIME_ZONE());
  EXPECT_TRUE(timestamp->isSpecialForm());
  EXPECT_EQ(timestamp->as<SpecialFormExpr>()->form(), SpecialForm::kAtTimeZone);
  EXPECT_EQ(
      timestamp->toString(), "AT_TIME_ZONE(timestamp, America/Los_Angeles)");

  auto timestampWithTimeZone = resolveAtTimeZone(
      atTimeZone(Col("timestamp_with_time_zone"), Lit("America/New_York")));
  VELOX_EXPECT_EQ_TYPES(
      timestampWithTimeZone->type(), TIMESTAMP_WITH_TIME_ZONE());
  EXPECT_TRUE(timestampWithTimeZone->isSpecialForm());
  EXPECT_EQ(
      timestampWithTimeZone->as<SpecialFormExpr>()->form(),
      SpecialForm::kAtTimeZone);
  EXPECT_EQ(
      timestampWithTimeZone->toString(),
      "AT_TIME_ZONE(timestamp_with_time_zone, America/New_York)");
}

TEST_F(ExprTest, invalidAtTimeZoneOperand) {
  const std::array invalidOperands{
      std::pair{
          "date",
          "AT TIME ZONE requires a time or timestamp value, but got: DATE"},
      std::pair{
          "timestamp_utc",
          "AT TIME ZONE requires a time or timestamp value, but got: TIMESTAMP UTC"},
      std::pair{
          "time_micro_utc",
          "AT TIME ZONE requires a time or timestamp value, but got: TIME MICRO UTC"},
  };
  for (const auto& [column, expectedError] : invalidOperands) {
    SCOPED_TRACE(column);
    VELOX_ASSERT_THROW(
        resolveAtTimeZone(atTimeZone(Col(column), Lit("UTC"))), expectedError);
  }
}

TEST_F(ExprTest, timeAtTimeZone) {
  auto time = resolveAtTimeZone(atTimeZone(Col("time"), Lit("+08:00")));
  VELOX_EXPECT_EQ_TYPES(time->type(), TIME_WITH_TIME_ZONE());
  EXPECT_EQ(
      time->toString(),
      "AT_TIME_ZONE(CAST(time AS TIME WITH TIME ZONE), +08:00)");
}

TEST_F(ExprTest, atTimezoneFunctionCoercion) {
  // Ordinary function calls retain the general coercion rule.
  VELOX_EXPECT_EQ_TYPES(
      resolveAtTimeZone(Call("at_timezone", Col("date"), Lit("UTC")))->type(),
      TIMESTAMP_WITH_TIME_ZONE());
}

TEST_F(ExprTest, inArray) {
  auto schema = ROW({"a", "d"}, {BIGINT(), DOUBLE()});
  auto resolve = [&](const ExprApi& expr) {
    return ExprResolver(nullptr, &velox::TypeCoercer::defaults())
        .resolveScalarTypes(expr.expr(), inputResolver(schema));
  };

  // A list of literals becomes one ARRAY constant of the common type.
  auto expr = resolve(In(Col("a"), Lit(1), Lit(2)));
  EXPECT_EQ(expr->toString(), "IN_ARRAY(a, [1,2])");
  EXPECT_EQ(*expr->inputAt(1)->type(), *ARRAY(BIGINT()));
  EXPECT_EQ(
      resolve(In(Col("a"), Lit(1), Lit(Variant{})))->toString(),
      "IN_ARRAY(a, [1,null])");

  // A conversion that changes a value coerces each literal.
  EXPECT_EQ(
      resolve(In(Col("d"), Lit(1), Lit(2)))->toString(),
      "IN(d, CAST(1 AS DOUBLE), CAST(2 AS DOUBLE))");

  // A single literal or a non-literal keeps the list.
  EXPECT_EQ(resolve(In(Col("a"), Lit(int64_t{1})))->toString(), "IN(a, 1)");
  EXPECT_EQ(
      resolve(In(Col("a"), Lit(int64_t{1}), Col("a")))->toString(),
      "IN(a, 1, a)");

  auto makeInArray = [&](Variant values, const TypePtr& type) {
    return std::make_shared<SpecialFormExpr>(
        BOOLEAN(),
        SpecialForm::kInArray,
        resolve(Col("a")),
        resolve(Lit(std::move(values), type)));
  };
  VELOX_ASSERT_THROW(
      makeInArray(Variant::array({1, 2}), ARRAY(INTEGER())),
      "IN_ARRAY values must be an array of the checked value's type");
  VELOX_ASSERT_THROW(
      makeInArray(Variant::null(TypeKind::ARRAY), ARRAY(BIGINT())),
      "IN_ARRAY values must not be NULL");
  VELOX_ASSERT_THROW(
      makeInArray(Variant::array(std::vector<Variant>{}), ARRAY(BIGINT())),
      "IN_ARRAY values must not be empty");
}

TEST_F(ExprTest, aggregateExprDistinctOrderBy) {
  auto makeAggregate =
      [this](const ExprApi& expr, const std::vector<SortKey>& ordering = {}) {
        return ExprResolver(nullptr, nullptr)
            .resolveAggregateTypes(
                expr.expr(), inputResolver(schema_), nullptr, ordering, false);
      };

  auto makeDistinctAggregate =
      [this](const ExprApi& expr, const std::vector<SortKey>& ordering = {}) {
        return ExprResolver(nullptr, nullptr)
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
    return ExprResolver(nullptr, nullptr)
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

TEST_F(ExprTest, reduceAggOverNull) {
  auto reduceAgg = resolveAggregate(
      "reduce_agg(null, 0, (s, x) -> s + x, (s1, s2) -> s1 + s2)");

  VELOX_EXPECT_EQ_TYPES(reduceAgg->type(), BIGINT());
}

TEST_F(ExprTest, aggregateWithLambdaEquality) {
  auto makeAggregate = [this](const ExprApi& expr) {
    return ExprResolver(nullptr, nullptr)
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
    return ExprResolver(nullptr, &velox::TypeCoercer::defaults())
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
    return ExprResolver(nullptr, nullptr)
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

TEST_F(ExprTest, transformOverNull) {
  // transform's result element type comes from the lambda body.
  VELOX_EXPECT_EQ_TYPES(
      resolveScalar("transform(null, x -> x + 1)")->type(), ARRAY(BIGINT()));
}

TEST_F(ExprTest, filterOverNull) {
  // filter returns the input array type, whose element stays unknown.
  VELOX_EXPECT_EQ_TYPES(
      resolveScalar("filter(null, x -> x > 0)")->type(), ARRAY(UNKNOWN()));
}

TEST_F(ExprTest, reduceOverNull) {
  // reduce returns the accumulator type from the initial state.
  VELOX_EXPECT_EQ_TYPES(
      resolveScalar("reduce(null, 0, (s, e) -> s + e, s -> s)")->type(),
      BIGINT());
}

TEST_F(ExprTest, reduceStateFromInputLambda) {
  // A NULL initial state takes the type the input lambda returns, and the
  // output lambda sees that type.
  VELOX_EXPECT_EQ_TYPES(
      resolveScalar(
          "reduce(arr, null, (s, x) -> coalesce(s, x), s -> s.a)",
          ROW("arr", ARRAY(ROW("a", INTEGER()))))
          ->type(),
      INTEGER());
  VELOX_EXPECT_EQ_TYPES(
      resolveScalar(
          "reduce(arr, null, (s, x) -> x, "
          "s -> CASE WHEN s IS NULL THEN '' ELSE concat('p/', s) END)",
          ROW("arr", ARRAY(VARCHAR())))
          ->type(),
      VARCHAR());
}

TEST_F(ExprTest, scalarLambdaWithTypedInput) {
  auto schema = ROW({"arr"}, {ARRAY(BIGINT())});

  // A typed (non-UNKNOWN) input still resolves.
  VELOX_EXPECT_EQ_TYPES(
      resolveScalar("transform(arr, x -> x + 1)", schema)->type(),
      ARRAY(BIGINT()));
}

TEST_F(ExprTest, scalarLambdaWithNonArrayInput) {
  auto schema = ROW({"i"}, {BIGINT()});

  // transform requires an array argument.
  VELOX_ASSERT_THROW(
      resolveScalar("transform(i, x -> x + 1)", schema), "Can't resolve");
}

TEST_F(ExprTest, nestedLambdaOverEmptyMap) {
  // Empty map() has UNKNOWN key/value types.
  VELOX_EXPECT_EQ_TYPES(
      resolveScalar(
          "reduce(ARRAY[MAP(ARRAY['a'], ARRAY[1])], map(), "
          "(s, x) -> map_zip_with(s, x, (k, v1, v2) -> "
          "COALESCE(v1, 0) + COALESCE(v2, 0)), s -> s)")
          ->type(),
      MAP(VARCHAR(), BIGINT()));
}

TEST_F(ExprTest, windowFrameValidation) {
  using BoundType = velox::core::WindowCallExpr::BoundType;

  auto resolveWindow = [this](const ExprApi& expr, const RowTypePtr& schema) {
    return ExprResolver(nullptr, nullptr)
        .resolveWindowTypes(
            *expr.expr()->as<velox::core::WindowCallExpr>(),
            inputResolver(schema));
  };

  // Non-offset bound (CURRENT ROW) must have a null bound value.
  VELOX_ASSERT_THROW(
      resolveWindow(
          Call("row_number")
              .over(
                  WindowSpec()
                      .orderBy({SortKey(Col("a"))})
                      .rows(
                          BoundType::kCurrentRow,
                          Lit(1LL),
                          BoundType::kUnboundedFollowing,
                          std::nullopt)),
          schema_),
      "Window frame bound value must be null for bound type: CURRENT ROW");

  // Offset bound (PRECEDING) requires a non-null bound value.
  VELOX_ASSERT_THROW(
      resolveWindow(
          Call("row_number")
              .over(
                  WindowSpec()
                      .orderBy({SortKey(Col("a"))})
                      .rows(
                          BoundType::kPreceding,
                          std::nullopt,
                          BoundType::kCurrentRow,
                          std::nullopt)),
          schema_),
      "Window frame bound value is required for bound type: PRECEDING");

  // RANGE + offset bound requires exactly one ORDER BY key.
  VELOX_ASSERT_THROW(
      resolveWindow(
          Call("row_number")
              .over(
                  WindowSpec()
                      .orderBy({SortKey(Col("a")), SortKey(Col("x"))})
                      .range(
                          BoundType::kPreceding,
                          Col("a"),
                          BoundType::kCurrentRow,
                          std::nullopt)),
          schema_),
      "Window RANGE frame with offset bound requires exactly one ORDER BY key");

  // RANGE + offset bound value type must match the ORDER BY key type.
  VELOX_ASSERT_THROW(
      resolveWindow(
          Call("row_number")
              .over(
                  WindowSpec()
                      .orderBy({SortKey(Col("r"))})
                      .range(
                          BoundType::kPreceding,
                          Lit(1LL),
                          BoundType::kCurrentRow,
                          std::nullopt)),
          ROW("r", REAL())),
      "Window RANGE frame bound must evaluate to the ORDER BY type");
}

} // namespace
} // namespace facebook::axiom::logical_plan
