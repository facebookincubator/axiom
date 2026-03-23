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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/Type.h"

using namespace facebook::velox;

namespace facebook::axiom::logical_plan {
namespace {

class ExprTest : public testing::Test {
 protected:
  static ExprPtr literal(int64_t val, TypePtr type = BIGINT()) {
    return std::make_shared<ConstantExpr>(
        std::move(type), std::make_shared<Variant>(Variant(val)));
  }

  static ExprPtr inputRef(std::string name, TypePtr type = BIGINT()) {
    return std::make_shared<InputReferenceExpr>(
        std::move(type), std::move(name));
  }

  static ExprPtr
  call(std::string name, std::vector<ExprPtr> inputs, TypePtr type = BIGINT()) {
    return std::make_shared<CallExpr>(
        std::move(type), std::move(name), std::move(inputs));
  }

  static ExprPtr cast(TypePtr type, ExprPtr input) {
    return std::make_shared<SpecialFormExpr>(
        std::move(type), SpecialForm::kCast, std::vector<ExprPtr>{input});
  }

  static void testLooksConstant(const ExprPtr& expr, bool expected) {
    EXPECT_EQ(expr->looksConstant(), expected);
  }
};

TEST_F(ExprTest, looksConstant) {
  testLooksConstant(literal(1), true);
  testLooksConstant(inputRef("a"), false);

  testLooksConstant(call("plus", {literal(1), literal(2)}), true);
  testLooksConstant(call("plus", {inputRef("a"), literal(1)}), false);

  testLooksConstant(cast(DOUBLE(), literal(1)), true);
  testLooksConstant(cast(DOUBLE(), inputRef("a")), false);
}

TEST_F(ExprTest, aggregateExprDistinctOrderBy) {
  auto x = inputRef("x");
  auto y = inputRef("y");
  auto z = inputRef("z");

  auto makeAggregate = [](const std::string& functionName,
                          std::vector<ExprPtr> inputs,
                          std::vector<SortingField> ordering = {}) {
    return std::make_shared<AggregateExpr>(
        BIGINT(),
        functionName,
        std::move(inputs),
        /*filter=*/nullptr,
        std::move(ordering),
        /*distinct=*/false);
  };

  auto makeDistinctAggregate = [](const std::string& functionName,
                                  std::vector<ExprPtr> inputs,
                                  std::vector<SortingField> ordering = {}) {
    return std::make_shared<AggregateExpr>(
        BIGINT(),
        functionName,
        std::move(inputs),
        /*filter=*/nullptr,
        std::move(ordering),
        /*distinct=*/true);
  };

  // DISTINCT without ORDER BY is valid.
  EXPECT_NO_THROW(makeDistinctAggregate("array_agg", {x}));

  // Non-DISTINCT with ORDER BY is valid.
  EXPECT_NO_THROW(
      makeAggregate("array_agg", {x}, {{y, SortOrder::kAscNullsFirst}}));

  // DISTINCT with ORDER BY key on single argument is valid.
  EXPECT_NO_THROW(makeDistinctAggregate(
      "array_agg", {x}, {{x, SortOrder::kAscNullsFirst}}));

  // DISTINCT with multiple arguments and ORDER BY key in arguments is valid.
  EXPECT_NO_THROW(makeDistinctAggregate(
      "array_agg", {x, y}, {{y, SortOrder::kAscNullsFirst}}));

  // DISTINCT with multiple ORDER BY keys, all in arguments, is valid.
  EXPECT_NO_THROW(makeDistinctAggregate(
      "array_agg",
      {x, y},
      {{x, SortOrder::kAscNullsFirst}, {y, SortOrder::kAscNullsFirst}}));

  // DISTINCT with ORDER BY keys as a subset of arguments in different order is
  // valid. min_by naturally takes 3 arguments: min_by(value, comparison, n).
  EXPECT_NO_THROW(makeDistinctAggregate(
      "min_by",
      {x, y, z},
      {{y, SortOrder::kAscNullsFirst}, {x, SortOrder::kAscNullsFirst}}));

  // DISTINCT with ORDER BY key in args is valid even when input and ordering
  // use different ExprPtr objects for the same column.
  auto orderX = inputRef("x");
  EXPECT_NE(x.get(), orderX.get());
  EXPECT_NO_THROW(makeDistinctAggregate(
      "array_agg", {x}, {{orderX, SortOrder::kAscNullsFirst}}));

  // DISTINCT with ORDER BY key NOT in arguments is not allowed.
  VELOX_ASSERT_THROW(
      makeDistinctAggregate("array_agg", {x}, {{y, SortOrder::kAscNullsFirst}}),
      "For DISTINCT aggregations, ORDER BY keys must appear in aggregation arguments");

  // DISTINCT with some ORDER BY key not in arguments is not allowed.
  VELOX_ASSERT_THROW(
      makeDistinctAggregate(
          "array_agg",
          {x},
          {{x, SortOrder::kAscNullsFirst}, {y, SortOrder::kAscNullsFirst}}),
      "For DISTINCT aggregations, ORDER BY keys must appear in aggregation arguments");
}

} // namespace
} // namespace facebook::axiom::logical_plan
