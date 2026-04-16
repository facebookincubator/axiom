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

#include "axiom/optimizer/tests/ExpressionMatcher.h"
#include <gtest/gtest-spi.h>
#include <gtest/gtest.h>
#include "velox/core/Expressions.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::core::em;

namespace {

TypedExprPtr makeConstant(TypePtr type, Variant value) {
  return std::make_shared<ConstantTypedExpr>(std::move(type), std::move(value));
}

TypedExprPtr makeField(const std::string& name, TypePtr type) {
  return std::make_shared<FieldAccessTypedExpr>(std::move(type), name);
}

TypedExprPtr makeCall(
    const std::string& name,
    TypePtr type,
    std::vector<TypedExprPtr> inputs) {
  return std::make_shared<CallTypedExpr>(
      std::move(type), std::move(inputs), name);
}

} // namespace

TEST(ExpressionMatcherTest, exactConstants) {
  EXPECT_TRUE(constant(int64_t(42))
                  .match(makeConstant(BIGINT(), Variant(int64_t(42)))));
  EXPECT_TRUE(constant(3.14).match(makeConstant(DOUBLE(), Variant(3.14))));
  EXPECT_TRUE(
      constant("hello").match(makeConstant(VARCHAR(), Variant("hello"))));
  EXPECT_TRUE(constant(true).match(makeConstant(BOOLEAN(), Variant(true))));
  EXPECT_TRUE(
      constant(int32_t(7)).match(makeConstant(INTEGER(), Variant(int32_t(7)))));
}

TEST(ExpressionMatcherTest, dateConstant) {
  auto days = DATE()->toDays("1994-01-01");
  EXPECT_TRUE(constant(DATE(), "1994-01-01")
                  .match(makeConstant(DATE(), Variant(days))));
}

TEST(ExpressionMatcherTest, fieldAccess) {
  EXPECT_TRUE(col("col_a").match(makeField("col_a", BIGINT())));
}

TEST(ExpressionMatcherTest, functionCall) {
  auto a = makeField("a", BIGINT());
  auto b = makeField("b", BIGINT());
  EXPECT_TRUE(col("a").eq(col("b")).match(makeCall("eq", BOOLEAN(), {a, b})));
  EXPECT_TRUE(call("plus", {col("a"), col("b")})
                  .match(makeCall("plus", BIGINT(), {a, b})));
}

TEST(ExpressionMatcherTest, fluentOperators) {
  auto x = makeField("x", BIGINT());
  auto c = makeConstant(BIGINT(), Variant(int64_t(5)));

  EXPECT_TRUE(col("x")
                  .neq(constant(int64_t(5)))
                  .match(makeCall("neq", BOOLEAN(), {x, c})));
  EXPECT_TRUE(col("x")
                  .lt(constant(int64_t(5)))
                  .match(makeCall("lt", BOOLEAN(), {x, c})));
  EXPECT_TRUE(col("x")
                  .lte(constant(int64_t(5)))
                  .match(makeCall("lte", BOOLEAN(), {x, c})));
  EXPECT_TRUE(col("x")
                  .gt(constant(int64_t(5)))
                  .match(makeCall("gt", BOOLEAN(), {x, c})));
  EXPECT_TRUE(col("x")
                  .gte(constant(int64_t(5)))
                  .match(makeCall("gte", BOOLEAN(), {x, c})));
  EXPECT_TRUE(col("x")
                  .like(constant("%foo%"))
                  .match(makeCall(
                      "like",
                      BOOLEAN(),
                      {x, makeConstant(VARCHAR(), Variant("%foo%"))})));
  EXPECT_TRUE(
      col("x")
          .eq(constant(int64_t(1)))
          .or_(col("x").eq(constant(int64_t(2))))
          .match(makeCall(
              "or",
              BOOLEAN(),
              {makeCall(
                   "eq",
                   BOOLEAN(),
                   {x, makeConstant(BIGINT(), Variant(int64_t(1)))}),
               makeCall(
                   "eq",
                   BOOLEAN(),
                   {x, makeConstant(BIGINT(), Variant(int64_t(2)))})})));
}

TEST(ExpressionMatcherTest, compoundFilter) {
  auto shipdate = makeField("l_shipdate", DATE());
  auto discount = makeField("l_discount", DOUBLE());
  auto quantity = makeField("l_quantity", DOUBLE());

  auto dateDays1 = DATE()->toDays("1994-01-01");
  auto dateDays2 = DATE()->toDays("1995-01-01");

  auto actual = makeCall(
      "and",
      BOOLEAN(),
      {makeCall(
           "and",
           BOOLEAN(),
           {makeCall(
                "and",
                BOOLEAN(),
                {makeCall(
                     "gte",
                     BOOLEAN(),
                     {shipdate, makeConstant(DATE(), Variant(dateDays1))}),
                 makeCall(
                     "lt",
                     BOOLEAN(),
                     {shipdate, makeConstant(DATE(), Variant(dateDays2))})}),
            makeCall(
                "between",
                BOOLEAN(),
                {discount,
                 makeConstant(DOUBLE(), Variant(0.06 - 0.01)),
                 makeConstant(DOUBLE(), Variant(0.07))})}),
       makeCall(
           "lt",
           BOOLEAN(),
           {quantity, makeConstant(DOUBLE(), Variant(24.0))})});

  auto matcher =
      col("l_shipdate")
          .gte(constant(DATE(), "1994-01-01"))
          .and_(col("l_shipdate").lt(constant(DATE(), "1995-01-01")))
          .and_(
              col("l_discount").between(constant(0.06 - 0.01), constant(0.07)))
          .and_(col("l_quantity").lt(constant(24.0)));

  EXPECT_TRUE(matcher.match(actual));
}

TEST(ExpressionMatcherTest, anyMatcher) {
  EXPECT_TRUE(any().match(makeConstant(BIGINT(), Variant(int64_t(42)))));
  EXPECT_TRUE(any().match(makeField("x", DOUBLE())));
  EXPECT_TRUE(
      any().match(makeCall("plus", BIGINT(), {makeField("a", BIGINT())})));
}

TEST(ExpressionMatcherTest, anyInsideCall) {
  auto actual = makeCall(
      "in",
      BOOLEAN(),
      {makeField("x", BIGINT()), makeConstant(BIGINT(), Variant(int64_t(1)))});
  EXPECT_TRUE(call("in", {col("x"), any()}).match(actual));
}

TEST(ExpressionMatcherTest, castExpression) {
  auto input = makeField("x", VARCHAR());
  auto castExpr = std::make_shared<CastTypedExpr>(DATE(), input, false);
  EXPECT_TRUE(cast(DATE(), col("x")).match(castExpr));
}

TEST(ExpressionMatcherTest, castTypeMismatch) {
  auto input = makeField("x", VARCHAR());
  auto castExpr = std::make_shared<CastTypedExpr>(DATE(), input, false);
  EXPECT_NONFATAL_FAILURE(
      cast(INTEGER(), col("x")).match(castExpr), "Expression mismatch");
}

TEST(ExpressionMatcherTest, nullActualExpression) {
  TypedExprPtr nullExpr = nullptr;
  EXPECT_NONFATAL_FAILURE(
      col("x").match(nullExpr), "Expected expression matching");
}

TEST(ExpressionMatcherTest, mismatchConstantValue) {
  auto actual = makeConstant(BIGINT(), Variant(int64_t(42)));
  EXPECT_NONFATAL_FAILURE(constant(99).match(actual), "Expression mismatch");
}

TEST(ExpressionMatcherTest, mismatchFieldName) {
  auto actual = makeField("a", BIGINT());
  EXPECT_NONFATAL_FAILURE(col("b").match(actual), "Expression mismatch");
}

TEST(ExpressionMatcherTest, mismatchCallName) {
  auto a = makeField("a", BIGINT());
  auto b = makeField("b", BIGINT());
  auto actual = makeCall("eq", BOOLEAN(), {a, b});
  EXPECT_NONFATAL_FAILURE(
      call("lt", {col("a"), col("b")}).match(actual), "Expression mismatch");
}

TEST(ExpressionMatcherTest, mismatchNodeKind) {
  auto actual = makeField("x", BIGINT());
  EXPECT_NONFATAL_FAILURE(constant(1).match(actual), "Expression mismatch");
}

TEST(ExpressionMatcherTest, mismatchCallArity) {
  auto a = makeField("a", BIGINT());
  auto actual = makeCall("f", BIGINT(), {a});
  EXPECT_NONFATAL_FAILURE(
      call("f", {col("a"), col("b")}).match(actual), "Expression mismatch");
}
