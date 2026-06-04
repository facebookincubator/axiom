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

#include "axiom/sql/presto/tests/ExprMatcher.h"
#include <gtest/gtest-spi.h>
#include <gtest/gtest.h>
#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/type/Type.h"

namespace facebook::axiom::logical_plan::test {
namespace {

using namespace facebook::velox;

// Helpers to build lp::Expr nodes.

ExprPtr field(const TypePtr& type, const std::string& name) {
  return std::make_shared<InputReferenceExpr>(type, name);
}

ExprPtr constant(const TypePtr& type, const Variant& value) {
  return std::make_shared<ConstantExpr>(type, std::make_shared<Variant>(value));
}

ExprPtr constantNull(const TypePtr& type) {
  return std::make_shared<ConstantExpr>(
      type, std::make_shared<Variant>(type->kind()));
}

ExprPtr call(
    const TypePtr& type,
    const std::string& name,
    std::vector<ExprPtr> inputs) {
  return std::make_shared<CallExpr>(type, name, std::move(inputs));
}

ExprPtr specialForm(
    const TypePtr& type,
    SpecialForm form,
    std::vector<ExprPtr> inputs) {
  return std::make_shared<SpecialFormExpr>(type, form, std::move(inputs));
}

// Helpers to build expected IExpr (untyped) nodes.

velox::core::ExprPtr wildcard() {
  return std::make_shared<velox::core::CallExpr>(
      std::string{ExprMatcher::kWildcard},
      std::vector<velox::core::ExprPtr>{},
      std::nullopt);
}

velox::core::ExprPtr existsWildcard() {
  return std::make_shared<velox::core::CallExpr>(
      std::string{ExprMatcher::kExistsWildcard},
      std::vector<velox::core::ExprPtr>{},
      std::nullopt);
}

ExprPtr exists() {
  auto subqueryPlan = std::make_shared<ValuesNode>(
      "values_0",
      ROW({"x"}, {BIGINT()}),
      ValuesNode::Variants{Variant::row({42LL})});
  auto subquery = std::make_shared<SubqueryExpr>(subqueryPlan);
  return specialForm(BOOLEAN(), SpecialForm::kExists, {subquery});
}

class ExprMatcherTest : public testing::Test {
 protected:
  velox::core::ExprPtr parseExpr(const std::string& sql) {
    return parser_.parseExpr(sql);
  }

  velox::core::ExprPtr parseAggregateExpr(const std::string& sql) {
    return parser_.parseAggregateExpr(sql);
  }

  velox::core::ExprPtr parseWindowExpr(const std::string& sql) {
    velox::parse::ParseOptions options;
    options.correctWindowFrameDefault = true;
    velox::parse::DuckSqlExpressionsParser windowParser(options);
    return windowParser.parseScalarOrWindowExpr(sql);
  }

  velox::parse::DuckSqlExpressionsParser parser_;
};

TEST_F(ExprMatcherTest, inputReference) {
  ExprMatcher::match(field(BIGINT(), "a"), parseExpr("a"));

  EXPECT_NONFATAL_FAILURE(
      ExprMatcher::match(field(BIGINT(), "a"), parseExpr("b")),
      R"(  inputRef->name()
    Which is: "a"
  field->name()
    Which is: "b")");
}

TEST_F(ExprMatcherTest, integerConstant) {
  ExprMatcher::match(constant(BIGINT(), Variant(42LL)), parseExpr("42"));
}

TEST_F(ExprMatcherTest, integerTypeTolerance) {
  // Plan uses INTEGER, DuckDB parses as BIGINT.
  ExprMatcher::match(constant(INTEGER(), Variant(42)), parseExpr("42"));
}

TEST_F(ExprMatcherTest, doubleEpsilon) {
  ExprMatcher::match(constant(DOUBLE(), Variant(0.1 + 0.2)), parseExpr("0.3"));
}

TEST_F(ExprMatcherTest, stringConstant) {
  ExprMatcher::match(
      constant(VARCHAR(), Variant("hello")), parseExpr("'hello'"));
}

TEST_F(ExprMatcherTest, nullConstant) {
  ExprMatcher::match(constantNull(BIGINT()), parseExpr("null"));
}

TEST_F(ExprMatcherTest, booleanConstant) {
  ExprMatcher::match(constant(BOOLEAN(), Variant(true)), parseExpr("true"));
  ExprMatcher::match(constant(BOOLEAN(), Variant(false)), parseExpr("false"));
}

TEST_F(ExprMatcherTest, constantMismatch) {
  EXPECT_NONFATAL_FAILURE(
      ExprMatcher::match(constant(BIGINT(), Variant(42LL)), parseExpr("99")),
      "Constant mismatch.");
}

TEST_F(ExprMatcherTest, functionCall) {
  auto actual =
      call(BIGINT(), "plus", {field(BIGINT(), "a"), field(BIGINT(), "b")});
  ExprMatcher::match(actual, parseExpr("a + b"));
}

TEST_F(ExprMatcherTest, nestedFunctionCall) {
  auto actual = call(
      BIGINT(),
      "mod",
      {call(
           BIGINT(),
           "plus",
           {field(BIGINT(), "a"), constant(BIGINT(), Variant(1LL))}),
       constant(BIGINT(), Variant(3LL))});
  ExprMatcher::match(actual, parseExpr("(a + 1) % 3"));
}

TEST_F(ExprMatcherTest, functionNameMismatch) {
  auto actual =
      call(BIGINT(), "minus", {field(BIGINT(), "a"), field(BIGINT(), "b")});
  EXPECT_NONFATAL_FAILURE(
      ExprMatcher::match(actual, parseExpr("a + b")),
      R"(  call->name()
    Which is: "minus"
  expectedCall->name()
    Which is: "plus")");
}

TEST_F(ExprMatcherTest, specialFormAnd) {
  auto actual = specialForm(
      BOOLEAN(),
      SpecialForm::kAnd,
      {field(BOOLEAN(), "a"), field(BOOLEAN(), "b")});
  ExprMatcher::match(actual, parseExpr("a AND b"));
}

TEST_F(ExprMatcherTest, specialFormOr) {
  auto actual = specialForm(
      BOOLEAN(),
      SpecialForm::kOr,
      {field(BOOLEAN(), "a"), field(BOOLEAN(), "b")});
  ExprMatcher::match(actual, parseExpr("a OR b"));
}

TEST_F(ExprMatcherTest, specialFormCast) {
  auto actual =
      specialForm(BIGINT(), SpecialForm::kCast, {field(INTEGER(), "a")});
  ExprMatcher::match(actual, parseExpr("cast(a as bigint)"));
}

TEST_F(ExprMatcherTest, specialFormTryCast) {
  auto actual =
      specialForm(VARCHAR(), SpecialForm::kTryCast, {field(BIGINT(), "a")});
  ExprMatcher::match(actual, parseExpr("try_cast(a as varchar)"));

  // CAST vs TRY_CAST mismatch.
  actual = specialForm(BIGINT(), SpecialForm::kCast, {field(INTEGER(), "a")});
  EXPECT_NONFATAL_FAILURE(
      ExprMatcher::match(actual, parseExpr("try_cast(a as bigint)")),
      "CAST vs TRY_CAST mismatch.");
}

TEST_F(ExprMatcherTest, specialFormCoalesce) {
  auto actual = specialForm(
      BIGINT(),
      SpecialForm::kCoalesce,
      {field(BIGINT(), "a"), constant(BIGINT(), Variant(0LL))});
  ExprMatcher::match(actual, parseExpr("coalesce(a, 0)"));
}

TEST_F(ExprMatcherTest, specialFormIf) {
  auto actual = specialForm(
      BIGINT(),
      SpecialForm::kIf,
      {field(BOOLEAN(), "cond"), field(BIGINT(), "a"), field(BIGINT(), "b")});
  ExprMatcher::match(actual, parseExpr("if(cond, a, b)"));
}

TEST_F(ExprMatcherTest, specialFormDereference) {
  auto structType = ROW({"x", "y"}, {BIGINT(), VARCHAR()});
  auto actual = specialForm(
      BIGINT(),
      SpecialForm::kDereference,
      {field(structType, "a"), constant(VARCHAR(), Variant("x"))});
  ExprMatcher::match(actual, parseExpr("(a).x"));
}

TEST_F(ExprMatcherTest, specialFormDereferenceByIndex) {
  auto structType = ROW({"x", "y"}, {BIGINT(), VARCHAR()});
  auto actual = specialForm(
      BIGINT(),
      SpecialForm::kDereference,
      {field(structType, "a"), constant(INTEGER(), Variant(0))});
  ExprMatcher::match(actual, parseExpr("(a).x"));
}

TEST_F(ExprMatcherTest, specialFormNullIf) {
  auto actual = specialForm(
      BIGINT(),
      SpecialForm::kNullIf,
      {field(BIGINT(), "a"), field(BIGINT(), "b"), constantNull(BIGINT())});
  ExprMatcher::match(actual, parseExpr("nullif(a, b)"));
}

TEST_F(ExprMatcherTest, aggregate) {
  auto actual = std::make_shared<AggregateExpr>(
      BIGINT(), "sum", std::vector<ExprPtr>{field(BIGINT(), "a")});
  ExprMatcher::match(actual, parseAggregateExpr("sum(a)"));
}

TEST_F(ExprMatcherTest, aggregateDistinct) {
  auto actual = std::make_shared<AggregateExpr>(
      BIGINT(),
      "sum",
      std::vector<ExprPtr>{field(BIGINT(), "a")},
      nullptr,
      std::vector<SortingField>{},
      true);
  ExprMatcher::match(actual, parseAggregateExpr("sum(DISTINCT a)"));
}

TEST_F(ExprMatcherTest, aggregateWithFilter) {
  auto filterExpr = call(
      BOOLEAN(),
      "gt",
      {field(BIGINT(), "a"), constant(BIGINT(), Variant(0LL))});

  auto actual = std::make_shared<AggregateExpr>(
      BIGINT(), "sum", std::vector<ExprPtr>{field(BIGINT(), "a")}, filterExpr);
  ExprMatcher::match(actual, parseAggregateExpr("sum(a) FILTER (WHERE a > 0)"));
}

TEST_F(ExprMatcherTest, window) {
  WindowExpr::Frame frame;
  frame.type = WindowExpr::WindowType::kRange;
  frame.startType = WindowExpr::BoundType::kUnboundedPreceding;
  frame.endType = WindowExpr::BoundType::kUnboundedFollowing;

  auto actual = std::make_shared<WindowExpr>(
      BIGINT(),
      "row_number",
      std::vector<ExprPtr>{},
      std::vector<ExprPtr>{field(BIGINT(), "a")},
      std::vector<SortingField>{},
      frame,
      false);
  ExprMatcher::match(
      actual, parseWindowExpr("row_number() OVER (PARTITION BY a)"));
}

TEST_F(ExprMatcherTest, windowWithOrderBy) {
  WindowExpr::Frame frame;
  frame.type = WindowExpr::WindowType::kRange;
  frame.startType = WindowExpr::BoundType::kUnboundedPreceding;
  frame.endType = WindowExpr::BoundType::kCurrentRow;

  auto actual = std::make_shared<WindowExpr>(
      BIGINT(),
      "row_number",
      std::vector<ExprPtr>{},
      std::vector<ExprPtr>{},
      std::vector<SortingField>{
          {field(BIGINT(), "a"), SortOrder::kAscNullsLast}},
      frame,
      false);
  ExprMatcher::match(
      actual, parseWindowExpr("row_number() OVER (ORDER BY a ASC NULLS LAST)"));
}

TEST_F(ExprMatcherTest, lambda) {
  auto signature = ROW({"x", "y"}, {BIGINT(), BIGINT()});
  auto body =
      call(BIGINT(), "plus", {field(BIGINT(), "x"), field(BIGINT(), "y")});
  auto actual = std::make_shared<LambdaExpr>(signature, body);
  ExprMatcher::match(actual, parseExpr("(x, y) -> x + y"));
}

TEST_F(ExprMatcherTest, wildcardMatchesInputRef) {
  ExprMatcher::match(field(BIGINT(), "a"), wildcard());
}

TEST_F(ExprMatcherTest, wildcardMatchesConstant) {
  ExprMatcher::match(constant(BIGINT(), Variant(42LL)), wildcard());
}

TEST_F(ExprMatcherTest, wildcardMatchesCall) {
  auto actual =
      call(BIGINT(), "plus", {field(BIGINT(), "a"), field(BIGINT(), "b")});
  ExprMatcher::match(actual, wildcard());
}

TEST_F(ExprMatcherTest, wildcardInSubtree) {
  auto actual =
      call(BIGINT(), "plus", {field(BIGINT(), "a"), field(BIGINT(), "b")});
  ExprMatcher::match(
      actual,
      std::make_shared<velox::core::CallExpr>(
          "plus",
          std::vector<velox::core::ExprPtr>{parseExpr("a"), wildcard()},
          std::nullopt));
}

TEST_F(ExprMatcherTest, existsWildcardMatchesExists) {
  ExprMatcher::match(exists(), existsWildcard());
}

TEST_F(ExprMatcherTest, existsWildcardRejectsNonExists) {
  EXPECT_NONFATAL_FAILURE(
      ExprMatcher::match(field(BOOLEAN(), "a"), existsWildcard()),
      "Expected EXISTS (any_exists() wildcard)");

  auto coalesce = specialForm(
      BIGINT(),
      SpecialForm::kCoalesce,
      {field(BIGINT(), "a"), field(BIGINT(), "b")});
  EXPECT_NONFATAL_FAILURE(
      ExprMatcher::match(coalesce, existsWildcard()),
      "Expected EXISTS special form, got COALESCE.");
}

} // namespace
} // namespace facebook::axiom::logical_plan::test
