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

#include "axiom/sql/spark/SparkSqlParser.h"

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace axiom::sql::spark {
namespace {

namespace lp = facebook::axiom::logical_plan;
using namespace facebook::velox;

class SparkSqlParserTest : public testing::Test {
 protected:
  static lp::ExprApi parse(std::string_view sql) {
    return SparkSqlParser(tz::locateZone("America/Los_Angeles"))
        .parseExpression(sql);
  }
};

TEST_F(SparkSqlParserTest, expressions) {
  const auto aliased = parse("transform(t.a, x -> x + 1)[0] AS b");
  EXPECT_EQ(aliased.alias(), "b");
  EXPECT_EQ(
      *aliased.expr(),
      *lp::Call(
           "subscript",
           lp::Call(
               "transform",
               lp::Col("a", lp::Col("t")),
               lp::Lambda({"x"}, lp::Call("add", lp::Col("x"), lp::Lit(1)))),
           lp::Lit(0))
           .expr());

  // Adjacent string literals concatenate, so 'it''s' is its.
  EXPECT_EQ(
      *parse("CASE WHEN a > 0 THEN 'it''s' ELSE \"no\" END").expr(),
      *lp::Call(
           "if",
           lp::Call("greaterthan", lp::Col("a"), lp::Lit(0)),
           lp::Lit("its"),
           lp::Lit("no"))
           .expr());

  EXPECT_EQ(
      *parse("CASE a WHEN 1 THEN 'one' ELSE 'other' END").expr(),
      *lp::Call(
           "case", lp::Col("a"), lp::Lit(1), lp::Lit("one"), lp::Lit("other"))
           .expr());

  EXPECT_EQ(
      *parse("count(DISTINCT `a``b`) <=> 1L").expr(),
      *lp::Call(
           "equalnullsafe",
           lp::Call("count", lp::Col("a`b")).distinct(),
           lp::Lit(int64_t{1}))
           .expr());

  EXPECT_EQ(
      *parse("cast(a AS decimal(10, 2)) NOT BETWEEN 1.5 AND 2e1").expr(),
      *lp::Call(
           "not",
           lp::Call(
               "between",
               lp::Cast(DECIMAL(10, 2), lp::Col("a")),
               lp::Lit(int64_t{15}, DECIMAL(2, 1)),
               lp::Lit(20.0)))
           .expr());

  // A TIMESTAMP without a time zone is in the session time zone.
  EXPECT_EQ(
      *parse("DATE '2025-01-01' < TIMESTAMP '2025-01-01 16:00:00'").expr(),
      *lp::Call(
           "lessthan",
           lp::Lit(Variant(20'089), DATE()),
           lp::Lit(Variant(Timestamp(1'735'776'000, 0)), TIMESTAMP()))
           .expr());
}

TEST_F(SparkSqlParserTest, dataTypes) {
  EXPECT_EQ(
      *SparkSqlParser::parseDataType(
          "struct<a: decimal(10, 2), b array<string>>"),
      *ROW({{"a", DECIMAL(10, 2)}, {"b", ARRAY(VARCHAR())}}));
  VELOX_ASSERT_USER_THROW(
      SparkSqlParser::parseDataType("decimal(266, 2)"),
      "Decimal precision must be between 1 and 38");
}

TEST_F(SparkSqlParserTest, errors) {
  VELOX_ASSERT_USER_THROW(parse("a +"), "Syntax error at line 1, position 4");
  VELOX_ASSERT_THROW(
      parse("a IN (SELECT 1)"), "Spark SQL IN subquery is not supported yet");
  VELOX_ASSERT_USER_THROW(
      parse(std::string(2'000, '(') + "1" + std::string(2'000, ')')),
      "Expression exceeds maximum nesting depth");
  VELOX_ASSERT_USER_THROW(
      parse("DATE '2025-13-01'"), "Date literal is not valid");
}

} // namespace
} // namespace axiom::sql::spark
