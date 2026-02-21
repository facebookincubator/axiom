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

#include <fmt/format.h>
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

namespace {

class ExpressionParserTest : public PrestoParserTestBase {
 protected:
  lp::ExprPtr parseExpr(std::string_view sql) {
    return makeParser().parseExpression(sql, true);
  }

  // Parses 'SELECT <expr> FROM nation' and verifies the project expression
  // matches expectedExpr via toString().
  void testNationExpr(std::string_view expr, std::string_view expectedExpr) {
    SCOPED_TRACE(expr);
    testSelect(
        fmt::format("SELECT {} FROM nation", expr),
        matchScan().project({std::string(expectedExpr)}));
  }

  // Parses a decimal literal and verifies its value and type.
  template <typename T>
  void testDecimal(std::string_view sql, T value, const TypePtr& type) {
    SCOPED_TRACE(sql);

    auto expr = parseExpr(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), type->toString());

    auto v = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(v->isNull());
    ASSERT_EQ(v->value<T>(), value);
  }
};

TEST_F(ExpressionParserTest, types) {
  // Verifies that cast and try_cast produce the expected type and are preserved
  // as kCast and kTryCast special forms respectively.
  auto test = [&](std::string_view castArgs, const TypePtr& expectedType) {
    SCOPED_TRACE(castArgs);

    auto castExpr = parseExpr(fmt::format("cast({})", castArgs));
    VELOX_EXPECT_EQ_TYPES(castExpr->type(), expectedType);
    ASSERT_TRUE(castExpr->isSpecialForm());
    ASSERT_EQ(
        castExpr->as<lp::SpecialFormExpr>()->form(), lp::SpecialForm::kCast);

    auto tryCastExpr = parseExpr(fmt::format("try_cast({})", castArgs));
    VELOX_EXPECT_EQ_TYPES(tryCastExpr->type(), expectedType);
    ASSERT_TRUE(tryCastExpr->isSpecialForm());
    ASSERT_EQ(
        tryCastExpr->as<lp::SpecialFormExpr>()->form(),
        lp::SpecialForm::kTryCast);
  };

  test("null as boolean", BOOLEAN());
  test("'1' as tinyint", TINYINT());
  test("null as smallint", SMALLINT());
  test("'2' as int", INTEGER());
  test("null as integer", INTEGER());
  test("'3' as bIgInT", BIGINT());
  test("'2020-01-01' as date", DATE());
  test("null as timestamp", TIMESTAMP());
  test("null as decimal(3, 2)", DECIMAL(3, 2));
  test("null as decimal(33, 10)", DECIMAL(33, 10));

  test("null as int array", ARRAY(INTEGER()));
  test("null as varchar array", ARRAY(VARCHAR()));
  test("null as map(integer, real)", MAP(INTEGER(), REAL()));
  test("null as row(int, double)", ROW({INTEGER(), DOUBLE()}));
  test("null as row(a int, b double)", ROW({"a", "b"}, {INTEGER(), DOUBLE()}));
}

TEST_F(ExpressionParserTest, intervalDayTime) {
  auto test = [&](std::string_view sql, int64_t expectedSeconds) {
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), INTERVAL_DAY_TIME()->toString());

    auto value = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(value->isNull());
    ASSERT_EQ(value->value<int64_t>(), expectedSeconds * 1'000);
  };

  test("INTERVAL '2' DAY", 2 * 24 * 60 * 60);
  test("INTERVAL '3' HOUR", 3 * 60 * 60);
  test("INTERVAL '4' MINUTE", 4 * 60);
  test("INTERVAL '5' SECOND", 5);

  test("INTERVAL '' DAY", 0);
  test("INTERVAL '0' HOUR", 0);

  test("INTERVAL '-2' DAY", -2 * 24 * 60 * 60);
  test("INTERVAL '-3' HOUR", -3 * 60 * 60);
  test("INTERVAL '-4' MINUTE", -4 * 60);
  test("INTERVAL '-5' SECOND", -5);
}

TEST_F(ExpressionParserTest, decimal) {
  auto testShort =
      [&](std::string_view sql, int64_t value, const TypePtr& type) {
        testDecimal<int64_t>(sql, value, type);
      };

  auto testLong =
      [&](std::string_view sql, std::string_view value, const TypePtr& type) {
        testDecimal<int128_t>(sql, folly::to<int128_t>(value), type);
      };

  // Short decimals.
  testShort("DECIMAL '1.2'", 12, DECIMAL(2, 1));
  testShort("DECIMAL '-1.23'", -123, DECIMAL(3, 2));
  testShort("DECIMAL '+12.3'", 123, DECIMAL(3, 1));
  testShort("DECIMAL '1.2345'", 12345, DECIMAL(5, 4));
  testShort("DECIMAL '12'", 12, DECIMAL(2, 0));
  testShort("DECIMAL '12.'", 12, DECIMAL(2, 0));
  testShort("DECIMAL '.12'", 12, DECIMAL(2, 2));
  testShort("DECIMAL '000001.2'", 12, DECIMAL(2, 1));
  testShort("DECIMAL '-000001.2'", -12, DECIMAL(2, 1));

  // Long decimals.
  testLong(
      "decimal '11111222223333344444555556666677777888'",
      "11111222223333344444555556666677777888",
      DECIMAL(38, 0));
  testLong(
      "decimal '000000011111222223333344444555556666677777888'",
      "11111222223333344444555556666677777888",
      DECIMAL(38, 0));
  testLong(
      "decimal '11111222223333344444.55'",
      "1111122222333334444455",
      DECIMAL(22, 2));
  testLong(
      "decimal '00000000000000011111222223333344444.55'",
      "1111122222333334444455",
      DECIMAL(22, 2));
  testLong(
      "decimal '-11111.22222333334444455555'",
      "-1111122222333334444455555",
      DECIMAL(25, 20));

  // Zeros.
  testShort("DECIMAL '0'", 0, DECIMAL(1, 0));
  testShort("DECIMAL '00000000000000000000000'", 0, DECIMAL(1, 0));
  testShort("DECIMAL '0.'", 0, DECIMAL(1, 0));
  testShort("DECIMAL '0.0'", 0, DECIMAL(1, 1));
  testShort("DECIMAL '0.000'", 0, DECIMAL(3, 3));
  testShort("DECIMAL '.0'", 0, DECIMAL(1, 1));

  testLong(
      "DECIMAL '0.00000000000000000000000000000000000000'",
      "0",
      DECIMAL(38, 38));
}

TEST_F(ExpressionParserTest, intervalYearMonth) {
  auto test = [&](std::string_view sql, int64_t expected) {
    auto expr = parseExpr(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), INTERVAL_YEAR_MONTH()->toString());

    auto value = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(value->isNull());
    ASSERT_EQ(value->value<int32_t>(), expected);
  };

  test("INTERVAL '2' YEAR", 2 * 12);
  test("INTERVAL '3' MONTH", 3);

  test("INTERVAL '' YEAR", 0);
  test("INTERVAL '0' MONTH", 0);

  test("INTERVAL '-2' YEAR", -2 * 12);
  test("INTERVAL '-3' MONTH", -3);
}

TEST_F(ExpressionParserTest, doubleLiteral) {
  auto test = [&](std::string_view sql, double expected) {
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), DOUBLE()->toString());

    auto value = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(value->isNull());
    ASSERT_DOUBLE_EQ(value->value<double>(), expected);
  };

  test("1E10", 1e10);
  test("1.5E10", 1.5e10);
  test("1.23E-5", 1.23e-5);
  test(".5E2", 0.5e2);
  test("1E+5", 1e5);
}

TEST_F(ExpressionParserTest, timestampLiteral) {
  auto test = [&](std::string_view sql, const TypePtr& expectedType) {
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);

    VELOX_ASSERT_EQ_TYPES(expr->type(), expectedType);
  };

  test("TIMESTAMP '2020-01-01'", TIMESTAMP());
  test("TIMESTAMP '2020-01-01 00:00:00'", TIMESTAMP());
  test("TIMESTAMP '2020-01-01 00:00:00.000'", TIMESTAMP());
  test(
      "TIMESTAMP '2020-01-01 00:00 America/Los_Angeles'",
      TIMESTAMP_WITH_TIME_ZONE());

  VELOX_ASSERT_THROW(
      parseExpr("TIMESTAMP 'foo'"), "Not a valid timestamp literal");
}

TEST_F(ExpressionParserTest, atTimeZone) {
  // AT TIME ZONE translates to at_timezone().
  EXPECT_EQ(
      "at_timezone(from_unixtime(CAST(1700000000 AS DOUBLE), UTC), America/New_York)",
      parseExpr(
          "from_unixtime(1700000000, 'UTC') AT TIME ZONE 'America/New_York'")
          ->toString());
  EXPECT_EQ(
      "date_format(date_trunc(hour, at_timezone(from_unixtime(CAST(1700000000 AS DOUBLE), UTC), GMT)), %Y-%m-%d+%H:00)",
      parseExpr(
          "date_format(date_trunc('hour', from_unixtime(1700000000, 'UTC') AT TIME ZONE 'GMT'), '%Y-%m-%d+%H:00')")
          ->toString());
}

TEST_F(ExpressionParserTest, extract) {
  // EXTRACT(field FROM x) translates to a scalar function call.
  auto test = [&](std::string_view field, const std::string& expectedFunction) {
    auto sql =
        fmt::format("EXTRACT({} FROM TIMESTAMP '2020-06-15 12:30:45')", field);
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);
    ASSERT_TRUE(expr->isCall());
    ASSERT_EQ(expr->as<lp::CallExpr>()->name(), expectedFunction);
  };

  test("YEAR", "year");
  test("MONTH", "month");
  test("DAY", "day");
  test("HOUR", "hour");
  test("MINUTE", "minute");
  test("SECOND", "second");
}

TEST_F(ExpressionParserTest, nullif) {
  // NULLIF(a, b) translates to IF(eq(a, b), null, a).
  EXPECT_EQ(
      "IF(eq(1, 2), CAST(null AS INTEGER), 1)",
      parseExpr("NULLIF(1, 2)")->toString());
  EXPECT_EQ(
      "IF(eq(1, 1), CAST(null AS INTEGER), 1)",
      parseExpr("nullif(1, 1)")->toString());
  EXPECT_EQ(
      "IF(eq(foo, bar), CAST(null AS VARCHAR), foo)",
      parseExpr("NULLIF('foo', 'bar')")->toString());
}

TEST_F(ExpressionParserTest, null) {
  EXPECT_EQ("is_null(1)", parseExpr("1 is null")->toString());
  EXPECT_EQ("is_null(1)", parseExpr("1 IS NULL")->toString());

  EXPECT_EQ("not(is_null(1))", parseExpr("1 is not null")->toString());
  EXPECT_EQ("not(is_null(1))", parseExpr("1 IS NOT NULL")->toString());
}

TEST_F(ExpressionParserTest, unaryArithmetic) {
  EXPECT_EQ("negate(1)", parseExpr("-1")->toString());
  EXPECT_EQ("1", parseExpr("+1")->toString());
}

TEST_F(ExpressionParserTest, distinctFrom) {
  EXPECT_EQ(
      "distinct_from(1, 2)", parseExpr("1 is distinct from 2")->toString());
  EXPECT_EQ(
      "not(distinct_from(1, 2))",
      parseExpr("1 is not distinct from 2")->toString());
}

TEST_F(ExpressionParserTest, ifClause) {
  EXPECT_EQ(
      "IF(gt(1, 2), 100, null)", parseExpr("if (1 > 2, 100)")->toString());
  EXPECT_EQ(
      "IF(gt(1, 2), 100, 200)", parseExpr("if (1 > 2, 100, 200)")->toString());

  testNationExpr(
      "if (n_nationkey between 10 and 13, 'foo')",
      "IF(between(n_nationkey, CAST(10 AS BIGINT), CAST(13 AS BIGINT)), foo, null)");
}

TEST_F(ExpressionParserTest, notBetween) {
  // NOT BETWEEN translates to not(between(x, low, high)).
  testNationExpr(
      "n_nationkey NOT BETWEEN 10 AND 20",
      "not(between(n_nationkey, CAST(10 AS BIGINT), CAST(20 AS BIGINT)))");
}

TEST_F(ExpressionParserTest, switch) {
  // Searched CASE (CASE WHEN cond THEN result).
  EXPECT_EQ(
      "SWITCH(gt(1, 2), 100, gt(3, 4), 200)",
      parseExpr("case when 1 > 2 then 100 when 3 > 4 then 200 end")
          ->toString());
  EXPECT_EQ(
      "SWITCH(gt(1, 2), 100, gt(3, 4), 200, 300)",
      parseExpr("case when 1 > 2 then 100 when 3 > 4 then 200 else 300 end")
          ->toString());

  // Simple CASE (CASE x WHEN v THEN result) desugars to SWITCH(eq(x, v), ...).
  EXPECT_EQ(
      "SWITCH(eq(1, 1), 100, eq(1, 2), 200)",
      parseExpr("case 1 when 1 then 100 when 2 then 200 end")->toString());
  EXPECT_EQ(
      "SWITCH(eq(1, 1), 100, eq(1, 2), 200, 300)",
      parseExpr("case 1 when 1 then 100 when 2 then 200 else 300 end")
          ->toString());
}

TEST_F(ExpressionParserTest, in) {
  EXPECT_EQ("IN(1, 2, 3, 4)", parseExpr("1 in (2,3,4)")->toString());
  EXPECT_EQ("IN(1, 2, 3, 4)", parseExpr("1 IN (2,3,4)")->toString());

  EXPECT_EQ("not(IN(1, 2, 3, 4))", parseExpr("1 not in (2,3,4)")->toString());
  EXPECT_EQ("not(IN(1, 2, 3, 4))", parseExpr("1 NOT IN (2,3,4)")->toString());

  // Coercions.
  testNationExpr(
      "n_nationkey in (1, 2, 3)",
      "IN(n_nationkey, CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT))");

  auto assertInSubquery = [](const lp::ExprPtr& expr) {
    ASSERT_EQ(lp::ExprKind::kSpecialForm, expr->kind());
    auto& in = *expr->as<lp::SpecialFormExpr>();
    ASSERT_EQ(in.form(), lp::SpecialForm::kIn);
    ASSERT_EQ(in.inputs().size(), 2);
    ASSERT_EQ(lp::ExprKind::kInputReference, in.inputAt(0)->kind());
    ASSERT_EQ(lp::ExprKind::kSubquery, in.inputAt(1)->kind());
  };

  // Subquery IN in WHERE clause produces a filter with IN(column, subquery).
  testSelect(
      "SELECT * FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name like 'A%')",
      matchScan().filter([&](const auto& node) {
        auto filter = std::dynamic_pointer_cast<const lp::FilterNode>(node);
        assertInSubquery(filter->predicate());
      }));

  // Subquery IN in SELECT clause produces a project with IN(column, subquery).
  testSelect(
      "SELECT n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name like 'A%') FROM nation",
      matchScan().project([&](const auto& node) {
        auto project = std::dynamic_pointer_cast<const lp::ProjectNode>(node);
        ASSERT_EQ(project->expressions().size(), 1);
        assertInSubquery(project->expressionAt(0));
      }));
}

TEST_F(ExpressionParserTest, notLike) {
  // NOT LIKE translates to not(like(x, pattern)).
  testNationExpr("n_name NOT LIKE 'A%'", "not(like(n_name, A%))");
}

TEST_F(ExpressionParserTest, likeEscape) {
  // LIKE with ESCAPE translates to a 3-argument like() call.
  testNationExpr("n_name LIKE 'A%' ESCAPE '#'", "like(n_name, A%, #)");
  testNationExpr("n_name NOT LIKE 'A%' ESCAPE '#'", "not(like(n_name, A%, #))");
}

TEST_F(ExpressionParserTest, coalesce) {
  testNationExpr("coalesce(n_name, 'foo')", "COALESCE(n_name, foo)");
  testNationExpr("COALESCE(n_name, 'foo')", "COALESCE(n_name, foo)");

  // Coercions.
  testNationExpr(
      "coalesce(n_regionkey, 1)", "COALESCE(n_regionkey, CAST(1 AS BIGINT))");
}

TEST_F(ExpressionParserTest, concat) {
  // || translates to concat().
  testNationExpr("n_name || n_comment", "concat(n_name, n_comment)");
}

TEST_F(ExpressionParserTest, position) {
  // POSITION(x IN y) translates to strpos(y, x).
  testNationExpr("POSITION('A' IN n_name)", "strpos(n_name, A)");
  testNationExpr("POSITION(n_comment IN n_name)", "strpos(n_name, n_comment)");
}

TEST_F(ExpressionParserTest, substringFrom) {
  // SUBSTRING(x FROM y FOR z) translates to substr(x, y, z).
  testNationExpr("SUBSTRING(n_name FROM 1)", "substr(n_name, 1)");
  testNationExpr("SUBSTRING(n_name FROM 2 FOR 3)", "substr(n_name, 2, 3)");
}

TEST_F(ExpressionParserTest, subscript) {
  EXPECT_EQ(
      "subscript(array_constructor(1, 2, 3), 1)",
      parseExpr("array[1, 2, 3][1]")->toString());
  EXPECT_EQ(
      "DEREFERENCE(row_constructor(1, 2), 1)",
      parseExpr("row(1, 2)[2]")->toString());
}

TEST_F(ExpressionParserTest, dereference) {
  // Named field dereference.
  EXPECT_EQ(
      "DEREFERENCE(CAST(row_constructor(1, 2) AS ROW<a:INTEGER,b:INTEGER>), a)",
      parseExpr("cast(row(1, 2) as row(a int, b int)).a")->toString());

  // Legacy field name dereference.
  EXPECT_EQ(
      "DEREFERENCE(row_constructor(1, 2), 0)",
      parseExpr("row(1, 2).field0")->toString());
  EXPECT_EQ(
      "DEREFERENCE(row_constructor(1, 2), 0)",
      parseExpr("row(1, 2).field000")->toString());
  EXPECT_EQ(
      "DEREFERENCE(row_constructor(1, 2), 1)",
      parseExpr("row(1, 2).field1")->toString());
  EXPECT_EQ(
      "DEREFERENCE(row_constructor(1, 2), 1)",
      parseExpr("row(1, 2).field01")->toString());

  VELOX_ASSERT_THROW(
      parseExpr("row(1, 2).field2"), "Invalid legacy field name: field2");

  VELOX_ASSERT_THROW(
      parseExpr("cast(row(1, 2) as row(a int, b int)).field0"),
      "Cannot access named field using legacy field name: field0 vs. a");

  auto expr = parseExpr(
      R"(cast(json_parse('{"foo": 1, "bar": 2}') as row(foo bigint, "BAR" int)).BAR)");
  VELOX_EXPECT_EQ_TYPES(expr->type(), INTEGER());
}

TEST_F(ExpressionParserTest, row) {
  testNationExpr(
      "row(n_regionkey, n_name)", "row_constructor(n_regionkey, n_name)");
}

TEST_F(ExpressionParserTest, lambda) {
  ASSERT_NO_THROW(parseExpr("filter(array[1,2,3], x -> x > 1)"));
  ASSERT_NO_THROW(parseExpr("FILTER(array[1,2,3], x -> x > 1)"));

  ASSERT_NO_THROW(parseExpr("filter(array[], x -> true)"));

  ASSERT_NO_THROW(parseExpr("reduce(array[], map(), (s, x) -> s, s -> 123)"));

  ASSERT_NO_THROW(parseExpr(
      "reduce(array[], map(), (s, x) -> map(array[1], array[2]), s -> 123)"));
}

} // namespace
} // namespace axiom::sql::presto::test
