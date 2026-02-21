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

#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

namespace {

class ExpressionParserTest : public PrestoParserTestBase {
 protected:
  lp::ExprPtr parseSqlExpression(std::string_view sql) {
    return makeParser().parseExpression(sql, true);
  }

  // Parses a decimal literal and verifies its value and type.
  template <typename T>
  void testDecimal(std::string_view sql, T value, const TypePtr& type) {
    SCOPED_TRACE(sql);

    auto parser = makeParser();
    auto expr = parser.parseExpression(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), type->toString());

    auto v = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(v->isNull());
    ASSERT_EQ(v->value<T>(), value);
  }
};

TEST_F(ExpressionParserTest, types) {
  auto parser = makeParser();

  auto test = [&](std::string_view sql, const TypePtr& expectedType) {
    SCOPED_TRACE(sql);
    auto expr = parser.parseExpression(sql);

    ASSERT_EQ(expr->type()->toString(), expectedType->toString());
  };

  test("cast(null as boolean)", BOOLEAN());
  test("cast('1' as tinyint)", TINYINT());
  test("cast(null as smallint)", SMALLINT());
  test("cast('2' as int)", INTEGER());
  test("cast(null as integer)", INTEGER());
  test("cast('3' as bIgInT)", BIGINT());
  test("cast('2020-01-01' as date)", DATE());
  test("cast(null as timestamp)", TIMESTAMP());
  test("cast(null as decimal(3, 2))", DECIMAL(3, 2));
  test("cast(null as decimal(33, 10))", DECIMAL(33, 10));

  test("cast(null as int array)", ARRAY(INTEGER()));
  test("cast(null as varchar array)", ARRAY(VARCHAR()));
  test("cast(null as map(integer, real))", MAP(INTEGER(), REAL()));
  test("cast(null as row(int, double))", ROW({INTEGER(), DOUBLE()}));
  test(
      "cast(null as row(a int, b double))",
      ROW({"a", "b"}, {INTEGER(), DOUBLE()}));

  test(
      R"(cast(json_parse('{"foo": 1, "bar": 2}') as row(foo bigint, "BAR" int)).BAR)",
      INTEGER());
}

TEST_F(ExpressionParserTest, intervalDayTime) {
  auto parser = makeParser();

  auto test = [&](std::string_view sql, int64_t expectedSeconds) {
    SCOPED_TRACE(sql);
    auto expr = parser.parseExpression(sql);

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
  auto parser = makeParser();

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
  auto parser = makeParser();

  auto test = [&](std::string_view sql, int64_t expected) {
    auto expr = parser.parseExpression(sql);

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
  auto parser = makeParser();

  auto test = [&](std::string_view sql, double expected) {
    SCOPED_TRACE(sql);
    auto expr = parser.parseExpression(sql);

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
  auto parser = makeParser();

  auto test = [&](std::string_view sql, const TypePtr& expectedType) {
    SCOPED_TRACE(sql);
    auto expr = parser.parseExpression(sql);

    VELOX_ASSERT_EQ_TYPES(expr->type(), expectedType);
  };

  test("TIMESTAMP '2020-01-01'", TIMESTAMP());
  test("TIMESTAMP '2020-01-01 00:00:00'", TIMESTAMP());
  test("TIMESTAMP '2020-01-01 00:00:00.000'", TIMESTAMP());
  test(
      "TIMESTAMP '2020-01-01 00:00 America/Los_Angeles'",
      TIMESTAMP_WITH_TIME_ZONE());

  VELOX_ASSERT_THROW(
      parser.parseExpression("TIMESTAMP 'foo'"),
      "Not a valid timestamp literal");
}

TEST_F(ExpressionParserTest, atTimeZone) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();
  testSelect(
      "SELECT from_unixtime(1700000000, 'UTC') AT TIME ZONE 'America/New_York'",
      matcher);
  testSelect(
      "SELECT date_format(date_trunc('hour', from_unixtime(1700000000, 'UTC') AT TIME ZONE 'GMT'), '%Y-%m-%d+%H:00')",
      matcher);
}

TEST_F(ExpressionParserTest, nullif) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();

  testSelect("SELECT NULLIF(1, 2)", matcher);
  testSelect("SELECT nullif(1, 1)", matcher);
  testSelect("SELECT NULLIF('foo', 'bar')", matcher);
}

TEST_F(ExpressionParserTest, null) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();
  testSelect("SELECT 1 is null", matcher);
  testSelect("SELECT 1 IS NULL", matcher);

  testSelect("SELECT 1 is not null", matcher);
  testSelect("SELECT 1 IS NOT NULL", matcher);
}

TEST_F(ExpressionParserTest, unaryArithmetic) {
  lp::ProjectNodePtr project;
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project(
      [&](const auto& node) {
        project = std::dynamic_pointer_cast<const lp::ProjectNode>(node);
      });

  testSelect("SELECT -1", matcher);
  ASSERT_EQ(project->expressions().size(), 1);
  ASSERT_EQ(project->expressionAt(0)->toString(), "negate(1)");

  testSelect("SELECT +1", matcher);
  ASSERT_EQ(project->expressions().size(), 1);
  ASSERT_EQ(project->expressionAt(0)->toString(), "1");
}

TEST_F(ExpressionParserTest, distinctFrom) {
  lp::ProjectNodePtr project;
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project(
      [&](const auto& node) {
        project = std::dynamic_pointer_cast<const lp::ProjectNode>(node);
      });

  testSelect("SELECT 1 is distinct from 2", matcher);
  ASSERT_EQ(project->expressions().size(), 1);
  ASSERT_EQ(project->expressionAt(0)->toString(), "distinct_from(1, 2)");

  testSelect("SELECT 1 is not distinct from 2", matcher);
  ASSERT_EQ(project->expressions().size(), 1);
  ASSERT_EQ(project->expressionAt(0)->toString(), "not(distinct_from(1, 2))");
}

TEST_F(ExpressionParserTest, ifClause) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();
    testSelect("SELECT if (1 > 2, 100)", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
    testSelect(
        "SELECT if (n_nationkey between 10 and 13, 'foo') FROM nation",
        matcher);
  }
}

TEST_F(ExpressionParserTest, switch) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();

  testSelect(
      "SELECT case when n_nationkey > 2 then 100 when n_name like 'A%' then 200 end FROM nation",
      matcher);
  testSelect(
      "SELECT case when n_nationkey > 2 then 100 when n_name like 'A%' then 200 else 300 end FROM nation",
      matcher);

  testSelect(
      "SELECT case n_nationkey when 1 then 100 when 2 then 200 end FROM nation",
      matcher);

  testSelect(
      "SELECT case n_nationkey when 1 then 100 when 2 then 200 else 300 end FROM nation",
      matcher);
}

TEST_F(ExpressionParserTest, in) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();
    testSelect("SELECT 1 in (2,3,4)", matcher);
    testSelect("SELECT 1 IN (2,3,4)", matcher);

    testSelect("SELECT 1 not in (2,3,4)", matcher);
    testSelect("SELECT 1 NOT IN (2,3,4)", matcher);
  }

  // Subquery.
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().filter();
    testSelect(
        "SELECT * FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name like 'A%')",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
    testSelect(
        "SELECT n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name like 'A%') FROM nation",
        matcher);
  }

  // Coercions.
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
    testSelect("SELECT n_nationkey in (1, 2, 3) FROM nation", matcher);
  }
}

TEST_F(ExpressionParserTest, coalesce) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSelect("SELECT coalesce(n_name, 'foo') FROM nation", matcher);
  testSelect("SELECT COALESCE(n_name, 'foo') FROM nation", matcher);

  // Coercions.
  testSelect("SELECT coalesce(n_regionkey, 1) FROM nation", matcher);
}

TEST_F(ExpressionParserTest, concat) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSelect("SELECT n_name || n_comment FROM nation", matcher);
}

TEST_F(ExpressionParserTest, position) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSelect("SELECT POSITION('A' IN n_name) FROM nation", matcher);
  testSelect("SELECT POSITION(n_comment IN n_name) FROM nation", matcher);
}

TEST_F(ExpressionParserTest, subscript) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSelect("SELECT array[1, 2, 3][1] FROM nation", matcher);
  testSelect("SELECT row(1,2)[2] FROM nation", matcher);
}

TEST_F(ExpressionParserTest, dereference) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder()
                     .values()
                     .unnest()
                     .project()
                     .project();

  testSelect("SELECT t.x FROM UNNEST(array[1, 2, 3]) as t(x)", matcher);

  testSelect("SELECT x FROM UNNEST(array[1, 2, 3]) as t(x)", matcher);

  testSelect(
      "SELECT t.x.a FROM UNNEST(array[cast(row(1, 2) as row(a int, b int))]) as t(x)",
      matcher);

  testSelect(
      "SELECT x.a FROM UNNEST(array[cast(row(1, 2) as row(a int, b int))]) as t(x)",
      matcher);

  testSelect("SELECT t.X FROM UNNEST(array[1, 2, 3]) as t(x)", matcher);
  testSelect("SELECT T.X FROM UNNEST(array[1, 2, 3]) as t(x)", matcher);
  testSelect("SELECT t.x FROM UNNEST(array[1, 2, 3]) as t(X)", matcher);

  testSelect(
      "SELECT t.x.field0 FROM UNNEST(array[row(1, 2)]) as t(x)", matcher);
  testSelect("SELECT x.field0 FROM UNNEST(array[row(1, 2)]) as t(x)", matcher);
  testSelect(
      "SELECT x.field000 FROM UNNEST(array[row(1, 2)]) as t(x)", matcher);

  testSelect("SELECT x.field1 FROM UNNEST(array[row(1, 2)]) as t(x)", matcher);
  testSelect("SELECT x.field01 FROM UNNEST(array[row(1, 2)]) as t(x)", matcher);

  VELOX_ASSERT_THROW(
      parseSql("SELECT x.field2 FROM UNNEST(array[row(1, 2)]) as t(x)"),
      "Invalid legacy field name: field2");

  VELOX_ASSERT_THROW(
      parseSql("SELECT cast(row(1, 2) as row(a int, b int)).field0"),
      "Cannot access named field using legacy field name: field0 vs. a");
}

TEST_F(ExpressionParserTest, row) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSelect("SELECT row(n_regionkey, n_name) FROM nation", matcher);
}

TEST_F(ExpressionParserTest, lambda) {
  ASSERT_NO_THROW(parseSqlExpression("filter(array[1,2,3], x -> x > 1)"));
  ASSERT_NO_THROW(parseSqlExpression("FILTER(array[1,2,3], x -> x > 1)"));

  ASSERT_NO_THROW(parseSqlExpression("filter(array[], x -> true)"));

  ASSERT_NO_THROW(
      parseSqlExpression("reduce(array[], map(), (s, x) -> s, s -> 123)"));

  ASSERT_NO_THROW(parseSqlExpression(
      "reduce(array[], map(), (s, x) -> map(array[1], array[2]), s -> 123)"));
}

} // namespace
} // namespace axiom::sql::presto::test
