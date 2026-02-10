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

#include "axiom/sql/presto/PrestoParser.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/sql/presto/PrestoParseError.h"
#include "axiom/sql/presto/tests/LogicalPlanMatcher.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace axiom::sql::presto {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class PrestoParserTest : public testing::Test {
 public:
  static constexpr const char* kTpchConnectorId = "tpch";
  static constexpr const char* kTinySchema = "tiny";
  static constexpr const char* kTestConnectorId = "test";

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    // Register TPC-H connector.
    {
      auto emptyConfig = std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>{});

      facebook::velox::connector::tpch::TpchConnectorFactory
          tpchConnectorFactory;
      auto tpchConnector =
          tpchConnectorFactory.newConnector(kTpchConnectorId, emptyConfig);
      facebook::velox::connector::registerConnector(tpchConnector);

      facebook::axiom::connector::ConnectorMetadata::registerMetadata(
          kTpchConnectorId,
          std::make_shared<
              facebook::axiom::connector::tpch::TpchConnectorMetadata>(
              dynamic_cast<facebook::velox::connector::tpch::TpchConnector*>(
                  tpchConnector.get())));
    }

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  static void TearDownTestCase() {
    facebook::axiom::connector::ConnectorMetadata::unregisterMetadata(
        kTpchConnectorId);
    facebook::velox::connector::unregisterConnector(kTpchConnectorId);
  }

  void SetUp() override {
    // Register Test connector.
    testConnector_ =
        std::make_shared<facebook::axiom::connector::TestConnector>(
            kTestConnectorId);
    facebook::velox::connector::registerConnector(testConnector_);
  }

  void TearDown() override {
    facebook::velox::connector::unregisterConnector(kTestConnectorId);
    testConnector_.reset();
  }

  void testExplain(
      std::string_view sql,
      lp::test::LogicalPlanMatcherBuilder& matcher) {
    SCOPED_TRACE(sql);
    auto parser = makeParser();

    auto statement = parser.parse(sql);
    ASSERT_TRUE(statement->isExplain());

    auto* explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(
        explainStatement->type() == ExplainStatement::Type::kExecutable);

    if (explainStatement->statement()->isSelect()) {
      auto* selectStatement =
          explainStatement->statement()->as<SelectStatement>();

      auto logicalPlan = selectStatement->plan();
      ASSERT_TRUE(matcher.build()->match(logicalPlan))
          << lp::PlanPrinter::toText(*logicalPlan);
    } else if (explainStatement->statement()->isInsert()) {
      auto* insertStatement =
          explainStatement->statement()->as<InsertStatement>();

      auto logicalPlan = insertStatement->plan();
      ASSERT_TRUE(matcher.build()->match(logicalPlan))
          << lp::PlanPrinter::toText(*logicalPlan);
    } else {
      FAIL() << "Unexpected statement: "
             << explainStatement->statement()->kindName();
    }
  }

  void testSql(
      std::string_view sql,
      lp::test::LogicalPlanMatcherBuilder& matcher,
      const std::unordered_set<std::string>& views = {}) {
    SCOPED_TRACE(sql);
    auto parser = makeParser();

    auto statement = parser.parse(sql, true);
    ASSERT_TRUE(statement->isSelect());

    auto* selectStatement = statement->as<SelectStatement>();

    auto logicalPlan = selectStatement->plan();
    ASSERT_TRUE(matcher.build()->match(logicalPlan))
        << lp::PlanPrinter::toText(*logicalPlan);

    ASSERT_EQ(views.size(), selectStatement->views().size());

    for (const auto& view : views) {
      ASSERT_TRUE(selectStatement->views().contains({kTpchConnectorId, view}))
          << "Missing view: " << view;
    }
  }

  SqlStatementPtr parseSql(std::string_view sql) {
    SCOPED_TRACE(sql);
    auto parser = makeParser();

    return parser.parse(sql, true);
  }

  void testSqlExpression(std::string_view sql) {
    SCOPED_TRACE(sql);
    auto parser = makeParser();

    ASSERT_NO_THROW(parser.parseExpression(sql, true));
  }

  void testInsertSql(
      std::string_view sql,
      lp::test::LogicalPlanMatcherBuilder& matcher) {
    SCOPED_TRACE(sql);
    auto parser = makeParser();

    auto statement = parser.parse(sql);
    ASSERT_TRUE(statement->isInsert());

    auto insertStatement = statement->as<InsertStatement>();

    auto logicalPlan = insertStatement->plan();
    ASSERT_TRUE(matcher.build()->match(logicalPlan))
        << lp::PlanPrinter::toText(*logicalPlan);
  }

  void testCtasSql(
      std::string_view sql,
      const std::string& tableName,
      const RowTypePtr& tableSchema,
      lp::test::LogicalPlanMatcherBuilder& matcher,
      const std::unordered_map<std::string, std::string>& properties = {}) {
    SCOPED_TRACE(sql);
    auto parser = makeParser();

    auto statement = parser.parse(sql);
    ASSERT_TRUE(statement->isCreateTableAsSelect());

    auto ctasStatement = statement->as<CreateTableAsSelectStatement>();

    ASSERT_EQ(ctasStatement->tableName(), tableName);
    ASSERT_TRUE(*ctasStatement->tableSchema() == *tableSchema);

    auto logicalPlan = ctasStatement->plan();
    ASSERT_TRUE(matcher.build()->match(logicalPlan))
        << lp::PlanPrinter::toText(*logicalPlan);

    const auto& actualProperties = ctasStatement->properties();
    ASSERT_EQ(properties.size(), actualProperties.size());

    for (const auto& [key, value] : properties) {
      ASSERT_TRUE(actualProperties.contains(key));
      ASSERT_EQ(lp::ExprPrinter::toText(*actualProperties.at(key)), value);
    }
  }

  void testCreateSql(
      std::string_view sql,
      const std::string& tableName,
      const RowTypePtr& tableSchema,
      const std::unordered_map<std::string, std::string>& properties = {},
      const std::vector<CreateTableStatement::Constraint>& constraints = {}) {
    SCOPED_TRACE(sql);
    auto parser = makeParser();

    auto statement = parser.parse(sql);
    ASSERT_TRUE(statement->isCreateTable());

    auto* createTable = statement->as<CreateTableStatement>();

    ASSERT_EQ(createTable->tableName(), tableName);
    ASSERT_TRUE(*createTable->tableSchema() == *tableSchema);

    const auto& actualProperties = createTable->properties();
    ASSERT_EQ(properties.size(), actualProperties.size());
    for (const auto& [key, value] : properties) {
      ASSERT_TRUE(actualProperties.contains(key));
      ASSERT_EQ(lp::ExprPrinter::toText(*actualProperties.at(key)), value);
    }

    const auto& actualConstraints = createTable->constraints();
    ASSERT_EQ(constraints.size(), actualConstraints.size());
    for (size_t i = 0; i < constraints.size(); ++i) {
      ASSERT_EQ(constraints[i].name, actualConstraints[i].name);
      ASSERT_EQ(constraints[i].type, actualConstraints[i].type);
      ASSERT_EQ(constraints[i].columns, actualConstraints[i].columns);
    }
  }

  void testInvalidSql(std::string_view sql, const std::string& errorMessage) {
    SCOPED_TRACE(sql);
    auto parser = makeParser();
    VELOX_ASSERT_THROW(parser.parse(sql), errorMessage);
  }

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

  PrestoParser makeParser() {
    return PrestoParser(defaultConnectorId_, defaultSchema_);
  }

  std::string defaultConnectorId_ = kTpchConnectorId;
  std::optional<std::string> defaultSchema_ = kTinySchema;

  std::shared_ptr<facebook::axiom::connector::TestConnector> testConnector_;
};

TEST_F(PrestoParserTest, parseMultiple) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1; select 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithTrailingSemicolon) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1; select 2;");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithWhitespace) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("  select 1  ;  \n  select 2  ;  \n  select 3  ");
  ASSERT_EQ(3, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
  ASSERT_TRUE(statements[2]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithComments) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "-- First query\nselect 1;\n-- Second query\nselect 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithBlockComments) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("/* First */ select 1; /* Second */ select 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithSingleQuotes) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("select 'hello; world'; select 'foo''bar; baz'");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithDoubleQuotes) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select 1 as \"col;name\"; select 2 as \"foo\"\"bar; baz\"");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleMixedStatements) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select * from nation; "
      "select n_name from nation where n_nationkey = 1; "
      "select 42");
  ASSERT_EQ(3, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
  ASSERT_TRUE(statements[2]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleSingleStatement) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1");
  ASSERT_EQ(1, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleEmptyStatements) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(";;;");
  ASSERT_EQ(0, statements.size());
}

TEST_F(PrestoParserTest, parseMultipleComplexQuery) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select n_nationkey, n_name "
      "from nation "
      "where n_regionkey = 1 "
      "order by n_name; "
      "select count(*) from nation");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, unnest) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().unnest();
    testSql("SELECT * FROM unnest(array[1, 2, 3])", matcher);

    testSql(
        "SELECT * FROM unnest(array[1, 2, 3], array[4, 5]) with ordinality",
        matcher);

    testSql(
        "SELECT * FROM unnest(map(array[1, 2, 3], array[10, 20, 30]))",
        matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().values().unnest().project();
    testSql("SELECT * FROM unnest(array[1, 2, 3]) as t(x)", matcher);

    testSql(
        "SELECT * FROM unnest(array[1, 2, 3], array[4, 5]) with ordinality as t(x, y)",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().unnest();
    testSql(
        "SELECT * FROM nation, unnest(array[n_nationkey, n_regionkey])",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().unnest();

    testSql(
        "SELECT * FROM nation, unnest(array[n_nationkey, n_regionkey]) as t(x)",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .values()
                       .project()
                       .unnest()
                       .project();

    testSql(
        "WITH a AS (SELECT array[1,2,3] as x) SELECT t.x + 1 FROM a, unnest(A.x) as T(X)",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().unnest();

    testSql(
        "SELECT * FROM (nation cross join unnest(array[1,2,3]) as t(x))",
        matcher);
  }
}

TEST_F(PrestoParserTest, syntaxErrors) {
  auto parser = makeParser();
  EXPECT_THAT(
      [&]() { parser.parse("SELECT * FROM"); },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 1:13: mismatched input '<EOF>'")));

  EXPECT_THAT(
      [&]() {
        parser.parse(
            "SELECT * FROM nation\n"
            "WHERE");
      },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 2:5: mismatched input '<EOF>'")));

  EXPECT_THAT(
      [&]() { parser.parse("SELECT * FROM (VALUES 1, 2, 3)) blah..."); },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 1:30: mismatched input ')' expecting <EOF>")));

  EXPECT_THAT(
      [&]() { parser.parse("CREATE TABLE t (price DECIMAL('abc', 'xyz'))"); },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 1:30: mismatched input ''abc''")));
}

TEST_F(PrestoParserTest, types) {
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

TEST_F(PrestoParserTest, intervalDayTime) {
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

TEST_F(PrestoParserTest, decimal) {
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

TEST_F(PrestoParserTest, intervalYearMonth) {
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

TEST_F(PrestoParserTest, doubleLiteral) {
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

TEST_F(PrestoParserTest, timestampLiteral) {
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

TEST_F(PrestoParserTest, atTimeZone) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();
  testSql(
      "SELECT from_unixtime(1700000000, 'UTC') AT TIME ZONE 'America/New_York'",
      matcher);
  testSql(
      "SELECT date_format(date_trunc('hour', from_unixtime(1700000000, 'UTC') AT TIME ZONE 'GMT'), '%Y-%m-%d+%H:00')",
      matcher);
}

TEST_F(PrestoParserTest, nullif) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();

  testSql("SELECT NULLIF(1, 2)", matcher);
  testSql("SELECT nullif(1, 1)", matcher);
  testSql("SELECT NULLIF('foo', 'bar')", matcher);
}

TEST_F(PrestoParserTest, null) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();
  testSql("SELECT 1 is null", matcher);
  testSql("SELECT 1 IS NULL", matcher);

  testSql("SELECT 1 is not null", matcher);
  testSql("SELECT 1 IS NOT NULL", matcher);
}

TEST_F(PrestoParserTest, unaryArithmetic) {
  lp::ProjectNodePtr project;
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project(
      [&](const auto& node) {
        project = std::dynamic_pointer_cast<const lp::ProjectNode>(node);
      });

  testSql("SELECT -1", matcher);
  ASSERT_EQ(project->expressions().size(), 1);
  ASSERT_EQ(project->expressionAt(0)->toString(), "negate(1)");

  testSql("SELECT +1", matcher);
  ASSERT_EQ(project->expressions().size(), 1);
  ASSERT_EQ(project->expressionAt(0)->toString(), "1");
}

TEST_F(PrestoParserTest, distinctFrom) {
  lp::ProjectNodePtr project;
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project(
      [&](const auto& node) {
        project = std::dynamic_pointer_cast<const lp::ProjectNode>(node);
      });

  testSql("SELECT 1 is distinct from 2", matcher);
  ASSERT_EQ(project->expressions().size(), 1);
  ASSERT_EQ(project->expressionAt(0)->toString(), "distinct_from(1, 2)");

  testSql("SELECT 1 is not distinct from 2", matcher);
  ASSERT_EQ(project->expressions().size(), 1);
  ASSERT_EQ(project->expressionAt(0)->toString(), "not(distinct_from(1, 2))");
}

TEST_F(PrestoParserTest, ifClause) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();
    testSql("SELECT if (1 > 2, 100)", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
    testSql(
        "SELECT if (n_nationkey between 10 and 13, 'foo') FROM nation",
        matcher);
  }
}

TEST_F(PrestoParserTest, switch) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();

  testSql(
      "SELECT case when n_nationkey > 2 then 100 when n_name like 'A%' then 200 end FROM nation",
      matcher);
  testSql(
      "SELECT case when n_nationkey > 2 then 100 when n_name like 'A%' then 200 else 300 end FROM nation",
      matcher);

  testSql(
      "SELECT case n_nationkey when 1 then 100 when 2 then 200 end FROM nation",
      matcher);

  testSql(
      "SELECT case n_nationkey when 1 then 100 when 2 then 200 else 300 end FROM nation",
      matcher);
}

TEST_F(PrestoParserTest, in) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();
    testSql("SELECT 1 in (2,3,4)", matcher);
    testSql("SELECT 1 IN (2,3,4)", matcher);

    testSql("SELECT 1 not in (2,3,4)", matcher);
    testSql("SELECT 1 NOT IN (2,3,4)", matcher);
  }

  // Subquery.
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().filter();
    testSql(
        "SELECT * FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name like 'A%')",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
    testSql(
        "SELECT n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name like 'A%') FROM nation",
        matcher);
  }

  // Coercions.
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
    testSql("SELECT n_nationkey in (1, 2, 3) FROM nation", matcher);
  }
}

TEST_F(PrestoParserTest, coalesce) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSql("SELECT coalesce(n_name, 'foo') FROM nation", matcher);
  testSql("SELECT COALESCE(n_name, 'foo') FROM nation", matcher);

  // Coercions.
  testSql("SELECT coalesce(n_regionkey, 1) FROM nation", matcher);
}

TEST_F(PrestoParserTest, concat) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSql("SELECT n_name || n_comment FROM nation", matcher);
}

TEST_F(PrestoParserTest, position) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSql("SELECT POSITION('A' IN n_name) FROM nation", matcher);
  testSql("SELECT POSITION(n_comment IN n_name) FROM nation", matcher);
}

TEST_F(PrestoParserTest, subscript) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSql("SELECT array[1, 2, 3][1] FROM nation", matcher);
  testSql("SELECT row(1,2)[2] FROM nation", matcher);
}

TEST_F(PrestoParserTest, dereference) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder()
                     .values()
                     .unnest()
                     .project()
                     .project();

  testSql("SELECT t.x FROM UNNEST(array[1, 2, 3]) as t(x)", matcher);

  testSql("SELECT x FROM UNNEST(array[1, 2, 3]) as t(x)", matcher);

  testSql(
      "SELECT t.x.a FROM UNNEST(array[cast(row(1, 2) as row(a int, b int))]) as t(x)",
      matcher);

  testSql(
      "SELECT x.a FROM UNNEST(array[cast(row(1, 2) as row(a int, b int))]) as t(x)",
      matcher);

  testSql("SELECT t.X FROM UNNEST(array[1, 2, 3]) as t(x)", matcher);
  testSql("SELECT T.X FROM UNNEST(array[1, 2, 3]) as t(x)", matcher);
  testSql("SELECT t.x FROM UNNEST(array[1, 2, 3]) as t(X)", matcher);

  testSql("SELECT t.x.field0 FROM UNNEST(array[row(1, 2)]) as t(x)", matcher);
  testSql("SELECT x.field0 FROM UNNEST(array[row(1, 2)]) as t(x)", matcher);
  testSql("SELECT x.field000 FROM UNNEST(array[row(1, 2)]) as t(x)", matcher);

  testSql("SELECT x.field1 FROM UNNEST(array[row(1, 2)]) as t(x)", matcher);
  testSql("SELECT x.field01 FROM UNNEST(array[row(1, 2)]) as t(x)", matcher);

  VELOX_ASSERT_THROW(
      parseSql("SELECT x.field2 FROM UNNEST(array[row(1, 2)]) as t(x)"),
      "Invalid legacy field name: field2");

  VELOX_ASSERT_THROW(
      parseSql("SELECT cast(row(1, 2) as row(a int, b int)).field0"),
      "Cannot access named field using legacy field name: field0 vs. a");
}

TEST_F(PrestoParserTest, row) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSql("SELECT row(n_regionkey, n_name) FROM nation", matcher);
}

TEST_F(PrestoParserTest, selectStar) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan();
    testSql("SELECT * FROM nation", matcher);
    testSql("(SELECT * FROM nation)", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
    testSql("SELECT *, * FROM nation", matcher);
    testSql("SELECT *, n_nationkey FROM nation", matcher);
    testSql("SELECT nation.* FROM nation", matcher);
    testSql("SELECT nation.*, n_nationkey + 1 FROM nation", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder()
            .tableScan()
            .join(lp::test::LogicalPlanMatcherBuilder().tableScan().build())
            .filter()
            .project();
    testSql(
        "SELECT nation.*, r_regionkey + 1 FROM nation, region WHERE n_regionkey = r_regionkey",
        matcher);
  }

  VELOX_ASSERT_THROW(parseSql("SELECT r.* FROM region"), "Alias not found: r");
}

TEST_F(PrestoParserTest, hiddenColumns) {
  defaultConnectorId_ = kTestConnectorId;
  defaultSchema_ = std::nullopt;

  testConnector_->addTable(
      "t", ROW({"a", "b"}, INTEGER()), ROW({"$c", "$d"}, VARCHAR()));

  auto verifyOutput = [&](const std::string& sql,
                          std::initializer_list<std::string> expectedNames) {
    lp::LogicalPlanNodePtr outputNode;
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project(
        [&](const auto& node) { outputNode = node; });

    testSql(sql, matcher);
    ASSERT_THAT(
        outputNode->outputType()->names(),
        ::testing::Pointwise(::testing::Eq(), expectedNames));
  };

  verifyOutput("SELECT * FROM t", {"a", "b"});
  verifyOutput("SELECT \"$c\", * FROM t", {"$c", "a", "b"});
  verifyOutput("SELECT a, \"$c\" FROM t", {"a", "$c"});

  verifyOutput("SELECT *, a FROM t", {"a", "b", "a_0"});
  verifyOutput("SELECT *, * FROM t", {"a", "b", "a_0", "b_1"});
}

TEST_F(PrestoParserTest, mixedCaseColumnNames) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSql("SELECT N_NAME, n_ReGiOnKeY FROM nation", matcher);
  testSql("SELECT nation.n_name FROM nation", matcher);
  testSql("SELECT NATION.n_name FROM nation", matcher);
  testSql("SELECT \"NATION\".n_name FROM nation", matcher);
}

TEST_F(PrestoParserTest, with) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();
  testSql("WITH a as (SELECT 1 as x) SELECT * FROM a", matcher);
  testSql("WITH a as (SELECT 1 as x) SELECT * FROM A", matcher);
  testSql("WITH A as (SELECT 1 as x) SELECT * FROM a", matcher);

  matcher.project();
  testSql("WITH a as (SELECT 1 as x) SELECT A.x FROM a", matcher);
}

TEST_F(PrestoParserTest, countStar) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate();

    testSql("SELECT count(*) FROM nation", matcher);
    testSql("SELECT count(1) FROM nation", matcher);

    testSql("SELECT count(1) \"count\" FROM nation", matcher);
    testSql("SELECT count(1) AS \"count\" FROM nation", matcher);
  }

  {
    // Global aggregation with HAVING clause.
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().filter();
    testSql("SELECT count(*) FROM nation HAVING count(*) > 100", matcher);
  }
}

TEST_F(PrestoParserTest, aggregateCoercions) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate();

  testSql("SELECT corr(n_nationkey, 1.2) FROM nation", matcher);
}

TEST_F(PrestoParserTest, simpleGroupBy) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate();

    testSql("SELECT n_name, count(1) FROM nation GROUP BY 1", matcher);
    testSql("SELECT n_name, count(1) FROM nation GROUP BY n_name", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().project();
    testSql(
        "SELECT count(1) FROM nation GROUP BY n_name, n_regionkey", matcher);
  }
}

TEST_F(PrestoParserTest, distinct) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().project().aggregate();
    testSql("SELECT DISTINCT n_regionkey FROM nation", matcher);
    testSql("SELECT DISTINCT n_regionkey, length(n_name) FROM nation", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .tableScan()
                       .aggregate()
                       .project()
                       .aggregate();
    testSql(
        "SELECT DISTINCT count(1) FROM nation GROUP BY n_regionkey", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate();
    testSql("SELECT DISTINCT * FROM nation", matcher);
  }
}

TEST_F(PrestoParserTest, groupingKeyExpr) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().project();

    testSql(
        "SELECT n_name, count(1), length(n_name) FROM nation GROUP BY 1",
        matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate();
    testSql(
        "SELECT substr(n_name, 1, 2), count(1) FROM nation GROUP BY 1",
        matcher);

    testSql(
        "SELECT r_regionkey IN (SELECT n_regionkey FROM nation), count(1) "
        "FROM region GROUP BY 1",
        matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().project();
    testSql(
        "SELECT count(1) FROM nation GROUP BY substr(n_name, 1, 2)", matcher);
  }
}

TEST_F(PrestoParserTest, having) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .tableScan()
                       .aggregate()
                       .filter()
                       .project();

    testSql(
        "SELECT n_name FROM nation GROUP BY 1 HAVING sum(length(n_comment)) > 10",
        matcher);
  }
}

TEST_F(PrestoParserTest, scalarOverAgg) {
  auto matcher =
      lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().project();

  testSql(
      "SELECT sum(n_regionkey) + count(1), avg(length(n_name)) * 0.3 "
      "FROM nation",
      matcher);

  testSql(
      "SELECT n_regionkey, sum(n_nationkey) + count(1), avg(length(n_name)) * 0.3 "
      "FROM nation "
      "GROUP BY 1",
      matcher);
}

TEST_F(PrestoParserTest, aggregateOptions) {
  lp::AggregateNodePtr agg;
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate(
      [&](const auto& node) {
        agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
      });

  testSql("SELECT array_agg(distinct n_regionkey) FROM nation", matcher);
  ASSERT_TRUE(agg != nullptr);
  ASSERT_EQ(1, agg->aggregates().size());
  ASSERT_TRUE(agg->aggregateAt(0)->isDistinct());
  ASSERT_TRUE(agg->aggregateAt(0)->filter() == nullptr);
  ASSERT_EQ(0, agg->aggregateAt(0)->ordering().size());

  testSql(
      "SELECT array_agg(n_nationkey ORDER BY n_regionkey) FROM nation",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  ASSERT_EQ(1, agg->aggregates().size());
  ASSERT_FALSE(agg->aggregateAt(0)->isDistinct());
  ASSERT_TRUE(agg->aggregateAt(0)->filter() == nullptr);
  ASSERT_EQ(1, agg->aggregateAt(0)->ordering().size());

  testSql(
      "SELECT array_agg(n_nationkey) FILTER (WHERE n_regionkey = 1) FROM nation",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  ASSERT_EQ(1, agg->aggregates().size());
  ASSERT_FALSE(agg->aggregateAt(0)->isDistinct());
  ASSERT_FALSE(agg->aggregateAt(0)->filter() == nullptr);
  ASSERT_EQ(0, agg->aggregateAt(0)->ordering().size());

  testSql(
      "SELECT array_agg(distinct n_regionkey) FILTER (WHERE n_name like 'A%') FROM nation",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  ASSERT_EQ(1, agg->aggregates().size());
  ASSERT_TRUE(agg->aggregateAt(0)->isDistinct());
  ASSERT_FALSE(agg->aggregateAt(0)->filter() == nullptr);
  ASSERT_EQ(0, agg->aggregateAt(0)->ordering().size());

  testSql(
      "SELECT array_agg(n_regionkey ORDER BY n_name) FILTER (WHERE n_name like 'A%') FROM nation",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  ASSERT_EQ(1, agg->aggregates().size());
  ASSERT_FALSE(agg->aggregateAt(0)->isDistinct());
  ASSERT_FALSE(agg->aggregateAt(0)->filter() == nullptr);
  ASSERT_EQ(1, agg->aggregateAt(0)->ordering().size());
}

TEST_F(PrestoParserTest, orderBy) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .tableScan()
                       .aggregate()
                       .sort()
                       .project();

    testSql(
        "select n_regionkey from nation group by 1 order by count(1)", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().sort();

    testSql(
        "select n_regionkey, count(1) from nation group by 1 order by count(1)",
        matcher);

    testSql(
        "select n_regionkey, count(1) from nation group by 1 order by 2",
        matcher);

    testSql(
        "select n_regionkey, count(1) as c from nation group by 1 order by c",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .tableScan()
                       .aggregate()
                       .project()
                       .sort();

    testSql(
        "select n_regionkey, count(1) * 2 from nation group by 1 order by 2",
        matcher);

    testSql(
        "select n_regionkey, count(1) * 2 from nation group by 1 order by count(1) * 2",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .tableScan()
                       .aggregate()
                       .project()
                       .sort()
                       .project();
    testSql(
        "select n_regionkey, count(1) * 2 from nation group by 1 order by count(1) * 3",
        matcher);
  }
}

TEST_F(PrestoParserTest, join) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().join(
        lp::test::LogicalPlanMatcherBuilder().tableScan().build());

    testSql("SELECT * FROM nation, region", matcher);

    testSql(
        "SELECT * FROM nation LEFT JOIN region ON n_regionkey = r_regionkey",
        matcher);

    testSql(
        "SELECT * FROM nation RIGHT JOIN region ON nation.n_regionkey = region.r_regionkey",
        matcher);

    testSql(
        "SELECT * FROM nation n LEFT JOIN region r ON n.n_regionkey = r.r_regionkey",
        matcher);

    testSql(
        "SELECT * FROM nation FULL OUTER JOIN region ON n_regionkey = r_regionkey",
        matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder()
            .tableScan()
            .join(lp::test::LogicalPlanMatcherBuilder().tableScan().build())
            .filter();

    testSql(
        "SELECT * FROM nation, region WHERE n_regionkey = r_regionkey",
        matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder()
            .tableScan()
            .join(lp::test::LogicalPlanMatcherBuilder().tableScan().build())
            .filter()
            .project();

    testSql(
        "SELECT n_name, r_name FROM nation, region WHERE n_regionkey = r_regionkey",
        matcher);
  }
}

TEST_F(PrestoParserTest, unionAll) {
  auto matcher =
      lp::test::LogicalPlanMatcherBuilder().tableScan().project().setOperation(
          lp::SetOperation::kUnionAll,
          lp::test::LogicalPlanMatcherBuilder().tableScan().project().build());

  testSql(
      "SELECT n_name FROM nation UNION ALL SELECT r_name FROM region", matcher);
}

TEST_F(PrestoParserTest, union) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder()
                     .tableScan()
                     .project()
                     .setOperation(
                         lp::SetOperation::kUnionAll,
                         lp::test::LogicalPlanMatcherBuilder()
                             .tableScan()
                             .project()
                             .build())
                     .aggregate();

  testSql("SELECT n_name FROM nation UNION SELECT r_name FROM region", matcher);
}

TEST_F(PrestoParserTest, except) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder()
                     .tableScan()
                     .project()
                     .setOperation(
                         lp::SetOperation::kExcept,
                         lp::test::LogicalPlanMatcherBuilder()
                             .tableScan()
                             .project()
                             .build())
                     .aggregate();

  testSql(
      "SELECT n_name FROM nation EXCEPT SELECT r_name FROM region", matcher);
}

TEST_F(PrestoParserTest, intersect) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder()
                     .tableScan()
                     .project()
                     .setOperation(
                         lp::SetOperation::kIntersect,
                         lp::test::LogicalPlanMatcherBuilder()
                             .tableScan()
                             .project()
                             .build())
                     .aggregate();

  testSql(
      "SELECT n_name FROM nation INTERSECT SELECT r_name FROM region", matcher);
}

TEST_F(PrestoParserTest, exists) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().filter();

    testSql(
        "SELECT * FROM region WHERE exists (SELECT * from nation WHERE n_name like 'A%' and r_regionkey = n_regionkey)",
        matcher);

    testSql(
        "SELECT * FROM region WHERE not exists (SELECT * from nation WHERE n_name like 'A%' and r_regionkey = n_regionkey)",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();

    testSql(
        "SELECT EXISTS (SELECT * from nation WHERE n_regionkey = r_regionkey) FROM region",
        matcher);
  }
}

TEST_F(PrestoParserTest, lambda) {
  testSqlExpression("filter(array[1,2,3], x -> x > 1)");
  testSqlExpression("FILTER(array[1,2,3], x -> x > 1)");

  testSqlExpression("filter(array[], x -> true)");

  testSqlExpression("reduce(array[], map(), (s, x) -> s, s -> 123)");

  testSqlExpression(
      "reduce(array[], map(), (s, x) -> map(array[1], array[2]), s -> 123)");
}

TEST_F(PrestoParserTest, specialDateTimeFunctions) {
  testSqlExpression("current_date");
  testSqlExpression("current_timestamp");
  testSqlExpression("localtime");

  testSqlExpression("date_add('day', -1000, current_date)");
  testSqlExpression("date_diff('day', current_date, date '2020-01-01')");

  testSqlExpression("current_date > date '2020-01-01'");
  testSqlExpression("current_date - interval '1' day");

  // TODO D92297380: These are currently unsupported in Velox
  {
    auto parser = makeParser();
    VELOX_ASSERT_THROW(
        parser.parseExpression("current_time"),
        "Unsupported date/time function");
    VELOX_ASSERT_THROW(
        parser.parseExpression("localtimestamp"),
        "Unsupported date/time function");
  }
}

TEST_F(PrestoParserTest, values) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values(
        ROW({"c0", "c1", "c2"}, {INTEGER(), DOUBLE(), VARCHAR()}));

    testSql(
        "SELECT * FROM (VALUES (1, 1.1, 'foo'), (2, null, 'bar'))", matcher);

    testSql(
        "SELECT * FROM (VALUES (1, null, 'foo'), (2, 2.2, 'bar'))", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().values(ROW({"c0"}, {INTEGER()}));
    testSql("SELECT * FROM (VALUES (1), (2), (3), (4))", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values(
        ROW({"c0", "c1"}, {REAL(), INTEGER()}));
    testSql("SELECT * FROM (VALUES (real '1', 1 + 2))", matcher);
  }
}

TEST_F(PrestoParserTest, tablesample) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().sample();

    testSql("SELECT * FROM nation TABLESAMPLE BERNOULLI (10.0)", matcher);
    testSql("SELECT * FROM nation TABLESAMPLE SYSTEM (1.5)", matcher);

    testSql("SELECT * FROM nation TABLESAMPLE BERNOULLI (10)", matcher);
    testSql("SELECT * FROM nation TABLESAMPLE BERNOULLI (1 + 2)", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().sample();

    testSql(
        "SELECT * FROM (SELECT l_orderkey, count(*) FROM lineitem GROUP BY 1) "
        "TABLESAMPLE BERNOULLI (1.5)",
        matcher);
  }
}

TEST_F(PrestoParserTest, everything) {
  auto matcher =
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .join(lp::test::LogicalPlanMatcherBuilder().tableScan().build())
          .filter()
          .aggregate()
          .sort();

  testSql(
      "SELECT r_name, count(*) FROM nation, region "
      "WHERE n_regionkey = r_regionkey "
      "GROUP BY 1 "
      "ORDER BY 2 DESC",
      matcher);
}

TEST_F(PrestoParserTest, explainSelect) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan();
    testExplain("EXPLAIN SELECT * FROM nation", matcher);
  }

  auto parser = makeParser();
  {
    auto statement = parser.parse("EXPLAIN ANALYZE SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_TRUE(explainStatement->isAnalyze());
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE LOGICAL) SELECT * FROM nation", true);
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(explainStatement->type() == ExplainStatement::Type::kLogical);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE GRAPH) SELECT * FROM nation", true);
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(explainStatement->type() == ExplainStatement::Type::kGraph);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE OPTIMIZED) SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(explainStatement->type() == ExplainStatement::Type::kOptimized);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE EXECUTABLE) SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(
        explainStatement->type() == ExplainStatement::Type::kExecutable);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE DISTRIBUTED) SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(
        explainStatement->type() == ExplainStatement::Type::kExecutable);
  }
}

TEST_F(PrestoParserTest, explainShow) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values();
  testExplain("EXPLAIN SHOW CATALOGS", matcher);

  testExplain("EXPLAIN SHOW COLUMNS FROM nation", matcher);

  testExplain("EXPLAIN SHOW FUNCTIONS", matcher);
}

TEST_F(PrestoParserTest, explainInsert) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().tableWrite();
    testExplain("EXPLAIN INSERT INTO region SELECT * FROM region", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().tableWrite();
    testExplain("EXPLAIN INSERT INTO region VALUES (1, 'foo', 'bar')", matcher);
  }
}

TEST_F(PrestoParserTest, showCatalogs) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values();
    testSql("SHOW CATALOGS", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().filter();
    testSql("SHOW CATALOGS LIKE 'tpch'", matcher);
  }
}

TEST_F(PrestoParserTest, describe) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values();
  testSql("DESCRIBE nation", matcher);

  testSql("DESC orders", matcher);

  testSql("SHOW COLUMNS FROM lineitem", matcher);
}

TEST_F(PrestoParserTest, showFunctions) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values();
    testSql("SHOW FUNCTIONS", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().filter();
    testSql("SHOW FUNCTIONS LIKE 'array%'", matcher);
  }
}

TEST_F(PrestoParserTest, insertIntoTable) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().values().project().tableWrite();
    testInsertSql(
        "INSERT INTO nation SELECT 100, 'n-100', 2, 'test comment'", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().tableWrite();
    testInsertSql("INSERT INTO nation SELECT * FROM nation", matcher);
  }

  // Omit n_comment. Expect to be filled with default value.
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().values().project().tableWrite();
    testInsertSql(
        "INSERT INTO nation(n_nationkey, n_name, n_regionkey) SELECT 100, 'n-100', 2",
        matcher);
  }

  // Change the order of columns.
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().values().project().tableWrite();
    testInsertSql(
        "INSERT INTO nation(n_nationkey, n_regionkey, n_name) SELECT 100, 2, 'n-100'",
        matcher);
  }

  // Wrong types.
  {
    auto parser = makeParser();

    VELOX_ASSERT_THROW(
        parser.parse("INSERT INTO nation SELECT 100, 'n-100', 2, 3"),
        "Wrong column type: INTEGER vs. VARCHAR, column 'n_comment' in table 'tiny.nation'");
  }
}

TEST_F(PrestoParserTest, createTableAsSelect) {
  {
    auto nationSchema = facebook::axiom::connector::ConnectorMetadata::metadata(
                            kTpchConnectorId)
                            ->findTable("nation")
                            ->type();

    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().tableWrite();
    testCtasSql(
        "CREATE TABLE t AS SELECT * FROM nation",
        "tiny.t",
        nationSchema,
        matcher);
  }

  auto matcher =
      lp::test::LogicalPlanMatcherBuilder().tableScan().project().tableWrite();

  testCtasSql(
      "CREATE TABLE t AS SELECT n_nationkey * 100 as a, n_name as b FROM nation",
      "tiny.t",
      ROW({"a", "b"}, {BIGINT(), VARCHAR()}),
      matcher);

  // Missing column names.
  {
    auto parser = makeParser();

    VELOX_ASSERT_THROW(
        parser.parse(
            "CREATE TABLE t AS SELECT n_nationkey * 100, n_name FROM nation"),
        "Column name not specified at position 1");
  }

  testCtasSql(
      "CREATE TABLE t(a, b) AS SELECT n_nationkey * 100, n_name FROM nation",
      "tiny.t",
      ROW({"a", "b"}, {BIGINT(), VARCHAR()}),
      matcher);

  // Table properties.
  testCtasSql(
      "CREATE TABLE t WITH (partitioned_by = ARRAY['ds']) AS "
      "SELECT n_nationkey, n_name, '2025-10-04' as ds FROM nation",
      "tiny.t",
      ROW({"n_nationkey", "n_name", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()}),
      matcher,
      {
          {"partitioned_by", "array_constructor(ds)"},
      });

  testCtasSql(
      "CREATE TABLE t WITH (partitioned_by = ARRAY['ds'], bucket_count = 4, bucketed_by = ARRAY['n_nationkey']) AS "
      "SELECT n_nationkey, n_name, '2025-10-04' as ds FROM nation",
      "tiny.t",
      ROW({"n_nationkey", "n_name", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()}),
      matcher,
      {
          {"partitioned_by", "array_constructor(ds)"},
          {"bucket_count", "4"},
          {"bucketed_by", "array_constructor(n_nationkey)"},
      });
}

TEST_F(PrestoParserTest, createTable) {
  testCreateSql(
      "CREATE TABLE t (id INTEGER)", "tiny.t", ROW({"id"}, {INTEGER()}));

  // if not exists
  {
    auto parser = makeParser();
    auto statement = parser.parse("CREATE TABLE IF NOT EXISTS t (id BIGINT)");
    ASSERT_TRUE(statement->isCreateTable());

    const auto* createTable = statement->as<CreateTableStatement>();
    ASSERT_EQ("tiny.t", createTable->tableName());
    ASSERT_TRUE(createTable->ifNotExists());
  }

  // properties
  testCreateSql(
      "CREATE TABLE t (id INTEGER, ds VARCHAR) "
      "WITH (partitioned_by = ARRAY['ds'], format = 'ORC')",
      "tiny.t",
      ROW({"id", "ds"}, {INTEGER(), VARCHAR()}),
      /*properties=*/
      {{"partitioned_by", "array_constructor(ds)"}, {"format", "ORC"}});

  // a variety of different types
  testCreateSql(
      "CREATE TABLE t ("
      "  tiny_col TINYINT,"
      "  small_col SMALLINT,"
      "  int_col INT,"
      "  big_col BIGINT,"
      "  real_col REAL,"
      "  double_col DOUBLE,"
      "  varchar_col VARCHAR,"
      "  bool_col BOOLEAN"
      ")",
      "tiny.t",
      ROW({"tiny_col",
           "small_col",
           "int_col",
           "big_col",
           "real_col",
           "double_col",
           "varchar_col",
           "bool_col"},
          {TINYINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR(),
           BOOLEAN()}));

  // complex types
  testCreateSql(
      "CREATE TABLE t (ids ARRAY<INTEGER>)",
      "tiny.t",
      ROW({"ids"}, {ARRAY(INTEGER())}));
  testCreateSql(
      "CREATE TABLE t (kv MAP<VARCHAR, BIGINT>)",
      "tiny.t",
      ROW({"kv"}, {MAP(VARCHAR(), BIGINT())}));
  testCreateSql(
      "CREATE TABLE t (nested_map MAP<VARCHAR, ARRAY<INTEGER>>)",
      "tiny.t",
      ROW({"nested_map"}, {MAP(VARCHAR(), ARRAY(INTEGER()))}));
  testCreateSql(
      "CREATE TABLE t (nested ROW(a INTEGER, b VARCHAR))",
      "tiny.t",
      ROW({"nested"}, {ROW({"a", "b"}, {INTEGER(), VARCHAR()})}));
  testCreateSql(
      "CREATE TABLE t (price DECIMAL(10, 2))",
      "tiny.t",
      ROW({"price"}, {DECIMAL(10, 2)}));

  // like clause
  {
    auto likeSchema = facebook::axiom::connector::ConnectorMetadata::metadata(
                          kTpchConnectorId)
                          ->findTable("nation")
                          ->type();
    testCreateSql("CREATE TABLE copy (LIKE nation)", "tiny.copy", likeSchema);
  }

  // like clause + some more columns
  {
    auto likeSchema = facebook::axiom::connector::ConnectorMetadata::metadata(
                          kTpchConnectorId)
                          ->findTable("nation")
                          ->type();

    // should respect the order of (before, LIKE, after)
    std::vector<std::string> names = {"before"};
    std::vector<TypePtr> types = {INTEGER()};
    for (int i = 0; i < likeSchema->size(); ++i) {
      names.push_back(likeSchema->nameOf(i));
      types.push_back(likeSchema->childAt(i));
    }
    names.push_back("after");
    types.push_back(DOUBLE());

    testCreateSql(
        "CREATE TABLE extended (before INTEGER, LIKE nation, after DOUBLE)",
        "tiny.extended",
        ROW(std::move(names), std::move(types)));
  }

  // primary key constraint
  {
    std::vector<CreateTableStatement::Constraint> constraints = {
        {.columns = {"id"},
         .type = CreateTableStatement::Constraint::Type::kPrimaryKey}};
    testCreateSql(
        "CREATE TABLE t (id INTEGER, PRIMARY KEY (id))",
        "tiny.t",
        ROW({"id"}, {INTEGER()}),
        /*properties=*/{},
        constraints);
  }

  // unique key constraint
  {
    std::vector<CreateTableStatement::Constraint> constraints = {
        {.name = "unique_name",
         .columns = {"name"},
         .type = CreateTableStatement::Constraint::Type::kUnique}};
    testCreateSql(
        "CREATE TABLE t (id INTEGER, name VARCHAR, CONSTRAINT unique_name UNIQUE (name))",
        "tiny.t",
        ROW({"id", "name"}, {INTEGER(), VARCHAR()}),
        /*properties=*/{},
        constraints);
  }

  // duplicate column names
  testInvalidSql(
      "CREATE TABLE t (id INTEGER, id VARCHAR)", "Duplicate column name: id");
  testInvalidSql(
      "CREATE TABLE t (id INTEGER, ID VARCHAR)", "Duplicate column name: ID");

  // unknown type
  testInvalidSql(
      "CREATE TABLE t (id UNKNOWNTYPE)", "Cannot resolve type: UNKNOWNTYPE");

  // invalid decimal type parameters
  testInvalidSql(
      "CREATE TABLE t (price DECIMAL(VARCHAR, ARRAY<INTEGER>))",
      "'VARCHAR' could not be converted to INTEGER_LITERAL");

  // unknown constraint column
  testInvalidSql(
      "CREATE TABLE t (id INTEGER, PRIMARY KEY (unknown))",
      "Constraint on unknown column: unknown");

  // duplicate constraint column
  testInvalidSql(
      "CREATE TABLE t (id INTEGER, PRIMARY KEY (id, id))",
      "Duplicate constraint column: id");
  testInvalidSql(
      "CREATE TABLE t (id INTEGER, PRIMARY KEY (id, ID))",
      "Duplicate constraint column: ID");

  // duplicate table property
  testInvalidSql(
      "CREATE TABLE t (id INTEGER) WITH (format = 'ORC', format = 'PARQUET')",
      "Duplicate property: format");
}

TEST_F(PrestoParserTest, dropTable) {
  auto parser = makeParser();

  {
    auto statement = parser.parse("DROP TABLE t");
    ASSERT_TRUE(statement->isDropTable());

    const auto* dropTable = statement->as<DropTableStatement>();
    ASSERT_EQ("tiny.t", dropTable->tableName());
    ASSERT_FALSE(dropTable->ifExists());
  }

  {
    auto statement = parser.parse("DROP TABLE IF EXISTS u");
    ASSERT_TRUE(statement->isDropTable());

    const auto* dropTable = statement->as<DropTableStatement>();
    ASSERT_EQ("tiny.u", dropTable->tableName());
    ASSERT_TRUE(dropTable->ifExists());
  }
}

TEST_F(PrestoParserTest, view) {
  auto* metadata =
      dynamic_cast<facebook::axiom::connector::tpch::TpchConnectorMetadata*>(
          facebook::axiom::connector::ConnectorMetadata::metadata(
              kTpchConnectorId));

  SCOPE_EXIT {
    metadata->dropView("view");
  };

  metadata->createView(
      "view",
      ROW({"n_nationkey", "cnt"}, {BIGINT(), BIGINT()}),
      "SELECT n_regionkey as regionkey, count(*) cnt FROM nation GROUP BY 1");

  auto matcher = lp::test::LogicalPlanMatcherBuilder()
                     .tableScan()
                     .join(
                         lp::test::LogicalPlanMatcherBuilder()
                             .tableScan()
                             .aggregate()
                             .project()
                             .build())
                     .filter()
                     .project();
  testSql(
      "SELECT n_name, n_regionkey, cnt FROM nation n, view v "
      "WHERE n_nationkey = regionkey",
      matcher,
      {"tiny.view"});
}

TEST_F(PrestoParserTest, unqualifiedAccessAfterJoin) {
  auto sql =
      "SELECT n_name FROM (SELECT n1.n_name as n_name FROM nation n1, nation n2)";

  auto matcher =
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .join(lp::test::LogicalPlanMatcherBuilder().tableScan().build())
          .project()
          .project();
  testSql(sql, matcher);
}

TEST_F(PrestoParserTest, createTableAndInsert) {
  auto parser = makeParser();

  // Parse CREATE TABLE and INSERT statements.
  const auto statements = parser.parseMultiple(
      "CREATE TABLE test_table AS SELECT n_nationkey, n_name FROM nation; "
      "INSERT INTO nation SELECT * FROM nation");

  // Verify both statements parsed correctly with expected write types.
  ASSERT_EQ(2, statements.size());
  ASSERT_TRUE(statements[0]->isCreateTableAsSelect());
  ASSERT_TRUE(statements[1]->isInsert());

  const auto* ctasWrite = statements[0]
                              ->as<CreateTableAsSelectStatement>()
                              ->plan()
                              ->as<lp::TableWriteNode>();
  ASSERT_EQ(lp::WriteKind::kCreate, ctasWrite->writeKind());

  const auto* insertWrite =
      statements[1]->as<InsertStatement>()->plan()->as<lp::TableWriteNode>();
  ASSERT_EQ(lp::WriteKind::kInsert, insertWrite->writeKind());
}

} // namespace
} // namespace axiom::sql::presto
