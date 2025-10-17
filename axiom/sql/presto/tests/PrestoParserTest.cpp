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
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/sql/presto/tests/LogicalPlanMatcher.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace axiom::sql::presto {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class PrestoParserTest : public testing::Test {
 public:
  static constexpr const char* kTpchConnectorId = "tpch";

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    auto emptyConfig = std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});

    facebook::velox::connector::tpch::TpchConnectorFactory tpchConnectorFactory;
    auto tpchConnector =
        tpchConnectorFactory.newConnector(kTpchConnectorId, emptyConfig);
    facebook::velox::connector::registerConnector(tpchConnector);

    facebook::axiom::connector::ConnectorMetadata::registerMetadata(
        kTpchConnectorId,
        std::make_shared<
            facebook::axiom::connector::tpch::TpchConnectorMetadata>(
            dynamic_cast<facebook::velox::connector::tpch::TpchConnector*>(
                tpchConnector.get())));

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  static void TearDownTestCase() {
    facebook::axiom::connector::ConnectorMetadata::unregisterMetadata(
        kTpchConnectorId);
    facebook::velox::connector::unregisterConnector(kTpchConnectorId);
  }

  memory::MemoryPool* pool() {
    return pool_.get();
  }

  void testSql(
      std::string_view sql,
      lp::test::LogicalPlanMatcherBuilder& matcher) {
    SCOPED_TRACE(sql);
    PrestoParser parser(kTpchConnectorId, pool());

    auto statement = parser.parse(sql);
    ASSERT_TRUE(statement->isSelect());

    auto logicalPlan = statement->as<SelectStatement>()->plan();
    ASSERT_TRUE(matcher.build()->match(logicalPlan))
        << lp::PlanPrinter::toText(*logicalPlan);
  }

  void testInsertSql(
      std::string_view sql,
      lp::test::LogicalPlanMatcherBuilder& matcher) {
    SCOPED_TRACE(sql);
    PrestoParser parser(kTpchConnectorId, pool());

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
    PrestoParser parser(kTpchConnectorId, pool());

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

  template <typename T>
  void testDecimal(std::string_view sql, T value, const TypePtr& type) {
    SCOPED_TRACE(sql);

    PrestoParser parser(kTpchConnectorId, pool());
    auto expr = parser.parseExpression(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), type->toString());

    auto v = expr->asUnchecked<lp::ConstantExpr>()->value();
    ASSERT_FALSE(v->isNull());
    ASSERT_EQ(v->value<T>(), value);
  }

 private:
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild("leaf")};
};

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
}

TEST_F(PrestoParserTest, syntaxErrors) {
  PrestoParser parser(kTpchConnectorId, pool());
  EXPECT_THAT(
      [&]() { parser.parse("SELECT * FROM"); },
      ThrowsMessage<std::runtime_error>(::testing::HasSubstr(
          "Syntax error at 1:13: mismatched input '<EOF>'")));

  EXPECT_THAT(
      [&]() {
        parser.parse(
            "SELECT * FROM nation\n"
            "WHERE");
      },
      ThrowsMessage<std::runtime_error>(::testing::HasSubstr(
          "Syntax error at 2:5: mismatched input '<EOF>'")));
}

TEST_F(PrestoParserTest, types) {
  PrestoParser parser(kTpchConnectorId, pool());

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
}

TEST_F(PrestoParserTest, intervalDayTime) {
  PrestoParser parser(kTpchConnectorId, pool());

  auto test = [&](std::string_view sql, int64_t expected) {
    SCOPED_TRACE(sql);
    auto expr = parser.parseExpression(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), INTERVAL_DAY_TIME()->toString());

    auto value = expr->asUnchecked<lp::ConstantExpr>()->value();
    ASSERT_FALSE(value->isNull());
    ASSERT_EQ(value->value<int64_t>(), expected);
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
  PrestoParser parser(kTpchConnectorId, pool());

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
  PrestoParser parser(kTpchConnectorId, pool());

  auto test = [&](std::string_view sql, int64_t expected) {
    auto expr = parser.parseExpression(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), INTERVAL_YEAR_MONTH()->toString());

    auto value = expr->asUnchecked<lp::ConstantExpr>()->value();
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

TEST_F(PrestoParserTest, selectStar) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan();
  testSql("SELECT * FROM nation", matcher);
}

TEST_F(PrestoParserTest, countStar) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate();

  testSql("SELECT count(*) FROM nation", matcher);
  testSql("SELECT count(1) FROM nation", matcher);
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
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().project();
    testSql(
        "SELECT count(1) FROM nation GROUP BY substr(n_name, 1, 2)", matcher);
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

TEST_F(PrestoParserTest, join) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().join(
        lp::test::LogicalPlanMatcherBuilder().tableScan().build());

    testSql("SELECT * FROM nation, region", matcher);

    testSql(
        "SELECT * FROM nation LEFT JOIN region ON n_regionkey = r_regionkey",
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

TEST_F(PrestoParserTest, explain) {
  PrestoParser parser(kTpchConnectorId, pool());

  {
    auto statement = parser.parse("EXPLAIN SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(
        explainStatement->type() == ExplainStatement::Type::kDistributed);

    auto selectStatement = explainStatement->statement();
    ASSERT_TRUE(selectStatement->isSelect());

    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan();

    auto logicalPlan = selectStatement->as<SelectStatement>()->plan();
    ASSERT_TRUE(matcher.build()->match(logicalPlan));
  }

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
        parser.parse("EXPLAIN (TYPE DISTRIBUTED) SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(
        explainStatement->type() == ExplainStatement::Type::kDistributed);
  }
}

TEST_F(PrestoParserTest, describe) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values();
  testSql("DESCRIBE nation", matcher);

  testSql("DESC orders", matcher);

  testSql("SHOW COLUMNS FROM lineitem", matcher);
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
    PrestoParser parser(kTpchConnectorId, pool());

    VELOX_ASSERT_THROW(
        parser.parse("INSERT INTO nation SELECT 100, 'n-100', 2, 3"),
        "Wrong column type: BIGINT vs. VARCHAR, column n_comment in table nation");
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
        "CREATE TABLE t AS SELECT * FROM nation", "t", nationSchema, matcher);
  }

  auto matcher =
      lp::test::LogicalPlanMatcherBuilder().tableScan().project().tableWrite();

  testCtasSql(
      "CREATE TABLE t AS SELECT n_nationkey * 100 as a, n_name as b FROM nation",
      "t",
      ROW({"a", "b"}, {BIGINT(), VARCHAR()}),
      matcher);

  // Missing column names.
  {
    PrestoParser parser(kTpchConnectorId, pool());

    VELOX_ASSERT_THROW(
        parser.parse(
            "CREATE TABLE t AS SELECT n_nationkey * 100, n_name FROM nation"),
        "Column name not specified at position 1");
  }

  testCtasSql(
      "CREATE TABLE t(a, b) AS SELECT n_nationkey * 100, n_name FROM nation",
      "t",
      ROW({"a", "b"}, {BIGINT(), VARCHAR()}),
      matcher);

  // Table properties.
  testCtasSql(
      "CREATE TABLE t WITH (partitioned_by = ARRAY['ds']) AS "
      "SELECT n_nationkey, n_name, '2025-10-04' as ds FROM nation",
      "t",
      ROW({"n_nationkey", "n_name", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()}),
      matcher,
      {
          {"partitioned_by", "array_constructor(ds)"},
      });

  testCtasSql(
      "CREATE TABLE t WITH (partitioned_by = ARRAY['ds'], bucket_count = 4, bucketed_by = ARRAY['n_nationkey']) AS "
      "SELECT n_nationkey, n_name, '2025-10-04' as ds FROM nation",
      "t",
      ROW({"n_nationkey", "n_name", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()}),
      matcher,
      {
          {"partitioned_by", "array_constructor(ds)"},
          {"bucket_count", "4"},
          {"bucketed_by", "array_constructor(n_nationkey)"},
      });
}

TEST_F(PrestoParserTest, dropTable) {
  PrestoParser parser(kTpchConnectorId, pool());

  {
    auto statement = parser.parse("DROP TABLE t");
    ASSERT_TRUE(statement->isDropTable());

    const auto* dropTable = statement->as<DropTableStatement>();
    ASSERT_EQ("t", dropTable->tableName());
    ASSERT_FALSE(dropTable->ifExists());
  }

  {
    auto statement = parser.parse("DROP TABLE IF EXISTS u");
    ASSERT_TRUE(statement->isDropTable());

    const auto* dropTable = statement->as<DropTableStatement>();
    ASSERT_EQ("u", dropTable->tableName());
    ASSERT_TRUE(dropTable->ifExists());
  }
}

} // namespace
} // namespace axiom::sql::presto
