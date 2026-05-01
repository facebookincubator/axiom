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

#include "axiom/cli/SqlQueryRunner.h"
#include <folly/dynamic.h>
// NOLINTNEXTLINE(facebook-unused-include-check)
#include <folly/init/Init.h>
#include <folly/json.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thread>
#include "axiom/cli/QueryIdGenerator.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/sql/presto/tests/ExpectPrestoSqlError.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace axiom::sql {
namespace {

class SqlQueryRunnerTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    facebook::velox::memory::MemoryManager::testingSetInstance(
        facebook::velox::memory::MemoryManager::Options{});
  }

  void SetUp() override {
    runner_ = makeRunner();
  }

  void TearDown() override {
    runner_.reset();
    for (const auto& id : connectorIds_) {
      facebook::velox::connector::ConnectorRegistry::global().erase(id);
    }
  }

  inline static const std::string kDefaultSchema{
      facebook::axiom::connector::TestConnector::kDefaultSchema};

  std::unique_ptr<SqlQueryRunner> makeRunner(
      const std::string& connectorId = "test",
      std::function<std::string()> queryIdGenerator = {},
      PermissionCheck permissionCheck = {}) {
    auto runner = std::make_unique<SqlQueryRunner>();

    auto initConnectors = [&]() {
      testConnector_ =
          std::make_shared<facebook::axiom::connector::TestConnector>(
              connectorId);
      facebook::velox::connector::ConnectorRegistry::global().insert(
          testConnector_->connectorId(), testConnector_);

      connectorIds_.emplace_back(testConnector_->connectorId());

      return std::make_pair(testConnector_->connectorId(), kDefaultSchema);
    };

    runner->initialize(
        initConnectors,
        std::move(permissionCheck),
        std::move(queryIdGenerator));

    return runner;
  }

  SqlQueryRunner::SqlResult run(std::string_view sql) {
    return runner_->run(sql, {});
  }

  RowVectorPtr fetchSingleRow(
      std::string_view sql,
      const SqlQueryRunner::RunOptions& options = {}) {
    auto result = runner_->run(sql, options);
    VELOX_CHECK(
        !result.message.has_value(), "Query failed: {}", *result.message);
    VELOX_CHECK_EQ(1, result.results.size());
    VELOX_CHECK_EQ(1, result.results[0]->size());
    return result.results[0];
  }

  void assertSchemas(const std::vector<std::string>& expected) {
    auto result = run("SHOW SCHEMAS");
    ASSERT_FALSE(result.message.has_value());
    ASSERT_EQ(1, result.results.size());
    test::assertEqualVectors(
        result.results[0],
        makeRowVector({"Schema"}, {makeFlatVector(expected)}));
  }

  void assertSessionProperty(
      std::string_view name,
      std::string_view expectedValue,
      std::string_view expectedDefault) {
    SCOPED_TRACE(name);
    auto row = fetchSingleRow(fmt::format("SHOW SESSION LIKE '{}'", name));
    ASSERT_EQ(row->childrenSize(), 5);
    EXPECT_EQ(
        row->childAt(0)->as<SimpleVector<StringView>>()->valueAt(0), name);
    EXPECT_EQ(
        row->childAt(1)->as<SimpleVector<StringView>>()->valueAt(0),
        expectedValue);
    EXPECT_EQ(
        row->childAt(2)->as<SimpleVector<StringView>>()->valueAt(0),
        expectedDefault);
  }

  std::unique_ptr<SqlQueryRunner> runner_;
  std::shared_ptr<facebook::axiom::connector::TestConnector> testConnector_;

 private:
  std::vector<std::string> connectorIds_;
};

TEST_F(SqlQueryRunnerTest, runSingleStatement) {
  test::assertEqualVectors(
      fetchSingleRow("SELECT 1"),
      makeRowVector({makeFlatVector<int32_t>({1})}));
}

TEST_F(SqlQueryRunnerTest, runRejectsMultipleStatements) {
  VELOX_ASSERT_THROW(run("SELECT 1; SELECT 2"), "Expected a single statement");
}

TEST_F(SqlQueryRunnerTest, parseAndRunMixedStatementTypes) {
  auto statements = runner_->parseMultiple(
      "SELECT 42; EXPLAIN (TYPE LOGICAL) SELECT 1; select 7", {});
  ASSERT_EQ(3, statements.size());

  auto selectResult = runner_->runUnchecked(*statements[0], {});
  ASSERT_FALSE(selectResult.message.has_value());
  test::assertEqualVectors(
      selectResult.results[0], makeRowVector({makeFlatVector<int32_t>({42})}));

  auto explainResult = runner_->runUnchecked(*statements[1], {});
  ASSERT_TRUE(explainResult.message.has_value());
  ASSERT_FALSE(explainResult.message.value().empty());

  auto lastResult = runner_->runUnchecked(*statements[2], {});
  ASSERT_FALSE(lastResult.message.has_value());
  test::assertEqualVectors(
      lastResult.results[0], makeRowVector({makeFlatVector<int32_t>({7})}));
}

TEST_F(SqlQueryRunnerTest, multipleRunnerInstances) {
  auto a = makeRunner("test_a");
  auto b = makeRunner("test_b");

  auto resultA = a->run("SELECT 1", {});
  auto resultB = b->run("SELECT 1 + 2, 'foo'", {});

  ASSERT_FALSE(resultA.message.has_value());
  ASSERT_EQ(1, resultA.results.size());

  test::assertEqualVectors(
      resultA.results[0], makeRowVector({makeFlatVector<int32_t>({1})}));

  ASSERT_FALSE(resultB.message.has_value());
  ASSERT_EQ(1, resultB.results.size());

  test::assertEqualVectors(
      resultB.results[0],
      makeRowVector({
          makeFlatVector<int32_t>({3}),
          makeFlatVector<std::string>({"foo"}),
      }));
}

TEST_F(SqlQueryRunnerTest, invalidStatementThrows) {
  EXPECT_THROW(run("INVALID SYNTAX HERE"), std::exception);
}

TEST_F(SqlQueryRunnerTest, parseMultipleWithInvalidStatement) {
  EXPECT_THROW(
      runner_->parseMultiple("SELECT 1; INVALID; SELECT 2", {}),
      std::exception);
}

TEST_F(SqlQueryRunnerTest, explainCtas) {
  {
    auto result = run("EXPLAIN (TYPE LOGICAL) CREATE TABLE t AS SELECT 1 AS x");
    ASSERT_TRUE(result.message.has_value());
    EXPECT_THAT(
        result.message.value(),
        ::testing::HasSubstr("- TableWrite CREATE: -> ROW<rows:BIGINT>"));
  }

  {
    auto result =
        run("EXPLAIN (TYPE OPTIMIZED) CREATE TABLE t AS SELECT 1 AS x");
    ASSERT_TRUE(result.message.has_value());
    EXPECT_THAT(
        result.message.value(),
        ::testing::HasSubstr("TableWrite [1.00 rows] ->"));
  }

  {
    auto result = run("EXPLAIN CREATE TABLE t AS SELECT 1 AS x");
    ASSERT_TRUE(result.message.has_value());
    EXPECT_THAT(result.message.value(), ::testing::HasSubstr("-- TableWrite"));
  }

  // EXPLAIN is side-effect-free.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      run("SELECT * FROM t"), "Table not found: t");

  // EXPLAIN ANALYZE runs the query and creates the table.
  {
    auto result = run("EXPLAIN ANALYZE CREATE TABLE t AS SELECT 1 AS x");
    ASSERT_TRUE(result.message.has_value());
    EXPECT_THAT(result.message.value(), ::testing::HasSubstr("-- TableWrite"));
  }

  {
    auto result = run("SELECT * FROM t");
    ASSERT_FALSE(result.message.has_value());
    ASSERT_EQ(1, result.results.size());
    test::assertEqualVectors(
        result.results[0], makeRowVector({makeFlatVector<int32_t>({1})}));
  }
}

TEST_F(SqlQueryRunnerTest, explainCreateTable) {
  auto result = run("EXPLAIN CREATE TABLE t(x int)");
  EXPECT_EQ(result.message, R"(CREATE TABLE test."default"."t")");

  // EXPLAIN is side-effect-free.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      run("SELECT * FROM t"), "Table not found: t");

  // Fails if the table already exists.
  run("CREATE TABLE t(x int)");
  VELOX_ASSERT_THROW(
      run("EXPLAIN CREATE TABLE t(x int)"), "Table already exists");

  // IF NOT EXISTS succeeds even if the table exists.
  result = run("EXPLAIN CREATE TABLE IF NOT EXISTS t(x int)");
  EXPECT_EQ(result.message, R"(CREATE TABLE IF NOT EXISTS test."default"."t")");
}

TEST_F(SqlQueryRunnerTest, explainDropTable) {
  // Fails if the table doesn't exist.
  VELOX_ASSERT_THROW(run("EXPLAIN DROP TABLE t"), "Table does not exist");

  // IF EXISTS succeeds even if the table doesn't exist.
  auto result = run("EXPLAIN DROP TABLE IF EXISTS t");
  EXPECT_EQ(result.message, R"(DROP TABLE IF EXISTS test."default"."t")");

  run("CREATE TABLE t(x int)");

  result = run("EXPLAIN DROP TABLE t");
  EXPECT_EQ(result.message, R"(DROP TABLE test."default"."t")");

  // EXPLAIN is side-effect-free.
  run("DROP TABLE t");
}

TEST_F(SqlQueryRunnerTest, explainIo) {
  testConnector_->addTpchTables();

  // Parses and re-serializes JSON with sorted keys for deterministic
  // comparison.
  auto normalizeJson = [](const std::string& jsonStr) {
    folly::json::serialization_opts opts;
    opts.pretty_formatting = true;
    opts.sort_keys = true;
    return folly::json::serialize(folly::parseJson(jsonStr), opts);
  };

  auto getJson = [&](const std::string& query) {
    auto result = run(query);
    VELOX_CHECK(result.message.has_value());
    return normalizeJson(result.message.value());
  };

  // SELECT with no table scans.
  ASSERT_EQ(
      getJson("EXPLAIN (TYPE IO) SELECT 1"),
      normalizeJson(R"({"inputTableColumnInfos": []})"));

  // SELECT with table scan.
  ASSERT_EQ(
      getJson("EXPLAIN (TYPE IO) SELECT * FROM nation"), normalizeJson(R"({
        "inputTableColumnInfos": [{
          "table": {
            "catalog": "test",
            "schemaTable": {"schema": "default", "table": "nation"}
          },
          "columnConstraints": []
        }]
      })"));

  // CTAS with output table.
  ASSERT_EQ(
      getJson("EXPLAIN (TYPE IO) CREATE TABLE t AS SELECT * FROM nation"),
      normalizeJson(R"({
        "inputTableColumnInfos": [{
          "table": {
            "catalog": "test",
            "schemaTable": {"schema": "default", "table": "nation"}
          },
          "columnConstraints": []
        }],
        "outputTable": {
          "catalog": "test",
          "schemaTable": {"schema": "default", "table": "t"}
        }
      })"));

  // INSERT with output table.
  testConnector_->addTable("t", ROW({"key", "name"}, {BIGINT(), VARCHAR()}));
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) INSERT INTO t(key, name) "
          "SELECT r_regionkey, r_name FROM region"),
      normalizeJson(R"({
        "inputTableColumnInfos": [{
          "table": {
            "catalog": "test",
            "schemaTable": {"schema": "default", "table": "region"}
          },
          "columnConstraints": []
        }],
        "outputTable": {
          "catalog": "test",
          "schemaTable": {"schema": "default", "table": "t"}
        }
      })"));

  // JOIN: multiple input tables sorted by name.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) SELECT * FROM nation, region WHERE n_regionkey = r_regionkey"),
      normalizeJson(R"({
        "inputTableColumnInfos": [
          {
            "table": {
              "catalog": "test",
              "schemaTable": {"schema": "default", "table": "nation"}
            },
            "columnConstraints": []
          },
          {
            "table": {
              "catalog": "test",
              "schemaTable": {"schema": "default", "table": "region"}
            },
            "columnConstraints": []
          }
        ]
      })"));
}

TEST_F(SqlQueryRunnerTest, explainIoColumnConstraints) {
  run("CREATE TABLE t (x BIGINT, ds VARCHAR, region VARCHAR) "
      "WITH (explain_io = ARRAY['ds', 'region'])");
  SCOPE_EXIT {
    run("DROP TABLE t");
  };

  auto normalizeJson = [](const std::string& jsonStr) {
    folly::json::serialization_opts opts;
    opts.pretty_formatting = true;
    opts.sort_keys = true;
    return folly::json::serialize(folly::parseJson(jsonStr), opts);
  };

  auto getJson = [&](const std::string& query) {
    auto result = run(query);
    VELOX_CHECK(result.message.has_value());
    return normalizeJson(result.message.value());
  };

  auto makeConstraint = [&](const std::string& columnName,
                            const std::string& typeSignature,
                            const std::string& domainJson) {
    return normalizeJson(
        fmt::format(
            R"({{"columnName": "{}", "typeSignature": "{}", "domain": {}}})",
            columnName,
            typeSignature,
            domainJson));
  };

  auto makeTable = [&](const std::string& constraintsJson) {
    return normalizeJson(
        fmt::format(
            R"({{
          "inputTableColumnInfos": [{{
            "table": {{
              "catalog": "test",
              "schemaTable": {{"schema": "default", "table": "t"}}
            }},
            "columnConstraints": [{}]
          }}]
        }})",
            constraintsJson));
  };

  // Equality constraint.
  ASSERT_EQ(
      getJson("EXPLAIN (TYPE IO) SELECT * FROM t WHERE ds = '2026-03-17'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({"nullsAllowed": false, "ranges": [
            {"low": {"value": "2026-03-17", "bound": "EXACTLY"},
             "high": {"value": "2026-03-17", "bound": "EXACTLY"}}]})")));

  // IN constraint.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) SELECT * FROM t "
          "WHERE ds IN ('2026-03-17', '2026-03-18')"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({"nullsAllowed": false, "ranges": [
            {"low": {"value": "2026-03-17", "bound": "EXACTLY"},
             "high": {"value": "2026-03-17", "bound": "EXACTLY"}},
            {"low": {"value": "2026-03-18", "bound": "EXACTLY"},
             "high": {"value": "2026-03-18", "bound": "EXACTLY"}}]})")));

  // Range constraint.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) SELECT * FROM t "
          "WHERE ds >= '2026-03-01' AND ds <= '2026-03-31'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({"nullsAllowed": false, "ranges": [
            {"low": {"value": "2026-03-01", "bound": "EXACTLY"},
             "high": {"value": "2026-03-31", "bound": "EXACTLY"}}]})")));

  auto noConstraints = normalizeJson(R"({
    "inputTableColumnInfos": [{
      "table": {
        "catalog": "test",
        "schemaTable": {"schema": "default", "table": "t"}
      },
      "columnConstraints": []
    }]
  })");

  // No constraint on explain_io column (filter on non-explain_io column only).
  ASSERT_EQ(
      getJson("EXPLAIN (TYPE IO) SELECT * FROM t WHERE x = 1"), noConstraints);

  // Unconvertible filter on explain_io column drops the column entirely.
  // NOT is not supported, so ds <> 'foo' (which becomes NOT(eq(ds, 'foo')))
  // causes the column to be dropped.
  ASSERT_EQ(
      getJson("EXPLAIN (TYPE IO) SELECT * FROM t WHERE ds <> 'foo'"),
      noConstraints);

  // Mix of convertible and unconvertible filters: unconvertible conjunct is
  // skipped (broader is safe), convertible conjunct is shown.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) SELECT * FROM t "
          "WHERE ds >= '2026-03-01' AND ds <> 'foo'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({"nullsAllowed": false, "ranges": [
            {"low": {"value": "2026-03-01", "bound": "EXACTLY"}}]})")));

  // IS NULL OR equality: nullsAllowed is true.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) SELECT * FROM t "
          "WHERE ds IS NULL OR ds = '2026-03-17'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({"nullsAllowed": true, "ranges": [
            {"low": {"value": "2026-03-17", "bound": "EXACTLY"},
             "high": {"value": "2026-03-17", "bound": "EXACTLY"}}]})")));

  // OR with unconvertible disjunct: the entire OR expression cannot be
  // converted (dropping a disjunct would narrow the result, which is unsafe).
  // The unconvertible OR is skipped as a conjunct (broader is safe).
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) SELECT * FROM t "
          "WHERE ds = '2026-03-17' OR length(ds) > 3"),
      noConstraints);

  // Constraints on both explain_io columns.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) SELECT * FROM t "
          "WHERE ds = '2026-03-17' AND region = 'us'"),
      normalizeJson(R"({
        "inputTableColumnInfos": [{
          "table": {
            "catalog": "test",
            "schemaTable": {"schema": "default", "table": "t"}
          },
          "columnConstraints": [
            {
              "columnName": "ds",
              "typeSignature": "VARCHAR",
              "domain": {
                "nullsAllowed": false,
                "ranges": [{
                  "low": {"value": "2026-03-17", "bound": "EXACTLY"},
                  "high": {"value": "2026-03-17", "bound": "EXACTLY"}
                }]
              }
            },
            {
              "columnName": "region",
              "typeSignature": "VARCHAR",
              "domain": {
                "nullsAllowed": false,
                "ranges": [{
                  "low": {"value": "us", "bound": "EXACTLY"},
                  "high": {"value": "us", "bound": "EXACTLY"}
                }]
              }
            }
          ]
        }]
      })"));

  // Greater-than (exclusive low bound).
  ASSERT_EQ(
      getJson("EXPLAIN (TYPE IO) SELECT * FROM t WHERE ds > '2026-03-01'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({"nullsAllowed": false, "ranges": [
            {"low": {"value": "2026-03-01", "bound": "ABOVE"}}]})")));

  // Subquery: constraints are extracted from the inner table scan.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM (SELECT * FROM t WHERE ds = '2026-03-17') sub"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({"nullsAllowed": false, "ranges": [
            {"low": {"value": "2026-03-17", "bound": "EXACTLY"},
             "high": {"value": "2026-03-17", "bound": "EXACTLY"}}]})")));

  // UNION ALL: same table scanned twice, merged into one entry with
  // united constraints.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t WHERE ds = '2026-03-17' "
          "UNION ALL "
          "SELECT * FROM t WHERE ds = '2026-03-18'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({"nullsAllowed": false, "ranges": [
            {"low": {"value": "2026-03-17", "bound": "EXACTLY"},
             "high": {"value": "2026-03-17", "bound": "EXACTLY"}},
            {"low": {"value": "2026-03-18", "bound": "EXACTLY"},
             "high": {"value": "2026-03-18", "bound": "EXACTLY"}}]})")));

  // UNION (distinct): domains from both branches are united.
  // (2026-03-17, +inf) ∪ (2026-03-18, +inf) = (2026-03-17, +inf).
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t WHERE ds > '2026-03-17' "
          "UNION "
          "SELECT * FROM t WHERE ds > '2026-03-18'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({"nullsAllowed": false, "ranges": [
            {"low": {"value": "2026-03-17", "bound": "ABOVE"}}]})")));

  // ds <= x OR ds >= x covers all values — constraint is omitted.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) SELECT * FROM t "
          "WHERE ds <= '2026-03-17' OR ds >= '2026-03-17'"),
      noConstraints);
}

TEST_F(SqlQueryRunnerTest, showStats) {
  // Create a table with 100 rows and 4 columns: x has 100 distinct values,
  // y has 7 distinct values, z is an array column, w is NULL for every 4th row.
  run("CREATE TABLE t AS "
      "SELECT x, x % 7 AS y, sequence(1, x % 7 + 1) AS z, "
      "if(x % 4 <> 0, x + x % 7) AS w "
      "FROM unnest(sequence(1, 100)) AS _(x)");

  {
    // SHOW STATS FOR <table>: reports raw connector stats.
    auto expected = makeRowVector({
        // row_count.
        makeNullableFlatVector<int64_t>(
            {100, std::nullopt, std::nullopt, std::nullopt, std::nullopt}),
        // column_name.
        makeNullableFlatVector<std::string>({std::nullopt, "x", "y", "z", "w"}),
        // nulls_fraction: 0.25 for w; 0 for the rest.
        makeNullableFlatVector<double>({std::nullopt, 0.0, 0.0, 0.0, 0.25}),
        // distinct_values_count: NULL for z (array).
        makeNullableFlatVector<int64_t>(
            {std::nullopt, 100, 7, std::nullopt, 75}),
        // avg_length: 3 for z (array); NULL for the rest.
        makeNullableFlatVector<int64_t>(
            {std::nullopt, std::nullopt, std::nullopt, 3, std::nullopt}),
        // low_value.
        makeNullableFlatVector<std::string>(
            {std::nullopt, "1", "0", std::nullopt, "2"}),
        // high_value.
        makeNullableFlatVector<std::string>(
            {std::nullopt, "100", "6", std::nullopt, "103"}),
    });

    auto result = run("SHOW STATS FOR t");
    ASSERT_FALSE(result.message.has_value());
    ASSERT_EQ(1, result.results.size());
    test::assertEqualVectors(expected, result.results[0]);

    // SHOW STATS FOR (SELECT * FROM <table>): reports optimizer estimates. The
    // optimizer always estimates NDV (defaults to numRows when unknown) and
    // doesn't track avgLength.
    expected = makeRowVector({
        expected->childAt(0), // row_count.
        expected->childAt(1), // column_name.
        expected->childAt(2), // nulls_fraction.
        // distinct_values_count: 100 for z (optimizer defaults to numRows).
        makeNullableFlatVector<int64_t>({std::nullopt, 100, 7, 100, 75}),
        // avg_length: not tracked by the optimizer.
        makeNullConstant(TypeKind::BIGINT, 5),
        expected->childAt(5), // low_value.
        expected->childAt(6), // high_value.
    });

    result = run("SHOW STATS FOR (SELECT * FROM t)");
    ASSERT_FALSE(result.message.has_value());
    ASSERT_EQ(1, result.results.size());
    test::assertEqualVectors(expected, result.results[0]);
  }
}

// Verifies that SHOW STATS FOR (<query>) reflects optimizer estimates after
// filter pushdown: reduced row count, tightened min/max, capped NDV.
TEST_F(SqlQueryRunnerTest, showStatsForQueryWithFilter) {
  run("CREATE TABLE t AS "
      "SELECT x, x % 7 AS y, sequence(1, x % 7 + 1) AS z, "
      "if(x % 4 <> 0, x + x % 7) AS w "
      "FROM unnest(sequence(1, 100)) AS _(x)");

  auto expected = makeRowVector({
      // row_count: 50 (filter x > 50 keeps half the rows).
      makeNullableFlatVector<int64_t>(
          {50, std::nullopt, std::nullopt, std::nullopt, std::nullopt}),
      // column_name.
      makeNullableFlatVector<std::string>({std::nullopt, "x", "y", "z", "w"}),
      // nulls_fraction.
      makeNullableFlatVector<double>({std::nullopt, 0.0, 0.0, 0.0, 0.25}),
      // distinct_values_count: reduced for x and w; y stays at 7 because the
      // optimizer assumes column independence — the filter on x doesn't reduce
      // y's domain (and 7 < 50 filtered rows, so NDV is not capped).
      makeNullableFlatVector<int64_t>({std::nullopt, 50, 7, 50, 50}),
      // avg_length: not tracked by the optimizer.
      makeNullConstant(TypeKind::BIGINT, 5),
      // low_value: x tightened to 51.
      makeNullableFlatVector<std::string>(
          {std::nullopt, "51", "0", std::nullopt, "2"}),
      // high_value.
      makeNullableFlatVector<std::string>(
          {std::nullopt, "100", "6", std::nullopt, "103"}),
  });

  auto result = run("SHOW STATS FOR (SELECT * FROM t WHERE x > 50)");
  ASSERT_FALSE(result.message.has_value());
  ASSERT_EQ(1, result.results.size());
  test::assertEqualVectors(expected, result.results[0]);
}

TEST_F(SqlQueryRunnerTest, createAndDropSchema) {
  assertSchemas({kDefaultSchema});

  auto createResult = run("CREATE SCHEMA foo");
  EXPECT_EQ("Created schema: foo", createResult.message);
  assertSchemas({kDefaultSchema, "foo"});

  auto dropResult = run("DROP SCHEMA foo");
  EXPECT_EQ("Dropped schema: foo", dropResult.message);
  assertSchemas({kDefaultSchema});
}

TEST_F(SqlQueryRunnerTest, createSchemaErrors) {
  VELOX_ASSERT_THROW(run("CREATE SCHEMA default"), "Schema already exists");

  auto result = run("CREATE SCHEMA IF NOT EXISTS default");
  EXPECT_EQ("Created schema: default", result.message);
}

TEST_F(SqlQueryRunnerTest, dropSchemaErrors) {
  VELOX_ASSERT_THROW(run("DROP SCHEMA nonexistent"), "Schema does not exist");

  auto result = run("DROP SCHEMA IF EXISTS nonexistent");
  EXPECT_EQ("Dropped schema: nonexistent", result.message);

  VELOX_ASSERT_THROW(run("DROP SCHEMA default"), "Cannot drop the default");
  assertSchemas({kDefaultSchema});

  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      run("DROP SCHEMA default CASCADE"), "CASCADE is not supported");
}

TEST_F(SqlQueryRunnerTest, createTableInNonExistentSchema) {
  VELOX_ASSERT_THROW(
      run("CREATE TABLE nonexistent.t AS SELECT 1 AS x"),
      "Schema does not exist: nonexistent");
}

// Verifies that current_timestamp returns the session start time.
TEST_F(SqlQueryRunnerTest, currentTimestamp) {
  SqlQueryRunner::RunOptions options;
  options.sessionStartTimeMs = facebook::velox::getCurrentTimeMs();

  auto row = fetchSingleRow("SELECT current_timestamp", options);
  auto packed = row->childAt(0)->as<SimpleVector<int64_t>>()->valueAt(0);
  EXPECT_EQ(unpackMillisUtc(packed), options.sessionStartTimeMs);
}

TEST_F(SqlQueryRunnerTest, explainFormatGraphviz) {
  for (const auto& type : {"LOGICAL", "GRAPH"}) {
    SCOPED_TRACE(type);

    // FORMAT GRAPHVIZ produces DOT output.
    auto graphviz = run(
        fmt::format("EXPLAIN (TYPE {}, FORMAT GRAPHVIZ) SELECT 1 AS x", type));
    ASSERT_TRUE(graphviz.message.has_value());
    EXPECT_THAT(graphviz.message.value(), ::testing::HasSubstr("digraph"));

    // Without FORMAT, produces text (no "digraph").
    auto text = run(fmt::format("EXPLAIN (TYPE {}) SELECT 1 AS x", type));
    ASSERT_TRUE(text.message.has_value());
    EXPECT_THAT(
        text.message.value(), ::testing::Not(::testing::HasSubstr("digraph")));
  }

  // FORMAT GRAPHVIZ with unsupported TYPE fails.
  VELOX_ASSERT_USER_THROW(
      run("EXPLAIN (TYPE OPTIMIZED, FORMAT GRAPHVIZ) SELECT 1 AS x"),
      "EXPLAIN FORMAT GRAPHVIZ is supported for TYPE LOGICAL and TYPE GRAPH only");

  VELOX_ASSERT_USER_THROW(
      run("EXPLAIN (FORMAT GRAPHVIZ) SELECT 1 AS x"),
      "EXPLAIN FORMAT GRAPHVIZ is supported for TYPE LOGICAL and TYPE GRAPH only");

  // FORMAT JSON is rejected.
  VELOX_ASSERT_USER_THROW(
      run("EXPLAIN (FORMAT JSON) SELECT 1 AS x"),
      "Unsupported EXPLAIN format: JSON");
}

TEST_F(SqlQueryRunnerTest, showSchemasWithLike) {
  run("CREATE SCHEMA dev");
  run("CREATE SCHEMA staging");
  SCOPE_EXIT {
    run("DROP SCHEMA IF EXISTS dev");
    run("DROP SCHEMA IF EXISTS staging");
  };

  auto result = run("SHOW SCHEMAS LIKE 'd%'");
  ASSERT_EQ(1, result.results.size());
  test::assertEqualVectors(
      result.results[0],
      makeRowVector(
          {"Schema"}, {makeFlatVector<std::string>({kDefaultSchema, "dev"})}));
}

TEST_F(SqlQueryRunnerTest, showCreateTable) {
  {
    run("CREATE TABLE t1 (id INTEGER, name VARCHAR)");
    auto row = fetchSingleRow("SHOW CREATE TABLE t1");
    auto ddl = row->childAt(0)->variantAt(0).value<std::string>();
    EXPECT_EQ(
        ddl,
        "CREATE TABLE test.\"default\".\"t1\" (\n"
        "   id INTEGER,\n"
        "   name VARCHAR\n"
        ")");
  }

  {
    // Hidden columns are not part of the schema and should not appear
    // in the DDL. The output should be equivalent to the original
    // CREATE TABLE statement.
    run("CREATE TABLE t2 (a INTEGER, b VARCHAR) "
        "WITH (hidden = ARRAY['h1', 'h2'])");
    auto row = fetchSingleRow("SHOW CREATE TABLE t2");
    auto ddl = row->childAt(0)->variantAt(0).value<std::string>();
    EXPECT_EQ(
        ddl,
        "CREATE TABLE test.\"default\".\"t2\" (\n"
        "   a INTEGER,\n"
        "   b VARCHAR\n"
        ")\n"
        "WITH (\n"
        "   hidden = ARRAY['h1', 'h2']\n"
        ")");
  }

  // Non-existent table.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      run("SHOW CREATE TABLE no_such_table"), "Table not found");
}

TEST_F(SqlQueryRunnerTest, generateQueryIdDefault) {
  // Default generator produces non-empty, unique IDs via run().
  std::string queryId1;
  std::string queryId2;
  SqlQueryRunner::RunOptions options;
  options.onComplete = [&](const QueryCompletionInfo& info) {
    queryId1 = info.startInfo.queryId;
  };
  runner_->run("SELECT 1", options);
  options.onComplete = [&](const QueryCompletionInfo& info) {
    queryId2 = info.startInfo.queryId;
  };
  runner_->run("SELECT 2", options);
  EXPECT_FALSE(queryId1.empty());
  EXPECT_FALSE(queryId2.empty());
  EXPECT_NE(queryId1, queryId2);
}

TEST_F(SqlQueryRunnerTest, generateQueryIdCustomGenerator) {
  auto generator = std::make_shared<cli::QueryIdGenerator>();
  auto runner = makeRunner("test_custom_gen", [generator]() {
    return generator->createNextQueryId();
  });

  std::string queryId;
  SqlQueryRunner::RunOptions options;
  options.onComplete = [&](const QueryCompletionInfo& info) {
    queryId = info.startInfo.queryId;
  };
  runner->run("SELECT 1", options);
  EXPECT_FALSE(queryId.empty());
  // Custom generator's suffix should appear in the generated ID.
  EXPECT_THAT(queryId, ::testing::HasSubstr(generator->suffix()));
}

TEST_F(SqlQueryRunnerTest, completionCallbackReceivesTiming) {
  QueryCompletionInfo captured;
  runner_->run("SELECT 1", {.onComplete = [&](const QueryCompletionInfo& info) {
                 captured = info;
               }});

  EXPECT_GT(captured.timing.parse, 0);
  EXPECT_GT(captured.timing.total, 0);
  EXPECT_GE(captured.timing.total, captured.timing.parse);
  EXPECT_GE(
      captured.timing.total,
      captured.timing.parse + captured.timing.optimize +
          captured.timing.execute);
}

TEST_F(SqlQueryRunnerTest, startCallbackFiredBeforeCompletion) {
  std::string startQueryId;
  std::string completionQueryId;

  runner_->run(
      "SELECT 1",
      {.onStart =
           [&](const QueryStartInfo& info) {
             startQueryId = info.queryId;
             EXPECT_EQ(info.query, "SELECT 1");
           },
       .onComplete =
           [&](const QueryCompletionInfo& info) {
             completionQueryId = info.startInfo.queryId;
           }});

  EXPECT_FALSE(startQueryId.empty());
  EXPECT_EQ(startQueryId, completionQueryId);
}

TEST_F(SqlQueryRunnerTest, callbacksReceiveQueryMetadata) {
  QueryStartInfo capturedStart;
  QueryCompletionInfo capturedCompletion;

  runner_->run(
      "SELECT 1",
      {.onStart = [&](const QueryStartInfo& info) { capturedStart = info; },
       .onComplete =
           [&](const QueryCompletionInfo& info) {
             capturedCompletion = info;
           }});

  EXPECT_EQ(capturedStart.catalog, runner_->defaultConnectorId());
  EXPECT_EQ(capturedStart.schema, runner_->defaultSchema());
  EXPECT_FALSE(capturedStart.queryType.has_value());
  EXPECT_EQ(
      capturedCompletion.startInfo.catalog, runner_->defaultConnectorId());
  EXPECT_EQ(capturedCompletion.startInfo.schema, runner_->defaultSchema());
  ASSERT_TRUE(capturedCompletion.startInfo.queryType.has_value());
  EXPECT_EQ(
      *capturedCompletion.startInfo.queryType,
      presto::SqlStatementKind::kSelect);
}

TEST_F(SqlQueryRunnerTest, completionCallbackOnError) {
  QueryCompletionInfo captured;

  EXPECT_THROW(
      runner_->run(
          "SELECT * FROM nonexistent_table",
          {.onComplete =
               [&](const QueryCompletionInfo& info) { captured = info; }}),
      std::exception);

  EXPECT_TRUE(captured.errorInfo.has_value());
  EXPECT_FALSE(captured.errorInfo->message.empty());
}

TEST_F(SqlQueryRunnerTest, multiStatementTimingPerStatement) {
  std::vector<QueryCompletionInfo> completions;

  for (const auto& sqlText : runner_->splitStatements("SELECT 1; SELECT 2")) {
    if (sqlText.empty()) {
      continue;
    }
    runner_->run(sqlText, {.onComplete = [&](const QueryCompletionInfo& info) {
                   completions.push_back(info);
                 }});
  }

  ASSERT_EQ(completions.size(), 2);
  for (const auto& info : completions) {
    EXPECT_GT(info.timing.parse, 0);
    EXPECT_GT(info.timing.total, 0);
    EXPECT_GE(info.timing.total, info.timing.parse);
  }
}

TEST_F(SqlQueryRunnerTest, totalTimingIncludesAllPhases) {
  QueryCompletionInfo captured;

  // Inject a permission check that sleeps 10ms to create a measurable gap
  // between parse and execute timers.
  auto runner = makeRunner(
      "test_timing",
      {},
      [&](std::string_view /*queryId*/,
          std::string_view /*sql*/,
          std::string_view /*catalog*/,
          std::optional<std::string_view> /*schema*/,
          const auto& /*views*/,
          const auto& /*referencedTables*/) {
        // NOLINTNEXTLINE(facebook-hte-BadCall-sleep_for)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        return std::shared_ptr<facebook::velox::filesystems::TokenProvider>{};
      });

  runner->run("SELECT 1", {.onComplete = [&](const QueryCompletionInfo& info) {
                captured = info;
              }});

  auto phaseSum = captured.timing.parse + captured.timing.optimize +
      captured.timing.execute;
  EXPECT_GT(captured.timing.total, 0);
  EXPECT_GE(captured.timing.total, phaseSum);
  // The permission check timing is now tracked explicitly.
  EXPECT_GT(captured.timing.checkPermission, 5'000)
      << "Expected at least 5ms from the slow permission check, got "
      << captured.timing.checkPermission << "us";
}

TEST_F(SqlQueryRunnerTest, onStartExceptionSwallowed) {
  QueryCompletionInfo captured;
  bool completionFired = false;

  auto result = runner_->run(
      "SELECT 1",
      {.onStart =
           [](const QueryStartInfo&) {
             throw std::runtime_error("start error");
           },
       .onComplete =
           [&](const QueryCompletionInfo& info) {
             captured = info;
             completionFired = true;
           }});

  // Query should still succeed despite onStart throwing.
  EXPECT_FALSE(result.message.has_value());
  ASSERT_EQ(result.results.size(), 1);
  EXPECT_TRUE(completionFired);
  EXPECT_FALSE(captured.errorInfo.has_value());
}

TEST_F(SqlQueryRunnerTest, completionCallbackOnParseFailure) {
  QueryCompletionInfo captured;

  EXPECT_THROW(
      runner_->run(
          "INVALID SYNTAX HERE",
          {.onComplete =
               [&](const QueryCompletionInfo& info) { captured = info; }}),
      std::exception);

  EXPECT_TRUE(captured.errorInfo.has_value());
  EXPECT_GT(captured.timing.parse, 0);
  EXPECT_EQ(captured.timing.optimize, 0);
  EXPECT_EQ(captured.timing.execute, 0);
}

TEST_F(SqlQueryRunnerTest, completionCallbackOnPermissionCheckFailure) {
  QueryCompletionInfo captured;

  auto runner = makeRunner(
      "test_perm_fail",
      {},
      [](std::string_view,
         std::string_view,
         std::string_view,
         std::optional<std::string_view>,
         const auto&,
         const auto&)
          -> std::shared_ptr<facebook::velox::filesystems::TokenProvider> {
        throw std::runtime_error("permission denied");
      });

  EXPECT_THROW(
      runner->run(
          "SELECT 1",
          {.onComplete =
               [&](const QueryCompletionInfo& info) { captured = info; }}),
      std::exception);

  EXPECT_TRUE(captured.errorInfo.has_value());
  EXPECT_THAT(captured.errorInfo->message, ::testing::HasSubstr("permission"));
  EXPECT_GT(captured.timing.parse, 0);
  EXPECT_GT(captured.timing.checkPermission, 0);
  EXPECT_EQ(captured.timing.optimize, 0);
  EXPECT_EQ(captured.timing.execute, 0);
}

TEST_F(SqlQueryRunnerTest, completionCallbackOnOptimizationFailure) {
  // SELECT * FROM nonexistent_table fails during optimization (table not
  // found). Verify onComplete fires with error info and partial timing.
  QueryCompletionInfo captured;

  EXPECT_THROW(
      runner_->run(
          "SELECT * FROM nonexistent_table",
          {.onComplete =
               [&](const QueryCompletionInfo& info) { captured = info; }}),
      std::exception);

  EXPECT_TRUE(captured.errorInfo.has_value());
  EXPECT_GT(captured.timing.parse, 0);
  // Optimization started but failed — timing may be > 0.
  EXPECT_EQ(captured.timing.execute, 0);
  EXPECT_GT(captured.timing.total, 0);
}

TEST_F(SqlQueryRunnerTest, startCallbackFiredBeforeCompletionOrdering) {
  int counter = 0;
  int startOrder = -1;
  int completeOrder = -1;

  runner_->run(
      "SELECT 1",
      {.onStart = [&](const QueryStartInfo&) { startOrder = counter++; },
       .onComplete =
           [&](const QueryCompletionInfo&) { completeOrder = counter++; }});

  EXPECT_EQ(startOrder, 0);
  EXPECT_EQ(completeOrder, 1);
}

TEST_F(SqlQueryRunnerTest, timingFieldsOnParseError) {
  QueryCompletionInfo captured;

  EXPECT_THROW(
      runner_->run(
          "TOTALLY INVALID SQL",
          {.onComplete =
               [&](const QueryCompletionInfo& info) { captured = info; }}),
      std::exception);

  EXPECT_TRUE(captured.errorInfo.has_value());
  EXPECT_GT(captured.timing.parse, 0);
  EXPECT_EQ(captured.timing.checkPermission, 0);
  EXPECT_EQ(captured.timing.optimize, 0);
  EXPECT_EQ(captured.timing.execute, 0);
  EXPECT_GT(captured.timing.total, 0);
  EXPECT_GE(captured.timing.total, captured.timing.parse);
}

TEST_F(SqlQueryRunnerTest, sessionProperties) {
  // Default value.
  assertSessionProperty("optimizer.sample_joins", "true", "true");

  // SET changes the value.
  auto result = run("SET SESSION optimizer.sample_joins = false");
  ASSERT_TRUE(result.message.has_value());
  EXPECT_EQ(*result.message, "Session 'optimizer.sample_joins' set to 'false'");
  assertSessionProperty("optimizer.sample_joins", "false", "true");

  // RESET restores the default.
  result = run("RESET SESSION optimizer.sample_joins");
  ASSERT_TRUE(result.message.has_value());
  EXPECT_EQ(*result.message, "Session 'optimizer.sample_joins' reset");
  assertSessionProperty("optimizer.sample_joins", "true", "true");

  // Invalid value.
  VELOX_ASSERT_THROW(
      run("SET SESSION optimizer.sample_joins = 42"), "Expected boolean value");

  // Unknown property.
  VELOX_ASSERT_THROW(
      run("SET SESSION optimizer.no_such_property = true"),
      "Unknown session property");
}

TEST_F(SqlQueryRunnerTest, showSession) {
  auto fetchNames = [&](std::string_view query) {
    auto sqlResult = run(query);
    VELOX_CHECK(!sqlResult.message.has_value());
    VELOX_CHECK_EQ(1, sqlResult.results.size());

    auto column =
        sqlResult.results[0]->childAt(0)->as<SimpleVector<StringView>>();

    std::vector<std::string> names;
    names.reserve(column->size());
    for (auto i = 0; i < column->size(); ++i) {
      names.emplace_back(column->valueAt(i));
    }
    return names;
  };

  // SHOW SESSION returns all properties.
  auto allNames = fetchNames("SHOW SESSION");
  EXPECT_THAT(allNames, ::testing::Contains("optimizer.sample_joins"));
  EXPECT_THAT(allNames, ::testing::Contains("test.collect_column_statistics"));

  // SHOW SESSION LIKE filters by prefix.
  {
    auto names = fetchNames("SHOW SESSION LIKE 'optimizer%'");
    EXPECT_THAT(names, ::testing::Each(::testing::StartsWith("optimizer.")));
    EXPECT_THAT(names, ::testing::Contains("optimizer.sample_joins"));
  }

  {
    auto names = fetchNames("SHOW SESSION LIKE 'test%'");
    EXPECT_THAT(names, ::testing::Each(::testing::StartsWith("test.")));
    EXPECT_THAT(names, ::testing::Contains("test.collect_column_statistics"));
  }
}

TEST_F(SqlQueryRunnerTest, connectorSessionProperties) {
  // Default value.
  assertSessionProperty("test.collect_column_statistics", "true", "true");

  // SET changes the value.
  auto result = run("SET SESSION test.collect_column_statistics = false");
  ASSERT_TRUE(result.message.has_value());
  EXPECT_EQ(
      *result.message,
      "Session 'test.collect_column_statistics' set to 'false'");
  assertSessionProperty("test.collect_column_statistics", "false", "true");

  // RESET restores the default.
  result = run("RESET SESSION test.collect_column_statistics");
  ASSERT_TRUE(result.message.has_value());
  EXPECT_EQ(*result.message, "Session 'test.collect_column_statistics' reset");
  assertSessionProperty("test.collect_column_statistics", "true", "true");

  // Invalid value.
  VELOX_ASSERT_THROW(
      run("SET SESSION test.collect_column_statistics = 42"),
      "Expected boolean value");
}

// Verifies end-to-end flow: SET SESSION for a connector property reaches the
// connector and affects query behavior.
TEST_F(SqlQueryRunnerTest, connectorSessionPropertyEffect) {
  const Variant null;

  const auto statsType = ROW(
      {
          "row_count",
          "column_name",
          "nulls_fraction",
          "distinct_values_count",
          "avg_length",
          "low_value",
          "high_value",
      },
      {
          BIGINT(),
          VARCHAR(),
          DOUBLE(),
          BIGINT(),
          BIGINT(),
          VARCHAR(),
          VARCHAR(),
      });

  // With stats collection enabled (default), SHOW STATS reports per-column
  // stats.
  {
    run("CREATE TABLE t AS SELECT x FROM unnest(sequence(1, 10)) AS _(x)");
    SCOPE_EXIT {
      run("DROP TABLE IF EXISTS t");
    };

    auto expected = BaseVector::createFromVariants(
        statsType,
        {
            Variant::row({10LL, null, null, null, null, null, null}),
            Variant::row({null, "x", 0.0, 10LL, null, "1", "10"}),
        },
        pool());

    auto result = run("SHOW STATS FOR t");
    ASSERT_FALSE(result.message.has_value());
    ASSERT_EQ(1, result.results.size());
    test::assertEqualVectors(expected, result.results[0]);
  }

  // With stats collection disabled, per-column stats are null.
  {
    run("SET SESSION test.collect_column_statistics = false");
    run("CREATE TABLE t AS SELECT x FROM unnest(sequence(1, 10)) AS _(x)");
    SCOPE_EXIT {
      run("DROP TABLE IF EXISTS t");
    };

    auto expected = BaseVector::createFromVariants(
        statsType,
        {
            Variant::row({10LL, null, null, null, null, null, null}),
            Variant::row({null, "x", null, null, null, null, null}),
        },
        pool());

    auto result = run("SHOW STATS FOR t");
    ASSERT_FALSE(result.message.has_value());
    ASSERT_EQ(1, result.results.size());
    test::assertEqualVectors(expected, result.results[0]);
  }
}

} // namespace
} // namespace axiom::sql

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
