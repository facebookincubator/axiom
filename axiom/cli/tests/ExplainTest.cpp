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

#include <folly/String.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/sql/presto/tests/ExpectPrestoSqlError.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox;

namespace axiom::sql {
namespace {

// Runs EXPLAIN under both optimizer v1 and v2 (the bool parameter).
class ExplainTest : public SqlQueryRunnerTestBase,
                    public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useV2_ = GetParam();
    SqlQueryRunnerTestBase::SetUp();
  }
};

// Plain EXPLAIN annotates each plan node with the optimizer's cardinality
// estimate, drawn from the registered TPC-H statistics and the WHERE
// selectivity: the filter and project carry the post-filter row count, the
// aggregation the distinct l_orderkey count within that range. A single-worker,
// single-driver plan collapses to one Aggregation with no repartition; the two
// optimizers differ only in whether the scan carries an estimate of its own.
// StartsWith covers the Aggregation and Project lines, whose
// optimizer-internal column names differ between v1 and v2.
TEST_P(ExplainTest, showsCardinalityEstimate) {
  testConnector_->addTpchTables(1);

  auto result = runner_->run(
      "EXPLAIN "
      "SELECT l_orderkey, sum(l_extendedprice * l_discount) "
      "FROM lineitem "
      "WHERE l_orderkey < 1000 "
      "GROUP BY 1",
      {.numWorkers = 1, .numDrivers = 1});
  ASSERT_TRUE(result.message.has_value());

  std::vector<std::string> lines;
  folly::split('\n', *result.message, lines);

  using ::testing::Eq;
  using ::testing::StartsWith;

  std::vector<::testing::Matcher<std::string>> expected{
      Eq("Fragment 0: fragment1 SINGLE:"),
      StartsWith("-- Aggregation[3][SINGLE [l_orderkey] sum := sum("),
      Eq("   Estimate: 249.75 rows"),
      StartsWith("  -- Project[2][expressions: (l_orderkey:BIGINT, "),
      Eq("     Estimate: 999.202 rows"),
      StartsWith("    -- Filter[1][expression: lt(\"l_orderkey\",1000)]"),
      Eq("       Estimate: 999.202 rows"),
      StartsWith("      -- TableScan[0][\"default\".\"lineitem\"]"),
  };
  if (useV2_) {
    // v2 reports the rows the scan reads before the filter; v1 prints no
    // estimate for the scan. Both report the rows that pass the filter.
    expected.push_back(Eq("         Estimate: 6.00122e+06 rows"));
  }
  expected.push_back(Eq(""));
  expected.push_back(Eq(""));

  EXPECT_THAT(lines, ::testing::ElementsAreArray(expected));
}

// Presto answers EXPLAIN (TYPE VALIDATE) with one row, column 'result', value
// true, and reports an invalid query as an error rather than false. The format
// option does not change that.
TEST_P(ExplainTest, explainValidate) {
  testConnector_->addTpchTables(1);

  auto expectValid = [&](const std::string& sql) {
    SCOPED_TRACE(sql);
    auto row = fetchSingleRow(sql);
    EXPECT_EQ(*row->type(), *ROW("result", BOOLEAN()));
    EXPECT_TRUE(row->childAt(0)->variantAt(0).value<bool>());
  };

  for (const auto& statement :
       {"SELECT l_orderkey FROM lineitem",
        "CREATE TABLE t AS SELECT l_orderkey FROM lineitem",
        "CREATE TABLE t(x BIGINT)",
        "DROP TABLE IF EXISTS t",
        // A DDL target is not resolved, so a missing table still validates.
        "DROP TABLE nosuchtable",
        // A statement kind EXPLAIN cannot render a plan for is still valid.
        "SHOW TABLES",
        "CREATE SCHEMA s"}) {
    expectValid(fmt::format("EXPLAIN (TYPE VALIDATE) {}", statement));
  }

  for (const auto& format : {"TEXT", "JSON", "GRAPHVIZ"}) {
    expectValid(
        fmt::format(
            "EXPLAIN (TYPE VALIDATE, FORMAT {}) SELECT l_orderkey FROM lineitem",
            format));
  }

  VELOX_ASSERT_THROW(
      run("EXPLAIN (TYPE VALIDATE) SELECT nosuchcolumn FROM lineitem"),
      "Cannot resolve column: nosuchcolumn");

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      run("EXPLAIN (TYPE VALIDATE) SELECT * FROM nosuchtable"),
      "Table not found: nosuchtable");
}

// USE only changes the session, so EXPLAIN reports the statement itself and
// resolves nothing: neither form names a catalog or schema that exists here.
TEST_P(ExplainTest, explainUse) {
  EXPECT_EQ(run("EXPLAIN USE other").message, "USE other");
  EXPECT_EQ(run("EXPLAIN USE test.other").message, "USE test.other");
  EXPECT_EQ(run("EXPLAIN USE nosuchcatalog.s").message, "USE nosuchcatalog.s");

  // EXPLAIN left the session where it was.
  EXPECT_EQ(runner_->defaultConnectorId(), "test");
  EXPECT_EQ(runner_->defaultSchema(), "default");
}

TEST_P(ExplainTest, explainSessionStatements) {
  // SHOW SESSION reads the properties as rows, so it explains like any query,
  // with the LIKE pattern as a filter over them.
  auto explainLines = [&](const std::string& sql) {
    auto result = run(sql);
    EXPECT_TRUE(result.message.has_value());
    std::vector<std::string> lines;
    folly::split('\n', result.message.value_or(""), lines);
    return lines;
  };

  EXPECT_THAT(
      explainLines("EXPLAIN (TYPE LOGICAL) SHOW SESSION"),
      ::testing::ElementsAre(
          ::testing::StartsWith("- Values: "), ::testing::Eq("")));

  EXPECT_THAT(
      explainLines("EXPLAIN (TYPE LOGICAL) SHOW SESSION LIKE 'optimizer.%'"),
      ::testing::ElementsAre(
          ::testing::StartsWith("- Filter: like(Name, optimizer.%) -> ROW<"),
          ::testing::StartsWith("  - Values: "),
          ::testing::Eq("")));

  // A statement that only changes the session has no plan; EXPLAIN reports the
  // statement itself, as written, and leaves the session alone.
  auto result = run("EXPLAIN SET SESSION optimizer.sample_joins = true");
  EXPECT_EQ(result.message, "SET SESSION optimizer.sample_joins = true");

  // The value reads back as a SQL literal: a string keeps its quotes,
  // doubling any it contains, and a number takes its shortest spelling.
  result = run("EXPLAIN SET SESSION optimizer.sample_joins = 'a''b'");
  EXPECT_EQ(result.message, "SET SESSION optimizer.sample_joins = 'a''b'");

  result = run("EXPLAIN SET SESSION optimizer.sample_joins = 1.5");
  EXPECT_EQ(result.message, "SET SESSION optimizer.sample_joins = 1.5");

  result = run("EXPLAIN RESET SESSION optimizer.sample_joins");
  EXPECT_EQ(result.message, "RESET SESSION optimizer.sample_joins");

  // Neither the property nor the catalog is resolved, as in Presto.
  result = run("EXPLAIN SET SESSION no.such_property = 1");
  EXPECT_EQ(result.message, "SET SESSION no.such_property = 1");

  // None of the EXPLAINs above took effect: the property the SET statements
  // name still holds its default, and the USE statements moved nothing.
  EXPECT_EQ(
      runner_->sessionConfig().effectiveValue("optimizer.sample_joins"),
      "false");
  EXPECT_EQ(runner_->defaultSchema(), "default");
  EXPECT_EQ(runner_->defaultConnectorId(), "test");
}

TEST_P(ExplainTest, explainCreateTable) {
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

TEST_P(ExplainTest, explainDropTable) {
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

TEST_P(ExplainTest, explainAddColumn) {
  auto findTable = [&]() {
    auto metadata = facebook::axiom::connector::ConnectorMetadataRegistry::get(
        testConnector_->connectorId());
    return metadata->findTable({"default", "t"});
  };

  run("CREATE TABLE t(x BIGINT)");

  auto result = run("EXPLAIN ALTER TABLE t ADD COLUMN y VARCHAR");
  EXPECT_EQ(
      result.message.value(),
      R"(ALTER TABLE test."default"."t" ADD COLUMN y VARCHAR)");

  // EXPLAIN must not have side effects.
  ASSERT_NE(nullptr, findTable());
  VELOX_ASSERT_EQ_TYPES(findTable()->type(), ROW({"x"}, {BIGINT()}));

  VELOX_ASSERT_THROW(
      run("EXPLAIN ALTER TABLE no_such_table ADD COLUMN y VARCHAR"),
      "Table does not exist");

  result =
      run("EXPLAIN ALTER TABLE IF EXISTS no_such_table ADD COLUMN y VARCHAR");
  EXPECT_EQ(
      result.message.value(),
      R"(ALTER TABLE IF EXISTS test."default"."no_such_table" ADD COLUMN y VARCHAR)");

  // EXPLAIN throws when column already exists and !ifNotExists.
  run("ALTER TABLE t ADD COLUMN dup VARCHAR");
  VELOX_ASSERT_THROW(
      run("EXPLAIN ALTER TABLE t ADD COLUMN dup INTEGER"),
      "Column already exists");

  // EXPLAIN with IF NOT EXISTS on existing column is a no-op (no throw).
  result = run("EXPLAIN ALTER TABLE t ADD COLUMN IF NOT EXISTS dup INTEGER");
  EXPECT_EQ(
      result.message.value(),
      R"(ALTER TABLE test."default"."t" ADD COLUMN IF NOT EXISTS dup INTEGER)");

  // EXPLAIN must not have mutated the column type.
  VELOX_EXPECT_EQ_TYPES(
      findTable()->type(), ROW({"x", "dup"}, {BIGINT(), VARCHAR()}));

  run("DROP TABLE t");
}

TEST_P(ExplainTest, explainCtas) {
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
        ::testing::HasSubstr(
            useV2_ ? "TableWrite" : "TableWrite [1.00 rows] ->"));
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

// `last_pass` stops the v2 pipeline after the named pass, so the IR is the one
// that pass produced rather than the finished plan.
TEST_P(ExplainTest, explainLastPass) {
  testConnector_->addTpchTables(1);

  if (!useV2_) {
    VELOX_ASSERT_USER_THROW(
        run("EXPLAIN (TYPE OPTIMIZED WITH (last_pass = 'translate')) "
            "SELECT 1 AS x"),
        "EXPLAIN settings are not supported with v1");
    return;
  }

  const std::string query =
      "SELECT l_orderkey FROM lineitem WHERE l_orderkey < 1000";

  auto afterTranslate =
      run("EXPLAIN (TYPE OPTIMIZED WITH (last_pass = 'translate')) " + query);
  ASSERT_TRUE(afterTranslate.message.has_value());

  auto afterEveryPass = run("EXPLAIN (TYPE OPTIMIZED) " + query);
  ASSERT_TRUE(afterEveryPass.message.has_value());

  // Estimates appear once leaf statistics have been read, and not before. The
  // values are checked by hand rather than pinned here.
  EXPECT_THAT(
      afterEveryPass.message.value(), ::testing::HasSubstr("Estimate:"));
  EXPECT_THAT(
      afterTranslate.message.value(),
      ::testing::Not(::testing::HasSubstr("Estimate:")));

  // Pushdown gives the scan its connector handle, so the pre-pushdown IR has
  // none.
  EXPECT_THAT(
      afterTranslate.message.value(),
      ::testing::Not(::testing::HasSubstr("handle:")));
  EXPECT_THAT(
      afterEveryPass.message.value(),
      ::testing::HasSubstr("handle: \"default\".\"lineitem\""));

  // Pass names are matched ignoring case.
  {
    auto result =
        run("EXPLAIN (TYPE OPTIMIZED WITH (last_pass = 'PLAN_PHYSICAL')) "
            "SELECT 1 AS x");
    ASSERT_TRUE(result.message.has_value());
  }

  // The message names every pass and echoes the value as written.
  VELOX_ASSERT_USER_THROW(
      run("EXPLAIN (TYPE OPTIMIZED WITH (last_pass = 'NoSuchPass')) "
          "SELECT 1 AS x"),
      "Invalid last_pass value: NoSuchPass. Expected one of (case insensitive): "
      "TRANSLATE, DECORRELATE, LIMIT_AND_ORDER, PUSHDOWN_AND_PRUNE, "
      "FOLD_METADATA_AGGREGATE, ESTIMATE_LEAF_STATS, PLAN_PHYSICAL");

  VELOX_ASSERT_USER_THROW(
      run("EXPLAIN (TYPE OPTIMIZED WITH (nosuchsetting = 'x')) SELECT 1 AS x"),
      "Unrecognized EXPLAIN setting");

  VELOX_ASSERT_USER_THROW(
      run("EXPLAIN (TYPE LOGICAL WITH (last_pass = 'translate')) SELECT 1 AS x"),
      "EXPLAIN settings are supported for TYPE OPTIMIZED only");
}

TEST_P(ExplainTest, explainPopulatesOptimizeTiming) {
  auto runWithTiming = [&](std::string_view sql) {
    QueryTiming timing;
    SqlQueryRunner::RunOptions options{
        .onComplete = [&](const QueryCompletionInfo& info) {
          timing = info.timing;
        }};
    runner_->run(sql, options);
    return timing;
  };

  // EXPLAIN (TYPE LOGICAL) does not populate optimize timing.
  EXPECT_EQ(0, runWithTiming("EXPLAIN (TYPE LOGICAL) SELECT 1").optimize);

  // TYPE GRAPH is v1-only; every other EXPLAIN variant populates optimize
  // timing under both optimizers.
  if (!useV2_) {
    EXPECT_GT(runWithTiming("EXPLAIN (TYPE GRAPH) SELECT 1").optimize, 0);
  }
  EXPECT_GT(runWithTiming("EXPLAIN (TYPE OPTIMIZED) SELECT 1").optimize, 0);
  EXPECT_GT(runWithTiming("EXPLAIN SELECT 1").optimize, 0);
  EXPECT_GT(runWithTiming("EXPLAIN (TYPE IO) SELECT 1").optimize, 0);

  // EXPLAIN ANALYZE populates both optimize and execute timing.
  auto analyzeTiming = runWithTiming("EXPLAIN ANALYZE SELECT 1");
  EXPECT_GT(analyzeTiming.optimize, 0);
  EXPECT_GT(analyzeTiming.execute, 0);
}

TEST_P(ExplainTest, explainFormatGraphviz) {
  // TYPE LOGICAL FORMAT GRAPHVIZ produces DOT output under both optimizers.
  {
    auto graphviz =
        run("EXPLAIN (TYPE LOGICAL, FORMAT GRAPHVIZ) SELECT 1 AS x");
    ASSERT_TRUE(graphviz.message.has_value());
    EXPECT_THAT(graphviz.message.value(), ::testing::HasSubstr("digraph"));

    // Without FORMAT, produces text (no "digraph").
    auto text = run("EXPLAIN (TYPE LOGICAL) SELECT 1 AS x");
    ASSERT_TRUE(text.message.has_value());
    EXPECT_THAT(
        text.message.value(), ::testing::Not(::testing::HasSubstr("digraph")));
  }

  // TYPE GRAPH is v1-only.
  if (useV2_) {
    VELOX_ASSERT_THROW(
        run("EXPLAIN (TYPE GRAPH, FORMAT GRAPHVIZ) SELECT 1 AS x"),
        "EXPLAIN TYPE GRAPH is not supported by the v2 optimizer");
  } else {
    auto graphviz = run("EXPLAIN (TYPE GRAPH, FORMAT GRAPHVIZ) SELECT 1 AS x");
    ASSERT_TRUE(graphviz.message.has_value());
    EXPECT_THAT(graphviz.message.value(), ::testing::HasSubstr("digraph"));

    auto text = run("EXPLAIN (TYPE GRAPH) SELECT 1 AS x");
    ASSERT_TRUE(text.message.has_value());
    EXPECT_THAT(
        text.message.value(), ::testing::Not(::testing::HasSubstr("digraph")));
  }

  // FORMAT GRAPHVIZ with an unsupported TYPE fails.
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

INSTANTIATE_TEST_SUITE_P(
    SqlQueryRunner,
    ExplainTest,
    ::testing::Values(false, true),
    [](const ::testing::TestParamInfo<bool>& info) {
      return info.param ? "v2" : "v1";
    });

} // namespace
} // namespace axiom::sql
