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
#include <folly/coro/Task.h>
#include <folly/dynamic.h>
#include <folly/executors/FunctionScheduler.h>
#include <folly/init/Init.h>
#include <folly/json.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdexcept>
#include <thread>
#include "axiom/cli/QueryIdGenerator.h"
#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/runner/QueryProgress.h"
#include "axiom/sql/presto/PrestoSqlError.h"
#include "axiom/sql/presto/tests/ExpectPrestoSqlError.h"
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace axiom::sql {
namespace {

namespace runner = facebook::axiom::runner;

class SqlQueryRunnerTest : public SqlQueryRunnerTestBase {
 protected:
  // Asserts SHOW SCHEMAS returns exactly 'expected', in order.
  void assertSchemas(const std::vector<std::string>& expected) {
    auto result = run("SHOW SCHEMAS");
    ASSERT_FALSE(result.message.has_value());
    ASSERT_EQ(1, result.results.size());
    test::assertEqualVectors(
        result.results[0],
        makeRowVector({"Schema"}, {makeFlatVector(expected)}));
  }

  // Asserts 'query' (SHOW TABLES by default) returns exactly 'expected', in
  // order.
  void assertTables(
      const std::vector<std::string>& expected,
      const std::string& query = "SHOW TABLES") {
    auto result = run(query);
    ASSERT_FALSE(result.message.has_value());
    ASSERT_EQ(1, result.results.size());
    test::assertEqualVectors(
        result.results[0],
        makeRowVector({"Table"}, {makeFlatVector(expected)}));
  }

  // Asserts SHOW SESSION LIKE 'name' reports the given current value and
  // default.
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
};

TEST_F(SqlQueryRunnerTest, runSingleStatement) {
  test::assertEqualVectors(
      fetchSingleRow("SELECT 1"),
      makeRowVector({makeFlatVector<int32_t>({1})}));
}

TEST_F(SqlQueryRunnerTest, executionTimeout) {
  // A six-way cross join over 25 rows produces 25^6 = ~244M rows, far more than
  // can be processed within the 10ms deadline, and the max of the concatenated
  // values cannot be short-circuited, so the run is still executing when the
  // deadline elapses and fails with a timeout user error. A single worker keeps
  // the plan to one fragment with no exchange: cancelling a multi-stage query
  // can leave a LocalExchangeSource pinning its memory pool until process exit
  // (the arbitrator then aborts), and a single-fragment plan avoids that.
  testConnector_->addTable("t", ROW("s", VARCHAR()))
      ->addData(makeRowVector({makeFlatVector<std::string>(
          25, [](auto row) { return fmt::format("value_{:04d}", row); })}));

  SqlQueryRunner::RunOptions options;
  options.timeoutMicros = 10'000; // 10ms
  options.numWorkers = 1;
  options.numDrivers = 1;
  VELOX_ASSERT_THROW(
      runner_->run(
          "SELECT max(a.s || b.s || c.s || d.s || e.s || f.s) "
          "FROM t a, t b, t c, t d, t e, t f",
          options),
      "exceeded maximum time limit");
}

TEST_F(SqlQueryRunnerTest, currentUser) {
  // CURRENT_USER returns the user passed to the SqlQueryRunner constructor.
  test::assertEqualVectors(
      fetchSingleRow("SELECT CURRENT_USER"),
      makeRowVector({makeFlatVector<std::string>({"test_user"})}));
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

TEST_F(SqlQueryRunnerTest, createTableRunsWritePath) {
  // Connectors that commit a new table in finishWrite (not createTable) rely on
  // CREATE TABLE running the write path. Inject a finishWrite error to confirm
  // it runs.
  run("SET SESSION test.finish_write_error = 'boom from finishWrite'");

  // EXPLAIN does not run the write path, so it does not hit the error.
  run("EXPLAIN CREATE TABLE t(x int)");

  // A real CREATE runs the write path, surfacing the commit error.
  VELOX_ASSERT_THROW(run("CREATE TABLE t(x int)"), "boom from finishWrite");
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

TEST_F(SqlQueryRunnerTest, showTables) {
  run("CREATE TABLE alpha AS SELECT 1 AS x");
  run("CREATE TABLE beta AS SELECT 2 AS y");
  SCOPE_EXIT {
    run("DROP TABLE IF EXISTS alpha");
    run("DROP TABLE IF EXISTS beta");
  };

  assertTables({"alpha", "beta"});
  assertTables({"alpha"}, "SHOW TABLES LIKE 'a%'");
}

TEST_F(SqlQueryRunnerTest, showCreateTable) {
  {
    run("CREATE TABLE t1 (id INTEGER, name VARCHAR)");
    auto ddl = fetchSingleValue<std::string>("SHOW CREATE TABLE t1");
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
    auto ddl = fetchSingleValue<std::string>("SHOW CREATE TABLE t2");
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

TEST_F(SqlQueryRunnerTest, showCreateView) {
  testConnector_->createView(
      facebook::axiom::SchemaTableName{kDefaultSchema, "v1"},
      ROW({"a", "b"}, {INTEGER(), VARCHAR()}),
      "SELECT 1 AS a, 'x' AS b");

  auto ddl = fetchSingleValue<std::string>("SHOW CREATE VIEW v1");
  EXPECT_EQ(
      ddl,
      "CREATE VIEW test.\"default\".\"v1\" AS\n"
      "SELECT 1 AS a, 'x' AS b");

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      run("SHOW CREATE VIEW no_such_view"), "View not found");
}

TEST_F(SqlQueryRunnerTest, tableSampleSystem) {
  // One split per appended row vector, so sampling drops whole splits.
  testConnector_->addTable("sampled", ROW("x", INTEGER()));
  auto vector = makeRowVector({makeFlatVector<int32_t>({1})});
  constexpr int64_t kNumSplits = 1000;
  for (int64_t i = 0; i < kNumSplits; ++i) {
    testConnector_->appendData("sampled", vector);
  }

  auto sample = [&](double percentage) {
    return fetchSingleValue<int64_t>(fmt::format(
        "SELECT count(*) FROM sampled TABLESAMPLE SYSTEM ({})", percentage));
  };

  EXPECT_EQ(
      fetchSingleValue<int64_t>("SELECT count(*) FROM sampled"), kNumSplits);

  // 0% produces an empty result; 100% is a no-op passthrough.
  EXPECT_EQ(sample(0), 0);
  EXPECT_EQ(sample(100), kNumSplits);

  // Intermediate rates keep some but not all splits. Counts are exact because
  // the split coin flip is deterministically seeded.
  EXPECT_EQ(sample(0.1), 2);
  EXPECT_EQ(sample(1), 10);
  EXPECT_EQ(sample(20), 176);
  EXPECT_EQ(sample(50), 493);

  // SYSTEM sampling has no single split source to sample over a join.
  VELOX_ASSERT_THROW(
      run("SELECT count(*) FROM (SELECT a.x FROM sampled a, sampled b) "
          "TABLESAMPLE SYSTEM (50)"),
      "TABLESAMPLE SYSTEM is only supported directly over a table");
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
  std::string progressQueryId;

  runner_->run(
      "SELECT 1",
      {.onStart =
           [&](const QueryStartInfo& info) {
             startQueryId = info.queryId;
             EXPECT_EQ(info.query, "SELECT 1");
           },
       .onProgress =
           [&](const runner::QueryProgress& info) {
             progressQueryId = info.queryId;
           },
       .onComplete =
           [&](const QueryCompletionInfo& info) {
             completionQueryId = info.startInfo.queryId;
           }});

  EXPECT_FALSE(startQueryId.empty());
  EXPECT_EQ(startQueryId, completionQueryId);
  // run() forwards onProgress to the runner; the teardown report carries the
  // id.
  EXPECT_EQ(progressQueryId, startQueryId);
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

// Captures the parameterized template, distinct from the substituted message,
// so similar failures can be grouped.
TEST_F(SqlQueryRunnerTest, completionCapturesFailureMessageTemplate) {
  QueryCompletionInfo captured;

  EXPECT_THROW(
      runner_->run(
          "SELECT * FROM nonexistent_table",
          {.onComplete =
               [&](const QueryCompletionInfo& info) { captured = info; }}),
      std::exception);

  ASSERT_TRUE(captured.errorInfo.has_value());
  EXPECT_EQ(captured.errorInfo->messageTemplate, "Table not found: {}");
  EXPECT_NE(captured.errorInfo->messageTemplate, captured.errorInfo->message);
}

TEST(MessageTemplateOfTest, veloxExceptionUsesTemplate) {
  try {
    VELOX_USER_FAIL("Not supported: {}", "rank");
    FAIL() << "expected VeloxUserError";
  } catch (const VeloxException& e) {
    EXPECT_EQ(messageTemplateOf(e), "Not supported: {}");
  }
}

// A messageless VELOX_CHECK has no template; synthesize a groupable key from
// its failing expression (mirroring QueryError::create) instead of logging
// nothing.
TEST(MessageTemplateOfTest, messagelessVeloxCheckSynthesizesFromExpression) {
  try {
    VELOX_CHECK(false);
    FAIL() << "expected VeloxRuntimeError";
  } catch (const VeloxException& e) {
    EXPECT_EQ(messageTemplateOf(e), "Check failed: false");
  }
}

// A VELOX_FAIL literal has no template (VeloxException returns the message), so
// it logs as-is, matching AxelQueryLogger. Pinned so a fix doesn't diverge.
TEST(MessageTemplateOfTest, veloxFailLiteralReturnsMessageMatchingAxel) {
  try {
    VELOX_FAIL("disk full");
    FAIL() << "expected VeloxRuntimeError";
  } catch (const VeloxException& e) {
    EXPECT_EQ(messageTemplateOf(e), "disk full");
  }
}

TEST(MessageTemplateOfTest, prestoSqlErrorUsesTemplate) {
  const presto::PrestoSqlError error(
      "Table not found: 'orders'",
      1,
      0,
      std::nullopt,
      presto::PrestoSqlErrorKind::kSemantic,
      "Table not found: {}");
  EXPECT_EQ(messageTemplateOf(error), "Table not found: {}");
}

// A PrestoSqlError without a template returns empty -- a substituted message is
// not a groupable template, so the caller omits the field.
TEST(MessageTemplateOfTest, prestoSqlErrorWithoutTemplateReturnsEmpty) {
  const presto::PrestoSqlError error("some parser error", 1, 0, std::nullopt);
  EXPECT_TRUE(messageTemplateOf(error).empty());
}

// Any other exception has no template; returns empty so the field is omitted.
TEST(MessageTemplateOfTest, plainExceptionReturnsEmpty) {
  const std::runtime_error error("connection reset");
  EXPECT_TRUE(messageTemplateOf(error).empty());
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

TEST_F(SqlQueryRunnerTest, classifiesSyntaxErrorAsUser) {
  QueryCompletionInfo captured;
  EXPECT_THROW(
      runner_->run(
          "SELECT * FROM",
          {.onComplete =
               [&](const QueryCompletionInfo& info) { captured = info; }}),
      std::exception);
  ASSERT_TRUE(captured.errorInfo.has_value());
  EXPECT_EQ(captured.errorInfo->errorSource, error_source::kErrorSourceUser);
  EXPECT_EQ(captured.errorInfo->errorCode, "SYNTAX_ERROR");
}

TEST_F(SqlQueryRunnerTest, classifiesSemanticErrorAsUser) {
  QueryCompletionInfo captured;
  EXPECT_THROW(
      runner_->run(
          "SELECT * FROM nonexistent_table",
          {.onComplete =
               [&](const QueryCompletionInfo& info) { captured = info; }}),
      std::exception);
  ASSERT_TRUE(captured.errorInfo.has_value());
  EXPECT_EQ(captured.errorInfo->errorSource, error_source::kErrorSourceUser);
  EXPECT_EQ(captured.errorInfo->errorCode, "GENERIC_USER_ERROR");
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
  assertSessionProperty("optimizer.sample_joins", "false", "false");

  // SET changes the value.
  auto result = run("SET SESSION optimizer.sample_joins = true");
  ASSERT_TRUE(result.message.has_value());
  EXPECT_EQ(*result.message, "Session 'optimizer.sample_joins' set to 'true'");
  assertSessionProperty("optimizer.sample_joins", "true", "false");

  // RESET restores the default.
  result = run("RESET SESSION optimizer.sample_joins");
  ASSERT_TRUE(result.message.has_value());
  EXPECT_EQ(*result.message, "Session 'optimizer.sample_joins' reset");
  assertSessionProperty("optimizer.sample_joins", "false", "false");

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

// Verifies that a SET SESSION connector property is visible to the
// connector during execution-time split generation.
TEST_F(SqlQueryRunnerTest, connectorProperties) {
  run("CREATE TABLE t AS SELECT x FROM unnest(sequence(1, 3)) AS _(x)");
  SCOPE_EXIT {
    run("DROP TABLE IF EXISTS t");
  };

  run("SET SESSION test.list_partitions_error = 'boom from split-gen'");

  VELOX_ASSERT_THROW(run("SELECT * FROM t"), "boom from split-gen");
}

TEST_F(SqlQueryRunnerTest, addColumn) {
  auto findTable = [&]() {
    auto metadata = facebook::axiom::connector::ConnectorMetadataRegistry::get(
        testConnector_->connectorId());
    return metadata->findTable({"default", "t"});
  };

  // Basic add followed by schema verification.
  run("CREATE TABLE t(x BIGINT)");
  auto result = run("ALTER TABLE t ADD COLUMN y VARCHAR");
  EXPECT_EQ(result.message.value(), R"(Added column 'y' to "default"."t")");
  VELOX_ASSERT_EQ_TYPES(
      findTable()->type(), ROW({"x", "y"}, {BIGINT(), VARCHAR()}));

  // Multiple sequential adds with complex types.
  run("ALTER TABLE t ADD COLUMN z DOUBLE");
  run("ALTER TABLE t ADD COLUMN w ARRAY(INTEGER)");
  VELOX_ASSERT_EQ_TYPES(
      findTable()->type(),
      ROW({"x", "y", "z", "w"},
          {BIGINT(), VARCHAR(), DOUBLE(), ARRAY(INTEGER())}));

  // Duplicate column without IF NOT EXISTS throws.
  VELOX_ASSERT_THROW(
      run("ALTER TABLE t ADD COLUMN x INTEGER"), "already exists");

  // IF NOT EXISTS is idempotent: only the name is checked, and a type mismatch
  // is ignored.
  result = run("ALTER TABLE t ADD COLUMN IF NOT EXISTS x VARCHAR");
  EXPECT_EQ(
      result.message.value(),
      R"(Column 'x' already exists in "default"."t" (no-op))");
  VELOX_ASSERT_EQ_TYPES(
      findTable()->type(),
      ROW({"x", "y", "z", "w"},
          {BIGINT(), VARCHAR(), DOUBLE(), ARRAY(INTEGER())}));

  // Without IF NOT EXISTS, ADD COLUMN still throws after an idempotent call.
  VELOX_ASSERT_THROW(
      run("ALTER TABLE t ADD COLUMN x VARCHAR"), "already exists");

  run("DROP TABLE t");

  // IF TABLE EXISTS on a missing table is a no-op.
  result = run("ALTER TABLE IF EXISTS no_such_table ADD COLUMN y VARCHAR");
  EXPECT_EQ(
      result.message.value(),
      R"(Table does not exist: test."default"."no_such_table")");

  // Without IF TABLE EXISTS, ADD COLUMN on a missing table throws.
  VELOX_ASSERT_THROW(
      run("ALTER TABLE no_such_table ADD COLUMN y VARCHAR"),
      "Table does not exist");
}

TEST_F(SqlQueryRunnerTest, call) {
  // increment(name, step = 1) adds 'step' to the named counter, exercising the
  // full CALL path: parse, bind, constant-fold arguments, and execute.
  auto counters = std::make_shared<std::unordered_map<std::string, int64_t>>();
  testConnector_->metadata()->addProcedure(
      {kDefaultSchema, "increment"},
      facebook::axiom::connector::Procedure{
          .parameters =
              {{"name", VARCHAR(), std::nullopt},
               {"step", BIGINT(), Variant(static_cast<int64_t>(1))}},
          .execute = [counters](
                         const facebook::axiom::connector::ConnectorSessionPtr&,
                         const std::vector<Variant>& arguments)
              -> folly::coro::Task<void> {
            (*counters)[arguments[0].value<std::string>()] +=
                arguments[1].value<int64_t>();
            co_return;
          }});

  // Omitted 'step' defaults to 1.
  auto result = run("CALL increment('foo')");
  EXPECT_EQ(result.message, "CALL");
  EXPECT_EQ((*counters)["foo"], 1);

  run("CALL increment('bar')");
  EXPECT_EQ((*counters)["bar"], 1);

  // Explicit 'step' is coerced to BIGINT and applied.
  run("CALL increment('foo', 10)");
  EXPECT_EQ((*counters)["foo"], 11);

  run("CALL increment('foo', 1 + 2)");
  EXPECT_EQ((*counters)["foo"], 14);

  // A constant-expression argument that fails to fold surfaces the error.
  VELOX_ASSERT_THROW(run("CALL increment('foo', 1 / 0)"), "division by zero");

  {
    // EXPLAIN describes the CALL without invoking it: the counter is unchanged.
    auto explained = run("EXPLAIN CALL increment('foo', 100)");
    EXPECT_EQ(
        explained.message,
        "CALL test.default.increment(foo, CAST(100 AS BIGINT))");
    EXPECT_EQ((*counters)["foo"], 14);
  }

  {
    auto explained = run("EXPLAIN CALL increment('foo')");
    EXPECT_EQ(explained.message, "CALL test.default.increment(foo, 1)");
    EXPECT_EQ((*counters)["foo"], 14);
  }

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      run("EXPLAIN CALL increment('foo', 'bar')"),
      "Cannot coerce procedure argument from VARCHAR to BIGINT");
}

} // namespace
} // namespace axiom::sql

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
