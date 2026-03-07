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
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"
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
      facebook::velox::connector::unregisterConnector(id);
    }
  }

  std::unique_ptr<SqlQueryRunner> makeRunner() {
    auto runner = std::make_unique<SqlQueryRunner>();

    runner->initialize([&]() {
      static int32_t kCounter = 0;

      auto testConnector =
          std::make_shared<facebook::axiom::connector::TestConnector>(
              fmt::format("test{}", kCounter++));
      facebook::velox::connector::registerConnector(testConnector);

      connectorIds_.emplace_back(testConnector->connectorId());

      return std::make_pair(testConnector->connectorId(), std::nullopt);
    });

    return runner;
  }

  std::unique_ptr<SqlQueryRunner> runner_;

 private:
  std::vector<std::string> connectorIds_;
};

TEST_F(SqlQueryRunnerTest, runSingleStatement) {
  auto result = runner_->run("SELECT 1", {});

  ASSERT_FALSE(result.message.has_value());
  ASSERT_EQ(1, result.results.size());
  test::assertEqualVectors(
      result.results[0], makeRowVector({makeFlatVector<int32_t>({1})}));
}

TEST_F(SqlQueryRunnerTest, runRejectsMultipleStatements) {
  EXPECT_THROW(
      runner_->run("SELECT 1; SELECT 2", {}), facebook::velox::VeloxException);
}

TEST_F(SqlQueryRunnerTest, parseAndRunMixedStatementTypes) {
  auto statements = runner_->parseMultiple(
      "SELECT 42; EXPLAIN (TYPE LOGICAL) SELECT 1; select 7", {});
  ASSERT_EQ(3, statements.size());

  // SELECT returns results.
  auto selectResult = runner_->run(*statements[0], {});
  ASSERT_FALSE(selectResult.message.has_value());
  test::assertEqualVectors(
      selectResult.results[0], makeRowVector({makeFlatVector<int32_t>({42})}));

  // EXPLAIN returns a message.
  auto explainResult = runner_->run(*statements[1], {});
  ASSERT_TRUE(explainResult.message.has_value());
  ASSERT_FALSE(explainResult.message.value().empty());

  // Last SELECT returns 7.
  auto lastResult = runner_->run(*statements[2], {});
  ASSERT_FALSE(lastResult.message.has_value());
  test::assertEqualVectors(
      lastResult.results[0], makeRowVector({makeFlatVector<int32_t>({7})}));
}

TEST_F(SqlQueryRunnerTest, multipleRunnerInstances) {
  auto a = makeRunner();
  auto b = makeRunner();

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
  // An invalid query should throw an exception.
  EXPECT_THROW(runner_->run("INVALID SYNTAX HERE", {}), std::exception);
}

TEST_F(SqlQueryRunnerTest, parseMultipleWithInvalidStatement) {
  // Parsing invalid SQL should throw.
  EXPECT_THROW(
      runner_->parseMultiple("SELECT 1; INVALID; SELECT 2", {}),
      std::exception);
}

TEST_F(SqlQueryRunnerTest, showStats) {
  // Create a table with 100 rows and 4 columns: x has 100 distinct values,
  // y has 7 distinct values, z is an array column, w is NULL for every 4th row.
  runner_->run(
      "CREATE TABLE t AS "
      "SELECT x, x % 7 AS y, sequence(1, x % 7 + 1) AS z, "
      "if(x % 4 <> 0, x + x % 7) AS w "
      "FROM unnest(sequence(1, 100)) AS _(x)",
      {});

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

    auto result = runner_->run("SHOW STATS FOR t", {});
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

    result = runner_->run("SHOW STATS FOR (SELECT * FROM t)", {});
    ASSERT_FALSE(result.message.has_value());
    ASSERT_EQ(1, result.results.size());
    test::assertEqualVectors(expected, result.results[0]);
  }
}

// Verifies that SHOW STATS FOR (<query>) reflects optimizer estimates after
// filter pushdown: reduced row count, tightened min/max, capped NDV.
TEST_F(SqlQueryRunnerTest, showStatsForQueryWithFilter) {
  runner_->run(
      "CREATE TABLE t AS "
      "SELECT x, x % 7 AS y, sequence(1, x % 7 + 1) AS z, "
      "if(x % 4 <> 0, x + x % 7) AS w "
      "FROM unnest(sequence(1, 100)) AS _(x)",
      {});

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

  auto result =
      runner_->run("SHOW STATS FOR (SELECT * FROM t WHERE x > 50)", {});
  ASSERT_FALSE(result.message.has_value());
  ASSERT_EQ(1, result.results.size());
  test::assertEqualVectors(expected, result.results[0]);
}

} // namespace
} // namespace axiom::sql

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
