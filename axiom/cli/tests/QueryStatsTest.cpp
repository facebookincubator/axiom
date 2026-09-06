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

#include <gtest/gtest.h>

#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"
#include "axiom/optimizer/Schema.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/Type.h"

namespace axiom::sql {
namespace {

constexpr float kTableRows = 100;

float estimatedValueBytes(const facebook::velox::TypePtr& type) {
  return facebook::axiom::optimizer::Value(type.get()).byteSize();
}

class QueryStatsTest : public SqlQueryRunnerTestBase {};

TEST_F(QueryStatsTest, filteredOutputWithMissingScanStats) {
  runner_->run(
      "CREATE TABLE t AS "
      "SELECT x FROM unnest(sequence(1, 100)) AS _(x)",
      {});

  const auto stats =
      runner_->estimateQueryStats("SELECT x FROM t WHERE x > 50", {});
  const auto rowBytes = estimatedValueBytes(facebook::velox::BIGINT());
  const auto expectedOutputRows = kTableRows / 2;

  EXPECT_EQ(stats.inputRows, std::nullopt);
  EXPECT_EQ(stats.inputBytes, std::nullopt);
  EXPECT_EQ(stats.outputRows, expectedOutputRows);
  EXPECT_EQ(stats.outputBytes, expectedOutputRows * rowBytes);
}

TEST_F(QueryStatsTest, scanlessQuery) {
  const auto stats = runner_->estimateQueryStats("SELECT 1", {});

  EXPECT_EQ(stats.inputRows, 0);
  EXPECT_EQ(stats.inputBytes, 0);
  EXPECT_EQ(stats.outputRows, 1);
  EXPECT_EQ(stats.outputBytes, estimatedValueBytes(facebook::velox::INTEGER()));
}

TEST_F(QueryStatsTest, insertQuery) {
  runner_->run("CREATE TABLE t (x INTEGER)", {});

  const auto stats =
      runner_->estimateQueryStats("INSERT INTO t VALUES 1, 2, 3", {});
  const auto outputRowBytes = estimatedValueBytes(facebook::velox::INTEGER());

  EXPECT_EQ(stats.inputRows, 0);
  EXPECT_EQ(stats.inputBytes, 0);
  EXPECT_EQ(stats.outputRows, 3);
  EXPECT_EQ(stats.outputBytes, 3 * outputRowBytes);
}

TEST_F(QueryStatsTest, createTableAsSelectQuery) {
  const auto stats =
      runner_->estimateQueryStats("CREATE TABLE t AS SELECT 1 AS x", {});

  EXPECT_EQ(stats.inputRows, 0);
  EXPECT_EQ(stats.inputBytes, 0);
  EXPECT_EQ(stats.outputRows, 1);
  EXPECT_EQ(stats.outputBytes, estimatedValueBytes(facebook::velox::INTEGER()));

  const auto result = runner_->run("CREATE TABLE t AS SELECT 1 AS x", {});
  ASSERT_EQ(result.results.size(), 1);
  EXPECT_EQ(result.results.front()->size(), 1);
}

TEST_F(QueryStatsTest, logicalPlan) {
  const auto statement = runner_->parseSingle("SELECT 1", {});
  const auto& plan = statement->as<presto::SelectStatement>()->plan();

  const auto stats = runner_->estimateQueryStats(*plan, {});

  EXPECT_EQ(stats.inputRows, 0);
  EXPECT_EQ(stats.inputBytes, 0);
  EXPECT_EQ(stats.outputRows, 1);
  EXPECT_EQ(stats.outputBytes, estimatedValueBytes(facebook::velox::INTEGER()));
}

TEST_F(QueryStatsTest, wrappedQueries) {
  for (const auto sql : {"EXPLAIN SELECT 1", "SHOW STATS FOR (SELECT 1)"}) {
    SCOPED_TRACE(sql);
    const auto stats = runner_->estimateQueryStats(sql, {});

    EXPECT_EQ(stats.inputRows, 0);
    EXPECT_EQ(stats.inputBytes, 0);
    EXPECT_EQ(stats.outputRows, 1);
    EXPECT_EQ(
        stats.outputBytes, estimatedValueBytes(facebook::velox::INTEGER()));
  }
}

TEST_F(QueryStatsTest, rejectsStatementWithoutPlan) {
  VELOX_ASSERT_USER_THROW(
      runner_->estimateQueryStats("SHOW SESSION", {}),
      "Query statistics require a statement with a logical plan, got: SHOW SESSION");
}

TEST_F(QueryStatsTest, checksPermission) {
  bool permissionChecked{false};
  runner_ = makeRunner(
      "query_stats_permission",
      /*queryIdGenerator=*/{},
      [&](std::string_view queryId,
          std::string_view sql,
          std::string_view catalog,
          std::optional<std::string_view> /*schema*/,
          const auto& /*views*/,
          const auto& /*referencedTables*/)
          -> std::shared_ptr<facebook::velox::filesystems::TokenProvider> {
        EXPECT_EQ(queryId, "stats_query");
        EXPECT_EQ(sql, "SELECT 1");
        EXPECT_EQ(catalog, "query_stats_permission");
        permissionChecked = true;
        return nullptr;
      });
  SqlQueryRunner::RunOptions options;
  options.queryId = "stats_query";

  runner_->estimateQueryStats("SELECT 1", options);

  EXPECT_TRUE(permissionChecked);
}

} // namespace
} // namespace axiom::sql
