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
#include <gmock/gmock.h>
#include "axiom/cli/Connectors.h"
#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace axiom::sql {
namespace {
using namespace facebook::velox;
using namespace facebook::axiom::connector;

class AnalyzeTest : public SqlQueryRunnerTestBase {
 protected:
  void registerTpch() {
    runner_.connectors().registerTpchConnector("tpch");
  }

  TestConnectorMetadata& metadata() {
    return *testConnector_->metadata();
  }
  std::shared_ptr<TestTable> createTableWithData() {
    auto table = testConnector_->addTable(
        "t", ROW({"x", "b", "s"}, {BIGINT(), BOOLEAN(), VARCHAR()}));
    testConnector_->appendData(
        "t",
        makeRowVector(
            {"x", "b", "s"},
            {makeNullableFlatVector<int64_t>({1, 2, 2, std::nullopt}),
             makeNullableFlatVector<bool>({true, false, true, std::nullopt}),
             makeNullableFlatVector<std::string>(
                 {"a", "bb", "a", std::nullopt})}));
    return table;
  }
  const ColumnStatistics& stat(
      const TestConnectorMetadata::AnalyzedStatistics& result,
      const std::string& name,
      size_t group = 0) {
    for (const auto& column : result.groupStats.at(group)) {
      if (column.name == name) {
        return column;
      }
    }
    VELOX_FAIL("Missing statistics for {}", name);
  }
};

// The statistics are the same however the plan splits the work: in one step,
// across drivers, and across workers.
TEST_F(AnalyzeTest, standard) {
  createTableWithData();
  for (const auto& [numWorkers, numDrivers] :
       std::vector<std::pair<int32_t, int32_t>>{{1, 1}, {1, 4}, {3, 2}}) {
    SCOPED_TRACE(fmt::format("{} workers, {} drivers", numWorkers, numDrivers));
    metadata().analyzedStatistics.reset();
    auto result = fetchSingleRow(
        "ANALYZE t", {.numWorkers = numWorkers, .numDrivers = numDrivers});
    EXPECT_EQ(result->rowType()->nameOf(0), "rows");
    EXPECT_EQ(result->childAt(0)->variantAt(0).value<int64_t>(), 4);
    ASSERT_TRUE(metadata().analyzedStatistics);
    const auto& stats = *metadata().analyzedStatistics;
    ASSERT_EQ(stats.groupingKeys->size(), 1);
    EXPECT_EQ(stats.groupingKeys->childAt(0)->variantAt(0).value<int64_t>(), 4);
    const auto& x = stat(stats, "x");
    EXPECT_EQ(x.numValues, 3);
    EXPECT_FLOAT_EQ(x.nullPct, 25);
    EXPECT_EQ(x.min->value<int64_t>(), 1);
    EXPECT_EQ(x.max->value<int64_t>(), 2);
    EXPECT_EQ(x.numDistinct, 2);
    const auto& b = stat(stats, "b");
    EXPECT_EQ(b.numValues, 3);
    EXPECT_EQ(b.numDistinct, 2);
    EXPECT_FLOAT_EQ(b.nullPct, 25);
    // The three strings hold 4 bytes of text plus a 4-byte length each: 16
    // bytes over 4 rows.
    EXPECT_EQ(stat(stats, "s").avgSizeInBytes, 4);
  }
}

TEST_F(AnalyzeTest, approximateDistinctCount) {
  testConnector_->addTable("t", ROW({"x"}, {BIGINT()}));
  testConnector_->appendData(
      "t", makeRowVector({"x"}, {makeFlatVector<int64_t>(10'000, [](auto row) {
                           return row % 1'000;
                         })}));
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 10'000);
  const auto& stats = stat(*metadata().analyzedStatistics, "x");
  ASSERT_TRUE(stats.numDistinct);
  EXPECT_NEAR(*stats.numDistinct, 1'000, 100);
}

// ANALYZE reads only the chosen columns. With none chosen it still counts the
// rows.
TEST_F(AnalyzeTest, columns) {
  createTableWithData();
  run("ANALYZE t WITH (columns = ARRAY['x'])");
  ASSERT_TRUE(metadata().analyzedStatistics);
  EXPECT_THAT(
      metadata().analyzedStatistics->groupStats,
      testing::ElementsAre(
          testing::ElementsAre(testing::Field(&ColumnStatistics::name, "x"))));
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t WITH (columns = ARRAY[])"), 4);
  EXPECT_THAT(
      metadata().analyzedStatistics->groupStats,
      testing::ElementsAre(testing::IsEmpty()));
  EXPECT_EQ(metadata().analyzedStatistics->numRows(), 4);
  const auto table = metadata().findTable({"default", "t"});
  EXPECT_FALSE(table->findColumn("x")->stats());
  EXPECT_FALSE(table->findColumn("b")->stats());
  EXPECT_FALSE(table->findColumn("s")->stats());
}

// ANALYZE of a bucketed table reads the chosen columns where the scan produces
// them. Unlike a write, it does not shuffle the rows into the table's buckets.
TEST_F(AnalyzeTest, bucketedTable) {
  testConnector_->addTable(
      "t", ROW({"k", "v"}, BIGINT()), ROW({}), TestBucketSpec{{"k"}, 8});
  testConnector_->appendData(
      "t",
      makeRowVector(
          {"k", "v"},
          {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
           makeFlatVector<int64_t>(100, [](auto row) { return row % 10; })}));
  const SqlQueryRunner::RunOptions options{.numWorkers = 3, .numDrivers = 2};
  constexpr auto kSql = "ANALYZE t WITH (columns = ARRAY['v'])";
  EXPECT_THAT(
      runner_->run(fmt::format("EXPLAIN {}", kSql), options).message.value(),
      testing::Not(testing::HasSubstr("HASH")));
  EXPECT_EQ(fetchSingleValue<int64_t>(kSql, options), 100);
  EXPECT_EQ(stat(*metadata().analyzedStatistics, "v").numDistinct, 10);
}

// EXPLAIN shows ANALYZE without committing anything. EXPLAIN ANALYZE runs it,
// as it runs a write.
TEST_F(AnalyzeTest, explainAndCommitFailure) {
  createTableWithData();
  int commits{0};
  metadata().onFinishWrite = [&](const auto&, const auto&, const auto&) {
    ++commits;
    VELOX_USER_FAIL("commit failed");
  };
  for (const auto* sql :
       {"EXPLAIN ANALYZE t",
        "EXPLAIN (TYPE OPTIMIZED) ANALYZE t",
        "EXPLAIN (TYPE IO) ANALYZE t",
        "EXPLAIN (TYPE VALIDATE) ANALYZE t"}) {
    run(sql);
    EXPECT_EQ(commits, 0);
    EXPECT_FALSE(metadata().analyzedStatistics);
  }
  VELOX_ASSERT_THROW(run("ANALYZE t"), "commit failed");
  EXPECT_EQ(commits, 1);
  VELOX_ASSERT_THROW(run("EXPLAIN ANALYZE ANALYZE t"), "commit failed");
  EXPECT_EQ(commits, 2);
  EXPECT_FALSE(metadata().analyzedStatistics);
}

TEST_F(AnalyzeTest, emptyAndAllNull) {
  testConnector_->addTable(
      "t", ROW({"x", "b", "s"}, {BIGINT(), BOOLEAN(), VARCHAR()}));
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 0);
  auto stats = *metadata().analyzedStatistics;
  EXPECT_EQ(stat(stats, "x").numValues, 0);
  EXPECT_FLOAT_EQ(stat(stats, "x").nullPct, 0);
  EXPECT_EQ(stat(stats, "x").numDistinct, 0);
  EXPECT_EQ(stat(stats, "b").numDistinct, 0);
  // With no rows there is no average size.
  EXPECT_FALSE(stat(stats, "s").avgSizeInBytes);
  testConnector_->appendData(
      "t",
      makeRowVector(
          {"x", "b", "s"},
          {makeNullableFlatVector<int64_t>({std::nullopt}),
           makeNullableFlatVector<bool>({std::nullopt}),
           makeNullableFlatVector<std::string>({std::nullopt})}));
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 1);
  stats = *metadata().analyzedStatistics;
  EXPECT_EQ(stat(stats, "x").numValues, 0);
  EXPECT_FLOAT_EQ(stat(stats, "x").nullPct, 100);
  EXPECT_FALSE(stat(stats, "x").min);
  EXPECT_EQ(stat(stats, "b").numDistinct, 0);
  EXPECT_EQ(stat(stats, "s").avgSizeInBytes, 0);
}

// A table can group its statistics, for example by partition. A null key is a
// group of its own, as in GROUP BY. ANALYZE reads and analyzes the grouping
// column even when the column list leaves it out.
TEST_F(AnalyzeTest, grouped) {
  testConnector_->addTable("t", ROW({"x", "b"}, {BIGINT(), BOOLEAN()}))
      ->setStatsGroupingKeys({"x"});
  testConnector_->appendData(
      "t",
      makeRowVector(
          {"x", "b"},
          {makeNullableFlatVector<int64_t>({2, 2, 2, std::nullopt}),
           makeNullableFlatVector<bool>(
               {false, true, std::nullopt, std::nullopt})}));

  EXPECT_EQ(
      fetchSingleValue<int64_t>(
          "ANALYZE t WITH (columns = ARRAY['b'])",
          {.numWorkers = 3, .numDrivers = 2}),
      4);
  ASSERT_TRUE(metadata().analyzedStatistics);
  const auto& stats = *metadata().analyzedStatistics;
  ASSERT_NE(stats.groupingKeys, nullptr);
  ASSERT_EQ(stats.groupingKeys->size(), 2);
  const auto& keys = stats.groupingKeys->childAt(0);
  const auto& rows = stats.groupingKeys->childAt(1);
  const auto nullGroup = keys->isNullAt(0) ? 0 : 1;
  const auto valueGroup = 1 - nullGroup;

  EXPECT_TRUE(keys->isNullAt(nullGroup));
  EXPECT_EQ(rows->variantAt(nullGroup).value<int64_t>(), 1);
  EXPECT_EQ(stat(stats, "b", nullGroup).numValues, 0);

  EXPECT_EQ(keys->variantAt(valueGroup), Variant(int64_t{2}));
  EXPECT_EQ(rows->variantAt(valueGroup).value<int64_t>(), 3);
  EXPECT_EQ(stat(stats, "x", valueGroup).numDistinct, 1);
  const auto& b = stat(stats, "b", valueGroup);
  EXPECT_EQ(b.numValues, 2);
  EXPECT_EQ(b.numDistinct, 2);
  EXPECT_FLOAT_EQ(b.nullPct, 100.0f / 3);
}

// A grouped ANALYZE of an empty table finds no groups.
TEST_F(AnalyzeTest, emptyGroups) {
  testConnector_->addTable("t", ROW({"x"}, BIGINT()))
      ->setStatsGroupingKeys({"x"});
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 0);
  ASSERT_TRUE(metadata().analyzedStatistics);
  EXPECT_EQ(metadata().analyzedStatistics->groupingKeys, nullptr);
}

// ANALYZE skips a statistic when its aggregate can't take the column's type,
// as writes do. JSON has no min, max or distinct count.
TEST_F(AnalyzeTest, leavesOutUnsupportedStatistics) {
  testConnector_->addTable("t", ROW({"a", "b"}, {JSON(), BIGINT()}));
  testConnector_->appendData(
      "t",
      makeRowVector(
          {"a", "b"},
          {makeFlatVector<std::string>({"1", "2"}, JSON()),
           makeFlatVector<int64_t>({1, 2})}));

  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 2);
  const auto& a = stat(*metadata().analyzedStatistics, "a");
  EXPECT_EQ(a.numValues, 2);
  EXPECT_FALSE(a.min.has_value());
  EXPECT_FALSE(a.max.has_value());
  EXPECT_FALSE(a.numDistinct.has_value());
  EXPECT_EQ(stat(*metadata().analyzedStatistics, "b").numDistinct, 2);
}

// ANALYZE reads and writes its table, and the plan check sees its plan, as it
// sees a write's.
TEST_F(AnalyzeTest, permissionsAndPlanCheck) {
  int checks{0};
  int plans{0};
  runner_ = makeRunner(
      "analyze_permissions",
      {},
      [&](std::string_view,
          std::string_view,
          std::string_view,
          std::optional<std::string_view>,
          const auto&,
          const auto& tables) -> std::shared_ptr<filesystems::TokenProvider> {
        ++checks;
        EXPECT_EQ(tables.inputTables.size(), 1);
        EXPECT_TRUE(tables.outputTable);
        EXPECT_EQ(tables.outputTable->schemaTableName.table, "t");
        return nullptr;
      },
      [&](const auto&) { ++plans; });
  createTableWithData();
  run("ANALYZE t");
  EXPECT_EQ(checks, 1);
  EXPECT_EQ(plans, 1);
}

// ANALYZE reads the table from the table's own catalog, even when the default
// catalog is a different one.
TEST_F(AnalyzeTest, scansTheTablesOwnConnector) {
  createTableWithData();
  registerTpch();
  run("USE tpch.sf1");
  EXPECT_EQ(
      fetchSingleValue<int64_t>(
          fmt::format("ANALYZE {}.default.t", testConnector_->connectorId())),
      4);
  EXPECT_EQ(metadata().analyzedStatistics->numRows(), 4);
}

// A connector refuses to ANALYZE a table it can't store statistics for when the
// optimizer begins the write, before anything runs, so EXPLAIN fails the same
// way.
TEST_F(AnalyzeTest, refusedBeforeRunning) {
  registerTpch();
  VELOX_ASSERT_USER_THROW_CODE(
      run("ANALYZE tpch.sf1.nation"), error_code::kUnsupported, "");
  VELOX_ASSERT_USER_THROW_CODE(
      run("EXPLAIN ANALYZE tpch.sf1.nation"), error_code::kUnsupported, "");
  EXPECT_FALSE(metadata().analyzedStatistics);
}

} // namespace
} // namespace axiom::sql
