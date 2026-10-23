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
#include "axiom/analyze/TableAnalyzer.h"
#include "axiom/cli/Connectors.h"
#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestConnectorContext.h"
#include "axiom/functions/prestosql/PrestoFunctions.h"
#include "axiom/sql/presto/tests/ExpectPrestoSqlError.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/core/TableWriteTraits.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace axiom::sql {
namespace {
using namespace facebook::velox;
using namespace facebook::axiom::connector;

class AnalyzeTest : public SqlQueryRunnerTestBase {
 protected:
  TestConnectorMetadata& metadata() {
    return *testConnector_->metadata();
  }
  void addData() {
    testConnector_->addTable(
        "t", ROW({"x", "b", "s"}, {BIGINT(), BOOLEAN(), VARCHAR()}));
    testConnector_->appendData(
        "t",
        makeRowVector(
            {"x", "b", "s"},
            {makeNullableFlatVector<int64_t>({1, 2, 2, std::nullopt}),
             makeNullableFlatVector<bool>({true, false, true, std::nullopt}),
             makeNullableFlatVector<std::string>(
                 {"a", "bb", "a", std::nullopt})}));
  }
  const ColumnStatistics& stat(
      const StatisticsResults& result,
      const std::string& name,
      size_t group = 0) {
    for (const auto& column : result.columnStats.at(group)) {
      if (column.name == name) {
        return column;
      }
    }
    VELOX_FAIL("Missing statistics for {}", name);
  }
};

TEST_F(AnalyzeTest, standard) {
  addData();
  auto result = fetchSingleRow("ANALYZE t");
  EXPECT_EQ(result->rowType()->nameOf(0), "rows");
  EXPECT_EQ(result->childAt(0)->variantAt(0).value<int64_t>(), 4);
  ASSERT_TRUE(metadata().analyzedStatistics);
  const auto& stats = *metadata().analyzedStatistics;
  ASSERT_EQ(stats.groups->size(), 1);
  EXPECT_EQ(stats.groups->childAt(0)->variantAt(0).value<int64_t>(), 4);
  const auto& x = stat(stats, "x");
  EXPECT_EQ(x.numValues, 3);
  EXPECT_FLOAT_EQ(x.nullPct, 25);
  EXPECT_EQ(x.min->value<int64_t>(), 1);
  EXPECT_EQ(x.max->value<int64_t>(), 2);
  EXPECT_EQ(x.numDistinct, 2);
  const auto& b = stat(stats, "b");
  EXPECT_EQ(b.numTrue, 2);
  EXPECT_EQ(b.numDistinct, 2);
  EXPECT_FLOAT_EQ(b.nullPct, 25);
  // The three strings hold 4 bytes of text plus a 4-byte length each: 16 bytes
  // over 4 rows.
  EXPECT_EQ(stat(stats, "s").avgSizeInBytes, 4);
}

TEST_F(AnalyzeTest, publishStatisticsWithoutChangingData) {
  auto original =
      testConnector_->addTable("t", ROW({"x", "b"}, {BIGINT(), BOOLEAN()}));
  original->addData(
      makeRowVector(
          {"x", "b"},
          {makeNullableFlatVector<int64_t>({1, 2, std::nullopt}),
           makeNullableFlatVector<bool>({true, false, std::nullopt})}),
      false);
  metadata().publishAnalyzedStatistics = true;
  EXPECT_EQ(original->numRows(), 3);
  EXPECT_FALSE(original->findColumn("x")->stats());
  run("ANALYZE t WITH (columns = ARRAY['x'])");
  auto first = metadata().findTable({"default", "t"});
  ASSERT_NE(first.get(), original.get());
  ASSERT_TRUE(first->findColumn("x")->stats());
  EXPECT_EQ(first->findColumn("x")->stats()->numDistinct, 2);
  EXPECT_FALSE(first->findColumn("b")->stats());
  EXPECT_FALSE(original->findColumn("x")->stats());
  metadata().onFinishAnalyze = [](const auto&) {
    VELOX_USER_FAIL("finish failed");
  };
  VELOX_ASSERT_THROW(run("ANALYZE t"), "finish failed");
  EXPECT_EQ(first, metadata().findTable({"default", "t"}));
  metadata().onFinishAnalyze = nullptr;
  run("ANALYZE t WITH (columns = ARRAY['b'])");
  auto second = metadata().findTable({"default", "t"});
  EXPECT_EQ(second->findColumn("x")->stats()->numDistinct, 2);
  EXPECT_EQ(second->findColumn("b")->stats()->numTrue, 1);
  EXPECT_EQ(
      std::static_pointer_cast<const TestTable>(second)->data()[0].get(),
      original->data()[0].get());
  testConnector_->appendData(
      "t",
      makeRowVector(
          {"x", "b"},
          {makeFlatVector<int64_t>({3}), makeFlatVector<bool>({true})}));
  auto appended = metadata().findTable({"default", "t"});
  EXPECT_EQ(appended->numRows(), 4);
  EXPECT_FALSE(appended->findColumn("x")->stats());
  run("ANALYZE t");
  auto final = metadata().findTable({"default", "t"});
  EXPECT_EQ(final->findColumn("x")->stats()->numDistinct, 3);
  EXPECT_EQ(final->findColumn("b")->stats()->numTrue, 2);
}

TEST_F(AnalyzeTest, approximateDistinctCount) {
  testConnector_->addTable("t", ROW({"x"}, {BIGINT()}));
  testConnector_->appendData(
      "t", makeRowVector({"x"}, {makeFlatVector<int64_t>(10000, [](auto row) {
                           return row % 1000;
                         })}));
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 10000);
  const auto& stats = stat(*metadata().analyzedStatistics, "x");
  ASSERT_TRUE(stats.numDistinct);
  EXPECT_NEAR(*stats.numDistinct, 1000, 100);
}

TEST_F(AnalyzeTest, columnsAndValidation) {
  addData();
  run("ANALYZE test.default.t WITH (columns = ARRAY['x'])");
  ASSERT_TRUE(metadata().analyzedStatistics);
  ASSERT_EQ(metadata().analyzedStatistics->columnStats[0].size(), 1);
  EXPECT_EQ(metadata().analyzedStatistics->columnStats[0][0].name, "x");
  for (const auto& [sql, error] :
       std::vector<std::pair<std::string, std::string>>{
           {"ANALYZE t WITH (columns = ARRAY['bad'])",
            "Unknown ANALYZE column: bad"},
           {"ANALYZE t WITH (columns = ARRAY['x', 'x'])",
            "Duplicate ANALYZE column: x"},
           {"ANALYZE t WITH (columns = 1)", "must be an array"},
           {"ANALYZE t WITH (columns = ARRAY[NULL])", "must be an array"},
           {"ANALYZE t WITH (unknown = 1)",
            "Unknown ANALYZE property: unknown"}}) {
    VELOX_ASSERT_THROW(run(sql), error);
  }
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      run("ANALYZE t WITH (columns = ARRAY['x'], columns = ARRAY['b'])"),
      "Duplicate property");
  run("ANALYZE t WITH (columns = ARRAY[])");
  EXPECT_TRUE(metadata().analyzedStatistics->columnStats[0].empty());
}

TEST_F(AnalyzeTest, explainAndCommitFailure) {
  addData();
  int commits = 0;
  metadata().onFinishAnalyze = [&](const auto&) {
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
  VELOX_ASSERT_THROW(
      run("EXPLAIN ANALYZE ANALYZE t"),
      "EXPLAIN ANALYZE ANALYZE is not supported");
  VELOX_ASSERT_THROW(
      run("EXPLAIN (TYPE VALIDATE) ANALYZE t WITH (columns = ARRAY['bad'])"),
      "Unknown ANALYZE column: bad");
  EXPECT_EQ(commits, 0);
  VELOX_ASSERT_THROW(run("ANALYZE t"), "commit failed");
  EXPECT_EQ(commits, 1);
  EXPECT_FALSE(metadata().analyzedStatistics);
}

TEST_F(AnalyzeTest, emptyAndAllNull) {
  testConnector_->addTable(
      "t", ROW({"x", "b", "s"}, {BIGINT(), BOOLEAN(), VARCHAR()}));
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 0);
  auto stats = *metadata().analyzedStatistics;
  EXPECT_FLOAT_EQ(stat(stats, "x").nullPct, 0);
  EXPECT_EQ(stat(stats, "x").numDistinct, 0);
  EXPECT_EQ(stat(stats, "b").numTrue, 0);
  EXPECT_EQ(stat(stats, "b").numDistinct, 0);
  EXPECT_EQ(stat(stats, "s").avgSizeInBytes, 0);
  testConnector_->appendData(
      "t",
      makeRowVector(
          {"x", "b", "s"},
          {makeNullableFlatVector<int64_t>({std::nullopt}),
           makeNullableFlatVector<bool>({std::nullopt}),
           makeNullableFlatVector<std::string>({std::nullopt})}));
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 1);
  stats = *metadata().analyzedStatistics;
  EXPECT_FLOAT_EQ(stat(stats, "x").nullPct, 100);
  EXPECT_FALSE(stat(stats, "x").min);
  EXPECT_EQ(stat(stats, "b").numDistinct, 0);
  EXPECT_EQ(stat(stats, "s").avgSizeInBytes, 0);
}

TEST_F(AnalyzeTest, groupedAndBooleanDependency) {
  addData();
  metadata().analyzeRequestFactory = [](const auto& table, const auto&) {
    auto request = std::make_shared<StatisticsRequest>();
    request->table = table->name();
    request->groupingColumns = {"x"};
    request->groups = std::vector<std::vector<Variant>>{
        {Variant(int64_t(2))}, {Variant::null(TypeKind::BIGINT)}};
    request->statistics = {{"b", ColumnStatisticKind::kTrueCount, {}, {}}};
    return request;
  };
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 3);
  const auto& stats = *metadata().analyzedStatistics;
  ASSERT_EQ(stats.groups->size(), 2);
  for (auto i = 0; i < stats.groups->size(); ++i) {
    const auto& b = stat(stats, "b", i);
    if (stats.groups->childAt(0)->isNullAt(i)) {
      EXPECT_EQ(b.numTrue, 0);
      EXPECT_EQ(b.numDistinct, 0);
    } else {
      EXPECT_EQ(b.numTrue, 1);
      EXPECT_EQ(b.numDistinct, 2);
    }
  }
}

TEST_F(AnalyzeTest, emptyGroups) {
  addData();
  metadata().analyzeRequestFactory = [](const auto& table, const auto&) {
    auto request = std::make_shared<StatisticsRequest>();
    request->table = table->name();
    request->groupingColumns = {"x"};
    request->groups.emplace();
    return request;
  };
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 0);
  ASSERT_TRUE(metadata().analyzedStatistics);
  EXPECT_EQ(metadata().analyzedStatistics->groups->size(), 0);
}

TEST_F(AnalyzeTest, sketchesAndConstantArguments) {
  addData();
  metadata().analyzeRequestFactory = [](const auto& table, const auto&) {
    auto request = std::make_shared<StatisticsRequest>();
    request->table = table->name();
    request->statistics = {
        {"x", ColumnStatisticKind::kDistinctCount, {Variant(0.01)}, {}},
        {"x", ColumnStatisticKind::kDistinctSketch, {Variant(0.01)}, {}}};
    return request;
  };
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 4);
  const auto& stats = *metadata().analyzedStatistics;
  EXPECT_EQ(stat(stats, "x").numDistinct, 2);
  ASSERT_EQ(stats.sketches->childrenSize(), 1);
  EXPECT_FALSE(stats.sketches->childAt(0)->isNullAt(0));
}

// A connector can ask, by name, for any aggregate that returns binary data.
TEST_F(AnalyzeTest, connectorAggregate) {
  addData();
  metadata().analyzeRequestFactory = [](const auto& table, const auto&) {
    auto request = std::make_shared<StatisticsRequest>();
    request->table = table->name();
    request->statistics = {
        {"x", ColumnStatisticKind::kAggregate, {}, "checksum"}};
    return request;
  };
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 4);
  const auto& sketches = metadata().analyzedStatistics->sketches;
  ASSERT_EQ(sketches->childrenSize(), 1);
  EXPECT_FALSE(sketches->childAt(0)->isNullAt(0));
}

// ANALYZE skips a statistic when its aggregate can't take the column's type,
// as writes do. JSON has no min, max or distinct count, and BIGINT has no true
// count. A skipped sketch comes back as a null column, so the sketch columns
// still match the request.
TEST_F(AnalyzeTest, leavesOutUnsupportedStatistics) {
  testConnector_->addTable("t", ROW({"a", "b"}, {JSON(), BIGINT()}));
  testConnector_->appendData(
      "t",
      makeRowVector(
          {"a", "b"},
          {makeFlatVector<std::string>({"1", "2"}, JSON()),
           makeFlatVector<int64_t>({1, 2})}));

  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 2);
  {
    const auto& a = stat(*metadata().analyzedStatistics, "a");
    EXPECT_EQ(a.numValues, 2);
    EXPECT_FALSE(a.min.has_value());
    EXPECT_FALSE(a.max.has_value());
    EXPECT_FALSE(a.numDistinct.has_value());
    EXPECT_EQ(stat(*metadata().analyzedStatistics, "b").numDistinct, 2);
  }

  metadata().analyzeRequestFactory = [](const auto& table, const auto&) {
    auto request = std::make_shared<StatisticsRequest>();
    request->table = table->name();
    request->statistics = {
        {"a", ColumnStatisticKind::kDistinctSketch, {}, {}},
        {"b", ColumnStatisticKind::kTrueCount, {}, {}},
        {"b", ColumnStatisticKind::kDistinctSketch, {}, {}}};
    return request;
  };
  EXPECT_EQ(fetchSingleValue<int64_t>("ANALYZE t"), 2);
  const auto& stats = *metadata().analyzedStatistics;
  EXPECT_FALSE(stat(stats, "b").numTrue.has_value());
  EXPECT_EQ(stat(stats, "b").numValues, 2);
  ASSERT_EQ(stats.sketches->childrenSize(), 2);
  EXPECT_TRUE(stats.sketches->childAt(0)->isNullAt(0));
  EXPECT_FALSE(stats.sketches->childAt(1)->isNullAt(0));
}

TEST_F(AnalyzeTest, badRequestDoesNotCommit) {
  addData();
  metadata().analyzeRequestFactory = [](const auto& table, const auto&) {
    auto request = std::make_shared<StatisticsRequest>();
    request->table = table->name();
    request->statistics = {
        {"x", ColumnStatisticKind::kAggregate, {}, "no_such_aggregate"}};
    return request;
  };
  VELOX_ASSERT_THROW(run("ANALYZE t"), "no_such_aggregate");
  EXPECT_FALSE(metadata().analyzedStatistics);
}

TEST_F(AnalyzeTest, permissionsAndInternalPlan) {
  int checks = 0;
  int plans = 0;
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
  addData();
  run("ANALYZE t");
  EXPECT_EQ(checks, 1);
  EXPECT_EQ(plans, 0);
}

// The statistics query reads the table from the table's own catalog, even when
// the default catalog is a different one.
TEST_F(AnalyzeTest, scansTheTablesOwnConnector) {
  addData();
  facebook::axiom::Connectors connectors;
  connectors.registerTpchConnector("tpch");
  auto metadata = ConnectorMetadataRegistry::get(testConnector_->connectorId());
  auto table = metadata->findTable({kDefaultSchema, "t"});
  ASSERT_NE(table, nullptr);
  auto context = makeTestContext("analyze-connector");
  facebook::axiom::logical_plan::PlanBuilder::Context builderContext(
      "tpch", "sf1");
  builderContext.connectorContext = context;

  facebook::axiom::analyze::TableAnalyzer analyzer(
      metadata,
      context->sessionFor(testConnector_->connectorId()),
      table,
      {},
      std::move(builderContext),
      *facebook::axiom::functions::prestosql::PrestoFunctions::makeRegistry());

  auto node = analyzer.plan();
  while (!node->inputs().empty()) {
    node = node->onlyInput();
  }
  const auto* scan = node->as<facebook::axiom::logical_plan::TableScanNode>();
  ASSERT_NE(scan, nullptr);
  EXPECT_EQ(scan->connectorId(), testConnector_->connectorId());
  EXPECT_EQ(scan->tableName(), table->name());
}

TEST_F(AnalyzeTest, unsupportedConnector) {
  facebook::axiom::Connectors connectors;
  connectors.registerTpchConnector("tpch");
  VELOX_ASSERT_THROW(
      run("ANALYZE tpch.sf1.nation"),
      "ANALYZE is not supported by connector: tpch");
  VELOX_ASSERT_THROW(
      run("EXPLAIN (TYPE VALIDATE) ANALYZE tpch.sf1.nation"),
      "ANALYZE is not supported by connector: tpch");
}

TEST_F(AnalyzeTest, queryFailureDoesNotCommit) {
  addData();
  metadata().analyzeRequestFactory = [](const auto& table, const auto&) {
    auto request = std::make_shared<StatisticsRequest>();
    request->table = table->name();
    request->statistics = {
        {"x", ColumnStatisticKind::kDistinctCount, {Variant(0.99)}, {}}};
    return request;
  };
  EXPECT_THROW(run("ANALYZE t"), std::exception);
  EXPECT_FALSE(metadata().analyzedStatistics);
}

TEST_F(AnalyzeTest, conversionBatchesAndRequestOrder) {
  StatisticsRequest request;
  request.statistics = {{"b", ColumnStatisticKind::kTrueCount, {}, {}}};
  request.addDependencies();
  request.addDependencies();
  ASSERT_EQ(request.statistics.size(), 2);
  auto batch = makeRowVector(
      {makeFlatVector<int64_t>({4}),
       makeFlatVector<int64_t>({2}),
       makeFlatVector<int64_t>({3})});
  auto results = StatisticsResults::fromAggregates(
      request, {true, true}, {batch, batch}, pool(), /*firstChannel=*/0);
  ASSERT_EQ(results.columnStats.size(), 2);
  for (const auto& columns : results.columnStats) {
    ASSERT_EQ(columns.size(), 1);
    EXPECT_EQ(columns[0].numDistinct, 2);
    EXPECT_EQ(columns[0].numTrue, 2);
    EXPECT_FLOAT_EQ(columns[0].nullPct, 25);
  }
}

// Writes collect what the write handle asks for and skip what a column's type
// can't take. If nothing is left, the write collects no statistics. A skipped
// sketch comes back as a null column.
TEST_F(AnalyzeTest, writeStatisticsFollowTheRequest) {
  testConnector_->addTable("t", ROW({"a", "b"}, {JSON(), BIGINT()}));
  std::vector<size_t> numResultColumns;
  std::optional<StatisticsResults> written;
  metadata().onFinishWrite = [&](const auto& writeResults, const auto& stats) {
    for (const auto& result : writeResults) {
      numResultColumns.push_back(result->childrenSize());
    }
    written = stats;
  };
  constexpr auto kInsert =
      "INSERT INTO t VALUES (json_parse('1'), 1), (json_parse('2'), 2)";

  metadata().writeStatisticsRequestFactory = [](const auto&) {
    StatisticsRequest request;
    request.statistics = {{"a", ColumnStatisticKind::kMin, {}, {}}};
    return request;
  };
  run(kInsert);
  ASSERT_TRUE(written.has_value());
  EXPECT_EQ(written->groups, nullptr);
  EXPECT_THAT(
      numResultColumns,
      testing::Each(
          facebook::velox::core::TableWriteTraits::outputType(std::nullopt)
              ->size()));

  metadata().writeStatisticsRequestFactory = [](const auto&) {
    StatisticsRequest request;
    request.statistics = {
        {"a", ColumnStatisticKind::kDistinctSketch, {}, {}},
        {"b", ColumnStatisticKind::kDistinctSketch, {}, {}}};
    return request;
  };
  written.reset();
  run(kInsert);
  ASSERT_TRUE(written.has_value());
  ASSERT_NE(written->sketches, nullptr);
  ASSERT_EQ(written->sketches->childrenSize(), 2);
  EXPECT_TRUE(written->sketches->childAt(0)->isNullAt(0));
  EXPECT_FALSE(written->sketches->childAt(1)->isNullAt(0));
}
} // namespace
} // namespace axiom::sql
