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

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/v2/Optimize.h"
#include "velox/expression/Expr.h"
#include "velox/type/Type.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

constexpr float kNationTableRows = 25;
constexpr float kRegionTableRows = 5;
constexpr int32_t kSystemSamplePercentage = 10;

float estimatedValueBytes(const velox::TypePtr& type) {
  return Value(type.get()).byteSize();
}

struct ResourceStats {
  std::optional<float> inputRows;
  std::optional<float> inputBytes;
  std::optional<float> outputRows;
  std::optional<float> outputBytes;
};

class QueryStatsTest : public optimizer::test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    optimizer::test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables(
        {velox::tpch::Table::TBL_NATION, velox::tpch::Table::TBL_REGION});
  }

  ResourceStats resourceStats(
      std::string_view sql,
      const std::optional<OptimizerOptions>& options = std::nullopt) {
    auto& veloxQueryContext = getQueryCtx();
    velox::HashStringAllocator allocator(&optimizerPool());
    auto graphContext = std::make_unique<QueryGraphContext>(
        allocator, OptimizerOptions::kMaxPlanObjectsDefault);
    optimizer::queryCtx() = graphContext.get();
    SCOPE_EXIT {
      optimizer::queryCtx() = nullptr;
    };

    velox::exec::SimpleExpressionEvaluator evaluator(
        veloxQueryContext.get(), &optimizerPool());
    connector::SchemaResolver schemaResolver{
        connector::ConnectorMetadataRegistry::global()};
    const OptimizerSession session{
        veloxQueryContext->queryId(),
        "test",
        options.value_or(optimizerOptions_),
        connectorSessionProperties_};

    const auto logicalPlan = parseSelect(sql);
    const auto stats =
        Optimizer(
            *logicalPlan, schemaResolver, session, evaluator, veloxQueryContext)
            .estimateQueryStats();
    return {
        .inputRows = stats.numInputRows,
        .inputBytes = stats.inputBytes,
        .outputRows = stats.cardinality,
        .outputBytes = stats.outputBytes,
    };
  }
};

TEST_F(QueryStatsTest, singleScan) {
  const auto stats = resourceStats("SELECT n_nationkey FROM nation");
  const auto rowBytes = estimatedValueBytes(velox::BIGINT());

  EXPECT_EQ(stats.inputRows, kNationTableRows);
  EXPECT_EQ(stats.inputBytes, kNationTableRows * rowBytes);
  EXPECT_EQ(stats.outputRows, kNationTableRows);
  EXPECT_EQ(stats.outputBytes, kNationTableRows * rowBytes);
}

TEST_F(QueryStatsTest, noScans) {
  const auto stats = resourceStats("SELECT 1");
  const auto outputRowBytes = estimatedValueBytes(velox::INTEGER());

  EXPECT_EQ(stats.inputRows, 0);
  EXPECT_EQ(stats.inputBytes, 0);
  EXPECT_EQ(stats.outputRows, 1);
  EXPECT_EQ(stats.outputBytes, outputRowBytes);
}

TEST_F(QueryStatsTest, filterColumnContributesToInputWidth) {
  const auto stats =
      resourceStats("SELECT n_name FROM nation WHERE n_regionkey = 1");
  const auto inputRowBytes = estimatedValueBytes(velox::VARCHAR()) +
      estimatedValueBytes(velox::BIGINT());
  const auto outputRowBytes = estimatedValueBytes(velox::VARCHAR());
  const auto expectedOutputRows = kNationTableRows / kRegionTableRows;

  EXPECT_EQ(stats.inputRows, kNationTableRows);
  EXPECT_EQ(stats.inputBytes, kNationTableRows * inputRowBytes);
  EXPECT_EQ(stats.outputRows, expectedOutputRows);
  EXPECT_EQ(stats.outputBytes, expectedOutputRows * outputRowBytes);
}

TEST_F(QueryStatsTest, joinsSumAllScans) {
  const auto stats = resourceStats(
      "SELECT n_name, r_name FROM nation, region "
      "WHERE n_regionkey = r_regionkey");
  const auto inputRowBytes = estimatedValueBytes(velox::VARCHAR()) +
      estimatedValueBytes(velox::BIGINT());
  const auto outputRowBytes = 2 * estimatedValueBytes(velox::VARCHAR());

  EXPECT_EQ(stats.inputRows, kNationTableRows + kRegionTableRows);
  EXPECT_EQ(
      stats.inputBytes, (kNationTableRows + kRegionTableRows) * inputRowBytes);
  EXPECT_EQ(stats.outputRows, kNationTableRows);
  EXPECT_EQ(stats.outputBytes, kNationTableRows * outputRowBytes);
}

TEST_F(QueryStatsTest, systemSampleReducesInput) {
  const auto stats = resourceStats(
      fmt::format(
          "SELECT n_nationkey FROM nation TABLESAMPLE SYSTEM ({})",
          kSystemSamplePercentage));
  const auto expectedRows = kNationTableRows * kSystemSamplePercentage / 100.0f;
  const auto rowBytes = estimatedValueBytes(velox::BIGINT());

  EXPECT_EQ(stats.inputRows, expectedRows);
  EXPECT_EQ(stats.inputBytes, expectedRows * rowBytes);
  EXPECT_EQ(stats.outputRows, expectedRows);
  EXPECT_EQ(stats.outputBytes, expectedRows * rowBytes);
}

TEST_F(QueryStatsTest, bernoulliSampleReadsAllInput) {
  const auto stats = resourceStats(
      "SELECT n_nationkey FROM nation TABLESAMPLE BERNOULLI (20)");
  const auto rowBytes = estimatedValueBytes(velox::BIGINT());

  // Bernoulli sampling reads every input row. Its nondeterministic predicate
  // is not estimated, so the output estimate also stays conservatively at the
  // unsampled cardinality.
  EXPECT_EQ(stats.inputRows, kNationTableRows);
  EXPECT_EQ(stats.inputBytes, kNationTableRows * rowBytes);
  EXPECT_EQ(stats.outputRows, kNationTableRows);
  EXPECT_EQ(stats.outputBytes, kNationTableRows * rowBytes);
}

TEST_F(QueryStatsTest, repeatedOutputColumn) {
  const auto stats = resourceStats("SELECT n_name, n_name FROM nation");
  const auto valueBytes = estimatedValueBytes(velox::VARCHAR());
  const auto outputRowBytes = 2 * valueBytes;

  EXPECT_EQ(stats.inputRows, kNationTableRows);
  EXPECT_EQ(stats.inputBytes, kNationTableRows * valueBytes);
  EXPECT_EQ(stats.outputRows, kNationTableRows);
  EXPECT_EQ(stats.outputBytes, kNationTableRows * outputRowBytes);
}

TEST_F(QueryStatsTest, missingInputStatistics) {
  OptimizerOptions options;
  options.useFilteredTableStats = false;

  const auto stats = resourceStats("SELECT n_nationkey FROM nation", options);
  const auto outputRowBytes = estimatedValueBytes(velox::BIGINT());

  EXPECT_EQ(stats.inputRows, std::nullopt);
  EXPECT_EQ(stats.inputBytes, std::nullopt);
  EXPECT_EQ(stats.outputRows, kNationTableRows);
  EXPECT_EQ(stats.outputBytes, kNationTableRows * outputRowBytes);
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test

// TPC-H data generation instantiates a folly Singleton (DBGenBackend), which
// requires folly::Init to have run registrationComplete().
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
