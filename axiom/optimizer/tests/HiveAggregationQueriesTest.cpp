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

#include <folly/init/Init.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/ParquetTpchTest.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/tests/utils/DfFunctions.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class HiveAggregationQueriesTest : public test::QueryTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  static void SetUpTestCase() {
    std::string path;
    if (FLAGS_data_path.empty()) {
      gTempDirectory = exec::test::TempDirectoryPath::create();
      path = gTempDirectory->getPath();
      test::ParquetTpchTest::createTables(path);
    } else {
      path = FLAGS_data_path;
      if (FLAGS_create_dataset) {
        test::ParquetTpchTest::createTables(path);
      }
    }

    LocalRunnerTestBase::testDataPath_ = path;
    LocalRunnerTestBase::localFileFormat_ = "parquet";
    LocalRunnerTestBase::SetUpTestCase();

    test::registerDfFunctions();
  }

  static void TearDownTestCase() {
    LocalRunnerTestBase::TearDownTestCase();
    gTempDirectory.reset();
  }

  void SetUp() override {
    QueryTestBase::SetUp();

    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);

    QueryTestBase::TearDown();
  }

  void checkSame(
      const lp::LogicalPlanNodePtr& planNode,
      const core::PlanNodePtr& referencePlan,
      const axiom::runner::MultiFragmentPlan::Options& options = {
          .numWorkers = 4,
          .numDrivers = 4}) {
    VELOX_CHECK_NOT_NULL(planNode);
    VELOX_CHECK_NOT_NULL(referencePlan);

    axiom::runner::MultiFragmentPlan::Options singleNodeOptions = {
        .numWorkers = 1, .numDrivers = 1};
    auto referenceResult = runVelox(referencePlan, singleNodeOptions);
    auto fragmentedPlan = planVelox(planNode, options);
    auto experimentResult = runFragmentedPlan(fragmentedPlan);

    exec::test::assertEqualResults(
        referenceResult.results, experimentResult.results);

    if (options.numWorkers != 1) {
      auto singleNodePlan = planVelox(
          planNode, {.numWorkers = 1, .numDrivers = options.numDrivers});
      auto singleNodeResult = runFragmentedPlan(singleNodePlan);

      exec::test::assertEqualResults(
          referenceResult.results, singleNodeResult.results);

      if (options.numDrivers != 1) {
        auto singleThreadPlan =
            planVelox(planNode, {.numWorkers = 1, .numDrivers = 1});
        auto singleThreadResult = runFragmentedPlan(singleThreadPlan);

        exec::test::assertEqualResults(
            referenceResult.results, singleThreadResult.results);
      }
    }
  }

  core::PlanNodePtr toSingleNodePlan(
      const lp::LogicalPlanNodePtr& logicalPlan,
      int32_t numDrivers = 1) {
    schema_ = std::make_shared<optimizer::SchemaResolver>();

    auto plan =
        planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = numDrivers})
            .plan;

    EXPECT_EQ(1, plan->fragments().size());
    return plan->fragments().at(0).fragment.planNode;
  }

  inline static std::shared_ptr<exec::test::TempDirectoryPath> gTempDirectory;

  std::shared_ptr<connector::TestConnector> testConnector_;
};

TEST_F(HiveAggregationQueriesTest, agg) {
  testConnector_->createTable(
      "numbers", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), VARCHAR()}));

  auto logicalPlan = lp::PlanBuilder()
                         .tableScan(kTestConnectorId, "numbers", {"a", "b"})
                         .aggregate({"a"}, {"sum(b)"})
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        core::PlanMatcherBuilder().tableScan().singleAggregation().build();

    ASSERT_TRUE(matcher->match(plan));
  }
  {
    auto plan = toSingleNodePlan(logicalPlan, 2);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .partialAggregation()
                       .localPartition()
                       .finalAggregation()
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }
}

TEST_F(HiveAggregationQueriesTest, aggFilter) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;

  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(connectorId, "nation", nationType->names())
          .aggregate({}, {"sum(n_nationkey) FILTER (WHERE n_nationkey > 10)"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan("nation")
                     .project()
                     .singleAggregation()
                     .build();

  ASSERT_TRUE(matcher->match(plan));

  auto referencePlan = exec::test::PlanBuilder()
                           .tableScan("nation", nationType)
                           .project({"n_nationkey"})
                           .filter("n_nationkey > 10")
                           .singleAggregation({}, {"sum(n_nationkey)"})
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveAggregationQueriesTest, aggDistinct) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;

  auto logicalPlan = lp::PlanBuilder()
                         .tableScan(connectorId, "nation", nationType->names())
                         .aggregate({}, {"count(distinct n_regionkey)"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan("nation")
                     .singleAggregation()
                     .build();

  ASSERT_TRUE(matcher->match(plan));

  auto referencePlan = exec::test::PlanBuilder()
                           .tableScan("nation", nationType)
                           .project({"n_regionkey"})
                           .singleAggregation({"n_regionkey"}, {})
                           .singleAggregation({}, {"count(1)"})
                           .planNode();

  // TODO with options:
  // https://github.com/facebookexperimental/verax/issues/396
  auto options = axiom::runner::MultiFragmentPlan::Options{
      .numWorkers = 1, .numDrivers = 1};
  checkSame(logicalPlan, referencePlan, options);
}

TEST_F(HiveAggregationQueriesTest, aggOrderBy) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;

  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(connectorId, "nation", nationType->names())
          .aggregate(
              {"n_regionkey"},
              {"array_agg(n_nationkey ORDER BY n_nationkey DESC)",
               "array_agg(n_name ORDER BY n_nationkey)"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan("nation")
                     .singleAggregation()
                     .build();

  ASSERT_TRUE(matcher->match(plan));

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", nationType)
          .singleAggregation(
              {"n_regionkey"},
              {"array_agg(n_nationkey ORDER BY n_nationkey DESC)",
               "array_agg(n_name ORDER BY n_nationkey)"})
          .planNode();

  // TODO with options:
  // https://github.com/facebookexperimental/verax/issues/397
  auto options = axiom::runner::MultiFragmentPlan::Options{
      .numWorkers = 1, .numDrivers = 1};
  checkSame(logicalPlan, referencePlan, options);
}

} // namespace
} // namespace facebook::axiom::optimizer
