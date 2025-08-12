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

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/ParquetTpchTest.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace lp = facebook::velox::logical_plan;

namespace facebook::velox::optimizer {
namespace {

class HiveLimitQueriesTest : public test::QueryTestBase {
 protected:
  static void SetUpTestCase() {
    test::ParquetTpchTest::createTables();

    LocalRunnerTestBase::testDataPath_ = FLAGS_data_path;
    LocalRunnerTestBase::localFileFormat_ = "parquet";
    LocalRunnerTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    LocalRunnerTestBase::TearDownTestCase();
  }

  RowTypePtr getSchema(const std::string& tableName) {
    return connector::getConnector(exec::test::kHiveConnectorId)
        ->metadata()
        ->findTable("nation")
        ->rowType();
  }

  void checkResults(
      const lp::LogicalPlanNodePtr& logicalPlan,
      const core::PlanNodePtr& referencePlan) {
    VELOX_CHECK_NOT_NULL(logicalPlan);
    VELOX_CHECK_NOT_NULL(referencePlan);

    auto referenceResults = runVelox(referencePlan);

    // Distributed.
    {
      auto plan = planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 4});
      checkResults(plan, referenceResults);
    }

    // Single node.
    for (auto numDrivers : {1, 4}) {
      SCOPED_TRACE(fmt::format("numWorkers: 1, numDrivers: {}", numDrivers));
      auto plan =
          planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = numDrivers});
      checkResults(plan, referenceResults);
    }
  }

  void checkResults(
      const PlanAndStats& plan,
      const test::TestResult& expected) {
    auto results = runFragmentedPlan(plan);
    exec::test::assertEqualResults(expected.results, results.results);
  }

  void checkSingleNodePlan(
      const PlanAndStats& plan,
      const std::shared_ptr<core::PlanMatcher>& matcher) {
    SCOPED_TRACE(plan.plan->toString());

    const auto& fragments = plan.plan->fragments();
    ASSERT_EQ(1, fragments.size());

    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));
  }
};

// LIMIT 10
TEST_F(HiveLimitQueriesTest, limit) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  const auto nationType = getSchema("nation");

  auto logicalPlan =
      lp::PlanBuilder(context).tableScan("nation").limit(10).build();

  auto referencePlan = exec::test::PlanBuilder()
                           .tableScan("nation", nationType)
                           .limit(0, 10, false)
                           .planNode();
  auto referenceResults = runVelox(referencePlan);

  // Single driver.
  {
    SCOPED_TRACE("numWorkers: 1, numDrivers: 1");
    auto plan = planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = 1});

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .project()
                       .finalLimit(0, 10)
                       .build();

    checkSingleNodePlan(plan, matcher);
    checkResults(plan, referenceResults);
  }

  // Single node.
  {
    SCOPED_TRACE("numWorkers: 1, numDrivers: 4");
    auto plan = planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = 4});

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .project()
                       .partialLimit(0, 10)
                       .localPartition()
                       .finalLimit(0, 10)
                       .build();

    checkSingleNodePlan(plan, matcher);
    checkResults(plan, referenceResults);
  }

  // Distributed.
  {
    SCOPED_TRACE("numWorkers: 4, numDrivers: 4");
    auto distributedPlan =
        planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 4});
    const auto& fragments = distributedPlan.plan->fragments();
    ASSERT_EQ(2, fragments.size());

    EXPECT_EQ(fragments.at(0).scans.size(), 1);
    EXPECT_EQ(fragments.at(1).scans.size(), 0);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .project()
                       .partialLimit(0, 10)
                       .localPartition()
                       .finalLimit(0, 10)
                       .partitionedOutput()
                       .build();
    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = core::PlanMatcherBuilder().exchange().finalLimit(0, 10).build();
    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));

    checkResults(distributedPlan, referenceResults);
  }
}

// OFFSET 5
// LIMIT 10
TEST_F(HiveLimitQueriesTest, offset) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  const auto nationType = getSchema("nation");

  auto logicalPlan =
      lp::PlanBuilder(context).tableScan("nation").limit(5, 10).build();

  auto referencePlan = exec::test::PlanBuilder()
                           .tableScan("nation", nationType)
                           .limit(5, 10, false)
                           .planNode();
  auto referenceResults = runVelox(referencePlan);

  // Single driver.
  {
    SCOPED_TRACE("numWorkers: 1, numDrivers: 1");
    auto plan = planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = 1});

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .project()
                       .finalLimit(5, 10)
                       .build();

    checkSingleNodePlan(plan, matcher);
    checkResults(plan, referenceResults);
  }

  // Single node.
  {
    SCOPED_TRACE("numWorkers: 1, numDrivers: 4");
    auto plan = planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = 4});

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .project()
                       .partialLimit(0, 15)
                       .localPartition()
                       .finalLimit(5, 10)
                       .build();

    checkSingleNodePlan(plan, matcher);
    checkResults(plan, referenceResults);
  }

  // Distributed.
  {
    SCOPED_TRACE("numWorkers: 4, numDrivers: 4");
    auto distributedPlan =
        planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 4});
    const auto& fragments = distributedPlan.plan->fragments();
    ASSERT_EQ(2, fragments.size());

    EXPECT_EQ(fragments.at(0).scans.size(), 1);
    EXPECT_EQ(fragments.at(1).scans.size(), 0);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .project()
                       .partialLimit(0, 15)
                       .localPartition()
                       .finalLimit(0, 15)
                       .partitionedOutput()
                       .build();
    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = core::PlanMatcherBuilder().exchange().finalLimit(5, 10).build();
    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));

    checkResults(distributedPlan, referenceResults);
  }
}

// OFFSET 5
// <no limit>
TEST_F(HiveLimitQueriesTest, offsetOnly) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  const auto nationType = getSchema("nation");

  auto plan = lp::PlanBuilder(context).tableScan("nation").offset(5).build();

  const auto noLimit = std::numeric_limits<int64_t>::max();
  auto reference = exec::test::PlanBuilder()
                       .tableScan("nation", nationType)
                       .limit(5, noLimit, false)
                       .planNode();

  checkResults(plan, reference);
}

// OFFSET <very large>
// <no limit>
// Verify handling of offset + limit exceeding max.
TEST_F(HiveLimitQueriesTest, veryLargeOffset) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  const auto nationType = getSchema("nation");

  const auto noLimit = std::numeric_limits<int64_t>::max();

  auto plan = lp::PlanBuilder(context)
                  .tableScan("nation")
                  .limit(noLimit - 5, 100)
                  .build();

  auto reference = exec::test::PlanBuilder()
                       .tableScan("nation", nationType)
                       .limit(noLimit - 5, 100, false)
                       .planNode();

  checkResults(plan, reference);
}

// ORDER BY name DESC
// LIMIT 10
TEST_F(HiveLimitQueriesTest, orderByLimit) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  const auto nationType = getSchema("nation");

  auto logicalPlan = lp::PlanBuilder(context)
                         .tableScan("nation")
                         .orderBy({"n_name desc"})
                         .limit(10)
                         .build();

  auto referencePlan = exec::test::PlanBuilder()
                           .tableScan("nation", nationType)
                           .orderBy({"n_name desc"}, false)
                           .limit(0, 10, false)
                           .planNode();
  auto referenceResults = runVelox(referencePlan);

  // Single driver
  {
    SCOPED_TRACE("numWorkers: 1, numDrivers: 1");
    auto plan = planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = 1});

    auto matcher =
        core::PlanMatcherBuilder().tableScan().topN(10).project().build();

    checkSingleNodePlan(plan, matcher);
    checkResults(plan, referenceResults);
  }

  // Single node.
  {
    SCOPED_TRACE("numWorkers: 1, numDrivers: 4");
    auto plan = planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = 4});

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .topN(10)
                       .localMerge()
                       .finalLimit(0, 10)
                       .project()
                       .build();

    checkSingleNodePlan(plan, matcher);
    checkResults(plan, referenceResults);
  }

  // Distributed.
  {
    SCOPED_TRACE("numWorkers: 4, numDrivers: 4");
    auto distributedPlan =
        planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 4});
    const auto& fragments = distributedPlan.plan->fragments();
    ASSERT_EQ(2, fragments.size());

    EXPECT_EQ(fragments.at(0).scans.size(), 1);
    EXPECT_EQ(fragments.at(1).scans.size(), 0);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .topN(10)
                       .localMerge()
                       .partitionedOutput()
                       .build();
    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = core::PlanMatcherBuilder()
                  .mergeExchange()
                  .finalLimit(0, 10)
                  .project()
                  .build();
    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));

    checkResults(distributedPlan, referenceResults);
  }
}

// ORDER BY name DESC
// OFFSET 5
// LIMIT 10
TEST_F(HiveLimitQueriesTest, orderByOffsetLimit) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  const auto nationType = getSchema("nation");

  auto plan = lp::PlanBuilder(context)
                  .tableScan("nation")
                  .orderBy({"n_name desc"})
                  .limit(5, 10)
                  .build();

  auto reference = exec::test::PlanBuilder()
                       .tableScan("nation", nationType)
                       .orderBy({"n_name desc"}, false)
                       .limit(5, 10, false)
                       .planNode();

  checkResults(plan, reference);
}

} // namespace
} // namespace facebook::velox::optimizer
