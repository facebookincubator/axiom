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

#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/ParquetTpchTest.h"

namespace facebook::axiom::optimizer::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

// static
void HiveQueriesTestBase::SetUpTestCase() {
  gTempDirectory = exec::test::TempDirectoryPath::create();
  test::ParquetTpchTest::createTables(gTempDirectory->getPath());

  LocalRunnerTestBase::testDataPath_ = gTempDirectory->getPath();
  LocalRunnerTestBase::localFileFormat_ = "parquet";
  LocalRunnerTestBase::SetUpTestCase();
}

// static
void HiveQueriesTestBase::TearDownTestCase() {
  LocalRunnerTestBase::TearDownTestCase();
  gTempDirectory.reset();
}

void HiveQueriesTestBase::SetUp() {
  test::QueryTestBase::SetUp();
  test::ParquetTpchTest::registerTpchConnector(kTpchConnectorId);

  prestoParser_ = std::make_unique<PrestoParser>(kTpchConnectorId, pool());
}

void HiveQueriesTestBase::TearDown() {
  test::ParquetTpchTest::unregisterTpchConnector(kTpchConnectorId);
  test::QueryTestBase::TearDown();
}

RowTypePtr HiveQueriesTestBase::getSchema(const std::string& tableName) {
  return connector::ConnectorMetadata::metadata(exec::test::kHiveConnectorId)
      ->findTable(tableName)
      ->type();
}

void HiveQueriesTestBase::checkResults(
    const std::string& sql,
    const core::PlanNodePtr& referencePlan) {
  SCOPED_TRACE(sql);
  VELOX_CHECK_NOT_NULL(referencePlan);

  auto statement = prestoParser_->parse(sql);

  ASSERT_TRUE(statement->isSelect());
  auto logicalPlan = statement->asUnchecked<test::SelectStatement>()->plan();

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

void HiveQueriesTestBase::checkResults(
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

void HiveQueriesTestBase::checkResults(
    const PlanAndStats& plan,
    const test::TestResult& expected) {
  auto results = runFragmentedPlan(plan);
  exec::test::assertEqualResults(expected.results, results.results);
}

void HiveQueriesTestBase::checkSingleNodePlan(
    const PlanAndStats& plan,
    const std::shared_ptr<core::PlanMatcher>& matcher) {
  SCOPED_TRACE(plan.plan->toString());

  const auto& fragments = plan.plan->fragments();
  ASSERT_EQ(1, fragments.size());

  ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));
}

} // namespace facebook::axiom::optimizer::test
