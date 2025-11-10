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
  test::QueryTestBase::SetUpTestCase();

  gTempDirectory = exec::test::TempDirectoryPath::create();
  test::ParquetTpchTest::createTables(gTempDirectory->getPath());

  LocalRunnerTestBase::localDataPath_ = gTempDirectory->getPath();
  LocalRunnerTestBase::localFileFormat_ =
      velox::dwio::common::FileFormat::PARQUET;
}

// static
void HiveQueriesTestBase::TearDownTestCase() {
  gTempDirectory.reset();
  test::QueryTestBase::TearDownTestCase();
}

void HiveQueriesTestBase::SetUp() {
  test::QueryTestBase::SetUp();

  prestoParser_ = std::make_unique<::axiom::sql::presto::PrestoParser>(
      exec::test::kHiveConnectorId, pool());
}

void HiveQueriesTestBase::TearDown() {
  test::QueryTestBase::TearDown();
}

RowTypePtr HiveQueriesTestBase::getSchema(std::string_view tableName) {
  return connector::ConnectorMetadata::metadata(exec::test::kHiveConnectorId)
      ->findTable(tableName)
      ->type();
}

axiom::optimizer::PlanAndStats HiveQueriesTestBase::checkResults(
    std::string_view sql,
    const core::PlanNodePtr& referencePlan) {
  SCOPED_TRACE(sql);
  VELOX_CHECK_NOT_NULL(referencePlan);

  auto statement = prestoParser_->parse(sql);

  VELOX_CHECK(statement->isSelect());
  auto logicalPlan =
      statement->as<::axiom::sql::presto::SelectStatement>()->plan();

  auto plan = planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = 1});
  checkSame(plan, referencePlan);
  return plan;
}

void HiveQueriesTestBase::checkResults(
    PlanAndStats& plan,
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

void HiveQueriesTestBase::explain(
    std::string_view sql,
    std::string* shortRel,
    std::string* longRel,
    std::string* graph,
    const runner::MultiFragmentPlan::Options& runnerOptions,
    const OptimizerOptions& optimizerOptions) {
  auto statement = prestoParser_->parse(sql);

  VELOX_CHECK(statement->isSelect());
  auto logicalPlan =
      statement->as<::axiom::sql::presto::SelectStatement>()->plan();

  QueryTestBase::explain(
      logicalPlan, shortRel, longRel, graph, runnerOptions, optimizerOptions);
}

} // namespace facebook::axiom::optimizer::test
