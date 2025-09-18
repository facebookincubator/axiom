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

#pragma once

#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/optimizer/tests/ParquetTpchTest.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/tests/utils/DfFunctions.h"

namespace facebook::axiom::optimizer {

class PlanTest : public test::QueryTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  static void SetUpTestCase() {
    std::string path;
    if (FLAGS_data_path.empty()) {
      gTempDirectory = velox::exec::test::TempDirectoryPath::create();
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
      const logical_plan::LogicalPlanNodePtr& planNode,
      const velox::core::PlanNodePtr& referencePlan,
      const axiom::runner::MultiFragmentPlan::Options& options = {
          .numWorkers = 4,
          .numDrivers = 4,
      }) {
    VELOX_CHECK_NOT_NULL(planNode);
    VELOX_CHECK_NOT_NULL(referencePlan);

    auto fragmentedPlan = planVelox(planNode, options);
    auto referenceResult = assertSame(referencePlan, fragmentedPlan);

    if (options.numWorkers != 1) {
      auto singleNodePlan = planVelox(
          planNode, {.numWorkers = 1, .numDrivers = options.numDrivers});
      auto singleNodeResult = runFragmentedPlan(singleNodePlan);

      velox::exec::test::assertEqualResults(
          referenceResult.results, singleNodeResult.results);

      if (options.numDrivers != 1) {
        auto singleThreadPlan =
            planVelox(planNode, {.numWorkers = 1, .numDrivers = 1});
        auto singleThreadResult = runFragmentedPlan(singleThreadPlan);

        velox::exec::test::assertEqualResults(
            referenceResult.results, singleThreadResult.results);
      }
    }
  }

  velox::core::PlanNodePtr toSingleNodePlan(
      const logical_plan::LogicalPlanNodePtr& logicalPlan,
      int32_t numDrivers = 1) {
    schema_ = std::make_shared<SchemaResolver>();

    auto plan =
        planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = numDrivers})
            .plan;

    EXPECT_EQ(1, plan->fragments().size());
    return plan->fragments().at(0).fragment.planNode;
  }

  inline static std::shared_ptr<velox::exec::test::TempDirectoryPath>
      gTempDirectory;

  std::shared_ptr<connector::TestConnector> testConnector_;
};

} // namespace facebook::axiom::optimizer
