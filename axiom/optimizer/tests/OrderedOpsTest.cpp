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

class OrderedOpsTest : public HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    HiveQueriesTestBase::SetUpTestCase();

    // Create bucketed and sorted tables for merge join testing.
    // Must be in SetUpTestCase because makeBucketedSortedTables has SCOPE_EXIT
    // that unregisters connectors, which would break the per-test SetUp.
    ParquetTpchTest::makeBucketedSortedTables(
        runner::test::LocalRunnerTestBase::localDataPath_);
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();
  }

  /// Tests merge join by running a query with merge join enabled and disabled,
  /// comparing results.
  /// @return PlanAndStats for the merge join enabled plan
  PlanAndStats checkMergeJoin(const std::string& sql) {
    // Run with merge join enabled (testingUseMergeJoin = true)
    optimizerOptions_.testingUseMergeJoin = true;
    auto logicalPlan = parseSelect(sql);
    auto mergeJoinPlan = planVelox(logicalPlan);
    auto mergeJoinResults = runFragmentedPlan(mergeJoinPlan);

    // Run with merge join disabled (testingUseMergeJoin = false)
    optimizerOptions_.testingUseMergeJoin = false;
    logicalPlan = parseSelect(sql);
    auto hashJoinPlan = planVelox(logicalPlan);
    auto hashJoinResults = runFragmentedPlan(hashJoinPlan);

    // Compare results
    velox::exec::test::assertEqualResults(
        mergeJoinResults.results, hashJoinResults.results);

    // Reset to default (nullopt) for future tests
    optimizerOptions_.testingUseMergeJoin = std::nullopt;

    return mergeJoinPlan;
  }
};

TEST_F(OrderedOpsTest, mergeBasic) {
  auto plan = checkMergeJoin(
      "select sum(p_partkey), sum(ps_supplycost) from part_bs, partsupp_bs "
      "where p_partkey = ps_partkey");

  // Verify that the plan executed successfully
  EXPECT_TRUE(plan.plan != nullptr);
}

} // namespace facebook::axiom::optimizer::test
