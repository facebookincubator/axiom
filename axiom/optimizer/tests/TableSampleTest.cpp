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
#include <gtest/gtest.h>
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class TableSampleTest : public test::QueryTestBase,
                        public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    test::QueryTestBase::SetUp();
    useV2_ = GetParam();
  }

  lp::LogicalPlanNodePtr parseSampledScan(const std::string& sample) {
    return parseSelect(
        "SELECT * FROM t TABLESAMPLE " + sample, kTestConnectorId);
  }
};

TEST_P(TableSampleTest, bernoulli) {
  testConnector_->addTable("t", ROW({"a", "b"}, INTEGER()));

  // BERNOULLI samples each row independently, lowered to filter(rand() < p).
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSampledScan("BERNOULLI (10)")),
      matchScan("t").filter("rand() < 0.1").build());

  // 100% is a no-op passthrough.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSampledScan("BERNOULLI (100)")),
      matchScan("t").build());

  // 0% produces an empty result.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSampledScan("BERNOULLI (0)")),
      matchValues().build());
}

TEST_P(TableSampleTest, system) {
  testConnector_->addTable("t", ROW({"a", "b"}, INTEGER()));

  // Returns the single fragment's plan node and its sampled-scan map.
  auto planSampled = [&](const std::string& sample) {
    auto result =
        planVelox(parseSampledScan(sample), {.numWorkers = 1, .numDrivers = 1});
    EXPECT_EQ(result.plan->fragments().size(), 1);
    const auto& fragment = result.plan->fragments().at(0);
    return std::make_pair(fragment.fragment.planNode, fragment.sampledScans);
  };

  // SYSTEM samples whole splits, so the percentage rides on the scan's split
  // generation with no per-row filter.
  {
    const auto& [plan, sampledScans] = planSampled("SYSTEM (50)");
    AXIOM_ASSERT_PLAN(plan, matchScan("t").build());
    ASSERT_EQ(sampledScans.size(), 1);
    EXPECT_EQ(sampledScans.begin()->second, 50);
  }

  // 100% is a no-op passthrough: a plain scan with nothing sampled.
  {
    const auto& [plan, sampledScans] = planSampled("SYSTEM (100)");
    AXIOM_ASSERT_PLAN(plan, matchScan("t").build());
    EXPECT_TRUE(sampledScans.empty());
  }

  // 0% produces an empty result: no scan, nothing sampled.
  {
    const auto& [plan, sampledScans] = planSampled("SYSTEM (0)");
    AXIOM_ASSERT_PLAN(plan, matchValues().build());
    EXPECT_TRUE(sampledScans.empty());
  }

  // SYSTEM needs a single scan to sample; a non-scan source fails.
  VELOX_ASSERT_THROW(
      planVelox(
          parseSelect(
              "SELECT * FROM (VALUES (1)) AS v (x) TABLESAMPLE SYSTEM (50)",
              kTestConnectorId),
          {.numWorkers = 1, .numDrivers = 1}),
      "TABLESAMPLE SYSTEM is only supported directly over a table");
}

TEST_P(TableSampleTest, explainShowsSampleRate) {
  testConnector_->addTable("t", ROW({"a", "b"}, INTEGER()));

  auto explain = [&](const std::string& sample) {
    return planVelox(
               parseSampledScan(sample), {.numWorkers = 1, .numDrivers = 1})
        .plan->toString();
  };

  EXPECT_EQ(
      explain("SYSTEM (50)"),
      "Fragment 0: fragment1 SINGLE:\n"
      "-- TableScan[0][\"default\".\"t\"] -> a:INTEGER, b:INTEGER\n"
      "   sample: 50%\n\n");

  // A fractional rate in scientific notation is accepted.
  EXPECT_EQ(
      explain("SYSTEM (1E-1)"),
      "Fragment 0: fragment1 SINGLE:\n"
      "-- TableScan[0][\"default\".\"t\"] -> a:INTEGER, b:INTEGER\n"
      "   sample: 0.1%\n\n");
}

TEST_P(TableSampleTest, sampledCardinality) {
  testConnector_->addTable("big", ROW({"bk", "bv"}, BIGINT()))
      ->setStats(100'000, {{"bk", {.numDistinct = 100'000}}});
  testConnector_->addTable("small", ROW({"sk", "sv"}, BIGINT()))
      ->setStats(1'000, {{"sk", {.numDistinct = 1'000}}});

  auto planSingleNode = [&](const std::string& big) {
    return toSingleNodePlan(parseSelect(
        "SELECT bk, bv, sv FROM " + big + " JOIN small ON bk = sk",
        kTestConnectorId));
  };

  auto matchJoin = [&](const std::string& probe, const std::string& build) {
    return matchScan(probe).hashJoinInner(matchScan(build).build()).build();
  };

  // Unsampled, 'small' (1K rows) is the smaller side.
  AXIOM_ASSERT_PLAN(planSingleNode("big"), matchJoin("big", "small"));

  // Sampling 'big' to 0.5% (~500 rows) makes it the smaller side.
  AXIOM_ASSERT_PLAN(
      planSingleNode("big TABLESAMPLE SYSTEM (0.5)"),
      matchJoin("small", "big"));
}

AXIOM_INSTANTIATE_V1_V2(TableSampleTest);

} // namespace
} // namespace facebook::axiom::optimizer
