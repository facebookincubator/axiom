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
#include <gtest/gtest.h>

#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;
using connector::PushdownRoot;

class ConnectorSubtreePushdownTest : public test::QueryTestBase {
 protected:
  void SetUp() override {
    QueryTestBase::SetUp();
    testMetadata_ = dynamic_cast<connector::TestConnectorMetadata*>(
        testConnector_->metadata().get());
    VELOX_CHECK_NOT_NULL(testMetadata_);
  }

  // Shared across all builders within one test so plan-node IDs and
  // column names stay globally unique. Each TEST_F gets a fresh fixture
  // instance, so a fresh Context per test.
  lp::PlanBuilder::Context context_{
      std::string(kTestConnectorId),
      std::string(kDefaultSchema)};

  // Typed metadata cached at SetUp so tests can install pushdown matchers
  // without per-call dynamic_cast.
  connector::TestConnectorMetadata* testMetadata_{nullptr};
};

// A Filter the connector absorbs collapses into a single scan over the pushdown
// table while the Aggregation above it survives.
TEST_F(ConnectorSubtreePushdownTest, absorbsSubtreeUnderAggregation) {
  const auto schema = ROW({"a", "b"}, BIGINT());
  testConnector_->addTable("t", schema);

  const lp::LogicalPlanNode* absorbed = nullptr;
  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .filter("a > 0")
                         .capturePlanNode(&absorbed)
                         .aggregate({"a"}, {"sum(b)"})
                         .build();

  auto pushdownTable = testConnector_->addTable("u", schema);
  testMetadata_->setPushdownMatcher(
      [absorbed, pushdownTable](const lp::LogicalPlanNode&) {
        return std::vector<PushdownRoot>{{absorbed, pushdownTable}};
      });

  auto plan = toSingleNodePlan(logicalPlan);
  auto matcher = matchScan("u").singleAggregation({"a"}, {"sum(b)"}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// The connector absorbs the Filter+Scan on the left of
// an inner join; the HashJoin and the build-side scan survive over
// the pushdown table.
TEST_F(ConnectorSubtreePushdownTest, absorbsProbeSideOfJoin) {
  const auto probeSchema = ROW({"a", "b"}, BIGINT());
  const auto buildSchema = ROW({"c", "d"}, BIGINT());
  testConnector_->addTable("t", probeSchema);
  testConnector_->addTable("u", buildSchema);

  const lp::LogicalPlanNode* absorbed = nullptr;
  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .filter("a > 0")
                         .capturePlanNode(&absorbed)
                         .join(
                             lp::PlanBuilder(context_).tableScan("u"),
                             "a = c",
                             lp::JoinType::kInner)
                         .build();

  auto pushdownTable = testConnector_->addTable("v", probeSchema);
  testMetadata_->setPushdownMatcher(
      [absorbed, pushdownTable](const lp::LogicalPlanNode&) {
        return std::vector<PushdownRoot>{{absorbed, pushdownTable}};
      });

  auto plan = toSingleNodePlan(logicalPlan);
  auto buildMatcher = matchScan("u").build();
  auto matcher = matchScan("v").hashJoin(buildMatcher).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
