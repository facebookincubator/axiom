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

#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class RemoteOutputTest : public test::QueryTestBase,
                         public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    test::QueryTestBase::SetUp();
    useV2_ = GetParam();
  }
};

TEST_P(RemoteOutputTest, scan) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan = parseSelect("SELECT * FROM t", kTestConnectorId);

  // Multi-worker with remote output: results stay distributed, each worker
  // wraps output in a PartitionedOutputNode for remote consumption.
  {
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 2, .numDrivers = 2, .remoteOutput = true});
    auto matcher = matchScan("t").partitionedOutputSingle().build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }

  // Multi-worker without remote output: a gather stage is added to collect
  // results onto a single node.
  {
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 2, .numDrivers = 2, .remoteOutput = false});
    auto matcher = matchScan("t").gather().build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }

  // Single worker with remote output: PartitionedOutputNode is still added
  // to enable remote consumption even though there is only one worker.
  {
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 1, .numDrivers = 2, .remoteOutput = true});
    auto matcher = matchScan("t").partitionedOutputSingle().build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }

  // Single worker without remote output: no extra nodes are added, results
  // are consumed locally.
  {
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 1, .numDrivers = 2, .remoteOutput = false});
    auto matcher = matchScan("t").build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }
}

TEST_P(RemoteOutputTest, globalAggregation) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan = parseSelect("SELECT sum(b) FROM t", kTestConnectorId);

  for (bool remoteOutput : {false, true}) {
    SCOPED_TRACE(remoteOutput ? "remoteOutput=true" : "remoteOutput=false");
    auto plan = planVelox(
        logicalPlan,
        {.numWorkers = 2, .numDrivers = 2, .remoteOutput = remoteOutput});

    auto builder = matchScan("t").distributedAggregation({}, {"sum(b)"});
    if (remoteOutput) {
      builder.partitionedOutputSingle();
    }

    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, builder.build());
  }
}

TEST_P(RemoteOutputTest, groupByAggregation) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan =
      parseSelect("SELECT sum(b) FROM t GROUP BY a", kTestConnectorId);

  for (bool remoteOutput : {false, true}) {
    SCOPED_TRACE(remoteOutput ? "remoteOutput=true" : "remoteOutput=false");
    auto plan = planVelox(
        logicalPlan,
        {.numWorkers = 2, .numDrivers = 2, .remoteOutput = remoteOutput});

    auto builder = matchScan("t")
                       .distributedAggregation({"a"}, {"sum(b) as sum"})
                       .project({"sum"});
    if (remoteOutput) {
      builder.partitionedOutputSingle();
    } else {
      builder.gather();
    }

    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, builder.build());
  }
}

TEST_P(RemoteOutputTest, repeatedOutputName) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan =
      parseSelect("SELECT a AS x, b AS x FROM t", kTestConnectorId);

  for (bool remoteOutput : {false, true}) {
    SCOPED_TRACE(remoteOutput ? "remoteOutput=true" : "remoteOutput=false");
    auto plan = planVelox(
        logicalPlan,
        {.numWorkers = 2, .numDrivers = 2, .remoteOutput = remoteOutput});

    auto builder = matchScan("t");
    if (remoteOutput) {
      builder.project({"a as x", "b as x"}).partitionedOutputSingle();
    } else {
      if (useV2_) {
        builder.gather().project({"a as x", "b as x"});
      } else {
        // TODO: v1 renames the two columns to unique names below the gather and
        // shuffles both, then assigns the colliding name above. It could
        // instead shuffle the columns unchanged and assign the output names
        // above, like v2.
        builder.project().gather().project();
      }
    }

    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, builder.build());
  }
}

TEST_P(RemoteOutputTest, sameColumnTwice) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan = parseSelect("SELECT a, a FROM t", kTestConnectorId);

  for (bool remoteOutput : {false, true}) {
    SCOPED_TRACE(remoteOutput ? "remoteOutput=true" : "remoteOutput=false");
    auto plan = planVelox(
        logicalPlan,
        {.numWorkers = 2, .numDrivers = 2, .remoteOutput = remoteOutput});

    auto builder = matchScan("t");
    if (remoteOutput) {
      builder.project({"a", "a"}).partitionedOutputSingle();
    } else {
      if (useV2_) {
        builder.gather().project({"a", "a"});
      } else {
        // TODO: v1 shuffles the same column twice (renamed to unique names)
        // instead of shuffling it once and duplicating it above the gather,
        // like v2.
        builder.project().gather().project();
      }
    }

    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, builder.build());
  }
}

AXIOM_INSTANTIATE_V1_V2(RemoteOutputTest);

} // namespace
} // namespace facebook::axiom::optimizer
