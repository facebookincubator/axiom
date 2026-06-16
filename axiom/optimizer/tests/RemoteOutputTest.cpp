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

class RemoteOutputTest : public test::QueryTestBase {};

TEST_F(RemoteOutputTest, simpleScan) {
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

TEST_F(RemoteOutputTest, simpleAggregation) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan = parseSelect("SELECT sum(b) FROM t", kTestConnectorId);

  // Multi-worker without remote output: partial aggregation on workers,
  // gather to single node, then final aggregation. No additional gather
  // needed since the final fragment is already single-worker.
  auto plan = planVelox(
      logicalPlan, {.numWorkers = 2, .numDrivers = 2, .remoteOutput = false});
  auto matcher = matchScan("t")
                     .partialAggregation({}, {"sum(b)"})
                     .gather()
                     .localGather()
                     .finalAggregation({}, {"sum(sum)"})
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

TEST_F(RemoteOutputTest, groupByAggregation) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  // Unique group-by key (NDV == row count): grouping does not reduce rows, so
  // partial pre-aggregation has no benefit and single-stage aggregation wins.
  testConnector_->setStats(
      "t",
      1'000,
      {{"a", {.numDistinct = 1'000}}, {"b", {.numDistinct = 1'000}}});

  auto logicalPlan =
      parseSelect("SELECT sum(b) FROM t GROUP BY a", kTestConnectorId);

  // Multi-worker without remote output: shuffle by group-by key, then
  // single aggregation. A gather stage collects results from multiple
  // workers onto a single node.
  auto plan = planVelox(
      logicalPlan, {.numWorkers = 2, .numDrivers = 2, .remoteOutput = false});
  auto matcher = matchScan("t")
                     .shuffle({"a"})
                     .localPartition({"a"})
                     .singleAggregation({"a"}, {"sum(b)"})
                     .project()
                     .gather()
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
