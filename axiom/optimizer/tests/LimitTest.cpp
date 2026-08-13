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

#include <fmt/core.h>

#include <limits>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

// A Limit's count when the query has OFFSET but no LIMIT.
constexpr int64_t kNoLimit = std::numeric_limits<int64_t>::max();

class LimitTest : public test::QueryTestBase,
                  public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useV2_ = GetParam();
    test::QueryTestBase::SetUp();
  }

  using QueryTestBase::toSingleNodePlan;

  core::PlanNodePtr toSingleNodePlan(
      std::string_view sql,
      int32_t numDrivers = 1) {
    auto logicalPlan = parseSelect(sql, kTestConnectorId);
    return QueryTestBase::toSingleNodePlan(logicalPlan, numDrivers);
  }

  MultiFragmentPlanPtr toDistributedPlan(std::string_view sql) {
    auto logicalPlan = parseSelect(sql, kTestConnectorId);
    return planVelox(logicalPlan).plan;
  }

  lp::PlanBuilder scan(const std::string& tableName) {
    lp::PlanBuilder::Context context{kTestConnectorId, kDefaultSchema};
    return lp::PlanBuilder(context).tableScan(tableName);
  }
};

// Adjacent limits are expressed with PlanBuilder rather than nested
// subqueries, so the test does not depend on the parser preserving both.
TEST_P(LimitTest, adjacentLimits) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(scan("nation").limit(10).limit(5).build()),
      matchScan("nation").finalLimit(0, 5).build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(scan("nation").limit(10).limit(15).build()),
      matchScan("nation").finalLimit(0, 10).build());

  // The outer offset skips rows the inner limit kept, leaving 3 of them.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(scan("nation").limit(10).offset(7).limit(5).build()),
      matchScan("nation").finalLimit(7, 3).build());
}

TEST_P(LimitTest, limit) {
  constexpr auto sql = "SELECT * FROM nation LIMIT 10";

  // A single driver needs no partial limit.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(sql), matchScan("nation").finalLimit(0, 10).build());

  // Multiple drivers on one node: each driver limits, then the results are
  // merged and limited again.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(sql, /*numDrivers=*/4),
      matchScan("nation").localLimit(0, 10).build());

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      toDistributedPlan(sql),
      matchScan("nation").distributedLimit(0, 10).build());
}

TEST_P(LimitTest, offsetAndLimit) {
  constexpr auto sql = "SELECT * FROM nation OFFSET 5 LIMIT 10";

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(sql), matchScan("nation").finalLimit(5, 10).build());

  // Partial limits must keep offset + count rows; only the final limit skips.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(sql, /*numDrivers=*/4),
      matchScan("nation").localLimit(5, 10).build());

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      toDistributedPlan(sql),
      matchScan("nation").distributedLimit(5, 10).build());
}

TEST_P(LimitTest, offsetOnly) {
  constexpr auto sql = "SELECT * FROM nation OFFSET 5";

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(sql),
      matchScan("nation").finalLimit(5, kNoLimit).build());

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      toDistributedPlan(sql),
      matchScan("nation").gather().finalLimit(5, kNoLimit).build());
}

TEST_P(LimitTest, veryLargeOffset) {
  // offset + count exceeds the maximum int64, so the partial limit would keep
  // every row and is dropped instead of wrapping around.
  const auto sql =
      fmt::format("SELECT * FROM nation OFFSET {} LIMIT 100", kNoLimit - 5);

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(sql),
      matchScan("nation").finalLimit(kNoLimit - 5, 100).build());

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      toDistributedPlan(sql),
      matchScan("nation").gather().finalLimit(kNoLimit - 5, 100).build());
}

TEST_P(LimitTest, orderByLimit) {
  constexpr auto sql = "SELECT * FROM nation ORDER BY n_name DESC LIMIT 10";

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(sql), matchScan("nation").topN(10).build());

  // Each driver produces its own top 10; the merge preserves order and the
  // final limit trims to 10.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(sql, /*numDrivers=*/4),
      matchScan("nation").topN(10).localMerge().finalLimit(0, 10).build());

  // Each worker limits what it sends over the merge exchange, and the final
  // limit trims the merged output of all workers. v1 omits the per-worker
  // limit and ships each worker's full merged TopN output.
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      toDistributedPlan(sql),
      matchScan("nation")
          .topN(10)
          .localMerge()
          .finalLimitIf(useV2_, 0, 10)
          .shuffleMerge()
          .finalLimit(0, 10)
          .build());
}

TEST_P(LimitTest, orderByOffsetAndLimit) {
  constexpr auto sql =
      "SELECT * FROM nation ORDER BY n_name DESC OFFSET 5 LIMIT 10";

  // The TopN must keep offset + count rows before the offset drops any.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(sql),
      matchScan("nation").topN(15).finalLimit(5, 10).build());

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      toDistributedPlan(sql),
      matchScan("nation")
          .topN(15)
          .localMerge()
          .finalLimitIf(useV2_, 0, 15)
          .shuffleMerge()
          .finalLimit(5, 10)
          .build());
}

TEST_P(LimitTest, orderByDirectlyBelowLimitBecomesTopN) {
  // Only the ORDER BY directly below a limit folds into a TopN. The trailing
  // ORDER BY has no limit and stays a full sort.
  auto plan = scan("nation")
                  .limit(20)
                  .orderBy({"n_nationkey"})
                  .limit(10)
                  .orderBy({"n_name desc"});

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(plan.build()),
      matchScan("nation")
          .finalLimit(0, 20)
          .topN(10)
          .orderBy({"n_name DESC NULLS LAST"})
          .build());
}

TEST_P(LimitTest, filtersDoNotCrossLimit) {
  // Moving a filter across a limit would change which rows the limit keeps.
  // Filters on the same side of a limit are combined.
  auto plan = scan("nation")
                  .filter("n_nationkey > 2")
                  .limit(10)
                  .filter("n_nationkey < 100")
                  .filter("n_regionkey > 10")
                  .limit(5)
                  .filter("n_nationkey > 70")
                  .filter("n_regionkey < 7");

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(plan.build()),
      matchScan("nation")
          .filter("n_nationkey > 2")
          .finalLimit(0, 10)
          .filter("n_nationkey < 100 AND n_regionkey > 10")
          .finalLimit(0, 5)
          .filter("n_nationkey > 70 AND n_regionkey < 7")
          .build());
}

TEST_P(LimitTest, limitBeforeProject) {
  testConnector_->addTable("t", ROW({"a", "b"}, INTEGER()));

  // The limit goes below the projection, so no more than 'limit' rows are
  // projected.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan("SELECT a + b as c FROM (SELECT * FROM t LIMIT 10)"),
      matchScan("t").finalLimit(0, 10).project({"a + b as c"}).build());

  // v1 pushes the limit below the projection only when it is at most 8192
  // rows; v2 does so for any limit.
  //
  // TODO: The limit gathers into a single pipeline, so everything above it
  // runs single-threaded. Pushing the limit below the projection
  // unconditionally restricts parallelism and can slow the query down when the
  // limit is large.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan("SELECT a + b as c FROM (SELECT * FROM t LIMIT 10000)"),
      useV2_
          ? matchScan("t").finalLimit(0, 10'000).project({"a + b as c"}).build()
          : matchScan("t")
                .project({"a + b as c"})
                .finalLimit(0, 10'000)
                .build());
}

TEST_P(LimitTest, limitAfterOrderBy) {
  testConnector_->addTable("t", ROW({"a", "b"}, INTEGER()));

  for (auto limit : {10, 10'000}) {
    const auto sql = fmt::format(
        "SELECT c FROM (SELECT a + b as c FROM t) ORDER BY c LIMIT {}", limit);
    SCOPED_TRACE(sql);

    // v1 adds a rename-only projection over the TopN output.
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(sql),
        matchScan("t")
            .project({"a + b as c"})
            .topN(limit)
            .projectIf(!useV2_, {"c"})
            .build());
  }
}

TEST_P(LimitTest, zeroLimit) {
  testConnector_->addTable("t", ROW("a", BIGINT()));
  testConnector_->addTable("u", ROW("x", BIGINT()));

  // A zero-row limit collapses the subtree into an empty Values node.
  {
    const auto matcher = matchValues().build();

    for (const auto* sql : {
             "SELECT * FROM t LIMIT 0",
             "SELECT * FROM t ORDER BY a LIMIT 0",
             "SELECT * FROM t OFFSET 1 LIMIT 0",
             "SELECT * FROM t ORDER BY a OFFSET 1 LIMIT 0",
             "SELECT * FROM (SELECT * FROM t LIMIT 1) OFFSET 1",
             "SELECT * FROM (SELECT * FROM t ORDER BY a LIMIT 1) OFFSET 1",
         }) {
      SCOPED_TRACE(sql);
      AXIOM_ASSERT_PLAN(toSingleNodePlan(sql), matcher);
    }
  }

  // Operators above the empty Values node are preserved.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan("SELECT count(*) FROM (SELECT * FROM t LIMIT 0)"),
      matchValues().singleAggregation({}, {"count(*) as cnt"}).build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          "SELECT * FROM t, (SELECT * FROM u LIMIT 0) s WHERE a = x"),
      matchScan("t").hashJoin(matchValues(), core::JoinType::kInner).build());
}

AXIOM_INSTANTIATE_V1_V2(LimitTest);

} // namespace
} // namespace facebook::axiom::optimizer
