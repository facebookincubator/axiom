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

#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;

class RankingTest : public test::QueryTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  void SetUp() override {
    test::QueryTestBase::SetUp();

    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);

    testConnector_->addTpchTables();
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);
    test::QueryTestBase::TearDown();
  }

  velox::core::PlanNodePtr toSingleNodePlan(
      std::string_view sql,
      int32_t numDrivers = 1) {
    auto logicalPlan = parseSelect(sql, kTestConnectorId);
    return QueryTestBase::toSingleNodePlan(logicalPlan, numDrivers);
  }

  runner::MultiFragmentPlanPtr toDistributedPlan(std::string_view sql) {
    auto logicalPlan = parseSelect(sql, kTestConnectorId);
    return planVelox(logicalPlan).plan;
  }

  std::shared_ptr<connector::TestConnector> testConnector_;
};

TEST_F(RankingTest, rowNumberWithoutOrderBy) {
  // row_number() without ORDER BY uses a specialized RowNumberNode.
  constexpr auto sql = "SELECT n_name, row_number() OVER () as rn FROM nation";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation").rowNumber({}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // No partition keys — gather to single node, then RowNumber.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation").gather().rowNumber({}).build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rowNumberWithPartitionByWithoutOrderBy) {
  // row_number() with PARTITION BY but no ORDER BY uses RowNumberNode.
  constexpr auto sql =
      "SELECT n_name, row_number() OVER (PARTITION BY n_regionkey) as rn "
      "FROM nation";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .rowNumber({"n_regionkey"})
                     .project({"n_name", "rn"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // TODO: n_regionkey is carried through gather unnecessarily. Drop it after
  // RowNumber once https://github.com/facebookincubator/velox/issues/16551 is
  // fixed.
  // Shuffle by partition keys before RowNumber.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .shuffle({"n_regionkey"})
                                .rowNumber({"n_regionkey"})
                                .project({"n_name", "rn"})
                                .gather()
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rowNumberWithLimit) {
  // row_number() without ORDER BY + LIMIT. LIMIT is pushed below RowNumber
  // because the row numbering is non-deterministic.
  constexpr auto sql =
      "SELECT n_name, row_number() OVER () as rn FROM nation LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation").finalLimit(0, 10).rowNumber({}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed: partial limit → local gather → final limit → gather →
  // final limit → RowNumber.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation").distributedLimit(0, 10).rowNumber({}).build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rowNumberWithPartitionByAndLimit) {
  // LIMIT pushed below RowNumber.
  constexpr auto sql =
      "SELECT n_name, row_number() OVER (PARTITION BY n_regionkey) as rn "
      "FROM nation LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .finalLimit(0, 10)
                     .rowNumber({"n_regionkey"})
                     .project({"n_name", "rn"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // TODO: n_regionkey is carried through the limit chain and gather
  // unnecessarily. Drop it after RowNumber once
  // https://github.com/facebookincubator/velox/issues/16551 is fixed.
  // Distributed: limit pushed below RowNumber, gather to single node.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .distributedLimit(0, 10)
                                .rowNumber({"n_regionkey"})
                                .project({"n_name", "rn"})
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rowNumberWithOrderByAndLimit) {
  // row_number() with ORDER BY + LIMIT → TopNRowNumber. No partition keys and
  // row_number never produces ties, so the LIMIT is fully absorbed.
  constexpr auto sql =
      "SELECT n_name, row_number() OVER (ORDER BY n_name) as rn "
      "FROM nation LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation").topNRowNumber({}, {"n_name"}, 10).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // No partition keys — gather, then TopNRowNumber.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation").gather().topNRowNumber({}, {"n_name"}, 10).build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rankWithOrderByAndLimit) {
  // rank() with ORDER BY + LIMIT → TopNRowNumber with LIMIT on top because
  // rank() may produce ties.
  constexpr auto sql =
      "SELECT n_name, rank() OVER (ORDER BY n_name) as rn "
      "FROM nation LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .topNRowNumber({}, {"n_name"}, 10)
                     .finalLimit(0, 10)
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // No partition keys — gather, then TopNRowNumber + finalLimit.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .topNRowNumber({}, {"n_name"}, 10)
                                .partialLimit(0, 10)
                                .localPartition()
                                .finalLimit(0, 10)
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, denseRankWithOrderByAndLimit) {
  // dense_rank() behaves like rank() — LIMIT preserved on top.
  constexpr auto sql =
      "SELECT n_name, dense_rank() OVER (ORDER BY n_name) as rn "
      "FROM nation LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .topNRowNumber({}, {"n_name"}, 10)
                     .finalLimit(0, 10)
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // No partition keys — gather, then TopNRowNumber + finalLimit.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .topNRowNumber({}, {"n_name"}, 10)
                                .partialLimit(0, 10)
                                .localPartition()
                                .finalLimit(0, 10)
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rowNumberWithPartitionAndOrderByAndLimit) {
  // With partition keys, LIMIT stays on top because per-partition limit differs
  // from global limit.
  constexpr auto sql =
      "SELECT n_name, "
      "row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name) as rn "
      "FROM nation LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .topNRowNumber({"n_regionkey"}, {"n_name"}, 10)
                     .finalLimit(0, 10)
                     .project({"n_name", "rn"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // TODO: n_regionkey is carried through the limit chain and gather
  // unnecessarily. Drop it after TopNRowNumber once
  // https://github.com/facebookincubator/velox/issues/16551 is fixed.
  // Shuffle by partition keys, then TopNRowNumber + limit + gather + limit +
  // project.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .shuffle({"n_regionkey"})
                                .topNRowNumber({"n_regionkey"}, {"n_name"}, 10)
                                .distributedLimit(0, 10)
                                .project({"n_name", "rn"})
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, multipleWindowFunctionsWithLimitNoOptimization) {
  // Multiple window functions in the same group — no ranking optimization.
  constexpr auto sql =
      "SELECT n_name, "
      "   row_number() OVER (ORDER BY n_name) as rn, "
      "   sum(n_regionkey) OVER (ORDER BY n_name) as s "
      "FROM nation LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .window(
                         {"row_number() OVER (ORDER BY n_name)",
                          "sum(n_regionkey) OVER (ORDER BY n_name)"})
                     .finalLimit(0, 10)
                     .project({"n_name", "rn", "s"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // No partition keys — gather, then Window + limit + project.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .window(
                                    {"row_number() OVER (ORDER BY n_name)",
                                     "sum(n_regionkey) OVER (ORDER BY n_name)"})
                                .partialLimit(0, 10)
                                .localPartition()
                                .finalLimit(0, 10)
                                .project({"n_name", "rn", "s"})
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rankWithoutOrderBy) {
  // rank() without ORDER BY stays as generic Window — only row_number() gets
  // the RowNumber optimization.
  constexpr auto sql =
      "SELECT n_name, rank() OVER (PARTITION BY n_regionkey) as rn "
      "FROM nation";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .window({"rank() OVER (PARTITION BY n_regionkey)"})
                     .project({"n_name", "rn"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // TODO: n_regionkey is carried through gather unnecessarily. Drop it after
  // the Window once https://github.com/facebookincubator/velox/issues/16551 is
  // fixed.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation")
          .shuffle({"n_regionkey"})
          .window({"rank() OVER (PARTITION BY n_regionkey)"})
          .project({"n_name", "rn"})
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, denseRankWithoutOrderBy) {
  // dense_rank() without ORDER BY stays as generic Window.
  constexpr auto sql =
      "SELECT n_name, dense_rank() OVER (PARTITION BY n_regionkey) as rn "
      "FROM nation";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .window({"dense_rank() OVER (PARTITION BY n_regionkey)"})
                     .project({"n_name", "rn"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // TODO: n_regionkey is carried through gather unnecessarily. Drop it after
  // the Window once https://github.com/facebookincubator/velox/issues/16551 is
  // fixed.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation")
          .shuffle({"n_regionkey"})
          .window({"dense_rank() OVER (PARTITION BY n_regionkey)"})
          .project({"n_name", "rn"})
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rankWithLimitWithoutOrderBy) {
  // rank() with LIMIT but without ORDER BY — Pattern 2 does NOT apply (only
  // row_number qualifies). LIMIT stays on top.
  constexpr auto sql =
      "SELECT n_name, rank() OVER (PARTITION BY n_regionkey) as rn "
      "FROM nation LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .window({"rank() OVER (PARTITION BY n_regionkey)"})
                     .finalLimit(0, 10)
                     .project({"n_name", "rn"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // TODO: n_regionkey is carried through the limit chain and gather
  // unnecessarily. Drop it after the Window once
  // https://github.com/facebookincubator/velox/issues/16551 is fixed.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation")
          .shuffle({"n_regionkey"})
          .window({"rank() OVER (PARTITION BY n_regionkey)"})
          .distributedLimit(0, 10)
          .project({"n_name", "rn"})
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rowNumberWithOrderByNoLimit) {
  // row_number() with ORDER BY but no LIMIT stays as generic Window — no TopN
  // optimization without LIMIT.
  constexpr auto sql =
      "SELECT n_name, row_number() OVER (ORDER BY n_name) as rn "
      "FROM nation";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .window({"row_number() OVER (ORDER BY n_name)"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // No partition keys — gather, then Window.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .window({"row_number() OVER (ORDER BY n_name)"})
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rankWithOrderByNoLimit) {
  // rank() with ORDER BY but no LIMIT stays as generic Window.
  constexpr auto sql =
      "SELECT n_name, rank() OVER (ORDER BY n_name) as rn "
      "FROM nation";

  auto plan = toSingleNodePlan(sql);
  auto matcher =
      matchScan("nation").window({"rank() OVER (ORDER BY n_name)"}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // No partition keys — gather, then Window.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .window({"rank() OVER (ORDER BY n_name)"})
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rowNumberWithRedundantQueryOrderBy) {
  // Query ORDER BY matches window ORDER BY — ORDER BY is redundant, absorbed
  // into TopNRowNumber.
  constexpr auto sql =
      "SELECT n_name, row_number() OVER (ORDER BY n_name) as rn "
      "FROM nation ORDER BY n_name LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation").topNRowNumber({}, {"n_name"}, 10).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // No partition keys — gather, then TopNRowNumber.
  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation").gather().topNRowNumber({}, {"n_name"}, 10).build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
