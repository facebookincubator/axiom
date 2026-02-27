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

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation").gather().topNRowNumber({}, {"n_name"}, 10).build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, rankWithOrderByAndLimit) {
  // rank() with ORDER BY + LIMIT → TopNRowNumber with LIMIT on top because
  // rank may produce ties.
  constexpr auto sql =
      "SELECT n_name, rank() OVER (ORDER BY n_name) as rn "
      "FROM nation LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .topNRowNumber({}, {"n_name"}, 10)
                     .finalLimit(0, 10)
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .topNRowNumber({}, {"n_name"}, 10)
                                .localLimit(0, 10)
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

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .window(
                                    {"row_number() OVER (ORDER BY n_name)",
                                     "sum(n_regionkey) OVER (ORDER BY n_name)"})
                                .localLimit(0, 10)
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
  // the Window once https://github.com/facebookincubator/velox/issues/16551
  // is fixed.
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

TEST_F(RankingTest, orderByWithoutLimit) {
  // Ranking function with ORDER BY but no LIMIT stays as generic Window — no
  // TopN optimization without LIMIT.
  for (const auto& func : {"row_number", "rank"}) {
    SCOPED_TRACE(func);
    auto sql = fmt::format(
        "SELECT n_name, {}() OVER (ORDER BY n_name) as rn FROM nation", func);

    auto windowExpr = fmt::format("{}() OVER (ORDER BY n_name)", func);
    auto plan = toSingleNodePlan(sql);
    auto matcher = matchScan("nation").window({windowExpr}).build();
    AXIOM_ASSERT_PLAN(plan, matcher);

    // No partition keys — gather, then Window.
    auto distributedPlan = toDistributedPlan(sql);
    auto distributedMatcher =
        matchScan("nation").gather().window({windowExpr}).build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
  }
}

TEST_F(RankingTest, redundantQueryOrderBy) {
  // Query ORDER BY matches window ORDER BY — ORDER BY is redundant, absorbed
  // into TopNRowNumber.
  constexpr auto sql =
      "SELECT n_name, row_number() OVER (ORDER BY n_name) as rn "
      "FROM nation ORDER BY n_name LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation").topNRowNumber({}, {"n_name"}, 10).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation").gather().topNRowNumber({}, {"n_name"}, 10).build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

// --- Ranking function + filter on output ---

TEST_F(RankingTest, filterOnOutput) {
  // Ranking function with ORDER BY + filter on output. The ranking predicate
  // is absorbed as a TopNRowNumber limit.
  for (const auto& func : {"row_number", "rank"}) {
    SCOPED_TRACE(func);
    auto sql = fmt::format(
        "SELECT * FROM ("
        "  SELECT n_name, {}() OVER (ORDER BY n_name) as rn "
        "  FROM nation"
        ") WHERE rn <= 5",
        func);

    auto plan = toSingleNodePlan(sql);
    auto matcher = matchScan("nation").topNRowNumber({}, {"n_name"}, 5).build();
    AXIOM_ASSERT_PLAN(plan, matcher);

    // No partition keys — gather, then TopNRowNumber.
    auto distributedPlan = toDistributedPlan(sql);
    auto distributedMatcher =
        matchScan("nation").gather().topNRowNumber({}, {"n_name"}, 5).build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
  }
}

TEST_F(RankingTest, filterOnRowNumberWithPartitionKeys) {
  // row_number() with PARTITION BY + ORDER BY + filter. The ranking predicate
  // is absorbed as a TopNRowNumber limit.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, "
      "    row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name) as rn "
      "  FROM nation"
      ") WHERE rn <= 5";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .topNRowNumber({"n_regionkey"}, {"n_name"}, 5)
                     .project({"n_name", "rn"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .shuffle({"n_regionkey"})
                                .topNRowNumber({"n_regionkey"}, {"n_name"}, 5)
                                .project({"n_name", "rn"})
                                .gather()
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, filterOnRowNumberWithoutOrderBy) {
  // row_number() without ORDER BY + filter. The ranking predicate is absorbed
  // as a RowNumber limit.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, row_number() OVER () as rn "
      "  FROM nation"
      ") WHERE rn <= 5";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation").rowNumber({}, 5).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation").gather().rowNumber({}, 5).build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, filterOnRowNumberLessThan) {
  // rn < 5 is absorbed as a TopNRowNumber limit of 4.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, row_number() OVER (ORDER BY n_name) as rn "
      "  FROM nation"
      ") WHERE rn < 5";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation").topNRowNumber({}, {"n_name"}, 4).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation").gather().topNRowNumber({}, {"n_name"}, 4).build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, filterOnRowNumberEquals1) {
  // rn = 1 is absorbed as a TopNRowNumber limit of 1.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, row_number() OVER (ORDER BY n_name) as rn "
      "  FROM nation"
      ") WHERE rn = 1";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation").topNRowNumber({}, {"n_name"}, 1).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation").gather().topNRowNumber({}, {"n_name"}, 1).build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, filterWithAdditionalPredicates) {
  // The ranking predicate (rn <= 5) is absorbed as a TopNRowNumber limit. The
  // non-window predicate (n_regionkey > 2) stays as a filter above because it
  // is not a partition key filter.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, n_regionkey, "
      "    row_number() OVER (ORDER BY n_name) as rn "
      "  FROM nation"
      ") WHERE rn <= 5 AND n_regionkey > 2";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .topNRowNumber({}, {"n_name"}, 5)
                     .filter("n_regionkey > 2")
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .topNRowNumber({}, {"n_name"}, 5)
                                .filter("n_regionkey > 2")
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, filterWithLowerBound) {
  // The upper bound (rn <= 10) is absorbed as a TopNRowNumber limit. The lower
  // bound (rn >= 3) stays as a filter above.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, row_number() OVER (ORDER BY n_name) as rn "
      "  FROM nation"
      ") WHERE rn >= 3 AND rn <= 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .topNRowNumber({}, {"n_name"}, 10)
                     .filter("rn >= 3")
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .topNRowNumber({}, {"n_name"}, 10)
                                .filter("rn >= 3")
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, filterOnOutputWithLimit) {
  // Both a ranking filter (rn <= 5) and a query LIMIT (3) are present.
  // The ranking filter is absorbed by TopNRowNumber. The LIMIT is on the
  // outer query and becomes a separate Limit node above.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, row_number() OVER (ORDER BY n_name) as rn "
      "  FROM nation"
      ") WHERE rn <= 5 LIMIT 3";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .topNRowNumber({}, {"n_name"}, 5)
                     .finalLimit(0, 3)
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .topNRowNumber({}, {"n_name"}, 5)
                                .localLimit(0, 3)
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, filterOnOutputWithLargerLimit) {
  // Ranking filter (rn <= 3) is tighter than the query LIMIT (10).
  // TopNRowNumber gets the ranking filter limit. The LIMIT 10 stays as a
  // separate node even though TopNRowNumber(limit=3) can never produce more
  // than 3 rows for row_number(). The optimizer does not reason about
  // TopNRowNumber output cardinality being bounded by its limit. For rank()
  // or dense_rank(), TopNRowNumber could produce more than 'limit' rows due
  // to ties, so the LIMIT would be meaningful.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, row_number() OVER (ORDER BY n_name) as rn "
      "  FROM nation"
      ") WHERE rn <= 3 LIMIT 10";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .topNRowNumber({}, {"n_name"}, 3)
                     .finalLimit(0, 10)
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .topNRowNumber({}, {"n_name"}, 3)
                                .localLimit(0, 10)
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, filterOnRowNumberWithMultipleWindowFunctions) {
  // Ranking predicate with multiple window functions in the same DT.
  // row_number() and count(*) over () share the same DT. The ranking predicate
  // must not be pushed below the window operator; it should become a filter
  // above all window operators.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, count(*) OVER () as cnt, "
      "    row_number() OVER () as rn "
      "  FROM nation"
      ") WHERE rn <= 5";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .window({"count() OVER ()", "row_number() OVER ()"})
                     .filter("rn <= 5")
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation")
          .gather()
          .window({"count() OVER ()", "row_number() OVER ()"})
          .filter("rn <= 5")
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, nonWindowFilterWithWindowFunction) {
  // A non-window predicate (n_regionkey > 2) must not be pushed below window
  // operators. Window functions compute over the full input; pushing filters
  // below changes their semantics.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, n_regionkey, "
      "    row_number() OVER (ORDER BY n_name) as rn "
      "  FROM nation"
      ") WHERE n_regionkey > 2";

  auto plan = toSingleNodePlan(sql);
  auto matcher = matchScan("nation")
                     .window({"row_number() OVER (ORDER BY n_name)"})
                     .filter("n_regionkey > 2")
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher = matchScan("nation")
                                .gather()
                                .window({"row_number() OVER (ORDER BY n_name)"})
                                .filter("n_regionkey > 2")
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, partitionKeyFilterPushdown) {
  // Filter on a partition key of the window function is pushed below the
  // window. This is safe because partitioning is independent per partition.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, n_regionkey, "
      "    row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name) as rn "
      "  FROM nation"
      ") WHERE n_regionkey = 2";

  auto plan = toSingleNodePlan(sql);
  auto matcher =
      matchScan("nation")
          .filter("n_regionkey = 2")
          .window(
              {"row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation")
          .filter("n_regionkey = 2")
          .shuffle({"n_regionkey"})
          .window(
              {"row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, nonPartitionKeyFilterStaysAbove) {
  // Filter on a non-partition-key column stays above the window.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, n_regionkey, "
      "    row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name) as rn "
      "  FROM nation"
      ") WHERE n_name = 'FRANCE'";

  auto plan = toSingleNodePlan(sql);
  auto matcher =
      matchScan("nation")
          .window(
              {"row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .filter("n_name = 'FRANCE'")
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation")
          .shuffle({"n_regionkey"})
          .window(
              {"row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .filter("n_name = 'FRANCE'")
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, partitionKeyFilterWithMultipleWindows) {
  // Filter on a column that is a partition key of every window function is
  // pushed below all window operators.
  //
  // TODO: These two window functions could be merged into a single Window
  // operator. They share the same partition keys and count(*)'s empty ORDER BY
  // is a prefix of row_number()'s ORDER BY n_name. The merge doesn't happen
  // because count(*) gets the default RANGE frame (per SQL standard), and the
  // ROWS-only merge rule requires all functions in the shorter-ORDER-BY group
  // to use ROWS frames. A full-partition frame (UNBOUNDED PRECEDING to
  // UNBOUNDED FOLLOWING) is order-independent regardless of frame type, so the
  // merge would be safe.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, n_regionkey, "
      "    row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name) as rn, "
      "    count(*) OVER (PARTITION BY n_regionkey) as cnt "
      "  FROM nation"
      ") WHERE n_regionkey = 2";

  auto plan = toSingleNodePlan(sql);
  auto matcher =
      matchScan("nation")
          .filter("n_regionkey = 2")
          .window(
              {"row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .window({"count() OVER (PARTITION BY n_regionkey)"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation")
          .filter("n_regionkey = 2")
          .shuffle({"n_regionkey"})
          .window(
              {"row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .window({"count() OVER (PARTITION BY n_regionkey)"})
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

TEST_F(RankingTest, partitionKeyFilterPartialMatch) {
  // Filter on a column that is a partition key of one window function but not
  // another stays above all window operators.
  //
  // TODO: A more optimal plan would be: scan → window(count OVER ()) →
  // filter(n_regionkey = 2) → window(row_number OVER PARTITION BY n_regionkey
  // ORDER BY n_name). The filter could be pushed between the two window
  // operators because count(*) OVER () needs all rows but row_number()'s
  // result for matching rows is unchanged (n_regionkey is its partition key).
  // This requires per-operator filter placement instead of all-or-nothing.
  constexpr auto sql =
      "SELECT * FROM ("
      "  SELECT n_name, n_regionkey, "
      "    row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name) as rn, "
      "    count(*) OVER () as cnt "
      "  FROM nation"
      ") WHERE n_regionkey = 2";

  auto plan = toSingleNodePlan(sql);
  auto matcher =
      matchScan("nation")
          .window(
              {"row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .window({"count() OVER ()"})
          .filter("n_regionkey = 2")
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  auto distributedPlan = toDistributedPlan(sql);
  auto distributedMatcher =
      matchScan("nation")
          .shuffle({"n_regionkey"})
          .window(
              {"row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .gather()
          .window({"count() OVER ()"})
          .filter("n_regionkey = 2")
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan, distributedMatcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
