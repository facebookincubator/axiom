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

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "axiom/connectors/hive/HiveMetadataConfig.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/TestDataPath.h"
#include "axiom/optimizer/tests/TpchQueries.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

DEFINE_int32(num_repeats, 1, "Number of repeats for optimization timing");

DECLARE_uint32(optimizer_trace);
DECLARE_string(history_save_path);

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class TpchPlanTest : public virtual test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables(velox::tpch::tables);
  }

  static void TearDownTestCase() {
    if (!FLAGS_history_save_path.empty()) {
      suiteHistory().saveToFile(FLAGS_history_save_path);
    }
    test::HiveQueriesTestBase::TearDownTestCase();
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();

    referenceBuilder_ =
        std::make_unique<exec::test::TpchQueryBuilder>(localFileFormat_);
    referenceBuilder_->initialize(localDataPath_);
  }

  void TearDown() override {
    HiveQueriesTestBase::TearDown();
  }

  lp::LogicalPlanNodePtr parseTpchSql(int32_t query) {
    auto sql = test::readTpchSql(query);

    auto statement = prestoParser().parse(sql);

    VELOX_CHECK(statement->isSelect());

    auto logicalPlan =
        statement->as<::axiom::sql::presto::SelectStatement>()->plan();
    VELOX_CHECK_NOT_NULL(logicalPlan);

    return logicalPlan;
  }

  void checkTpchSql(int32_t query) {
    auto sql = test::readTpchSql(query);
    auto referencePlan = referenceBuilder_->getQueryPlan(query).plan;
    checkResults(sql, referencePlan);
  }

  velox::core::PlanNodePtr planTpch(int32_t query) {
    return toSingleNodePlan(parseTpchSql(query));
  }

  // Parses a named TPC-H query file (e.g. "q9_alt").
  lp::LogicalPlanNodePtr parseTpchSql(const std::string& name) {
    return parseSelect(test::readTpchSql(name));
  }

  velox::core::PlanNodePtr planTpch(const std::string& name) {
    return toSingleNodePlan(parseTpchSql(name));
  }

  std::unique_ptr<exec::test::TpchQueryBuilder> referenceBuilder_;
};

TEST_F(TpchPlanTest, stats) {
  auto verifyStats = [&](const auto& tableName, auto cardinality) {
    SCOPED_TRACE(tableName);

    lp::PlanBuilder::Context ctx{exec::test::kHiveConnectorId, kDefaultSchema};
    auto logicalPlan = lp::PlanBuilder(ctx).tableScan(tableName).build();

    auto planAndStats = planVelox(logicalPlan);
    auto stats = planAndStats.prediction;
    ASSERT_FALSE(stats.empty());

    // Every node in a bare-scan plan reports the table cardinality since no
    // intermediate operator changes the row count.
    for (const auto& [nodeId, prediction] : stats) {
      EXPECT_EQ(prediction.cardinality, cardinality);
    }
  };

  verifyStats("region", 5);
  verifyStats("nation", 25);
  verifyStats("orders", 150'000);
  verifyStats("lineitem", 600'572);
}

TEST_F(TpchPlanTest, q01) {
  checkTpchSql(1);

  // The query is a simple scan of lineitem with a very low cardinality group
  // by. All the work is in the table scan (65%) and partial aggregation (30%).

  auto matcher =
      core::PlanMatcherBuilder()
          .hiveScan(
              "lineitem", test::lte("l_shipdate", DATE()->toDays("1998-09-02")))
          .project()
          .aggregation()
          .orderBy()
          .build();

  auto plan = planTpch(1);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(1)));
}

TEST_F(TpchPlanTest, q02) {
  checkTpchSql(2);

  auto joinNationWithRegion = matchHiveScan("nation").hashJoinInner(
      core::PlanMatcherBuilder()
          .hiveScan("region", test::eq("r_name", "EUROPE"))
          .build());

  // The subquery (min cost per part) is very selective — it matches at most
  // one partsupp row per part. It is joined before supplier because its low
  // fanout reduces the row count before the supplier join.
  //
  // ((partsupp INNER part)
  //  INNER
  //  agg(((supplier INNER (partsupp LEFT SEMI (FILTER) part))
  //       INNER (nation INNER region)))
  //  INNER
  //  (supplier INNER (nation INNER region)))
  auto matcher =
      matchHiveScan("partsupp")
          .hashJoinInner(matchHiveScan("part").build())
          .hashJoinInner(
              matchHiveScan("partsupp")
                  .hashJoinLeftSemiFilter(matchHiveScan("part").build())
                  .hashJoinInner(
                      matchHiveScan("supplier")
                          .hashJoinInner(joinNationWithRegion.build())
                          .build())
                  .aggregation()
                  .project()
                  .build())
          .hashJoinInner(matchHiveScan("supplier")
                             .hashJoinInner(joinNationWithRegion.build())
                             .build())
          .topN()
          .project()
          .build();

  auto plan = planTpch(2);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(2)));
}

TEST_F(TpchPlanTest, q03) {
  checkTpchSql(3);

  // The query is straightforward to do by hash. We select 1/5 of customer and
  // 1/2 of orders. We first join orders x customer, build on that and then
  // probe on lineitem. There is anti-correlation between the date filters on
  // lineitem and orders but that does not affect the best plan choice.

  auto matcher =
      matchHiveScan("lineitem")
          .hashJoinInner(
              matchHiveScan("orders")
                  .hashJoinInner(
                      matchHiveScan("customer").build(),
                      {.outputColumnNames =
                           {{"o_orderkey", "o_orderdate", "o_shippriority"}}})
                  .build(),
              {.outputColumnNames =
                   {{"l_orderkey",
                     "l_extendedprice",
                     "l_discount",
                     "o_orderdate",
                     "o_shippriority"}}})
          .project()
          .aggregation()
          .topN()
          .project()
          .build();

  auto plan = planTpch(3);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(3)));
}

TEST_F(TpchPlanTest, q04) {
  checkTpchSql(4);

  // The trick in q4 is using a right hand semijoin to do the exists. If we
  // probed with orders and built on lineitem, we would get a much larger build
  // side. But using the right hand semijoin, we get to build on the smaller
  // side. If we had shared key order between lineitem and orders we could look
  // at other plans but in the hash based plan space we have the best outcome.

  auto matcher = core::PlanMatcherBuilder()
                     .hiveScan("lineitem", {}, "l_commitdate < l_receiptdate")
                     .hashJoinRightSemiFilter(
                         core::PlanMatcherBuilder()
                             .hiveScan(
                                 "orders",
                                 test::between(
                                     "o_orderdate",
                                     DATE()->toDays("1993-07-01"),
                                     DATE()->toDays("1993-09-30")))
                             .build())
                     .aggregation()
                     .orderBy()
                     .build();

  auto plan = planTpch(4);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(4)));
}

TEST_F(TpchPlanTest, q05) {
  checkTpchSql(5);

  // The filters are on region and order date. There is also a diamond between
  // supplier and customer, this being that they have the same nation.
  //
  // Lineitem is the driving table that is joined to 1/5 of supplier and then
  // 1/7 of orders. Finally we join with customer on c_custkey and c_nationkey.
  // The build of customer could have been restricted on c_nationkey being in
  // the range of s_nationkey but we did not pick this restriction because this
  // would have gone through a single equality in a join edge of two equalities.
  // The plan is otherwise good and the extra reduction on customer ends up not
  // being very important. This is a possible enhancement for completeness.

  // agg((
  //   (lineitem INNER ((orders INNER customer) INNER (nation INNER region)))
  //   INNER
  //   (supplier LEFT SEMI (FILTER) (nation INNER region))
  // ))

  auto joinNationWithRegion = matchHiveScan("nation").hashJoinInner(
      core::PlanMatcherBuilder()
          .hiveScan("region", test::eq("r_name", "ASIA"))
          .build());

  auto matcher =
      matchHiveScan("lineitem")
          .hashJoinInner(matchHiveScan("orders")
                             .hashJoinInner(matchHiveScan("customer").build())
                             .hashJoinInner(joinNationWithRegion.build())
                             .build())
          .hashJoinInner(matchHiveScan("supplier")
                             .hashJoinLeftSemiFilter(
                                 joinNationWithRegion.project().build())
                             .build())
          .project()
          .aggregation()
          .orderBy()
          .build();

  auto plan = planTpch(5);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(5)));
}

TEST_F(TpchPlanTest, q06) {
  checkTpchSql(6);

  // We have a single scan, so there are no query optimization choices here.

  auto subfieldFilters =
      velox::common::test::SubfieldFiltersBuilder()
          .add(
              "l_shipdate",
              velox::exec::between(
                  DATE()->toDays("1994-01-01"), DATE()->toDays("1994-12-31")))
          .add("l_discount", velox::exec::betweenDouble(0.05, 0.07))
          .add("l_quantity", velox::exec::lessThanDouble(24))
          .build();

  auto matcher = core::PlanMatcherBuilder()
                     .hiveScan("lineitem", std::move(subfieldFilters))
                     .project()
                     .aggregation()
                     .build();

  auto plan = planTpch(6);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(6)));
}

TEST_F(TpchPlanTest, q07) {
  checkTpchSql(7);

  // TODO Verify the plan.

  ASSERT_NO_THROW(planTpch(7));
  ASSERT_NO_THROW(planVelox(parseTpchSql(7)));
}

TEST_F(TpchPlanTest, q08) {
  checkTpchSql(8);

  // TODO Verify the plan.

  ASSERT_NO_THROW(planTpch(8));
  ASSERT_NO_THROW(planVelox(parseTpchSql(8)));
}

TEST_F(TpchPlanTest, q09) {
  checkTpchSql(9);

  // No plan-shape assertion: q9's `p_name like '%green%'` is not estimable from
  // column stats, so the optimizer over-estimates it and orders the join
  // suboptimally. q09Alt locks the optimal shape using an estimable filter of
  // similar selectivity. See tpch/queries/statistics.md.

  ASSERT_NO_THROW(planTpch(9));
  ASSERT_NO_THROW(planVelox(parseTpchSql(9)));
}

TEST_F(TpchPlanTest, q09Alt) {
  // q9 with `p_name like '%green%'` replaced by the estimable `p_size <= 3`
  // (~6%, similar selectivity). With an accurate estimate the optimizer reduces
  // lineitem by the selective `part` first — the optimal join order. See
  // tpch/queries/statistics.md.
  //
  // TODO: Verify results (no TpchQueryBuilder reference exists for this
  // variant; this asserts plan shape only).
  auto plan = planTpch("q9_alt");

  // Optimal part-first order:
  //
  // agg((
  //   orders INNER (
  //     ((partsupp INNER supplier)
  //        INNER
  //        ((lineitem INNER part) LEFT SEMI (FILTER) (supplier INNER nation)))
  //     INNER nation)))
  auto matcher =
      matchHiveScan("orders")
          .hashJoinInner(
              matchHiveScan("partsupp")
                  .hashJoinInner(matchHiveScan("supplier").build())
                  .hashJoinInner(
                      matchHiveScan("lineitem")
                          .hashJoinInner(matchHiveScan("part").build())
                          .hashJoinLeftSemiFilter(
                              matchHiveScan("supplier")
                                  .hashJoinInner(
                                      matchHiveScan("nation").build())
                                  .project()
                                  .build())
                          .build())
                  .hashJoinInner(matchHiveScan("nation").build())
                  .build())
          .project()
          .aggregation()
          .orderBy()
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(TpchPlanTest, q10) {
  checkTpchSql(10);

  // TODO Verify the plan.

  ASSERT_NO_THROW(planTpch(10));
  ASSERT_NO_THROW(planVelox(parseTpchSql(10)));
}

TEST_F(TpchPlanTest, q11) {
  checkTpchSql(11);

  // The join order is the usual, from large to small. The only particularity is
  // the non-correlated subquery that repeats the same join steps. An
  // optimization opportunity could be to reuse build sides but this is not
  // something that Velox plans support at this time. Also, practical need for
  // this is not very high.

  auto matcher =
      matchHiveScan("partsupp")
          .hashJoinInner(
              matchHiveScan("supplier")
                  .hashJoinInner(
                      core::PlanMatcherBuilder()
                          .hiveScan("nation", test::eq("n_name", "GERMANY"))
                          .build())
                  .build())
          .project()
          .aggregation()
          .nestedLoopJoin(
              matchHiveScan("partsupp")
                  .hashJoinInner(
                      matchHiveScan("supplier")
                          .hashJoinInner(
                              core::PlanMatcherBuilder()
                                  .hiveScan(
                                      "nation", test::eq("n_name", "GERMANY"))
                                  .build())
                          .build())
                  .project()
                  .aggregation()
                  .project()
                  .build())
          .filter()
          .orderBy()
          .project() // TODO Move this 'project' below the 'orderBy'.
          .build();

  auto plan = planTpch(11);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(11)));
}

TEST_F(TpchPlanTest, q12) {
  checkTpchSql(12);

  // In this query, we end up building on lineitem since the filters on it make
  // it the smaller of the two tables. Everything else is unsurprising.

  auto subfieldFilters =
      velox::common::test::SubfieldFiltersBuilder()
          .add(
              "l_shipmode",
              velox::exec::in(std::vector<std::string>{"MAIL", "SHIP"}))
          .add(
              "l_receiptdate",
              velox::exec::between(
                  DATE()->toDays("1994-01-01"), DATE()->toDays("1994-12-31")))
          .build();

  auto matcher =
      matchHiveScan("orders")
          .hashJoinInner(
              core::PlanMatcherBuilder()
                  .hiveScan(
                      "lineitem",
                      std::move(subfieldFilters),
                      "l_commitdate < l_receiptdate AND l_shipdate < l_commitdate")
                  .build())
          .project()
          .aggregation()
          .orderBy()
          .build();

  auto plan = planTpch(12);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(12)));
}

TEST_F(TpchPlanTest, q13) {
  checkTpchSql(13);

  // This query has only two possible plans, left and right hand outer join. We
  // correctly produce the right outer join, building on the left, i.e.
  // customer, as it is much smaller than orders.

  auto matcher = matchHiveScan("orders")
                     .hashJoinRight(matchHiveScan("customer").build())
                     .aggregation()
                     .project()
                     .aggregation()
                     .orderBy()
                     .build();

  auto plan = planTpch(13);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(13)));
}

TEST_F(TpchPlanTest, q14) {
  checkTpchSql(14);

  // The only noteworthy aspect is that we build on lineitem since its filters
  // (1 month out of 7 years) make it smaller than part.

  auto matcher = matchHiveScan("part")
                     .hashJoinInner(
                         core::PlanMatcherBuilder()
                             .hiveScan(
                                 "lineitem",
                                 test::between(
                                     "l_shipdate",
                                     DATE()->toDays("1995-09-01"),
                                     DATE()->toDays("1995-09-30")))
                             .build())
                     .project()
                     .aggregation()
                     .project()
                     .build();

  auto plan = planTpch(14);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(14)));
}

TEST_F(TpchPlanTest, q15) {
  checkTpchSql(15);

  // TODO Verify the plan.

  ASSERT_NO_THROW(planTpch(15));
  ASSERT_NO_THROW(planVelox(parseTpchSql(15)));
}

TEST_F(TpchPlanTest, q16) {
  // TODO Fix "DISTINCT option for aggregation is supported only in single
  // worker, single thread mode" and restore the original text of q16 that uses
  // count(distinct).
  checkTpchSql(16);

  // The join is biggest table first, with part joined first because it is quite
  // selective, more so than the exists with supplier.

  auto matcher =
      matchHiveScan("partsupp")
          .hashJoinInner(matchHiveScan("part").build())
          .hashJoinAnti(matchHiveScan("supplier").build(), {.nullAware = true})
          .aggregation()
          .orderBy()
          .build();

  auto plan = planTpch(16);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(16)));
}

TEST_F(TpchPlanTest, q17) {
  checkTpchSql(17);

  // The trick here is that we have a correlated subquery that flattens into a
  // group by that aggregates over all of lineitem. We correctly observe that
  // only lineitems with a very specific part will occur on the probe side, so
  // we copy the restriction inside the group by as a semijoin (exists).

  auto matcher =
      matchHiveScan("lineitem")
          .hashJoinInner(matchHiveScan("part").build())
          .hashJoinLeft(
              matchHiveScan("lineitem")
                  .hashJoinLeftSemiFilter(matchHiveScan("part").build())
                  .aggregation()
                  .project() // TODO Figure out if it can be removed.
                  .build())
          .filter()
          .aggregation()
          .project()
          .build();

  auto plan = planTpch(17);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(17)));
}

TEST_F(TpchPlanTest, q18) {
  checkTpchSql(18);

  // The subquery (aggregated lineitem with HAVING sum > 300) is very
  // selective. It is joined first via an implied semi-join through the
  // l_orderkey = o_orderkey equivalence class, reducing lineitem before
  // joining with orders and customer.

  // agg((
  //   (lineitem INNER (orders INNER customer))
  //   LEFT SEMI (FILTER)
  //   agg(lineitem)
  // ))

  auto matcher =
      matchHiveScan("lineitem")
          .hashJoinLeftSemiFilter(matchHiveScan("lineitem")
                                      .aggregation()
                                      .filter()
                                      .project()
                                      .build())
          .hashJoinInner(matchHiveScan("orders")
                             .hashJoinInner(matchHiveScan("customer").build())
                             .build())
          .aggregation()
          .topN()
          .build();

  auto plan = planTpch(18);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(18)));
}

TEST_F(TpchPlanTest, q19) {
  checkTpchSql(19);

  // The trick is to extract common pieces to push down into the scan of
  // lineitem and part from the OR of three ANDs in the single where clause. We
  // extract the join condition that is present in all three disjuncts of the
  // or. Then we extract an OR to push down into the scan of part and lineitem.
  // We build on part, as it is the smaller table.

  auto lineitemFilters =
      common::test::SubfieldFiltersBuilder()
          .add("l_shipinstruct", exec::equal("DELIVER IN PERSON"))
          .add(
              "l_shipmode",
              exec::in(std::vector<std::string>{"AIR", "AIR REG"}))
          .add("l_quantity", exec::betweenDouble(1.0, 30.0))
          .build();

  auto matcher =
      core::PlanMatcherBuilder()
          .hiveScan("lineitem", std::move(lineitemFilters))
          .hashJoinInner(
              core::PlanMatcherBuilder()
                  .hiveScan(
                      "part",
                      {},
                      "\"or\"(\"and\"(p_size between 1 and 15, (p_brand = 'Brand#34' AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))), "
                      "   \"or\"(\"and\"(p_size between 1 and 5, (p_brand = 'Brand#12' AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))), "
                      "          \"and\"(p_size between 1 and 10, (p_brand = 'Brand#23' AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')))))")
                  .build())
          .filter()
          .project()
          .aggregation()
          .build();

  auto plan = planTpch(19);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(19)));
}

TEST_F(TpchPlanTest, q20) {
  checkTpchSql(20);

  // The lineitem aggregation (sum of quantity per part+supplier) is joined to
  // partsupp via a multi-key existence semijoin pushed into the aggregation DT.
  // The part filter (name LIKE 'forest%') further reduces partsupp rows.
  //
  // ((agg(lineitem)
  //  RIGHT
  //  (part RIGHT SEMI (FILTER) (partsupp LEFT SEMI (FILTER) (supplier INNER
  //  nation)))) RIGHT SEMI (FILTER) (supplier INNER nation))
  auto matcher =
      matchHiveScan("lineitem")
          .aggregation()
          .project()
          .hashJoinRight(
              matchHiveScan("part")
                  .hashJoinRightSemiFilter(
                      matchHiveScan("partsupp")
                          .hashJoinLeftSemiFilter(
                              matchHiveScan("supplier")
                                  .hashJoinInner(
                                      matchHiveScan("nation").build())
                                  .project()
                                  .build())
                          .build())
                  .build())
          .filter()
          .project()
          .hashJoinRightSemiFilter(
              matchHiveScan("supplier")
                  .hashJoinInner(matchHiveScan("nation").build())
                  .build())
          .orderBy()
          .build();

  auto plan = planTpch(20);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(20)));
}

TEST_F(TpchPlanTest, q21) {
  checkTpchSql(21);

  // TODO Verify the plan.

  ASSERT_NO_THROW(planTpch(21));
  ASSERT_NO_THROW(planVelox(parseTpchSql(21)));
}

TEST_F(TpchPlanTest, q22) {
  checkTpchSql(22);

  // The query is straightforward, with the not exists resolved with a right
  // semijoin and the non-correlated subquery becoming a cross join to the one
  // row result set of the non-grouped aggregation.

  auto matcher =
      matchHiveScan("orders")
          .hashJoinRightSemiProject(
              matchHiveScan("customer")
                  .nestedLoopJoin(
                      matchHiveScan("customer").aggregation().build())
                  .filter()
                  .build(),
              {.nullAware = false})
          .filter()
          .project()
          .aggregation()
          .orderBy()
          .build();

  auto plan = planTpch(22);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(22)));
}

// Use to re-generate the plans stored in tpch/plans directory.
TEST_F(TpchPlanTest, DISABLED_makePlans) {
  const auto path = test::getTestFilePath("tpch/plans");

  const MultiFragmentPlan::Options options{.numWorkers = 1, .numDrivers = 1};

  for (auto q = 1; q <= 22; ++q) {
    LOG(ERROR) << "q" << q;

    auto logicalPlan = parseTpchSql(q);
    planVelox(
        logicalPlan,
        options,
        /*optimizerOptions=*/std::nullopt,
        fmt::format("{}/q{}", path, q));
  }
}

} // namespace
} // namespace facebook::axiom::optimizer

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
