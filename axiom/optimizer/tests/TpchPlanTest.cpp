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
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/tests/TestDataPath.h"
#include "axiom/optimizer/tests/TpchQueries.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

// Asserts the single-node plan shape of each TPC-H query. Statistics are
// injected via `TestConnector::addTpchTables(scaleFactor)`, so no data is
// generated and the optimizer plans at any scale instantly. Foreign keys are
// consistent at every scale, so the plans reflect real TPC-H rather than the
// data generator's behavior below scale factor 1. Result correctness is checked
// separately in `TpchResultTest`. Filters become separate nodes here because
// the test connector does not push them into the scan.
class TpchPlanTest : public test::QueryTestBase {
 protected:
  static constexpr double kScaleFactor = 1.0;

  void configureTestConnector() override {
    testConnector_->addTpchTables(kScaleFactor);
  }

  lp::LogicalPlanNodePtr parseTpch(int32_t query) {
    return parseSelect(test::readTpchSql(query), kTestConnectorId);
  }

  velox::core::PlanNodePtr planTpch(int32_t query) {
    return toSingleNodePlan(parseTpch(query));
  }

  velox::core::PlanNodePtr planTpch(const std::string& name) {
    return toSingleNodePlan(
        parseSelect(test::readTpchSql(name), kTestConnectorId));
  }
};

// Verifies that the injected statistics produce the expected base-table
// cardinalities at 'kScaleFactor'. Every node in a bare-scan plan reports the
// table cardinality since no operator changes the row count.
TEST_F(TpchPlanTest, stats) {
  auto verifyStats = [&](const std::string& tableName, int64_t cardinality) {
    SCOPED_TRACE(tableName);
    lp::PlanBuilder::Context ctx{kTestConnectorId, "default"};
    auto logicalPlan = lp::PlanBuilder(ctx).tableScan(tableName).build();
    auto prediction = planVelox(logicalPlan).prediction;
    ASSERT_FALSE(prediction.empty());
    for (const auto& [nodeId, value] : prediction) {
      EXPECT_EQ(value.cardinality, cardinality);
    }
  };

  verifyStats("region", 5);
  verifyStats("nation", 25);
  verifyStats("supplier", 10'000);
  verifyStats("orders", 1'500'000);
  verifyStats("lineitem", 6'001'215);
}

TEST_F(TpchPlanTest, q01) {
  auto matcher = matchScan("lineitem")
                     .filter("l_shipdate < date '1998-09-03'")
                     .project()
                     .aggregation()
                     .orderBy()
                     .build();
  AXIOM_ASSERT_PLAN(planTpch(1), matcher);
}

TEST_F(TpchPlanTest, q02) {
  auto matcher =
      matchScan("partsupp")
          .hashJoinInner(matchScan("part")
                             .filter("p_size = 15 and p_type like '%BRASS'")
                             .build())
          .hashJoinInner(
              matchScan("supplier")
                  .hashJoinInner(
                      matchScan("nation")
                          .hashJoinInner(matchScan("region")
                                             .filter("r_name = 'EUROPE'")
                                             .build())
                          .build())
                  .build())
          .hashJoinInner(
              matchScan("partsupp")
                  .hashJoinLeftSemiFilter(
                      matchScan("part")
                          .filter("p_size = 15 and p_type like '%BRASS'")
                          .build())
                  .hashJoinInner(
                      matchScan("supplier")
                          .hashJoinInner(
                              matchScan("nation")
                                  .hashJoinInner(
                                      matchScan("region")
                                          .aliases(
                                              {std::nullopt, "region_name"})
                                          .filter("region_name = 'EUROPE'")
                                          .build())
                                  .build())
                          .build())
                  .aggregation()
                  .project()
                  .build())
          .topN()
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(2), matcher);
}

TEST_F(TpchPlanTest, q03) {
  auto matcher =
      matchScan("lineitem")
          .filter("l_shipdate > date '1995-03-15'")
          .hashJoinInner(
              matchScan("orders")
                  .filter("o_orderdate < date '1995-03-15'")
                  .hashJoinInner(matchScan("customer")
                                     .filter("c_mktsegment = 'BUILDING'")
                                     .build())
                  .build())
          .project()
          .aggregation()
          .topN()
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(3), matcher);
}

TEST_F(TpchPlanTest, q04) {
  auto matcher =
      matchScan("lineitem")
          .filter("l_commitdate < l_receiptdate")
          .hashJoinRightSemiFilter(
              matchScan("orders")
                  .filter(
                      "o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-10-01'")
                  .build())
          .aggregation()
          .orderBy()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(4), matcher);
}

TEST_F(TpchPlanTest, q05) {
  auto matcher =
      matchScan("lineitem")
          .hashJoinInner(
              matchScan("orders")
                  .filter(
                      "o_orderdate >= date '1994-01-01' and o_orderdate < date '1995-01-01'")
                  .hashJoinInner(
                      matchScan("customer")
                          .hashJoinInner(
                              matchScan("nation")
                                  .hashJoinInner(matchScan("region")
                                                     .filter("r_name = 'ASIA'")
                                                     .build())
                                  .build())
                          .build())
                  .build())
          .hashJoinInner(
              matchScan("supplier")
                  .hashJoinLeftSemiFilter(
                      matchScan("nation")
                          .hashJoinInner(matchScan("region")
                                             .filter("r_name = 'ASIA'")
                                             .build())
                          .project()
                          .build())
                  .build())
          .project()
          .aggregation()
          .orderBy()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(5), matcher);
}

TEST_F(TpchPlanTest, q06) {
  auto matcher =
      matchScan("lineitem")
          .filter(
              "\"and\"(l_shipdate >= date '1994-01-01', l_shipdate < date '1995-01-01', "
              "         l_discount between 0.05 and 0.07, l_quantity < 24.0)")
          .project()
          .aggregation()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(6), matcher);
}

TEST_F(TpchPlanTest, q07) {
  ASSERT_NO_THROW(planTpch(7));
}

TEST_F(TpchPlanTest, q08) {
  ASSERT_NO_THROW(planTpch(8));
}

// No plan-shape assertion: q9's `p_name like '%green%'` is not estimable from
// column stats, so the optimizer over-estimates it and orders the join
// suboptimally. q09Alt locks the optimal shape using an estimable filter of
// similar selectivity. See tpch/queries/statistics.md.
TEST_F(TpchPlanTest, q09) {
  ASSERT_NO_THROW(planTpch(9));
}

// q9 with `p_name like '%green%'` replaced by the estimable `p_size <= 3`
// (~6%, similar selectivity). With an accurate estimate the optimizer reduces
// lineitem by the selective `part` first. See tpch/queries/statistics.md.
TEST_F(TpchPlanTest, q09Alt) {
  auto matcher =
      matchScan("orders")
          .hashJoinInner(
              matchScan("lineitem")
                  .hashJoinInner(
                      matchScan("partsupp")
                          .hashJoinInner(
                              matchScan("part").filter("p_size <= 3").build())
                          .build())
                  .build())
          .hashJoinInner(matchScan("supplier").build())
          .hashJoinInner(matchScan("nation").build())
          .project()
          .aggregation()
          .orderBy()
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch("q9_alt"), matcher);
}

TEST_F(TpchPlanTest, q10) {
  ASSERT_NO_THROW(planTpch(10));
}

TEST_F(TpchPlanTest, q11) {
  auto matcher =
      matchScan("partsupp")
          .hashJoinInner(
              matchScan("supplier")
                  .hashJoinInner(
                      matchScan("nation").filter("n_name = 'GERMANY'").build())
                  .build())
          .project()
          .aggregation()
          .nestedLoopJoin(
              matchScan("partsupp")
                  .hashJoinInner(
                      matchScan("supplier")
                          .hashJoinInner(
                              matchScan("nation")
                                  .aliases({std::nullopt, "nation_name"})
                                  .filter("nation_name = 'GERMANY'")
                                  .build())
                          .build())
                  .project()
                  .aggregation()
                  .project()
                  .build())
          .filter("value > expr")
          .orderBy()
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(11), matcher);
}

TEST_F(TpchPlanTest, q12) {
  auto matcher =
      matchScan("orders")
          .hashJoinInner(
              matchScan("lineitem")
                  .filter(
                      "\"and\"(l_shipmode in ('MAIL', 'SHIP'), l_receiptdate >= date '1994-01-01', "
                      "         l_receiptdate < date '1995-01-01', l_commitdate < l_receiptdate, "
                      "         l_shipdate < l_commitdate)")
                  .build())
          .project()
          .aggregation()
          .orderBy()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(12), matcher);
}

TEST_F(TpchPlanTest, q13) {
  auto matcher = matchScan("orders")
                     .filter("o_comment not like '%special%requests%'")
                     .hashJoinRight(matchScan("customer").build())
                     .aggregation()
                     .project()
                     .aggregation()
                     .orderBy()
                     .build();
  AXIOM_ASSERT_PLAN(planTpch(13), matcher);
}

TEST_F(TpchPlanTest, q14) {
  auto matcher =
      matchScan("part")
          .hashJoinInner(
              matchScan("lineitem")
                  .filter(
                      "l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-10-01'")
                  .build())
          .project()
          .aggregation()
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(14), matcher);
}

TEST_F(TpchPlanTest, q15) {
  ASSERT_NO_THROW(planTpch(15));
}

TEST_F(TpchPlanTest, q16) {
  auto matcher =
      matchScan("partsupp")
          .hashJoinInner(
              matchScan("part")
                  .filter(
                      "\"and\"(p_brand <> 'Brand#45', p_type not like 'MEDIUM POLISHED%', "
                      "         p_size in (49, 14, 23, 45, 19, 3, 36, 9))")
                  .build())
          .hashJoinAnti(
              matchScan("supplier")
                  .filter("s_comment like '%Customer%Complaints%'")
                  .build(),
              {.nullAware = true})
          .aggregation()
          .orderBy()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(16), matcher);
}

TEST_F(TpchPlanTest, q17) {
  auto matcher =
      matchScan("lineitem")
          .hashJoinInner(
              matchScan("part")
                  .filter("p_brand = 'Brand#23' and p_container = 'MED BOX'")
                  .build())
          .hashJoinLeft(
              matchScan("lineitem")
                  .hashJoinLeftSemiFilter(
                      matchScan("part")
                          .filter(
                              "p_brand = 'Brand#23' and p_container = 'MED BOX'")
                          .build())
                  .aggregation()
                  .project()
                  .aliases({std::nullopt, "quantity_limit"})
                  .build())
          .filter("l_quantity < quantity_limit")
          .aggregation()
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(17), matcher);
}

TEST_F(TpchPlanTest, q18) {
  auto matcher = matchScan("lineitem")
                     .hashJoinLeftSemiFilter(matchScan("lineitem")
                                                 .aggregation()
                                                 .filter("\"sum\" > 300.0")
                                                 .project()
                                                 .build())
                     .hashJoinInner(matchScan("orders").build())
                     .hashJoinInner(matchScan("customer").build())
                     .aggregation()
                     .topN()
                     .build();
  AXIOM_ASSERT_PLAN(planTpch(18), matcher);
}

TEST_F(TpchPlanTest, q19) {
  auto matcher =
      matchScan("lineitem")
          .filter(
              "\"and\"(l_shipmode in ('AIR', 'AIR REG'), l_shipinstruct = 'DELIVER IN PERSON', "
              "       \"or\"(\"and\"(l_quantity >= 20.0, l_quantity <= 30.0), "
              "             \"or\"(\"and\"(l_quantity >= 1.0, l_quantity <= 11.0), "
              "                   \"and\"(l_quantity >= 10.0, l_quantity <= 20.0))))")
          .hashJoinInner(
              matchScan("part")
                  .filter(
                      "\"or\"(\"and\"(p_size between 1 and 15, \"and\"(p_brand = 'Brand#34', p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))), "
                      "      \"or\"(\"and\"(p_size between 1 and 5, \"and\"(p_brand = 'Brand#12', p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))), "
                      "            \"and\"(p_size between 1 and 10, \"and\"(p_brand = 'Brand#23', p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')))))")
                  .build())
          .filter(
              "\"or\"(\"and\"(p_size between 1 and 15, \"and\"(l_quantity <= 30.0, \"and\"(l_quantity >= 20.0, \"and\"(p_brand = 'Brand#34', p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))))), "
              "      \"or\"(\"and\"(p_size between 1 and 5, \"and\"(l_quantity <= 11.0, \"and\"(l_quantity >= 1.0, \"and\"(p_brand = 'Brand#12', p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))))), "
              "            \"and\"(p_size between 1 and 10, \"and\"(l_quantity <= 20.0, \"and\"(l_quantity >= 10.0, \"and\"(p_brand = 'Brand#23', p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')))))))")
          .project()
          .aggregation()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(19), matcher);
}

TEST_F(TpchPlanTest, q20) {
  auto matcher =
      matchScan("lineitem")
          .filter(
              "l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01'")
          .aggregation()
          .project()
          .hashJoinRight(
              matchScan("part")
                  .filter("p_name like 'forest%'")
                  .hashJoinRightSemiFilter(
                      matchScan("partsupp")
                          .hashJoinLeftSemiFilter(
                              matchScan("supplier")
                                  .hashJoinInner(
                                      matchScan("nation")
                                          .filter("n_name = 'CANADA'")
                                          .build())
                                  .project()
                                  .build())
                          .build())
                  .build())
          .filter("expr < cast(ps_availqty as double)")
          .project()
          .hashJoinRightSemiFilter(
              matchScan("supplier")
                  .hashJoinInner(
                      matchScan("nation").filter("n_name = 'CANADA'").build())
                  .build())
          .orderBy()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(20), matcher);
}

TEST_F(TpchPlanTest, q21) {
  ASSERT_NO_THROW(planTpch(21));
}

TEST_F(TpchPlanTest, q22) {
  auto matcher =
      matchScan("orders")
          .hashJoinRightSemiProject(
              matchScan("customer")
                  .filter(
                      "substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')")
                  .nestedLoopJoin(
                      matchScan("customer")
                          .aliases({"acctbal", "phone"})
                          .filter(
                              "acctbal > 0.0 and substr(phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')")
                          .singleAggregation(
                              {}, {"avg(acctbal) as avg_acctbal"})
                          .build())
                  .filter("c_acctbal > avg_acctbal")
                  .build(),
              {.nullAware = false})
          .aliases({std::nullopt, std::nullopt, "exists_mark"})
          .filter("not(exists_mark)")
          .project()
          .aggregation()
          .orderBy()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(22), matcher);
}

// Use to re-generate the plans stored in the tpch/plans directory.
TEST_F(TpchPlanTest, DISABLED_makePlans) {
  const auto path = test::getTestFilePath("tpch/plans");
  const MultiFragmentPlan::Options options{.numWorkers = 1, .numDrivers = 1};
  for (int32_t query = 1; query <= 22; ++query) {
    LOG(ERROR) << "q" << query;
    planVelox(
        parseTpch(query),
        options,
        /*optimizerOptions=*/std::nullopt,
        fmt::format("{}/q{}", path, query));
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
