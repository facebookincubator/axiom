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
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/tests/TpchQueries.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

using namespace facebook::velox;

// Asserts the single-node plan shape of each TPC-H query as produced by the v2
// optimizer. Statistics are injected via `TestConnector::addTpchTables`, so no
// data is generated and the optimizer plans at any scale instantly. Queries
// whose v2 plan already matches v1's optimal shape carry a full matcher; the
// rest assert only that planning succeeds, with the remaining gaps tracked in
// `axiom/optimizer/v2/docs/TpchV1V2PlanComparison.md`. Result correctness is
// checked separately in `TpchResultTest`.
class TpchPlanTest : public optimizer::test::QueryTestBase {
 public:
  TpchPlanTest() {
    useV2_ = true;
  }

 protected:
  static constexpr double kScaleFactor = 1.0;

  void configureTestConnector() override {
    testConnector_->addTpchTables(kScaleFactor);
  }

  velox::core::PlanNodePtr planTpch(int32_t query) {
    return toSingleNodePlan(
        parseSelect(optimizer::test::readTpchSql(query), kTestConnectorId));
  }

  velox::core::PlanNodePtr planTpch(const std::string& name) {
    return toSingleNodePlan(
        parseSelect(optimizer::test::readTpchSql(name), kTestConnectorId));
  }

  // Options for a multi-node plan-shape check. numWorkers > 1 splits the plan
  // into fragments; numDrivers > 1 adds intra-node local exchanges.
  struct MultiNodeOptions {
    int32_t query;
    int32_t numWorkers{2};
    int32_t numDrivers{1};
  };

  std::shared_ptr<const MultiFragmentPlan> planTpchMultiNode(
      const MultiNodeOptions& options) {
    return planVelox(
               parseSelect(
                   optimizer::test::readTpchSql(options.query),
                   kTestConnectorId),
               {
                   .numWorkers = options.numWorkers,
                   .numDrivers = options.numDrivers,
               })
        .plan;
  }
};

TEST_F(TpchPlanTest, q01) {
  auto matcher = matchScan("lineitem")
                     .filter("l_shipdate < date '1998-09-03'")
                     .project()
                     .aggregation()
                     .orderBy()
                     .build();
  AXIOM_ASSERT_PLAN(planTpch(1), matcher);

  auto distributed = [&](bool isMultiThreaded) {
    return matchScan("lineitem")
        .multiThreaded(isMultiThreaded)
        .filter("l_shipdate < date '1998-09-03'")
        .project(
            {"l_returnflag",
             "l_linestatus",
             "l_quantity",
             "l_extendedprice",
             "l_extendedprice * (1.0 - l_discount) as disc_price",
             "l_extendedprice * (1.0 - l_discount) * (l_tax + 1.0) as charge",
             "l_discount"})
        .distributedAggregation(
            {"l_returnflag", "l_linestatus"},
            {"sum(l_quantity)",
             "sum(l_extendedprice)",
             "sum(disc_price)",
             "sum(charge)",
             "avg(l_quantity)",
             "avg(l_extendedprice)",
             "avg(l_discount)",
             "count()"})
        .distributedOrderBy({"l_returnflag", "l_linestatus"})
        .build();
  };

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planTpchMultiNode({.query = 1, .numDrivers = 1}), distributed(false));
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planTpchMultiNode({.query = 1, .numDrivers = 4}), distributed(true));
}

TEST_F(TpchPlanTest, q02) {
  // The correlated min-cost subquery is decorrelated by aggregating partsupp
  // once per ps_partkey and joining the result back to the outer, so
  // min(ps_supplycost) is computed once per part rather than once per outer
  // row, with no AssignUniqueId.
  //
  // region appears on both the outer and subquery sides, so bind its r_name to
  // a stable alias rather than the per-side disambiguated name.
  auto supplierInEurope = [](const std::string& regionName) {
    return matchScan("supplier")
        .hashJoinInner(
            matchScan("nation")
                .hashJoinInner(matchScan("region")
                                   .aliases({std::nullopt, regionName})
                                   .filter(regionName + " = 'EUROPE'")
                                   .build())
                .build());
  };

  // The outer query's table tree: partsupp joined to the filtered part and to
  // supplier in Europe.
  auto outerTables =
      matchScan("partsupp")
          .hashJoinInner(matchScan("part")
                             .filter("p_size = 15 and p_type like '%BRASS'")
                             .build())
          .hashJoinInner(supplierInEurope("r_name_outer").build())
          .build();

  // The aggregated subquery min(ps_supplycost) per ps_partkey, joined back to
  // the outer table tree.
  auto matcher = matchScan("partsupp")
                     .hashJoinInner(supplierInEurope("r_name_sub").build())
                     .aggregation()
                     .hashJoinInner(outerTables)
                     .project()
                     .topN()
                     .build();
  AXIOM_ASSERT_PLAN(planTpch(2), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 2, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 2, .numDrivers = 4}));
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
          .project()
          .topN()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(3), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 3, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 3, .numDrivers = 4}));
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

  ASSERT_NO_THROW(planTpchMultiNode({.query = 4, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 4, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q05) {
  // Transitive equality (c_nationkey = s_nationkey = n_nationkey) lets the
  // cyclic nation⋈region[ASIA] diamond attach to customer rather than supplier:
  // customer ⋈ (nation ⋈ region[ASIA]) reduces orders[date], lineitem probes
  // that, and supplier joins last. lineitem then receives both the orderkey
  // (orders) and suppkey (supplier) dynamic filters — 6M -> 184k on SF1.
  auto matcher =
      matchScan("lineitem")
          .hashJoinInner(
              matchScan("orders")
                  .filter(
                      "o_orderdate >= date '1994-01-01' "
                      "and o_orderdate < date '1995-01-01'")
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
          .hashJoinInner(matchScan("supplier").build())
          .project()
          .aggregation()
          .orderBy()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(5), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 5, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 5, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q06) {
  const auto filter =
      "\"and\"(l_shipdate >= date '1994-01-01', l_shipdate < date '1995-01-01', "
      "         l_discount between 0.05 and 0.07, l_quantity < 24.0)";
  auto matcher =
      matchScan("lineitem").filter(filter).project().aggregation().build();
  AXIOM_ASSERT_PLAN(planTpch(6), matcher);

  auto distributed = [&](bool isMultiThreaded) {
    return matchScan("lineitem")
        .multiThreaded(isMultiThreaded)
        .filter(filter)
        .project({"l_extendedprice * l_discount as revenue"})
        .distributedAggregation({}, {"sum(revenue)"})
        .build();
  };

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planTpchMultiNode({.query = 6, .numDrivers = 1}), distributed(false));
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planTpchMultiNode({.query = 6, .numDrivers = 4}), distributed(true));
}

TEST_F(TpchPlanTest, q07) {
  // Join order: (lineitem INNER (supplier INNER nation)) INNER
  //             (orders INNER (customer INNER nation)).
  //
  // The cross-nation OR is split into a per-side restriction pushed to each
  // nation scan, with deterministic — and opposite — operand order: across the
  // disjuncts (n1=FRANCE, n2=GERMANY) then (n1=GERMANY, n2=FRANCE), supplier's
  // nation (n1) sees {FRANCE, GERMANY} and customer's nation (n2) sees
  // {GERMANY, FRANCE}.
  //
  // The full OR rides as the top join's filter; matching hashJoin -> project
  // here asserts there is no intervening post-join Filter. nation columns are
  // bound by alias since the suffix (n_name vs n_name_1) varies.
  auto matcher =
      matchScan("lineitem")
          .filter("l_shipdate between date '1995-01-01' and date '1996-12-31'")
          .hashJoinInner(
              matchScan("supplier")
                  .hashJoinInner(
                      matchScan("nation")
                          .aliases({std::nullopt, "supp_nation"})
                          .filter(
                              "supp_nation = 'FRANCE' or supp_nation = 'GERMANY'")
                          .build())
                  .build())
          .hashJoinInner(
              matchScan("orders")
                  .hashJoinInner(
                      matchScan("customer")
                          .hashJoinInner(
                              matchScan("nation")
                                  .aliases({std::nullopt, "cust_nation"})
                                  .filter(
                                      "cust_nation = 'GERMANY' or cust_nation = 'FRANCE'")
                                  .build())
                          .build())
                  .build())
          .project()
          .aggregation()
          .orderBy()
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(7), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 7, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 7, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q08) {
  // All-inner join order:
  //
  // supplier INNER (((orders[date] INNER (lineitem INNER part)) INNER
  // (customer INNER (nation INNER region))) INNER nation.
  //
  // supplier probes; the orders[date]/lineitem/part and customer/nation/region
  // subtrees build, and the second nation (supplying the supplier's nation
  // name) joins last. part (`p_type`) and region (`r_name`) are filtered; the
  // selective part build reduces the lineitem fact scan at runtime via dynamic
  // filtering.
  auto matcher =
      matchScan("supplier")
          .hashJoinInner(
              matchScan("orders")
                  .filter(
                      "o_orderdate between date '1995-01-01' and date '1996-12-31'")
                  .hashJoinInner(
                      matchScan("lineitem")
                          .hashJoinInner(
                              matchScan("part")
                                  .filter("p_type = 'ECONOMY ANODIZED STEEL'")
                                  .build())
                          .build())
                  .hashJoinInner(
                      matchScan("customer")
                          .hashJoinInner(
                              matchScan("nation")
                                  .hashJoinInner(
                                      matchScan("region")
                                          .filter("r_name = 'AMERICA'")
                                          .build())
                                  .build())
                          .build())
                  .build())
          .hashJoinInner(matchScan("nation").build())
          .project()
          .project()
          .aggregation()
          .project()
          .orderBy()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(8), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 8, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 8, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q09) {
  // No plan-shape assertion: q9's `p_name like '%green%'` is not estimable from
  // column stats, so the optimizer plans the join order without knowing the
  // filter is selective and the resulting shape is not optimal. q09Alt locks
  // the optimal shape using an estimable filter of similar selectivity.
  ASSERT_NO_THROW(planTpch(9));

  ASSERT_NO_THROW(planTpchMultiNode({.query = 9, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 9, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q09Alt) {
  // q9 with `p_name like '%green%'` replaced by the estimable `p_size <= 3`
  // (~6%, similar selectivity). With an accurate estimate the optimizer reduces
  // lineitem by the selective part first: part ⋈ partsupp builds, lineitem
  // probes on the composite (l_partkey, l_suppkey) = (ps_partkey, ps_suppkey)
  // key, then orders and supplier ⋈ nation join above.
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
          .hashJoinInner(matchScan("supplier")
                             .hashJoinInner(matchScan("nation").build())
                             .build())
          .project()
          .singleAggregation({"n_name", "o_year"}, {"sum(amount)"})
          .orderBy({"n_name", "o_year DESC"})
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch("q9_alt"), matcher);

  auto logicalPlan =
      parseSelect(optimizer::test::readTpchSql("q9_alt"), kTestConnectorId);
  ASSERT_NO_THROW(planVelox(logicalPlan, {.numWorkers = 2, .numDrivers = 1}));
  ASSERT_NO_THROW(planVelox(logicalPlan, {.numWorkers = 2, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q10) {
  // The selective 3-month `orders` date range drives the deepest join:
  // lineitem[returnflag=R] ⋈ orders[date] builds, customer probes, and nation
  // joins last.
  auto matcher =
      matchScan("customer")
          .hashJoinInner(
              matchScan("lineitem")
                  .filter("l_returnflag = 'R'")
                  .hashJoinInner(matchScan("orders")
                                     .filter(
                                         "o_orderdate >= date '1993-10-01' "
                                         "and o_orderdate < date '1994-01-01'")
                                     .build())
                  .build())
          .hashJoinInner(matchScan("nation").build())
          .project(
              {"c_custkey",
               "c_name",
               "c_acctbal",
               "c_phone",
               "n_name",
               "c_address",
               "c_comment",
               "l_extendedprice * (1.0 - l_discount) as rev"})
          .singleAggregation(
              {"c_custkey",
               "c_name",
               "c_acctbal",
               "c_phone",
               "n_name",
               "c_address",
               "c_comment"},
              {"sum(rev) as revenue"})
          .project()
          .topN(20)
          .build();
  AXIOM_ASSERT_PLAN(planTpch(10), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 10, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 10, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q11) {
  // The HAVING threshold is an uncorrelated scalar subquery, so it is computed
  // once and cross-joined to the per-partkey aggregation via a NestedLoopJoin
  // applying value > threshold. The subquery is a global aggregate (exactly one
  // row), so no EnforceSingleRow guard is needed. Both sides scan
  // partsupp ⋈ supplier ⋈ nation[GERMANY] independently.

  auto germanyPartsupp =
      [](const std::string& nationName,
         std::vector<std::optional<std::string>> psAliases = {}) {
        auto partsupp = matchScan("partsupp");
        if (!psAliases.empty()) {
          partsupp.aliases(std::move(psAliases));
        }
        return partsupp.hashJoinInner(
            matchScan("supplier")
                .hashJoinInner(matchScan("nation")
                                   .aliases({std::nullopt, nationName})
                                   .filter(nationName + " = 'GERMANY'")
                                   .build())
                .build());
      };

  auto matcher =
      germanyPartsupp("n_name")
          .project(
              {"ps_partkey",
               "ps_supplycost * cast(ps_availqty as double) as v"})
          .singleAggregation({"ps_partkey"}, {"sum(v) as value"})
          .nestedLoopJoin(
              germanyPartsupp("n_name_sub", {std::nullopt, "aq", "sc"})
                  .project({"sc * cast(aq as double) as s"})
                  .singleAggregation({}, {"sum(s) as totalSum"})
                  .project({"totalSum * 0.0001 as expr"})
                  .build())
          .orderBy({"value DESC"})
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(11), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 11, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 11, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q12) {
  const auto filter =
      "\"and\"(l_shipmode in ('MAIL', 'SHIP'), l_commitdate < l_receiptdate, "
      "         l_shipdate < l_commitdate, l_receiptdate >= date '1994-01-01', "
      "         l_receiptdate < date '1995-01-01')";

  auto matcher =
      matchScan("orders")
          .hashJoinInner(matchScan("lineitem").filter(filter).build())
          .project()
          .aggregation()
          .orderBy()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(12), matcher);

  auto distributed = [&](bool isMultiThreaded) {
    return matchScan("orders")
        .multiThreaded(isMultiThreaded)
        .hashJoinInner(matchScan("lineitem").filter(filter).broadcast().build())
        .project({
            "l_shipmode",
            "if(o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH', 1, 0) as high_line_count",
            "if(o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH', 1, 0) as low_line_count",
        })
        .distributedAggregation(
            {"l_shipmode"}, {"sum(high_line_count)", "sum(low_line_count)"})
        .distributedOrderBy({"l_shipmode"})
        .build();
  };

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planTpchMultiNode({.query = 12, .numDrivers = 1}), distributed(false));
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planTpchMultiNode({.query = 12, .numDrivers = 4}), distributed(true));
}

TEST_F(TpchPlanTest, q13) {
  // The LEFT OUTER customer-orders count is computed as a RIGHT join (orders
  // probe, customer build, so every customer is preserved) feeding a
  // per-custkey count(o_orderkey), then a group-by on that count for custdist.
  // The orders filter sits in the join's ON clause, so it restricts orders
  // before the outer join, not after.
  //
  // TODO: The inner count keeps its natural name `count` through the plan,
  // leaving a redundant identity Project above the inner aggregation and a
  // trailing Project that renames count -> c_count at the very end. The rename
  // should fold into the inner aggregation's output. See Gap 2 in
  // TpchV1V2PlanComparison.md.
  auto matcher =
      matchScan("orders")
          .filter("o_comment not like '%special%requests%'")
          .hashJoinRight(matchScan("customer").build())
          .singleAggregation({"c_custkey"}, {"count(o_orderkey) as cnt"})
          .project()
          .singleAggregation({"cnt"}, {"count() as custdist"})
          .orderBy({"custdist DESC", "cnt DESC"})
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(13), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 13, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 13, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q14) {
  const auto filter =
      "l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-10-01'";
  auto matcher =
      matchScan("part")
          .hashJoinInner(matchScan("lineitem").filter(filter).build())
          .project()
          .aggregation()
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(14), matcher);

  auto distributed = [&](bool isMultiThreaded) {
    return matchScan("part")
        .multiThreaded(isMultiThreaded)
        .hashJoinInner(matchScan("lineitem").filter(filter).broadcast().build())
        .project({
            "if(p_type like 'PROMO%', l_extendedprice * (1.0 - l_discount), 0.0) as promo",
            "l_extendedprice * (1.0 - l_discount) as disc_price",
        })
        .distributedAggregation({}, {"sum(promo)", "sum(disc_price)"})
        .project()
        .build();
  };

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planTpchMultiNode({.query = 14, .numDrivers = 1}), distributed(false));
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planTpchMultiNode({.query = 14, .numDrivers = 4}), distributed(true));
}

TEST_F(TpchPlanTest, q15) {
  // The `revenue` CTE (per-supplier revenue over the 3-month window) is
  // computed twice: once for the supplier join and once inside the scalar max
  // subquery. Velox has no CTE/common-subplan sharing, so the duplication is
  // expected. The max subquery is a global aggregate (exactly one row), so no
  // EnforceSingleRow guard is needed.
  auto revenueAgg = [](std::vector<std::optional<std::string>> aliases = {}) {
    auto lineitem = matchScan("lineitem");
    if (!aliases.empty()) {
      lineitem.aliases(std::move(aliases));
    }
    return lineitem
        .filter(
            "l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-04-01'")
        .project(
            {"l_suppkey as gk", "l_extendedprice * (1.0 - l_discount) as rev"})
        .singleAggregation({"gk"}, {"sum(rev) as total_revenue"});
  };

  auto matcher =
      matchScan("supplier")
          .hashJoinInner(
              revenueAgg()
                  .hashJoinInner(revenueAgg({"l_suppkey",
                                             "l_extendedprice",
                                             "l_discount",
                                             std::nullopt})
                                     .project()
                                     .singleAggregation(
                                         {}, {"max(total_revenue) as maxRev"})
                                     .build())
                  .build())
          .orderBy({"s_suppkey"})
          .build();
  AXIOM_ASSERT_PLAN(planTpch(15), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 15, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 15, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q16) {
  const auto partFilter =
      "\"and\"(p_brand <> 'Brand#45', p_type not like 'MEDIUM POLISHED%', "
      "         p_size in (49, 14, 23, 45, 19, 3, 36, 9))";

  auto matcher =
      matchScan("partsupp")
          .hashJoinInner(matchScan("part").filter(partFilter).build())
          .hashJoinAnti(
              matchScan("supplier")
                  .filter("s_comment like '%Customer%Complaints%'")
                  .build(),
              {.nullAware = true})
          .singleAggregation(
              {"p_brand", "p_type", "p_size"},
              {"count(distinct ps_suppkey) as supplier_cnt"})
          .orderBy()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(16), matcher);

  auto distributed = [&](bool isMultiThreaded) {
    auto builder =
        matchScan("partsupp")
            .multiThreaded(isMultiThreaded)
            .hashJoinInner(
                matchScan("part").filter(partFilter).broadcast().build())
            .hashJoinAnti(
                matchScan("supplier")
                    .filter("s_comment like '%Customer%Complaints%'")
                    .broadcast()
                    .build(),
                {.nullAware = true})
            .shuffle({"p_brand", "p_type", "p_size"});
    if (isMultiThreaded) {
      builder.localPartition({"p_brand", "p_type", "p_size"});
    }
    return builder
        .singleAggregation(
            {"p_brand", "p_type", "p_size"},
            {"count(distinct ps_suppkey) as supplier_cnt"})
        .distributedOrderBy(
            {"supplier_cnt DESC", "p_brand", "p_type", "p_size"})
        .build();
  };

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planTpchMultiNode({.query = 16, .numDrivers = 1}), distributed(false));
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planTpchMultiNode({.query = 16, .numDrivers = 4}), distributed(true));
}

TEST_F(TpchPlanTest, q17) {
  // The correlated avg(l_quantity)-per-part subquery is decorrelated by
  // aggregating lineitem once per l_partkey and joining the result back to the
  // outer, so each part's average is computed once rather than once per outer
  // row (Gap 1 in TpchV1V2PlanComparison.md), with no AssignUniqueId.
  auto matcher =
      matchScan("lineitem")
          .hashJoinInner(
              matchScan("lineitem")
                  .aggregation()
                  .hashJoinRight(
                      matchScan("part")
                          .filter(
                              "p_brand = 'Brand#23' and p_container = 'MED BOX'")
                          .build())
                  .build())
          .filter()
          .project()
          .project()
          .aggregation()
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(17), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 17, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 17, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q18) {
  // The uncorrelated IN-subquery (orderkeys with sum(l_quantity) > 300) is a
  // plain LEFT SEMI applied to orders ⋈ customer, reducing it to the big
  // orders. The main lineitem then probes that reduced set and is dynamic-
  // filtered to only the big orders' rows (6M -> 399 on SF1, EXPLAIN ANALYZE),
  // so the only full lineitem scan is the subquery's required aggregation.
  auto matcher =
      matchScan("lineitem")
          .hashJoinInner(
              matchScan("orders")
                  .hashJoinInner(matchScan("customer").build())
                  .hashJoinLeftSemiFilter(
                      matchScan("lineitem")
                          .aliases({"l_orderkey", "l_quantity"})
                          .singleAggregation(
                              {"l_orderkey"}, {"sum(l_quantity) as s"})
                          .filter("s > 300.0")
                          .project()
                          .build())
                  .build())
          .aggregation()
          .topN()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(18), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 18, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 18, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q19) {
  // The three-branch disjunction is decomposed rather than left as one
  // post-join Filter: the common per-side conjuncts are pushed to each scan
  // (shipmode / shipinstruct / quantity-range on lineitem; brand / container /
  // size on part), and only the cross-side terms ride as the join filter.
  // hashJoin -> project here asserts there is no intervening post-join Filter.
  auto matcher =
      matchScan("lineitem")
          .filter(
              "\"and\"(l_shipmode in ('AIR', 'AIR REG'), "
              "l_shipinstruct = 'DELIVER IN PERSON', "
              "(l_quantity >= 1.0 and l_quantity <= 11.0) or "
              "(l_quantity >= 10.0 and l_quantity <= 20.0) or "
              "(l_quantity >= 20.0 and l_quantity <= 30.0))")
          .hashJoinInner(
              matchScan("part")
                  .filter(
                      "(p_brand = 'Brand#12' and p_container in ('SM CASE', "
                      "'SM BOX', 'SM PACK', 'SM PKG') and p_size between 1 and 5) or "
                      "(p_brand = 'Brand#23' and p_container in ('MED BAG', "
                      "'MED BOX', 'MED PKG', 'MED PACK') and p_size between 1 and 10) or "
                      "(p_brand = 'Brand#34' and p_container in ('LG CASE', "
                      "'LG BOX', 'LG PACK', 'LG PKG') and p_size between 1 and 15)")
                  .build())
          .project()
          .aggregation()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(19), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 19, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 19, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q20) {
  // The availqty subquery decorrelates to a lineitem aggregation grouped by
  // (l_partkey, l_suppkey) RIGHT-joined back to the forest partsupps; the outer
  // s_suppkey IN is a top RIGHT SEMI against supplier ⋈ nation[CANADA]. A
  // magic-set reduction (pushing the outer supplier keys into the partsupp
  // subquery) would be cheaper, but v2 does not insert reducing semijoins; see
  // Gap 1 in TpchV1V2PlanComparison.md.
  auto matcher =
      matchScan("lineitem")
          .filter(
              "l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01'")
          .aggregation()
          .hashJoinRight(matchScan("partsupp")
                             .hashJoinLeftSemiProject(
                                 matchScan("part")
                                     .filter("like(p_name, 'forest%')")
                                     .build())
                             .filter("mark1")
                             .build())
          // TODO Optimize if(true, sum * 0.5, null) to sum * 0.5.
          .filter("cast(ps_availqty as double) > if(true, sum * 0.5, null)")
          .project()
          .project()
          .hashJoinRightSemiFilter(
              matchScan("supplier")
                  .hashJoinInner(
                      matchScan("nation").filter("n_name = 'CANADA'").build())
                  .build())
          .orderBy()
          .project()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(20), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 20, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 20, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q21) {
  // Base: nation[SAUDI] ⋈ supplier ⋈ l1[receiptdate>commitdate] ⋈ orders[F].
  // NOT EXISTS (another late supplier on the order) is a RIGHT SEMI (PROJECT)
  // mark filtered by not(mark); EXISTS (another supplier on the order) is a
  // RIGHT SEMI (FILTER).
  auto matcher =
      matchScan("lineitem")
          .hashJoinRightSemiFilter(
              matchScan("lineitem")
                  .aliases(
                      {std::nullopt,
                       std::nullopt,
                       "l_commitdate",
                       "l_receiptdate"})
                  .filter("l_commitdate < l_receiptdate")
                  .hashJoinRightSemiProject(
                      matchScan("orders")
                          .filter("o_orderstatus = 'F'")
                          .hashJoinInner(
                              matchScan("lineitem")
                                  .filter("l_commitdate < l_receiptdate")
                                  .hashJoinInner(
                                      matchScan("supplier")
                                          .hashJoinInner(
                                              matchScan("nation")
                                                  .filter(
                                                      "n_name = 'SAUDI ARABIA'")
                                                  .build())
                                          .build())
                                  .build())
                          .build())
                  // Bind the semijoin's trailing mark column by position rather
                  // than its internal name.
                  .aliases({std::nullopt, std::nullopt, std::nullopt, "mark"})
                  .filter("not(mark)")
                  .project()
                  .build())
          .aggregation()
          .topN()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(21), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 21, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 21, .numDrivers = 4}));
}

TEST_F(TpchPlanTest, q22) {
  // Filtered customer ⋈ scalar avg(c_acctbal) (NestedLoopJoin with c_acctbal >
  // avg), then NOT EXISTS orders as a RIGHT SEMI (PROJECT) mark + not(mark).
  // The avg subquery is a global aggregate (exactly one row), so no
  // EnforceSingleRow guard is needed.
  const std::string countryFilter =
      "substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')";
  auto matcher =
      matchScan("orders")
          .hashJoinRightSemiProject(
              matchScan("customer")
                  .filter(countryFilter)
                  .nestedLoopJoin(
                      matchScan("customer")
                          .aliases({"c_acctbal", std::nullopt})
                          .filter("c_acctbal > 0.0 and " + countryFilter)
                          .aggregation()
                          .build())
                  .build())
          .aliases({std::nullopt, std::nullopt, std::nullopt, "mark"})
          .filter("not(mark)")
          .project()
          .project()
          .aggregation()
          .orderBy()
          .build();
  AXIOM_ASSERT_PLAN(planTpch(22), matcher);

  ASSERT_NO_THROW(planTpchMultiNode({.query = 22, .numDrivers = 1}));
  ASSERT_NO_THROW(planTpchMultiNode({.query = 22, .numDrivers = 4}));
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
