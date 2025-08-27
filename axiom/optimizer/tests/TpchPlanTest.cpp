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
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"

DEFINE_int32(num_repeats, 1, "Number of repeats for optimization timing");

DECLARE_int32(optimizer_trace);
DECLARE_int32(num_workers);
DECLARE_string(history_save_path);

namespace lp = facebook::velox::logical_plan;

namespace facebook::velox::optimizer {
namespace {

class TpchPlanTest : public virtual test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    if (!FLAGS_history_save_path.empty()) {
      suiteHistory().saveToFile(FLAGS_history_save_path);
    }
    test::HiveQueriesTestBase::TearDownTestCase();
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();

    referenceBuilder_ = std::make_unique<exec::test::TpchQueryBuilder>(
        dwio::common::FileFormat::PARQUET);
    referenceBuilder_->initialize(LocalRunnerTestBase::testDataPath_);
  }

  void TearDown() override {
    HiveQueriesTestBase::TearDown();
  }

  void checkTpch(int32_t query, const lp::LogicalPlanNodePtr& logicalPlan) {
    auto referencePlan = referenceBuilder_->getQueryPlan(query).plan;
    checkResults(logicalPlan, referencePlan);
  }

  static std::string readSqlFromFile(const std::string& filePath) {
    auto path = velox::test::getDataFilePath("axiom/optimizer/tests", filePath);
    std::ifstream inputFile(path, std::ifstream::binary);

    VELOX_CHECK(inputFile, "Failed to open SQL file: {}", path);

    // Find out file size.
    auto begin = inputFile.tellg();
    inputFile.seekg(0, std::ios::end);
    auto end = inputFile.tellg();

    const auto fileSize = end - begin;
    VELOX_CHECK_GT(fileSize, 0, "SQL file is empty: {}", path);

    // Read the file.
    std::string sql;
    sql.resize(fileSize);

    inputFile.seekg(begin);
    inputFile.read(sql.data(), fileSize);
    inputFile.close();

    return sql;
  }

  void checkTpchSql(int32_t query) {
    auto sql = readSqlFromFile(fmt::format("tpch.queries/q{}.sql", query));
    auto referencePlan = referenceBuilder_->getQueryPlan(query).plan;
    checkResults(sql, referencePlan);
  }

  std::unique_ptr<exec::test::TpchQueryBuilder> referenceBuilder_;
};

TEST_F(TpchPlanTest, q01) {
  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(exec::test::kHiveConnectorId, "lineitem")
          .filter("l_shipdate < '1998-09-03'::date")
          .aggregate(
              {"l_returnflag", "l_linestatus"},
              {
                  "sum(l_quantity) as sum_qty",
                  "sum(l_extendedprice) as sum_base_price",
                  "sum(l_extendedprice * (1.0 - l_discount)) as sum_disc_price",
                  "sum(l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)) as sum_charge",
                  "avg(l_quantity) as avg_qty",
                  "avg(l_extendedprice) as avg_price",
                  "avg(l_discount) as avg_disc",
                  "count(*) as count_order",
              })
          .orderBy({"l_returnflag", "l_linestatus"})
          .build();

  checkTpch(1, logicalPlan);

  checkTpchSql(1);
}

TEST_F(TpchPlanTest, DISABLED_q02) {
  // TODO Implement. Requires subqueries support.
}

TEST_F(TpchPlanTest, q03) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .from({"customer", "orders", "lineitem"})
          .filter(
              "c_mktsegment = 'BUILDING' "
              "and c_custkey = o_custkey "
              "and l_orderkey = o_orderkey "
              "and o_orderdate < '1995-03-15'::date "
              "and l_shipdate > '1995-03-15'::date")
          .aggregate(
              {"l_orderkey", "o_orderdate", "o_shippriority"},
              {"sum(l_extendedprice * (1.0 - l_discount)) as revenue"})
          .project({"l_orderkey", "revenue", "o_orderdate", "o_shippriority"})
          .orderBy({"revenue desc", "o_orderdate"})
          .limit(10)
          .build();

  checkTpch(3, logicalPlan);

  checkTpchSql(3);
}

TEST_F(TpchPlanTest, DISABLED_q04) {
  // Incorrect with distributed plan at larger scales.
  // TODO Implement.
}

TEST_F(TpchPlanTest, q05) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .from({
              "customer",
              "orders",
              "lineitem",
              "supplier",
              "nation",
              "region",
          })
          .filter(
              "c_custkey = o_custkey "
              "and l_orderkey = o_orderkey "
              "and l_suppkey = s_suppkey "
              "and c_nationkey = s_nationkey "
              "and s_nationkey = n_nationkey "
              "and n_regionkey = r_regionkey "
              "and r_name = 'ASIA' "
              "and o_orderdate >= '1994-01-01'::date "
              "and o_orderdate < '1994-12-31'::date")
          .aggregate(
              {"n_name"},
              {"sum(l_extendedprice * (1.0 - l_discount)) as revenue"})
          .orderBy({"revenue desc"})
          .build();

  checkTpch(5, logicalPlan);

  checkTpchSql(5);
}

TEST_F(TpchPlanTest, q06) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("lineitem")
          .filter(
              "l_shipdate >= '1994-01-01'::date and l_shipdate <= '1994-12-31'::date "
              "and l_discount between 0.05 and 0.07 and l_quantity < 24.0")
          .aggregate({}, {"sum(l_extendedprice * l_discount) as revenue"})
          .build();

  checkTpch(6, logicalPlan);

  checkTpchSql(6);
}

TEST_F(TpchPlanTest, q07) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .from({"supplier", "lineitem", "orders", "customer", "nation"})
          // TODO Allow to use table aliases in 'from'.
          .crossJoin(
              lp::PlanBuilder(context)
                  .from({"nation"})
                  .project(
                      {"n_nationkey as n2_nationkey", "n_name as n2_name"}))
          .filter(
              "s_suppkey = l_suppkey "
              "and o_orderkey = l_orderkey"
              " and c_custkey = o_custkey "
              "and s_nationkey = n_nationkey "
              "and c_nationkey = n2_nationkey "
              "and ((n_name = 'FRANCE' and n2_name = 'GERMANY') or (n_name = 'GERMANY' and n2_name = 'FRANCE')) "
              "and l_shipdate between '1995-01-01'::date and '1996-12-31'::date")
          .project({
              "n_name as supp_nation",
              "n2_name as cust_nation",
              "year(l_shipdate) as l_year",
              "l_extendedprice * (1.0 - l_discount) as volume",
          })
          .aggregate(
              {"supp_nation", "cust_nation", "l_year"},
              {"sum(volume) as revenue"})
          .orderBy({"supp_nation", "cust_nation", "l_year"})
          .build();

  checkTpch(7, logicalPlan);

  checkTpchSql(7);
}

TEST_F(TpchPlanTest, q08) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .from(
              {"part",
               "supplier",
               "lineitem",
               "orders",
               "customer",
               "nation",
               "region"})
          // TODO Allow to use table aliases in 'from'.
          .crossJoin(
              lp::PlanBuilder(context)
                  .from({"nation"})
                  .project(
                      {"n_nationkey as n2_nationkey", "n_name as n2_name"}))
          .filter(
              "p_partkey = l_partkey "
              "and s_suppkey = l_suppkey "
              "and l_orderkey = o_orderkey "
              "and o_custkey = c_custkey "
              "and c_nationkey = n_nationkey "
              "and n_regionkey = r_regionkey "
              "and r_name = 'AMERICA' "
              "and s_nationkey = n2_nationkey "
              "and o_orderdate between '1995-01-01'::date and '1996-12-31'::date "
              "and p_type = 'ECONOMY ANODIZED STEEL'")
          .project({
              "year(o_orderdate) as o_year",
              "l_extendedprice * (1.0 - l_discount) as volume",
              "n2_name as nation",
          })
          .aggregate(
              {"o_year"},
              {"sum(if(nation = 'BRAZIL', volume, 0.0)) as brazil",
               "sum(volume) as total"})
          .project({"o_year", "brazil / total as mkt_share"})
          .orderBy({"o_year"})
          .build();

  checkTpch(8, logicalPlan);

  checkTpchSql(8);
}

TEST_F(TpchPlanTest, q09) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .from(
              {"part", "supplier", "lineitem", "partsupp", "orders", "nation"})
          .filter(
              "s_suppkey = l_suppkey "
              "and ps_suppkey = l_suppkey "
              "and ps_partkey = l_partkey "
              "and p_partkey = l_partkey "
              "and o_orderkey = l_orderkey "
              "and s_nationkey = n_nationkey "
              "and p_name like '%green%'")
          .project({
              "n_name as nation",
              "year(o_orderdate) as o_year",
              "l_extendedprice * (1.0 - l_discount) - ps_supplycost * l_quantity as amount",
          })
          .aggregate({"nation", "o_year"}, {"sum(amount) as sum_profit"})
          .orderBy({"nation", "o_year desc"})
          .build();

  // Plan does not minimize build size. To adjust build cost and check that
  // import of existences to build side does not affect join cardinality.
  checkTpch(9, logicalPlan);

  checkTpchSql(9);
}

TEST_F(TpchPlanTest, q10) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .from({"customer", "orders", "lineitem", "nation"})
          .filter(
              "c_custkey = o_custkey "
              "and l_orderkey = o_orderkey "
              "and o_orderdate between '1993-10-01'::date and '1993-12-31'::date "
              "and l_returnflag = 'R' "
              "and c_nationkey = n_nationkey")
          .aggregate(
              {"c_custkey",
               "c_name",
               "c_acctbal",
               "c_phone",
               "n_name",
               "c_address",
               "c_comment"},
              {"sum(l_extendedprice * (1.0 - l_discount)) as revenue"})
          .orderBy({"revenue desc"})
          .project(
              {"c_custkey",
               "c_name",
               "revenue",
               "c_acctbal",
               "n_name",
               "c_address",
               "c_phone",
               "c_comment"})
          .build();

  checkTpch(10, logicalPlan);

  checkTpchSql(10);
}

TEST_F(TpchPlanTest, q11) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .from({"partsupp", "supplier", "nation"})
          .filter(
              "ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'")
          .aggregate(
              {"ps_partkey"},
              {"sum(ps_supplycost * ps_availqty::double) as value"})
          .filter(
              lp::Col("value") >
              lp::Subquery(
                  lp::PlanBuilder(context)
                      .from({"partsupp", "supplier", "nation"})
                      .filter(
                          "ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'")
                      .aggregate(
                          {},
                          {"sum(ps_supplycost * ps_availqty::double) as total"})
                      .project({"total * 0.0001"})
                      .build()))
          .orderBy({"value desc"})
          .build();

  // TODO Make above plan with a non-correlated subquery work.
  logicalPlan =
      lp::PlanBuilder(context)
          .from({"partsupp", "supplier", "nation"})
          .filter(
              "ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'")
          .aggregate(
              {"ps_partkey"},
              {"sum(ps_supplycost * ps_availqty::double) as value"})
          .crossJoin(
              lp::PlanBuilder(context)
                  .from({"partsupp", "supplier", "nation"})
                  .filter(
                      "ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'")
                  .aggregate(
                      {}, {"sum(ps_supplycost * ps_availqty::double) as total"})
                  .project({"total * 0.0001 as threshold"}))
          .filter("value > threshold")
          .orderBy({"value desc"})
          .project({"ps_partkey", "value"})
          .build();

  checkTpch(11, logicalPlan);

  // TODO Add subquery support to the optimizer.
  // checkTpchSql(11);
}

TEST_F(TpchPlanTest, q12) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .from({"orders", "lineitem"})
          .filter(
              "l_orderkey = o_orderkey "
              "and l_shipmode in ('MAIL', 'SHIP') "
              "and l_commitdate < l_receiptdate "
              "and l_shipdate < l_commitdate "
              "and l_receiptdate >= '1994-01-01'::date "
              "and l_receiptdate <= date '1994-12-31'::date")
          .aggregate(
              {"l_shipmode"},
              {
                  "sum(if(o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH', 1, 0)) as high_line_count",
                  "sum(if(o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH', 1, 0)) as low_line_count",
              })
          .orderBy({"l_shipmode"})
          .build();

  // Fix string in filter
  checkTpch(12, logicalPlan);

  checkTpchSql(12);
}

TEST_F(TpchPlanTest, q13) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("customer")
          .join(
              lp::PlanBuilder(context).tableScan("orders"),
              "c_custkey = o_custkey and o_comment not like '%special%requests%'",
              lp::JoinType::kLeft)
          .aggregate({"c_custkey"}, {"count(o_orderkey) as c_count"})
          .aggregate({"c_count"}, {"count(*) as custdist"})
          .orderBy({"custdist desc", "c_count desc"})
          .build();

  checkTpch(13, logicalPlan);

  checkTpchSql(13);
}

TEST_F(TpchPlanTest, q14) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .from({"lineitem", "part"})
          .filter(
              "l_partkey = p_partkey "
              "and l_shipdate between '1995-09-01'::date and '1995-09-30'::date")
          .aggregate(
              std::vector<std::string>{},
              {
                  "sum(if(p_type like 'PROMO%', l_extendedprice * (1.0 - l_discount), 0.0)) as promo",
                  "sum(l_extendedprice * (1.0 - l_discount)) as total",
              })
          .project({"100.00 * promo / total as promo_revenue"})
          .build();

  checkTpch(14, logicalPlan);

  checkTpchSql(14);
}

TEST_F(TpchPlanTest, DISABLED_q15) {
  // TODO Implement.
}

TEST_F(TpchPlanTest, DISABLED_q16) {
  // TODO Implement.
}

TEST_F(TpchPlanTest, DISABLED_q17) {
  // TODO Implement.
}

TEST_F(TpchPlanTest, DISABLED_q18) {
  // TODO Implement.
}

TEST_F(TpchPlanTest, q19) {
  lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
  auto logicalPlan =
      lp::PlanBuilder(context)
          .from({"lineitem", "part"})
          .filter(
              "(p_partkey = l_partkey "
              "  and p_brand = 'Brand#12' "
              "  and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
              "  and l_quantity >= 1.0 and l_quantity <= 1.0 + 10.0 "
              "  and p_size between 1::int and 5::int "
              "  and l_shipmode in ('AIR', 'AIR REG') "
              "  and l_shipinstruct = 'DELIVER IN PERSON') "
              "or (p_partkey = l_partkey "
              "       and p_brand = 'Brand#23' "
              "       and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')"
              "       and l_quantity >= 10.0 and l_quantity <= 10.0 + 10.0"
              "       and p_size between 1::int and 10::int"
              "       and l_shipmode in ('AIR', 'AIR REG')"
              "       and l_shipinstruct = 'DELIVER IN PERSON') "
              "or (p_partkey = l_partkey "
              "       and p_brand = 'Brand#34' "
              "       and p_container in ('LG BAG', 'LG BOX', 'LG PKG', 'LG PACK')"
              "       and l_quantity >= 20.0 and l_quantity <= 20.0 + 10.0"
              "       and p_size between 1::int and 15::int"
              "       and l_shipmode in ('AIR', 'AIR REG')"
              "       and l_shipinstruct = 'DELIVER IN PERSON')")
          .aggregate(
              {}, {"sum(l_extendedprice * (1.0 - l_discount)) as revenue"})
          .build();

  checkTpch(19, logicalPlan);

  checkTpchSql(19);
}

TEST_F(TpchPlanTest, DISABLED_q20) {
  // TODO Implement.
}

TEST_F(TpchPlanTest, DISABLED_q21) {
  // TODO Implement.
}

TEST_F(TpchPlanTest, DISABLED_q22) {
  // TODO Implement.
}

} // namespace
} // namespace facebook::velox::optimizer

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
