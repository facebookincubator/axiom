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
#include <fstream>
#include <functional>
#include <sstream>
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcherGenerator.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"

DEFINE_int32(num_repeats, 1, "Number of repeats for optimization timing");
DEFINE_bool(record_plans, false, "Record plan checkers to files");

DECLARE_uint32(optimizer_trace);
DECLARE_string(history_save_path);

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

struct CheckerKey {
  int32_t queryNo;
  int32_t numWorkers;
  int32_t numDrivers;

  bool operator==(const CheckerKey& other) const {
    return queryNo == other.queryNo && numWorkers == other.numWorkers &&
        numDrivers == other.numDrivers;
  }
};

struct CheckerKeyHash {
  std::size_t operator()(const CheckerKey& key) const {
    // Combine hash values using a simple hash combination technique
    std::size_t h1 = std::hash<int32_t>{}(key.queryNo);
    std::size_t h2 = std::hash<int32_t>{}(key.numWorkers);
    std::size_t h3 = std::hash<int32_t>{}(key.numDrivers);
    return h1 ^ (h2 << 1) ^ (h3 << 2);
  }
};

class TpchPlanTest : public virtual test::HiveQueriesTestBase {
 protected:
  using PlanChecker = std::function<void(const PlanAndStats&)>;

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
        LocalRunnerTestBase::localFileFormat_);
    referenceBuilder_->initialize(LocalRunnerTestBase::localDataPath_);
  }

  void TearDown() override {
    HiveQueriesTestBase::TearDown();
  }

  void setChecker(
      int32_t queryNo,
      int32_t numWorkers,
      int32_t numDrivers,
      PlanChecker checker) {
    checkers_[CheckerKey{queryNo, numWorkers, numDrivers}] = std::move(checker);
  }

  PlanChecker*
  getChecker(int32_t queryNo, int32_t numWorkers, int32_t numDrivers) {
    auto it = checkers_.find(CheckerKey{queryNo, numWorkers, numDrivers});
    if (it != checkers_.end()) {
      return &it->second;
    }
    return nullptr;
  }

  void recordPlanCheckerStart(int32_t queryNo) {
    if (!FLAGS_record_plans) {
      return;
    }

    auto filename = fmt::format("check_{}.inc", queryNo);
    std::ofstream file(filename);

    if (!file.is_open()) {
      LOG(ERROR) << "Failed to open file: " << filename;
      return;
    }

    file << "void defineCheckers" << queryNo << "() {\n";
    file.close();
  }

  void recordPlanCheckerEnd(int32_t queryNo) {
    if (!FLAGS_record_plans) {
      return;
    }

    auto filename = fmt::format("check_{}.inc", queryNo);
    std::ofstream file(filename, std::ios::app);

    if (!file.is_open()) {
      LOG(ERROR) << "Failed to open file: " << filename;
      return;
    }

    file << "}\n";
    file.close();
  }

  void recordPlanChecker(
      int32_t queryNo,
      const lp::LogicalPlanNodePtr& logicalPlan,
      const PlanAndStats& planAndStats,
      int32_t numWorkers,
      int32_t numDrivers) {
    if (!FLAGS_record_plans) {
      return;
    }

    auto filename = fmt::format("check_{}.inc", queryNo);
    std::ofstream file(filename, std::ios::app);

    if (!file.is_open()) {
      LOG(ERROR) << "Failed to open file: " << filename;
      return;
    }

    // Get the short RelationOp representation
    std::string shortRel;
    QueryTestBase::explain(
        logicalPlan,
        &shortRel,
        nullptr,
        nullptr,
        runner::MultiFragmentPlan::Options{
            .numWorkers = numWorkers, .numDrivers = numDrivers});

    file << "// Configuration: numWorkers=" << numWorkers
         << ", numDrivers=" << numDrivers << "\n";
    file << "setChecker(" << queryNo << ", " << numWorkers << ", " << numDrivers
         << ", [](const PlanAndStats& planAndStats) {\n";

    // Add the plan as a comment for readability
    file << "  // Plan:\n";
    std::istringstream planStream(shortRel);
    std::string line;
    while (std::getline(planStream, line)) {
      file << "  // " << line << "\n";
    }
    file << "\n";

    const auto& fragments = planAndStats.plan->fragments();
    for (size_t i = 0; i < fragments.size(); ++i) {
      const auto& fragment = fragments[i];
      const auto& topNode = fragment.fragment.planNode;

      file << "  // Fragment " << i << "\n";
      file << "  {\n";
      file << "    " << velox::core::generatePlanMatcherCode(topNode, "matcher")
           << ";\n";
      file << "    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()["
           << i << "].fragment.planNode));\n";
      file << "  }\n";
    }

    file << "});\n\n";
    file.close();
  }

  void checkTpch(int32_t query, const lp::LogicalPlanNodePtr& logicalPlan) {
    auto referencePlan = referenceBuilder_->getQueryPlan(query).plan;

    // Define configurations to test
    std::vector<std::pair<int32_t, int32_t>> configs = {
        {1, 1}, {1, 4}, {4, 1}, {4, 4}};

    // Loop over configs and test the plan against reference builder for each
    for (const auto& [numWorkers, numDrivers] : configs) {
      // Generate plan with this config and test against reference builder
      planVelox(
          logicalPlan,
          runner::MultiFragmentPlan::Options{
              .numWorkers = numWorkers, .numDrivers = numDrivers});
      checkSame(logicalPlan, referencePlan);
    }
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

  std::string readTpchSql(int32_t query) {
    return readSqlFromFile(fmt::format("tpch.queries/q{}.sql", query));
  }

  lp::LogicalPlanNodePtr parseTpchSql(int32_t query) {
    auto sql = readTpchSql(query);

    ::axiom::sql::presto::PrestoParser prestoParser(
        exec::test::kHiveConnectorId, pool());
    auto statement = prestoParser.parse(sql);

    VELOX_CHECK(statement->isSelect());

    auto logicalPlan =
        statement->as<::axiom::sql::presto::SelectStatement>()->plan();
    VELOX_CHECK_NOT_NULL(logicalPlan);

    return logicalPlan;
  }

  void checkTpchSql(int32_t query) {
    auto sql = readTpchSql(query);
    auto referencePlan = referenceBuilder_->getQueryPlan(query).plan;

    // Define configurations to test
    std::vector<std::pair<int32_t, int32_t>> configs = {
        {1, 1}, {1, 4}, {4, 1}, {4, 4}};

    // Loop over configs
    for (size_t i = 0; i < configs.size(); ++i) {
      const auto& [numWorkers, numDrivers] = configs[i];
      bool isFirstConfig = (i == 0);
      bool isLastConfig = (i == configs.size() - 1);

      // First run the SQL query and compare with reference builder
      auto logicalPlan = parseTpchSql(query);
      auto planAndStats = planVelox(
          logicalPlan,
          runner::MultiFragmentPlan::Options{
              .numWorkers = numWorkers, .numDrivers = numDrivers});

      // Check the optimized query against the reference builder
      checkSame(logicalPlan, referencePlan);

      // If FLAGS_record_plans is set, initialize the recording at first config
      if (FLAGS_record_plans && isFirstConfig) {
        recordPlanCheckerStart(query);
      }

      // If FLAGS_record_plans is set, record the checker
      if (FLAGS_record_plans) {
        recordPlanChecker(
            query, logicalPlan, planAndStats, numWorkers, numDrivers);
      }

      // If FLAGS_record_plans is set, after recording with the last config,
      // finalize the recording
      if (FLAGS_record_plans && isLastConfig) {
        recordPlanCheckerEnd(query);
      }

      // If FLAGS_record_plans is not set and there is a checker for the query
      // and config, run the checker
      if (!FLAGS_record_plans) {
        auto* checker = getChecker(query, numWorkers, numDrivers);
        if (checker) {
          (*checker)(planAndStats);
        }
      }

      // Continue loop with the next config
    }
  }

  std::unique_ptr<exec::test::TpchQueryBuilder> referenceBuilder_;
  std::unordered_map<CheckerKey, PlanChecker, CheckerKeyHash> checkers_;

#include "check_1.inc"
#include "check_13.inc"
#include "check_3.inc"
#include "check_4.inc"
};

TEST_F(TpchPlanTest, stats) {
  auto verifyStats = [&](const auto& tableName, auto cardinality) {
    SCOPED_TRACE(tableName);

    auto logicalPlan = lp::PlanBuilder()
                           .tableScan(exec::test::kHiveConnectorId, tableName)
                           .build();

    auto planAndStats = planVelox(logicalPlan);
    auto stats = planAndStats.prediction;

    // We expect a prediction for the table scan and
    ASSERT_EQ(stats.size(), 2);

    // Node ids start at 0, the scan is always first.
    ASSERT_EQ(stats.at("0").cardinality, cardinality);
  };

  verifyStats("region", 5);
  verifyStats("nation", 25);
  verifyStats("orders", 150'000);
  verifyStats("lineitem", 600'572);
}

TEST_F(TpchPlanTest, q01) {
  defineCheckers1();
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

TEST_F(TpchPlanTest, q02) {
  checkTpchSql(2);
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

TEST_F(TpchPlanTest, q04) {
  defineCheckers4();
  checkTpchSql(4);
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
          .limit(20)
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

  checkTpch(11, logicalPlan);

  checkTpchSql(11);
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

  checkTpch(12, logicalPlan);

  checkTpchSql(12);
}

TEST_F(TpchPlanTest, q13) {
  defineCheckers13();
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

TEST_F(TpchPlanTest, q15) {
  checkTpchSql(15);
}

TEST_F(TpchPlanTest, q16) {
  checkTpchSql(16);
}

TEST_F(TpchPlanTest, q17) {
  checkTpchSql(17);
}

TEST_F(TpchPlanTest, q18) {
  checkTpchSql(18);
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

TEST_F(TpchPlanTest, q20) {
  // TODO Fix the plan when 'enableReducingExistences' is true.
  const bool originalEnableReducingExistences =
      optimizerOptions_.enableReducingExistences;
  optimizerOptions_.enableReducingExistences = false;
  SCOPE_EXIT {
    optimizerOptions_.enableReducingExistences =
        originalEnableReducingExistences;
  };
  checkTpchSql(20);
}

TEST_F(TpchPlanTest, q21) {
  checkTpchSql(21);
}

TEST_F(TpchPlanTest, q22) {
  checkTpchSql(22);
}

// Use to re-generate the plans stored in tpch.plans directory.
TEST_F(TpchPlanTest, DISABLED_makePlans) {
  const auto path =
      velox::test::getDataFilePath("axiom/optimizer/tests", "tpch.plans");

  const runner::MultiFragmentPlan::Options options{
      .numWorkers = 1, .numDrivers = 1};

  for (auto q = 1; q <= 22; ++q) {
    LOG(ERROR) << "q" << q;
    const bool originalEnableReducingExistences =
        optimizerOptions_.enableReducingExistences;
    optimizerOptions_.enableReducingExistences = (q != 20);
    SCOPE_EXIT {
      optimizerOptions_.enableReducingExistences =
          originalEnableReducingExistences;
    };

    auto logicalPlan = parseTpchSql(q);
    planVelox(logicalPlan, options, fmt::format("{}/q{}", path, q));
  }
}

TEST_F(TpchPlanTest, supplierAggregationJoin) {
  using namespace facebook::velox;

  auto lineitemType = ROW({{"l_suppkey", BIGINT()}, {"l_quantity", DOUBLE()}});

  auto supplierType = ROW(
      {{"s_suppkey", BIGINT()},
       {"s_name", VARCHAR()},
       {"s_acctbal", DOUBLE()},
       {"s_nationkey", BIGINT()}});

  auto nationType = ROW({{"n_nationkey", BIGINT()}, {"n_name", VARCHAR()}});

  // Create shared plan node ID generator
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  // Test 1: Simple supplier join
  {
    auto sql =
        "select s_name, s_acctbal, volume "
        "from (select l_suppkey, sum(l_quantity) as volume from lineitem group by l_suppkey), supplier "
        "where s_suppkey = l_suppkey and s_acctbal < 100";
    SCOPED_TRACE(sql);

    // Build reference plan: lineitem scan -> aggregation -> hash join with
    // filtered supplier -> project
    auto referencePlan =
        exec::test::PlanBuilder(planNodeIdGenerator, pool())
            .localPartition(
                {},
                {exec::test::PlanBuilder(planNodeIdGenerator, pool())
                     .tableScan("lineitem", lineitemType)
                     .singleAggregation({"l_suppkey"}, {"sum(l_quantity)"})
                     .planNode()})
            .hashJoin(
                {"l_suppkey"},
                {"s_suppkey"},
                exec::test::PlanBuilder(planNodeIdGenerator, pool())
                    .tableScan("supplier", supplierType)
                    .filter("s_acctbal < 100.0")
                    .planNode(),
                "",
                {"s_name", "s_acctbal", "a0"})
            .project({"s_name", "s_acctbal", "a0 as volume"})
            .planNode();

    auto sqlPlan = checkResults(sql, referencePlan);

    // Verify the optimized plan contains aggregation above filter semijoin
    ASSERT_EQ(1, sqlPlan.plan->fragments().size());
    auto sqlPlanNode = sqlPlan.plan->fragments().at(0).fragment.planNode;

    checkPlanText(sqlPlanNode, {"Aggregation", "LEFT SEMI \\(FILTER\\)"});
  }

  // Test 2: Supplier join with nation filter
  {
    auto sql =
        "select s_name, s_acctbal, volume "
        "from (select l_suppkey, sum(l_quantity) as volume from lineitem group by l_suppkey), supplier, nation "
        "where s_suppkey = l_suppkey and s_acctbal - 100 < 0 and s_nationkey = n_nationkey and n_name = 'FRANCE'";
    SCOPED_TRACE(sql);

    // Build reference plan: lineitem scan -> aggregation -> hash join with
    // supplier joined to nation and filtered -> project
    auto referencePlan =
        exec::test::PlanBuilder(planNodeIdGenerator, pool())
            .localPartition(
                {},
                {exec::test::PlanBuilder(planNodeIdGenerator, pool())
                     .tableScan("lineitem", lineitemType)
                     .singleAggregation({"l_suppkey"}, {"sum(l_quantity)"})
                     .planNode()})
            .hashJoin(
                {"l_suppkey"},
                {"s_suppkey"},
                exec::test::PlanBuilder(planNodeIdGenerator, pool())
                    .tableScan("supplier", supplierType)
                    .hashJoin(
                        {"s_nationkey"},
                        {"n_nationkey"},
                        exec::test::PlanBuilder(planNodeIdGenerator, pool())
                            .tableScan("nation", nationType)
                            .filter("n_name = 'FRANCE'")
                            .planNode(),
                        "",
                        {"s_suppkey", "s_name", "s_acctbal"})
                    .filter("s_acctbal - 100.0 < 0.0")
                    .planNode(),
                "",
                {"s_name", "s_acctbal", "a0"})
            .project({"s_name", "s_acctbal", "a0 as volume"})
            .planNode();

    auto sqlPlan = checkResults(sql, referencePlan);

    // Verify the optimized plan contains aggregation above filter semijoin
    // with nation joined to supplier
    ASSERT_EQ(1, sqlPlan.plan->fragments().size());
    auto sqlPlanNode = sqlPlan.plan->fragments().at(0).fragment.planNode;

    checkPlanText(
        sqlPlanNode,
        {"Aggregation", "LEFT SEMI \\(FILTER\\)", "supplier", "nation"});
  }
}

} // namespace
} // namespace facebook::axiom::optimizer

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
