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
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
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
        exec::test::kHiveConnectorId, std::nullopt, pool());
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
    checkResults(sql, referencePlan);
  }

  velox::core::PlanNodePtr planTpch(int32_t query) {
    return toSingleNodePlan(parseTpchSql(query));
  }

  std::unique_ptr<exec::test::TpchQueryBuilder> referenceBuilder_;
};

TEST_F(TpchPlanTest, stats) {
  auto verifyStats = [&](const auto& tableName, auto cardinality) {
    SCOPED_TRACE(tableName);

    auto logicalPlan = lp::PlanBuilder()
                           .tableScan(exec::test::kHiveConnectorId, tableName)
                           .build();

    auto planAndStats = planVelox(logicalPlan);
    auto stats = planAndStats.prediction;
    ASSERT_EQ(stats.size(), 1);

    ASSERT_EQ(stats.begin()->first, logicalPlan->id());
    ASSERT_EQ(stats.begin()->second.cardinality, cardinality);
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

  // TODO Verify the plan.

  ASSERT_NO_THROW(planTpch(2));
  ASSERT_NO_THROW(planVelox(parseTpchSql(2)));
}

TEST_F(TpchPlanTest, q03) {
  checkTpchSql(3);

  // The query is straightforward to do by hash. We select 1/5 of customer and
  // 1/2 of orders. We first join orders x customer, build on that and then
  // probe on lineitem. There is anti-correlation between the date filters on
  // lineitem and orders but that does not affect the best plan choice.

  auto startMatcher = [&](const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  auto matcher =
      startMatcher("lineitem")
          .hashJoin(
              startMatcher("orders")
                  .hashJoin(
                      startMatcher("customer").build(), core::JoinType::kInner)
                  .build(),
              core::JoinType::kInner)
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
                     .hashJoin(
                         core::PlanMatcherBuilder()
                             .hiveScan(
                                 "orders",
                                 test::between(
                                     "o_orderdate",
                                     DATE()->toDays("1993-07-01"),
                                     DATE()->toDays("1993-09-30")))
                             .build(),
                         core::JoinType::kRightSemiFilter)
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

  auto startMatcher = [&](const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  // agg((
  //   (lineitem INNER ((orders INNER customer) INNER (nation INNER region)))
  //   INNER
  //   (supplier LEFT SEMI (FILTER) (nation INNER region))
  // ))

  auto joinNationWithRegion = startMatcher("nation").hashJoin(
      core::PlanMatcherBuilder()
          .hiveScan("region", test::eq("r_name", "ASIA"))
          .build(),
      core::JoinType::kInner);

  auto matcher =
      startMatcher("lineitem")
          .hashJoin(
              startMatcher("orders")
                  .hashJoin(
                      startMatcher("customer").build(), core::JoinType::kInner)
                  .hashJoin(
                      joinNationWithRegion.build(), core::JoinType::kInner)
                  .build())
          .hashJoin(startMatcher("supplier")
                        .hashJoin(
                            joinNationWithRegion.project().build(),
                            core::JoinType::kLeftSemiFilter)
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

  // TODO Verify the plan.

  ASSERT_NO_THROW(planTpch(9));
  ASSERT_NO_THROW(planVelox(parseTpchSql(9)));
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

  auto startMatcher = [&](const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  auto matcher =
      startMatcher("partsupp")
          .hashJoin(
              startMatcher("supplier")
                  .hashJoin(
                      core::PlanMatcherBuilder()
                          .hiveScan("nation", test::eq("n_name", "GERMANY"))
                          .build(),
                      core::JoinType::kInner)
                  .build(),
              core::JoinType::kInner)
          .project()
          .aggregation()
          .nestedLoopJoin(
              startMatcher("partsupp")
                  .hashJoin(
                      startMatcher("supplier")
                          .hashJoin(
                              core::PlanMatcherBuilder()
                                  .hiveScan(
                                      "nation", test::eq("n_name", "GERMANY"))
                                  .build(),
                              core::JoinType::kInner)
                          .build(),
                      core::JoinType::kInner)
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
      core::PlanMatcherBuilder()
          .tableScan("orders")
          .hashJoin(
              core::PlanMatcherBuilder()
                  .hiveScan(
                      "lineitem",
                      std::move(subfieldFilters),
                      "l_commitdate < l_receiptdate AND l_shipdate < l_commitdate")
                  .build(),
              core::JoinType::kInner)
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

  auto startMatcher = [&](const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  auto matcher =
      startMatcher("orders")
          .hashJoin(startMatcher("customer").build(), core::JoinType::kRight)
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

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan("part")
                     .hashJoin(
                         core::PlanMatcherBuilder()
                             .hiveScan(
                                 "lineitem",
                                 test::between(
                                     "l_shipdate",
                                     DATE()->toDays("1995-09-01"),
                                     DATE()->toDays("1995-09-30")))
                             .build(),
                         core::JoinType::kInner)
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

  auto startMatcher = [&](const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  auto matcher =
      startMatcher("partsupp")
          .hashJoin(startMatcher("part").build(), core::JoinType::kInner)
          .hashJoin(startMatcher("supplier").build(), core::JoinType::kAnti)
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

  auto startMatcher = [&](const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  auto matcher =
      startMatcher("lineitem")
          .hashJoin(startMatcher("part").build(), core::JoinType::kInner)
          .hashJoin(
              startMatcher("lineitem")
                  .hashJoin(
                      startMatcher("part").build(),
                      core::JoinType::kLeftSemiFilter)
                  .aggregation()
                  .project() // TODO Figure out if it can be removed.
                  .build(),
              core::JoinType::kLeft)
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

  // TODO Verify the plan.

  ASSERT_NO_THROW(planTpch(18));
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
          .hashJoin(
              core::PlanMatcherBuilder()
                  .hiveScan(
                      "part",
                      {},
                      "\"or\"(\"and\"(p_size between 1 and 15, (p_brand = 'Brand#34' AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))), "
                      "   \"or\"(\"and\"(p_size between 1 and 5, (p_brand = 'Brand#12' AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))), "
                      "          \"and\"(p_size between 1 and 10, (p_brand = 'Brand#23' AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')))))")
                  .build(),
              core::JoinType::kInner)
          .filter()
          .project()
          .aggregation()
          .build();

  auto plan = planTpch(19);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(19)));
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

  // TODO Verify the plan.

  ASSERT_NO_THROW(planTpch(20));
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

  auto startMatcher = [&](const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  // The query is straightforward, with the not exists resolved with a right
  // semijoin and the non-correlated subquery becoming a cross join to the one
  // row result set of the non-grouped aggregation.

  auto matcher = startMatcher("orders")
                     .hashJoin(
                         startMatcher("customer")
                             .nestedLoopJoin(
                                 startMatcher("customer").aggregation().build())
                             .filter()
                             .build(),
                         velox::core::JoinType::kRightSemiProject)
                     .filter()
                     .project()
                     .aggregation()
                     .orderBy()
                     .build();

  auto plan = planTpch(22);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(parseTpchSql(22)));
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

} // namespace
} // namespace facebook::axiom::optimizer

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
