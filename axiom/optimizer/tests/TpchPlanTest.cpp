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

  static std::filesystem::path makePath(const std::string& filePath) {
    return std::filesystem::path{__FILE__}.parent_path() / filePath;
  }

  static std::string readSqlFromFile(const std::string& filePath) {
    const auto path = makePath(filePath);
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

  AXIOM_ASSERT_PLAN(planTpch(1), matcher);
}

TEST_F(TpchPlanTest, q02) {
  checkTpchSql(2);

  // TODO Verify the plan.
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

  AXIOM_ASSERT_PLAN(planTpch(3), matcher);
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
                         core::JoinType::kRightSemiProject)
                     .filter()
                     .aggregation()
                     .orderBy()
                     .build();

  AXIOM_ASSERT_PLAN(planTpch(4), matcher);
}

TEST_F(TpchPlanTest, q05) {
  checkTpchSql(5);

  // TODO Verify the plan.
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

  AXIOM_ASSERT_PLAN(planTpch(6), matcher);
}

TEST_F(TpchPlanTest, q07) {
  checkTpchSql(7);

  // TODO Verify the plan.
}

TEST_F(TpchPlanTest, q08) {
  checkTpchSql(8);

  // TODO Verify the plan.
}

TEST_F(TpchPlanTest, q09) {
  checkTpchSql(9);

  // TODO Verify the plan.
}

TEST_F(TpchPlanTest, q10) {
  checkTpchSql(10);

  // TODO Verify the plan.
}

TEST_F(TpchPlanTest, q11) {
  checkTpchSql(11);

  // TODO Verify the plan.
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

  AXIOM_ASSERT_PLAN(planTpch(12), matcher);
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

  AXIOM_ASSERT_PLAN(planTpch(13), matcher);
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

  AXIOM_ASSERT_PLAN(planTpch(14), matcher);
}

TEST_F(TpchPlanTest, q15) {
  checkTpchSql(15);

  // TODO Verify the plan.
}

TEST_F(TpchPlanTest, q16) {
  checkTpchSql(16);

  // The join is biggest table first, with  part joined first because it is
  // quite selective, more so than the exists with supplier.

  auto startMatcher = [&](const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  auto matcher =
      startMatcher("partsupp")
          .hashJoin(startMatcher("part").build(), core::JoinType::kInner)
          .hashJoin(
              startMatcher("supplier").build(),
              core::JoinType::kLeftSemiProject)
          .filter()
          .aggregation()
          .orderBy()
          .build();

  AXIOM_ASSERT_PLAN(planTpch(16), matcher);
}

TEST_F(TpchPlanTest, q17) {
  checkTpchSql(17);

  // TODO Verify the plan.
}

TEST_F(TpchPlanTest, q18) {
  checkTpchSql(18);

  // TODO Verify the plan.
}

TEST_F(TpchPlanTest, q19) {
  checkTpchSql(19);

  // TODO Verify the plan.
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
}

TEST_F(TpchPlanTest, q21) {
  checkTpchSql(21);

  // TODO Verify the plan.
}

TEST_F(TpchPlanTest, q22) {
  checkTpchSql(22);

  // TODO Verify the plan.
}

// Use to re-generate the plans stored in tpch.plans directory.
TEST_F(TpchPlanTest, DISABLED_makePlans) {
  const auto path = makePath("tpch.plans");

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
