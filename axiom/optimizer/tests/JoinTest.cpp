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
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class JoinTest : public test::QueryTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  void SetUp() override {
    test::QueryTestBase::SetUp();

    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);

    test::QueryTestBase::TearDown();
  }

  std::shared_ptr<connector::TestConnector> testConnector_;
};

struct JoinOptions {
  size_t numJoins = 0;
  JoinOrder joinOrder = JoinOrder::kCost;
  bool sample = false;
  bool reducingExistences = false;
};

constexpr size_t kNumTables = 301;
#ifdef NDEBUG
constexpr bool kIsDebug = false;
#else
constexpr bool kIsDebug = true;
#endif

// TODO Move this test into its own file.
TEST_F(JoinTest, perfJoinChain) {
  for (int i = 0; i < kNumTables; ++i) {
    std::vector<std::string> columns;
    columns.reserve(kNumTables);
    for (int j = 0; j < kNumTables; ++j) {
      columns.push_back(fmt::format("t{}c{}", i, j));
    }
    // std::cout << "Table t" << i << " schema: ";
    // for (const auto& col : columns) {
    //   std::cout << col << " ";
    // }
    // std::cout << "\n";
    testConnector_->addTable(
        fmt::format("t{}", i), ROW(std::move(columns), BIGINT()));
  }

  static constexpr std::array kJoinOptions = {
      JoinOptions{
          .numJoins = kIsDebug ? 150 : 200,
          .joinOrder = JoinOrder::kSyntactic,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 150 : 200,
          .joinOrder = JoinOrder::kGreedy,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 14 : 18,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 14 : 18,
          .sample = true,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 14 : 18,
          .sample = true,
          .reducingExistences = true,
      },
  };

  auto optimizerOptionsOld = optimizerOptions_;
  SCOPE_EXIT {
    optimizerOptions_ = optimizerOptionsOld;
  };
  for (const auto& joinOptions : kJoinOptions) {
    ASSERT_GT(kNumTables, joinOptions.numJoins)
        << "Not enough tables for the test";
    lp::PlanBuilder::Context context(kTestConnectorId);
    std::vector<lp::PlanBuilder> planBuilders;
    planBuilders.emplace_back(lp::PlanBuilder(context).tableScan("t0"));
    for (int i = 1; i <= joinOptions.numJoins; ++i) {
      planBuilders.emplace_back(planBuilders.back().join(
          lp::PlanBuilder(context).tableScan(fmt::format("t{}", i)),
          fmt::format("t{}c{} = t{}c{}", i - 1, i - 1, i, i - 1),
          lp::JoinType::kInner));
    }
    planBuilders.back().project({"t0c0"});
    const auto logicalPlan = planBuilders.back().build();
    // std::cout << "Logical:\n" << logicalPlan->toString()  << std::endl;

    optimizerOptions_.sampleFilters = joinOptions.sample;
    optimizerOptions_.sampleJoins = joinOptions.sample;
    optimizerOptions_.joinOrder = joinOptions.joinOrder;
    optimizerOptions_.enableReducingExistences = joinOptions.reducingExistences;

    const auto start = std::chrono::steady_clock::now();
    const auto plan = toSingleNodePlan(logicalPlan);
    const auto end = std::chrono::steady_clock::now();
    const auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();
    std::cout << "Linear join chain optimized in " << duration << " ms"
              << " (options: "
              << "numJoins=" << joinOptions.numJoins << ", "
              << "joinOrder=" << toString(joinOptions.joinOrder) << ", "
              << "reducingExistences=" << joinOptions.reducingExistences << ", "
              << "sample=" << joinOptions.sample << ")" << std::endl;
    // std::cout << "\nExecution:\n" << plan->toString(true, true) << std::endl;
  }
}

// TODO Move this test into its own file.
TEST_F(JoinTest, perfJoinStar) {
  for (int i = 0; i < kNumTables; ++i) {
    std::vector<std::string> columns;
    columns.reserve(kNumTables);
    for (int j = 0; j < kNumTables; ++j) {
      columns.push_back(fmt::format("t{}c{}", i, j));
    }
    // std::cout << "Table t" << i << " schema: ";
    // for (const auto& col : columns) {
    //   std::cout << col << " ";
    // }
    // std::cout << "\n";
    testConnector_->addTable(
        fmt::format("t{}", i), ROW(std::move(columns), BIGINT()));
  }

  static constexpr std::array kJoinOptions = {
      JoinOptions{
          .numJoins = kIsDebug ? 150 : 200,
          .joinOrder = JoinOrder::kSyntactic,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 150 : 200,
          .joinOrder = JoinOrder::kGreedy,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 8 : 9,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 8 : 9,
          .sample = true,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 8 : 9,
          .sample = true,
          .reducingExistences = true,
      },
  };

  auto optimizerOptionsOld = optimizerOptions_;
  SCOPE_EXIT {
    optimizerOptions_ = optimizerOptionsOld;
  };
  for (const auto& joinOptions : kJoinOptions) {
    ASSERT_GT(kNumTables, joinOptions.numJoins)
        << "Not enough tables for the test";
    lp::PlanBuilder::Context context(kTestConnectorId);
    std::vector<lp::PlanBuilder> planBuilders;
    planBuilders.emplace_back(lp::PlanBuilder(context).tableScan("t0"));
    for (int i = 1; i <= joinOptions.numJoins; ++i) {
      planBuilders.emplace_back(planBuilders.back().join(
          lp::PlanBuilder(context).tableScan(fmt::format("t{}", i)),
          fmt::format("t{}c{} = t{}c{}", 0, i, i, i),
          lp::JoinType::kInner));
    }
    planBuilders.back().project({"t0c0"});
    const auto logicalPlan = planBuilders.back().build();
    // std::cout << "Logical:\n" << logicalPlan->toString()  << std::endl;

    optimizerOptions_.sampleFilters = joinOptions.sample;
    optimizerOptions_.sampleJoins = joinOptions.sample;
    optimizerOptions_.joinOrder = joinOptions.joinOrder;
    optimizerOptions_.enableReducingExistences = joinOptions.reducingExistences;

    const auto start = std::chrono::steady_clock::now();
    const auto plan = toSingleNodePlan(logicalPlan);
    const auto end = std::chrono::steady_clock::now();
    const auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();
    std::cout << "Star join optimized in " << duration << " ms"
              << " (options: "
              << "numJoins=" << joinOptions.numJoins << ", "
              << "joinOrder=" << toString(joinOptions.joinOrder) << ", "
              << "reducingExistences=" << joinOptions.reducingExistences << ", "
              << "sample=" << joinOptions.sample << ")" << std::endl;
    // std::cout << "\nExecution:\n" << plan->toString(true, true) << std::endl;
  }
}

// TODO Move this test into its own file.
TEST_F(JoinTest, perfJoinClique) {
  for (size_t i = 0; i < kNumTables; ++i) {
    std::vector<std::string> columns;
    columns.push_back(fmt::format("c{}", i));
    // std::cout << "Table t" << i << " schema: ";
    // for (const auto& col : columns) {
    //   std::cout << col << " ";
    // }
    // std::cout << "\n";
    testConnector_->addTable(
        fmt::format("t{}", i), ROW(std::move(columns), BIGINT()));
  }

  static constexpr std::array kJoinOptions = {
      JoinOptions{
          .numJoins = kIsDebug ? 130 : 260,
          .joinOrder = JoinOrder::kSyntactic,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 29 : 43,
          .joinOrder = JoinOrder::kGreedy,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 5 : 6,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 5 : 6,
          .sample = true,
      },
      JoinOptions{
          .numJoins = kIsDebug ? 5 : 6,
          .sample = true,
          .reducingExistences = true,
      },
  };

  auto optimizerOptionsOld = optimizerOptions_;
  SCOPE_EXIT {
    optimizerOptions_ = optimizerOptionsOld;
  };
  for (const auto& joinOptions : kJoinOptions) {
    ASSERT_GT(kNumTables, joinOptions.numJoins)
        << "Not enough tables for the test";
    lp::PlanBuilder::Context context(kTestConnectorId);
    std::vector<lp::PlanBuilder> planBuilders;
    planBuilders.emplace_back(lp::PlanBuilder(context).tableScan("t0"));
    for (int i = 1; i <= joinOptions.numJoins; ++i) {
      planBuilders.emplace_back(planBuilders.back().join(
          lp::PlanBuilder(context).tableScan(fmt::format("t{}", i)),
          fmt::format("c{} = c{}", i - 1, i),
          lp::JoinType::kInner));
    }
    planBuilders.back().project({"c0"});
    const auto logicalPlan = planBuilders.back().build();
    // std::cout << "Logical:\n" << logicalPlan->toString()  << std::endl;

    optimizerOptions_.sampleFilters = joinOptions.sample;
    optimizerOptions_.sampleJoins = joinOptions.sample;
    optimizerOptions_.joinOrder = joinOptions.joinOrder;
    optimizerOptions_.enableReducingExistences = joinOptions.reducingExistences;

    const auto start = std::chrono::steady_clock::now();
    const auto plan = toSingleNodePlan(logicalPlan);
    const auto end = std::chrono::steady_clock::now();
    const auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();
    std::cout << "Linear join clique optimized in " << duration << " ms"
              << " (options: "
              << "numJoins=" << joinOptions.numJoins << ", "
              << "joinOrder=" << toString(joinOptions.joinOrder) << ", "
              << "reducingExistences=" << joinOptions.reducingExistences << ", "
              << "sample=" << joinOptions.sample << ")" << std::endl;
    // std::cout << "\nExecution:\n" << plan->toString(true, true) << std::endl;
  }
}

TEST_F(JoinTest, pushdownFilterThroughJoin) {
  testConnector_->addTable("t", ROW({"t_id", "t_data"}, BIGINT()));
  testConnector_->addTable("u", ROW({"u_id", "u_data"}, BIGINT()));

  auto makePlan = [&](lp::JoinType joinType) {
    lp::PlanBuilder::Context ctx{kTestConnectorId};
    return lp::PlanBuilder{ctx}
        .tableScan("t")
        .join(lp::PlanBuilder{ctx}.tableScan("u"), "t_id = u_id", joinType)
        .filter("t_data IS NULL")
        .filter("u_data IS NULL")
        .build();
  };

  {
    SCOPED_TRACE("Inner Join");
    auto logicalPlan = makePlan(lp::JoinType::kInner);
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan("t")
                       .filter("t_data IS NULL")
                       .hashJoin(
                           core::PlanMatcherBuilder{}
                               .tableScan("u")
                               .filter("u_data IS NULL")
                               .build(),
                           core::JoinType::kInner)
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("Left Join");
    auto logicalPlan = makePlan(lp::JoinType::kLeft);
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan("t")
                       .filter("t_data IS NULL")
                       .hashJoin(
                           core::PlanMatcherBuilder{}.tableScan("u").build(),
                           core::JoinType::kLeft)
                       .filter("u_data IS NULL")
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("Right Join");
    auto logicalPlan = makePlan(lp::JoinType::kRight);
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan("t")
                       .hashJoin(
                           core::PlanMatcherBuilder{}
                               .tableScan("u")
                               .filter("u_data IS NULL")
                               .build(),
                           core::JoinType::kRight)
                       .filter("t_data IS NULL")
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("Full Join");
    auto logicalPlan = makePlan(lp::JoinType::kFull);
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan("t")
                       .hashJoin(
                           core::PlanMatcherBuilder{}.tableScan("u").build(),
                           core::JoinType::kFull)
                       .filter("t_data IS NULL AND u_data IS NULL")
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, hyperEdge) {
  testConnector_->addTable("t", ROW({"t_id", "t_key", "t_data"}, BIGINT()));
  testConnector_->addTable("u", ROW({"u_id", "u_key", "u_data"}, BIGINT()));
  testConnector_->addTable("v", ROW({"v_key", "v_data"}, BIGINT()));

  lp::PlanBuilder::Context ctx{kTestConnectorId};
  auto logicalPlan = lp::PlanBuilder{ctx}
                         .from({"t", "u"})
                         .filter("t_id = u_id")
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("v"),
                             "t_key = v_key AND u_key = v_key",
                             lp::JoinType::kLeft)
                         .build();

  auto matcher = core::PlanMatcherBuilder{}
                     .tableScan("t")
                     .hashJoin(
                         core::PlanMatcherBuilder{}.tableScan("u").build(),
                         core::JoinType::kInner)
                     .hashJoin(
                         core::PlanMatcherBuilder{}.tableScan("v").build(),
                         core::JoinType::kLeft)
                     .build();
  auto plan = toSingleNodePlan(logicalPlan);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(JoinTest, joinWithFilterOverLimit) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y", "z"}, BIGINT()));

  lp::PlanBuilder::Context ctx(kTestConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(ctx)
          .tableScan("t")
          .limit(100)
          .filter("b > 50")
          .join(
              lp::PlanBuilder(ctx).tableScan("u").limit(50).filter("y < 100"),
              "a = x",
              lp::JoinType::kInner)
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("t")
                       .limit()
                       .filter("b > 50")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("u")
                               .limit()
                               .filter("y < 100")
                               .build())
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, outerJoinWithInnerJoin) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("v", ROW({"vx", "vy", "vz"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y", "z"}, BIGINT()));

  auto startMatcher = [&](const auto& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  {
    SCOPED_TRACE("left join with inner join on right");

    lp::PlanBuilder::Context ctx(kTestConnectorId);
    auto logicalPlan = lp::PlanBuilder(ctx)
                           .tableScan("t")
                           .filter("b > 50")
                           .join(
                               lp::PlanBuilder(ctx).tableScan("u").join(
                                   lp::PlanBuilder(ctx).tableScan("v"),
                                   "x = vx",
                                   lp::JoinType::kInner),
                               "a = x",
                               lp::JoinType::kLeft)
                           .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        startMatcher("u")
            .hashJoin(startMatcher("v").build(), core::JoinType::kInner)
            .hashJoin(
                startMatcher("t").filter("b > 50").build(),
                core::JoinType::kRight)
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("aggregation left join filter over inner join");

    lp::PlanBuilder::Context ctx(kTestConnectorId);
    auto logicalPlan = lp::PlanBuilder(ctx)
                           .tableScan("t")
                           .filter("b > 50")
                           .aggregate({"a", "b"}, {"sum(c)"})
                           .join(
                               lp::PlanBuilder(ctx)
                                   .tableScan("u")
                                   .join(
                                       lp::PlanBuilder(ctx).tableScan("v"),
                                       "x = vx",
                                       lp::JoinType::kInner)
                                   .filter("not(x = vy)"),
                               "a = x",
                               lp::JoinType::kLeft)
                           .build();

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        startMatcher("u")
            .hashJoin(startMatcher("v").build())
            .filter()
            .hashJoin(startMatcher("t").filter().aggregation().build())
            .project()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
