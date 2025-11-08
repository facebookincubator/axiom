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
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class JoinTest : public QueryTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  void SetUp() override {
    QueryTestBase::SetUp();

    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);

    QueryTestBase::TearDown();
  }

  std::shared_ptr<connector::TestConnector> testConnector_;
};

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

  {
    SCOPED_TRACE("left join with inner join on right");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("t")
            .filter("b > 50")
            .hashJoin(
                core::PlanMatcherBuilder()
                    .tableScan("u")
                    .hashJoin(core::PlanMatcherBuilder().tableScan("v").build())

                    .build())
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  logicalPlan = lp::PlanBuilder(ctx)
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

  {
    SCOPED_TRACE("aggregation left join filter over inner join");
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("t")
            .filter()
            .aggregation()
            .hashJoin(
                core::PlanMatcherBuilder()
                    .tableScan("u")
                    .hashJoin(core::PlanMatcherBuilder().tableScan("v").build())
                    .filter()
                    .build())
            .project()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, pushdownFilterThroughJoin) {
  testConnector_->addTable("t1", ROW({"id1", "data1"}, {BIGINT(), BIGINT()}));
  testConnector_->addTable("t2", ROW({"id2", "data2"}, {BIGINT(), BIGINT()}));

  auto makePlan = [&](lp::JoinType joinType) {
    lp::PlanBuilder::Context ctx{kTestConnectorId};
    return lp::PlanBuilder{ctx}
        .from({"t1"})
        .join(lp::PlanBuilder{ctx}.from({"t2"}), "id1 = id2", joinType)
        .filter("data1 IS NULL")
        .filter("data2 IS NULL")
        .build();
  };

  {
    SCOPED_TRACE("Inner Join");
    auto logicalPlan = makePlan(lp::JoinType::kInner);
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan("t1")
                       .filter("data1 IS NULL")
                       .hashJoin(
                           core::PlanMatcherBuilder{}
                               .tableScan("t2")
                               .filter("data2 IS NULL")
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
                       .tableScan("t1")
                       .filter("data1 IS NULL")
                       .hashJoin(
                           core::PlanMatcherBuilder{}.tableScan("t2").build(),
                           core::JoinType::kLeft)
                       .filter("data2 IS NULL")
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
  {
    SCOPED_TRACE("Right Join");
    // This is needed because without this we cannot test right join
    // properly as it gets converted to left join by swapping inputs.
    auto wasSyntacticJoinOrder = optimizerOptions_.syntacticJoinOrder;
    optimizerOptions_.syntacticJoinOrder = true;
    SCOPE_EXIT {
      optimizerOptions_.syntacticJoinOrder = wasSyntacticJoinOrder;
    };
    auto logicalPlan = makePlan(lp::JoinType::kRight);
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan("t1")
                       .hashJoin(
                           core::PlanMatcherBuilder{}
                               .tableScan("t2")
                               .filter("data2 IS NULL")
                               .build(),
                           core::JoinType::kRight)
                       .filter("data1 IS NULL")
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
  {
    SCOPED_TRACE("Full Join");
    auto logicalPlan = makePlan(lp::JoinType::kFull);
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan("t1")
                       .hashJoin(
                           core::PlanMatcherBuilder{}.tableScan("t2").build(),
                           core::JoinType::kFull)
                       .filter("data1 IS NULL AND data2 IS NULL")
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer::test
