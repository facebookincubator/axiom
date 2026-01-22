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
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class SubqueryTest : public test::HiveQueriesTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  void SetUp() override {
    test::HiveQueriesTestBase::SetUp();

    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);

    HiveQueriesTestBase::TearDown();
  }

  std::shared_ptr<connector::TestConnector> testConnector_;
};

TEST_F(SubqueryTest, scalar) {
  // = <subquery>
  {
    auto query =
        "select * from nation where n_regionkey "
        "= (select r_regionkey from region where r_name like 'AF%')";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .hiveScan("region", {}, "r_name like 'AF%'")
                               .build(),
                           velox::core::JoinType::kInner)
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IN <subquery>
  {
    auto query =
        "select * from nation where n_regionkey "
        "IN (select r_regionkey from region where r_name > 'ASIA')";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .hiveScan("region", test::gt("r_name", "ASIA"))
                               .build(),
                           velox::core::JoinType::kLeftSemiFilter)
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IN <subquery> with coercion. The subquery returns a tinyint, which needs to
  // be coerced to bigint to match the left side of the IN predicate.
  {
    auto query =
        "select * from nation where n_regionkey "
        "IN (select cast(r_regionkey as tinyint) from region where r_name > 'ASIA')";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder()
                    .hiveScan("region", test::gt("r_name", "ASIA"))
                    .project({"cast(cast(r_regionkey as tinyint) as bigint)"})
                    .build(),
                velox::core::JoinType::kLeftSemiFilter)
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // NOT IN <subquery>
  {
    auto query =
        "select * from nation where n_regionkey "
        "NOT IN (select r_regionkey from region where r_name > 'ASIA')";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .hiveScan("region", test::gt("r_name", "ASIA"))
                               .build(),
                           velox::core::JoinType::kAnti)
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, foldable) {
  testConnector_->addTable("t", ROW({"a", "ds"}, {INTEGER(), VARCHAR()}));
  testConnector_->setDiscreteValues(
      "t",
      {"ds"},
      {
          Variant::row({"2025-10-29"}),
          Variant::row({"2025-10-30"}),
          Variant::row({"2025-10-31"}),
          Variant::row({"2025-11-01"}),
          Variant::row({"2025-11-02"}),
          Variant::row({"2025-11-03"}),
      });

  auto parseSql = [&](const std::string& sql) {
    return parseSelect(sql, kTestConnectorId);
  };

  auto makeMatcher = [&](const std::string& filter) {
    return core::PlanMatcherBuilder().tableScan("t").filter(filter).build();
  };

  {
    auto logicalPlan =
        parseSql("SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t)");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("ds = '2025-11-03'");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto logicalPlan =
        parseSql("SELECT * FROM t WHERE ds = (SELECT min(ds) FROM t)");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("ds = '2025-10-29'");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t WHERE ds < '2025-11-02')");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("ds = '2025-11-01'");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t WHERE ds like '%-10-%')");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("ds = '2025-10-31'");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t WHERE ds < '2025-01-01')");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("ds = null");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, correlatedExists) {
  {
    auto query =
        "SELECT * FROM nation WHERE "
        "EXISTS (SELECT * FROM region WHERE r_regionkey = n_regionkey)";

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().hiveScan("region", {}).build(),
                velox::core::JoinType::kLeftSemiFilter)
            .build();

    {
      SCOPED_TRACE(query);
      auto plan = toSingleNodePlan(query);
      AXIOM_ASSERT_PLAN(plan, matcher);
    }

    query =
        "SELECT * FROM nation WHERE "
        "EXISTS (SELECT 1 FROM region WHERE r_regionkey = n_regionkey)";

    {
      SCOPED_TRACE(query);
      auto plan = toSingleNodePlan(query);
      AXIOM_ASSERT_PLAN(plan, matcher);
    }
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
