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

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class HiveQueriesTest : public test::HiveQueriesTestBase {};

TEST_F(HiveQueriesTest, basic) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto scan = [&](const std::string& tableName) {
    return exec::test::PlanBuilder(planNodeIdGenerator)
        .tableScan(tableName, getSchema(tableName));
  };

  checkResults("SELECT * FROM nation", scan("nation").planNode());

  checkResults(
      "SELECT * FROM nation LIMIT 5",
      scan("nation").limit(0, 5, false).planNode());

  checkResults(
      "SELECT * FROM nation OFFSET 7 LIMIT 5",
      scan("nation").limit(7, 5, false).planNode());

  checkResults(
      "SELECT count(*) FROM nation",
      scan("nation")
          .localPartition({})
          .singleAggregation({}, {"count(*)"})
          .planNode());

  checkResults(
      "SELECT DISTINCT n_regionkey FROM nation",
      scan("nation").singleAggregation({"n_regionkey"}, {}).planNode());

  checkResults(
      "SELECT r_name, count(*) FROM region, nation WHERE r_regionkey = n_regionkey GROUP BY 1",
      scan("region")
          .hashJoin(
              {"r_regionkey"},
              {"n_regionkey"},
              scan("nation").planNode(),
              "",
              {"r_name"})
          .localPartition({})
          .singleAggregation({"r_name"}, {"count(*)"})
          .planNode());
}

TEST_F(HiveQueriesTest, crossJoin) {
  auto statement =
      prestoParser().parse("SELECT * FROM nation JOIN region ON true");

  ASSERT_TRUE(statement->isSelect());
  auto logicalPlan =
      statement->as<::axiom::sql::presto::SelectStatement>()->plan();
  ASSERT_TRUE(logicalPlan != nullptr);
}

TEST_F(HiveQueriesTest, orderOfOperations) {
  auto test = [&](lp::PlanBuilder& planBuilder,
                  core::PlanMatcherBuilder& matcherBuilder) {
    auto plan =
        planVelox(planBuilder.build(), {.numWorkers = 1, .numDrivers = 1});

    auto matcher = matcherBuilder.build();
    checkSingleNodePlan(plan, matcher);
  };

  auto scan = [&](const std::string& tableName) {
    lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
    return lp::PlanBuilder(context).tableScan(tableName);
  };

  auto scanMatcher = [&]() { return core::PlanMatcherBuilder().tableScan(); };

  // Multiple limits.
  test(scan("nation").limit(10).limit(5), scanMatcher().finalLimit(0, 5));

  test(scan("nation").limit(10).limit(15), scanMatcher().finalLimit(0, 10));

  test(
      scan("nation").limit(10).offset(7).limit(5),
      scanMatcher().finalLimit(7, 3));

  // Multiple orderBys. Last one wins.
  test(
      scan("nation").orderBy({"n_nationkey"}).orderBy({"n_name desc"}),
      scanMatcher().orderBy({"n_name desc"}));

  // orderBy -> limit becomes topN.
  // limit -> orderBy stays as is.
  test(
      scan("nation")
          .limit(20)
          .orderBy({"n_nationkey"})
          .limit(10)
          .orderBy({"n_name desc"}),
      scanMatcher().limit().topN().orderBy({"n_name desc"}));

  // GroupBy drops preceding orderBy.
  test(
      scan("nation")
          .orderBy({"n_nationkey"})
          .aggregate({"n_name"}, {"count(1)"})
          .orderBy({"n_name desc"}),
      // Fix this plan. There should be no partial agg.
      scanMatcher().singleAggregation().orderBy({"n_name desc"}));

  // Multiple filters after groupBy. Filters that depend solely on grouping
  // keys are pushed down below the groupBy.
  test(
      scan("nation")
          .aggregate({"n_name"}, {"count(1) as cnt"})
          .filter("n_name > 'a'")
          .filter("cnt > 10")
          .filter("length(n_name) < cnt"),
      scanMatcher().singleAggregation().filter(
          "cnt > 10 and cnt > length(n_name)"));

  // Multiple filters are allowed before a limit.
  test(
      scan("nation")
          .filter("n_nationkey > 2")
          .limit(10)
          .filter("n_nationkey < 100")
          .filter("n_regionkey > 10")
          .limit(5)
          .filter("n_nationkey > 70")
          .filter("n_regionkey < 7"),
      scanMatcher()
          .finalLimit(0, 10)
          .filter("n_nationkey < 100 AND n_regionkey > 10")
          .finalLimit(0, 5)
          .filter("n_nationkey > 70 AND n_regionkey < 7"));

  // Limit on both sides of the join.
  {
    lp::PlanBuilder::Context context{exec::test::kHiveConnectorId};
    auto logicalPlan =
        lp::PlanBuilder(context).tableScan("nation").limit(0, 10).join(
            lp::PlanBuilder(context).tableScan("region").limit(0, 5),
            "n_regionkey = r_regionkey",
            lp::JoinType::kInner);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .finalLimit(0, 10)
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("region")
                               .finalLimit(0, 5)
                               .build(),
                           core::JoinType::kInner);

    test(logicalPlan, matcher);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
