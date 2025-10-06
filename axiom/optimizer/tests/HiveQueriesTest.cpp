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

  checkResults(
      "SELECT * FROM nation, region",
      scan("nation")
          .nestedLoopJoin(
              scan("region").planNode(),
              "",
              {"n_nationkey",
               "n_name",
               "n_regionkey",
               "n_comment",
               "r_regionkey",
               "r_name",
               "r_comment"})
          .planNode());

  {
    auto secondRegion =
        scan("region").project({"r_name as r2_name"}).planNode();
    auto plan =
        scan("nation")
            .nestedLoopJoin(
                scan("region").planNode(),
                "",
                {
                    "n_name",
                    "r_name",
                })
            .nestedLoopJoin(secondRegion, "", {"n_name", "r_name", "r2_name"})
            .planNode();
    checkResults(
        "SELECT n.n_name, r1.r_name AS r_name, r2.r_name AS r2_name FROM nation n, region r1, region r2",
        plan);
  }

  checkResults(
      "SELECT c.c_custkey, n.n_name, r.r_name FROM customer c INNER JOIN nation n ON c.c_nationkey = n.n_regionkey CROSS JOIN region r",
      scan("customer")
          .project({"c_custkey", "c_nationkey"})
          .hashJoin(
              {"c_nationkey"},
              {"n_regionkey"},
              scan("nation").project({"n_name", "n_regionkey"}).planNode(),
              "",
              {"c_custkey", "c_nationkey", "n_name"})
          .nestedLoopJoin(
              scan("region").project({"r_name"}).planNode(),
              "",
              {"c_custkey", "n_name", "r_name"})
          .planNode());
}

TEST_F(HiveQueriesTest, crossJoin) {
  auto statement =
      prestoParser().parse("SELECT * FROM nation JOIN region ON true");

  ASSERT_TRUE(statement->isSelect());
  auto logicalPlan = statement->asUnchecked<test::SelectStatement>()->plan();
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
}

} // namespace
} // namespace facebook::axiom::optimizer
