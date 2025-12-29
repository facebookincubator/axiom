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

#include "axiom/optimizer/ConstantExprEvaluator.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/ParquetTpchTest.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer::test {
using namespace facebook::velox;

class CopartitionTest : public HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    HiveQueriesTestBase::SetUpTestCase();
    ParquetTpchTest::makeBucketedTables(
        runner::test::LocalRunnerTestBase::localDataPath_);
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();
    parquet::registerParquetWriterFactory();
  }

  void TearDown() override {
    parquet::unregisterParquetWriterFactory();
    HiveQueriesTestBase::TearDown();
  }

  int countTableScans(const velox::core::PlanNodePtr& plan) {
    int count = 0;
    if (plan->name() == "TableScan") {
      count++;
    }
    for (const auto& source : plan->sources()) {
      count += countTableScans(source);
    }
    return count;
  }
};

TEST_F(CopartitionTest, join) {
  auto sql =
      "SELECT l_orderkey, o_orderdate, count(*) AS cnt "
      "FROM lineitem_b, orders_b "
      "WHERE o_orderkey = l_orderkey "
      "GROUP BY l_orderkey, o_orderdate";

  auto plan = parseSelect(sql);
  auto veloxPlan = planVelox(plan, {.numWorkers = 4, .numDrivers = 4});
  auto result = runFragmentedPlan(veloxPlan);

  // Get expected row count from lineitem
  int64_t expectedCount = 150'000;

  // Check that we have as many result rows as the count of orders
  int64_t actualCount = 0;
  for (const auto& batch : result.results) {
    actualCount += batch->size();
  }
  EXPECT_EQ(expectedCount, actualCount);

  // Check that the PlanAndStats has a plan with 2 fragments
  EXPECT_EQ(2, veloxPlan.plan->fragments().size());
  auto rightMatcher = core::PlanMatcherBuilder().tableScan("orders_b").build();

  auto matcher =
      core::PlanMatcherBuilder()
          .tableScan("lineitem_b")
          .hashJoin(rightMatcher, velox::core::JoinType::kInner)
          .localPartition()
          .singleAggregation({"l_orderkey", "o_orderdate"}, {"count() AS cnt"})
          .partitionedOutput()
          .build();

  AXIOM_ASSERT_PLAN(veloxPlan.plan->fragments()[0].fragment.planNode, matcher);
}

TEST_F(CopartitionTest, group) {
  auto sql =
      "SELECT count(*) "
      "FROM ("
      "  SELECT "
      "    prev.l_partkey, "
      "    next.sales - prev.sales AS change "
      "  FROM "
      "    (SELECT "
      "       l_partkey, "
      "       sum(l_extendedprice) sales "
      "     FROM lineitem "
      "     WHERE l_shipdate BETWEEN cast('1995-01-01' AS date) "
      "                      AND cast('1995-12-31' AS date) "
      "     GROUP BY l_partkey) prev, "
      "    (SELECT "
      "       l_partkey, "
      "       sum(l_extendedprice) sales "
      "     FROM lineitem "
      "     WHERE l_shipdate BETWEEN cast('1996-01-01' AS date) "
      "                      AND cast('1996-12-31' AS date) "
      "     GROUP BY l_partkey) next "
      "  WHERE prev.l_partkey = next.l_partkey"
      ") "
      "WHERE change > 0 - 1000000000";

  auto plan = parseSelect(sql);
  auto veloxPlan = planVelox(plan, {.numWorkers = 4, .numDrivers = 4});
  auto result = runFragmentedPlan(veloxPlan);

  EXPECT_EQ(1, result.results.size());
  EXPECT_EQ(1, result.results[0]->size());
  int64_t actualCount = result.results[0]
                            ->childAt(0)
                            ->as<velox::SimpleVector<int64_t>>()
                            ->valueAt(0);
  EXPECT_GT(actualCount, 19000);

  auto rightMatcher =
      core::PlanMatcherBuilder()
          .exchange()
          .localPartition()
          .singleAggregation(
              {"l_partkey_1"}, {"sum(l_extendedprice_5) AS sales_17"})
          .project()
          .build();

  auto matcher =
      core::PlanMatcherBuilder()
          .exchange()
          .localPartition()
          .singleAggregation({"l_partkey"}, {"sum(l_extendedprice) AS sales"})
          .hashJoin(rightMatcher, velox::core::JoinType::kInner)
          .filter()
          .partialAggregation({}, {"count() AS count"})
          .partitionedOutput()
          .build();

  ASSERT_EQ(4, veloxPlan.plan->fragments().size());
  AXIOM_ASSERT_PLAN(veloxPlan.plan->fragments()[2].fragment.planNode, matcher);
}

TEST_F(CopartitionTest, writeWider) {
  // Create part_b2 table with 16 buckets (part_b has 8 buckets)
  auto createTableSql =
      "CREATE TABLE part_b2 "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['p_partkey']) "
      "AS SELECT * FROM part_b";

  // Parse CREATE TABLE AS SELECT statement
  auto statement = prestoParser().parse(createTableSql);
  VELOX_CHECK(statement->isCreateTableAsSelect());
  auto ctasStatement =
      statement->as<::axiom::sql::presto::CreateTableAsSelectStatement>();

  // Create the table
  hiveMetadata().dropTableIfExists("part_b2");
  folly::F14FastMap<std::string, velox::Variant> options;
  for (const auto& [key, value] : ctasStatement->properties()) {
    options[key] =
        optimizer::ConstantExprEvaluator::evaluateConstantExpr(*value);
  }
  auto session = std::make_shared<connector::ConnectorSession>("test");
  auto table = hiveMetadata().createTable(
      session,
      ctasStatement->tableName(),
      ctasStatement->tableSchema(),
      options);

  // Set up schema resolver for the write
  connector::SchemaResolver schemaResolver;
  schemaResolver.setTargetTable(velox::exec::test::kHiveConnectorId, table);

  // Plan and execute the CTAS
  auto createVeloxPlan = planVelox(
      ctasStatement->plan(),
      schemaResolver,
      {.numWorkers = 4, .numDrivers = 4});
  auto createResult = runFragmentedPlan(createVeloxPlan);

  // Get the number of rows written
  EXPECT_EQ(1, createResult.results.size());
  EXPECT_EQ(1, createResult.results[0]->size());
  int64_t rowsWritten = createResult.results[0]
                            ->childAt(0)
                            ->as<velox::SimpleVector<int64_t>>()
                            ->valueAt(0);

  // Run the join query
  auto joinSql =
      "SELECT count(*) FROM part_b t1, part_b2 t2 "
      "WHERE t1.p_partkey = t2.p_partkey";

  auto joinPlan = parseSelect(joinSql);
  auto joinVeloxPlan = planVelox(joinPlan, {.numWorkers = 4, .numDrivers = 4});
  auto joinResult = runFragmentedPlan(joinVeloxPlan);

  // Check that result count matches rows written
  EXPECT_EQ(1, joinResult.results.size());
  EXPECT_EQ(1, joinResult.results[0]->size());
  int64_t actualCount = joinResult.results[0]
                            ->childAt(0)
                            ->as<velox::SimpleVector<int64_t>>()
                            ->valueAt(0);
  EXPECT_EQ(rowsWritten, actualCount);

  // Plan matcher to be filled in later
  auto createMatcher = core::PlanMatcherBuilder()
                           .tableScan("part_b")
                           .localPartition()
                           .tableWrite()
                           .partitionedOutput()
                           .build();

  AXIOM_ASSERT_PLAN(
      createVeloxPlan.plan->fragments()[0].fragment.planNode, createMatcher);

  auto rightMatcher = core::PlanMatcherBuilder().tableScan("part_b2").build();

  auto joinMatcher = core::PlanMatcherBuilder()
                         .tableScan("part_b")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .partialAggregation({}, {"count() AS count"})
                         .partitionedOutput()
                         .build();

  AXIOM_ASSERT_PLAN(
      joinVeloxPlan.plan->fragments()[0].fragment.planNode, joinMatcher);
}

} // namespace facebook::axiom::optimizer::test
