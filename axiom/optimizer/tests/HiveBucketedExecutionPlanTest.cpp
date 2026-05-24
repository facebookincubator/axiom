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

#include <gtest/gtest.h>
#include <re2/re2.h>
#include "axiom/connectors/hive/HiveConnectorMetadata.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class HiveBucketedExecutionTest : public test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables({velox::tpch::Table::TBL_CUSTOMER});
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();
  }

  void TearDown() override {
    for (const auto& name : tablesToDrop_) {
      hiveMetadata().dropTableIfExists({kDefaultSchema, name});
    }
    tablesToDrop_.clear();
    HiveQueriesTestBase::TearDown();
  }

  void createBucketedTable(const std::string& name, const std::string& sql) {
    tablesToDrop_.push_back(name);
    runCtas(sql);
    verifyBucketed(name, parseBucketCount(sql));
  }

  void verifyBucketed(const std::string& name, int32_t expectedBuckets) {
    auto table = hiveMetadata().findTable({kDefaultSchema, name});
    ASSERT_NE(table, nullptr);
    ASSERT_FALSE(table->layouts().empty());
    const auto* layout =
        table->layouts().at(0)->as<connector::hive::HiveTableLayout>();
    ASSERT_NE(layout, nullptr);
    const auto partitionType = layout->partitionType();
    ASSERT_NE(partitionType, nullptr);
    EXPECT_EQ(partitionType->numPartitions(), expectedBuckets);
  }

  static int32_t parseBucketCount(const std::string& sql) {
    static const re2::RE2 kPattern(R"(bucket_count\s*=\s*(\d+))");
    int32_t count;
    VELOX_CHECK(re2::RE2::PartialMatch(sql, kPattern, &count));
    return count;
  }

  void assertBucketedExecution(const lp::LogicalPlanNodePtr& logicalPlan) {
    // Single-node execution produces an unbucketed reference; compare
    // against the multi-worker bucketed run.
    auto referencePlan = planVelox(
        logicalPlan,
        MultiFragmentPlan::Options::singleNode(),
        optimizerOptions_);
    auto reference = runFragmentedPlan(referencePlan).results;
    ASSERT_GT(reference.size(), 0);
    checkSame(logicalPlan, reference);
  }

  std::vector<std::string> tablesToDrop_;
};

TEST_F(HiveBucketedExecutionTest, join) {
  createBucketedTable(
      "j_left",
      "CREATE TABLE j_left "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "j_right",
      "CREATE TABLE j_right "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT DISTINCT c_nationkey, "
      "cast(c_nationkey as varchar) as label FROM customer");

  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM j_left JOIN j_right "
        "ON j_left.c_nationkey = j_right.c_nationkey");
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("j_left")
            .hashJoin(matchHiveScan("j_right").build(), core::JoinType::kInner)
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .project()
            .build());
    assertBucketedExecution(logicalPlan);
  }

  for (std::string_view sql : {
           "SELECT * FROM j_left RIGHT JOIN j_right "
           "ON j_left.c_nationkey = j_right.c_nationkey",
           "SELECT * FROM j_left FULL OUTER JOIN j_right "
           "ON j_left.c_nationkey = j_right.c_nationkey",
       }) {
    SCOPED_TRACE(sql);
    auto logicalPlan = parseSelect(sql);
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    bool foundBucketed = std::any_of(
        plan.plan->fragments().begin(),
        plan.plan->fragments().end(),
        [](const auto& f) { return !f.groupedNodes.empty(); });
    EXPECT_TRUE(foundBucketed);
    assertBucketedExecution(logicalPlan);
  }

  // LEFT join: at least one fragment must be bucketed.
  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM j_left LEFT JOIN j_right "
        "ON j_left.c_nationkey = j_right.c_nationkey");
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    bool foundBucketed = std::any_of(
        plan.plan->fragments().begin(),
        plan.plan->fragments().end(),
        [](const auto& f) { return !f.groupedNodes.empty(); });
    EXPECT_TRUE(foundBucketed);
    assertBucketedExecution(logicalPlan);
  }

  tablesToDrop_.emplace_back("j_unbucketed");
  runCtas(
      "CREATE TABLE j_unbucketed AS "
      "SELECT DISTINCT c_nationkey, "
      "cast(c_nationkey as varchar) as label FROM customer");
  auto logicalPlan = parseSelect(
      "SELECT * FROM j_left JOIN j_unbucketed "
      "ON j_left.c_nationkey = j_unbucketed.c_nationkey");
  auto plan = planVelox(
      logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
  int32_t bucketedScans = 0;
  int32_t hashExchanges = 0;
  for (const auto& fragment : plan.plan->fragments()) {
    for (const auto& [_, partitionType] : fragment.groupedNodes) {
      if (partitionType == nullptr) {
        ++hashExchanges;
      } else {
        ++bucketedScans;
      }
    }
  }
  EXPECT_EQ(bucketedScans, 1);
  EXPECT_EQ(hashExchanges, 1);
  assertBucketedExecution(logicalPlan);
}

TEST_F(HiveBucketedExecutionTest, semijoin) {
  createBucketedTable(
      "sj_left",
      "CREATE TABLE sj_left "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "sj_right",
      "CREATE TABLE sj_right "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT DISTINCT c_nationkey FROM customer WHERE c_nationkey < 5");

  {
    auto logicalPlan = parseSelect(
        "SELECT c_custkey, c_nationkey FROM sj_left "
        "WHERE c_nationkey IN (SELECT c_nationkey FROM sj_right)");
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("sj_left")
            .hashJoin(
                matchHiveScan("sj_right").build(),
                core::JoinType::kLeftSemiFilter)
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
    assertBucketedExecution(logicalPlan);
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT c_custkey, c_nationkey FROM sj_left "
        "WHERE c_nationkey NOT IN (SELECT c_nationkey FROM sj_right)");
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    bool foundBucketed = std::any_of(
        plan.plan->fragments().begin(),
        plan.plan->fragments().end(),
        [](const auto& f) { return !f.groupedNodes.empty(); });
    EXPECT_TRUE(foundBucketed);
    assertBucketedExecution(logicalPlan);
  }
}

TEST_F(HiveBucketedExecutionTest, aggregation) {
  createBucketedTable(
      "a_customers",
      "CREATE TABLE a_customers "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT * FROM customer");

  {
    auto logicalPlan = parseSelect(
        "SELECT c_nationkey, count(*) FROM a_customers GROUP BY c_nationkey");
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("a_customers")
            .localPartition({"c_nationkey"})
            .singleAggregation({"c_nationkey"}, {"count(*)"})
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
    assertBucketedExecution(logicalPlan);
  }

  for (std::string_view sql : {
           "SELECT c_nationkey, c_mktsegment, count(*) FROM a_customers "
           "GROUP BY c_nationkey, c_mktsegment",
           "SELECT c_nationkey, count(*), min(c_acctbal), max(c_acctbal) "
           "FROM a_customers GROUP BY c_nationkey",
           "SELECT c_nationkey, count(DISTINCT c_mktsegment) FROM a_customers "
           "GROUP BY c_nationkey",
           "SELECT c_nationkey, count(*) AS cnt FROM a_customers "
           "GROUP BY c_nationkey HAVING count(*) > 100",
       }) {
    SCOPED_TRACE(sql);
    auto logicalPlan = parseSelect(sql);
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    bool foundBucketed = std::any_of(
        plan.plan->fragments().begin(),
        plan.plan->fragments().end(),
        [](const auto& f) { return !f.groupedNodes.empty(); });
    EXPECT_TRUE(foundBucketed);
    assertBucketedExecution(logicalPlan);
  }

  // Non-bucket grouping key falls through to partial+final.
  {
    auto logicalPlan = parseSelect(
        "SELECT c_mktsegment, count(*) FROM a_customers GROUP BY c_mktsegment");
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    bool foundBucketedProducer = false;
    for (size_t i = 0; i < plan.plan->fragments().size(); ++i) {
      if (!plan.plan->fragments()[i].groupedNodes.empty()) {
        foundBucketedProducer = true;
      } else if (i > 0) {
        EXPECT_TRUE(plan.plan->fragments()[i].groupedNodes.empty());
      }
    }
    EXPECT_TRUE(foundBucketedProducer);
    assertBucketedExecution(logicalPlan);
  }
}

TEST_F(HiveBucketedExecutionTest, select) {
  createBucketedTable(
      "s_one",
      "CREATE TABLE s_one "
      "WITH (bucket_count = 1, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT * FROM customer");
  {
    auto logicalPlan = parseSelect(
        "SELECT c_nationkey, count(*) FROM s_one GROUP BY c_nationkey");
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("s_one")
            .localPartition({"c_nationkey"})
            .singleAggregation({"c_nationkey"}, {"count(*)"})
            .bucketed()
            .fragmentWidth(1)
            .gather()
            .build());
    assertBucketedExecution(logicalPlan);
  }

  // gcd(16, 8) = 8.
  createBucketedTable(
      "s_match",
      "CREATE TABLE s_match "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "s_down",
      "CREATE TABLE s_down "
      "WITH (bucket_count = 8, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT DISTINCT c_nationkey, "
      "cast(c_nationkey as varchar) as label FROM customer");
  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM s_match JOIN s_down "
        "ON s_match.c_nationkey = s_down.c_nationkey");
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    bool foundScaledFragment = false;
    for (const auto& fragment : plan.plan->fragments()) {
      if (!fragment.groupedNodes.empty() && fragment.width.has_value() &&
          *fragment.width == 4) {
        foundScaledFragment = true;
      }
    }
    EXPECT_TRUE(foundScaledFragment);
    assertBucketedExecution(logicalPlan);
  }

  createBucketedTable(
      "s_composite",
      "CREATE TABLE s_composite "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey', 'c_mktsegment']) "
      "AS SELECT * FROM customer");
  {
    auto logicalPlan = parseSelect(
        "SELECT c_nationkey, c_mktsegment, count(*) FROM s_composite "
        "GROUP BY c_nationkey, c_mktsegment");
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("s_composite")
            .localPartition({"c_nationkey", "c_mktsegment"})
            .singleAggregation({"c_nationkey", "c_mktsegment"}, {"count(*)"})
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
    assertBucketedExecution(logicalPlan);
  }

  // Strict subset of bucket keys.
  {
    auto logicalPlan = parseSelect(
        "SELECT c_nationkey, count(*) FROM s_composite "
        "GROUP BY c_nationkey");
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    ASSERT_GT(plan.plan->fragments().size(), 1);
    assertBucketedExecution(logicalPlan);
  }
}

TEST_F(HiveBucketedExecutionTest, unionall) {
  // Different bucket keys (same type) — must not co-fragment.
  createBucketedTable(
      "u_diff_a",
      "CREATE TABLE u_diff_a "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT c_nationkey, c_custkey FROM customer");
  createBucketedTable(
      "u_diff_b",
      "CREATE TABLE u_diff_b "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_custkey']) "
      "AS SELECT c_nationkey, c_custkey FROM customer");

  {
    auto logicalPlan = parseSelect(
        "SELECT c_nationkey, count(*) FROM ("
        "  SELECT c_nationkey FROM u_diff_a"
        "  UNION ALL"
        "  SELECT c_custkey AS c_nationkey FROM u_diff_b"
        ") GROUP BY c_nationkey");
    auto plan = planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
    ASSERT_GT(plan.plan->fragments().size(), 1);
    assertBucketedExecution(logicalPlan);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
