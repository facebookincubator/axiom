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

#include <folly/ScopeGuard.h>
#include "axiom/connectors/hive/HiveMetadataConfig.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

// Runs the constant-fold cases under both the v1 and v2 optimizers. The fold
// lists a table's discrete-predicate (e.g. partition) values and aggregates
// them instead of reading the table, whether the aggregation is the body of a
// scalar subquery or stands on its own.
class SubqueryFoldTest : public test::HiveQueriesTestBase,
                         public testing::WithParamInterface<bool> {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables({velox::tpch::Table::TBL_NATION});
  }

  void SetUp() override {
    test::HiveQueriesTestBase::SetUp();
    useV2_ = GetParam();
  }

  void TearDown() override {
    hiveMetadata().dropTableIfExists("pt");
    test::HiveQueriesTestBase::TearDown();
  }

  // Creates the Hive table 'pt' with partition keys 'ds' and 'k' (ds='1' maps
  // to k=1, other ds to k=0) and data column 'x'. Three partitions:
  // (ds='0',k=0), (ds='1',k=1), (ds='2',k=0).
  void createPartitionedTable() {
    runCtas(
        "CREATE TABLE pt WITH (partitioned_by = ARRAY['ds', 'k']) AS "
        "SELECT "
        "     n_nationkey AS x, "
        "     CAST(n_nationkey % 3 AS VARCHAR) AS ds, "
        "     CAST(IF(n_nationkey % 3 = 1, 1, 0) AS INTEGER) AS k "
        "FROM nation");
  }
};

// Folds a scalar aggregate over a TestConnector table's discrete-predicate
// columns, and declines when a filter references a non-discrete column.
TEST_P(SubqueryFoldTest, foldable) {
  testConnector_->addTable("t", ROW({"a", "ds"}, {INTEGER(), VARCHAR()}))
      ->setDiscreteValues(
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

  auto matchFilter = [&](const std::string& filter,
                         const std::vector<std::string>& projections = {}) {
    return matchScan("t")
        .filter(filter)
        .projectIf(useV2_ && !projections.empty(), projections)
        .build();
  };

  {
    auto logicalPlan =
        parseSql("SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t)");

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan, matchFilter("ds = '2025-11-03'", {"a", "'2025-11-03'"}));
  }

  {
    auto logicalPlan =
        parseSql("SELECT * FROM t WHERE ds = (SELECT min(ds) FROM t)");

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan, matchFilter("ds = '2025-10-29'", {"a", "'2025-10-29'"}));
  }

  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t WHERE ds < '2025-11-02')");

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan, matchFilter("ds = '2025-11-01'", {"a", "'2025-11-01'"}));
  }

  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t WHERE ds like '%-10-%')");

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan, matchFilter("ds = '2025-10-31'", {"a", "'2025-10-31'"}));
  }

  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t WHERE ds < '2025-01-01')");

    // The subquery is empty, so max(ds) folds to null and `ds = null` is always
    // null. v2 proves the predicate unsatisfiable and prunes to an empty
    // relation; v1 keeps a constant-null filter over the scan.
    auto matcher = useV2_ ? matchValues().build() : matchFilter("null");
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  // IN list with a foldable subquery and a literal. The subquery is
  // constant-folded, resulting in a regular IN with two constants.
  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds IN ((SELECT max(ds) FROM t), '2025-10-29')");

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matchFilter("ds in ('2025-11-03', '2025-10-29')"));
  }

  // IN list with two foldable subqueries (prod query pattern). Both subqueries
  // are constant-folded, resulting in a regular IN with two constants.
  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds IN "
        "((SELECT max(ds) FROM t), (SELECT min(ds) FROM t))");

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matchFilter("ds in ('2025-11-03', '2025-10-29')"));
  }

  // A subquery over a one-row VALUES is that row's value.
  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE a = (SELECT y FROM (VALUES 1) AS v(y))");
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN_V2(plan, matchFilter("a = 1", {"1", "ds"}));
  }

  // A subquery over a multi-row VALUES cannot be a scalar and is rejected.
  if (useV2_) {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE a = (SELECT y FROM (VALUES 1, 2, 3) AS v(y))");
    VELOX_ASSERT_THROW(
        toSingleNodePlan(logicalPlan),
        "Scalar subquery produced more than one row");
  }

  // A filter on the non-discrete column 'a' prevents the fold: the subquery is
  // evaluated normally rather than by listing discrete values.
  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t WHERE a > 5)");
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = matchScan("t")
                       .hashJoinInner(matchScan("t")
                                          .aliases({"ds", "a"})
                                          .filter("a > 5")
                                          .projectIf(useV2_, {"ds"})
                                          .aggregation())
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// Folds a scalar max() over Hive partition metadata: narrowing by a filter on
// the aggregated partition key or on another partition key, declining when a
// non-partition column is filtered, and enforcing the max-partitions session
// property.
TEST_P(SubqueryFoldTest, foldableHivePartitions) {
  createPartitionedTable();

  // max(ds) folds to the greatest partition value.
  {
    auto plan = toSingleNodePlan(
        "SELECT x FROM pt WHERE ds = (SELECT max(ds) FROM pt)");
    AXIOM_ASSERT_PLAN(plan, matchHiveScan("pt", test::eq("ds", "2")).build());
  }

  // A filter on the aggregated partition key narrows the listing.
  {
    auto plan = toSingleNodePlan(
        "SELECT x FROM pt WHERE ds = (SELECT max(ds) FROM pt WHERE ds < '2')");
    AXIOM_ASSERT_PLAN(plan, matchHiveScan("pt", test::eq("ds", "1")).build());
  }

  // A filter on a different partition key narrows the listing: only ds='1' has
  // k > 0, so max(ds) folds to '1'.
  {
    auto plan = toSingleNodePlan(
        "SELECT x FROM pt WHERE ds = (SELECT max(ds) FROM pt WHERE k > 0)");
    AXIOM_ASSERT_PLAN(plan, matchHiveScan("pt", test::eq("ds", "1")).build());
  }

  // A filter on the non-partition column 'x' prevents the fold: the subquery is
  // evaluated normally rather than by listing partitions.
  {
    auto plan = toSingleNodePlan(
        "SELECT x FROM pt WHERE ds = (SELECT max(ds) FROM pt WHERE x > 0)");
    auto matcher =
        matchHiveScan("pt")
            .hashJoinInner(
                matchHiveScan("pt", test::gt("x", int64_t{0})).aggregation())
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // The max-partitions session property bounds the listing: the un-narrowed
  // listing (3 partitions) exceeds a limit of 2 and fails, while a partition
  // filter narrows it under the limit and still folds.
  {
    setHiveConnectorSession(
        connector::hive::HiveMetadataConfig::
            kMaxPartitionsPerDiscretePredicates,
        "2");
    SCOPE_EXIT {
      clearConnectorSession();
    };

    VELOX_ASSERT_THROW(
        toSingleNodePlan(
            "SELECT x FROM pt WHERE ds = (SELECT max(ds) FROM pt)"),
        "(3 vs. 2) Discrete-predicate listing exceeds the max-partitions "
        "limit");

    auto plan = toSingleNodePlan(
        "SELECT x FROM pt WHERE ds = (SELECT max(ds) FROM pt WHERE ds < '2')");
    AXIOM_ASSERT_PLAN(plan, matchHiveScan("pt", test::eq("ds", "1")).build());
  }
}

// An uncorrelated IN subquery over partition columns becomes an IN list of the
// partition values its filters select, which prunes the outer scan.
TEST_P(SubqueryFoldTest, inSubqueryOverPartitions) {
  if (!useV2_) {
    return;
  }

  createPartitionedTable();

  // Several partitions give an IN list.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          "SELECT x FROM pt WHERE ds IN (SELECT ds FROM pt WHERE k = 0)"),
      matchHiveScan("pt", test::in("ds", {"0", "2"})).build());

  // One partition gives an equality.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          "SELECT x FROM pt WHERE ds IN (SELECT ds FROM pt WHERE k > 0)"),
      matchHiveScan("pt", test::eq("ds", "1")).build());

  // No partition gives no rows.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          "SELECT x FROM pt WHERE ds IN (SELECT ds FROM pt WHERE k > 1)"),
      core::PlanMatcherBuilder().values().build());

  // A filter on the non-partition column 'x' prevents the fold: the subquery is
  // evaluated normally rather than by listing partitions.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          "SELECT x FROM pt WHERE ds IN (SELECT ds FROM pt WHERE x > 0)"),
      matchHiveScan("pt")
          .hashJoin(
              matchHiveScan("pt", test::gt("x", int64_t{0})),
              core::JoinType::kLeftSemiFilter)
          .build());
}

// A global aggregation reading only partition columns is answered from the
// partition listing, with no scan. Aggregates that are sensitive to duplicate
// inputs, and columns that are not partition keys, are not.
TEST_P(SubqueryFoldTest, foldableAggregationOverPartitions) {
  createPartitionedTable();

  {
    auto plan = toSingleNodePlan("SELECT max(ds) FROM pt");
    AXIOM_ASSERT_PLAN_V2(plan, matchValues<std::string>({{"2"}}).build());
  }

  // Every aggregate is answered from the one listing.
  {
    auto plan = toSingleNodePlan("SELECT max(ds), min(ds) FROM pt");
    AXIOM_ASSERT_PLAN_V2(plan, matchValues<std::string>({{"2", "0"}}).build());
  }

  // A grouping-only aggregation is answered from the distinct partition
  // values.
  {
    auto plan = toSingleNodePlan("SELECT DISTINCT ds FROM pt");
    AXIOM_ASSERT_PLAN_V2(
        plan, matchValues<std::string>({{"0"}, {"1"}, {"2"}}).build());
  }

  // Grouping removes duplicate values from different partitions.
  {
    auto plan = toSingleNodePlan("SELECT DISTINCT k FROM pt");
    AXIOM_ASSERT_PLAN_V2(plan, matchValues<int32_t>({{0}, {1}}).build());
  }

  // A filter on one partition column restricts the distinct values of
  // another partition column.
  {
    auto plan = toSingleNodePlan("SELECT DISTINCT ds FROM pt WHERE k = 0");
    AXIOM_ASSERT_PLAN_V2(
        plan, matchValues<std::string>({{"0"}, {"2"}}).build());
  }

  // DISTINCT returns no rows when no partition value matches the filter.
  {
    auto plan = toSingleNodePlan("SELECT DISTINCT ds FROM pt WHERE ds > '2'");
    AXIOM_ASSERT_PLAN_V2(
        plan,
        matchValues(makeRowVector({makeFlatVector<std::string>({})})).build());
  }

  // Grouping and a duplicate-insensitive aggregate are answered together
  // from the partition listing.
  {
    auto plan = toSingleNodePlan("SELECT k, max(ds) FROM pt GROUP BY k");
    AXIOM_ASSERT_PLAN_V2(
        plan,
        matchValues(makeRowVector(
                        {makeFlatVector<int32_t>({0, 1}),
                         makeFlatVector<std::string>({"2", "1"})}))
            .build());
  }

  // Each UNION ALL branch folds on its own, and the value both branches share
  // is computed once above the union.
  {
    auto plan = toSingleNodePlan(
        "SELECT max(ds) AS m FROM pt UNION ALL SELECT max(ds) AS m FROM pt");

    auto emptyRow = makeRowVector(ROW({}), 1);
    AXIOM_ASSERT_PLAN_V2(
        plan,
        matchValues(emptyRow)
            .localPartition({matchValues(emptyRow)})
            .project({"'2' as m"})
            .build());
  }

  // count() counts rows, not partitions, so the listing cannot answer it.
  {
    auto plan = toSingleNodePlan("SELECT count(*) FROM pt");
    AXIOM_ASSERT_PLAN_V2(
        plan,
        matchHiveScan("pt").singleAggregation({}, {"count(*) as c"}).build());
  }

  // An aggregation whose value is never read emits its single row without
  // reading the table.
  {
    auto plan =
        toSingleNodePlan("SELECT count(*) FROM (SELECT max(ds) FROM pt)");
    AXIOM_ASSERT_PLAN_V2(
        plan,
        matchValues(velox::ROW({}, {}))
            .singleAggregation({}, {"count(*) as c"})
            .build());
  }

  // An aggregate over a constant reads no column, so the listing holds nothing
  // that answers it.
  {
    auto plan = toSingleNodePlan("SELECT max(1) FROM pt");
    AXIOM_ASSERT_PLAN_V2(
        plan,
        matchHiveScan("pt").singleAggregation({}, {"max(1) as m"}).build());
  }

  // 'x' is not a partition key, so the listing does not hold its values.
  {
    auto plan = toSingleNodePlan("SELECT max(x) FROM pt");
    AXIOM_ASSERT_PLAN_V2(
        plan,
        matchHiveScan("pt").singleAggregation({}, {"max(x) as m"}).build());
  }
}

AXIOM_INSTANTIATE_V1_V2(SubqueryFoldTest);

} // namespace
} // namespace facebook::axiom::optimizer
