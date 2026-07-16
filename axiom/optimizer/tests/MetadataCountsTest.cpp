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

#include <gmock/gmock.h>

#include "axiom/optimizer/tests/HiveQueriesTestBase.h"

using namespace facebook::velox;

namespace facebook::axiom::optimizer {
namespace {

// Tests the metadata aggregates (approx_count_star, approx_null_count,
// approx_non_null_count) in the v2 optimizer. A query whose grouping and
// filters are answerable from partition metadata folds to a Values node holding
// the exact counts, so no data is read; anything else falls back to reading the
// data. Result correctness under execution is covered by
// sql/metadataAggregate.sql.
//
// Table t: column a is null exactly in partition k=0; partitions k = 0/1/2 have
// 9/8/8 rows (25 total), so a has 9 nulls and 16 non-nulls.
class MetadataCountsTest : public test::HiveQueriesTestBase {
 protected:
  MetadataCountsTest() {
    useV2_ = true;
  }

  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables({velox::tpch::Table::TBL_NATION});
  }

  void SetUp() override {
    test::HiveQueriesTestBase::SetUp();

    runCtas(
        "CREATE TABLE t WITH (partitioned_by = ARRAY['k']) AS "
        "SELECT IF(n_nationkey % 3 > 0, n_nationkey) AS a, n_nationkey % 3 AS k "
        "FROM nation");
  }

  void TearDown() override {
    hiveMetadata().dropTableIfExists("t");
    test::HiveQueriesTestBase::TearDown();
  }
};

// --- Answered from partition metadata: folds to a constant, reads no data. ---

TEST_F(MetadataCountsTest, rowCount) {
  auto makeExpected = [&](int64_t count) {
    return makeRowVector({makeFlatVector<int64_t>({count})});
  };

  {
    auto plan = toSingleNodePlan("SELECT approx_count_star() FROM t");
    AXIOM_ASSERT_PLAN(plan, matchValues(makeExpected(25)).build());
    EXPECT_THAT(
        plan->outputType()->names(),
        ::testing::ElementsAre("approx_count_star"));
  }

  {
    auto plan = toSingleNodePlan(
        "SELECT k, approx_count_star() as x FROM t GROUP BY 1");
    AXIOM_ASSERT_PLAN(
        plan,
        matchValues(makeRowVector({
                        makeFlatVector<int64_t>({0, 1, 2}),
                        makeFlatVector<int64_t>({9, 8, 8}),
                    }))
            .build());
    EXPECT_THAT(plan->outputType()->names(), ::testing::ElementsAre("k", "x"));
  }

  // Unpartitioned table: a single whole-table count.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan("SELECT approx_count_star() FROM nation"),
      matchValues(makeExpected(25)).build());

  // A filter on a partition column is pushed into the scan and answered from
  // metadata; only the matching partition is counted.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan("SELECT approx_count_star() FROM t WHERE k = 1"),
      matchValues(makeExpected(8)).build());

  // A filter matching no partition yields one row: 0.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan("SELECT approx_count_star() FROM t WHERE k = 9"),
      matchValues(makeExpected(0)).build());
}

TEST_F(MetadataCountsTest, nullCounts) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan("SELECT k, approx_null_count(a) FROM t GROUP BY 1"),
      matchValues(makeRowVector({
                      makeFlatVector<int64_t>({0, 1, 2}),
                      makeFlatVector<int64_t>({9, 0, 0}),
                  }))
          .build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan("SELECT k, approx_non_null_count(a) FROM t GROUP BY 1"),
      matchValues(makeRowVector({
                      makeFlatVector<int64_t>({0, 1, 2}),
                      makeFlatVector<int64_t>({0, 8, 8}),
                  }))
          .build());
}

TEST_F(MetadataCountsTest, multipleAndDerivedAggregates) {
  // Several metadata aggregates fold together into a single row.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          "SELECT approx_count_star(), approx_null_count(a), "
          "approx_non_null_count(a) FROM t"),
      matchValues(makeRowVector({
                      makeFlatVector<int64_t>({25}),
                      makeFlatVector<int64_t>({9}),
                      makeFlatVector<int64_t>({16}),
                  }))
          .build());

  // An expression over metadata aggregates folds, with the arithmetic left as a
  // projection over the folded counts.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          "SELECT approx_count_star() - approx_null_count(a) FROM t"),
      matchValues(makeRowVector({
                      makeFlatVector<int64_t>({25}),
                      makeFlatVector<int64_t>({9}),
                  }))
          .project({"c0 - c1"})
          .build());
}

// --- Not answerable from metadata: falls back to reading the data. ---

TEST_F(MetadataCountsTest, fallsBack) {
  // Grouping by a data column.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan("SELECT a, approx_count_star() FROM t GROUP BY a"),
      matchHiveScan("t").singleAggregation({"a"}, {"count(*)"}).build());

  // Filter on a data column.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan("SELECT approx_count_star() FROM t WHERE a > 5"),
      matchHiveScan("t").singleAggregation({}, {"count(*)"}).build());

  // A regular aggregate alongside a metadata aggregate.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan("SELECT approx_count_star(), sum(a) FROM t"),
      matchHiveScan("t").singleAggregation({}, {"count(*)", "sum(a)"}).build());
}

TEST_F(MetadataCountsTest, unionAll) {
  // One leg is answered from metadata (folds to a constant), the other reads
  // the data.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          "SELECT approx_count_star() as x FROM t UNION ALL SELECT sum(a) FROM t"),
      matchValues(makeRowVector({makeFlatVector<int64_t>({25})}))
          .project()
          .localPartition({
              matchHiveScan("t")
                  .aliases({"a"})
                  .singleAggregation({}, {"sum(a)"})
                  .project()
                  .build(),
          })
          .build());
}

} // namespace
} // namespace facebook::axiom::optimizer
