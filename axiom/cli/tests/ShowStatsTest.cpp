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

#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"

using namespace facebook::velox;

namespace axiom::sql {
namespace {

// Runs the SHOW STATS cases under both optimizer v1 and v2 (the bool
// parameter), which must produce identical results.
class ShowStatsTest : public SqlQueryRunnerTestBase,
                      public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useV2_ = GetParam();
    SqlQueryRunnerTestBase::SetUp();
  }
};

TEST_P(ShowStatsTest, tableAndQuery) {
  // Create a table with 100 rows and 4 columns: x has 100 distinct values,
  // y has 7 distinct values, z is an array column, w is NULL for every 4th row.
  run("CREATE TABLE t AS "
      "SELECT x, x % 7 AS y, sequence(1, x % 7 + 1) AS z, "
      "if(x % 4 <> 0, x + x % 7) AS w "
      "FROM unnest(sequence(1, 100)) AS _(x)");

  // SHOW STATS FOR <table>: reports raw connector stats.
  auto expected = makeRowVector({
      // row_count.
      makeNullableFlatVector<int64_t>(
          {100, std::nullopt, std::nullopt, std::nullopt, std::nullopt}),
      // column_name.
      makeNullableFlatVector<std::string>({std::nullopt, "x", "y", "z", "w"}),
      // nulls_fraction: 0.25 for w; 0 for the rest.
      makeNullableFlatVector<double>({std::nullopt, 0.0, 0.0, 0.0, 0.25}),
      // distinct_values_count: NULL for z (array).
      makeNullableFlatVector<int64_t>({std::nullopt, 100, 7, std::nullopt, 75}),
      // avg_length: 3 for z (array); NULL for the rest.
      makeNullableFlatVector<int64_t>(
          {std::nullopt, std::nullopt, std::nullopt, 3, std::nullopt}),
      // low_value.
      makeNullableFlatVector<std::string>(
          {std::nullopt, "1", "0", std::nullopt, "2"}),
      // high_value.
      makeNullableFlatVector<std::string>(
          {std::nullopt, "100", "6", std::nullopt, "103"}),
  });

  auto result = run("SHOW STATS FOR t");
  ASSERT_FALSE(result.message.has_value());
  ASSERT_EQ(1, result.results.size());
  test::assertEqualVectors(expected, result.results[0]);

  // SHOW STATS FOR (SELECT * FROM <table>): reports optimizer estimates. The
  // optimizer reports its NDV estimate, which is unknown for a column it
  // cannot estimate (the array z), and doesn't track avgLength.
  expected = makeRowVector({
      expected->childAt(0), // row_count.
      expected->childAt(1), // column_name.
      expected->childAt(2), // nulls_fraction.
      // distinct_values_count: NULL for z (array NDV is unknown).
      makeNullableFlatVector<int64_t>({std::nullopt, 100, 7, std::nullopt, 75}),
      // avg_length: not tracked by the optimizer.
      makeNullConstant(TypeKind::BIGINT, 5),
      expected->childAt(5), // low_value.
      expected->childAt(6), // high_value.
  });

  result = run("SHOW STATS FOR (SELECT * FROM t)");
  ASSERT_FALSE(result.message.has_value());
  ASSERT_EQ(1, result.results.size());
  test::assertEqualVectors(expected, result.results[0]);
}

// Verifies that SHOW STATS FOR (<query>) reflects optimizer estimates after
// filter pushdown: reduced row count, tightened min/max, capped NDV.
TEST_P(ShowStatsTest, queryWithFilter) {
  run("CREATE TABLE t AS "
      "SELECT x, x % 7 AS y, sequence(1, x % 7 + 1) AS z, "
      "if(x % 4 <> 0, x + x % 7) AS w "
      "FROM unnest(sequence(1, 100)) AS _(x)");

  auto expected = makeRowVector({
      // row_count: 50 (filter x > 50 keeps half the rows).
      makeNullableFlatVector<int64_t>(
          {50, std::nullopt, std::nullopt, std::nullopt, std::nullopt}),
      // column_name.
      makeNullableFlatVector<std::string>({std::nullopt, "x", "y", "z", "w"}),
      // nulls_fraction.
      makeNullableFlatVector<double>({std::nullopt, 0.0, 0.0, 0.0, 0.25}),
      // distinct_values_count: reduced for x and w; y stays at 7 because the
      // optimizer assumes column independence — the filter on x doesn't reduce
      // y's domain (and 7 < 50 filtered rows, so NDV is not capped). NULL for z
      // (array NDV is unknown).
      makeNullableFlatVector<int64_t>({std::nullopt, 50, 7, std::nullopt, 50}),
      // avg_length: not tracked by the optimizer.
      makeNullConstant(TypeKind::BIGINT, 5),
      // low_value: x tightened to 51.
      makeNullableFlatVector<std::string>(
          {std::nullopt, "51", "0", std::nullopt, "2"}),
      // high_value.
      makeNullableFlatVector<std::string>(
          {std::nullopt, "100", "6", std::nullopt, "103"}),
  });

  auto result = run("SHOW STATS FOR (SELECT * FROM t WHERE x > 50)");
  ASSERT_FALSE(result.message.has_value());
  ASSERT_EQ(1, result.results.size());
  test::assertEqualVectors(expected, result.results[0]);
}

INSTANTIATE_TEST_SUITE_P(
    SqlQueryRunner,
    ShowStatsTest,
    ::testing::Values(false, true),
    [](const ::testing::TestParamInfo<bool>& info) {
      return info.param ? "v2" : "v1";
    });

} // namespace
} // namespace axiom::sql
