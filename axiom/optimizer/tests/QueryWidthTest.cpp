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

#include "axiom/optimizer/tests/HiveQueriesTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

// TPC-H nation has 25 rows, region 5.
constexpr int64_t kNationRows = 25;
constexpr int64_t kRegionRows = 5;
constexpr int32_t kWorkersAvailable = 4;

// Verifies that the optimizer collapses a query onto fewer workers when its
// scans are estimated to read few enough rows.
class QueryWidthTest : public test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables(
        {velox::tpch::Table::TBL_NATION, velox::tpch::Table::TBL_REGION});
  }

  void SetUp() override {
    useV2_ = true;
    test::HiveQueriesTestBase::SetUp();
  }

  PlanAndStats plan(std::string_view sql, const OptimizerOptions& options) {
    return planVelox(
        parseSelect(sql),
        {.maxRemotePartitions = kWorkersAvailable, .maxLocalPartitions = 2},
        options);
  }

  // Options that narrow a query reading at most 'maxRawInputRows' to a single
  // worker.
  static OptimizerOptions narrowingAt(int64_t maxRawInputRows) {
    OptimizerOptions options;
    options.smallQueryMaxScanRows = maxRawInputRows;
    return options;
  }

  static int32_t maxRemotePartitions(const PlanAndStats& result) {
    return result.plan->options().maxRemotePartitions;
  }

  // Sums the raw-input estimates the optimizer recorded for the plan's scans,
  // which is what the width decision reads. nullopt when no scan reported one.
  static std::optional<uint64_t> scanRawInputRows(const PlanAndStats& result) {
    std::optional<uint64_t> total;
    for (const auto& [id, prediction] : result.prediction) {
      if (prediction.numRawInputRows.has_value()) {
        total = total.value_or(0) + *prediction.numRawInputRows;
      }
    }
    return total;
  }
};

// A query narrows when the rows its scan is estimated to read are at most the
// threshold, which is inclusive. A threshold of zero disables the decision.
// The decision follows what the query reads, so an aggregation over a scan
// behaves like the scan alone.
TEST_F(QueryWidthTest, scan) {
  for (const std::string_view sql :
       {"SELECT n_nationkey FROM nation",
        "SELECT n_regionkey, count(*) FROM nation GROUP BY 1"}) {
    SCOPED_TRACE(sql);

    {
      const auto result = plan(sql, narrowingAt(kNationRows));
      EXPECT_EQ(scanRawInputRows(result), kNationRows);
      EXPECT_EQ(maxRemotePartitions(result), 1);
    }

    {
      const auto result = plan(sql, narrowingAt(kNationRows - 1));
      EXPECT_EQ(scanRawInputRows(result), kNationRows);
      EXPECT_EQ(maxRemotePartitions(result), kWorkersAvailable);
    }

    {
      const auto result = plan(sql, narrowingAt(0));
      EXPECT_EQ(scanRawInputRows(result), kNationRows);
      EXPECT_EQ(maxRemotePartitions(result), kWorkersAvailable);
    }
  }
}

// A query reading several tables is sized by their total.
TEST_F(QueryWidthTest, join) {
  const std::string_view sql =
      "SELECT n_name, r_name FROM nation, region "
      "WHERE n_regionkey = r_regionkey";
  const int64_t bothTables = kNationRows + kRegionRows;

  {
    const auto result = plan(sql, narrowingAt(bothTables));
    EXPECT_EQ(scanRawInputRows(result), bothTables);
    EXPECT_EQ(maxRemotePartitions(result), 1);
  }

  {
    const auto result = plan(sql, narrowingAt(bothTables - 1));
    EXPECT_EQ(scanRawInputRows(result), bothTables);
    EXPECT_EQ(maxRemotePartitions(result), kWorkersAvailable);
  }
}

// Narrowing on a partial total would under-size a query by however much the
// untracked table reads, so an unknown estimate declines to narrow rather than
// counting as zero.
TEST_F(QueryWidthTest, noStats) {
  auto options = narrowingAt(1'000'000);
  options.useFilteredTableStats = false;

  const auto result = plan("SELECT n_nationkey FROM nation", options);
  EXPECT_EQ(scanRawInputRows(result), std::nullopt);
  EXPECT_EQ(maxRemotePartitions(result), kWorkersAvailable);
}

// A narrowed query gets the configured number of workers, never more than the
// caller had available.
TEST_F(QueryWidthTest, narrowWidth) {
  const std::string_view sql = "SELECT n_nationkey FROM nation";

  auto options = narrowingAt(kNationRows);
  {
    options.smallQueryNumWorkers = 2;
    EXPECT_EQ(maxRemotePartitions(plan(sql, options)), 2);
  }

  {
    options.smallQueryNumWorkers = kWorkersAvailable * 2;
    EXPECT_EQ(maxRemotePartitions(plan(sql, options)), kWorkersAvailable);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
