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

#include <filesystem>
#include <fstream>
#include "axiom/connectors/hive/LocalTableMetadata.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

// Returns the statistics of 'column' in the '.stats' file in 'directory'.
connector::ColumnStatistics storedStats(
    const std::string& directory,
    std::string_view column) {
  const auto stats = connector::hive::PersistedStats::read(directory);
  VELOX_CHECK(stats.has_value());
  for (const auto& columnStats : stats->columns) {
    if (columnStats.name == column) {
      return columnStats;
    }
  }
  VELOX_FAIL("No statistics for column: {}", column);
}

// ANALYZE of Hive tables, which replaces the statistics in their '.stats'
// files.
class AnalyzeTest : public test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables({velox::tpch::Table::TBL_NATION});
  }

  lp::LogicalPlanNodePtr parseAnalyze(std::string_view sql) {
    auto statement = prestoParser().parse(sql);
    VELOX_CHECK(statement->isAnalyze());
    return statement->as<::axiom::sql::presto::AnalyzeStatement>()->plan();
  }

  // Runs 'sql' and returns the number of rows it reports analyzing.
  int64_t runAnalyze(std::string_view sql) {
    SCOPED_TRACE(sql);
    return runVelox(parseAnalyze(sql)).getOnlyResult<int64_t>();
  }
};

// ANALYZE replaces the statistics of the columns it analyzes. The table gets
// the same rows again with new values of x and s. Writes merge distinct counts
// by taking the larger one, so the stored counts stay at those of the first
// write. ANALYZE fixes the count of x. s keeps the count the writes left.
// Then copies of the data files double the rows outside any write. ANALYZE of
// x alone scales the non-null count of s to the new row count, so s still has
// no nulls.
TEST_F(AnalyzeTest, unpartitionedTable) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("test");
  };
  runCtas(
      "CREATE TABLE test AS SELECT n_nationkey AS x, n_name AS s FROM nation");
  runVelox(parseInsert(
      "INSERT INTO test "
      "SELECT n_nationkey + 25, concat(n_name, '2') FROM nation"));
  const auto path = localTablePath("test");
  EXPECT_NEAR(*storedStats(path, "x").numDistinct, 25, 1);

  EXPECT_EQ(runAnalyze("ANALYZE test WITH (columns = ARRAY['x'])"), 50);
  EXPECT_EQ(hiveMetadata().findTable({kDefaultSchema, "test"})->numRows(), 50);
  EXPECT_EQ(connector::hive::PersistedStats::read(path)->numRows, 50);
  const auto x = storedStats(path, "x");
  EXPECT_EQ(x.numValues, 50);
  EXPECT_NEAR(*x.numDistinct, 50, 2);
  EXPECT_NEAR(*storedStats(path, "s").numDistinct, 25, 1);

  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(path)) {
    if (entry.is_regular_file() &&
        entry.path().filename().string().front() != '.') {
      files.push_back(entry.path());
    }
  }
  for (const auto& file : files) {
    std::filesystem::copy_file(
        file, file.parent_path() / ("copy_" + file.filename().string()));
  }
  hiveMetadata().reloadTableFromPath({kDefaultSchema, "test"});

  EXPECT_EQ(runAnalyze("ANALYZE test WITH (columns = ARRAY['x'])"), 100);
  EXPECT_EQ(connector::hive::PersistedStats::read(path)->numRows, 100);
  EXPECT_EQ(storedStats(path, "x").numValues, 100);
  const auto s = storedStats(path, "s");
  EXPECT_EQ(s.numValues, 100);
  EXPECT_NEAR(*s.numDistinct, 25, 1);
}

// A partitioned table keeps statistics per partition, so ANALYZE groups them by
// the partition column. It reads that column even when the column list leaves
// it out. Each partition's row count is replaced, not added to.
TEST_F(AnalyzeTest, partitionedTable) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("test");
  };
  runCtas(
      "CREATE TABLE test WITH (partitioned_by = ARRAY['pk']) AS "
      "SELECT n_nationkey AS x, n_nationkey % 3 AS pk FROM nation");

  EXPECT_EQ(runAnalyze("ANALYZE test WITH (columns = ARRAY['x'])"), 25);
  EXPECT_EQ(hiveMetadata().findTable({kDefaultSchema, "test"})->numRows(), 25);
  // Keys 0 to 24 split 9, 8 and 8 over pk 0, 1 and 2.
  for (const auto& [pk, numRows] :
       std::vector<std::pair<int32_t, int64_t>>{{0, 9}, {1, 8}, {2, 8}}) {
    SCOPED_TRACE(fmt::format("pk={}", pk));
    const auto directory = fmt::format("{}/pk={}", localTablePath("test"), pk);
    EXPECT_EQ(
        connector::hive::PersistedStats::read(directory)->numRows, numRows);
    const auto x = storedStats(directory, "x");
    EXPECT_EQ(x.numValues, numRows);
    EXPECT_EQ(x.numDistinct, numRows);
    EXPECT_EQ(storedStats(directory, "pk").numDistinct, 1);
  }
}

// An ANALYZE that does not run, as under EXPLAIN, aborts and leaves the
// statistics as they were. So does one whose query fails.
TEST_F(AnalyzeTest, abort) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("test");
  };
  runCtas("CREATE TABLE test AS SELECT n_nationkey AS x FROM nation");
  const auto path = localTablePath("test");

  auto plan = planVelox(parseAnalyze("ANALYZE test"));
  EXPECT_NO_THROW(std::move(plan.finishWrite).abort().get());
  EXPECT_EQ(connector::hive::PersistedStats::read(path)->numRows, 25);

  // The reader cannot read this file, so the query fails.
  std::ofstream(path + "/corrupt") << "not a data file";
  hiveMetadata().reloadTableFromPath({kDefaultSchema, "test"});
  EXPECT_THROW(runAnalyze("ANALYZE test"), VeloxException);
  EXPECT_EQ(connector::hive::PersistedStats::read(path)->numRows, 25);
}

} // namespace
} // namespace facebook::axiom::optimizer
