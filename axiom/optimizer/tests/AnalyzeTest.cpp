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

// A subset ANALYZE replaces the row count and discards omitted columns' stats,
// both when the number of rows is unchanged and after external data changes.
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
  EXPECT_EQ(connector::hive::PersistedStats::read(path)->columns.size(), 1);
  EXPECT_FALSE(
      hiveMetadata()
          .findTable({kDefaultSchema, "test"})
          ->findColumn("s")
          ->stats());

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
  EXPECT_EQ(connector::hive::PersistedStats::read(path)->columns.size(), 1);
  EXPECT_EQ(runAnalyze("ANALYZE test WITH (columns = ARRAY[])"), 100);
  const auto rowCountOnly = connector::hive::PersistedStats::read(path);
  EXPECT_EQ(rowCountOnly->numRows, 100);
  EXPECT_TRUE(rowCountOnly->columns.empty());
  EXPECT_EQ(runAnalyze("ANALYZE test"), 100);
  EXPECT_NEAR(*storedStats(path, "s").numDistinct, 50, 2);
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
      "SELECT n_nationkey AS x, n_name AS s, n_nationkey % 3 AS pk "
      "FROM nation");

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
    EXPECT_EQ(
        connector::hive::PersistedStats::read(directory)->columns.size(), 2);
  }
}

// Missing statistics must survive reloads and later writes as unknown. Query
// both the omitted columns and columns whose statistics are still complete.
TEST_F(AnalyzeTest, omittedStatisticsStayUnknown) {
  for (const bool partitioned : {false, true}) {
    SCOPED_TRACE(partitioned);
    SCOPE_EXIT {
      hiveMetadata().dropTableIfExists("test");
    };
    runCtas(
        fmt::format(
            "CREATE TABLE test{} AS SELECT n_nationkey AS x, "
            "if(n_nationkey % 5 = 0, cast(null AS varchar), n_name) AS s{} "
            "FROM nation",
            partitioned ? " WITH (partitioned_by = ARRAY['pk'])" : "",
            partitioned ? ", n_nationkey % 3 AS pk" : ""));
    const auto count = [&](std::string_view expression,
                           std::string_view filter = "") {
      return runVelox(
                 toSingleNodePlan(
                     fmt::format("SELECT {} FROM test{}", expression, filter)))
          .getOnlyResult<int64_t>();
    };
    const auto unknownPlan = [&]() {
      EXPECT_NE(
          toSingleNodePlan("SELECT approx_non_null_count(s) FROM test")
              ->toString(true, true)
              .find("TableScan"),
          std::string::npos);
    };
    EXPECT_EQ(runAnalyze("ANALYZE test WITH (columns = ARRAY['x'])"), 25);
    hiveMetadata().reloadTableFromPath({kDefaultSchema, "test"});
    EXPECT_EQ(count("approx_non_null_count(s)"), 20);
    EXPECT_EQ(count("approx_null_count(s)"), 5);
    EXPECT_EQ(count("count(s)"), 20);
    EXPECT_EQ(count("approx_count_star()"), 25);
    unknownPlan();

    // A new partition has complete statistics; the existing partitions still
    // lack s's statistics. An unpartitioned INSERT merges into the old group.
    runVelox(parseInsert(
        fmt::format(
            "INSERT INTO test SELECT n_nationkey + 25, n_name{} FROM nation",
            partitioned ? ", 10" : "")));
    hiveMetadata().reloadTableFromPath({kDefaultSchema, "test"});
    EXPECT_EQ(count("approx_non_null_count(s)"), 45);
    EXPECT_EQ(count("approx_null_count(s)"), 5);
    EXPECT_EQ(count("count(s)"), 45);
    EXPECT_EQ(count("approx_count_star()"), 50);
    unknownPlan();
    if (partitioned) {
      EXPECT_EQ(count("approx_non_null_count(s)", " WHERE pk = 10"), 25);
      EXPECT_EQ(
          toSingleNodePlan(
              "SELECT approx_non_null_count(s) FROM test WHERE pk = 10")
              ->toString(true, true)
              .find("TableScan"),
          std::string::npos);
    } else {
      const auto stats =
          connector::hive::PersistedStats::read(localTablePath("test"));
      ASSERT_TRUE(stats);
      EXPECT_EQ(stats->unknownColumns, std::vector<std::string>{"s"});
      EXPECT_EQ(stats->columns.size(), 1);
    }

    EXPECT_EQ(runAnalyze("ANALYZE test WITH (columns = ARRAY[])"), 50);
    EXPECT_EQ(count("approx_non_null_count(x)"), 50);
    EXPECT_EQ(count("approx_non_null_count(s)"), 45);
    EXPECT_EQ(count("approx_count_star()"), 50);
    EXPECT_EQ(runAnalyze("ANALYZE test"), 50);
    EXPECT_EQ(count("approx_non_null_count(s)"), 45);
    EXPECT_EQ(
        toSingleNodePlan("SELECT approx_non_null_count(s) FROM test")
            ->toString(true, true)
            .find("TableScan"),
        std::string::npos);
  }
}

TEST_F(AnalyzeTest, omittedStatisticsOnEmptyTable) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("test");
  };
  runCtas(
      "CREATE TABLE test AS SELECT n_nationkey AS x, n_name AS s "
      "FROM nation WHERE false");
  EXPECT_EQ(runAnalyze("ANALYZE test WITH (columns = ARRAY['x'])"), 0);
  runVelox(
      parseInsert("INSERT INTO test SELECT n_nationkey, n_name FROM nation"));
  const auto stats =
      connector::hive::PersistedStats::read(localTablePath("test"));
  ASSERT_TRUE(stats);
  EXPECT_TRUE(stats->unknownColumns.empty());
  EXPECT_EQ(storedStats(localTablePath("test"), "s").numValues, 25);
  const auto plan =
      toSingleNodePlan("SELECT approx_non_null_count(s) FROM test");
  EXPECT_EQ(plan->toString(true, true).find("TableScan"), std::string::npos);
  EXPECT_EQ(runVelox(plan).getOnlyResult<int64_t>(), 25);
}

TEST_F(AnalyzeTest, nullAndEmptyStatistics) {
  for (const bool empty : {false, true}) {
    SCOPED_TRACE(empty);
    SCOPE_EXIT {
      hiveMetadata().dropTableIfExists("test");
    };
    runCtas(
        fmt::format(
            "CREATE TABLE test AS SELECT "
            "if(n_nationkey % 2 = 0, cast(null AS bigint), n_nationkey) AS x "
            "FROM nation{}",
            empty ? " WHERE false" : ""));
    EXPECT_EQ(runAnalyze("ANALYZE test"), empty ? 0 : 25);
    const auto stats = storedStats(localTablePath("test"), "x");
    EXPECT_EQ(stats.numValues, empty ? 0 : 12);
    EXPECT_FLOAT_EQ(stats.nullPct, empty ? 0 : 52);
    const auto table = hiveMetadata().findTable({kDefaultSchema, "test"});
    ASSERT_TRUE(table->findColumn("x")->stats());
    EXPECT_FLOAT_EQ(table->findColumn("x")->stats()->nullPct, empty ? 0 : 52);
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
