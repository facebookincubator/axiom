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
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

// Deletes against Hive, which removes the rows by dropping whole partitions.
class DeleteTest : public test::HiveQueriesTestBase {
 protected:
  const std::string kDefaultSchema{
      connector::hive::LocalHiveConnectorMetadata::kDefaultSchema};

  void SetUp() override {
    test::HiveQueriesTestBase::SetUp();
    useV2_ = true;
  }

  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables({velox::tpch::Table::TBL_NATION});
  }

  lp::LogicalPlanNodePtr parseDelete(std::string_view sql) {
    auto statement = prestoParser().parse(sql);
    VELOX_CHECK(statement->isDelete());
    return statement->as<::axiom::sql::presto::DeleteStatement>()->plan();
  }

  // Runs 'sql' and returns the number of rows it reports removing.
  int64_t runDelete(std::string_view sql) {
    SCOPED_TRACE(sql);
    return runVelox(parseDelete(sql)).getOnlyResult<int64_t>();
  }

  // Returns the number of rows selected by 'fromClause', e.g. "FROM test
  // WHERE pk = 1".
  int64_t runCount(std::string_view fromClause) {
    const auto sql = fmt::format("SELECT count(*) {}", fromClause);
    SCOPED_TRACE(sql);
    return runVelox(parseSelect(sql)).getOnlyResult<int64_t>();
  }
};

// Deletes against one table partitioned by two columns: a predicate on both
// partition columns, then on one, then none at all. Creating a Hive table is
// expensive, so these share a table.
TEST_F(DeleteTest, partitionPredicates) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("test");
  };

  runCtas(
      "CREATE TABLE test WITH (partitioned_by = ARRAY['pk', 'qk']) AS "
      "SELECT n_nationkey, n_nationkey % 3 AS pk, n_nationkey % 2 AS qk "
      "FROM nation");

  // A predicate no partition satisfies removes nothing.
  EXPECT_EQ(0, runDelete("DELETE FROM test WHERE pk IN (7, 8)"));
  EXPECT_EQ(25, runCount("FROM test"));

  // Both partition columns: one leaf partition.
  EXPECT_EQ(4, runDelete("DELETE FROM test WHERE pk = 1 AND qk = 0"));
  EXPECT_EQ(21, runCount("FROM test"));

  // The outer column alone: every partition remaining under it.
  EXPECT_EQ(4, runDelete("DELETE FROM test WHERE pk = 1"));
  EXPECT_EQ(17, runCount("FROM test"));
  EXPECT_EQ(0, runCount("FROM test WHERE pk = 1"));

  VELOX_ASSERT_USER_THROW(
      runDelete("DELETE FROM test WHERE n_nationkey = 1"),
      "DELETE supports only filters on partition columns: n_nationkey");

  // A predicate the connector cannot reduce to a partition filter, even though
  // it reads only a partition column.
  VELOX_ASSERT_USER_THROW(
      runDelete("DELETE FROM test WHERE pk % 2 = 0"),
      "DELETE supports only range filters on partition columns");

  // Deleting from a table other than the one scanned fails. SQL cannot express
  // this, so build the plan directly.
  lp::PlanBuilder::Context context{
      std::string(velox::exec::test::kHiveConnectorId), kDefaultSchema};
  VELOX_ASSERT_USER_THROW(
      runVelox(
          lp::PlanBuilder(context)
              .tableScan("nation")
              .tableDelete("test")
              .build()),
      R"(DELETE scans the wrong table: deletes "default"."test", scans "default"."nation")");

  // No predicate: every partition.
  EXPECT_EQ(17, runDelete("DELETE FROM test"));
  EXPECT_EQ(0, runCount("FROM test"));
}

// Deleting one partition of a bucketed table removes that partition's rows and
// leaves the rest.
TEST_F(DeleteTest, bucketedTable) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("test");
  };

  runCtas(
      "CREATE TABLE test WITH (bucket_count = 8, "
      "bucketed_by = ARRAY['n_nationkey'], partitioned_by = ARRAY['pk']) AS "
      "SELECT n_nationkey, n_nationkey % 3 AS pk FROM nation");

  EXPECT_EQ(9, runDelete("DELETE FROM test WHERE pk = 0"));
  EXPECT_EQ(16, runCount("FROM test"));
}

// An unpartitioned table has no partitions to drop, so a delete removes the
// data files and leaves an empty table rather than dropping it.
TEST_F(DeleteTest, unpartitionedTable) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("test");
  };

  runCtas("CREATE TABLE test AS SELECT n_nationkey, n_name FROM nation");

  EXPECT_EQ(25, runDelete("DELETE FROM test"));
  EXPECT_EQ(0, runCount("FROM test"));

  // With the stats gone there is nothing left to count the removed rows from.
  EXPECT_TRUE(
      runVelox(parseDelete("DELETE FROM test")).getOnlyResult().isNull());
  EXPECT_EQ(0, runCount("FROM test"));
}

} // namespace

// $row_id resolves, so a row-level delete parses. The optimizer rejects it:
// it supports only deletes the connector can carry out as a metadata change.
TEST_F(DeleteTest, rowLevelDelete) {
  const auto plan = parseDelete(
      "DELETE FROM nation WHERE \"$row_id\" IN "
      "(SELECT \"$row_id\" FROM nation WHERE n_regionkey = 1)");
  ASSERT_NE(plan, nullptr);

  VELOX_ASSERT_THROW(
      planVelox(plan), "DELETE requires a scan with an optional filter");
}

} // namespace facebook::axiom::optimizer
