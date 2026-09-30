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

#include <algorithm>
#include <array>

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

// Deletes against Hive, which removes the rows by dropping whole partitions.
class DeleteTest : public test::HiveQueriesTestBase {
 protected:
  const std::string kDefaultSchema{
      connector::hive::LocalHiveConnectorMetadata::kDefaultSchema};

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

  lp::LogicalPlanNodePtr parseTestDelete(std::string_view sql) {
    ::axiom::sql::presto::PrestoParser parser(
        kTestConnectorId,
        kDefaultSchema,
        std::make_shared<::axiom::sql::presto::ParserSession>(
            connector::makeTestContext("test"),
            connector::makeTestStatWriter(),
            connector::Properties{},
            ::axiom::sql::presto::ParserOptions{}));
    auto statement = parser.parse(sql);
    VELOX_CHECK(statement->isDelete());
    return statement->as<::axiom::sql::presto::DeleteStatement>()->plan();
  }

  int64_t runTestCount(std::string_view fromClause) {
    return runVelox(parseSelect(
                        fmt::format("SELECT count(*) {}", fromClause),
                        kTestConnectorId))
        .getOnlyResult<int64_t>();
  }

  void addTestRows(std::string_view tableName) {
    auto table = testConnector_->addTable(
        std::string{tableName}, ROW({"id", "value"}, BIGINT()));
    table->addData(makeRowVector(
        {makeFlatVector<int64_t>({0, 1}), makeFlatVector<int64_t>({10, 20})}));
    table->addData(makeRowVector(
        {makeFlatVector<int64_t>({2, 3}), makeFlatVector<int64_t>({30, 40})}));
  }

  std::shared_ptr<connector::TestTable> testTable(std::string_view name) {
    return velox::checkedPointerCast<connector::TestTable>(
        testConnector_->metadata()->findTableInternal(
            {std::string(kDefaultSchema), std::string(name)}));
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

  // A predicate no partition satisfies is proven empty by metadata and
  // removes nothing.
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

// A delete whose rows come from a subquery over the same table selects rows
// inside a partition, which a connector that removes them by dropping whole
// partitions cannot carry out.
TEST_F(DeleteTest, subqueryWithoutRowLevelDelete) {
  const auto plan = parseDelete(
      "DELETE FROM nation WHERE \"$row_id\" IN "
      "(SELECT \"$row_id\" FROM nation WHERE n_regionkey = 1)");
  ASSERT_NE(plan, nullptr);

  VELOX_ASSERT_USER_THROW(
      planVelox(plan), "DELETE requires row-level support from the connector");
}

// A filter left above the scan selects rows for the writer rather than
// preventing a row-level DELETE.
TEST_F(DeleteTest, unabsorbedFilter) {
  addTestRows("rows");
  auto plan = planVelox(
      parseTestDelete("DELETE FROM rows WHERE value >= 30"),
      {.maxRemotePartitions = 1, .maxLocalPartitions = 1});

  ASSERT_EQ(plan.plan->fragments().size(), 1);
  AXIOM_ASSERT_PLAN(
      plan.plan->fragments().front().fragment.planNode,
      matchScan("rows")
          .filter("value >= 30")
          .project({"\"$row_id\""})
          .tableWrite({std::string{connector::TestTable::kRowId}})
          .build());
  EXPECT_EQ(runFragmentedPlan(plan).getOnlyResult<int64_t>(), 2);
  EXPECT_EQ(runTestCount("FROM rows"), 2);
  EXPECT_EQ(runTestCount("FROM rows WHERE value >= 30"), 0);
  EXPECT_EQ(testTable("rows")->deleteLog(), std::vector<int64_t>{2});
}

// A second DELETE correctly reads row IDs from data retained by the first.
TEST_F(DeleteTest, consecutiveDeletes) {
  addTestRows("rows");
  auto first = planVelox(parseTestDelete("DELETE FROM rows WHERE value >= 30"));
  EXPECT_EQ(runFragmentedPlan(first).getOnlyResult<int64_t>(), 2);
  auto second = planVelox(parseTestDelete("DELETE FROM rows WHERE id = 1"));
  EXPECT_EQ(runFragmentedPlan(second).getOnlyResult<int64_t>(), 1);
  EXPECT_EQ(runTestCount("FROM rows"), 1);
}

// A membership subquery can select exact row IDs for a row-level DELETE.
TEST_F(DeleteTest, rowIdSubquery) {
  addTestRows("rows");
  auto plan = planVelox(
      parseTestDelete(
          "DELETE FROM rows WHERE \"$row_id\" IN "
          "(SELECT \"$row_id\" FROM rows WHERE id IN (1, 3))"),
      {.maxRemotePartitions = 1, .maxLocalPartitions = 1});

  ASSERT_EQ(plan.plan->fragments().size(), 1);
  AXIOM_ASSERT_PLAN(
      plan.plan->fragments().front().fragment.planNode,
      matchScan("rows")
          .aliases({"target_row_id"})
          .hashJoin(
              matchScan("rows")
                  .aliases({"selected_row_id", "selected_id"})
                  .filter("selected_id IN (1, 3)")
                  .project({"selected_row_id"}),
              velox::core::JoinType::kLeftSemiFilter)
          .tableWrite({std::string{connector::TestTable::kRowId}})
          .build());
  EXPECT_EQ(runFragmentedPlan(plan).getOnlyResult<int64_t>(), 2);
  EXPECT_EQ(runTestCount("FROM rows"), 2);
  EXPECT_EQ(runTestCount("FROM rows WHERE id IN (1, 3)"), 0);
}

// A writer with no distribution requirement retains the input's parallelism.
TEST_F(DeleteTest, noDeleteDistributionRequirement) {
  auto table = testConnector_->addTable(
      "unconstrained_delete_rows",
      ROW({"id", "value"}, BIGINT()),
      ROW({}),
      connector::TestBucketSpec{{"id"}, 4});
  table->addData(makeRowVector(
      {makeFlatVector<int64_t>({0, 1, 2, 3}),
       makeFlatVector<int64_t>({10, 20, 30, 40})}));

  auto plan = planVelox(
      parseTestDelete("DELETE FROM unconstrained_delete_rows WHERE value > 0"),
      {.maxRemotePartitions = 4, .maxLocalPartitions = 2});

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("unconstrained_delete_rows")
          .filter("value > 0")
          .project({"\"$row_id\""})
          .tableWrite({std::string{connector::TestTable::kRowId}})
          .gather()
          .build());
}

// Missing writer properties add the required shuffle, local partition, and
// sort before the row-level DELETE writer.
TEST_F(DeleteTest, distributedRowLevelDelete) {
  auto table = testConnector_->addTable(
      "distributed_rows", ROW({"id", "value"}, BIGINT()));
  table->addData(makeRowVector(
      {makeFlatVector<int64_t>({0, 1, 2, 3}),
       makeFlatVector<int64_t>({10, 20, 30, 40})}));
  table->addData(makeRowVector(
      {makeFlatVector<int64_t>({4, 5, 6, 7}),
       makeFlatVector<int64_t>({50, 60, 70, 80})}));

  auto rowId = std::make_shared<velox::core::FieldAccessTypedExpr>(
      BIGINT(), std::string{connector::TestTable::kRowId});
  auto group = std::make_shared<velox::core::CallTypedExpr>(
      BIGINT(),
      "mod",
      rowId,
      std::make_shared<velox::core::ConstantTypedExpr>(
          BIGINT(), velox::Variant(int64_t{2})));
  auto rowKey = std::make_shared<velox::core::ConcatTypedExpr>(
      std::vector<std::string>{"id"},
      std::vector<velox::core::TypedExprPtr>{rowId});
  table->setDeleteInput(
      connector::DeleteInput{
          {{"delete_group", std::move(group)}, {"row_key", std::move(rowKey)}},
          {"delete_group"},
          {std::string{connector::TestTable::kRowId}},
          {{/*isAscending=*/true, /*isNullsFirst=*/false}}});

  auto plan = planVelox(
      parseTestDelete("DELETE FROM distributed_rows WHERE value > 0"),
      {.maxRemotePartitions = 4, .maxLocalPartitions = 2});
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("distributed_rows")
          .filter("value > 0")
          .project(
              {"\"$row_id\"",
               "mod(\"$row_id\", 2) as delete_group",
               "row_constructor(\"$row_id\") as row_key"})
          .shuffle({"delete_group"})
          .localPartition({"delete_group"})
          .orderBy({"\"$row_id\""})
          .tableWrite(
              {std::string{connector::TestTable::kRowId},
               "delete_group",
               "row_key"})
          .gather()
          .build());

  EXPECT_EQ(runFragmentedPlan(plan).getOnlyResult<int64_t>(), 8);
  EXPECT_EQ(runTestCount("FROM distributed_rows"), 0);
  ASSERT_GT(table->deleteWriterLog().size(), 1);
  std::array<std::optional<size_t>, 2> writerByGroup;
  for (size_t writer = 0; writer < table->deleteWriterLog().size(); ++writer) {
    const auto& rowIds = table->deleteWriterLog()[writer];
    EXPECT_TRUE(std::is_sorted(rowIds.begin(), rowIds.end()));
    for (const auto row : rowIds) {
      const auto groupId = static_cast<size_t>(row % 2);
      if (writerByGroup[groupId].has_value()) {
        EXPECT_EQ(writerByGroup[groupId], writer);
      } else {
        writerByGroup[groupId] = writer;
      }
    }
  }
  EXPECT_TRUE(writerByGroup[0].has_value());
  EXPECT_TRUE(writerByGroup[1].has_value());
}

// Input already bucketed by the writer's key does not add a remote shuffle.
TEST_F(DeleteTest, bucketedDelete) {
  auto table = testConnector_->addTable(
      "bucketed_delete_rows",
      ROW({"id", "value"}, BIGINT()),
      ROW({}),
      connector::TestBucketSpec{
          {std::string{connector::TestTable::kRowId}}, 4});
  table->addData(makeRowVector(
      {makeFlatVector<int64_t>({0, 1, 2, 3}),
       makeFlatVector<int64_t>({10, 20, 30, 40})}));
  table->addData(makeRowVector(
      {makeFlatVector<int64_t>({4, 5, 6, 7}),
       makeFlatVector<int64_t>({50, 60, 70, 80})}));
  table->setDeleteInput(
      connector::DeleteInput{
          {}, {std::string{connector::TestTable::kRowId}}, {}, {}});

  auto plan = planVelox(
      parseTestDelete("DELETE FROM bucketed_delete_rows WHERE value > 0"),
      {.maxRemotePartitions = 4, .maxLocalPartitions = 1});
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("bucketed_delete_rows")
          .filter("value > 0")
          .project({"\"$row_id\""})
          .tableWrite({std::string{connector::TestTable::kRowId}})
          .gather()
          .build());

  EXPECT_EQ(runFragmentedPlan(plan).getOnlyResult<int64_t>(), 8);
  EXPECT_EQ(runTestCount("FROM bucketed_delete_rows"), 0);
}

} // namespace
} // namespace facebook::axiom::optimizer
