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

#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

#include <algorithm>
#include <array>
#include <functional>
#include <optional>

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;

class RowLevelDeleteTest : public test::QueryTestBase {
 protected:
  void TearDown() override {
    testConnector_->metadata()->setPushdownMatcher(nullptr);
    testConnector_->dropTablesIfExists({"t", "u"});
    test::QueryTestBase::TearDown();
  }

  int64_t runDelete(std::string_view sql) {
    auto plan = planVelox(parseDelete(sql));
    return runFragmentedPlan(plan).getOnlyResult<int64_t>();
  }

  int64_t runCount(std::string_view fromClause) {
    std::string sql{"SELECT count(*) "};
    sql.append(fromClause);
    return runVelox(parseSelect(sql, kTestConnectorId))
        .getOnlyResult<int64_t>();
  }

  std::shared_ptr<connector::TestTable> addRows(std::string_view tableName) {
    testConnector_->dropTableIfExists(tableName);
    auto table = testConnector_->addTable(
        std::string{tableName}, ROW({"id", "value"}, BIGINT()));
    table->addData(makeRowVector(
        {makeFlatVector<int64_t>({0, 1}), makeFlatVector<int64_t>({10, 20})}));
    table->addData(makeRowVector(
        {makeFlatVector<int64_t>({2, 3}), makeFlatVector<int64_t>({30, 40})}));
    return table;
  }

  std::shared_ptr<connector::TestTable> table(std::string_view name) {
    return velox::checkedPointerCast<connector::TestTable>(
        testConnector_->metadata()->findTableInternal(
            {kDefaultSchema, std::string(name)}));
  }
};

TEST_F(RowLevelDeleteTest, targetIdentity) {
  addRows("t");
  const auto logicalPlan = parseDelete(
      "DELETE FROM t "
      "WHERE \"$row_id\" IN ("
      "    SELECT \"$row_id\" FROM t WHERE id IN (1, 3))");

  const auto verifyTarget = [&](v2::NodeCP root, v2::Optimizer::Pass pass) {
    ASSERT_TRUE(root->is(v2::NodeType::kTableWrite));
    const auto* write = root->as<v2::TableWrite>();
    const auto targetScan =
        v2::TableWrite::findTargetScan(write->input(), write->deleteTarget());
    ASSERT_NE(targetScan, nullptr);
    // From pushdown on, the target is a rebuilt scan that carries a handle.
    if (pass != v2::Optimizer::Pass::kTranslate) {
      EXPECT_NE(targetScan->scanHandle(), nullptr);
    }
    EXPECT_FALSE(
        std::ranges::any_of(targetScan->outputColumns(), [](ColumnCP column) {
          return std::string_view{column->name()} == "id";
        }));

    int32_t numTargetScans{0};
    std::function<void(v2::NodeCP)> visit = [&](v2::NodeCP node) {
      if (node->is(v2::NodeType::kScan) &&
          node->as<v2::Scan>()->baseTable()->schemaTable->connectorTable ==
              write->table()) {
        ++numTargetScans;
      }
      for (v2::NodeCP input : node->inputs()) {
        visit(input);
      }
    };
    visit(write->input());
    EXPECT_EQ(numTargetScans, 2);
  };

  for (const auto pass : {
           v2::Optimizer::Pass::kTranslate,
           v2::Optimizer::Pass::kPushdownAndPrune,
           v2::Optimizer::Pass::kConnectorPushdown,
           v2::Optimizer::Pass::kPlanPhysical,
       }) {
    verifyOptimization(
        *logicalPlan, pass, [&](v2::NodeCP root) { verifyTarget(root, pass); });
  }
}

TEST_F(RowLevelDeleteTest, selectedRows) {
  {
    SCOPED_TRACE("filter");
    addRows("t");
    auto plan = planVelox(
        parseDelete("DELETE FROM t WHERE value >= 30"),
        {.maxRemotePartitions = 1, .maxLocalPartitions = 1});

    ASSERT_EQ(plan.plan->fragments().size(), 1);
    AXIOM_ASSERT_PLAN(
        plan.plan->fragments().front().fragment.planNode,
        matchScan("t")
            .filter("value >= 30")
            .project({"\"$row_id\""})
            .tableWrite({std::string{connector::TestTable::kRowId}})
            .build());
    EXPECT_EQ(runFragmentedPlan(plan).getOnlyResult<int64_t>(), 2);
    EXPECT_EQ(runCount("FROM t"), 2);
    EXPECT_EQ(runCount("FROM t WHERE value >= 30"), 0);
    EXPECT_EQ(table("t")->deleteLog(), std::vector<int64_t>{2});
  }

  {
    SCOPED_TRACE("consecutive deletes");
    addRows("t");
    EXPECT_EQ(runDelete("DELETE FROM t WHERE value >= 30"), 2);
    EXPECT_EQ(runDelete("DELETE FROM t WHERE id = 1"), 1);
    EXPECT_EQ(runCount("FROM t"), 1);
  }

  {
    SCOPED_TRACE("row-ID subquery");
    addRows("t");
    auto plan = planVelox(
        parseDelete(
            "DELETE FROM t "
            "WHERE \"$row_id\" IN ("
            "    SELECT \"$row_id\" FROM t WHERE id IN (1, 3))"),
        {.maxRemotePartitions = 1, .maxLocalPartitions = 1});

    ASSERT_EQ(plan.plan->fragments().size(), 1);
    AXIOM_ASSERT_PLAN(
        plan.plan->fragments().front().fragment.planNode,
        matchScan("t")
            .aliases({"target_row_id"})
            .hashJoin(
                matchScan("t")
                    .aliases({"selected_row_id", "selected_id"})
                    .filter("selected_id IN (1, 3)")
                    .project({"selected_row_id"}),
                velox::core::JoinType::kLeftSemiFilter)
            .tableWrite({std::string{connector::TestTable::kRowId}})
            .build());
    EXPECT_EQ(runFragmentedPlan(plan).getOnlyResult<int64_t>(), 2);
    EXPECT_EQ(runCount("FROM t"), 2);
    EXPECT_EQ(runCount("FROM t WHERE id IN (1, 3)"), 0);
  }

  {
    SCOPED_TRACE("co-located subquery");
    for (const auto* name : {"t", "u"}) {
      testConnector_->dropTableIfExists(name);
      auto table = testConnector_->addTable(
          name,
          ROW({"id", "value"}, BIGINT()),
          ROW({}),
          connector::TestBucketSpec{{"id"}, 4});
      table->addData(makeRowVector(
          {makeFlatVector<int64_t>({0, 1, 2, 3}),
           makeFlatVector<int64_t>({10, 20, 30, 40})}));
    }
    auto plan = planVelox(
        parseDelete(
            "DELETE FROM t WHERE id IN (SELECT id FROM u WHERE value > 15)"),
        {.maxRemotePartitions = 4, .maxLocalPartitions = 2});

    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .hashJoin(
                matchScan("u")
                    .aliases({"id", "value"})
                    .filter("value > 15")
                    .project({"id"}),
                velox::core::JoinType::kLeftSemiFilter)
            .fragment({.width = 4, .bucketedScans = 2})
            .tableWrite({std::string{connector::TestTable::kRowId}})
            .gather()
            .build());
    EXPECT_EQ(runFragmentedPlan(plan).getOnlyResult<int64_t>(), 3);
    EXPECT_EQ(runCount("FROM t"), 1);
  }

  // A DELETE whose input is proven empty, by its WHERE clause alone or by a
  // conjunct held above a subquery join, removes nothing.
  {
    SCOPED_TRACE("no rows");
    addRows("t");
    for (const auto* sql : {
             "DELETE FROM t WHERE false",
             "DELETE FROM t WHERE value > 0 AND 1 = 0",
             "DELETE FROM t WHERE id = 1 AND id = 2",
             "DELETE FROM t "
             "WHERE (id = 1 AND id = 2 AND value = 10) "
             "   OR (id = 3 AND id = 4 AND value = 20)",
             "DELETE FROM t "
             "WHERE ((id = 1 AND id = 2 AND value = 10) "
             "    OR (id = 3 AND id = 4 AND value = 20)) "
             "  AND (id IN (SELECT id FROM t) OR value > 100)",
             "DELETE FROM t "
             "WHERE id IN ("
             "    SELECT id "
             "    FROM t "
             "    WHERE (id = 1 AND id = 2 AND value = 10) "
             "       OR (id = 3 AND id = 4 AND value = 20)) "
             "  AND (value IN (SELECT value FROM t) OR id > 100)",
         }) {
      SCOPED_TRACE(sql);
      EXPECT_EQ(runDelete(sql), 0);
    }
    EXPECT_EQ(runCount("FROM t"), 4);
    EXPECT_TRUE(table("t")->deleteLog().empty());
  }
}

TEST_F(RowLevelDeleteTest, executionFailure) {
  addRows("t");
  auto metadata = testConnector_->metadata();
  auto plan = planVelox(parseDelete("DELETE FROM t WHERE value / id > 0"));
  const auto abortsBefore = metadata->numAbortDeleteCalls();

  VELOX_ASSERT_THROW(runFragmentedPlan(plan), "division by zero");

  EXPECT_EQ(metadata->numAbortDeleteCalls(), abortsBefore + 1);
  EXPECT_EQ(runCount("FROM t"), 4);
  EXPECT_TRUE(table("t")->deleteLog().empty());
}

// A connector that deletes only through metadata rejects rows that a filter
// above its scan selects.
TEST_F(RowLevelDeleteTest, metadataOnlyLayout) {
  addRows("t")->setDeleteLayout(connector::DeleteLayout::metadata());
  VELOX_ASSERT_USER_THROW(
      planVelox(parseDelete("DELETE FROM t WHERE value > 15")),
      "DELETE requires row-level support from the connector");
}

// A layout whose shuffle key type differs from the partition type's key type is
// rejected.
TEST_F(RowLevelDeleteTest, shuffleKeyType) {
  addRows("t")->setDeleteLayout(
      connector::DeleteLayout::rowLevel(
          {},
          {std::string{connector::TestTable::kRowId}},
          std::make_shared<connector::TestPartitionType>(
              4,
              std::vector<velox::TypePtr>{VARCHAR()},
              ROW(std::string{connector::TestTable::kRowId}, VARCHAR())),
          {},
          {}));
  VELOX_ASSERT_THROW(
      planVelox(
          parseDelete("DELETE FROM t WHERE value > 0"),
          {.maxRemotePartitions = 4, .maxLocalPartitions = 2}),
      "Delete shuffle key type does not match the partition type: $row_id");
}

TEST_F(RowLevelDeleteTest, writerRequirements) {
  {
    SCOPED_TRACE("unconstrained");
    addRows("t");
    auto plan = planVelox(
        parseDelete("DELETE FROM t WHERE value > 0"),
        {.maxRemotePartitions = 4,
         .maxLocalPartitions = 2,
         .remoteOutput = true});

    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .filter("value > 0")
            .project({"\"$row_id\""})
            .tableWrite({std::string{connector::TestTable::kRowId}})
            .gather()
            .partitionedOutputSingle()
            .output(FragmentType::kSingle)
            .build());
  }

  {
    SCOPED_TRACE("distributed and sorted");
    auto table = addRows("t");
    table->addData(makeRowVector(
        {makeFlatVector<int64_t>({4, 5, 6, 7}),
         makeFlatVector<int64_t>({50, 60, 70, 80})}));
    auto rowId = std::make_shared<velox::core::FieldAccessTypedExpr>(
        BIGINT(), std::string{connector::TestTable::kRowId});
    auto group = std::make_shared<velox::core::CallTypedExpr>(
        BIGINT(),
        "coalesce",
        std::make_shared<velox::core::CallTypedExpr>(
            BIGINT(),
            "mod",
            rowId,
            std::make_shared<velox::core::ConstantTypedExpr>(
                BIGINT(), velox::Variant(int64_t{4}))),
        std::make_shared<velox::core::ConstantTypedExpr>(
            BIGINT(), velox::Variant(int64_t{0})));
    auto rowKey = std::make_shared<velox::core::ConcatTypedExpr>(
        std::vector<std::string>{"id"},
        std::vector<velox::core::TypedExprPtr>{rowId});
    table->setDeleteLayout(
        connector::DeleteLayout::rowLevel(
            {
                {"delete_group", std::move(group)},
                {"row_key", std::move(rowKey)},
            },
            {"delete_group"},
            std::make_shared<connector::TestPartitionType>(
                4,
                std::vector<velox::TypePtr>{BIGINT()},
                ROW({std::string{connector::TestTable::kRowId},
                     "delete_group",
                     "row_key"},
                    {BIGINT(), BIGINT(), ROW("id", BIGINT())})),
            {std::string{connector::TestTable::kRowId}},
            {{/*isAscending=*/true, /*isNullsFirst=*/false}}));

    // On one worker, the drivers still partition and sort the rows for their
    // writers.
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(
            parseDelete("DELETE FROM t WHERE value > 0"),
            {.maxRemotePartitions = 1, .maxLocalPartitions = 2})
            .plan,
        matchScan("t")
            .filter("value > 0")
            .project(
                {"\"$row_id\"",
                 "coalesce(mod(\"$row_id\", 4), 0) as delete_group",
                 "row_constructor(\"$row_id\") as row_key"})
            .localPartition({"delete_group"})
            .partialOrderBy({"\"$row_id\""})
            .tableWrite(
                {std::string{connector::TestTable::kRowId},
                 "delete_group",
                 "row_key"})
            .build());

    auto plan = planVelox(
        parseDelete("DELETE FROM t WHERE value > 0"),
        {.maxRemotePartitions = 4, .maxLocalPartitions = 2});
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .filter("value > 0")
            .project(
                {"\"$row_id\"",
                 "coalesce(mod(\"$row_id\", 4), 0) as delete_group",
                 "row_constructor(\"$row_id\") as row_key"})
            .shuffle({"delete_group"})
            .localPartition({"delete_group"})
            .partialOrderBy({"\"$row_id\""})
            .tableWrite(
                {std::string{connector::TestTable::kRowId},
                 "delete_group",
                 "row_key"})
            .gather()
            .build());

    EXPECT_EQ(runFragmentedPlan(plan).getOnlyResult<int64_t>(), 8);
    EXPECT_EQ(runCount("FROM t"), 0);
    ASSERT_GT(table->deleteWriterLog().size(), 1);
    std::array<std::optional<size_t>, 4> writerByGroup;
    for (size_t writer = 0; writer < table->deleteWriterLog().size();
         ++writer) {
      const auto& rowIds = table->deleteWriterLog()[writer];
      EXPECT_TRUE(std::is_sorted(rowIds.begin(), rowIds.end()));
      for (const auto deletedRowId : rowIds) {
        const auto groupId = static_cast<size_t>(deletedRowId % 4);
        if (writerByGroup[groupId].has_value()) {
          EXPECT_EQ(writerByGroup[groupId], writer);
        } else {
          writerByGroup[groupId] = writer;
        }
      }
    }
    EXPECT_TRUE(std::ranges::all_of(writerByGroup, [](const auto& writer) {
      return writer.has_value();
    }));
  }

  {
    SCOPED_TRACE("already bucketed");
    testConnector_->dropTableIfExists("t");
    auto table = testConnector_->addTable(
        "t",
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
    table->setDeleteLayout(
        connector::DeleteLayout::rowLevel(
            {},
            {std::string{connector::TestTable::kRowId}},
            table->layouts().front()->partitionType(),
            {},
            {}));

    auto plan = planVelox(
        parseDelete("DELETE FROM t WHERE value > 0"),
        {.maxRemotePartitions = 4, .maxLocalPartitions = 2});
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .fragment({.width = 4, .bucketedScans = 1})
            .filter("value > 0")
            .project({"\"$row_id\""})
            .localPartition({"$row_id"})
            .tableWrite({std::string{connector::TestTable::kRowId}})
            .gather()
            .build());

    EXPECT_EQ(runFragmentedPlan(plan).getOnlyResult<int64_t>(), 8);
    EXPECT_EQ(runCount("FROM t"), 0);
  }
}

TEST_F(RowLevelDeleteTest, virtualTablePushdown) {
  {
    SCOPED_TRACE("target is not replaced");
    addRows("t");
    auto replacement =
        testConnector_->addTable("u", ROW({"value", "row_id"}, BIGINT()));
    testConnector_->metadata()->setPushdownMatcher(
        [replacement = std::move(replacement)](const v2::Node& subtree) {
          return std::vector<connector::PushdownRoot>{{&subtree, replacement}};
        });

    auto plan = planVelox(parseDelete("DELETE FROM t WHERE value >= 30"));
    testConnector_->metadata()->setPushdownMatcher(nullptr);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .filter("value >= 30")
            .project({"\"$row_id\""})
            .tableWrite({std::string{connector::TestTable::kRowId}})
            .gather()
            .build());
    EXPECT_EQ(runFragmentedPlan(plan).getOnlyResult<int64_t>(), 2);
    EXPECT_EQ(runCount("FROM t"), 2);
  }

  {
    SCOPED_TRACE("non-target read is replaced");
    testConnector_->dropTablesIfExists({"t", "u"});
    addRows("t");
    auto replacement = testConnector_->addTable("u", ROW("row_id", BIGINT()));
    replacement->addData(makeRowVector({makeFlatVector<int64_t>({1, 3})}));
    testConnector_->metadata()->setPushdownMatcher(
        [replacement = std::move(replacement)](const v2::Node& subtree) {
          const std::function<bool(v2::NodeCP)> readsId = [&](v2::NodeCP node) {
            if (node->is(v2::NodeType::kScan) &&
                std::ranges::any_of(node->outputColumns(), [](ColumnCP column) {
                  return std::string_view{column->name()} == "id";
                })) {
              return true;
            }
            return std::ranges::any_of(node->inputs(), readsId);
          };
          std::function<v2::NodeCP(v2::NodeCP)> findNonTargetRoot =
              [&](v2::NodeCP node) -> v2::NodeCP {
            for (v2::NodeCP input : node->inputs()) {
              if (!input->is(v2::NodeType::kScan) &&
                  input->outputColumns().size() == 1 && readsId(input)) {
                return input;
              }
              if (auto* result = findNonTargetRoot(input)) {
                return result;
              }
            }
            return nullptr;
          };
          auto* acceptedRoot = findNonTargetRoot(&subtree);
          VELOX_CHECK_NOT_NULL(acceptedRoot);
          return std::vector<connector::PushdownRoot>{
              {acceptedRoot, replacement}};
        });

    auto plan = planVelox(parseDelete(
        "DELETE FROM t "
        "WHERE \"$row_id\" IN ("
        "    SELECT \"$row_id\" FROM t WHERE id IN (1, 3))"));
    testConnector_->metadata()->setPushdownMatcher(nullptr);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .aliases({"target_row_id"})
            .hashJoin(
                matchScan("u")
                    .aliases({"row_id"})
                    .project({"row_id"})
                    .aliases({"selected_row_id"})
                    .broadcast(),
                velox::core::JoinType::kLeftSemiFilter)
            .tableWrite({std::string{connector::TestTable::kRowId}})
            .gather()
            .build());
    EXPECT_EQ(runFragmentedPlan(plan).getOnlyResult<int64_t>(), 2);
    EXPECT_EQ(runCount("FROM t"), 2);
    EXPECT_EQ(runCount("FROM t WHERE id IN (1, 3)"), 0);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
