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

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/exec/TableWriter.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class TestConnectorQueryTest : public QueryTestBase {
 protected:
  MultiFragmentPlanPtr appendTableWrite(
      const MultiFragmentPlanPtr& plan,
      const RowTypePtr& schema,
      const std::string& tableName) {
    EXPECT_EQ(plan->fragments().size(), 1);
    auto executableFragment = plan->fragments().back();
    auto fragment = executableFragment.fragment;

    auto source = fragment.planNode;
    auto handle = std::make_shared<core::InsertTableHandle>(
        kTestConnectorId,
        std::make_shared<connector::TestInsertTableHandle>(SchemaTableName{
            std::string(connector::TestConnector::kDefaultSchema), tableName}));
    auto write = std::make_shared<core::TableWriteNode>(
        "writenodeid",
        source->outputType(),
        schema->names(),
        /*columnStatsSpec=*/std::nullopt,
        std::move(handle),
        /*hasPartitioningScheme=*/false,
        exec::TableWriteTraits::outputType(std::nullopt),
        velox::connector::CommitStrategy::kTaskCommit,
        source);

    ExecutableFragment writeFragment(executableFragment);
    writeFragment.fragment = core::PlanFragment(
        write,
        fragment.executionStrategy,
        fragment.numSplitGroups,
        fragment.groupedExecutionLeafNodeIds);
    std::vector<ExecutableFragment> fragments = {writeFragment};

    return std::make_shared<MultiFragmentPlan>(fragments, options_);
  }

  const MultiFragmentPlan::Options options_{.numWorkers = 1, .numDrivers = 16};
};

TEST_F(TestConnectorQueryTest, selectFiltered) {
  auto vector = makeRowVector({"a"}, {makeFlatVector<int64_t>({0, 1, 2})});
  auto schema = vector->rowType();

  testConnector_->addTable("t", schema);
  testConnector_->appendData("t", vector);

  lp::PlanBuilder::Context context(kTestConnectorId, kDefaultSchema);
  auto logicalPlan =
      lp::PlanBuilder(context).tableScan("t").filter("a > 0").build();
  auto expected = makeRowVector({makeFlatVector<int64_t>({1, 2})});

  auto results = runVelox(logicalPlan, options_);
  exec::test::assertEqualResults(results.results, {expected});
}

TEST_F(TestConnectorQueryTest, filterColumnNotInProjection) {
  auto vector = makeRowVector(
      {"a", "b", "c"},
      {makeNullableFlatVector<int64_t>({1, 2, std::nullopt, 4, 5}),
       makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
       makeFlatVector<int64_t>({100, 200, 300, 400, 500})});
  testConnector_->addTable("t_subset", vector->rowType());
  testConnector_->appendData("t_subset", vector);

  {
    SCOPED_TRACE("bare SELECT");
    auto plan = parseSelect(
        "SELECT b FROM t_subset WHERE a IS NOT NULL", kTestConnectorId);
    auto expected = makeRowVector({makeFlatVector<int64_t>({10, 20, 40, 50})});
    auto results = runVelox(plan, options_);
    exec::test::assertEqualResults(results.results, {expected});
  }

  {
    SCOPED_TRACE("with LIMIT");
    auto plan = parseSelect(
        "SELECT b FROM t_subset WHERE a IS NOT NULL LIMIT 3", kTestConnectorId);
    auto results = runVelox(plan, options_);
    int64_t totalRows = 0;
    for (const auto& batch : results.results) {
      totalRows += batch->size();
    }
    EXPECT_EQ(totalRows, 3);
  }

  {
    SCOPED_TRACE("with ORDER BY");
    auto plan = parseSelect(
        "SELECT b FROM t_subset WHERE a IS NOT NULL ORDER BY b",
        kTestConnectorId);
    auto expected = makeRowVector({makeFlatVector<int64_t>({10, 20, 40, 50})});
    auto results = runVelox(plan, options_);
    exec::test::assertEqualResults(results.results, {expected});
  }

  {
    SCOPED_TRACE("multi-column SELECT");
    auto plan = parseSelect(
        "SELECT b, c FROM t_subset WHERE a IS NOT NULL", kTestConnectorId);
    auto expected = makeRowVector(
        {"b", "c"},
        {makeFlatVector<int64_t>({10, 20, 40, 50}),
         makeFlatVector<int64_t>({100, 200, 400, 500})});
    auto results = runVelox(plan, options_);
    exec::test::assertEqualResults(results.results, {expected});
  }
}

TEST_F(TestConnectorQueryTest, writeFiltered) {
  auto vector = makeRowVector(
      {"b", "c"},
      {makeFlatVector<int64_t>({0, 1, 2}),
       makeFlatVector<StringView>({"str", "ing", "val"})});
  auto schema = vector->rowType();

  auto table = testConnector_->addTable("u", schema);
  EXPECT_NE(table, nullptr);

  lp::PlanBuilder::Context context;
  auto logicalPlan =
      lp::PlanBuilder(context).values({vector}).filter("b < 2").build();
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1}),
      makeFlatVector<StringView>({"str", "ing"}),
  });

  auto fragmentedPlan = planVelox(logicalPlan, options_);
  fragmentedPlan.plan = appendTableWrite(fragmentedPlan.plan, schema, "u");
  runFragmentedPlan(fragmentedPlan);

  EXPECT_EQ(table->data().size(), 1);
  auto actual = table->data().front();
  velox::test::assertEqualVectors(actual, expected);
}

} // namespace
} // namespace facebook::axiom::optimizer::test
