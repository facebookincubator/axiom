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

class TestConnectorQueryTest : public QueryTestBase,
                               public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    QueryTestBase::SetUp();
    useV2_ = GetParam();
  }

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

TEST_P(TestConnectorQueryTest, selectFiltered) {
  auto data = makeRowVector(
      {"a", "b"},
      {
          makeFlatVector<int64_t>({0, 1, 2}),
          makeFlatVector<int64_t>({0, 10, 20}),
      });
  testConnector_->addTable("t", data->rowType())->addData(data);

  auto expected = makeRowVector({makeFlatVector<int64_t>({1, 2})});

  // Filter on a selected column.
  lp::PlanBuilder::Context context(kTestConnectorId, kDefaultSchema);
  {
    auto logicalPlan =
        lp::PlanBuilder(context).tableScan("t", {"a"}).filter("a > 0").build();
    auto results = runVelox(logicalPlan, options_);
    exec::test::assertEqualResults(results.results, {expected});
  }

  // Filter on a column that is not selected: it must not appear in the output.
  {
    auto logicalPlan = lp::PlanBuilder(context)
                           .tableScan("t", {"a", "b"})
                           .filter("b > 0")
                           .project({"a"})
                           .build();
    auto results = runVelox(logicalPlan, options_);
    exec::test::assertEqualResults(results.results, {expected});
  }
}

TEST_P(TestConnectorQueryTest, writeFiltered) {
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

// A chain of CTEs that each square the previous column inlines into a single
// expression whose shared subexpressions form a DAG with 2^depth-way sharing.
// Converting that expression to a Velox plan must reuse each shared node's
// result rather than re-expanding it per reference, which would be exponential.
TEST_P(TestConnectorQueryTest, sharedSubexpressionConversion) {
  testConnector_->addTable("t", ROW({"x"}, {BIGINT()}));

  constexpr int kDepth = 28;
  std::string sql = "WITH t0 AS (SELECT x FROM t)";
  for (int i = 1; i <= kDepth; ++i) {
    sql += fmt::format(", t{0} AS (SELECT x * x AS x FROM t{1})", i, i - 1);
  }
  sql += fmt::format(" SELECT x FROM t{}", kDepth);

  auto logicalPlan = parseSelect(sql, kTestConnectorId);
  ASSERT_NO_THROW(toSingleNodePlan(logicalPlan));
}

AXIOM_INSTANTIATE_V1_V2(TestConnectorQueryTest);

} // namespace
} // namespace facebook::axiom::optimizer::test
