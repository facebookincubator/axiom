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

#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/exec/TableWriter.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class TestConnectorQueryTest : public QueryTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  void SetUp() override {
    QueryTestBase::SetUp();
    connector_ = std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(connector_);
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);
    connector_.reset();
    QueryTestBase::TearDown();
  }

  runner::MultiFragmentPlanPtr appendTableWrite(
      const runner::MultiFragmentPlanPtr& plan,
      const RowTypePtr& schema,
      const std::string& tableName) {
    EXPECT_EQ(plan->fragments().size(), 1);
    auto executableFragment = plan->fragments().back();
    auto fragment = executableFragment.fragment;

    auto source = fragment.planNode;
    auto handle = std::make_shared<core::InsertTableHandle>(
        kTestConnectorId,
        std::make_shared<connector::TestInsertTableHandle>(tableName));
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

    runner::ExecutableFragment writeFragment(executableFragment);
    writeFragment.fragment = core::PlanFragment(
        write,
        fragment.executionStrategy,
        fragment.numSplitGroups,
        fragment.groupedExecutionLeafNodeIds);
    std::vector<runner::ExecutableFragment> fragments = {writeFragment};

    return std::make_shared<runner::MultiFragmentPlan>(fragments, options_);
  }

  std::shared_ptr<connector::TestConnector> connector_;
  const runner::MultiFragmentPlan::Options options_{
      .numWorkers = 1,
      .numDrivers = 16};
};

TEST_F(TestConnectorQueryTest, selectFiltered) {
  auto vector = makeRowVector({"a"}, {makeFlatVector<int64_t>({0, 1, 2})});
  auto schema = vector->rowType();

  connector_->addTable("t", schema);
  connector_->appendData("t", vector);

  lp::PlanBuilder::Context context(kTestConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context).tableScan("t").filter("a > 0").build();
  auto expected = makeRowVector({makeFlatVector<int64_t>({1, 2})});

  auto results = runVelox(logicalPlan, options_);
  exec::test::assertEqualResults(results.results, {expected});
}

TEST_F(TestConnectorQueryTest, writeFiltered) {
  auto vector = makeRowVector(
      {"b", "c"},
      {makeFlatVector<int64_t>({0, 1, 2}),
       makeFlatVector<StringView>({"str", "ing", "val"})});
  auto schema = vector->rowType();

  auto table = connector_->addTable("u", schema);
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
