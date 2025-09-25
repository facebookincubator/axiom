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
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class AggregationPlanTest : public testing::Test {
 protected:
  static constexpr auto kTestConnectorId = "test";

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  void SetUp() override {
    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);

    rootPool_ = memory::memoryManager()->addRootPool("root");
    optimizerPool_ = rootPool_->addLeafChild("optimizer");
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);
  }

  velox::core::PlanNodePtr planVelox(
      const logical_plan::LogicalPlanNodePtr& plan);

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> optimizerPool_;
  std::shared_ptr<connector::TestConnector> testConnector_;
};

TEST_F(AggregationPlanTest, dedupGroupingKeysAndAggregates) {
  testConnector_->createTable(
      "numbers", ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), DOUBLE()}));

  {
    auto logicalPlan = lp::PlanBuilder{}
                           .tableScan(kTestConnectorId, "numbers")
                           .project({"a + b as x", "a + b as y", "c"})
                           .aggregate({"x", "y"}, {"count(1)", "count(1)"})
                           .build();

    auto plan = planVelox(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       // TODO Fix the optiimizer to not create projections for
                       // constant inputs to aggregate functions.
                       .project({"1", "a + b"})
                       .singleAggregation({"x"}, {"count(__r2)"})
                       .project({"x", "x", "count", "count"})
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }
}

velox::core::PlanNodePtr AggregationPlanTest::planVelox(
    const logical_plan::LogicalPlanNodePtr& plan) {
  auto queryCtx = core::QueryCtx::create();

  // The default Locus for planning is the system and data of 'connector_'.
  optimizer::Locus locus(
      testConnector_->connectorId().c_str(), testConnector_.get());
  auto allocator = std::make_unique<HashStringAllocator>(optimizerPool_.get());
  auto context = std::make_unique<optimizer::QueryGraphContext>(*allocator);
  optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };
  exec::SimpleExpressionEvaluator evaluator(
      queryCtx.get(), optimizerPool_.get());

  SchemaResolver schema;
  VeloxHistory history;

  optimizer::Schema veraxSchema("test", &schema, &locus);
  optimizer::Optimization opt(
      *plan,
      veraxSchema,
      history,
      queryCtx,
      evaluator,
      {}, // optimizerOptions
      {.numWorkers = 1, .numDrivers = 1});
  auto best = opt.bestPlan();
  auto distributedPlan = opt.toVeloxPlan(best->op).plan;

  VELOX_CHECK_EQ(1, distributedPlan->fragments().size());
  return distributedPlan->fragments().at(0).fragment.planNode;
}

} // namespace
} // namespace facebook::axiom::optimizer
