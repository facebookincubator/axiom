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

#include "axiom/optimizer/AggregationPlanner.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;

namespace lp = facebook::axiom::logical_plan;

namespace facebook::axiom::optimizer {
namespace {

class AggregationPlannerTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  void SetUp() override {
    rootPool_ = memory::memoryManager()->addRootPool("root");
    optimizerPool_ = rootPool_->addLeafChild("optimizer");
  }

  void runTest(
      const lp::LogicalPlanNodePtr& plan,
      const std::function<void(DerivedTableCP derivedTable)>& testRoutine) {
    auto allocator =
        std::make_unique<HashStringAllocator>(optimizerPool_.get());
    auto context = std::make_unique<QueryGraphContext>(*allocator);
    queryCtx() = context.get();
    SCOPE_EXIT {
      queryCtx() = nullptr;
    };

    auto veloxQueryCtx = core::QueryCtx::create();
    exec::SimpleExpressionEvaluator evaluator(
        veloxQueryCtx.get(), optimizerPool_.get());

    VeloxHistory history;
    auto schemaResolver = std::make_shared<connector::SchemaResolver>();
    auto session = std::make_shared<Session>(veloxQueryCtx->queryId());

    Optimization opt{
        session,
        *plan,
        *schemaResolver,
        history,
        veloxQueryCtx,
        evaluator,
        {},
        {.numWorkers = 1, .numDrivers = 1}};

    testRoutine(opt.rootDt());
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> optimizerPool_;
};

// Two `ExprCP` entries that refer to the same logical column (different
// pointers, made equivalent via `Column::equals`) collapse to a single
// pre-grouped key. Verified for both `orderKeys` and `clusterKeys` since each
// has its own dedup loop.
TEST_F(AggregationPlannerTest, dedupSemanticDuplicates) {
  auto logicalPlan = lp::PlanBuilder{}
                         .values(ROW({"k"}, INTEGER()), {variant::row({1})})
                         .aggregate({"k"}, {"sum(k)"})
                         .build();

  runTest(logicalPlan, [&](DerivedTableCP derivedTable) {
    auto* groupingKey = derivedTable->aggregation->groupingKeys()[0];
    auto* duplicateKey =
        make<Column>(toName("k"), derivedTable, Value(toType(INTEGER()), 1));
    groupingKey->as<Column>()->equals(duplicateKey);

    Distribution viaOrderKeys(
        Distribution::Kind::kPartitioned,
        /*partitionType=*/nullptr,
        /*partitionKeys=*/{},
        /*orderKeys=*/{groupingKey, duplicateKey},
        /*orderTypes=*/
        {OrderType::kAscNullsLast, OrderType::kAscNullsLast});
    EXPECT_THAT(
        detail::computePreGroupedKeys(viaOrderKeys, {groupingKey}),
        ::testing::ElementsAre(groupingKey));

    Distribution viaClusterKeys(
        Distribution::Kind::kPartitioned,
        /*partitionType=*/nullptr,
        /*partitionKeys=*/{},
        /*orderKeys=*/{},
        /*orderTypes=*/{},
        /*numKeysUnique=*/0,
        /*clusterKeys=*/{groupingKey, duplicateKey});
    EXPECT_THAT(
        detail::computePreGroupedKeys(viaClusterKeys, {groupingKey}),
        ::testing::ElementsAre(groupingKey));
  });
}

} // namespace
} // namespace facebook::axiom::optimizer
