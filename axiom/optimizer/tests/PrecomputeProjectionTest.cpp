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

#include "axiom/optimizer/PrecomputeProjection.h"
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

class PrecomputeProjectionTest : public ::testing::Test {
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
      const std::function<void(DerivedTableCP dt)>& testRoutine) {
    auto allocator =
        std::make_unique<velox::HashStringAllocator>(optimizerPool_.get());
    auto context = std::make_unique<QueryGraphContext>(*allocator);
    queryCtx() = context.get();
    SCOPE_EXIT {
      queryCtx() = nullptr;
    };

    auto veloxQueryCtx = velox::core::QueryCtx::create();
    velox::exec::SimpleExpressionEvaluator evaluator(
        veloxQueryCtx.get(), optimizerPool_.get());

    VeloxHistory history;

    auto schemaResolver = std::make_shared<connector::SchemaResolver>();
    Schema schema("default", schemaResolver.get(), /* locus */ nullptr);

    Optimization opt{
        *plan,
        schema,
        history,
        veloxQueryCtx,
        evaluator,
        {}, // optimizerOptions
        {.numWorkers = 1, .numDrivers = 1}};

    testRoutine(opt.rootDt());
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> optimizerPool_;
};

TEST_F(PrecomputeProjectionTest, basic) {
  auto logicalPlan = lp::PlanBuilder{/*enableCoersions=*/true}
                         .values(
                             ROW({"a", "b", "c"}, INTEGER()),
                             {
                                 variant::row({1, 2, 3}),
                                 variant::row({10, 20, 30}),
                             })
                         .aggregate({"a", "a + b"}, {"sum(c)", "sum(c * 2)"})
                         .build();

  runTest(logicalPlan, [&](auto dt) {
    const auto* valuesTable = dt->tables[0]->template as<ValuesTable>();
    RelationOpPtr input{make<Values>(*valuesTable, valuesTable->columns)};

    // No expressions.
    {
      PrecomputeProjection precompute(input, dt);
      auto project = std::move(precompute).maybeProject();
      ASSERT_EQ(project.get(), input.get());
    }

    auto* agg = dt->aggregation;

    // A few expressions. Some duplicates.
    {
      PrecomputeProjection precompute(input, dt);

      // Column "a".
      auto* column = precompute.toColumn(agg->groupingKeys()[0]);
      ASSERT_EQ(column, agg->groupingKeys()[0]);

      // Expression "a + b".
      column = precompute.toColumn(agg->groupingKeys()[1]);
      ASSERT_NE(column, agg->groupingKeys()[0]);
      ASSERT_NE(column, agg->groupingKeys()[1]);
      ASSERT_TRUE(column->is(PlanType::kColumnExpr));

      auto* duplicate = precompute.toColumn(agg->groupingKeys()[1]);
      ASSERT_EQ(column, duplicate);

      auto project = std::move(precompute).maybeProject();
      ASSERT_NE(project.get(), input.get());
      ASSERT_TRUE(project->is(RelType::kProject));

      // The Project node should have all columns from input + an "a + b"
      // column.
      ASSERT_EQ(project->columns().size(), input->columns().size() + 1);
      ASSERT_EQ(
          project->as<Project>()->exprs().back()->toString(),
          "plus(vt2.a, vt2.b)");
    }

    // Same, but with projectAllInputs = false.
    {
      PrecomputeProjection precompute(input, dt, /*projectAllInputs=*/false);

      // Column "a".
      auto* column = precompute.toColumn(agg->groupingKeys()[0]);
      ASSERT_EQ(column, agg->groupingKeys()[0]);

      // Expression "a + b".
      column = precompute.toColumn(agg->groupingKeys()[1]);
      ASSERT_NE(column, agg->groupingKeys()[0]);
      ASSERT_NE(column, agg->groupingKeys()[1]);
      ASSERT_TRUE(column->is(PlanType::kColumnExpr));

      auto* duplicate = precompute.toColumn(agg->groupingKeys()[1]);
      ASSERT_EQ(column, duplicate);

      auto project = std::move(precompute).maybeProject();
      ASSERT_NE(project.get(), input.get());
      ASSERT_TRUE(project->is(RelType::kProject));

      // The Project node should have only 2 columns: "a" and "a + b".
      ASSERT_EQ(project->columns().size(), 2);
      ASSERT_EQ(
          project->as<Project>()->exprs().back()->toString(),
          "plus(vt2.a, vt2.b)");
    }

    // Literals.
    Variant v{123};
    auto* literal = make<Literal>(Value(toType(INTEGER()), 1), &v);
    {
      PrecomputeProjection precompute(input, dt);

      auto* column = precompute.toColumn(literal);
      ASSERT_NE(column, literal);
      ASSERT_TRUE(column->is(PlanType::kColumnExpr));

      auto project = std::move(precompute).maybeProject();
      ASSERT_NE(project.get(), input.get());
      ASSERT_EQ(project->columns().size(), input->columns().size() + 1);
      ASSERT_EQ(project->as<Project>()->exprs().back()->toString(), "123");
    }

    {
      PrecomputeProjection precompute(input, dt);

      auto* column = precompute.toColumn(
          literal, /*alias=*/nullptr, /*preserveLiterals=*/true);
      ASSERT_EQ(column, literal);

      auto project = std::move(precompute).maybeProject();
      ASSERT_EQ(project.get(), input.get());
    }

    // Aliases.
    {
      PrecomputeProjection precompute(input, dt, /*projectAllInputs=*/false);

      auto* outputColumn =
          make<Column>(toName("aaa"), dt, Value(toType(INTEGER()), 1));

      auto* column = precompute.toColumn(agg->groupingKeys()[0], outputColumn);
      ASSERT_NE(column, agg->groupingKeys()[0]);

      auto project = std::move(precompute).maybeProject();
      ASSERT_NE(project.get(), input.get());
      ASSERT_EQ(project->columns().size(), 1);
      ASSERT_EQ(
          project->as<Project>()->columns().back()->alias(), toName("aaa"));
    }
  });
}

} // namespace
} // namespace facebook::axiom::optimizer
