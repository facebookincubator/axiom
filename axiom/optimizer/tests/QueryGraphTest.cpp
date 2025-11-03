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
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;
namespace facebook::axiom::optimizer {
namespace {

class QueryGraphTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  void SetUp() override {
    rootPool_ = memory::memoryManager()->addRootPool("root");
    optimizerPool_ = rootPool_->addLeafChild("optimizer");
    allocator_ =
        std::make_unique<velox::HashStringAllocator>(optimizerPool_.get());
    context_ = std::make_unique<QueryGraphContext>(*allocator_);
    queryCtx() = context_.get();
  }

  void TearDown() override {
    queryCtx() = nullptr;
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> optimizerPool_;
  std::unique_ptr<velox::HashStringAllocator> allocator_;
  std::unique_ptr<QueryGraphContext> context_;
};

TEST_F(QueryGraphTest, sameOrEqual) {
  Variant variant1 = 1;
  Variant variant2 = 2;
  auto* literal1 = make<Literal>(Value{toType(velox::INTEGER()), 1}, &variant1);
  auto* literal2 = make<Literal>(Value{toType(velox::INTEGER()), 1}, &variant1);
  auto* literal3 = make<Literal>(Value{toType(velox::INTEGER()), 1}, &variant2);

  {
    SCOPED_TRACE("Literals");

    EXPECT_TRUE(literal1->sameOrEqual(*literal1));
    EXPECT_TRUE(literal2->sameOrEqual(*literal2));
    EXPECT_TRUE(literal3->sameOrEqual(*literal3));

    EXPECT_TRUE(literal1->sameOrEqual(*literal2));
    EXPECT_TRUE(literal2->sameOrEqual(*literal1));

    EXPECT_FALSE(literal1->sameOrEqual(*literal3));
    EXPECT_FALSE(literal3->sameOrEqual(*literal1));
  }
  {
    SCOPED_TRACE("Calls(name different)");

    auto* call1 = make<Call>(
        "least",
        Value{toType(velox::INTEGER()), 1},
        ExprVector{literal1, literal2},
        FunctionSet{});
    auto* call2 = make<Call>(
        "greatest",
        Value{toType(velox::INTEGER()), 1},
        ExprVector{literal1, literal2},
        FunctionSet{});

    EXPECT_TRUE(call1->sameOrEqual(*call1));
    EXPECT_TRUE(call2->sameOrEqual(*call2));

    EXPECT_FALSE(call1->sameOrEqual(*call2));
    EXPECT_FALSE(call2->sameOrEqual(*call1));
  }
  {
    SCOPED_TRACE("Calls(args count different)");

    auto* call1 = make<Call>(
        "least",
        Value{toType(velox::INTEGER()), 1},
        ExprVector{literal1, literal1},
        FunctionSet{});
    auto* call2 = make<Call>(
        "least",
        Value{toType(velox::INTEGER()), 1},
        ExprVector{literal1, literal1, literal1},
        FunctionSet{});

    EXPECT_TRUE(call1->sameOrEqual(*call1));
    EXPECT_TRUE(call2->sameOrEqual(*call2));

    EXPECT_FALSE(call1->sameOrEqual(*call2));
    EXPECT_FALSE(call2->sameOrEqual(*call1));
  }
  {
    SCOPED_TRACE("Calls(args different)");

    auto* call1 = make<Call>(
        "least",
        Value{toType(velox::INTEGER()), 1},
        ExprVector{literal1, literal2},
        FunctionSet{});
    auto* call2 = make<Call>(
        "least",
        Value{toType(velox::INTEGER()), 1},
        ExprVector{literal1, literal2},
        FunctionSet{});
    auto* call3 = make<Call>(
        "least",
        Value{toType(velox::INTEGER()), 1},
        ExprVector{literal1, literal3},
        FunctionSet{});

    EXPECT_TRUE(call1->sameOrEqual(*call1));
    EXPECT_TRUE(call2->sameOrEqual(*call2));
    EXPECT_TRUE(call3->sameOrEqual(*call3));

    EXPECT_TRUE(call1->sameOrEqual(*call2));
    EXPECT_TRUE(call2->sameOrEqual(*call1));

    EXPECT_FALSE(call1->sameOrEqual(*call3));
    EXPECT_FALSE(call3->sameOrEqual(*call1));
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
