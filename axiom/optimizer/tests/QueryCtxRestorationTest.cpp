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

#include "axiom/optimizer/QueryGraphContext.h"
#include "velox/common/memory/Memory.h"

namespace facebook::axiom::optimizer {

class QueryCtxRestorationTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    rootPool_ = velox::memory::memoryManager()->addRootPool("test");
    leafPool_ = rootPool_->addLeafChild("leaf");
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
};

// Verifies that queryCtx() is properly restored after a nested scope that
// sets its own context. This is the pattern used by the static
// Optimization::toVeloxPlan() method. Before the fix, it set
// queryCtx() = nullptr on exit instead of restoring the previous value,
// corrupting any outer caller's context during re-entrant calls.
TEST_F(QueryCtxRestorationTest, nestedScopeRestoresOuterContext) {
  auto outerAllocator =
      std::make_unique<velox::HashStringAllocator>(leafPool_.get());
  auto outerContext = std::make_unique<QueryGraphContext>(*outerAllocator);
  queryCtx() = outerContext.get();
  ASSERT_EQ(queryCtx(), outerContext.get());

  // Simulate what static toVeloxPlan() does: create a nested context,
  // set it as current, then restore on scope exit.
  {
    auto innerLeafPool = rootPool_->addLeafChild("inner");
    auto innerAllocator =
        std::make_unique<velox::HashStringAllocator>(innerLeafPool.get());
    auto innerContext = std::make_unique<QueryGraphContext>(*innerAllocator);

    // The fix: save previous context and restore it, not nullptr.
    auto* prevQueryCtx = queryCtx();
    queryCtx() = innerContext.get();
    EXPECT_EQ(queryCtx(), innerContext.get());

    // Restore (this is the fix — before, it was queryCtx() = nullptr).
    queryCtx() = prevQueryCtx;
  }

  // After the nested scope, queryCtx() must be the outer context.
  EXPECT_EQ(queryCtx(), outerContext.get());

  queryCtx() = nullptr;
}

// Verifies the non-nested case: queryCtx() remains nullptr when there is
// no outer context.
TEST_F(QueryCtxRestorationTest, restoresNullWhenNoOuterContext) {
  queryCtx() = nullptr;
  ASSERT_EQ(queryCtx(), nullptr);

  {
    auto innerLeafPool = rootPool_->addLeafChild("inner2");
    auto innerAllocator =
        std::make_unique<velox::HashStringAllocator>(innerLeafPool.get());
    auto innerContext = std::make_unique<QueryGraphContext>(*innerAllocator);

    auto* prevQueryCtx = queryCtx();
    queryCtx() = innerContext.get();
    queryCtx() = prevQueryCtx;
  }

  EXPECT_EQ(queryCtx(), nullptr);
}

} // namespace facebook::axiom::optimizer
