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

#pragma once

#include <gtest/gtest.h>
#include <memory>
#include <string_view>

#include "axiom/optimizer/PlanObject.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/v2/Builder.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"

namespace facebook::axiom::optimizer::v2::test {

/// Base for unit tests that construct optimizer IR directly via
/// `make<T>` / `Builder` rather than going through the full
/// SQL/Logical Plan → translate → emit pipeline. Sets up the per-test
/// `QueryGraphContext`, arena allocator, and Velox memory pool so
/// `queryCtx()` is valid for the duration of the test.
///
/// Use this for substrate-level tests (RelationSet, Relation,
/// JoinHypergraph, NodePrinter, etc.). For tests that need a full
/// pipeline, inherit from `QueryTestBase` instead.
class UnitTestBase : public ::testing::Test {
 protected:
  void SetUp() override {
    velox::memory::MemoryManager::testingSetInstance({});
    pool_ = velox::memory::memoryManager()->addLeafPool(
        ::testing::UnitTest::GetInstance()->current_test_info()->name());
    allocator_ = std::make_unique<velox::HashStringAllocator>(pool_.get());
    context_ = std::make_unique<optimizer::QueryGraphContext>(*allocator_);
    optimizer::queryCtx() = context_.get();
    builder_ = std::make_unique<Builder>();
  }

  void TearDown() override {
    builder_.reset();
    optimizer::queryCtx() = nullptr;
    context_.reset();
    allocator_.reset();
    pool_.reset();
  }

  /// Synthesizes a `Column` with a `prefix`-derived unique name and
  /// the given Velox type. NDV is unknown.
  optimizer::ColumnCP makeColumn(
      std::string_view prefix,
      const velox::TypePtr& type) {
    return optimizer::Column::create(
        prefix, optimizer::Value(optimizer::queryCtx()->toType(type)));
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<velox::HashStringAllocator> allocator_;
  std::unique_ptr<optimizer::QueryGraphContext> context_;
  std::unique_ptr<Builder> builder_;
};

} // namespace facebook::axiom::optimizer::v2::test
