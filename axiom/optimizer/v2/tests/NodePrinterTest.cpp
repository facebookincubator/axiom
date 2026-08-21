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

#include <folly/String.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/tests/UnitTestBase.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

using testing::AllOf;
using testing::ElementsAre;
using testing::Eq;
using testing::HasSubstr;
using testing::StartsWith;

class NodePrinterTest : public UnitTestBase {
 protected:
  static constexpr int32_t kMaxIterations = 10;

  std::vector<std::string> toLines(NodeCP node) {
    std::vector<std::string> lines;
    folly::split('\n', node->toString(), lines);
    return lines;
  }

  ExprCP literal(int32_t value) {
    return builder_->makeLiteral(
        velox::Variant(value), queryCtx()->toType(velox::INTEGER()));
  }

  ExprCP
  call(std::string_view name, ExprVector args, const velox::TypePtr& type) {
    return builder_->makeCall(
        toName(name),
        optimizer::Value(queryCtx()->toType(type)),
        std::move(args),
        FunctionSet{});
  }

  NodeCP workingTable(const ColumnVector& state) {
    return builder_->make<WorkingTable>(WorkingTable::Key{
        .name = toName("counter"),
        .outputColumns = state,
        .readMode = WorkingTableReadMode::kLatestDelta,
    });
  }

  // `step` reads the latest delta, keeps rows below a bound, and increments.
  NodeCP makeStep(const ColumnVector& state) {
    NodeCP filter = builder_->make<Filter>(Filter::Key{
        .input = workingTable(state),
        .predicates = {call("lt", {state[0], literal(10)}, velox::BOOLEAN())},
    });
    return builder_->make<Project>(Project::Key{
        .input = filter,
        .exprs = {call("plus", {state[0], literal(1)}, velox::INTEGER())},
        .outputColumns = {makeColumn("next", velox::INTEGER())},
    });
  }

  // `convergence` stops the loop once an iteration writes no rows.
  NodeCP makeConvergence(const ColumnVector& state) {
    ColumnCP count = makeColumn("count", velox::BIGINT());
    NodeCP aggregate = builder_->make<Aggregate>(Aggregate::Key{
        .input = workingTable(state),
        .groupingKeys = {},
        .aggregates = {builder_->makeAggregate(
            toName("count"),
            optimizer::Value(queryCtx()->toType(velox::BIGINT())),
            /*args=*/{},
            FunctionSet{},
            /*isDistinct=*/false,
            /*condition=*/nullptr,
            queryCtx()->toType(velox::BIGINT()),
            /*orderKeys=*/{},
            /*orderTypes=*/{})},
        .outputColumns = {count},
        .step = AggregateStep::kSingle,
    });
    return builder_->make<Project>(Project::Key{
        .input = aggregate,
        .exprs = {builder_->makeCall(
            toName("eq"),
            optimizer::Value(queryCtx()->toType(velox::BOOLEAN())),
            {count,
             builder_->makeLiteral(
                 velox::Variant(int64_t{0}),
                 queryCtx()->toType(velox::BIGINT()))},
            FunctionSet{})},
        .outputColumns = {makeColumn("converged", velox::BOOLEAN())},
    });
  }
};

TEST_F(NodePrinterTest, fixedPoint) {
  ColumnVector state{makeColumn("n", velox::INTEGER())};
  NodeCP anchor = builder_->makeEmptyValues(state);
  NodeCP fixedPoint = builder_->make<FixedPoint>(FixedPoint::Key{
      .anchor = anchor,
      .step = makeStep(state),
      .convergence = makeConvergence(state),
      .name = toName("counter"),
      .outputColumns = state,
      .maxIterations = kMaxIterations,
      .recursiveNumDrivers = std::nullopt,
  });

  EXPECT_THAT(
      toLines(fixedPoint),
      ElementsAre(
          AllOf(
              StartsWith(
                  "- FixedPoint[name=counter, maxIterations=10, recursiveNumDrivers=unplanned] ->"),
              HasSubstr(":INTEGER")),
          Eq("  anchor:"),
          StartsWith("  - Values ->"),
          Eq("  step:"),
          StartsWith("  - Project ->"),
          HasSubstr(":= plus("),
          StartsWith("    - Filter ->"),
          StartsWith("      predicate: lt("),
          StartsWith(
              "      - WorkingTable[name=counter, readMode=latestDelta] ->"),
          Eq("  convergence:"),
          StartsWith("  - Project ->"),
          HasSubstr(":= eq("),
          StartsWith("    - Aggregate ->"),
          Eq("      aggregates: count()"),
          StartsWith(
              "      - WorkingTable[name=counter, readMode=latestDelta] ->"),
          Eq("")));
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
