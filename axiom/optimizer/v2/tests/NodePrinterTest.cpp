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

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/ConstantFold.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/TranslatePass.h"
#include "axiom/optimizer/v2/tests/UnitTestBase.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

namespace lp = facebook::axiom::logical_plan;

using testing::ElementsAre;
using testing::Eq;
using testing::HasSubstr;
using testing::StartsWith;

class NodePrinterTest : public UnitTestBase {
 protected:
  static constexpr int32_t kMaxIterations = 10;

  static void SetUpTestCase() {
    velox::functions::prestosql::registerAllScalarFunctions();
    optimizer::FunctionRegistry::registerPrestoFunctions();
  }

  void SetUp() override {
    UnitTestBase::SetUp();
    veloxQueryCtx_ = velox::core::QueryCtx::create();
    evaluator_ = std::make_unique<velox::exec::SimpleExpressionEvaluator>(
        veloxQueryCtx_.get(), pool_.get());
    optimizer::OptimizerOptions options;
    options.recursionLimit = kMaxIterations;
    session_ = std::make_shared<optimizer::OptimizerSession>(
        veloxQueryCtx_->queryId(),
        "test",
        options,
        connector::ConnectorProperties{});
    schemaResolver_ = std::make_unique<connector::SchemaResolver>(
        connector::ConnectorMetadataRegistry::global());
    schema_ = std::make_unique<optimizer::Schema>(*schemaResolver_);
  }

  NodeCP translate(const lp::LogicalPlanNodePtr& plan) {
    optimizer::ConstantPlanRunner constantPlanRunner{veloxQueryCtx_};
    return TranslatePass::run(
               *plan,
               *schema_,
               *evaluator_,
               *builder_,
               *session_,
               constantPlanRunner)
        .root;
  }

  std::vector<std::string> toLines(NodeCP node) {
    std::vector<std::string> lines;
    folly::split('\n', node->toString(), lines);
    return lines;
  }

  lp::PlanBuilder singleRow(const std::string& name, int64_t value) {
    return lp::PlanBuilder(context_).values(
        ROW(name, velox::BIGINT()),
        std::vector<velox::Variant>{velox::Variant::row({value})});
  }

  lp::PlanBuilder::Context context_;
  std::shared_ptr<velox::core::QueryCtx> veloxQueryCtx_;
  std::unique_ptr<velox::exec::SimpleExpressionEvaluator> evaluator_;
  std::shared_ptr<optimizer::OptimizerSession> session_;
  std::unique_ptr<connector::SchemaResolver> schemaResolver_;
  std::unique_ptr<optimizer::Schema> schema_;
};

TEST_F(NodePrinterTest, fixedPoint) {
  auto anchor = singleRow("n", 1);
  auto step = lp::PlanBuilder(context_)
                  .recursiveRef("counter", anchor)
                  .filter("n < 10")
                  .project({"n + 1"})
                  .planNode();
  auto plan = anchor.fixedPoint("counter", step).build();

  // Names carry NameAllocator ids, so assert structure and drop the tail.
  EXPECT_THAT(
      toLines(translate(plan)),
      ElementsAre(
          StartsWith(
              "- FixedPoint[name=counter, maxIterations=10, recursiveNumDrivers=unplanned] ->"),
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
