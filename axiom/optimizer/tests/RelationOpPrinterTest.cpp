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
#include "axiom/optimizer/RelationOpPrinter.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;

namespace lp = facebook::axiom::logical_plan;

namespace facebook::axiom::optimizer {
namespace {

class RelationOpPrinterTest : public ::testing::Test {
 protected:
  static constexpr auto kTestConnectorId = "test";

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  void SetUp() override {
    rootPool_ = memory::memoryManager()->addRootPool("root");
    optimizerPool_ = rootPool_->addLeafChild("optimizer");

    connector_ = std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(connector_);
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);
  }

  std::vector<std::string> toLines(const std::string& sql) {
    return toLines(*parse(sql));
  }

  std::vector<std::string> toLines(const lp::LogicalPlanNode& logicalPlan) {
    std::vector<std::string> lines;
    optimize(logicalPlan, [&](const RelationOp& op) {
      const auto planString = RelationOpPrinter::toText(op);

      LOG(INFO) << std::endl << planString;
      folly::split('\n', planString, lines);
    });
    return lines;
  }

  std::string toOneline(const std::string& sql) {
    return toOneline(*parse(sql));
  }

  std::string toOneline(const lp::LogicalPlanNode& logicalPlan) {
    std::string planString;
    optimize(logicalPlan, [&](const RelationOp& op) {
      planString = RelationOpPrinter::toOneline(op);
    });
    return planString;
  }

  lp::LogicalPlanNodePtr parse(const std::string& sql) {
    ::axiom::sql::presto::PrestoParser parser{
        kTestConnectorId, optimizerPool_.get()};
    auto statement = parser.parse(sql);
    VELOX_CHECK(statement->isSelect());

    return statement->as<::axiom::sql::presto::SelectStatement>()->plan();
  }

  void optimize(
      const lp::LogicalPlanNode& logicalPlan,
      const std::function<void(const RelationOp& op)>& consume) {
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

    auto session = std::make_shared<Session>(veloxQueryCtx->queryId());

    Optimization opt{
        session,
        logicalPlan,
        *schemaResolver,
        history,
        veloxQueryCtx,
        evaluator,
        {.sampleJoins = false, .sampleFilters = false},
        {.numWorkers = 1, .numDrivers = 1}};

    auto* plan = opt.bestPlan();
    consume(*plan->op);
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> optimizerPool_;
  std::shared_ptr<connector::TestConnector> connector_;
};

TEST_F(RelationOpPrinterTest, basic) {
  connector_->addTable("t", ROW({"t_key", "a"}, INTEGER()));
  connector_->addTable("u", ROW({"u_key", "b"}, INTEGER()));
  connector_->addTable("v", ROW({"v_key", "c"}, INTEGER()));

  {
    const auto sql = "SELECT count(*) FROM t";

    auto lines = toLines(sql);
    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::StartsWith("Project"),
            testing::StartsWith("  Aggregation"),
            testing::StartsWith("    TableScan"),
            testing::StartsWith("      table: t"),
            testing::Eq("")));

    EXPECT_EQ("t", toOneline(sql));
  }

  {
    const auto sql = "SELECT count(*) FROM t LEFT JOIN u ON t_key = u_key";
    auto lines = toLines(sql);
    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::StartsWith("Project"),
            testing::StartsWith("  Aggregation"),
            testing::StartsWith("    Join LEFT"),
            testing::StartsWith("      TableScan"),
            testing::StartsWith("        table: t"),
            testing::StartsWith("      HashBuild"),
            testing::StartsWith("        TableScan"),
            testing::StartsWith("          table: u"),
            testing::Eq("")));

    EXPECT_EQ("(t LEFT u)", toOneline(sql));
  }

  EXPECT_EQ(
      "((t INNER u) INNER v)",
      toOneline(
          "SELECT count(*) FROM t, u, v WHERE t_key = u_key AND u_key = v_key"));

  {
    const auto sql = "SELECT count(*) FROM t UNION ALL SELECT count(*) FROM u";

    auto lines = toLines(sql);
    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::StartsWith("Project"),
            testing::StartsWith("  Aggregation"),
            testing::StartsWith("    TableScan"),
            testing::StartsWith("      table: t"),
            testing::Eq("")));

    EXPECT_EQ("t", toOneline(sql));
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
