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

#include "axiom/optimizer/DerivedTablePrinter.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/optimizer/tests/PrestoParser.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;

namespace lp = facebook::axiom::logical_plan;

namespace facebook::axiom::optimizer {
namespace {

class DerivedTablePrinterTest : public ::testing::Test {
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
    test::PrestoParser parser{kTestConnectorId, optimizerPool_.get()};
    auto statement = parser.parse(sql);
    VELOX_CHECK(statement->isSelect());

    const auto& plan = *statement->asUnchecked<test::SelectStatement>()->plan();

    return toLines(plan);
  }

  std::vector<std::string> toLines(const lp::LogicalPlanNode& plan) {
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

    auto session = std::make_shared<Session>(veloxQueryCtx->queryId());

    Optimization opt{
        session,
        plan,
        schema,
        history,
        veloxQueryCtx,
        evaluator,
        {.sampleJoins = false, .sampleFilters = false},
        {.numWorkers = 1, .numDrivers = 1}};

    const auto dtString = DerivedTablePrinter::toText(*opt.rootDt());

    LOG(INFO) << std::endl << dtString;

    std::vector<std::string> lines;
    folly::split('\n', dtString, lines);

    return lines;
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> optimizerPool_;
  std::shared_ptr<connector::TestConnector> connector_;
};

TEST_F(DerivedTablePrinterTest, basic) {
  connector_->addTable("t", ROW({"a", "b"}, INTEGER()));
  connector_->addTable("u", ROW({"x", "y"}, INTEGER()));

  {
    auto lines = toLines("SELECT count(*) FROM t");
    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::Eq("dt1: count"),
            testing::Eq("  output:"),
            testing::Eq("    count := dt1.count"),
            testing::Eq("  tables: t2"),
            testing::Eq("  aggregates: count() AS count"),
            testing::Eq(""),
            testing::Eq("t2: "),
            testing::Eq("  table: t"),
            testing::Eq("")));
  }

  {
    auto lines = toLines(
        "SELECT a, sum(b) as s FROM t WHERE a > b "
        "GROUP BY 1 "
        "HAVING s > 100 "
        "ORDER BY 1 DESC");

    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::Eq("dt1: a, s"),
            testing::Eq("  output:"),
            testing::Eq("    a := t2.a"),
            testing::Eq("    s := dt1.s"),
            testing::Eq("  tables: t2"),
            testing::Eq("  aggregates: sum(t2.b) AS s"),
            testing::Eq("  grouping keys: t2.a"),
            testing::Eq("  having: gt(dt1.s, 100)"),
            testing::Eq("  orderBy: t2.a DESC NULLS LAST"),
            testing::Eq(""),
            testing::Eq("t2: a, b"),
            testing::Eq("  table: t"),
            testing::Eq("  multi-column filters: gt(t2.a, t2.b)"),
            testing::Eq("")));
  }

  {
    auto lines = toLines(
        "SELECT a, sum(b * y) FROM t LEFT JOIN u ON a = x "
        "GROUP BY 1");

    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::Eq("dt1: a, sum"),
            testing::Eq("  output:"),
            testing::Eq("    a := t2.a"),
            testing::Eq("    sum := dt1.sum"),
            testing::Eq("  tables: t2, t3"),
            testing::Eq("  joins:"),
            testing::Eq("    t2 LEFT t3 ON t2.a = t3.x"),
            testing::Eq("  syntactic join order: 3, 8"),
            testing::Eq("  aggregates: sum(multiply(t2.b, t3.y)) AS sum"),
            testing::Eq("  grouping keys: t2.a"),
            testing::Eq(""),
            testing::Eq("t2: a, b"),
            testing::Eq("  table: t"),
            testing::Eq(""),
            testing::Eq("t3: x, y"),
            testing::Eq("  table: u"),
            testing::Eq("")));
  }
}

TEST_F(DerivedTablePrinterTest, write) {
  connector_->addTable("c", ROW({"a", "b"}, INTEGER()));
  connector_->addTable("z", ROW({"x", "y"}, INTEGER()));

  auto plan = lp::PlanBuilder()
                  .tableScan(kTestConnectorId, "c")
                  .tableWrite(
                      kTestConnectorId,
                      "z",
                      lp::WriteKind::kInsert,
                      {"y", "x"},
                      {"a", "b"})
                  .build();

  auto lines = toLines(*plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("dt1: rows"),
          testing::Eq("  output:"),
          testing::Eq("    rows := dt1.rows"),
          testing::Eq("  tables: dt2"),
          testing::Eq("  write (INSERT) to: z"),
          testing::Eq("    columns: dt2.b, dt2.a"),
          testing::Eq(""),
          testing::Eq("dt2: a, b"),
          testing::Eq("  output:"),
          testing::Eq("    a := t3.a"),
          testing::Eq("    b := t3.b"),
          testing::Eq("  tables: t3"),
          testing::Eq(""),
          testing::Eq("t3: a, b"),
          testing::Eq("  table: c"),
          testing::Eq("")));
}

} // namespace
} // namespace facebook::axiom::optimizer
