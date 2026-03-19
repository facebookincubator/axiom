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
  static const inline std::string kDefaultSchema{
      connector::TestConnector::kDefaultSchema};

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

  std::vector<std::string> toLines(
      const std::string& sql,
      const RelationOpToTextOptions& options = {}) {
    return toLines(*parse(sql), options);
  }

  std::vector<std::string> toLines(
      const lp::LogicalPlanNode& logicalPlan,
      const RelationOpToTextOptions& options = {}) {
    std::vector<std::string> lines;
    optimize(logicalPlan, [&](const RelationOp& op) {
      const auto planString = RelationOpPrinter::toText(op, options);

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
    ::axiom::sql::presto::PrestoParser parser{kTestConnectorId, kDefaultSchema};
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

  lp::PlanBuilder::Context makeContext() const {
    return lp::PlanBuilder::Context{kTestConnectorId, kDefaultSchema};
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
    const auto sql =
        "SELECT sum(a + 1) as s FROM t WHERE a > 0 GROUP BY t_key HAVING sum(a + 1) < 10";

    auto lines = toLines(sql);
    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::StartsWith("Project"),
            testing::StartsWith("    "),
            testing::StartsWith("  Filter"),
            testing::HasSubstr("lt"), // s < 10
            testing::StartsWith("    Aggregation"),
            testing::StartsWith("        "), // sum(a + 1)
            testing::StartsWith("      Project"),
            testing::StartsWith("        "),
            testing::HasSubstr("plus"), // a + 1
            testing::StartsWith("        TableScan"),
            testing::StartsWith("          table: \"default\".\"t\""),
            testing::HasSubstr("gt"), // a > 0
            testing::Eq("")));

    EXPECT_EQ("agg(\"default\".\"t\")", toOneline(sql));
  }

  {
    const auto sql =
        "SELECT count(*) FROM t LEFT JOIN u ON t_key = u_key AND a > b";
    auto lines = toLines(sql);
    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::StartsWith("Project (redundant)"),
            testing::StartsWith("    "),
            testing::StartsWith("  Aggregation"),
            testing::StartsWith("      "), // count(*)
            testing::StartsWith("    Join LEFT Hash "),
            testing::StartsWith("      "), // t_key = u_key
            testing::HasSubstr("gt"), // a > b
            testing::StartsWith("      TableScan"),
            testing::StartsWith("        table: \"default\".\"t\""),
            testing::StartsWith("      HashBuild"),
            testing::StartsWith("        TableScan"),
            testing::StartsWith("          table: \"default\".\"u\""),
            testing::Eq("")));

    EXPECT_EQ(
        "agg((\"default\".\"t\" LEFT \"default\".\"u\"))", toOneline(sql));
  }

  EXPECT_EQ(
      "agg(((\"default\".\"t\" INNER \"default\".\"u\") INNER \"default\".\"v\"))",
      toOneline(
          "SELECT count(*) FROM t, u, v WHERE t_key = u_key AND u_key = v_key"));
}

TEST_F(RelationOpPrinterTest, unnest) {
  connector_->addTable("t", ROW({"t_key", "a"}, {INTEGER(), ARRAY(INTEGER())}));

  const auto sql = "SELECT t_key, x FROM t CROSS JOIN UNNEST(a) AS t(x)";
  auto lines = toLines(sql);
  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("Project (redundant)"),
          testing::StartsWith("    "),
          testing::StartsWith("    "),
          testing::StartsWith("  Unnest"),
          testing::StartsWith("      "),
          testing::StartsWith("    TableScan"),
          testing::StartsWith("      table: \"default\".\"t\""),
          testing::Eq("")));
}

TEST_F(RelationOpPrinterTest, limit) {
  connector_->addTable("t", ROW({"t_key", "a"}, INTEGER()));

  {
    auto plan = lp::PlanBuilder(makeContext()).tableScan("t").limit(10).build();
    auto lines = toLines(*plan);
    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::StartsWith("Project (redundant)"),
            testing::StartsWith("    "),
            testing::StartsWith("    "),
            testing::StartsWith("  Limit (10)"),
            testing::StartsWith("    TableScan"),
            testing::StartsWith("      table: \"default\".\"t\""),
            testing::Eq("")));
  }

  {
    auto plan = lp::PlanBuilder(makeContext())
                    .tableScan("t")
                    .offset(5)
                    .limit(10)
                    .build();
    auto lines = toLines(*plan);
    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::StartsWith("Project (redundant)"),
            testing::StartsWith("    "),
            testing::StartsWith("    "),
            testing::StartsWith("  Limit (10 offset 5)"),
            testing::StartsWith("    TableScan"),
            testing::StartsWith("      table: \"default\".\"t\""),
            testing::Eq("")));
  }
}

TEST_F(RelationOpPrinterTest, unionAll) {
  connector_->addTable("t", ROW({"t_key", "a"}, INTEGER()));
  connector_->addTable("u", ROW({"u_key", "b"}, INTEGER()));

  auto context = makeContext();
  auto plan = lp::PlanBuilder(context)
                  .tableScan("t")
                  .unionAll(lp::PlanBuilder(context).tableScan("u"))
                  .build();
  auto lines = toLines(*plan);
  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("Project (redundant)"),
          testing::StartsWith("    "),
          testing::StartsWith("    "),
          testing::StartsWith("  UnionAll"),
          testing::StartsWith("    Project (redundant)"),
          testing::StartsWith("        "),
          testing::StartsWith("        "),
          testing::StartsWith("      TableScan"),
          testing::StartsWith("        table: \"default\".\"t\""),
          testing::StartsWith("    Project"),
          testing::StartsWith("        "),
          testing::StartsWith("        "),
          testing::StartsWith("      TableScan"),
          testing::StartsWith("        table: \"default\".\"u\""),
          testing::Eq("")));
}

TEST_F(RelationOpPrinterTest, cost) {
  connector_->addTable("t", ROW({"t_key", "a"}, INTEGER()));
  connector_->addTable("u", ROW({"u_key", "b"}, INTEGER()));

  const auto sql =
      "SELECT count(*) FROM t LEFT JOIN u ON t_key = u_key AND a > b";
  auto lines = toLines(sql, {.includeCost = true});
  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("Project (redundant)"),
          testing::StartsWith("  Estimates: fanout"),
          testing::StartsWith("    "),
          testing::StartsWith("  Aggregation"),
          testing::StartsWith("    Estimates: fanout"),
          testing::StartsWith("      "), // count(*)
          testing::StartsWith("    Join LEFT Hash "),
          testing::StartsWith("      Estimates: fanout"),
          testing::StartsWith("      "), // t_key = u_key
          testing::HasSubstr("gt"), // a > b
          testing::StartsWith("      TableScan"),
          testing::StartsWith("        Estimates: fanout"),
          testing::StartsWith("        table: \"default\".\"t\""),
          testing::StartsWith("      HashBuild"),
          testing::StartsWith("        Estimates: fanout"),
          testing::StartsWith("        TableScan"),
          testing::StartsWith("          Estimates: fanout"),
          testing::StartsWith("          table: \"default\".\"u\""),
          testing::Eq("")));
}

TEST_F(RelationOpPrinterTest, maxDepth) {
  connector_->addTable("t", ROW({"t_key", "a"}, INTEGER()));
  connector_->addTable("u", ROW({"u_key", "b"}, INTEGER()));

  const auto sql =
      "SELECT count(*) FROM t LEFT JOIN u ON t_key = u_key AND a > b";

  {
    auto lines = toLines(sql, {.maxDepth = 0});

    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::StartsWith("Project (redundant)"),
            testing::StartsWith("    "),
            testing::Eq("")));
  }

  {
    auto lines = toLines(sql, {.maxDepth = 1});

    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::StartsWith("Project (redundant)"),
            testing::StartsWith("    "),
            testing::StartsWith("  Aggregation"),
            testing::StartsWith("      "), // count(*)
            testing::Eq("")));
  }

  {
    auto lines = toLines(sql, {.maxDepth = 3});

    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::StartsWith("Project (redundant)"),
            testing::StartsWith("    "),
            testing::StartsWith("  Aggregation"),
            testing::StartsWith("      "), // count(*)
            testing::StartsWith("    Join LEFT Hash "),
            testing::StartsWith("      "), // t_key = u_key
            testing::HasSubstr("gt"), // a > b
            testing::StartsWith("      TableScan"),
            testing::StartsWith("        table: \"default\".\"t\""),
            testing::StartsWith("      HashBuild"),
            testing::Eq("")));
  }

  {
    auto lines = toLines(sql, {.maxDepth = 10});

    EXPECT_THAT(
        lines,
        testing::ElementsAre(
            testing::StartsWith("Project (redundant)"),
            testing::StartsWith("    "),
            testing::StartsWith("  Aggregation"),
            testing::StartsWith("      "), // count(*)
            testing::StartsWith("    Join LEFT Hash "),
            testing::StartsWith("      "), // t_key = u_key
            testing::HasSubstr("gt"), // a > b
            testing::StartsWith("      TableScan"),
            testing::StartsWith("        table: \"default\".\"t\""),
            testing::StartsWith("      HashBuild"),
            testing::StartsWith("        TableScan"),
            testing::StartsWith("          table: \"default\".\"u\""),
            testing::Eq("")));
  }
}

TEST_F(RelationOpPrinterTest, groupId) {
  connector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), DOUBLE()}));

  auto lines =
      toLines("SELECT a, b, sum(c) AS total FROM t GROUP BY ROLLUP(a, b)");
  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("Project"),
          testing::StartsWith("    "), // dt1.a := t2.a
          testing::StartsWith("    "), // dt1.b := t2.b
          testing::StartsWith("    "), // dt1.total := dt1.total
          testing::StartsWith("    "), // grouping set ID column
          testing::StartsWith("  Aggregation"),
          testing::HasSubstr("sum(t2.c)"),
          testing::AllOf(
              testing::StartsWith("    GroupId"),
              testing::HasSubstr("[gk3, gk4], [gk3], []")),
          testing::HasSubstr("gk3 := "),
          testing::HasSubstr("gk4 := "),
          testing::HasSubstr("groupIdColumn:"),
          testing::StartsWith("      TableScan"),
          testing::HasSubstr("table:"),
          testing::Eq("")));

  auto oneline =
      toOneline("SELECT a, b, sum(c) AS total FROM t GROUP BY ROLLUP(a, b)");
  EXPECT_THAT(oneline, testing::HasSubstr("groupid("));
}

} // namespace
} // namespace facebook::axiom::optimizer
