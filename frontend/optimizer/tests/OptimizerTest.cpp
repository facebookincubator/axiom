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

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/frontend/optimizer/Plan.h"
#include "velox/frontend/optimizer/SchemaResolver.h"
#include "velox/frontend/optimizer/VeloxHistory.h"
#include "velox/frontend/optimizer/connectors/tests/TestConnector.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

namespace facebook::velox::optimizer::test {
namespace {

class OptimizerTest : public ::testing::Test {
 public:
  void SetUp() override {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
    rootPool_ = velox::memory::memoryManager()->addRootPool("optimizer_test");
    optimizerPool_ = rootPool_->addLeafChild("optimizer");

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();

    connector_ = std::make_shared<connector::TestConnector>("test");
    connector_->addTable(
        "orders",
        ROW(
            {{"o_orderkey", BIGINT()},
             {"o_custkey", BIGINT()},
             {"o_orderstatus", VARCHAR()},
             {"o_totalprice", DOUBLE()},
             {"o_orderdate", VARCHAR()},
             {"o_orderpriority", VARCHAR()},
             {"o_clerk", VARCHAR()},
             {"o_shippriority", INTEGER()},
             {"o_comment", VARCHAR()}}),
        50 * 1000);
    connector_->addTable(
        "customer",
        ROW(
            {{"c_custkey", BIGINT()},
             {"c_name", VARCHAR()},
             {"c_address", VARCHAR()},
             {"c_nationkey", BIGINT()},
             {"c_phone", VARCHAR()},
             {"c_acctbal", DOUBLE()},
             {"c_mktsegment", VARCHAR()},
             {"c_comment", VARCHAR()}}),
        1000);

    schema_ = std::make_shared<SchemaResolver>(connector_, "");
    history_ = std::make_unique<VeloxHistory>();

    queryCtx_ = core::QueryCtx::create();
    evaluator_ = std::make_unique<exec::SimpleExpressionEvaluator>(
        queryCtx_.get(), optimizerPool_.get());

    allocator_ = std::make_unique<HashStringAllocator>(optimizerPool_.get());
    context_ = std::make_unique<QueryGraphContext>(*allocator_);
    queryCtx() = context_.get();
  }

 protected:
  PlanAndStats optimize(core::PlanNodePtr plan) {
    runner::MultiFragmentPlan::Options opts;
    opts.numWorkers = 1;
    opts.numDrivers = 1;
    opts.queryId = "test";

    Locus locus(connector_->connectorId().c_str(), connector_.get());
    Schema schema("test", schema_.get(), &locus);

    OptimizerOptions optimizerOpts;
    Optimization opt(
        *plan, schema, *history_, queryCtx_, *evaluator_, optimizerOpts, opts);
    return opt.toVeloxPlan(std::move(opt.bestPlan()->op), opts);
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> optimizerPool_;
  std::shared_ptr<connector::TestConnector> connector_;
  std::shared_ptr<SchemaResolver> schema_;
  std::unique_ptr<VeloxHistory> history_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  std::unique_ptr<exec::SimpleExpressionEvaluator> evaluator_;
  std::unique_ptr<HashStringAllocator> allocator_;
  std::unique_ptr<QueryGraphContext> context_;
};

TEST_F(OptimizerTest, select) {
  auto orderType = ROW(
      {{"o_orderkey", BIGINT()},
       {"o_custkey", BIGINT()},
       {"o_totalprice", DOUBLE()}});
  auto plan = exec::test::PlanBuilder()
                  .tableScan("orders", orderType)
                  .filter("o_custkey < 100")
                  .project({"o_orderkey", "o_custkey", "o_totalprice"})
                  .planNode();

  auto optimizedPlan = optimize(plan);
  ASSERT_TRUE(optimizedPlan.plan != nullptr);
  auto fragments = optimizedPlan.plan->fragments();
  ASSERT_EQ(fragments.size(), 1);
  auto node = fragments[0].fragment.planNode;

  // Expect plan structured like the following:
  // Project[1][expressions:
  //        (dt1.o_orderkey:BIGINT, "t2.o_orderkey"),
  //        (dt1.o_custkey:BIGINT, "t2.o_custkey"),
  //        (dt1.o_totalprice:DOUBLE, "t2.o_totalprice")]
  //   TableScan[0][orders]
  ASSERT_EQ(node->name(), "Project");
  auto projectNode = std::dynamic_pointer_cast<const core::ProjectNode>(node);
  ASSERT_TRUE(projectNode != nullptr);

  auto scanNode = projectNode->sources()[0];
  ASSERT_EQ(scanNode->name(), "TableScan");
  auto tableScanNode =
      std::dynamic_pointer_cast<const core::TableScanNode>(scanNode);
  ASSERT_TRUE(tableScanNode != nullptr);
  ASSERT_EQ(tableScanNode->tableHandle()->name(), "orders");

  auto handle = std::dynamic_pointer_cast<const connector::TestTableHandle>(
      tableScanNode->tableHandle());
  ASSERT_TRUE(handle != nullptr);
  // these filters were created via filter pushdown
  const auto& filters = handle->filters();
  ASSERT_EQ(filters.size(), 1);
  ASSERT_EQ(filters[0]->toString(), "lt(\"o_custkey\",100)");
}

TEST_F(OptimizerTest, join) {
  auto orderType = ROW(
      {{"o_orderkey", BIGINT()},
       {"o_custkey", BIGINT()},
       {"o_totalprice", DOUBLE()}});
  auto customerType = ROW(
      {{"c_custkey", BIGINT()},
       {"c_name", VARCHAR()},
       {"c_acctbal", DOUBLE()}});

  auto plan =
      exec::test::PlanBuilder()
          .tableScan("orders", orderType)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              exec::test::PlanBuilder()
                  .tableScan("customer", customerType)
                  .planNode(),
              "",
              {"o_orderkey", "o_totalprice", "c_name", "c_acctbal"},
              core::JoinType::kInner)
          .project({"o_orderkey", "o_totalprice", "c_name", "c_acctbal"})
          .planNode();

  auto optimizedPlan = optimize(plan);
  ASSERT_TRUE(optimizedPlan.plan != nullptr);
  auto fragments = optimizedPlan.plan->fragments();
  ASSERT_EQ(fragments.size(), 1);
  auto node = fragments[0].fragment.planNode;

  // Expect plan structured like the following:
  // Project[3][expressions:
  //        (dt1.o_orderkey:BIGINT, "t2.o_orderkey"),
  //        (dt1.o_totalprice:DOUBLE, "t2.o_totalprice"),
  //        (dt1.c_name:VARCHAR, "t3.c_name"),
  //        (dt1.c_acctbal:DOUBLE, "t3.c_acctbal")]
  //   HashJoin[2][INNER t3.c_custkey=t2.o_custkey]
  //     TableScan[0][customer]
  //     TableScan[1][orders]
  ASSERT_EQ(node->name(), "Project");
  auto projectNode = std::dynamic_pointer_cast<const core::ProjectNode>(node);
  ASSERT_TRUE(projectNode != nullptr);

  auto joinNode = projectNode->sources()[0];
  ASSERT_EQ(joinNode->name(), "HashJoin");
  auto hashJoinNode =
      std::dynamic_pointer_cast<const core::HashJoinNode>(joinNode);
  ASSERT_TRUE(hashJoinNode != nullptr);
  ASSERT_EQ(hashJoinNode->joinType(), core::JoinType::kInner);

  auto inputNode1 = hashJoinNode->sources()[0];
  ASSERT_EQ(inputNode1->name(), "TableScan");
  auto scanNode1 =
      std::dynamic_pointer_cast<const core::TableScanNode>(inputNode1);
  ASSERT_TRUE(scanNode1 != nullptr);
  ASSERT_EQ(scanNode1->tableHandle()->name(), "customer");

  auto inputNode2 = hashJoinNode->sources()[1];
  ASSERT_EQ(inputNode2->name(), "TableScan");
  auto scanNode2 =
      std::dynamic_pointer_cast<const core::TableScanNode>(inputNode2);
  ASSERT_TRUE(scanNode2 != nullptr);
  ASSERT_EQ(scanNode2->tableHandle()->name(), "orders");
}

TEST_F(OptimizerTest, aggregation) {
  auto orderType = ROW(
      {{"o_orderkey", BIGINT()},
       {"o_custkey", BIGINT()},
       {"o_totalprice", DOUBLE()}});
  auto plan = exec::test::PlanBuilder()
                  .tableScan("orders", orderType)
                  .filter("o_totalprice > 1000.0")
                  .aggregation(
                      {"o_custkey"},
                      {},
                      {"sum(o_totalprice) AS total_price",
                       "count(o_orderkey) AS order_count"},
                      {},
                      core::AggregationNode::Step::kSingle,
                      false)
                  .planNode();

  auto optimizedPlan = optimize(plan);
  ASSERT_TRUE(optimizedPlan.plan != nullptr);
  auto fragments = optimizedPlan.plan->fragments();
  ASSERT_EQ(fragments.size(), 1);
  auto node = fragments[0].fragment.planNode;

  // Expect plan structured like the following:
  // Project[3][expressions:
  //    (dt1.o_custkey:BIGINT, "t2.o_custkey"),
  //    (dt1.total_price:DOUBLE, "dt1.total_price"),
  //    (dt1.order_count:BIGINT, "dt1.order_count")]
  //   Aggregation[2][FINAL [t2.o_custkey]]
  //     Aggregation[1][PARTIAL [t2.o_custkey]]
  //       TableScan[0][orders]
  ASSERT_EQ(node->name(), "Project");
  auto projectNode = std::dynamic_pointer_cast<const core::ProjectNode>(node);
  ASSERT_TRUE(projectNode != nullptr);

  auto finalAggNode = projectNode->sources()[0];
  ASSERT_EQ(finalAggNode->name(), "Aggregation");
  auto finalAggregationNode =
      std::dynamic_pointer_cast<const core::AggregationNode>(finalAggNode);
  ASSERT_TRUE(finalAggregationNode != nullptr);
  ASSERT_EQ(finalAggregationNode->step(), core::AggregationNode::Step::kFinal);

  auto partialAggNode = finalAggregationNode->sources()[0];
  ASSERT_EQ(partialAggNode->name(), "Aggregation");
  auto partialAggregationNode =
      std::dynamic_pointer_cast<const core::AggregationNode>(partialAggNode);
  ASSERT_TRUE(partialAggregationNode != nullptr);
  ASSERT_EQ(
      partialAggregationNode->step(), core::AggregationNode::Step::kPartial);

  auto scanNode = partialAggregationNode->sources()[0];
  ASSERT_EQ(scanNode->name(), "TableScan");
  auto tableScanNode =
      std::dynamic_pointer_cast<const core::TableScanNode>(scanNode);
  ASSERT_TRUE(tableScanNode != nullptr);
  ASSERT_EQ(tableScanNode->tableHandle()->name(), "orders");

  auto handle = std::dynamic_pointer_cast<const connector::TestTableHandle>(
      tableScanNode->tableHandle());
  ASSERT_TRUE(handle != nullptr);
  // these filters were created via filter pushdown
  const auto& filters = handle->filters();
  ASSERT_EQ(filters.size(), 1);
  ASSERT_EQ(filters[0]->toString(), "gt(\"o_totalprice\",1000)");
}

TEST_F(OptimizerTest, create) {
  auto orderType = ROW(
      {{"o_orderkey", BIGINT()},
       {"o_custkey", BIGINT()},
       {"o_totalprice", DOUBLE()}});
  auto plan =
      exec::test::PlanBuilder()
          .tableScan("orders", orderType)
          .project({"o_orderkey", "o_custkey", "o_totalprice"})
          .tableWrite("/tmp/test_output", dwio::common::FileFormat::DWRF)
          .planNode();

  auto optimizedPlan = optimize(plan);
  ASSERT_TRUE(optimizedPlan.plan != nullptr);
  auto fragments = optimizedPlan.plan->fragments();
  ASSERT_EQ(fragments.size(), 1);
  auto node = fragments[0].fragment.planNode;

  // Expect plan structured like the following:
  // TableWrite[2][InsertTableHandle]
  //   Project[1][expressions:
  //        (dt1.o_orderkey:BIGINT, "t2.o_orderkey"),
  //        (dt1.o_custkey:BIGINT, "t2.o_custkey"),
  //        (dt1.o_totalprice:DOUBLE, "t2.o_totalprice")]
  //     TableScan[0][orders]
  ASSERT_EQ(node->name(), "TableWrite");
  auto tableWriteNode =
      std::dynamic_pointer_cast<const core::TableWriteNode>(node);
  ASSERT_TRUE(tableWriteNode != nullptr);

  auto projectNode = tableWriteNode->sources()[0];
  ASSERT_EQ(projectNode->name(), "Project");
  auto projectNodeCast =
      std::dynamic_pointer_cast<const core::ProjectNode>(projectNode);
  ASSERT_TRUE(projectNodeCast != nullptr);

  auto scanNode = projectNodeCast->sources()[0];
  ASSERT_EQ(scanNode->name(), "TableScan");
  auto tableScanNode =
      std::dynamic_pointer_cast<const core::TableScanNode>(scanNode);
  ASSERT_TRUE(tableScanNode != nullptr);
  ASSERT_EQ(tableScanNode->tableHandle()->name(), "orders");
}

} // namespace
} // namespace facebook::velox::optimizer::test
