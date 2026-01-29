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

#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "velox/common/serialization/Serializable.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/type/Type.h"

using namespace facebook::velox;

namespace facebook::axiom::logical_plan {
namespace {

class LogicalPlanNodeSerdeTest : public testing::Test {
 protected:
  static constexpr auto kTestConnectorId = "test_connector";

  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    Type::registerSerDe();
    Expr::registerSerDe();
    LogicalPlanNode::registerSerDe();
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  void SetUp() override {
    connector_ = std::make_shared<connector::TestConnector>(kTestConnectorId);
    connector_->addTable(
        "test_table",
        ROW({"a", "b", "arr"}, {BIGINT(), VARCHAR(), ARRAY(BIGINT())}));
    connector_->addTable(
        "output_table", ROW({"col_a", "col_b"}, {BIGINT(), VARCHAR()}));
    velox::connector::registerConnector(connector_);
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);
    connector_.reset();
  }

  // Verifies round-trip serialization by comparing toString() outputs.
  void testRoundTrip(const LogicalPlanNodePtr& node) {
    auto serialized = node->serialize();
    auto deserialized =
        ISerializable::deserialize<LogicalPlanNode>(serialized, nullptr);
    ASSERT_NE(deserialized, nullptr);
    EXPECT_EQ(node->toString(), deserialized->toString());
  }

  std::shared_ptr<connector::TestConnector> connector_;
};

TEST_F(LogicalPlanNodeSerdeTest, valuesNodeWithVariants) {
  auto plan = PlanBuilder()
                  .values(
                      ROW({"a"}, {BIGINT()}),
                      std::vector<Variant>{Variant::row({123LL})})
                  .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, tableScanNode) {
  auto plan = std::make_shared<TableScanNode>(
      "scan_0",
      ROW({"a", "b"}, {BIGINT(), VARCHAR()}),
      "test_connector",
      "test_table",
      std::vector<std::string>{"col_a", "col_b"});
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, filterNode) {
  auto plan = PlanBuilder()
                  .values(
                      ROW({"a"}, {BIGINT()}),
                      std::vector<Variant>{Variant::row({123LL})})
                  .filter("a > 10")
                  .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, projectNode) {
  auto plan = PlanBuilder()
                  .values(
                      ROW({"a"}, {BIGINT()}),
                      std::vector<Variant>{Variant::row({123LL})})
                  .project({"a", "a + 1 as b"})
                  .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, aggregateNode) {
  auto plan = PlanBuilder()
                  .values(
                      ROW({"a", "b"}, {BIGINT(), BIGINT()}),
                      std::vector<Variant>{Variant::row({1LL, 2LL})})
                  .aggregate({"a"}, {"sum(b) as total"})
                  .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, joinNode) {
  auto right = PlanBuilder().values(
      ROW({"b"}, {BIGINT()}), std::vector<Variant>{Variant::row({1LL})});

  auto plan =
      PlanBuilder()
          .values(
              ROW({"a"}, {BIGINT()}), std::vector<Variant>{Variant::row({1LL})})
          .join(right, "a = b", JoinType::kInner)
          .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, joinNodeCrossJoin) {
  auto right = PlanBuilder().values(
      ROW({"b"}, {BIGINT()}), std::vector<Variant>{Variant::row({1LL})});

  auto plan =
      PlanBuilder()
          .values(
              ROW({"a"}, {BIGINT()}), std::vector<Variant>{Variant::row({1LL})})
          .crossJoin(right)
          .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, sortNode) {
  auto plan = PlanBuilder()
                  .values(
                      ROW({"a"}, {BIGINT()}),
                      std::vector<Variant>{Variant::row({123LL})})
                  .sort({"a ASC NULLS FIRST"})
                  .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, limitNode) {
  auto plan = PlanBuilder()
                  .values(
                      ROW({"a"}, {BIGINT()}),
                      std::vector<Variant>{Variant::row({123LL})})
                  .limit(10, 100)
                  .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, unionAllNode) {
  auto other = PlanBuilder().values(
      ROW({"a"}, {BIGINT()}), std::vector<Variant>{Variant::row({2LL})});

  auto plan =
      PlanBuilder()
          .values(
              ROW({"a"}, {BIGINT()}), std::vector<Variant>{Variant::row({1LL})})
          .unionAll(other)
          .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, unnestNode) {
  auto plan = PlanBuilder()
                  .tableScan(kTestConnectorId, "test_table", {"arr"})
                  .unnest({Col("arr").unnestAs("elem")})
                  .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, unnestNodeWithOrdinality) {
  auto plan = PlanBuilder()
                  .tableScan(kTestConnectorId, "test_table", {"arr"})
                  .unnest({Col("arr").unnestAs("elem")}, true)
                  .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, sampleNode) {
  auto plan = PlanBuilder()
                  .values(
                      ROW({"a"}, {BIGINT()}),
                      std::vector<Variant>{Variant::row({123LL})})
                  .sample(50.0, SampleNode::SampleMethod::kBernoulli)
                  .build();
  testRoundTrip(plan);
}

TEST_F(LogicalPlanNodeSerdeTest, tableWriteNode) {
  auto plan = PlanBuilder()
                  .tableScan(kTestConnectorId, "test_table", {"a", "b"})
                  .tableWrite(
                      kTestConnectorId,
                      "output_table",
                      WriteKind::kInsert,
                      {"col_a", "col_b"},
                      {"a", "b"},
                      {{"compression", "gzip"}})
                  .build();
  testRoundTrip(plan);
}

} // namespace
} // namespace facebook::axiom::logical_plan
