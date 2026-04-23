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
#include <memory>

#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/pyspark/SparkToAxiom.h"
#include "axiom/pyspark/SparkVeloxConverter.h"
#include "axiom/pyspark/third-party/protos/relations.grpc.pb.h" // @manual=fbcode//axiom/pyspark/third-party/protos:collagen_proto-cpp
#include "velox/common/memory/Memory.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook;

namespace axiom::collagen::test {
namespace {

void registerTestConnector(const std::string& connectorId) {
  auto connector =
      std::make_shared<facebook::axiom::connector::TestConnector>(connectorId);

  connector->addTable(
      "training_table", velox::ROW({"viewer_rid"}, {velox::BIGINT()}));
  connector->addTable(
      "feature_table", velox::ROW({"primary_rid"}, {velox::BIGINT()}));

  velox::connector::registerConnector(connector);
}

class SparkToAxiomSetOperationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    velox::functions::prestosql::registerAllScalarFunctions();

    // Initialize memory manager and create a memory pool for testing
    velox::memory::MemoryManager::initialize({});
    pool_ = velox::memory::MemoryManager::getInstance()->addLeafPool();

    // Set up test connector
    registerTestConnector("test_connector");
  }

  void TearDown() override {
    // Cleanup is handled by the connector destructor
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

// Test the toAxiomSetOperation conversion function
TEST_F(SparkToAxiomSetOperationTest, toAxiomSetOperationConversion) {
  // Test UNION without ALL flag
  EXPECT_EQ(
      facebook::axiom::logical_plan::SetOperation::kUnion,
      toAxiomSetOperation(
          spark::connect::SetOperation::SET_OP_TYPE_UNION, false));

  // Test UNION with ALL flag
  EXPECT_EQ(
      facebook::axiom::logical_plan::SetOperation::kUnionAll,
      toAxiomSetOperation(
          spark::connect::SetOperation::SET_OP_TYPE_UNION, true));

  // Test INTERSECT
  EXPECT_EQ(
      facebook::axiom::logical_plan::SetOperation::kIntersect,
      toAxiomSetOperation(
          spark::connect::SetOperation::SET_OP_TYPE_INTERSECT, false));

  // Test EXCEPT
  EXPECT_EQ(
      facebook::axiom::logical_plan::SetOperation::kExcept,
      toAxiomSetOperation(
          spark::connect::SetOperation::SET_OP_TYPE_EXCEPT, false));
}

// Helper function to create a simple table relation for testing
spark::connect::Relation createTestTableRelation(
    const std::string& tableName,
    int64_t planId) {
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(planId);

  auto* read = relation.mutable_read();
  auto* namedTable = read->mutable_named_table();
  namedTable->set_unparsed_identifier(tableName);

  return relation;
}

// Helper function to create a SetOperation relation
spark::connect::Relation createSetOperationRelation(
    const spark::connect::Relation& left,
    const spark::connect::Relation& right,
    spark::connect::SetOperation::SetOpType opType,
    bool isAll = false,
    int64_t planId = 1) {
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(planId);

  auto* setOp = relation.mutable_set_op();
  *setOp->mutable_left_input() = left;
  *setOp->mutable_right_input() = right;
  setOp->set_set_op_type(opType);
  setOp->set_is_all(isAll);

  return relation;
}

TEST_F(SparkToAxiomSetOperationTest, visitSetOperationUnion) {
  // Create left and right table relations
  auto leftRelation = createTestTableRelation("training_table", 2);
  auto rightRelation = createTestTableRelation("feature_table", 3);

  // Create UNION operation
  auto unionRelation = createSetOperationRelation(
      leftRelation,
      rightRelation,
      spark::connect::SetOperation::SET_OP_TYPE_UNION,
      false, // isAll = false
      1);

  // Convert to Axiom
  SparkToAxiom converter("test_connector", "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.SparkPlanVisitor::visit(unionRelation, context);

  // Verify the result
  auto planNode = converter.planNode();
  ASSERT_NE(planNode, nullptr);
  EXPECT_EQ(planNode->kind(), facebook::axiom::logical_plan::NodeKind::kSet);

  auto setNode = planNode->as<facebook::axiom::logical_plan::SetNode>();
  ASSERT_NE(setNode, nullptr);
  EXPECT_EQ(
      setNode->operation(),
      facebook::axiom::logical_plan::SetOperation::kUnion);
  EXPECT_EQ(setNode->inputs().size(), 2);
}

TEST_F(SparkToAxiomSetOperationTest, visitSetOperationUnionAll) {
  // Create left and right table relations
  auto leftRelation = createTestTableRelation("training_table", 2);
  auto rightRelation = createTestTableRelation("feature_table", 3);

  // Create UNION ALL operation
  auto unionAllRelation = createSetOperationRelation(
      leftRelation,
      rightRelation,
      spark::connect::SetOperation::SET_OP_TYPE_UNION,
      true, // isAll = true
      1);

  // Convert to Axiom
  SparkToAxiom converter("test_connector", "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.SparkPlanVisitor::visit(unionAllRelation, context);

  // Verify the result
  auto planNode = converter.planNode();
  ASSERT_NE(planNode, nullptr);
  EXPECT_EQ(planNode->kind(), facebook::axiom::logical_plan::NodeKind::kSet);

  auto setNode = planNode->as<facebook::axiom::logical_plan::SetNode>();
  ASSERT_NE(setNode, nullptr);
  EXPECT_EQ(
      setNode->operation(),
      facebook::axiom::logical_plan::SetOperation::kUnionAll);
  EXPECT_EQ(setNode->inputs().size(), 2);
}

TEST_F(SparkToAxiomSetOperationTest, visitSetOperationIntersect) {
  // Create left and right table relations
  auto leftRelation = createTestTableRelation("training_table", 2);
  auto rightRelation = createTestTableRelation("feature_table", 3);

  // Create INTERSECT operation
  auto intersectRelation = createSetOperationRelation(
      leftRelation,
      rightRelation,
      spark::connect::SetOperation::SET_OP_TYPE_INTERSECT,
      false,
      1);

  // Convert to Axiom
  SparkToAxiom converter("test_connector", "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.SparkPlanVisitor::visit(intersectRelation, context);

  // Verify the result
  auto planNode = converter.planNode();
  ASSERT_NE(planNode, nullptr);
  EXPECT_EQ(planNode->kind(), facebook::axiom::logical_plan::NodeKind::kSet);

  auto setNode = planNode->as<facebook::axiom::logical_plan::SetNode>();
  ASSERT_NE(setNode, nullptr);
  EXPECT_EQ(
      setNode->operation(),
      facebook::axiom::logical_plan::SetOperation::kIntersect);
  EXPECT_EQ(setNode->inputs().size(), 2);
}

TEST_F(SparkToAxiomSetOperationTest, visitSetOperationExcept) {
  // Create left and right table relations
  auto leftRelation = createTestTableRelation("training_table", 2);
  auto rightRelation = createTestTableRelation("feature_table", 3);

  // Create EXCEPT operation
  auto exceptRelation = createSetOperationRelation(
      leftRelation,
      rightRelation,
      spark::connect::SetOperation::SET_OP_TYPE_EXCEPT,
      false,
      1);

  // Convert to Axiom
  SparkToAxiom converter("test_connector", "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.SparkPlanVisitor::visit(exceptRelation, context);

  // Verify the result
  auto planNode = converter.planNode();
  ASSERT_NE(planNode, nullptr);
  EXPECT_EQ(planNode->kind(), facebook::axiom::logical_plan::NodeKind::kSet);

  auto setNode = planNode->as<facebook::axiom::logical_plan::SetNode>();
  ASSERT_NE(setNode, nullptr);
  EXPECT_EQ(
      setNode->operation(),
      facebook::axiom::logical_plan::SetOperation::kExcept);
  EXPECT_EQ(setNode->inputs().size(), 2);
}

TEST_F(SparkToAxiomSetOperationTest, visitSetOperationWithComplexInputs) {
  // Create left table relation
  auto leftTableRelation = createTestTableRelation("training_table", 3);

  // Create right table relation
  auto rightTableRelation = createTestTableRelation("feature_table", 4);

  // Create a nested UNION operation as the left input
  auto nestedUnionRelation = createSetOperationRelation(
      leftTableRelation,
      rightTableRelation,
      spark::connect::SetOperation::SET_OP_TYPE_UNION,
      false,
      2);

  // Create another table for the right input
  auto rightRelation = createTestTableRelation("training_table", 5);

  // Create final INTERSECT operation with nested input
  auto finalRelation = createSetOperationRelation(
      nestedUnionRelation,
      rightRelation,
      spark::connect::SetOperation::SET_OP_TYPE_INTERSECT,
      false,
      1);

  // Convert to Axiom
  SparkToAxiom converter("test_connector", "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.SparkPlanVisitor::visit(finalRelation, context);

  // Verify the result
  auto planNode = converter.planNode();
  ASSERT_NE(planNode, nullptr);
  EXPECT_EQ(planNode->kind(), facebook::axiom::logical_plan::NodeKind::kSet);

  auto setNode = planNode->as<facebook::axiom::logical_plan::SetNode>();
  ASSERT_NE(setNode, nullptr);
  EXPECT_EQ(
      setNode->operation(),
      facebook::axiom::logical_plan::SetOperation::kIntersect);
  EXPECT_EQ(setNode->inputs().size(), 2);

  // Verify the left input is also a set operation
  auto leftInput = setNode->inputs()[0];
  EXPECT_EQ(leftInput->kind(), facebook::axiom::logical_plan::NodeKind::kSet);

  auto leftSetNode = leftInput->as<facebook::axiom::logical_plan::SetNode>();
  ASSERT_NE(leftSetNode, nullptr);
  EXPECT_EQ(
      leftSetNode->operation(),
      facebook::axiom::logical_plan::SetOperation::kUnion);

  // Verify the right input is a table scan
  auto rightInput = setNode->inputs()[1];
  EXPECT_EQ(
      rightInput->kind(), facebook::axiom::logical_plan::NodeKind::kTableScan);
}

} // namespace
} // namespace axiom::collagen::test
