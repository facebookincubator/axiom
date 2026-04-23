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
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/pyspark/SparkToAxiom.h"
#include "axiom/pyspark/third-party/protos/relations.pb.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/Connector.h"
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

class SparkToAxiomWithColumnsRenamedTest : public ::testing::Test {
 public:
  void SetUp() override {
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::memory::MemoryManager::testingSetInstance({});

    connectorId_ = "test-connector";
    registerTestConnector(connectorId_);

    pool_ = velox::memory::memoryManager()->addLeafPool();
  }

  void TearDown() override {
    velox::connector::unregisterConnector(connectorId_);
  }

 protected:
  std::string connectorId_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(SparkToAxiomWithColumnsRenamedTest, BasicColumnRename) {
  // Create a basic WithColumnsRenamed plan
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(1);

  auto* withColumnsRenamed = relation.mutable_with_columns_renamed();

  // Create input relation (table scan)
  auto* input = withColumnsRenamed->mutable_input();
  input->mutable_common()->set_plan_id(2);
  auto* read = input->mutable_read();
  auto* namedTable = read->mutable_named_table();
  namedTable->set_unparsed_identifier("training_table");

  // Set up column rename mapping
  auto& renameMap = *withColumnsRenamed->mutable_rename_columns_map();
  renameMap["viewer_rid"] = "user_id";

  // Convert using SparkToAxiom
  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  const auto& planNode = converter.planNode();
  ASSERT_NE(planNode, nullptr);

  // Verify it's a ProjectNode
  const auto* projectNode =
      dynamic_cast<const facebook::axiom::logical_plan::ProjectNode*>(
          planNode.get());
  ASSERT_NE(projectNode, nullptr);

  // Verify output schema
  const auto& outputType = projectNode->outputType();
  EXPECT_EQ(outputType->size(), 1);
  EXPECT_EQ(outputType->nameOf(0), "user_id");
  EXPECT_EQ(outputType->childAt(0), velox::BIGINT());

  // Verify expressions
  const auto& expressions = projectNode->expressions();
  EXPECT_EQ(expressions.size(), 1);

  // Verify the expression is an InputReferenceExpr
  const auto* inputRefExpr =
      dynamic_cast<const facebook::axiom::logical_plan::InputReferenceExpr*>(
          expressions[0].get());
  ASSERT_NE(inputRefExpr, nullptr);
  EXPECT_EQ(
      inputRefExpr->name(), "viewer_rid"); // References original column name
  EXPECT_EQ(inputRefExpr->type(), velox::BIGINT());
}

TEST_F(SparkToAxiomWithColumnsRenamedTest, MultipleColumnRename) {
  // Create a more complex table with multiple columns
  // First, we need to create a custom table with multiple columns for this test
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(1);

  auto* withColumnsRenamed = relation.mutable_with_columns_renamed();

  // Create input relation using LocalRelation with multiple columns
  auto* input = withColumnsRenamed->mutable_input();
  input->mutable_common()->set_plan_id(2);
  auto* localRelation = input->mutable_local_relation();

  // Set schema for multi-column table
  std::string schema = R"({
    "type": "struct",
    "fields": [
      {"name": "id", "type": "long"},
      {"name": "name", "type": "string"},
      {"name": "age", "type": "integer"},
      {"name": "active", "type": "boolean"}
    ]
  })";
  localRelation->set_schema(schema);

  // Set up column rename mapping
  auto& renameMap = *withColumnsRenamed->mutable_rename_columns_map();
  renameMap["id"] = "user_id";
  renameMap["name"] = "user_name";
  // Note: age and active are not renamed (should keep original names)

  // Convert using SparkToAxiom
  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  const auto& planNode = converter.planNode();
  ASSERT_NE(planNode, nullptr);

  // Verify it's a ProjectNode
  const auto* projectNode =
      dynamic_cast<const facebook::axiom::logical_plan::ProjectNode*>(
          planNode.get());
  ASSERT_NE(projectNode, nullptr);

  // Verify output schema
  const auto& outputType = projectNode->outputType();
  EXPECT_EQ(outputType->size(), 4);

  // Check renamed columns
  EXPECT_EQ(outputType->nameOf(0), "user_id");
  EXPECT_EQ(outputType->nameOf(1), "user_name");

  // Check non-renamed columns
  EXPECT_EQ(outputType->nameOf(2), "age");
  EXPECT_EQ(outputType->nameOf(3), "active");

  // Verify types are preserved
  EXPECT_EQ(outputType->childAt(0), velox::BIGINT());
  EXPECT_EQ(outputType->childAt(1), velox::VARCHAR());
  EXPECT_EQ(outputType->childAt(2), velox::INTEGER());
  EXPECT_EQ(outputType->childAt(3), velox::BOOLEAN());

  // Verify expressions
  const auto& expressions = projectNode->expressions();
  EXPECT_EQ(expressions.size(), 4);

  // Check that all expressions are InputReferenceExpr and reference original
  // column names
  for (size_t i = 0; i < expressions.size(); ++i) {
    const auto* inputRefExpr =
        dynamic_cast<const facebook::axiom::logical_plan::InputReferenceExpr*>(
            expressions[i].get());
    ASSERT_NE(inputRefExpr, nullptr);
  }

  // Verify original column names are referenced in expressions
  const auto* expr0 =
      dynamic_cast<const facebook::axiom::logical_plan::InputReferenceExpr*>(
          expressions[0].get());
  const auto* expr1 =
      dynamic_cast<const facebook::axiom::logical_plan::InputReferenceExpr*>(
          expressions[1].get());
  const auto* expr2 =
      dynamic_cast<const facebook::axiom::logical_plan::InputReferenceExpr*>(
          expressions[2].get());
  const auto* expr3 =
      dynamic_cast<const facebook::axiom::logical_plan::InputReferenceExpr*>(
          expressions[3].get());

  EXPECT_EQ(expr0->name(), "id");
  EXPECT_EQ(expr1->name(), "name");
  EXPECT_EQ(expr2->name(), "age");
  EXPECT_EQ(expr3->name(), "active");
}

TEST_F(SparkToAxiomWithColumnsRenamedTest, NoColumnRename) {
  // Test case where no columns are renamed (empty rename map)
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(1);

  auto* withColumnsRenamed = relation.mutable_with_columns_renamed();

  // Create input relation
  auto* input = withColumnsRenamed->mutable_input();
  input->mutable_common()->set_plan_id(2);
  auto* read = input->mutable_read();
  auto* namedTable = read->mutable_named_table();
  namedTable->set_unparsed_identifier("training_table");

  // Set up empty column rename mapping
  // (no entries in the map)

  // Convert using SparkToAxiom
  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  const auto& planNode = converter.planNode();
  ASSERT_NE(planNode, nullptr);

  // Verify it's a ProjectNode
  const auto* projectNode =
      dynamic_cast<const facebook::axiom::logical_plan::ProjectNode*>(
          planNode.get());
  ASSERT_NE(projectNode, nullptr);

  // Verify output schema (should be the same as input)
  const auto& outputType = projectNode->outputType();
  EXPECT_EQ(outputType->size(), 1);
  EXPECT_EQ(outputType->nameOf(0), "viewer_rid"); // Original name preserved
  EXPECT_EQ(outputType->childAt(0), velox::BIGINT());
}

TEST_F(SparkToAxiomWithColumnsRenamedTest, RenameNonExistentColumn) {
  // Test case where we try to rename a column that doesn't exist
  // This should not cause an error but should simply be ignored
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(1);

  auto* withColumnsRenamed = relation.mutable_with_columns_renamed();

  // Create input relation
  auto* input = withColumnsRenamed->mutable_input();
  input->mutable_common()->set_plan_id(2);
  auto* read = input->mutable_read();
  auto* namedTable = read->mutable_named_table();
  namedTable->set_unparsed_identifier("training_table");

  // Set up column rename mapping with non-existent column
  auto& renameMap = *withColumnsRenamed->mutable_rename_columns_map();
  renameMap["viewer_rid"] = "user_id"; // Valid rename
  renameMap["non_existent_column"] =
      "new_name"; // Invalid rename (should be ignored)

  // Convert using SparkToAxiom
  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  const auto& planNode = converter.planNode();
  ASSERT_NE(planNode, nullptr);

  // Verify it's a ProjectNode
  const auto* projectNode =
      dynamic_cast<const facebook::axiom::logical_plan::ProjectNode*>(
          planNode.get());
  ASSERT_NE(projectNode, nullptr);

  // Verify output schema (should only have the existing column, renamed)
  const auto& outputType = projectNode->outputType();
  EXPECT_EQ(outputType->size(), 1);
  EXPECT_EQ(outputType->nameOf(0), "user_id");
  EXPECT_EQ(outputType->childAt(0), velox::BIGINT());
}

TEST_F(SparkToAxiomWithColumnsRenamedTest, ChainedWithColumnsRenamed) {
  // Test chaining multiple WithColumnsRenamed operations
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(1);

  auto* withColumnsRenamed2 = relation.mutable_with_columns_renamed();

  // Create first WithColumnsRenamed as input
  auto* input = withColumnsRenamed2->mutable_input();
  input->mutable_common()->set_plan_id(2);
  auto* withColumnsRenamed1 = input->mutable_with_columns_renamed();

  // Create base table as input to first WithColumnsRenamed
  auto* baseInput = withColumnsRenamed1->mutable_input();
  baseInput->mutable_common()->set_plan_id(3);
  auto* localRelation = baseInput->mutable_local_relation();

  // Set schema for multi-column table
  std::string schema = R"({
    "type": "struct",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "string"}
    ]
  })";
  localRelation->set_schema(schema);

  // First rename: a -> x, b -> y
  auto& renameMap1 = *withColumnsRenamed1->mutable_rename_columns_map();
  renameMap1["a"] = "x";
  renameMap1["b"] = "y";

  // Second rename: x -> final_a, y -> final_b
  auto& renameMap2 = *withColumnsRenamed2->mutable_rename_columns_map();
  renameMap2["x"] = "final_a";
  renameMap2["y"] = "final_b";

  // Convert using SparkToAxiom
  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  const auto& planNode = converter.planNode();
  ASSERT_NE(planNode, nullptr);

  // Verify it's a ProjectNode (the outer one)
  const auto* projectNode =
      dynamic_cast<const facebook::axiom::logical_plan::ProjectNode*>(
          planNode.get());
  ASSERT_NE(projectNode, nullptr);

  // Verify final output schema
  const auto& outputType = projectNode->outputType();
  EXPECT_EQ(outputType->size(), 2);
  EXPECT_EQ(outputType->nameOf(0), "final_a");
  EXPECT_EQ(outputType->nameOf(1), "final_b");
  EXPECT_EQ(outputType->childAt(0), velox::BIGINT());
  EXPECT_EQ(outputType->childAt(1), velox::VARCHAR());

  // Verify the plan structure contains nested ProjectNodes
  const auto& inputNode = projectNode->inputs()[0];
  const auto* innerProjectNode =
      dynamic_cast<const facebook::axiom::logical_plan::ProjectNode*>(
          inputNode.get());
  ASSERT_NE(innerProjectNode, nullptr);

  // Verify inner project output schema
  const auto& innerOutputType = innerProjectNode->outputType();
  EXPECT_EQ(innerOutputType->size(), 2);
  EXPECT_EQ(innerOutputType->nameOf(0), "x");
  EXPECT_EQ(innerOutputType->nameOf(1), "y");
}

TEST_F(SparkToAxiomWithColumnsRenamedTest, PlanToString) {
  // Test that the plan can be converted to string representation
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(1);

  auto* withColumnsRenamed = relation.mutable_with_columns_renamed();

  // Create input relation
  auto* input = withColumnsRenamed->mutable_input();
  input->mutable_common()->set_plan_id(2);
  auto* read = input->mutable_read();
  auto* namedTable = read->mutable_named_table();
  namedTable->set_unparsed_identifier("training_table");

  // Set up column rename mapping
  auto& renameMap = *withColumnsRenamed->mutable_rename_columns_map();
  renameMap["viewer_rid"] = "user_id";

  // Convert using SparkToAxiom
  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  // Test toString method
  std::string planString = converter.toString();
  EXPECT_FALSE(planString.empty());

  // Verify the string contains expected information
  EXPECT_NE(planString.find("Project"), std::string::npos);
  EXPECT_NE(planString.find("user_id"), std::string::npos);
}

} // namespace
} // namespace axiom::collagen::test
