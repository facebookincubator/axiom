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
      "training_table",
      velox::ROW(
          {"viewer_rid", "feature_a"}, {velox::BIGINT(), velox::DOUBLE()}));

  velox::connector::registerConnector(connector);
}

class SparkToAxiomWithColumnsTest : public ::testing::Test {
 public:
  void SetUp() override {
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::memory::MemoryManager::testingSetInstance({});

    connectorId_ = "test-connector-with-columns";
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

TEST_F(SparkToAxiomWithColumnsTest, AddNewColumn) {
  // Test adding a new column that doesn't exist in input
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(1);

  auto* withColumns = relation.mutable_with_columns();

  // Create input relation (table scan)
  auto* input = withColumns->mutable_input();
  input->mutable_common()->set_plan_id(2);
  auto* read = input->mutable_read();
  auto* namedTable = read->mutable_named_table();
  namedTable->set_unparsed_identifier("training_table");

  // Add a new column: new_col = 42 (literal)
  auto* alias = withColumns->add_aliases();
  alias->add_name("new_col");
  auto* literal = alias->mutable_expr()->mutable_literal();
  literal->set_long_(42);

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

  // Verify output schema has 3 columns now (original 2 + new 1)
  const auto& outputType = projectNode->outputType();
  EXPECT_EQ(outputType->size(), 3);
  EXPECT_EQ(outputType->nameOf(0), "viewer_rid");
  EXPECT_EQ(outputType->nameOf(1), "feature_a");
  EXPECT_EQ(outputType->nameOf(2), "new_col");

  // Verify types
  EXPECT_EQ(outputType->childAt(0), velox::BIGINT());
  EXPECT_EQ(outputType->childAt(1), velox::DOUBLE());
  EXPECT_EQ(outputType->childAt(2), velox::BIGINT());
}

TEST_F(SparkToAxiomWithColumnsTest, ReplaceExistingColumn) {
  // Test replacing an existing column with a new expression
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(1);

  auto* withColumns = relation.mutable_with_columns();

  // Create input relation using LocalRelation
  auto* input = withColumns->mutable_input();
  input->mutable_common()->set_plan_id(2);
  auto* localRelation = input->mutable_local_relation();

  std::string schema = R"({
    "type": "struct",
    "fields": [
      {"name": "id", "type": "long"},
      {"name": "value", "type": "integer"}
    ]
  })";
  localRelation->set_schema(schema);

  // Replace 'value' column with literal 100
  auto* alias = withColumns->add_aliases();
  alias->add_name("value");
  auto* literal = alias->mutable_expr()->mutable_literal();
  literal->set_integer(100);

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

  // Verify output schema still has 2 columns (replaced, not added)
  const auto& outputType = projectNode->outputType();
  EXPECT_EQ(outputType->size(), 2);
  EXPECT_EQ(outputType->nameOf(0), "id");
  EXPECT_EQ(outputType->nameOf(1), "value");

  // Verify types - value should now be INTEGER (from the literal)
  EXPECT_EQ(outputType->childAt(0), velox::BIGINT());
  EXPECT_EQ(outputType->childAt(1), velox::INTEGER());

  // Verify the expression for 'value' is a ConstantExpr
  const auto& expressions = projectNode->expressions();
  EXPECT_EQ(expressions.size(), 2);

  const auto* constExpr =
      dynamic_cast<const facebook::axiom::logical_plan::ConstantExpr*>(
          expressions[1].get());
  ASSERT_NE(constExpr, nullptr);
}

TEST_F(SparkToAxiomWithColumnsTest, AddMultipleColumns) {
  // Test adding multiple new columns at once
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(1);

  auto* withColumns = relation.mutable_with_columns();

  // Create input relation using LocalRelation
  auto* input = withColumns->mutable_input();
  input->mutable_common()->set_plan_id(2);
  auto* localRelation = input->mutable_local_relation();

  std::string schema = R"({
    "type": "struct",
    "fields": [
      {"name": "base_col", "type": "string"}
    ]
  })";
  localRelation->set_schema(schema);

  // Add first new column: col_a = 1
  auto* alias1 = withColumns->add_aliases();
  alias1->add_name("col_a");
  auto* literal1 = alias1->mutable_expr()->mutable_literal();
  literal1->set_long_(1);

  // Add second new column: col_b = 2
  auto* alias2 = withColumns->add_aliases();
  alias2->add_name("col_b");
  auto* literal2 = alias2->mutable_expr()->mutable_literal();
  literal2->set_long_(2);

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

  // Verify output schema has 3 columns (1 original + 2 new)
  const auto& outputType = projectNode->outputType();
  EXPECT_EQ(outputType->size(), 3);
  EXPECT_EQ(outputType->nameOf(0), "base_col");

  // Note: The order of new columns may vary since they're stored in a map
  // Just verify they exist
  bool hasColA = false;
  bool hasColB = false;
  for (uint32_t i = 1; i < outputType->size(); ++i) {
    if (outputType->nameOf(i) == "col_a") {
      hasColA = true;
    }
    if (outputType->nameOf(i) == "col_b") {
      hasColB = true;
    }
  }
  EXPECT_TRUE(hasColA);
  EXPECT_TRUE(hasColB);
}

TEST_F(SparkToAxiomWithColumnsTest, MixedAddAndReplace) {
  // Test adding new columns and replacing existing ones in the same operation
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(1);

  auto* withColumns = relation.mutable_with_columns();

  // Create input relation using LocalRelation
  auto* input = withColumns->mutable_input();
  input->mutable_common()->set_plan_id(2);
  auto* localRelation = input->mutable_local_relation();

  std::string schema = R"({
    "type": "struct",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "string"}
    ]
  })";
  localRelation->set_schema(schema);

  // Replace 'a' with literal 99
  auto* alias1 = withColumns->add_aliases();
  alias1->add_name("a");
  auto* literal1 = alias1->mutable_expr()->mutable_literal();
  literal1->set_long_(99);

  // Add new column 'c'
  auto* alias2 = withColumns->add_aliases();
  alias2->add_name("c");
  auto* literal2 = alias2->mutable_expr()->mutable_literal();
  literal2->set_boolean(true);

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

  // Verify output schema has 3 columns (2 original with 1 replaced + 1 new)
  const auto& outputType = projectNode->outputType();
  EXPECT_EQ(outputType->size(), 3);

  // First two should be original columns (with 'a' replaced)
  EXPECT_EQ(outputType->nameOf(0), "a");
  EXPECT_EQ(outputType->nameOf(1), "b");

  // Third should be the new column 'c'
  EXPECT_EQ(outputType->nameOf(2), "c");

  // Verify types
  EXPECT_EQ(outputType->childAt(0), velox::BIGINT());
  EXPECT_EQ(outputType->childAt(1), velox::VARCHAR());
  EXPECT_EQ(outputType->childAt(2), velox::BOOLEAN());
}

} // namespace
} // namespace axiom::collagen::test
