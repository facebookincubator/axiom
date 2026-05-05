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
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/pyspark/SparkToAxiom.h"
#include "axiom/pyspark/third-party/protos/relations.grpc.pb.h" // @manual=fbcode//axiom/pyspark/third-party/protos:collagen_proto-cpp
#include "velox/common/memory/Memory.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook;

namespace axiom::collagen::test {
namespace {

class SparkToAxiomUnnestTest : public ::testing::Test {
 protected:
  void SetUp() override {
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::memory::MemoryManager::testingSetInstance({});

    connectorId_ = "test-connector-unnest";
    auto connector =
        std::make_shared<facebook::axiom::connector::TestConnector>(
            connectorId_);
    connector->addTable(
        "test_table_with_array",
        velox::ROW(
            {"id", "arr"}, {velox::BIGINT(), velox::ARRAY(velox::INTEGER())}));
    connector->addTable(
        "test_table_with_map",
        velox::ROW(
            {"id", "m"},
            {velox::BIGINT(), velox::MAP(velox::VARCHAR(), velox::INTEGER())}));
    velox::connector::ConnectorRegistry::global().insert(
        connectorId_, connector);

    pool_ = velox::memory::memoryManager()->addLeafPool();
  }

  void TearDown() override {
    velox::connector::ConnectorRegistry::global().erase(connectorId_);
  }

  // Builds a Project relation wrapping a table scan with the given generator
  // expressions. Each entry in generators is {function_name, column_name,
  // alias_names}. Non-generator expressions are added via nonGenColumns.
  spark::connect::Relation buildProjectWithExplode(
      const std::string& tableName,
      const std::string& genFuncName,
      const std::string& genColumn,
      const std::vector<std::string>& aliasNames,
      const std::vector<std::string>& nonGenColumns = {}) {
    spark::connect::Relation relation;
    relation.mutable_common()->set_plan_id(1);

    auto* project = relation.mutable_project();

    auto* input = project->mutable_input();
    input->mutable_common()->set_plan_id(2);
    input->mutable_read()->mutable_named_table()->set_unparsed_identifier(
        tableName);

    for (const auto& col : nonGenColumns) {
      project->add_expressions()
          ->mutable_unresolved_attribute()
          ->set_unparsed_identifier(col);
    }

    auto* genExpr = project->add_expressions();
    if (!aliasNames.empty()) {
      auto* alias = genExpr->mutable_alias();
      for (const auto& name : aliasNames) {
        alias->add_name(name);
      }
      auto* func = alias->mutable_expr()->mutable_unresolved_function();
      func->set_function_name(genFuncName);
      func->add_arguments()
          ->mutable_unresolved_attribute()
          ->set_unparsed_identifier(genColumn);
    } else {
      auto* func = genExpr->mutable_unresolved_function();
      func->set_function_name(genFuncName);
      func->add_arguments()
          ->mutable_unresolved_attribute()
          ->set_unparsed_identifier(genColumn);
    }

    return relation;
  }

  std::string connectorId_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(SparkToAxiomUnnestTest, explodeCreatesUnnestNode) {
  auto relation = buildProjectWithExplode(
      "test_table_with_array", "explode", "arr", {"elem"}, {"id"});

  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  const auto& planNode = converter.planNode();
  ASSERT_NE(planNode, nullptr);

  const auto* projectNode =
      dynamic_cast<const facebook::axiom::logical_plan::ProjectNode*>(
          planNode.get());
  ASSERT_NE(projectNode, nullptr);

  ASSERT_EQ(projectNode->inputs().size(), 1);
  const auto* unnestNode =
      dynamic_cast<const facebook::axiom::logical_plan::UnnestNode*>(
          projectNode->inputs()[0].get());
  ASSERT_NE(unnestNode, nullptr);

  ASSERT_EQ(unnestNode->unnestExpressions().size(), 1);
  EXPECT_TRUE(unnestNode->unnestExpressions()[0]->type()->isArray());

  ASSERT_EQ(unnestNode->unnestedNames().size(), 1);
  ASSERT_EQ(unnestNode->unnestedNames()[0].size(), 1);
  EXPECT_EQ(unnestNode->unnestedNames()[0][0], "elem");

  const auto& outputType = projectNode->outputType();
  bool hasId = false;
  bool hasElem = false;
  for (uint32_t i = 0; i < outputType->size(); ++i) {
    if (outputType->nameOf(i) == "id") {
      hasId = true;
    }
    if (outputType->nameOf(i) == "elem") {
      hasElem = true;
    }
  }
  EXPECT_TRUE(hasId);
  EXPECT_TRUE(hasElem);
}

TEST_F(SparkToAxiomUnnestTest, explodeWithoutAlias) {
  auto relation =
      buildProjectWithExplode("test_table_with_array", "explode", "arr", {});

  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  const auto* unnestNode =
      dynamic_cast<const facebook::axiom::logical_plan::UnnestNode*>(
          converter.planNode().get());
  ASSERT_NE(unnestNode, nullptr);

  ASSERT_EQ(unnestNode->unnestedNames().size(), 1);
  EXPECT_EQ(unnestNode->unnestedNames()[0][0], "col");
}

TEST_F(SparkToAxiomUnnestTest, explodeOuterThrowsNYI) {
  auto relation = buildProjectWithExplode(
      "test_table_with_array", "explode_outer", "arr", {"elem"});

  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  EXPECT_THROW(converter.visit(relation, context), std::exception);
}

TEST_F(SparkToAxiomUnnestTest, posexplodeOuterThrowsNYI) {
  auto relation = buildProjectWithExplode(
      "test_table_with_array", "posexplode_outer", "arr", {"elem"});

  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  EXPECT_THROW(converter.visit(relation, context), std::exception);
}

TEST_F(SparkToAxiomUnnestTest, multipleGeneratorsRejected) {
  // Build a project with two explode calls — Spark rejects this too.
  spark::connect::Relation relation;
  relation.mutable_common()->set_plan_id(1);

  auto* project = relation.mutable_project();
  auto* input = project->mutable_input();
  input->mutable_common()->set_plan_id(2);
  input->mutable_read()->mutable_named_table()->set_unparsed_identifier(
      "test_table_with_array");

  auto* gen1 = project->add_expressions()->mutable_unresolved_function();
  gen1->set_function_name("explode");
  gen1->add_arguments()
      ->mutable_unresolved_attribute()
      ->set_unparsed_identifier("arr");

  auto* gen2 = project->add_expressions()->mutable_unresolved_function();
  gen2->set_function_name("explode");
  gen2->add_arguments()
      ->mutable_unresolved_attribute()
      ->set_unparsed_identifier("arr");

  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  EXPECT_THROW(converter.visit(relation, context), std::exception);
}

TEST_F(SparkToAxiomUnnestTest, mapExplodeWithMultiNameAlias) {
  auto relation = buildProjectWithExplode(
      "test_table_with_map", "explode", "m", {"k", "v"});

  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  const auto* unnestNode =
      dynamic_cast<const facebook::axiom::logical_plan::UnnestNode*>(
          converter.planNode().get());
  ASSERT_NE(unnestNode, nullptr);

  ASSERT_EQ(unnestNode->unnestedNames().size(), 1);
  ASSERT_EQ(unnestNode->unnestedNames()[0].size(), 2);
  EXPECT_EQ(unnestNode->unnestedNames()[0][0], "k");
  EXPECT_EQ(unnestNode->unnestedNames()[0][1], "v");
}

TEST_F(SparkToAxiomUnnestTest, mapExplodeDefaultNames) {
  auto relation =
      buildProjectWithExplode("test_table_with_map", "explode", "m", {});

  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  const auto* unnestNode =
      dynamic_cast<const facebook::axiom::logical_plan::UnnestNode*>(
          converter.planNode().get());
  ASSERT_NE(unnestNode, nullptr);

  ASSERT_EQ(unnestNode->unnestedNames().size(), 1);
  ASSERT_EQ(unnestNode->unnestedNames()[0].size(), 2);
  EXPECT_EQ(unnestNode->unnestedNames()[0][0], "key");
  EXPECT_EQ(unnestNode->unnestedNames()[0][1], "value");
}

// posexplode is always wrapped with a ProjectNode that subtracts 1 from the
// ordinality column to convert Velox's 1-based indexing to Spark's 0-based.
TEST_F(SparkToAxiomUnnestTest, posexplodeWrapsWithMinusOne) {
  auto relation =
      buildProjectWithExplode("test_table_with_array", "posexplode", "arr", {});

  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  const auto* projectNode =
      dynamic_cast<const facebook::axiom::logical_plan::ProjectNode*>(
          converter.planNode().get());
  ASSERT_NE(projectNode, nullptr)
      << "posexplode must always wrap with ProjectNode for pos-1 conversion";

  const auto* unnestNode =
      dynamic_cast<const facebook::axiom::logical_plan::UnnestNode*>(
          projectNode->inputs()[0].get());
  ASSERT_NE(unnestNode, nullptr);

  // UnnestNode keeps default names: element="col", ordinality="pos".
  ASSERT_EQ(unnestNode->unnestedNames()[0][0], "col");
  ASSERT_TRUE(unnestNode->ordinalityName().has_value());
  EXPECT_EQ(unnestNode->ordinalityName().value(), "pos");

  // The ProjectNode replaces the pos column with `pos - 1`. Find that column
  // in the projections and verify it's a CallExpr for "minus".
  const auto& outputType = projectNode->outputType();
  const auto& projections = projectNode->expressions();
  bool foundPosMinusOne = false;
  for (uint32_t i = 0; i < outputType->size(); ++i) {
    if (outputType->nameOf(i) == "pos") {
      const auto* callExpr =
          dynamic_cast<const facebook::axiom::logical_plan::CallExpr*>(
              projections[i].get());
      ASSERT_NE(callExpr, nullptr) << "pos column must be a CallExpr (pos - 1)";
      EXPECT_EQ(callExpr->name(), "minus");
      foundPosMinusOne = true;
    }
  }
  EXPECT_TRUE(foundPosMinusOne);
}

// Spark posexplode alias order is (pos, element). The first alias names the
// position column; the second names the element column.
TEST_F(SparkToAxiomUnnestTest, posexplodeAliasOrderingIsPosThenElement) {
  auto relation = buildProjectWithExplode(
      "test_table_with_array", "posexplode", "arr", {"my_pos", "my_elem"});

  SparkToAxiom converter(connectorId_, "default", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(relation, context);

  const auto* projectNode =
      dynamic_cast<const facebook::axiom::logical_plan::ProjectNode*>(
          converter.planNode().get());
  ASSERT_NE(projectNode, nullptr);

  const auto* unnestNode =
      dynamic_cast<const facebook::axiom::logical_plan::UnnestNode*>(
          projectNode->inputs()[0].get());
  ASSERT_NE(unnestNode, nullptr);

  ASSERT_TRUE(unnestNode->ordinalityName().has_value());
  EXPECT_EQ(unnestNode->ordinalityName().value(), "my_pos");
  ASSERT_EQ(unnestNode->unnestedNames()[0][0], "my_elem");

  // Output schema must contain both aliased names.
  const auto& outputType = projectNode->outputType();
  bool hasMyPos = false;
  bool hasMyElem = false;
  for (uint32_t i = 0; i < outputType->size(); ++i) {
    if (outputType->nameOf(i) == "my_pos") {
      hasMyPos = true;
    }
    if (outputType->nameOf(i) == "my_elem") {
      hasMyElem = true;
    }
  }
  EXPECT_TRUE(hasMyPos);
  EXPECT_TRUE(hasMyElem);
}

} // namespace
} // namespace axiom::collagen::test
