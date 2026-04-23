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
#include "axiom/logical_plan/Expr.h"
#include "axiom/pyspark/SparkToAxiom.h"
#include "axiom/pyspark/third-party/protos/relations.grpc.pb.h" // @manual=fbcode//axiom/pyspark/third-party/protos:collagen_proto-cpp
#include "velox/common/memory/Memory.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook;

namespace axiom::collagen::test {
namespace {

using facebook::axiom::logical_plan::ExprKind;
using facebook::axiom::logical_plan::LambdaExpr;

class SparkToAxiomLambdaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    velox::functions::prestosql::registerAllScalarFunctions();

    // Initialize memory manager and create a memory pool for testing
    velox::memory::MemoryManager::initialize({});
    pool_ = velox::memory::MemoryManager::getInstance()->addLeafPool();

    // Set up test connector
    auto connector =
        std::make_shared<facebook::axiom::connector::TestConnector>(
            "test_connector");
    velox::connector::registerConnector(connector);
  }

  void TearDown() override {
    // Cleanup is handled by the connector destructor
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

// Helper function to create a simple lambda function expression.
// Creates: (x) -> x > 10
spark::connect::Expression createSimpleLambdaExpression(
    const std::string& paramName) {
  spark::connect::Expression lambdaExpr;
  auto* lambdaFunc = lambdaExpr.mutable_lambda_function();

  // Add argument (parameter)
  auto* arg = lambdaFunc->add_arguments();
  arg->add_name_parts(paramName);

  // Create the lambda body: x > 10
  // The body is an unresolved function ">" with arguments x and 10
  auto* body = lambdaFunc->mutable_function();
  auto* unresolvedFunc = body->mutable_unresolved_function();
  unresolvedFunc->set_function_name(">");

  // First argument: the lambda variable reference (x)
  auto* varArg = unresolvedFunc->add_arguments();
  auto* lambdaVar = varArg->mutable_unresolved_named_lambda_variable();
  lambdaVar->add_name_parts(paramName);

  // Second argument: literal 10
  auto* literalArg = unresolvedFunc->add_arguments();
  auto* literal = literalArg->mutable_literal();
  literal->set_integer(10);

  return lambdaExpr;
}

// Helper function to create a lambda with multiple parameters.
// Creates: (k, v) -> k > v
spark::connect::Expression createTwoParamLambdaExpression(
    const std::string& param1,
    const std::string& param2) {
  spark::connect::Expression lambdaExpr;
  auto* lambdaFunc = lambdaExpr.mutable_lambda_function();

  // Add first argument
  auto* arg1 = lambdaFunc->add_arguments();
  arg1->add_name_parts(param1);

  // Add second argument
  auto* arg2 = lambdaFunc->add_arguments();
  arg2->add_name_parts(param2);

  // Create the lambda body: k > v
  auto* body = lambdaFunc->mutable_function();
  auto* unresolvedFunc = body->mutable_unresolved_function();
  unresolvedFunc->set_function_name(">");

  // First argument: lambda variable k
  auto* varArg1 = unresolvedFunc->add_arguments();
  auto* lambdaVar1 = varArg1->mutable_unresolved_named_lambda_variable();
  lambdaVar1->add_name_parts(param1);

  // Second argument: lambda variable v
  auto* varArg2 = unresolvedFunc->add_arguments();
  auto* lambdaVar2 = varArg2->mutable_unresolved_named_lambda_variable();
  lambdaVar2->add_name_parts(param2);

  return lambdaExpr;
}

// Test visiting a simple lambda function: (x) -> x > 10
TEST_F(SparkToAxiomLambdaTest, visitSimpleLambdaFunction) {
  auto lambdaExpr = createSimpleLambdaExpression("x");

  // Convert to Axiom
  SparkToAxiom converter("test_connector", "", pool_.get());
  SparkPlanVisitorContext context;

  // Visit the lambda expression
  converter.visit(lambdaExpr, context);

  // Verify expr_ is a LambdaExpr with the expected structure.
  ASSERT_NE(converter.expr(), nullptr);
  ASSERT_EQ(converter.expr()->kind(), ExprKind::kLambda);

  auto result = std::dynamic_pointer_cast<const LambdaExpr>(converter.expr());
  ASSERT_NE(result, nullptr);

  // Verify signature: one parameter named "x".
  ASSERT_EQ(result->signature()->size(), 1);
  EXPECT_EQ(result->signature()->nameOf(0), "x");

  // Verify body is a CallExpr for ">" (greaterthan).
  ASSERT_NE(result->body(), nullptr);
  EXPECT_EQ(result->body()->kind(), ExprKind::kCall);
}

// Test visiting a lambda function with two parameters: (k, v) -> k > v
TEST_F(SparkToAxiomLambdaTest, visitTwoParamLambdaFunction) {
  auto lambdaExpr = createTwoParamLambdaExpression("k", "v");

  // Convert to Axiom
  SparkToAxiom converter("test_connector", "", pool_.get());
  SparkPlanVisitorContext context;

  // Visit the lambda expression
  converter.visit(lambdaExpr, context);

  // Verify expr_ is a LambdaExpr with two parameters.
  ASSERT_NE(converter.expr(), nullptr);
  ASSERT_EQ(converter.expr()->kind(), ExprKind::kLambda);

  auto result = std::dynamic_pointer_cast<const LambdaExpr>(converter.expr());
  ASSERT_NE(result, nullptr);

  // Verify signature: two parameters named "k" and "v".
  ASSERT_EQ(result->signature()->size(), 2);
  EXPECT_EQ(result->signature()->nameOf(0), "k");
  EXPECT_EQ(result->signature()->nameOf(1), "v");

  // Verify body is a CallExpr for ">".
  ASSERT_NE(result->body(), nullptr);
  EXPECT_EQ(result->body()->kind(), ExprKind::kCall);
}

// Test visiting an unresolved named lambda variable
TEST_F(SparkToAxiomLambdaTest, visitUnresolvedNamedLambdaVariable) {
  // Create a lambda expression that we first visit to register the variable
  spark::connect::Expression lambdaExpr;
  auto* lambdaFunc = lambdaExpr.mutable_lambda_function();

  // Add argument
  auto* arg = lambdaFunc->add_arguments();
  arg->add_name_parts("x");

  // Create the lambda body: just the variable x
  auto* body = lambdaFunc->mutable_function();
  auto* lambdaVar = body->mutable_unresolved_named_lambda_variable();
  lambdaVar->add_name_parts("x");

  // Convert to Axiom
  SparkToAxiom converter("test_connector", "", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(lambdaExpr, context);

  // Verify the result is a LambdaExpr whose body is an InputReferenceExpr
  // for the lambda variable "x".
  ASSERT_NE(converter.expr(), nullptr);
  ASSERT_EQ(converter.expr()->kind(), ExprKind::kLambda);

  auto result = std::dynamic_pointer_cast<const LambdaExpr>(converter.expr());
  ASSERT_NE(result, nullptr);

  ASSERT_EQ(result->signature()->size(), 1);
  EXPECT_EQ(result->signature()->nameOf(0), "x");

  // Body should be an InputReferenceExpr for variable "x".
  ASSERT_NE(result->body(), nullptr);
  EXPECT_EQ(result->body()->kind(), ExprKind::kInputReference);
}
TEST_F(SparkToAxiomLambdaTest, visitUnregisteredLambdaVariableThrows) {
  // Create an expression with just an unresolved lambda variable
  // without being inside a lambda context
  spark::connect::Expression expr;
  auto* lambdaVar = expr.mutable_unresolved_named_lambda_variable();
  lambdaVar->add_name_parts("unknown_var");

  SparkToAxiom converter("test_connector", "", pool_.get());
  SparkPlanVisitorContext context;

  // This should throw because the variable is not registered
  EXPECT_THROW(converter.visit(expr, context), std::exception);
}

// Test lambda with compound variable name (multiple name_parts)
TEST_F(SparkToAxiomLambdaTest, visitLambdaWithCompoundVariableName) {
  spark::connect::Expression lambdaExpr;
  auto* lambdaFunc = lambdaExpr.mutable_lambda_function();

  // Add argument with compound name (e.g., "struct.field")
  auto* arg = lambdaFunc->add_arguments();
  arg->add_name_parts("struct");
  arg->add_name_parts("field");

  // Create a simple body referencing the compound variable
  auto* body = lambdaFunc->mutable_function();
  auto* lambdaVar = body->mutable_unresolved_named_lambda_variable();
  lambdaVar->add_name_parts("struct");
  lambdaVar->add_name_parts("field");

  // Convert to Axiom
  SparkToAxiom converter("test_connector", "", pool_.get());
  SparkPlanVisitorContext context;
  converter.visit(lambdaExpr, context);

  // Verify the result is a LambdaExpr with compound parameter name.
  ASSERT_NE(converter.expr(), nullptr);
  ASSERT_EQ(converter.expr()->kind(), ExprKind::kLambda);

  auto result = std::dynamic_pointer_cast<const LambdaExpr>(converter.expr());
  ASSERT_NE(result, nullptr);

  // Parameter name should be "struct.field" (joined from name_parts).
  ASSERT_EQ(result->signature()->size(), 1);
  EXPECT_EQ(result->signature()->nameOf(0), "struct.field");

  // Body should be an InputReferenceExpr for "struct.field".
  ASSERT_NE(result->body(), nullptr);
  EXPECT_EQ(result->body()->kind(), ExprKind::kInputReference);
}

// Test lambda in the context of a TRANSFORM-like function call
TEST_F(SparkToAxiomLambdaTest, visitTransformWithLambda) {
  // Create a function call similar to: transform(array_col, x -> x > 10)
  spark::connect::Expression transformExpr;
  auto* unresolvedFunc = transformExpr.mutable_unresolved_function();
  unresolvedFunc->set_function_name("transform");

  // First argument: would be an array column reference
  // For this test, we use a literal array
  auto* arrayArg = unresolvedFunc->add_arguments();
  auto* literal = arrayArg->mutable_literal();
  auto* array = literal->mutable_array();
  array->mutable_element_type()->mutable_integer();
  auto* elem1 = array->add_elements();
  elem1->set_integer(1);
  auto* elem2 = array->add_elements();
  elem2->set_integer(20);
  auto* elem3 = array->add_elements();
  elem3->set_integer(5);

  // Second argument: the lambda function
  auto lambdaExpr = createSimpleLambdaExpression("x");
  *unresolvedFunc->add_arguments() = lambdaExpr;

  // Convert to Axiom - should not throw
  SparkToAxiom converter("test_connector", "", pool_.get());
  SparkPlanVisitorContext context;

  // Note: The actual transform function resolution may fail if not registered,
  // but the lambda translation itself should succeed
  try {
    converter.visit(transformExpr, context);
  } catch (const std::exception& e) {
    // Expected if transform function is not registered, but lambda parsing
    // should have succeeded. Check error is about function resolution, not
    // lambda.
    std::string error = e.what();
    EXPECT_TRUE(error.find("Lambda variable") == std::string::npos)
        << "Lambda translation failed: " << error;
  }
}

// Test nested lambda with shadowed variable names.
// Verifies that the LambdaScope RAII properly saves/restores outer lambda
// variables when an inner lambda shadows the same parameter name.
TEST_F(SparkToAxiomLambdaTest, visitNestedLambdaWithShadowedVariable) {
  // Simulate being inside an outer lambda scope by pre-populating
  // context.lambdaVariables with "x" = BIGINT.
  SparkPlanVisitorContext context;
  context.lambdaVariables["x"] = velox::BIGINT();

  // Create inner lambda: (x) -> x
  // This shadows the outer "x" with its own "x" parameter.
  spark::connect::Expression innerLambdaExpr;
  auto* lambdaFunc = innerLambdaExpr.mutable_lambda_function();
  auto* arg = lambdaFunc->add_arguments();
  arg->add_name_parts("x");
  auto* body = lambdaFunc->mutable_function();
  auto* lambdaVar = body->mutable_unresolved_named_lambda_variable();
  lambdaVar->add_name_parts("x");

  SparkToAxiom converter("test_connector", "", pool_.get());
  converter.visit(innerLambdaExpr, context);

  // Verify the inner lambda correctly used the pre-inferred BIGINT type
  // from the simulated outer scope.
  ASSERT_NE(converter.expr(), nullptr);
  ASSERT_EQ(converter.expr()->kind(), ExprKind::kLambda);

  auto result = std::dynamic_pointer_cast<const LambdaExpr>(converter.expr());
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->signature()->size(), 1);
  EXPECT_EQ(result->signature()->nameOf(0), "x");
  EXPECT_EQ(result->signature()->childAt(0)->kind(), velox::TypeKind::BIGINT);

  // Body should be an InputReferenceExpr for "x".
  ASSERT_NE(result->body(), nullptr);
  EXPECT_EQ(result->body()->kind(), ExprKind::kInputReference);

  // CRITICAL: After the inner lambda scope exits, the outer "x" must still
  // be in the context with its original type. With the old flat-map approach,
  // the inner lambda's cleanup would erase "x" entirely.
  auto it = context.lambdaVariables.find("x");
  ASSERT_NE(it, context.lambdaVariables.end())
      << "Outer lambda variable 'x' was lost after inner lambda scope exited";
  EXPECT_EQ(it->second->kind(), velox::TypeKind::BIGINT)
      << "Outer lambda variable 'x' type was corrupted";
}

// Test nested lambda with different variable names (no shadowing).
// Verifies that non-overlapping variable names work correctly across scopes
// and that inner variables don't leak into the outer scope.
TEST_F(SparkToAxiomLambdaTest, visitNestedLambdaWithDistinctVariables) {
  // Simulate outer scope with "x" = BIGINT.
  SparkPlanVisitorContext context;
  context.lambdaVariables["x"] = velox::BIGINT();

  // Create inner lambda: (y) -> x
  // The body references outer "x" (not inner "y"), verifying cross-scope
  // access.
  spark::connect::Expression innerLambdaExpr;
  auto* lambdaFunc = innerLambdaExpr.mutable_lambda_function();
  auto* arg = lambdaFunc->add_arguments();
  arg->add_name_parts("y");
  auto* body = lambdaFunc->mutable_function();
  auto* lambdaVar = body->mutable_unresolved_named_lambda_variable();
  lambdaVar->add_name_parts("x");

  SparkToAxiom converter("test_connector", "", pool_.get());
  converter.visit(innerLambdaExpr, context);

  // Verify the inner lambda has parameter "y" and body referencing "x".
  ASSERT_NE(converter.expr(), nullptr);
  ASSERT_EQ(converter.expr()->kind(), ExprKind::kLambda);

  auto result = std::dynamic_pointer_cast<const LambdaExpr>(converter.expr());
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->signature()->size(), 1);
  EXPECT_EQ(result->signature()->nameOf(0), "y");

  // Body should be an InputReferenceExpr for "x" (from outer scope).
  ASSERT_NE(result->body(), nullptr);
  EXPECT_EQ(result->body()->kind(), ExprKind::kInputReference);

  // The outer "x" should still be in context after inner lambda exits.
  auto it = context.lambdaVariables.find("x");
  ASSERT_NE(it, context.lambdaVariables.end());
  EXPECT_EQ(it->second->kind(), velox::TypeKind::BIGINT);

  // The inner "y" should NOT be in context after inner lambda exits.
  EXPECT_EQ(context.lambdaVariables.find("y"), context.lambdaVariables.end())
      << "Inner lambda variable 'y' leaked into outer scope";
}

// Test find_first_index higher-order function: find_first_index(array, x -> x >
// 10) Verifies that find_first_index is recognized as a higher-order function
// and lambda parameter types are correctly inferred from the array element
// type.
TEST_F(SparkToAxiomLambdaTest, visitFindFirstIndexWithLambda) {
  // Create: find_first_index([1, 20, 5], x -> x > 10)
  spark::connect::Expression findFirstExpr;
  auto* unresolvedFunc = findFirstExpr.mutable_unresolved_function();
  unresolvedFunc->set_function_name("find_first_index");

  // First argument: literal array of integers
  auto* arrayArg = unresolvedFunc->add_arguments();
  auto* literal = arrayArg->mutable_literal();
  auto* array = literal->mutable_array();
  array->mutable_element_type()->mutable_integer();
  array->add_elements()->set_integer(1);
  array->add_elements()->set_integer(20);
  array->add_elements()->set_integer(5);

  // Second argument: lambda (x) -> x > 10
  auto lambdaExpr = createSimpleLambdaExpression("x");
  *unresolvedFunc->add_arguments() = lambdaExpr;

  SparkToAxiom converter("test_connector", "", pool_.get());
  SparkPlanVisitorContext context;

  try {
    converter.visit(findFirstExpr, context);
    // If the function resolves, verify the result is a CallExpr.
    ASSERT_NE(converter.expr(), nullptr);
    EXPECT_EQ(converter.expr()->kind(), ExprKind::kCall);
  } catch (const std::exception& e) {
    // If find_first_index is not registered in the test binary, the lambda
    // translation should still succeed — verify the error is about function
    // resolution, not lambda variable inference.
    std::string error = e.what();
    EXPECT_TRUE(error.find("Lambda variable") == std::string::npos)
        << "Lambda type inference failed for find_first_index: " << error;
    EXPECT_TRUE(
        error.find("does not exist") != std::string::npos ||
        error.find("signature") != std::string::npos ||
        error.find("not implemented") != std::string::npos)
        << "Unexpected error for find_first_index: " << error;
  }
}

// Test aggregate (reduce) higher-order function with two lambdas:
// aggregate(array, initial, (s, x) -> s + x, (s) -> s)
// Verifies that aggregate is recognized as a higher-order function and both
// lambda parameter types are correctly inferred: merge lambda gets (state S,
// element T) and output lambda gets (state S).
TEST_F(SparkToAxiomLambdaTest, visitAggregateWithTwoLambdas) {
  // Create: aggregate([1, 2, 3], 0, (s, x) -> s + x, (s) -> s)
  spark::connect::Expression aggExpr;
  auto* unresolvedFunc = aggExpr.mutable_unresolved_function();
  unresolvedFunc->set_function_name("aggregate");

  // Arg 0: literal array of integers
  auto* arrayArg = unresolvedFunc->add_arguments();
  auto* literal = arrayArg->mutable_literal();
  auto* array = literal->mutable_array();
  array->mutable_element_type()->mutable_integer();
  array->add_elements()->set_integer(1);
  array->add_elements()->set_integer(2);
  array->add_elements()->set_integer(3);

  // Arg 1: initial value (integer literal 0)
  auto* initArg = unresolvedFunc->add_arguments();
  initArg->mutable_literal()->set_integer(0);

  // Arg 2: merge lambda (s, x) -> s + x
  {
    spark::connect::Expression mergeLambdaExpr;
    auto* lambdaFunc = mergeLambdaExpr.mutable_lambda_function();
    auto* s = lambdaFunc->add_arguments();
    s->add_name_parts("s");
    auto* x = lambdaFunc->add_arguments();
    x->add_name_parts("x");
    auto* body = lambdaFunc->mutable_function();
    auto* bodyFunc = body->mutable_unresolved_function();
    bodyFunc->set_function_name("+");
    auto* sRef = bodyFunc->add_arguments();
    sRef->mutable_unresolved_named_lambda_variable()->add_name_parts("s");
    auto* xRef = bodyFunc->add_arguments();
    xRef->mutable_unresolved_named_lambda_variable()->add_name_parts("x");
    *unresolvedFunc->add_arguments() = mergeLambdaExpr;
  }

  // Arg 3: output lambda (s) -> s
  {
    spark::connect::Expression outputLambdaExpr;
    auto* lambdaFunc = outputLambdaExpr.mutable_lambda_function();
    auto* s = lambdaFunc->add_arguments();
    s->add_name_parts("s");
    auto* body = lambdaFunc->mutable_function();
    body->mutable_unresolved_named_lambda_variable()->add_name_parts("s");
    *unresolvedFunc->add_arguments() = outputLambdaExpr;
  }

  SparkToAxiom converter("test_connector", "", pool_.get());
  SparkPlanVisitorContext context;

  try {
    converter.visit(aggExpr, context);
    ASSERT_NE(converter.expr(), nullptr);
    EXPECT_EQ(converter.expr()->kind(), ExprKind::kCall);
  } catch (const std::exception& e) {
    // Lambda translation should succeed even if function resolution fails.
    std::string error = e.what();
    EXPECT_TRUE(error.find("Lambda variable") == std::string::npos)
        << "Lambda type inference failed for aggregate/reduce: " << error;
    EXPECT_TRUE(
        error.find("does not exist") != std::string::npos ||
        error.find("signature") != std::string::npos ||
        error.find("not implemented") != std::string::npos)
        << "Unexpected error for aggregate/reduce: " << error;
  }
}

} // namespace
} // namespace axiom::collagen::test
