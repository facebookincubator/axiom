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

#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/ExprResolver.h"
#include "velox/common/serialization/Serializable.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/type/Type.h"

namespace facebook::axiom::logical_plan {
namespace {

class ExprSerdeTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::Type::registerSerDe();
    Expr::registerSerDe();
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::aggregate::prestosql::registerAllAggregateFunctions();
  }

  // Verifies round-trip serialization by comparing toString() outputs.
  void testRoundTrip(const ExprPtr& expr) {
    auto serialized = expr->serialize();
    auto deserialized =
        velox::ISerializable::deserialize<Expr>(serialized, nullptr);
    ASSERT_NE(deserialized, nullptr);
    EXPECT_EQ(expr->toString(), deserialized->toString());
  }

  // Resolves an ExprApi expression and verifies round-trip serialization.
  void testRoundTrip(const ExprApi& expr, velox::RowTypePtr schema) {
    testRoundTrip(resolve(expr, schema));
  }

  // Helper to test constant expression serialization.
  void testConstant(velox::TypePtr type, velox::Variant value) {
    testRoundTrip(
        std::make_shared<ConstantExpr>(
            std::move(type),
            std::make_shared<velox::Variant>(std::move(value))));
  }

  // Resolves an ExprApi expression to an Expr using a schema for column types.
  ExprPtr resolve(const ExprApi& expr, velox::RowTypePtr schema) {
    ExprResolver resolver(/*queryCtx=*/nullptr, /*enableCoercions=*/false);
    return resolver.resolveScalarTypes(
        expr.expr(),
        [&schema](const std::optional<std::string>&, const std::string& name)
            -> ExprPtr {
          return std::make_shared<InputReferenceExpr>(
              schema->findChild(name), name);
        });
  }
};

TEST_F(ExprSerdeTest, constantInteger) {
  testConstant(velox::BIGINT(), velox::Variant(123LL));
}

TEST_F(ExprSerdeTest, constantDouble) {
  testConstant(velox::DOUBLE(), velox::Variant(3.14));
}

TEST_F(ExprSerdeTest, constantString) {
  testConstant(velox::VARCHAR(), velox::Variant("hello world"));
}

TEST_F(ExprSerdeTest, constantBoolean) {
  testConstant(velox::BOOLEAN(), velox::Variant(true));
}

TEST_F(ExprSerdeTest, constantNull) {
  testConstant(velox::BIGINT(), velox::Variant::null(velox::TypeKind::BIGINT));
}

TEST_F(ExprSerdeTest, constantArray) {
  testConstant(
      velox::ARRAY(velox::INTEGER()), velox::Variant::array({1, 2, 3}));
}

TEST_F(ExprSerdeTest, inputReference) {
  auto schema = velox::ROW({{"column_name", velox::BIGINT()}});
  testRoundTrip(Col("column_name"), schema);
}

TEST_F(ExprSerdeTest, callExpr) {
  auto schema = velox::ROW({{"a", velox::BIGINT()}, {"b", velox::BIGINT()}});
  testRoundTrip(Call("plus", Col("a"), Col("b")), schema);
}

TEST_F(ExprSerdeTest, specialFormExpr) {
  auto schema =
      velox::ROW({{"cond", velox::BOOLEAN()}, {"x", velox::BIGINT()}});
  testRoundTrip(Call("if", Col("cond"), Lit(1LL), Lit(2LL)), schema);
}

TEST_F(ExprSerdeTest, aggregateExpr) {
  auto schema = velox::ROW({{"x", velox::BIGINT()}});
  auto x = resolve(Col("x"), schema);
  testRoundTrip(
      std::make_shared<AggregateExpr>(
          velox::BIGINT(), "sum", std::vector<ExprPtr>{x}));
}

TEST_F(ExprSerdeTest, aggregateExprWithFilter) {
  auto schema =
      velox::ROW({{"x", velox::BIGINT()}, {"flag", velox::BOOLEAN()}});
  auto x = resolve(Col("x"), schema);
  auto flag = resolve(Col("flag"), schema);
  testRoundTrip(
      std::make_shared<AggregateExpr>(
          velox::BIGINT(),
          "sum",
          std::vector<ExprPtr>{x},
          flag,
          std::vector<SortingField>{},
          false));
}

TEST_F(ExprSerdeTest, aggregateExprDistinct) {
  auto schema = velox::ROW({{"x", velox::BIGINT()}});
  auto x = resolve(Col("x"), schema);
  testRoundTrip(
      std::make_shared<AggregateExpr>(
          velox::BIGINT(),
          "sum",
          std::vector<ExprPtr>{x},
          nullptr,
          std::vector<SortingField>{},
          true));
}

TEST_F(ExprSerdeTest, windowExpr) {
  auto schema =
      velox::ROW({{"x", velox::BIGINT()}, {"category", velox::VARCHAR()}});
  auto x = resolve(Col("x"), schema);
  auto category = resolve(Col("category"), schema);
  WindowExpr::Frame frame{
      WindowExpr::WindowType::kRows,
      WindowExpr::BoundType::kUnboundedPreceding,
      nullptr,
      WindowExpr::BoundType::kCurrentRow,
      nullptr};
  testRoundTrip(
      std::make_shared<WindowExpr>(
          velox::BIGINT(),
          "sum",
          std::vector<ExprPtr>{x},
          std::vector<ExprPtr>{category},
          std::vector<SortingField>{},
          frame,
          false));
}

TEST_F(ExprSerdeTest, lambdaExpr) {
  auto schema = velox::ROW({{"arr", velox::ARRAY(velox::BIGINT())}});
  testRoundTrip(
      Call(
          "filter", Col("arr"), Lambda({"x"}, Call("gt", Col("x"), Lit(10LL)))),
      schema);
}

} // namespace
} // namespace facebook::axiom::logical_plan
