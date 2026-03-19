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
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/type/Type.h"

using namespace facebook::velox;

namespace facebook::axiom::logical_plan {
namespace {

class ExprEqualityTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::aggregate::prestosql::registerAllAggregateFunctions();
    velox::window::prestosql::registerAllWindowFunctions();
  }

  static ExprResolver::InputNameResolver inputResolver(
      const RowTypePtr& schema) {
    return [schema](
               const std::optional<std::string>&,
               const std::string& name) -> ExprPtr {
      return std::make_shared<InputReferenceExpr>(
          schema->findChild(name), name);
    };
  }

  ExprPtr resolve(const ExprApi& expr) {
    return resolve(expr, schema_);
  }

  ExprPtr resolve(const ExprApi& expr, const RowTypePtr& schema) {
    return ExprResolver(nullptr, false)
        .resolveScalarTypes(expr.expr(), inputResolver(schema));
  }

  RowTypePtr schema_ =
      ROW({"a", "b", "x", "y", "z"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), ARRAY(BIGINT())});
};

TEST_F(ExprEqualityTest, inputReference) {
  auto x = resolve(Col("x"));
  auto xCopy = resolve(Col("x"));
  auto y = resolve(Col("y"));
  auto xDouble = resolve(Col("x"), ROW("x", DOUBLE()));

  // Same column name and type.
  EXPECT_EQ(*x, *xCopy);
  // Different name.
  EXPECT_NE(*x, *y);
  // Different type.
  EXPECT_NE(*x, *xDouble);
}

TEST_F(ExprEqualityTest, constant) {
  auto literalOne = resolve(Lit(1LL));
  auto literalOneCopy = resolve(Lit(1LL));
  auto literalTwo = resolve(Lit(2LL));
  auto literalNull = resolve(Lit(Variant::null(TypeKind::BIGINT), BIGINT()));
  auto literalNullCopy =
      resolve(Lit(Variant::null(TypeKind::BIGINT), BIGINT()));

  EXPECT_EQ(*literalOne, *literalOneCopy);
  EXPECT_NE(*literalOne, *literalTwo);
  EXPECT_EQ(*literalNull, *literalNullCopy);
  EXPECT_NE(*literalOne, *literalNull);

  // Different types are never equal even if values look similar.
  EXPECT_NE(*literalOne, *resolve(Lit(1.0)));
}

TEST_F(ExprEqualityTest, call) {
  auto plus = resolve(Call("plus", Col("a"), Col("b")));
  auto plusCopy = resolve(Call("plus", Col("a"), Col("b")));
  auto minus = resolve(Call("minus", Col("a"), Col("b")));

  // Same name and inputs.
  EXPECT_EQ(*plus, *plusCopy);
  // Different function name.
  EXPECT_NE(*plus, *minus);
  // Different number of inputs.
  auto plusOneArg = std::make_shared<CallExpr>(
      BIGINT(), "plus", std::vector<ExprPtr>{resolve(Col("a"))});
  EXPECT_NE(*plus, *plusOneArg);
  // Different input values.
  EXPECT_NE(*plus, *resolve(Call("plus", Col("b"), Col("a"))));
}

TEST_F(ExprEqualityTest, specialForm) {
  auto castA = resolve(Cast(DOUBLE(), Col("a")));
  auto castACopy = resolve(Cast(DOUBLE(), Col("a")));
  auto castB = resolve(Cast(DOUBLE(), Col("b")));
  auto castAInteger = resolve(Cast(INTEGER(), Col("a")));

  EXPECT_EQ(*castA, *castACopy);
  EXPECT_NE(*castA, *castB);
  EXPECT_NE(*castA, *castAInteger);

  // Different special form kind.
  EXPECT_NE(*castA, *resolve(TryCast(DOUBLE(), Col("a"))));
}

TEST_F(ExprEqualityTest, aggregate) {
  auto resolveAggregate = [this](
                              const ExprApi& expr,
                              const velox::core::ExprPtr& filter,
                              std::vector<SortKey> ordering,
                              bool distinct) {
    return ExprResolver(nullptr, false)
        .resolveAggregateTypes(
            expr.expr(), inputResolver(schema_), filter, ordering, distinct);
  };

  auto sum = resolveAggregate(
      Call("sum", Col("x")),
      /*filter=*/nullptr,
      /*ordering=*/{},
      /*distinct=*/false);
  auto sumCopy = resolveAggregate(
      Call("sum", Col("x")),
      /*filter=*/nullptr,
      /*ordering=*/{},
      /*distinct=*/false);
  auto avg = resolveAggregate(
      Call("avg", Col("x")),
      /*filter=*/nullptr,
      /*ordering=*/{},
      /*distinct=*/false);

  // Same function, same inputs.
  EXPECT_EQ(*sum, *sumCopy);
  // Different function name.
  EXPECT_NE(*sum, *avg);

  // With distinct flag.
  auto sumDistinct = resolveAggregate(
      Call("sum", Col("x")),
      /*filter=*/nullptr,
      /*ordering=*/{},
      /*distinct=*/true);
  EXPECT_NE(*sum, *sumDistinct);

  // Two distinct aggregates match.
  auto sumDistinctCopy = resolveAggregate(
      Call("sum", Col("x")),
      /*filter=*/nullptr,
      /*ordering=*/{},
      /*distinct=*/true);
  EXPECT_EQ(*sumDistinct, *sumDistinctCopy);

  // With filter.
  auto sumFiltered = resolveAggregate(
      Call("sum", Col("x")),
      /*filter=*/Call("gt", Col("x"), Lit(10LL)).expr(),
      /*ordering=*/{},
      /*distinct=*/false);
  auto sumFilteredCopy = resolveAggregate(
      Call("sum", Col("x")),
      /*filter=*/Call("gt", Col("x"), Lit(10LL)).expr(),
      /*ordering=*/{},
      /*distinct=*/false);
  EXPECT_EQ(*sumFiltered, *sumFilteredCopy);
  EXPECT_NE(*sum, *sumFiltered);

  // With ordering.
  auto sumOrdered = resolveAggregate(
      Call("sum", Col("x")),
      /*filter=*/nullptr,
      /*ordering=*/{SortKey(Col("y"))},
      /*distinct=*/false);
  auto sumOrderedCopy = resolveAggregate(
      Call("sum", Col("x")),
      /*filter=*/nullptr,
      /*ordering=*/{SortKey(Col("y"))},
      /*distinct=*/false);
  EXPECT_EQ(*sumOrdered, *sumOrderedCopy);
  EXPECT_NE(*sum, *sumOrdered);

  // Different sort order.
  auto sumOrderedDesc = resolveAggregate(
      Call("sum", Col("x")),
      /*filter=*/nullptr,
      /*ordering=*/{SortKey(Col("y"), DESC)},
      /*distinct=*/false);
  EXPECT_NE(*sumOrdered, *sumOrderedDesc);
}

TEST_F(ExprEqualityTest, window) {
  auto resolveWindow = [this](const ExprApi& expr) {
    return ExprResolver(nullptr, false)
        .resolveWindowTypes(
            expr.expr(), *expr.windowSpec(), inputResolver(schema_));
  };

  auto spec = WindowSpec()
                  .partitionBy({Col("x")})
                  .orderBy({SortKey(Col("y"))})
                  .rows(
                      WindowExpr::BoundType::kUnboundedPreceding,
                      std::nullopt,
                      WindowExpr::BoundType::kCurrentRow,
                      std::nullopt);

  auto rowNum = resolveWindow(Call("row_number").over(spec));
  auto rowNumCopy = resolveWindow(Call("row_number").over(spec));
  EXPECT_EQ(*rowNum, *rowNumCopy);

  // Different function name.
  EXPECT_NE(*rowNum, *resolveWindow(Call("rank").over(spec)));

  // Different partition keys.
  auto noPartitionSpec = WindowSpec()
                             .orderBy({SortKey(Col("y"))})
                             .rows(
                                 WindowExpr::BoundType::kUnboundedPreceding,
                                 std::nullopt,
                                 WindowExpr::BoundType::kCurrentRow,
                                 std::nullopt);
  EXPECT_NE(*rowNum, *resolveWindow(Call("row_number").over(noPartitionSpec)));

  // Different frame type.
  auto rangeSpec = WindowSpec()
                       .partitionBy({Col("x")})
                       .orderBy({SortKey(Col("y"))})
                       .range(
                           WindowExpr::BoundType::kUnboundedPreceding,
                           std::nullopt,
                           WindowExpr::BoundType::kCurrentRow,
                           std::nullopt);
  EXPECT_NE(*rowNum, *resolveWindow(Call("row_number").over(rangeSpec)));

  // Different ignoreNulls.
  auto ignoreNullsSpec = WindowSpec()
                             .partitionBy({Col("x")})
                             .orderBy({SortKey(Col("y"))})
                             .rows(
                                 WindowExpr::BoundType::kUnboundedPreceding,
                                 std::nullopt,
                                 WindowExpr::BoundType::kCurrentRow,
                                 std::nullopt)
                             .ignoreNulls();
  EXPECT_NE(*rowNum, *resolveWindow(Call("row_number").over(ignoreNullsSpec)));

  // Frame with bound values.
  WindowExpr::Frame frame{
      WindowExpr::WindowType::kRows,
      WindowExpr::BoundType::kPreceding,
      resolve(Lit(5LL)),
      WindowExpr::BoundType::kFollowing,
      resolve(Lit(3LL))};
  WindowExpr::Frame frameCopy{
      WindowExpr::WindowType::kRows,
      WindowExpr::BoundType::kPreceding,
      resolve(Lit(5LL)),
      WindowExpr::BoundType::kFollowing,
      resolve(Lit(3LL))};
  WindowExpr::Frame frameDifferent{
      WindowExpr::WindowType::kRows,
      WindowExpr::BoundType::kPreceding,
      resolve(Lit(10LL)),
      WindowExpr::BoundType::kFollowing,
      resolve(Lit(3LL))};
  EXPECT_EQ(frame, frameCopy);
  EXPECT_NE(frame, frameDifferent);
}

TEST_F(ExprEqualityTest, lambda) {
  auto resolveLambda = [this](const ExprApi& lambda) {
    return resolve(Call("filter", Col("z"), lambda))->inputAt(1);
  };

  auto lam = resolveLambda(Lambda({"x"}, Call("gt", Col("x"), Lit(10LL))));
  auto lamCopy = resolveLambda(Lambda({"x"}, Call("gt", Col("x"), Lit(10LL))));
  EXPECT_EQ(*lam, *lamCopy);

  // Different body.
  EXPECT_NE(
      *lam, *resolveLambda(Lambda({"x"}, Call("lt", Col("x"), Lit(10LL)))));

  // Different signature.
  EXPECT_NE(
      *lam, *resolveLambda(Lambda({"y"}, Call("gt", Col("y"), Lit(10LL)))));
}

TEST_F(ExprEqualityTest, subquery) {
  auto valuesType = ROW({"x", "y"}, {BIGINT(), BIGINT()});
  auto makeValues = [this, &valuesType](std::string id) {
    return std::make_shared<ValuesNode>(
        std::move(id),
        valuesType,
        ValuesNode::Exprs{
            {resolve(Lit(1LL)), resolve(Lit(10LL))},
            {resolve(Lit(2LL)), resolve(Lit(20LL))},
            {resolve(Lit(3LL)), resolve(Lit(30LL))}});
  };

  VELOX_ASSERT_THROW(
      *std::make_shared<SubqueryExpr>(makeValues("1")) ==
          *std::make_shared<SubqueryExpr>(makeValues("1")),
      "Equality comparison is not supported for SubqueryExpr.");
}

TEST_F(ExprEqualityTest, crossKind) {
  auto input = resolve(Col("x"));
  auto constant = resolve(Lit(1LL));
  auto callExpr = resolve(Call("abs", Col("x")));

  // Different kinds are never equal.
  EXPECT_NE(*input, *constant);
  EXPECT_NE(*input, *callExpr);
  EXPECT_NE(*constant, *callExpr);
}

TEST_F(ExprEqualityTest, sortingField) {
  SortingField sortX{resolve(Col("x")), SortOrder::kAscNullsFirst};
  SortingField sortXCopy{resolve(Col("x")), SortOrder::kAscNullsFirst};
  SortingField sortY{resolve(Col("y")), SortOrder::kAscNullsFirst};
  SortingField sortXDesc{resolve(Col("x")), SortOrder::kDescNullsFirst};

  EXPECT_EQ(sortX, sortXCopy);
  EXPECT_NE(sortX, sortY);
  EXPECT_NE(sortX, sortXDesc);
}

} // namespace
} // namespace facebook::axiom::logical_plan
