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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/optimizer/VeloxExprImporter.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;

class VeloxExprImporterTest : public optimizer::test::QueryTestBase {
 protected:
  void configureTestConnector() override {
    testConnector_->addTable(
        "t", ROW({"a", "b", "r"}, {BIGINT(), BIGINT(), ROW("x", BIGINT())}));
  }
};

TEST_F(VeloxExprImporterTest, expressions) {
  const auto plan = parseSelect("SELECT * FROM t");

  verifyOptimization(*plan, Optimizer::Pass::kTranslate, [&](NodeCP root) {
    Builder builder;
    folly::F14FastMap<std::string_view, ColumnCP> columns;
    for (ColumnCP column : root->outputColumns()) {
      columns.emplace(column->name(), column);
    }
    VeloxExprImporter importer(builder, columns);

    const auto a = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "a");
    const auto b = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "b");
    const auto r =
        std::make_shared<core::FieldAccessTypedExpr>(ROW("x", BIGINT()), "r");
    const auto one = std::make_shared<core::ConstantTypedExpr>(
        BIGINT(), Variant(int64_t{1}));
    const auto aGreaterThanOne = std::make_shared<core::CallTypedExpr>(
        BOOLEAN(), "gt", std::vector<core::TypedExprPtr>{a, one});
    const auto& andName = queryCtx()->functionRegistry().specialForm(
        logical_plan::SpecialForm::kAnd);

    const std::vector<std::pair<core::TypedExprPtr, std::string>> cases{
        {a, "a"},
        {one, "1"},
        {std::make_shared<core::CallTypedExpr>(
             BIGINT(), "plus", std::vector<core::TypedExprPtr>{a, one}),
         "plus(a, 1)"},
        {std::make_shared<core::CallTypedExpr>(
             BOOLEAN(),
             andName,
             std::vector<core::TypedExprPtr>{aGreaterThanOne, aGreaterThanOne}),
         "__and(gt(a, 1), gt(a, 1))"},
        {std::make_shared<core::CastTypedExpr>(VARCHAR(), a, false),
         "__cast(a as VARCHAR)"},
        {std::make_shared<core::CastTypedExpr>(VARCHAR(), a, true),
         "__try_cast(a as VARCHAR)"},
        {std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), r, "x"), "r.x"},
        {std::make_shared<core::DereferenceTypedExpr>(BIGINT(), r, 0), "r.0"},
        {std::make_shared<core::InputTypedExpr>(ROW({"a", "b"}, BIGINT())),
         "__row_constructor(a, b)"},
        {std::make_shared<core::ConcatTypedExpr>(
             std::vector<std::string>{"a", "b"},
             std::vector<core::TypedExprPtr>{a, b}),
         "__row_constructor(a, b)"},
        {std::make_shared<core::NullIfTypedExpr>(a, b, BIGINT()),
         "__nullif(a, b, null)"},
    };
    for (const auto& [expr, expected] : cases) {
      SCOPED_TRACE(expr->toString());
      EXPECT_EQ(importer.importExpr(expr)->toString(), expected);
    }

    // A lambda binds its own parameter; its body also reads an outer column.
    ExprCP lambda = importer.importExpr(
        std::make_shared<core::LambdaTypedExpr>(
            ROW("x", BIGINT()),
            std::make_shared<core::CallTypedExpr>(
                BIGINT(),
                "plus",
                std::vector<core::TypedExprPtr>{
                    std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "x"),
                    a})));
    ASSERT_TRUE(lambda->is(PlanType::kLambdaExpr));
    const auto& parameters = lambda->as<Lambda>()->args();
    ASSERT_EQ(parameters.size(), 1);
    const auto* body = lambda->as<Lambda>()->body()->as<Call>();
    EXPECT_EQ(std::string_view{body->name()}, "plus");
    EXPECT_THAT(
        body->args(),
        testing::UnorderedElementsAre(
            static_cast<ExprCP>(columns.at("a")),
            static_cast<ExprCP>(parameters.front())));
    VELOX_ASSERT_THROW(
        importer.importExpr(
            std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "x")),
        "Expression reads a column outside its bindings: x");
  });
}

TEST_F(VeloxExprImporterTest, unboundColumn) {
  const auto plan = parseSelect("SELECT * FROM t");

  verifyOptimization(*plan, Optimizer::Pass::kTranslate, [&](NodeCP root) {
    Builder builder;
    folly::F14FastMap<std::string_view, ColumnCP> columns;
    columns.emplace(
        root->outputColumns().front()->name(), root->outputColumns().front());
    VeloxExprImporter importer(builder, std::move(columns));

    VELOX_ASSERT_THROW(
        importer.importExpr(
            std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "b")),
        "Expression reads a column outside its bindings: b");
  });
}

} // namespace
} // namespace facebook::axiom::optimizer::test
