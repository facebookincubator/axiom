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

#include "axiom/logical_plan/Expr.h"
#include <gtest/gtest.h>
#include "velox/type/Type.h"

using namespace facebook::velox;

namespace facebook::axiom::logical_plan {
namespace {

class ExprTest : public testing::Test {
 protected:
  static ExprPtr literal(int64_t val, TypePtr type = BIGINT()) {
    return std::make_shared<ConstantExpr>(
        std::move(type), std::make_shared<Variant>(Variant(val)));
  }

  static ExprPtr inputRef(std::string name, TypePtr type = BIGINT()) {
    return std::make_shared<InputReferenceExpr>(
        std::move(type), std::move(name));
  }

  static ExprPtr
  call(std::string name, std::vector<ExprPtr> inputs, TypePtr type = BIGINT()) {
    return std::make_shared<CallExpr>(
        std::move(type), std::move(name), std::move(inputs));
  }

  static ExprPtr cast(TypePtr type, ExprPtr input) {
    return std::make_shared<SpecialFormExpr>(
        std::move(type), SpecialForm::kCast, std::vector<ExprPtr>{input});
  }

  static void testLooksConstant(const ExprPtr& expr, bool expected) {
    EXPECT_EQ(expr->looksConstant(), expected);
  }
};

TEST_F(ExprTest, looksConstant) {
  testLooksConstant(literal(1), true);
  testLooksConstant(inputRef("a"), false);

  testLooksConstant(call("plus", {literal(1), literal(2)}), true);
  testLooksConstant(call("plus", {inputRef("a"), literal(1)}), false);

  testLooksConstant(cast(DOUBLE(), literal(1)), true);
  testLooksConstant(cast(DOUBLE(), inputRef("a")), false);
}

} // namespace
} // namespace facebook::axiom::logical_plan
