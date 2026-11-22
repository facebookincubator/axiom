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

#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/expression/SpecialFormRegistry.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;

// Returns its single argument. Registered only as a callable special form.
class IdentityCallToSpecialForm : public exec::FunctionCallToSpecialForm {
 public:
  TypePtr resolveType(const std::vector<TypePtr>& argTypes) override {
    VELOX_CHECK_EQ(argTypes.size(), 1);
    return argTypes[0];
  }

  exec::ExprPtr constructSpecialForm(
      const TypePtr& /*type*/,
      std::vector<exec::ExprPtr>&& compiledChildren,
      bool /*trackCpuUsage*/,
      const core::QueryConfig& /*config*/) override {
    return std::move(compiledChildren[0]);
  }
};

class CallableSpecialFormTest : public test::QueryTestBase {};

TEST_F(CallableSpecialFormTest, call) {
  exec::registerFunctionCallToSpecialForm(
      "test_identity", std::make_unique<IdentityCallToSpecialForm>());

  auto plan = toSingleNodePlan(
      parseSelect("SELECT test_identity(n_name) AS name FROM nation"));

  auto matcher =
      matchScan("nation").project({"test_identity(n_name) as name"}).build();
  AXIOM_ASSERT_PLAN_V2(plan, matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
