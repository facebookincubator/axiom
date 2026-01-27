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

#include "axiom/logical_plan/PlanBuilder.h"
#include <gtest/gtest.h>
#include "axiom/sql/presto/tests/LogicalPlanMatcher.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;

namespace facebook::axiom::logical_plan {
namespace {

class PlanBuilderTest : public testing::Test {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
  }

 protected:
  PlanBuilder makeEmptyValues(
      PlanBuilder::Context& context,
      const std::vector<TypePtr>& types) {
    std::vector<std::string> names;
    names.reserve(types.size());
    for (size_t i = 0; i < types.size(); ++i) {
      names.push_back(fmt::format("c{}", i));
    }
    return PlanBuilder(context).values(
        ROW(std::move(names), types), ValuesNode::Variants{});
  }
};

TEST_F(PlanBuilderTest, outputNames) {
  auto builder = PlanBuilder()
                     .values(
                         ROW({"a"}, {BIGINT()}),
                         std::vector<Variant>{Variant::row({123LL})})
                     .project({"a + 1", "a + 2 as b"});

  EXPECT_EQ(2, builder.numOutput());
  EXPECT_EQ("expr", builder.findOrAssignOutputNameAt(0));
  EXPECT_EQ("b", builder.findOrAssignOutputNameAt(1));

  builder.with({"b * 2"});

  EXPECT_EQ(3, builder.numOutput());

  const auto outputNames = builder.findOrAssignOutputNames();
  EXPECT_EQ(3, outputNames.size());
  EXPECT_EQ("expr", outputNames[0]);
  EXPECT_EQ("b", outputNames[1]);
  EXPECT_EQ("expr_0", outputNames[2]);
}

TEST_F(PlanBuilderTest, setOperationTypeCoercion) {
  auto startMatcher = [] { return test::LogicalPlanMatcherBuilder().values(); };

  // (INTEGER, REAL) + (BIGINT, DOUBLE) -> (BIGINT, DOUBLE)
  {
    PlanBuilder::Context context;
    auto plan = PlanBuilder(context, /*enableCoercions=*/true)
                    .setOperation(
                        SetOperation::kUnionAll,
                        {
                            makeEmptyValues(context, {INTEGER(), REAL()}),
                            makeEmptyValues(context, {BIGINT(), DOUBLE()}),
                        })
                    .build();

    EXPECT_EQ(*plan->outputType(), *ROW({"c0", "c1"}, {BIGINT(), DOUBLE()}));

    auto matcher =
        startMatcher()
            .project()
            .setOperation(SetOperation::kUnionAll, startMatcher().build())
            .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString();
  }

  // Same types stay the same. No project nodes needed.
  {
    PlanBuilder::Context context;
    auto plan = PlanBuilder(context, /*enableCoercions=*/true)
                    .setOperation(
                        SetOperation::kUnionAll,
                        {
                            makeEmptyValues(context, {BIGINT()}),
                            makeEmptyValues(context, {BIGINT()}),
                        })
                    .build();

    EXPECT_EQ(*plan->outputType(), *ROW({"c0"}, {BIGINT()}));

    auto matcher =
        startMatcher()
            .setOperation(SetOperation::kUnionAll, startMatcher().build())
            .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString();
  }

  // Incompatible types fail.
  {
    PlanBuilder::Context context;
    VELOX_ASSERT_THROW(
        PlanBuilder(context, /*enableCoercions=*/true)
            .setOperation(
                SetOperation::kUnionAll,
                {
                    makeEmptyValues(context, {VARCHAR()}),
                    makeEmptyValues(context, {INTEGER()}),
                })
            .build(),
        "Output schemas of all inputs to a Set operation must match");
  }

  // Mismatched types fail when coercions are disabled.
  {
    PlanBuilder::Context context;
    VELOX_ASSERT_THROW(
        PlanBuilder(context, /*enableCoercions=*/false)
            .setOperation(
                SetOperation::kUnionAll,
                {
                    makeEmptyValues(context, {INTEGER()}),
                    makeEmptyValues(context, {BIGINT()}),
                })
            .build(),
        "Output schemas of all inputs to a Set operation must match");
  }
}

} // namespace
} // namespace facebook::axiom::logical_plan
