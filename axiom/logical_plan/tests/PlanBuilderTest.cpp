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
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::logical_plan {
namespace {

class PlanBuilderTest : public testing::Test {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
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

} // namespace
} // namespace facebook::velox::logical_plan
