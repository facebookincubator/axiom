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

#include "axiom/optimizer/tests/PlanMatcherGenerator.h"
#include <gtest/gtest.h>
#include <iostream>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class PlanMatcherGeneratorTest : public test::HiveQueriesTestBase {};

TEST_F(PlanMatcherGeneratorTest, filterProject) {
  // Test with an actual SQL query - parse and get logical plan
  auto statement =
      prestoParser().parse("SELECT n_name FROM nation WHERE n_regionkey > 2");
  ASSERT_TRUE(statement->isSelect());
  auto logicalPlan =
      statement->as<::axiom::sql::presto::SelectStatement>()->plan();

  // Convert to single node plan
  auto plan = toSingleNodePlan(logicalPlan);

  // Generate the matcher code
  auto code = core::generatePlanMatcherCode(plan);

  std::cout
      << "Generated code for 'SELECT n_name FROM nation WHERE n_regionkey > 2':\n"
      << code << std::endl;

  EXPECT_FALSE(code.empty());
}

TEST_F(PlanMatcherGeneratorTest, aggregation) {
  // Test with an aggregation query
  auto statement = prestoParser().parse(
      "SELECT n_regionkey, count(*) FROM nation GROUP BY n_regionkey");
  ASSERT_TRUE(statement->isSelect());
  auto logicalPlan =
      statement->as<::axiom::sql::presto::SelectStatement>()->plan();

  // Convert to single node plan
  auto plan = toSingleNodePlan(logicalPlan);

  // Generate the matcher code
  auto code = core::generatePlanMatcherCode(plan);

  std::cout
      << "Generated code for 'SELECT n_regionkey, count(*) FROM nation GROUP BY n_regionkey':\n"
      << code << std::endl;

  EXPECT_FALSE(code.empty());
  EXPECT_TRUE(code.find("Aggregation") != std::string::npos);
}

TEST_F(PlanMatcherGeneratorTest, join) {
  // Test with a join query
  auto statement = prestoParser().parse(
      "SELECT n_name, r_name FROM nation, region WHERE n_regionkey = r_regionkey");
  ASSERT_TRUE(statement->isSelect());
  auto logicalPlan =
      statement->as<::axiom::sql::presto::SelectStatement>()->plan();

  // Convert to single node plan
  auto plan = toSingleNodePlan(logicalPlan);

  // Generate the matcher code
  auto code = core::generatePlanMatcherCode(plan);

  std::cout << "Generated code for join query:\n" << code << std::endl;

  EXPECT_FALSE(code.empty());
  EXPECT_TRUE(code.find("hashJoin") != std::string::npos);
  EXPECT_TRUE(code.find("rightMatcher") != std::string::npos);
}

} // namespace
} // namespace facebook::axiom::optimizer
