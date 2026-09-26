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

#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

using namespace facebook::velox;

class FilterSimplificationTest : public optimizer::test::QueryTestBase {
 protected:
  void configureTestConnector() override {
    testConnector_->addTable("t", ROW({"i8", "i64"}, {TINYINT(), BIGINT()}));
  }

  core::PlanNodePtr plan(std::string_view filter) {
    return toSingleNodePlan(parseSelect(
        "SELECT * FROM t WHERE " + std::string(filter), kTestConnectorId));
  }
};

TEST_F(FilterSimplificationTest, alwaysFalseIntegralDomains) {
  const std::vector<std::string> filters{
      "i64 > 0 and i8 < tinyint '-128'",
      "i8 > tinyint '127'",
      "i8 = 256",
      "i8 in (-129, 128)",
      "i64 = 1 and i64 = 2",
      "not (i64 in (1, null))",
  };

  for (const auto& filter : filters) {
    SCOPED_TRACE(filter);
    AXIOM_ASSERT_PLAN(plan(filter), matchValues().build());
  }
}

TEST_F(FilterSimplificationTest, simplifiedIntegralDomains) {
  struct TestCase {
    std::string input;
    std::string expected;
  };

  const std::vector<TestCase> testCases{
      {
          .input = "i8 between -200 and 200",
          .expected = "i8 is not null",
      },
      {
          .input = "i64 in (1, 2, 3) and i64 > 1",
          .expected = "i64 in (2, 3)",
      },
      {
          .input = "i64 between -10 and 10 and i64 > 0",
          .expected = "i64 > 0 and i64 <= 10",
      },
      {
          .input = "i64 between -10 and 10 and i64 >= 0",
          .expected = "i64 between 0 and 10",
      },
      {
          .input = "i64 < 0 or i64 < 10",
          .expected = "i64 < 10",
      },
      {
          .input = "i64 < -10 or i64 > 10",
          .expected = "not (i64 between -10 and 10)",
      },
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.input);
    AXIOM_ASSERT_PLAN(
        plan(testCase.input), matchScan("t").filter(testCase.expected).build());
  }
}

TEST_F(FilterSimplificationTest, alwaysTrueIntegralDomain) {
  AXIOM_ASSERT_PLAN(
      plan("i8 between -200 and 200 or i8 is null"), matchScan("t").build());
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
