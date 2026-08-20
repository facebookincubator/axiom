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

#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/ExprMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/parse/ExpressionsParser.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

// Verifies the canonical form of the filter expressions the optimizer hands to
// a connector's createTableHandle for pushdown. This shape is the contract
// documented on ConnectorMetadata::createTableHandle; connector authors rely on
// it.
class ConnectorPushdownTest : public QueryTestBase,
                              public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useV2_ = GetParam();
    QueryTestBase::SetUp();
    testConnector_->addTable(
        "t",
        ROW({"a", "b", "c", "s"}, {BIGINT(), BIGINT(), BIGINT(), VARCHAR()}));
  }

  // Optimizes 'scan t where <filter>' and returns the conjuncts
  // createTableHandle received.
  std::vector<core::TypedExprPtr> pushedFilters(const std::string& filter) {
    std::vector<core::TypedExprPtr> createHandle;
    testConnector_->setOnCreateTableHandle(
        [&](const std::vector<core::TypedExprPtr>& filters) {
          createHandle = filters;
        });
    SCOPE_EXIT {
      testConnector_->setOnCreateTableHandle(nullptr);
    };

    lp::PlanBuilder::Context context(kTestConnectorId, kDefaultSchema);
    auto logicalPlan =
        lp::PlanBuilder(context).tableScan("t").filter(filter).build();
    toSingleNodePlan(logicalPlan);

    return createHandle;
  }

  // Asserts the pushed conjuncts structurally match 'expected' (each an
  // expression in SQL syntax).
  void expectPushed(
      const std::string& filter,
      const std::vector<std::string>& expected) {
    SCOPED_TRACE("filter: " + filter);
    matchAll(pushedFilters(filter), expected);
  }

  void matchAll(
      const std::vector<core::TypedExprPtr>& actual,
      const std::vector<std::string>& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
      core::ExprMatcher::match(actual[i], parser_.parseExpr(expected[i]));
    }
  }

  parse::DuckSqlExpressionsParser parser_;
};

// Equality is canonicalized with the column as the first argument, regardless
// of how the predicate was written.
TEST_P(ConnectorPushdownTest, equality) {
  expectPushed("a = 5", {"a = 5"});
  expectPushed("5 = a", {"a = 5"});
  expectPushed("s = 'x'", {"s = 'x'"});
}

// Reversible comparisons also put the column first, flipping the operator when
// the constant was written on the left.
TEST_P(ConnectorPushdownTest, comparison) {
  expectPushed("a < 10", {"a < 10"});
  expectPushed("10 > a", {"a < 10"});
  expectPushed("a > 5", {"a > 5"});
  expectPushed("5 < a", {"a > 5"});
  expectPushed("a <= 10", {"a <= 10"});
  expectPushed("10 >= a", {"a <= 10"});
  expectPushed("a >= 5", {"a >= 5"});
  expectPushed("5 <= a", {"a >= 5"});
}

// An all-literal IN list is pushed as a deduplicated IN; the multi-value form
// may be a constant array (as v1 emits) or varargs. A list that dedups to a
// single value folds to an equality.
TEST_P(ConnectorPushdownTest, inList) {
  expectPushed("a in (1, 2, 3)", {"a in (1, 2, 3)"});
  expectPushed("a in (5, 5)", {"a = 5"});
}

// Predicates are not combined across conjuncts: a connector may receive several
// predicates on the same column and must handle them.
TEST_P(ConnectorPushdownTest, duplicateColumnPredicates) {
  expectPushed("a = 1 and a = 2", {"a = 1", "a = 2"});
}

// A top-level conjunction is flattened: each leaf predicate is a separate
// conjunct, even when nested.
TEST_P(ConnectorPushdownTest, nestedAndFlattened) {
  expectPushed(
      "a = 1 and (b = 2 and (c = 3 and s = 'x'))",
      {"a = 1", "b = 2", "c = 3", "s = 'x'"});
}

AXIOM_INSTANTIATE_V1_V2(ConnectorPushdownTest);

} // namespace
} // namespace facebook::axiom::optimizer::test
