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

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/optimizer/tests/ExprMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

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

  // Optimizes 'logicalPlan' and returns the conjuncts createTableHandle
  // received.
  std::vector<core::TypedExprPtr> pushedFilters(
      const lp::LogicalPlanNodePtr& logicalPlan) {
    std::vector<core::TypedExprPtr> createHandle;
    testConnector_->setOnCreateTableHandle(
        [&](const std::vector<core::TypedExprPtr>& filters) {
          createHandle = filters;
        });
    SCOPE_EXIT {
      testConnector_->setOnCreateTableHandle(nullptr);
    };

    toSingleNodePlan(logicalPlan);

    return createHandle;
  }

  // Optimizes 'scan t where <filter>'.
  std::vector<core::TypedExprPtr> pushedFilters(std::string_view filter) {
    return pushedFilters(
        parseSelect(fmt::format("SELECT * FROM t WHERE {}", filter)));
  }

  // Asserts the pushed conjuncts structurally match 'expected' (each an
  // expression in SQL syntax).
  void expectPushed(
      std::string_view filter,
      std::initializer_list<std::string> expected) {
    SCOPED_TRACE(fmt::format("filter: {}", filter));
    matchAll(pushedFilters(filter), std::vector<std::string>(expected));
  }

  void matchAll(
      const std::vector<core::TypedExprPtr>& actual,
      const std::vector<std::string>& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
      core::ExprMatcher::match(actual[i], lp::DuckSql(expected[i]).expr());
    }
  }
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

// A singleton row-valued IN becomes independently pushable field equalities.
TEST_P(ConnectorPushdownTest, singleRowIn) {
  if (!useV2_) {
    return;
  }

  expectPushed("(a, b) IN ((1, 2))", {"a = 1", "b = 2"});
  expectPushed("(a, b) IN ((1, 2), (1, 2))", {"a = 1", "b = 2"});
  expectPushed("(a, b) = (1, 2)", {"a = 1", "b = 2"});

  AXIOM_ASSERT_PLAN_V2(
      toSingleNodePlan(
          parseSelect("SELECT * FROM t WHERE (a, b) IN ((1, NULL))")),
      matchValues().build());
}

// A multi-row IN exposes the values for each field while retaining the
// original predicate to preserve correlations between fields.
TEST_P(ConnectorPushdownTest, multiRowIn) {
  if (!useV2_) {
    return;
  }

  {
    const std::string rowIn = R"("in"(row_constructor(a, b), "any"()))";

    expectPushed(
        "(a, b) IN ((1, 2), (3, 4))", {rowIn, "a IN (1, 3)", "b IN (2, 4)"});
    expectPushed(
        "(a, b) IN ((1, 2), (1, 3), NULL)", {rowIn, "a = 1", "b IN (2, 3)"});
    expectPushed(
        "(a, b) IN ((1, NULL), (2, 3))", {rowIn, "a IN (1, 2)", "b = 3"});
    expectPushed("(a, b) IN ((1, 2), NULL)", {rowIn, "a = 1", "b = 2"});
  }

  expectPushed(
      "(random(), b) IN ((DOUBLE '1', 2), (DOUBLE '3', 4))",
      {R"("in"(row_constructor(rand(), b), "any"()))", "b IN (2, 4)"});

  expectPushed("ROW(a) IN (ROW(1), ROW(2))", {"a IN (1, 2)"});
  expectPushed("ROW(a) IN (ROW(1), NULL)", {"a = 1"});
}

// Predicates that cannot be combined remain separate, so a connector must
// handle several predicates on the same column.
TEST_P(ConnectorPushdownTest, duplicateColumnPredicates) {
  expectPushed("s > 'a' and s < 'z'", {"s > 'a'", "s < 'z'"});
}

// A top-level conjunction is flattened: each leaf predicate is a separate
// conjunct, even when nested.
TEST_P(ConnectorPushdownTest, nestedAndFlattened) {
  expectPushed(
      "a = 1 and (b = 2 and (c = 3 and s = 'x'))",
      {"a = 1", "b = 2", "c = 3", "s = 'x'"});
}

// Derived filters include only columns present in every disjunct. The original
// OR remains to preserve correlations between columns.
TEST_P(ConnectorPushdownTest, filtersImpliedByOr) {
  if (!useV2_) {
    return;
  }

  const std::string lowRange = "a > 1 and a < 5 and a <> 2 and a <> 3";
  const std::string highRange = "a > 20 and a < 30 and a <> 21 and a <> 22";
  const std::string firstMixedShape =
      "(" + lowRange + ") or (" + highRange + " and b = 20)";
  const std::string secondMixedShape =
      "(" + lowRange + " and c = 10) or (" + highRange + ")";
  const std::string nestedAllColumns =
      "(a = 1 and ((b = 10 and c = 100) or (b = 20 and c = 200))) or "
      "(a = 2 and ((b = 30 and c = 300) or (b = 40 and c = 400)))";
  const std::string nestedPartialColumns =
      "(a = 1 and (b = 10 or c = 100)) or "
      "(a = 2 and (b = 20 or c = 200))";
  const std::vector<std::pair<std::string, std::vector<std::string>>> cases{
      {
          "(a = 1 and b = 10) or (a = 2 and b = 20)",
          {
              "(a = 1 and b = 10) or (a = 2 and b = 20)",
              "a in (1, 2)",
              "b in (10, 20)",
          },
      },
      {
          nestedAllColumns,
          {
              nestedAllColumns,
              "a in (1, 2)",
              "b in (10, 20, 30, 40)",
              "c in (100, 200, 300, 400)",
          },
      },
      {
          nestedPartialColumns,
          {
              nestedPartialColumns,
              "a in (1, 2)",
          },
      },
      {
          "(a > 1 and a < 5 and b = 10) or "
          "(a > 20 and a < 30 and b = 20)",
          {
              "\"and\"(a > 1, a < 5, b = 10) or "
              "\"and\"(a > 20, a < 30, b = 20)",
              "(a > 1 and a < 5) or (a > 20 and a < 30)",
              "b in (10, 20)",
          },
      },
      {
          "(a = 1 and b = 10) or a = 2",
          {
              "(a = 1 and b = 10) or a = 2",
              "a in (1, 2)",
          },
      },
      {
          "a = 1 or a = 2",
          {
              "a in (1, 2)",
          },
      },
      {
          "a = 1 or a = 2 or a = 3",
          {
              "a in (1, 2, 3)",
          },
      },
      {
          "(a = 1 or a = 2 or a = 3 or a = 4) and "
          "((a = 1 and b = 10) or (a = 2 and b = 20) or "
          "(a = 3 and b = 30) or (a = 4 and b = 40))",
          {
              "a in (1, 2, 3, 4)",
              "\"or\"(a = 1 and b = 10, a = 2 and b = 20, a = 3 and b = 30, a = 4 and b = 40)",
              "b in (10, 20, 30, 40)",
          },
      },
      {
          "(" + firstMixedShape + ") and (" + secondMixedShape + ")",
          {
              "\"and\"(a > 1, a < 5, a <> 2, a <> 3) or "
              "\"and\"(a > 20, a < 30, a <> 21, a <> 22, b = 20)",
              "\"and\"(a > 1, a < 5, a <> 2, a <> 3, c = 10) or "
              "\"and\"(a > 20, a < 30, a <> 21, a <> 22)",
              "\"and\"(a > 1 and a < 5, a <> 2 and a <> 3) or "
              "\"and\"(a > 20 and a < 30, a <> 21 and a <> 22)",
          },
      },
  };
  for (const auto& [filter, expected] : cases) {
    SCOPED_TRACE(filter);
    matchAll(pushedFilters(filter), expected);
  }
}

AXIOM_INSTANTIATE_V1_V2(ConnectorPushdownTest);

} // namespace
} // namespace facebook::axiom::optimizer::test
