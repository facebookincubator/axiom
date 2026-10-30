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

#include <folly/String.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/v2/EstimateProvider.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

// Asserts that the constraint has the expected min and max bounds.
// Use std::nullopt for either bound to assert it is absent.
#define AXIOM_ASSERT_RANGE(constraint, expectedMin, expectedMax) \
  do {                                                           \
    std::optional<int64_t> _min = (expectedMin);                 \
    std::optional<int64_t> _max = (expectedMax);                 \
    if (_min.has_value()) {                                      \
      ASSERT_NE((constraint).min, nullptr);                      \
      EXPECT_EQ((constraint).min->value<int64_t>(), *_min);      \
    } else {                                                     \
      ASSERT_EQ((constraint).min, nullptr);                      \
    }                                                            \
    if (_max.has_value()) {                                      \
      ASSERT_NE((constraint).max, nullptr);                      \
      EXPECT_EQ((constraint).max->value<int64_t>(), *_max);      \
    } else {                                                     \
      ASSERT_EQ((constraint).max, nullptr);                      \
    }                                                            \
  } while (false)

// Asserts that the constraint has no min or max bounds.
#define AXIOM_ASSERT_NORANGE(constraint)  \
  do {                                    \
    ASSERT_EQ((constraint).min, nullptr); \
    ASSERT_EQ((constraint).max, nullptr); \
  } while (false)

class EstimateProviderTest : public optimizer::test::QueryTestBase {
 protected:
  void configureTestConnector() override {
    // No statistics for 'e'.
    testConnector_->addTable("t", velox::ROW({"a", "b", "e"}, velox::BIGINT()))
        ->setStats(
            10'000,
            {
                // 1'000 values from 0 to 999, no nulls.
                {"a",
                 {
                     .min = velox::Variant(int64_t{0}),
                     .max = velox::Variant(int64_t{999}),
                     .numDistinct = 1'000,
                 }},
                // 10 values; half of the rows are null.
                {"b",
                 {
                     .nullPct = 50,
                     .numDistinct = 10,
                 }},
            });
    // No column statistics: every NDV of 'u' is unknown.
    testConnector_->addTable("u", velox::ROW({"c", "d"}, velox::BIGINT()))
        ->setStats(1'000, {});
    // 500 distinct rows 'r'. Field 'x' has 100 values from 0 to 99; field 'y'
    // has no statistics.
    testConnector_
        ->addTable(
            "v", velox::ROW({"r"}, {velox::ROW({"x", "y"}, velox::BIGINT())}))
        ->setStats(
            1'000,
            {{"r",
              {
                  .numDistinct = 500,
                  .children =
                      {
                          {
                              .min = velox::Variant(int64_t{0}),
                              .max = velox::Variant(int64_t{99}),
                              .numDistinct = 100,
                          },
                          {},
                      },
              }}});
  }

  // Same as the overload below, for the plan 'sql' parses to.
  void verifyEstimate(
      std::string_view sql,
      std::optional<float> cardinality,
      const std::function<void(NodeCP root, const Estimate& estimate)>& check) {
    SCOPED_TRACE(sql);
    verifyEstimate(*parseSelect(sql), cardinality, check);
  }

  // Plans 'plan' to just before and just after physical planning, checks that
  // the plan's root has 'cardinality' rows at both, and calls 'check' with the
  // root and its estimate at both: an estimate does not depend on placement.
  void verifyEstimate(
      const logical_plan::LogicalPlanNode& plan,
      std::optional<float> cardinality,
      const std::function<void(NodeCP root, const Estimate& estimate)>& check) {
    for (const auto pass :
         {Optimizer::Pass::kLowerRepeat, Optimizer::Pass::kPlanPhysical}) {
      SCOPED_TRACE(
          pass == Optimizer::Pass::kLowerRepeat ? "before placement"
                                                : "after placement");
      verifyOptimization(plan, pass, [&](NodeCP root) {
        EstimateProvider estimateProvider;
        const Estimate& estimate = estimateProvider.estimate(root);
        EXPECT_EQ(estimate.cardinality, cardinality);
        check(root, estimate);
      });
    }
  }

  // Returns the estimated statistics of the output column of 'root'
  // named 'name'.
  static const Value&
  column(NodeCP root, const Estimate& estimate, std::string_view name) {
    for (ColumnCP column : root->outputColumns()) {
      if (column->outputName() == name) {
        return value(estimate.constraints, column);
      }
    }
    std::vector<std::string> names;
    for (ColumnCP column : root->outputColumns()) {
      names.push_back(column->outputName());
    }
    VELOX_FAIL(
        "No output column: {}; outputs are {}", name, folly::join(", ", names));
  }

  // Returns the first node of 'type' in the tree under 'root', depth first.
  static NodeCP findNode(NodeCP root, NodeType type) {
    if (root->is(type)) {
      return root;
    }
    for (NodeCP input : root->inputs()) {
      if (NodeCP found = findNode(input, type)) {
        return found;
      }
    }
    return nullptr;
  }
};

TEST_F(EstimateProviderTest, scan) {
  verifyEstimate(
      "SELECT a, b FROM t", 10'000, [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'000);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, 0);

        const Value& b = column(root, estimate, "b");
        EXPECT_EQ(b.cardinality, 10);
        AXIOM_ASSERT_NORANGE(b);
        EXPECT_EQ(b.nullFraction, 0.5f);
      });

  verifyEstimate(
      "SELECT c FROM u", 1'000, [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& c = column(root, estimate, "c");
        EXPECT_EQ(c.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(c);
        EXPECT_EQ(c.nullFraction, 0);
      });
}

TEST_F(EstimateProviderTest, filter) {
  // One of 1'000 values of 'a': 10'000 / 1'000 = 10 rows, each with 'a' = 5.
  verifyEstimate(
      "SELECT a FROM t WHERE a = 5",
      10,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1);
        AXIOM_ASSERT_RANGE(a, 5, 5);
        EXPECT_EQ(a.nullFraction, 0);
      });

  // The selectivity is unknown without the NDV of 'c'.
  verifyEstimate(
      "SELECT c FROM u WHERE c = 5",
      std::nullopt,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& c = column(root, estimate, "c");
        EXPECT_EQ(c.cardinality, 1);
        AXIOM_ASSERT_RANGE(c, 5, 5);
        EXPECT_EQ(c.nullFraction, 0);
      });
}

TEST_F(EstimateProviderTest, projection) {
  // A comparison has at most two values, whatever the NDVs it compares.
  verifyEstimate(
      "SELECT a = b AS c FROM t",
      10'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& c = column(root, estimate, "c");
        EXPECT_EQ(c.cardinality, 2);
      });

  // A lambda's inputs are the columns it reads from outside: 'a', 1'000
  // values.
  verifyEstimate(
      "SELECT transform(ARRAY[CAST(1 AS BIGINT)], x -> x + a) AS y FROM t",
      10'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& y = column(root, estimate, "y");
        EXPECT_EQ(y.cardinality, 1'000);
      });
}

TEST_F(EstimateProviderTest, aggregate) {
  verifyEstimate(
      "SELECT count(*) AS n FROM t",
      1,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& n = column(root, estimate, "n");
        EXPECT_EQ(n.cardinality, 1);
        AXIOM_ASSERT_NORANGE(n);
        EXPECT_EQ(n.nullFraction, std::nullopt);
      });

  // A grouping key keeps its statistics.
  verifyEstimate(
      "SELECT a, count(*) AS n FROM t GROUP BY a",
      1'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'000);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, 0);

        const Value& n = column(root, estimate, "n");
        EXPECT_EQ(n.cardinality, 1'000);
        AXIOM_ASSERT_NORANGE(n);
        EXPECT_EQ(n.nullFraction, std::nullopt);
      });

  // The nulls of a grouping key form one more group: 10 + 1 = 11 groups, one
  // of them null.
  verifyEstimate(
      "SELECT b, count(*) AS n FROM t GROUP BY b",
      11,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& b = column(root, estimate, "b");
        EXPECT_EQ(b.cardinality, 10);
        AXIOM_ASSERT_NORANGE(b);
        EXPECT_EQ(b.nullFraction, 1.0f / 11);

        const Value& n = column(root, estimate, "n");
        EXPECT_EQ(n.cardinality, 11);
        AXIOM_ASSERT_NORANGE(n);
        EXPECT_EQ(n.nullFraction, std::nullopt);
      });

  // 1'000 * (10 + 1) = 11'000 groups, capped at 10'000 rows. 'b' is null in
  // 1 of its 11 groups.
  verifyEstimate(
      "SELECT a, b, count(*) AS n FROM t GROUP BY a, b",
      10'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(3, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'000);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, 0);

        const Value& b = column(root, estimate, "b");
        EXPECT_EQ(b.cardinality, 10);
        AXIOM_ASSERT_NORANGE(b);
        EXPECT_EQ(b.nullFraction, 1.0f / 11);

        const Value& n = column(root, estimate, "n");
        EXPECT_EQ(n.cardinality, 10'000);
        AXIOM_ASSERT_NORANGE(n);
        EXPECT_EQ(n.nullFraction, std::nullopt);
      });

  // The group count is unknown without the NDV of 'e', but 'b' is still null
  // in 1 of its 11 groups.
  verifyEstimate(
      "SELECT b, e, count(*) AS n FROM t GROUP BY b, e",
      std::nullopt,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(3, root->outputColumns().size());

        const Value& b = column(root, estimate, "b");
        EXPECT_EQ(b.cardinality, 10);
        AXIOM_ASSERT_NORANGE(b);
        EXPECT_EQ(b.nullFraction, 1.0f / 11);

        const Value& e = column(root, estimate, "e");
        EXPECT_EQ(e.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(e);
        EXPECT_EQ(e.nullFraction, 0);

        const Value& n = column(root, estimate, "n");
        EXPECT_EQ(n.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(n);
        EXPECT_EQ(n.nullFraction, std::nullopt);
      });

  // An all-null key forms a single group.
  verifyEstimate(
      "SELECT x, count(*) AS n "
      "FROM (SELECT CAST(NULL AS BIGINT) AS x, a FROM t) "
      "GROUP BY x",
      1,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& x = column(root, estimate, "x");
        EXPECT_EQ(x.cardinality, 1);
        AXIOM_ASSERT_NORANGE(x);
        EXPECT_EQ(x.nullFraction, 1);

        const Value& n = column(root, estimate, "n");
        EXPECT_EQ(n.cardinality, 1);
        AXIOM_ASSERT_NORANGE(n);
        EXPECT_EQ(n.nullFraction, std::nullopt);
      });

  verifyEstimate(
      "SELECT c, count(*) AS n FROM u GROUP BY c",
      std::nullopt,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& c = column(root, estimate, "c");
        EXPECT_EQ(c.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(c);
        EXPECT_EQ(c.nullFraction, 0);

        const Value& n = column(root, estimate, "n");
        EXPECT_EQ(n.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(n);
        EXPECT_EQ(n.nullFraction, std::nullopt);
      });
}

TEST_F(EstimateProviderTest, join) {
  // 10'000 * 10'000 / 1'000 = 100'000 rows: each pair matches on one of 1'000
  // values.
  verifyEstimate(
      "SELECT t.a, t.b FROM t JOIN (SELECT a AS k FROM t) v ON t.a = v.k",
      100'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'000);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, 0);

        const Value& b = column(root, estimate, "b");
        EXPECT_EQ(b.cardinality, 10);
        AXIOM_ASSERT_NORANGE(b);
        EXPECT_EQ(b.nullFraction, 0.5f);
      });

  // The NDVs of 'u' are unknown.
  verifyEstimate(
      "SELECT t.a, u.d FROM t JOIN u ON t.a = u.c",
      std::nullopt,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'000);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, 0);

        const Value& d = column(root, estimate, "d");
        EXPECT_EQ(d.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(d);
        EXPECT_EQ(d.nullFraction, 0);
      });

  // A null-extended column has an unknown null fraction.
  verifyEstimate(
      "SELECT t.a, u.d FROM t LEFT JOIN u ON t.a = u.c",
      std::nullopt,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'000);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, 0);

        const Value& d = column(root, estimate, "d");
        EXPECT_EQ(d.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(d);
        EXPECT_EQ(d.nullFraction, std::nullopt);
      });

  // The right side has one row per join key, so each row of 't' appears once,
  // although the NDVs of 'u' are unknown.
  verifyEstimate(
      "SELECT t.a, v.n "
      "FROM t LEFT JOIN (SELECT c, count(*) AS n FROM u GROUP BY c) v ON t.a = v.c",
      10'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'000);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, 0);

        const Value& n = column(root, estimate, "n");
        EXPECT_EQ(n.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(n);
        EXPECT_EQ(n.nullFraction, std::nullopt);
      });

  // The left side has one row per join key, so each row of 'u' appears once,
  // although the NDVs of 'u' are unknown.
  verifyEstimate(
      "SELECT v.b, u.d "
      "FROM (SELECT b, count(*) AS n FROM t GROUP BY b) v "
      "RIGHT JOIN u ON v.b = u.c",
      1'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& b = column(root, estimate, "b");
        EXPECT_EQ(b.cardinality, 10);
        AXIOM_ASSERT_NORANGE(b);
        EXPECT_EQ(b.nullFraction, std::nullopt);

        const Value& d = column(root, estimate, "d");
        EXPECT_EQ(d.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(d);
        EXPECT_EQ(d.nullFraction, 0);
      });

  // The right side is a join that keeps the unique key 'c' of its left input,
  // whose rows each match at most one distinct 'd'.
  verifyEstimate(
      "SELECT t.a, v.n "
      "FROM t "
      "LEFT JOIN ("
      "  SELECT w.c, w.n "
      "  FROM (SELECT c, count(*) AS n FROM u GROUP BY c) w "
      "  JOIN (SELECT DISTINCT d FROM u) x ON w.n = x.d"
      ") v ON t.a = v.c",
      10'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'000);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, 0);

        const Value& n = column(root, estimate, "n");
        EXPECT_EQ(n.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(n);
        EXPECT_EQ(n.nullFraction, std::nullopt);
      });

  // 't' may repeat a join key.
  verifyEstimate(
      "SELECT v.c, t.a "
      "FROM t RIGHT JOIN (SELECT DISTINCT c FROM u) v ON t.a = v.c",
      std::nullopt,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'000);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, std::nullopt);

        const Value& c = column(root, estimate, "c");
        EXPECT_EQ(c.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(c);
        EXPECT_EQ(c.nullFraction, 0);
      });
}

TEST_F(EstimateProviderTest, limit) {
  for (std::string_view sql :
       {"SELECT a FROM t LIMIT 10", "SELECT a FROM t ORDER BY a LIMIT 10"}) {
    verifyEstimate(sql, 10, [&](NodeCP root, const Estimate& estimate) {
      ASSERT_EQ(1, root->outputColumns().size());

      const Value& a = column(root, estimate, "a");
      EXPECT_EQ(a.cardinality, 10);
      AXIOM_ASSERT_RANGE(a, 0, 999);
      EXPECT_EQ(a.nullFraction, 0);
    });
  }

  verifyEstimate(
      "SELECT c FROM u LIMIT 10",
      10,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& c = column(root, estimate, "c");
        EXPECT_EQ(c.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(c);
        EXPECT_EQ(c.nullFraction, 0);
      });
}

TEST_F(EstimateProviderTest, rowNumber) {
  // At most 2 rows for each of 10 + 1 values of 'b', including null:
  // 11 * 2 = 22 rows.
  verifyEstimate(
      "SELECT a, b "
      "FROM ("
      "  SELECT a, b, row_number() OVER (PARTITION BY b ORDER BY a) AS r "
      "  FROM t"
      ") "
      "WHERE r <= 2",
      22,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 22);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, 0);

        const Value& b = column(root, estimate, "b");
        EXPECT_EQ(b.cardinality, 10);
        AXIOM_ASSERT_NORANGE(b);
        EXPECT_EQ(b.nullFraction, 0.5f);
      });
}

// An unnested array's elements, and an unnested map's keys and values, take
// the statistics of the values they are drawn from.
TEST_F(EstimateProviderTest, unnest) {
  // An array of unknown size has 10 elements: 10'000 * 10 = 100'000 rows.
  verifyEstimate(
      "SELECT x FROM t CROSS JOIN UNNEST(sequence(1, a)) AS r(x)",
      100'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& x = column(root, estimate, "x");
        EXPECT_EQ(x.cardinality, 1'000);
        AXIOM_ASSERT_NORANGE(x);
        EXPECT_EQ(x.nullFraction, std::nullopt);
      });

  // array_agg draws its elements from 'a', and map_agg its keys from 'a' and
  // its values from 'b'. One row of 10 elements caps each NDV at 10.
  verifyEstimate(
      "SELECT x FROM (SELECT array_agg(a) AS arr FROM t) "
      "CROSS JOIN UNNEST(arr) AS r(x)",
      10,
      [&](NodeCP root, const Estimate& estimate) {
        const Value& x = column(root, estimate, "x");
        EXPECT_EQ(x.cardinality, 10);
        AXIOM_ASSERT_RANGE(x, 0, 999);
      });

  verifyEstimate(
      "SELECT k, v FROM (SELECT map_agg(a, b) AS m FROM t) "
      "CROSS JOIN UNNEST(m) AS r(k, v)",
      10,
      [&](NodeCP root, const Estimate& estimate) {
        const Value& k = column(root, estimate, "k");
        EXPECT_EQ(k.cardinality, 10);
        AXIOM_ASSERT_RANGE(k, 0, 999);
        const Value& v = column(root, estimate, "v");
        EXPECT_EQ(v.cardinality, 10);
        AXIOM_ASSERT_NORANGE(v);
      });

  // EXISTS over a row's array unnests it keeping the rows with an empty
  // array, whose element is NULL. The element's bounds and NDV still hold, and
  // the share of NULLs is unknown.
  testConnector_
      ->addTable("w", velox::ROW("arr", velox::ARRAY(velox::BIGINT())))
      ->setStats(
          100,
          {{"arr",
            {.children = {{
                 .min = velox::Variant(int64_t{0}),
                 .max = velox::Variant(int64_t{9}),
                 .numDistinct = 10,
             }}}}});
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("w");
  };
  verifyEstimate(
      "SELECT 1 FROM w "
      "WHERE EXISTS (SELECT 1 FROM UNNEST(w.arr) AS u(x) WHERE x > 5)",
      50,
      [&](NodeCP root, const Estimate& /*estimate*/) {
        NodeCP node = findNode(root, NodeType::kUnnest);
        ASSERT_NE(node, nullptr);
        const auto* unnest = node->as<Unnest>();
        ASSERT_NE(unnest->markerColumn(), nullptr);
        EstimateProvider estimateProvider;
        const Value& x = value(
            estimateProvider.estimate(unnest).constraints,
            unnest->unnestColumns()[0][0]);
        EXPECT_EQ(x.cardinality, 10);
        AXIOM_ASSERT_RANGE(x, 0, 9);
        EXPECT_EQ(x.nullFraction, std::nullopt);
      });

  // Unnesting two arrays pads the shorter one with NULL: x is NULL past the
  // end of a row's array.
  verifyEstimate(
      "SELECT x, y FROM w CROSS JOIN UNNEST(w.arr, ARRAY[1, 2, 3]) AS r(x, y)",
      1'000,
      [&](NodeCP root, const Estimate& estimate) {
        const Value& x = column(root, estimate, "x");
        EXPECT_EQ(x.cardinality, 10);
        AXIOM_ASSERT_RANGE(x, 0, 9);
        EXPECT_EQ(x.nullFraction, std::nullopt);
      });
}

TEST_F(EstimateProviderTest, unionAll) {
  // 10'000 + 1'000 = 11'000 rows. The NDV and bounds of 'c' are unknown, so the
  // union's are too; neither input has nulls.
  verifyEstimate(
      "SELECT a FROM t UNION ALL SELECT c FROM u",
      11'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(a);
        EXPECT_EQ(a.nullFraction, 0);
      });

  // An input with an unknown row count leaves the union's unknown. The value 5
  // falls in the range of 'a', so the union has 1'000 + 1 - 1 = 1'000 values
  // from 0 to 999.
  verifyEstimate(
      "SELECT a FROM t UNION ALL SELECT c FROM u WHERE c = 5",
      std::nullopt,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'000);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, std::nullopt);
      });

  // The values 1'000 and up fall outside the range of 'a': 1'000 + 1 values.
  verifyEstimate(
      "SELECT a FROM t UNION ALL SELECT c FROM u WHERE c = 1000",
      std::nullopt,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'001);
        AXIOM_ASSERT_RANGE(a, 0, 1'000);
        EXPECT_EQ(a.nullFraction, std::nullopt);
      });

  // The inputs' values do not overlap, so they add up in any order: 3 values.
  verifyEstimate(
      "SELECT c AS x FROM u WHERE c = 0 "
      "UNION ALL "
      "SELECT c FROM u WHERE c = 2 "
      "UNION ALL "
      "SELECT c FROM u WHERE c = 1",
      std::nullopt,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& x = column(root, estimate, "x");
        EXPECT_EQ(x.cardinality, 3);
        AXIOM_ASSERT_RANGE(x, 0, 2);
        EXPECT_EQ(x.nullFraction, std::nullopt);
      });

  // An all-null input adds no values: both other inputs hold only 5.
  verifyEstimate(
      "SELECT CAST(NULL AS BIGINT) AS x FROM u "
      "UNION ALL "
      "SELECT c FROM u WHERE c = 5 "
      "UNION ALL "
      "SELECT c FROM u WHERE c = 5",
      std::nullopt,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& x = column(root, estimate, "x");
        EXPECT_EQ(x.cardinality, 1);
        AXIOM_ASSERT_RANGE(x, 5, 5);
        EXPECT_EQ(x.nullFraction, std::nullopt);
      });
}

TEST_F(EstimateProviderTest, rowField) {
  // A field of a row column takes the statistics the connector reports for it.
  // A field without an NDV has at most the row's.
  verifyEstimate(
      "SELECT r.x AS x, r.y AS y FROM v",
      1'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& x = column(root, estimate, "x");
        EXPECT_EQ(x.cardinality, 100);
        AXIOM_ASSERT_RANGE(x, 0, 99);
        EXPECT_EQ(x.nullFraction, 0);

        const Value& y = column(root, estimate, "y");
        EXPECT_EQ(y.cardinality, 500);
        AXIOM_ASSERT_NORANGE(y);
      });

  // A field of a constructed row takes the estimate of the expression it was
  // built from: 'a' after the filter, 100 values from 0 to 99. 100 groups, one
  // per value of 'a', the largest NDV among the row's fields.
  verifyEstimate(
      "SELECT s[1] AS x "
      "FROM (SELECT ROW(a, b) AS s FROM t WHERE a < 100 GROUP BY 1)",
      100,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& x = column(root, estimate, "x");
        EXPECT_EQ(x.cardinality, 100);
        AXIOM_ASSERT_RANGE(x, 0, 99);
        EXPECT_EQ(x.nullFraction, 0);
      });
}

// A function or aggregate whose values are drawn from an argument carries
// that argument's NDV and range. The grouping key is a field of one of the
// aggregated rows, whose 'a' has 5 values from 0 to 4, so there are 5 groups.
TEST_F(EstimateProviderTest, drawnFrom) {
  verifyEstimate(
      "SELECT array_min_by(c.centers, x -> abs(x[2] - t.b))[1] AS cluster "
      "FROM t "
      "CROSS JOIN (SELECT array_agg(ROW(a, b)) AS centers FROM t WHERE a < 5) c "
      "GROUP BY 1",
      5,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& cluster = column(root, estimate, "cluster");
        EXPECT_EQ(cluster.cardinality, 5);
        AXIOM_ASSERT_RANGE(cluster, 0, 4);
      });

  // max(x, n) returns an array of x's values.
  verifyEstimate(
      "SELECT max(a, 3) AS m FROM t WHERE a < 10",
      1,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value* element = column(root, estimate, "m").child(0);
        ASSERT_NE(element, nullptr);
        EXPECT_EQ(element->cardinality, 10);
        AXIOM_ASSERT_RANGE(*element, 0, 9);
      });

  // An element of a literal array is one of its 3 values, from 1 to 3.
  verifyEstimate(
      "SELECT element_at("
      "  ARRAY[CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT)],"
      "  a % 3 + 1) AS x "
      "FROM t",
      10'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& x = column(root, estimate, "x");
        EXPECT_EQ(x.cardinality, 3);
        AXIOM_ASSERT_RANGE(x, 1, 3);
      });
}

TEST_F(EstimateProviderTest, values) {
  verifyEstimate(
      "SELECT x * 2 AS y FROM (VALUES 1, 2, 3) AS v(x)",
      3,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& y = column(root, estimate, "y");
        EXPECT_EQ(y.cardinality, 3);
        AXIOM_ASSERT_NORANGE(y);
        EXPECT_EQ(y.nullFraction, std::nullopt);
      });
}

TEST_F(EstimateProviderTest, fixedPoint) {
  // A loop that appends returns the rows of every iteration. The number of
  // iterations is unknown, so the row count is too.
  verifyEstimate(
      "WITH RECURSIVE r(n) AS ("
      "  SELECT 1 "
      "  UNION ALL "
      "  SELECT n + 1 FROM r WHERE n < 10"
      ") "
      "SELECT n FROM r",
      std::nullopt,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(1, root->outputColumns().size());

        const Value& n = column(root, estimate, "n");
        EXPECT_EQ(n.cardinality, std::nullopt);
        AXIOM_ASSERT_NORANGE(n);
        EXPECT_EQ(n.nullFraction, std::nullopt);
      });

  // A loop that replaces its state returns its state, which the estimate
  // describes for every iteration.
  auto context = makeContext();
  auto anchor = logical_plan::PlanBuilder(context).tableScan("t", {"a", "b"});
  // A loop that replaces its state, starting with the rows of 't', with what
  // 'step' computes from it.
  const auto loop =
      [&](const std::function<void(logical_plan::PlanBuilder&)>& step) {
        auto state =
            logical_plan::PlanBuilder(context).fixedPointState("s", anchor);
        step(state);
        return logical_plan::PlanBuilder(anchor)
            .iterate("s", state, /*numIterations=*/10, /*keyColumns=*/{})
            .build();
      };

  // The step keeps half of the rows the first time and all of them after
  // that, so the state never has more rows than the anchor: it keeps the
  // anchor's 10'000 rows. Each column keeps the widest bounds and the largest
  // NDV the state has in any iteration, the anchor's.
  verifyEstimate(
      *loop([](auto& state) { state.filter("a < 500"); }),
      10'000,
      [&](NodeCP root, const Estimate& estimate) {
        ASSERT_EQ(2, root->outputColumns().size());

        const Value& a = column(root, estimate, "a");
        EXPECT_EQ(a.cardinality, 1'000);
        AXIOM_ASSERT_RANGE(a, 0, 999);
        EXPECT_EQ(a.nullFraction, 0);

        const Value& b = column(root, estimate, "b");
        EXPECT_EQ(b.cardinality, 10);
        AXIOM_ASSERT_NORANGE(b);
        EXPECT_EQ(b.nullFraction, 0.5f);
      });

  // Each row of the state matches 10 rows of 't': the state grows, so its
  // size is unknown.
  verifyEstimate(
      *loop([&](auto& state) {
        state
            .join(
                logical_plan::PlanBuilder(context)
                    .tableScan("t", {"a"})
                    .project({"a AS k"}),
                "a = k",
                logical_plan::JoinType::kInner)
            .project({"a", "b"});
      }),
      std::nullopt,
      [&](NodeCP root, const Estimate& /*estimate*/) {
        ASSERT_EQ(2, root->outputColumns().size());
      });
}

#undef AXIOM_ASSERT_RANGE
#undef AXIOM_ASSERT_NORANGE

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
