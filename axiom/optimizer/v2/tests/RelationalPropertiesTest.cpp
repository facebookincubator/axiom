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

#include <set>

#include <folly/String.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

// Relational properties with each unique key given as its columns' output
// names.
struct Properties {
  std::set<std::set<std::string>> unique;
  bool singleRow{false};

  bool operator==(const Properties&) const = default;
};

void PrintTo(const Properties& properties, std::ostream* out) {
  *out << "{unique: {";
  for (const auto& key : properties.unique) {
    *out << "{" << folly::join(", ", key) << "} ";
  }
  *out << "}, singleRow: " << properties.singleRow << "}";
}

struct TestCase {
  std::string_view sql;
  Properties expected;
};

class RelationalPropertiesTest : public optimizer::test::QueryTestBase {
 protected:
  void configureTestConnector() override {
    testConnector_->addTable("t", velox::ROW({"a", "b"}, velox::BIGINT()));
    testConnector_->addTable("u", velox::ROW({"c", "d"}, velox::BIGINT()));
  }

  // Returns the relational properties of the root of 'sql''s plan, just before
  // physical planning.
  Properties relationalProperties(std::string_view sql) {
    Properties result;
    verifyOptimization(
        *parseSelect(sql), Optimizer::Pass::kLowerRepeat, [&](NodeCP root) {
          const RelationalProperties& properties = root->relationalProperties();
          for (const PlanObjectSet& key : properties.unique) {
            std::set<std::string> names;
            for (ColumnCP column : key.toObjects<Column>()) {
              names.emplace(column->outputName());
            }
            result.unique.insert(std::move(names));
          }
          result.singleRow = properties.singleRow;
        });
    return result;
  }

  void verify(const std::vector<TestCase>& testCases) {
    for (const auto& testCase : testCases) {
      SCOPED_TRACE(testCase.sql);
      EXPECT_EQ(relationalProperties(testCase.sql), testCase.expected);
    }
  }
};

TEST_F(RelationalPropertiesTest, aggregate) {
  verify({
      {"SELECT a, b, count(*) AS n FROM t GROUP BY a, b", {{{"a", "b"}}}},
      // A computed grouping key is unique like a column.
      {"SELECT a % 10 AS m, count(*) AS n FROM t GROUP BY 1", {{{"m"}}}},
      // Without grouping keys: exactly one row, unique on no columns.
      {"SELECT count(*) AS n FROM t", {{{}}, true}},
  });
}

TEST_F(RelationalPropertiesTest, project) {
  verify({
      // A key passed through survives; a key that is not projected does not.
      {"SELECT a, n * 2 AS d "
       "FROM (SELECT a, count(*) AS n FROM t GROUP BY a)",
       {{{"a"}}}},
      {"SELECT n * 2 AS d "
       "FROM (SELECT a, count(*) AS n FROM t GROUP BY a)",
       {}},
      // A single row stays a single row.
      {"SELECT n * 2 AS d FROM (SELECT count(*) AS n FROM t)", {{{}}, true}},
  });
}

TEST_F(RelationalPropertiesTest, passThrough) {
  verify({
      // A filter keeps the keys and may drop the single row.
      {"SELECT a, n "
       "FROM (SELECT a, count(*) AS n FROM t GROUP BY a) "
       "WHERE n > 1",
       {{{"a"}}}},
      {"SELECT n FROM (SELECT count(*) AS n FROM t) WHERE n > 1", {{{}}}},
      // Sorting and window functions keep every row and its keys.
      {"SELECT a, n "
       "FROM (SELECT a, count(*) AS n FROM t GROUP BY a) "
       "ORDER BY n",
       {{{"a"}}}},
      {"SELECT a, n, sum(n) OVER () AS s "
       "FROM (SELECT a, count(*) AS n FROM t GROUP BY a)",
       {{{"a"}}}},
      {"SELECT a, n, row_number() OVER (ORDER BY n) AS r "
       "FROM (SELECT a, count(*) AS n FROM t GROUP BY a)",
       {{{"a"}}}},
  });
}

TEST_F(RelationalPropertiesTest, join) {
  verify({
      // An inner join keeps the keys of a side whose rows match at most one
      // row of the other side, in either order. The join keys differ, so 'c'
      // stays distinct from 'a'.
      {"SELECT u.c, u.d, u.n "
       "FROM (SELECT DISTINCT a FROM t) t "
       "JOIN (SELECT c, d, count(*) AS n FROM u GROUP BY c, d) u "
       "ON t.a = u.c + 1",
       {{{"c", "d"}}}},
      {"SELECT u.c, u.d, u.n "
       "FROM (SELECT c, d, count(*) AS n FROM u GROUP BY c, d) u "
       "JOIN (SELECT DISTINCT a FROM t) t "
       "ON t.a = u.c + 1",
       {{{"c", "d"}}}},
      // An outer join keeps the keys of its kept side when the other side has
      // one row per join key.
      {"SELECT t.a, u.n "
       "FROM (SELECT DISTINCT a FROM t) t "
       "LEFT JOIN (SELECT c, count(*) AS n FROM u GROUP BY c) u "
       "ON t.a = u.c",
       {{{"a"}}}},
      {"SELECT u.c, u.d "
       "FROM (SELECT DISTINCT a FROM t) t "
       "RIGHT JOIN (SELECT c, d, count(*) AS n FROM u GROUP BY c, d) u "
       "ON t.a = u.c + 1",
       {{{"c", "d"}}}},
      // The other side may repeat a join key.
      {"SELECT t.a, u.d "
       "FROM (SELECT DISTINCT a FROM t) t "
       "LEFT JOIN u ON t.a = u.c",
       {}},
      // A side of exactly one row, including a scalar subquery.
      {"SELECT t.a, u.n "
       "FROM (SELECT DISTINCT a FROM t) t "
       "CROSS JOIN (SELECT count(*) AS n FROM u) u",
       {{{"a"}}}},
      {"SELECT a, (SELECT c FROM u LIMIT 1) AS x "
       "FROM (SELECT DISTINCT a FROM t)",
       {{{"a"}}}},
      // Semi and anti joins emit rows of one side only.
      {"SELECT a, n "
       "FROM (SELECT a, count(*) AS n FROM t GROUP BY a) "
       "WHERE a IN (SELECT c FROM u)",
       {{{"a"}}}},
      {"SELECT a, n "
       "FROM (SELECT a, count(*) AS n FROM t GROUP BY a) "
       "WHERE a NOT IN (SELECT c FROM u)",
       {{{"a"}}}},
  });
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
