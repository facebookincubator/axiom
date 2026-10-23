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

#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "axiom/sql/presto/ParserOptions.h"
#include "axiom/sql/presto/PrestoSqlError.h"
#include "axiom/sql/presto/grammar/SyntaxParser.h"

namespace axiom::sql::presto {
namespace {

void parseQuery(std::string_view sql, bool isValid = true) {
  SyntaxParser parser(sql, ParserOptions::kMaxExpressionDepthDefault);
  if (isValid) {
    EXPECT_NO_THROW(parser.parse()) << "Failed to parse: " << sql;
  } else {
    EXPECT_THROW(parser.parse(), PrestoSqlError)
        << "Should have failed to parse SQL: " << sql;
  }
}

TEST(GrammarTest, selectQueries) {
  static const std::vector<std::string> validQueries = {
      // Simple examples
      "SELECT a FROM b",
      "SELECT a FROM b WHERE c=9",
      "SELECT a FROM b WHERE c = 9 AND d = 10",
      "SELECT a FROM b WHERE c = 9 AND (d = 10 OR e = 11)",
      "SELECT a, b FROM c",
      "SELECT a.col FROM b",
      "SELECT * FROM b",
      // Non-strictly-relational use cases:
      "SELECT a.* FROM b",
      "SELECT b.a.col FROM b",
      "SELECT a[b] FROM c",
      "SELECT a",
      "SELECT a FROM b , c",
      "SELECT a FROM b JOIN c ON b.id=c.id",
      "SELECT a, COUNT(*) FROM b GROUP BY a",
      // Intersection
      "SELECT 123 INTERSECT DISTINCT SELECT 345 INTERSECT ALL SELECT 456",
      // Union
      "SELECT a FROM t1 UNION ALL SELECT a FROM t2",
      "SELECT 123 UNION DISTINCT SELECT 234 UNION ALL SELECT 456",
      // Limit
      "SELECT * FROM (VALUES (1, '1'), (2, '2')) LIMIT 3",
      // Substring
      "SELECT SUBSTRING('ABCDEF' FROM 2)",
      "SELECT SUBSTRING('ABCDEF' FROM 2 FOR 3)",
      // Cast
      "SELECT CAST(ROW(11, 12) AS ROW(COL0 INTEGER, COL1 INTEGER)).col0",
      // Order by
      "SELECT * FROM table1 ORDER BY a",
      // Group by
      "SELECT * FROM table1 GROUP BY a",
      "SELECT * FROM table1 GROUP BY a, b",
      // WINDOW remains usable as an identifier.
      "SELECT window FROM table1",
      "SELECT window.a FROM table1 AS window",
      "CREATE FUNCTION f(window BIGINT) RETURNS BIGINT RETURN window",
      // Implicit Join
      "SELECT * FROM a, b",
      // Join
      "SELECT * FROM a CROSS JOIN b LEFT JOIN c ON TRUE",
      "SELECT * FROM a CROSS JOIN b NATURAL "
      "JOIN c CROSS JOIN d NATURAL JOIN e",
      // Unnest
      "SELECT * FROM t CROSS JOIN UNNEST(a)",
      // Lateral
      "SELECT * FROM t CROSS JOIN LATERAL (VALUES 1)",
      // Exists
      "SELECT EXISTS(SELECT 1)",
      "SELECT EXISTS(SELECT 1) = EXISTS(SELECT 2)",
      // AggregationFilter
      "SELECT SUM(x) FILTER (WHERE x > 4)",
  };

  for (auto& q : validQueries) {
    parseQuery(q);
  }
}

TEST(GrammarTest, invalidQuery) {
  parseQuery("SELECT a INVALID b", /* isValid= */ false);
}

TEST(GrammarTest, graphQueries) {
  static const std::vector<std::string> validQueries = {
      "SELECT * FROM GRAPH_TABLE (g MATCH (a) COLUMNS (a.x))",
      "SELECT * FROM GRAPH_TABLE (s.g MATCH (a)-(b) COLUMNS (a.x AS c)) t",
      "SELECT * FROM GRAPH_TABLE (g "
      "MATCH (a IS A WHERE a.x = 1) -[e IS B WHERE e.y > 0]-> (b), (b) <- (c) "
      "WHERE a.x < c.x "
      "COLUMNS (a.x, sum(e.y) AS s, path_length(p)))",
      // Edge forms.
      "SELECT * FROM GRAPH_TABLE (g MATCH (a) <-[e]- (b) -[f]- (c) COLUMNS (1))",
      "SELECT * FROM GRAPH_TABLE (g MATCH ()->()<-()-() COLUMNS (1))",
      // Label expressions.
      "SELECT * FROM GRAPH_TABLE (g MATCH (a IS !(A | B) & %) COLUMNS (1))",
      // Quantifiers, optional subpaths and subpath variables.
      "SELECT * FROM GRAPH_TABLE (g "
      "MATCH (a) -[e]->{1,3} (b) -[f]->{2} (c) -[h]->* (d) -[i]->+ (j) "
      "-[k]->{,4} (l) -[m]->{5,} (n) COLUMNS (1))",
      "SELECT * FROM GRAPH_TABLE (g "
      "MATCH (a) (q = TRAIL (x) -> (y) WHERE x.w > 0){1,2} ((y) -> (z))? (b) "
      "COLUMNS (1))",
      // Selectors and path modes.
      "SELECT * FROM GRAPH_TABLE (g "
      "MATCH p = ANY SHORTEST TRAIL PATH (a)->+(b) COLUMNS (1))",
      "SELECT * FROM GRAPH_TABLE (g MATCH ALL SHORTEST (a)->(b) COLUMNS (1))",
      "SELECT * FROM GRAPH_TABLE (g MATCH ANY 3 ACYCLIC (a)->(b) COLUMNS (1))",
      "SELECT * FROM GRAPH_TABLE (g MATCH SHORTEST 2 (a)->(b) COLUMNS (1))",
      "SELECT * FROM GRAPH_TABLE (g MATCH SHORTEST GROUPS (a)->(b) COLUMNS (1))",
      "SELECT * FROM GRAPH_TABLE (g MATCH ALL SIMPLE PATHS (a)->(b) COLUMNS (1))",
      "SELECT * FROM GRAPH_TABLE (g MATCH WALK (a)->(b) COLUMNS (1))",
      // Graph definitions.
      "WITH GRAPH g AS (VERTEX TABLES (t KEY (a))) SELECT 1",
      "WITH t AS (SELECT 1), GRAPH g AS ("
      "VERTEX TABLES (t KEY (a) LABEL A PROPERTIES (a, b + 1 AS c), "
      "s.u AS v KEY (a) PROPERTIES (b)) "
      "EDGE TABLES (w KEY (a, b) "
      "SOURCE KEY (a) REFERENCES t (a) DESTINATION KEY (b) REFERENCES v (a) "
      "LABEL B LABEL C PROPERTIES (c))) "
      "SELECT * FROM t",
      // Graph keywords are non-reserved and usable as names.
      "SELECT path, paths, label, source, destination, edge, vertex, match, "
      "references, shortest, walk, trail, acyclic, simple FROM t",
      "SELECT * FROM graph_table",
      "SELECT graph_table.a FROM t AS graph_table",
      "WITH graph AS (SELECT 1) SELECT * FROM graph",
      "WITH graph (a) AS (SELECT 1) SELECT * FROM graph",
      // '|', '!', '%' and '->' keep their expression meaning.
      "SELECT a || b, a != b, a % b FROM t",
      "SELECT x -> x + 1 FROM t",
      "SELECT a<-1 FROM t",
  };

  for (const auto& query : validQueries) {
    SCOPED_TRACE(query);
    parseQuery(query);
  }

  static const std::vector<std::string> invalidQueries = {
      // Quantifier on a node pattern.
      "SELECT * FROM GRAPH_TABLE (g MATCH (a)* COLUMNS (1))",
      // Optional edge pattern.
      "SELECT * FROM GRAPH_TABLE (g MATCH (a) -[e]->? (b) COLUMNS (1))",
      // Undirected edges.
      "SELECT * FROM GRAPH_TABLE (g MATCH (a) ~[e]~ (b) COLUMNS (1))",
      "SELECT * FROM GRAPH_TABLE (g MATCH (a) <-[e]-> (b) COLUMNS (1))",
      // Double negation without parentheses.
      "SELECT * FROM GRAPH_TABLE (g MATCH (a IS !!A) COLUMNS (1))",
      // Quantifier and '?' together.
      "SELECT * FROM GRAPH_TABLE (g MATCH ((a)->(b))*? COLUMNS (1))",
      // Missing COLUMNS.
      "SELECT * FROM GRAPH_TABLE (g MATCH (a))",
      // Graph definition without a key.
      "WITH GRAPH g AS (VERTEX TABLES (t)) SELECT 1",
      // Edge table without a destination.
      "WITH GRAPH g AS (VERTEX TABLES (t KEY (a)) "
      "EDGE TABLES (u KEY (a) SOURCE KEY (a) REFERENCES t (a))) SELECT 1",
  };

  for (const auto& query : invalidQueries) {
    SCOPED_TRACE(query);
    parseQuery(query, /*isValid=*/false);
  }
}

} // namespace
} // namespace axiom::sql::presto
