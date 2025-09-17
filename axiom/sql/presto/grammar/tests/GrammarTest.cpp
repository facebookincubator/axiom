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

#include <antlr4-runtime.h>
#include <gtest/gtest.h>

#include "axiom/sql/presto/ast/UpperCaseInputStream.h"
#include "axiom/sql/presto/grammar/PrestoSqlLexer.h"
#include "axiom/sql/presto/grammar/PrestoSqlParser.h"

namespace axiom::sql::presto {
namespace {

void parseQuery(std::string_view sql) {
  UpperCaseInputStream input(sql);
  PrestoSqlLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  PrestoSqlParser parser(&tokens);
  parser.query();

  EXPECT_EQ(0, parser.getNumberOfSyntaxErrors()) << "Failed to parse:" << sql;
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
} // namespace
} // namespace axiom::sql::presto
