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
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/sql/presto/tests/ExpectPrestoSqlError.h"
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"

namespace axiom::sql::presto {
namespace {

using TableName = facebook::axiom::CatalogSchemaTableName;
using TableNameSet = std::unordered_set<TableName>;

// Tests that extractSyntacticTableNames() names tables from SQL text
// without resolving them against connector metadata.
class SyntacticTableNamesTest : public test::PrestoParserTestBase {
 protected:
  void assertSyntacticTableNames(
      std::string_view sql,
      const TableNameSet& expectedInputs,
      const std::optional<TableName>& expectedOutput,
      std::string_view expectedKindName) {
    SCOPED_TRACE(sql);
    const auto tables = makeParser().extractSyntacticTableNames(sql);
    ASSERT_THAT(
        tables.inputTables, testing::UnorderedElementsAreArray(expectedInputs));
    ASSERT_EQ(tables.outputTable, expectedOutput);
    ASSERT_EQ(tables.kindName, expectedKindName);
  }
};

TEST_F(SyntacticTableNamesTest, select) {
  // Names use the parser defaults, as written. No table named here exists
  // in the test connector.
  assertSyntacticTableNames(
      "SELECT * FROM t", {{"test", {"default", "t"}}}, std::nullopt, "SELECT");
  assertSyntacticTableNames(
      "SELECT * FROM test.default.t",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "SELECT");
  assertSyntacticTableNames(
      "SELECT * FROM u.t", {{"test", {"u", "t"}}}, std::nullopt, "SELECT");
  assertSyntacticTableNames(
      "SELECT * FROM t JOIN u ON t.a = u.a",
      {{"test", {"default", "t"}}, {"test", {"default", "u"}}},
      std::nullopt,
      "SELECT");
  assertSyntacticTableNames(
      "SELECT * FROM t WHERE a IN (SELECT a FROM u)",
      {{"test", {"default", "t"}}, {"test", {"default", "u"}}},
      std::nullopt,
      "SELECT");
  assertSyntacticTableNames(
      "WITH v AS (SELECT * FROM t) SELECT * FROM v",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "SELECT");
}

TEST_F(SyntacticTableNamesTest, unreferencedCte) {
  // A CTE body names its tables even when the CTE is never read.
  assertSyntacticTableNames(
      "WITH v AS (SELECT * FROM t) SELECT 1",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "SELECT");
}

TEST_F(SyntacticTableNamesTest, cteScope) {
  // A CTE alias does not leak past its WITH clause. The second branch
  // names the real table t.
  assertSyntacticTableNames(
      "SELECT * FROM (WITH t AS (SELECT 1) SELECT * FROM t) "
      "UNION ALL SELECT * FROM t",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "SELECT");
}

TEST_F(SyntacticTableNamesTest, recursiveCte) {
  // A recursive CTE names only the tables its body reads outside
  // the recursion. The self reference names the CTE, not a table.
  assertSyntacticTableNames(
      "WITH RECURSIVE v AS (SELECT * FROM v UNION ALL SELECT * FROM t) "
      "SELECT * FROM v",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "SELECT");
}

TEST_F(SyntacticTableNamesTest, recursiveShadowing) {
  // A nested same-name WITH shadows the outer CTE, so the outer
  // CTE does not reference itself and the physical table is named.
  // The nonrecursive control names the same table.
  assertSyntacticTableNames(
      "WITH RECURSIVE t AS ("
      "  WITH t AS (SELECT * FROM t) SELECT * FROM t) "
      "SELECT * FROM t",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "SELECT");
  assertSyntacticTableNames(
      "WITH t AS (WITH t AS (SELECT * FROM t) SELECT * FROM t) "
      "SELECT * FROM t",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "SELECT");
}

TEST_F(SyntacticTableNamesTest, viewIsNotExpanded) {
  // A real view still reports only its own name. The same query
  // through parse() reports the underlying table instead.
  facebook::axiom::SchemaTableName viewName{std::string(kDefaultSchema), "v"};
  connector_->createView(
      viewName,
      facebook::velox::ROW({"o_orderkey"}, {facebook::velox::BIGINT()}),
      "SELECT o_orderkey FROM orders");
  SCOPE_EXIT {
    connector_->dropView(viewName);
  };

  assertSyntacticTableNames(
      "SELECT * FROM v", {{"test", {"default", "v"}}}, std::nullopt, "SELECT");
  const auto statement = parseSql("SELECT * FROM v");
  const TableNameSet expectedOrders{{"test", {"default", "orders"}}};
  ASSERT_EQ(statement->referencedTables().inputTables, expectedOrders);
}

TEST_F(SyntacticTableNamesTest, missingTable) {
  // A table the connector does not know is still named, and the same
  // SQL fails parse(), which resolves every table.
  assertSyntacticTableNames(
      "SELECT * FROM t", {{"test", {"default", "t"}}}, std::nullopt, "SELECT");
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("SELECT * FROM t"), "Table not found");
}

TEST_F(SyntacticTableNamesTest, cteCase) {
  // CTE matching ignores letter case, quoted or not. Neither query
  // reads a physical table.
  assertSyntacticTableNames(
      "WITH v AS (SELECT 1) SELECT * FROM V", {}, std::nullopt, "SELECT");
  assertSyntacticTableNames(
      "WITH \"V\" AS (SELECT 1) SELECT * FROM v", {}, std::nullopt, "SELECT");
  assertSyntacticTableNames(
      "WITH v AS (SELECT * FROM t) SELECT * FROM V",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "SELECT");
}

TEST_F(SyntacticTableNamesTest, caseCanonicalized) {
  // Reported names use the connector canonicalization, as parse()
  // does, quoted or not.
  assertSyntacticTableNames(
      "SELECT * FROM Orders",
      {{"test", {"default", "orders"}}},
      std::nullopt,
      "SELECT");
  assertSyntacticTableNames(
      "SELECT * FROM \"Orders\"",
      {{"test", {"default", "orders"}}},
      std::nullopt,
      "SELECT");
}

TEST_F(SyntacticTableNamesTest, graph) {
  parserOptions_.graphQueries = true;
  assertSyntacticTableNames(
      "WITH GRAPH g AS ("
      "  VERTEX TABLES ("
      "    t KEY (a) LABEL N,"
      "    u KEY (a) LABEL R)"
      "  EDGE TABLES ("
      "    t AS in_region KEY (a)"
      "      SOURCE KEY (a) REFERENCES t (a)"
      "      DESTINATION KEY (a) REFERENCES u (a)"
      "      LABEL InRegion)) "
      "SELECT * FROM GRAPH_TABLE (g "
      "MATCH (a IS N) -[e IS InRegion]-> (b IS R) "
      "COLUMNS (a.a, b.a))",
      {{"test", {"default", "t"}}, {"test", {"default", "u"}}},
      std::nullopt,
      "SELECT");
}

TEST_F(SyntacticTableNamesTest, like) {
  // The LIKE source supplies the new table schema, so it is named
  // as an input even though no row is read from it.
  assertSyntacticTableNames(
      "CREATE TABLE dst (LIKE t INCLUDING PROPERTIES)",
      {{"test", {"default", "t"}}},
      TableName{"test", {"default", "dst"}},
      "CREATE TABLE");
}

TEST_F(SyntacticTableNamesTest, writes) {
  assertSyntacticTableNames(
      "INSERT INTO t SELECT * FROM u",
      {{"test", {"default", "u"}}},
      TableName{"test", {"default", "t"}},
      "INSERT");
  assertSyntacticTableNames(
      "CREATE TABLE t AS SELECT * FROM u",
      {{"test", {"default", "u"}}},
      TableName{"test", {"default", "t"}},
      "CREATE TABLE AS SELECT");
  assertSyntacticTableNames(
      "CREATE TABLE t (a INTEGER)",
      {},
      TableName{"test", {"default", "t"}},
      "CREATE TABLE");
  assertSyntacticTableNames(
      "DROP TABLE t", {}, TableName{"test", {"default", "t"}}, "DROP TABLE");
  assertSyntacticTableNames(
      "DELETE FROM t WHERE a = 1",
      {{"test", {"default", "t"}}},
      TableName{"test", {"default", "t"}},
      "DELETE");
  assertSyntacticTableNames(
      "REFRESH VECTOR INDEX i ON t WHERE ds = '2026-01-01'",
      {{"test", {"default", "t"}}},
      TableName{"test", {"default", "t"}},
      "REFRESH VECTOR INDEX");
}

TEST_F(SyntacticTableNamesTest, show) {
  assertSyntacticTableNames(
      "SHOW COLUMNS FROM t",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "SELECT");
  assertSyntacticTableNames(
      "SHOW CREATE TABLE t",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "SELECT");
  assertSyntacticTableNames(
      "SHOW CREATE VIEW v",
      {{"test", {"default", "v"}}},
      std::nullopt,
      "SELECT");
  assertSyntacticTableNames(
      "SHOW STATS FOR t", {{"test", {"default", "t"}}}, std::nullopt, "SELECT");
  assertSyntacticTableNames(
      "SHOW STATS FOR (SELECT * FROM t)",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "SHOW STATS FOR");
}

TEST_F(SyntacticTableNamesTest, kind) {
  assertSyntacticTableNames(
      "EXPLAIN SELECT * FROM t",
      {{"test", {"default", "t"}}},
      std::nullopt,
      "EXPLAIN");
  assertSyntacticTableNames(
      "SET SESSION optimize = true", {}, std::nullopt, "SET SESSION");
  assertSyntacticTableNames("USE test.default", {}, std::nullopt, "USE");
  assertSyntacticTableNames(
      "SELECT * FROM (VALUES (1, 'a')) AS t(a)", {}, std::nullopt, "SELECT");
}

TEST_F(SyntacticTableNamesTest, parityWithParse) {
  // Queries with no view or missing table agree between the two
  // APIs. Queries that differ by design, such as view expansion,
  // are covered by their own tests.
  const std::vector<std::string_view> queries = {
      "SELECT * FROM orders",
      "SELECT * FROM test.default.orders",
      "SELECT * FROM customer JOIN nation "
      "ON customer.c_nationkey = nation.n_nationkey",
      "SELECT * FROM customer c "
      "INNER JOIN nation n ON c.c_nationkey = n.n_nationkey "
      "LEFT JOIN region r ON n.n_regionkey = r.r_regionkey",
      "SELECT * FROM nation WHERE n_regionkey IN "
      "(SELECT r_regionkey FROM region)",
      "WITH cte AS (SELECT * FROM orders) SELECT * FROM cte",
      "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)",
      "INSERT INTO nation SELECT * FROM nation",
      "DROP TABLE IF EXISTS orders",
      "CREATE TABLE new_table (col INTEGER)",
      "CREATE TABLE new_table AS SELECT * FROM orders",
      "SHOW COLUMNS FROM orders",
      "SHOW CREATE TABLE orders",
      "SHOW STATS FOR orders",
      "SET SESSION optimize = true",
      "USE test.default",
  };
  for (const auto& sql : queries) {
    SCOPED_TRACE(sql);
    const auto statement = parseSql(sql);
    const auto syntactic = makeParser().extractSyntacticTableNames(sql);
    ASSERT_EQ(syntactic.inputTables, statement->referencedTables().inputTables);
    ASSERT_EQ(syntactic.outputTable, statement->referencedTables().outputTable);
    ASSERT_EQ(syntactic.kindName, statement->kindName());
  }
}

TEST_F(SyntacticTableNamesTest, invalidSql) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      makeParser().extractSyntacticTableNames("THIS IS NOT SQL"),
      "mismatched input");
}

TEST_F(SyntacticTableNamesTest, unsupportedForms) {
  // Statement forms the AST builder does not support fail with an
  // internal error, not a syntax error.
  EXPECT_THROW(
      makeParser().extractSyntacticTableNames("UPDATE t SET a = 1"),
      facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      makeParser().extractSyntacticTableNames(
          "CREATE VIEW v AS SELECT * FROM t"),
      facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      makeParser().extractSyntacticTableNames("ANALYZE t"),
      facebook::velox::VeloxRuntimeError);
}

TEST_F(SyntacticTableNamesTest, invalidQualification) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      makeParser().extractSyntacticTableNames("SELECT * FROM a.b.c.d"),
      "Table name must have 1-3 components");
}

} // namespace
} // namespace axiom::sql::presto
