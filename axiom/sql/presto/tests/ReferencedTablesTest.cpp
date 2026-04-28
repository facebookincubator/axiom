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
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"

namespace axiom::sql::presto {
namespace {

using TableName = facebook::axiom::CatalogSchemaTableName;
using TableNameSet = std::unordered_set<TableName>;

// Tests that parse() populates referencedTables() on the returned SqlStatement.
class ReferencedTablesTest : public test::PrestoParserTestBase {
 protected:
  void assertReferencedTables(
      const SqlStatement& statement,
      const TableNameSet& expectedInputs,
      const std::optional<TableName>& expectedOutput) {
    const auto& tables = statement.referencedTables();
    ASSERT_EQ(tables.inputTables, expectedInputs);
    ASSERT_EQ(tables.outputTable, expectedOutput);
  }

  void testReferencedTables(
      std::string_view sql,
      const TableNameSet& expectedInputs,
      const std::optional<TableName>& expectedOutput) {
    SCOPED_TRACE(sql);
    const auto statement = parseSql(sql);
    assertReferencedTables(*statement, expectedInputs, expectedOutput);
  }
};

TEST_F(ReferencedTablesTest, select) {
  testReferencedTables(
      "SELECT * FROM orders", {{"test", {"default", "orders"}}}, std::nullopt);
}

TEST_F(ReferencedTablesTest, selectFullyQualified) {
  testReferencedTables(
      "SELECT * FROM test.default.orders",
      {{"test", {"default", "orders"}}},
      std::nullopt);
}

TEST_F(ReferencedTablesTest, showStats) {
  testReferencedTables(
      "SHOW STATS FOR orders", {{"test", {"default", "orders"}}}, std::nullopt);
}

TEST_F(ReferencedTablesTest, showStatsForQuery) {
  facebook::axiom::SchemaTableName viewName{
      std::string(kDefaultSchema), "orders_view"};
  connector_->createView(
      viewName,
      facebook::velox::ROW({"o_orderkey"}, {facebook::velox::BIGINT()}),
      "SELECT o_orderkey FROM orders");
  SCOPE_EXIT {
    connector_->dropView(viewName);
  };

  auto sql = "SHOW STATS FOR (SELECT * FROM orders_view)";
  TableNameSet expectedInputs = {{"test", {"default", "orders"}}};
  auto statement = parseSql(sql);
  ASSERT_TRUE(statement->isShowStatsForQuery());
  assertReferencedTables(*statement, expectedInputs, std::nullopt);
  ASSERT_EQ(statement->views().size(), 1);
  ASSERT_TRUE(statement->views().contains(
      TableName{"test", {"default", "orders_view"}}));

  const auto& inner = statement->as<ShowStatsForQueryStatement>()->statement();
  assertReferencedTables(*inner, expectedInputs, std::nullopt);
  ASSERT_EQ(inner->views().size(), 1);
  ASSERT_TRUE(
      inner->views().contains(TableName{"test", {"default", "orders_view"}}));
}

TEST_F(ReferencedTablesTest, explain) {
  facebook::axiom::SchemaTableName viewName{
      std::string(kDefaultSchema), "orders_view"};
  connector_->createView(
      viewName,
      facebook::velox::ROW({"o_orderkey"}, {facebook::velox::BIGINT()}),
      "SELECT o_orderkey FROM orders");
  SCOPE_EXIT {
    connector_->dropView(viewName);
  };

  auto sql = "EXPLAIN SELECT * FROM orders_view";
  TableNameSet expectedInputs = {{"test", {"default", "orders"}}};
  auto statement = parseSql(sql);
  ASSERT_TRUE(statement->isExplain());
  assertReferencedTables(*statement, expectedInputs, std::nullopt);
  ASSERT_EQ(statement->views().size(), 1);
  ASSERT_TRUE(statement->views().contains(
      TableName{"test", {"default", "orders_view"}}));

  const auto& inner = statement->as<ExplainStatement>()->statement();
  assertReferencedTables(*inner, expectedInputs, std::nullopt);
  ASSERT_EQ(inner->views().size(), 1);
  ASSERT_TRUE(
      inner->views().contains(TableName{"test", {"default", "orders_view"}}));
}

TEST_F(ReferencedTablesTest, viewResolvesToBaseTables) {
  facebook::axiom::SchemaTableName viewName{
      std::string(kDefaultSchema), "my_view"};
  connector_->createView(
      viewName,
      facebook::velox::ROW(
          {"n_nationkey", "n_name"},
          {facebook::velox::BIGINT(), facebook::velox::VARCHAR()}),
      "SELECT n_nationkey, n_name FROM nation");
  SCOPE_EXIT {
    connector_->dropView(viewName);
  };

  // parse() resolves views to their underlying base tables.
  // The view itself is tracked in views(), not in inputTables.
  auto statement = parseSql("SELECT * FROM my_view");
  const auto& tables = statement->referencedTables();
  ASSERT_THAT(
      tables.inputTables,
      testing::UnorderedElementsAre(TableName{"test", {"default", "nation"}}));
  ASSERT_EQ(tables.outputTable, std::nullopt);

  ASSERT_EQ(statement->views().size(), 1);
  ASSERT_TRUE(
      statement->views().contains(TableName{"test", {"default", "my_view"}}));
}

TEST_F(ReferencedTablesTest, join) {
  testReferencedTables(
      "SELECT * FROM customer JOIN nation ON customer.c_nationkey = nation.n_nationkey",
      {{"test", {"default", "customer"}}, {"test", {"default", "nation"}}},
      std::nullopt);
}

TEST_F(ReferencedTablesTest, multiJoin) {
  testReferencedTables(
      "SELECT * FROM customer c "
      "INNER JOIN nation n ON c.c_nationkey = n.n_nationkey "
      "LEFT JOIN region r ON n.n_regionkey = r.r_regionkey",
      {{"test", {"default", "customer"}},
       {"test", {"default", "nation"}},
       {"test", {"default", "region"}}},
      std::nullopt);
}

TEST_F(ReferencedTablesTest, subquery) {
  testReferencedTables(
      "SELECT * FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region)",
      {{"test", {"default", "nation"}}, {"test", {"default", "region"}}},
      std::nullopt);
}

TEST_F(ReferencedTablesTest, cte) {
  testReferencedTables(
      "WITH cte AS (SELECT * FROM orders) SELECT * FROM cte",
      {{"test", {"default", "orders"}}},
      std::nullopt);
}

TEST_F(ReferencedTablesTest, values) {
  testReferencedTables(
      "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)",
      {},
      std::nullopt);
}

TEST_F(ReferencedTablesTest, insert) {
  testReferencedTables(
      "INSERT INTO nation SELECT * FROM nation",
      {{"test", {"default", "nation"}}},
      TableName{"test", {"default", "nation"}});
}

TEST_F(ReferencedTablesTest, dropTable) {
  testReferencedTables(
      "DROP TABLE IF EXISTS orders",
      {},
      TableName{"test", {"default", "orders"}});
}

TEST_F(ReferencedTablesTest, createTable) {
  testReferencedTables(
      "CREATE TABLE new_table (col INTEGER)",
      {},
      TableName{"test", {"default", "new_table"}});
}

TEST_F(ReferencedTablesTest, createTableAsSelect) {
  testReferencedTables(
      "CREATE TABLE new_table AS SELECT * FROM orders",
      {{"test", {"default", "orders"}}},
      TableName{"test", {"default", "new_table"}});
}

TEST_F(ReferencedTablesTest, showColumns) {
  testReferencedTables(
      "SHOW COLUMNS FROM orders",
      {{"test", {"default", "orders"}}},
      std::nullopt);
}

TEST_F(ReferencedTablesTest, showCreateTable) {
  testReferencedTables(
      "SHOW CREATE TABLE orders",
      {{"test", {"default", "orders"}}},
      std::nullopt);
}

TEST_F(ReferencedTablesTest, setSession) {
  testReferencedTables("SET SESSION optimize = true", {}, std::nullopt);
}

TEST_F(ReferencedTablesTest, use) {
  testReferencedTables("USE test.default", {}, std::nullopt);
}

} // namespace
} // namespace axiom::sql::presto
