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

#include "axiom/connectors/MaterializedViewDefinition.h"
#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"

namespace facebook::axiom::connector {
namespace {

class MaterializedViewDefinitionTest : public testing::Test {};

TEST_F(MaterializedViewDefinitionTest, basic) {
  MaterializedViewDefinition mvDef(
      "SELECT * FROM base_table",
      "test_schema",
      "test_mv",
      {SchemaTableName{"test_schema", "base_table"}},
      "owner_user",
      ViewSecurity::kDefiner,
      {},
      {},
      std::nullopt);

  EXPECT_EQ(mvDef.originalSql(), "SELECT * FROM base_table");
  EXPECT_EQ(mvDef.schema(), "test_schema");
  EXPECT_EQ(mvDef.table(), "test_mv");
  EXPECT_EQ(mvDef.schemaTableName().schema, "test_schema");
  EXPECT_EQ(mvDef.schemaTableName().table, "test_mv");
  EXPECT_EQ(mvDef.baseTables().size(), 1);
  EXPECT_EQ(mvDef.baseTables()[0].schema, "test_schema");
  EXPECT_EQ(mvDef.baseTables()[0].table, "base_table");
  EXPECT_TRUE(mvDef.owner().has_value());
  EXPECT_EQ(mvDef.owner().value(), "owner_user");
  EXPECT_TRUE(mvDef.securityMode().has_value());
  EXPECT_EQ(mvDef.securityMode().value(), ViewSecurity::kDefiner);
  EXPECT_TRUE(mvDef.columnMappings().empty());
  EXPECT_TRUE(mvDef.baseTablesOnOuterJoinSide().empty());
  EXPECT_FALSE(mvDef.validRefreshColumns().has_value());
}

TEST_F(MaterializedViewDefinitionTest, withColumnMappings) {
  std::vector<ColumnMapping> columnMappings = {
      {TableColumn{SchemaTableName{"schema", "mv"}, "col1", std::nullopt},
       {TableColumn{SchemaTableName{"schema", "base"}, "base_col1", true}}},
      {TableColumn{SchemaTableName{"schema", "mv"}, "col2", std::nullopt},
       {TableColumn{SchemaTableName{"schema", "base"}, "base_col2", false}}}};

  MaterializedViewDefinition mvDef(
      "SELECT base_col1 as col1, base_col2 as col2 FROM base",
      "schema",
      "mv",
      {SchemaTableName{"schema", "base"}},
      std::nullopt,
      ViewSecurity::kInvoker,
      columnMappings,
      {},
      std::nullopt);

  EXPECT_EQ(mvDef.columnMappings().size(), 2);
  EXPECT_EQ(mvDef.columnMappings()[0].viewColumn.columnName, "col1");
  EXPECT_EQ(
      mvDef.columnMappings()[0].baseTableColumns[0].columnName, "base_col1");
  EXPECT_TRUE(
      mvDef.columnMappings()[0].baseTableColumns[0].isDirectMapped.value());

  EXPECT_EQ(mvDef.columnMappings()[1].viewColumn.columnName, "col2");
  EXPECT_FALSE(
      mvDef.columnMappings()[1].baseTableColumns[0].isDirectMapped.value());

  EXPECT_EQ(mvDef.securityMode().value(), ViewSecurity::kInvoker);
}

TEST_F(MaterializedViewDefinitionTest, withOuterJoinSideTables) {
  std::vector<SchemaTableName> outerJoinTables = {
      SchemaTableName{"schema", "left_table"},
      SchemaTableName{"schema", "right_table"}};

  MaterializedViewDefinition mvDef(
      "SELECT * FROM left_table LEFT JOIN right_table ON ...",
      "schema",
      "mv",
      {SchemaTableName{"schema", "left_table"},
       SchemaTableName{"schema", "right_table"}},
      std::nullopt,
      std::nullopt,
      {},
      outerJoinTables,
      std::nullopt);

  EXPECT_EQ(mvDef.baseTablesOnOuterJoinSide().size(), 2);
  EXPECT_EQ(mvDef.baseTablesOnOuterJoinSide()[0].table, "left_table");
  EXPECT_EQ(mvDef.baseTablesOnOuterJoinSide()[1].table, "right_table");
}

TEST_F(MaterializedViewDefinitionTest, withValidRefreshColumns) {
  std::vector<std::string> refreshCols = {"ds", "hr"};

  MaterializedViewDefinition mvDef(
      "SELECT * FROM base",
      "schema",
      "mv",
      {SchemaTableName{"schema", "base"}},
      std::nullopt,
      std::nullopt,
      {},
      {},
      refreshCols);

  EXPECT_TRUE(mvDef.validRefreshColumns().has_value());
  EXPECT_EQ(mvDef.validRefreshColumns().value().size(), 2);
  EXPECT_EQ(mvDef.validRefreshColumns().value()[0], "ds");
  EXPECT_EQ(mvDef.validRefreshColumns().value()[1], "hr");
}

TEST_F(MaterializedViewDefinitionTest, toString) {
  MaterializedViewDefinition mvDef(
      "SELECT * FROM base",
      "schema",
      "mv",
      {SchemaTableName{"schema", "base"}},
      "owner",
      ViewSecurity::kDefiner,
      {},
      {},
      std::vector<std::string>{"ds"});

  std::string str = mvDef.toString();

  EXPECT_TRUE(str.find("originalSql=SELECT * FROM base") != std::string::npos);
  EXPECT_TRUE(str.find("schema=schema") != std::string::npos);
  EXPECT_TRUE(str.find("table=mv") != std::string::npos);
  EXPECT_TRUE(
      str.find("baseTables=[\"schema\".\"base\"]") != std::string::npos);
  EXPECT_TRUE(str.find("owner=owner") != std::string::npos);
  EXPECT_TRUE(str.find("securityMode=DEFINER") != std::string::npos);
  EXPECT_TRUE(str.find("validRefreshColumns=[ds]") != std::string::npos);
}

TEST_F(MaterializedViewDefinitionTest, schemaTableNameEquality) {
  SchemaTableName stn1{"schema", "table"};
  SchemaTableName stn2{"schema", "table"};
  SchemaTableName stn3{"other_schema", "table"};
  SchemaTableName stn4{"schema", "other_table"};

  EXPECT_EQ(stn1, stn2);
  EXPECT_NE(stn1, stn3);
  EXPECT_NE(stn1, stn4);
  EXPECT_EQ(stn1.toString(), "\"schema\".\"table\"");
}

TEST_F(MaterializedViewDefinitionTest, tableColumnEquality) {
  TableColumn tc1{SchemaTableName{"s", "t"}, "col", true};
  TableColumn tc2{SchemaTableName{"s", "t"}, "col", true};
  TableColumn tc3{SchemaTableName{"s", "t"}, "col", false};
  TableColumn tc4{SchemaTableName{"s", "t"}, "other_col", true};

  EXPECT_EQ(tc1, tc2);
  EXPECT_NE(tc1, tc3);
  EXPECT_NE(tc1, tc4);
}

TEST_F(MaterializedViewDefinitionTest, columnMappingEquality) {
  TableColumn viewCol{SchemaTableName{"s", "mv"}, "col", std::nullopt};
  TableColumn baseCol{SchemaTableName{"s", "base"}, "base_col", true};

  ColumnMapping cm1{viewCol, {baseCol}};
  ColumnMapping cm2{viewCol, {baseCol}};
  ColumnMapping cm3{viewCol, {}};

  EXPECT_EQ(cm1, cm2);
  EXPECT_NE(cm1, cm3);
}

TEST_F(MaterializedViewDefinitionTest, tableIntegration) {
  velox::memory::MemoryManager::testingSetInstance(
      velox::memory::MemoryManager::Options{});
  auto connector = std::make_shared<TestConnector>("mv_test");
  auto table =
      connector->addTable("test_table", velox::ROW({"a"}, {velox::BIGINT()}));

  // A regular table is not a materialized view.
  EXPECT_FALSE(table->isMaterializedView());
  EXPECT_EQ(table->materializedViewDefinition(), nullptr);

  // Set a MaterializedViewDefinition on the table.
  auto mvDef = std::make_shared<MaterializedViewDefinition>(
      "SELECT * FROM base",
      "schema",
      "test_table",
      std::vector<SchemaTableName>{SchemaTableName{"schema", "base"}},
      std::nullopt,
      std::nullopt,
      std::vector<ColumnMapping>{},
      std::vector<SchemaTableName>{},
      std::nullopt);
  table->setMaterializedViewDefinition(mvDef);

  // Now the table should be recognized as a materialized view.
  EXPECT_TRUE(table->isMaterializedView());
  ASSERT_NE(table->materializedViewDefinition(), nullptr);
  EXPECT_EQ(
      table->materializedViewDefinition()->originalSql(), "SELECT * FROM base");
  EXPECT_EQ(table->materializedViewDefinition()->table(), "test_table");
  EXPECT_EQ(table->materializedViewDefinition()->baseTables().size(), 1);
  EXPECT_EQ(table->materializedViewDefinition()->baseTables()[0].table, "base");
}

} // namespace
} // namespace facebook::axiom::connector
