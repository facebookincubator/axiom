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

#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "axiom/sql/presto/tests/LogicalPlanMatcher.h"

namespace axiom::sql::presto::test {

/// Shared test fixture for PrestoParser tests. Registers scalar and
/// aggregate functions once per test suite and creates a TestConnector
/// with TPC-H table schemas for each test.
class PrestoParserTestBase : public testing::Test {
 public:
  static constexpr const char* kConnectorId = "test";

  /// Registers Presto scalar and aggregate functions. Called once per suite.
  static void SetUpTestCase();

  /// Creates and registers a TestConnector with TPC-H table schemas.
  void SetUp() override;

  /// Unregisters the connector.
  void TearDown() override;

  /// Parses a SQL statement with expression optimization enabled.
  SqlStatementPtr parseSql(std::string_view sql);

  /// Parses an EXPLAIN statement and verifies the underlying plan matches.
  void testExplain(
      std::string_view sql,
      facebook::axiom::logical_plan::test::LogicalPlanMatcherBuilder& matcher);

  /// Verifies that EXPLAIN can parse DDL statements (CREATE TABLE, DROP TABLE).
  /// DDL statements do not have logical plans, so we only verify parsing.
  void testExplainDdl(std::string_view sql, SqlStatementKind expectedKind);

  /// Parses a SELECT statement and verifies the logical plan matches.
  /// Optionally checks that the expected set of views were accessed.
  void testSelect(
      std::string_view sql,
      facebook::axiom::logical_plan::test::LogicalPlanMatcherBuilder& matcher,
      const std::unordered_set<std::string>& views = {});

  /// Parses an INSERT statement and verifies the logical plan matches.
  void testInsert(
      std::string_view sql,
      facebook::axiom::logical_plan::test::LogicalPlanMatcherBuilder& matcher);

  /// Parses a CREATE TABLE AS SELECT statement and verifies the table name,
  /// schema, logical plan, and optional table properties.
  void testCtas(
      std::string_view sql,
      const std::string& tableName,
      const facebook::velox::RowTypePtr& tableSchema,
      facebook::axiom::logical_plan::test::LogicalPlanMatcherBuilder& matcher,
      const std::unordered_map<std::string, std::string>& properties = {});

  /// Parses a CREATE TABLE statement and verifies the table name, schema,
  /// optional properties, and optional constraints.
  void testCreateTable(
      std::string_view sql,
      const std::string& tableName,
      const facebook::velox::RowTypePtr& tableSchema,
      const std::unordered_map<std::string, std::string>& properties = {},
      const std::vector<CreateTableStatement::Constraint>& constraints = {});

 protected:
  /// Returns a matcher builder starting with a table scan node.
  static facebook::axiom::logical_plan::test::LogicalPlanMatcherBuilder
  matchScan() {
    return facebook::axiom::logical_plan::test::LogicalPlanMatcherBuilder()
        .tableScan();
  }

  /// Returns a matcher builder starting with a table scan node for the given
  /// table.
  static facebook::axiom::logical_plan::test::LogicalPlanMatcherBuilder
  matchScan(const std::string& tableName) {
    return facebook::axiom::logical_plan::test::LogicalPlanMatcherBuilder()
        .tableScan(tableName);
  }

  /// Returns a matcher builder starting with a values node.
  static facebook::axiom::logical_plan::test::LogicalPlanMatcherBuilder
  matchValues() {
    return facebook::axiom::logical_plan::test::LogicalPlanMatcherBuilder()
        .values();
  }

  /// Creates a PrestoParser configured with the test connector.
  PrestoParser makeParser() {
    return PrestoParser(kConnectorId, std::nullopt);
  }

  /// Test connector with TPC-H table schemas. Supports addTable, createView,
  /// and dropView for test-specific customization.
  std::shared_ptr<facebook::axiom::connector::TestConnector> connector_;
};

} // namespace axiom::sql::presto::test
