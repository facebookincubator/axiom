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

#include "axiom/optimizer/tests/SqlFile.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer::test {
namespace {

// Convenience: parse just the queries from 'content' assuming no setup
// directives. baseDir is empty since setup_file isn't used.
std::vector<QueryEntry> parseQueries(const std::string& content) {
  return SqlFile::parse(content, /*baseDir=*/"").entries;
}

class SqlFileTest : public ::testing::Test {};

TEST_F(SqlFileTest, empty) {
  EXPECT_THAT(parseQueries(""), testing::IsEmpty());
}

TEST_F(SqlFileTest, singleQuery) {
  auto entries = parseQueries("SELECT 1");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT 1");
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kResults);
  EXPECT_EQ(entries[0].lineNumber, 1);
}

TEST_F(SqlFileTest, multipleQueries) {
  auto entries = parseQueries(
      "SELECT 1\n"
      "----\n"
      "SELECT 2\n"
      "----\n"
      "SELECT 3");
  ASSERT_THAT(entries, testing::SizeIs(3));
  EXPECT_EQ(entries[0].sql, "SELECT 1");
  EXPECT_EQ(entries[1].sql, "SELECT 2");
  EXPECT_EQ(entries[2].sql, "SELECT 3");
}

TEST_F(SqlFileTest, multiLineQuery) {
  auto entries = parseQueries(
      "SELECT a, b\n"
      "FROM t\n"
      "WHERE a > 1");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT a, b\nFROM t\nWHERE a > 1");
  EXPECT_EQ(entries[0].lineNumber, 1);
}

TEST_F(SqlFileTest, ordered) {
  auto entries = parseQueries(
      "-- ordered\n"
      "SELECT a FROM t ORDER BY a");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kOrdered);
  EXPECT_EQ(entries[0].sql, "SELECT a FROM t ORDER BY a");
  EXPECT_EQ(entries[0].lineNumber, 2);
}

TEST_F(SqlFileTest, count) {
  auto entries = parseQueries(
      "-- count 42\n"
      "SELECT * FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kCount);
  EXPECT_EQ(entries[0].expectedCount, 42);
}

TEST_F(SqlFileTest, error) {
  auto entries = parseQueries(
      "-- error: Column not found\n"
      "SELECT missing FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].expectedError, "Column not found");
  // `-- error:` sets both per-optimizer fields.
  EXPECT_EQ(entries[0].expectedErrorV1, "Column not found");
  EXPECT_EQ(entries[0].expectedErrorV2, "Column not found");
}

TEST_F(SqlFileTest, errorPerSuite) {
  auto entries = parseQueries(
      "-- error_v1: v1 message\n"
      "-- error_v2: v2 message\n"
      "SELECT bad FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kResults);
  EXPECT_EQ(entries[0].expectedError, "");
  EXPECT_EQ(entries[0].expectedErrorV1, "v1 message");
  EXPECT_EQ(entries[0].expectedErrorV2, "v2 message");
}

TEST_F(SqlFileTest, errorV1Only) {
  auto entries = parseQueries(
      "-- error_v1: v1 message\n"
      "SELECT bad FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].expectedErrorV1, "v1 message");
  EXPECT_EQ(entries[0].expectedErrorV2, "");
}

TEST_F(SqlFileTest, errorAndPerSuiteAreExclusive) {
  VELOX_ASSERT_THROW(
      parseQueries(
          "-- error: both\n"
          "-- error_v1: v1 message\n"
          "SELECT bad FROM t"),
      "cannot be combined");
  VELOX_ASSERT_THROW(
      parseQueries(
          "-- error_v2: v2 message\n"
          "-- error: both\n"
          "SELECT bad FROM t"),
      "cannot be combined");
}

TEST_F(SqlFileTest, duckdb) {
  auto entries = parseQueries(
      "-- duckdb: SELECT 1 AS x\n"
      "SELECT 1 x");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kResults);
  EXPECT_EQ(entries[0].duckDbSql, std::optional<std::string>("SELECT 1 AS x"));
  EXPECT_EQ(entries[0].sql, "SELECT 1 x");
}

TEST_F(SqlFileTest, disabled) {
  auto entries = parseQueries(
      "-- disabled\n"
      "SELECT broken_query\n"
      "----\n"
      "SELECT 1");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT 1");
}

TEST_F(SqlFileTest, trailingWhitespace) {
  auto entries = parseQueries("SELECT 1   \n\n");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT 1");
}

TEST_F(SqlFileTest, lineNumbers) {
  auto entries = parseQueries(
      "-- ordered\n"
      "SELECT 1\n"
      "----\n"
      "-- count 5\n"
      "SELECT *\n"
      "FROM t");
  ASSERT_THAT(entries, testing::SizeIs(2));
  EXPECT_EQ(entries[0].lineNumber, 2);
  EXPECT_EQ(entries[1].lineNumber, 5);
}

TEST_F(SqlFileTest, sqlCommentInBody) {
  auto entries = parseQueries(
      "SELECT 1\n"
      "-- this is a SQL comment\n"
      "FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT 1\n-- this is a SQL comment\nFROM t");
}

TEST_F(SqlFileTest, commentBeforeSql) {
  auto entries = parseQueries(
      "-- This query tests basic arithmetic.\n"
      "-- It divides column a by 2.\n"
      "SELECT a / 2 FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT a / 2 FROM t");
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kResults);
}

TEST_F(SqlFileTest, columns) {
  auto entries = parseQueries(
      "-- columns\n"
      "SELECT a, b FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_TRUE(entries[0].checkColumnNames);
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kResults);
}

TEST_F(SqlFileTest, columnsWithOrdered) {
  auto entries = parseQueries(
      "-- ordered\n"
      "-- columns\n"
      "SELECT a FROM t ORDER BY a");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_TRUE(entries[0].checkColumnNames);
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kOrdered);
}

TEST_F(SqlFileTest, columnsWithCount) {
  VELOX_ASSERT_THROW(
      parseQueries(
          "-- count 5\n"
          "-- columns\n"
          "SELECT * FROM t"),
      "'-- columns' can only be used with 'results' or 'ordered' queries");
}

TEST_F(SqlFileTest, columnsWithError) {
  VELOX_ASSERT_THROW(
      parseQueries(
          "-- error: bad\n"
          "-- columns\n"
          "SELECT * FROM t"),
      "'-- columns' can only be used with 'results' or 'ordered' queries");
}

TEST_F(SqlFileTest, setupBlock) {
  auto file = SqlFile::parse(
      "-- setup\n"
      "CREATE TABLE t(a BIGINT)\n"
      "----\n"
      "INSERT INTO t VALUES (1), (2)\n"
      "-- end_setup\n"
      "\n"
      "SELECT * FROM t",
      /*baseDir=*/"");

  ASSERT_THAT(file.setupStatements, testing::SizeIs(2));
  EXPECT_EQ(file.setupStatements[0], "CREATE TABLE t(a BIGINT)");
  EXPECT_EQ(file.setupStatements[1], "INSERT INTO t VALUES (1), (2)");
  ASSERT_THAT(file.entries, testing::SizeIs(1));
  EXPECT_EQ(file.entries[0].sql, "SELECT * FROM t");
}

TEST_F(SqlFileTest, setupBlockPreservesQueryAnnotations) {
  // Plain comments after a setup block should stay with the queries so
  // their '-- duckdb:' / '-- ordered' / etc. annotations are honored.
  auto file = SqlFile::parse(
      "-- setup\n"
      "CREATE TABLE t(a BIGINT)\n"
      "-- end_setup\n"
      "\n"
      "-- ordered\n"
      "SELECT * FROM t ORDER BY a",
      /*baseDir=*/"");

  ASSERT_THAT(file.entries, testing::SizeIs(1));
  EXPECT_EQ(file.entries[0].type, QueryEntry::Type::kOrdered);
  EXPECT_EQ(file.entries[0].sql, "SELECT * FROM t ORDER BY a");
}

TEST_F(SqlFileTest, unterminatedSetupBlock) {
  VELOX_ASSERT_THROW(
      SqlFile::parse(
          "-- setup\n"
          "CREATE TABLE t(a BIGINT)\n"
          "\n"
          "SELECT * FROM t",
          /*baseDir=*/""),
      "Unterminated setup block");
}

TEST_F(SqlFileTest, setupFileResolvesRelativePath) {
  // Write a small setup file in a temp directory and reference it via
  // setup_file.
  auto tempDir = std::filesystem::temp_directory_path() /
      "axiom_sql_file_test_setup_relative";
  std::filesystem::create_directories(tempDir);
  auto setupPath = tempDir / "shared_setup.sql";
  {
    std::ofstream out(setupPath);
    out << "CREATE TABLE u(x BIGINT)\n"
        << "----\n"
        << "INSERT INTO u VALUES (1), (2), (3)\n";
  }

  auto file = SqlFile::parse(
      "-- setup_file: shared_setup.sql\n"
      "\n"
      "SELECT count(*) FROM u",
      tempDir.string());

  ASSERT_THAT(file.setupStatements, testing::SizeIs(2));
  EXPECT_EQ(file.setupStatements[0], "CREATE TABLE u(x BIGINT)");
  EXPECT_EQ(file.setupStatements[1], "INSERT INTO u VALUES (1), (2), (3)");
  ASSERT_THAT(file.entries, testing::SizeIs(1));
  EXPECT_EQ(file.entries[0].sql, "SELECT count(*) FROM u");

  std::filesystem::remove_all(tempDir);
}

TEST_F(SqlFileTest, setupFileRequiresBaseDir) {
  VELOX_ASSERT_THROW(
      SqlFile::parse(
          "-- setup_file: shared_setup.sql\n"
          "SELECT 1",
          /*baseDir=*/""),
      "setup_file directive requires a non-empty baseDir");
}

TEST_F(SqlFileTest, setupDirectiveAfterFirstQueryIsRejected) {
  // setup_file appearing after a query has started must be rejected; the
  // directive would otherwise be silently swallowed as an unknown
  // annotation.
  VELOX_ASSERT_THROW(
      parseQueries(
          "SELECT 1\n"
          "----\n"
          "-- setup_file: extra.sql\n"
          "SELECT 2"),
      "Setup directives must appear before the first query");
}

TEST_F(SqlFileTest, setupBlockDirectiveAfterFirstQueryIsRejected) {
  VELOX_ASSERT_THROW(
      parseQueries(
          "SELECT 1\n"
          "----\n"
          "-- setup\n"
          "CREATE TABLE u(a BIGINT)\n"
          "-- end_setup\n"
          "SELECT 2"),
      "Setup directives must appear before the first query");
}

TEST_F(SqlFileTest, lineNumbersUnaffectedBySetup) {
  // Query line numbers should reflect their position in the original
  // source even after setup directives are consumed.
  auto file = SqlFile::parse(
      "-- setup\n" //  line 1
      "CREATE TABLE t(a BIGINT)\n" //  line 2
      "-- end_setup\n" //  line 3
      "\n" //  line 4
      "SELECT 1\n" //  line 5
      "----\n" //  line 6
      "SELECT 2", //  line 7
      /*baseDir=*/"");

  ASSERT_THAT(file.entries, testing::SizeIs(2));
  EXPECT_EQ(file.entries[0].lineNumber, 5);
  EXPECT_EQ(file.entries[1].lineNumber, 7);
}

TEST_F(SqlFileTest, connectorDefaultsToTest) {
  EXPECT_EQ(SqlFile::parse("SELECT 1", "").connector, TestConnectorKind::kTest);
}

TEST_F(SqlFileTest, connectorDirective) {
  {
    auto file = SqlFile::parse(
        "-- connector: hive\n"
        "SELECT 1",
        "");
    EXPECT_EQ(file.connector, TestConnectorKind::kLocalHive);
    ASSERT_EQ(file.entries.size(), 1);
    EXPECT_EQ(file.entries[0].sql, "SELECT 1");
    // The directive line is consumed, so query line numbers are unaffected.
    EXPECT_EQ(file.entries[0].lineNumber, 2);
  }
  {
    auto file = SqlFile::parse("-- connector: test\nSELECT 1", "");
    EXPECT_EQ(file.connector, TestConnectorKind::kTest);
  }
}

TEST_F(SqlFileTest, connectorDirectiveWithSetup) {
  auto file = SqlFile::parse(
      "-- connector: hive\n"
      "-- setup\n"
      "CREATE TABLE t(a BIGINT)\n"
      "-- end_setup\n"
      "SELECT a FROM t",
      "");
  EXPECT_EQ(file.connector, TestConnectorKind::kLocalHive);
  ASSERT_EQ(file.setupStatements.size(), 1);
  EXPECT_EQ(file.setupStatements[0], "CREATE TABLE t(a BIGINT)");
  ASSERT_EQ(file.entries.size(), 1);
  EXPECT_EQ(file.entries[0].sql, "SELECT a FROM t");
}

TEST_F(SqlFileTest, connectorUnknownRejected) {
  VELOX_ASSERT_THROW(
      SqlFile::parse("-- connector: postgres\nSELECT 1", ""),
      "Unknown connector directive 'postgres'");
}

TEST_F(SqlFileTest, customDirectiveCaptured) {
  auto file = SqlFile::parse(
      "-- tables: orders, nation\n"
      "SELECT * FROM orders",
      /*baseDir=*/"",
      {.customSetupDirectives = {"tables"}});
  EXPECT_EQ(file.directives.at("tables"), "orders, nation");
  ASSERT_THAT(file.entries, testing::SizeIs(1));
  EXPECT_EQ(file.entries[0].sql, "SELECT * FROM orders");
  // Query line numbers are unaffected by the captured directive.
  EXPECT_EQ(file.entries[0].lineNumber, 2);
}

TEST_F(SqlFileTest, customDirectiveNotListedStaysComment) {
  // A '-- name: value' line whose name is not requested is an ordinary
  // comment and is not captured.
  auto file = SqlFile::parse(
      "-- tables: orders\n"
      "SELECT 1",
      /*baseDir=*/"");
  EXPECT_THAT(file.directives, testing::IsEmpty());
  ASSERT_THAT(file.entries, testing::SizeIs(1));
  EXPECT_EQ(file.entries[0].sql, "SELECT 1");
}

TEST_F(SqlFileTest, customDirectiveAbsent) {
  auto file = SqlFile::parse(
      "SELECT 1", /*baseDir=*/"", {.customSetupDirectives = {"tables"}});
  EXPECT_THAT(file.directives, testing::IsEmpty());
}

TEST_F(SqlFileTest, customDirectiveWithSetupAndConnector) {
  auto file = SqlFile::parse(
      "-- connector: hive\n"
      "-- tables: none\n"
      "-- setup\n"
      "CREATE TABLE t(a BIGINT)\n"
      "-- end_setup\n"
      "SELECT a FROM t",
      /*baseDir=*/"",
      {.customSetupDirectives = {"tables"}});
  EXPECT_EQ(file.connector, TestConnectorKind::kLocalHive);
  EXPECT_EQ(file.directives.at("tables"), "none");
  ASSERT_THAT(file.setupStatements, testing::SizeIs(1));
  ASSERT_THAT(file.entries, testing::SizeIs(1));
  EXPECT_EQ(file.entries[0].sql, "SELECT a FROM t");
}

} // namespace
} // namespace facebook::axiom::optimizer::test
