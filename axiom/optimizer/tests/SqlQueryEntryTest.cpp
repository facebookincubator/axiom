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

#include "axiom/optimizer/tests/SqlQueryEntry.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer::test {
namespace {

class SqlQueryEntryTest : public ::testing::Test {};

TEST_F(SqlQueryEntryTest, empty) {
  auto entries = QueryEntry::parse("");
  EXPECT_THAT(entries, testing::IsEmpty());
}

TEST_F(SqlQueryEntryTest, singleQuery) {
  auto entries = QueryEntry::parse("SELECT 1");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT 1");
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kResults);
  EXPECT_EQ(entries[0].lineNumber, 1);
}

TEST_F(SqlQueryEntryTest, multipleQueries) {
  auto entries = QueryEntry::parse(
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

TEST_F(SqlQueryEntryTest, multiLineQuery) {
  auto entries = QueryEntry::parse(
      "SELECT a, b\n"
      "FROM t\n"
      "WHERE a > 1");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT a, b\nFROM t\nWHERE a > 1");
  EXPECT_EQ(entries[0].lineNumber, 1);
}

TEST_F(SqlQueryEntryTest, ordered) {
  auto entries = QueryEntry::parse(
      "-- ordered\n"
      "SELECT a FROM t ORDER BY a");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kOrdered);
  EXPECT_EQ(entries[0].sql, "SELECT a FROM t ORDER BY a");
  EXPECT_EQ(entries[0].lineNumber, 2);
}

TEST_F(SqlQueryEntryTest, count) {
  auto entries = QueryEntry::parse(
      "-- count 42\n"
      "SELECT * FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kCount);
  EXPECT_EQ(entries[0].expectedCount, 42);
}

TEST_F(SqlQueryEntryTest, error) {
  auto entries = QueryEntry::parse(
      "-- error: Column not found\n"
      "SELECT missing FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kError);
  EXPECT_EQ(entries[0].expectedError, "Column not found");
}

TEST_F(SqlQueryEntryTest, duckdb) {
  auto entries = QueryEntry::parse(
      "-- duckdb: SELECT 1 AS x\n"
      "SELECT 1 x");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kResults);
  EXPECT_EQ(entries[0].duckDbSql, std::optional<std::string>("SELECT 1 AS x"));
  EXPECT_EQ(entries[0].sql, "SELECT 1 x");
}

TEST_F(SqlQueryEntryTest, disabled) {
  auto entries = QueryEntry::parse(
      "-- disabled\n"
      "SELECT broken_query\n"
      "----\n"
      "SELECT 1");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT 1");
}

TEST_F(SqlQueryEntryTest, trailingWhitespace) {
  auto entries = QueryEntry::parse("SELECT 1   \n\n");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT 1");
}

TEST_F(SqlQueryEntryTest, lineNumbers) {
  auto entries = QueryEntry::parse(
      "-- ordered\n"
      "SELECT 1\n"
      "----\n"
      "-- count 5\n"
      "SELECT *\n"
      "FROM t");
  ASSERT_THAT(entries, testing::SizeIs(2));
  // First query: annotation on line 1, SQL starts on line 2.
  EXPECT_EQ(entries[0].lineNumber, 2);
  // Second query: annotation on line 4, SQL starts on line 5.
  EXPECT_EQ(entries[1].lineNumber, 5);
}

TEST_F(SqlQueryEntryTest, sqlCommentInBody) {
  // A "-- " comment after SQL has started is part of the SQL body, not an
  // annotation.
  auto entries = QueryEntry::parse(
      "SELECT 1\n"
      "-- this is a SQL comment\n"
      "FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT 1\n-- this is a SQL comment\nFROM t");
}

TEST_F(SqlQueryEntryTest, commentBeforeSql) {
  // Unrecognized "-- " lines before SQL starts are treated as comments.
  auto entries = QueryEntry::parse(
      "-- This query tests basic arithmetic.\n"
      "-- It divides column a by 2.\n"
      "SELECT a / 2 FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_EQ(entries[0].sql, "SELECT a / 2 FROM t");
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kResults);
}

TEST_F(SqlQueryEntryTest, columns) {
  auto entries = QueryEntry::parse(
      "-- columns\n"
      "SELECT a, b FROM t");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_TRUE(entries[0].checkColumnNames);
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kResults);
}

TEST_F(SqlQueryEntryTest, columnsWithOrdered) {
  auto entries = QueryEntry::parse(
      "-- ordered\n"
      "-- columns\n"
      "SELECT a FROM t ORDER BY a");
  ASSERT_THAT(entries, testing::SizeIs(1));
  EXPECT_TRUE(entries[0].checkColumnNames);
  EXPECT_EQ(entries[0].type, QueryEntry::Type::kOrdered);
}

TEST_F(SqlQueryEntryTest, columnsWithCount) {
  VELOX_ASSERT_THROW(
      QueryEntry::parse(
          "-- count 5\n"
          "-- columns\n"
          "SELECT * FROM t"),
      "'-- columns' can only be used with 'results' or 'ordered' queries");
}

TEST_F(SqlQueryEntryTest, columnsWithError) {
  VELOX_ASSERT_THROW(
      QueryEntry::parse(
          "-- error: bad\n"
          "-- columns\n"
          "SELECT * FROM t"),
      "'-- columns' can only be used with 'results' or 'ordered' queries");
}

} // namespace
} // namespace facebook::axiom::optimizer::test
