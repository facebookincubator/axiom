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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/sql/presto/PrestoParser.h"

namespace axiom::sql::presto {
namespace {

class TableExtractorTest : public testing::Test {
 protected:
  void testInputsOutputs(
      const std::string& sql,
      const std::unordered_set<std::string>& expectedInputs,
      const std::optional<std::string>& expectedOutput) {
    SCOPED_TRACE(sql);
    PrestoParser parser = PrestoParser(kDefaultCatalog, kDefaultSchema);
    auto result = parser.getReferencedTables(sql);
    ASSERT_EQ(result.inputTables, expectedInputs);
    ASSERT_EQ(result.outputTable, expectedOutput);
  }

  void testInputs(
      const std::string& sql,
      const std::unordered_set<std::string>& expectedInputs) {
    testInputsOutputs(sql, expectedInputs, std::nullopt);
  }

  void testInput(const std::string& sql, const std::string& expectedInput) {
    testInputsOutputs(sql, {expectedInput}, std::nullopt);
  }

  void testOutputs(
      const std::string& sql,
      const std::optional<std::string>& expectedOutput) {
    testInputsOutputs(sql, {}, expectedOutput);
  }

 private:
  static constexpr const char* kDefaultCatalog = "foo";
  static constexpr const char* kDefaultSchema = "bar";
};

TEST_F(TableExtractorTest, select) {
  testInput("SELECT * FROM t", "foo.bar.t");
  testInput("SELECT * FROM schema.table1", "foo.schema.table1");
  testInput("SELECT * FROM catalog.schema.table1", "catalog.schema.table1");
}

TEST_F(TableExtractorTest, joins) {
  testInputs(
      "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
      {"foo.bar.t1", "foo.bar.t2"});
  testInputs(
      "SELECT * "
      "FROM t1 "
      "INNER JOIN t2 ON t1.id = t2.id "
      "LEFT JOIN t3 ON t2.id = t3.id"
      "RIGHT JOIN t4 ON t3.id = t4.id"
      "OUTER JOIN t5 ON t4.id = t5.id ",
      {"foo.bar.t1", "foo.bar.t2", "foo.bar.t3", "foo.bar.t4", "foo.bar.t5"});
}

TEST_F(TableExtractorTest, subquery) {
  testInputs(
      "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)",
      {"foo.bar.t1", "foo.bar.t2"});
}

TEST_F(TableExtractorTest, cte) {
  testInput(
      "WITH cte AS (SELECT * FROM t1) "
      "SELECT * FROM cte",
      "foo.bar.t1");
  testInput(
      "WITH t1 AS (SELECT * FROM t1) "
      "SELECT * FROM t1",
      "foo.bar.t1");
  testInputs(
      "WITH cte1 AS (SELECT * FROM t1), "
      "     cte2 AS (SELECT * FROM cte1 JOIN t2 ON cte1.id = t2.id) "
      "SELECT * FROM cte2",
      {"foo.bar.t1", "foo.bar.t2"});
}

TEST_F(TableExtractorTest, values) {
  testInputs("SELECT * FROM (VALUES (1, 'a'), (2, 'b'))", {});
}

TEST_F(TableExtractorTest, modify) {
  testInputsOutputs(
      "INSERT INTO target SELECT * FROM source",
      {"foo.bar.source"},
      "foo.bar.target");
  testInputsOutputs(
      "INSERT INTO target VALUES (1, 2, 3)", {}, "foo.bar.target");

  // This set of query shapes are not yet supported:
  //   - UPDATE
  //   - DELETE FROM
  // Add test coverage for them once we can parse these.
}

TEST_F(TableExtractorTest, create) {
  testInputsOutputs(
      "CREATE TABLE new_table AS SELECT * FROM source",
      {"foo.bar.source"},
      "foo.bar.new_table");

  // This set of query shapes are not yet supported:
  //   - CREATE TABLE
  //   - CREATE VIEW
  //   - CREATE MATERIALIZED VIEW
  // Add test coverage for them once we can parse these.
}

TEST_F(TableExtractorTest, drop) {
  testOutputs("DROP TABLE t", "foo.bar.t");
  testOutputs("DROP TABLE IF EXISTS schema.t", "foo.schema.t");

  // This set of query shapes are not yet supported:
  //   - DROP VIEW
  //   - DROP MATERIALIZED VIEW
  // Add test coverage for them once we can parse these.
}

} // namespace
} // namespace axiom::sql::presto
