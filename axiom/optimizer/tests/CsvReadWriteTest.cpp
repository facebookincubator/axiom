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

#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/dwio/text/RegisterTextReader.h"
#include "velox/dwio/text/RegisterTextWriter.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;

class CsvReadWriteTest : public test::HiveQueriesTestBase,
                         public ::testing::WithParamInterface<bool> {
 protected:
  const std::string kDefaultSchema{
      connector::hive::LocalHiveConnectorMetadata::kDefaultSchema};

  void SetUp() override {
    useV2_ = GetParam();
    HiveQueriesTestBase::SetUp();
    velox::text::registerTextReaderFactory();
    velox::text::registerTextWriterFactory();
  }

  void TearDown() override {
    velox::text::unregisterTextReaderFactory();
    velox::text::unregisterTextWriterFactory();
    HiveQueriesTestBase::TearDown();
  }

  void dropTableIfExists(std::string_view tableName) {
    hiveMetadata().dropTableIfExists(tableName);
  }

  // Returns the DDL string from SHOW CREATE TABLE.
  std::string showCreateTable(const std::string& tableName) {
    auto sql = fmt::format("SHOW CREATE TABLE {}", tableName);
    auto statement = prestoParser().parse(sql);
    auto result = runVelox(
        statement->as<::axiom::sql::presto::SelectStatement>()->plan());
    return result.getOnlyResult().value<std::string>();
  }
};

// Create a TEXT table via SQL and verify SHOW CREATE TABLE output.
TEST_P(CsvReadWriteTest, createTextTable) {
  SCOPE_EXIT {
    dropTableIfExists("text_test");
  };

  runCtas(
      "CREATE TABLE text_test(id, name) "
      "WITH (file_format = 'TEXT', compression_kind = 'NONE') "
      "AS SELECT 1, 'Alice'");

  EXPECT_EQ(
      showCreateTable("text_test"),
      "CREATE TABLE test-hive.\"default\".\"text_test\" (\n"
      "   id INTEGER,\n"
      "   name VARCHAR\n"
      ")\n"
      "WITH (\n"
      "   compression_kind = 'none',\n"
      "   file_format = 'text'\n"
      ")");
}

// Create a CSV table (TEXT with comma delimiter) via SQL and verify
// SHOW CREATE TABLE output includes field.delim and serialization.null.format.
TEST_P(CsvReadWriteTest, createCsvTable) {
  SCOPE_EXIT {
    dropTableIfExists("csv_test");
  };

  runCtas(
      "CREATE TABLE csv_test(id, name) "
      "WITH (file_format = 'TEXT', \"field.delim\" = ',', "
      "\"serialization.null.format\" = '\\N') "
      "AS SELECT 1, 'Alice'");

  EXPECT_EQ(
      showCreateTable("csv_test"),
      "CREATE TABLE test-hive.\"default\".\"csv_test\" (\n"
      "   id INTEGER,\n"
      "   name VARCHAR\n"
      ")\n"
      "WITH (\n"
      "   field.delim = ',',\n"
      "   file_format = 'text',\n"
      "   serialization.null.format = '\\N'\n"
      ")");
}

TEST_P(CsvReadWriteTest, validateSchemaFileProperties) {
  SCOPE_EXIT {
    dropTableIfExists("custom_delimiter_test");
  };

  auto tableType = ROW({
      {"id", INTEGER()},
      {"count", BIGINT()},
      {"price", DOUBLE()},
      {"name", VARCHAR()},
      {"active", BOOLEAN()},
  });

  folly::F14FastMap<std::string, velox::Variant> options = {
      {"file_format", "text"},
      {"field.delim", "|"},
      {"serialization.null.format", ""},
  };

  std::string csvFilePath = getTestDataPath("custom_delim/data.csv");
  createTableFromFiles(
      "custom_delimiter_test", tableType, {csvFilePath}, options);

  hiveMetadata().reloadTableFromPath({kDefaultSchema, "custom_delimiter_test"});

  auto table =
      hiveMetadata().findTable({kDefaultSchema, "custom_delimiter_test"});
  ASSERT_NE(table, nullptr);

  const auto& tableOptions = table->options();

  ASSERT_TRUE(tableOptions.count("file_format"));
  EXPECT_EQ(tableOptions.at("file_format").value<std::string>(), "text");

  ASSERT_TRUE(tableOptions.count("field.delim"));
  EXPECT_EQ(tableOptions.at("field.delim").value<std::string>(), "|");

  ASSERT_TRUE(tableOptions.count("serialization.null.format"));
  EXPECT_EQ(
      tableOptions.at("serialization.null.format").value<std::string>(), "");
}

// Test reading CSV with custom delimiter and representative data types.
TEST_P(CsvReadWriteTest, readCustomDelimiterWithVariousTypes) {
  SCOPE_EXIT {
    dropTableIfExists("custom_delimiter_test");
  };

  auto tableType = ROW({
      {"id", INTEGER()},
      {"count", BIGINT()},
      {"price", DOUBLE()},
      {"name", VARCHAR()},
      {"active", BOOLEAN()},
  });

  folly::F14FastMap<std::string, velox::Variant> options = {
      {"file_format", "text"},
      {"field.delim", "|"},
      {"serialization.null.format", ""},
  };

  std::string csvFilePath = getTestDataPath("custom_delim/data.csv");
  createTableFromFiles(
      "custom_delimiter_test", tableType, {csvFilePath}, options);

  auto expectedData = makeRowVector(
      {"id", "count", "price", "name", "active"},
      {
          makeFlatVector<int32_t>({1, 2, 3, 4}),
          makeFlatVector<int64_t>({100, 200, 300, 400}),
          makeNullableFlatVector<double>({99.5, 149.99, std::nullopt, 29.99}),
          makeNullableFlatVector<std::string>(
              {"Product A", std::nullopt, "Product C", "Product D"}),
          makeFlatVector<bool>({true, false, true, false}),
      });

  checkTableData("custom_delimiter_test", {expectedData});
}

// Test writing CSV using INSERT statement with various data types.
TEST_P(CsvReadWriteTest, writeWithInsertStatement) {
  SCOPE_EXIT {
    dropTableIfExists("csv_write_test");
  };

  auto tableType = ROW({
      {"id", INTEGER()},
      {"description", VARCHAR()},
      {"amount", DOUBLE()},
      {"is_active", BOOLEAN()},
  });

  folly::F14FastMap<std::string, velox::Variant> options = {
      {"file_format", "text"},
      {"field.delim", "\1"},
      {"serialization.null.format", "\\N"},
  };

  createEmptyTable("csv_write_test", tableType, options);

  auto expectedData = makeRowVector(
      {"id", "description", "amount", "is_active"},
      {
          makeNullableFlatVector<int32_t>({10, std::nullopt, 30, 40}),
          makeNullableFlatVector<std::string>(
              {"First item", std::nullopt, "Third item", "Fourth item"}),
          makeNullableFlatVector<double>({100.5, 200.0, std::nullopt, 400.25}),
          makeNullableFlatVector<bool>({true, false, std::nullopt, true}),
      });

  auto logicalPlan = parseInsert(
      "INSERT INTO csv_write_test "
      "SELECT * FROM (VALUES "
      "  (10, 'First item', 100.5, true), "
      "  (NULL, NULL, 200.0, false), "
      "  (30, 'Third item', NULL, NULL), "
      "  (40, 'Fourth item', 400.25, true)"
      ")");

  runVelox(logicalPlan);

  checkTableData("csv_write_test", {expectedData});
}

AXIOM_INSTANTIATE_V1_V2(CsvReadWriteTest);

} // namespace
} // namespace facebook::axiom::optimizer
