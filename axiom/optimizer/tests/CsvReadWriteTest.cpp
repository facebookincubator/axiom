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
namespace lp = facebook::axiom::logical_plan;

class CsvReadWriteTest : public test::HiveQueriesTestBase {
 protected:
  void SetUp() override {
    HiveQueriesTestBase::SetUp();
    velox::text::registerTextReaderFactory();
    velox::text::registerTextWriterFactory();
  }

  void TearDown() override {
    velox::text::unregisterTextReaderFactory();
    velox::text::unregisterTextWriterFactory();
    HiveQueriesTestBase::TearDown();
  }
};

TEST_F(CsvReadWriteTest, validateSchemaFileProperties) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("custom_delimiter_test");
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

  hiveMetadata().reloadTableFromPath("custom_delimiter_test");

  auto table = hiveMetadata().findTable("custom_delimiter_test");
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
TEST_F(CsvReadWriteTest, readCustomDelimiterWithVariousTypes) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("custom_delimiter_test");
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
TEST_F(CsvReadWriteTest, writeWithInsertStatement) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("csv_write_test");
  };

  auto tableType = ROW({
      {"id", BIGINT()},
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
          makeNullableFlatVector<int64_t>({10, std::nullopt, 30, 40}),
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

} // namespace
} // namespace facebook::axiom::optimizer
