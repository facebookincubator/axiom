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

#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
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

  void createExternalCsvTable(
      const std::string& tableName,
      const RowTypePtr& tableType,
      const std::string& csvFilePath) {
    folly::F14FastMap<std::string, velox::Variant> options = {
        {"file_format", "text"},
        {"field.delim", ","},
        {"serialization.null.format", ""},
    };

    auto session = std::make_shared<connector::ConnectorSession>("test");
    hiveMetadata().createTable(session, tableName, tableType, options);

    auto tablePath = hiveMetadata().tablePath(tableName);
    std::string targetCsvPath = fmt::format("{}/data.csv", tablePath);
    ASSERT_TRUE(std::filesystem::exists(csvFilePath))
        << "CSV file does not exist: " << csvFilePath;
    std::filesystem::copy_file(
        csvFilePath,
        targetCsvPath,
        std::filesystem::copy_options::overwrite_existing);

    hiveMetadata().loadTableFromPath(tableName);
  }

  static std::string getTestDataPath(const std::string& filename) {
    auto path = velox::test::getDataFilePath(
        "axiom/optimizer/tests", fmt::format("test_data/{}", filename));
    return path;
  }
};

TEST_F(CsvReadWriteTest, writeAndRead) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("csv_test");
  };

  auto tableType = ROW({
      {"id", INTEGER()},
      {"name", VARCHAR()},
      {"score", DOUBLE()},
  });

  folly::F14FastMap<std::string, velox::Variant> options = {
      {"file_format", "text"},
  };

  createEmptyTable("csv_test", tableType, options);

  auto data = makeRowVector(
      {"id", "name", "score"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<StringView>(
              {StringView("Alice"), StringView("Bob"), StringView("Charlie")}),
          makeFlatVector<double>({99.5, 85.0, 92.3}),
      });

  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto writePlan = lp::PlanBuilder(context)
                       .values({data})
                       .tableWrite(
                           exec::test::kHiveConnectorId,
                           "csv_test",
                           lp::WriteKind::kInsert,
                           {"id", "name", "score"},
                           {"id", "name", "score"})
                       .build();

  auto writeResult = runVelox(writePlan);
  ASSERT_EQ(1, writeResult.results.size());
  ASSERT_EQ(1, writeResult.results[0]->size());

  checkTableData("csv_test", {data});
}

TEST_F(CsvReadWriteTest, writeAndReadWithNulls) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("csv_null_test");
  };

  auto tableType = ROW({
      {"id", INTEGER()},
      {"name", VARCHAR()},
      {"score", DOUBLE()},
  });

  folly::F14FastMap<std::string, velox::Variant> options = {
      {"file_format", "text"},
  };

  createEmptyTable("csv_null_test", tableType, options);

  auto data = makeRowVector(
      {"id", "name", "score"},
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeNullableFlatVector<StringView>(
           {StringView("Alice"), std::nullopt, StringView("Charlie")}),
       makeNullableFlatVector<double>({99.5, 85.0, std::nullopt})});

  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto writePlan = lp::PlanBuilder(context)
                       .values({data})
                       .tableWrite(
                           exec::test::kHiveConnectorId,
                           "csv_null_test",
                           lp::WriteKind::kInsert,
                           {"id", "name", "score"},
                           {"id", "name", "score"})
                       .build();

  auto writeResult = runVelox(writePlan);
  ASSERT_EQ(1, writeResult.results.size());

  checkTableData("csv_null_test", {data});
}

TEST_F(CsvReadWriteTest, writeWithCustomDelimiter) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("csv_tab_delimited");
  };

  auto tableType = ROW({
      {"col1", INTEGER()},
      {"col2", VARCHAR()},
  });

  folly::F14FastMap<std::string, velox::Variant> options = {
      {"file_format", "text"},
  };

  createEmptyTable("csv_tab_delimited", tableType, options);

  auto data = makeRowVector(
      {"col1", "col2"},
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeFlatVector<StringView>(
           {StringView("value1"),
            StringView("value2"),
            StringView("value3")})});

  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto writePlan = lp::PlanBuilder(context)
                       .values({data})
                       .tableWrite(
                           exec::test::kHiveConnectorId,
                           "csv_tab_delimited",
                           lp::WriteKind::kInsert,
                           {"col1", "col2"},
                           {"col1", "col2"})
                       .build();

  auto writeResult = runVelox(writePlan);
  ASSERT_EQ(1, writeResult.results.size());

  checkTableData("csv_tab_delimited", {data});
}

TEST_F(CsvReadWriteTest, multipleBatchWrite) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("csv_multi_batch");
  };

  auto tableType = ROW({
      {"id", BIGINT()},
      {"value", VARCHAR()},
  });

  folly::F14FastMap<std::string, velox::Variant> options = {
      {"file_format", "text"},
  };

  createEmptyTable("csv_multi_batch", tableType, options);

  std::vector<RowVectorPtr> batches;
  for (int i = 0; i < 3; i++) {
    const auto start = i * 10;
    batches.push_back(makeRowVector(
        {"id", "value"},
        {makeFlatVector<int64_t>(10, [start](auto row) { return start + row; }),
         makeFlatVector<std::string>(10, [start](auto row) {
           return fmt::format("value_{}", start + row);
         })}));
  }

  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto writePlan = lp::PlanBuilder(context)
                       .values(batches)
                       .tableWrite(
                           exec::test::kHiveConnectorId,
                           "csv_multi_batch",
                           lp::WriteKind::kInsert,
                           {"id", "value"},
                           {"id", "value"})
                       .build();

  auto writeResult = runVelox(writePlan);
  ASSERT_EQ(1, writeResult.results.size());

  checkTableData("csv_multi_batch", batches);
}

TEST_F(CsvReadWriteTest, readFromExternalFile) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("external_employees");
  };

  auto tableType = ROW({
      {"id", INTEGER()},
      {"name", VARCHAR()},
      {"score", DOUBLE()},
  });

  std::string csvFilePath = getTestDataPath("employees/employees.csv");
  createExternalCsvTable("external_employees", tableType, csvFilePath);

  auto expectedData = makeRowVector(
      {"id", "name", "score"},
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeFlatVector<StringView>(
           {StringView("Alice"), StringView("Bob"), StringView("Charlie")}),
       makeFlatVector<double>({99.5, 85.0, 92.3})});

  checkTableData("external_employees", {expectedData});
}

TEST_F(CsvReadWriteTest, readFromExternalFileMultipleColumns) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("external_products");
  };

  auto tableType = ROW({
      {"product_id", INTEGER()},
      {"product_name", VARCHAR()},
      {"price", DOUBLE()},
      {"quantity", INTEGER()},
  });

  std::string csvFilePath = getTestDataPath("products/products.csv");
  createExternalCsvTable("external_products", tableType, csvFilePath);

  auto expectedData = makeRowVector(
      {"product_id", "product_name", "price", "quantity"},
      {makeFlatVector<int32_t>({10, 20, 30, 40}),
       makeFlatVector<StringView>(
           {StringView("Product A"),
            StringView("Product B"),
            StringView("Product C"),
            StringView("Product D")}),
       makeFlatVector<double>({29.99, 49.99, 19.99, 99.99}),
       makeFlatVector<int32_t>({100, 50, 200, 25})});

  checkTableData("external_products", {expectedData});
}

} // namespace
} // namespace facebook::axiom::optimizer
