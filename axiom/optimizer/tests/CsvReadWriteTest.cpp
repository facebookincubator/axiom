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
#include "velox/dwio/text/RegisterTextReader.h"
#include "velox/dwio/text/RegisterTextWriter.h"

#include <algorithm>

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

// Validate that CSV format with comma delimiters and custom null format works
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
      // Velox writer does not use these properly
      // so we will test against only defaults for now
      // but revert back to commented fields after Velox fix lands
      // {"field.delim", ","},
      {"field.delim", "\1"},
      // {"serialization.null.format", "NULL"},
      {"serialization.null.format", "\\N"},
  };

  createEmptyTable("csv_null_test", tableType, options);

  auto data = makeRowVector(
      {"id", "name", "score"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeNullableFlatVector<std::string>(
              {"Alice", std::nullopt, "Charlie"}),
          makeNullableFlatVector<double>({99.5, 85.0, std::nullopt}),
      });

  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto writePlan =
      lp::PlanBuilder(context)
          .values({data})
          .tableWrite(
              "csv_null_test", lp::WriteKind::kInsert, {"id", "name", "score"})
          .build();

  runVelox(writePlan);

  // Verify the actual CSV file format with comma delimiters and NULL strings
  auto tablePath = hiveMetadata().tablePath("csv_null_test");
  std::vector<std::string> csvFiles;
  for (const auto& entry : std::filesystem::directory_iterator(tablePath)) {
    if (entry.is_regular_file()) {
      auto filename = entry.path().filename().string();
      // Skip schema files
      if (filename != ".schema") {
        csvFiles.push_back(entry.path().string());
      }
    }
  }

  ASSERT_FALSE(csvFiles.empty()) << "Expected at least one CSV file";

  // Read all CSV files and collect all lines
  std::vector<std::string> allLines;
  for (const auto& csvFile : csvFiles) {
    std::ifstream file(csvFile);
    std::string line;
    while (std::getline(file, line)) {
      if (!line.empty()) { // Skip empty lines
        allLines.push_back(line);
      }
    }
  }

  // Sort lines for consistent comparison (order may vary across files)
  std::sort(allLines.begin(), allLines.end());

  // Verify we have exactly our 3 rows
  ASSERT_EQ(allLines.size(), 3)
      << "Expected 3 lines of data total, got " << allLines.size();

  // Verify CSV format with \1 delimiter and \N for nulls
  // Note: Doubles may have extra precision (99.500000 vs 99.5)
  EXPECT_TRUE(allLines[0].starts_with("1\1Alice\199.5"));
  EXPECT_TRUE(allLines[1].starts_with("2\1\\N\185"));
  EXPECT_TRUE(allLines[2].starts_with("3\1Charlie\1\\N"));

  // Verify round-trip read works correctly
  checkTableData("csv_null_test", {data});
}

// validate an E2E read from an external file
TEST_F(CsvReadWriteTest, readFromExternalFile) {
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("external_employees");
  };

  auto tableType = ROW({
      {"id", INTEGER()},
      {"name", VARCHAR()},
      {"score", DOUBLE()},
  });

  folly::F14FastMap<std::string, velox::Variant> options = {
      {"file_format", "text"},
      {"field.delim", ","},
      {"serialization.null.format", ""},
  };

  std::string csvFilePath = getTestDataPath("employees/employees.csv");
  createTableFromFile("external_employees", tableType, csvFilePath, options);

  auto expectedData = makeRowVector(
      {"id", "name", "score"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<std::string>({"Alice", "Bob", "Charlie"}),
          makeNullableFlatVector<double>({99.5, 85.0, std::nullopt}),
      });

  checkTableData("external_employees", {expectedData});
}

} // namespace
} // namespace facebook::axiom::optimizer
