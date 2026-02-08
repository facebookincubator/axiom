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

#include <boost/algorithm/string.hpp>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/ParquetTpcdsTest.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/text/RegisterTextReader.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

DECLARE_uint32(optimizer_trace);

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class TpcdsPlanTest : public virtual test::QueryTestBase {
 protected:
  static void SetUpTestCase() {
    test::QueryTestBase::SetUpTestCase();

    gTempDirectory_ = exec::test::TempDirectoryPath::create();
    test::ParquetTpcdsTest::createTables(gTempDirectory_->getPath());

    LocalRunnerTestBase::localDataPath_ = gTempDirectory_->getPath();
    LocalRunnerTestBase::localFileFormat_ =
        velox::dwio::common::FileFormat::PARQUET;
  }

  static void TearDownTestCase() {
    gTempDirectory_.reset();
    test::QueryTestBase::TearDownTestCase();
  }

  void SetUp() override {
    test::QueryTestBase::SetUp();

    prestoParser_ = std::make_unique<::axiom::sql::presto::PrestoParser>(
        exec::test::kHiveConnectorId, std::nullopt);

    connector_ = velox::connector::getConnector(exec::test::kHiveConnectorId);
    metadata_ = dynamic_cast<connector::hive::LocalHiveConnectorMetadata*>(
        connector::ConnectorMetadata::metadata(exec::test::kHiveConnectorId));
  }

  void TearDown() override {
    metadata_ = nullptr;
    connector_.reset();

    test::QueryTestBase::TearDown();
  }

  static std::string readSqlFromFile(const std::string& filePath) {
    auto path = velox::test::getDataFilePath("axiom/optimizer/tests", filePath);
    std::ifstream inputFile(path, std::ifstream::binary);

    VELOX_CHECK(inputFile, "Failed to open SQL file: {}", path);

    auto begin = inputFile.tellg();
    inputFile.seekg(0, std::ios::end);
    auto end = inputFile.tellg();

    const auto fileSize = end - begin;
    VELOX_CHECK_GT(fileSize, 0, "SQL file is empty: {}", path);

    std::string sql;
    sql.resize(fileSize);

    inputFile.seekg(begin);
    inputFile.read(sql.data(), fileSize);
    inputFile.close();

    return sql;
  }

  static std::string readTpcdsSql(const std::string& queryName) {
    auto sql =
        readSqlFromFile(fmt::format("tpcds.queries/{}.sql", queryName));

    // Strip the presto test metadata comment line.
    if (sql.starts_with("--")) {
      auto pos = sql.find('\n');
      if (pos != std::string::npos) {
        sql = sql.substr(pos + 1);
      }
    }

    // Drop trailing semicolon.
    boost::trim_right(sql);
    if (!sql.empty() && sql.back() == ';') {
      sql.pop_back();
    }
    return sql;
  }

  lp::LogicalPlanNodePtr parseTpcdsSql(const std::string& queryName) {
    auto sql = readTpcdsSql(queryName);

    auto statement = prestoParser_->parse(sql);

    VELOX_CHECK(statement->isSelect());

    auto logicalPlan =
        statement->as<::axiom::sql::presto::SelectStatement>()->plan();
    VELOX_CHECK_NOT_NULL(logicalPlan);

    return logicalPlan;
  }

  lp::LogicalPlanNodePtr parseSelect(std::string_view sql) {
    return test::QueryTestBase::parseSelect(
        sql, velox::exec::test::kHiveConnectorId);
  }

  /// Parses a Velox type from a TPC-DS result file type name.
  static TypePtr parseResultType(const std::string& typeName) {
    if (typeName == "CHAR" || typeName == "VARCHAR") {
      return VARCHAR();
    }
    if (typeName == "INTEGER") {
      return INTEGER();
    }
    if (typeName == "BIGINT") {
      return BIGINT();
    }
    if (typeName == "DECIMAL") {
      // TPC-DS qualification results use plain "DECIMAL" without
      // precision/scale. We read them as VARCHAR and let the caller cast if
      // needed, but DOUBLE is a pragmatic match for comparison purposes.
      return DOUBLE();
    }
    if (typeName == "DATE") {
      return DATE();
    }
    if (typeName == "DOUBLE" || typeName == "REAL" || typeName == "FLOAT") {
      return DOUBLE();
    }
    if (typeName == "BOOLEAN") {
      return BOOLEAN();
    }
    if (typeName == "SMALLINT") {
      return SMALLINT();
    }
    if (typeName == "TINYINT") {
      return TINYINT();
    }
    VELOX_FAIL("Unknown TPC-DS result type: {}", typeName);
  }

  /// Reads a TPC-DS reference result file and returns its rows as
  /// RowVectors.  'query' is the file name without the .result suffix,
  /// e.g. "q01".  The first line of the file declares the delimiter and
  /// column types:
  ///   -- delimiter: |; types: INTEGER|CHAR|DECIMAL
  /// CHAR is mapped to VARCHAR; other names are mapped to the Velox type
  /// with the matching name.
  std::vector<RowVectorPtr> readReferenceResult(std::string_view query) {
    auto filePath = velox::test::getDataFilePath(
        "axiom/optimizer/tests",
        fmt::format("tpcds.answers/{}.result", query));

    std::ifstream in(filePath);
    VELOX_CHECK(in.good(), "Cannot open result file: {}", filePath);

    // --- Parse the header line to extract column types. ---
    std::string header;
    std::getline(in, header);
    // Header format: "-- delimiter: |; types: TYPE1|TYPE2|..."
    auto typesPos = header.find("types:");
    VELOX_CHECK(
        typesPos != std::string::npos,
        "Missing 'types:' in header: {}",
        header);
    auto typesStr = header.substr(typesPos + 6);
    boost::trim(typesStr);

    // Split on '|' to get individual type names.
    std::vector<std::string> typeNames;
    boost::split(typeNames, typesStr, boost::is_any_of("|"));
    // Remove empty trailing entries (the string may end with '|' or
    // whitespace).
    while (!typeNames.empty()) {
      auto& last = typeNames.back();
      boost::trim(last);
      if (last.empty()) {
        typeNames.pop_back();
      } else {
        break;
      }
    }

    std::vector<std::string> columnNames;
    std::vector<TypePtr> columnTypes;
    for (size_t i = 0; i < typeNames.size(); ++i) {
      auto name = typeNames[i];
      boost::trim(name);
      boost::to_upper(name);
      columnTypes.push_back(parseResultType(name));
      columnNames.push_back(fmt::format("c{}", i));
    }

    auto rowType = ROW(std::move(columnNames), std::move(columnTypes));

    // --- Write a cleaned data file (strip trailing '|' on each line). ---
    auto tempDir = exec::test::TempDirectoryPath::create();
    auto dataFilePath = fmt::format("{}/data.txt", tempDir->getPath());
    {
      std::ofstream out(dataFilePath);
      std::string line;
      while (std::getline(in, line)) {
        // Skip empty lines.
        if (line.empty() || (line.size() == 1 && line[0] == '\r')) {
          continue;
        }
        // Remove trailing carriage return if present.
        if (!line.empty() && line.back() == '\r') {
          line.pop_back();
        }
        // Remove trailing '|' so the reader sees the correct number of
        // fields.
        if (!line.empty() && line.back() == '|') {
          line.pop_back();
        }
        out << line << "\n";
      }
    }
    in.close();

    // --- Read the cleaned file using PlanBuilder + Hive text reader. ---
    velox::text::registerTextReaderFactory();
    SCOPE_EXIT {
      velox::text::unregisterTextReaderFactory();
    };

    auto plan = velox::exec::test::PlanBuilder()
                    .tableScan(rowType)
                    .planNode();

    auto split =
        velox::connector::hive::HiveConnectorSplitBuilder(dataFilePath)
            .fileFormat(dwio::common::FileFormat::TEXT)
            .serdeParameters({{"field.delim", "|"},
                              {"serialization.null.format", "null"}})
            .build();

    auto pool = memory::memoryManager()->addRootPool();
    auto leafPool = pool->addLeafChild("readResult");
    auto result = exec::test::AssertQueryBuilder(plan)
                      .split(std::move(split))
                      .copyResults(leafPool.get());
    return {result};
  }

  inline static std::shared_ptr<velox::exec::test::TempDirectoryPath>
      gTempDirectory_;

  std::unique_ptr<::axiom::sql::presto::PrestoParser> prestoParser_;
  std::shared_ptr<velox::connector::Connector> connector_;
  connector::hive::LocalHiveConnectorMetadata* metadata_{};
};

TEST_F(TpcdsPlanTest, readResult) {
  auto results = readReferenceResult("test");
  ASSERT_EQ(results.size(), 1);
  auto result = results[0];
  ASSERT_EQ(result->size(), 2);

  // Column 0: INTEGER, value 42, then null.
  auto* intCol = result->childAt(0)->asFlatVector<int32_t>();
  ASSERT_FALSE(intCol->isNullAt(0));
  EXPECT_EQ(intCol->valueAt(0), 42);
  EXPECT_TRUE(intCol->isNullAt(1));

  // Column 1: BIGINT, value 123456789, then null.
  auto* bigintCol = result->childAt(1)->asFlatVector<int64_t>();
  ASSERT_FALSE(bigintCol->isNullAt(0));
  EXPECT_EQ(bigintCol->valueAt(0), 123456789);
  EXPECT_TRUE(bigintCol->isNullAt(1));

  // Column 2: VARCHAR (from VARCHAR), value "hello world", then null.
  auto* varcharCol = result->childAt(2)->asFlatVector<StringView>();
  ASSERT_FALSE(varcharCol->isNullAt(0));
  EXPECT_EQ(varcharCol->valueAt(0).str(), "hello world");
  EXPECT_TRUE(varcharCol->isNullAt(1));

  // Column 3: VARCHAR (from CHAR), value "ABCD", then null.
  auto* charCol = result->childAt(3)->asFlatVector<StringView>();
  ASSERT_FALSE(charCol->isNullAt(0));
  EXPECT_EQ(charCol->valueAt(0).str(), "ABCD");
  EXPECT_TRUE(charCol->isNullAt(1));

  // Column 4: DOUBLE (from DOUBLE), value 3.14, then null.
  auto* doubleCol = result->childAt(4)->asFlatVector<double>();
  ASSERT_FALSE(doubleCol->isNullAt(0));
  EXPECT_DOUBLE_EQ(doubleCol->valueAt(0), 3.14);
  EXPECT_TRUE(doubleCol->isNullAt(1));

  // Column 5: DOUBLE (from DECIMAL), value 99.95, then null.
  auto* decimalCol = result->childAt(5)->asFlatVector<double>();
  ASSERT_FALSE(decimalCol->isNullAt(0));
  EXPECT_DOUBLE_EQ(decimalCol->valueAt(0), 99.95);
  EXPECT_TRUE(decimalCol->isNullAt(1));

  // Column 6: DATE, value 2024-01-15, then null.
  auto* dateCol = result->childAt(6)->asFlatVector<int32_t>();
  ASSERT_FALSE(dateCol->isNullAt(0));
  EXPECT_EQ(dateCol->valueAt(0), DATE()->toDays("2024-01-15"));
  EXPECT_TRUE(dateCol->isNullAt(1));
}

TEST_F(TpcdsPlanTest, q01) {
  auto logicalPlan = parseTpcdsSql("q01");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q02) {
  auto logicalPlan = parseTpcdsSql("q02");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q03) {
  auto logicalPlan = parseTpcdsSql("q03");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q04) {
  auto logicalPlan = parseTpcdsSql("q04");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q05) {
  auto logicalPlan = parseTpcdsSql("q05");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q06) {
  auto logicalPlan = parseTpcdsSql("q06");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q07) {
  auto logicalPlan = parseTpcdsSql("q07");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q08) {
  auto logicalPlan = parseTpcdsSql("q08");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q09) {
  auto logicalPlan = parseTpcdsSql("q09");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q10) {
  auto logicalPlan = parseTpcdsSql("q10");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q11) {
  auto logicalPlan = parseTpcdsSql("q11");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q12) {
  auto logicalPlan = parseTpcdsSql("q12");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q13) {
  auto logicalPlan = parseTpcdsSql("q13");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q14_1) {
  auto logicalPlan = parseTpcdsSql("q14_1");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q14_2) {
  auto logicalPlan = parseTpcdsSql("q14_2");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q15) {
  auto logicalPlan = parseTpcdsSql("q15");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q16) {
  auto logicalPlan = parseTpcdsSql("q16");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q17) {
  auto logicalPlan = parseTpcdsSql("q17");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q18) {
  auto logicalPlan = parseTpcdsSql("q18");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q19) {
  auto logicalPlan = parseTpcdsSql("q19");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q20) {
  auto logicalPlan = parseTpcdsSql("q20");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q21) {
  auto logicalPlan = parseTpcdsSql("q21");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q22) {
  auto logicalPlan = parseTpcdsSql("q22");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q23_1) {
  auto logicalPlan = parseTpcdsSql("q23_1");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q23_2) {
  auto logicalPlan = parseTpcdsSql("q23_2");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q24_1) {
  auto logicalPlan = parseTpcdsSql("q24_1");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q24_2) {
  auto logicalPlan = parseTpcdsSql("q24_2");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q25) {
  auto logicalPlan = parseTpcdsSql("q25");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q26) {
  auto logicalPlan = parseTpcdsSql("q26");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q27) {
  auto logicalPlan = parseTpcdsSql("q27");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q28) {
  auto logicalPlan = parseTpcdsSql("q28");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q29) {
  auto logicalPlan = parseTpcdsSql("q29");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q30) {
  auto logicalPlan = parseTpcdsSql("q30");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q31) {
  auto logicalPlan = parseTpcdsSql("q31");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q32) {
  auto logicalPlan = parseTpcdsSql("q32");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q33) {
  auto logicalPlan = parseTpcdsSql("q33");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q34) {
  auto logicalPlan = parseTpcdsSql("q34");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q35) {
  auto logicalPlan = parseTpcdsSql("q35");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q36) {
  auto logicalPlan = parseTpcdsSql("q36");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q37) {
  auto logicalPlan = parseTpcdsSql("q37");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q38) {
  auto logicalPlan = parseTpcdsSql("q38");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q39_1) {
  auto logicalPlan = parseTpcdsSql("q39_1");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q39_2) {
  auto logicalPlan = parseTpcdsSql("q39_2");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q40) {
  auto logicalPlan = parseTpcdsSql("q40");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q41) {
  auto logicalPlan = parseTpcdsSql("q41");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q42) {
  auto logicalPlan = parseTpcdsSql("q42");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q43) {
  auto logicalPlan = parseTpcdsSql("q43");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q44) {
  auto logicalPlan = parseTpcdsSql("q44");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q45) {
  auto logicalPlan = parseTpcdsSql("q45");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q46) {
  auto logicalPlan = parseTpcdsSql("q46");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q47) {
  auto logicalPlan = parseTpcdsSql("q47");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q48) {
  auto logicalPlan = parseTpcdsSql("q48");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q49) {
  auto logicalPlan = parseTpcdsSql("q49");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q50) {
  auto logicalPlan = parseTpcdsSql("q50");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q51) {
  auto logicalPlan = parseTpcdsSql("q51");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q52) {
  auto logicalPlan = parseTpcdsSql("q52");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q53) {
  auto logicalPlan = parseTpcdsSql("q53");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q54) {
  auto logicalPlan = parseTpcdsSql("q54");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q55) {
  auto logicalPlan = parseTpcdsSql("q55");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q56) {
  auto logicalPlan = parseTpcdsSql("q56");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q57) {
  auto logicalPlan = parseTpcdsSql("q57");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q58) {
  auto logicalPlan = parseTpcdsSql("q58");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q59) {
  auto logicalPlan = parseTpcdsSql("q59");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q60) {
  auto logicalPlan = parseTpcdsSql("q60");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q61) {
  auto logicalPlan = parseTpcdsSql("q61");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q62) {
  auto logicalPlan = parseTpcdsSql("q62");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q63) {
  auto logicalPlan = parseTpcdsSql("q63");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q64) {
  auto logicalPlan = parseTpcdsSql("q64");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q65) {
  auto logicalPlan = parseTpcdsSql("q65");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q66) {
  auto logicalPlan = parseTpcdsSql("q66");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q67) {
  auto logicalPlan = parseTpcdsSql("q67");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q68) {
  auto logicalPlan = parseTpcdsSql("q68");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q69) {
  auto logicalPlan = parseTpcdsSql("q69");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q70) {
  auto logicalPlan = parseTpcdsSql("q70");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q71) {
  auto logicalPlan = parseTpcdsSql("q71");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q72) {
  auto logicalPlan = parseTpcdsSql("q72");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q73) {
  auto logicalPlan = parseTpcdsSql("q73");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q74) {
  auto logicalPlan = parseTpcdsSql("q74");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q75) {
  auto logicalPlan = parseTpcdsSql("q75");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q76) {
  auto logicalPlan = parseTpcdsSql("q76");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q77) {
  auto logicalPlan = parseTpcdsSql("q77");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q78) {
  auto logicalPlan = parseTpcdsSql("q78");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q79) {
  auto logicalPlan = parseTpcdsSql("q79");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q80) {
  auto logicalPlan = parseTpcdsSql("q80");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q81) {
  auto logicalPlan = parseTpcdsSql("q81");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q82) {
  auto logicalPlan = parseTpcdsSql("q82");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q83) {
  auto logicalPlan = parseTpcdsSql("q83");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q84) {
  auto logicalPlan = parseTpcdsSql("q84");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q85) {
  auto logicalPlan = parseTpcdsSql("q85");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q86) {
  auto logicalPlan = parseTpcdsSql("q86");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q87) {
  auto logicalPlan = parseTpcdsSql("q87");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q88) {
  auto logicalPlan = parseTpcdsSql("q88");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q89) {
  auto logicalPlan = parseTpcdsSql("q89");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q90) {
  auto logicalPlan = parseTpcdsSql("q90");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q91) {
  auto logicalPlan = parseTpcdsSql("q91");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q92) {
  auto logicalPlan = parseTpcdsSql("q92");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q93) {
  auto logicalPlan = parseTpcdsSql("q93");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q94) {
  auto logicalPlan = parseTpcdsSql("q94");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q95) {
  auto logicalPlan = parseTpcdsSql("q95");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q96) {
  auto logicalPlan = parseTpcdsSql("q96");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q97) {
  auto logicalPlan = parseTpcdsSql("q97");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q98) {
  auto logicalPlan = parseTpcdsSql("q98");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(TpcdsPlanTest, q99) {
  auto logicalPlan = parseTpcdsSql("q99");
  ASSERT_NO_THROW(planVelox(logicalPlan));
}

} // namespace
} // namespace facebook::axiom::optimizer
