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

#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::axiom::optimizer::test {

class HiveQueriesTestBase : public QueryTestBase {
 protected:
  static const inline std::string kDefaultSchema{
      connector::hive::LocalHiveConnectorMetadata::kDefaultSchema};

  /// Creates a temporary data directory and sets 'localDataPath_' and
  /// 'localFileFormat_' (Parquet by default). Registers Parquet reader and
  /// writer. Subclasses should call this, then use createTpchTables() to
  /// populate test tables.
  static void SetUpTestCase();

  /// Sets up the Hive connector with LocalHiveConnectorMetadata, and
  /// initializes the Presto SQL parser.
  void SetUp() override;

  /// Generates TPC-H data for the specified tables using 'localFileFormat_'.
  static void createTpchTables(const std::vector<velox::tpch::Table>& tables);

  /// Unregisters Hive connector metadata.
  void TearDown() override;

  /// Unregisters Parquet reader and writer.
  static void TearDownTestCase();

  static connector::ConnectorSessionPtr makeSession() {
    return std::make_shared<connector::ConnectorSession>(
        /*queryId=*/"test", /*user=*/"test");
  }

  /// Returns a schema of a table.
  velox::RowTypePtr getSchema(std::string_view tableName);

  using QueryTestBase::parseSelect;

  logical_plan::LogicalPlanNodePtr parseSelect(std::string_view sql) {
    return QueryTestBase::parseSelect(sql, velox::exec::test::kHiveConnectorId);
  }

  logical_plan::LogicalPlanNodePtr parseInsert(std::string_view sql);

  using QueryTestBase::toSingleNodePlan;

  velox::core::PlanNodePtr toSingleNodePlan(
      std::string_view sql,
      int32_t numDrivers = 1);

  void checkResults(
      std::string_view sql,
      const velox::core::PlanNodePtr& referencePlan);

  void checkResults(PlanAndStats& plan, const test::TestResult& expected);

  void checkSingleNodePlan(
      const PlanAndStats& plan,
      const std::shared_ptr<velox::core::PlanMatcher>& matcher);

  ::axiom::sql::presto::PrestoParser& prestoParser() {
    return *prestoParser_;
  }

  connector::hive::LocalHiveConnectorMetadata& hiveMetadata() const {
    return *hiveMetadata_;
  }

  /// Hive connector configuration. Entries set before SetUp() are passed to
  /// the connector via setupHiveConnector().
  inline static std::unordered_map<std::string, std::string> hiveConfig_;

  /// The top level directory with the test data.
  inline static std::string localDataPath_;
  inline static velox::dwio::common::FileFormat localFileFormat_{
      velox::dwio::common::FileFormat::DWRF};

  /// Creates an empty table with the given schema and options.
  /// If a table with the same name already exists, it is dropped first.
  void createEmptyTable(
      const std::string& name,
      const velox::RowTypePtr& tableType,
      const folly::F14FastMap<std::string, velox::Variant>& options = {});

  /// Checks that the data in the specified table matches the expected data.
  void checkTableData(
      const std::string& tableName,
      const std::vector<velox::RowVectorPtr>& expectedData);

  /// Creates a table from external files by copying them into the table
  /// directory.
  void createTableFromFiles(
      const std::string& tableName,
      const velox::RowTypePtr& tableType,
      const std::vector<std::string>& filePaths,
      const folly::F14FastMap<std::string, velox::Variant>& options = {});

  /// Parses a CREATE TABLE AS SELECT statement, creates the table, and
  /// populates it with data. Drops the table first if it already exists.
  void runCtas(const std::string& sql);

  static velox::core::PlanMatcherBuilder matchHiveScan(
      const std::string& tableName) {
    return velox::core::PlanMatcherBuilder().tableScan(tableName);
  }

 private:
  // Re-creates the Hive connector using 'localDataPath_' and
  // 'localFileFormat_' and registers LocalHiveConnectorMetadata to provide
  // metadata access to local tables.
  void setupHiveConnector();

  inline static std::shared_ptr<velox::common::testutil::TempDirectoryPath>
      gTempDirectory;

  std::unique_ptr<::axiom::sql::presto::PrestoParser> prestoParser_;
  connector::hive::LocalHiveConnectorMetadata* hiveMetadata_{};
};

} // namespace facebook::axiom::optimizer::test
