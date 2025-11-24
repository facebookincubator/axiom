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

namespace facebook::axiom::optimizer::test {

class HiveQueriesTestBase : public QueryTestBase {
 protected:
  static void SetUpTestCase();

  /// Creates TPC-H tables in a temp directory using PARQUET file format.
  void SetUp() override;

  void TearDown() override;

  static void TearDownTestCase();

  /// Returns a schema of a TPC-H table.
  velox::RowTypePtr getSchema(std::string_view tableName);

  logical_plan::LogicalPlanNodePtr parseSelect(std::string_view sql);

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

  velox::connector::Connector& hiveConnector() const {
    return *connector_;
  }

  connector::hive::LocalHiveConnectorMetadata& hiveMetadata() const {
    return *metadata_;
  }

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

 private:
  inline static std::shared_ptr<velox::exec::test::TempDirectoryPath>
      gTempDirectory;

  std::unique_ptr<::axiom::sql::presto::PrestoParser> prestoParser_;
  std::shared_ptr<velox::connector::Connector> connector_;
  connector::hive::LocalHiveConnectorMetadata* metadata_{};
};

} // namespace facebook::axiom::optimizer::test
