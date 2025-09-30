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

#include "axiom/runner/LocalRunner.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace facebook::axiom::runner::test {

struct TableSpec {
  std::string name;
  velox::RowTypePtr columns;
  int32_t rowsPerVector{10'000};
  int32_t numVectorsPerFile{5};
  int32_t numFiles{5};

  /// Function applied to generated RowVectors for the table before writing.
  /// May be used to insert non-random data on top of the random data from
  /// HiveConnectorTestBase::makeVectors.
  std::function<void(const velox::RowVectorPtr& vector)> customizeData;
};

/// Test helper class that manages a TestCase with a set of generated
/// tables and a HiveConnector that exposes the files and their
/// metadata. The lifetime of the test data is the test case consisting
/// of multiple Google unit test cases.
class LocalRunnerTestBase : public velox::exec::test::HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    HiveConnectorTestBase::SetUpTestCase();
    schemaExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(4);
  }

  static void TearDownTestCase() {
    initialized_ = false;
    files_.reset();
    HiveConnectorTestBase::TearDownTestCase();
  }

  /// Creates test tables with randomly-generated data using 'testTables_'
  /// specs. Writes tables to 'localDataPath_' in 'localFileFormat_' format. If
  /// 'localDataPath_' is not set, creates a temp directory. Initializes
  /// LocalHiveConnectorMetadata to provide metadata access to newly created
  /// tables.
  void SetUp() override;

  void TearDown() override;

  /// Reads 'localDataPath_' directory and picks up new tables.
  void tablesCreated();

  /// Creates a QueryCtx with 'pool'. 'pool' must be a root pool.
  std::shared_ptr<velox::core::QueryCtx> makeQueryCtx(
      const std::string& queryId);

  /// Fetch all remaining data from the 'runner'. Calls LocalRunner::next() in a
  /// loop until it returns nullptr.
  static std::vector<velox::RowVectorPtr> readCursor(
      const std::shared_ptr<LocalRunner>& runner);

  /// Configs for creating QueryCtx. Must be set before calling
  /// 'makeQueryCtx()'.
  inline static std::unordered_map<std::string, std::string> config_;
  inline static std::unordered_map<std::string, std::string> hiveConfig_;

  /// The specification of the test data. The data is created in
  /// ensureTestData() called from each SetUp()(.
  inline static std::vector<TableSpec> testTables_;

  /// The top level directory with the test data.
  inline static std::string localDataPath_;
  inline static velox::dwio::common::FileFormat localFileFormat_{
      velox::dwio::common::FileFormat::DWRF};

 private:
  void makeTables();

  // Re-creates the connector with kHiveConnectorId with a config
  // that points to the temp directory created by 'this'. If the
  // connector factory is wired to capture metadata then the metadata
  // will be available through the connector.
  void setupConnector();

  inline static bool initialized_;
  inline static std::shared_ptr<velox::exec::test::TempDirectoryPath> files_;
  /// Map from table name to list of file system paths.
  inline static std::unordered_map<std::string, std::vector<std::string>>
      tableFilePaths_;
  inline static std::unique_ptr<folly::CPUThreadPoolExecutor> schemaExecutor_;
};

} // namespace facebook::axiom::runner::test
