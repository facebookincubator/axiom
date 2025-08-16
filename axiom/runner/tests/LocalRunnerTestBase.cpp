/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"

namespace facebook::axiom::runner::test {

void LocalRunnerTestBase::SetUp() {
  HiveConnectorTestBase::SetUp();
  velox::exec::ExchangeSource::factories().clear();
  velox::exec::ExchangeSource::registerFactory(
      velox::exec::test::createLocalExchangeSource);
  ensureTestData();
}

std::shared_ptr<velox::core::QueryCtx> LocalRunnerTestBase::makeQueryCtx(
    const std::string& queryId,
    velox::memory::MemoryPool* rootPool) {
  auto& config = config_;
  auto hiveConfig = hiveConfig_;
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      connectorConfigs;
  connectorConfigs[velox::exec::test::kHiveConnectorId] =
      std::make_shared<velox::config::ConfigBase>(std::move(hiveConfig));

  return velox::core::QueryCtx::create(
      schemaExecutor_.get(),
      velox::core::QueryConfig(config),
      std::move(connectorConfigs),
      velox::cache::AsyncDataCache::getInstance(),
      rootPool->shared_from_this(),
      nullptr,
      queryId);
}

void LocalRunnerTestBase::ensureTestData() {
  if (!files_) {
    makeTables(testTables_, files_);
  }
  // Destroy and rebuild the testing connector. The connector will
  // show the metadata if the connector is wired for metadata.
  setupConnector();
}

void LocalRunnerTestBase::setupConnector() {
  velox::connector::unregisterConnector(velox::exec::test::kHiveConnectorId);

  std::unordered_map<std::string, std::string> configs;
  configs[velox::connector::hive::HiveConfig::kLocalDataPath] = testDataPath_;
  configs[velox::connector::hive::HiveConfig::kLocalFileFormat] =
      localFileFormat_;
  auto hiveConnector =
      velox::connector::getConnectorFactory(
          velox::connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              velox::exec::test::kHiveConnectorId,
              std::make_shared<velox::config::ConfigBase>(std::move(configs)),
              ioExecutor_.get());
  velox::connector::registerConnector(hiveConnector);
}

void LocalRunnerTestBase::makeTables(
    std::vector<TableSpec> specs,
    std::shared_ptr<velox::exec::test::TempDirectoryPath>& directory) {
  if (initialized_) {
    return;
  }
  initialized_ = true;
  if (testDataPath_.empty()) {
    directory = velox::exec::test::TempDirectoryPath::create();
    testDataPath_ = directory->getPath();
  }
  for (auto& spec : specs) {
    auto tablePath = fmt::format("{}/{}", testDataPath_, spec.name);
    auto fs = velox::filesystems::getFileSystem(tablePath, {});
    fs->mkdir(tablePath);
    for (auto i = 0; i < spec.numFiles; ++i) {
      auto vectors = HiveConnectorTestBase::makeVectors(
          spec.columns, spec.numVectorsPerFile, spec.rowsPerVector);
      if (spec.customizeData) {
        for (auto& vector : vectors) {
          spec.customizeData(vector);
        }
      }
      auto filePath = fmt::format("{}/f{}", tablePath, i);
      tableFilePaths_[spec.name].push_back(filePath);
      writeToFile(filePath, vectors);
    }
  }
}

std::shared_ptr<runner::SimpleSplitSourceFactory>
LocalRunnerTestBase::makeSimpleSplitSourceFactory(
    const runner::MultiFragmentPlanPtr& plan) {
  std::unordered_map<
      velox::core::PlanNodeId,
      std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>>
      nodeSplitMap;
  for (auto& fragment : plan->fragments()) {
    for (auto& scan : fragment.scans) {
      auto& name = scan->tableHandle()->name();
      auto files = tableFilePaths_[name];
      VELOX_CHECK(!files.empty(), "No splits known for {}", name);
      std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits;
      for (auto& file : files) {
        splits.push_back(velox::connector::hive::HiveConnectorSplitBuilder(file)
                             .connectorId(velox::exec::test::kHiveConnectorId)
                             .fileFormat(velox::dwio::common::FileFormat::DWRF)
                             .build());
      }
      nodeSplitMap[scan->id()] = std::move(splits);
    }
  };
  return std::make_shared<runner::SimpleSplitSourceFactory>(
      std::move(nodeSplitMap));
}

std::vector<velox::RowVectorPtr> readCursor(
    std::shared_ptr<runner::LocalRunner> runner) {
  // 'result' borrows memory from cursor so the life cycle must be shorter.
  std::vector<velox::RowVectorPtr> result;

  while (auto rows = runner->next()) {
    result.push_back(rows);
  }
  return result;
}

} // namespace facebook::axiom::runner::test
