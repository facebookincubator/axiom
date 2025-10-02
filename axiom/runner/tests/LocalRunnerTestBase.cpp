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

#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"

namespace facebook::axiom::runner::test {

void LocalRunnerTestBase::SetUp() {
  HiveConnectorTestBase::SetUp();

  velox::parquet::registerParquetReaderFactory();

  velox::exec::ExchangeSource::factories().clear();
  velox::exec::ExchangeSource::registerFactory(
      velox::exec::test::createLocalExchangeSource);

  if (!files_) {
    makeTables();
  }
  // Destroy and rebuild the testing connector. The connector will
  // show the metadata if the connector is wired for metadata.
  setupConnector();
}

void LocalRunnerTestBase::TearDown() {
  connector::ConnectorMetadata::unregisterMetadata(
      velox::exec::test::kHiveConnectorId);
  velox::exec::ExchangeSource::factories().clear();
  velox::parquet::unregisterParquetReaderFactory();
  HiveConnectorTestBase::TearDown();
}

std::shared_ptr<velox::core::QueryCtx> LocalRunnerTestBase::makeQueryCtx(
    const std::string& queryId) {
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      connectorConfigs;
  connectorConfigs[velox::exec::test::kHiveConnectorId] =
      std::make_shared<velox::config::ConfigBase>(folly::copy(hiveConfig_));

  return velox::core::QueryCtx::create(
      schemaExecutor_.get(),
      velox::core::QueryConfig(folly::copy(config_)),
      std::move(connectorConfigs),
      velox::cache::AsyncDataCache::getInstance(),
      /*pool=*/nullptr,
      /*spillExecutor=*/nullptr,
      queryId);
}

void LocalRunnerTestBase::setupConnector() {
  std::unordered_map<std::string, std::string> configs;
  configs[velox::connector::hive::HiveConfig::kLocalDataPath] = localDataPath_;
  configs[velox::connector::hive::HiveConfig::kLocalFileFormat] =
      velox::dwio::common::toString(localFileFormat_);

  resetHiveConnector(
      std::make_shared<velox::config::ConfigBase>(std::move(configs)));

  auto hiveConnector =
      velox::connector::getConnector(velox::exec::test::kHiveConnectorId);

  connector::ConnectorMetadata::registerMetadata(
      velox::exec::test::kHiveConnectorId,
      std::make_shared<connector::hive::LocalHiveConnectorMetadata>(
          dynamic_cast<velox::connector::hive::HiveConnector*>(
              hiveConnector.get())));
}

void LocalRunnerTestBase::makeTables() {
  if (initialized_) {
    return;
  }
  initialized_ = true;

  if (localDataPath_.empty()) {
    files_ = velox::exec::test::TempDirectoryPath::create();
    localDataPath_ = files_->getPath();
  }

  auto fs = velox::filesystems::getFileSystem(localDataPath_, {});
  for (const auto& spec : testTables_) {
    const auto tablePath = fmt::format("{}/{}", localDataPath_, spec.name);
    fs->mkdir(tablePath);
    for (auto i = 0; i < spec.numFiles; ++i) {
      auto vectors = HiveConnectorTestBase::makeVectors(
          spec.columns, spec.numVectorsPerFile, spec.rowsPerVector);
      if (spec.customizeData) {
        for (const auto& vector : vectors) {
          spec.customizeData(vector);
        }
      }
      auto filePath = fmt::format("{}/f{}", tablePath, i);
      tableFilePaths_[spec.name].push_back(filePath);
      writeToFile(filePath, vectors);
    }
  }
}

void LocalRunnerTestBase::tablesCreated() {
  auto hiveConnector =
      velox::connector::getConnector(velox::exec::test::kHiveConnectorId);

  auto metadata = dynamic_cast<connector::hive::LocalHiveConnectorMetadata*>(
      connector::ConnectorMetadata::metadata(hiveConnector.get()));
  VELOX_CHECK_NOT_NULL(metadata);
  metadata->reinitialize();
}

// static
std::vector<velox::RowVectorPtr> LocalRunnerTestBase::readCursor(
    const std::shared_ptr<LocalRunner>& runner) {
  std::vector<velox::RowVectorPtr> result;

  while (auto rowVector = runner->next()) {
    result.push_back(rowVector);
  }
  return result;
}

} // namespace facebook::axiom::runner::test
