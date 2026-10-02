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

#include "axiom/cli/Connectors.h"

#include <folly/system/HardwareConcurrency.h>
#include "axiom/common/SessionConfig.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/file/FileConnector.h"
#include "axiom/connectors/file/core/FileConnectorMetadata.h"
#include "axiom/connectors/file/parquet/ParquetFileHandler.h"
#include "axiom/connectors/hive/HiveMetadataConfig.h"
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/connectors/system/SystemConnector.h"
#include "axiom/connectors/system/SystemConnectorMetadata.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include "velox/common/base/Exceptions.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/dwio/text/RegisterTextReader.h"
#include "velox/dwio/text/RegisterTextWriter.h"
#include "velox/functions/prestosql/types/PrestoTypes.h"

namespace facebook::axiom {

namespace {

void initializeFileFormats() {
  velox::dwio::common::registerFileSinks();
  velox::parquet::registerParquetReaderFactory();
  velox::parquet::registerParquetWriterFactory();
  velox::dwrf::registerDwrfReaderFactory();
  velox::dwrf::registerDwrfWriterFactory();
  velox::text::registerTextReaderFactory();
  velox::text::registerTextWriterFactory();
}

// Adapts SessionConfig to the SessionPropertiesProvider interface for the
// system connector's metadata.session_properties table.
class SessionConfigPropertiesProvider
    : public connector::system::SessionPropertiesProvider {
 public:
  explicit SessionConfigPropertiesProvider(
      std::shared_ptr<const SessionConfig> sessionConfig)
      : sessionConfig_{std::move(sessionConfig)} {
    VELOX_CHECK_NOT_NULL(sessionConfig_);
  }

  std::vector<connector::system::SessionPropertyInfo> getSessionProperties()
      const override {
    using velox::config::ConfigPropertyTypeName;

    auto entries = sessionConfig_->all();
    std::vector<connector::system::SessionPropertyInfo> result;
    result.reserve(entries.size());
    for (const auto& entry : entries) {
      result.push_back({
          entry.prefix,
          entry.property.name,
          std::string(ConfigPropertyTypeName::toName(entry.property.type)),
          entry.property.defaultValue.value_or(""),
          entry.currentValue.value_or(""),
          entry.property.description,
      });
    }
    return result;
  }

 private:
  const std::shared_ptr<const SessionConfig> sessionConfig_;
};

} // namespace

Connectors::Connectors(
    std::shared_ptr<velox::connector::ConnectorRegistry::Registry>
        connectorRegistry,
    std::shared_ptr<connector::ConnectorMetadataRegistry::Registry>
        metadataRegistry)
    : connectorRegistry_{std::move(connectorRegistry)},
      metadataRegistry_{std::move(metadataRegistry)} {
  VELOX_CHECK_NOT_NULL(connectorRegistry_);
  VELOX_CHECK_NOT_NULL(metadataRegistry_);
  initialize();
}

// static
std::shared_ptr<folly::IOThreadPoolExecutor> Connectors::getSharedIoExecutor() {
  static auto executor = std::make_shared<folly::IOThreadPoolExecutor>(
      folly::available_concurrency(),
      std::make_shared<folly::NamedThreadFactory>("io"));
  return executor;
}

void Connectors::initialize() {
  static folly::once_flag kInitialized;
  folly::call_once(kInitialized, []() { initializeFileFormats(); });
  // Every instance gets a shared_ptr to the singleton executor.
  ioExecutor_ = getSharedIoExecutor();
}

void Connectors::registerCatalog(
    std::shared_ptr<velox::connector::Connector> connector,
    std::shared_ptr<connector::ConnectorMetadata> metadata) {
  registerCatalog(
      *connectorRegistry_,
      *metadataRegistry_,
      std::move(connector),
      std::move(metadata));
}

// static
void Connectors::registerCatalog(
    velox::connector::ConnectorRegistry::Registry& connectorRegistry,
    connector::ConnectorMetadataRegistry::Registry& metadataRegistry,
    std::shared_ptr<velox::connector::Connector> connector,
    std::shared_ptr<connector::ConnectorMetadata> metadata) {
  VELOX_CHECK_NOT_NULL(connector);
  VELOX_CHECK_NOT_NULL(metadata);
  const auto connectorId = connector->connectorId();
  connectorRegistry.insert(connectorId, std::move(connector));
  try {
    metadataRegistry.insert(connectorId, std::move(metadata));
  } catch (...) {
    connectorRegistry.erase(connectorId);
    throw;
  }
}

void Connectors::registerMetadataCatalog(
    std::string connectorId,
    std::shared_ptr<connector::ConnectorMetadata> metadata) {
  VELOX_CHECK_NOT_NULL(metadata);
  metadataRegistry_->insert(std::move(connectorId), std::move(metadata));
}

std::shared_ptr<velox::connector::Connector> Connectors::registerTpchConnector(
    const std::string& connectorId) {
  auto emptyConfig = std::make_shared<velox::config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  velox::connector::tpch::TpchConnectorFactory factory;
  auto connector = factory.newConnector(connectorId, emptyConfig);

  auto tpchConnector =
      dynamic_cast<velox::connector::tpch::TpchConnector*>(connector.get());
  VELOX_CHECK_NOT_NULL(tpchConnector);
  registerCatalog(
      connector,
      std::make_shared<connector::tpch::TpchConnectorMetadata>(tpchConnector));

  return connector;
}

std::shared_ptr<velox::connector::Connector>
Connectors::registerLocalHiveConnector(
    const std::string& dataPath,
    const std::string& dataFormat,
    const std::string& connectorId,
    std::shared_ptr<velox::memory::MemoryPool> rootPool) {
  std::unordered_map<std::string, std::string> connectorConfig = {
      {connector::hive::HiveMetadataConfig::kLocalDataPath, dataPath},
      {connector::hive::HiveMetadataConfig::kLocalFileFormat, dataFormat},
  };

  auto configBase =
      std::make_shared<velox::config::ConfigBase>(std::move(connectorConfig));

  velox::connector::hive::HiveConnectorFactory factory;
  auto connector = factory.newConnector(connectorId, configBase, ioExecutor());

  auto hiveConnector =
      dynamic_cast<velox::connector::hive::HiveConnector*>(connector.get());
  VELOX_CHECK_NOT_NULL(hiveConnector);
  registerCatalog(
      connector,
      std::make_shared<connector::hive::LocalHiveConnectorMetadata>(
          hiveConnector,
          rootPool ? std::move(rootPool)
                   : velox::memory::memoryManager()->addRootPool()));

  return connector;
}

std::shared_ptr<velox::connector::Connector> Connectors::registerConnector(
    std::string_view connectorName,
    const folly::F14FastMap<std::string, std::string>& connectorConfig,
    const std::string& connectorId) {
  if (connectorName == "tpch") {
    return registerTpchConnector(connectorId);
  }

  if (connectorName == "hive") {
    auto dataPathIterator = connectorConfig.find(
        connector::hive::HiveMetadataConfig::kLocalDataPath);
    VELOX_USER_CHECK(
        dataPathIterator != connectorConfig.end(),
        "Hive catalog config is missing required property {}: {}",
        connector::hive::HiveMetadataConfig::kLocalDataPath,
        connectorId);

    auto fileFormatIterator = connectorConfig.find(
        connector::hive::HiveMetadataConfig::kLocalFileFormat);
    const std::string fileFormat = fileFormatIterator == connectorConfig.end()
        ? "parquet"
        : fileFormatIterator->second;

    return registerLocalHiveConnector(
        dataPathIterator->second, fileFormat, connectorId);
  }

  if (connectorName == "test") {
    return registerTestConnector(connectorId, connectorConfig);
  }

  VELOX_USER_FAIL(
      "Unsupported connector.name in catalog config: {} for {}",
      connectorName,
      connectorId);
}

std::shared_ptr<velox::connector::Connector> Connectors::registerTestConnector(
    const std::string& connectorId,
    const folly::F14FastMap<std::string, std::string>& connectorConfig) {
  auto config = std::make_shared<velox::config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          connectorConfig.begin(), connectorConfig.end()});

  connector::TestConnectorFactory factory(connectorId.c_str());
  auto connector = factory.newConnector(connectorId, std::move(config));

  auto* testConnector =
      dynamic_cast<connector::TestConnector*>(connector.get());
  VELOX_CHECK_NOT_NULL(testConnector);
  registerCatalog(connector, testConnector->metadata());

  return connector;
}

void Connectors::registerSystemConnector(
    std::shared_ptr<const SessionConfig> sessionConfig,
    const std::string& connectorId) {
  auto sessionPropertiesProvider =
      std::make_shared<SessionConfigPropertiesProvider>(
          std::move(sessionConfig));

  // The CLI speaks Presto SQL, so information_schema spells types the way
  // Presto does.
  auto connector = std::make_shared<connector::system::SystemConnector>(
      connectorId,
      /*queryInfoProvider=*/nullptr,
      std::move(sessionPropertiesProvider),
      velox::PrestoTypes::displayName,
      *metadataRegistry_);
  registerCatalog(
      connector,
      std::make_shared<connector::system::SystemConnectorMetadata>(
          connector.get(), *metadataRegistry_));
}

void Connectors::registerFileConnector(const std::string& connectorId) {
  connector::file::registerParquetHandler();

  auto connector =
      std::make_shared<connector::file::FileConnector>(connectorId);
  registerCatalog(
      connector,
      std::make_shared<connector::file::FileConnectorMetadata>(
          connector.get()));
}

} // namespace facebook::axiom
