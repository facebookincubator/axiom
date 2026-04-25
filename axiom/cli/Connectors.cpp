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
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/hive/HiveMetadataConfig.h"
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/connectors/system/SystemConnector.h"
#include "axiom/connectors/system/SystemConnectorMetadata.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
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
  explicit SessionConfigPropertiesProvider(const SessionConfig& sessionConfig)
      : sessionConfig_(sessionConfig) {}

  std::vector<connector::system::SessionPropertyInfo> getSessionProperties()
      const override {
    using velox::config::ConfigPropertyTypeName;

    auto entries = sessionConfig_.all();
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
  const SessionConfig& sessionConfig_;
};

} // namespace

Connectors::Connectors() {
  initialize();
}

Connectors::~Connectors() {
  for (const auto& connectorId : connectorIds_) {
    // Unregister metadata first since it may reference the connector.
    connector::ConnectorMetadataRegistry::global().erase(connectorId);
    velox::connector::ConnectorRegistry::global().erase(connectorId);
  }
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

void Connectors::registerConnector(
    const std::shared_ptr<velox::connector::Connector>& connector) {
  connectorIds_.push_back(connector->connectorId());
  velox::connector::ConnectorRegistry::global().insert(
      connector->connectorId(), connector);
}

std::shared_ptr<velox::connector::Connector> Connectors::registerTpchConnector(
    const std::string& connectorId) {
  auto emptyConfig = std::make_shared<velox::config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  velox::connector::tpch::TpchConnectorFactory factory;
  auto connector = factory.newConnector(connectorId, emptyConfig);
  registerConnector(connector);

  auto tpchConnector =
      dynamic_cast<velox::connector::tpch::TpchConnector*>(connector.get());
  VELOX_CHECK_NOT_NULL(tpchConnector);
  connector::ConnectorMetadataRegistry::global().insert(
      connector->connectorId(),
      std::make_shared<connector::tpch::TpchConnectorMetadata>(tpchConnector));

  return connector;
}

std::shared_ptr<velox::connector::Connector>
Connectors::registerLocalHiveConnector(
    const std::string& dataPath,
    const std::string& dataFormat,
    const std::string& connectorId) {
  std::unordered_map<std::string, std::string> connectorConfig = {
      {connector::hive::HiveMetadataConfig::kLocalDataPath, dataPath},
      {connector::hive::HiveMetadataConfig::kLocalFileFormat, dataFormat},
  };

  auto config =
      std::make_shared<velox::config::ConfigBase>(std::move(connectorConfig));

  velox::connector::hive::HiveConnectorFactory factory;
  auto connector = factory.newConnector(connectorId, config, ioExecutor());
  registerConnector(connector);

  auto hiveConnector =
      dynamic_cast<velox::connector::hive::HiveConnector*>(connector.get());
  VELOX_CHECK_NOT_NULL(hiveConnector);
  connector::ConnectorMetadataRegistry::global().insert(
      connector->connectorId(),
      std::make_shared<connector::hive::LocalHiveConnectorMetadata>(
          hiveConnector));

  return connector;
}

std::shared_ptr<velox::connector::Connector> Connectors::registerTestConnector(
    const std::string& connectorId) {
  connector::TestConnectorFactory factory(connectorId.c_str());
  auto connector = factory.newConnector(connectorId);
  registerConnector(connector);
  return connector;
}

void Connectors::registerSystemConnector(
    const SessionConfig& sessionConfig,
    const std::string& connectorId) {
  sessionPropertiesProvider_ =
      std::make_unique<SessionConfigPropertiesProvider>(sessionConfig);

  auto connector = std::make_shared<connector::system::SystemConnector>(
      connectorId,
      /*queryInfoProvider=*/nullptr,
      sessionPropertiesProvider_.get());
  registerConnector(connector);
  connector::ConnectorMetadataRegistry::global().insert(
      connector->connectorId(),
      std::make_shared<connector::system::SystemConnectorMetadata>(
          connector.get()));
}

} // namespace facebook::axiom
