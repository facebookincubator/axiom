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

#include <folly/container/F14Map.h>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "axiom/cli/CatalogProperties.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/system/SystemConnector.h"
#include "folly/executors/IOThreadPoolExecutor.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/ConnectorRegistry.h"

namespace facebook::axiom {

class SessionConfig;

/**
 * Populates application-owned Axiom and Velox connector registries.
 *
 * Registers catalogs for the TPC-H, local Hive and test connectors, either
 * one at a time or from catalog descriptions such as `--etc_dir` files.
 * Subclasses add connector names by overriding registerConnector(). Catalogs
 * remain in the registries after this helper is destroyed.
 *
 * @code
 * auto connectorRegistry = velox::connector::ConnectorRegistry::create();
 * auto metadataRegistry = connector::ConnectorMetadataRegistry::create();
 * Connectors connectors{*connectorRegistry, *metadataRegistry};
 * connectors.registerCatalogs(
 *     ::axiom::sql::loadCatalogProperties(etcDir),
 *     {{.catalogName = "tpch", .connectorName = "tpch"}});
 * @endcode
 */
class Connectors {
 public:
  static constexpr const char* kTpchConnectorId = "tpch";
  static constexpr const char* kLocalHiveConnectorId = "hive";
  static constexpr const char* kTestConnectorId = "test";
  static constexpr const char* kSystemConnectorId = "system";

  /// Registers catalogs into the supplied registries, which must outlive this
  /// helper.
  Connectors(
      velox::connector::ConnectorRegistry::Registry& connectorRegistry,
      connector::ConnectorMetadataRegistry::Registry& metadataRegistry);

  Connectors(const Connectors&) = delete;
  Connectors& operator=(const Connectors&) = delete;
  Connectors(Connectors&&) = default;
  Connectors& operator=(Connectors&&) = default;

  virtual ~Connectors() = default;

  /// Registers the TPCH connector under the connector ID "tpch".
  /// This allows queries like "select * from tpch.sf1.lineitem".
  std::shared_ptr<velox::connector::Connector> registerTpchConnector(
      const std::string& connectorId = kTpchConnectorId);

  /// Registers the connector for tables stored in the local filesystem under
  /// `dataPath`. Table "foo" will be stored as files in format `dataFormat` in
  /// the directory `${dataPath}/foo`. Allowed formats are "parquet", "dwrf",
  /// and "text". "text" is a simple text format with one row per line, with
  /// 0x01 as a column separator.
  ///
  /// The connector is registered under `connectorId`, so queries can access
  /// local tables like "INSERT INTO ${connectorId}.write_table SELECT * FROM
  /// ${connectorId}.read_table".
  /// 'rootPool' backs the connector's LocalHiveConnectorMetadata; defaults to a
  /// root pool from the global MemoryManager. Tests with a standalone
  /// MemoryManager pass one from it.
  std::shared_ptr<velox::connector::Connector> registerLocalHiveConnector(
      const std::string& dataPath,
      const std::string& dataFormat,
      const std::string& connectorId = kLocalHiveConnectorId,
      std::shared_ptr<velox::memory::MemoryPool> rootPool = nullptr);

  /// Registers a connector from configuration properties.
  ///
  /// Delegates to specific registration helpers for each supported connector
  /// implementation. For example:
  /// - `tpch` uses registerTpchConnector
  /// - `hive` uses registerLocalHiveConnector
  /// - `test` uses registerTestConnector
  ///
  /// The `connectorConfig` provides connector-specific properties; for Hive,
  /// this typically includes hive_local_data_path and optionally
  /// hive_local_file_format. The `connectorId` is the catalog name used to
  /// reference this connector in queries. Returns the registered connector, or
  /// null for a catalog that registers metadata only.
  ///
  /// Subclasses override this to support more connector names and call it for
  /// the rest.
  virtual std::shared_ptr<velox::connector::Connector> registerConnector(
      std::string_view connectorName,
      const folly::F14FastMap<std::string, std::string>& connectorConfig,
      const std::string& connectorId);

  /// Registers each of `catalogs` through registerConnector(), then each of
  /// `builtInCatalogs` whose name is absent from `catalogs`. A catalog in
  /// `catalogs` thus replaces the built-in catalog of the same name. Returns
  /// the registered catalogs, `catalogs` first.
  std::vector<::axiom::sql::CatalogProperties> registerCatalogs(
      std::vector<::axiom::sql::CatalogProperties> catalogs,
      const std::vector<::axiom::sql::CatalogProperties>& builtInCatalogs);

  /// Registers an in-memory test connector under `connectorId`. The
  /// `connectorConfig` is passed through to the connector verbatim; the test
  /// connector interprets its own properties (e.g. `tables` naming JSON files
  /// of table schemas and statistics to preload).
  std::shared_ptr<velox::connector::Connector> registerTestConnector(
      const std::string& connectorId = kTestConnectorId,
      const folly::F14FastMap<std::string, std::string>& connectorConfig = {});

  /// Registers the system connector for the runtime.queries and
  /// metadata.session_properties tables.
  void registerSystemConnector(
      const SessionConfig& sessionConfig,
      const std::string& connectorId = kSystemConnectorId);

  /// Registers the file connector for querying raw files via SQL.
  void registerFileConnector(const std::string& connectorId = "file");

 protected:
  /// Initialize file formats and ioExecutor. Must be called before
  /// registering any connectors.
  void initialize();

  /// Returns ioExecutor_ if initialized, otherwise nullptr.
  folly::Executor* ioExecutor() {
    return ioExecutor_.get();
  }

  velox::connector::ConnectorRegistry::Registry& connectorRegistry() {
    return *connectorRegistry_;
  }

  connector::ConnectorMetadataRegistry::Registry& metadataRegistry() {
    return *metadataRegistry_;
  }

 private:
  static std::shared_ptr<folly::IOThreadPoolExecutor> getSharedIoExecutor();
  std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor_;

  // Registry this helper registers execution connectors into. The caller owns
  // it.
  velox::connector::ConnectorRegistry::Registry* connectorRegistry_;

  // Registry this helper registers metadata into. The caller owns it.
  connector::ConnectorMetadataRegistry::Registry* metadataRegistry_;
};

} // namespace facebook::axiom
