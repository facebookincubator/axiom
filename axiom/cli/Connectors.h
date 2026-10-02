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

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <folly/container/F14Map.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/system/SystemConnector.h"
#include "folly/executors/IOThreadPoolExecutor.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/ConnectorRegistry.h"

namespace facebook::axiom {

class SessionConfig;

/// Registers catalogs into one engine's connector and metadata registries.
///
/// Example:
///   auto connectors = Connectors(
///       velox::connector::ConnectorRegistry::create(),
///       connector::ConnectorMetadataRegistry::create());
///   connectors.registerTpchConnector();
///
/// Both registries must be non-null and must outlive all queries that use the
/// registered catalogs. A catalog's connector and metadata are installed as a
/// pair unless the catalog intentionally provides metadata only.
class Connectors {
 public:
  static constexpr const char* kTpchConnectorId = "tpch";
  static constexpr const char* kLocalHiveConnectorId = "hive";
  static constexpr const char* kTestConnectorId = "test";
  static constexpr const char* kSystemConnectorId = "system";

  Connectors(
      std::shared_ptr<velox::connector::ConnectorRegistry::Registry>
          connectorRegistry,
      std::shared_ptr<connector::ConnectorMetadataRegistry::Registry>
          metadataRegistry);

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
  /// reference this connector in queries.
  std::shared_ptr<velox::connector::Connector> registerConnector(
      std::string_view connectorName,
      const folly::F14FastMap<std::string, std::string>& connectorConfig,
      const std::string& connectorId);

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
      std::shared_ptr<const SessionConfig> sessionConfig,
      const std::string& connectorId = kSystemConnectorId);

  /// Registers the file connector for querying raw files via SQL.
  void registerFileConnector(const std::string& connectorId = "file");

  /// Registers an execution connector and its metadata as one catalog. If
  /// metadata registration fails, the connector insertion is rolled back.
  void registerCatalog(
      std::shared_ptr<velox::connector::Connector> connector,
      std::shared_ptr<connector::ConnectorMetadata> metadata);

  /// Registers an execution connector and its metadata into the supplied
  /// registries as one catalog. Application roots that create connectors
  /// directly use this overload to preserve the same rollback contract.
  static void registerCatalog(
      velox::connector::ConnectorRegistry::Registry& connectorRegistry,
      connector::ConnectorMetadataRegistry::Registry& metadataRegistry,
      std::shared_ptr<velox::connector::Connector> connector,
      std::shared_ptr<connector::ConnectorMetadata> metadata);

  /// Registers metadata for a catalog that has no execution connector, such
  /// as Capella's user-defined types and SQL functions.
  void registerMetadataCatalog(
      std::string connectorId,
      std::shared_ptr<connector::ConnectorMetadata> metadata);

 protected:
  /// Initialize file formats and ioExecutor. Must be called before
  /// registering any connectors.
  void initialize();

  /// Returns ioExecutor_ if initialized, otherwise nullptr.
  folly::Executor* ioExecutor() {
    return ioExecutor_.get();
  }

 private:
  // Returns the process-shared I/O executor retained by each instance.
  static std::shared_ptr<folly::IOThreadPoolExecutor> getSharedIoExecutor();
  // Retains the executor used by connectors created through this instance.
  std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor_;

  // Registry that receives execution connectors for this engine.
  const std::shared_ptr<velox::connector::ConnectorRegistry::Registry>
      connectorRegistry_;
  // Registry that receives connector metadata for this engine.
  const std::shared_ptr<connector::ConnectorMetadataRegistry::Registry>
      metadataRegistry_;
};

} // namespace facebook::axiom
