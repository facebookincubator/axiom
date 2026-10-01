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

#include "axiom/connectors/ConnectorEnvironment.h"
#include "axiom/connectors/system/SystemConnector.h"
#include "folly/executors/IOThreadPoolExecutor.h"
#include "velox/connectors/Connector.h"

namespace facebook::axiom {

class SessionConfig;

/// Registers standard connectors through one ConnectorEnvironment builder.
///
///   auto builder = connector::ConnectorEnvironment::Builder::create();
///   Connectors connectors{builder};
///   connectors.registerTpchConnector();
///   auto environment = builder->build();
///
/// Invariants:
///   - Exactly one builder or process-wide global environment is non-null.
///   - The builder remains incomplete while catalogs are registered.
class Connectors {
 public:
  static constexpr const char* kTpchConnectorId = "tpch";
  static constexpr const char* kLocalHiveConnectorId = "hive";
  static constexpr const char* kTestConnectorId = "test";
  static constexpr const char* kSystemConnectorId = "system";

  /// Registers connectors into `builder` before it produces an environment.
  explicit Connectors(
      std::shared_ptr<connector::ConnectorEnvironment::Builder> builder);

  /// Registers connectors into the process-wide global environment.
  explicit Connectors(
      std::shared_ptr<connector::ConnectorEnvironment> environment);

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

 protected:
  /// Initialize file formats and ioExecutor. Must be called before
  /// registering any connectors.
  void initialize();

  /// Returns ioExecutor_ if initialized, otherwise nullptr.
  folly::Executor* ioExecutor() {
    return ioExecutor_.get();
  }

  /// Registers an execution connector and its metadata as one catalog.
  void registerConnector(
      std::shared_ptr<velox::connector::Connector> connector,
      std::shared_ptr<connector::ConnectorMetadata> metadata);

  /// Registers metadata for a catalog that has no execution connector.
  void registerMetadata(
      std::string connectorId,
      std::shared_ptr<connector::ConnectorMetadata> metadata);

  /// Returns the metadata registry receiving catalog registrations.
  const connector::ConnectorMetadataRegistry::Registry& metadataRegistry()
      const;

 private:
  static std::shared_ptr<folly::IOThreadPoolExecutor> getSharedIoExecutor();
  std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor_;

  // Owns mutable registration state for an isolated environment.
  const std::shared_ptr<connector::ConnectorEnvironment::Builder> builder_;

  // Identifies the process-wide registration destination.
  const std::shared_ptr<connector::ConnectorEnvironment>
      processWideEnvironment_;
};

} // namespace facebook::axiom
