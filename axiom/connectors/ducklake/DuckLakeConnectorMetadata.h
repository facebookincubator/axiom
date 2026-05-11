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

#include "axiom/connectors/ducklake/DuckLakeCatalogClient.h"
#include "axiom/connectors/hive/HiveConnectorMetadata.h"
#include "velox/connectors/hive/iceberg/IcebergConnector.h"
#include "velox/dwio/parquet/ParquetFieldId.h"

namespace facebook::axiom::connector::ducklake {

class DuckLakeConnectorMetadata;

/// Enumerates Velox scan splits for DuckLake table layouts.
///
/// DuckLake tables are not exposed as Hive directory partitions. The split
/// manager therefore returns a single logical partition and then expands the
/// live data files from DuckLake metadata into Iceberg-compatible Hive splits.
class DuckLakeSplitManager : public ConnectorSplitManager {
 public:
  /// Creates a split manager backed by non-null DuckLake connector metadata.
  explicit DuckLakeSplitManager(DuckLakeConnectorMetadata* metadata);

  /// Lists the single unpartitioned DuckLake partition for a table scan.
  folly::coro::Task<std::vector<PartitionHandlePtr>> co_listPartitions(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle) override;

  /// Returns a split source over the live DuckLake data files in the layout.
  std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions) override;

 private:
  // Provides access to the DuckLake catalog and table layouts.
  DuckLakeConnectorMetadata* metadata_;
};

/// Describes the default physical layout for a DuckLake table.
///
/// The layout inherits Hive layout behavior so Axiom can reuse existing Hive
/// metadata abstractions, but it creates Iceberg column handles so Velox reads
/// Parquet columns by DuckLake column id instead of by name alone.
class DuckLakeTableLayout : public hive::HiveTableLayout {
 public:
  /// Creates a layout with visible columns, DuckLake field ids, and live files.
  DuckLakeTableLayout(
      const std::string& label,
      const Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns,
      std::vector<DuckLakeColumnMetadata> duckLakeColumns,
      std::vector<DuckLakeDataFile> dataFiles);

  /// Returns the live data files selected from the DuckLake snapshot.
  const std::vector<DuckLakeDataFile>& dataFiles() const {
    return dataFiles_;
  }

  /// Creates Velox column handles for visible and hidden DuckLake columns.
  ///
  /// Visible DuckLake columns use Iceberg column handles carrying Parquet field
  /// ids. Hidden Hive-style columns such as `$path` and `$file_size` use Hive
  /// synthesized column handles.
  velox::connector::ColumnHandlePtr createColumnHandle(
      const ConnectorSessionPtr& session,
      const std::string& columnName,
      std::vector<velox::common::Subfield> subfields = {},
      std::optional<velox::TypePtr> castToType = std::nullopt,
      SubfieldMapping subfieldMapping = {}) const override;

 private:
  // Maps top-level DuckLake column names to their Iceberg field ids.
  folly::F14FastMap<std::string, velox::parquet::ParquetFieldId> fieldIds_;

  // Keeps the live files owned by this layout for split generation.
  std::vector<DuckLakeDataFile> dataFiles_;
};

/// Represents a DuckLake table loaded from catalog metadata.
///
/// The table owns its physical layouts and exposes stable layout pointers to
/// the optimizer through the base ConnectorMetadata APIs.
class DuckLakeTable : public hive::HiveTable {
 public:
  /// Creates a table from snapshot-consistent DuckLake metadata.
  explicit DuckLakeTable(DuckLakeTableMetadata metadata);

  /// Returns the physical layouts for this table.
  const std::vector<const TableLayout*>& layouts() const override {
    return exportedLayouts_;
  }

  /// Adds a DuckLake table layout owned by this table.
  void addLayout(std::unique_ptr<DuckLakeTableLayout> layout);

  /// Returns the table row count from DuckLake metadata.
  uint64_t numRows() const override {
    return numRows_;
  }

 private:
  // Owns the physical layouts for this table.
  std::vector<std::unique_ptr<TableLayout>> layouts_;

  // Exposes stable layout pointers to the optimizer.
  std::vector<const TableLayout*> exportedLayouts_;

  // Stores the row count read from DuckLake table statistics.
  uint64_t numRows_;
};

/// Provides Axiom metadata for DuckLake tables backed by Velox Iceberg reads.
///
/// This class is the bridge between DuckLake catalog metadata and Velox scan
/// execution. It resolves schemas, table definitions, column ids, and live file
/// lists from the catalog client, then presents them through Axiom's generic
/// ConnectorMetadata interface.
class DuckLakeConnectorMetadata : public ConnectorMetadata {
 public:
  /// Creates DuckLake metadata from an Iceberg connector configuration.
  ///
  /// The catalog URL is read from the connector config, parsed as a DuckLake
  /// catalog spec, and opened through DuckLakeCatalogClient.
  explicit DuckLakeConnectorMetadata(
      velox::connector::hive::iceberg::IcebergConnector* icebergConnector);

  /// Creates DuckLake metadata with an injected catalog client.
  ///
  /// This constructor is intended for tests and for future catalog backends
  /// that want to provide a specialized DuckLakeCatalogClient implementation.
  DuckLakeConnectorMetadata(
      velox::connector::hive::iceberg::IcebergConnector* icebergConnector,
      std::unique_ptr<DuckLakeCatalogClient> catalog);

  /// Finds a DuckLake table in the latest catalog snapshot.
  TablePtr findTable(const SchemaTableName& tableName) override;

  /// Lists schemas in the latest DuckLake catalog snapshot.
  std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr& session) override;

  /// Returns true when a DuckLake schema exists in the latest snapshot.
  bool schemaExists(
      const ConnectorSessionPtr& session,
      const std::string& schemaName) override;

  /// Returns the DuckLake split manager.
  ConnectorSplitManager* splitManager() override {
    return &splitManager_;
  }

  /// Returns the Iceberg connector used for Velox scan execution.
  velox::connector::hive::iceberg::IcebergConnector* icebergConnector() const {
    return icebergConnector_;
  }

 private:
  // Creates an immutable Axiom table from DuckLake metadata.
  TablePtr makeTable(DuckLakeTableMetadata tableMetadata) const;

  // Owns split enumeration for this connector.
  DuckLakeSplitManager splitManager_;

  // Points to the Velox connector used to create Iceberg data sources.
  velox::connector::hive::iceberg::IcebergConnector* icebergConnector_;

  // Reads DuckLake metadata from the catalog database.
  std::unique_ptr<DuckLakeCatalogClient> catalog_;
};

} // namespace facebook::axiom::connector::ducklake
