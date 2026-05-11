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

#include "axiom/connectors/ducklake/DuckLakeCatalogClient.h"
#include "axiom/connectors/hive/HiveConnectorMetadata.h"
#include "velox/connectors/hive/iceberg/IcebergConnector.h"
#include "velox/dwio/parquet/ParquetFieldId.h"

namespace facebook::axiom::connector::ducklake {

/// Enumerates Velox scan splits for DuckLake table layouts.
///
/// DuckLake stores partition specs and file partition values in the catalog
/// instead of requiring partition values to appear in directory paths. The
/// split manager reads those catalog rows, exposes matching partition handles
/// to Axiom, and expands selected files into Iceberg-compatible Hive splits.
class DuckLakeSplitManager : public ConnectorSplitManager {
 public:
  /// Creates a split manager backed by shared DuckLake and Iceberg state.
  DuckLakeSplitManager(
      std::shared_ptr<DuckLakeCatalogClient> catalog,
      std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
          icebergConnector);

  /// Lists DuckLake partitions that match partition-column filters.
  folly::coro::Task<std::vector<PartitionHandlePtr>> co_listPartitions(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle) override;

  /// Returns a split source over the live DuckLake data files in the layout.
  std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions) override;

 private:
  // Keeps the catalog client alive while split generation loads table files.
  std::shared_ptr<DuckLakeCatalogClient> catalog_;

  // Keeps the Velox connector alive while split generation creates split
  // sources over Iceberg-compatible files.
  std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
      icebergConnector_;
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
      std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
          icebergConnector,
      std::vector<const Column*> columns,
      std::vector<const Column*> hivePartitionColumns,
      std::vector<DuckLakeColumnMetadata> duckLakeColumns,
      std::vector<DuckLakeDataFile> dataFiles);

  /// Returns the live data files selected from the DuckLake snapshot.
  const std::vector<DuckLakeDataFile>& dataFiles() const {
    return dataFiles_;
  }

  /// Creates Velox column handles for visible and hidden DuckLake columns.
  ///
  /// Visible DuckLake columns use Iceberg column handles carrying Parquet field
  /// ids. Hidden Hive-style columns such as `$path` and `$file_size` use
  /// synthesized Iceberg column handles because Velox routes them from split
  /// metadata instead of Parquet fields.
  velox::connector::ColumnHandlePtr createColumnHandle(
      const ConnectorSessionPtr& session,
      const std::string& columnName,
      std::vector<velox::common::Subfield> subfields = {},
      std::optional<velox::TypePtr> castToType = std::nullopt,
      SubfieldMapping subfieldMapping = {}) const override;

 private:
  // Keeps the Velox connector alive for the borrowed connector pointer stored
  // by the Hive layout base class.
  std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
      icebergConnector_;

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
  /// catalog spec, and opened through DuckLakeCatalogClient. The shared pointer
  /// keeps the Velox connector alive for table layout and split generation.
  explicit DuckLakeConnectorMetadata(
      std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
          icebergConnector);

  /// Creates DuckLake metadata with an injected catalog client.
  ///
  /// This constructor is intended for tests and for future catalog backends
  /// that want to provide a specialized DuckLakeCatalogClient implementation.
  /// The shared pointer keeps the Velox connector alive for table layout and
  /// split generation.
  DuckLakeConnectorMetadata(
      std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
          icebergConnector,
      std::shared_ptr<DuckLakeCatalogClient> catalog);

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
    return splitManager_.get();
  }

  /// Returns shared ownership of the Iceberg connector used for scan execution.
  std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
  icebergConnector() const {
    return icebergConnector_;
  }

 private:
  // Creates an immutable Axiom table from DuckLake metadata.
  TablePtr makeTable(DuckLakeTableMetadata tableMetadata) const;

  // Keeps the Velox connector alive while layouts create Iceberg data sources.
  std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
      icebergConnector_;

  // Reads DuckLake metadata from the catalog database.
  std::shared_ptr<DuckLakeCatalogClient> catalog_;

  // Owns split enumeration for this connector.
  std::unique_ptr<DuckLakeSplitManager> splitManager_;
};

} // namespace facebook::axiom::connector::ducklake
