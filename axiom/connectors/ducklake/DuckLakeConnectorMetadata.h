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
/// The split manager reads live DuckLake data files from the catalog and
/// expands them into Iceberg-compatible Hive splits.
class DuckLakeSplitManager : public ConnectorSplitManager {
 public:
  /// Creates a split manager backed by shared DuckLake and Iceberg state.
  ///
  /// @param catalog Catalog client used to load DuckLake metadata snapshots.
  /// @param icebergConnector Velox Iceberg connector used for Parquet reads.
  DuckLakeSplitManager(
      std::shared_ptr<DuckLakeCatalogClient> catalog,
      std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
          icebergConnector);

  /// Returns the single logical partition used for DuckLake scans.
  ///
  /// @param session Connector session for the query.
  /// @param tableHandle Velox Hive table handle containing the table name.
  /// @returns A single partition handle for the table scan.
  folly::coro::Task<std::vector<PartitionHandlePtr>> co_listPartitions(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle) override;

  /// Returns a split source over the live DuckLake data files in the layout.
  ///
  /// @param session Connector session for the query.
  /// @param tableHandle Velox Hive table handle containing the table name.
  /// @param partitions Partition handles selected by co_listPartitions.
  /// @returns Split source over the table's live DuckLake Parquet files.
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
  ///
  /// @param label Human-readable layout label.
  /// @param table Owning DuckLake table.
  /// @param icebergConnector Velox Iceberg connector used for Parquet reads.
  /// @param columns Columns exposed through this table layout.
  /// @param duckLakeColumns DuckLake columns carrying stable field ids.
  /// @param dataFiles Live data files selected from the DuckLake snapshot.
  DuckLakeTableLayout(
      const std::string& label,
      const Table* table,
      std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
          icebergConnector,
      std::vector<const Column*> columns,
      std::vector<DuckLakeColumn> duckLakeColumns,
      std::vector<DuckLakeDataFile> dataFiles);

  /// Returns the live data files selected from the DuckLake snapshot.
  ///
  /// @returns Data files owned by this layout.
  const std::vector<DuckLakeDataFile>& dataFiles() const {
    return dataFiles_;
  }

  /// Creates Velox column handles for visible and hidden DuckLake columns.
  ///
  /// Visible DuckLake columns use Iceberg column handles carrying Parquet field
  /// ids. Hidden Hive-style columns such as `$path` and `$file_size` use
  /// synthesized Iceberg column handles because Velox routes them from split
  /// metadata instead of Parquet fields.
  ///
  /// @param session Connector session for the query.
  /// @param columnName Column to resolve to a Velox handle.
  /// @param subfields Required subfields for complex column pruning.
  /// @param castToType Optional requested read type. DuckLake does not support
  /// this yet.
  /// @param subfieldMapping Optional mapping for complex-type casts. DuckLake
  /// does not support this yet.
  /// @returns Velox Iceberg column handle for the requested column.
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
  ///
  /// @param metadata DuckLake table metadata for one snapshot.
  explicit DuckLakeTable(DuckLakeTableMetadata metadata);

  /// Returns the physical layouts for this table.
  ///
  /// @returns Layouts owned by this table.
  const std::vector<const TableLayout*>& layouts() const override {
    return exportedLayouts_;
  }

  /// Adds a DuckLake table layout owned by this table.
  ///
  /// @param layout Layout to own and expose through layouts().
  void addLayout(std::unique_ptr<DuckLakeTableLayout> layout);

  /// Returns the table row count from DuckLake metadata.
  ///
  /// @returns Catalog row count, or zero when DuckLake has no table stats.
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
  ///
  /// @param icebergConnector Velox Iceberg connector used for Parquet reads.
  explicit DuckLakeConnectorMetadata(
      std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
          icebergConnector);

  /// Creates DuckLake metadata with an injected catalog client.
  ///
  /// This constructor is intended for tests and for future catalog backends
  /// that want to provide a specialized DuckLakeCatalogClient implementation.
  /// The shared pointer keeps the Velox connector alive for table layout and
  /// split generation.
  ///
  /// @param icebergConnector Velox Iceberg connector used for Parquet reads.
  /// @param catalog DuckLake catalog client to use for metadata reads.
  DuckLakeConnectorMetadata(
      std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
          icebergConnector,
      std::shared_ptr<DuckLakeCatalogClient> catalog);

  /// Finds a DuckLake table in the latest catalog snapshot.
  ///
  /// @param tableName Schema-qualified table name to resolve.
  /// @returns Loaded table, or nullptr when the table does not exist.
  TablePtr findTable(const SchemaTableName& tableName) override;

  /// Lists schemas in the latest DuckLake catalog snapshot.
  ///
  /// @param session Connector session for the query.
  /// @returns Schema names visible in the latest snapshot.
  std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr& session) override;

  /// Returns true when a DuckLake schema exists in the latest snapshot.
  ///
  /// @param session Connector session for the query.
  /// @param schemaName Schema name to look up.
  /// @returns True when the schema exists in the latest snapshot.
  bool schemaExists(
      const ConnectorSessionPtr& session,
      const std::string& schemaName) override;

  /// Returns the DuckLake split manager.
  ///
  /// @returns Split manager owned by this metadata instance.
  ConnectorSplitManager* splitManager() override {
    return splitManager_.get();
  }

  /// Returns shared ownership of the Iceberg connector used for scan execution.
  ///
  /// @returns Velox Iceberg connector used for Parquet reads.
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
