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

/// Enumerates splits for DuckLake tables.
class DuckLakeSplitManager : public ConnectorSplitManager {
 public:
  explicit DuckLakeSplitManager(DuckLakeConnectorMetadata* metadata)
      : metadata_{metadata} {}

  /// Lists the single unpartitioned DuckLake partition for a table scan.
  folly::coro::Task<std::vector<PartitionHandlePtr>> co_listPartitions(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle) override;

  /// Returns a split source over live DuckLake data files.
  std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions,
      SplitOptions options = {}) override;

 private:
  // Provides access to the DuckLake catalog and table layouts.
  DuckLakeConnectorMetadata* metadata_;
};

/// Describes the default DuckLake table layout.
class DuckLakeTableLayout : public hive::HiveTableLayout {
 public:
  DuckLakeTableLayout(
      const std::string& label,
      const Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns,
      std::vector<DuckLakeColumnMetadata> duckLakeColumns,
      std::vector<DuckLakeDataFile> dataFiles);

  /// Returns the live data files for this layout.
  const std::vector<DuckLakeDataFile>& dataFiles() const {
    return dataFiles_;
  }

  /// Creates Hive or Iceberg column handles for this layout.
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

/// Represents a DuckLake table loaded from the catalog.
class DuckLakeTable : public hive::HiveTable {
 public:
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
class DuckLakeConnectorMetadata : public ConnectorMetadata {
 public:
  /// Creates DuckLake metadata from an Iceberg connector configuration.
  explicit DuckLakeConnectorMetadata(
      velox::connector::hive::iceberg::IcebergConnector* icebergConnector);

  /// Creates DuckLake metadata with an injected catalog client.
  DuckLakeConnectorMetadata(
      velox::connector::hive::iceberg::IcebergConnector* icebergConnector,
      std::unique_ptr<DuckLakeCatalogClient> catalog);

  /// Finds a DuckLake table in the current catalog snapshot.
  TablePtr findTable(const SchemaTableName& tableName) override;

  /// Lists schemas in the current DuckLake catalog snapshot.
  std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr& session) override;

  /// Returns true when a DuckLake schema exists in the current snapshot.
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
