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
#include <optional>
#include <string>
#include <vector>

#include "axiom/common/SchemaTableName.h"
#include "axiom/connectors/ducklake/DuckLakeMetadataConfig.h"
#include "velox/type/Type.h"

namespace facebook::axiom::connector::ducklake {

/// Describes a top-level DuckLake table column from catalog metadata.
struct DuckLakeColumnMetadata {
  /// Stores the stable DuckLake column id used as the Parquet field id.
  int64_t columnId;

  /// Orders the column within the visible DuckLake table schema.
  int64_t columnOrder;

  /// Stores the visible SQL column name used by Axiom planning.
  std::string name;

  /// Stores the original DuckLake type string before conversion to Velox type.
  std::string duckLakeType;

  /// Stores the Velox type used by the optimizer and scan operator.
  velox::TypePtr type;

  /// Records whether the DuckLake column definition allows null values.
  bool nullsAllowed;
};

/// Describes a live DuckLake data file selected from the current snapshot.
struct DuckLakeDataFile {
  /// Stores the stable DuckLake data file id from catalog metadata.
  int64_t dataFileId;

  /// Stores the fully resolved file path passed to Velox scan splits.
  std::string path;

  /// Records the number of logical rows stored in the file.
  uint64_t recordCount{0};

  /// Records the file size when DuckLake metadata provides it.
  std::optional<uint64_t> fileSizeBytes;

  /// Records the Parquet footer size when DuckLake metadata provides it.
  std::optional<uint64_t> footerSize;

  /// Stores the first DuckLake logical row id for the file when available.
  std::optional<int64_t> rowIdStart;
};

/// Describes the table metadata needed by Axiom planning and split generation.
struct DuckLakeTableMetadata {
  /// Stores the schema-qualified table name requested by the query.
  SchemaTableName name;

  /// Stores the stable DuckLake table id used to join catalog metadata tables.
  int64_t tableId;

  /// Identifies the DuckLake snapshot that all table metadata was read from.
  int64_t snapshotId;

  /// Stores the resolved catalog-level data path from global metadata.
  std::string dataPath;

  /// Stores the resolved schema-level data path for the table schema.
  std::string schemaPath;

  /// Stores the resolved table-level data path used as the base for files.
  std::string tablePath;

  /// Stores the visible table schema converted to a Velox row type.
  velox::RowTypePtr rowType;

  /// Lists the visible top-level columns in DuckLake column order.
  std::vector<DuckLakeColumnMetadata> columns;

  /// Lists the live Parquet data files for the selected snapshot.
  std::vector<DuckLakeDataFile> dataFiles;

  /// Records the table-level row count when DuckLake metadata provides it.
  uint64_t numRows{0};
};

/// Reads DuckLake table metadata from a relational catalog database.
///
/// The catalog client owns all backend-specific metadata access. It returns
/// snapshot-consistent table schemas, table paths, and live data files, but it
/// does not read table data. Data file reads are delegated to Velox through the
/// connector metadata and split manager.
class DuckLakeCatalogClient {
 public:
  virtual ~DuckLakeCatalogClient() = default;

  /// Creates a catalog client for the specified backend and metadata location.
  ///
  /// DuckDB catalogs are opened read-only. Other parsed backends currently
  /// throw explicit user-facing errors until their clients are implemented.
  static std::shared_ptr<DuckLakeCatalogClient> create(
      DuckLakeCatalogSpec spec);

  /// Lists schema names visible in the latest DuckLake snapshot.
  virtual std::vector<std::string> listSchemaNames() = 0;

  /// Loads table metadata for the latest DuckLake snapshot.
  ///
  /// Returns std::nullopt when the table is not present in the current
  /// snapshot. Implementations throw user-facing errors when the table uses a
  /// DuckLake feature that this connector cannot read yet, such as delete
  /// files, encrypted files, inlined data, or unsupported column types.
  virtual std::optional<DuckLakeTableMetadata> loadTable(
      const SchemaTableName& tableName) = 0;
};

} // namespace facebook::axiom::connector::ducklake
