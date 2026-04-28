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

/// Describes a top-level DuckLake table column.
struct DuckLakeColumnMetadata {
  /// Stores the stable DuckLake column id.
  int64_t columnId;

  /// Orders the column within the DuckLake table schema.
  int64_t columnOrder;

  /// Stores the visible SQL column name.
  std::string name;

  /// Stores the original DuckLake type string.
  std::string duckLakeType;

  /// Stores the Velox type used for query planning and execution.
  velox::TypePtr type;

  /// Records whether the DuckLake schema allows null values.
  bool nullsAllowed;
};

/// Describes a live DuckLake data file.
struct DuckLakeDataFile {
  /// Stores the stable DuckLake data file id.
  int64_t dataFileId;

  /// Stores the resolved file path passed to Velox splits.
  std::string path;

  /// Records the number of rows in the file.
  uint64_t recordCount{0};

  /// Records the file size when DuckLake metadata provides it.
  std::optional<uint64_t> fileSizeBytes;

  /// Records the Parquet footer size when DuckLake metadata provides it.
  std::optional<uint64_t> footerSize;

  /// Stores the first DuckLake logical row id when available.
  std::optional<int64_t> rowIdStart;
};

/// Describes the table metadata needed by Axiom planning and split generation.
struct DuckLakeTableMetadata {
  /// Stores the schema-qualified table name.
  SchemaTableName name;

  /// Stores the stable DuckLake table id.
  int64_t tableId;

  /// Identifies the DuckLake snapshot used to read metadata.
  int64_t snapshotId;

  /// Stores the resolved catalog-level data path.
  std::string dataPath;

  /// Stores the resolved schema-level data path.
  std::string schemaPath;

  /// Stores the resolved table-level data path.
  std::string tablePath;

  /// Stores the visible table schema as a Velox row type.
  velox::RowTypePtr rowType;

  /// Lists the visible top-level columns in table order.
  std::vector<DuckLakeColumnMetadata> columns;

  /// Lists the live data files for the selected snapshot.
  std::vector<DuckLakeDataFile> dataFiles;

  /// Records the table-level row count when DuckLake metadata provides it.
  uint64_t numRows{0};
};

/// Reads table metadata from a DuckLake catalog database.
class DuckLakeCatalogClient {
 public:
  virtual ~DuckLakeCatalogClient() = default;

  /// Creates a catalog client for the specified backend.
  static std::unique_ptr<DuckLakeCatalogClient> create(
      DuckLakeCatalogSpec spec);

  /// Lists schemas visible in the current DuckLake snapshot.
  virtual std::vector<std::string> listSchemaNames() = 0;

  /// Loads table metadata for the current DuckLake snapshot.
  virtual std::optional<DuckLakeTableMetadata> loadTable(
      const SchemaTableName& tableName) = 0;
};

} // namespace facebook::axiom::connector::ducklake
