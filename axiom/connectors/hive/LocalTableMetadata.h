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

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <folly/dynamic.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "folly/container/F14Map.h"
#include "velox/dwio/common/Options.h"
#include "velox/type/Type.h"

namespace facebook::axiom::connector::hive {

/// Describes a file in a table. Used for split enumeration and stats
/// collection.
struct FileInfo {
  /// Absolute path to the data file.
  std::string path;

  /// Partition key=value pairs parsed from parent directory names.
  folly::F14FastMap<std::string, std::optional<std::string>> partitionKeys;

  /// Bucket number extracted from the file name, if bucketed.
  std::optional<int32_t> bucketNumber;

  /// Row count from file header metadata.
  std::optional<uint64_t> numRows;

  /// Per-column stats from file header metadata, keyed by column name.
  folly::F14FastMap<std::string, ColumnStatistics> columnStats;

  /// Recursively lists files under 'path', populating FileInfo entries with
  /// partition keys parsed from directory names (key=value format) and optional
  /// bucket numbers.
  static void listFiles(
      std::string_view path,
      const std::function<int32_t(std::string_view)>& parseBucketNumber,
      int32_t prefixSize,
      std::vector<std::unique_ptr<const FileInfo>>& result);
};

/// Table-level statistics persisted in .stats files.
struct PersistedStats {
  /// Total number of rows in the table or partition.
  uint64_t numRows{0};

  /// Per-column statistics (min, max, numValues, numDistinct, etc.).
  std::vector<ColumnStatistics> columns;

  /// Reads persisted stats from the .stats file in 'directory'. Returns
  /// std::nullopt if the file doesn't exist.
  static std::optional<PersistedStats> read(const std::string& directory);

  /// Writes stats to a .stats file in 'directory', merging with any existing
  /// stats from prior writes.
  static void write(const std::string& directory, PersistedStats stats);
};

/// Returns the path to the .schema file in the given directory.
std::string schemaPath(std::string_view path);

/// Writes a minimal .schema file for the given row type and file format.
/// All columns are treated as data columns (no partitioning or bucketing).
void writeSchemaFile(
    const std::string& tablePath,
    const velox::RowTypePtr& rowType,
    velox::dwio::common::FileFormat fileFormat);

/// Builds the {"name": ..., "type": ...} JSON object used in .schema files for
/// a single column.
folly::dynamic columnToJson(std::string_view name, const velox::TypePtr& type);

} // namespace facebook::axiom::connector::hive
