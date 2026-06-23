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

#include <string>
#include <string_view>
#include <vector>

#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/connectors/Connector.h"

namespace facebook::axiom::connector::file {

/// Carries the file path for a single file read.
struct FileSplit : public velox::connector::ConnectorSplit {
  FileSplit(const std::string& connectorId, std::string filePath)
      : ConnectorSplit(connectorId), filePath(std::move(filePath)) {}

  const std::string filePath;
};

/// Column handle that wraps a single column name for projection.
class FileColumnHandle : public velox::connector::ColumnHandle {
 public:
  explicit FileColumnHandle(const std::string& name) : name_(name) {}

  const std::string& name() const override {
    return name_;
  }

 private:
  const std::string name_;
};

/// Carries the resolved file path, metadata suffix, full schema, and
/// column handles for a file connector query.
class FileTableHandle : public velox::connector::ConnectorTableHandle {
 public:
  FileTableHandle(
      const std::string& connectorId,
      SchemaTableName schemaTableName,
      std::string filePath,
      std::string suffix,
      velox::RowTypePtr fullSchema,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles)
      : ConnectorTableHandle(connectorId),
        schemaTableName_(std::move(schemaTableName)),
        filePath_(std::move(filePath)),
        suffix_(std::move(suffix)),
        fullSchema_(std::move(fullSchema)),
        qualifiedName_(
            fmt::format(
                "{}.{}",
                schemaTableName_.schema,
                schemaTableName_.table)),
        columnHandles_(std::move(columnHandles)) {}

  const std::string& name() const override {
    return qualifiedName_;
  }

  std::string toString() const override {
    return name();
  }

  const SchemaTableName& schemaTableName() const {
    return schemaTableName_;
  }

  const std::string& filePath() const {
    return filePath_;
  }

  /// Metadata table suffix (e.g. "encodings"). Empty for data tables.
  const std::string& suffix() const {
    return suffix_;
  }

  /// Full table schema resolved during planning.
  const velox::RowTypePtr& fullSchema() const {
    return fullSchema_;
  }

  const std::vector<velox::connector::ColumnHandlePtr>& columnHandles() const {
    return columnHandles_;
  }

 private:
  const SchemaTableName schemaTableName_;
  const std::string filePath_;
  const std::string suffix_;
  const velox::RowTypePtr fullSchema_;
  const std::string qualifiedName_;
  const std::vector<velox::connector::ColumnHandlePtr> columnHandles_;
};

/// Defines the physical layout for a file connector table.
class FileTableLayout : public TableLayout {
 public:
  FileTableLayout(
      Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns,
      std::string filePath,
      std::string suffix);

  bool supportsSampling() const override {
    return false;
  }

  velox::connector::ColumnHandlePtr createColumnHandle(
      const ConnectorSessionPtr& session,
      const std::string& columnName,
      std::vector<velox::common::Subfield> subfields,
      std::optional<velox::TypePtr> castToType,
      SubfieldMapping subfieldMapping) const override;

  velox::connector::ConnectorTableHandlePtr createTableHandle(
      const ConnectorSessionPtr& session,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles,
      velox::core::ExpressionEvaluator& evaluator,
      std::vector<velox::core::TypedExprPtr> filters,
      std::vector<velox::core::TypedExprPtr>& rejectedFilters,
      velox::RowTypePtr dataColumns,
      std::optional<LookupKeys> lookupKeys) const override;

 private:
  std::string filePath_;
  std::string suffix_;
};

/// Represents a table backed by a file on disk.
class FileTable : public Table {
 public:
  FileTable(
      const SchemaTableName& tableName,
      const velox::RowTypePtr& schema,
      velox::connector::Connector* connector,
      std::string filePath,
      std::string suffix);

  /// Splits "<path>$<suffix>" into {path, suffix}. Takes the part after the
  /// last '$' as the suffix only when that '$' isn't the first character and
  /// the part is a bare token (no '/' or '.'); otherwise the whole name is the
  /// path with an empty suffix. Examples:
  ///   "/data/f.parquet"               -> {"/data/f.parquet", ""}
  ///   "/data/f.parquet$column_chunks" -> {"/data/f.parquet", "column_chunks"}
  ///   "/data/$bak/f.parquet"          -> {"/data/$bak/f.parquet", ""}
  ///   "/tmp/foo$bar.parquet"          -> {"/tmp/foo$bar.parquet", ""}
  /// Known limitation: a '$' in an extensionless file name is read as a suffix
  /// separator, so "/data/foo$bar" splits into {"/data/foo", "bar"}.
  static std::pair<std::string, std::string> parseName(
      const std::string& tableName);

  /// Returns true if 'name' is a valid metadata-table name (non-empty and free
  /// of '$', '.', and '/' - reserved by the "<file path>$<name>" addressing
  /// scheme).
  static bool isValidName(std::string_view name);

  const std::vector<const TableLayout*>& layouts() const override {
    return layouts_;
  }

  uint64_t numRows() const override {
    return 0;
  }

 private:
  std::vector<const TableLayout*> layouts_;
  std::unique_ptr<FileTableLayout> layout_;
};

} // namespace facebook::axiom::connector::file
