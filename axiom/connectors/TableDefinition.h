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

#include <optional>
#include <string>
#include <vector>

#include <folly/container/F14Map.h>

#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::axiom::connector {

/// A column CREATE TABLE or ADD COLUMN declares.
struct ColumnDefinition {
  /// The column's name, unique within the table.
  std::string name;

  /// The column's type.
  velox::TypePtr type;

  /// False for a column declared NOT NULL.
  bool nullable{true};

  /// The column's description. Nullopt when the statement declares none.
  std::optional<std::string> comment;

  /// The column's properties, from its WITH clause, by name.
  folly::F14FastMap<std::string, velox::Variant> properties;
};

/// A vector index CREATE TABLE or ADD VECTOR INDEX declares (see
/// connector::VectorIndex).
struct VectorIndexDefinition {
  /// The index's name, unique within its table.
  std::string name;

  /// The ARRAY(REAL) column the index indexes.
  std::string column;

  /// The columns a search returns for each match, which find the row it came
  /// from, from KEY. Empty when the statement leaves it out: the table's
  /// primary key.
  std::vector<std::string> key;

  /// The index's properties, from its WITH clause, by name: the index's own
  /// (see VectorIndexProperties) and those of its storage tables.
  folly::F14FastMap<std::string, velox::Variant> properties;
};

/// What CREATE TABLE declares about a new table: its columns, comment,
/// properties, primary key and vector indexes.
///
///   TableDefinition definition{
///       .columns = {{.name = "id", .type = BIGINT()}},
///       .primaryKey = {"id"},
///   };
///   co_await metadata.co_createTable(session, tableName, definition, ...);
struct TableDefinition {
  /// The table's columns, in order.
  std::vector<ColumnDefinition> columns;

  /// The table's description. Nullopt when CREATE TABLE declares none.
  std::optional<std::string> comment;

  /// The table's properties, from its WITH clause, by name: partitioning,
  /// format and the like, as the connector defines them.
  folly::F14FastMap<std::string, velox::Variant> properties;

  /// The primary key's columns, each a column of 'columns'. Empty: the table
  /// has no primary key.
  std::vector<std::string> primaryKey;

  /// The table's vector indexes, each on a column of 'columns'.
  std::vector<VectorIndexDefinition> vectorIndexes;

  /// Returns a minimal definition: columns with the names and types of
  /// 'rowType', nullable, no comments or properties, and the table properties
  /// 'properties'. For callers that declare nothing else.
  static TableDefinition create(
      const velox::RowTypePtr& rowType,
      folly::F14FastMap<std::string, velox::Variant> properties);

  /// Returns the names and types of the columns.
  velox::RowTypePtr rowType() const;

  /// Returns the comments of the columns that have one, by column name.
  folly::F14FastMap<std::string, std::string> columnComments() const;
};

/// The parts of a TableDefinition a connector stores beyond its columns' names
/// and types and its properties (see ConnectorMetadata::supportsCreateTable()).
struct TableDefinitionSupport {
  /// Columns declared NOT NULL: ColumnDefinition::nullable.
  bool nullability{false};

  /// Column descriptions: ColumnDefinition::comment.
  bool columnComments{false};

  /// Column properties: ColumnDefinition::properties.
  bool columnProperties{false};

  /// The table's description: TableDefinition::comment.
  bool tableComment{false};

  /// The primary key: TableDefinition::primaryKey.
  bool primaryKey{false};

  /// Vector indexes: TableDefinition::vectorIndexes, and ADD VECTOR INDEX.
  bool vectorIndexes{false};

  /// Throws a user error for the first part of 'definition' this does not
  /// include.
  void check(const TableDefinition& definition) const;

  /// Throws a user error for the first part of 'column' this does not include.
  void check(const ColumnDefinition& column) const;

  /// Throws a user error if this does not include vector indexes.
  void check(const VectorIndexDefinition& index) const;
};

} // namespace facebook::axiom::connector
