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

#include "axiom/connectors/ConnectorMetadata.h"

namespace facebook::axiom::connector::system {

/// Table handle for an information_schema relation. Carries the schemas and,
/// for table-level relations, the tables the scan describes. Scans of
/// 'tables', 'views', and 'columns' are accepted only when their filters name
/// the tables. A 'schemata' scan carries the catalog's advertised schemas plus
/// 'information_schema'; all of its filters remain above the scan.
///
/// A scan of the 'columns' relation of catalog 'hive', whose filters pin
/// table_schema to 'sales' and table_name to 'orders' and 'lineitem', carries
/// catalog() = 'hive', relation() = 'columns', schemas() = {'sales'} and
/// tables() = {'lineitem', 'orders'}, and describes each (schema, table)
/// pair.
class InformationSchemaTableHandle
    : public velox::connector::ConnectorTableHandle {
 public:
  InformationSchemaTableHandle(
      const std::string& connectorId,
      SchemaTableName schemaTableName,
      std::vector<std::string> schemas,
      std::vector<std::string> tables);

  const std::string& name() const override {
    return qualifiedName_;
  }

  std::string toString() const override {
    return name();
  }

  /// Catalog whose metadata produces the rows.
  const std::string& catalog() const {
    return catalog_;
  }

  /// Relation being read: 'schemata', 'tables', 'views', or 'columns'.
  const std::string& relation() const {
    return schemaTableName_.table;
  }

  /// Schemas the scan describes. For 'schemata', these are the names advertised
  /// by the source catalog plus 'information_schema'. For other relations,
  /// these are the names admitted by pushed filters; empty describes nothing.
  const std::vector<std::string>& schemas() const {
    return schemas_;
  }

  /// Table names the query names. Empty for 'schemata', or when filters admit
  /// no table name. Every (schema, table) pair is described; a pair no table
  /// answers to contributes no rows.
  const std::vector<std::string>& tables() const {
    return tables_;
  }

  folly::dynamic serialize() const override;

  static velox::connector::ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe() {
    velox::registerDeserializerWithContext<InformationSchemaTableHandle>();
  }

  VELOX_DEFINE_CLASS_NAME(InformationSchemaTableHandle)

 private:
  const SchemaTableName schemaTableName_;
  const std::string catalog_;
  const std::string qualifiedName_;
  const std::vector<std::string> schemas_;
  const std::vector<std::string> tables_;
};

/// The information_schema relations — 'schemata', 'tables', 'views', and
/// 'columns' — as served for one catalog.
class InformationSchema {
 public:
  /// Spells a column's type for information_schema.columns.data_type. A SQL
  /// dialect that writes types differently registers its own; see
  /// 'SystemConnector'.
  using TypeNameFormatter = std::function<std::string(const velox::Type&)>;

  /// The default 'TypeNameFormatter': the type as Velox renders it, e.g.
  /// 'BIGINT', 'ARRAY<REAL>', 'ROW<x:BIGINT,y:VARCHAR>'. A dialect whose
  /// clients read these names registers a formatter that writes them its own
  /// way.
  static std::string defaultTypeName(const velox::Type& type);

  /// Prefix under which a catalog's relations live in this connector's schema
  /// namespace: the relations of catalog 'foo' are the tables of schema
  /// '$info_schema@foo', so the catalog whose metadata produces the rows is
  /// named by the schema. The prefix is reserved, which keeps a catalog name
  /// clear of the connector's own schemas. Whoever resolves a name for a
  /// catalog maps it onto this schema.
  static constexpr std::string_view kPrefix = "$info_schema@";

  /// Schema that exposes a catalog's metadata relations.
  static constexpr std::string_view kInformationSchema = "information_schema";

  /// The relations, one per kind of object described.
  static constexpr std::string_view kSchemata = "schemata";
  static constexpr std::string_view kTables = "tables";
  static constexpr std::string_view kViews = "views";
  static constexpr std::string_view kColumns = "columns";

  /// Values of kTables.table_type.
  static constexpr std::string_view kBaseTableType = "BASE TABLE";
  static constexpr std::string_view kViewType = "VIEW";

  /// Returns the schema the relations of 'catalog' live in, e.g. 'foo' ->
  /// '$info_schema@foo'.
  static std::string schemaName(std::string_view catalog);

  /// Returns the catalog a schema name refers to, as a view into
  /// 'schemaName', or std::nullopt if it is not an information_schema name.
  static std::optional<std::string_view> catalog(std::string_view schemaName);

  /// Returns the schema names advertised by 'metadata' plus
  /// kInformationSchema, without duplicates.
  static std::vector<std::string> listedSchemaNames(
      ConnectorMetadata& metadata,
      const ConnectorSessionPtr& session);

  /// Names of the relations, sorted.
  static const std::vector<std::string>& tableNames();

  /// Returns the row type of a relation, or nullptr if 'tableName' names none.
  static const velox::RowTypePtr& tableSchema(std::string_view tableName);

  /// Returns the relation 'tableName' names, or nullptr when it names none or
  /// its catalog is not registered.
  ///
  /// @param serving The connector that serves the relations. Not the catalog
  /// being described, which 'tableName''s schema names.
  static TablePtr findTable(
      const SchemaTableName& tableName,
      velox::connector::Connector* serving);

  /// Returns a data source reading the relation 'tableHandle' names from the
  /// metadata of the catalog it names. Rows come in batches of the size the
  /// scan asks for: a relation may describe many tables, and kColumns returns
  /// a row per column of each.
  /// @param typeName Spelling of the types kColumns reports. Must outlive the
  /// returned data source.
  static std::unique_ptr<velox::connector::DataSource> makeDataSource(
      const std::shared_ptr<const InformationSchemaTableHandle>& tableHandle,
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool,
      const TypeNameFormatter& typeName);
};

} // namespace facebook::axiom::connector::system
