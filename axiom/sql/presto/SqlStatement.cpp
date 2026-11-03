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

#include "axiom/sql/presto/SqlStatement.h"

#include <folly/container/F14Map.h>

namespace axiom::sql::presto {

namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

const auto& statementKindNames() {
  static const folly::F14FastMap<SqlStatementKind, std::string_view> kNames = {
      {SqlStatementKind::kSelect, "SELECT"},
      {SqlStatementKind::kCreateTable, "CREATE TABLE"},
      {SqlStatementKind::kCreateTableAsSelect, "CREATE TABLE AS SELECT"},
      {SqlStatementKind::kInsert, "INSERT"},
      {SqlStatementKind::kDelete, "DELETE"},
      {SqlStatementKind::kDropTable, "DROP TABLE"},
      {SqlStatementKind::kCreateVectorIndex, "CREATE VECTOR INDEX"},
      {SqlStatementKind::kDropVectorIndex, "DROP VECTOR INDEX"},
      {SqlStatementKind::kCreateSchema, "CREATE SCHEMA"},
      {SqlStatementKind::kDropSchema, "DROP SCHEMA"},
      {SqlStatementKind::kExplain, "EXPLAIN"},
      {SqlStatementKind::kShowStatsForQuery, "SHOW STATS FOR"},
      {SqlStatementKind::kShowSession, "SHOW SESSION"},
      {SqlStatementKind::kSetSession, "SET SESSION"},
      {SqlStatementKind::kResetSession, "RESET SESSION"},
      {SqlStatementKind::kUse, "USE"},
      {SqlStatementKind::kAddColumn, "ALTER TABLE ADD COLUMN"},
      {SqlStatementKind::kCall, "CALL"},
  };

  return kNames;
}

std::string toLower(const std::string& s) {
  std::string result = s;
  std::transform(
      result.begin(), result.end(), result.begin(), [](unsigned char c) {
        return std::tolower(c);
      });
  return result;
}

// Duplicate column checks are performed case-insensitively, so
// a schema with 'COL0' and 'col0' will be considered non-unique.
std::unordered_set<std::string> checkUniqueColumns(const RowTypePtr& schema) {
  std::unordered_set<std::string> columns;
  for (const auto& name : schema->names()) {
    auto lower = toLower(name);
    VELOX_USER_CHECK(
        columns.insert(lower).second, "Duplicate column name: {}", name);
  }
  return columns;
}

// Checks that all properties appear constant such that
// they can be evaluated without any runtime dependencies.
void checkPropertiesLookConstant(
    const folly::F14FastMap<std::string, lp::ExprPtr>& properties) {
  for (const auto& [name, expr] : properties) {
    VELOX_USER_CHECK(
        expr->looksConstant(),
        "Property {} = {} is not constant",
        name,
        expr->toString());
  }
}

const auto& explainTypeNames() {
  static const folly::F14FastMap<ExplainStatement::Type, std::string_view>
      kNames = {
          {ExplainStatement::Type::kLogical, "LOGICAL"},
          {ExplainStatement::Type::kGraph, "GRAPH"},
          {ExplainStatement::Type::kOptimized, "OPTIMIZED"},
          {ExplainStatement::Type::kExecutable, "EXECUTABLE"},
          {ExplainStatement::Type::kIo, "IO"},
          {ExplainStatement::Type::kValidate, "VALIDATE"},
      };
  return kNames;
}

const auto& explainFormatNames() {
  static const folly::F14FastMap<ExplainStatement::Format, std::string_view>
      kNames = {
          {ExplainStatement::Format::kText, "TEXT"},
          {ExplainStatement::Format::kGraphviz, "GRAPHVIZ"},
          {ExplainStatement::Format::kJson, "JSON"},
      };
  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(SqlStatementKind, statementKindNames);
AXIOM_DEFINE_EMBEDDED_ENUM_NAME(ExplainStatement, Type, explainTypeNames);
AXIOM_DEFINE_EMBEDDED_ENUM_NAME(ExplainStatement, Format, explainFormatNames);

std::string_view SqlStatement::kindName() const {
  return SqlStatementKindName::toName(kind_);
}

CreateVectorIndexStatement::CreateVectorIndexStatement(
    std::string connectorId,
    facebook::axiom::SchemaTableName indexName,
    RowTypePtr columns,
    folly::F14FastMap<std::string, lp::ExprPtr> properties,
    bool ifNotExists)
    : SqlStatement(
          SqlStatementKind::kCreateVectorIndex,
          /*views=*/{},
          lp::ReferencedTables{
              /*inputTables=*/{},
              facebook::axiom::CatalogSchemaTableName{connectorId, indexName}}),
      connectorId_{std::move(connectorId)},
      indexName_{std::move(indexName)},
      columns_{std::move(columns)},
      properties_{std::move(properties)},
      ifNotExists_{ifNotExists} {
  checkUniqueColumns(columns_);
  checkPropertiesLookConstant(properties_);
}

namespace {
// Returns 'primaryKey' with each column spelled as 'schema' declares it.
// Matches column names ignoring case.
std::vector<std::string> resolvePrimaryKey(
    const std::vector<std::string>& primaryKey,
    const RowTypePtr& schema) {
  std::unordered_map<std::string, std::string> declaredNames;
  for (const auto& name : schema->names()) {
    declaredNames.emplace(toLower(name), name);
  }
  std::unordered_set<std::string> keyColumns;
  std::vector<std::string> resolved;
  resolved.reserve(primaryKey.size());
  for (const auto& column : primaryKey) {
    const auto lower = toLower(column);
    const auto it = declaredNames.find(lower);
    VELOX_USER_CHECK(
        it != declaredNames.end(), "Constraint on unknown column: {}", column);
    VELOX_USER_CHECK(
        keyColumns.insert(lower).second,
        "Duplicate constraint column: {}",
        column);
    resolved.push_back(it->second);
  }
  return resolved;
}

RowTypePtr toRowType(const std::vector<ColumnDeclaration>& columns) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(columns.size());
  types.reserve(columns.size());
  for (const auto& column : columns) {
    names.push_back(column.name);
    types.push_back(column.type);
  }
  return ROW(std::move(names), std::move(types));
}
} // namespace

facebook::axiom::connector::ColumnDefinition ColumnDeclaration::definition(
    const PropertyEvaluator& evaluate) const {
  return {
      .name = name,
      .type = type,
      .nullable = nullable,
      .comment = comment,
      .properties = evaluate(properties),
  };
}

CreateTableStatement::CreateTableStatement(
    std::string connectorId,
    facebook::axiom::SchemaTableName tableName,
    std::vector<ColumnDeclaration> columns,
    std::optional<std::string> comment,
    folly::F14FastMap<std::string, lp::ExprPtr> properties,
    std::vector<std::string> primaryKey,
    bool ifNotExists)
    : SqlStatement(
          SqlStatementKind::kCreateTable,
          /*views=*/{},
          lp::ReferencedTables{
              /*inputTables=*/{},
              facebook::axiom::CatalogSchemaTableName{connectorId, tableName}}),
      connectorId_{std::move(connectorId)},
      tableName_{std::move(tableName)},
      columns_{std::move(columns)},
      tableSchema_{toRowType(columns_)},
      comment_{std::move(comment)},
      properties_{std::move(properties)},
      primaryKey_{resolvePrimaryKey(primaryKey, tableSchema_)},
      ifNotExists_{ifNotExists} {
  VELOX_CHECK_GT(tableSchema_->size(), 0);
  checkUniqueColumns(tableSchema_);
  checkPropertiesLookConstant(properties_);
  for (const auto& column : columns_) {
    checkPropertiesLookConstant(column.properties);
  }
}

facebook::axiom::connector::TableDefinition CreateTableStatement::definition(
    const PropertyEvaluator& evaluate) const {
  facebook::axiom::connector::TableDefinition definition{
      .comment = comment_,
      .properties = evaluate(properties_),
      .primaryKey = primaryKey_,
  };
  definition.columns.reserve(columns_.size());
  for (const auto& column : columns_) {
    definition.columns.push_back(column.definition(evaluate));
  }
  return definition;
}

CreateTableAsSelectStatement::CreateTableAsSelectStatement(
    std::string connectorId,
    facebook::axiom::SchemaTableName tableName,
    RowTypePtr tableSchema,
    std::optional<std::string> comment,
    folly::F14FastMap<std::string, lp::ExprPtr> properties,
    lp::LogicalPlanNodePtr plan,
    ViewMap views,
    std::unordered_set<facebook::axiom::CatalogSchemaTableName> inputTables)
    : SqlStatement(
          SqlStatementKind::kCreateTableAsSelect,
          std::move(views),
          lp::ReferencedTables{
              std::move(inputTables),
              facebook::axiom::CatalogSchemaTableName{connectorId, tableName}}),
      connectorId_{std::move(connectorId)},
      tableName_{std::move(tableName)},
      tableSchema_{std::move(tableSchema)},
      comment_{std::move(comment)},
      properties_{std::move(properties)},
      plan_{std::move(plan)} {
  VELOX_CHECK_GT(tableSchema_->size(), 0);
  checkUniqueColumns(tableSchema_);
  checkPropertiesLookConstant(properties_);
}

facebook::axiom::connector::TableDefinition
CreateTableAsSelectStatement::definition(
    const PropertyEvaluator& evaluate) const {
  auto definition = facebook::axiom::connector::TableDefinition::create(
      tableSchema_, evaluate(properties_));
  definition.comment = comment_;
  return definition;
}

} // namespace axiom::sql::presto
