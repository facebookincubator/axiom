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
      {SqlStatementKind::kRefreshVectorIndex, "REFRESH VECTOR INDEX"},
      {SqlStatementKind::kDelete, "DELETE"},
      {SqlStatementKind::kDropTable, "DROP TABLE"},
      {SqlStatementKind::kAddVectorIndex, "ADD VECTOR INDEX"},
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
      {SqlStatementKind::kAnalyze, "ANALYZE"},
  };

  return kNames;
}

std::unordered_set<std::string> checkUniqueColumns(const RowTypePtr& schema) {
  std::unordered_set<std::string> columns;
  for (const auto& name : schema->names()) {
    VELOX_USER_CHECK(
        columns.insert(name).second, "Duplicate column name: {}", name);
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

facebook::axiom::connector::QueryKind SqlStatement::queryKind() const {
  using facebook::axiom::connector::QueryKind;
  switch (kind_) {
    case SqlStatementKind::kSelect:
    case SqlStatementKind::kShowStatsForQuery:
    case SqlStatementKind::kShowSession:
    case SqlStatementKind::kSetSession:
    case SqlStatementKind::kResetSession:
    case SqlStatementKind::kUse:
      return QueryKind::kRead;
    case SqlStatementKind::kExplain: {
      // EXPLAIN ANALYZE runs the statement; the others only plan it.
      const auto* explain = as<ExplainStatement>();
      return explain->isAnalyze() ? explain->statement()->queryKind()
                                  : QueryKind::kRead;
    }
    case SqlStatementKind::kCreateTable:
    case SqlStatementKind::kCreateTableAsSelect:
    case SqlStatementKind::kInsert:
    case SqlStatementKind::kRefreshVectorIndex:
    case SqlStatementKind::kDelete:
    case SqlStatementKind::kDropTable:
    case SqlStatementKind::kAddVectorIndex:
    case SqlStatementKind::kDropVectorIndex:
    case SqlStatementKind::kCreateSchema:
    case SqlStatementKind::kDropSchema:
    case SqlStatementKind::kAddColumn:
    case SqlStatementKind::kCall:
      return QueryKind::kWrite;
  }
  VELOX_UNREACHABLE();
}

facebook::axiom::connector::VectorIndexDefinition
VectorIndexDeclaration::definition(const PropertyEvaluator& evaluate) const {
  return {
      .name = name,
      .column = column,
      .key = key,
      .properties = evaluate(properties),
  };
}

AddVectorIndexStatement::AddVectorIndexStatement(
    std::string connectorId,
    facebook::axiom::SchemaTableName tableName,
    VectorIndexDeclaration index,
    bool ifTableExists,
    bool ifNotExists)
    : SqlStatement(
          SqlStatementKind::kAddVectorIndex,
          /*views=*/{},
          lp::ReferencedTables{
              /*inputTables=*/{},
              facebook::axiom::CatalogSchemaTableName{connectorId, tableName}}),
      connectorId_{std::move(connectorId)},
      tableName_{std::move(tableName)},
      index_{std::move(index)},
      ifTableExists_{ifTableExists},
      ifNotExists_{ifNotExists} {
  checkPropertiesLookConstant(index_.properties);
}

namespace {
// Checks that each column of 'primaryKey' is a column of 'schema', once, and
// returns 'primaryKey'.
std::vector<std::string> checkPrimaryKey(
    std::vector<std::string> primaryKey,
    const RowTypePtr& schema) {
  std::unordered_set<std::string> keyColumns;
  for (const auto& column : primaryKey) {
    VELOX_USER_CHECK(
        schema->containsChild(column),
        "Constraint on unknown column: {}",
        column);
    VELOX_USER_CHECK(
        keyColumns.insert(column).second,
        "Duplicate constraint column: {}",
        column);
  }
  return primaryKey;
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
    std::vector<VectorIndexDeclaration> vectorIndexes,
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
      primaryKey_{checkPrimaryKey(std::move(primaryKey), tableSchema_)},
      vectorIndexes_{std::move(vectorIndexes)},
      ifNotExists_{ifNotExists} {
  VELOX_CHECK_GT(tableSchema_->size(), 0);
  checkUniqueColumns(tableSchema_);
  checkPropertiesLookConstant(properties_);
  for (const auto& column : columns_) {
    checkPropertiesLookConstant(column.properties);
  }
  for (const auto& index : vectorIndexes_) {
    checkPropertiesLookConstant(index.properties);
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
  definition.vectorIndexes.reserve(vectorIndexes_.size());
  for (const auto& index : vectorIndexes_) {
    definition.vectorIndexes.push_back(index.definition(evaluate));
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
