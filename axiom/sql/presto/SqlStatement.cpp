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
      {SqlStatementKind::kDropTable, "DROP TABLE"},
      {SqlStatementKind::kCreateSchema, "CREATE SCHEMA"},
      {SqlStatementKind::kDropSchema, "DROP SCHEMA"},
      {SqlStatementKind::kExplain, "EXPLAIN"},
      {SqlStatementKind::kShowStatsForQuery, "SHOW STATS FOR"},
      {SqlStatementKind::kShowSession, "SHOW SESSION"},
      {SqlStatementKind::kSetSession, "SET SESSION"},
      {SqlStatementKind::kUse, "USE"},
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
    const std::unordered_map<std::string, lp::ExprPtr>& properties) {
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

CreateTableStatement::CreateTableStatement(
    std::string connectorId,
    facebook::axiom::SchemaTableName tableName,
    RowTypePtr tableSchema,
    std::unordered_map<std::string, lp::ExprPtr> properties,
    bool ifNotExists,
    std::vector<Constraint> constraints)
    : SqlStatement(SqlStatementKind::kCreateTable),
      connectorId_{std::move(connectorId)},
      tableName_{std::move(tableName)},
      tableSchema_{std::move(tableSchema)},
      properties_{std::move(properties)},
      ifNotExists_{ifNotExists},
      constraints_{std::move(constraints)} {
  VELOX_CHECK_GT(tableSchema_->size(), 0);

  auto columns = checkUniqueColumns(tableSchema_);
  for (const auto& constraint : constraints_) {
    VELOX_CHECK_GT(constraint.columns.size(), 0);

    std::unordered_set<std::string> constraintColumns;
    for (const auto& col : constraint.columns) {
      auto lower = toLower(col);
      VELOX_USER_CHECK(
          columns.count(lower) > 0, "Constraint on unknown column: {}", col);
      VELOX_USER_CHECK(
          constraintColumns.insert(lower).second,
          "Duplicate constraint column: {}",
          col);
    }
  }
  checkPropertiesLookConstant(properties_);
}

CreateTableAsSelectStatement::CreateTableAsSelectStatement(
    std::string connectorId,
    facebook::axiom::SchemaTableName tableName,
    RowTypePtr tableSchema,
    std::unordered_map<std::string, lp::ExprPtr> properties,
    lp::LogicalPlanNodePtr plan,
    ViewMap views)
    : SqlStatement(SqlStatementKind::kCreateTableAsSelect, std::move(views)),
      connectorId_{std::move(connectorId)},
      tableName_{std::move(tableName)},
      tableSchema_{std::move(tableSchema)},
      properties_{std::move(properties)},
      plan_{std::move(plan)} {
  VELOX_CHECK_GT(tableSchema_->size(), 0);
  checkUniqueColumns(tableSchema_);
  checkPropertiesLookConstant(properties_);
}

} // namespace axiom::sql::presto
