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

#include "axiom/common/Enums.h"
#include "axiom/logical_plan/LogicalPlanNode.h"

namespace axiom::sql::presto {

enum class SqlStatementKind {
  kSelect,
  kCreateTableAsSelect,
  kInsert,
  kDropTable,
  kExplain,
};

AXIOM_DECLARE_ENUM_NAME(SqlStatementKind);

class SqlStatement {
 public:
  explicit SqlStatement(
      SqlStatementKind kind,
      std::unordered_map<std::pair<std::string, std::string>, std::string>
          views = {})
      : kind_{kind}, views_{std::move(views)} {}

  virtual ~SqlStatement() = default;

  std::string_view kindName() const;

  SqlStatementKind kind() const {
    return kind_;
  }

  bool isSelect() const {
    return kind_ == SqlStatementKind::kSelect;
  }

  bool isCreateTableAsSelect() const {
    return kind_ == SqlStatementKind::kCreateTableAsSelect;
  }

  bool isInsert() const {
    return kind_ == SqlStatementKind::kInsert;
  }

  bool isDropTable() const {
    return kind_ == SqlStatementKind::kDropTable;
  }

  bool isExplain() const {
    return kind_ == SqlStatementKind::kExplain;
  }

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }

  /// A set of views used in the query. Each view is identified by a connector
  /// ID and a view name. Map value contains the text of the view.
  const std::unordered_map<std::pair<std::string, std::string>, std::string>&
  views() const {
    return views_;
  }

 private:
  const SqlStatementKind kind_;
  const std::unordered_map<std::pair<std::string, std::string>, std::string>
      views_;
};

using SqlStatementPtr = std::shared_ptr<const SqlStatement>;

class SelectStatement : public SqlStatement {
 public:
  explicit SelectStatement(
      facebook::axiom::logical_plan::LogicalPlanNodePtr plan,
      std::unordered_map<std::pair<std::string, std::string>, std::string>
          views = {})
      : SqlStatement(SqlStatementKind::kSelect, std::move(views)),
        plan_{std::move(plan)} {}

  const facebook::axiom::logical_plan::LogicalPlanNodePtr& plan() const {
    return plan_;
  }

 private:
  const facebook::axiom::logical_plan::LogicalPlanNodePtr plan_;
};

class InsertStatement : public SqlStatement {
 public:
  explicit InsertStatement(
      facebook::axiom::logical_plan::LogicalPlanNodePtr plan,
      std::unordered_map<std::pair<std::string, std::string>, std::string>
          views = {})
      : SqlStatement(SqlStatementKind::kInsert, std::move(views)),
        plan_{std::move(plan)} {}

  const facebook::axiom::logical_plan::LogicalPlanNodePtr& plan() const {
    return plan_;
  }

 private:
  const facebook::axiom::logical_plan::LogicalPlanNodePtr plan_;
};

class CreateTableAsSelectStatement : public SqlStatement {
 public:
  CreateTableAsSelectStatement(
      std::string connectorId,
      std::string tableName,
      facebook::velox::RowTypePtr tableSchema,
      std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>
          properties,
      facebook::axiom::logical_plan::LogicalPlanNodePtr plan,
      std::unordered_map<std::pair<std::string, std::string>, std::string>
          views = {})
      : SqlStatement(SqlStatementKind::kCreateTableAsSelect, std::move(views)),
        connectorId_{std::move(connectorId)},
        tableName_{std::move(tableName)},
        tableSchema_{std::move(tableSchema)},
        properties_{std::move(properties)},
        plan_{std::move(plan)} {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const std::string& tableName() const {
    return tableName_;
  }

  const facebook::velox::RowTypePtr& tableSchema() const {
    return tableSchema_;
  }

  const std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>&
  properties() const {
    return properties_;
  }

  const facebook::axiom::logical_plan::LogicalPlanNodePtr& plan() const {
    return plan_;
  }

 private:
  const std::string connectorId_;
  const std::string tableName_;
  const facebook::velox::RowTypePtr tableSchema_;
  std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>
      properties_;
  const facebook::axiom::logical_plan::LogicalPlanNodePtr plan_;
};

class DropTableStatement : public SqlStatement {
 public:
  DropTableStatement(
      std::string connectorId,
      std::string tableName,
      bool ifExists)
      : SqlStatement(SqlStatementKind::kDropTable),
        connectorId_{std::move(connectorId)},
        tableName_{std::move(tableName)},
        ifExists_{ifExists} {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const std::string& tableName() const {
    return tableName_;
  }

  bool ifExists() const {
    return ifExists_;
  }

 private:
  const std::string connectorId_;
  const std::string tableName_;
  const bool ifExists_;
};

class ExplainStatement : public SqlStatement {
 public:
  enum class Type {
    /// Logical plan. Input to the optimizer.
    kLogical,

    /// Query graph.
    kGraph,

    /// Optimize physical plan.
    kOptimized,

    /// Executable Velox plan.
    kExecutable,
  };

  /// 'type' applies only when 'analyze' is false.
  explicit ExplainStatement(
      SqlStatementPtr statement,
      bool analyze = false,
      Type type = Type::kLogical)
      : SqlStatement(SqlStatementKind::kExplain),
        statement_{std::move(statement)},
        analyze_{analyze},
        type_{type} {}

  const SqlStatementPtr& statement() const {
    return statement_;
  }

  bool isAnalyze() const {
    return analyze_;
  }

  Type type() const {
    return type_;
  }

 private:
  const SqlStatementPtr statement_;
  const bool analyze_;
  const Type type_;
};

} // namespace axiom::sql::presto
