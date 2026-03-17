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

#include <unordered_set>
#include "axiom/common/Enums.h"
#include "axiom/common/SchemaTableName.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "velox/common/base/Exceptions.h"

namespace axiom::sql::presto {

/// Key for the views map: connectorId + schema-qualified view name.
using ViewKey = std::pair<std::string, facebook::axiom::SchemaTableName>;

/// Hash function for ViewKey.
struct ViewKeyHash {
  size_t operator()(const ViewKey& key) const {
    auto hash = std::hash<std::string>{}(key.first);
    hash ^= std::hash<facebook::axiom::SchemaTableName>{}(key.second) +
        0x9e3779b9 + (hash << 6) + (hash >> 2);
    return hash;
  }
};

/// Map from (connectorId, SchemaTableName) to view SQL text.
using ViewMap = std::unordered_map<ViewKey, std::string, ViewKeyHash>;

enum class SqlStatementKind {
  kSelect,
  kCreateTable,
  kCreateTableAsSelect,
  kInsert,
  kDropTable,
  kCreateSchema,
  kDropSchema,
  kExplain,
  kShowStatsForQuery,
  kUse,
};

AXIOM_DECLARE_ENUM_NAME(SqlStatementKind);

class SqlStatement {
 public:
  explicit SqlStatement(SqlStatementKind kind, ViewMap views = {})
      : kind_{kind}, views_{std::move(views)} {}

  virtual ~SqlStatement() = default;

  std::string_view kindName() const;

  SqlStatementKind kind() const {
    return kind_;
  }

  bool isSelect() const {
    return kind_ == SqlStatementKind::kSelect;
  }

  bool isCreateTable() const {
    return kind_ == SqlStatementKind::kCreateTable;
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

  bool isCreateSchema() const {
    return kind_ == SqlStatementKind::kCreateSchema;
  }

  bool isDropSchema() const {
    return kind_ == SqlStatementKind::kDropSchema;
  }

  bool isExplain() const {
    return kind_ == SqlStatementKind::kExplain;
  }

  bool isShowStatsForQuery() const {
    return kind_ == SqlStatementKind::kShowStatsForQuery;
  }

  bool isUse() const {
    return kind_ == SqlStatementKind::kUse;
  }

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }

  /// A set of views used in the query. Each view is identified by a connector
  /// ID and a schema-qualified view name. Map value contains the text of the
  /// view.
  const ViewMap& views() const {
    return views_;
  }

 private:
  const SqlStatementKind kind_;
  const ViewMap views_;
};

using SqlStatementPtr = std::shared_ptr<const SqlStatement>;

class SelectStatement : public SqlStatement {
 public:
  explicit SelectStatement(
      facebook::axiom::logical_plan::LogicalPlanNodePtr plan,
      ViewMap views = {})
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
      ViewMap views = {})
      : SqlStatement(SqlStatementKind::kInsert, std::move(views)),
        plan_{std::move(plan)} {}

  const facebook::axiom::logical_plan::LogicalPlanNodePtr& plan() const {
    return plan_;
  }

 private:
  const facebook::axiom::logical_plan::LogicalPlanNodePtr plan_;
};

class CreateTableStatement : public SqlStatement {
 public:
  /// A unique or primary key constraint. 'name' is the optional constraint
  /// name. 'columns' is a list of columns that the constraint applies to.
  /// 'type' specifies the constraint type, Unique or Primary Key.
  ///
  /// A primary key constraint ensures that the specified column(s) contain
  /// unique, non-null values. A unique constraint ensures that the specified
  /// column(s) contain only unique values. A table can have only one primary
  /// key but multiple unique constraints. The handling of column constraints
  /// is connector-specific and not all implementations may support them.
  struct Constraint {
    enum class Type {
      /// e.g., 'CONSTRAINT constraint_name UNIQUE(col)'
      kUnique,

      /// e.g., 'PRIMARY KEY (col)'
      kPrimaryKey
    };

    std::string name;
    std::vector<std::string> columns;
    Type type;
  };

  CreateTableStatement(
      std::string connectorId,
      facebook::axiom::SchemaTableName tableName,
      facebook::velox::RowTypePtr tableSchema,
      std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>
          properties,
      bool ifNotExists,
      std::vector<Constraint> constraints = {});

  const std::string& connectorId() const {
    return connectorId_;
  }

  const facebook::axiom::SchemaTableName& tableName() const {
    return tableName_;
  }

  /// Returns the table layout. Columns are case-insensitively checked for
  /// duplicates, e.g., a schema with columns 'ID' and 'id' is not permitted.
  const facebook::velox::RowTypePtr& tableSchema() const {
    return tableSchema_;
  }

  /// Returns the table properties as a map from property name to expression.
  /// Properties are connector-specific settings (e.g., partitioned_by, format)
  /// which set additional table attributes. An expression type is used because
  /// because some properties (e.g., ARRAY['ds']) require runtime evaluation.
  const std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>&
  properties() const {
    return properties_;
  }

  /// Create the table only if a table of the same name does not exist.
  bool ifNotExists() const {
    return ifNotExists_;
  }

  /// Returns the list of constraints (primary key, unique) on table columns.
  /// See CreateTableStatement::Constraint documentation for contents.
  const std::vector<Constraint>& constraints() const {
    return constraints_;
  }

 private:
  const std::string connectorId_;
  const facebook::axiom::SchemaTableName tableName_;
  const facebook::velox::RowTypePtr tableSchema_;
  std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>
      properties_;
  const bool ifNotExists_;
  const std::vector<Constraint> constraints_;
};

class CreateTableAsSelectStatement : public SqlStatement {
 public:
  CreateTableAsSelectStatement(
      std::string connectorId,
      facebook::axiom::SchemaTableName tableName,
      facebook::velox::RowTypePtr tableSchema,
      std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>
          properties,
      facebook::axiom::logical_plan::LogicalPlanNodePtr plan,
      ViewMap views = {});

  const std::string& connectorId() const {
    return connectorId_;
  }

  const facebook::axiom::SchemaTableName& tableName() const {
    return tableName_;
  }

  const facebook::velox::RowTypePtr& tableSchema() const {
    return tableSchema_;
  }

  /// See CreateTableStatement::properties() for usage.
  const std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>&
  properties() const {
    return properties_;
  }

  const facebook::axiom::logical_plan::LogicalPlanNodePtr& plan() const {
    return plan_;
  }

 private:
  const std::string connectorId_;
  const facebook::axiom::SchemaTableName tableName_;
  const facebook::velox::RowTypePtr tableSchema_;
  std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>
      properties_;
  const facebook::axiom::logical_plan::LogicalPlanNodePtr plan_;
};

class DropTableStatement : public SqlStatement {
 public:
  DropTableStatement(
      std::string connectorId,
      facebook::axiom::SchemaTableName tableName,
      bool ifExists)
      : SqlStatement(SqlStatementKind::kDropTable),
        connectorId_{std::move(connectorId)},
        tableName_{std::move(tableName)},
        ifExists_{ifExists} {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const facebook::axiom::SchemaTableName& tableName() const {
    return tableName_;
  }

  bool ifExists() const {
    return ifExists_;
  }

 private:
  const std::string connectorId_;
  const facebook::axiom::SchemaTableName tableName_;
  const bool ifExists_;
};

class CreateSchemaStatement : public SqlStatement {
 public:
  CreateSchemaStatement(
      std::string connectorId,
      std::string schemaName,
      bool ifNotExists,
      std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>
          properties)
      : SqlStatement(SqlStatementKind::kCreateSchema),
        connectorId_{std::move(connectorId)},
        schemaName_{std::move(schemaName)},
        ifNotExists_{ifNotExists},
        properties_{std::move(properties)} {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const std::string& schemaName() const {
    return schemaName_;
  }

  bool ifNotExists() const {
    return ifNotExists_;
  }

  const std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>&
  properties() const {
    return properties_;
  }

 private:
  const std::string connectorId_;
  const std::string schemaName_;
  const bool ifNotExists_;
  const std::unordered_map<std::string, facebook::axiom::logical_plan::ExprPtr>
      properties_;
};

class DropSchemaStatement : public SqlStatement {
 public:
  DropSchemaStatement(
      std::string connectorId,
      std::string schemaName,
      bool ifExists)
      : SqlStatement(SqlStatementKind::kDropSchema),
        connectorId_{std::move(connectorId)},
        schemaName_{std::move(schemaName)},
        ifExists_{ifExists} {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const std::string& schemaName() const {
    return schemaName_;
  }

  bool ifExists() const {
    return ifExists_;
  }

 private:
  const std::string connectorId_;
  const std::string schemaName_;
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

/// Wraps a SELECT statement whose logical plan should be optimized to extract
/// per-column and table-level statistics from the optimizer.
class ShowStatsForQueryStatement : public SqlStatement {
 public:
  explicit ShowStatsForQueryStatement(SqlStatementPtr statement)
      : SqlStatement(SqlStatementKind::kShowStatsForQuery),
        statement_{std::move(statement)} {}

  const SqlStatementPtr& statement() const {
    return statement_;
  }

 private:
  const SqlStatementPtr statement_;
};

/// Sets the default catalog and schema for subsequent queries.
class UseStatement : public SqlStatement {
 public:
  UseStatement(std::optional<std::string> catalog, std::string schema)
      : SqlStatement(SqlStatementKind::kUse),
        catalog_{std::move(catalog)},
        schema_{std::move(schema)} {}

  const std::optional<std::string>& catalog() const {
    return catalog_;
  }

  const std::string& schema() const {
    return schema_;
  }

 private:
  const std::optional<std::string> catalog_;
  const std::string schema_;
};

} // namespace axiom::sql::presto
