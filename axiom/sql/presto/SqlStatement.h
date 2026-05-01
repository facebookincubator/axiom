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

#include "axiom/common/CatalogSchemaTableName.h"
#include "axiom/common/Enums.h"
#include "axiom/logical_plan/LogicalPlanNode.h"

namespace axiom::sql::presto {

/// Map from table identifier to view text.
using ViewMap =
    std::unordered_map<facebook::axiom::CatalogSchemaTableName, std::string>;

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
  kShowSession,
  kSetSession,
  kResetSession,
  kUse,
  kAddColumn,
};

AXIOM_DECLARE_ENUM_NAME(SqlStatementKind);

/// Tables referenced by a SQL statement, extracted during parsing.
/// Each table is a CatalogSchemaTableName with three parts:
///   - catalogName: the connector ID (e.g., "prism", "impulse", "tpch").
///   - schemaTableName.schema: the schema or namespace (e.g., "di", "default").
///   - schemaTableName.table: the table name (e.g., "orders", "lineitem").
///
/// Uses the same CatalogSchemaTableName format as ViewMap keys.
struct ReferencedTables {
  /// Tables read by the query (e.g., FROM, JOIN, subquery sources),
  /// or empty if the query does not read any tables.
  std::unordered_set<facebook::axiom::CatalogSchemaTableName> inputTables;

  /// Table or view modified by the query (e.g., INSERT INTO, CREATE TABLE AS
  /// SELECT, CREATE VIEW, DROP VIEW), or nullopt if the query does not target
  /// any table or view.
  std::optional<facebook::axiom::CatalogSchemaTableName> outputTable;
};

class SqlStatement {
 public:
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

  bool isShowSession() const {
    return kind_ == SqlStatementKind::kShowSession;
  }

  bool isSetSession() const {
    return kind_ == SqlStatementKind::kSetSession;
  }

  bool isResetSession() const {
    return kind_ == SqlStatementKind::kResetSession;
  }

  bool isUse() const {
    return kind_ == SqlStatementKind::kUse;
  }

  bool isAddColumn() const {
    return kind_ == SqlStatementKind::kAddColumn;
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

  /// Tables referenced by this statement. Each table is identified by
  /// connector ID (catalogName), schema, and table name — the same
  /// CatalogSchemaTableName format used as ViewMap keys.
  const ReferencedTables& referencedTables() const {
    return referencedTables_;
  }

 protected:
  explicit SqlStatement(SqlStatementKind kind)
      : kind_{kind}, views_{}, referencedTables_{} {}

  explicit SqlStatement(
      SqlStatementKind kind,
      ViewMap views,
      ReferencedTables referencedTables)
      : kind_{kind},
        views_{std::move(views)},
        referencedTables_{std::move(referencedTables)} {}

 private:
  const SqlStatementKind kind_;
  const ViewMap views_;
  // Tables referenced by this statement, populated during construction.
  const ReferencedTables referencedTables_;
};

using SqlStatementPtr = std::shared_ptr<const SqlStatement>;

class SelectStatement : public SqlStatement {
 public:
  SelectStatement(
      facebook::axiom::logical_plan::LogicalPlanNodePtr plan,
      ViewMap views,
      ReferencedTables referencedTables)
      : SqlStatement(
            SqlStatementKind::kSelect,
            std::move(views),
            std::move(referencedTables)),
        plan_{std::move(plan)} {}

  const facebook::axiom::logical_plan::LogicalPlanNodePtr& plan() const {
    return plan_;
  }

 private:
  const facebook::axiom::logical_plan::LogicalPlanNodePtr plan_;
};

class InsertStatement : public SqlStatement {
 public:
  InsertStatement(
      facebook::axiom::logical_plan::LogicalPlanNodePtr plan,
      ViewMap views,
      ReferencedTables referencedTables)
      : SqlStatement(
            SqlStatementKind::kInsert,
            std::move(views),
            std::move(referencedTables)),
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
      ViewMap views,
      std::unordered_set<facebook::axiom::CatalogSchemaTableName> inputTables);

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
      : SqlStatement(
            SqlStatementKind::kDropTable,
            /*views=*/{},
            ReferencedTables{/*inputTables=*/{},
                             facebook::axiom::CatalogSchemaTableName{
                                 connectorId,
                                 tableName}}),
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

class AddColumnStatement : public SqlStatement {
 public:
  AddColumnStatement(
      std::string connectorId,
      facebook::axiom::SchemaTableName tableName,
      std::string columnName,
      facebook::velox::TypePtr columnType,
      bool ifTableExists,
      bool ifNotExists)
      : SqlStatement(
            SqlStatementKind::kAddColumn,
            /*views=*/{},
            ReferencedTables{/*inputTables=*/{},
                             facebook::axiom::CatalogSchemaTableName{
                                 connectorId,
                                 tableName}}),
        connectorId_{std::move(connectorId)},
        tableName_{std::move(tableName)},
        columnName_{std::move(columnName)},
        columnType_{std::move(columnType)},
        ifTableExists_{ifTableExists},
        ifNotExists_{ifNotExists} {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const facebook::axiom::SchemaTableName& tableName() const {
    return tableName_;
  }

  const std::string& columnName() const {
    return columnName_;
  }

  const facebook::velox::TypePtr& columnType() const {
    return columnType_;
  }

  bool ifTableExists() const {
    return ifTableExists_;
  }

  bool ifNotExists() const {
    return ifNotExists_;
  }

 private:
  const std::string connectorId_;
  const facebook::axiom::SchemaTableName tableName_;
  const std::string columnName_;
  const facebook::velox::TypePtr columnType_;
  const bool ifTableExists_;
  const bool ifNotExists_;
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

    /// Optimized physical plan.
    kOptimized,

    /// Executable Velox plan.
    kExecutable,

    /// IO plan: input/output tables and partition column constraints.
    kIo,
  };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(Type);

  enum class Format {
    kText,
    kGraphviz,
    kJson,
  };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(Format);

  ExplainStatement(
      SqlStatementPtr statement,
      bool analyze,
      Type type,
      Format format)
      : SqlStatement(
            SqlStatementKind::kExplain,
            statement->views(),
            statement->referencedTables()),
        statement_{std::move(statement)},
        analyze_{analyze},
        type_{type},
        format_{format} {}

  const SqlStatementPtr& statement() const {
    return statement_;
  }

  bool isAnalyze() const {
    return analyze_;
  }

  Type type() const {
    return type_;
  }

  Format format() const {
    return format_;
  }

 private:
  const SqlStatementPtr statement_;
  const bool analyze_;
  const Type type_;
  const Format format_;
};

/// Wraps a SELECT statement whose logical plan should be optimized to extract
/// per-column and table-level statistics from the optimizer.
class ShowStatsForQueryStatement : public SqlStatement {
 public:
  explicit ShowStatsForQueryStatement(SqlStatementPtr statement)
      : SqlStatement(
            SqlStatementKind::kShowStatsForQuery,
            statement->views(),
            statement->referencedTables()),
        statement_{std::move(statement)} {}

  const SqlStatementPtr& statement() const {
    return statement_;
  }

 private:
  const SqlStatementPtr statement_;
};

/// Sets the default catalog and schema for subsequent queries.
class ShowSessionStatement : public SqlStatement {
 public:
  explicit ShowSessionStatement(
      std::optional<std::string> likePattern = std::nullopt)
      : SqlStatement(SqlStatementKind::kShowSession),
        likePattern_{std::move(likePattern)} {}

  const std::optional<std::string>& likePattern() const {
    return likePattern_;
  }

 private:
  const std::optional<std::string> likePattern_;
};

class SetSessionStatement : public SqlStatement {
 public:
  SetSessionStatement(std::string name, std::string value)
      : SqlStatement(SqlStatementKind::kSetSession),
        name_{std::move(name)},
        value_{std::move(value)} {}

  const std::string& name() const {
    return name_;
  }

  const std::string& value() const {
    return value_;
  }

 private:
  const std::string name_;
  const std::string value_;
};

class ResetSessionStatement : public SqlStatement {
 public:
  explicit ResetSessionStatement(std::string name)
      : SqlStatement(SqlStatementKind::kResetSession), name_{std::move(name)} {}

  const std::string& name() const {
    return name_;
  }

 private:
  const std::string name_;
};

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

AXIOM_EMBEDDED_ENUM_FORMATTER(axiom::sql::presto::ExplainStatement, Type);
AXIOM_EMBEDDED_ENUM_FORMATTER(axiom::sql::presto::ExplainStatement, Format);
