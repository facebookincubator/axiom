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

#include <folly/container/F14Map.h>
#include "axiom/common/CatalogSchemaTableName.h"
#include "axiom/common/Enums.h"
#include "axiom/common/SchemaProcedureName.h"
#include "axiom/connectors/ConnectorSession.h"
#include "axiom/connectors/TableDefinition.h"
#include "axiom/connectors/VectorIndex.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/logical_plan/ReferencedTables.h"
#include "velox/type/Variant.h"

namespace facebook::axiom::connector {
struct Procedure;
using ProcedurePtr = std::shared_ptr<const Procedure>;
} // namespace facebook::axiom::connector

namespace axiom::sql::presto {

/// Map from table identifier to view text.
using ViewMap =
    std::unordered_map<facebook::axiom::CatalogSchemaTableName, std::string>;

enum class SqlStatementKind {
  kSelect,
  kCreateTable,
  kCreateTableAsSelect,
  kInsert,
  kRefreshVectorIndex,
  kDelete,
  kDropTable,
  kAddVectorIndex,
  kDropVectorIndex,
  kCreateSchema,
  kDropSchema,
  kExplain,
  kShowStatsForQuery,
  kShowSession,
  kSetSession,
  kResetSession,
  kUse,
  kAddColumn,
  kCall,
};

AXIOM_DECLARE_ENUM_NAME(SqlStatementKind);

class SqlStatement {
 public:
  virtual ~SqlStatement() = default;

  std::string_view kindName() const;

  /// Returns whether running this statement changes data or metadata.
  facebook::axiom::connector::QueryKind queryKind() const;

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

  bool isRefreshVectorIndex() const {
    return kind_ == SqlStatementKind::kRefreshVectorIndex;
  }

  bool isDelete() const {
    return kind_ == SqlStatementKind::kDelete;
  }

  bool isDropTable() const {
    return kind_ == SqlStatementKind::kDropTable;
  }

  bool isAddVectorIndex() const {
    return kind_ == SqlStatementKind::kAddVectorIndex;
  }

  bool isDropVectorIndex() const {
    return kind_ == SqlStatementKind::kDropVectorIndex;
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

  bool isCall() const {
    return kind_ == SqlStatementKind::kCall;
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
  const facebook::axiom::logical_plan::ReferencedTables& referencedTables()
      const {
    return referencedTables_;
  }

 protected:
  explicit SqlStatement(SqlStatementKind kind)
      : kind_{kind}, views_{}, referencedTables_{} {}

  explicit SqlStatement(
      SqlStatementKind kind,
      ViewMap views,
      facebook::axiom::logical_plan::ReferencedTables referencedTables)
      : kind_{kind},
        views_{std::move(views)},
        referencedTables_{std::move(referencedTables)} {}

 private:
  const SqlStatementKind kind_;
  const ViewMap views_;
  // Tables referenced by this statement, populated during construction.
  const facebook::axiom::logical_plan::ReferencedTables referencedTables_;
};

using SqlStatementPtr = std::shared_ptr<const SqlStatement>;

class SelectStatement : public SqlStatement {
 public:
  SelectStatement(
      facebook::axiom::logical_plan::LogicalPlanNodePtr plan,
      ViewMap views,
      facebook::axiom::logical_plan::ReferencedTables referencedTables)
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
      facebook::axiom::logical_plan::ReferencedTables referencedTables)
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

/// REFRESH VECTOR INDEX: builds a vector index for the partitions of its table
/// a condition selects. A host runs it as one index write (see
/// connector::ConnectorMetadata::co_beginVectorIndexWrite): it begins the
/// write, runs the statement's plans in order with the write's codec id as
/// the value of ?1, committing each before the next starts, and finishes the
/// write, or aborts it on a failure. The statement's result is the number of
/// embeddings the write wrote, which the finish returns.
class RefreshVectorIndexStatement : public SqlStatement {
 public:
  /// The plans of the refresh, in the order a host runs them. Each has one
  /// placeholder, ?1 of type BIGINT, the codec id of the index write (see
  /// connector::VectorIndexWriteHandle).
  struct Plans {
    /// Trains a codec for each partition the refresh reads and writes them to
    /// the codec table.
    facebook::axiom::logical_plan::LogicalPlanNodePtr trainPlan;
    /// Assigns each row to the nearest cluster of its partition's codec,
    /// builds the local indexes and writes them to the index table.
    facebook::axiom::logical_plan::LogicalPlanNodePtr buildPlan;
  };

  RefreshVectorIndexStatement(
      std::string connectorId,
      facebook::axiom::connector::VectorIndexPtr index,
      Plans plans,
      ViewMap views,
      facebook::axiom::logical_plan::ReferencedTables referencedTables)
      : SqlStatement(
            SqlStatementKind::kRefreshVectorIndex,
            std::move(views),
            std::move(referencedTables)),
        connectorId_{std::move(connectorId)},
        index_{std::move(index)},
        plans_{std::move(plans)} {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const facebook::axiom::connector::VectorIndexPtr& index() const {
    return index_;
  }

  const Plans& plans() const {
    return plans_;
  }

 private:
  const std::string connectorId_;
  const facebook::axiom::connector::VectorIndexPtr index_;
  const Plans plans_;
};

/// DELETE statement. 'plan' is a TableWriteNode with WriteKind::kDelete over
/// the filtered scan of the target table.
class DeleteStatement : public SqlStatement {
 public:
  DeleteStatement(
      facebook::axiom::logical_plan::LogicalPlanNodePtr plan,
      ViewMap views,
      facebook::axiom::logical_plan::ReferencedTables referencedTables)
      : SqlStatement(
            SqlStatementKind::kDelete,
            std::move(views),
            std::move(referencedTables)),
        plan_{std::move(plan)} {}

  const facebook::axiom::logical_plan::LogicalPlanNodePtr& plan() const {
    return plan_;
  }

 private:
  const facebook::axiom::logical_plan::LogicalPlanNodePtr plan_;
};

/// Evaluates table or column properties, expressions that look constant, to
/// their values.
using PropertyEvaluator =
    std::function<folly::F14FastMap<std::string, facebook::velox::Variant>(
        const folly::
            F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>&)>;

/// A column CREATE TABLE or ADD COLUMN declares. Its properties are
/// unevaluated, as a table's are (see CreateTableStatement::properties()).
struct ColumnDeclaration {
  /// The column's name.
  std::string name;

  /// The column's type.
  facebook::velox::TypePtr type;

  /// False for a column declared NOT NULL.
  bool nullable{true};

  /// Nullopt when the column has no COMMENT.
  std::optional<std::string> comment;

  /// The column's WITH properties, by name, as constant expressions.
  folly::F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>
      properties;

  /// Returns the definition to create the column with, its properties
  /// evaluated by 'evaluate'.
  facebook::axiom::connector::ColumnDefinition definition(
      const PropertyEvaluator& evaluate) const;
};

/// A vector index CREATE TABLE or ALTER TABLE ADD VECTOR INDEX declares. Its
/// properties are unevaluated, as a table's are (see
/// CreateTableStatement::properties()).
struct VectorIndexDeclaration {
  /// The index's name.
  std::string name;

  /// The column the index indexes.
  std::string column;

  /// The KEY columns. Empty without KEY.
  std::vector<std::string> key;

  /// The index's WITH properties, by name, as constant expressions.
  folly::F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>
      properties;

  /// Returns the definition to add the index with, its properties evaluated by
  /// 'evaluate'.
  facebook::axiom::connector::VectorIndexDefinition definition(
      const PropertyEvaluator& evaluate) const;
};

class CreateTableStatement : public SqlStatement {
 public:
  /// @param primaryKey The columns of PRIMARY KEY, each a column of 'columns'
  /// once. Empty without PRIMARY KEY.
  /// @param vectorIndexes The VECTOR INDEX elements, with names unique among
  /// them.
  CreateTableStatement(
      std::string connectorId,
      facebook::axiom::SchemaTableName tableName,
      std::vector<ColumnDeclaration> columns,
      std::optional<std::string> comment,
      folly::F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>
          properties,
      std::vector<std::string> primaryKey,
      std::vector<VectorIndexDeclaration> vectorIndexes,
      bool ifNotExists);

  const std::string& connectorId() const {
    return connectorId_;
  }

  const facebook::axiom::SchemaTableName& tableName() const {
    return tableName_;
  }

  /// Returns the columns. Their names are case-insensitively unique: 'ID' and
  /// 'id' are not permitted together.
  const std::vector<ColumnDeclaration>& columns() const {
    return columns_;
  }

  /// Returns the columns' names and types.
  const facebook::velox::RowTypePtr& tableSchema() const {
    return tableSchema_;
  }

  /// Nullopt when the table has no COMMENT.
  const std::optional<std::string>& comment() const {
    return comment_;
  }

  /// Returns the table properties as a map from property name to expression.
  /// Properties are connector-specific settings (e.g., partitioned_by, format)
  /// which set additional table attributes. An expression type is used because
  /// some properties (e.g., ARRAY['ds']) require runtime evaluation.
  const folly::F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>&
  properties() const {
    return properties_;
  }

  /// Returns the columns of PRIMARY KEY, spelled as declared. Empty without
  /// PRIMARY KEY.
  const std::vector<std::string>& primaryKey() const {
    return primaryKey_;
  }

  const std::vector<VectorIndexDeclaration>& vectorIndexes() const {
    return vectorIndexes_;
  }

  /// Create the table only if a table of the same name does not exist.
  bool ifNotExists() const {
    return ifNotExists_;
  }

  /// Returns the definition to create the table with, its properties and its
  /// columns' properties evaluated by 'evaluate'.
  facebook::axiom::connector::TableDefinition definition(
      const PropertyEvaluator& evaluate) const;

 private:
  const std::string connectorId_;
  const facebook::axiom::SchemaTableName tableName_;
  const std::vector<ColumnDeclaration> columns_;
  const facebook::velox::RowTypePtr tableSchema_;
  const std::optional<std::string> comment_;
  folly::F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>
      properties_;
  const std::vector<std::string> primaryKey_;
  const std::vector<VectorIndexDeclaration> vectorIndexes_;
  const bool ifNotExists_;
};

class CreateTableAsSelectStatement : public SqlStatement {
 public:
  CreateTableAsSelectStatement(
      std::string connectorId,
      facebook::axiom::SchemaTableName tableName,
      facebook::velox::RowTypePtr tableSchema,
      std::optional<std::string> comment,
      folly::F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>
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

  /// Nullopt when the table has no COMMENT.
  const std::optional<std::string>& comment() const {
    return comment_;
  }

  /// See CreateTableStatement::properties() for usage.
  const folly::F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>&
  properties() const {
    return properties_;
  }

  const facebook::axiom::logical_plan::LogicalPlanNodePtr& plan() const {
    return plan_;
  }

  /// Returns the definition to create the table with: the query's columns,
  /// nullable, and the comment and properties, evaluated by 'evaluate'.
  facebook::axiom::connector::TableDefinition definition(
      const PropertyEvaluator& evaluate) const;

 private:
  const std::string connectorId_;
  const facebook::axiom::SchemaTableName tableName_;
  const facebook::velox::RowTypePtr tableSchema_;
  const std::optional<std::string> comment_;
  folly::F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>
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
            facebook::axiom::logical_plan::ReferencedTables{
                /*inputTables=*/{},
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

/// ALTER TABLE ADD VECTOR INDEX.
class AddVectorIndexStatement : public SqlStatement {
 public:
  AddVectorIndexStatement(
      std::string connectorId,
      facebook::axiom::SchemaTableName tableName,
      VectorIndexDeclaration index,
      bool ifTableExists,
      bool ifNotExists);

  const std::string& connectorId() const {
    return connectorId_;
  }

  const facebook::axiom::SchemaTableName& tableName() const {
    return tableName_;
  }

  const VectorIndexDeclaration& index() const {
    return index_;
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
  const VectorIndexDeclaration index_;
  const bool ifTableExists_;
  const bool ifNotExists_;
};

/// ALTER TABLE DROP VECTOR INDEX.
class DropVectorIndexStatement : public SqlStatement {
 public:
  DropVectorIndexStatement(
      std::string connectorId,
      facebook::axiom::SchemaTableName tableName,
      std::string name,
      bool ifTableExists,
      bool ifExists)
      : SqlStatement(
            SqlStatementKind::kDropVectorIndex,
            /*views=*/{},
            facebook::axiom::logical_plan::ReferencedTables{
                /*inputTables=*/{},
                facebook::axiom::CatalogSchemaTableName{
                    connectorId,
                    tableName}}),
        connectorId_{std::move(connectorId)},
        tableName_{std::move(tableName)},
        name_{std::move(name)},
        ifTableExists_{ifTableExists},
        ifExists_{ifExists} {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const facebook::axiom::SchemaTableName& tableName() const {
    return tableName_;
  }

  /// The index's name.
  const std::string& name() const {
    return name_;
  }

  bool ifTableExists() const {
    return ifTableExists_;
  }

  bool ifExists() const {
    return ifExists_;
  }

 private:
  const std::string connectorId_;
  const facebook::axiom::SchemaTableName tableName_;
  const std::string name_;
  const bool ifTableExists_;
  const bool ifExists_;
};

class AddColumnStatement : public SqlStatement {
 public:
  AddColumnStatement(
      std::string connectorId,
      facebook::axiom::SchemaTableName tableName,
      ColumnDeclaration column,
      bool ifTableExists,
      bool ifNotExists)
      : SqlStatement(
            SqlStatementKind::kAddColumn,
            /*views=*/{},
            facebook::axiom::logical_plan::ReferencedTables{
                /*inputTables=*/{},
                facebook::axiom::CatalogSchemaTableName{
                    connectorId,
                    tableName}}),
        connectorId_{std::move(connectorId)},
        tableName_{std::move(tableName)},
        column_{std::move(column)},
        ifTableExists_{ifTableExists},
        ifNotExists_{ifNotExists} {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const facebook::axiom::SchemaTableName& tableName() const {
    return tableName_;
  }

  /// Returns the column to add.
  const ColumnDeclaration& column() const {
    return column_;
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
  const ColumnDeclaration column_;
  const bool ifTableExists_;
  const bool ifNotExists_;
};

/// A stored-procedure invocation. Resolved at parse time: the procedure is
/// looked up on its connector and the call's arguments are bound to its
/// parameters (matched positionally or by name, coerced to the declared
/// parameter types, defaults filled) in declared parameter order, as constant
/// expressions. The runner folds them to values; the connector then executes
/// the procedure directly. Produces no rows.
class CallStatement : public SqlStatement {
 public:
  CallStatement(
      std::string connectorId,
      facebook::axiom::SchemaProcedureName procedureName,
      facebook::axiom::connector::ProcedurePtr procedure,
      std::vector<facebook::axiom::logical_plan::ExprPtr> arguments)
      : SqlStatement(SqlStatementKind::kCall),
        connectorId_{std::move(connectorId)},
        procedureName_{std::move(procedureName)},
        procedure_{std::move(procedure)},
        arguments_{std::move(arguments)} {}

  const std::string& connectorId() const {
    return connectorId_;
  }

  const facebook::axiom::SchemaProcedureName& procedureName() const {
    return procedureName_;
  }

  /// Bound arguments as constant expressions, in declared parameter order.
  const std::vector<facebook::axiom::logical_plan::ExprPtr>& arguments() const {
    return arguments_;
  }

  const facebook::axiom::connector::ProcedurePtr& procedure() const {
    return procedure_;
  }

 private:
  const std::string connectorId_;
  const facebook::axiom::SchemaProcedureName procedureName_;
  const facebook::axiom::connector::ProcedurePtr procedure_;
  const std::vector<facebook::axiom::logical_plan::ExprPtr> arguments_;
};

class CreateSchemaStatement : public SqlStatement {
 public:
  CreateSchemaStatement(
      std::string connectorId,
      std::string schemaName,
      bool ifNotExists,
      folly::F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>
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

  const folly::F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>&
  properties() const {
    return properties_;
  }

 private:
  const std::string connectorId_;
  const std::string schemaName_;
  const bool ifNotExists_;
  const folly::F14FastMap<std::string, facebook::axiom::logical_plan::ExprPtr>
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

    /// Parse and analyze only, reporting whether the query is valid. Produces
    /// no plan.
    kValidate,
  };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(Type);

  enum class Format {
    kText,
    kGraphviz,
    kJson,
  };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(Format);

  /// Settings written as `TYPE <type> WITH (name = 'value')`, keyed by name.
  /// What a name means, and which types accept one, is for the engine running
  /// the EXPLAIN to decide; the parser only carries them.
  using Settings = folly::F14FastMap<std::string, std::string>;

  ExplainStatement(
      SqlStatementPtr statement,
      bool analyze,
      Type type,
      Format format,
      Settings settings = {})
      : SqlStatement(
            SqlStatementKind::kExplain,
            statement->views(),
            statement->referencedTables()),
        statement_{std::move(statement)},
        analyze_{analyze},
        type_{type},
        format_{format},
        settings_{std::move(settings)} {}

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

  const Settings& settings() const {
    return settings_;
  }

 private:
  const SqlStatementPtr statement_;
  const bool analyze_;
  const Type type_;
  const Format format_;
  const Settings settings_;
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
  SetSessionStatement(std::string name, std::string value, std::string valueSql)
      : SqlStatement(SqlStatementKind::kSetSession),
        name_{std::move(name)},
        value_{std::move(value)},
        valueSql_{std::move(valueSql)} {}

  const std::string& name() const {
    return name_;
  }

  /// The value to store, with a string literal's quotes stripped.
  const std::string& value() const {
    return value_;
  }

  /// The value rendered as a SQL literal: a string keeps its quotes, a
  /// number the shortest spelling that reads back the same.
  const std::string& valueSql() const {
    return valueSql_;
  }

 private:
  const std::string name_;
  const std::string value_;
  const std::string valueSql_;
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

AXIOM_ENUM_FORMATTER(axiom::sql::presto::SqlStatementKind);
AXIOM_EMBEDDED_ENUM_FORMATTER(axiom::sql::presto::ExplainStatement, Type);
AXIOM_EMBEDDED_ENUM_FORMATTER(axiom::sql::presto::ExplainStatement, Format);
