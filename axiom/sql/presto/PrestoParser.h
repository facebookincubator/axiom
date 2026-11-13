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
#include <string_view>
#include <unordered_set>

#include "axiom/common/CatalogSchemaTableName.h"
#include "axiom/sql/presto/ParserSession.h"
#include "axiom/sql/presto/SqlStatement.h"
#include "velox/common/base/Exceptions.h"

namespace axiom::sql::presto {

/// Table names read from SQL text without looking any table up.
///
/// Names are canonicalized as parse() canonicalizes them, with the
/// parser defaults applied. A name may name a view, a table that
/// does not exist, or a table a CTE never reads. Names also include
/// metadata sources, such as LIKE and SHOW targets. Use the result
/// only where a table name is a hint, such as routing. Do not use
/// the result where every table a query reads or writes must be
/// known, such as permission checks or lineage.
struct SyntacticTableNames {
  /// Tables the SQL text names for reading.
  std::unordered_set<facebook::axiom::CatalogSchemaTableName> inputTables;

  /// Table the SQL text names as the target of a write, if any.
  std::optional<facebook::axiom::CatalogSchemaTableName> outputTable;

  /// Name of the statement kind. Planner labels name statements
  /// parse() supports, and AST node labels name the rest. The name
  /// has static storage.
  std::string_view kindName;
};

/// SQL Parser compatible with PrestoSQL dialect.
class PrestoParser {
 public:
  /// @param defaultConnectorId Connector ID to use for tables that do not
  /// specify catalog, i.e. SELECT * FROM schema.name.
  /// @param defaultSchema Default schema to use for tables that do not
  /// specify schema, i.e. SELECT * FROM name.
  /// @param session Parser-scoped session used to spawn ConnectorSessions
  /// for parse-time metadata calls.
  PrestoParser(
      const std::string& defaultConnectorId,
      const std::string& defaultSchema,
      ParserSessionPtr session)
      : defaultConnectorId_{defaultConnectorId},
        defaultSchema_{defaultSchema},
        session_{std::move(session)} {
    VELOX_CHECK_NOT_NULL(session_);
  }

  SqlStatementPtr parse(std::string_view sql, bool enableTracing = false);

  /// Extracts table names from SQL text without planning the query
  /// or looking a table up. The session supplies only parser options
  /// and the connector registry used for name canonicalization.
  ///
  /// @param sql SQL text containing one statement.
  /// @return Table names canonicalized as parse() canonicalizes them,
  /// with the parser defaults applied.
  /// @throws PrestoSqlError if the SQL is syntactically invalid.
  /// @throws VeloxRuntimeError for a statement form the AST builder
  /// does not support, such as UPDATE, CREATE VIEW, or ANALYZE.
  SyntacticTableNames extractSyntacticTableNames(std::string_view sql) const;

  /// Parses multiple semicolon-separated SQL statements.
  /// @param sql SQL text containing one or more statements separated by
  /// semicolons.
  /// @param enableTracing If true, enables tracing for debugging purposes.
  /// @return Vector of parsed statements.
  /// @throws PrestoSqlError if any statement fails to parse. The error's
  /// line and column are relative to the full input @p sql, not the individual
  /// sub-statement.
  std::vector<SqlStatementPtr> parseMultiple(
      std::string_view sql,
      bool enableTracing = false);

  /// @throws PrestoSqlError if any statement fails to parse.
  facebook::axiom::logical_plan::ExprPtr parseExpression(
      std::string_view sql,
      bool enableTracing = false);

  /// Splits SQL text into individual statements by semicolon delimiters.
  /// @param sql SQL text containing one or more statements.
  /// @return Vector of string_views into 'sql' for individual SQL statements.
  static std::vector<std::string_view> splitStatements(std::string_view sql);

 private:
  SqlStatementPtr doParse(std::string_view sql, bool enableTracing);

  const std::string defaultConnectorId_;
  const std::string defaultSchema_;
  const ParserSessionPtr session_;
};

} // namespace axiom::sql::presto
