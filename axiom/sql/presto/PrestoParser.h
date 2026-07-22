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

#include "axiom/sql/presto/ParserSession.h"
#include "axiom/sql/presto/SqlStatement.h"
#include "velox/common/base/Exceptions.h"

namespace axiom::sql::presto {

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

/// Returns the literal leading substring of a SQL LIKE 'pattern' up to the
/// first unescaped '%' or '_' wildcard, honoring 'escape'. Returns "" when the
/// pattern starts with a wildcard (e.g. '%foo'), i.e. has no usable prefix.
/// @throws A user error if 'escape' is set but is not a single character.
std::string likePrefix(
    std::string_view pattern,
    const std::optional<std::string>& escape);

} // namespace axiom::sql::presto
