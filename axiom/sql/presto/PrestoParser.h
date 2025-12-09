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

#include "axiom/sql/presto/SqlStatement.h"

namespace axiom::sql::presto {

/// SQL Parser compatible with PrestoSQL dialect.
class PrestoParser {
 public:
  /// @param defaultConnectorId Connector ID to use for tables that do not
  /// specify catalog, i.e. SELECT * FROM schema.name.
  /// @param defaultSchema Optional default schema to use for tables that do not
  /// specify schema, i.e. SELECT * FROM name.
  PrestoParser(
      const std::string& defaultConnectorId,
      const std::optional<std::string>& defaultSchema,
      facebook::velox::memory::MemoryPool* pool)
      : defaultConnectorId_{defaultConnectorId},
        defaultSchema_{defaultSchema},
        pool_{pool} {}

  SqlStatementPtr parse(std::string_view sql, bool enableTracing = false);

  /// Parses multiple semicolon-separated SQL statements.
  /// @param sql SQL text containing one or more statements separated by
  /// semicolons.
  /// @param enableTracing If true, enables tracing for debugging purposes.
  /// @return Vector of parsed statements.
  std::vector<SqlStatementPtr> parseMultiple(
      std::string_view sql,
      bool enableTracing = false);

  facebook::axiom::logical_plan::ExprPtr parseExpression(
      std::string_view sql,
      bool enableTracing = false);

 private:
  SqlStatementPtr doParse(std::string_view sql, bool enableTracing);

  /// Splits SQL text into individual statements by semicolon delimiters.
  /// @param sql SQL text containing one or more statements.
  /// @return Vector of individual SQL statements with semicolons removed.
  static std::vector<std::string> splitStatements(std::string_view sql);

  const std::string defaultConnectorId_;
  const std::optional<std::string> defaultSchema_;
  facebook::velox::memory::MemoryPool* pool_;
};

} // namespace axiom::sql::presto
