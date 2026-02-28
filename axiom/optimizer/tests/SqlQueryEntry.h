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

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "axiom/common/Enums.h"

namespace facebook::axiom::optimizer::test {

/// Represents a single SQL query parsed from a test file, along with its
/// assertion type and any annotation parameters.
struct QueryEntry {
  enum class Type { kResults, kOrdered, kCount, kError };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(Type)

  std::string sql;
  Type type{Type::kResults};
  std::optional<std::string> duckDbSql;
  uint64_t expectedCount{0};
  std::string expectedError;
  bool checkColumnNames{false};
  int32_t lineNumber{0};

  /// Parses 'content' into a list of QueryEntry. Queries are separated by
  /// '----'. Comment lines (starting with '-- ') before the SQL are allowed;
  /// recognized annotations are:
  ///   -- ordered         -> assertOrderedResults
  ///   -- count N         -> assertResultCount(sql, N)
  ///   -- error: message  -> assertFailure(sql, "message")
  ///   -- duckdb: sql     -> use alternate SQL for DuckDB comparison
  ///   -- columns         -> verify column names match DuckDB
  ///   -- disabled        -> skip this query
  /// Unrecognized '-- ' lines before SQL starts are treated as comments and
  /// ignored. '-- ' lines after SQL starts are part of the SQL body.
  static std::vector<QueryEntry> parse(const std::string& content);
};

} // namespace facebook::axiom::optimizer::test
