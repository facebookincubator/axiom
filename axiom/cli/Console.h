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

#include <functional>
#include <unordered_map>
#include <utility>
#include "axiom/cli/SqlQueryRunner.h"

DECLARE_string(data_path);
DECLARE_string(data_format);
DECLARE_bool(debug);

namespace axiom::sql {

/// Function that generates a unique query ID string.
using QueryIdGenerator = std::function<std::string()>;

/// Permission check callback: (queryId, sql, catalog, schema, views) -> throws
/// on denial. Empty (nullptr) by default -- no permission checking.
using PermissionCheck = std::function<void(
    std::string_view queryId,
    std::string_view sql,
    std::string_view catalog,
    std::optional<std::string_view> schema,
    const std::unordered_map<std::pair<std::string, std::string>, std::string>&
        views)>;

class Console {
 public:
  /// @param permissionCheck Optional callback invoked after each statement is
  /// parsed but before it is executed. Throws on denial.
  /// @param queryIdGenerator Optional callback that produces a unique query ID.
  /// Defaults to a randomly-generated suffix based generator.
  explicit Console(
      SqlQueryRunner& runner,
      PermissionCheck permissionCheck = nullptr,
      QueryIdGenerator queryIdGenerator = nullptr);

  /// Initializes the CLI with usage message and logging settings.
  void initialize();

  /// Runs the CLI, either executing a single query if passed in
  /// or entering interactive mode to read commands from stdin.
  void run();

 private:
  // Executes SQL and prints results, catching any exceptions.
  // @param sql The SQL text to execute, which may contain multiple
  // semicolon-separated statements.
  // @param isInteractive If true, shows timing after each statement for
  // multi-statement queries.
  void runNoThrow(std::string_view sql, bool isInteractive = true);

  // Reads and executes commands from standard input in interactive mode.
  void readCommands(const std::string& prompt);

  SqlQueryRunner& runner_;
  PermissionCheck permissionCheck_;
  QueryIdGenerator queryIdGenerator_;
};

} // namespace axiom::sql
