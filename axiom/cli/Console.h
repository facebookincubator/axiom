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
#include <memory>
#include <unordered_map>
#include <utility>
#include "axiom/cli/SqlQueryRunner.h"
#include "axiom/sql/presto/SqlStatement.h"

DECLARE_string(data_path);
DECLARE_string(data_format);
DECLARE_bool(debug);

namespace axiom::cli {
class QueryIdGenerator;
} // namespace axiom::cli

namespace axiom::sql {

/// Permission check callback: (queryId, sql, catalog, schema, views) -> throws
/// on denial, and may return a per-query token provider.
/// Empty (nullptr) by default -- no permission checking.
using PermissionCheck =
    std::function<std::shared_ptr<facebook::velox::filesystems::TokenProvider>(
        std::string_view queryId,
        std::string_view sql,
        std::string_view catalog,
        std::optional<std::string_view> schema,
        const presto::ViewMap& views)>;

class Console {
 public:
  /// @param permissionCheck Optional callback invoked after each statement is
  /// parsed but before it is executed. Throws on denial.
  /// @param queryIdGenerator Optional query ID generator. Defaults to a
  /// generator with a random base-32 suffix.
  explicit Console(
      SqlQueryRunner& runner,
      PermissionCheck permissionCheck = nullptr,
      std::shared_ptr<cli::QueryIdGenerator> queryIdGenerator = nullptr);

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
  // Generates unique query IDs for each statement execution.
  std::shared_ptr<cli::QueryIdGenerator> queryIdGenerator_;
};

} // namespace axiom::sql
