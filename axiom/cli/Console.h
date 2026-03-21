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

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
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

/// Holds query metadata captured at query start time.
struct QueryStartInfo {
  /// Unique identifier for this query execution.
  std::string queryId;

  /// SQL text of the query. Owned copy so callbacks can store this safely.
  std::string query;

  /// Wall-clock time when the query was created.
  std::chrono::system_clock::time_point createTime;
};

/// Holds error details when a query fails.
struct ErrorInfo {
  /// Human-readable error message from the caught exception.
  std::string message;
};

/// Holds query metadata at completion time (success or failure).
struct QueryCompletionInfo {
  /// Query identification and timing from start.
  QueryStartInfo startInfo;

  /// Set when the query fails; std::nullopt on success.
  std::optional<ErrorInfo> errorInfo;

  /// Human-readable distributed Velox plan with optimizer cardinality and
  /// memory estimates. Empty for DDL statements.
  std::optional<std::string> planString;

  /// Time spent parsing the SQL statement, in microseconds.
  uint64_t parseMicros{0};

  /// Time spent in the optimizer, in microseconds.
  uint64_t optimizeMicros{0};

  /// Time spent executing the statement (excludes optimization), in
  /// microseconds.
  uint64_t executionMicros{0};

  /// Total wall-clock time from parse through execution, in microseconds.
  /// Includes inter-phase time (e.g. permission checks) not captured by the
  /// per-phase timers.
  uint64_t totalMicros{0};

  /// Number of rows in the query result.
  int64_t numOutputRows{0};

  /// Wall-clock time when the query finished.
  std::chrono::system_clock::time_point endTime;
};

using QueryStartCallback = std::function<void(const QueryStartInfo&)>;
using QueryCompletionCallback = std::function<void(const QueryCompletionInfo&)>;

class Console {
 public:
  /// @param permissionCheck Optional callback invoked after each statement is
  /// parsed but before it is executed. Throws on denial.
  /// @param queryIdGenerator Optional query ID generator. Defaults to a
  /// generator with a random base-32 suffix.
  /// @param startCallback Optional callback invoked before parse, for every
  /// query.
  /// @param completionCallback Optional callback invoked after execution
  /// completes (success or failure).
  explicit Console(
      SqlQueryRunner& runner,
      PermissionCheck permissionCheck = nullptr,
      std::shared_ptr<cli::QueryIdGenerator> queryIdGenerator = nullptr,
      QueryStartCallback startCallback = nullptr,
      QueryCompletionCallback completionCallback = nullptr);

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

  // Invokes startCallback_ if set, swallowing any exceptions.
  void notifyStart(const QueryStartInfo& info);

  // Invokes completionCallback_ if set, swallowing any exceptions.
  void notifyCompletion(const QueryCompletionInfo& info);

  // Reads and executes commands from standard input in interactive mode.
  void readCommands(const std::string& prompt);

  SqlQueryRunner& runner_;
  PermissionCheck permissionCheck_;
  // Generates unique query IDs for each statement execution.
  std::shared_ptr<cli::QueryIdGenerator> queryIdGenerator_;
  // Invoked before parse for every query.
  QueryStartCallback startCallback_;
  // Invoked after execution completes (success or failure).
  QueryCompletionCallback completionCallback_;
};

} // namespace axiom::sql
