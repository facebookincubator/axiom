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

#include <folly/CancellationToken.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/FunctionScheduler.h>
#include <chrono>
#include <exception>
#include <functional>
#include <vector>
#include "axiom/common/ConfigRegistry.h"
#include "axiom/common/QueryRuntimeStats.h"
#include "axiom/common/SessionConfig.h"
#include "axiom/optimizer/DerivedTable.h"
#include "axiom/optimizer/ToVelox.h"
#include "axiom/runner/LocalRunner.h"
#include "axiom/runner/ProgressReporter.h"
#include "axiom/runner/QueryProgress.h"
#include "axiom/sql/presto/SqlStatement.h"
#include "velox/common/file/TokenProvider.h"

namespace axiom::sql {

/// Checks permissions before query execution. Throws on denial and may return
/// a per-query token provider. No-op (nullptr) by default.
using PermissionCheck =
    std::function<std::shared_ptr<facebook::velox::filesystems::TokenProvider>(
        std::string_view queryId,
        std::string_view sql,
        std::string_view catalog,
        std::optional<std::string_view> schema,
        const presto::ViewMap& views,
        const presto::ReferencedTables& referencedTables)>;

/// Holds query metadata captured at query start time.
struct QueryStartInfo {
  /// Unique identifier for this query execution.
  std::string queryId;

  /// SQL text of the query.
  std::string query;

  /// Wall-clock time when the query was created.
  std::chrono::system_clock::time_point createTime;

  /// Holds the session catalog in effect when the query started.
  std::string catalog;

  /// Holds the session schema in effect when the query started.
  std::string schema;

  /// Statement kind resolved during parsing. Remains empty in the start
  /// callback because parsing happens after onStart fires.
  std::optional<presto::SqlStatementKind> queryType;
};

/// Holds error details when a query fails.
struct ErrorInfo {
  /// Human-readable error message from the caught exception.
  std::string message;

  /// Template with placeholders (e.g. "Cannot resolve type {}") for grouping
  /// similar failures.
  std::string messageTemplate;

  /// Raw Velox error code (e.g. "INVALID_ARGUMENT", "MEM_CAP_EXCEEDED") from
  /// the caught VeloxException. Empty for non-Velox exceptions.
  std::string errorCode;

  /// Raw Velox error source (e.g. "USER", "RUNTIME", "SYSTEM", "EXTERNAL") from
  /// the caught VeloxException. Empty for non-Velox exceptions.
  std::string errorSource;
};

/// Returns a format template (placeholders, not substituted values) for
/// grouping similar failures. A VeloxException with no explicit template
/// synthesizes "Check failed: <expr>" from its failing expression (mirroring
/// QueryError::create); a PrestoSqlError or other exception without a template
/// yields empty, and the caller omits the field. Caveat: VELOX_FAIL with a
/// literal message returns that message, because VeloxException cannot
/// distinguish a literal from a format string; matches AxelQueryLogger.
std::string messageTemplateOf(const std::exception& e);

/// Per-phase wall-clock timing in microseconds, ordered by lifecycle stage.
struct QueryTiming {
  uint64_t onStart{0};
  uint64_t parse{0};
  uint64_t checkPermission{0};
  uint64_t optimize{0};
  uint64_t execute{0};
  /// End-to-end time from query creation through execution, excluding
  /// onComplete. Includes onStart, parse, permission check, optimize,
  /// and execute.
  uint64_t total{0};
};

/// Holds query metadata at completion time (success or failure).
struct QueryCompletionInfo {
  /// Carries query ID, SQL text, and creation timestamp from the start event.
  QueryStartInfo startInfo;

  /// Tables referenced by the parsed statement. Unset when parsing fails.
  std::optional<presto::ReferencedTables> referencedTables;

  /// Contains error details when the query fails; std::nullopt on success.
  std::optional<ErrorInfo> errorInfo;

  /// Serialized Velox execution plan.
  std::string planString;

  /// Per-phase wall-clock and CPU timing.
  QueryTiming timing;

  /// Number of rows in the query result.
  int64_t numOutputRows{0};

  /// Wall-clock time when the query finished, excluding onComplete.
  std::chrono::system_clock::time_point endTime;

  /// Per-component runtime metrics (timing, counters) collected across the
  /// query pipeline. Populated during execution and serialized by loggers.
  std::shared_ptr<facebook::axiom::QueryRuntimeStats> runtimeStats;
};

/// Invoked when a query starts, before parsing.
using QueryStartCallback = std::function<void(const QueryStartInfo&)>;

/// Invoked when a query completes, whether it succeeded or failed.
using QueryCompletionCallback = std::function<void(const QueryCompletionInfo&)>;

/// Executes SQL queries.
class SqlQueryRunner {
 public:
  /// Prefix for optimizer session properties (e.g., "optimizer.sample_joins").
  static constexpr const char* kOptimizerPrefix = "optimizer";

  /// Prefix for runner session properties.
  static constexpr const char* kRunnerPrefix = "runner";

  /// Prefix for parser session properties (e.g., "parser.friendly_sql").
  static constexpr const char* kParserPrefix = "parser";

  /// Prefix for Velox execution session properties (e.g.,
  /// "execution.session_timezone").
  static constexpr const char* kExecutionPrefix = "execution";

  /// Constructs the runner with the given session user identity. The user
  /// must be non-empty; it is exposed by the SQL `CURRENT_USER` function and
  /// used as the default table owner in DDL.
  ///
  /// `progressScheduler` (not owned, must outlive this runner) drives progress
  /// polling for queries that set RunOptions::onProgress. If null, such a query
  /// fails loudly rather than running without the progress it asked for.
  /// @param useOptimizerV2 When true, all queries route through the v2
  /// optimizer. EXPLAIN (type graph|optimized|io) and SHOW STATS FOR (<query>)
  /// fail with a user error under v2, because they inspect optimizer internals
  /// the v2 pipeline does not expose.
  explicit SqlQueryRunner(
      std::string user,
      folly::FunctionScheduler* progressScheduler = nullptr,
      bool useOptimizerV2 = false)
      : user_{std::move(user)},
        useOptimizerV2_{useOptimizerV2},
        progressScheduler_{progressScheduler} {
    VELOX_USER_CHECK(!user_.empty(), "SqlQueryRunner user must be non-empty");
  }

  virtual ~SqlQueryRunner() = default;

  /// Identity of the user running queries. Used as the table owner in DDL and
  /// for the SQL `CURRENT_USER` function.
  const std::string& user() const {
    return user_;
  }

  /// Whether live progress reporting is available, i.e. a progress scheduler
  /// was supplied at construction. A query that sets RunOptions::onProgress
  /// requires this; callers gate progress display on it.
  bool supportsProgress() const {
    return progressScheduler_ != nullptr;
  }

  /// Initializes the runner with connectors, an optional permission check, and
  /// a query ID generator (defaults to QueryIdGenerator if not provided).
  /// Call once before running queries.
  void initialize(
      const std::function<std::pair<std::string, std::string>()>&
          initializeConnectors,
      PermissionCheck permissionCheck = {},
      std::function<std::string()> queryIdGenerator = {});

  struct RunOptions {
    int32_t numWorkers{4};
    int32_t numDrivers{4};
    uint64_t splitTargetBytes{16 << 20};

    /// Cooperative execution deadline in microseconds; 0 means no limit. Bounds
    /// the execution phase only -- not parsing, permission checks, or
    /// optimization -- and is cooperative rather than a hard wall-clock cap: a
    /// non-yielding operator can overrun it, and winding the cancelled query
    /// down adds teardown latency. When exceeded, run() fails with a user
    /// error.
    int64_t timeoutMicros{0};

    /// Cancels the running query from another thread. run() blocks the calling
    /// thread, so a client cancels by holding the paired
    /// folly::CancellationSource and calling requestCancellation() from a
    /// different thread (the CLI from a SIGINT handler; other clients from
    /// their own cancel entry point). Empty by default (no external
    /// cancellation).
    ///
    /// Scope: this interrupts query execution (the drain of results), not
    /// planning. A cancel requested during parse/optimize (or a plan-only
    /// statement such as SHOW STATS) is not observed until execution begins, at
    /// which point the query stops immediately; if the statement never
    /// executes, the cancel has no effect. This matches the cooperative model
    /// -- see also the not-yet-cancelable metastore partition listing in
    /// start().
    folly::CancellationToken cancellationToken;

    std::optional<std::string> defaultConnectorId;
    std::optional<std::string> defaultSchema;

    /// Query ID override. If set, run() uses this value instead of generating
    /// a new one. Useful for correlating queries across systems.
    std::optional<std::string> queryId;

    /// Token provider for authenticated file system access.
    /// Used by runUnchecked() as-is. Overwritten by run() with the result of
    /// the permission check callback.
    std::shared_ptr<facebook::velox::filesystems::TokenProvider> tokenProvider;

    /// Override for session start time (milliseconds since epoch). If not
    /// set, uses the current time. Used by tests to verify current_timestamp.
    std::optional<uint64_t> sessionStartTimeMs;

    /// If true, EXPLAIN ANALYZE output includes custom operator stats.
    bool debugMode{false};

    /// Called before parsing. Receives query metadata (query ID, SQL text,
    /// creation time, and session catalog/schema).
    QueryStartCallback onStart;

    /// Called to report progress while the query runs; see
    /// progressReportIntervalMs for cadence.
    facebook::axiom::runner::QueryProgressCallback onProgress;

    /// Approximate interval between progress reports, in milliseconds (plus a
    /// best-effort final report at completion). Clamped up to
    /// runner::ProgressReporter::kMinProgressReportIntervalMs.
    int32_t progressReportIntervalMs{facebook::axiom::runner::ProgressReporter::
                                         kDefaultProgressReportIntervalMs};

    /// Called after execution completes (success or failure). Carries
    /// telemetry only: timing, query ID, error info, row counts. Results
    /// are returned from run(), not passed through callbacks.
    QueryCompletionCallback onComplete;
  };

  /// Results of running a query. SELECT queries return a vector of results.
  /// Other queries return a message. SELECT query that returns no rows returns
  /// std::nullopt message and empty vector of results.
  struct SqlResult {
    std::optional<std::string> message;
    std::vector<facebook::velox::RowVectorPtr> results;
  };

  /// Runs a single SQL statement with full lifecycle: generates a query ID,
  /// fires onStart, parses, checks permissions, executes, fires onComplete,
  /// and returns the result. On failure, fires onComplete with error telemetry
  /// then re-throws.
  virtual SqlResult run(std::string_view sql, const RunOptions& options);

  /// Runs a single parsed SQL statement without lifecycle hooks (no permission
  /// check, no callbacks). Use run(string_view, RunOptions) for the full
  /// lifecycle.
  SqlResult runUnchecked(
      const presto::SqlStatement& statement,
      const RunOptions& options);

  /// Runs a single SELECT or INSERT statement and returns the LocalRunner. The
  /// caller is responsible for retrieving the results from LocalRunner.
  /// @return LocalRunner executing the provided `statement`.
  /// @throws VeloxUserError if `statement` is not SELECT or INSERT
  std::shared_ptr<facebook::axiom::runner::LocalRunner> executeSelectOrInsert(
      const presto::SqlStatement& statement,
      const RunOptions& options);

  /// Splits SQL text into individual statements separated by semicolons.
  /// @param sql SQL text containing one or more statements.
  /// @return Vector of individual SQL statements.
  std::vector<std::string_view> splitStatements(std::string_view sql);

  /// Parses SQL text containing one or more semicolon-separated statements.
  /// @param sql SQL text to parse.
  /// @return Vector of parsed statements.
  std::vector<presto::SqlStatementPtr> parseMultiple(
      std::string_view sql,
      const RunOptions& options);

  /// Parses SQL text containing one statement.
  /// @return Parsed statement.
  /// @throw VeloxUserError if the SQL text contains multiple statements.
  presto::SqlStatementPtr parseSingle(
      std::string_view sql,
      const RunOptions& options);

  /// Generates DOT representation of the query graph for a single SELECT
  /// statement. The output can be rendered using Graphviz:
  ///   dot -Tsvg output.dot -o output.svg
  /// @param sql A single SELECT or EXPLAIN SELECT statement.
  std::string toQueryGraphDot(std::string_view sql);

  /// Generates DOT representation of the logical plan for a single SELECT
  /// statement. The output can be rendered using Graphviz:
  ///   dot -Tsvg output.dot -o output.svg
  /// @param sql A single SELECT or EXPLAIN SELECT statement.
  std::string toLogicalPlanDot(std::string_view sql);

  /// Generates DOT representation of the multi-fragment plan for a single
  /// SELECT statement. The output can be rendered using Graphviz:
  ///   dot -Tsvg output.dot -o output.svg
  /// @param sql A single SELECT or EXPLAIN SELECT statement.
  /// @param numWorkers Number of workers for distributed planning.
  /// @param numDrivers Number of drivers per worker.
  std::string toMultiFragmentPlanDot(
      std::string_view sql,
      int32_t numWorkers,
      int32_t numDrivers);

  /// Returns the session configuration.
  facebook::axiom::SessionConfig& sessionConfig() {
    return *sessionConfig_;
  }

  facebook::axiom::connector::TablePtr createTable(
      std::string_view queryId,
      const presto::CreateTableStatement& statement,
      bool explain = false);

  facebook::axiom::connector::TablePtr createTable(
      std::string_view queryId,
      const presto::CreateTableAsSelectStatement& statement,
      bool explain = false);

  std::string dropTable(
      std::string_view queryId,
      const presto::DropTableStatement& statement);

  std::string addColumn(
      std::string_view queryId,
      const presto::AddColumnStatement& statement,
      bool explain = false);

  std::string createSchema(
      std::string_view queryId,
      const presto::CreateSchemaStatement& statement);

  std::string dropSchema(
      std::string_view queryId,
      const presto::DropSchemaStatement& statement);

  /// Returns the default connector ID set during initialization.
  const std::string& defaultConnectorId() const {
    return defaultConnectorId_;
  }

  /// Returns the default schema set during initialization.
  const std::string& defaultSchema() const {
    return defaultSchema_;
  }

 private:
  // Checks permissions for the query via the configured PermissionCheck
  // callback. Returns a TokenProvider for authenticated file system access.
  std::shared_ptr<facebook::velox::filesystems::TokenProvider> checkPermission(
      const RunOptions& options,
      QueryCompletionInfo& completionInfo,
      const presto::ViewMap& views,
      const presto::ReferencedTables& referencedTables);

  std::shared_ptr<facebook::velox::core::QueryCtx> newQuery(
      const RunOptions& options);

  std::string runExplain(
      const facebook::axiom::logical_plan::LogicalPlanNodePtr& logicalPlan,
      presto::ExplainStatement::Type type,
      presto::ExplainStatement::Format format,
      const RunOptions& options,
      QueryTiming& timing,
      std::shared_ptr<facebook::axiom::QueryRuntimeStats> runtimeStats,
      std::shared_ptr<facebook::axiom::connector::SchemaResolver>
          schemaResolver = nullptr);

  std::string runExplainAnalyze(
      const facebook::axiom::logical_plan::LogicalPlanNodePtr& logicalPlan,
      const RunOptions& options,
      QueryTiming& timing,
      std::shared_ptr<facebook::axiom::QueryRuntimeStats> runtimeStats,
      std::shared_ptr<facebook::axiom::connector::SchemaResolver>
          schemaResolver = nullptr);

  // Runs SHOW STATS FOR (<query>): optimizes the inner query's logical plan
  // and returns per-column and table-level statistics as a VALUES result.
  std::vector<facebook::velox::RowVectorPtr> runShowStatsForQuery(
      const presto::SqlStatement& sqlStatement,
      const RunOptions& options);

  // Parses SQL and returns the logical plan.
  facebook::axiom::logical_plan::LogicalPlanNodePtr toLogicalPlan(
      std::string_view sql);

  // Optimizes provided logical plan.
  // @param checkDerivedTable Optional lambda to call after to-graph stage of
  // optimization. If returns 'false', the optimization stops and returns an
  // empty result.
  // @param checkBestPlan Optional lambda to call towards the end of
  // optimization after best plan is found. If returns 'false', the optimization
  // stops and returns an empty result.
  facebook::axiom::optimizer::PlanAndStats optimize(
      const facebook::axiom::logical_plan::LogicalPlanNodePtr& logicalPlan,
      const std::shared_ptr<facebook::velox::core::QueryCtx>& queryCtx,
      const RunOptions& options,
      const std::function<bool(
          const facebook::axiom::optimizer::DerivedTable&)>& checkDerivedTable =
          nullptr,
      const std::function<bool(const facebook::axiom::optimizer::RelationOp&)>&
          checkBestPlan = nullptr,
      std::shared_ptr<facebook::axiom::connector::SchemaResolver>
          schemaResolver = nullptr,
      bool explain = false,
      std::shared_ptr<facebook::axiom::QueryRuntimeStats> runtimeStats =
          nullptr);

  std::shared_ptr<facebook::axiom::runner::LocalRunner> makeLocalRunner(
      facebook::axiom::optimizer::PlanAndStats& planAndStats,
      const std::shared_ptr<facebook::velox::core::QueryCtx>& queryCtx,
      const RunOptions& options,
      facebook::axiom::QueryRuntimeStats& runtimeStats);

  // Returns a ProgressReporter polling `runner` (starting the shared scheduler
  // first) when options.onProgress is set, otherwise nullptr. Held behind a
  // unique_ptr because ProgressReporter is non-movable; keep it in scope for as
  // long as progress should be reported.
  std::unique_ptr<facebook::axiom::runner::ProgressReporter>
  startProgressReporter(
      facebook::axiom::runner::Runner& runner,
      std::string_view queryId,
      const RunOptions& options);

  // Runs a parsed SQL statement, writing optimize/execute timing into 'timing'
  // and the serialized Velox plan into 'planString'.
  SqlResult runUnchecked(
      const presto::SqlStatement& statement,
      const RunOptions& options,
      QueryTiming& timing,
      std::string& planString,
      std::shared_ptr<facebook::axiom::QueryRuntimeStats> runtimeStats =
          nullptr);

  SqlResult showSession(
      const presto::ShowSessionStatement& statement,
      const RunOptions& options,
      QueryTiming& timing,
      std::string& planString);

  // Optimizes and executes a logical plan. Writes timing and plan string
  // directly into the passed-in references so values survive exceptions.
  SqlResult runLogicalPlan(
      const facebook::axiom::logical_plan::LogicalPlanNodePtr& logicalPlan,
      const RunOptions& options,
      QueryTiming& timing,
      std::string& planString,
      std::shared_ptr<facebook::axiom::connector::SchemaResolver>
          schemaResolver = nullptr,
      std::shared_ptr<facebook::axiom::QueryRuntimeStats> runtimeStats =
          nullptr);

  // Builds a ConnectorSession for `connectorId` carrying the caller's
  // queryId, the runner's user, and the connector's effective session
  // properties from `sessionConfig_`.
  facebook::axiom::connector::ConnectorSessionPtr makeConnectorSession(
      std::string_view queryId,
      std::string_view connectorId) const;

  // Permission check callback invoked before query execution.
  PermissionCheck permissionCheck_;

  // Generates unique query IDs.
  std::function<std::string()> queryIdGenerator_;
  std::shared_ptr<facebook::velox::cache::AsyncDataCache> cache_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> optimizerPool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> executorPool_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<facebook::axiom::ConfigRegistry> configRegistry_;
  std::shared_ptr<facebook::axiom::SessionConfig> sessionConfig_;
  std::string defaultConnectorId_;
  std::string defaultSchema_;
  const std::string user_;
  std::atomic<int32_t> queryCounter_{0};
  const bool useOptimizerV2_{false};

  // Progress-polling scheduler (see constructor). Started idempotently before
  // each progress-reporting query.
  folly::FunctionScheduler* const progressScheduler_;

  // Noop stats instance for code paths that don't track runtime metrics.
  facebook::axiom::QueryRuntimeStats noopRuntimeStats_;
};

} // namespace axiom::sql
