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

#include "axiom/cli/SqlQueryRunner.h"
#include <fmt/ranges.h>
#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/container/F14Map.h>
#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Coroutine.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/Task.h>
#include <folly/coro/Timeout.h>
#include <folly/coro/WithCancellation.h>
#include <folly/system/HardwareConcurrency.h>
#include <algorithm>
#include <cmath>
#include <functional>
#include <memory>
#include <optional>
#include <vector>
#include "velox/common/base/VeloxException.h"
#include "velox/common/process/ProcessBase.h"

#include "axiom/cli/QueryIdGenerator.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/graphviz/DerivedTableDotPrinter.h"
#include "axiom/graphviz/LogicalPlanDotPrinter.h"
#include "axiom/graphviz/MultiFragmentPlanDotPrinter.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/optimizer/ConstantExprEvaluator.h"
#include "axiom/optimizer/DerivedTablePrinter.h"
#include "axiom/optimizer/ExplainIo.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/OptimizerOptions.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/RelationOpPrinter.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/optimizer/v2/Optimize.h"
#include "axiom/runner/ProgressReporter.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "axiom/sql/presto/PrestoSqlError.h"
#include "axiom/sql/presto/ShowStatsBuilder.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryConfigProvider.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/PrestoConfigProvider.h"
#include "velox/functions/prestosql/PrestoQueryConfig.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace velox = facebook::velox;
using namespace facebook::axiom;
using connector::ConnectorMetadataRegistry;

namespace {

// Times a synchronous phase and records its wall and calling-thread CPU
// durations.
class PhaseTimer {
 public:
  PhaseTimer(
      uint64_t& wallMicros,
      QueryRuntimeStats* runtimeStats,
      std::string_view wallKey,
      std::string_view cpuKey)
      : runtimeStats_(runtimeStats),
        wallKey_(wallKey),
        cpuKey_(cpuKey),
        wallMicros_(wallMicros),
        cpuStartNanos_(velox::process::threadCpuNanos()),
        timer_(std::in_place, &wallMicros) {}

  ~PhaseTimer() {
    // Destroy the wall timer first so wallMicros_ is finalized before we
    // read it below.
    timer_.reset();
    if (runtimeStats_ != nullptr) {
      runtimeStats_->recordTiming(
          wallKey_, std::chrono::microseconds(wallMicros_));
      const auto cpuNs = velox::process::threadCpuNanos() - cpuStartNanos_;
      runtimeStats_->recordTiming(cpuKey_, std::chrono::nanoseconds(cpuNs));
    }
  }

  PhaseTimer(const PhaseTimer&) = delete;
  PhaseTimer& operator=(const PhaseTimer&) = delete;
  PhaseTimer(PhaseTimer&&) = delete;
  PhaseTimer& operator=(PhaseTimer&&) = delete;

 private:
  QueryRuntimeStats* const runtimeStats_;
  const std::string_view wallKey_;
  const std::string_view cpuKey_;
  uint64_t& wallMicros_;
  const uint64_t cpuStartNanos_;
  std::optional<velox::MicrosecondTimer> timer_;
};

// Wraps a Velox connector's ConfigProvider pointer into an owned
// ConfigProvider for use with ConfigRegistry. The underlying connector
// must outlive this wrapper.
class ConnectorConfigProvider : public velox::config::ConfigProvider {
 public:
  explicit ConnectorConfigProvider(
      const velox::config::ConfigProvider* provider)
      : provider_(provider) {}

  std::vector<velox::config::ConfigProperty> properties() const override {
    return provider_->properties();
  }

  std::string normalize(std::string_view name, std::string_view value)
      const override {
    return provider_->normalize(name, value);
  }

 private:
  const velox::config::ConfigProvider* provider_;
};

// Returns the system's local IANA timezone name. Checks TZ environment variable
// first, then reads /etc/localtime symlink, falls back to "UTC".
std::string getLocalTimezone() {
  // Check TZ environment variable first.
  if (const char* tz = std::getenv("TZ"); tz != nullptr && tz[0] != '\0') {
    // TZ may be a POSIX spec (e.g. "EST5EDT") or an IANA name. Validate it.
    if (velox::tz::locateZone(tz, false) != nullptr) {
      return tz;
    }
  }

  // Read /etc/localtime symlink (e.g. /usr/share/zoneinfo/America/Los_Angeles).
  std::array<char, 256> buf{};
  auto len = readlink("/etc/localtime", buf.data(), buf.size() - 1);
  if (len > 0) {
    std::string_view target(buf.data(), len);
    const std::string_view kZoneInfo = "zoneinfo/";
    auto pos = target.rfind(kZoneInfo);
    if (pos != std::string_view::npos) {
      auto name = target.substr(pos + kZoneInfo.size());
      if (velox::tz::locateZone(name, false) != nullptr) {
        return std::string(name);
      }
    }
  }

  return "UTC";
}

// Owns the optimizer QueryGraphContext for its lifetime and installs it as the
// thread-local queryCtx(), clearing it on destruction.
class OptimizerContext {
 public:
  explicit OptimizerContext(velox::memory::MemoryPool* pool)
      : allocator_(std::make_unique<velox::HashStringAllocator>(pool)),
        context_(std::make_unique<optimizer::QueryGraphContext>(*allocator_)) {
    optimizer::queryCtx() = context_.get();
  }

  ~OptimizerContext() {
    optimizer::queryCtx() = nullptr;
  }

  OptimizerContext(const OptimizerContext&) = delete;
  OptimizerContext& operator=(const OptimizerContext&) = delete;

 private:
  std::unique_ptr<velox::HashStringAllocator> allocator_;
  std::unique_ptr<optimizer::QueryGraphContext> context_;
};

// Returns 'schemaResolver' if non-null, else a default resolver over the global
// connector metadata registry.
std::shared_ptr<connector::SchemaResolver> orDefaultSchemaResolver(
    std::shared_ptr<connector::SchemaResolver> schemaResolver) {
  if (schemaResolver != nullptr) {
    return schemaResolver;
  }
  return std::make_shared<connector::SchemaResolver>(
      connector::ConnectorMetadataRegistry::global());
}

// Returns the destination table for an EXPLAIN (TYPE IO) over an INSERT or
// CTAS, or nullopt for a plain SELECT.
std::optional<CatalogSchemaTableName> explainIoOutputTable(
    const axiom::sql::presto::SqlStatement& statement,
    const logical_plan::LogicalPlanNode& plan) {
  if (statement.isCreateTableAsSelect()) {
    const auto* ctas =
        statement.as<axiom::sql::presto::CreateTableAsSelectStatement>();
    return CatalogSchemaTableName{ctas->connectorId(), ctas->tableName()};
  }

  if (statement.isInsert()) {
    const auto* writeNode = plan.as<logical_plan::TableWriteNode>();
    return CatalogSchemaTableName{
        writeNode->connectorId(), writeNode->tableName()};
  }

  return std::nullopt;
}

} // namespace

namespace axiom::sql {

void SqlQueryRunner::initialize(
    const std::function<std::pair<std::string, std::string>()>&
        initializeConnectors,
    PermissionCheck permissionCheck,
    std::function<std::string()> queryIdGenerator) {
  static folly::once_flag kInitialized;

  folly::call_once(kInitialized, []() {
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::aggregate::prestosql::registerAllAggregateFunctions();
    velox::window::prestosql::registerAllWindowFunctions();
    velox::parse::registerTypeResolver();

    optimizer::FunctionRegistry::registerPrestoFunctions();

    velox::filesystems::registerLocalFileSystem();

    velox::exec::ExchangeSource::registerFactory(
        velox::exec::test::createLocalExchangeSource);
    if (!velox::isRegisteredVectorSerde()) {
      velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    velox::serializer::presto::PrestoVectorSerde::tryRegisterNamedVectorSerde();
  });

  static std::atomic<int32_t> kCounter{0};

  rootPool_ = velox::memory::memoryManager()->addRootPool(
      fmt::format("axiom_sql{}", kCounter++));
  optimizerPool_ = rootPool_->addLeafChild("optimizer");
  executorPool_ = rootPool_->addLeafChild("executor");

  auto [defaultConnectorId, defaultSchema] = initializeConnectors();
  defaultConnectorId_ = std::move(defaultConnectorId);
  defaultSchema_ = std::move(defaultSchema);

  permissionCheck_ = std::move(permissionCheck);

  if (queryIdGenerator) {
    queryIdGenerator_ = std::move(queryIdGenerator);
  } else {
    auto generator = std::make_shared<cli::QueryIdGenerator>();
    queryIdGenerator_ = [generator]() {
      return generator->createNextQueryId();
    };
  }

  configRegistry_ = std::make_shared<facebook::axiom::ConfigRegistry>();
  configRegistry_->add(
      kOptimizerPrefix,
      std::make_shared<facebook::axiom::optimizer::OptimizerOptions>());
  configRegistry_->add(
      kParserPrefix, std::make_shared<presto::ParserOptions>());
  configRegistry_->add(
      kExecutionPrefix,
      std::make_shared<facebook::velox::core::QueryConfigProvider>());
  configRegistry_->add(
      velox::functions::prestosql::PrestoQueryConfig::kPrefix,
      std::make_shared<
          facebook::velox::functions::prestosql::PrestoConfigProvider>());

  // Register config providers for connectors that support session properties.
  for (const auto& [connectorId, connector] :
       velox::connector::ConnectorRegistry::global().snapshot()) {
    if (const auto* provider = connector->configProvider()) {
      configRegistry_->add(
          connectorId, std::make_shared<ConnectorConfigProvider>(provider));
    }
  }

  sessionConfig_ =
      std::make_shared<facebook::axiom::SessionConfig>(configRegistry_);
  sessionConfig_->set(
      kExecutionPrefix,
      velox::core::QueryConfig::kSessionTimezone,
      getLocalTimezone());
  sessionConfig_->set(
      kExecutionPrefix,
      velox::core::QueryConfig::kAdjustTimestampToTimezone,
      "true");
  sessionConfig_->set(
      velox::functions::prestosql::PrestoQueryConfig::kPrefix,
      velox::functions::prestosql::PrestoQueryConfig::kArrayAggIgnoreNulls,
      "true");
}

namespace {

// Returns per-connector property maps for connectors with at least one
// effective value set.
connector::ConnectorProperties collectConnectorProperties(
    const SessionConfig& config) {
  connector::ConnectorProperties result;
  for (const auto& id :
       connector::ConnectorMetadataRegistry::allMetadataIds()) {
    connector::Properties properties = config.effectiveValues(id);
    if (!properties.empty()) {
      result.emplace(id, std::move(properties));
    }
  }
  return result;
}

// Fires the start callback if set, swallowing exceptions.
void onStart(
    const sql::SqlQueryRunner::RunOptions& options,
    sql::QueryCompletionInfo& completionInfo) {
  if (options.onStart) {
    velox::MicrosecondTimer t(&completionInfo.timing.onStart);
    try {
      options.onStart(completionInfo.startInfo);
    } catch (const std::exception& ex) {
      LOG(WARNING) << "Start callback failed: " << ex.what();
    }
  }
}

// Fires the completion callback if set, swallowing exceptions.
void onComplete(
    const sql::SqlQueryRunner::RunOptions& options,
    const sql::QueryCompletionInfo& completionInfo) {
  if (options.onComplete) {
    try {
      options.onComplete(completionInfo);
    } catch (const std::exception& ex) {
      LOG(WARNING) << "Completion callback failed: " << ex.what();
    }
  }
}

} // namespace

connector::TablePtr SqlQueryRunner::createTable(
    std::string_view queryId,
    const presto::CreateTableStatement& statement,
    bool explain) {
  auto metadata = ConnectorMetadataRegistry::get(statement.connectorId());

  folly::F14FastMap<std::string, velox::Variant> options;
  for (const auto& [key, value] : statement.properties()) {
    options[key] =
        optimizer::ConstantExprEvaluator::evaluateConstantExpr(*value);
  }

  auto session = makeConnectorSession(queryId, statement.connectorId());
  auto table = metadata->createTable(
      session,
      statement.tableName(),
      statement.tableSchema(),
      options,
      statement.ifNotExists(),
      explain);
  VELOX_CHECK(table != nullptr || statement.ifNotExists());

  // Some connectors only stage the table in createTable and commit it in
  // finishWrite. Run an empty create-write so the table is persisted.
  if (table != nullptr && !explain) {
    auto handle = metadata->beginWrite(
        session, table, connector::WriteKind::kCreate, /*explain=*/false);
    // TODO: Make the commit timeout configurable (e.g. via a DdlOptions).
    constexpr std::chrono::seconds kCommitTimeout{60};
    try {
      // Await the write so the table is committed, and any failure surfaced,
      // before returning. The row count is unused for an empty create-write.
      metadata
          ->finishWrite(
              session,
              handle,
              /*writeResults=*/{},
              /*groupingKeys=*/nullptr,
              /*groupStats=*/{})
          .get(kCommitTimeout);
    } catch (...) {
      // Best-effort abort so a failed commit does not leak connector-side
      // state; await it so an async connector actually runs the abort. The
      // original error still propagates.
      try {
        metadata->abortWrite(session, handle).get(kCommitTimeout);
      } catch (...) {
        // Ignore abort failures; the original error is the useful one.
      }
      throw;
    }
  }
  return table;
}

connector::TablePtr SqlQueryRunner::createTable(
    std::string_view queryId,
    const presto::CreateTableAsSelectStatement& statement,
    bool explain) {
  auto metadata = ConnectorMetadataRegistry::get(statement.connectorId());

  folly::F14FastMap<std::string, velox::Variant> options;
  for (const auto& [key, value] : statement.properties()) {
    options[key] =
        optimizer::ConstantExprEvaluator::evaluateConstantExpr(*value);
  }

  auto table = metadata->createTable(
      makeConnectorSession(queryId, statement.connectorId()),
      statement.tableName(),
      statement.tableSchema(),
      options,
      /*ifNotExists=*/false,
      explain);
  VELOX_CHECK_NOT_NULL(table);
  return table;
}

std::string SqlQueryRunner::dropTable(
    std::string_view queryId,
    const presto::DropTableStatement& statement) {
  auto metadata = ConnectorMetadataRegistry::get(statement.connectorId());

  const auto& tableName = statement.tableName();

  const bool dropped = metadata->dropTable(
      makeConnectorSession(queryId, statement.connectorId()),
      statement.tableName(),
      statement.ifExists(),
      /*explain=*/false);

  if (dropped) {
    return fmt::format("Dropped table: {}", tableName);
  } else {
    return fmt::format("Table doesn't exist: {}", tableName);
  }
}

folly::coro::Task<std::string> SqlQueryRunner::co_call(
    std::string_view queryId,
    const presto::CallStatement& statement) {
  // Fold each bound argument expression to a value.
  std::vector<velox::Variant> boundArguments;
  boundArguments.reserve(statement.arguments().size());
  for (const auto& argument : statement.arguments()) {
    boundArguments.push_back(
        optimizer::ConstantExprEvaluator::evaluateConstantExpr(*argument));
  }

  // Procedure implementations may retain references to their call arguments in
  // a lazy coroutine, so procedure, session, and boundArguments must all
  // outlive the awaited task; hold them in named locals rather than awaiting a
  // temporary.
  auto procedure = statement.procedure();
  auto session = makeConnectorSession(queryId, statement.connectorId());
  auto task = procedure->execute(session, boundArguments);
  co_await std::move(task);
  co_return "CALL";
}

std::string SqlQueryRunner::addColumn(
    std::string_view queryId,
    const presto::AddColumnStatement& statement,
    bool explain) {
  auto metadata = ConnectorMetadataRegistry::get(statement.connectorId());

  auto result = metadata->addColumn(
      makeConnectorSession(queryId, statement.connectorId()),
      statement.tableName(),
      statement.columnName(),
      statement.columnType(),
      statement.ifTableExists(),
      statement.ifNotExists(),
      explain);

  if (!result.has_value()) {
    return fmt::format(
        "Table does not exist: {}.{}",
        statement.connectorId(),
        statement.tableName());
  }
  if (!result.value()) {
    return fmt::format(
        "Column '{}' already exists in {} (no-op)",
        statement.columnName(),
        statement.tableName());
  }
  return fmt::format(
      "Added column '{}' to {}", statement.columnName(), statement.tableName());
}

std::string SqlQueryRunner::createSchema(
    std::string_view queryId,
    const presto::CreateSchemaStatement& statement) {
  auto metadata = ConnectorMetadataRegistry::get(statement.connectorId());

  folly::F14FastMap<std::string, velox::Variant> properties;
  for (const auto& [key, value] : statement.properties()) {
    properties[key] =
        optimizer::ConstantExprEvaluator::evaluateConstantExpr(*value);
  }

  metadata->createSchema(
      makeConnectorSession(queryId, statement.connectorId()),
      statement.schemaName(),
      statement.ifNotExists(),
      properties);
  return fmt::format("Created schema: {}", statement.schemaName());
}

std::string SqlQueryRunner::dropSchema(
    std::string_view queryId,
    const presto::DropSchemaStatement& statement) {
  auto metadata = ConnectorMetadataRegistry::get(statement.connectorId());
  metadata->dropSchema(
      makeConnectorSession(queryId, statement.connectorId()),
      statement.schemaName(),
      statement.ifExists());
  return fmt::format("Dropped schema: {}", statement.schemaName());
}

std::string messageTemplateOf(const std::exception& e) {
  if (const auto* veloxError = dynamic_cast<const velox::VeloxException*>(&e)) {
    // An empty template means no explicit format string (e.g. messageless
    // VELOX_CHECK); key it by the failing expression so these still group.
    if (veloxError->messageTemplate().empty()) {
      const auto& expression = veloxError->failingExpression();
      return expression.empty() ? ""
                                : fmt::format("Check failed: {}", expression);
    }
    return std::string(veloxError->messageTemplate());
  }
  if (const auto* prestoError =
          dynamic_cast<const presto::PrestoSqlError*>(&e)) {
    return std::string(prestoError->messageTemplate());
  }
  return "";
}

namespace {

// Finalizes query telemetry for successful, cancelled, and failed executions.
class QueryFinalizer {
 public:
  QueryFinalizer(
      const SqlQueryRunner::RunOptions& options,
      QueryCompletionInfo& completionInfo)
      : options_{options}, completionInfo_{completionInfo} {}

  // Records the output row count and completes a successful query.
  void succeed(int64_t numOutputRows) {
    completionInfo_.numOutputRows = numOutputRows;
    finalize();
  }

  // Marks the query as cancelled and completes its telemetry.
  void cancel() {
    completionInfo_.cancelled = true;
    finalize();
  }

  // Records a Velox failure and completes the query telemetry.
  void fail(const velox::VeloxException& error) {
    completionInfo_.errorInfo = ErrorInfo{
        .message = error.what(),
        .messageTemplate = messageTemplateOf(error),
        .errorCode = error.errorCode(),
        .errorSource = error.errorSource()};
    finalize();
  }

  // Records a Presto SQL failure and completes the query telemetry.
  void fail(const presto::PrestoSqlError& error) {
    const auto classification = presto::classify(error.kind());
    completionInfo_.errorInfo = ErrorInfo{
        .message = error.what(),
        .messageTemplate = messageTemplateOf(error),
        .errorCode = std::string(classification.errorCode),
        .errorSource = std::string(classification.errorSource)};
    finalize();
  }

  // Records a failure without a structured error code or source.
  void fail(const std::exception& error) {
    completionInfo_.errorInfo = ErrorInfo{
        .message = error.what(), .messageTemplate = messageTemplateOf(error)};
    finalize();
  }

 private:
  // Captures end-to-end timing and fires the completion callback.
  void finalize() {
    completionInfo_.endTime = std::chrono::system_clock::now();
    completionInfo_.timing.total =
        std::chrono::duration_cast<std::chrono::microseconds>(
            completionInfo_.endTime - completionInfo_.startInfo.createTime)
            .count();
    onComplete(options_, completionInfo_);
  }

  // Non-owning run options held by the enclosing query coroutine.
  const SqlQueryRunner::RunOptions& options_;

  // Non-owning completion record held by the enclosing query coroutine.
  QueryCompletionInfo& completionInfo_;
};

} // namespace

SqlQueryRunner::SqlResult SqlQueryRunner::run(
    std::string_view sql,
    const RunOptions& options) {
  // Compose the caller's cancellation token onto the drain so tripping it stops
  // the query (surfacing QueryCancelledError). A default token never cancels.
  return folly::coro::blockingWait(
      folly::coro::co_withCancellation(
          options.cancellationToken,
          folly::coro::co_invoke([&]() -> folly::coro::Task<SqlResult> {
            SqlResult result;
            auto generator = co_run(std::string(sql), options);
            while (auto chunk = co_await generator.next()) {
              if (chunk->message.has_value()) {
                result.message = std::move(chunk->message);
              }
              if (chunk->batch != nullptr) {
                result.results.push_back(std::move(chunk->batch));
              }
            }
            co_return result;
          })));
}

folly::coro::AsyncGenerator<SqlQueryRunner::SqlResultChunk>
SqlQueryRunner::co_run(std::string sql, RunOptions options) {
  auto runOptions = std::move(options);
  runOptions.queryId = runOptions.queryId.value_or(queryIdGenerator_());
  const auto& catalog =
      runOptions.defaultConnectorId.value_or(defaultConnectorId_);
  const auto& schema = runOptions.defaultSchema.value_or(defaultSchema_);

  QueryCompletionInfo completionInfo{
      .startInfo = {
          *runOptions.queryId,
          std::string(sql),
          std::chrono::system_clock::now(),
          std::string(catalog),
          std::string(schema),
          std::nullopt}};
  completionInfo.runtimeStats = std::make_shared<QueryRuntimeStats>();

  onStart(runOptions, completionInfo);
  QueryFinalizer finalizer{runOptions, completionInfo};

  int64_t numOutputRows{0};
  try {
    presto::SqlStatementPtr statement;
    uint64_t parseCpuNanos;
    {
      auto cpuStart = velox::process::threadCpuNanos();
      velox::MicrosecondTimer parseTimer(&completionInfo.timing.parse);
      statement = parseSingle(sql, runOptions);
      parseCpuNanos = velox::process::threadCpuNanos() - cpuStart;
    }
    completionInfo.startInfo.queryType = statement->kind();
    completionInfo.referencedTables = statement->referencedTables();
    completionInfo.runtimeStats->recordTiming(
        QueryRuntimeStats::kParseWallNanos,
        std::chrono::microseconds(completionInfo.timing.parse));
    completionInfo.runtimeStats->recordTiming(
        QueryRuntimeStats::kParseCpuNanos,
        std::chrono::nanoseconds(parseCpuNanos));

    {
      auto permissionCpuStart = velox::process::threadCpuNanos();
      runOptions.tokenProvider = checkPermission(
          runOptions,
          completionInfo,
          statement->views(),
          statement->referencedTables());
      completionInfo.runtimeStats->recordTiming(
          QueryRuntimeStats::kPermissionCheckCpuNanos,
          std::chrono::nanoseconds(
              velox::process::threadCpuNanos() - permissionCpuStart));
    }
    completionInfo.runtimeStats->recordTiming(
        QueryRuntimeStats::kPermissionCheckWallNanos,
        std::chrono::microseconds(completionInfo.timing.checkPermission));

    auto generator = co_runUnchecked(
        *statement,
        runOptions,
        completionInfo.timing,
        completionInfo.planString,
        completionInfo.runtimeStats);
    while (auto chunk = co_await generator.next()) {
      if (chunk->batch != nullptr) {
        numOutputRows += chunk->batch->size();
      }
      co_yield std::move(*chunk);
    }

    finalizer.succeed(numOutputRows);
  } catch (const folly::OperationCancelled&) {
    // Cancellation from the awaiting scope's token (any async path -- the drain
    // or a CALL procedure) is a benign stop: record no errorInfo, mark it
    // cancelled so telemetry can tell it apart from an empty success, finalize,
    // and surface the dedicated type so the caller reports it distinctly.
    finalizer.cancel();
    throw QueryCancelledError{};
  } catch (const velox::VeloxException& e) {
    finalizer.fail(e);
    throw;
  } catch (const presto::PrestoSqlError& e) {
    finalizer.fail(e);
    throw;
  } catch (const std::exception& e) {
    finalizer.fail(e);
    throw;
  }
}

std::string SqlQueryRunner::toQueryGraphDot(std::string_view sql) {
  const auto logicalPlan = toLogicalPlan(sql);

  std::string dotOutput;
  RunOptions options;
  optimize(logicalPlan, newQuery(options), options, [&](const auto& dt) {
    std::ostringstream out;
    graphviz::DerivedTableDotPrinter::print(dt, out);
    dotOutput = out.str();
    return false; // Stop optimization.
  });
  return dotOutput;
}

std::string SqlQueryRunner::toLogicalPlanDot(std::string_view sql) {
  const auto logicalPlan = toLogicalPlan(sql);

  std::ostringstream out;
  graphviz::LogicalPlanDotPrinter::print(*logicalPlan, out);
  return out.str();
}

std::string SqlQueryRunner::toMultiFragmentPlanDot(
    std::string_view sql,
    int32_t numWorkers,
    int32_t numDrivers) {
  const auto logicalPlan = toLogicalPlan(sql);

  RunOptions options;
  options.numWorkers = numWorkers;
  options.numDrivers = numDrivers;
  auto queryCtx = newQuery(options);
  auto planAndStats = optimize(logicalPlan, queryCtx, options);
  VELOX_CHECK_NOT_NULL(planAndStats.plan);

  std::ostringstream out;
  graphviz::MultiFragmentPlanDotPrinter::print(
      *planAndStats.plan, planAndStats.prediction, out);
  return out.str();
}

logical_plan::LogicalPlanNodePtr SqlQueryRunner::toLogicalPlan(
    std::string_view sql) {
  RunOptions options;
  auto statements = parseMultiple(sql, options);
  VELOX_CHECK_EQ(statements.size(), 1, "Expected a single SELECT statement.");

  auto statement = statements[0];
  if (statement->isExplain()) {
    statement = statement->as<presto::ExplainStatement>()->statement();
  }

  VELOX_CHECK(
      statement->isSelect(),
      "Expected SELECT or EXPLAIN SELECT statement, got: {}",
      statement->kindName());

  return statement->as<presto::SelectStatement>()->plan();
}

std::vector<std::string_view> SqlQueryRunner::splitStatements(
    std::string_view sql) {
  return presto::PrestoParser::splitStatements(sql);
}

std::vector<presto::SqlStatementPtr> SqlQueryRunner::parseMultiple(
    std::string_view sql,
    const RunOptions& options) {
  const std::string& defaultConnectorId =
      options.defaultConnectorId.value_or(defaultConnectorId_);
  const auto& defaultSchema = options.defaultSchema.value_or(defaultSchema_);

  auto parserSession = std::make_shared<presto::ParserSession>(
      options.queryId.value_or(queryIdGenerator_()),
      user_,
      presto::ParserOptions::from(
          sessionConfig_->effectiveValues(kParserPrefix)),
      collectConnectorProperties(*sessionConfig_));
  auto prestoParser = std::make_unique<presto::PrestoParser>(
      defaultConnectorId, defaultSchema, std::move(parserSession));
  return prestoParser->parseMultiple(sql, /*enableTracing=*/options.debugMode);
}

presto::SqlStatementPtr SqlQueryRunner::parseSingle(
    std::string_view sql,
    const RunOptions& options) {
  std::vector<presto::SqlStatementPtr> statements = parseMultiple(sql, options);
  VELOX_USER_CHECK_EQ(
      statements.size(),
      1,
      "Expected a single statement. "
      "If you want to run multiple statements, use parseMultiple().");
  return statements.front();
}

SqlQueryRunner::SqlResult SqlQueryRunner::runUnchecked(
    const presto::SqlStatement& sqlStatement,
    const RunOptions& options) {
  QueryTiming timing;
  std::string planString;
  return folly::coro::blockingWait(
      folly::coro::co_invoke([&]() -> folly::coro::Task<SqlResult> {
        SqlResult result;
        auto generator =
            co_runUnchecked(sqlStatement, options, timing, planString);
        while (auto chunk = co_await generator.next()) {
          if (chunk->message.has_value()) {
            result.message = std::move(chunk->message);
          }
          if (chunk->batch != nullptr) {
            result.results.push_back(std::move(chunk->batch));
          }
        }
        co_return result;
      }));
}

folly::coro::AsyncGenerator<SqlQueryRunner::SqlResultChunk>
SqlQueryRunner::co_runExplainStatement(
    const presto::ExplainStatement& explain,
    std::string_view queryId,
    const RunOptions& options,
    QueryTiming& timing,
    std::shared_ptr<QueryRuntimeStats> runtimeStats) {
  const auto& statement = explain.statement();

  logical_plan::LogicalPlanNodePtr logicalPlan;
  std::shared_ptr<connector::SchemaResolver> schemaResolver;

  if (statement->isSelect()) {
    logicalPlan = statement->as<presto::SelectStatement>()->plan();
  } else if (statement->isInsert()) {
    logicalPlan = statement->as<presto::InsertStatement>()->plan();
  } else if (statement->isCreateTableAsSelect()) {
    const auto* ctas = statement->as<presto::CreateTableAsSelectStatement>();
    logicalPlan = ctas->plan();

    // EXPLAIN ANALYZE runs the query for real, so createTable must not
    // be in explain mode. Regular EXPLAIN must be side-effect-free.
    auto table = createTable(queryId, *ctas, /*explain=*/!explain.isAnalyze());
    schemaResolver = std::make_shared<connector::SchemaResolver>(
        connector::ConnectorMetadataRegistry::global());
    schemaResolver->setTargetTable(
        ctas->connectorId(), ctas->tableName(), table);
  } else if (statement->isCreateTable()) {
    const auto* create = statement->as<presto::CreateTableStatement>();
    createTable(queryId, *create, /*explain=*/true);
    co_yield SqlResultChunk{fmt::format(
        "CREATE TABLE {}{}.{}",
        create->ifNotExists() ? "IF NOT EXISTS " : "",
        create->connectorId(),
        create->tableName())};
    co_return;
  } else if (statement->isDropTable()) {
    const auto* drop = statement->as<presto::DropTableStatement>();
    if (!drop->ifExists()) {
      auto metadata = ConnectorMetadataRegistry::get(drop->connectorId());
      VELOX_USER_CHECK(
          metadata->findTable(drop->tableName()),
          "Table does not exist: {}.{}",
          drop->connectorId(),
          drop->tableName());
    }
    co_yield SqlResultChunk{fmt::format(
        "DROP TABLE {}{}.{}",
        drop->ifExists() ? "IF EXISTS " : "",
        drop->connectorId(),
        drop->tableName())};
    co_return;
  } else if (statement->isAddColumn()) {
    const auto* add = statement->as<presto::AddColumnStatement>();
    addColumn(queryId, *add, /*explain=*/true);
    co_yield SqlResultChunk{fmt::format(
        "ALTER TABLE {}{}.{} ADD COLUMN {}{} {}",
        add->ifTableExists() ? "IF EXISTS " : "",
        add->connectorId(),
        add->tableName(),
        add->ifNotExists() ? "IF NOT EXISTS " : "",
        add->columnName(),
        add->columnType()->toString())};
    co_return;
  } else if (statement->isCall()) {
    // EXPLAIN must be side-effect-free: echo the resolved call without
    // invoking the procedure.
    const auto* call = statement->as<presto::CallStatement>();

    std::vector<std::string> arguments;
    arguments.reserve(call->arguments().size());
    for (const auto& argument : call->arguments()) {
      // TODO: render arguments as Presto SQL (add Expr::toSql); toString()
      // emits debug form (e.g. array_constructor(...)), not valid SQL.
      arguments.push_back(argument->toString());
    }

    co_yield SqlResultChunk{fmt::format(
        "CALL {}.{}({})",
        call->connectorId(),
        call->procedureName(),
        fmt::join(arguments, ", "))};
    co_return;
  } else {
    VELOX_NYI("Unsupported EXPLAIN query: {}", statement->kindName());
  }

  if (explain.type() == presto::ExplainStatement::Type::kIo) {
    co_yield SqlResultChunk{runExplainIo(
        *statement,
        logicalPlan,
        options,
        timing,
        schemaResolver,
        runtimeStats)};
    co_return;
  }

  if (explain.isAnalyze()) {
    co_yield SqlResultChunk{co_await co_runExplainAnalyze(
        logicalPlan, options, timing, runtimeStats, schemaResolver)};
    co_return;
  }
  co_yield SqlResultChunk{runExplain(
      logicalPlan,
      explain.type(),
      explain.format(),
      options,
      timing,
      runtimeStats,
      schemaResolver)};
}

folly::coro::AsyncGenerator<SqlQueryRunner::SqlResultChunk>
SqlQueryRunner::co_runPlanStatement(
    const presto::SqlStatement& sqlStatement,
    std::string_view queryId,
    const RunOptions& options,
    QueryTiming& timing,
    std::string& planString,
    std::shared_ptr<QueryRuntimeStats> runtimeStats) {
  // Keep the plan in this coroutine frame because co_runLogicalPlan takes it
  // by const reference and pulls batches lazily.
  logical_plan::LogicalPlanNodePtr logicalPlan;
  std::shared_ptr<connector::SchemaResolver> schemaResolver;

  if (sqlStatement.isCreateTableAsSelect()) {
    const auto* ctas = sqlStatement.as<presto::CreateTableAsSelectStatement>();
    auto table = createTable(queryId, *ctas);

    schemaResolver = std::make_shared<connector::SchemaResolver>(
        connector::ConnectorMetadataRegistry::global());
    schemaResolver->setTargetTable(
        ctas->connectorId(), ctas->tableName(), table);
    logicalPlan = ctas->plan();
  } else if (sqlStatement.isInsert()) {
    logicalPlan = sqlStatement.as<presto::InsertStatement>()->plan();
  } else if (sqlStatement.isSelect()) {
    logicalPlan = sqlStatement.as<presto::SelectStatement>()->plan();
  } else {
    VELOX_UNREACHABLE("Unexpected plan statement: {}", sqlStatement.kindName());
  }

  auto generator = co_runLogicalPlan(
      logicalPlan, options, timing, planString, schemaResolver, runtimeStats);
  while (auto batch = co_await generator.next()) {
    co_yield SqlResultChunk{std::move(*batch)};
  }
}

std::string SqlQueryRunner::runDataDefinitionStatement(
    const presto::SqlStatement& sqlStatement,
    std::string_view queryId) {
  if (sqlStatement.isCreateTable()) {
    const auto* create = sqlStatement.as<presto::CreateTableStatement>();
    auto table = createTable(queryId, *create);
    if (!table) {
      return fmt::format(
          "Table already exists: {}.{}",
          create->connectorId(),
          create->tableName());
    }
    return fmt::format("Created table: {}", create->tableName());
  }

  if (sqlStatement.isDropTable()) {
    const auto* drop = sqlStatement.as<presto::DropTableStatement>();
    return dropTable(queryId, *drop);
  }

  if (sqlStatement.isAddColumn()) {
    const auto* add = sqlStatement.as<presto::AddColumnStatement>();
    return addColumn(queryId, *add);
  }

  if (sqlStatement.isCreateSchema()) {
    const auto* create = sqlStatement.as<presto::CreateSchemaStatement>();
    return createSchema(queryId, *create);
  }

  if (sqlStatement.isDropSchema()) {
    const auto* drop = sqlStatement.as<presto::DropSchemaStatement>();
    return dropSchema(queryId, *drop);
  }

  VELOX_UNREACHABLE(
      "Unexpected data definition statement: {}", sqlStatement.kindName());
}

folly::coro::AsyncGenerator<SqlQueryRunner::SqlResultChunk>
SqlQueryRunner::co_runSessionStatement(
    const presto::SqlStatement& sqlStatement,
    const RunOptions& options,
    QueryTiming& timing,
    std::string& planString) {
  if (sqlStatement.isShowSession()) {
    auto generator = co_showSession(
        *sqlStatement.as<presto::ShowSessionStatement>(),
        options,
        timing,
        planString);
    while (auto batch = co_await generator.next()) {
      co_yield SqlResultChunk{std::move(*batch)};
    }
    co_return;
  }

  if (sqlStatement.isSetSession()) {
    const auto* setSession = sqlStatement.as<presto::SetSessionStatement>();
    const auto& name = setSession->name();
    sessionConfig_->set(name, setSession->value());
    co_yield SqlResultChunk{
        fmt::format("Session '{}' set to '{}'", name, setSession->value())};
    co_return;
  }

  if (sqlStatement.isResetSession()) {
    const auto* resetSession = sqlStatement.as<presto::ResetSessionStatement>();
    const auto& name = resetSession->name();
    sessionConfig_->reset(name);
    co_yield SqlResultChunk{fmt::format("Session '{}' reset", name)};
    co_return;
  }

  if (sqlStatement.isUse()) {
    const auto* use = sqlStatement.as<presto::UseStatement>();
    const auto& connectorId = use->catalog().has_value()
        ? use->catalog().value()
        : defaultConnectorId_;
    VELOX_USER_CHECK(
        ConnectorMetadataRegistry::tryGet(connectorId) != nullptr,
        "Catalog does not exist: {}",
        connectorId);
    defaultConnectorId_ = connectorId;
    defaultSchema_ = use->schema();
    co_yield SqlResultChunk{
        fmt::format("Using {}.{}", defaultConnectorId_, use->schema())};
    co_return;
  }

  VELOX_UNREACHABLE(
      "Unexpected session statement: {}", sqlStatement.kindName());
}

folly::coro::AsyncGenerator<SqlQueryRunner::SqlResultChunk>
SqlQueryRunner::co_runUnchecked(
    const presto::SqlStatement& sqlStatement,
    const RunOptions& options,
    QueryTiming& timing,
    std::string& planString,
    std::shared_ptr<QueryRuntimeStats> runtimeStats) {
  const std::string queryId = options.queryId.value_or(queryIdGenerator_());

  if (sqlStatement.isExplain()) {
    auto generator = co_runExplainStatement(
        *sqlStatement.as<presto::ExplainStatement>(),
        queryId,
        options,
        timing,
        runtimeStats);
    while (auto chunk = co_await generator.next()) {
      co_yield std::move(*chunk);
    }
    co_return;
  }

  if (sqlStatement.isCreateTableAsSelect() || sqlStatement.isInsert()) {
    auto generator = co_runPlanStatement(
        sqlStatement, queryId, options, timing, planString, runtimeStats);
    while (auto chunk = co_await generator.next()) {
      co_yield std::move(*chunk);
    }
    co_return;
  }

  if (sqlStatement.isCreateTable() || sqlStatement.isDropTable() ||
      sqlStatement.isAddColumn() || sqlStatement.isCreateSchema() ||
      sqlStatement.isDropSchema()) {
    co_yield SqlResultChunk{runDataDefinitionStatement(sqlStatement, queryId)};
    co_return;
  }

  if (sqlStatement.isCall()) {
    const auto* call = sqlStatement.as<presto::CallStatement>();
    co_yield SqlResultChunk{co_await co_call(queryId, *call)};
    co_return;
  }

  if (sqlStatement.isShowStatsForQuery()) {
    auto results = runShowStatsForQuery(sqlStatement, options);
    for (auto& batch : results) {
      co_yield SqlResultChunk{std::move(batch)};
    }
    co_return;
  }

  if (sqlStatement.isShowSession() || sqlStatement.isSetSession() ||
      sqlStatement.isResetSession() || sqlStatement.isUse()) {
    auto generator =
        co_runSessionStatement(sqlStatement, options, timing, planString);
    while (auto chunk = co_await generator.next()) {
      co_yield std::move(*chunk);
    }
    co_return;
  }

  VELOX_CHECK(sqlStatement.isSelect());
  auto generator = co_runPlanStatement(
      sqlStatement, queryId, options, timing, planString, runtimeStats);
  while (auto chunk = co_await generator.next()) {
    co_yield std::move(*chunk);
  }
}

std::shared_ptr<velox::core::QueryCtx> SqlQueryRunner::newQuery(
    const RunOptions& options) {
  executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(std::max<int32_t>(
      folly::available_concurrency() * 2,
      options.numWorkers * options.numDrivers * 2 + 2));

  const auto queryId =
      options.queryId.value_or(fmt::format("query_{}", ++queryCounter_));

  // Build Velox QueryConfig from session properties.
  auto executionProps = sessionConfig_->effectiveValues(kExecutionPrefix);
  std::unordered_map<std::string, std::string> queryConfig(
      executionProps.begin(), executionProps.end());
  // effectiveValues() strips the prefix; re-qualify (e.g. 'foo' ->
  // 'presto.foo') so PrestoQueryConfig accessors find the value.
  for (const auto& [name, value] : sessionConfig_->effectiveValues(
           velox::functions::prestosql::PrestoQueryConfig::kPrefix)) {
    queryConfig[velox::functions::prestosql::PrestoQueryConfig::qualify(name)] =
        value;
  }
  // Per-query value, not a session property.
  queryConfig[velox::core::QueryConfig::kSessionStartTime] = std::to_string(
      options.sessionStartTimeMs.value_or(velox::getCurrentTimeMs()));

  // Build per-connector session properties.
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      connectorConfigs;
  for (const auto& [connectorId, connector] :
       velox::connector::ConnectorRegistry::global().snapshot()) {
    if (connector->configProvider()) {
      auto connectorProps = sessionConfig_->effectiveValues(connectorId);
      if (!connectorProps.empty()) {
        connectorConfigs[connectorId] =
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>(
                    connectorProps.begin(), connectorProps.end()));
      }
    }
  }

  return velox::core::QueryCtx::create(
      executor_.get(),
      velox::core::QueryConfig(std::move(queryConfig)),
      std::move(connectorConfigs),
      velox::cache::AsyncDataCache::getInstance(),
      rootPool_->shared_from_this(),
      /*spillExecutor=*/nullptr,
      queryId,
      options.tokenProvider);
}

std::string SqlQueryRunner::runExplainIo(
    const presto::SqlStatement& statement,
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const RunOptions& options,
    QueryTiming& timing,
    std::shared_ptr<connector::SchemaResolver> schemaResolver,
    std::shared_ptr<QueryRuntimeStats> runtimeStats) {
  std::optional<CatalogSchemaTableName> outputTable =
      explainIoOutputTable(statement, *logicalPlan);

  auto queryCtx = newQuery(options);
  PhaseTimer phaseTimer(
      timing.optimize,
      runtimeStats.get(),
      QueryRuntimeStats::kOptimizeWallNanos,
      QueryRuntimeStats::kOptimizeCpuNanos);

  if (useOptimizerV2_) {
    auto resolver = orDefaultSchemaResolver(schemaResolver);
    OptimizerContext optimizerContext(optimizerPool_.get());
    velox::exec::SimpleExpressionEvaluator evaluator(
        queryCtx.get(), optimizerPool_.get());
    auto session = makeOptimizerSession(
        queryCtx->queryId(),
        collectConnectorProperties(*sessionConfig_),
        /*explain=*/true);
    optimizer::v2::Optimizer optimizer(
        *logicalPlan, *resolver, *session, evaluator, queryCtx);
    return optimizer.explainIo(std::move(outputTable));
  }
  std::string text;
  optimize(
      logicalPlan,
      queryCtx,
      options,
      [&](const auto& dt) {
        text = optimizer::explainIo(&dt, outputTable);
        return false; // Stop optimization.
      },
      nullptr,
      std::move(schemaResolver),
      /*explain=*/true,
      std::move(runtimeStats));
  return text;
}

std::string SqlQueryRunner::runExplain(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    presto::ExplainStatement::Type type,
    presto::ExplainStatement::Format format,
    const RunOptions& options,
    QueryTiming& timing,
    std::shared_ptr<QueryRuntimeStats> runtimeStats,
    std::shared_ptr<connector::SchemaResolver> schemaResolver) {
  const bool explain = schemaResolver != nullptr;

  VELOX_USER_CHECK_NE(
      format,
      presto::ExplainStatement::Format::kJson,
      "Unsupported EXPLAIN format: JSON. Supported formats: TEXT, GRAPHVIZ.");

  if (format == presto::ExplainStatement::Format::kGraphviz) {
    VELOX_USER_CHECK(
        type == presto::ExplainStatement::Type::kLogical ||
            type == presto::ExplainStatement::Type::kGraph,
        "EXPLAIN FORMAT GRAPHVIZ is supported for TYPE LOGICAL and TYPE GRAPH only.");
  }

  switch (type) {
    case presto::ExplainStatement::Type::kLogical:
      if (format == presto::ExplainStatement::Format::kGraphviz) {
        std::ostringstream out;
        graphviz::LogicalPlanDotPrinter::print(*logicalPlan, out);
        return out.str();
      }
      return logical_plan::PlanPrinter::toText(*logicalPlan);

    case presto::ExplainStatement::Type::kGraph: {
      VELOX_USER_CHECK(
          !useOptimizerV2_, "EXPLAIN TYPE GRAPH is not supported with --v2");
      std::string text;
      auto queryCtx = newQuery(options);
      {
        PhaseTimer phaseTimer(
            timing.optimize,
            runtimeStats.get(),
            QueryRuntimeStats::kOptimizeWallNanos,
            QueryRuntimeStats::kOptimizeCpuNanos);
        optimize(
            logicalPlan,
            queryCtx,
            options,
            [&](const auto& dt) {
              if (format == presto::ExplainStatement::Format::kGraphviz) {
                std::ostringstream out;
                graphviz::DerivedTableDotPrinter::print(dt, out);
                text = out.str();
              } else {
                text = optimizer::DerivedTablePrinter::toText(dt);
              }
              return false; // Stop optimization.
            },
            nullptr,
            schemaResolver,
            explain,
            runtimeStats);
      }
      return text;
    }

    case presto::ExplainStatement::Type::kOptimized: {
      VELOX_USER_CHECK(
          !useOptimizerV2_,
          "EXPLAIN TYPE OPTIMIZED is not supported with --v2");
      std::string text;
      auto queryCtx = newQuery(options);
      {
        PhaseTimer phaseTimer(
            timing.optimize,
            runtimeStats.get(),
            QueryRuntimeStats::kOptimizeWallNanos,
            QueryRuntimeStats::kOptimizeCpuNanos);
        optimize(
            logicalPlan,
            queryCtx,
            options,
            nullptr,
            [&](const auto& plan) {
              text = optimizer::RelationOpPrinter::toText(
                  plan,
                  {
                      .includeCost = true,
                      .includeConstraints = options.debugMode,
                  });
              return false; // Stop optimization.
            },
            schemaResolver,
            explain,
            runtimeStats);
      }
      return text;
    }

    case presto::ExplainStatement::Type::kExecutable: {
      optimizer::PlanAndStats planAndStats;
      auto queryCtx = newQuery(options);
      {
        PhaseTimer phaseTimer(
            timing.optimize,
            runtimeStats.get(),
            QueryRuntimeStats::kOptimizeWallNanos,
            QueryRuntimeStats::kOptimizeCpuNanos);
        planAndStats = optimize(
            logicalPlan,
            queryCtx,
            options,
            nullptr,
            nullptr,
            schemaResolver,
            explain,
            runtimeStats);
      }
      return planAndStats.toString();
    }

    case presto::ExplainStatement::Type::kIo:
      // Handled in run() before calling runExplain().
      VELOX_UNREACHABLE();
  }
  VELOX_UNREACHABLE();
}

std::shared_ptr<facebook::axiom::runner::LocalRunner>
SqlQueryRunner::executeSelectOrInsert(
    const presto::SqlStatement& statement,
    const RunOptions& options) {
  logical_plan::LogicalPlanNodePtr logicalPlan;
  if (statement.isSelect()) {
    logicalPlan = statement.as<presto::SelectStatement>()->plan();
  } else if (statement.isInsert()) {
    logicalPlan = statement.as<presto::InsertStatement>()->plan();
  } else {
    VELOX_USER_FAIL(
        "Only SELECT and INSERT statements are supported for executeSelectOrInsert, found: {}",
        statement.kindName());
  }

  auto queryCtx = newQuery(options);
  auto planAndStats = optimize(logicalPlan, queryCtx, options);
  return makeLocalRunner(planAndStats, queryCtx, options, noopRuntimeStats_);
}

namespace {
std::string printPlanWithStats(
    runner::LocalRunner& runner,
    const optimizer::NodePredictionMap& estimates,
    bool includeCustomStats) {
  return runner.printPlanWithStats(
      includeCustomStats,
      [&](const velox::core::PlanNodeId& nodeId,
          std::string_view indentation,
          std::ostream& out) {
        auto it = estimates.find(nodeId);
        if (it != estimates.end()) {
          out << indentation << "Estimate: " << it->second.cardinality
              << " rows, " << velox::succinctBytes(it->second.peakMemory)
              << " peak memory" << std::endl;
        }
      });
}

// Sums finalized Velox driver CPU time across all runner tasks.
int64_t executionCpuNanos(const runner::Runner& runner) {
  int64_t cpuNs{0};
  for (const auto& taskStats : runner.stats()) {
    for (const auto& pipelineStats : taskStats.pipelineStats) {
      for (const auto& operatorStats : pipelineStats.operatorStats) {
        const auto it = operatorStats.runtimeStats.find(
            velox::exec::OperatorStats::kDriverCpuTime);
        if (it != operatorStats.runtimeStats.end()) {
          cpuNs += it->second.sum;
        }
      }
    }
  }
  return cpuNs;
}

// Drives the runner under the awaiting scope's cancellation and an optional
// deadline, yielding each result batch as it is produced and reaping via
// co_close() once the stream ends. The deadline (folly::FutureTimeout) becomes
// a VELOX_USER_FAIL; an external cancel re-raises folly::OperationCancelled for
// co_run() to normalize; a genuine execution error propagates as itself.
// A consumer that stops early must cancel via the token; silently destroying
// this generator mid-stream skips the shielded reap and leaks the Velox task.
// Finalized wall and Velox task CPU timings are recorded after the reap.
folly::coro::AsyncGenerator<velox::RowVectorPtr> co_drainQuery(
    runner::Runner& runner,
    int64_t timeoutMicros,
    uint64_t& wallMicros,
    QueryRuntimeStats* runtimeStats) {
  std::exception_ptr error;
  bool cancelled{false};
  bool timedOut{false};
  {
    velox::MicrosecondTimer wallTimer(&wallMicros);
    // Keeps the generator in this coroutine's frame so its AsyncGenerator
    // producer (execute()) outlives the per-batch timeout and the shielded
    // co_close() reap. External cancellation flows in ambiently through the
    // awaiting scope's token.
    auto generator = runner.execute();
    // Absolute deadline for the whole drain: folly::coro::timeout can't wrap a
    // loop that co_yields, so each next() is bounded by the time remaining,
    // which still caps total execution time (matching the pre-streaming
    // behavior).
    std::optional<std::chrono::steady_clock::time_point> deadline;
    if (timeoutMicros > 0) {
      deadline = std::chrono::steady_clock::now() +
          std::chrono::microseconds(timeoutMicros);
    }
    try {
      while (true) {
        if (deadline) {
          // folly::coro::timeout -> folly::futures::sleep takes microseconds,
          // so round the remaining time up to microseconds once and reuse it
          // for the guard and the pull. ceil (not duration_cast) avoids
          // truncating a positive sub-microsecond remainder to zero, which
          // would time out early.
          const auto remaining = std::chrono::ceil<std::chrono::microseconds>(
              *deadline - std::chrono::steady_clock::now());
          if (remaining.count() <= 0) {
            timedOut = true;
            break;
          }
          auto batch =
              co_await folly::coro::timeout(generator.next(), remaining);
          if (!batch) {
            break;
          }
          co_yield std::move(*batch);
        } else {
          auto batch = co_await generator.next();
          if (!batch) {
            break;
          }
          co_yield std::move(*batch);
        }
      }
    } catch (const folly::FutureTimeout&) {
      timedOut = true;
    } catch (const folly::OperationCancelled&) {
      cancelled = true;
    } catch (...) {
      // Any failure (std::exception or not) still hits the shielded reap below,
      // so capture it and follow the error path.
      error = std::current_exception();
    }
    // Reap regardless, shielded from the caller's cancellation so a
    // cancelled scope still winds the run down. A reap failure must not
    // mask the original stop reason: if the drain already timed out,
    // cancelled, or failed, keep that and let the reap error be secondary.
    try {
      co_await folly::coro::co_withCancellation(
          folly::CancellationToken{}, runner.co_close());
    } catch (const std::exception& e) {
      if (!timedOut && !cancelled && !error) {
        error = std::current_exception();
      } else {
        LOG(WARNING) << "co_close() failed during reap, surfacing the "
                        "original stop reason instead: "
                     << e.what();
      }
    } catch (...) {
      // A non-std exception must not mask the original stop reason either.
      if (!timedOut && !cancelled && !error) {
        error = std::current_exception();
      } else {
        LOG(WARNING) << "co_close() failed during reap with a non-standard "
                        "exception, surfacing the original stop reason instead";
      }
    }
  }

  if (runtimeStats != nullptr) {
    runtimeStats->recordTiming(
        QueryRuntimeStats::kExecuteWallNanos,
        std::chrono::microseconds(wallMicros));
    runtimeStats->recordTiming(
        QueryRuntimeStats::kExecuteCpuNanos,
        std::chrono::nanoseconds(executionCpuNanos(runner)));
  }
  if (timedOut) {
    VELOX_USER_FAIL(
        "Query exceeded maximum time limit of {:.2f}s",
        timeoutMicros / 1'000'000.0);
  }
  if (cancelled) {
    // Re-raise after the shielded reap; co_run() normalizes it to the dedicated
    // QueryCancelledError so every async path reports cancellation uniformly.
    throw folly::OperationCancelled{};
  }
  if (error) {
    std::rethrow_exception(error);
  }
}

} // namespace

connector::ConnectorSessionPtr SqlQueryRunner::makeConnectorSession(
    std::string_view queryId,
    std::string_view connectorId) const {
  return std::make_shared<connector::ConnectorSession>(
      std::string(queryId),
      user_,
      sessionConfig_->effectiveValues(connectorId));
}

std::unique_ptr<runner::ProgressReporter> SqlQueryRunner::startProgressReporter(
    runner::Runner& runner,
    std::string_view queryId,
    const RunOptions& options) {
  if (!options.onProgress) {
    return nullptr;
  }
  VELOX_USER_CHECK_NOT_NULL(
      progressScheduler_,
      "onProgress requires a scheduler supplied to SqlQueryRunner");
  progressScheduler_->start();
  return std::make_unique<runner::ProgressReporter>(
      runner,
      *progressScheduler_,
      std::string(queryId),
      options.onProgress,
      options.progressReportIntervalMs);
}

folly::coro::Task<std::string> SqlQueryRunner::co_runExplainAnalyze(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const RunOptions& options,
    QueryTiming& timing,
    std::shared_ptr<QueryRuntimeStats> runtimeStats,
    std::shared_ptr<connector::SchemaResolver> schemaResolver) {
  auto queryCtx = newQuery(options);
  optimizer::PlanAndStats planAndStats;
  {
    PhaseTimer phaseTimer(
        timing.optimize,
        runtimeStats.get(),
        QueryRuntimeStats::kOptimizeWallNanos,
        QueryRuntimeStats::kOptimizeCpuNanos);
    planAndStats = optimize(
        logicalPlan,
        queryCtx,
        options,
        nullptr,
        nullptr,
        schemaResolver,
        /*explain=*/false,
        runtimeStats);
  }

  auto runner = makeLocalRunner(
      planAndStats,
      queryCtx,
      options,
      runtimeStats ? *runtimeStats : noopRuntimeStats_);

  {
    auto progress =
        startProgressReporter(*runner, queryCtx->queryId(), options);
    // Executed for its runtime stats (printed below); the result batches are
    // not used, so drain and discard them.
    auto generator = co_drainQuery(
        *runner, options.timeoutMicros, timing.execute, runtimeStats.get());
    while (co_await generator.next()) {
    }
  }

  std::stringstream out;
  out << printPlanWithStats(
      *runner, planAndStats.prediction, options.debugMode);

  co_return out.str();
}

std::shared_ptr<optimizer::OptimizerSession>
SqlQueryRunner::makeOptimizerSession(
    std::string_view queryId,
    connector::ConnectorProperties connectorProperties,
    bool explain) {
  auto optimizerOptions = optimizer::OptimizerOptions::from(
      sessionConfig_->effectiveValues(kOptimizerPrefix));
  optimizerOptions.explain = explain;
  return std::make_shared<optimizer::OptimizerSession>(
      std::string(queryId),
      user_,
      std::move(optimizerOptions),
      std::move(connectorProperties));
}

optimizer::PlanAndStats SqlQueryRunner::optimize(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const std::shared_ptr<velox::core::QueryCtx>& queryCtx,
    const RunOptions& options,
    const std::function<bool(const optimizer::DerivedTable&)>&
        checkDerivedTable,
    const std::function<bool(const optimizer::RelationOp&)>& checkBestPlan,
    std::shared_ptr<facebook::axiom::connector::SchemaResolver> schemaResolver,
    bool explain,
    std::shared_ptr<QueryRuntimeStats> runtimeStats) {
  optimizer::MultiFragmentPlan::Options opts;
  opts.numWorkers = options.numWorkers;
  opts.numDrivers = options.numDrivers;
  OptimizerContext optimizerContext(optimizerPool_.get());

  velox::exec::SimpleExpressionEvaluator evaluator(
      queryCtx.get(), optimizerPool_.get());

  auto history = std::make_unique<optimizer::VeloxHistory>();
  schemaResolver = orDefaultSchemaResolver(std::move(schemaResolver));

  auto connectorProperties = collectConnectorProperties(*sessionConfig_);
  auto optimizerSession =
      makeOptimizerSession(queryCtx->queryId(), connectorProperties, explain);
  auto runnerSession = std::make_shared<runner::RunnerSession>(
      queryCtx->queryId(),
      user_,
      sessionConfig_->effectiveValues(kRunnerPrefix),
      std::move(connectorProperties));

  if (useOptimizerV2_) {
    VELOX_USER_CHECK(
        checkDerivedTable == nullptr && checkBestPlan == nullptr,
        "DerivedTable / RelationOp inspection hooks are not supported by the v2 optimizer");
    // The v2 optimizer runs as a single phase, so the per-phase timing
    // breakdown keys (toGraph / bestPlan / toVelox) are not populated; only
    // the overall `kOptimizeWallNanos` / `kOptimizeCpuNanos` are recorded,
    // from the caller-side `PhaseTimer`.
    return optimizer::v2::Optimizer(
               *logicalPlan,
               *schemaResolver,
               *optimizerSession,
               evaluator,
               queryCtx)
        .optimize(opts);
  }

  uint64_t toGraphNanos{0};
  uint64_t toGraphCpuNanos{0};
  auto toGraphCpuStart = velox::process::threadCpuNanos();
  auto toGraphStart = std::chrono::steady_clock::now();
  optimizer::Optimization optimization(
      std::move(optimizerSession),
      std::move(runnerSession),
      *logicalPlan,
      *schemaResolver,
      *history,
      queryCtx,
      evaluator,
      opts,
      runtimeStats);

  if (checkDerivedTable && !checkDerivedTable(*optimization.rootDt())) {
    return {};
  }
  toGraphCpuNanos = velox::process::threadCpuNanos() - toGraphCpuStart;
  toGraphNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                     std::chrono::steady_clock::now() - toGraphStart)
                     .count();

  uint64_t bestPlanNanos{0};
  uint64_t bestPlanCpuNanos{0};
  optimizer::PlanP best;
  {
    auto cpuStart = velox::process::threadCpuNanos();
    velox::NanosecondTimer timer(&bestPlanNanos);
    best = optimization.bestPlan();
    bestPlanCpuNanos = velox::process::threadCpuNanos() - cpuStart;
  }
  if (checkBestPlan && !checkBestPlan(*best->op)) {
    return {};
  }

  uint64_t toVeloxNanos{0};
  uint64_t toVeloxCpuNanos{0};
  optimizer::PlanAndStats result;
  {
    auto cpuStart = velox::process::threadCpuNanos();
    velox::NanosecondTimer timer(&toVeloxNanos);
    result = optimization.toVeloxPlan(best->op);
    toVeloxCpuNanos = velox::process::threadCpuNanos() - cpuStart;
  }

  if (runtimeStats) {
    runtimeStats->recordTiming(
        QueryRuntimeStats::kOptimizeToGraphWallNanos,
        std::chrono::nanoseconds(toGraphNanos));
    runtimeStats->recordTiming(
        QueryRuntimeStats::kOptimizeToGraphCpuNanos,
        std::chrono::nanoseconds(toGraphCpuNanos));
    runtimeStats->recordTiming(
        QueryRuntimeStats::kOptimizeBestPlanWallNanos,
        std::chrono::nanoseconds(bestPlanNanos));
    runtimeStats->recordTiming(
        QueryRuntimeStats::kOptimizeBestPlanCpuNanos,
        std::chrono::nanoseconds(bestPlanCpuNanos));
    runtimeStats->recordTiming(
        QueryRuntimeStats::kOptimizeToVeloxWallNanos,
        std::chrono::nanoseconds(toVeloxNanos));
    runtimeStats->recordTiming(
        QueryRuntimeStats::kOptimizeToVeloxCpuNanos,
        std::chrono::nanoseconds(toVeloxCpuNanos));
  }

  return result;
}

std::shared_ptr<runner::LocalRunner> SqlQueryRunner::makeLocalRunner(
    optimizer::PlanAndStats& planAndStats,
    const std::shared_ptr<velox::core::QueryCtx>& queryCtx,
    const RunOptions& options,
    QueryRuntimeStats& runtimeStats) {
  auto runnerSession = std::make_shared<runner::RunnerSession>(
      queryCtx->queryId(),
      user_,
      sessionConfig_->effectiveValues(kRunnerPrefix),
      collectConnectorProperties(*sessionConfig_));
  return std::make_shared<runner::LocalRunner>(
      std::move(runnerSession),
      planAndStats.plan,
      std::move(planAndStats.finishWrite),
      queryCtx,
      std::make_shared<runner::ConnectorSplitSourceFactory>(runtimeStats),
      executorPool_,
      /*baseSpillDirectory=*/"",
      runtimeStats);
}

folly::coro::AsyncGenerator<velox::RowVectorPtr> SqlQueryRunner::co_showSession(
    const presto::ShowSessionStatement& statement,
    const RunOptions& options,
    QueryTiming& timing,
    std::string& planString) {
  using facebook::velox::config::ConfigPropertyTypeName;

  // Collect and sort entries by qualified name.
  auto entries = sessionConfig_->all();
  std::sort(
      entries.begin(), entries.end(), [](const auto& lhs, const auto& rhs) {
        return std::tie(lhs.prefix, lhs.property.name) <
            std::tie(rhs.prefix, rhs.property.name);
      });

  std::vector<velox::Variant> data;
  data.reserve(entries.size());
  for (const auto& entry : entries) {
    auto qualifiedName = entry.prefix + "." + entry.property.name;
    data.emplace_back(
        velox::Variant::row({
            qualifiedName,
            entry.currentValue.value_or(""),
            entry.property.defaultValue.value_or(""),
            std::string(ConfigPropertyTypeName::toName(entry.property.type)),
            entry.property.description,
        }));
  }

  namespace lp = facebook::axiom::logical_plan;

  lp::PlanBuilder::Context context(defaultConnectorId_);
  lp::PlanBuilder builder(context);
  builder.values(
      ROW({"Name", "Value", "Default", "Type", "Description"},
          velox::VARCHAR()),
      std::move(data));

  if (statement.likePattern().has_value()) {
    builder.filter(
        lp::Call(
            "like", lp::Col("Name"), lp::Lit(statement.likePattern().value())));
  }

  // Materialize the built plan into a local before the await: `build()` returns
  // by value, and binding that temporary to co_runLogicalPlan's `const&`
  // parameter would leave it dangling once GCC destroys co_await-operand
  // temporaries at the suspension point.
  auto plan = builder.build();
  auto generator = co_runLogicalPlan(plan, options, timing, planString);
  while (auto batch = co_await generator.next()) {
    co_yield std::move(*batch);
  }
}

folly::coro::AsyncGenerator<velox::RowVectorPtr>
SqlQueryRunner::co_runLogicalPlan(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const RunOptions& options,
    QueryTiming& timing,
    std::string& planString,
    std::shared_ptr<facebook::axiom::connector::SchemaResolver> schemaResolver,
    std::shared_ptr<QueryRuntimeStats> runtimeStats) {
  auto queryCtx = newQuery(options);

  optimizer::PlanAndStats planAndStats;
  {
    PhaseTimer phaseTimer(
        timing.optimize,
        runtimeStats.get(),
        QueryRuntimeStats::kOptimizeWallNanos,
        QueryRuntimeStats::kOptimizeCpuNanos);
    planAndStats = optimize(
        logicalPlan,
        queryCtx,
        options,
        nullptr,
        nullptr,
        std::move(schemaResolver),
        /*explain=*/false,
        runtimeStats);
  }

  planString = planAndStats.toString();

  auto runner = makeLocalRunner(
      planAndStats,
      queryCtx,
      options,
      runtimeStats ? *runtimeStats : noopRuntimeStats_);

  {
    auto progress =
        startProgressReporter(*runner, queryCtx->queryId(), options);
    auto generator = co_drainQuery(
        *runner, options.timeoutMicros, timing.execute, runtimeStats.get());
    while (auto batch = co_await generator.next()) {
      co_yield std::move(*batch);
    }
  }
}

namespace {
// Converts an optional to a different scalar type, preserving nullopt.
template <typename To, typename From>
std::optional<To> castOpt(const std::optional<From>& value) {
  if (value.has_value()) {
    return static_cast<To>(*value);
  }
  return std::nullopt;
}

// Rounds an optional cardinality estimate to an integer count, preserving
// nullopt for an unknown cardinality.
std::optional<int64_t> roundCardinality(std::optional<float> cardinality) {
  if (cardinality.has_value()) {
    return std::llround(*cardinality);
  }
  return std::nullopt;
}
} // namespace

std::vector<velox::RowVectorPtr> SqlQueryRunner::runShowStatsForQuery(
    const presto::SqlStatement& sqlStatement,
    const RunOptions& options) {
  const auto* showStats = sqlStatement.as<presto::ShowStatsForQueryStatement>();
  const auto& innerStatement = showStats->statement();
  VELOX_CHECK(innerStatement->isSelect());

  const auto logicalPlan =
      innerStatement->as<presto::SelectStatement>()->plan();

  std::vector<velox::Variant> data;

  if (useOptimizerV2_) {
    auto queryCtx = newQuery(options);
    OptimizerContext optimizerContext(optimizerPool_.get());
    velox::exec::SimpleExpressionEvaluator evaluator(
        queryCtx.get(), optimizerPool_.get());
    auto resolver = orDefaultSchemaResolver(nullptr);
    auto session = makeOptimizerSession(
        queryCtx->queryId(),
        collectConnectorProperties(*sessionConfig_),
        /*explain=*/false);

    const auto stats =
        optimizer::v2::Optimizer(
            *logicalPlan, *resolver, *session, evaluator, queryCtx)
            .estimateQueryStats();

    presto::ShowStatsBuilder builder(roundCardinality(stats.cardinality));
    for (const auto& column : stats.columns) {
      builder.addColumn(
          column.name,
          *column.type,
          castOpt<double>(column.nullFraction),
          roundCardinality(column.distinctCount),
          /*avgLength=*/std::nullopt,
          column.min,
          column.max);
    }
    data = builder.rows();
  } else {
    optimize(
        logicalPlan,
        newQuery(options),
        options,
        [&](const optimizer::DerivedTable& rootDt) {
          presto::ShowStatsBuilder builder(
              roundCardinality(rootDt.cardinality()));

          for (const auto* column : rootDt.columns) {
            const auto& value = column->value();

            builder.addColumn(
                column->outputName(),
                *value.type,
                castOpt<double>(value.nullFraction),
                roundCardinality(value.cardinality),
                /*avgLength=*/std::nullopt,
                value.min,
                value.max);
          }

          data = builder.rows();
          return false; // Stop optimization.
        });
  }

  auto result = std::dynamic_pointer_cast<velox::RowVector>(
      velox::BaseVector::createFromVariants(
          presto::ShowStatsBuilder::outputType(), data, optimizerPool_.get()));
  return {result};
}

std::shared_ptr<velox::filesystems::TokenProvider>
SqlQueryRunner::checkPermission(
    const RunOptions& options,
    QueryCompletionInfo& completionInfo,
    const presto::ViewMap& views,
    const presto::ReferencedTables& referencedTables) {
  if (permissionCheck_) {
    velox::MicrosecondTimer timer(&completionInfo.timing.checkPermission);
    return permissionCheck_(
        completionInfo.startInfo.queryId,
        completionInfo.startInfo.query,
        options.defaultConnectorId.value_or(defaultConnectorId_),
        options.defaultSchema
            ? std::optional<std::string_view>{*options.defaultSchema}
            : std::optional<std::string_view>{defaultSchema_},
        views,
        referencedTables);
  }
  return nullptr;
}

} // namespace axiom::sql
