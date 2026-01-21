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
#include <folly/system/HardwareConcurrency.h>
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/optimizer/ConstantExprEvaluator.h"
#include "axiom/optimizer/DerivedTablePrinter.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/RelationOpPrinter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"

namespace velox = facebook::velox;

using namespace facebook;
using namespace facebook::axiom;

namespace axiom::sql {

std::string Timing::toString() const {
  double pct = 0;
  if (micros > 0) {
    pct = 100 * (userNanos + systemNanos) / (micros * 1000);
  }

  std::stringstream out;
  out << velox::succinctNanos(micros * 1000) << " / "
      << velox::succinctNanos(userNanos) << " user / "
      << velox::succinctNanos(systemNanos) << " system (" << pct << "%)";
  return out.str();
}

void SqlQueryRunner::initialize(
    const std::function<std::pair<std::string, std::optional<std::string>>(
        optimizer::VeloxHistory& history)>& initializeConnectors) {
  static folly::once_flag kInitialized;

  folly::call_once(kInitialized, []() {
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::aggregate::prestosql::registerAllAggregateFunctions();
    velox::parse::registerTypeResolver();

    optimizer::FunctionRegistry::registerPrestoFunctions();

    velox::filesystems::registerLocalFileSystem();

    velox::exec::ExchangeSource::registerFactory(
        velox::exec::test::createLocalExchangeSource);
    velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
    if (!isRegisteredNamedVectorSerde(velox::VectorSerde::Kind::kPresto)) {
      velox::serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
    }
  });

  static std::atomic<int32_t> kCounter{0};

  rootPool_ = velox::memory::memoryManager()->addRootPool(
      fmt::format("axiom_sql{}", kCounter++));
  optimizerPool_ = rootPool_->addLeafChild("optimizer");

  history_ = std::make_unique<optimizer::VeloxHistory>();

  const auto [defaultConnectorId, defaultSchema] =
      initializeConnectors(*history_);

  schema_ = std::make_shared<connector::SchemaResolver>();

  prestoParser_ = std::make_unique<presto::PrestoParser>(
      defaultConnectorId, defaultSchema, optimizerPool_.get());

  spillExecutor_ = std::make_shared<folly::IOThreadPoolExecutor>(4);
}

namespace {
std::vector<velox::RowVectorPtr> fetchResults(runner::LocalRunner& runner) {
  std::vector<velox::RowVectorPtr> results;
  while (auto rows = runner.next()) {
    results.push_back(rows);
  }
  return results;
}

CommandResult exitHandler(const std::string&) {
  return CommandResult{.handled = true, .outcomes = {}, .shouldExit = true};
}

CommandResult helpHandler(const std::string&) {
  static const char* helpText =
      "Axiom Interactive SQL\n\n"
      "Type SQL and end with ';'.\n"
      "To set a flag, type 'flag <gflag_name> = <value>;' Leave a space on either side of '='.\n\n"
      "Useful flags:\n\n"
      "num_workers - Make a distributed plan for this many workers. Runs it in-process with remote exchanges with serialization and passing data in memory. If num_workers is 1, makes single node plans without remote exchanges.\n\n"
      "num_drivers - Specifies the parallelism for workers. This many threads per pipeline per worker.\n\n";
  return CommandResult{
      .handled = true, .outcomes = {{.message = helpText, .data = {}}}};
}

CommandResult sessionHandler(
    const std::string& command,
    std::unordered_map<std::string, std::string>& config) {
  char* name = nullptr;
  char* value = nullptr;
  SCOPE_EXIT {
    if (name != nullptr) {
      free(name);
    }
    if (value != nullptr) {
      free(value);
    }
  };

  if (sscanf(command.c_str(), "session %ms = %ms", &name, &value) == 2) {
    config[std::string(name)] = std::string(value);
    return CommandResult{
        .handled = true,
        .outcomes = {
            {.message = fmt::format(
                 "Session '{}' set to '{}'",
                 std::string(name),
                 std::string(value)),
             .data = {}}}};
  }
  return CommandResult{
      .handled = true,
      .outcomes = {{.message = "Usage: session <name> = <value>", .data = {}}}};
}

} // namespace

connector::TablePtr SqlQueryRunner::createTable(
    const presto::CreateTableAsSelectStatement& statement) {
  auto metadata =
      connector::ConnectorMetadata::metadata(statement.connectorId());

  folly::F14FastMap<std::string, velox::Variant> options;
  for (const auto& [key, value] : statement.properties()) {
    options[key] =
        optimizer::ConstantExprEvaluator::evaluateConstantExpr(*value);
  }

  auto session = std::make_shared<connector::ConnectorSession>("test");
  return metadata->createTable(
      session, statement.tableName(), statement.tableSchema(), options);
}

std::string SqlQueryRunner::dropTable(
    const presto::DropTableStatement& statement) {
  auto metadata =
      connector::ConnectorMetadata::metadata(statement.connectorId());

  const auto& tableName = statement.tableName();

  auto session = std::make_shared<connector::ConnectorSession>("test");
  const bool dropped =
      metadata->dropTable(session, tableName, statement.ifExists());

  if (dropped) {
    return fmt::format("Dropped table: {}", tableName);
  } else {
    return fmt::format("Table doesn't exist: {}", tableName);
  }
}

SqlQueryRunner::SqlResult SqlQueryRunner::run(
    std::string_view sql,
    const RunOptions& options) {
  auto statements = parseMultiple(sql, options);
  VELOX_CHECK_EQ(
      statements.size(),
      1,
      "run() expects a single statement. "
      "Use parseMultiple() + run(statement) for multiple statements.");
  return run(*statements[0], options);
}

std::vector<presto::SqlStatementPtr> SqlQueryRunner::parseMultiple(
    std::string_view sql,
    const RunOptions& options) {
  return prestoParser_->parseMultiple(sql, /*enableTracing=*/options.debugMode);
}

void SqlQueryRunner::registerCommands(const RunOptions& options) {
  registerCommand("exit", exitHandler);
  registerCommand("quit", exitHandler);
  registerCommand("help", helpHandler);

  registerCommand(
      "savehistory",
      [this, historyPath = options.historyPath](const std::string&) {
        saveHistory(historyPath);
        return CommandResult{
            .handled = true,
            .outcomes = {{.message = "History saved.", .data = {}}}};
      });

  registerCommand("clearhistory", [this](const std::string&) {
    clearHistory();
    return CommandResult{
        .handled = true,
        .outcomes = {{.message = "History cleared.", .data = {}}}};
  });

  registerCommand("session", [this](const std::string& command) {
    return sessionHandler(command, config_);
  });

  setExecuteHandler([this, options](const std::string& command) {
    std::vector<StatementOutcome> outcomes;
    try {
      auto statements = parseMultiple(command, options);
      outcomes.reserve(statements.size());
      for (const auto& statement : statements) {
        Timing statementTiming;
        auto sqlResult = time<SqlResult>(
            [&]() { return run(*statement, options); }, statementTiming);

        StatementOutcome outcome{
            .message = std::move(sqlResult.message),
            .data = std::move(sqlResult.results)};

        if (options.measureTiming) {
          outcome.timing = std::move(statementTiming);
        }
        outcomes.push_back(std::move(outcome));
      }
      return CommandResult{.handled = true, .outcomes = std::move(outcomes)};
    } catch (const std::exception& e) {
      outcomes.push_back({.message = e.what(), .data = {}});
      return CommandResult{.handled = true, .outcomes = std::move(outcomes)};
    }
  });
}

CommandResult SqlQueryRunner::handleCommand(const std::string& command) const {
  std::string bestMatch;
  const CommandHandler* bestHandler = nullptr;

  for (const auto& [prefix, handler] : commands_) {
    if (command.starts_with(prefix)) {
      if (prefix.size() > bestMatch.size()) {
        bestMatch = prefix;
        bestHandler = &handler;
      }
    }
  }

  if (bestHandler) {
    return (*bestHandler)(command);
  }

  return executeHandler_(command);
}

SqlQueryRunner::SqlResult SqlQueryRunner::run(
    const presto::SqlStatement& sqlStatement,
    const RunOptions& options) {
  if (sqlStatement.isExplain()) {
    const auto* explain = sqlStatement.as<presto::ExplainStatement>();

    const auto& statement = explain->statement();

    logical_plan::LogicalPlanNodePtr logicalPlan;
    if (statement->isSelect()) {
      logicalPlan = statement->as<presto::SelectStatement>()->plan();
    } else if (statement->isInsert()) {
      logicalPlan = statement->as<presto::InsertStatement>()->plan();
    } else {
      // TODO Add support for EXPLAIN CREATE TABLE AS SELECT ...
      VELOX_NYI("Unsupported EXPLAIN query: {}", statement->kindName());
    }

    if (explain->isAnalyze()) {
      return {.message = runExplainAnalyze(logicalPlan, options)};
    } else {
      return {.message = runExplain(logicalPlan, explain->type(), options)};
    }
  }

  if (sqlStatement.isCreateTableAsSelect()) {
    const auto* ctas = sqlStatement.as<presto::CreateTableAsSelectStatement>();

    auto table = createTable(*ctas);

    auto originalSchemaResolver = schema_;
    SCOPE_EXIT {
      schema_ = originalSchemaResolver;
    };

    schema_ = std::make_shared<connector::SchemaResolver>();
    schema_->setTargetTable(ctas->connectorId(), table);

    return {.results = runSql(ctas->plan(), options)};
  }

  if (sqlStatement.isInsert()) {
    const auto* insert = sqlStatement.as<presto::InsertStatement>();
    return {.results = runSql(insert->plan(), options)};
  }

  if (sqlStatement.isDropTable()) {
    const auto* drop = sqlStatement.as<presto::DropTableStatement>();

    return {.message = dropTable(*drop)};
  }

  VELOX_CHECK(sqlStatement.isSelect());

  const auto logicalPlan = sqlStatement.as<presto::SelectStatement>()->plan();

  return {.results = runSql(logicalPlan, options)};
}

std::shared_ptr<velox::core::QueryCtx> SqlQueryRunner::newQuery(
    const RunOptions& options) {
  ++queryCounter_;

  executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(std::max<int32_t>(
      folly::hardware_concurrency() * 2,
      options.numWorkers * options.numDrivers * 2 + 2));

  return velox::core::QueryCtx::create(
      executor_.get(),
      velox::core::QueryConfig(config_),
      {},
      velox::cache::AsyncDataCache::getInstance(),
      rootPool_->shared_from_this(),
      spillExecutor_.get(),
      fmt::format("query_{}", queryCounter_));
}

std::string SqlQueryRunner::runExplain(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    presto::ExplainStatement::Type type,
    const RunOptions& options) {
  switch (type) {
    case presto::ExplainStatement::Type::kLogical:
      return logical_plan::PlanPrinter::toText(*logicalPlan);

    case presto::ExplainStatement::Type::kGraph: {
      std::string text;
      optimize(logicalPlan, newQuery(options), options, [&](const auto& dt) {
        text = optimizer::DerivedTablePrinter::toText(dt);
        return false; // Stop optimization.
      });
      return text;
    }

    case presto::ExplainStatement::Type::kOptimized: {
      std::string text;
      optimize(
          logicalPlan,
          newQuery(options),
          options,
          nullptr,
          [&](const auto& plan) {
            text = optimizer::RelationOpPrinter::toText(
                plan, {.includeCost = true});
            return false; // Stop optimization.
          });
      return text;
    }

    case presto::ExplainStatement::Type::kExecutable:
      return optimize(logicalPlan, newQuery(options), options).toString();
  }
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
} // namespace

std::string SqlQueryRunner::runExplainAnalyze(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const RunOptions& options) {
  auto queryCtx = newQuery(options);
  auto planAndStats = optimize(logicalPlan, queryCtx, options);

  auto runner = makeLocalRunner(planAndStats, queryCtx, options);
  SCOPE_EXIT {
    waitForCompletion(runner);
  };

  auto results = fetchResults(*runner);

  std::stringstream out;
  out << printPlanWithStats(
      *runner, planAndStats.prediction, options.debugMode);

  return out.str();
}

optimizer::PlanAndStats SqlQueryRunner::optimize(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const std::shared_ptr<velox::core::QueryCtx>& queryCtx,
    const RunOptions& options,
    const std::function<bool(const optimizer::DerivedTable&)>&
        checkDerivedTable,
    const std::function<bool(const optimizer::RelationOp&)>& checkBestPlan) {
  runner::MultiFragmentPlan::Options opts;
  opts.numWorkers = options.numWorkers;
  opts.numDrivers = options.numDrivers;
  auto allocator =
      std::make_unique<velox::HashStringAllocator>(optimizerPool_.get());
  auto context = std::make_unique<optimizer::QueryGraphContext>(*allocator);

  optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };

  velox::exec::SimpleExpressionEvaluator evaluator(
      queryCtx.get(), optimizerPool_.get());

  auto session = std::make_shared<Session>(queryCtx->queryId());

  optimizer::Optimization optimization(
      session,
      *logicalPlan,
      *schema_,
      *history_,
      queryCtx,
      evaluator,
      {.traceFlags = options.optimizerTraceFlags},
      opts);

  if (checkDerivedTable && !checkDerivedTable(*optimization.rootDt())) {
    return {};
  }

  auto best = optimization.bestPlan();
  if (checkBestPlan && !checkBestPlan(*best->op)) {
    return {};
  }

  return optimization.toVeloxPlan(best->op);
}

std::shared_ptr<runner::LocalRunner> SqlQueryRunner::makeLocalRunner(
    optimizer::PlanAndStats& planAndStats,
    const std::shared_ptr<velox::core::QueryCtx>& queryCtx,
    const RunOptions& options) {
  connector::SplitOptions splitOptions{
      .targetSplitCount =
          static_cast<int32_t>(options.numWorkers * options.numDrivers * 2),
      .fileBytesPerSplit = options.splitTargetBytes,
  };

  return std::make_shared<runner::LocalRunner>(
      planAndStats.plan,
      std::move(planAndStats.finishWrite),
      queryCtx,
      std::make_shared<runner::ConnectorSplitSourceFactory>(splitOptions),
      optimizerPool_);
}

std::vector<velox::RowVectorPtr> SqlQueryRunner::runSql(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const RunOptions& options) {
  auto queryCtx = newQuery(options);
  auto planAndStats = optimize(logicalPlan, queryCtx, options);

  auto runner = makeLocalRunner(planAndStats, queryCtx, options);
  SCOPE_EXIT {
    waitForCompletion(runner);
  };

  auto results = fetchResults(*runner);

  const auto stats = runner->stats();
  history_->recordVeloxExecution(planAndStats, stats);

  return results;
}

} // namespace axiom::sql
