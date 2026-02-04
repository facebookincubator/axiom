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
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/logical_plan/LogicalPlanDotPrinter.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/optimizer/ConstantExprEvaluator.h"
#include "axiom/optimizer/DerivedTableDotPrinter.h"
#include "axiom/optimizer/DerivedTablePrinter.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/RelationOpPrinter.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"

namespace velox = facebook::velox;
using namespace facebook::axiom;

namespace axiom::sql {

void SqlQueryRunner::initialize(
    const std::function<std::pair<std::string, std::optional<std::string>>()>&
        initializeConnectors) {
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
  executorPool_ = rootPool_->addLeafChild("executor");

  auto [defaultConnectorId, defaultSchema] = initializeConnectors();
  defaultConnectorId_ = std::move(defaultConnectorId);
  defaultSchema_ = std::move(defaultSchema);
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
  auto statement = parseSingle(sql, options);
  return run(*statement, options);
}

std::string SqlQueryRunner::toQueryGraphDot(std::string_view sql) {
  const auto logicalPlan = toLogicalPlan(sql);

  std::string dotOutput;
  RunOptions options;
  optimize(logicalPlan, newQuery(options), options, [&](const auto& dt) {
    std::ostringstream out;
    optimizer::DerivedTableDotPrinter::print(dt, out);
    dotOutput = out.str();
    return false; // Stop optimization.
  });
  return dotOutput;
}

std::string SqlQueryRunner::toLogicalPlanDot(std::string_view sql) {
  const auto logicalPlan = toLogicalPlan(sql);

  std::ostringstream out;
  logical_plan::LogicalPlanDotPrinter::print(*logicalPlan, out);
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

std::vector<presto::SqlStatementPtr> SqlQueryRunner::parseMultiple(
    std::string_view sql,
    const RunOptions& options) {
  const std::string& defaultConnectorId =
      options.defaultConnectorId.value_or(defaultConnectorId_);
  const std::optional<std::string>& defaultSchema =
      options.defaultSchema ? options.defaultSchema : defaultSchema_;

  auto prestoParser =
      std::make_unique<presto::PrestoParser>(defaultConnectorId, defaultSchema);
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

    auto schema = std::make_shared<connector::SchemaResolver>();
    schema->setTargetTable(ctas->connectorId(), table);

    return {.results = runLogicalPlan(ctas->plan(), options, schema)};
  }

  if (sqlStatement.isInsert()) {
    const auto* insert = sqlStatement.as<presto::InsertStatement>();
    return {.results = runLogicalPlan(insert->plan(), options)};
  }

  if (sqlStatement.isDropTable()) {
    const auto* drop = sqlStatement.as<presto::DropTableStatement>();

    return {.message = dropTable(*drop)};
  }

  VELOX_CHECK(sqlStatement.isSelect());

  const auto logicalPlan = sqlStatement.as<presto::SelectStatement>()->plan();

  return {.results = runLogicalPlan(logicalPlan, options)};
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
  return makeLocalRunner(planAndStats, queryCtx, options);
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
    waitForCompletion(runner, options.timeoutMicros);
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
    const std::function<bool(const optimizer::RelationOp&)>& checkBestPlan,
    std::shared_ptr<facebook::axiom::connector::SchemaResolver> schemaResolver

) {
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
  auto history = std::make_unique<optimizer::VeloxHistory>();
  if (schemaResolver == nullptr) {
    schemaResolver = std::make_shared<connector::SchemaResolver>();
  }

  optimizer::Optimization optimization(
      session,
      *logicalPlan,
      *schemaResolver,
      *history,
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
      executorPool_);
}

std::vector<velox::RowVectorPtr> SqlQueryRunner::runLogicalPlan(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const RunOptions& options,
    std::shared_ptr<facebook::axiom::connector::SchemaResolver>
        schemaResolver) {
  auto queryCtx = newQuery(options);
  auto planAndStats = optimize(
      logicalPlan, queryCtx, options, nullptr, nullptr, schemaResolver);

  auto runner = makeLocalRunner(planAndStats, queryCtx, options);
  SCOPE_EXIT {
    waitForCompletion(runner, options.timeoutMicros);
  };

  return fetchResults(*runner);
}

} // namespace axiom::sql
