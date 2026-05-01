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
#include <cmath>

#include "axiom/cli/QueryIdGenerator.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/logical_plan/LogicalPlanDotPrinter.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/optimizer/ConstantExprEvaluator.h"
#include "axiom/optimizer/DerivedTableDotPrinter.h"
#include "axiom/optimizer/DerivedTablePrinter.h"
#include "axiom/optimizer/ExplainIo.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/OptimizerOptions.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/RelationOpPrinter.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "axiom/sql/presto/ShowStatsBuilder.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryConfigProvider.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/expression/Expr.h"
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
      kExecutionPrefix,
      std::make_shared<facebook::velox::core::QueryConfigProvider>());

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
      kExecutionPrefix,
      velox::core::QueryConfig::kPrestoArrayAggIgnoreNulls,
      "true");
}

namespace {
std::vector<velox::RowVectorPtr> fetchResults(runner::LocalRunner& runner) {
  std::vector<velox::RowVectorPtr> results;
  while (auto rows = runner.next()) {
    results.push_back(rows);
  }
  return results;
}

int64_t countRows(const std::vector<velox::RowVectorPtr>& results) {
  int64_t numRows{0};
  for (const auto& rowVector : results) {
    numRows += rowVector->size();
  }
  return numRows;
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
    const presto::CreateTableStatement& statement,
    bool explain) {
  auto metadata = ConnectorMetadataRegistry::get(statement.connectorId());

  folly::F14FastMap<std::string, velox::Variant> options;
  for (const auto& [key, value] : statement.properties()) {
    options[key] =
        optimizer::ConstantExprEvaluator::evaluateConstantExpr(*value);
  }

  auto session = std::make_shared<connector::ConnectorSession>("test", user_);
  auto table = metadata->createTable(
      session,
      statement.tableName(),
      statement.tableSchema(),
      options,
      statement.ifNotExists(),
      explain);
  VELOX_CHECK(table != nullptr || statement.ifNotExists());
  return table;
}

connector::TablePtr SqlQueryRunner::createTable(
    const presto::CreateTableAsSelectStatement& statement,
    bool explain) {
  auto metadata = ConnectorMetadataRegistry::get(statement.connectorId());

  folly::F14FastMap<std::string, velox::Variant> options;
  for (const auto& [key, value] : statement.properties()) {
    options[key] =
        optimizer::ConstantExprEvaluator::evaluateConstantExpr(*value);
  }

  auto session = std::make_shared<connector::ConnectorSession>("test", user_);
  auto table = metadata->createTable(
      session,
      statement.tableName(),
      statement.tableSchema(),
      options,
      /*ifNotExists=*/false,
      explain);
  VELOX_CHECK_NOT_NULL(table);
  return table;
}

std::string SqlQueryRunner::dropTable(
    const presto::DropTableStatement& statement) {
  auto metadata = ConnectorMetadataRegistry::get(statement.connectorId());

  const auto& tableName = statement.tableName();

  auto session = std::make_shared<connector::ConnectorSession>("test");
  const bool dropped = metadata->dropTable(
      session,
      statement.tableName(),
      statement.ifExists(),
      /*explain=*/false);

  if (dropped) {
    return fmt::format("Dropped table: {}", tableName);
  } else {
    return fmt::format("Table doesn't exist: {}", tableName);
  }
}

std::string SqlQueryRunner::createSchema(
    const presto::CreateSchemaStatement& statement) {
  auto metadata = ConnectorMetadataRegistry::get(statement.connectorId());

  folly::F14FastMap<std::string, velox::Variant> properties;
  for (const auto& [key, value] : statement.properties()) {
    properties[key] =
        optimizer::ConstantExprEvaluator::evaluateConstantExpr(*value);
  }

  auto session = std::make_shared<connector::ConnectorSession>("test");
  metadata->createSchema(
      session, statement.schemaName(), statement.ifNotExists(), properties);
  return fmt::format("Created schema: {}", statement.schemaName());
}

std::string SqlQueryRunner::dropSchema(
    const presto::DropSchemaStatement& statement) {
  auto metadata = ConnectorMetadataRegistry::get(statement.connectorId());
  auto session = std::make_shared<connector::ConnectorSession>("test");
  metadata->dropSchema(session, statement.schemaName(), statement.ifExists());
  return fmt::format("Dropped schema: {}", statement.schemaName());
}

SqlQueryRunner::SqlResult SqlQueryRunner::run(
    std::string_view sql,
    const RunOptions& options) {
  auto runOptions = options;
  runOptions.queryId = options.queryId.value_or(queryIdGenerator_());
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

  onStart(runOptions, completionInfo);

  auto finalize = [&]() {
    completionInfo.endTime = std::chrono::system_clock::now();
    completionInfo.timing.total =
        std::chrono::duration_cast<std::chrono::microseconds>(
            completionInfo.endTime - completionInfo.startInfo.createTime)
            .count();
    onComplete(runOptions, completionInfo);
  };

  try {
    presto::SqlStatementPtr statement;
    {
      velox::MicrosecondTimer parseTimer(&completionInfo.timing.parse);
      statement = parseSingle(sql, runOptions);
    }
    completionInfo.startInfo.queryType = statement->kind();

    runOptions.tokenProvider = checkPermission(
        runOptions,
        completionInfo,
        statement->views(),
        statement->referencedTables());

    auto result = runUnchecked(
        *statement,
        runOptions,
        completionInfo.timing,
        completionInfo.planString);

    completionInfo.numOutputRows = countRows(result.results);
    finalize();
    return result;
  } catch (const std::exception& e) {
    completionInfo.errorInfo = ErrorInfo{e.what()};
    finalize();
    throw;
  }
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

SqlQueryRunner::SqlResult SqlQueryRunner::runUnchecked(
    const presto::SqlStatement& sqlStatement,
    const RunOptions& options) {
  QueryTiming timing;
  std::string planString;
  return runUnchecked(sqlStatement, options, timing, planString);
}

SqlQueryRunner::SqlResult SqlQueryRunner::runUnchecked(
    const presto::SqlStatement& sqlStatement,
    const RunOptions& options,
    QueryTiming& timing,
    std::string& planString) {
  if (sqlStatement.isExplain()) {
    const auto* explain = sqlStatement.as<presto::ExplainStatement>();

    const auto& statement = explain->statement();

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
      auto table = createTable(*ctas, /*explain=*/!explain->isAnalyze());
      schemaResolver = std::make_shared<connector::SchemaResolver>();
      schemaResolver->setTargetTable(
          ctas->connectorId(), ctas->tableName(), table);
    } else if (statement->isCreateTable()) {
      const auto* create = statement->as<presto::CreateTableStatement>();
      createTable(*create, /*explain=*/true);
      return {
          .message = fmt::format(
              "CREATE TABLE {}{}.{}",
              create->ifNotExists() ? "IF NOT EXISTS " : "",
              create->connectorId(),
              create->tableName())};
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
      return {
          .message = fmt::format(
              "DROP TABLE {}{}.{}",
              drop->ifExists() ? "IF EXISTS " : "",
              drop->connectorId(),
              drop->tableName())};
    } else {
      VELOX_NYI("Unsupported EXPLAIN query: {}", statement->kindName());
    }

    if (explain->type() == presto::ExplainStatement::Type::kIo) {
      std::optional<CatalogSchemaTableName> outputTable;
      if (statement->isCreateTableAsSelect()) {
        const auto* ctas =
            statement->as<presto::CreateTableAsSelectStatement>();
        outputTable =
            CatalogSchemaTableName{ctas->connectorId(), ctas->tableName()};
      } else if (statement->isInsert()) {
        const auto* writeNode = logicalPlan->as<logical_plan::TableWriteNode>();
        outputTable = CatalogSchemaTableName{
            writeNode->connectorId(), writeNode->tableName()};
      }

      std::string text;
      optimize(
          logicalPlan,
          newQuery(options),
          options,
          [&](const auto& dt) {
            text = optimizer::explainIo(&dt, outputTable);
            return false; // Stop optimization.
          },
          nullptr,
          schemaResolver,
          /*explain=*/true);
      return {.message = std::move(text)};
    }

    if (explain->isAnalyze()) {
      return {
          .message = runExplainAnalyze(logicalPlan, options, schemaResolver)};
    } else {
      return {
          .message = runExplain(
              logicalPlan,
              explain->type(),
              explain->format(),
              options,
              schemaResolver)};
    }
  }

  if (sqlStatement.isCreateTable()) {
    const auto* create = sqlStatement.as<presto::CreateTableStatement>();
    auto table = createTable(*create);
    if (!table) {
      return {
          .message = fmt::format(
              "Table already exists: {}.{}",
              create->connectorId(),
              create->tableName())};
    }
    return {.message = fmt::format("Created table: {}", create->tableName())};
  }

  if (sqlStatement.isCreateTableAsSelect()) {
    const auto* ctas = sqlStatement.as<presto::CreateTableAsSelectStatement>();
    auto table = createTable(*ctas);

    auto schema = std::make_shared<connector::SchemaResolver>();
    schema->setTargetTable(ctas->connectorId(), ctas->tableName(), table);

    return runLogicalPlan(ctas->plan(), options, timing, planString, schema);
  }

  if (sqlStatement.isInsert()) {
    const auto* insert = sqlStatement.as<presto::InsertStatement>();
    return runLogicalPlan(insert->plan(), options, timing, planString);
  }

  if (sqlStatement.isDropTable()) {
    const auto* drop = sqlStatement.as<presto::DropTableStatement>();

    return {.message = dropTable(*drop)};
  }

  if (sqlStatement.isCreateSchema()) {
    const auto* create = sqlStatement.as<presto::CreateSchemaStatement>();
    return {.message = createSchema(*create)};
  }

  if (sqlStatement.isDropSchema()) {
    const auto* drop = sqlStatement.as<presto::DropSchemaStatement>();
    return {.message = dropSchema(*drop)};
  }

  if (sqlStatement.isShowStatsForQuery()) {
    return {.results = runShowStatsForQuery(sqlStatement, options)};
  }

  if (sqlStatement.isShowSession()) {
    return showSession(
        *sqlStatement.as<presto::ShowSessionStatement>(),
        options,
        timing,
        planString);
  }

  if (sqlStatement.isSetSession()) {
    const auto* setSession = sqlStatement.as<presto::SetSessionStatement>();
    const auto& name = setSession->name();
    sessionConfig_->set(name, setSession->value());
    return {
        .message =
            fmt::format("Session '{}' set to '{}'", name, setSession->value())};
  }

  if (sqlStatement.isResetSession()) {
    const auto* resetSession = sqlStatement.as<presto::ResetSessionStatement>();
    const auto& name = resetSession->name();
    sessionConfig_->reset(name);
    return {.message = fmt::format("Session '{}' reset", name)};
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
    return {
        .message =
            fmt::format("Using {}.{}", defaultConnectorId_, use->schema())};
  }

  VELOX_CHECK(sqlStatement.isSelect());

  const auto logicalPlan = sqlStatement.as<presto::SelectStatement>()->plan();

  return runLogicalPlan(logicalPlan, options, timing, planString);
}

std::shared_ptr<velox::core::QueryCtx> SqlQueryRunner::newQuery(
    const RunOptions& options) {
  executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(std::max<int32_t>(
      folly::available_concurrency() * 2,
      options.numWorkers * options.numDrivers * 2 + 2));

  const auto queryId =
      options.queryId.value_or(fmt::format("query_{}", ++queryCounter_));

  // Build Velox QueryConfig from execution session properties.
  auto executionProps = sessionConfig_->effectiveValues(kExecutionPrefix);
  std::unordered_map<std::string, std::string> queryConfig(
      executionProps.begin(), executionProps.end());
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

std::string SqlQueryRunner::runExplain(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    presto::ExplainStatement::Type type,
    presto::ExplainStatement::Format format,
    const RunOptions& options,
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
        logical_plan::LogicalPlanDotPrinter::print(*logicalPlan, out);
        return out.str();
      }
      return logical_plan::PlanPrinter::toText(*logicalPlan);

    case presto::ExplainStatement::Type::kGraph: {
      std::string text;
      optimize(
          logicalPlan,
          newQuery(options),
          options,
          [&](const auto& dt) {
            if (format == presto::ExplainStatement::Format::kGraphviz) {
              std::ostringstream out;
              optimizer::DerivedTableDotPrinter::print(dt, out);
              text = out.str();
            } else {
              text = optimizer::DerivedTablePrinter::toText(dt);
            }
            return false; // Stop optimization.
          },
          nullptr,
          schemaResolver,
          explain);
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
                plan,
                {
                    .includeCost = true,
                    .includeConstraints = options.debugMode,
                });
            return false; // Stop optimization.
          },
          schemaResolver,
          explain);
      return text;
    }

    case presto::ExplainStatement::Type::kExecutable:
      return optimize(
                 logicalPlan,
                 newQuery(options),
                 options,
                 nullptr,
                 nullptr,
                 schemaResolver,
                 explain)
          .toString();

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
    const RunOptions& options,
    std::shared_ptr<connector::SchemaResolver> schemaResolver) {
  auto queryCtx = newQuery(options);
  auto planAndStats = optimize(
      logicalPlan, queryCtx, options, nullptr, nullptr, schemaResolver);

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
    std::shared_ptr<facebook::axiom::connector::SchemaResolver> schemaResolver,
    bool explain) {
  optimizer::MultiFragmentPlan::Options opts;
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

  auto optimizerProps = sessionConfig_->effectiveValues(kOptimizerPrefix);
  auto optimizerOptions = optimizer::OptimizerOptions::from(optimizerProps);
  optimizerOptions.explain = explain;

  optimizer::Optimization optimization(
      session,
      *logicalPlan,
      *schemaResolver,
      *history,
      queryCtx,
      evaluator,
      optimizerOptions,
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

SqlQueryRunner::SqlResult SqlQueryRunner::showSession(
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

  return runLogicalPlan(builder.build(), options, timing, planString);
}

SqlQueryRunner::SqlResult SqlQueryRunner::runLogicalPlan(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const RunOptions& options,
    QueryTiming& timing,
    std::string& planString,
    std::shared_ptr<facebook::axiom::connector::SchemaResolver>
        schemaResolver) {
  auto queryCtx = newQuery(options);

  SqlResult result;

  optimizer::PlanAndStats planAndStats;
  {
    velox::MicrosecondTimer timer(&timing.optimize);
    planAndStats = optimize(
        logicalPlan,
        queryCtx,
        options,
        nullptr,
        nullptr,
        std::move(schemaResolver));
  }

  planString = planAndStats.toString();

  auto runner = makeLocalRunner(planAndStats, queryCtx, options);
  SCOPE_EXIT {
    waitForCompletion(runner, options.timeoutMicros);
  };

  {
    velox::MicrosecondTimer timer(&timing.execute);
    result.results = fetchResults(*runner);
  }

  return result;
}

std::vector<velox::RowVectorPtr> SqlQueryRunner::runShowStatsForQuery(
    const presto::SqlStatement& sqlStatement,
    const RunOptions& options) {
  const auto* showStats = sqlStatement.as<presto::ShowStatsForQueryStatement>();
  const auto& innerStatement = showStats->statement();
  VELOX_CHECK(innerStatement->isSelect());

  const auto logicalPlan =
      innerStatement->as<presto::SelectStatement>()->plan();

  std::vector<velox::Variant> data;

  optimize(
      logicalPlan,
      newQuery(options),
      options,
      [&](const optimizer::DerivedTable& rootDt) {
        presto::ShowStatsBuilder builder(std::llround(rootDt.cardinality));

        for (const auto* column : rootDt.columns) {
          const auto& value = column->value();

          builder.addColumn(
              column->outputName(),
              *value.type,
              static_cast<double>(value.nullFraction),
              static_cast<int64_t>(value.cardinality),
              /*avgLength=*/std::nullopt,
              value.min,
              value.max);
        }

        data = builder.rows();
        return false; // Stop optimization.
      });

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
