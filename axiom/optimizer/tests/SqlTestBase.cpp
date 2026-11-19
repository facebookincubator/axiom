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

#include "axiom/optimizer/tests/SqlTestBase.h"
#include <folly/coro/BlockingWait.h>
#include "axiom/application/SessionProperties.h"
#include "axiom/common/ConfigRegistry.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/connectors/tests/TestConnectorContext.h"
#include "axiom/functions/prestosql/PrestoFunctions.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Optimize.h"
#include "axiom/optimizer/OptimizerOptions.h"
#include "axiom/optimizer/v1/Optimization.h"
#include "axiom/optimizer/v1/VeloxHistory.h"
#include "axiom/runner/LocalRunner.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "axiom/sql/presto/SqlStatement.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryConfigProvider.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/expression/Expr.h"
#include "velox/functions/axiom/Register.h"
#include "velox/functions/prestosql/PrestoConfigProvider.h"
#include "velox/functions/prestosql/PrestoQueryConfig.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/vector/tests/utils/VectorAssertions.h"

namespace facebook::axiom::optimizer::test {

using namespace facebook::velox;

namespace {

// Ids of the registered connectors.
std::vector<std::string> connectorIds() {
  std::vector<std::string> ids;
  for (const auto& [id, _] :
       velox::connector::ConnectorRegistry::global().snapshot()) {
    ids.push_back(id);
  }
  return ids;
}

// Properties queries can set, keyed by the prefixes above and connector ids.
std::shared_ptr<const ConfigRegistry> makeConfigRegistry() {
  auto registry = std::make_shared<ConfigRegistry>();
  registry->add(
      SqlTestBase::kOptimizerPrefix, std::make_shared<OptimizerOptions>());
  registry->add(
      SqlTestBase::kParserPrefix,
      std::make_shared<::axiom::sql::presto::ParserOptions>());
  registry->add(
      SqlTestBase::kExecutionPrefix,
      std::make_shared<velox::core::QueryConfigProvider>());
  registry->add(
      velox::functions::prestosql::PrestoQueryConfig::kPrefix,
      std::make_shared<velox::functions::prestosql::PrestoConfigProvider>());
  for (const auto& [id, connector] :
       velox::connector::ConnectorRegistry::global().snapshot()) {
    std::shared_ptr<const velox::config::ConfigProvider> executionProvider;
    if (const auto* provider = connector->configProvider()) {
      executionProvider = {connector, provider};
    }
    std::shared_ptr<const velox::config::ConfigProvider> metadataProvider;
    if (const auto metadata =
            connector::ConnectorMetadataRegistry::global().find(id)) {
      if (const auto* provider = metadata->configProvider()) {
        metadataProvider = {metadata, provider};
      }
    }
    if (executionProvider == nullptr && metadataProvider == nullptr) {
      continue;
    }
    registry->add(
        id,
        ConfigRegistry::combineProviders(
            id,
            {
                {"execution", executionProvider},
                {"metadata", metadataProvider},
            }));
  }
  return registry;
}
} // namespace

SessionConfig SqlTestBase::makeSessionConfig() {
  SessionConfig session{makeConfigRegistry()};
  session.set(
      kExecutionPrefix,
      velox::core::QueryConfig::kSessionTimezone,
      "Asia/Kolkata");
  session.set(
      kParserPrefix,
      ::axiom::sql::presto::ParserOptions::kGraphQueries,
      "true");
  return session;
}

void SqlTestBase::SetUpTestCase() {
  OperatorTestBase::SetUpTestCase();

  velox::functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  window::prestosql::registerAllWindowFunctions();
  functionRegistry_ = functions::prestosql::PrestoFunctions::makeRegistry();
  velox::functions::axiom::registerFunctions("axiom_");
  functionRegistry_->registerInternalPrefix("axiom_");
}

void SqlTestBase::SetUp() {
  OperatorTestBase::SetUp();

  velox::exec::ExchangeSource::factories().clear();
  velox::exec::ExchangeSource::registerFactory(
      velox::exec::test::createLocalExchangeSource);

  optimizerPool_ = rootPool_->addLeafChild("optimizer");
  executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(4);
  sessionConfig_.emplace(makeSessionConfig());
}

void SqlTestBase::TearDown() {
  optimizerPool_.reset();
  velox::exec::ExchangeSource::factories().clear();
  executor_.reset();

  OperatorTestBase::TearDown();
}

void SqlTestBase::createTable(
    const std::string& name,
    const std::vector<RowVectorPtr>& data) {
  VELOX_CHECK(!data.empty(), "Table data must not be empty: {}", name);

  connector_->addTable(name, asRowType(data[0]->type()));
  for (const auto& vector : data) {
    connector_->appendData(name, vector);
  }

  duckDbRunner().createTable(name, data);
}

namespace {
// Shared setup for makeLocalRunner / makeLocalRunnerV2: builds the query and
// optimizer contexts, sessions, and plan options, invokes 'plan' to optimize
// through the chosen optimizer, and wraps the result in a LocalRunner. 'plan'
// receives the prepared context and returns the executable plan.
template <typename PlanFn>
std::shared_ptr<runner::LocalRunner> makeLocalRunnerImpl(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    folly::Executor* executor,
    cache::AsyncDataCache* asyncDataCache,
    const std::shared_ptr<memory::MemoryPool>& rootPool,
    const std::shared_ptr<memory::MemoryPool>& optimizerPool,
    const std::shared_ptr<const FunctionRegistry>& functionRegistry,
    int32_t numWorkers,
    int32_t numDrivers,
    const SessionConfig& session,
    PlanFn plan) {
  static std::atomic<int32_t> queryCounter{0};
  auto queryId = fmt::format("sql_test_{}", ++queryCounter);
  const auto connectors = connectorIds();
  auto queryCtx = core::QueryCtx::create(
      executor,
      core::QueryConfig{application::SessionProperties::queryConfig(
          session, SqlTestBase::kExecutionPrefix)},
      application::SessionProperties::connectorConfigs(session, connectors),
      asyncDataCache,
      rootPool->addAggregateChild(queryId),
      /*spillExecutor=*/nullptr,
      // Task ids are built from this, and exchanges find their source by task
      // id.
      queryId);
  queryCtx->setRegistry(
      velox::common::vectorindex::LocalVectorIndexFormat::kRegistryKey,
      functions::prestosql::PrestoFunctions::makeVectorIndexFormats());

  auto allocator = std::make_unique<HashStringAllocator>(optimizerPool.get());
  auto graphContext = std::make_unique<optimizer::QueryGraphContext>(
      *allocator, OptimizerOptions::kMaxPlanObjectsDefault, functionRegistry);
  optimizer::queryCtx() = graphContext.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };

  exec::SimpleExpressionEvaluator evaluator(
      queryCtx.get(), optimizerPool.get());

  connector::SchemaResolver schemaResolver{
      connector::ConnectorMetadataRegistry::global()};

  MultiFragmentPlan::Options options;
  options.maxRemotePartitions = numWorkers;
  options.maxLocalPartitions = numDrivers;
  options.queryId = queryId;

  auto context = connector::makeTestContext(
      queryId,
      "test",
      application::SessionProperties::connectorProperties(session, connectors));
  auto optimizerProperties =
      session.effectiveValues(SqlTestBase::kOptimizerPrefix);
  auto optimizerOptions = OptimizerOptions::from(optimizerProperties);
  auto optimizerSession = std::make_shared<OptimizerSession>(
      context,
      functionRegistry,
      connector::makeTestStatWriter(),
      std::move(optimizerProperties),
      std::move(optimizerOptions));
  auto runnerSession = std::make_shared<runner::RunnerSession>(
      context, connector::makeTestStatWriter(), runner::Properties{});

  auto planAndStats = plan(
      *logicalPlan,
      schemaResolver,
      evaluator,
      options,
      optimizerSession,
      runnerSession,
      queryCtx);

  return std::make_shared<runner::LocalRunner>(
      std::move(runnerSession),
      planAndStats.plan,
      std::move(planAndStats.finishWrite),
      queryCtx,
      std::make_shared<runner::ConnectorSplitSourceFactory>(),
      optimizerPool,
      /*baseSpillDirectory=*/"");
}
} // namespace

std::shared_ptr<runner::LocalRunner> SqlTestBase::makeLocalRunner(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    folly::Executor* executor,
    cache::AsyncDataCache* asyncDataCache,
    const std::shared_ptr<memory::MemoryPool>& rootPool,
    const std::shared_ptr<memory::MemoryPool>& optimizerPool,
    int32_t numWorkers,
    int32_t numDrivers,
    const SessionConfig& session) {
  return makeLocalRunnerImpl(
      logicalPlan,
      executor,
      asyncDataCache,
      rootPool,
      optimizerPool,
      functionRegistry_,
      numWorkers,
      numDrivers,
      session,
      [](const logical_plan::LogicalPlanNode& logicalPlan,
         const connector::SchemaResolver& schemaResolver,
         velox::core::ExpressionEvaluator& evaluator,
         const MultiFragmentPlan::Options& options,
         const std::shared_ptr<OptimizerSession>& optimizerSession,
         const std::shared_ptr<runner::RunnerSession>& runnerSession,
         const std::shared_ptr<core::QueryCtx>& queryCtx) {
        v1::VeloxHistory history;
        v1::Optimization optimization(
            optimizerSession,
            runnerSession,
            logicalPlan,
            schemaResolver,
            history,
            queryCtx,
            evaluator,
            options);
        auto best = optimization.deprecatedBestPlan();
        return optimization.toVeloxPlan(best->op);
      });
}

std::shared_ptr<runner::LocalRunner> SqlTestBase::makeLocalRunnerV2(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    folly::Executor* executor,
    cache::AsyncDataCache* asyncDataCache,
    const std::shared_ptr<memory::MemoryPool>& rootPool,
    const std::shared_ptr<memory::MemoryPool>& optimizerPool,
    int32_t numWorkers,
    int32_t numDrivers,
    const SessionConfig& session,
    const std::vector<velox::Variant>& placeholderValues) {
  return makeLocalRunnerImpl(
      logicalPlan,
      executor,
      asyncDataCache,
      rootPool,
      optimizerPool,
      functionRegistry_,
      numWorkers,
      numDrivers,
      session,
      [&placeholderValues](
          const logical_plan::LogicalPlanNode& logicalPlan,
          const connector::SchemaResolver& schemaResolver,
          velox::core::ExpressionEvaluator& evaluator,
          const MultiFragmentPlan::Options& options,
          const std::shared_ptr<OptimizerSession>& optimizerSession,
          const std::shared_ptr<runner::RunnerSession>& /*runnerSession*/,
          const std::shared_ptr<core::QueryCtx>& queryCtx) {
        return Optimizer(
                   logicalPlan,
                   placeholderValues,
                   schemaResolver,
                   *optimizerSession,
                   evaluator,
                   queryCtx)
            .optimize(options);
      });
}

std::shared_ptr<runner::LocalRunner> SqlTestBase::makeRunner(
    std::string_view sql) {
  // Parse SQL to logical plan.
  auto parserProperties = sessionConfig_->effectiveValues(kParserPrefix);
  auto parserOptions =
      ::axiom::sql::presto::ParserOptions::from(parserProperties);
  ::axiom::sql::presto::PrestoParser parser(
      connectorId_,
      defaultSchema_,
      std::make_shared<::axiom::sql::presto::ParserSession>(
          connector::makeTestContext(
              "test",
              "test",
              application::SessionProperties::connectorProperties(
                  *sessionConfig_, connectorIds())),
          connector::makeTestStatWriter(),
          std::move(parserProperties),
          std::move(parserOptions),
          /*vectorIndexCodecTrainer=*/nullptr));
  auto statement = parser.parse(sql);

  VELOX_CHECK(
      statement->isSelect(), "Only SELECT statements are supported: {}", sql);

  return makeRunner(
      statement->as<::axiom::sql::presto::SelectStatement>()->plan());
}

std::shared_ptr<runner::LocalRunner> SqlTestBase::makeRunner(
    const logical_plan::LogicalPlanNodePtr& logicalPlan) {
  if (useV2_) {
    return makeLocalRunnerV2(
        logicalPlan,
        executor_.get(),
        cache::AsyncDataCache::getInstance(),
        rootPool_,
        optimizerPool_,
        numWorkers_,
        numDrivers_,
        *sessionConfig_);
  }
  return makeLocalRunner(
      logicalPlan,
      executor_.get(),
      cache::AsyncDataCache::getInstance(),
      rootPool_,
      optimizerPool_,
      numWorkers_,
      numDrivers_,
      *sessionConfig_);
}

std::vector<RowVectorPtr> SqlTestBase::runAndCollect(std::string_view sql) {
  auto runner = makeRunner(sql);

  std::vector<RowVectorPtr> results;
  runner->drain(
      [&](RowVectorPtr batch) { results.push_back(std::move(batch)); });

  return results;
}

void SqlTestBase::assertResults(
    std::string_view sql,
    bool checkColumnNames,
    std::optional<std::string> duckDbSql) {
  SCOPED_TRACE(sql);

  auto axiomResults = runAndCollect(sql);

  VELOX_CHECK(!axiomResults.empty(), "Axiom returned no results for: {}", sql);

  auto referenceSql = duckDbSql.value_or(std::string(sql));
  auto resultType = axiomResults[0]->rowType();

  velox::test::VectorAssertions::equalMultisets(
      duckDbRunner().execute(referenceSql, resultType),
      resultType,
      axiomResults);

  if (checkColumnNames) {
    auto duckDbResult = duckDbRunner().execute(referenceSql);
    ASSERT_EQ(resultType->size(), duckDbResult->names.size())
        << "Column count mismatch";
    for (size_t i = 0; i < resultType->size(); ++i) {
      ASSERT_EQ(resultType->nameOf(i), duckDbResult->names[i])
          << "Column name mismatch at position " << i;
    }
  }
}

void SqlTestBase::assertOrderedResults(
    std::string_view sql,
    bool checkColumnNames,
    std::optional<std::string> duckDbSql) {
  SCOPED_TRACE(sql);

  auto axiomResults = runAndCollect(sql);

  VELOX_CHECK(!axiomResults.empty(), "Axiom returned no results for: {}", sql);

  auto referenceSql = duckDbSql.value_or(std::string(sql));
  auto resultType = axiomResults[0]->rowType();

  ASSERT_NO_FATAL_FAILURE(
      velox::test::VectorAssertions::equalOrdered(
          duckDbRunner().executeOrdered(referenceSql, resultType),
          resultType,
          axiomResults,
          /*epsilon=*/false));

  if (checkColumnNames) {
    auto duckDbResult = duckDbRunner().execute(referenceSql);
    ASSERT_EQ(resultType->size(), duckDbResult->names.size())
        << "Column count mismatch";
    for (size_t i = 0; i < resultType->size(); ++i) {
      ASSERT_EQ(resultType->nameOf(i), duckDbResult->names[i])
          << "Column name mismatch at position " << i;
    }
  }
}

uint64_t SqlTestBase::run(std::string_view sql) {
  auto runner = makeRunner(sql);

  uint64_t numRows = 0;
  runner->drain([&](RowVectorPtr batch) { numRows += batch->size(); });
  return numRows;
}

void SqlTestBase::assertResultCount(
    std::string_view sql,
    uint64_t expectedCount) {
  SCOPED_TRACE(sql);

  ASSERT_EQ(run(sql), expectedCount);
}

void SqlTestBase::assertFailure(
    std::string_view sql,
    std::string_view expectedMessage) {
  SCOPED_TRACE(sql);

  VELOX_ASSERT_THROW(run(sql), expectedMessage);
}

} // namespace facebook::axiom::optimizer::test
