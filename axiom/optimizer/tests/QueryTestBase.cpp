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

#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/connectors/tests/TestConnectorContext.h"
#include "axiom/optimizer/OptimizerOptions.h"

#include <folly/coro/BlockingWait.h>
#include <ctime>
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/functions/prestosql/PrestoFunctions.h"
#include "axiom/optimizer/Optimize.h"
#include "axiom/optimizer/tests/TestDataPath.h"
#include "axiom/optimizer/v1/Optimization.h"
#include "axiom/optimizer/v1/Plan.h"
#include "axiom/optimizer/v1/VeloxHistory.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/expression/Expr.h"
#include "velox/functions/axiom/Register.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

DECLARE_string(data_path);

DEFINE_uint32(optimizer_trace, 0, "Optimizer trace level");

DEFINE_string(
    history_save_path,
    "",
    "Path to save sampling after the test suite");

using namespace facebook::velox;

namespace facebook::axiom::optimizer::test {

// static
void QueryTestBase::SetUpTestCase() {
  HiveConnectorTestBase::SetUpTestCase();
  velox::window::prestosql::registerAllWindowFunctions();

  functionRegistry_ = functions::prestosql::PrestoFunctions::makeRegistry();
  velox::functions::axiom::registerFunctions("axiom_");
  functionRegistry_->registerInternalPrefix("axiom_");
}

// static
void QueryTestBase::TearDownTestCase() {
  functionRegistry_.reset();
  HiveConnectorTestBase::TearDownTestCase();
}

void QueryTestBase::SetUp() {
  HiveConnectorTestBase::SetUp();

  velox::exec::ExchangeSource::factories().clear();
  velox::exec::ExchangeSource::registerFactory(
      velox::exec::test::createLocalExchangeSource);

  testConnector_ = std::make_shared<connector::TestConnector>(kTestConnectorId);
  velox::connector::registerConnector(testConnector_);
  connector::ConnectorMetadataRegistry::global().insert(
      kTestConnectorId, testConnector_->metadata());
  configureTestConnector();

  optimizerPool_ = rootPool_->addLeafChild("optimizer");

  if (gSuiteHistory) {
    history_ = std::move(gSuiteHistory);
  } else {
    history_ = std::make_unique<optimizer::v1::VeloxHistory>();
  }

  optimizerOptions_ = OptimizerOptions();
  optimizerOptions_.traceFlags = FLAGS_optimizer_trace;
}

connector::StatWriterProvider QueryTestBase::connectorStatWriterProvider() {
  // Non-owning: the fixture outlives every context it makes.
  return [this](std::string_view) {
    return std::shared_ptr<velox::BaseRuntimeStatWriter>(
        &connectorStatsWriter_, [](auto*) {});
  };
}

connector::ConnectorContextPtr QueryTestBase::makeConnectorContext(
    std::string queryId) {
  return std::make_shared<connector::ConnectorContext>(
      std::move(queryId),
      "test",
      connectorSessionProperties_,
      connectorStatWriterProvider(),
      connector::ConnectorMetadataRegistry::global());
}

void QueryTestBase::configureTestConnector() {
  testConnector_->addTpchTables();
}

void QueryTestBase::TearDown() {
  // If we mean to save the history of running the suite, move the local history
  // to its static location.
  if (!FLAGS_history_save_path.empty()) {
    gSuiteHistory = std::move(history_);
  }
  optimizerPool_.reset();
  connector::ConnectorMetadataRegistry::global().erase(kTestConnectorId);
  velox::connector::unregisterConnector(kTestConnectorId);
  testConnector_.reset();
  velox::exec::ExchangeSource::factories().clear();
  HiveConnectorTestBase::TearDown();
}

// static
logical_plan::PlanBuilder::Context QueryTestBase::makeContext(
    const std::string& connectorId) {
  logical_plan::PlanBuilder::Context context{connectorId, kDefaultSchema};
  context.connectorContext = connector::makeTestContext("test");
  return context;
}

// static
velox::core::PlanMatcherBuilder QueryTestBase::matchEmptyValues() {
  return velox::core::PlanMatcherBuilder().values(
      [](const velox::core::PlanNodePtr& node) {
        for (const auto& vector :
             node->as<velox::core::ValuesNode>()->values()) {
          EXPECT_EQ(vector->size(), 0);
        }
      });
}

namespace {
::axiom::sql::presto::SqlStatementPtr parseStatement(
    std::string_view sql,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema) {
  ::axiom::sql::presto::ParserOptions parserOptions;
  parserOptions.graphQueries = true;
  ::axiom::sql::presto::PrestoParser parser(
      defaultConnectorId,
      defaultSchema,
      std::make_shared<::axiom::sql::presto::ParserSession>(
          connector::makeTestContext("test"),
          connector::makeTestStatWriter(),
          connector::Properties{},
          parserOptions));
  return parser.parse(sql);
}
} // namespace

logical_plan::LogicalPlanNodePtr QueryTestBase::parseSelect(
    std::string_view sql,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema) {
  auto statement = parseStatement(sql, defaultConnectorId, defaultSchema);

  VELOX_CHECK(statement->isSelect());
  return statement->as<::axiom::sql::presto::SelectStatement>()->plan();
}

logical_plan::LogicalPlanNodePtr QueryTestBase::parseDelete(
    std::string_view sql) {
  auto statement = parseStatement(sql, defaultConnectorId_, "default");

  VELOX_CHECK(statement->isDelete());
  return statement->as<::axiom::sql::presto::DeleteStatement>()->plan();
}

::axiom::sql::presto::RefreshVectorIndexStatement::Plans
QueryTestBase::parseRefresh(std::string_view sql) {
  auto statement = parseStatement(sql, defaultConnectorId_, "default");

  VELOX_CHECK(statement->isRefreshVectorIndex());
  return statement->as<::axiom::sql::presto::RefreshVectorIndexStatement>()
      ->plans();
}

namespace {
// Non-owning: the fixture outlives every session it makes.
std::shared_ptr<velox::BaseRuntimeStatWriter> borrow(
    velox::BaseRuntimeStatWriter& writer) {
  return std::shared_ptr<velox::BaseRuntimeStatWriter>(&writer, [](auto*) {});
}

OptimizerSessionPtr makeOptimizerSession(
    const connector::ConnectorContextPtr& connectorContext,
    std::shared_ptr<const FunctionRegistry> functionRegistry,
    OptimizerOptions options,
    velox::BaseRuntimeStatWriter& statsWriter) {
  return std::make_shared<OptimizerSession>(
      connectorContext,
      std::move(functionRegistry),
      borrow(statsWriter),
      connector::Properties{},
      std::move(options));
}

runner::RunnerSessionPtr makeRunnerSession(
    const connector::ConnectorContextPtr& connectorContext,
    velox::BaseRuntimeStatWriter& statsWriter) {
  return std::make_shared<runner::RunnerSession>(
      connectorContext, borrow(statsWriter), runner::Properties{});
}
} // namespace

TestResult QueryTestBase::runVelox(const core::PlanNodePtr& plan) {
  PlannedQuery query;
  query.queryCtx = newQueryCtx();
  query.connectorContext = makeConnectorContext(query.queryCtx->queryId());
  MultiFragmentPlan::Options options;
  options.maxRemotePartitions = 1;
  options.maxLocalPartitions = 1;
  options.queryId = query.queryCtx->queryId();

  query.plan = std::make_shared<MultiFragmentPlan>(
      std::vector<core::Stage>{{.id = 1, .plan = plan}},
      folly::F14FastMap<int32_t, FragmentScans>{},
      std::move(options));

  return runFragmentedPlan(query);
}

TestResult QueryTestBase::runFragmentedPlan(PlannedQuery& query) {
  VELOX_CHECK_NOT_NULL(query.queryCtx, "Query has already been executed");
  auto queryCtx = std::move(query.queryCtx);
  auto connectorContext = std::move(query.connectorContext);
  auto runnerSession = makeRunnerSession(connectorContext, statsWriter_);
  auto runner = std::make_shared<runner::LocalRunner>(
      std::move(runnerSession),
      query.plan,
      std::move(query.finishWrite),
      std::move(queryCtx),
      std::make_shared<runner::ConnectorSplitSourceFactory>(),
      optimizerPool_,
      /*baseSpillDirectory=*/"");

  TestResult result;
  result.runner = std::move(runner);
  result.runner->drain([&](velox::RowVectorPtr batch) {
    result.results.push_back(std::move(batch));
  });
  result.stats = result.runner->stats();
  history_->recordVeloxExecution(query, result.stats);

  return result;
}

std::shared_ptr<core::QueryCtx> QueryTestBase::newQueryCtx() {
  return makeQueryCtx(fmt::format("q{}", ++gQueryCounter));
}

v1::PlanCost QueryTestBase::optimizationCost(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const MultiFragmentPlan::Options& options,
    const std::optional<OptimizerOptions>& optimizerOptions) {
  auto queryCtx = newQueryCtx();
  auto allocator = std::make_unique<HashStringAllocator>(optimizerPool_.get());
  auto context = std::make_unique<optimizer::QueryGraphContext>(
      *allocator, OptimizerOptions::kMaxPlanObjectsDefault, functionRegistry_);
  optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };
  exec::SimpleExpressionEvaluator evaluator(
      queryCtx.get(), optimizerPool_.get());
  connector::SchemaResolver schemaResolver{
      connector::ConnectorMetadataRegistry::global()};
  const auto connectorContext = makeConnectorContext(queryCtx->queryId());
  v1::Optimization opt(
      makeOptimizerSession(
          connectorContext,
          functionRegistry_,
          optimizerOptions.value_or(optimizerOptions_),
          statsWriter_),
      makeRunnerSession(connectorContext, statsWriter_),
      *logicalPlan,
      schemaResolver,
      *history_,
      queryCtx,
      evaluator,
      options);
  return opt.deprecatedBestPlan()->cost;
}

void QueryTestBase::verifyOptimization(
    const logical_plan::LogicalPlanNode& logicalPlan,
    const std::function<void(v1::Optimization&)>& callback,
    const std::optional<OptimizerOptions>& optimizerOptions) {
  auto veloxQueryCtx = newQueryCtx();

  HashStringAllocator allocator(optimizerPool_.get());
  auto context = std::make_unique<optimizer::QueryGraphContext>(
      allocator, OptimizerOptions::kMaxPlanObjectsDefault, functionRegistry_);
  optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };

  exec::SimpleExpressionEvaluator evaluator(
      veloxQueryCtx.get(), optimizerPool_.get());

  connector::SchemaResolver schemaResolver{
      connector::ConnectorMetadataRegistry::global()};
  v1::VeloxHistory history;

  const auto connectorContext = makeConnectorContext(veloxQueryCtx->queryId());
  v1::Optimization optimization(
      makeOptimizerSession(
          connectorContext,
          functionRegistry_,
          optimizerOptions.value_or(optimizerOptions_),
          statsWriter_),
      makeRunnerSession(connectorContext, statsWriter_),
      logicalPlan,
      schemaResolver,
      history,
      veloxQueryCtx,
      evaluator,
      MultiFragmentPlan::Options{
          .maxRemotePartitions = 1, .maxLocalPartitions = 1});

  callback(optimization);
}

void QueryTestBase::verifyOptimization(
    const logical_plan::LogicalPlanNode& logicalPlan,
    Optimizer::Pass pass,
    const std::function<void(NodeCP)>& callback,
    const MultiFragmentPlan::Options& options,
    const std::optional<OptimizerOptions>& optimizerOptions) {
  auto veloxQueryCtx = newQueryCtx();

  HashStringAllocator allocator(optimizerPool_.get());
  auto context = std::make_unique<optimizer::QueryGraphContext>(
      allocator, OptimizerOptions::kMaxPlanObjectsDefault, functionRegistry_);
  optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };

  exec::SimpleExpressionEvaluator evaluator(
      veloxQueryCtx.get(), optimizerPool_.get());
  connector::SchemaResolver schemaResolver{
      connector::ConnectorMetadataRegistry::global()};
  const auto connectorContext = makeConnectorContext(veloxQueryCtx->queryId());
  auto session = makeOptimizerSession(
      connectorContext,
      functionRegistry_,
      optimizerOptions.value_or(optimizerOptions_),
      statsWriter_);
  Optimizer optimizer{
      logicalPlan, schemaResolver, *session, evaluator, veloxQueryCtx};
  const auto debugPlan = optimizer.debugPlanTo(options, pass);
  callback(debugPlan.root);
}

PlannedQuery QueryTestBase::planVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const MultiFragmentPlan::Options& options,
    const std::optional<OptimizerOptions>& optimizerOptions,
    const std::optional<std::string>& planFilePathPrefix) {
  connector::SchemaResolver schemaResolver{
      connector::ConnectorMetadataRegistry::global()};
  return planVelox(
      plan, schemaResolver, options, optimizerOptions, planFilePathPrefix);
}

optimizer::PlanAndStats QueryTestBase::planVeloxWithPlaceholders(
    const logical_plan::LogicalPlanNodePtr& plan,
    const std::vector<velox::Variant>& placeholderValues) {
  connector::SchemaResolver schemaResolver{
      connector::ConnectorMetadataRegistry::global()};
  return planVelox(
      plan,
      placeholderValues,
      schemaResolver,
      {.maxRemotePartitions = 4, .maxLocalPartitions = 4},
      /*optimizerOptions=*/std::nullopt,
      /*planFilePathPrefix=*/std::nullopt);
}

namespace {
std::string formatCurrentTime() {
  const std::time_t now = std::time(nullptr);
  std::tm localNow{};
  ::localtime_r(&now, &localNow);
  char timestamp[64];
  std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &localNow);
  return timestamp;
}
} // namespace

PlannedQuery QueryTestBase::planVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const connector::SchemaResolver& schemaResolver,
    const MultiFragmentPlan::Options& options,
    const std::optional<OptimizerOptions>& optimizerOptions,
    const std::optional<std::string>& planFilePathPrefix) {
  return planVelox(
      plan,
      /*placeholderValues=*/{},
      schemaResolver,
      options,
      optimizerOptions,
      planFilePathPrefix);
}

PlannedQuery QueryTestBase::planVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const std::vector<velox::Variant>& placeholderValues,
    const connector::SchemaResolver& schemaResolver,
    const MultiFragmentPlan::Options& options,
    const std::optional<OptimizerOptions>& optimizerOptions,
    const std::optional<std::string>& planFilePathPrefix) {
  auto queryCtx =
      options.queryId.empty() ? newQueryCtx() : makeQueryCtx(options.queryId);
  auto queryOptions = options;
  queryOptions.queryId = queryCtx->queryId();

  auto allocator = std::make_unique<HashStringAllocator>(optimizerPool_.get());
  auto context = std::make_unique<optimizer::QueryGraphContext>(
      *allocator, OptimizerOptions::kMaxPlanObjectsDefault, functionRegistry_);
  optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };
  exec::SimpleExpressionEvaluator evaluator(
      queryCtx.get(), optimizerPool_.get());

  std::unique_ptr<std::ofstream> planPath;
  if (planFilePathPrefix.has_value()) {
    planPath = std::make_unique<std::ofstream>(
        fmt::format("{}.plans", planFilePathPrefix.value()));

    *planPath << "generated: " << formatCurrentTime() << " (snapshot)\n";
    *planPath << "maxRemotePartitions: " << options.maxRemotePartitions << "\n";
    *planPath << "maxLocalPartitions: " << options.maxLocalPartitions << "\n\n";
  }

  SCOPE_EXIT {
    if (planPath != nullptr) {
      planPath->close();
    }
  };

  auto connectorContext = makeConnectorContext(queryCtx->queryId());
  auto session = makeOptimizerSession(
      connectorContext,
      functionRegistry_,
      optimizerOptions.value_or(optimizerOptions_),
      statsWriter_);

  optimizer::PlanAndStats planAndStats;
  if (useV2_) {
    planAndStats = Optimizer(
                       *plan,
                       placeholderValues,
                       schemaResolver,
                       *session,
                       evaluator,
                       queryCtx)
                       .optimize(queryOptions, afterPass_);
  } else {
    VELOX_CHECK(
        placeholderValues.empty(), "Placeholders require the v2 optimizer");
    optimizer::v1::Optimization opt(
        session,
        makeRunnerSession(connectorContext, statsWriter_),
        *plan,
        schemaResolver,
        *history_,
        queryCtx,
        evaluator,
        queryOptions);
    // The query-graph and optimized-plan sections are v1-only; v2 exposes
    // neither here.
    if (planPath != nullptr) {
      *planPath << "Query Graph:\n\n" << opt.rootDt()->toString() << "\n\n";
    }

    auto best = opt.deprecatedBestPlan();
    if (planPath != nullptr) {
      *planPath << "Optimized plan (oneline):\n\n"
                << best->op->toOneline() << "\n\n";
      *planPath << "Optimized plan:\n\n" << best->op->toString() << "\n\n";
    }

    planAndStats = opt.toVeloxPlan(best->op);
  }

  if (planPath != nullptr) {
    *planPath << "Executable Velox plan:\n\n" << planAndStats.plan->toString();
    *planPath << "___END___\n";
  }

  return {
      std::move(planAndStats),
      std::move(queryCtx),
      std::move(connectorContext),
  };
}

TestResult QueryTestBase::runVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const MultiFragmentPlan::Options& options) {
  auto veloxPlan = planVelox(plan, options);
  return runFragmentedPlan(veloxPlan);
}

TestResult QueryTestBase::runVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const connector::SchemaResolver& schemaResolver,
    const MultiFragmentPlan::Options& options) {
  auto veloxPlan = planVelox(plan, schemaResolver, options);
  return runFragmentedPlan(veloxPlan);
}

TestResult QueryTestBase::checkSame(
    PlannedQuery& experiment,
    const core::PlanNodePtr& reference) {
  auto referenceResult = runVelox(reference);
  auto experimentResult = runFragmentedPlan(experiment);

  exec::test::assertEqualResults(
      referenceResult.results, experimentResult.results);

  return referenceResult;
}

void QueryTestBase::checkSame(
    const logical_plan::LogicalPlanNodePtr& planNode,
    const velox::core::PlanNodePtr& referencePlan,
    const MultiFragmentPlan::Options& options) {
  VELOX_CHECK_NOT_NULL(planNode);
  VELOX_CHECK_NOT_NULL(referencePlan);

  SCOPED_TRACE("reference plan:\n" + referencePlan->toString(true, true));
  auto referenceResult = runVelox(referencePlan);
  checkSame(planNode, referenceResult.results, options);
}

void QueryTestBase::checkSame(
    const logical_plan::LogicalPlanNodePtr& planNode,
    const std::vector<velox::RowVectorPtr>& referenceResult,
    const MultiFragmentPlan::Options& options) {
  VELOX_CHECK_NOT_NULL(planNode);

  std::vector<MultiFragmentPlan::Options> testOptions = {
      {.maxRemotePartitions = 1, .maxLocalPartitions = 1},
  };

  if (options.maxLocalPartitions > 1) {
    testOptions.push_back(
        {.maxRemotePartitions = 1,
         .maxLocalPartitions = options.maxLocalPartitions});
  }

  if (options.maxRemotePartitions > 1) {
    testOptions.push_back(
        {.maxRemotePartitions = options.maxRemotePartitions,
         .maxLocalPartitions = 1});
  }

  if (options.maxRemotePartitions > 1 && options.maxLocalPartitions > 1) {
    testOptions.push_back(options);
  }

  for (const auto& test : testOptions) {
    SCOPED_TRACE(
        fmt::format(
            "workers: {}, drivers: {}",
            test.maxRemotePartitions,
            test.maxLocalPartitions));

    auto plan = planVelox(planNode, test);

    SCOPED_TRACE("plan:\n" + plan.plan->toString());
    auto result = runFragmentedPlan(plan);
    velox::exec::test::assertEqualResults(referenceResult, result.results);
  }
}

velox::core::PlanNodePtr QueryTestBase::toSingleNodePlan(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const std::vector<velox::Variant>& placeholderValues,
    int32_t numDrivers) {
  connector::SchemaResolver schemaResolver{
      connector::ConnectorMetadataRegistry::global()};
  auto plan = planVelox(
                  logicalPlan,
                  placeholderValues,
                  schemaResolver,
                  {.maxRemotePartitions = 1, .maxLocalPartitions = numDrivers},
                  std::nullopt,
                  std::nullopt)
                  .plan;
  EXPECT_EQ(1, plan->stages().size());
  return plan->stages().at(0).plan;
}

std::string QueryTestBase::getTestDataPath(const std::string& filename) {
  return test::getTestFilePath(fmt::format("test_data/{}", filename));
}

std::shared_ptr<velox::core::QueryCtx> QueryTestBase::makeQueryCtx(
    const std::string& queryId) {
  auto queryCtx = velox::core::QueryCtx::create(
      executor_.get(),
      velox::core::QueryConfig(folly::copy(config_)),
      /*connectorConfigs=*/{},
      velox::cache::AsyncDataCache::getInstance(),
      /*pool=*/nullptr,
      /*spillExecutor=*/nullptr,
      queryId);
  queryCtx->setRegistry(
      velox::common::vectorindex::LocalVectorIndexFormat::kRegistryKey,
      functions::prestosql::PrestoFunctions::makeVectorIndexFormats());
  return queryCtx;
}

} // namespace facebook::axiom::optimizer::test
