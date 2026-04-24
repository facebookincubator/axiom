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
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/optimizer/tests/TestDataPath.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/expression/Expr.h"
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

  executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(4);
  optimizer::FunctionRegistry::registerPrestoFunctions();
}

// static
void QueryTestBase::TearDownTestCase() {
  executor_.reset();
  HiveConnectorTestBase::TearDownTestCase();
}

void QueryTestBase::SetUp() {
  HiveConnectorTestBase::SetUp();

  velox::exec::ExchangeSource::factories().clear();
  velox::exec::ExchangeSource::registerFactory(
      velox::exec::test::createLocalExchangeSource);

  testConnector_ = std::make_shared<connector::TestConnector>(kTestConnectorId);
  velox::connector::registerConnector(testConnector_);
  testConnector_->addTpchTables();

  optimizerPool_ = rootPool_->addLeafChild("optimizer");

  if (gSuiteHistory) {
    history_ = std::move(gSuiteHistory);
  } else {
    history_ = std::make_unique<optimizer::VeloxHistory>();
  }

  optimizerOptions_ = OptimizerOptions();
  optimizerOptions_.traceFlags = FLAGS_optimizer_trace;
}

void QueryTestBase::TearDown() {
  // If we mean to save the history of running the suite, move the local history
  // to its static location.
  if (!FLAGS_history_save_path.empty()) {
    gSuiteHistory = std::move(history_);
  }
  queryCtx_.reset();
  optimizerPool_.reset();
  velox::connector::unregisterConnector(kTestConnectorId);
  testConnector_.reset();
  velox::exec::ExchangeSource::factories().clear();
  HiveConnectorTestBase::TearDown();
}

logical_plan::LogicalPlanNodePtr QueryTestBase::parseSelect(
    std::string_view sql,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema) {
  ::axiom::sql::presto::PrestoParser parser(defaultConnectorId, defaultSchema);

  auto statement = parser.parse(sql);

  VELOX_CHECK(statement->isSelect());
  return statement->as<::axiom::sql::presto::SelectStatement>()->plan();
}

namespace {
constexpr int32_t kWaitTimeoutUs = 500'000;

void waitForCompletion(const std::shared_ptr<runner::LocalRunner>& runner) {
  if (runner) {
    VELOX_CHECK(
        runner->waitForCompletion(kWaitTimeoutUs),
        "Timed out waiting for query completion");
  }
}
} // namespace

TestResult QueryTestBase::runVelox(const core::PlanNodePtr& plan) {
  MultiFragmentPlan::Options options;
  options.numWorkers = 1;
  options.numDrivers = 1;
  options.queryId = fmt::format("q{}", ++gQueryCounter);

  ExecutableFragment fragment(fmt::format("{}.0", options.queryId));
  fragment.fragment = core::PlanFragment(plan);

  optimizer::PlanAndStats planAndStats = {
      std::make_shared<MultiFragmentPlan>(
          std::vector<ExecutableFragment>{std::move(fragment)},
          std::move(options)),
  };

  return runFragmentedPlan(planAndStats);
}

TestResult QueryTestBase::runFragmentedPlan(
    optimizer::PlanAndStats& planAndStats) {
  TestResult result;

  SCOPE_EXIT {
    waitForCompletion(result.runner);
    queryCtx_.reset();
  };

  result.runner = std::make_shared<runner::LocalRunner>(
      planAndStats.plan,
      std::move(planAndStats.finishWrite),
      getQueryCtx(),
      std::make_shared<runner::ConnectorSplitSourceFactory>(),
      optimizerPool_);
  result.results = readCursor(result.runner);
  result.stats = result.runner->stats();
  history_->recordVeloxExecution(planAndStats, result.stats);

  return result;
}

std::shared_ptr<core::QueryCtx>& QueryTestBase::getQueryCtx() {
  if (queryCtx_) {
    return queryCtx_;
  }

  queryCtx_ = makeQueryCtx(fmt::format("q{}", ++gQueryCounter));

  return queryCtx_;
}

PlanCost QueryTestBase::optimizationCost(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    const MultiFragmentPlan::Options& options,
    const std::optional<OptimizerOptions>& optimizerOptions) {
  auto& queryCtx = getQueryCtx();
  auto allocator = std::make_unique<HashStringAllocator>(optimizerPool_.get());
  auto context = std::make_unique<optimizer::QueryGraphContext>(*allocator);
  optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };
  exec::SimpleExpressionEvaluator evaluator(
      queryCtx.get(), optimizerPool_.get());
  auto session = std::make_shared<Session>(queryCtx->queryId());
  connector::SchemaResolver schemaResolver;
  Optimization opt(
      session,
      *logicalPlan,
      schemaResolver,
      *history_,
      queryCtx,
      evaluator,
      optimizerOptions.value_or(optimizerOptions_),
      options);
  return opt.bestPlan()->cost;
}

void QueryTestBase::verifyOptimization(
    const logical_plan::LogicalPlanNode& logicalPlan,
    const std::function<void(Optimization&)>& callback,
    const std::optional<OptimizerOptions>& optimizerOptions) {
  auto& veloxQueryCtx = getQueryCtx();

  HashStringAllocator allocator(optimizerPool_.get());
  auto context = std::make_unique<optimizer::QueryGraphContext>(allocator);
  optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };

  exec::SimpleExpressionEvaluator evaluator(
      veloxQueryCtx.get(), optimizerPool_.get());

  auto session = std::make_shared<Session>(veloxQueryCtx->queryId());

  connector::SchemaResolver schemaResolver;
  VeloxHistory history;

  Optimization optimization(
      session,
      logicalPlan,
      schemaResolver,
      history,
      veloxQueryCtx,
      evaluator,
      optimizerOptions.value_or(optimizerOptions_),
      MultiFragmentPlan::Options{.numWorkers = 1, .numDrivers = 1});

  callback(optimization);
}

optimizer::PlanAndStats QueryTestBase::planVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const MultiFragmentPlan::Options& options,
    const std::optional<OptimizerOptions>& optimizerOptions,
    const std::optional<std::string>& planFilePathPrefix) {
  connector::SchemaResolver schemaResolver;
  return planVelox(
      plan, schemaResolver, options, optimizerOptions, planFilePathPrefix);
}

optimizer::PlanAndStats QueryTestBase::planVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const connector::SchemaResolver& schemaResolver,
    const MultiFragmentPlan::Options& options,
    const std::optional<OptimizerOptions>& optimizerOptions,
    const std::optional<std::string>& planFilePathPrefix) {
  auto& queryCtx = getQueryCtx();

  auto allocator = std::make_unique<HashStringAllocator>(optimizerPool_.get());
  auto context = std::make_unique<optimizer::QueryGraphContext>(*allocator);
  optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };
  exec::SimpleExpressionEvaluator evaluator(
      queryCtx.get(), optimizerPool_.get());

  auto session = std::make_shared<Session>(queryCtx->queryId());

  std::unique_ptr<std::ofstream> planPath;
  if (planFilePathPrefix.has_value()) {
    planPath = std::make_unique<std::ofstream>(
        fmt::format("{}.plans", planFilePathPrefix.value()));

    *planPath << "numWorkers: " << options.numWorkers << "\n";
    *planPath << "numDrivers: " << options.numDrivers << "\n\n";
  }

  SCOPE_EXIT {
    if (planPath != nullptr) {
      planPath->close();
    }
  };

  optimizer::Optimization opt(
      session,
      *plan,
      schemaResolver,
      *history_,
      queryCtx,
      evaluator,
      optimizerOptions.value_or(optimizerOptions_),
      options);
  if (planPath != nullptr) {
    *planPath << "Query Graph:\n\n" << opt.rootDt()->toString() << "\n\n";
  }

  auto best = opt.bestPlan();
  if (planPath != nullptr) {
    *planPath << "Optimized plan (oneline):\n\n"
              << best->op->toOneline() << "\n\n";
    *planPath << "Optimized plan:\n\n" << best->op->toString() << "\n\n";
  }

  auto planAndStats = opt.toVeloxPlan(best->op);
  if (planPath != nullptr) {
    *planPath << "Executable Velox plan:\n\n" << planAndStats.plan->toString();
    *planPath << "___END___\n";
  }

  return planAndStats;
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
    optimizer::PlanAndStats& experiment,
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
      {.numWorkers = 1, .numDrivers = 1},
  };

  if (options.numDrivers > 1) {
    testOptions.push_back({.numWorkers = 1, .numDrivers = options.numDrivers});
  }

  if (options.numWorkers > 1) {
    testOptions.push_back({.numWorkers = options.numWorkers, .numDrivers = 1});
  }

  if (options.numWorkers > 1 && options.numDrivers > 1) {
    testOptions.push_back(options);
  }

  for (const auto& test : testOptions) {
    SCOPED_TRACE(
        fmt::format(
            "workers: {}, drivers: {}", test.numWorkers, test.numDrivers));

    auto plan = planVelox(planNode, test);

    SCOPED_TRACE("plan:\n" + plan.plan->toString());
    auto result = runFragmentedPlan(plan);
    velox::exec::test::assertEqualResults(referenceResult, result.results);
  }
}

velox::core::PlanNodePtr QueryTestBase::toSingleNodePlan(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    int32_t numDrivers) {
  auto plan =
      planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = numDrivers}).plan;

  EXPECT_EQ(1, plan->fragments().size());
  return plan->fragments().at(0).fragment.planNode;
}

std::string QueryTestBase::getTestDataPath(const std::string& filename) {
  return test::getTestFilePath(fmt::format("test_data/{}", filename));
}

std::shared_ptr<velox::core::QueryCtx> QueryTestBase::makeQueryCtx(
    const std::string& queryId) {
  return velox::core::QueryCtx::create(
      executor_.get(),
      velox::core::QueryConfig(folly::copy(config_)),
      /*connectorConfigs=*/{},
      velox::cache::AsyncDataCache::getInstance(),
      /*pool=*/nullptr,
      /*spillExecutor=*/nullptr,
      queryId);
}

// static
std::vector<velox::RowVectorPtr> QueryTestBase::readCursor(
    const std::shared_ptr<runner::LocalRunner>& runner) {
  std::vector<velox::RowVectorPtr> result;
  while (auto rowVector = runner->next()) {
    result.push_back(rowVector);
  }
  return result;
}

} // namespace facebook::axiom::optimizer::test
