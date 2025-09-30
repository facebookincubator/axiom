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

#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/SchemaResolver.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/expression/Expr.h"
#include "velox/serializers/PrestoSerializer.h"

DECLARE_string(data_path);

DEFINE_uint32(optimizer_trace, 0, "Optimizer trace level");

DEFINE_bool(print_plan, false, "Print optimizer results");

DEFINE_string(data_format, "parquet", "Data format");

DEFINE_string(
    history_save_path,
    "",
    "Path to save sampling after the test suite");

using namespace facebook::velox;

namespace facebook::axiom::optimizer::test {
using namespace facebook::velox::exec;

void QueryTestBase::SetUp() {
  runner::test::LocalRunnerTestBase::SetUp();
  connector_ = velox::connector::getConnector(exec::test::kHiveConnectorId);
  rootPool_ = memory::memoryManager()->addRootPool("axiom_sql");
  optimizerPool_ = rootPool_->addLeafChild("optimizer");

  parquet::registerParquetReaderFactory();
  dwrf::registerDwrfReaderFactory();
  exec::ExchangeSource::registerFactory(exec::test::createLocalExchangeSource);
  if (!isRegisteredVectorSerde()) {
    serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }

  schema_ = std::make_shared<optimizer::SchemaResolver>();
  if (gSuiteHistory) {
    history_ = std::move(gSuiteHistory);
  } else {
    history_ = std::make_unique<optimizer::VeloxHistory>();
  }
  optimizerOptions_ = OptimizerOptions();
  optimizerOptions_.traceFlags = FLAGS_optimizer_trace;

  optimizer::FunctionRegistry::registerPrestoFunctions();
}

void QueryTestBase::TearDown() {
  // If we mean to save the history of running the suite, move the local history
  // to its static location.
  if (!FLAGS_history_save_path.empty()) {
    gSuiteHistory = std::move(history_);
  }
  queryCtx_.reset();
  velox::connector::unregisterConnector(exec::test::kHiveConnectorId);
  connector_.reset();
  optimizerPool_.reset();
  schema_.reset();
  rootPool_.reset();
  LocalRunnerTestBase::TearDown();
}

void QueryTestBase::tablesCreated() {
  auto metadata = dynamic_cast<connector::hive::LocalHiveConnectorMetadata*>(
      connector::ConnectorMetadata::metadata(connector_.get()));
  VELOX_CHECK_NOT_NULL(metadata);
  metadata->reinitialize();
}

namespace {
void waitForCompletion(const std::shared_ptr<runner::LocalRunner>& runner) {
  if (runner) {
    try {
      runner->waitForCompletion(50000);
    } catch (const std::exception& /*ignore*/) {
    }
  }
}
} // namespace

TestResult QueryTestBase::runVelox(
    const core::PlanNodePtr& plan,
    runner::MultiFragmentPlan::Options options) {
  if (options.queryId.empty()) {
    options.queryId = fmt::format("q{}", ++gQueryCounter);
  }

  runner::ExecutableFragment fragment(fmt::format("{}.0", options.queryId));
  fragment.fragment = core::PlanFragment(plan);

  optimizer::PlanAndStats planAndStats = {
      .plan = std::make_shared<runner::MultiFragmentPlan>(
          std::vector<runner::ExecutableFragment>{std::move(fragment)},
          std::move(options))};

  return runFragmentedPlan(planAndStats);
}

TestResult QueryTestBase::runFragmentedPlan(
    const optimizer::PlanAndStats& fragmentedPlan) {
  TestResult result;
  result.veloxString = veloxString(fragmentedPlan.plan);

  SCOPE_EXIT {
    waitForCompletion(result.runner);
    queryCtx_.reset();
  };

  result.runner =
      std::make_shared<runner::LocalRunner>(fragmentedPlan.plan, getQueryCtx());

  while (auto rows = result.runner->next()) {
    result.results.push_back(std::move(rows));
  }
  result.stats = result.runner->stats();
  history_->recordVeloxExecution(fragmentedPlan, result.stats);

  return result;
}

std::shared_ptr<core::QueryCtx> QueryTestBase::getQueryCtx() {
  if (queryCtx_) {
    return queryCtx_;
  }

  ++gQueryCounter;

  std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
      connectorConfigs = {
          {exec::test::kHiveConnectorId,
           std::make_shared<config::ConfigBase>(folly::copy(hiveConfig_))}};

  queryCtx_ = core::QueryCtx::create(
      executor_.get(),
      core::QueryConfig(config_),
      std::move(connectorConfigs),
      cache::AsyncDataCache::getInstance(),
      rootPool_->shared_from_this(),
      spillExecutor_.get(),
      fmt::format("query_{}", gQueryCounter));
  return queryCtx_;
}

optimizer::PlanAndStats QueryTestBase::planVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const runner::MultiFragmentPlan::Options& options,
    std::string* planString) {
  auto queryCtx = getQueryCtx();

  // The default Locus for planning is the system and data of 'connector_'.
  optimizer::Locus locus(connector_->connectorId().c_str(), connector_.get());
  auto allocator = std::make_unique<HashStringAllocator>(optimizerPool_.get());
  auto context = std::make_unique<optimizer::QueryGraphContext>(*allocator);
  optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };
  exec::SimpleExpressionEvaluator evaluator(
      queryCtx_.get(), optimizerPool_.get());

  optimizer::Schema veraxSchema("test", schema_.get(), &locus);
  optimizer::Optimization opt(
      *plan,
      veraxSchema,
      *history_,
      queryCtx_,
      evaluator,
      optimizerOptions_,
      options);
  auto best = opt.bestPlan();
  if (planString) {
    *planString = best->op->toString(true, false);
  }
  return opt.toVeloxPlan(best->op);
}

TestResult QueryTestBase::runVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const runner::MultiFragmentPlan::Options& options) {
  TestResult result;
  auto veloxPlan = planVelox(plan, options, &result.planString);
  return runFragmentedPlan(veloxPlan);
}

std::string QueryTestBase::veloxString(
    const runner::MultiFragmentPlanPtr& plan) {
  folly::F14FastMap<core::PlanNodeId, const core::TableScanNode*> scans;
  for (const auto& fragment : plan->fragments()) {
    velox::core::PlanNode::findFirstNode(
        fragment.fragment.planNode.get(), [&](const auto* node) {
          if (auto scan = dynamic_cast<const core::TableScanNode*>(node)) {
            scans.emplace(scan->id(), scan);
          }
          return false;
        });
  }

  auto planNodeDetails = [&](const core::PlanNodeId& planNodeId,
                             std::string_view indentation,
                             std::ostream& stream) {
    auto it = scans.find(planNodeId);
    if (it != scans.end()) {
      const auto* scan = it->second;
      for (const auto& [name, handle] : scan->assignments()) {
        stream << indentation << name << " = " << handle->name() << std::endl;
      }
    }
  };

  return plan->toString(true, planNodeDetails);
}

TestResult QueryTestBase::assertSame(
    const core::PlanNodePtr& reference,
    const optimizer::PlanAndStats& experiment,
    const runner::MultiFragmentPlan::Options& options) {
  auto referenceResult = runVelox(reference, options);
  auto experimentResult = runFragmentedPlan(experiment);

  exec::test::assertEqualResults(
      referenceResult.results, experimentResult.results);

  return referenceResult;
}

void QueryTestBase::checkSame(
    const logical_plan::LogicalPlanNodePtr& planNode,
    const velox::core::PlanNodePtr& referencePlan,
    const axiom::runner::MultiFragmentPlan::Options& options) {
  VELOX_CHECK_NOT_NULL(planNode);
  VELOX_CHECK_NOT_NULL(referencePlan);

  auto referenceResult = runVelox(referencePlan);
  SCOPED_TRACE("reference plan:\n" + referencePlan->toString(true, true));
  {
    SCOPED_TRACE("single node and single thread");
    auto plan = planVelox(planNode, {.numWorkers = 1, .numDrivers = 1});
    SCOPED_TRACE("plan:\n" + plan.plan->toString());
    auto result = runFragmentedPlan(plan);
    velox::exec::test::assertEqualResults(
        referenceResult.results, result.results);
  }
  if (options.numDrivers > 1) {
    SCOPED_TRACE("single node and multi thread");
    auto plan = planVelox(
        planNode, {.numWorkers = 1, .numDrivers = options.numDrivers});
    SCOPED_TRACE("plan:\n" + plan.plan->toString());
    auto result = runFragmentedPlan(plan);
    velox::exec::test::assertEqualResults(
        referenceResult.results, result.results);
  }
  if (options.numWorkers > 1) {
    SCOPED_TRACE("multi node and single thread");
    auto plan = planVelox(
        planNode, {.numWorkers = options.numWorkers, .numDrivers = 1});
    SCOPED_TRACE("plan:\n" + plan.plan->toString());
    auto result = runFragmentedPlan(plan);
    velox::exec::test::assertEqualResults(
        referenceResult.results, result.results);
  }
  if (options.numWorkers > 1 && options.numDrivers > 1) {
    SCOPED_TRACE("multi node and multi thread");
    auto plan = planVelox(
        planNode,
        {.numWorkers = options.numWorkers, .numDrivers = options.numDrivers});
    SCOPED_TRACE("plan:\n" + plan.plan->toString());
    auto result = runFragmentedPlan(plan);
    velox::exec::test::assertEqualResults(
        referenceResult.results, result.results);
  }
}

velox::core::PlanNodePtr QueryTestBase::toSingleNodePlan(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    int32_t numDrivers) {
  schema_ = std::make_shared<SchemaResolver>();

  auto plan =
      planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = numDrivers}).plan;

  EXPECT_EQ(1, plan->fragments().size());
  return plan->fragments().at(0).fragment.planNode;
}

} // namespace facebook::axiom::optimizer::test
