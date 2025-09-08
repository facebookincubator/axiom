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

#include "axiom/optimizer/connectors/hive/LocalHiveConnectorMetadata.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/SchemaResolver.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/optimizer/connectors/ConnectorSplitSource.h"
#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/expression/Expr.h"
#include "velox/serializers/PrestoSerializer.h"

DECLARE_string(data_path);

DEFINE_uint32(optimizer_trace, 0, "Optimizer trace level");

DEFINE_bool(print_plan, false, "Print optimizer results");

DEFINE_int32(num_drivers, 4, "Number of drivers");
DEFINE_int32(num_workers, 4, "Number of in-process workers");

DEFINE_string(data_format, "parquet", "Data format");

DEFINE_string(
    history_save_path,
    "",
    "Path to save sampling after the test suite");

namespace facebook::velox::optimizer::test {
using namespace facebook::velox::exec;

void QueryTestBase::SetUp() {
  axiom::runner::test::LocalRunnerTestBase::SetUp();
  connector_ = connector::getConnector(exec::test::kHiveConnectorId);
  rootPool_ = memory::memoryManager()->addRootPool("velox_sql");
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

  schema_ = std::make_shared<velox::optimizer::SchemaResolver>();
  if (suiteHistory_) {
    history_ = std::move(suiteHistory_);
  } else {
    history_ = std::make_unique<velox::optimizer::VeloxHistory>();
  }
  optimizerOptions_ = OptimizerOptions();
  optimizerOptions_.traceFlags = FLAGS_optimizer_trace;

  velox::optimizer::FunctionRegistry::registerPrestoFunctions();
}

void QueryTestBase::TearDown() {
  // If we mean to save the history of running the suite, move the local history
  // to its static location.
  if (!FLAGS_history_save_path.empty()) {
    suiteHistory_ = std::move(history_);
  }
  queryCtx_.reset();
  connector::unregisterConnector(exec::test::kHiveConnectorId);
  connector_.reset();
  optimizerPool_.reset();
  schema_.reset();
  rootPool_.reset();
  LocalRunnerTestBase::TearDown();
}

void QueryTestBase::tablesCreated() {
  auto metadata = dynamic_cast<connector::hive::LocalHiveConnectorMetadata*>(
      connector_->metadata());
  VELOX_CHECK_NOT_NULL(metadata);
  metadata->reinitialize();
}

namespace {
void waitForCompletion(
    const std::shared_ptr<axiom::runner::LocalRunner>& runner) {
  if (runner) {
    try {
      runner->waitForCompletion(50000);
    } catch (const std::exception& /*ignore*/) {
    }
  }
}

void gatherScans(
    const core::PlanNodePtr& plan,
    std::vector<core::TableScanNodePtr>& scans) {
  if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(plan)) {
    scans.push_back(scan);
    return;
  }
  for (auto& source : plan->sources()) {
    gatherScans(source, scans);
  }
}

} // namespace

TestResult QueryTestBase::runVelox(const core::PlanNodePtr& plan) {
  axiom::runner::MultiFragmentPlan::Options options = {
      .queryId = fmt::format("q{}", ++queryCounter_),
      .numWorkers = 1,
      .numDrivers = FLAGS_num_drivers};

  axiom::runner::ExecutableFragment fragment(
      fmt::format("{}.0", options.queryId));
  fragment.fragment = core::PlanFragment(plan);
  gatherScans(plan, fragment.scans);

  optimizer::PlanAndStats planAndStats = {
      .plan = std::make_shared<axiom::runner::MultiFragmentPlan>(
          std::vector<axiom::runner::ExecutableFragment>{std::move(fragment)},
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

  result.runner = std::make_shared<axiom::runner::LocalRunner>(
      fragmentedPlan.plan,
      getQueryCtx(),
      std::make_shared<connector::ConnectorSplitSourceFactory>());

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

  ++queryCounter_;

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
      fmt::format("query_{}", queryCounter_));
  return queryCtx_;
}

optimizer::PlanAndStats QueryTestBase::planVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    std::string* planString) {
  return planVelox(
      plan,
      {.numWorkers = FLAGS_num_workers, .numDrivers = FLAGS_num_drivers},
      planString);
}

optimizer::PlanAndStats QueryTestBase::planVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const axiom::runner::MultiFragmentPlan::Options& options,
    std::string* planString) {
  auto queryCtx = getQueryCtx();

  // The default Locus for planning is the system and data of 'connector_'.
  optimizer::Locus locus(connector_->connectorId().c_str(), connector_.get());
  auto allocator = std::make_unique<HashStringAllocator>(optimizerPool_.get());
  auto context =
      std::make_unique<velox::optimizer::QueryGraphContext>(*allocator);
  velox::optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    velox::optimizer::queryCtx() = nullptr;
  };
  exec::SimpleExpressionEvaluator evaluator(
      queryCtx_.get(), optimizerPool_.get());

  velox::optimizer::Schema veraxSchema("test", schema_.get(), &locus);
  velox::optimizer::Optimization opt(
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
    const logical_plan::LogicalPlanNodePtr& plan) {
  TestResult result;
  auto veloxPlan = planVelox(plan, &result.planString);
  return runFragmentedPlan(veloxPlan);
}

TestResult QueryTestBase::runVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const axiom::runner::MultiFragmentPlan::Options& options) {
  TestResult result;
  auto veloxPlan = planVelox(plan, options, &result.planString);
  return runFragmentedPlan(veloxPlan);
}

std::string QueryTestBase::veloxString(
    const axiom::runner::MultiFragmentPlanPtr& plan) {
  std::unordered_map<core::PlanNodeId, const core::TableScanNode*> scans;
  for (const auto& fragment : plan->fragments()) {
    for (const auto& scan : fragment.scans) {
      scans.emplace(scan->id(), scan.get());
    }
  }

  auto planNodeDetails = [&](const core::PlanNodeId& planNodeId,
                             const std::string& indentation,
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
    const optimizer::PlanAndStats& experiment) {
  auto referenceResult = runVelox(reference);
  auto experimentResult = runFragmentedPlan(experiment);

  exec::test::assertEqualResults(
      referenceResult.results, experimentResult.results);

  return referenceResult;
}

} // namespace facebook::velox::optimizer::test
