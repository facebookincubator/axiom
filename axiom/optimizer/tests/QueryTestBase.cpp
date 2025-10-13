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
#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/expression/Expr.h"

DECLARE_string(data_path);

DEFINE_uint32(optimizer_trace, 0, "Optimizer trace level");

DEFINE_string(
    history_save_path,
    "",
    "Path to save sampling after the test suite");

using namespace facebook::velox;

namespace facebook::axiom::optimizer::test {

void QueryTestBase::SetUp() {
  runner::test::LocalRunnerTestBase::SetUp();

  optimizerPool_ = rootPool_->addLeafChild("optimizer");

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
  optimizerPool_.reset();
  LocalRunnerTestBase::TearDown();
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

TestResult QueryTestBase::runVelox(const core::PlanNodePtr& plan) {
  runner::MultiFragmentPlan::Options options;
  options.numWorkers = 1;
  options.numDrivers = 1;
  options.queryId = fmt::format("q{}", ++gQueryCounter);

  runner::ExecutableFragment fragment(fmt::format("{}.0", options.queryId));
  fragment.fragment = core::PlanFragment(plan);

  optimizer::PlanAndStats planAndStats = {
      std::make_shared<runner::MultiFragmentPlan>(
          std::vector<runner::ExecutableFragment>{std::move(fragment)},
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

  queryCtx_ = runner::test::LocalRunnerTestBase::makeQueryCtx(
      fmt::format("q{}", ++gQueryCounter));

  return queryCtx_;
}

optimizer::PlanAndStats QueryTestBase::planVelox(
    const logical_plan::LogicalPlanNodePtr& plan,
    const runner::MultiFragmentPlan::Options& options,
    std::string* planString) {
  auto& queryCtx = getQueryCtx();

  auto allocator = std::make_unique<HashStringAllocator>(optimizerPool_.get());
  auto context = std::make_unique<optimizer::QueryGraphContext>(*allocator);
  optimizer::queryCtx() = context.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };
  exec::SimpleExpressionEvaluator evaluator(
      queryCtx.get(), optimizerPool_.get());

  connector::SchemaResolver schemaResolver;
  optimizer::Schema veraxSchema("test", &schemaResolver, /*locus=*/nullptr);

  auto session = std::make_shared<Session>(queryCtx->queryId());

  optimizer::Optimization opt(
      session,
      *plan,
      veraxSchema,
      *history_,
      queryCtx,
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
  auto veloxPlan = planVelox(plan, options);
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
    const axiom::runner::MultiFragmentPlan::Options& options) {
  VELOX_CHECK_NOT_NULL(planNode);
  VELOX_CHECK_NOT_NULL(referencePlan);

  SCOPED_TRACE("reference plan:\n" + referencePlan->toString(true, true));
  auto referenceResult = runVelox(referencePlan);
  checkSame(planNode, referenceResult.results, options);
}

void QueryTestBase::checkSame(
    const logical_plan::LogicalPlanNodePtr& planNode,
    const std::vector<velox::RowVectorPtr>& referenceResult,
    const axiom::runner::MultiFragmentPlan::Options& options) {
  VELOX_CHECK_NOT_NULL(planNode);

  std::vector<axiom::runner::MultiFragmentPlan::Options> testOptions = {
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
    SCOPED_TRACE(fmt::format(
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

} // namespace facebook::axiom::optimizer::test
