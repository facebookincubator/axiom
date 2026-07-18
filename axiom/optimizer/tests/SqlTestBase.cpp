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
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/optimizer/v2/Optimize.h"
#include "axiom/runner/LocalRunner.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "axiom/sql/presto/SqlStatement.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

namespace facebook::axiom::optimizer::test {

using namespace facebook::velox;

void SqlTestBase::SetUpTestCase() {
  OperatorTestBase::SetUpTestCase();

  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  window::prestosql::registerAllWindowFunctions();
  FunctionRegistry::registerPrestoFunctions();
}

void SqlTestBase::SetUp() {
  OperatorTestBase::SetUp();

  velox::exec::ExchangeSource::factories().clear();
  velox::exec::ExchangeSource::registerFactory(
      velox::exec::test::createLocalExchangeSource);

  optimizerPool_ = rootPool_->addLeafChild("optimizer");
  executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(4);
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
    int32_t numWorkers,
    int32_t numDrivers,
    bool syntacticJoinOrder,
    PlanFn plan) {
  static std::atomic<int32_t> queryCounter{0};
  auto queryId = fmt::format("sql_test_{}", ++queryCounter);
  auto queryCtx = core::QueryCtx::create(
      executor,
      core::QueryConfig{{}},
      {},
      asyncDataCache,
      rootPool->addAggregateChild(queryId));

  auto allocator = std::make_unique<HashStringAllocator>(optimizerPool.get());
  auto graphContext =
      std::make_unique<optimizer::QueryGraphContext>(*allocator);
  optimizer::queryCtx() = graphContext.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };

  exec::SimpleExpressionEvaluator evaluator(
      queryCtx.get(), optimizerPool.get());

  connector::SchemaResolver schemaResolver{
      connector::ConnectorMetadataRegistry::global()};

  MultiFragmentPlan::Options options;
  options.numWorkers = numWorkers;
  options.numDrivers = numDrivers;
  options.queryId = queryId;

  OptimizerOptions optimizerOptions;
  optimizerOptions.syntacticJoinOrder = syntacticJoinOrder;
  auto optimizerSession = std::make_shared<OptimizerSession>(
      queryId, "test", optimizerOptions, connector::ConnectorProperties{});
  auto runnerSession = std::make_shared<runner::RunnerSession>(
      queryId, "test", runner::Properties{}, connector::ConnectorProperties{});

  auto planAndStats = plan(
      *logicalPlan,
      schemaResolver,
      evaluator,
      options,
      optimizerSession,
      runnerSession,
      queryCtx);

  static QueryRuntimeStats noopStats;
  return std::make_shared<runner::LocalRunner>(
      std::move(runnerSession),
      planAndStats.plan,
      std::move(planAndStats.finishWrite),
      queryCtx,
      std::make_shared<runner::ConnectorSplitSourceFactory>(noopStats),
      optimizerPool,
      /*baseSpillDirectory=*/"",
      noopStats);
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
    bool syntacticJoinOrder) {
  return makeLocalRunnerImpl(
      logicalPlan,
      executor,
      asyncDataCache,
      rootPool,
      optimizerPool,
      numWorkers,
      numDrivers,
      syntacticJoinOrder,
      [](const logical_plan::LogicalPlanNode& logicalPlan,
         const connector::SchemaResolver& schemaResolver,
         velox::core::ExpressionEvaluator& evaluator,
         const MultiFragmentPlan::Options& options,
         const std::shared_ptr<OptimizerSession>& optimizerSession,
         const std::shared_ptr<runner::RunnerSession>& runnerSession,
         const std::shared_ptr<core::QueryCtx>& queryCtx) {
        VeloxHistory history;
        Optimization optimization(
            optimizerSession,
            runnerSession,
            logicalPlan,
            schemaResolver,
            history,
            queryCtx,
            evaluator,
            options);
        auto best = optimization.bestPlan();
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
    bool syntacticJoinOrder) {
  return makeLocalRunnerImpl(
      logicalPlan,
      executor,
      asyncDataCache,
      rootPool,
      optimizerPool,
      numWorkers,
      numDrivers,
      syntacticJoinOrder,
      [](const logical_plan::LogicalPlanNode& logicalPlan,
         const connector::SchemaResolver& schemaResolver,
         velox::core::ExpressionEvaluator& evaluator,
         const MultiFragmentPlan::Options& options,
         const std::shared_ptr<OptimizerSession>& optimizerSession,
         const std::shared_ptr<runner::RunnerSession>& /*runnerSession*/,
         const std::shared_ptr<core::QueryCtx>& /*queryCtx*/) {
        return v2::optimize(
            logicalPlan, schemaResolver, *optimizerSession, evaluator, options);
      });
}

std::shared_ptr<runner::LocalRunner> SqlTestBase::makeRunner(
    std::string_view sql) {
  // Parse SQL to logical plan.
  ::axiom::sql::presto::PrestoParser parser(
      connectorId_,
      defaultSchema_,
      std::make_shared<::axiom::sql::presto::ParserSession>(
          /*queryId=*/"test",
          /*user=*/"test",
          ::axiom::sql::presto::ParserOptions{},
          connector::ConnectorProperties{}));
  auto statement = parser.parse(sql, true);

  VELOX_CHECK(
      statement->isSelect(), "Only SELECT statements are supported: {}", sql);

  return makeRunner(
      statement->as<::axiom::sql::presto::SelectStatement>()->plan());
}

std::shared_ptr<runner::LocalRunner> SqlTestBase::makeRunner(
    const logical_plan::LogicalPlanNodePtr& logicalPlan) {
  const auto buildRunner = useV2_ ? makeLocalRunnerV2 : makeLocalRunner;
  return buildRunner(
      logicalPlan,
      executor_.get(),
      cache::AsyncDataCache::getInstance(),
      rootPool_,
      optimizerPool_,
      numWorkers_,
      numDrivers_,
      syntacticJoinOrder_);
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

  velox::exec::test::assertResults(
      axiomResults, resultType, referenceSql, duckDbRunner());

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

  auto expectedRows = duckDbRunner().executeOrdered(referenceSql, resultType);

  // Materialize Axiom results into an ordered list of rows.
  std::vector<velox::exec::test::MaterializedRow> actualRows;
  for (const auto& batch : axiomResults) {
    auto batchRows = velox::exec::test::materialize(batch);
    actualRows.insert(actualRows.end(), batchRows.begin(), batchRows.end());
  }

  ASSERT_EQ(expectedRows.size(), actualRows.size());

  for (size_t i = 0; i < expectedRows.size(); ++i) {
    ASSERT_EQ(expectedRows[i], actualRows[i]) << "Mismatch at row " << i;
  }

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

void SqlTestBase::assertContains(
    std::string_view sql,
    std::optional<std::string> duckDbSql) {
  SCOPED_TRACE(sql);

  auto axiomResults = runAndCollect(sql);

  VELOX_CHECK(!axiomResults.empty(), "Axiom returned no results for: {}", sql);

  auto referenceSql = duckDbSql.value_or(std::string(sql));
  auto resultType = axiomResults[0]->rowType();

  const auto expected = duckDbRunner().execute(referenceSql, resultType);
  const auto actual = velox::exec::test::materialize(axiomResults);
  for (const auto& row : expected) {
    EXPECT_GE(actual.count(row), expected.count(row))
        << "Result is missing a row expected by the reference query";
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
