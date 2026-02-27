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
#include "axiom/common/Session.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/runner/LocalRunner.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "axiom/sql/presto/SqlStatement.h"
#include "velox/common/base/tests/GTestUtils.h"
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
}

void SqlTestBase::SetUp() {
  OperatorTestBase::SetUp();

  velox::exec::ExchangeSource::factories().clear();
  velox::exec::ExchangeSource::registerFactory(
      velox::exec::test::createLocalExchangeSource);

  connector_ = std::make_shared<connector::TestConnector>(kTestConnectorId);
  velox::connector::registerConnector(connector_);

  optimizerPool_ = rootPool_->addLeafChild("optimizer");
  executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(4);
}

void SqlTestBase::TearDown() {
  optimizerPool_.reset();
  velox::connector::unregisterConnector(kTestConnectorId);
  connector_.reset();
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

  duckDbQueryRunner_.createTable(name, data);
}

std::shared_ptr<runner::LocalRunner> SqlTestBase::makeRunner(
    std::string_view sql) {
  // Parse SQL to logical plan.
  ::axiom::sql::presto::PrestoParser parser(kTestConnectorId, std::nullopt);
  auto statement = parser.parse(sql, true);

  VELOX_CHECK(
      statement->isSelect(), "Only SELECT statements are supported: {}", sql);

  auto logicalPlan =
      statement->as<::axiom::sql::presto::SelectStatement>()->plan();

  // Create query context.
  static std::atomic<int32_t> queryCounter{0};
  auto queryId = fmt::format("sql_test_{}", ++queryCounter);
  auto queryCtx = core::QueryCtx::create(
      executor_.get(),
      core::QueryConfig{{}},
      {},
      cache::AsyncDataCache::getInstance(),
      rootPool_->addAggregateChild(queryId));

  // Optimize: logical plan -> Velox multi-fragment plan.
  auto allocator = std::make_unique<HashStringAllocator>(optimizerPool_.get());
  auto graphContext =
      std::make_unique<optimizer::QueryGraphContext>(*allocator);
  optimizer::queryCtx() = graphContext.get();
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };

  exec::SimpleExpressionEvaluator evaluator(
      queryCtx.get(), optimizerPool_.get());

  auto session = std::make_shared<Session>(queryId);
  connector::SchemaResolver schemaResolver;
  VeloxHistory history;

  runner::MultiFragmentPlan::Options options;
  options.numWorkers = numWorkers_;
  options.numDrivers = numDrivers_;
  options.queryId = queryId;

  Optimization optimization(
      session,
      *logicalPlan,
      schemaResolver,
      history,
      queryCtx,
      evaluator,
      OptimizerOptions(),
      options);

  auto best = optimization.bestPlan();
  auto planAndStats = optimization.toVeloxPlan(best->op);

  return std::make_shared<runner::LocalRunner>(
      planAndStats.plan,
      std::move(planAndStats.finishWrite),
      queryCtx,
      std::make_shared<runner::ConnectorSplitSourceFactory>(),
      optimizerPool_);
}

namespace {
constexpr uint64_t kMaxWaitMicros{50'000};
} // namespace

std::vector<RowVectorPtr> SqlTestBase::runAndCollect(std::string_view sql) {
  auto runner = makeRunner(sql);

  std::vector<RowVectorPtr> results;
  for (auto batch = runner->next(); batch != nullptr; batch = runner->next()) {
    results.push_back(std::move(batch));
  }

  runner->waitForCompletion(kMaxWaitMicros);

  return results;
}

void SqlTestBase::assertResults(
    std::string_view sql,
    std::string_view duckDbSql) {
  SCOPED_TRACE(sql);

  auto axiomResults = runAndCollect(sql);

  VELOX_CHECK(!axiomResults.empty(), "Axiom returned no results for: {}", sql);

  auto referenceSql = duckDbSql.empty() ? sql : duckDbSql;
  auto resultType = axiomResults[0]->rowType();

  velox::exec::test::assertResults(
      axiomResults, resultType, std::string(referenceSql), duckDbQueryRunner_);
}

void SqlTestBase::assertOrderedResults(
    std::string_view sql,
    std::string_view duckDbSql) {
  SCOPED_TRACE(sql);

  auto axiomResults = runAndCollect(sql);

  VELOX_CHECK(!axiomResults.empty(), "Axiom returned no results for: {}", sql);

  auto referenceSql = duckDbSql.empty() ? sql : duckDbSql;
  auto resultType = axiomResults[0]->rowType();

  auto expectedRows =
      duckDbQueryRunner_.executeOrdered(std::string(referenceSql), resultType);

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
}

uint64_t SqlTestBase::run(std::string_view sql) {
  auto runner = makeRunner(sql);
  SCOPE_EXIT {
    runner->waitForCompletion(kMaxWaitMicros);
  };

  uint64_t numRows = 0;
  for (auto batch = runner->next(); batch != nullptr; batch = runner->next()) {
    numRows += batch->size();
  }

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
