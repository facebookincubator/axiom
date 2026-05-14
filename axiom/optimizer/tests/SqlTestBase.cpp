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
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/VeloxHistory.h"
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

namespace {
constexpr uint64_t kMaxWaitMicros{50'000};
} // namespace

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

  if (createsConnectorPerTest()) {
    connector_ = std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::ConnectorRegistry::global().insert(
        kTestConnectorId, connector_);
    connector::ConnectorMetadataRegistry::global().insert(
        kTestConnectorId, connector_->metadata());
  }

  optimizerPool_ = rootPool_->addLeafChild("optimizer");
  executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(4);
}

void SqlTestBase::TearDown() {
  optimizerPool_.reset();
  if (createsConnectorPerTest()) {
    connector::ConnectorMetadataRegistry::global().erase(kTestConnectorId);
    velox::connector::ConnectorRegistry::global().erase(kTestConnectorId);
    connector_.reset();
  }
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

std::shared_ptr<runner::LocalRunner> SqlTestBase::makeLocalRunner(
    const logical_plan::LogicalPlanNodePtr& logicalPlan,
    folly::Executor* executor,
    cache::AsyncDataCache* asyncDataCache,
    const std::shared_ptr<memory::MemoryPool>& rootPool,
    const std::shared_ptr<memory::MemoryPool>& optimizerPool,
    int32_t numWorkers,
    int32_t numDrivers) {
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

  auto session = std::make_shared<Session>(queryId);
  connector::SchemaResolver schemaResolver;
  VeloxHistory history;

  MultiFragmentPlan::Options options;
  options.numWorkers = numWorkers;
  options.numDrivers = numDrivers;
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
      optimizerPool);
}

void SqlTestBase::runSetupStatement(
    const std::string& sql,
    connector::TestConnector& connector,
    velox::exec::test::DuckDbQueryRunner& duckDb,
    const std::function<std::shared_ptr<runner::LocalRunner>(
        const logical_plan::LogicalPlanNodePtr&)>& runnerFactory) {
  duckDb.execute(sql);

  ::axiom::sql::presto::PrestoParser parser(
      kTestConnectorId, std::string(connector::TestConnector::kDefaultSchema));
  auto stmt = parser.parse(sql, /*enableTracing=*/true);

  if (stmt->isCreateTable()) {
    const auto& create =
        *stmt->as<::axiom::sql::presto::CreateTableStatement>();
    connector.addTable(create.tableName().table, create.tableSchema());
    return;
  }

  logical_plan::LogicalPlanNodePtr plan;
  if (stmt->isInsert()) {
    plan = stmt->as<::axiom::sql::presto::InsertStatement>()->plan();
  } else if (stmt->isCreateTableAsSelect()) {
    const auto& ctas =
        *stmt->as<::axiom::sql::presto::CreateTableAsSelectStatement>();
    connector.addTable(ctas.tableName().table, ctas.tableSchema());
    plan = ctas.plan();
  } else {
    VELOX_USER_FAIL(
        "Unsupported setup statement (only CREATE TABLE, INSERT INTO, "
        "and CREATE TABLE AS SELECT are supported): {}",
        sql);
  }

  auto runner = runnerFactory(plan);
  while (auto batch = runner->next()) {
  }
  runner->waitForCompletion(kMaxWaitMicros);
}

void SqlTestBase::runSetupStatement(const std::string& sql) {
  runSetupStatement(
      sql,
      *connector_,
      duckDbRunner(),
      [this](const logical_plan::LogicalPlanNodePtr& plan) {
        return makeRunner(plan);
      });
}

std::shared_ptr<runner::LocalRunner> SqlTestBase::makeRunner(
    std::string_view sql) {
  // Parse SQL to logical plan.
  ::axiom::sql::presto::PrestoParser parser(
      kTestConnectorId, std::string(connector::TestConnector::kDefaultSchema));
  auto statement = parser.parse(sql, true);

  VELOX_CHECK(
      statement->isSelect(), "Only SELECT statements are supported: {}", sql);

  return makeRunner(
      statement->as<::axiom::sql::presto::SelectStatement>()->plan());
}

std::shared_ptr<runner::LocalRunner> SqlTestBase::makeRunner(
    const logical_plan::LogicalPlanNodePtr& logicalPlan) {
  return makeLocalRunner(
      logicalPlan,
      executor_.get(),
      cache::AsyncDataCache::getInstance(),
      rootPool_,
      optimizerPool_,
      numWorkers_,
      numDrivers_);
}

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
