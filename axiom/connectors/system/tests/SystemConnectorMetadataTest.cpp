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

#include <folly/coro/BlockingWait.h>
#include <folly/json/json.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/system/SystemConnector.h"
#include "axiom/connectors/system/SystemConnectorMetadata.h"
#include "velox/connectors/Connector.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/expression/Expr.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::axiom::connector::system {
namespace {

// Test functions for the system.metadata.functions table.

// toss() -> boolean. Non-deterministic coin toss.
template <typename T>
struct TossFunction {
  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void call(bool& result) {
    result = folly::Random::rand32(2) == 0;
  }
};

// roll() -> integer and roll(integer) -> integer. Non-deterministic dice roll.
template <typename T>
struct RollFunction {
  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void call(int32_t& result, int32_t faces = 6) {
    result = folly::Random::rand32(faces);
  }
};

// plus(integer, integer, ...) -> integer and plus(real, real, ...) -> real.
// Variadic addition.
template <typename TExec>
struct PlusFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  FOLLY_ALWAYS_INLINE void call(
      T& result,
      const T& first,
      const T& second,
      const arg_type<velox::Variadic<T>>& remaining) {
    result = first + second;
    for (const auto& value : remaining) {
      if (value.has_value()) {
        result += value.value();
      }
    }
  }
};

// Registers the test functions once per process.
void registerTestFunctions() {
  static bool kRegistered = false;
  if (kRegistered) {
    return;
  }
  kRegistered = true;

  velox::registerFunction<TossFunction, bool>({"toss"});
  velox::registerFunction<RollFunction, int32_t>({"roll"});
  velox::registerFunction<RollFunction, int32_t, int32_t>({"roll"});
  velox::registerFunction<
      PlusFunction,
      int32_t,
      int32_t,
      int32_t,
      velox::Variadic<int32_t>>({"plus"});
  velox::registerFunction<
      PlusFunction,
      float,
      float,
      float,
      velox::Variadic<float>>({"plus"});
}

const char* const kSystemCatalog = "test-system";

/// In-memory mock that stores QueryInfos directly.
class MockQueryInfoProvider : public QueryInfoProvider {
 public:
  void addQuery(QueryInfo snapshot) {
    snapshots_.push_back(std::move(snapshot));
  }

  std::vector<QueryInfo> getQueryInfos() const override {
    return snapshots_;
  }

 private:
  std::vector<QueryInfo> snapshots_;
};

/// In-memory mock that returns a fixed set of session properties.
class MockSessionPropertiesProvider : public SessionPropertiesProvider {
 public:
  void addProperty(SessionPropertyInfo property) {
    properties_.push_back(std::move(property));
  }

  std::vector<SessionPropertyInfo> getSessionProperties() const override {
    return properties_;
  }

 private:
  std::vector<SessionPropertyInfo> properties_;
};

class SystemConnectorMetadataTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registerTestFunctions();
    velox::memory::MemoryManager::testingSetInstance({});

    queryProvider_ = std::make_unique<MockQueryInfoProvider>();
    sessionProvider_ = std::make_unique<MockSessionPropertiesProvider>();

    connector_ = std::make_shared<SystemConnector>(
        kSystemCatalog, queryProvider_.get(), sessionProvider_.get());
    velox::connector::registerConnector(connector_);

    metadata_ = std::make_shared<SystemConnectorMetadata>(connector_.get());
    ConnectorMetadataRegistry::global().insert(kSystemCatalog, metadata_);

    pool_ = velox::memory::memoryManager()->addLeafPool();
    queryCtx_ = velox::core::QueryCtx::create();
  }

  void TearDown() override {
    metadata_.reset();
    ConnectorMetadataRegistry::global().erase(kSystemCatalog);
    velox::connector::unregisterConnector(kSystemCatalog);
    connector_.reset();
  }

  // Creates a DataSource for the given schema/table through the connector,
  // adds a split, and returns the first batch of results.
  velox::RowVectorPtr readTable(
      const SchemaTableName& tableName,
      const velox::RowTypePtr& outputType) {
    auto table = metadata_->findTable(tableName);
    VELOX_CHECK_NOT_NULL(table);

    auto session = std::make_shared<ConnectorSession>("test-query");
    velox::exec::SimpleExpressionEvaluator evaluator(
        queryCtx_.get(), pool_.get());

    velox::connector::ColumnHandleMap columnHandles;
    std::vector<velox::connector::ColumnHandlePtr> handleList;
    for (const auto& name : outputType->names()) {
      auto handle = table->layouts()[0]->createColumnHandle(session, name);
      columnHandles[name] = handle;
      handleList.push_back(handle);
    }

    std::vector<velox::core::TypedExprPtr> filters;
    std::vector<velox::core::TypedExprPtr> rejectedFilters;
    auto tableHandle = table->layouts()[0]->createTableHandle(
        session,
        std::move(handleList),
        evaluator,
        std::move(filters),
        rejectedFilters);

    auto emptyConfig = std::make_shared<velox::config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});
    auto connectorQueryCtx =
        std::make_unique<velox::connector::ConnectorQueryCtx>(
            pool_.get(),
            pool_.get(),
            emptyConfig.get(),
            /*spillConfig=*/nullptr,
            velox::common::PrefixSortConfig{},
            /*expressionEvaluator=*/nullptr,
            /*cache=*/nullptr,
            "test-query",
            "test-task",
            "test-plan-node",
            /*driverId=*/0,
            /*sessionTimezone=*/"UTC");

    auto dataSource = connector_->createDataSource(
        outputType, tableHandle, columnHandles, connectorQueryCtx.get());

    auto split = std::make_shared<SystemSplit>(kSystemCatalog);
    dataSource->addSplit(split);

    velox::ContinueFuture future;
    auto result = dataSource->next(1024, future);
    VELOX_CHECK(result.has_value());

    // Verify the data source is exhausted.
    auto next = dataSource->next(1024, future);
    VELOX_CHECK(next.has_value());
    VELOX_CHECK_NULL(next.value());

    return result.value();
  }

  std::unique_ptr<MockQueryInfoProvider> queryProvider_;
  std::unique_ptr<MockSessionPropertiesProvider> sessionProvider_;
  std::shared_ptr<SystemConnector> connector_;
  std::shared_ptr<SystemConnectorMetadata> metadata_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<velox::core::QueryCtx> queryCtx_;
};

// ===================== system.runtime.queries tests =====================

TEST_F(SystemConnectorMetadataTest, findTable) {
  auto table = metadata_->findTable(kQueriesTable);
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->name(), kQueriesTable);
}

TEST_F(SystemConnectorMetadataTest, findTableCaching) {
  auto table1 = metadata_->findTable(kQueriesTable);
  auto table2 = metadata_->findTable(kQueriesTable);
  EXPECT_EQ(table1.get(), table2.get());
}

TEST_F(SystemConnectorMetadataTest, findTableUnknown) {
  EXPECT_EQ(
      metadata_->findTable({kQueriesTable.schema, "nonexistent"}), nullptr);
  EXPECT_EQ(
      metadata_->findTable(
          {kSessionPropertiesTable.schema, kQueriesTable.table}),
      nullptr);
  EXPECT_EQ(metadata_->findTable({"", ""}), nullptr);
}

TEST_F(SystemConnectorMetadataTest, schema) {
  auto table = metadata_->findTable(kQueriesTable);
  ASSERT_NE(table, nullptr);

  auto expectedSchema = queriesTableSchema();
  ASSERT_EQ(table->type()->size(), expectedSchema->size());
  for (size_t i = 0; i < expectedSchema->size(); ++i) {
    EXPECT_EQ(table->type()->nameOf(i), expectedSchema->nameOf(i));
    EXPECT_EQ(
        table->type()->childAt(i)->kind(), expectedSchema->childAt(i)->kind());
  }
}

TEST_F(SystemConnectorMetadataTest, splitManager) {
  EXPECT_NE(metadata_->splitManager(), nullptr);
}

TEST_F(SystemConnectorMetadataTest, tableLayout) {
  auto table = metadata_->findTable(kQueriesTable);
  ASSERT_NE(table, nullptr);

  const auto& layouts = table->layouts();
  ASSERT_EQ(layouts.size(), 1);
  EXPECT_EQ(layouts[0]->label(), "default");
  EXPECT_TRUE(layouts[0]->supportsScan());
  EXPECT_EQ(layouts[0]->connectorId(), kSystemCatalog);

  // Verify column handle creation.
  auto session = std::make_shared<ConnectorSession>("test-query");
  auto columnHandle = layouts[0]->createColumnHandle(session, "query_id");
  ASSERT_NE(columnHandle, nullptr);
  EXPECT_EQ(columnHandle->name(), "query_id");
}

TEST_F(SystemConnectorMetadataTest, tableHandle) {
  auto table = metadata_->findTable(kQueriesTable);
  ASSERT_NE(table, nullptr);

  const auto& layouts = table->layouts();
  ASSERT_EQ(layouts.size(), 1);

  auto session = std::make_shared<ConnectorSession>("test-query");
  auto columnHandle = layouts[0]->createColumnHandle(session, "query_id");

  velox::exec::SimpleExpressionEvaluator evaluator(
      queryCtx_.get(), pool_.get());

  std::vector<velox::core::TypedExprPtr> filters;
  std::vector<velox::core::TypedExprPtr> rejectedFilters;
  auto tableHandle = layouts[0]->createTableHandle(
      session, {columnHandle}, evaluator, std::move(filters), rejectedFilters);
  ASSERT_NE(tableHandle, nullptr);
  EXPECT_EQ(tableHandle->connectorId(), kSystemCatalog);
  EXPECT_EQ(tableHandle->name(), "runtime.queries");
}

TEST_F(SystemConnectorMetadataTest, splitSource) {
  auto table = metadata_->findTable(kQueriesTable);
  ASSERT_NE(table, nullptr);

  auto session = std::make_shared<ConnectorSession>("test-query");

  velox::exec::SimpleExpressionEvaluator evaluator(
      queryCtx_.get(), pool_.get());

  std::vector<velox::core::TypedExprPtr> filters;
  std::vector<velox::core::TypedExprPtr> rejectedFilters;
  auto tableHandle = table->layouts()[0]->createTableHandle(
      session, {}, evaluator, std::move(filters), rejectedFilters);

  auto* splitManager = metadata_->splitManager();
  auto partitions = folly::coro::blockingWait(
      splitManager->co_listPartitions(session, tableHandle));
  EXPECT_EQ(partitions.size(), 1);

  auto splitSource =
      splitManager->getSplitSource(session, tableHandle, partitions);
  ASSERT_NE(splitSource, nullptr);

  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits;
  while (true) {
    auto batch = folly::coro::blockingWait(splitSource->co_getSplits(1000));
    for (auto& split : batch.splits) {
      splits.push_back(std::move(split));
    }
    if (batch.noMoreSplits) {
      break;
    }
  }
  ASSERT_EQ(splits.size(), 1);
  ASSERT_NE(splits[0], nullptr);
}

TEST_F(SystemConnectorMetadataTest, dataSourceAllColumns) {
  QueryInfo snapshot;
  snapshot.queryId = "q1";
  snapshot.state = "RUNNING";
  snapshot.query = "SELECT * FROM t";
  snapshot.catalog = "cat";
  snapshot.schema = "sch";
  snapshot.user = "testuser";
  snapshot.source = "testsource";
  snapshot.queryType = "SELECT";
  snapshot.planningTimeMs = 100;
  snapshot.totalSplits = 10;
  snapshot.outputRows = 1000;
  snapshot.createTime = std::chrono::system_clock::now();

  auto expectedSeconds = std::chrono::duration_cast<std::chrono::seconds>(
                             snapshot.createTime.time_since_epoch())
                             .count();
  auto expectedNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           snapshot.createTime.time_since_epoch())
                           .count() %
      1'000'000'000;

  queryProvider_->addQuery(std::move(snapshot));

  auto vector = readTable(kQueriesTable, queriesTableSchema());
  ASSERT_NE(vector, nullptr);
  ASSERT_EQ(vector->size(), 1);

  auto values = vector->variantAt(0).row();

  // VARCHAR columns.
  EXPECT_EQ(values[0].value<std::string>(), "q1");
  EXPECT_EQ(values[1].value<std::string>(), "RUNNING");
  EXPECT_EQ(values[2].value<std::string>(), "SELECT * FROM t");
  EXPECT_EQ(values[3].value<std::string>(), "cat");
  EXPECT_EQ(values[5].value<std::string>(), "testuser");
  EXPECT_EQ(values[6].value<std::string>(), "testsource");
  EXPECT_EQ(values[7].value<std::string>(), "SELECT");

  // BIGINT columns.
  EXPECT_EQ(values[8].value<int64_t>(), 100); // planning_time_ms
  EXPECT_EQ(values[15].value<int64_t>(), 10); // total_splits
  EXPECT_EQ(values[19].value<int64_t>(), 1000); // output_rows

  // TIMESTAMP columns.
  auto ts = values[27].value<velox::Timestamp>();
  EXPECT_EQ(ts.getSeconds(), expectedSeconds);
  EXPECT_EQ(ts.getNanos(), expectedNanos);
  EXPECT_TRUE(values[29].isNull()); // end_time (epoch = not set)
}

TEST_F(SystemConnectorMetadataTest, dataSourceColumnPruning) {
  QueryInfo snapshot;
  snapshot.queryId = "q2";
  snapshot.state = "QUEUED";
  snapshot.query = "SELECT 1";
  snapshot.catalog = "c";
  snapshot.schema = "s";
  snapshot.user = "u";

  queryProvider_->addQuery(std::move(snapshot));

  auto outputType =
      velox::ROW({{"query_id", velox::VARCHAR()}, {"state", velox::VARCHAR()}});
  auto vector = readTable(kQueriesTable, outputType);
  ASSERT_NE(vector, nullptr);
  ASSERT_EQ(vector->size(), 1);
  ASSERT_EQ(vector->type()->size(), 2);

  auto* qid = vector->childAt(0)->asFlatVector<velox::StringView>();
  EXPECT_EQ(qid->valueAt(0).str(), "q2");

  auto* state = vector->childAt(1)->asFlatVector<velox::StringView>();
  EXPECT_EQ(state->valueAt(0).str(), "QUEUED");
}

TEST_F(SystemConnectorMetadataTest, dataSourceEmpty) {
  auto vector = readTable(kQueriesTable, queriesTableSchema());
  ASSERT_NE(vector, nullptr);
  EXPECT_EQ(vector->size(), 0);
}

TEST_F(SystemConnectorMetadataTest, dataSourceNullSource) {
  QueryInfo snapshot;
  snapshot.queryId = "q3";
  snapshot.state = "FINISHED";
  snapshot.query = "SELECT 1";
  snapshot.catalog = "c";
  snapshot.schema = "s";
  snapshot.user = "u";
  // source is not set (std::nullopt by default).

  queryProvider_->addQuery(std::move(snapshot));

  auto outputType = velox::ROW({{"source", velox::VARCHAR()}});
  auto vector = readTable(kQueriesTable, outputType);
  ASSERT_NE(vector, nullptr);
  ASSERT_EQ(vector->size(), 1);

  // source should be null since snapshot.source is std::nullopt.
  EXPECT_TRUE(vector->childAt(0)->isNullAt(0));
}

TEST_F(SystemConnectorMetadataTest, findSessionPropertiesTable) {
  auto table = metadata_->findTable(kSessionPropertiesTable);
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->name(), kSessionPropertiesTable);

  // Wrong schema.
  EXPECT_EQ(
      metadata_->findTable(
          {kQueriesTable.schema, kSessionPropertiesTable.table}),
      nullptr);
}

TEST_F(SystemConnectorMetadataTest, sessionPropertiesSchema) {
  auto table = metadata_->findTable(kSessionPropertiesTable);
  ASSERT_NE(table, nullptr);

  auto expectedSchema = sessionPropertiesTableSchema();
  ASSERT_EQ(table->type()->size(), expectedSchema->size());
  for (size_t i = 0; i < expectedSchema->size(); ++i) {
    EXPECT_EQ(table->type()->nameOf(i), expectedSchema->nameOf(i));
    EXPECT_EQ(
        table->type()->childAt(i)->kind(), expectedSchema->childAt(i)->kind());
  }
}

TEST_F(SystemConnectorMetadataTest, schemas) {
  auto session = std::make_shared<ConnectorSession>("test-query");
  EXPECT_THAT(
      metadata_->listSchemaNames(session),
      testing::UnorderedElementsAre(
          kQueriesTable.schema, kSessionPropertiesTable.schema));
  EXPECT_TRUE(metadata_->schemaExists(session, kQueriesTable.schema));
  EXPECT_TRUE(metadata_->schemaExists(session, kSessionPropertiesTable.schema));
  EXPECT_FALSE(metadata_->schemaExists(session, "information_schema"));
}

TEST_F(SystemConnectorMetadataTest, sessionPropertiesAllColumns) {
  sessionProvider_->addProperty(
      {"a", "x", "boolean", "true", "false", "First."});
  sessionProvider_->addProperty({"b", "y", "string", "", "hello", "Second."});

  auto expected = velox::BaseVector::createFromVariants(
      sessionPropertiesTableSchema(),
      {
          velox::Variant::row({"a", "x", "boolean", "true", "false", "First."}),
          velox::Variant::row({"b", "y", "string", "", "hello", "Second."}),
      },
      pool_.get());

  auto actual =
      readTable(kSessionPropertiesTable, sessionPropertiesTableSchema());
  velox::test::assertEqualVectors(expected, actual);
}

// ===================== system.metadata.functions tests =====================

TEST_F(SystemConnectorMetadataTest, findFunctionsTable) {
  auto table = metadata_->findTable(kFunctionsTable);
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->name(), kFunctionsTable);

  // Wrong schema.
  EXPECT_EQ(
      metadata_->findTable({kQueriesTable.schema, kFunctionsTable.table}),
      nullptr);
}

TEST_F(SystemConnectorMetadataTest, functionsDataSource) {
  auto actual = readTable(kFunctionsTable, functionsTableSchema());
  ASSERT_NE(actual, nullptr);

  folly::json::serialization_opts opts;
  opts.sort_keys = true;
  auto kDeterministic = folly::json::serialize(
      folly::dynamic::object("deterministic", true)(
          "default_null_behavior", true),
      opts);
  auto kNonDeterministic = folly::json::serialize(
      folly::dynamic::object("deterministic", false)(
          "default_null_behavior", true),
      opts);

  auto expected = std::dynamic_pointer_cast<velox::RowVector>(
      velox::BaseVector::createFromVariants(
          functionsTableSchema(),
          {
              // plus(integer, integer, integer...) -> integer.
              velox::Variant::row({
                  "plus",
                  "scalar",
                  "integer",
                  velox::Variant::array({"integer", "integer", "integer"}),
                  true,
                  "",
                  kDeterministic,
              }),
              // plus(real, real, real...) -> real.
              velox::Variant::row({
                  "plus",
                  "scalar",
                  "real",
                  velox::Variant::array({"real", "real", "real"}),
                  true,
                  "",
                  kDeterministic,
              }),
              // roll() -> integer.
              velox::Variant::row({
                  "roll",
                  "scalar",
                  "integer",
                  velox::Variant::array({}),
                  false,
                  "",
                  kNonDeterministic,
              }),
              // roll(integer) -> integer.
              velox::Variant::row({
                  "roll",
                  "scalar",
                  "integer",
                  velox::Variant::array({"integer"}),
                  false,
                  "",
                  kNonDeterministic,
              }),
              // toss() -> boolean.
              velox::Variant::row({
                  "toss",
                  "scalar",
                  "boolean",
                  velox::Variant::array({}),
                  false,
                  "",
                  kNonDeterministic,
              }),
          },
          pool_.get()));

  velox::exec::test::assertEqualResults({expected}, {actual});
}

} // namespace
} // namespace facebook::axiom::connector::system
