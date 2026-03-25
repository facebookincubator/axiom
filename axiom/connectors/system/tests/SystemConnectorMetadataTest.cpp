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

#include <folly/coro/Task.h>
#include <gtest/gtest.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/system/SystemConnector.h"
#include "axiom/connectors/system/SystemConnectorMetadata.h"
#include "velox/connectors/Connector.h"
#include "velox/expression/Expr.h"

namespace facebook::axiom::connector::system {
namespace {

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

class SystemConnectorMetadataTest : public ::testing::Test {
 protected:
  void SetUp() override {
    velox::memory::MemoryManager::testingSetInstance({});

    mockProvider_ = std::make_unique<MockQueryInfoProvider>();

    // SystemConnector no longer auto-registers metadata.
    connector_ =
        std::make_shared<SystemConnector>(kSystemCatalog, mockProvider_.get());
    velox::connector::registerConnector(connector_);
    ConnectorMetadata::registerMetadata(kSystemCatalog, connector_->metadata());

    // Create a separate metadata instance for direct testing.
    metadata_ = std::make_shared<SystemConnectorMetadata>(connector_.get());

    pool_ = velox::memory::memoryManager()->addLeafPool();
    queryCtx_ = velox::core::QueryCtx::create();
  }

  void TearDown() override {
    metadata_.reset();
    ConnectorMetadata::unregisterMetadata(kSystemCatalog);
    velox::connector::unregisterConnector(kSystemCatalog);
    connector_.reset();
  }

  std::unique_ptr<MockQueryInfoProvider> mockProvider_;
  std::shared_ptr<SystemConnector> connector_;
  std::shared_ptr<SystemConnectorMetadata> metadata_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<velox::core::QueryCtx> queryCtx_;
};

TEST_F(SystemConnectorMetadataTest, findTable) {
  auto table = metadata_->findTable({"runtime", "queries"});
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->name(), (SchemaTableName{"runtime", "queries"}));
}

TEST_F(SystemConnectorMetadataTest, findTableCaching) {
  auto table1 = metadata_->findTable({"runtime", "queries"});
  auto table2 = metadata_->findTable({"runtime", "queries"});
  EXPECT_EQ(table1.get(), table2.get());
}

TEST_F(SystemConnectorMetadataTest, findTableUnknown) {
  EXPECT_EQ(metadata_->findTable({"runtime", "nonexistent"}), nullptr);
  EXPECT_EQ(metadata_->findTable({"runtime", "other"}), nullptr);
  EXPECT_EQ(metadata_->findTable({"", ""}), nullptr);
}

TEST_F(SystemConnectorMetadataTest, schema) {
  auto table = metadata_->findTable({"runtime", "queries"});
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
  auto table = metadata_->findTable({"runtime", "queries"});
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
  auto table = metadata_->findTable({"runtime", "queries"});
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
  EXPECT_EQ(tableHandle->name(), R"("runtime"."queries")");
}

TEST_F(SystemConnectorMetadataTest, splitSource) {
  auto table = metadata_->findTable({"runtime", "queries"});
  ASSERT_NE(table, nullptr);

  auto session = std::make_shared<ConnectorSession>("test-query");

  velox::exec::SimpleExpressionEvaluator evaluator(
      queryCtx_.get(), pool_.get());

  std::vector<velox::core::TypedExprPtr> filters;
  std::vector<velox::core::TypedExprPtr> rejectedFilters;
  auto tableHandle = table->layouts()[0]->createTableHandle(
      session, {}, evaluator, std::move(filters), rejectedFilters);

  auto* splitManager = metadata_->splitManager();
  auto partitions = splitManager->listPartitions(session, tableHandle);
  EXPECT_EQ(partitions.size(), 1);

  auto splitSource =
      splitManager->getSplitSource(session, tableHandle, partitions);
  ASSERT_NE(splitSource, nullptr);

  // First call should return one split.
  auto splits1 = splitSource->getSplits(1024);
  ASSERT_EQ(splits1.size(), 1);
  ASSERT_NE(splits1[0].split, nullptr);

  // Second call should signal done (nullptr split).
  auto splits2 = splitSource->getSplits(1024);
  ASSERT_EQ(splits2.size(), 1);
  EXPECT_EQ(splits2[0].split, nullptr);
}

TEST_F(SystemConnectorMetadataTest, dataSourceAllColumns) {
  // Set up test data.
  auto now = std::chrono::system_clock::now();

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
  snapshot.createTime = now;

  mockProvider_->addQuery(std::move(snapshot));

  auto fullSchema = queriesTableSchema();

  // Build column handles for all columns.
  velox::connector::ColumnHandleMap columnHandles;
  for (const auto& name : fullSchema->names()) {
    columnHandles[name] = std::make_shared<SystemColumnHandle>(name);
  }

  auto dataSource = std::make_unique<SystemDataSource>(
      fullSchema, columnHandles, mockProvider_.get(), pool_.get());

  auto split = std::make_shared<SystemSplit>(kSystemCatalog);
  dataSource->addSplit(split);

  velox::ContinueFuture future;
  auto result = dataSource->next(1024, future);
  ASSERT_TRUE(result.has_value());
  ASSERT_NE(result.value(), nullptr);
  auto vector = result.value();
  ASSERT_EQ(vector->size(), 1);

  // query_id (column 0)
  auto* queryIdVec = vector->childAt(0)->asFlatVector<velox::StringView>();
  EXPECT_EQ(queryIdVec->valueAt(0).str(), "q1");

  // state (column 1)
  auto* stateVec = vector->childAt(1)->asFlatVector<velox::StringView>();
  EXPECT_EQ(stateVec->valueAt(0).str(), "RUNNING");

  // query (column 2)
  auto* queryVec = vector->childAt(2)->asFlatVector<velox::StringView>();
  EXPECT_EQ(queryVec->valueAt(0).str(), "SELECT * FROM t");

  // catalog (column 3)
  auto* catalogVec = vector->childAt(3)->asFlatVector<velox::StringView>();
  EXPECT_EQ(catalogVec->valueAt(0).str(), "cat");

  // user (column 5)
  auto* userVec = vector->childAt(5)->asFlatVector<velox::StringView>();
  EXPECT_EQ(userVec->valueAt(0).str(), "testuser");

  // source (column 6) — should not be null
  EXPECT_FALSE(vector->childAt(6)->isNullAt(0));
  auto* sourceVec = vector->childAt(6)->asFlatVector<velox::StringView>();
  EXPECT_EQ(sourceVec->valueAt(0).str(), "testsource");

  // query_type (column 7)
  auto* qtVec = vector->childAt(7)->asFlatVector<velox::StringView>();
  EXPECT_EQ(qtVec->valueAt(0).str(), "SELECT");

  // planning_time_ms (column 8)
  auto* planVec = vector->childAt(8)->asFlatVector<int64_t>();
  EXPECT_EQ(planVec->valueAt(0), 100);

  // total_splits (column 15)
  auto* splitVec = vector->childAt(15)->asFlatVector<int64_t>();
  EXPECT_EQ(splitVec->valueAt(0), 10);

  // output_rows (column 19)
  auto* outVec = vector->childAt(19)->asFlatVector<int64_t>();
  EXPECT_EQ(outVec->valueAt(0), 1000);

  // create_time (column 27) — should not be null
  EXPECT_FALSE(vector->childAt(27)->isNullAt(0));

  // end_time (column 29) — should be null (epoch = not set)
  EXPECT_TRUE(vector->childAt(29)->isNullAt(0));

  // Second call returns no more data.
  auto result2 = dataSource->next(1024, future);
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(result2.value(), nullptr);
}

TEST_F(SystemConnectorMetadataTest, dataSourceColumnPruning) {
  QueryInfo snapshot;
  snapshot.queryId = "q2";
  snapshot.state = "QUEUED";
  snapshot.query = "SELECT 1";
  snapshot.catalog = "c";
  snapshot.schema = "s";
  snapshot.user = "u";

  mockProvider_->addQuery(std::move(snapshot));

  // Request only query_id and state.
  auto outputType =
      velox::ROW({{"query_id", velox::VARCHAR()}, {"state", velox::VARCHAR()}});
  velox::connector::ColumnHandleMap columnHandles;
  columnHandles["query_id"] = std::make_shared<SystemColumnHandle>("query_id");
  columnHandles["state"] = std::make_shared<SystemColumnHandle>("state");

  auto dataSource = std::make_unique<SystemDataSource>(
      outputType, columnHandles, mockProvider_.get(), pool_.get());

  auto split = std::make_shared<SystemSplit>(kSystemCatalog);
  dataSource->addSplit(split);

  velox::ContinueFuture future;
  auto result = dataSource->next(1024, future);
  ASSERT_TRUE(result.has_value());
  ASSERT_NE(result.value(), nullptr);
  auto vector = result.value();
  ASSERT_EQ(vector->size(), 1);
  ASSERT_EQ(vector->type()->size(), 2);

  auto* qid = vector->childAt(0)->asFlatVector<velox::StringView>();
  EXPECT_EQ(qid->valueAt(0).str(), "q2");

  auto* state = vector->childAt(1)->asFlatVector<velox::StringView>();
  EXPECT_EQ(state->valueAt(0).str(), "QUEUED");
}

TEST_F(SystemConnectorMetadataTest, dataSourceEmpty) {
  // No queries in the mock.
  auto fullSchema = queriesTableSchema();
  velox::connector::ColumnHandleMap columnHandles;
  for (const auto& name : fullSchema->names()) {
    columnHandles[name] = std::make_shared<SystemColumnHandle>(name);
  }

  auto dataSource = std::make_unique<SystemDataSource>(
      fullSchema, columnHandles, mockProvider_.get(), pool_.get());

  auto split = std::make_shared<SystemSplit>(kSystemCatalog);
  dataSource->addSplit(split);

  velox::ContinueFuture future;
  auto result = dataSource->next(1024, future);
  ASSERT_TRUE(result.has_value());
  ASSERT_NE(result.value(), nullptr);
  EXPECT_EQ(result.value()->size(), 0);
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

  mockProvider_->addQuery(std::move(snapshot));

  // Request only the source column.
  auto outputType = velox::ROW({{"source", velox::VARCHAR()}});
  velox::connector::ColumnHandleMap columnHandles;
  columnHandles["source"] = std::make_shared<SystemColumnHandle>("source");

  auto dataSource = std::make_unique<SystemDataSource>(
      outputType, columnHandles, mockProvider_.get(), pool_.get());

  auto split = std::make_shared<SystemSplit>(kSystemCatalog);
  dataSource->addSplit(split);

  velox::ContinueFuture future;
  auto result = dataSource->next(1024, future);
  ASSERT_TRUE(result.has_value());
  ASSERT_NE(result.value(), nullptr);
  auto vector = result.value();
  ASSERT_EQ(vector->size(), 1);

  // source should be null since snapshot.source is std::nullopt.
  EXPECT_TRUE(vector->childAt(0)->isNullAt(0));
}

} // namespace
} // namespace facebook::axiom::connector::system
