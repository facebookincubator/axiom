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

#include <folly/ScopeGuard.h>
#include <folly/coro/BlockingWait.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/synthetic/SyntheticExecutionConnector.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/core/Expressions.h"
#include "velox/expression/Expr.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::axiom::connector::synthetic {
namespace {

using namespace facebook::velox;

// Provider returning a fixed batch, so the scan mechanism can be exercised
// without any concrete metadata source.
class FixedRowProvider : public SyntheticRowProvider {
 public:
  explicit FixedRowProvider(RowVectorPtr rows)
      : rows_{std::move(rows)}, rowType_{asRowType(rows_->type())} {}

  const RowTypePtr& rowType() const override {
    return rowType_;
  }

  RowVectorPtr buildRows(
      const std::vector<core::TypedExprPtr>& /*acceptedFilters*/,
      memory::MemoryPool* /*pool*/) const override {
    return rows_;
  }

 private:
  const RowVectorPtr rows_;
  const RowTypePtr rowType_;
};

// Records the acceptedFilters handed to buildRows, to verify the scan forwards
// them from the handle down to the provider.
class RecordingProvider : public SyntheticRowProvider {
 public:
  explicit RecordingProvider(RowVectorPtr rows)
      : rows_{std::move(rows)}, rowType_{asRowType(rows_->type())} {}

  const RowTypePtr& rowType() const override {
    return rowType_;
  }

  RowVectorPtr buildRows(
      const std::vector<core::TypedExprPtr>& acceptedFilters,
      memory::MemoryPool* /*pool*/) const override {
    receivedFilters_ = acceptedFilters;
    return rows_;
  }

  const std::vector<core::TypedExprPtr>& receivedFilters() const {
    return receivedFilters_;
  }

 private:
  const RowVectorPtr rows_;
  const RowTypePtr rowType_;
  mutable std::vector<core::TypedExprPtr> receivedFilters_;
};

// Declares one schema but returns a different one from buildRows, to exercise
// the schema-consistency check.
class MisorderedProvider : public SyntheticRowProvider {
 public:
  MisorderedProvider(RowTypePtr declaredType, RowVectorPtr rows)
      : declaredType_{std::move(declaredType)}, rows_{std::move(rows)} {}

  const RowTypePtr& rowType() const override {
    return declaredType_;
  }

  RowVectorPtr buildRows(
      const std::vector<core::TypedExprPtr>& /*acceptedFilters*/,
      memory::MemoryPool* /*pool*/) const override {
    return rows_;
  }

 private:
  const RowTypePtr declaredType_;
  const RowVectorPtr rows_;
};

// Accepts the first filter for pushdown and rejects the rest, to exercise the
// accept/reject split in the layout.
class SplittingProvider : public SyntheticRowProvider {
 public:
  explicit SplittingProvider(RowTypePtr rowType)
      : rowType_{std::move(rowType)} {}

  const RowTypePtr& rowType() const override {
    return rowType_;
  }

  std::vector<core::TypedExprPtr> acceptFilters(
      std::vector<core::TypedExprPtr> filters,
      std::vector<core::TypedExprPtr>& rejected) const override {
    std::vector<core::TypedExprPtr> accepted;
    for (size_t i = 0; i < filters.size(); ++i) {
      if (i == 0) {
        accepted.push_back(filters[i]);
      } else {
        rejected.push_back(filters[i]);
      }
    }
    return accepted;
  }

  RowVectorPtr buildRows(
      const std::vector<core::TypedExprPtr>& /*acceptedFilters*/,
      memory::MemoryPool* pool) const override {
    return BaseVector::create<RowVector>(rowType_, 0, pool);
  }

 private:
  const RowTypePtr rowType_;
};

// Serves a single synthetic relation from a fixed provider, to drive the routed
// scan path end to end.
class SingleRelationMetadata : public ConnectorMetadata {
 public:
  SingleRelationMetadata(
      SchemaTableName name,
      std::shared_ptr<const SyntheticRowProvider> provider)
      : name_{std::move(name)}, provider_{std::move(provider)} {}

  std::shared_ptr<const SyntheticRowProvider> findSyntheticProvider(
      const SchemaTableName& name) override {
    return name == name_ ? provider_ : nullptr;
  }

  TablePtr findTable(const SchemaTableName&) override {
    return nullptr;
  }

  ConnectorSplitManager* splitManager() override {
    return nullptr;
  }

  std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr&) override {
    return {};
  }

  bool schemaExists(const ConnectorSessionPtr&, const std::string&) override {
    return false;
  }

  std::vector<std::string> listTableNames(
      const ConnectorSessionPtr&,
      const std::string&) override {
    return {};
  }

 private:
  const SchemaTableName name_;
  const std::shared_ptr<const SyntheticRowProvider> provider_;
};

class SyntheticMechanismTest : public testing::Test,
                               public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    registerSyntheticExecutionConnector();
  }

  void TearDown() override {
    unregisterSyntheticExecutionConnector();
  }

  static ConnectorSessionPtr makeSession() {
    return std::make_shared<ConnectorSession>(
        /*queryId=*/"test", /*user=*/"test", Properties{});
  }

  static const std::string& kCatalog() {
    static const std::string kValue{"fixed-catalog"};
    return kValue;
  }
};

// A scan requesting a subset of columns gets the provider's full schema
// projected down to just those columns.
TEST_F(SyntheticMechanismTest, columnProjection) {
  auto provider = std::make_shared<FixedRowProvider>(makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<std::string>({"x", "y", "z"})}));

  // Request only "b" so the data source must project the provider's full-schema
  // output down to the scan's output columns.
  auto outputType = ROW({{"b", VARCHAR()}});
  velox::connector::ColumnHandleMap columnHandles;
  columnHandles["b"] = std::make_shared<SyntheticColumnHandle>("b");

  SyntheticDataSource dataSource(
      outputType, columnHandles, provider, /*acceptedFilters=*/{}, pool_.get());
  dataSource.addSplit(
      std::make_shared<SyntheticSplit>(std::string(kSyntheticConnectorId)));

  ContinueFuture future;
  auto batch = dataSource.next(1024, future);
  ASSERT_TRUE(batch.has_value());
  test::assertEqualVectors(
      makeRowVector({"b"}, {makeFlatVector<std::string>({"x", "y", "z"})}),
      batch.value());

  // A second call signals end-of-data with a null vector; nullopt would instead
  // mean the source is blocked.
  auto exhausted = dataSource.next(1024, future);
  ASSERT_TRUE(exhausted.has_value());
  EXPECT_EQ(exhausted.value(), nullptr);
}

// next(size) hands the rows out in batches of at most 'size'.
TEST_F(SyntheticMechanismTest, batchSize) {
  auto provider = std::make_shared<FixedRowProvider>(
      makeRowVector({"a"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5})}));

  velox::connector::ColumnHandleMap columnHandles;
  columnHandles["a"] = std::make_shared<SyntheticColumnHandle>("a");

  SyntheticDataSource dataSource(
      ROW({{"a", BIGINT()}}),
      columnHandles,
      provider,
      /*acceptedFilters=*/{},
      pool_.get());
  dataSource.addSplit(
      std::make_shared<SyntheticSplit>(std::string(kSyntheticConnectorId)));

  // Five rows requested two at a time arrive as 2 + 2 + 1, not one batch.
  ContinueFuture future;
  std::vector<vector_size_t> batchSizes;
  while (auto batch = dataSource.next(/*size=*/2, future)) {
    if (batch.value() == nullptr) {
      break;
    }
    batchSizes.push_back(batch.value()->size());
  }
  EXPECT_THAT(batchSizes, testing::ElementsAre(2, 2, 1));
}

// The accepted filters recorded on the handle are forwarded to the provider's
// buildRows() at scan time.
TEST_F(SyntheticMechanismTest, acceptedFiltersReachProvider) {
  auto provider = std::make_shared<RecordingProvider>(
      makeRowVector({"a"}, {makeFlatVector<int64_t>({1, 2, 3})}));

  core::TypedExprPtr filter =
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "a");
  velox::connector::ColumnHandleMap columnHandles;
  columnHandles["a"] = std::make_shared<SyntheticColumnHandle>("a");

  SyntheticDataSource dataSource(
      ROW({{"a", BIGINT()}}),
      columnHandles,
      provider,
      /*acceptedFilters=*/{filter},
      pool_.get());
  dataSource.addSplit(
      std::make_shared<SyntheticSplit>(std::string(kSyntheticConnectorId)));

  ContinueFuture future;
  dataSource.next(1024, future);
  ASSERT_EQ(provider->receivedFilters().size(), 1);
  EXPECT_EQ(provider->receivedFilters().at(0), filter);
}

// buildRows() output whose schema disagrees with the provider's declared
// rowType() (here, columns in a different order) is rejected.
TEST_F(SyntheticMechanismTest, providerSchemaMismatch) {
  auto declaredType = ROW({{"a", BIGINT()}, {"b", BIGINT()}});
  // Same types, names swapped: this passes the looser equivalent() check (which
  // ignores names) but must be caught by the stricter *type == *type
  // comparison.
  auto rows = makeRowVector(
      {"b", "a"}, {makeFlatVector<int64_t>({1}), makeFlatVector<int64_t>({2})});
  auto provider =
      std::make_shared<MisorderedProvider>(declaredType, std::move(rows));

  velox::connector::ColumnHandleMap columnHandles;
  columnHandles["a"] = std::make_shared<SyntheticColumnHandle>("a");
  columnHandles["b"] = std::make_shared<SyntheticColumnHandle>("b");

  SyntheticDataSource dataSource(
      declaredType,
      columnHandles,
      provider,
      /*acceptedFilters=*/{},
      pool_.get());
  dataSource.addSplit(
      std::make_shared<SyntheticSplit>(std::string(kSyntheticConnectorId)));

  ContinueFuture future;
  EXPECT_ANY_THROW(dataSource.next(1024, future));
}

// createTableHandle() delegates the accept/reject split to the provider: the
// accepted filter is stored on the handle, the rest are returned to the engine.
TEST_F(SyntheticMechanismTest, filterSplit) {
  const SchemaTableName name{"s", "t"};
  auto provider = std::make_shared<SplittingProvider>(
      ROW({{"a", BIGINT()}, {"b", BIGINT()}}));
  auto metadata =
      std::make_shared<SingleRelationMetadata>(name, std::move(provider));
  ConnectorMetadataRegistry::global().insert(kCatalog(), metadata);
  auto registryGuard = folly::makeGuard(
      [&] { ConnectorMetadataRegistry::global().erase(kCatalog()); });

  auto table = findSyntheticTable(name, metadata.get(), kCatalog());
  ASSERT_NE(table, nullptr);
  const auto* layout = table->layouts().at(0);

  auto session = makeSession();
  std::vector<velox::connector::ColumnHandlePtr> columnHandles;
  for (const auto& columnName : table->type()->names()) {
    columnHandles.push_back(layout->createColumnHandle(session, columnName));
  }

  auto queryCtx = core::QueryCtx::create();
  exec::SimpleExpressionEvaluator evaluator(queryCtx.get(), pool_.get());
  std::vector<core::TypedExprPtr> filters{
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "a"),
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "b")};
  std::vector<core::TypedExprPtr> rejected;
  auto tableHandle = layout->createTableHandle(
      session, columnHandles, evaluator, std::move(filters), rejected);

  auto* syntheticHandle =
      dynamic_cast<const SyntheticTableHandle*>(tableHandle.get());
  ASSERT_NE(syntheticHandle, nullptr);
  EXPECT_EQ(syntheticHandle->acceptedFilters().size(), 1);
  EXPECT_EQ(rejected.size(), 1);
}

// Drives the full routed scan end to end: findSyntheticTable() through the
// layout, handle, and split to a DataSource on the shared connector, which
// rebuilds the provider from the origin catalog's metadata.
TEST_F(SyntheticMechanismTest, e2eRoutedScan) {
  const SchemaTableName name{"s", "t"};
  auto metadata = std::make_shared<SingleRelationMetadata>(
      name,
      std::make_shared<FixedRowProvider>(
          makeRowVector({"a"}, {makeFlatVector<int64_t>({10, 20})})));
  ConnectorMetadataRegistry::global().insert(kCatalog(), metadata);
  // Erase even if an assertion below returns early, so the global registry does
  // not leak this catalog into other tests.
  auto registryGuard = folly::makeGuard(
      [&] { ConnectorMetadataRegistry::global().erase(kCatalog()); });

  auto table = findSyntheticTable(name, metadata.get(), kCatalog());
  ASSERT_NE(table, nullptr);
  const auto* layout = table->layouts().at(0);
  const auto& outputType = table->type();

  auto session = makeSession();
  std::vector<velox::connector::ColumnHandlePtr> columnHandles;
  velox::connector::ColumnHandleMap columnHandleMap;
  for (const auto& columnName : outputType->names()) {
    auto handle = layout->createColumnHandle(session, columnName);
    columnHandleMap[columnName] = handle;
    columnHandles.push_back(std::move(handle));
  }

  auto queryCtx = core::QueryCtx::create();
  exec::SimpleExpressionEvaluator evaluator(queryCtx.get(), pool_.get());
  std::vector<core::TypedExprPtr> rejected;
  auto tableHandle = layout->createTableHandle(
      session, columnHandles, evaluator, /*filters=*/{}, rejected);

  // The provider lives in the source catalog, but the scan and split routing
  // resolve to the shared synthetic execution connector.
  auto* syntheticHandle =
      dynamic_cast<const SyntheticTableHandle*>(tableHandle.get());
  ASSERT_NE(syntheticHandle, nullptr);
  EXPECT_EQ(syntheticHandle->connectorId(), kSyntheticConnectorId);
  EXPECT_EQ(syntheticHandle->sourceCatalog(), kCatalog());

  auto* splitManager =
      ConnectorMetadataRegistry::get(tableHandle->connectorId())
          ->splitManager();
  auto partitions = folly::coro::blockingWait(
      splitManager->co_listPartitions(session, tableHandle));
  QueryRuntimeStats runtimeStats;
  auto splitSource = splitManager->getSplitSource(
      session,
      tableHandle,
      partitions,
      /*partitionType=*/nullptr,
      runtimeStats);
  auto splitBatch = folly::coro::blockingWait(
      splitSource->co_getSplits(/*maxSplitCount=*/10));
  ASSERT_EQ(splitBatch.splits.size(), 1);
  folly::coro::blockingWait(splitSource->co_close());

  auto config = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});
  auto connectorQueryCtx =
      std::make_unique<velox::connector::ConnectorQueryCtx>(
          pool_.get(),
          pool_.get(),
          config.get(),
          /*spillConfig=*/nullptr,
          common::PrefixSortConfig{},
          /*expressionEvaluator=*/nullptr,
          /*cache=*/nullptr,
          "test-query",
          "test-task",
          "test-plan-node",
          /*driverId=*/0,
          /*sessionTimezone=*/"UTC");

  auto dataSource =
      velox::connector::ConnectorRegistry::tryGet(tableHandle->connectorId())
          ->createDataSource(
              outputType,
              tableHandle,
              columnHandleMap,
              connectorQueryCtx.get());
  dataSource->addSplit(splitBatch.splits.front().connectorSplit);

  ContinueFuture future;
  auto batch = dataSource->next(1024, future);
  ASSERT_TRUE(batch.has_value());
  test::assertEqualVectors(
      makeRowVector({"a"}, {makeFlatVector<int64_t>({10, 20})}), batch.value());
}

// findSyntheticTable() returns null for a relation the catalog does not serve.
TEST_F(SyntheticMechanismTest, unknownRelation) {
  const SchemaTableName served{"s", "t"};
  SingleRelationMetadata metadata{
      served,
      std::make_shared<FixedRowProvider>(
          makeRowVector({"a"}, {makeFlatVector<int64_t>({1})}))};

  EXPECT_EQ(
      findSyntheticTable(SchemaTableName{"s", "u"}, &metadata, kCatalog()),
      nullptr);
}

} // namespace
} // namespace facebook::axiom::connector::synthetic
