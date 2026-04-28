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

#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/common/SchemaTableName.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/hive/HiveConnectorMetadata.h"

#include <folly/coro/GtestHelpers.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <numeric>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/expression/Expr.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::axiom::connector {
namespace {

using namespace facebook::velox;

class TestConnectorTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static const inline std::string kDefaultSchema{TestConnector::kDefaultSchema};

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    connector_ = std::make_shared<TestConnector>("test");
    velox::connector::registerConnector(connector_);
    metadata_ = ConnectorMetadataRegistry::get(connector_->connectorId());
  }

  void TearDown() override {
    metadata_.reset();
    velox::connector::unregisterConnector(connector_->connectorId());
  }

  std::shared_ptr<TestConnector> connector_;
  std::shared_ptr<ConnectorMetadata> metadata_;
};

TEST_F(TestConnectorTest, connectorRegister) {
  const auto connectorId = "registration-test";
  VELOX_ASSERT_THROW(
      velox::connector::getConnector(connectorId),
      "Connector with ID is not registered: registration-test");

  auto connector = std::make_shared<TestConnector>(connectorId);
  EXPECT_EQ(connector->connectorId(), connectorId);

  registerConnector(connector);

  EXPECT_EQ(velox::connector::getConnector(connectorId).get(), connector.get());
  EXPECT_NE(ConnectorMetadataRegistry::tryGet(connectorId), nullptr);

  velox::connector::unregisterConnector(connectorId);
  VELOX_ASSERT_THROW(
      velox::connector::getConnector(connectorId),
      "Connector with ID is not registered: registration-test");
}

TEST_F(TestConnectorTest, table) {
  auto schema = ROW({{"a", INTEGER()}, {"b", VARCHAR()}});
  connector_->addTable("table", schema);
  auto table = metadata_->findTable({kDefaultSchema, "table"});
  EXPECT_NE(table, nullptr);
  EXPECT_EQ(table->name(), (SchemaTableName{kDefaultSchema, "table"}));
  EXPECT_EQ(table->numRows(), 0);
  EXPECT_EQ(table->columnMap().size(), 2);
  EXPECT_TRUE(table->columnMap().contains("a"));
  EXPECT_TRUE(table->columnMap().contains("b"));

  auto vector = makeRowVector(
      {makeFlatVector<int>({0, 1, 2}),
       makeFlatVector<StringView>({"a", "b", "c"})});
  connector_->appendData("table", vector);
  EXPECT_EQ(table->numRows(), 3);

  vector = makeRowVector({makeFlatVector<int>({0, 1, 2})});
  VELOX_ASSERT_THROW(
      connector_->appendData("table", vector),
      "appended data type ROW<c0:INTEGER> must match table type ROW<a:INTEGER,b:VARCHAR>");

  connector_->addTable("noschema", velox::ROW({}));
  table = metadata_->findTable({kDefaultSchema, "noschema"});
  EXPECT_NE(table, nullptr);
  EXPECT_EQ(table->numRows(), 0);
  EXPECT_EQ(table->columnMap().size(), 0);

  table = metadata_->findTable({kDefaultSchema, "notable"});
  EXPECT_EQ(table, nullptr);
}

TEST_F(TestConnectorTest, columnHandle) {
  auto schema = ROW({{"a", INTEGER()}, {"b", VARCHAR()}});
  connector_->addTable("table", schema);

  auto table = metadata_->findTable({kDefaultSchema, "table"});
  auto& layout = *table->layouts()[0];

  auto columnHandle = layout.createColumnHandle(/*session=*/nullptr, "a");
  EXPECT_NE(columnHandle, nullptr);

  auto testColumnHandle =
      std::dynamic_pointer_cast<const TestColumnHandle>(columnHandle);
  EXPECT_NE(testColumnHandle, nullptr);
  EXPECT_EQ(testColumnHandle->name(), "a");
  EXPECT_EQ(testColumnHandle->type()->kind(), TypeKind::INTEGER);
}

CO_TEST_F(TestConnectorTest, splitManager) {
  auto schema = ROW({"a"}, {INTEGER()});
  auto table = connector_->addTable("test_table", schema);

  auto vector = makeRowVector({makeFlatVector<int>({1})});
  constexpr size_t kNumSplits = 1024;
  for (size_t i = 0; i < kNumSplits; ++i) {
    connector_->appendData("test_table", vector);
  }

  auto& layout = *table->layouts()[0];

  auto splitManager = metadata_->splitManager();
  EXPECT_NE(splitManager, nullptr);

  auto evaluator =
      std::make_unique<exec::SimpleExpressionEvaluator>(nullptr, nullptr);
  std::vector<velox::connector::ColumnHandlePtr> columns;
  columns.push_back(layout.createColumnHandle(nullptr, "a"));
  std::vector<core::TypedExprPtr> empty;
  auto tableHandle = layout.createTableHandle(
      nullptr, std::move(columns), *evaluator, empty, empty);

  auto partitions =
      co_await splitManager->co_listPartitions(nullptr, tableHandle);
  auto splitSource =
      splitManager->getSplitSource(nullptr, tableHandle, partitions, {});
  EXPECT_NE(splitSource, nullptr);

  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits;
  while (true) {
    auto batch = co_await splitSource->co_getSplits(/*maxSplitCount=*/1000);
    for (auto& split : batch.splits) {
      splits.push_back(std::move(split));
    }
    if (batch.noMoreSplits) {
      break;
    }
  }
  EXPECT_EQ(splits.size(), kNumSplits);
  for (size_t i = 0; i < kNumSplits; ++i) {
    auto split = std::dynamic_pointer_cast<TestConnectorSplit>(splits[i]);
    EXPECT_NE(split, nullptr);
    EXPECT_EQ(split->index(), i);
  }
}

TEST_F(TestConnectorTest, splits) {
  auto split = std::make_shared<velox::connector::ConnectorSplit>(
      connector_->connectorId());
  EXPECT_EQ(split->connectorId, connector_->connectorId());
}

TEST_F(TestConnectorTest, dataSink) {
  auto schema = ROW({"a"}, {INTEGER()});
  auto handle = std::make_shared<TestInsertTableHandle>(
      SchemaTableName{std::string(TestConnector::kDefaultSchema), "table"});
  auto table = connector_->addTable("table", schema);
  EXPECT_EQ(table->numRows(), 0);

  auto emptyConfig = std::make_shared<config::ConfigBase>(
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

  auto dataSink = connector_->createDataSink(
      schema,
      handle,
      connectorQueryCtx.get(),
      velox::connector::CommitStrategy::kNoCommit);
  EXPECT_NE(dataSink, nullptr);

  auto vector = makeRowVector({makeFlatVector<int>({0, 1, 2})});
  dataSink->appendData(vector);
  EXPECT_EQ(table->numRows(), 3);

  vector = makeRowVector({makeFlatVector<int>({3, 4})});
  dataSink->appendData(vector);
  EXPECT_EQ(table->numRows(), 5);

  dataSink->appendData(nullptr);
  EXPECT_EQ(table->numRows(), 5);

  EXPECT_TRUE(dataSink->finish());
  EXPECT_TRUE(dataSink->close().empty());
}

TEST_F(TestConnectorTest, dataSource) {
  auto schema = ROW({"a", "b"}, {INTEGER(), VARCHAR()});
  auto table = connector_->addTable("table", schema);
  auto& layout = *table->layouts()[0];

  std::vector<velox::connector::ColumnHandlePtr> columns;
  columns.push_back(layout.createColumnHandle(/*session=*/nullptr, "a"));
  columns.push_back(layout.createColumnHandle(/*session=*/nullptr, "b"));

  auto evaluator =
      std::make_unique<exec::SimpleExpressionEvaluator>(nullptr, nullptr);
  std::vector<core::TypedExprPtr> empty;
  auto tableHandle = layout.createTableHandle(
      /*session=*/nullptr,

      std::move(columns),
      *evaluator,
      empty,
      empty);

  auto vector1 = makeRowVector(
      {makeFlatVector<int>({0, 1}), makeFlatVector<StringView>({"a", "b"})});
  connector_->appendData("table", vector1);
  auto vector2 = makeRowVector(
      {makeFlatVector<int>({3, 4}), makeFlatVector<StringView>({"d", "e"})});
  connector_->appendData("table", vector2);

  velox::connector::ColumnHandleMap handleMap;
  handleMap.emplace("a", layout.createColumnHandle(/*session=*/nullptr, "a"));
  handleMap.emplace("b", layout.createColumnHandle(/*session=*/nullptr, "b"));
  auto dataSource = std::make_shared<TestDataSource>(
      schema, std::move(handleMap), table, pool());
  EXPECT_EQ(dataSource->getCompletedRows(), 0);

  auto split0 =
      std::make_shared<TestConnectorSplit>(connector_->connectorId(), 0);
  dataSource->addSplit(split0);

  velox::ContinueFuture future;
  auto result = dataSource->next(0, future);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result.value()->size(), 2);
  EXPECT_EQ(dataSource->getCompletedRows(), 2);
  test::assertEqualVectors(vector1, result.value());

  result = dataSource->next(0, future);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), nullptr);

  auto split1 =
      std::make_shared<TestConnectorSplit>(connector_->connectorId(), 1);
  dataSource->addSplit(split1);

  result = dataSource->next(0, future);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result.value()->size(), 2);
  EXPECT_EQ(dataSource->getCompletedRows(), 4);
  test::assertEqualVectors(vector2, result.value());

  result = dataSource->next(0, future);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), nullptr);
}

TEST_F(TestConnectorTest, testColumnHandleCreation) {
  auto columnHandle = std::make_shared<TestColumnHandle>("col", INTEGER());
  EXPECT_EQ(columnHandle->name(), "col");
  EXPECT_EQ(columnHandle->type()->kind(), TypeKind::INTEGER);
}

TEST_F(TestConnectorTest, tableLayout) {
  auto schema = ROW({"a", "b"}, {INTEGER(), VARCHAR()});
  auto table = connector_->addTable("table", schema);

  auto& layout = *table->layouts()[0];
  EXPECT_EQ(layout.label(), "table");

  const auto& columnMap = table->columnMap();
  EXPECT_NE(columnMap.find("a"), columnMap.end());
  EXPECT_NE(columnMap.find("b"), columnMap.end());

  auto col1 = layout.findColumn("a");
  EXPECT_NE(col1, nullptr);
  EXPECT_EQ(col1->name(), "a");
  EXPECT_EQ(col1->type()->kind(), TypeKind::INTEGER);

  auto col2 = layout.findColumn("b");
  EXPECT_NE(col2, nullptr);
  EXPECT_EQ(col2->name(), "b");
  EXPECT_EQ(col2->type()->kind(), TypeKind::VARCHAR);

  auto nonExistent = layout.findColumn("nonexistent");
  EXPECT_EQ(nonExistent, nullptr);
}

// Verifies that addData computes per-column statistics incrementally.
TEST_F(TestConnectorTest, addDataStats) {
  auto table = connector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));

  connector_->appendData(
      "t",
      makeRowVector(
          {makeFlatVector<int64_t>({10, 20, 30}),
           makeFlatVector<int64_t>({1, 1, 2}),
           makeNullConstant(TypeKind::BIGINT, 3)}));

  EXPECT_EQ(table->numRows(), 3);

  {
    SCOPED_TRACE("column a");
    auto* column = table->findColumn("a");
    ASSERT_NE(column, nullptr);
    auto* stats = column->stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->numValues, 3);
    EXPECT_EQ(stats->nullPct, 0);
    EXPECT_TRUE(stats->nonNull);
    EXPECT_EQ(stats->numDistinct, 3);
    EXPECT_EQ(stats->min, Variant(10LL));
    EXPECT_EQ(stats->max, Variant(30LL));
  }

  {
    SCOPED_TRACE("column b");
    auto* column = table->findColumn("b");
    ASSERT_NE(column, nullptr);
    auto* stats = column->stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->numValues, 3);
    EXPECT_EQ(stats->nullPct, 0);
    EXPECT_TRUE(stats->nonNull);
    EXPECT_EQ(stats->numDistinct, 2);
    EXPECT_EQ(stats->min, Variant(1LL));
    EXPECT_EQ(stats->max, Variant(2LL));
  }

  {
    SCOPED_TRACE("column c - all nulls");
    auto* column = table->findColumn("c");
    ASSERT_NE(column, nullptr);
    auto* stats = column->stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->numValues, 0);
    EXPECT_EQ(stats->nullPct, 100);
    EXPECT_FALSE(stats->nonNull);
    EXPECT_EQ(stats->numDistinct, 0);
    EXPECT_FALSE(stats->min.has_value());
    EXPECT_FALSE(stats->max.has_value());
  }
}

// Verifies incremental stat computation across multiple addData calls.
TEST_F(TestConnectorTest, addDataStatsMultipleBatches) {
  auto table = connector_->addTable("t", ROW("a", BIGINT()));

  connector_->appendData(
      "t", makeRowVector({makeFlatVector<int64_t>({10, 20})}));
  connector_->appendData(
      "t", makeRowVector({makeFlatVector<int64_t>({20, 30})}));

  EXPECT_EQ(table->numRows(), 4);

  auto* column = table->findColumn("a");
  ASSERT_NE(column, nullptr);
  auto* stats = column->stats();
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->numValues, 4);
  EXPECT_EQ(stats->nullPct, 0);
  EXPECT_TRUE(stats->nonNull);
  // 20 appears in both batches, so numDistinct should be 3.
  EXPECT_EQ(stats->numDistinct, 3);
  EXPECT_EQ(stats->min, Variant(10LL));
  EXPECT_EQ(stats->max, Variant(30LL));
}

// Verifies null statistics.
TEST_F(TestConnectorTest, addDataStatsWithNulls) {
  auto table = connector_->addTable("t", ROW("a", BIGINT()));

  connector_->appendData(
      "t",
      makeRowVector({makeNullableFlatVector<int64_t>(
          {10, std::nullopt, 30, std::nullopt})}));

  auto* column = table->findColumn("a");
  ASSERT_NE(column, nullptr);
  auto* stats = column->stats();
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->numValues, 2);
  EXPECT_EQ(stats->nullPct, 50);
  EXPECT_FALSE(stats->nonNull);
  EXPECT_EQ(stats->numDistinct, 2);
  EXPECT_EQ(stats->min, Variant(10LL));
  EXPECT_EQ(stats->max, Variant(30LL));
}

// Verifies VARCHAR stats including maxLength.
TEST_F(TestConnectorTest, addDataStatsVarchar) {
  auto table = connector_->addTable("t", ROW({"a", "b"}, VARCHAR()));

  connector_->appendData(
      "t",
      makeRowVector(
          {makeFlatVector<std::string>({"abc", "z", "hello"}),
           makeFlatVector<std::string>({"", "", ""})}));

  {
    SCOPED_TRACE("non-empty strings");
    auto* column = table->findColumn("a");
    ASSERT_NE(column, nullptr);
    auto* stats = column->stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->numValues, 3);
    EXPECT_EQ(stats->nullPct, 0);
    EXPECT_TRUE(stats->nonNull);
    EXPECT_EQ(stats->numDistinct, 3);
    EXPECT_EQ(stats->min, Variant("abc"));
    EXPECT_EQ(stats->max, Variant("z"));
    EXPECT_EQ(stats->maxLength, 5); // "hello" has 5 bytes
  }

  {
    SCOPED_TRACE("empty strings");
    auto* column = table->findColumn("b");
    ASSERT_NE(column, nullptr);
    auto* stats = column->stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->numValues, 3);
    EXPECT_EQ(stats->nullPct, 0);
    EXPECT_TRUE(stats->nonNull);
    EXPECT_EQ(stats->numDistinct, 1);
    EXPECT_EQ(stats->min, Variant(""));
    EXPECT_EQ(stats->max, Variant(""));
    EXPECT_EQ(stats->maxLength, 0);
  }
}

// Verifies ARRAY stats: maxLength only, no numDistinct or min/max.
TEST_F(TestConnectorTest, addDataStatsArray) {
  auto table = connector_->addTable("t", ROW({"a", "b"}, ARRAY(BIGINT())));

  connector_->appendData(
      "t",
      makeRowVector(
          {makeArrayVector<int64_t>({{1, 2, 3}, {4}, {5, 6}}),
           makeArrayVector<int64_t>({{}, {}, {}})}));

  {
    SCOPED_TRACE("non-empty arrays");
    auto* column = table->findColumn("a");
    ASSERT_NE(column, nullptr);
    auto* stats = column->stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->numValues, 3);
    EXPECT_EQ(stats->nullPct, 0);
    EXPECT_TRUE(stats->nonNull);
    EXPECT_FALSE(stats->numDistinct.has_value());
    EXPECT_FALSE(stats->min.has_value());
    EXPECT_FALSE(stats->max.has_value());
    EXPECT_EQ(stats->maxLength, 3); // {1, 2, 3} has 3 elements
  }

  {
    SCOPED_TRACE("empty arrays");
    auto* column = table->findColumn("b");
    ASSERT_NE(column, nullptr);
    auto* stats = column->stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->numValues, 3);
    EXPECT_EQ(stats->nullPct, 0);
    EXPECT_TRUE(stats->nonNull);
    EXPECT_FALSE(stats->numDistinct.has_value());
    EXPECT_FALSE(stats->min.has_value());
    EXPECT_FALSE(stats->max.has_value());
    EXPECT_EQ(stats->maxLength, 0);
  }
}

// Verifies MAP stats: maxLength only, no numDistinct or min/max.
TEST_F(TestConnectorTest, addDataStatsMap) {
  auto table = connector_->addTable("t", ROW("m", MAP(BIGINT(), VARCHAR())));

  connector_->appendData(
      "t",
      makeRowVector({makeMapVector<int64_t, std::string>({
          {{1, "a"}, {2, "b"}},
          {{3, "c"}},
      })}));

  auto* column = table->findColumn("m");
  ASSERT_NE(column, nullptr);
  auto* stats = column->stats();
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->numValues, 2);
  EXPECT_EQ(stats->nullPct, 0);
  EXPECT_TRUE(stats->nonNull);
  EXPECT_FALSE(stats->numDistinct.has_value());
  EXPECT_FALSE(stats->min.has_value());
  EXPECT_FALSE(stats->max.has_value());
  EXPECT_EQ(stats->maxLength, 2); // first map has 2 entries
}

// Verifies that setStats and addData cannot be used on the same table.
TEST_F(TestConnectorTest, addDataAndSetStatsMutualExclusion) {
  {
    SCOPED_TRACE("setStats then addData");
    auto table = connector_->addTable("t1", ROW("a", BIGINT()));
    table->setStats(100, {{"a", {.numDistinct = 50}}});
    VELOX_ASSERT_THROW(
        connector_->appendData(
            "t1", makeRowVector({makeFlatVector<int64_t>({1})})),
        "Cannot use both setStats and addData on table");
  }

  {
    SCOPED_TRACE("addData then setStats");
    auto table = connector_->addTable("t2", ROW("a", BIGINT()));
    connector_->appendData("t2", makeRowVector({makeFlatVector<int64_t>({1})}));
    VELOX_ASSERT_THROW(
        table->setStats(100, {{"a", {.numDistinct = 50}}}),
        "Cannot use both setStats and addData on table");
  }
}

namespace {

std::vector<uint32_t> referenceBucketIds(
    const velox::RowVectorPtr& input,
    const std::vector<velox::column_index_t>& channels,
    int32_t numBuckets) {
  velox::connector::hive::HivePartitionFunctionSpec spec(
      numBuckets, channels, /*constValues=*/{});
  std::vector<uint32_t> bucketIds;
  if (auto single = spec.create(numBuckets, /*localExchange=*/false)
                        ->partition(*input, bucketIds);
      single.has_value()) {
    bucketIds.assign(input->size(), single.value());
  }
  return bucketIds;
}

velox::connector::ConnectorTableHandlePtr makeScanHandle(
    const TableLayout& layout,
    const std::vector<std::string>& columnNames) {
  auto evaluator =
      std::make_unique<exec::SimpleExpressionEvaluator>(nullptr, nullptr);
  std::vector<velox::connector::ColumnHandlePtr> columns;
  columns.reserve(columnNames.size());
  for (const auto& name : columnNames) {
    columns.push_back(layout.createColumnHandle(nullptr, name));
  }
  std::vector<core::TypedExprPtr> empty;
  return layout.createTableHandle(
      nullptr, std::move(columns), *evaluator, empty, empty);
}

} // namespace

CO_TEST_F(TestConnectorTest, bucketedTable) {
  constexpr int32_t kNumBuckets = 4;
  auto schema = ROW({"key"}, {BIGINT()});
  auto table = connector_->addTable(
      "bucketed", schema, velox::ROW({}), TestBucketSpec{{"key"}, kNumBuckets});

  std::vector<int64_t> keys(32);
  std::iota(keys.begin(), keys.end(), 0);
  auto input = makeRowVector({makeFlatVector<int64_t>(keys)});
  connector_->appendData("bucketed", input);

  const auto expected = referenceBucketIds(input, {0}, kNumBuckets);
  EXPECT_EQ(table->data().size(), table->dataBucketIds().size());
  for (size_t i = 0; i < table->data().size(); ++i) {
    const auto firstKey =
        table->data()[i]->childAt(0)->as<SimpleVector<int64_t>>()->valueAt(0);
    EXPECT_EQ(expected[firstKey], table->dataBucketIds()[i]);
  }

  auto tableHandle = makeScanHandle(*table->layouts()[0], {"key"});
  auto* splitManager = metadata_->splitManager();
  auto partitions =
      co_await splitManager->co_listPartitions(nullptr, tableHandle);
  EXPECT_EQ(partitions.size(), kNumBuckets);

  constexpr int32_t kSelectedBucket = 2;
  auto source = splitManager->getSplitSource(
      nullptr, tableHandle, {partitions[kSelectedBucket]}, {});
  while (true) {
    auto batch = co_await source->co_getSplits(/*maxSplitCount=*/16);
    for (const auto& split : batch.splits) {
      auto testSplit =
          std::dynamic_pointer_cast<const TestConnectorSplit>(split);
      CO_ASSERT_NE(testSplit, nullptr);
      EXPECT_EQ(table->dataBucketIds()[testSplit->index()], kSelectedBucket);
    }
    if (batch.noMoreSplits) {
      break;
    }
  }
}

TEST_F(TestConnectorTest, bucketedTableNonExistentBucketColumn) {
  VELOX_ASSERT_THROW(
      connector_->addTable(
          "bad_bucket_col",
          ROW({"a"}, {BIGINT()}),
          velox::ROW({}),
          TestBucketSpec{{"missing"}, 4}),
      "Bucket column does not exist: missing");
}

CO_TEST_F(TestConnectorTest, bucketedTableNumBucketsExceedsNumRows) {
  constexpr int32_t kNumBuckets = 16;
  constexpr int32_t kNumRows = 4;
  auto schema = ROW({"key"}, {BIGINT()});
  auto table = connector_->addTable(
      "sparse", schema, velox::ROW({}), TestBucketSpec{{"key"}, kNumBuckets});

  auto input = makeRowVector({makeFlatVector<int64_t>({0, 1, 2, 3})});
  connector_->appendData("sparse", input);

  EXPECT_LE(table->data().size(), kNumRows);
  EXPECT_EQ(table->data().size(), table->dataBucketIds().size());
  for (const auto& vector : table->data()) {
    EXPECT_GT(vector->size(), 0);
  }

  auto tableHandle = makeScanHandle(*table->layouts()[0], {"key"});
  auto partitions = co_await metadata_->splitManager()->co_listPartitions(
      nullptr, tableHandle);
  EXPECT_EQ(partitions.size(), kNumBuckets);
}

TEST_F(TestConnectorTest, bucketedTableMultiColumnKeys) {
  constexpr int32_t kNumBuckets = 8;
  constexpr int32_t kNumRows = 64;
  auto schema = ROW({"k1", "k2", "v"}, {BIGINT(), VARCHAR(), DOUBLE()});
  auto table = connector_->addTable(
      "multi_key",
      schema,
      velox::ROW({}),
      TestBucketSpec{{"k1", "k2"}, kNumBuckets});

  auto& layout = *table->layouts()[0];
  EXPECT_NE(layout.partitionType(), nullptr);
  EXPECT_EQ(layout.partitionColumns().size(), 2);

  std::vector<int64_t> k1Values(kNumRows);
  std::iota(k1Values.begin(), k1Values.end(), 0);
  std::vector<std::string> k2Values;
  k2Values.reserve(kNumRows);
  for (int i = 0; i < kNumRows; ++i) {
    k2Values.push_back(fmt::format("s{}", i));
  }
  std::vector<double> vValues(kNumRows, 3.14);

  auto input = makeRowVector(
      {makeFlatVector<int64_t>(k1Values),
       makeFlatVector<std::string>(k2Values),
       makeFlatVector<double>(vValues)});
  connector_->appendData("multi_key", input);

  EXPECT_EQ(table->data().size(), table->dataBucketIds().size());

  const auto expected = referenceBucketIds(input, {0, 1}, kNumBuckets);
  for (size_t i = 0; i < table->data().size(); ++i) {
    const auto firstKey =
        table->data()[i]->childAt(0)->as<SimpleVector<int64_t>>()->valueAt(0);
    EXPECT_EQ(expected[firstKey], table->dataBucketIds()[i]);
  }
}

} // namespace
} // namespace facebook::axiom::connector
