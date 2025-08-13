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

#include "axiom/optimizer/connectors/tests/TestConnector.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/Expr.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::connector {
namespace {

class TestConnectorTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    connector_ = std::make_shared<TestConnector>(connectorId_);
  }

  std::shared_ptr<TestConnector> connector_;
  std::string connectorId_{"test"};
};

TEST_F(TestConnectorTest, connectorRegister) {
  VELOX_ASSERT_THROW(
      getConnector(connectorId_), "Connector with ID 'test' not registered");
  registerConnector(connector_);

  auto connector = getConnector(connectorId_);
  EXPECT_EQ(connector_.get(), connector.get());
  EXPECT_EQ(connector->connectorId(), connectorId_);
  EXPECT_NE(connector->metadata(), nullptr);

  unregisterConnector(connectorId_);
  VELOX_ASSERT_THROW(
      getConnector(connectorId_), "Connector with ID 'test' not registered");
}

TEST_F(TestConnectorTest, table) {
  auto metadata = connector_->metadata();

  auto schema = ROW({{"a", INTEGER()}, {"b", VARCHAR()}});
  connector_->createTable("table", schema);
  auto table = metadata->findTable("table");
  EXPECT_NE(table, nullptr);
  EXPECT_EQ(table->name(), "table");
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

  connector_->createTable("noschema");
  table = metadata->findTable("noschema");
  EXPECT_NE(table, nullptr);
  EXPECT_EQ(table->numRows(), 0);
  EXPECT_EQ(table->columnMap().size(), 0);

  table = metadata->findTable("notable");
  EXPECT_EQ(table, nullptr);
}

TEST_F(TestConnectorTest, columnHandle) {
  auto schema = ROW({{"a", INTEGER()}, {"b", VARCHAR()}});
  connector_->createTable("table", schema);

  auto metadata = connector_->metadata();
  auto table = metadata->findTable("table");
  auto& layout = *table->layouts()[0];

  auto columnHandle = metadata->createColumnHandle(layout, "a");
  EXPECT_NE(columnHandle, nullptr);

  auto testColumnHandle =
      std::dynamic_pointer_cast<const TestColumnHandle>(columnHandle);
  EXPECT_NE(testColumnHandle, nullptr);
  EXPECT_EQ(testColumnHandle->name(), "a");
  EXPECT_EQ(testColumnHandle->type()->kind(), TypeKind::INTEGER);
}

TEST_F(TestConnectorTest, splitManager) {
  auto schema = ROW({"a"}, {INTEGER()});
  connector_->createTable("test_table", schema);

  auto metadata = connector_->metadata();
  auto splitManager = metadata->splitManager();
  EXPECT_NE(splitManager, nullptr);
}

TEST_F(TestConnectorTest, splits) {
  auto split = std::make_shared<ConnectorSplit>(connectorId_);
  EXPECT_EQ(split->connectorId, connectorId_);
}

TEST_F(TestConnectorTest, dataSink) {
  auto schema = ROW({"a"}, {INTEGER()});
  auto handle = std::make_shared<TestInsertTableHandle>("table");
  auto table = connector_->createTable("table", schema);
  EXPECT_EQ(table->numRows(), 0);

  auto dataSink = connector_->createDataSink(
      schema, handle, nullptr, CommitStrategy::kNoCommit);
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
  auto table = connector_->createTable("table", schema);
  auto& layout = *table->layouts()[0];
  auto metadata = connector_->metadata();

  std::vector<ColumnHandlePtr> columns;
  columns.push_back(metadata->createColumnHandle(layout, "a"));
  columns.push_back(metadata->createColumnHandle(layout, "b"));

  auto evaluator =
      std::make_unique<exec::SimpleExpressionEvaluator>(nullptr, nullptr);
  std::vector<core::TypedExprPtr> empty;
  auto tableHandle = metadata->createTableHandle(
      layout, std::move(columns), *evaluator, empty, empty);

  auto vector1 = makeRowVector(
      {makeFlatVector<int>({0, 1}), makeFlatVector<StringView>({"a", "b"})});
  connector_->appendData("table", vector1);
  auto vector2 = makeRowVector(
      {makeFlatVector<int>({3, 4}), makeFlatVector<StringView>({"d", "e"})});
  connector_->appendData("table", vector2);

  ColumnHandleMap handleMap;
  handleMap.emplace("a", metadata->createColumnHandle(layout, "a"));
  handleMap.emplace("b", metadata->createColumnHandle(layout, "b"));
  auto dataSource = std::make_shared<TestDataSource>(
      schema, std::move(handleMap), table, pool());
  EXPECT_EQ(dataSource->getCompletedRows(), 0);

  auto split = std::make_shared<ConnectorSplit>(connectorId_);
  dataSource->addSplit(split);

  velox::ContinueFuture future;
  auto result = dataSource->next(0, future);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result.value()->size(), 2);
  EXPECT_EQ(dataSource->getCompletedRows(), 2);
  test::assertEqualVectors(vector1, result.value());

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
  auto table = connector_->createTable("table", schema);

  auto& layout = *table->layouts()[0];
  EXPECT_EQ(layout.name(), "table");

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

} // namespace
} // namespace facebook::velox::connector

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
