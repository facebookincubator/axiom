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
#include "velox/common/base/VeloxException.h"
#include "velox/expression/Expr.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace facebook::velox::connector;

namespace facebook::velox::connector {
namespace {

class TestConnectorTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    connector_ = std::make_shared<TestConnector>("connector");
  }

  std::shared_ptr<TestConnector> connector_;
};

TEST_F(TestConnectorTest, ConnectorRegister) {
  registerConnector(connector_);

  auto connector = getConnector("connector");
  EXPECT_EQ(connector_.get(), connector.get());
  EXPECT_EQ(connector->connectorId(), "connector");
  EXPECT_NE(connector->metadata(), nullptr);

  unregisterConnector("connector");
  EXPECT_THROW(getConnector("connector"), VeloxRuntimeError);
}

TEST_F(TestConnectorTest, Table) {
  auto metadata = connector_->metadata();

  auto schema = ROW({{"col1", INTEGER()}, {"col2", VARCHAR()}});
  connector_->addTable("table", schema, 100);
  auto table = metadata->findTable("table");
  EXPECT_NE(table, nullptr);
  EXPECT_EQ(table->name(), "table");
  EXPECT_EQ(table->numRows(), 100);
  EXPECT_EQ(table->columnMap().size(), 2);
  EXPECT_NE(table->columnMap().find("col1"), table->columnMap().end());
  EXPECT_NE(table->columnMap().find("col2"), table->columnMap().end());

  connector_->addTable("noschema");
  table = metadata->findTable("noschema");
  EXPECT_NE(table, nullptr);
  EXPECT_EQ(table->numRows(), 1000);
  EXPECT_EQ(table->columnMap().size(), 0);

  EXPECT_THROW(metadata->findTable("notable"), VeloxUserError);
}

TEST_F(TestConnectorTest, ColumnHandle) {
  auto schema = ROW({{"col1", INTEGER()}, {"col2", VARCHAR()}});
  connector_->addTable("table", schema, 50);

  auto metadata = connector_->metadata();
  auto table = metadata->findTable("table");
  auto& layout = *table->layouts()[0];

  auto columnHandle = metadata->createColumnHandle(layout, "col1");
  EXPECT_NE(columnHandle, nullptr);

  auto testColumnHandle =
      std::dynamic_pointer_cast<const TestColumnHandle>(columnHandle);
  EXPECT_NE(testColumnHandle, nullptr);
  EXPECT_EQ(testColumnHandle->name(), "col1");
  EXPECT_EQ(testColumnHandle->type()->kind(), TypeKind::INTEGER);
}

TEST_F(TestConnectorTest, SplitManager) {
  auto schema = ROW({"col1"}, {INTEGER()});
  connector_->addTable("test_table", schema, 100);

  auto metadata = connector_->metadata();
  auto splitManager = metadata->splitManager();
  EXPECT_NE(splitManager, nullptr);
}

TEST_F(TestConnectorTest, Splits) {
  auto split = std::make_shared<TestSplit>("connector");
  EXPECT_EQ(split->connectorId, "connector");
}

TEST_F(TestConnectorTest, DataSink) {
  auto schema = ROW({"col1"}, {INTEGER()});
  auto dataSink = connector_->createDataSink(
      schema, nullptr, nullptr, CommitStrategy::kNoCommit);
  EXPECT_NE(dataSink, nullptr);

  dataSink->appendData(nullptr);
  EXPECT_TRUE(dataSink->finish());
  EXPECT_TRUE(dataSink->close().empty());
}

TEST_F(TestConnectorTest, DataSource) {
  auto schema = ROW({"col1", "col2"}, {INTEGER(), VARCHAR()});
  connector_->addTable("table", schema, 100);

  auto metadata = connector_->metadata();
  auto table = metadata->findTable("table");
  auto& layout = *table->layouts()[0];

  std::vector<ColumnHandlePtr> columnHandles;
  columnHandles.push_back(metadata->createColumnHandle(layout, "col1"));
  columnHandles.push_back(metadata->createColumnHandle(layout, "col2"));

  auto evaluator =
      std::make_unique<exec::SimpleExpressionEvaluator>(nullptr, nullptr);
  std::vector<core::TypedExprPtr> empty;
  auto tableHandle = metadata->createTableHandle(
      layout, std::move(columnHandles), *evaluator, empty, empty);

  ColumnHandleMap emptyColumns;
  auto dataSource =
      connector_->createDataSource(schema, tableHandle, emptyColumns, nullptr);
  EXPECT_NE(dataSource, nullptr);
  EXPECT_EQ(dataSource->getCompletedBytes(), 0);
  EXPECT_EQ(dataSource->getCompletedRows(), 0);

  auto split = std::make_shared<TestSplit>("connector");
  dataSource->addSplit(split);

  velox::ContinueFuture future;
  auto result = dataSource->next(0, future);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result.value()->childrenSize(), 2);
  EXPECT_EQ(result.value()->childAt(0)->type()->kind(), TypeKind::INTEGER);
  EXPECT_EQ(result.value()->childAt(1)->type()->kind(), TypeKind::VARCHAR);

  result = dataSource->next(0, future);
  EXPECT_FALSE(result.has_value());
}

TEST_F(TestConnectorTest, TestColumnHandleCreation) {
  auto columnHandle = std::make_shared<TestColumnHandle>("col", INTEGER());
  EXPECT_EQ(columnHandle->name(), "col");
  EXPECT_EQ(columnHandle->type()->kind(), TypeKind::INTEGER);
}

TEST_F(TestConnectorTest, TableLayout) {
  auto schema = ROW({"col1", "col2"}, {INTEGER(), VARCHAR()});
  connector_->addTable("table", schema, 300);

  auto metadata = connector_->metadata();
  auto table = metadata->findTable("table");
  auto& layout = *table->layouts()[0];
  EXPECT_EQ(layout.name(), "table");

  const auto& columnMap = table->columnMap();
  EXPECT_NE(columnMap.find("col1"), columnMap.end());
  EXPECT_NE(columnMap.find("col2"), columnMap.end());

  auto col1 = layout.findColumn("col1");
  EXPECT_NE(col1, nullptr);
  EXPECT_EQ(col1->name(), "col1");
  EXPECT_EQ(col1->type()->kind(), TypeKind::INTEGER);

  auto col2 = layout.findColumn("col2");
  EXPECT_NE(col2, nullptr);
  EXPECT_EQ(col2->name(), "col2");
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
