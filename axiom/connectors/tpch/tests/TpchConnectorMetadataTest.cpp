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

#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/expression/Expr.h"

namespace facebook::axiom::connector::tpch {
namespace {

class TpchConnectorMetadataTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto config = std::make_shared<velox::config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});
    connector_ = std::make_unique<velox::connector::tpch::TpchConnector>(
        velox::connector::tpch::TpchConnectorFactory::kTpchConnectorName,
        config,
        nullptr);
    metadata_ = std::make_unique<TpchConnectorMetadata>(connector_.get());
  }

  std::unique_ptr<velox::connector::tpch::TpchConnector> connector_;
  std::unique_ptr<TpchConnectorMetadata> metadata_;
};

TEST_F(TpchConnectorMetadataTest, findAllTables) {
  std::vector<std::string> expectedTables = {
      "lineitem",
      "orders",
      "customer",
      "nation",
      "region",
      "part",
      "supplier",
      "partsupp"};
  for (const auto& tableName : expectedTables) {
    auto table = metadata_->findTable(tableName);
    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->name(), fmt::format("tiny.{}", tableName));
  }
}

TEST_F(TpchConnectorMetadataTest, findAllScaleFactors) {
  const std::vector<int> scaleFactors = {
      1, 10, 100, 1000, 10000, 100000, 30, 300, 3000, 30000};

  for (const auto& scaleFactor : scaleFactors) {
    const auto qualifiedName = fmt::format("sf{}.customer", scaleFactor);

    auto table = metadata_->findTable(qualifiedName);
    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->name(), qualifiedName);

    auto tpchTable = std::dynamic_pointer_cast<const TpchTable>(table);
    ASSERT_NE(tpchTable, nullptr);
    EXPECT_DOUBLE_EQ(tpchTable->scaleFactor(), scaleFactor);
  }
}

TEST_F(TpchConnectorMetadataTest, invalidLookups) {
  auto table = metadata_->findTable("invalidtable");
  EXPECT_EQ(table, nullptr);

  table = metadata_->findTable("invalidschema.lineitem");
  EXPECT_EQ(table, nullptr);

  table = metadata_->findTable("sflarge.customer");
  EXPECT_EQ(table, nullptr);

  table = metadata_->findTable("sf000.supplier");
  EXPECT_EQ(table, nullptr);
}

TEST_F(TpchConnectorMetadataTest, verifyTpchSchema) {
  std::vector<std::string> tableNames = {
      "lineitem",
      "orders",
      "customer",
      "nation",
      "region",
      "part",
      "supplier",
      "partsupp"};
  for (const auto& tableName : tableNames) {
    auto table = metadata_->findTable(tableName);
    ASSERT_NE(table, nullptr);
    auto idx = velox::tpch::fromTableName(tableName);
    auto schema = velox::tpch::getTableSchema(idx);

    const auto& columnMap = table->columnMap();
    for (const auto& column : columnMap) {
      ASSERT_NE(schema->findChild(column.first), nullptr);
    }
  }
}

TEST_F(TpchConnectorMetadataTest, createColumnHandle) {
  auto table = metadata_->findTable("lineitem");
  ASSERT_NE(table, nullptr);

  const auto& layouts = table->layouts();
  ASSERT_FALSE(layouts.empty());

  auto columnHandle = metadata_->createColumnHandle(*layouts[0], "orderkey");
  ASSERT_NE(columnHandle, nullptr);

  auto* tpchColumnHandle =
      dynamic_cast<const velox::connector::tpch::TpchColumnHandle*>(
          columnHandle.get());
  ASSERT_NE(tpchColumnHandle, nullptr);
  EXPECT_EQ(tpchColumnHandle->name(), "orderkey");
}

TEST_F(TpchConnectorMetadataTest, createTableHandle) {
  metadata_->initialize();
  auto table = metadata_->findTable("lineitem");
  ASSERT_NE(table, nullptr);
  const auto& layouts = table->layouts();
  ASSERT_EQ(layouts.size(), 1);
  auto* tpchLayout =
      dynamic_cast<const facebook::axiom::connector::tpch::TpchTableLayout*>(
          layouts[0]);

  std::vector<velox::connector::ColumnHandlePtr> columnHandles;
  std::vector<velox::core::TypedExprPtr> empty;
  auto evaluator = std::make_unique<velox::exec::SimpleExpressionEvaluator>(
      nullptr, nullptr);
  auto tableHandle = metadata_->createTableHandle(
      *layouts[0], columnHandles, *evaluator, empty, empty);
  ASSERT_NE(tableHandle, nullptr);

  auto* tpchTableHandle =
      dynamic_cast<const velox::connector::tpch::TpchTableHandle*>(
          tableHandle.get());
  ASSERT_NE(tpchTableHandle, nullptr);

  EXPECT_EQ(tpchTableHandle->getTable(), tpchLayout->getTpchTable());
  EXPECT_DOUBLE_EQ(
      tpchTableHandle->getScaleFactor(), tpchLayout->getScaleFactor());
}

TEST_F(TpchConnectorMetadataTest, splitGeneration) {
  auto table = metadata_->findTable("lineitem");
  ASSERT_NE(table, nullptr);

  const auto& layouts = table->layouts();
  ASSERT_FALSE(layouts.empty());

  auto splitManager = metadata_->splitManager();
  ASSERT_NE(splitManager, nullptr);

  std::vector<velox::connector::ColumnHandlePtr> columnHandles;
  std::vector<velox::core::TypedExprPtr> empty;
  auto evaluator = std::make_unique<velox::exec::SimpleExpressionEvaluator>(
      nullptr, nullptr);
  auto tableHandle = metadata_->createTableHandle(
      *layouts[0], columnHandles, *evaluator, empty, empty);
  ASSERT_NE(tableHandle, nullptr);

  auto partitions = splitManager->listPartitions(tableHandle);
  ASSERT_EQ(partitions.size(), 1);

  auto splitSource = splitManager->getSplitSource(tableHandle, partitions);
  ASSERT_NE(splitSource, nullptr);
  auto splits = splitSource->getSplits(1024 * 1024); // 1MB target
  ASSERT_FALSE(splits.empty());
}
} // namespace
} // namespace facebook::axiom::connector::tpch

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  facebook::velox::memory::MemoryManager::Options options;
  facebook::velox::memory::initializeMemoryManager(options);
  return RUN_ALL_TESTS();
}
