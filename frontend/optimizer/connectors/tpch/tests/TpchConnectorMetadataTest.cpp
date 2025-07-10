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

#include "optimizer/connectors/tpch/TpchConnectorMetadata.h" //@manual
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "velox/connectors/tpch/TpchConnector.h"

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::tpch;

class TpchConnectorMetadataTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto config = std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});
    tpchConnector_ =
        std::make_shared<TpchConnector>("test-tpch", config, nullptr);

    metadata_ =
        std::make_unique<TpchConnectorMetadata>(tpchConnector_.get(), 0.01);
  }

  void TearDown() override {
    metadata_.reset();
    tpchConnector_.reset();
  }

  std::shared_ptr<TpchConnector> tpchConnector_;
  std::unique_ptr<TpchConnectorMetadata> metadata_;
};

TEST_F(TpchConnectorMetadataTest, FindAllTables) {
  metadata_->initialize();

  // Test finding all TPC-H tables
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
    ASSERT_NE(table, nullptr) << "Table " << tableName << " should exist";
    EXPECT_EQ(table->name(), tableName);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  facebook::velox::memory::MemoryManagerOptions options;
  facebook::velox::memory::initializeMemoryManager(options);
  return RUN_ALL_TESTS();
}
