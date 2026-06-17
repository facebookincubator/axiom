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

#include "axiom/connectors/tests/TestTableJson.h"

#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

#include "axiom/connectors/tests/TestConnector.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/type/Type.h"

namespace facebook::axiom::connector {
namespace {

using namespace facebook::velox;

class TestTableJsonTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    connector_ = std::make_shared<TestConnector>("test");
  }

  TablePtr findTable(const std::string& name) {
    return connector_->metadata()->findTable(
        {std::string(TestConnector::kDefaultSchema), name});
  }

  static const ColumnStatistics& columnStats(
      const TablePtr& table,
      const std::string& column) {
    const auto* stats = table->columnMap().at(column)->stats();
    VELOX_CHECK_NOT_NULL(stats);
    return *stats;
  }

  std::shared_ptr<TestConnector> connector_;
};

TEST_F(TestTableJsonTest, load) {
  TestTableJson::load(*connector_, R"({
    "name": "orders",
    "numRows": 1000,
    "columns": [
      {"name": "o_orderkey", "type": "BIGINT", "numDistinct": 1000,
       "min": 1, "max": 6000, "nullFraction": 0.0},
      {"name": "o_orderstatus", "type": "VARCHAR", "numDistinct": 3,
       "nullFraction": 0.5}
    ]
  })");

  auto table = findTable("orders");
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->numRows(), 1000);

  const auto& orderkey = columnStats(table, "o_orderkey");
  EXPECT_EQ(orderkey.numDistinct, 1000);
  EXPECT_EQ(orderkey.min, Variant(static_cast<int64_t>(1)));
  EXPECT_EQ(orderkey.max, Variant(static_cast<int64_t>(6000)));
  EXPECT_TRUE(orderkey.nonNull);
  EXPECT_EQ(orderkey.nullPct, 0);

  const auto& status = columnStats(table, "o_orderstatus");
  EXPECT_EQ(status.numDistinct, 3);
  EXPECT_FALSE(status.nonNull);
  EXPECT_EQ(status.nullPct, 50);
}

TEST_F(TestTableJsonTest, columnWithoutStats) {
  // A column with only name and type leaves every statistic unset, so the
  // optimizer applies its no-stat defaults just as it would in production.
  TestTableJson::load(*connector_, R"({
    "name": "t",
    "numRows": 100,
    "columns": [{"name": "c", "type": "BIGINT"}]
  })");

  const auto& stats = columnStats(findTable("t"), "c");
  EXPECT_FALSE(stats.numDistinct.has_value());
  EXPECT_FALSE(stats.min.has_value());
  EXPECT_FALSE(stats.max.has_value());
  EXPECT_FALSE(stats.nonNull);
  EXPECT_EQ(stats.nullPct, 0);
  EXPECT_EQ(stats.numValues, 0);
}

TEST_F(TestTableJsonTest, distinctCappedToRowCount) {
  // The metastore reports NDV per partition, which can exceed a partition's
  // row count; the loader caps it to satisfy setStats' invariant.
  TestTableJson::load(*connector_, R"({
    "name": "t",
    "numRows": 10,
    "columns": [{"name": "c", "type": "BIGINT", "numDistinct": 100}]
  })");

  EXPECT_EQ(columnStats(findTable("t"), "c").numDistinct, 10);
}

TEST_F(TestTableJsonTest, loadFromConfigGlob) {
  const auto directory = velox::common::testutil::TempDirectoryPath::create();
  const std::filesystem::path path = directory->getPath();
  std::ofstream(path / "a.json")
      << R"({"name": "a", "numRows": 1, "columns": []})";
  std::ofstream(path / "b.json")
      << R"({"name": "b", "numRows": 2, "columns": []})";

  velox::config::ConfigBase config({
      {std::string(TestTableJson::kTables), "*.json"},
      {std::string(TestTableJson::kConfigDir), directory->getPath()},
  });
  TestTableJson::loadFromConfig(*connector_, config);

  EXPECT_NE(findTable("a"), nullptr);
  EXPECT_NE(findTable("b"), nullptr);
}

TEST_F(TestTableJsonTest, loadFromConfigMissingLiteralThrows) {
  velox::config::ConfigBase config({
      {std::string(TestTableJson::kTables), "does_not_exist.json"},
      {std::string(TestTableJson::kConfigDir), "/tmp"},
  });
  VELOX_ASSERT_THROW(
      TestTableJson::loadFromConfig(*connector_, config),
      "Failed to open table JSON file");
}

TEST_F(TestTableJsonTest, noTablesProperty) {
  velox::config::ConfigBase config({});
  // No 'tables' property: nothing is registered and no error is raised.
  TestTableJson::loadFromConfig(*connector_, config);
  EXPECT_TRUE(
      connector_->metadata()
          ->listTableNames(
              /*session=*/nullptr, std::string(TestConnector::kDefaultSchema))
          .empty());
}

} // namespace
} // namespace facebook::axiom::connector
