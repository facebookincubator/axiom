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

#include "axiom/cli/CatalogProperties.h"

#include <filesystem>
#include <fstream>

#include <fmt/core.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TempDirectoryPath.h"

namespace axiom::sql {
namespace {

class CatalogPropertiesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    directory_ = facebook::velox::common::testutil::TempDirectoryPath::create();
    propertiesDirectory_ = std::filesystem::path(directory_->getPath());
    std::filesystem::create_directories(propertiesDirectory_);
  }

  std::filesystem::path writeCatalogFile(
      const std::string& name,
      const std::string& contents) {
    auto filePath = propertiesDirectory_ / fmt::format("{}.properties", name);
    std::ofstream output(filePath);
    output << contents;
    return filePath;
  }

  std::shared_ptr<facebook::velox::common::testutil::TempDirectoryPath>
      directory_;
  std::filesystem::path propertiesDirectory_;
};

TEST_F(CatalogPropertiesTest, load) {
  writeCatalogFile(
      "analytics",
      R"(connector.name = hive
hive_local_data_path = /tmp/hive
hive_local_file_format = parquet
)");
  writeCatalogFile(
      "sandbox",
      R"(# comment
connector.name=test
)");
  std::ofstream(propertiesDirectory_ / "ignored.txt")
      << "connector.name=test\n";

  auto catalogs = loadCatalogProperties(directory_->getPath());

  EXPECT_EQ(catalogs.size(), 2);
  EXPECT_EQ(catalogs[0].catalogName, "analytics");
  EXPECT_EQ(catalogs[0].connectorName, "hive");
  EXPECT_THAT(
      catalogs[0].connectorConfig,
      ::testing::UnorderedElementsAre(
          ::testing::Pair("hive_local_data_path", "/tmp/hive"),
          ::testing::Pair("hive_local_file_format", "parquet")));
  EXPECT_EQ(catalogs[1].catalogName, "sandbox");
  EXPECT_EQ(catalogs[1].connectorName, "test");
  EXPECT_TRUE(catalogs[1].connectorConfig.empty());
}

TEST_F(CatalogPropertiesTest, rejectInvalid) {
  writeCatalogFile("broken", "hive_local_data_path=/tmp/hive\n");
  VELOX_ASSERT_THROW(
      loadCatalogProperties(directory_->getPath()), "connector.name");
}

} // namespace
} // namespace axiom::sql
