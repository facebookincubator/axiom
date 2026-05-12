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

#include "axiom/connectors/ducklake/DuckLakeCatalogClient.h"

#include <duckdb.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fmt/core.h>

#include "velox/common/testutil/TempDirectoryPath.h"

#include "axiom/connectors/ducklake/tests/DuckLakeTestUtils.h"

namespace facebook::axiom::connector::ducklake {
namespace {

using testing::ElementsAre;

class DuckLakeCatalogClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    directory_ = velox::common::testutil::TempDirectoryPath::create();
    dbPath_ = directory_->getPath() + "/metadata.ducklake";

    if (auto error = createDuckLakeTable(directory_->getPath())) {
      if (error->isEnvironmentIssue) {
        GTEST_SKIP() << "DuckLake extension is not available in Axiom-linked "
                     << "DuckDB " << duckdb::DuckDB::LibraryVersion() << ": "
                     << error->message;
      }
      FAIL() << "Failed to create DuckLake test data with DuckDB "
             << duckdb::DuckDB::LibraryVersion() << ": " << error->message;
    }
  }

  std::shared_ptr<DuckLakeCatalogClient> createClient() {
    auto spec = DuckLakeCatalogSpec::parse(fmt::format("ducklake:{}", dbPath_));
    return DuckLakeCatalogClient::create(spec);
  }

  std::shared_ptr<velox::common::testutil::TempDirectoryPath> directory_;
  std::string dbPath_;
};

TEST_F(DuckLakeCatalogClientTest, listSchemaNames) {
  auto client = createClient();
  auto schemas = client->listSchemaNames();
  // "lake" db defaults to "main" schema.
  EXPECT_THAT(schemas, ElementsAre("main"));
}

TEST_F(DuckLakeCatalogClientTest, loadTable) {
  auto client = createClient();
  auto metadata = client->loadTable(SchemaTableName{"main", "numbers"});

  ASSERT_TRUE(metadata.has_value());
  EXPECT_EQ(metadata->name.schema, "main");
  EXPECT_EQ(metadata->name.table, "numbers");

  ASSERT_EQ(metadata->columns.size(), 2);
  EXPECT_EQ(metadata->columns[0].name, "id");
  EXPECT_EQ(metadata->columns[1].name, "name");
  EXPECT_EQ(metadata->columns[0].type->kind(), velox::TypeKind::INTEGER);
  EXPECT_EQ(metadata->columns[1].type->kind(), velox::TypeKind::VARCHAR);

  ASSERT_EQ(metadata->dataFiles.size(), 1);
  EXPECT_EQ(metadata->dataFiles[0].recordCount, 3);
}

TEST_F(DuckLakeCatalogClientTest, loadTableMissing) {
  auto client = createClient();
  auto metadata = client->loadTable(SchemaTableName{"main", "missing_table"});
  EXPECT_FALSE(metadata.has_value());
}

} // namespace
} // namespace facebook::axiom::connector::ducklake
