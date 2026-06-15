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

#include <gtest/gtest.h>

#include "axiom/connectors/file/FileConnector.h"
#include "axiom/connectors/file/core/FileConnectorMetadata.h"
#include "axiom/connectors/file/parquet/ParquetFileHandler.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/ConnectorRegistry.h"

namespace facebook::axiom::connector::file {
namespace {

using namespace facebook::velox;

constexpr const char* kConnectorId = "file";

class FileConnectorMetadataApiTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    registerParquetHandler();
  }

  void SetUp() override {
    connector_ = std::make_shared<FileConnector>(kConnectorId);
    velox::connector::ConnectorRegistry::global().insert(
        kConnectorId, connector_);
  }

  void TearDown() override {
    velox::connector::ConnectorRegistry::global().erase(kConnectorId);
  }

  std::shared_ptr<FileConnector> connector_;
};

TEST_F(FileConnectorMetadataApiTest, listSchemaNames) {
  FileConnectorMetadata metadata(connector_.get());
  auto names = metadata.listSchemaNames(nullptr);
  EXPECT_FALSE(names.empty());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "parquet") != names.end());
}

TEST_F(FileConnectorMetadataApiTest, schemaExistsForRegistered) {
  FileConnectorMetadata metadata(connector_.get());
  EXPECT_TRUE(metadata.schemaExists(nullptr, "parquet"));
}

TEST_F(FileConnectorMetadataApiTest, schemaDoesNotExistForUnknown) {
  FileConnectorMetadata metadata(connector_.get());
  EXPECT_FALSE(metadata.schemaExists(nullptr, "orc"));
}

TEST_F(FileConnectorMetadataApiTest, listTableNamesReturnsEmpty) {
  FileConnectorMetadata metadata(connector_.get());
  EXPECT_TRUE(metadata.listTableNames(nullptr, "parquet").empty());
}

TEST_F(FileConnectorMetadataApiTest, splitManagerNotNull) {
  FileConnectorMetadata metadata(connector_.get());
  EXPECT_NE(metadata.splitManager(), nullptr);
}

TEST_F(FileConnectorMetadataApiTest, createDataSinkThrows) {
  VELOX_ASSERT_THROW(
      connector_->createDataSink(nullptr, nullptr, nullptr, {}),
      "FileConnector does not support writes");
}

} // namespace
} // namespace facebook::axiom::connector::file
