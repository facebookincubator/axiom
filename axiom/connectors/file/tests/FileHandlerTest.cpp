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

#include <memory>
#include <string>
#include <utility>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/connectors/file/FileHandler.h"
#include "axiom/connectors/file/parquet/ParquetFileHandler.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/Type.h"

namespace facebook::axiom::connector::file {
namespace {

using namespace facebook::velox;

// Exposes the protected addMetadataTable() so its name validation can be tested
// directly without a format-specific handler.
class TestFileHandler : public FileHandler {
 public:
  void registerTable(std::string suffix) {
    addMetadataTable(
        std::move(suffix),
        ROW({"x"}, {BIGINT()}),
        [](const velox::RowTypePtr&,
           const velox::connector::ColumnHandleMap&,
           velox::memory::MemoryPool*) { return nullptr; });
  }

  velox::RowTypePtr resolve(const std::string&, velox::memory::MemoryPool*)
      const override {
    return nullptr;
  }

 protected:
  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr&,
      const velox::RowTypePtr&,
      const velox::connector::ColumnHandleMap&,
      velox::memory::MemoryPool*) const override {
    return nullptr;
  }
};

class FileHandlerTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    registerParquetHandler();
  }
};

TEST_F(FileHandlerTest, validatesMetadataTableName) {
  TestFileHandler handler;

  // '$' is the suffix separator; '.' and '/' appear in file paths; an empty
  // name is meaningless.
  for (const auto* name : {"foo$bar", "foo.bar", "foo/bar", ""}) {
    SCOPED_TRACE(name);
    VELOX_ASSERT_THROW(
        handler.registerTable(name), "Invalid metadata table name");
  }

  ASSERT_NO_THROW(handler.registerTable("row_groups"));
}

TEST_F(FileHandlerTest, registry) {
  // A registered schema is reported as present and listed by schemas(); an
  // unknown one is absent and fails to resolve.
  EXPECT_TRUE(hasHandler("parquet"));
  EXPECT_FALSE(hasHandler("orc"));
  EXPECT_FALSE(hasHandler("default"));
  VELOX_ASSERT_THROW(handler("orc"), "Unsupported file format: orc");

  auto names = schemas();
  EXPECT_THAT(names, ::testing::Contains("parquet"));

  // Re-registering an existing schema fails.
  auto& existing = handler("parquet");
  VELOX_ASSERT_THROW(
      registerHandler("parquet", existing),
      "FileHandler already registered for schema: parquet");
}

} // namespace
} // namespace facebook::axiom::connector::file
