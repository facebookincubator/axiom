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

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "axiom/connectors/file/FileConnector.h"
#include "axiom/connectors/file/core/FileConnectorMetadata.h"
#include "axiom/connectors/file/parquet/ParquetFileHandler.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::axiom::connector::file {
namespace {

using namespace facebook::velox;

constexpr const char* kConnectorId = "file";

class FileTableTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    velox::filesystems::registerLocalFileSystem();
    dwio::common::LocalFileSink::registerFactory();
    registerParquetHandler();
  }

  void SetUp() override {
    connector_ = std::make_shared<FileConnector>(kConnectorId);
    velox::connector::ConnectorRegistry::global().insert(
        kConnectorId, connector_);

    tempFile_ = exec::test::TempFilePath::create();
    parquetPath_ = tempFile_->getPath() + ".parquet";

    auto ids = makeFlatVector<int64_t>({1, 2, 3});
    auto names = makeFlatVector<StringView>({"a", "b", "c"});
    auto rowVector = makeRowVector({"id", "name"}, {ids, names});
    auto schema = asRowType(rowVector->type());

    dwio::common::WriterOptions writerOptions;
    writerOptions.memoryPool = rootPool_.get();
    auto sink = dwio::common::FileSink::create(
        fmt::format("file:{}", parquetPath_), {.pool = rootPool_.get()});
    auto writer = std::make_unique<parquet::Writer>(
        std::move(sink), writerOptions, schema);
    writer->write(rowVector);
    writer->close();
  }

  void TearDown() override {
    velox::connector::ConnectorRegistry::global().erase(kConnectorId);
    std::remove(parquetPath_.c_str());
  }

  std::shared_ptr<FileConnector> connector_;
  std::shared_ptr<exec::test::TempFilePath> tempFile_;
  std::string parquetPath_;
};

TEST_F(FileTableTest, numRowsReturnsNullopt) {
  FileConnectorMetadata metadata(connector_.get());
  auto table = metadata.findTable({"parquet", parquetPath_});
  EXPECT_EQ(table->numRows(), std::nullopt);
}

TEST_F(FileTableTest, hasOneLayout) {
  FileConnectorMetadata metadata(connector_.get());
  auto table = metadata.findTable({"parquet", parquetPath_});
  EXPECT_EQ(table->layouts().size(), 1);
}

TEST_F(FileTableTest, columnMapMatchesSchema) {
  FileConnectorMetadata metadata(connector_.get());
  auto table = metadata.findTable({"parquet", parquetPath_});

  const auto& columns = table->columnMap();
  EXPECT_EQ(columns.size(), 2);
  EXPECT_TRUE(columns.contains("id"));
  EXPECT_TRUE(columns.contains("name"));
}

TEST_F(FileTableTest, metadataTableColumnMap) {
  FileConnectorMetadata metadata(connector_.get());
  auto table = metadata.findTable({"parquet", parquetPath_ + "$column_chunks"});

  const auto& columns = table->columnMap();
  EXPECT_TRUE(columns.contains("row_group_id"));
  EXPECT_TRUE(columns.contains("column_id"));
  EXPECT_TRUE(columns.contains("name"));
  EXPECT_TRUE(columns.contains("compression"));
  EXPECT_TRUE(columns.contains("encodings"));
  EXPECT_TRUE(columns.contains("compressed_size"));
  EXPECT_TRUE(columns.contains("uncompressed_size"));
  EXPECT_TRUE(columns.contains("num_values"));
  EXPECT_TRUE(columns.contains("min"));
  EXPECT_TRUE(columns.contains("max"));
  EXPECT_TRUE(columns.contains("null_count"));
}

TEST_F(FileTableTest, parseName) {
  struct TestCase {
    std::string tableName;
    std::string expectedPath;
    std::string expectedSuffix;
  };

  const std::vector<TestCase> testCases = {
      // A plain path resolves to the data table (empty suffix).
      {"/data/file.parquet", "/data/file.parquet", ""},
      // A recognized metadata suffix is split off.
      {"/data/file.parquet$column_chunks",
       "/data/file.parquet",
       "column_chunks"},
      // A '$' in a directory name stays part of the path.
      {"/data/$backup/file.parquet", "/data/$backup/file.parquet", ""},
      // A '$' in a file name with an extension stays part of the path.
      {"/tmp/foo$bar.parquet", "/tmp/foo$bar.parquet", ""},
      // Known limitation: a bare token after '$' is taken as a suffix even when
      // the file has no extension, so the name splits instead of resolving to
      // the data table.
      {"/data/foo$bar", "/data/foo", "bar"},
      // An unknown bare-token suffix is still split off so the handler can
      // reject it with a clear "unsupported metadata table" error rather than a
      // file-not-found error.
      {"/data/file.parquet$stripes", "/data/file.parquet", "stripes"},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.tableName);
    EXPECT_EQ(
        FileTable::parseName(testCase.tableName),
        std::make_pair(testCase.expectedPath, testCase.expectedSuffix));
  }
}

} // namespace
} // namespace facebook::axiom::connector::file
