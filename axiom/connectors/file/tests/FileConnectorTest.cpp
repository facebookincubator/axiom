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

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "axiom/cli/Connectors.h"
#include "axiom/cli/SqlQueryRunner.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::axiom::connector::file {
namespace {

using namespace facebook::velox;

constexpr const char* kConnectorId = "file";

// Drives the file connector through the full SQL stack
class FileConnectorTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    velox::filesystems::registerLocalFileSystem();
  }

  void SetUp() override {
    connectors_ = std::make_unique<Connectors>();
    runner_ = std::make_unique<::axiom::sql::SqlQueryRunner>("test_user");
    runner_->initialize([this]() {
      connectors_->registerFileConnector(kConnectorId);
      return std::make_pair(std::string(kConnectorId), std::string("parquet"));
    });

    tempFile_ = exec::test::TempFilePath::create();
    parquetPath_ = tempFile_->getPath() + ".parquet";
    writeTestParquetFile(parquetPath_);
  }

  void TearDown() override {
    runner_.reset();
    // Connectors' destructor unregisters the connector and its metadata.
    connectors_.reset();
    std::remove(parquetPath_.c_str());
  }

  void writeTestParquetFile(const std::string& path) {
    auto rowVector = makeRowVector(
        {"id", "name"},
        {makeFlatVector<int64_t>({1, 2, 3}),
         makeFlatVector<StringView>({"alpha", "beta", "gamma"})});
    auto schema = asRowType(rowVector->type());

    dwio::common::WriterOptions writerOptions;
    writerOptions.memoryPool = rootPool_.get();
    auto sink = dwio::common::FileSink::create(
        fmt::format("file:{}", path), {.pool = rootPool_.get()});
    auto writer = std::make_unique<parquet::Writer>(
        std::move(sink), writerOptions, rootPool_, schema);
    writer->write(rowVector);
    writer->close();
  }

  // Runs a SELECT and returns its single result vector.
  RowVectorPtr query(const std::string& sql) {
    auto result = runner_->run(sql, {});
    VELOX_CHECK(
        !result.message.has_value(), "Query produced no result: {}", sql);
    VELOX_CHECK_EQ(result.results.size(), 1);
    return result.results[0];
  }

  // SQL reference for the data table.
  std::string table() const {
    return fmt::format("file.\"parquet\".\"{}\"", parquetPath_);
  }

  // SQL reference for a $-suffix metadata table.
  std::string metadataTable(const std::string& suffix) const {
    return fmt::format("file.\"parquet\".\"{}${}\"", parquetPath_, suffix);
  }

  std::unique_ptr<Connectors> connectors_;
  std::unique_ptr<::axiom::sql::SqlQueryRunner> runner_;
  std::shared_ptr<exec::test::TempFilePath> tempFile_;
  std::string parquetPath_;
};

TEST_F(FileConnectorTest, fullScan) {
  test::assertEqualVectors(
      query(fmt::format("SELECT * FROM {} ORDER BY id", table())),
      makeRowVector(
          {"id", "name"},
          {makeFlatVector<int64_t>({1, 2, 3}),
           makeFlatVector<StringView>({"alpha", "beta", "gamma"})}));
}

TEST_F(FileConnectorTest, projection) {
  test::assertEqualVectors(
      query(fmt::format("SELECT name FROM {} ORDER BY name", table())),
      makeRowVector(
          {"name"}, {makeFlatVector<StringView>({"alpha", "beta", "gamma"})}));
}

TEST_F(FileConnectorTest, rowGroupsMetadata) {
  // The test file is written as a single row group holding all three rows.
  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT row_group_id, num_rows FROM {}",
              metadataTable("row_groups"))),
      makeRowVector(
          {"row_group_id", "num_rows"},
          {makeFlatVector<int64_t>({0}), makeFlatVector<int64_t>({3})}));
}

TEST_F(FileConnectorTest, columnChunksMetadata) {
  // One row per column chunk: the two columns of the single row group.
  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT column_id, name, num_values FROM {} ORDER BY column_id",
              metadataTable("column_chunks"))),
      makeRowVector(
          {"column_id", "name", "num_values"},
          {makeFlatVector<int64_t>({0, 1}),
           makeFlatVector<StringView>({"id", "name"}),
           makeFlatVector<int64_t>({3, 3})}));
}

TEST_F(FileConnectorTest, columnChunksStatistics) {
  // Column statistics come from the Parquet column-chunk metadata. The test
  // data has no nulls, so null_count is zero for both columns.
  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT min, max, null_count FROM {} ORDER BY column_id",
              metadataTable("column_chunks"))),
      makeRowVector(
          {"min", "max", "null_count"},
          {makeFlatVector<StringView>({"1", "alpha"}),
           makeFlatVector<StringView>({"3", "gamma"}),
           makeFlatVector<int64_t>({0, 0})}));
}

TEST_F(FileConnectorTest, unsupportedMetadataTableFails) {
  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format("SELECT * FROM {}", metadataTable("stripes")), {}),
      "Unsupported metadata table: stripes");
}

} // namespace
} // namespace facebook::axiom::connector::file

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
