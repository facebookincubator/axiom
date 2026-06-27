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
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/parquet/writer/Writer.h"
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

    tempDir_ = velox::common::testutil::TempDirectoryPath::create();
    parquetPath_ = fmt::format("{}/test.parquet", tempDir_->getPath());
    writeTestParquetFile(parquetPath_);
  }

  void TearDown() override {
    runner_.reset();
    // Connectors' destructor unregisters the connector and its metadata.
    connectors_.reset();
  }

  void writeTestParquetFile(const std::string& path) {
    writeParquet(
        path,
        makeRowVector(
            {"id", "name"},
            {makeFlatVector<int64_t>({1, 2, 3}),
             makeFlatVector<StringView>({"alpha", "beta", "gamma"})}));
  }

  // Writes 'rowVector' to 'path' as a single-row-group Parquet file.
  void writeParquet(const std::string& path, const RowVectorPtr& rowVector) {
    auto schema = asRowType(rowVector->type());

    dwio::common::WriterOptions writerOptions;
    writerOptions.memoryPool = rootPool_.get();
    auto sink = dwio::common::FileSink::create(
        fmt::format("file:{}", path), {.pool = rootPool_.get()});
    auto writer = std::make_unique<parquet::Writer>(
        std::move(sink), writerOptions, schema);
    writer->write(rowVector);
    writer->close();
  }

  // Runs 'sql' and returns its result batches, failing with the query's own
  // error message if it reported one.
  std::vector<RowVectorPtr> run(const std::string& sql) {
    auto result = runner_->run(sql, {});
    VELOX_CHECK(!result.message.has_value(), "{}", *result.message);
    return std::move(result.results);
  }

  // Runs a SELECT and returns its single result vector.
  RowVectorPtr query(const std::string& sql) {
    auto results = run(sql);
    VELOX_CHECK_EQ(results.size(), 1);
    return results[0];
  }

  // Total number of result rows across all batches, so an empty result (zero
  // batches or a single empty batch) can be asserted without query()'s
  // single-vector requirement.
  int64_t countResultRows(const std::string& sql) {
    int64_t total = 0;
    for (const auto& batch : run(sql)) {
      total += batch->size();
    }
    return total;
  }

  // SQL reference for the data table.
  std::string table() const {
    return fmt::format("file.\"parquet\".\"{}\"", parquetPath_);
  }

  // SQL reference for a $-suffix metadata table.
  std::string metadataTable(const std::string& suffix) const {
    return fmt::format("file.\"parquet\".\"{}${}\"", parquetPath_, suffix);
  }

  // Runs DESC on 'name' and returns its (column, type) result.
  RowVectorPtr describeTable(const std::string& name) {
    return query(fmt::format("DESC {}", name));
  }

  // Asserts DESC on 'name' reports 'schema' as (column, type) rows in schema
  // order. Expected type strings are derived from 'schema' via
  // Type::toString(), matching how DESC renders them.
  void assertDescribeTable(const std::string& name, const RowTypePtr& schema) {
    std::vector<std::string> types;
    for (const auto& child : schema->children()) {
      types.push_back(child->toString());
    }
    test::assertEqualVectors(
        describeTable(name),
        makeRowVector(
            {"column", "type"},
            {makeFlatVector(schema->names()), makeFlatVector(types)}));
  }

  // Asserts SHOW SCHEMAS via 'sql' returns exactly 'expectedSchema' in the
  // single-column "Schema" result.
  void assertShowSchemas(
      const std::string& sql,
      const std::string& expectedSchema) {
    test::assertEqualVectors(
        query(sql),
        makeRowVector(
            {"Schema"}, {makeFlatVector<std::string>({expectedSchema})}));
  }

  std::unique_ptr<Connectors> connectors_;
  std::unique_ptr<::axiom::sql::SqlQueryRunner> runner_;
  // Temp directory holding all Parquet files; its destructor removes the
  // directory and everything written into it.
  std::shared_ptr<velox::common::testutil::TempDirectoryPath> tempDir_;
  // Path of the default test Parquet file inside tempDir_.
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

TEST_F(FileConnectorTest, showSchemas) {
  // SHOW SCHEMAS lists the registered file-format handlers; only the Parquet
  // handler is registered. The assertion is exact, so no other schema may leak
  // in.
  assertShowSchemas("SHOW SCHEMAS FROM file", "parquet");

  // Without an explicit FROM, SHOW SCHEMAS resolves against the session's
  // default connector ('file').
  assertShowSchemas("SHOW SCHEMAS", "parquet");

  // A LIKE pattern that matches no handler yields zero rows.
  EXPECT_EQ(countResultRows("SHOW SCHEMAS FROM file LIKE 'orc'"), 0);
}

TEST_F(FileConnectorTest, describe) {
  // DESC resolves the file path through the handler and reports the file schema
  // as (column, type) rows in file-schema order.
  assertDescribeTable(table(), ROW({"id", "name"}, {BIGINT(), VARCHAR()}));

  // DESC on a $-suffix metadata table reports that table's fixed metadata
  // schema rather than the data file's schema.
  assertDescribeTable(
      metadataTable("row_groups"),
      ROW({"row_group_id",
           "num_rows",
           "total_byte_size",
           "total_compressed_size"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT()}));
}

TEST_F(FileConnectorTest, describeErrors) {
  // DESC on a schema with no registered handler fails.
  VELOX_ASSERT_THROW(
      runner_->run(fmt::format("DESC file.\"orc\".\"{}\"", parquetPath_), {}),
      "Unsupported file format: orc");

  // DESC on an unknown $-suffix metadata table fails during schema resolution.
  VELOX_ASSERT_THROW(
      runner_->run(fmt::format("DESC {}", metadataTable("stripes")), {}),
      "Unsupported metadata table: stripes");

  // DESC on a path with no underlying file fails when the handler reads the
  // footer.
  VELOX_ASSERT_THROW(
      runner_->run(
          "DESC file.\"parquet\".\"/tmp/axiom_no_such_file.parquet\"", {}),
      "No such file or directory: /tmp/axiom_no_such_file.parquet");
}

TEST_F(FileConnectorTest, unsupportedMetadataTableFails) {
  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format("SELECT * FROM {}", metadataTable("stripes")), {}),
      "Unsupported metadata table: stripes");
}

TEST_F(FileConnectorTest, separatorInFileName) {
  // A '$' in the file name is part of the path, not a metadata-table suffix:
  // the text after it contains a '.', so the name resolves to the data table.
  auto path = fmt::format("{}/foo$bar.parquet", tempDir_->getPath());
  writeTestParquetFile(path);

  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}\" ORDER BY id", path)),
      makeRowVector(
          {"id", "name"},
          {makeFlatVector<int64_t>({1, 2, 3}),
           makeFlatVector<StringView>({"alpha", "beta", "gamma"})}));
}

} // namespace
} // namespace facebook::axiom::connector::file

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
