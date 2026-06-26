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

#include <folly/ScopeGuard.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "axiom/cli/Connectors.h"
#include "axiom/cli/SqlQueryRunner.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
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

  // Writes 'data' to 'path' as a Parquet file. A positive 'rowsInRowGroup'
  // caps each row group at that many rows; 0 leaves the whole file in one row
  // group.
  void writeParquetFile(
      const std::string& path,
      const RowVectorPtr& data,
      int32_t rowsInRowGroup = 0) {
    dwio::common::WriterOptions writerOptions;
    writerOptions.memoryPool = rootPool_.get();
    if (rowsInRowGroup > 0) {
      writerOptions.flushPolicyFactory = [rowsInRowGroup] {
        return std::make_unique<parquet::DefaultFlushPolicy>(
            rowsInRowGroup, /*bytesInRowGroup=*/128 << 20);
      };
    }
    auto sink = dwio::common::FileSink::create(
        fmt::format("file:{}", path), {.pool = rootPool_.get()});
    auto writer = std::make_unique<parquet::Writer>(
        std::move(sink), writerOptions, rootPool_, asRowType(data->type()));
    writer->write(data);
    writer->close();
  }

  void writeTestParquetFile(const std::string& path) {
    // Rows are unsorted on every column so ORDER BY changes the result;
    // 'score' carries nulls so null_count statistics are non-zero; and a
    // two-row row-group limit splits the five rows into three row groups
    // (2, 2, 1).
    writeParquetFile(
        path,
        makeRowVector(
            {"id", "name", "score"},
            {makeFlatVector<int64_t>({3, 1, 5, 2, 4}),
             makeFlatVector<StringView>(
                 {"gamma", "alpha", "epsilon", "beta", "delta"}),
             makeNullableFlatVector<int64_t>(
                 {std::nullopt, 100, std::nullopt, 200, 300})}),
        /*rowsInRowGroup=*/2);
  }

  // Writes a file with struct, array, and map columns so the row group holds
  // more leaf column chunks than top-level fields, exercising every branch of
  // the leaf-flattening that labels chunks by dotted path.
  void writeNestedParquetFile(const std::string& path) {
    auto address = makeRowVector(
        {"city", "zip"},
        {makeFlatVector<StringView>({"nyc", "sf"}),
         makeFlatVector<int64_t>({10001, 94016})});
    auto tags = makeArrayVector<int32_t>({{1, 2}, {3}});
    auto lookup =
        makeMapVector<StringView, int64_t>({{{"a", 10}}, {{"b", 20}}});
    writeParquetFile(
        path,
        makeRowVector(
            {"id", "address", "tags", "lookup"},
            {makeFlatVector<int64_t>({1, 2}), address, tags, lookup}));
  }

  // Runs a SELECT and returns all of its result batches.
  std::vector<RowVectorPtr> queryAll(
      const std::string& sql,
      const ::axiom::sql::SqlQueryRunner::RunOptions& options = {}) {
    auto result = runner_->run(sql, options);
    VELOX_CHECK(
        !result.message.has_value(), "Query produced no result: {}", sql);
    return result.results;
  }

  // Runs a SELECT that is expected to return a single result vector.
  RowVectorPtr query(const std::string& sql) {
    auto results = queryAll(sql);
    VELOX_CHECK_EQ(results.size(), 1);
    return results[0];
  }

  // Concatenates result batches into a single vector so multi-batch results
  // can be compared against the expected rows regardless of batch boundaries.
  RowVectorPtr concatenate(const std::vector<RowVectorPtr>& batches) {
    VELOX_CHECK(!batches.empty());
    vector_size_t total = 0;
    for (const auto& batch : batches) {
      total += batch->size();
    }
    auto result =
        BaseVector::create<RowVector>(batches[0]->type(), total, pool());
    vector_size_t offset = 0;
    for (const auto& batch : batches) {
      result->copy(batch.get(), offset, 0, batch->size());
      offset += batch->size();
    }
    return result;
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
  // ORDER BY id reorders the unsorted file rows into id order.
  test::assertEqualVectors(
      query(fmt::format("SELECT * FROM {} ORDER BY id", table())),
      makeRowVector(
          {"id", "name", "score"},
          {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
           makeFlatVector<StringView>(
               {"alpha", "beta", "gamma", "delta", "epsilon"}),
           makeNullableFlatVector<int64_t>(
               {100, 200, std::nullopt, 300, std::nullopt})}));
}

TEST_F(FileConnectorTest, projection) {
  // ORDER BY name produces an order distinct from both file order and id order.
  test::assertEqualVectors(
      query(fmt::format("SELECT name FROM {} ORDER BY name", table())),
      makeRowVector(
          {"name"},
          {makeFlatVector<StringView>(
              {"alpha", "beta", "delta", "epsilon", "gamma"})}));
}

TEST_F(FileConnectorTest, streamingMultipleBatches) {
  // Cap the output batch at two rows so the five-row scan is delivered as
  // several batches, exercising the connector's batch-by-batch streaming read.
  runner_->sessionConfig().set("execution", "preferred_output_batch_rows", "2");
  runner_->sessionConfig().set("execution", "max_output_batch_rows", "2");

  // A single worker and driver keep the scan output as-is: no gather stage
  // coalesces the per-batch output. No ORDER BY, so the scan batches flow
  // straight to the result and rows stay in file order.
  ::axiom::sql::SqlQueryRunner::RunOptions options;
  options.numWorkers = 1;
  options.numDrivers = 1;
  auto results = queryAll(fmt::format("SELECT * FROM {}", table()), options);
  EXPECT_GT(results.size(), 1);
  test::assertEqualVectors(
      concatenate(results),
      makeRowVector(
          {"id", "name", "score"},
          {makeFlatVector<int64_t>({3, 1, 5, 2, 4}),
           makeFlatVector<StringView>(
               {"gamma", "alpha", "epsilon", "beta", "delta"}),
           makeNullableFlatVector<int64_t>(
               {std::nullopt, 100, std::nullopt, 200, 300})}));
}

TEST_F(FileConnectorTest, rowGroupsMetadata) {
  // The file holds three row groups (2, 2, 1 rows), so the row_groups table has
  // one row per group with incrementing ids and per-group row counts.
  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT row_group_id, num_rows FROM {} ORDER BY row_group_id",
              metadataTable("row_groups"))),
      makeRowVector(
          {"row_group_id", "num_rows"},
          {makeFlatVector<int64_t>({0, 1, 2}),
           makeFlatVector<int64_t>({2, 2, 1})}));
}

TEST_F(FileConnectorTest, columnChunksMetadata) {
  // One row per column chunk: three columns in each of the three row groups.
  // num_values tracks each group's row count (2, 2, 1).
  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT row_group_id, column_id, name, num_values FROM {} "
              "ORDER BY row_group_id, column_id",
              metadataTable("column_chunks"))),
      makeRowVector(
          {"row_group_id", "column_id", "name", "num_values"},
          {makeFlatVector<int64_t>({0, 0, 0, 1, 1, 1, 2, 2, 2}),
           makeFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}),
           makeFlatVector<StringView>(
               {"id",
                "name",
                "score",
                "id",
                "name",
                "score",
                "id",
                "name",
                "score"}),
           makeFlatVector<int64_t>({2, 2, 2, 2, 2, 2, 1, 1, 1})}));
}

TEST_F(FileConnectorTest, columnChunksStatistics) {
  // Per-row-group statistics from the Parquet column-chunk metadata. 'score'
  // has one null in each of the first two row groups, so null_count is
  // non-zero there.
  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT min, max, null_count FROM {} "
              "ORDER BY row_group_id, column_id",
              metadataTable("column_chunks"))),
      makeRowVector(
          {"min", "max", "null_count"},
          {makeFlatVector<StringView>(
               {"1", "alpha", "100", "2", "beta", "200", "4", "delta", "300"}),
           makeFlatVector<StringView>(
               {"3",
                "gamma",
                "100",
                "5",
                "epsilon",
                "200",
                "4",
                "delta",
                "300"}),
           makeFlatVector<int64_t>({0, 0, 1, 0, 0, 1, 0, 0, 0})}));
}

TEST_F(FileConnectorTest, columnChunksNestedSchema) {
  // Struct, array, and map columns each flatten to one leaf column chunk per
  // leaf field, labeled by its full dotted path so leaves under the same parent
  // stay distinct.
  auto nestedPath = tempFile_->getPath() + ".nested.parquet";
  writeNestedParquetFile(nestedPath);

  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT column_id, name FROM file.\"parquet\".\"{}$column_chunks\" "
              "ORDER BY column_id",
              nestedPath)),
      makeRowVector(
          {"column_id", "name"},
          {makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5}),
           makeFlatVector<StringView>(
               {"id",
                "address.city",
                "address.zip",
                "tags.element",
                "lookup.key",
                "lookup.value"})}));

  // The flattened leaf type is used to decode statistics, so a nested leaf
  // reports typed min/max instead of falling back to none. address.zip holds
  // {10001, 94016} with no nulls.
  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT min, max, null_count FROM "
              "file.\"parquet\".\"{}$column_chunks\" WHERE name = 'address.zip'",
              nestedPath)),
      makeRowVector(
          {"min", "max", "null_count"},
          {makeFlatVector<StringView>({"10001"}),
           makeFlatVector<StringView>({"94016"}),
           makeFlatVector<int64_t>({0})}));

  std::remove(nestedPath.c_str());
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
  auto path = tempFile_->getPath() + "foo$bar.parquet";
  writeTestParquetFile(path);
  SCOPE_EXIT {
    std::remove(path.c_str());
  };

  exec::test::assertEqualResults(
      {query(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}\" ORDER BY id", path))},
      {makeRowVector(
          {"id", "name", "score"},
          {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
           makeFlatVector<StringView>(
               {"alpha", "beta", "gamma", "delta", "epsilon"}),
           makeNullableFlatVector<int64_t>(
               {100, 200, std::nullopt, 300, std::nullopt})})});
}

} // namespace
} // namespace facebook::axiom::connector::file

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
