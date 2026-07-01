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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/cli/Connectors.h"
#include "axiom/cli/SqlQueryRunner.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
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
        std::move(sink), writerOptions, data->rowType());
    writer->write(data);
    writer->close();
  }

  void writeTestParquetFile(const std::string& path) {
    // 'score' carries nulls so null_count statistics are non-zero, and a
    // two-row row-group limit splits the five rows into three row groups
    // (2, 2, 1).
    writeParquetFile(
        path,
        makeRowVector(
            {"id", "name", "score"},
            {makeFlatVector<int64_t>({3, 1, 5, 2, 4}),
             makeFlatVector<std::string>(
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

  // Runs 'sql' and returns its result batches, failing with the query's own
  // error message if it reported one.
  std::vector<RowVectorPtr> run(const std::string& sql) {
    auto result = runner_->run(sql, {});
    VELOX_CHECK(!result.message.has_value(), "{}", *result.message);
    return std::move(result.results);
  }

  // Runs 'sql' with optional run options and returns all of its result batches.
  std::vector<RowVectorPtr> queryAll(
      const std::string& sql,
      const ::axiom::sql::SqlQueryRunner::RunOptions& options = {}) {
    auto result = runner_->run(sql, options);
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
  exec::test::assertEqualResults(
      {makeRowVector(
          {"id", "name", "score"},
          {makeFlatVector<int64_t>({3, 1, 5, 2, 4}),
           makeFlatVector<std::string>(
               {"gamma", "alpha", "epsilon", "beta", "delta"}),
           makeNullableFlatVector<int64_t>(
               {std::nullopt, 100, std::nullopt, 200, 300})})},
      queryAll(fmt::format("SELECT * FROM {}", table())));
}

TEST_F(FileConnectorTest, projection) {
  exec::test::assertEqualResults(
      {makeRowVector(
          {"name"},
          {makeFlatVector<std::string>(
              {"gamma", "alpha", "epsilon", "beta", "delta"})})},
      queryAll(fmt::format("SELECT name FROM {}", table())));
}

TEST_F(FileConnectorTest, streamingMultipleBatches) {
  // Cap the output batch below the row-group size (2 rows) so the connector
  // must subdivide each row group. This pins the scan to the requested output
  // batch size rather than emitting one row group per batch: the five rows come
  // back as five single-row batches, not the file's 2/2/1 row-group layout.
  runner_->sessionConfig().set("execution", "preferred_output_batch_rows", "1");
  runner_->sessionConfig().set("execution", "max_output_batch_rows", "1");

  // A single worker and driver keep the scan output as-is, with no gather stage
  // coalescing the per-batch output.
  ::axiom::sql::SqlQueryRunner::RunOptions options{
      .numWorkers = 1, .numDrivers = 1};
  auto results = queryAll(fmt::format("SELECT * FROM {}", table()), options);

  std::vector<vector_size_t> batchSizes;
  batchSizes.reserve(results.size());
  for (const auto& batch : results) {
    batchSizes.push_back(batch->size());
  }
  EXPECT_THAT(batchSizes, testing::ElementsAre(1, 1, 1, 1, 1));

  exec::test::assertEqualResults(
      {makeRowVector(
          {"id", "name", "score"},
          {makeFlatVector<int64_t>({3, 1, 5, 2, 4}),
           makeFlatVector<std::string>(
               {"gamma", "alpha", "epsilon", "beta", "delta"}),
           makeNullableFlatVector<int64_t>(
               {std::nullopt, 100, std::nullopt, 200, 300})})},
      results);
}

TEST_F(FileConnectorTest, rowGroupsMetadata) {
  // The file holds three row groups (2, 2, 1 rows), so the row_groups table has
  // one row per group with incrementing ids and per-group row counts.
  exec::test::assertEqualResults(
      {makeRowVector(
          {"row_group_id", "num_rows"},
          {makeFlatVector<int64_t>({0, 1, 2}),
           makeFlatVector<int64_t>({2, 2, 1})})},
      queryAll(
          fmt::format(
              "SELECT row_group_id, num_rows FROM {}",
              metadataTable("row_groups"))));
}

TEST_F(FileConnectorTest, columnChunksMetadata) {
  // One row per column chunk: three columns in each of the three row groups.
  // num_values tracks each group's row count (2, 2, 1).
  exec::test::assertEqualResults(
      {makeRowVector(
          {"row_group_id", "column_id", "name", "num_values"},
          {makeFlatVector<int64_t>({0, 0, 0, 1, 1, 1, 2, 2, 2}),
           makeFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}),
           makeFlatVector<std::string>(
               {"id",
                "name",
                "score",
                "id",
                "name",
                "score",
                "id",
                "name",
                "score"}),
           makeFlatVector<int64_t>({2, 2, 2, 2, 2, 2, 1, 1, 1})})},
      queryAll(
          fmt::format(
              "SELECT row_group_id, column_id, name, num_values FROM {}",
              metadataTable("column_chunks"))));
}

TEST_F(FileConnectorTest, columnChunksStatistics) {
  // Per-row-group statistics from the Parquet column-chunk metadata. 'score'
  // has one null in each of the first two row groups, so null_count is
  // non-zero there.
  exec::test::assertEqualResults(
      {makeRowVector(
          {"min", "max", "null_count"},
          {makeFlatVector<std::string>(
               {"1", "alpha", "100", "2", "beta", "200", "4", "delta", "300"}),
           makeFlatVector<std::string>(
               {"3",
                "gamma",
                "100",
                "5",
                "epsilon",
                "200",
                "4",
                "delta",
                "300"}),
           makeFlatVector<int64_t>({0, 0, 1, 0, 0, 1, 0, 0, 0})})},
      queryAll(
          fmt::format(
              "SELECT min, max, null_count FROM {}",
              metadataTable("column_chunks"))));
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
  assertDescribeTable(
      table(), ROW({"id", "name", "score"}, {BIGINT(), VARCHAR(), BIGINT()}));

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

TEST_F(FileConnectorTest, columnChunksNestedSchema) {
  // Struct, array, and map columns each flatten to one leaf column chunk per
  // leaf field, labeled by its physical path_in_schema. Arrays and maps carry
  // the synthetic "list"/"key_value" group levels, so the chunk for 'tags' is
  // "tags.list.element" rather than the logical "tags.element".
  auto nestedPath = fmt::format("{}/nested.parquet", tempDir_->getPath());
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
                "tags.list.element",
                "lookup.key_value.key",
                "lookup.key_value.value"})}));

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
}

TEST_F(FileConnectorTest, columnChunksDeeplyNestedSchema) {
  // Leaves under an array-of-struct and a map-of-struct are labeled by their
  // physical path_in_schema, recursing through the "list"/"key_value" group
  // levels and the element/key/value segments into the struct field names.
  auto path = fmt::format("{}/deep.parquet", tempDir_->getPath());

  // events: ARRAY<ROW(kind VARCHAR, count BIGINT)>
  auto eventRows = makeRowVector(
      {"kind", "count"},
      {makeFlatVector<StringView>({"click", "view"}),
       makeFlatVector<int64_t>({1, 2})});
  auto events = makeArrayVector({0, 1}, eventRows);

  // by_region: MAP<VARCHAR, ROW(total BIGINT)>
  auto regionValues =
      makeRowVector({"total"}, {makeFlatVector<int64_t>({10, 20})});
  auto byRegion = makeMapVector(
      {0, 1}, makeFlatVector<StringView>({"us", "eu"}), regionValues);

  writeParquetFile(
      path,
      makeRowVector(
          {"id", "events", "by_region"},
          {makeFlatVector<int64_t>({1, 2}), events, byRegion}));

  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT column_id, name FROM file.\"parquet\".\"{}$column_chunks\" "
              "ORDER BY column_id",
              path)),
      makeRowVector(
          {"column_id", "name"},
          {makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
           makeFlatVector<StringView>(
               {"id",
                "events.list.element.kind",
                "events.list.element.count",
                "by_region.key_value.key",
                "by_region.key_value.value.total"})}));
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

  exec::test::assertEqualResults(
      {makeRowVector(
          {"id", "name", "score"},
          {makeFlatVector<int64_t>({3, 1, 5, 2, 4}),
           makeFlatVector<std::string>(
               {"gamma", "alpha", "epsilon", "beta", "delta"}),
           makeNullableFlatVector<int64_t>(
               {std::nullopt, 100, std::nullopt, 200, 300})})},
      queryAll(fmt::format("SELECT * FROM file.\"parquet\".\"{}\"", path)));
}

} // namespace
} // namespace facebook::axiom::connector::file

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
