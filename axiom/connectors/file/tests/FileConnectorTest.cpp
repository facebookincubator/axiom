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

#include <filesystem>
#include <fstream>

#include <folly/init/Init.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/cli/Connectors.h"
#include "axiom/cli/SqlQueryRunner.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/common/file/tests/FaultyFileSystemOperations.h"
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
    velox::tests::utils::registerFaultyFileSystem();
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

  // The standard test data: three scalar columns plus a struct, an array, a
  // map, and an array-of-struct, so a single fixture exercises every leaf kind
  // (scalar, ROW child, ARRAY element, MAP key/value, and recursion through an
  // ARRAY into a ROW). 'score' carries nulls so null_count statistics are
  // non-zero. Each array and map holds exactly one element per row, so every
  // leaf column chunk has the same value count as its row group's row count.
  RowVectorPtr testData() {
    auto address = makeRowVector(
        {"city", "zip"},
        {makeFlatVector<StringView>({"nyc", "sf", "la", "dc", "atx"}),
         makeFlatVector<int64_t>({10001, 94016, 90001, 20001, 73301})});
    auto tags = makeArrayVector<int32_t>({{1}, {2}, {3}, {4}, {5}});
    auto lookup = makeMapVector<StringView, int64_t>(
        {{{"a", 10}}, {{"b", 20}}, {{"c", 30}}, {{"d", 40}}, {{"e", 50}}});
    auto eventElements = makeRowVector(
        {"kind", "count"},
        {makeFlatVector<StringView>(
             {"click", "view", "buy", "scroll", "hover"}),
         makeFlatVector<int64_t>({1, 2, 3, 4, 5})});
    auto events = makeArrayVector({0, 1, 2, 3, 4}, eventElements);
    return makeRowVector(
        {"id", "name", "score", "address", "tags", "lookup", "events"},
        {makeFlatVector<int64_t>({3, 1, 5, 2, 4}),
         makeFlatVector<std::string>(
             {"gamma", "alpha", "epsilon", "beta", "delta"}),
         makeNullableFlatVector<int64_t>(
             {std::nullopt, 100, std::nullopt, 200, 300}),
         address,
         tags,
         lookup,
         events});
  }

  // The schema of testData(), used to assert DESC on the data table.
  static RowTypePtr testSchema() {
    return ROW(
        {"id", "name", "score", "address", "tags", "lookup", "events"},
        {BIGINT(),
         VARCHAR(),
         BIGINT(),
         ROW({"city", "zip"}, {VARCHAR(), BIGINT()}),
         ARRAY(INTEGER()),
         MAP(VARCHAR(), BIGINT()),
         ARRAY(ROW({"kind", "count"}, {VARCHAR(), BIGINT()}))});
  }

  void writeTestParquetFile(const std::string& path) {
    // A two-row row-group limit splits the five rows into three row groups
    // (2, 2, 1).
    writeParquetFile(path, testData(), /*rowsInRowGroup=*/2);
  }

  // Writes a single-row-group file with an (id, name) schema. Used to populate
  // a directory with several files sharing one schema.
  void writeRows(
      const std::string& path,
      const std::vector<int64_t>& ids,
      const std::vector<StringView>& names) {
    writeParquetFile(
        path,
        makeRowVector(
            {"id", "name"},
            {makeFlatVector<int64_t>(ids), makeFlatVector<StringView>(names)}));
  }

  // Fails with the query's own error message if it reported one.
  std::vector<RowVectorPtr> queryAll(
      const std::string& sql,
      const ::axiom::sql::SqlQueryRunner::RunOptions& options = {}) {
    auto result = runner_->run(sql, options);
    VELOX_CHECK(!result.message.has_value(), "{}", *result.message);
    return std::move(result.results);
  }

  // Runs a SELECT and returns its single result vector.
  RowVectorPtr query(const std::string& sql) {
    auto results = queryAll(sql);
    VELOX_CHECK_EQ(results.size(), 1);
    return results[0];
  }

  // Total number of result rows across all batches, so an empty result (zero
  // batches or a single empty batch) can be asserted without query()'s
  // single-vector requirement.
  int64_t countResultRows(const std::string& sql) {
    int64_t total = 0;
    for (const auto& batch : queryAll(sql)) {
      total += batch->size();
    }
    return total;
  }

  // SQL reference for the data table at 'path', defaulting to the standard
  // fixture.
  std::string table(std::optional<std::string_view> path = std::nullopt) const {
    return fmt::format("file.\"parquet\".\"{}\"", path.value_or(parquetPath_));
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
      {testData()}, queryAll(fmt::format("SELECT * FROM {}", table())));
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

  // Every batch is a single row; the five-row result assertion below pins the
  // total.
  for (const auto& batch : results) {
    EXPECT_EQ(batch->size(), 1);
  }

  exec::test::assertEqualResults({testData()}, results);
}

TEST_F(FileConnectorTest, rowGroupsMetadata) {
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
  // One row per leaf column chunk. The struct, array, map, and array-of-struct
  // columns each flatten to one chunk per leaf, identified by its physical
  // path_in_schema as ordered segments (arrays carry the "list"/"element"
  // levels, maps carry "key_value"/"key"/"value"). Three row groups (2, 2, 1
  // rows) with one element per array/map entry give every leaf a num_values
  // equal to its row group's row count.
  const std::vector<std::vector<std::string>> leafPaths = {
      {"id"},
      {"name"},
      {"score"},
      {"address", "city"},
      {"address", "zip"},
      {"tags", "list", "element"},
      {"lookup", "key_value", "key"},
      {"lookup", "key_value", "value"},
      {"events", "list", "element", "kind"},
      {"events", "list", "element", "count"}};
  const std::vector<int64_t> rowGroupRowCounts = {2, 2, 1};

  std::vector<int64_t> rowGroupIds;
  std::vector<int64_t> columnIds;
  std::vector<std::vector<std::string>> paths;
  std::vector<int64_t> numValues;
  for (size_t rg = 0; rg < rowGroupRowCounts.size(); ++rg) {
    for (size_t col = 0; col < leafPaths.size(); ++col) {
      rowGroupIds.push_back(static_cast<int64_t>(rg));
      columnIds.push_back(static_cast<int64_t>(col));
      paths.push_back(leafPaths[col]);
      numValues.push_back(rowGroupRowCounts[rg]);
    }
  }

  exec::test::assertEqualResults(
      {makeRowVector(
          {"row_group_id", "column_id", "path", "num_values"},
          {makeFlatVector<int64_t>(rowGroupIds),
           makeFlatVector<int64_t>(columnIds),
           makeArrayVector<std::string>(paths),
           makeFlatVector<int64_t>(numValues)})},
      queryAll(
          fmt::format(
              "SELECT row_group_id, column_id, path, num_values FROM {}",
              metadataTable("column_chunks"))));
}

TEST_F(FileConnectorTest, columnChunksStatistics) {
  // Per-row-group statistics decoded from the Parquet column-chunk metadata,
  // including the nested struct leaves address.city and address.zip: a nested
  // leaf reports its typed min/max instead of falling back to no statistics.
  // 'score' has one null in each of the first two row groups. Restricted to the
  // scalar and struct leaves (column_id < 5), whose values are fully determined
  // by the fixture (array and map leaf statistics are writer-dependent, so they
  // are not asserted here).
  const std::vector<std::vector<std::string>> scalarAndStructPaths = {
      {"id"}, {"name"}, {"score"}, {"address", "city"}, {"address", "zip"}};
  std::vector<std::vector<std::string>> paths;
  for (int rg = 0; rg < 3; ++rg) {
    paths.insert(
        paths.end(), scalarAndStructPaths.begin(), scalarAndStructPaths.end());
  }

  exec::test::assertEqualResults(
      {makeRowVector(
          {"path", "min", "max", "null_count"},
          {makeArrayVector<std::string>(paths),
           makeFlatVector<std::string>(
               {"1",
                "alpha",
                "100",
                "nyc",
                "10001",
                "2",
                "beta",
                "200",
                "dc",
                "20001",
                "4",
                "delta",
                "300",
                "atx",
                "73301"}),
           makeFlatVector<std::string>(
               {"3",
                "gamma",
                "100",
                "sf",
                "94016",
                "5",
                "epsilon",
                "200",
                "la",
                "90001",
                "4",
                "delta",
                "300",
                "atx",
                "73301"}),
           makeFlatVector<int64_t>(
               {0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0})})},
      queryAll(
          fmt::format(
              "SELECT path, min, max, null_count FROM {} WHERE column_id < 5",
              metadataTable("column_chunks"))));
}

TEST_F(FileConnectorTest, showSchemas) {
  // Only the Parquet handler is registered.
  assertShowSchemas("SHOW SCHEMAS FROM file", "parquet");

  // Without an explicit FROM, SHOW SCHEMAS resolves against the session's
  // default connector ('file').
  assertShowSchemas("SHOW SCHEMAS", "parquet");

  // A LIKE pattern that matches no handler yields zero rows.
  EXPECT_EQ(countResultRows("SHOW SCHEMAS FROM file LIKE 'orc'"), 0);
}

TEST_F(FileConnectorTest, describe) {
  // DESC resolves the file path through the handler and reports the file schema
  // as (column, type) rows in file-schema order, including the nested columns.
  assertDescribeTable(table(), testSchema());

  // A $-suffix metadata table reports that table's fixed metadata schema, not
  // the data file's schema.
  assertDescribeTable(
      metadataTable("row_groups"),
      ROW({"file_path",
           "row_group_id",
           "num_rows",
           "total_byte_size",
           "total_compressed_size"},
          {VARCHAR(), BIGINT(), BIGINT(), BIGINT(), BIGINT()}));
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

TEST_F(FileConnectorTest, multipleFilesInDirectory) {
  // A trailing-slash path is a directory; the connector reads every Parquet
  // file it contains as one logical table.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeRows(
      fmt::format("{}/a.parquet", dir->getPath()), {1, 3}, {"alpha", "gamma"});
  writeRows(
      fmt::format("{}/b.parquet", dir->getPath()), {2, 4}, {"beta", "delta"});

  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}/\" ORDER BY id",
              dir->getPath())),
      makeRowVector(
          {"id", "name"},
          {makeFlatVector<int64_t>({1, 2, 3, 4}),
           makeFlatVector<StringView>({"alpha", "beta", "gamma", "delta"})}));
}

TEST_F(FileConnectorTest, rowGroupsAcrossFilesInDirectory) {
  // Metadata over a directory emits rows for every file. Each file's
  // row_group_id restarts at 0 (both files here are a single row group), so
  // file_path is what keeps rows from different files distinct. file_path is
  // the name relative to the queried directory, not the full path.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeRows(
      fmt::format("{}/a.parquet", dir->getPath()), {1, 2}, {"alpha", "beta"});
  writeRows(
      fmt::format("{}/b.parquet", dir->getPath()), {3, 4, 5}, {"c", "d", "e"});

  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT file_path, row_group_id, num_rows "
              "FROM file.\"parquet\".\"{}/$row_groups\" ORDER BY file_path",
              dir->getPath())),
      makeRowVector(
          {"file_path", "row_group_id", "num_rows"},
          {makeFlatVector<std::string>({"a.parquet", "b.parquet"}),
           makeFlatVector<int64_t>({0, 0}),
           makeFlatVector<int64_t>({2, 3})}));
}

TEST_F(FileConnectorTest, columnChunksAcrossFilesInDirectory) {
  // column_chunks over a directory also restarts row_group_id at 0 per file.
  // file_path keeps each file's chunks distinct, so keying on
  // (file_path, row_group_id) does not mix metadata across files. file_path is
  // the name relative to the queried directory, not the full path.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeRows(
      fmt::format("{}/a.parquet", dir->getPath()), {1, 2}, {"alpha", "beta"});
  writeRows(fmt::format("{}/b.parquet", dir->getPath()), {3, 4}, {"c", "d"});

  test::assertEqualVectors(
      query(
          fmt::format(
              "SELECT file_path, row_group_id, column_id, path "
              "FROM file.\"parquet\".\"{}/$column_chunks\" "
              "ORDER BY file_path, column_id",
              dir->getPath())),
      makeRowVector(
          {"file_path", "row_group_id", "column_id", "path"},
          {makeFlatVector<std::string>(
               {"a.parquet", "a.parquet", "b.parquet", "b.parquet"}),
           makeFlatVector<int64_t>({0, 0, 0, 0}),
           makeFlatVector<int64_t>({0, 1, 0, 1}),
           makeArrayVector<std::string>(
               {{"id"}, {"name"}, {"id"}, {"name"}})}));
}

TEST_F(FileConnectorTest, emptyDirectoryFails) {
  // A directory with no data files has nothing to scan.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format("SELECT * FROM file.\"parquet\".\"{}/\"", dir->getPath()),
          {}),
      "No data files found in directory");
}

TEST_F(FileConnectorTest, directoryReadsExtensionlessFilesAndSkipsMarkers) {
  // Data-lake directories often hold extensionless part files alongside writer
  // marker files (e.g. _SUCCESS, .crc). The scan reads every file regardless of
  // extension but skips names beginning with '_' or '.'.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeRows(
      fmt::format("{}/part-00000", dir->getPath()), {1, 2}, {"alpha", "beta"});
  writeRows(fmt::format("{}/part-00001", dir->getPath()), {3}, {"gamma"});

  // Marker files with non-Parquet content: reading them as data would throw, so
  // a passing test proves they are skipped.
  for (const auto* marker : {"_SUCCESS", ".part-00000.crc"}) {
    std::ofstream{fmt::format("{}/{}", dir->getPath(), marker)}
        << "not parquet";
  }

  exec::test::assertEqualResults(
      {makeRowVector(
          {"id", "name"},
          {makeFlatVector<int64_t>({1, 2, 3}),
           makeFlatVector<std::string>({"alpha", "beta", "gamma"})})},
      queryAll(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}/\"", dir->getPath())));
}

TEST_F(FileConnectorTest, directorySkipsNonDataEntries) {
  // The '_'/'.' name rule does not cover a visible non-Parquet sidecar file or
  // a nested partition directory. The sidecar is skipped by the Parquet magic
  // check (it would otherwise fail the reader); the subdirectory is skipped as
  // a directory during listing. The subdirectory holds its own Parquet file to
  // confirm listing does not recurse into it.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeRows(
      fmt::format("{}/a.parquet", dir->getPath()), {1, 2}, {"alpha", "beta"});

  // Visible non-Parquet sidecar (no '_'/'.' prefix to hide it).
  std::ofstream{fmt::format("{}/manifest.json", dir->getPath())}
      << "{\"not\": \"parquet\"}";

  // Nested directory with its own Parquet file; its rows must not appear.
  const auto subDir = fmt::format("{}/nested", dir->getPath());
  std::filesystem::create_directory(subDir);
  writeRows(fmt::format("{}/b.parquet", subDir), {3}, {"gamma"});

  exec::test::assertEqualResults(
      {makeRowVector(
          {"id", "name"},
          {makeFlatVector<int64_t>({1, 2}),
           makeFlatVector<std::string>({"alpha", "beta"})})},
      queryAll(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}/\"", dir->getPath())));
}

TEST_F(FileConnectorTest, directoryReadErrorPropagates) {
  // A genuine I/O error while probing a directory entry must propagate, not be
  // swallowed as "not a data file" — otherwise the file is silently dropped and
  // the query returns incomplete results with no error. Inject a read failure
  // on one file's magic-bytes probe and confirm the query fails with it.
  auto dir = velox::common::testutil::TempDirectoryPath::create(
      /*enableFaultInjection=*/true);
  writeRows(
      fmt::format("{}/a.parquet", dir->getDelegatePath()), {1}, {"alpha"});
  writeRows(fmt::format("{}/b.parquet", dir->getDelegatePath()), {2}, {"beta"});

  const auto faultyB = fmt::format("{}/b.parquet", dir->getPath());
  auto faultyFs = velox::tests::utils::faultyFileSystem();
  faultyFs->setFileInjectionHook(
      [faultyB](velox::tests::utils::FaultFileOperation* op) {
        if (op->path == faultyB) {
          VELOX_FAIL("Injected read failure");
        }
      });

  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}/\" ORDER BY id",
              dir->getPath()),
          {}),
      "Injected read failure");

  faultyFs->clearFileFaultInjections();
}

TEST_F(FileConnectorTest, nonParquetFileFails) {
  // Without extension filtering, a non-Parquet file reaches the reader. It must
  // fail with a clear error naming the file rather than a cryptic reader error.
  auto path = fmt::format("{}/data.bin", tempDir_->getPath());
  std::ofstream{path} << "this is not a parquet file";
  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format("SELECT * FROM file.\"parquet\".\"{}\"", path), {}),
      "Not a Parquet file");
}

TEST_F(FileConnectorTest, directorySkipsFileWithoutFooterMagic) {
  // A file that begins with the "PAR1" magic but lacks the trailing footer
  // magic is not a valid Parquet file. The directory-listing filter checks both
  // the header and the footer, so it skips this file rather than treating it as
  // data and failing the reader.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeRows(
      fmt::format("{}/a.parquet", dir->getPath()), {1, 2}, {"alpha", "beta"});
  // Header-only "Parquet-looking" file: starts with PAR1, but no footer magic.
  std::ofstream{fmt::format("{}/b.parquet", dir->getPath())}
      << "PAR1 header only, missing the trailing magic";

  exec::test::assertEqualResults(
      {makeRowVector(
          {"id", "name"},
          {makeFlatVector<int64_t>({1, 2}),
           makeFlatVector<std::string>({"alpha", "beta"})})},
      queryAll(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}/\"", dir->getPath())));
}

TEST_F(FileConnectorTest, mismatchedSchemaInDirectoryFails) {
  // The table schema is taken from the first file (a.parquet). A later file
  // whose column types differ must fail clearly rather than misreading it as
  // the table's type.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeRows(fmt::format("{}/a.parquet", dir->getPath()), {1}, {"alpha"});
  // b.parquet shares the column names but 'name' is BIGINT, not VARCHAR.
  writeParquetFile(
      fmt::format("{}/b.parquet", dir->getPath()),
      makeRowVector(
          {"id", "name"},
          {makeFlatVector<int64_t>({2}), makeFlatVector<int64_t>({99})}));

  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}/\" ORDER BY id",
              dir->getPath()),
          {}),
      "does not match the table schema");
}

TEST_F(FileConnectorTest, missingColumnInDirectoryFails) {
  // A directory file missing a column from the table schema (taken from the
  // first file) fails clearly rather than silently reading nulls.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeRows(fmt::format("{}/a.parquet", dir->getPath()), {1}, {"alpha"});
  // b.parquet has only 'id' — it is missing 'name'.
  writeParquetFile(
      fmt::format("{}/b.parquet", dir->getPath()),
      makeRowVector({"id"}, {makeFlatVector<int64_t>({2})}));

  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}/\" ORDER BY id",
              dir->getPath()),
          {}),
      "does not match the table schema");
}

TEST_F(FileConnectorTest, directorySchemaValidatedUpfrontForAllColumns) {
  // Validation runs at plan time against the full table schema, so a directory
  // with a type-mismatched column fails before any rows are read — even when
  // the query does not project that column.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeRows(fmt::format("{}/a.parquet", dir->getPath()), {1}, {"alpha"});
  // b.parquet shares the column names but 'name' is BIGINT, not VARCHAR.
  writeParquetFile(
      fmt::format("{}/b.parquet", dir->getPath()),
      makeRowVector(
          {"id", "name"},
          {makeFlatVector<int64_t>({2}), makeFlatVector<int64_t>({99})}));

  // 'name' is not selected, yet planning still validates it and fails upfront.
  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format(
              "SELECT id FROM file.\"parquet\".\"{}/\" ORDER BY id",
              dir->getPath()),
          {}),
      "does not match the table schema");
}

TEST_F(FileConnectorTest, extraColumnInDirectoryFails) {
  // A directory file with an extra column not in the table schema (taken from
  // the first file) fails clearly rather than silently dropping the column.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeRows(fmt::format("{}/a.parquet", dir->getPath()), {1}, {"alpha"});
  // b.parquet has an additional 'score' column.
  writeParquetFile(
      fmt::format("{}/b.parquet", dir->getPath()),
      makeRowVector(
          {"id", "name", "score"},
          {makeFlatVector<int64_t>({2}),
           makeFlatVector<StringView>({"beta"}),
           makeFlatVector<int64_t>({99})}));

  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}/\" ORDER BY id",
              dir->getPath()),
          {}),
      "does not match the table schema");
}

TEST_F(FileConnectorTest, renamedNestedFieldInDirectoryFails) {
  // A directory file whose nested struct field is renamed must fail: the field
  // types are equivalent but the names differ, so reading it under the table
  // schema (taken from the first file) would misread the field by name.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeParquetFile(
      fmt::format("{}/a.parquet", dir->getPath()),
      makeRowVector(
          {"id", "address"},
          {makeFlatVector<int64_t>({1}),
           makeRowVector(
               {"city", "zip"},
               {makeFlatVector<StringView>({"nyc"}),
                makeFlatVector<int64_t>({10001})})}));
  // b.parquet renames the nested 'city' field to 'town'.
  writeParquetFile(
      fmt::format("{}/b.parquet", dir->getPath()),
      makeRowVector(
          {"id", "address"},
          {makeFlatVector<int64_t>({2}),
           makeRowVector(
               {"town", "zip"},
               {makeFlatVector<StringView>({"sf"}),
                makeFlatVector<int64_t>({94016})})}));

  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}/\" ORDER BY id",
              dir->getPath()),
          {}),
      "does not match the table schema");
}

TEST_F(FileConnectorTest, reorderedColumnsInDirectoryFails) {
  // The table schema, including column order, is taken from the first file.
  // Schema equality is positional, so a later file with the same columns in a
  // different order is rejected upfront rather than read under a mismatched
  // order.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  writeRows(fmt::format("{}/a.parquet", dir->getPath()), {1}, {"alpha"});
  // b.parquet has the same columns as a.parquet but in (name, id) order.
  writeParquetFile(
      fmt::format("{}/b.parquet", dir->getPath()),
      makeRowVector(
          {"name", "id"},
          {makeFlatVector<StringView>({"beta"}),
           makeFlatVector<int64_t>({2})}));

  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}/\" ORDER BY id",
              dir->getPath()),
          {}),
      "does not match the table schema");
}

TEST_F(FileConnectorTest, unsupportedMetadataTableFails) {
  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format("SELECT * FROM {}", metadataTable("stripes")), {}),
      "Unsupported metadata table: stripes");
}

TEST_F(FileConnectorTest, unsupportedMetadataTableOnDirectoryFails) {
  // The metadata suffix is validated before the path is probed, so an
  // unsupported suffix on an empty directory fails with "Unsupported metadata
  // table" — not with the "No data files found in directory" listing error that
  // probing the empty directory would otherwise raise first.
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  VELOX_ASSERT_THROW(
      runner_->run(
          fmt::format(
              "SELECT * FROM file.\"parquet\".\"{}/$stripes\"", dir->getPath()),
          {}),
      "Unsupported metadata table: stripes");
}

TEST_F(FileConnectorTest, separatorInFileName) {
  // A '$' in the file name is part of the path, not a metadata-table suffix:
  // the text after it contains a '.', so the name resolves to the data table.
  auto path = fmt::format("{}/foo$bar.parquet", tempDir_->getPath());
  writeTestParquetFile(path);

  exec::test::assertEqualResults(
      {testData()}, queryAll(fmt::format("SELECT * FROM {}", table(path))));
}

} // namespace
} // namespace facebook::axiom::connector::file

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
