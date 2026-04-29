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

#include <duckdb.hpp>

#include <filesystem>
#include <optional>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/cli/Connectors.h"
#include "axiom/cli/SqlQueryRunner.h"
#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace facebook::axiom::connector::ducklake {
namespace {

std::optional<std::string> runDuckDb(
    duckdb::Connection& connection,
    std::string_view sql) {
  auto result = connection.Query(std::string{sql});
  if (result->HasError()) {
    return result->GetError();
  }
  return std::nullopt;
}

std::string quoteSqlString(std::string_view value) {
  std::string result{"'"};
  result.reserve(value.size() + 2);
  for (const auto c : value) {
    if (c == '\'') {
      result += "''";
    } else {
      result.push_back(c);
    }
  }
  result.push_back('\'');
  return result;
}

std::string quotePath(const std::filesystem::path& path) {
  return quoteSqlString(path.string());
}

void writeParquetFile(
    const std::filesystem::path& path,
    const RowVectorPtr& data,
    memory::MemoryPool* pool) {
  if (!dwio::common::hasWriterFactory(dwio::common::FileFormat::PARQUET)) {
    parquet::registerParquetWriterFactory();
  }

  auto options = std::make_shared<parquet::WriterOptions>();
  options->schema = data->type();
  options->memoryPool = pool;

  auto writeFile = std::make_unique<LocalWriteFile>(path.string(), true, false);
  auto sink = std::make_unique<dwio::common::WriteFileSink>(
      std::move(writeFile), path.string());
  auto writer =
      dwio::common::getWriterFactory(dwio::common::FileFormat::PARQUET)
          ->createWriter(std::move(sink), options);
  writer->write(data);
  writer->close();
}

std::optional<std::string> createDuckLakeTable(
    const std::filesystem::path& directory,
    const RowVectorPtr& data,
    memory::MemoryPool* pool) {
  const auto catalogPath = directory / "metadata.ducklake";
  const auto dataDirectory = directory / "metadata.ducklake.files";
  const auto tableDirectory = dataDirectory / "main" / "numbers";
  const auto dataFile =
      tableDirectory / "ducklake-00000000-0000-0000-0000-000000000001.parquet";
  std::filesystem::create_directories(tableDirectory);
  writeParquetFile(dataFile, data, pool);
  const auto dataFileSize = std::filesystem::file_size(dataFile);

  duckdb::DuckDB db(catalogPath.string());
  duckdb::Connection connection{db};

  std::vector<std::string> sqls{
      "CREATE TABLE ducklake_metadata("
      "\"key\" VARCHAR, value VARCHAR, scope VARCHAR, scope_id BIGINT)",
      "CREATE TABLE ducklake_snapshot("
      "snapshot_id BIGINT, snapshot_time TIMESTAMP WITH TIME ZONE, "
      "schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)",
      "CREATE TABLE ducklake_schema("
      "schema_id BIGINT, schema_uuid UUID, begin_snapshot BIGINT, "
      "end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, "
      "path_is_relative BOOLEAN)",
      "CREATE TABLE ducklake_table("
      "table_id BIGINT, table_uuid UUID, begin_snapshot BIGINT, "
      "end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, "
      "path VARCHAR, path_is_relative BOOLEAN)",
      "CREATE TABLE ducklake_table_stats("
      "table_id BIGINT, record_count BIGINT, next_row_id BIGINT, "
      "file_size_bytes BIGINT)",
      "CREATE TABLE ducklake_column("
      "column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, "
      "table_id BIGINT, column_order BIGINT, column_name VARCHAR, "
      "column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, "
      "nulls_allowed BOOLEAN, parent_column BIGINT, "
      "default_value_type VARCHAR, default_value_dialect VARCHAR)",
      "CREATE TABLE ducklake_data_file("
      "data_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, "
      "end_snapshot BIGINT, file_order BIGINT, path VARCHAR, "
      "path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, "
      "file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, "
      "partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, "
      "partial_max BIGINT)",
      "CREATE TABLE ducklake_delete_file("
      "delete_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, "
      "end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, "
      "path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, "
      "file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, "
      "partial_max BIGINT)",
      "CREATE TABLE ducklake_inlined_data_tables("
      "table_id BIGINT, table_name VARCHAR, schema_version BIGINT)",
      "INSERT INTO ducklake_metadata VALUES "
      "('version', '1.0', NULL, NULL), "
      "('created_by', 'Axiom DuckLakeQueryTest', NULL, NULL), "
      "('encrypted', 'false', NULL, NULL)",
      fmt::format(
          "INSERT INTO ducklake_metadata VALUES "
          "('data_path', {}, NULL, NULL)",
          quotePath(dataDirectory)),
      "INSERT INTO ducklake_snapshot VALUES "
      "(1, current_timestamp, 1, 2, 2)",
      "INSERT INTO ducklake_schema VALUES "
      "(0, NULL, 0, NULL, 'main', 'main/', true)",
      "INSERT INTO ducklake_table VALUES "
      "(1, NULL, 1, NULL, 0, 'numbers', 'numbers/', true)",
      fmt::format(
          "INSERT INTO ducklake_table_stats VALUES (1, 3, 3, {})",
          dataFileSize),
      "INSERT INTO ducklake_column VALUES "
      "(1, 1, NULL, 1, 1, 'id', 'int32', NULL, NULL, true, NULL, NULL, NULL), "
      "(2, 1, NULL, 1, 2, 'name', 'varchar', NULL, NULL, true, NULL, NULL, NULL)",
      fmt::format(
          "INSERT INTO ducklake_data_file VALUES "
          "(1, 1, 1, NULL, NULL, {}, true, 'parquet', 3, {}, NULL, 0, "
          "NULL, NULL, NULL, NULL)",
          quoteSqlString(dataFile.filename().string()),
          dataFileSize),
  };

  for (const auto& sql : sqls) {
    if (auto error = runDuckDb(connection, sql)) {
      return error;
    }
  }

  return std::nullopt;
}

class DuckLakeQueryTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    directory_ = common::testutil::TempDirectoryPath::create();
    auto data = makeRowVector({
        makeFlatVector<int32_t>({1, 2, 3}),
        makeFlatVector<std::string>({"one", "two", "three"}),
    });
    if (auto error =
            createDuckLakeTable(directory_->getPath(), data, rootPool_.get())) {
      FAIL() << "Failed to create DuckLake test data with DuckDB "
             << duckdb::DuckDB::LibraryVersion() << ": " << error.value();
    }

    connectors_ = std::make_unique<Connectors>();
    runner_ = std::make_unique<::axiom::sql::SqlQueryRunner>();
    runner_->initialize([&]() {
      auto connector = connectors_->registerDuckLakeConnector(
          fmt::format("ducklake:{}/metadata.ducklake", directory_->getPath()));
      return std::make_pair(connector->connectorId(), std::string{"main"});
    });
  }

  void TearDown() override {
    runner_.reset();
    connectors_.reset();
    directory_.reset();
  }

  ::axiom::sql::SqlQueryRunner::SqlResult run(std::string_view sql) {
    ::axiom::sql::SqlQueryRunner::RunOptions options;
    options.numWorkers = 1;
    options.numDrivers = 1;
    return runner_->run(sql, options);
  }

  std::shared_ptr<common::testutil::TempDirectoryPath> directory_;
  std::unique_ptr<Connectors> connectors_;
  std::unique_ptr<::axiom::sql::SqlQueryRunner> runner_;
};

TEST_F(DuckLakeQueryTest, readsDataFilesThroughAxiom) {
  auto result = run("SELECT id, name FROM numbers ORDER BY id");

  ASSERT_FALSE(result.message.has_value()) << result.message.value_or("");
  ASSERT_THAT(result.results, testing::SizeIs(1));
  test::assertEqualVectors(
      result.results[0],
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<std::string>({"one", "two", "three"}),
      }));
}

TEST_F(DuckLakeQueryTest, supportsQualifiedTableName) {
  auto result = run("SELECT count(*) FROM ducklake.main.numbers");

  ASSERT_FALSE(result.message.has_value()) << result.message.value_or("");
  ASSERT_THAT(result.results, testing::SizeIs(1));
  test::assertEqualVectors(
      result.results[0],
      makeRowVector({
          makeFlatVector<int64_t>({3}),
      }));
}

} // namespace
} // namespace facebook::axiom::connector::ducklake
