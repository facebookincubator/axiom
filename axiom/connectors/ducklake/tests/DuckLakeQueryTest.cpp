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
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/cli/Connectors.h"
#include "axiom/cli/SqlQueryRunner.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace facebook::axiom::connector::ducklake {
namespace {

struct DuckLakeCreationError {
  std::string message;
  bool isEnvironmentIssue{false};
};

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

bool isDuckLakeExtensionSetupFailure(
    std::string_view sql,
    std::string_view error) {
  if (sql == "INSTALL ducklake" || sql == "LOAD ducklake") {
    return true;
  }
  return sql.find("ATTACH 'ducklake:") != std::string_view::npos &&
      (error.find("Extension") != std::string_view::npos ||
       error.find("extension") != std::string_view::npos ||
       error.find("ducklake") != std::string_view::npos);
}

bool hasParquetFile(const std::filesystem::path& directory) {
  if (!std::filesystem::exists(directory)) {
    return false;
  }
  for (const auto& entry :
       std::filesystem::recursive_directory_iterator(directory)) {
    if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
      return true;
    }
  }
  return false;
}

std::optional<DuckLakeCreationError> createDuckLakeTable(
    const std::filesystem::path& directory) {
  const auto catalogPath = directory / "metadata.ducklake";
  const auto dataDirectory = directory / "metadata.ducklake.files";

  duckdb::DuckDB db(nullptr);
  duckdb::Connection connection{db};
  std::vector<std::string> statements{
      fmt::format(
          "ATTACH {} AS lake (DATA_INLINING_ROW_LIMIT 0)",
          quoteSqlString(fmt::format("ducklake:{}", catalogPath.string()))),
      "USE lake",
      "CREATE TABLE numbers(id INTEGER, name VARCHAR)",
      "INSERT INTO numbers VALUES (1, 'one'), (2, 'two'), (3, 'three')",
  };

  if (auto error = runDuckDb(connection, "INSTALL ducklake")) {
    return DuckLakeCreationError{
        fmt::format("DuckDB failed to install DuckLake: {}", error.value()),
        true,
    };
  }
  if (auto error = runDuckDb(connection, "LOAD ducklake")) {
    return DuckLakeCreationError{
        fmt::format("DuckDB failed to load DuckLake: {}", error.value()),
        true,
    };
  }
  for (const auto& sql : statements) {
    if (auto error = runDuckDb(connection, sql)) {
      return DuckLakeCreationError{
          fmt::format("DuckDB failed to run '{}': {}", sql, error.value()),
          isDuckLakeExtensionSetupFailure(sql, error.value()),
      };
    }
  }

  if (!std::filesystem::exists(catalogPath)) {
    return DuckLakeCreationError{
        fmt::format(
            "DuckLake catalog was not created: {}", catalogPath.string()),
    };
  }
  if (!hasParquetFile(dataDirectory)) {
    return DuckLakeCreationError{
        fmt::format(
            "DuckLake data file was not created: {}", dataDirectory.string()),
    };
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
    if (auto error = createDuckLakeTable(directory_->getPath())) {
      if (error->isEnvironmentIssue) {
        GTEST_SKIP() << "DuckLake extension is not available in Axiom-linked "
                     << "DuckDB " << duckdb::DuckDB::LibraryVersion() << ": "
                     << error->message;
      }
      FAIL() << "Failed to create DuckLake test data with DuckDB "
             << duckdb::DuckDB::LibraryVersion() << ": " << error->message;
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
