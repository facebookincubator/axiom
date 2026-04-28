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
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/ConnectorRegistry.h"
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

class CurrentPathGuard {
 public:
  explicit CurrentPathGuard(const std::filesystem::path& path)
      : previousPath_{std::filesystem::current_path()} {
    std::filesystem::current_path(path);
  }

  ~CurrentPathGuard() {
    std::filesystem::current_path(previousPath_);
  }

 private:
  std::filesystem::path previousPath_;
};

std::optional<std::string> createDuckLakeTable(
    const std::filesystem::path& directory) {
  CurrentPathGuard currentPath{directory};
  std::filesystem::create_directories("data");

  duckdb::DuckDB db(nullptr);
  duckdb::Connection connection{db};

  for (const auto& sql : {
           "INSTALL ducklake",
           "LOAD ducklake",
           "ATTACH 'ducklake:metadata.ducklake' AS lake "
           "(DATA_PATH 'data', DATA_INLINING_ROW_LIMIT 0)",
           "USE lake",
           "CREATE TABLE numbers(id INTEGER, name VARCHAR)",
           "INSERT INTO numbers VALUES (1, 'one'), (2, 'two'), (3, 'three')",
       }) {
    if (auto error = runDuckDb(connection, sql)) {
      return error;
    }
  }

  return std::nullopt;
}

class DuckLakeQueryTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(
        memory::MemoryManager::Options{});
  }

  void SetUp() override {
    directory_ = common::testutil::TempDirectoryPath::create();
    if (auto error = createDuckLakeTable(directory_->getPath())) {
      GTEST_SKIP() << "DuckLake extension is not available in DuckDB "
                   << duckdb::DuckDB::LibraryVersion() << ": " << error.value();
    }

    connectors_ = std::make_unique<Connectors>();
    runner_ = std::make_unique<::axiom::sql::SqlQueryRunner>();
    runner_->initialize([&]() {
      auto connector = connectors_->registerDuckLakeConnector(fmt::format(
          "ducklake:{}/metadata.ducklake", directory_->getPath()));
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
