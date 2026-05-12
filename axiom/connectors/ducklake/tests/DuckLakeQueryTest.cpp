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
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "axiom/connectors/ducklake/tests/DuckLakeTestUtils.h"

namespace facebook::axiom::connector::ducklake {
namespace {

class DuckLakeQueryTest : public ::testing::Test,
                          public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    facebook::velox::memory::MemoryManager::testingSetInstance(
        facebook::velox::memory::MemoryManager::Options{});
  }

  void SetUp() override {
    directory_ = facebook::velox::common::testutil::TempDirectoryPath::create();
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

  void assertResultEquals(
      const ::axiom::sql::SqlQueryRunner::SqlResult& result,
      const facebook::velox::RowVectorPtr& expected) {
    ASSERT_FALSE(result.message.has_value()) << result.message.value_or("");
    ASSERT_THAT(result.results, testing::SizeIs(1));
    facebook::velox::test::assertEqualVectors(result.results[0], expected);
  }

  std::shared_ptr<facebook::velox::common::testutil::TempDirectoryPath>
      directory_;
  std::unique_ptr<Connectors> connectors_;
  std::unique_ptr<::axiom::sql::SqlQueryRunner> runner_;
};

TEST_F(DuckLakeQueryTest, all) {
  auto result = run("SELECT id, name FROM numbers ORDER BY id");

  assertResultEquals(
      result,
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<std::string>({"one", "two", "three"}),
      }));
}

TEST_F(DuckLakeQueryTest, aggregates) {
  auto result = run("SELECT count(*), sum(id) FROM numbers WHERE id >= 2");

  assertResultEquals(
      result,
      makeRowVector({
          makeFlatVector<int64_t>({2}),
          makeFlatVector<int64_t>({5}),
      }));
}

TEST_F(DuckLakeQueryTest, synthesizedColumns) {
  auto result =
      run("SELECT count(*) FROM numbers "
          "WHERE \"$path\" LIKE '%.parquet' AND \"$file_size\" > 0");

  assertResultEquals(
      result,
      makeRowVector({
          makeFlatVector<int64_t>({3}),
      }));
}

TEST_F(DuckLakeQueryTest, qualifiedTableName) {
  auto result = run("SELECT count(*) FROM ducklake.main.numbers");

  assertResultEquals(
      result,
      makeRowVector({
          makeFlatVector<int64_t>({3}),
      }));
}

} // namespace
} // namespace facebook::axiom::connector::ducklake
