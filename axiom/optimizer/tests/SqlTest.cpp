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

#include <folly/FileUtil.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "axiom/optimizer/tests/SqlQueryEntry.h"
#include "axiom/optimizer/tests/SqlTestBase.h"
#include "axiom/optimizer/tests/TestDataPath.h"

namespace facebook::axiom::optimizer::test {
namespace {

// Test fixture that runs a single QueryEntry through SqlTestBase.
class SqlTest : public SqlTestBase {
 public:
  explicit SqlTest(QueryEntry entry) : entry_(std::move(entry)) {}

 protected:
  void SetUp() override {
    SqlTestBase::SetUp();

    createTable(
        "t",
        {makeRowVector(
             {"a", "b", "c"},
             {makeFlatVector<int64_t>({1, 2, 3, 1, 2}),
              makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
              makeFlatVector<double>({1.5, 2.5, 3.5, 4.5, 5.5})}),
         makeRowVector(
             {"a", "b", "c"},
             {makeFlatVector<int64_t>({3, 1, 2, 3, 1}),
              makeFlatVector<int64_t>({60, 70, 80, 90, 100}),
              makeFlatVector<double>({6.5, 7.5, 8.5, 9.5, 10.5})}),
         makeRowVector(
             {"a", "b", "c"},
             {makeFlatVector<int64_t>({2, 3, 1, 2, 3}),
              makeFlatVector<int64_t>({110, 120, 130, 140, 150}),
              makeFlatVector<double>({11.5, 12.5, 13.5, 14.5, 15.5})})});
  }

  void TestBody() override {
    switch (entry_.type) {
      case QueryEntry::Type::kResults:
        assertResults(entry_.sql, entry_.checkColumnNames, entry_.duckDbSql);
        break;
      case QueryEntry::Type::kOrdered:
        assertOrderedResults(
            entry_.sql, entry_.checkColumnNames, entry_.duckDbSql);
        break;
      case QueryEntry::Type::kCount:
        assertResultCount(entry_.sql, entry_.expectedCount);
        break;
      case QueryEntry::Type::kError:
        assertFailure(entry_.sql, entry_.expectedError);
        break;
    }
  }

 private:
  QueryEntry entry_;
};

// Reads the entire contents of a file.
std::string readFile(const std::string& path) {
  std::string content;
  auto success = folly::readFile(path.c_str(), content);
  VELOX_CHECK(success, "Failed to read file: {}", path);
  return content;
}

// Registers all queries from a .sql file as individual gtest tests.
void registerQueryFile(const std::string& fileName) {
  auto path = getTestFilePath(fmt::format("sql/{}", fileName));
  auto content = readFile(path);
  auto entries = QueryEntry::parse(content);

  // Strip the .sql extension to use as a test name prefix.
  auto baseName = fileName.substr(0, fileName.rfind('.'));

  for (const auto& entry : entries) {
    auto testName = fmt::format("{}_l{}", baseName, entry.lineNumber);
    auto capturedEntry = entry;
    testing::RegisterTest(
        "SqlTest",
        testName.c_str(),
        /*type_param=*/nullptr,
        /*value_param=*/nullptr,
        path.c_str(),
        entry.lineNumber,
        [capturedEntry = std::move(capturedEntry)]() -> SqlTest* {
          return new SqlTest(capturedEntry);
        });
  }
}

} // namespace
} // namespace facebook::axiom::optimizer::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);

  facebook::axiom::optimizer::test::registerQueryFile("basic.sql");
  facebook::axiom::optimizer::test::registerQueryFile("join.sql");
  facebook::axiom::optimizer::test::registerQueryFile("subquery.sql");
  facebook::axiom::optimizer::test::registerQueryFile("window.sql");
  facebook::axiom::optimizer::test::registerQueryFile("set.sql");
  facebook::axiom::optimizer::test::registerQueryFile("limit.sql");
  facebook::axiom::optimizer::test::registerQueryFile("aggregation.sql");
  facebook::axiom::optimizer::test::registerQueryFile("subfield.sql");
  facebook::axiom::optimizer::test::registerQueryFile("coercion.sql");
  facebook::axiom::optimizer::test::registerQueryFile(
      "distinctAggregation.sql");

  return RUN_ALL_TESTS();
}
