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

#include "axiom/cli/ResultPrinter.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sstream>
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace axiom::cli {
namespace {

class ResultPrinterTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  // Formats 'results' the way the CLI does and returns the table as text.
  std::string print(
      const std::vector<RowVectorPtr>& results,
      int64_t maxRows,
      int32_t expectedNumRows) {
    std::ostringstream output;
    EXPECT_EQ(printResults(results, maxRows, output), expectedNumRows);
    return output.str();
  }
};

TEST_F(ResultPrinterTest, table) {
  auto results = makeRowVector(
      {"n", "s"},
      {makeFlatVector<int64_t>({1, 22}),
       makeFlatVector<std::string>({"a", "bb"})});

  // Numbers are right-aligned and strings left-aligned, both columns as wide as
  // their widest value or their name, and the last column is not padded.
  EXPECT_EQ(
      print({results}, 10, 2),
      "---+---\n"
      " n | s\n"
      "---+---\n"
      " 1 | a\n"
      "22 | bb\n"
      "(2 rows in 1 batches)\n"
      "\n");
}

TEST_F(ResultPrinterTest, maxRowsTruncatesAndCountsAllRows) {
  auto results = makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4, 5})});

  // The footer counts every row, not just the printed ones.
  EXPECT_EQ(
      print({results}, 2, 5),
      "--\n"
      "c0\n"
      "--\n"
      " 1\n"
      " 2\n"
      "\n"
      "...3 more rows.\n"
      "(5 rows in 1 batches)\n"
      "\n");
}

TEST_F(ResultPrinterTest, multipleBatches) {
  std::vector<RowVectorPtr> results = {
      makeRowVector({makeFlatVector<int64_t>({1})}),
      makeRowVector({makeFlatVector<int64_t>({2})}),
  };

  EXPECT_THAT(
      print(results, 10, 2), testing::HasSubstr("(2 rows in 2 batches)"));
}

TEST_F(ResultPrinterTest, noRowsPrintsFooterOnly) {
  EXPECT_EQ(print({}, 10, 0), "(0 rows in 0 batches)\n\n");
}

} // namespace
} // namespace axiom::cli
