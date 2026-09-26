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

#include <folly/json.h>
#include <gtest/gtest.h>

#include "axiom/cli/Connectors.h"
#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/Options.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace axiom::sql {
namespace {

using namespace facebook::velox;

// Drives DELETE end to end through the CLI over a Hive connector backed by a
// temporary directory. Hive removes the rows by dropping partitions.
class DeleteTest : public SqlQueryRunnerTestBase {
 protected:
  static constexpr std::string_view kConnectorId = "hive";
  static constexpr std::string_view kHiveSchema = "default";
  static constexpr auto kFileFormat = dwio::common::FileFormat::DWRF;

  void SetUp() override {
    dataPath_ = exec::test::TempDirectoryPath::create();

    runner_ = makeRunner([&]() {
      connectors_.registerLocalHiveConnector(
          dataPath_->getPath(),
          std::string(dwio::common::FileFormatName::toName(kFileFormat)),
          std::string(kConnectorId));
      return std::make_pair(
          std::string(kConnectorId), std::string(kHiveSchema));
    });
  }

  // Parses and re-serializes JSON with sorted keys so the comparison does not
  // depend on key order.
  static std::string normalizeJson(const std::string& json) {
    folly::json::serialization_opts opts;
    opts.pretty_formatting = true;
    opts.sort_keys = true;
    return folly::json::serialize(folly::parseJson(json), opts);
  }

  // Runs 'sql' under EXPLAIN and returns the plan text.
  std::string runExplain(std::string_view sql) {
    auto result = run(fmt::format("EXPLAIN {}", sql));
    VELOX_CHECK(result.message.has_value());
    return result.message.value();
  }

  // Runs 'sql' under EXPLAIN (TYPE IO) and returns the normalized IO JSON.
  std::string runExplainIo(std::string_view sql) {
    auto result = run(fmt::format("EXPLAIN (TYPE IO) {}", sql));
    VELOX_CHECK(result.message.has_value());
    return normalizeJson(result.message.value());
  }

  // Returns the number of rows selected by 'fromClause', e.g. "FROM test
  // WHERE pk = 1".
  int64_t runCount(std::string_view fromClause) {
    return fetchSingleValue<int64_t>(
        fmt::format("SELECT count(*) {}", fromClause));
  }

  std::shared_ptr<exec::test::TempDirectoryPath> dataPath_;
  facebook::axiom::Connectors connectors_;
};

// Explains and then runs a delete against one table: creating a Hive table is
// expensive, so both share it.
TEST_F(DeleteTest, partitionPredicate) {
  run("CREATE TABLE test WITH (partitioned_by = ARRAY['pk']) AS "
      "SELECT x, x % 3 AS pk FROM unnest(array[1, 2, 3, 4, 5, 6]) AS t(x)");

  EXPECT_EQ(
      runExplain("DELETE FROM test WHERE pk = 1"),
      R"(Metadata write via connector 'hive': drop partitions of "default"."test" matching pk BigintRange: [1, 1] no nulls)");

  // The rows removed are the rows scanned, so a delete reports the table it
  // reads and no output table.
  EXPECT_EQ(
      runExplainIo("DELETE FROM test WHERE pk = 1"),
      normalizeJson(
          R"({
              "inputTableColumnInfos": [
                {
                  "columnConstraints": [
                    {
                      "domain": {
                        "ranges": [
                          {
                            "high": {
                              "bound": "EXACTLY",
                              "value": 1
                            },
                            "low": {
                              "bound": "EXACTLY",
                              "value": 1
                            }
                          }
                        ],
                        "nullsAllowed": false
                      },
                      "typeSignature": "INTEGER",
                      "columnName": "pk"
                    }
                  ],
                  "table": {
                    "schemaTable": {
                      "table": "test",
                      "schema": "default"
                    },
                    "catalog": "hive"
                  }
                }
              ]
            })"));

  // EXPLAIN is side-effect-free.
  EXPECT_EQ(6, runCount("FROM test"));

  EXPECT_EQ(2, fetchSingleValue<int64_t>("DELETE FROM test WHERE pk = 1"));
  EXPECT_EQ(4, runCount("FROM test"));
  EXPECT_EQ(0, runCount("FROM test WHERE pk = 1"));

  // A delete with no predicate selects no partition in particular.
  EXPECT_EQ(
      runExplain("DELETE FROM test"),
      R"(Metadata write via connector 'hive': drop all rows of "default"."test")");

  EXPECT_EQ(
      runExplainIo("DELETE FROM test"),
      normalizeJson(
          R"({
              "inputTableColumnInfos": [
                {
                  "columnConstraints": [],
                  "table": {
                    "schemaTable": {
                      "table": "test",
                      "schema": "default"
                    },
                    "catalog": "hive"
                  }
                }
              ]
            })"));

  EXPECT_EQ(4, fetchSingleValue<int64_t>("DELETE FROM test"));
  EXPECT_EQ(0, runCount("FROM test"));
}

// A delete drops whole partitions, so a predicate on a data column is refused.
TEST_F(DeleteTest, unsupportedPredicate) {
  run("CREATE TABLE t WITH (partitioned_by = ARRAY['pk']) AS "
      "SELECT x, x % 3 AS pk FROM unnest(array[1, 2, 3, 4, 5, 6]) AS t(x)");

  VELOX_ASSERT_THROW(
      run("DELETE FROM t WHERE x = 1"),
      "DELETE supports only filters on partition columns: x");
}

} // namespace
} // namespace axiom::sql
