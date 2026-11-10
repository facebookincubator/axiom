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

#include <fmt/ranges.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace axiom::sql {
namespace {

using namespace facebook::velox;

// Drives REFRESH VECTOR INDEX end to end through the CLI over a Hive connector
// backed by a temporary directory.
class VectorIndexRefreshTest : public SqlQueryRunnerTestBase {
 protected:
  void SetUp() override {
    dataPath_ = exec::test::TempDirectoryPath::create();
    runner_ = makeLocalHiveRunner(dataPath_->getPath());
  }

  // Creates the table 't' of points, (id, embedding) and the VARCHAR partition
  // columns 'partitionColumns', with the index 'i' of 3 clusters over the
  // embeddings, keyed by the id.
  void createTable(const std::vector<std::string>& partitionColumns = {"ds"}) {
    std::vector<std::string> columns;
    std::vector<std::string> names;
    for (const auto& name : partitionColumns) {
      columns.push_back(fmt::format("{} VARCHAR", name));
      names.push_back(fmt::format("'{}'", name));
    }
    run(fmt::format(
        "CREATE TABLE t ("
        "   id BIGINT, "
        "   embedding ARRAY(REAL), "
        "   {}, "
        "   VECTOR INDEX i (embedding) KEY (id) WITH ("
        "       distance_metric = 'cosine', "
        "       index_type = 'flat', "
        "       num_clusters = 3, "
        "       dimension = 2"
        "   )"
        ") "
        "WITH (partitioned_by = ARRAY[{}])",
        fmt::join(columns, ", "),
        fmt::join(names, ", ")));
  }

  // Expects 'text' to show the two plans of a refresh: the train plan, which
  // trains the codecs, then the build plan, which builds the local indexes.
  static void expectRefreshPlans(const std::string& text) {
    const auto buildStart = text.find("Build plan:");
    ASSERT_NE(buildStart, std::string::npos);
    EXPECT_THAT(
        text.substr(0, buildStart),
        testing::AllOf(
            testing::HasSubstr("Train plan:"),
            testing::HasSubstr("vector_index_train")));
    EXPECT_THAT(
        text.substr(buildStart), testing::HasSubstr("vector_index_build"));
  }

  // Returns the query that produces 3 * 'groupSize' points in partition 'ds':
  // three groups of unit vectors within 10 degrees of 0, 120 and 240 degrees.
  static std::string makePoints(std::string_view ds, int64_t groupSize) {
    const auto angle =
        fmt::format("n / {0} * 120 + n % {0} * 10e0 / {0}", groupSize);
    return fmt::format(
        "SELECT n AS id, "
        "   ARRAY[CAST(cos(radians({0})) AS REAL), "
        "   CAST(sin(radians({0})) AS REAL)] AS embedding, "
        "   '{1}' AS ds "
        "FROM unnest(sequence(0, {2})) AS v(n)",
        angle,
        ds,
        3 * groupSize - 1);
  }

  std::shared_ptr<exec::test::TempDirectoryPath> dataPath_;
};

// Each refresh trains a codec for every partition it reads, under one codec id
// per refresh, builds a local index for every cluster of every partition, and
// deletes the other codecs of those partitions. Refreshing a partition the
// index covers fails and leaves the index as it was.
TEST_F(VectorIndexRefreshTest, refresh) {
  createTable();
  run(fmt::format(
      "INSERT INTO t {} UNION ALL {}",
      makePoints("a", 10),
      makePoints("b", 10)));

  // The refresh reports the embeddings it wrote, and its completion info has
  // both of its plans.
  {
    std::optional<QueryCompletionInfo> completion;
    SqlQueryRunner::RunOptions options;
    options.onComplete = [&](const QueryCompletionInfo& info) {
      completion = info;
    };
    EXPECT_EQ(
        fetchSingleInt64(
            "REFRESH VECTOR INDEX i ON t WHERE ds IN ('a', 'b')", options),
        60);
    ASSERT_TRUE(completion.has_value());
    expectRefreshPlans(completion->planString);
  }

  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__codecs"), 2);
  EXPECT_EQ(
      fetchSingleInt64("SELECT count(DISTINCT codec_id) FROM t__i__codecs"), 1);
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__index"), 6);
  EXPECT_EQ(
      fetchSingleInt64("SELECT vector_count(local_index) FROM t__i__index"),
      60);

  // A refresh deletes the other codecs of the partitions it writes, and leaves
  // the rest of a partition's directory alone.
  run(fmt::format("INSERT INTO t {}", makePoints("c", 10)));
  run("INSERT INTO t__i__codecs "
      "SELECT codec, 'c', BIGINT '1' FROM t__i__codecs WHERE ds = 'a'");
  const auto otherFile =
      std::filesystem::path(dataPath_->getPath()) / "t__i__codecs/ds=c/.other";
  std::ofstream{otherFile};
  EXPECT_EQ(fetchSingleInt64("REFRESH VECTOR INDEX i ON t WHERE ds = 'c'"), 30);
  EXPECT_TRUE(std::filesystem::exists(otherFile));
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__codecs"), 3);
  EXPECT_EQ(
      fetchSingleInt64("SELECT count(DISTINCT codec_id) FROM t__i__codecs"), 2);
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__index"), 9);

  VELOX_ASSERT_THROW(
      run("REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"),
      "Partition already exists");
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__codecs"), 3);
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__index"), 9);
}

// EXPLAIN shows the two plans of a refresh and writes nothing. EXPLAIN ANALYZE
// runs the refresh and shows both plans with their runtime stats. EXPLAIN
// (TYPE IO) is not supported.
TEST_F(VectorIndexRefreshTest, explain) {
  createTable();
  run(fmt::format("INSERT INTO t {}", makePoints("a", 10)));

  for (const auto* explain : {"EXPLAIN", "EXPLAIN (TYPE OPTIMIZED)"}) {
    SCOPED_TRACE(explain);
    const auto result = run(
        fmt::format("{} REFRESH VECTOR INDEX i ON t WHERE ds = 'a'", explain));
    ASSERT_TRUE(result.message.has_value());
    expectRefreshPlans(*result.message);
  }

  VELOX_ASSERT_THROW(
      run("EXPLAIN (TYPE IO) REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"),
      "EXPLAIN (TYPE IO) is not supported for REFRESH VECTOR INDEX");

  // EXPLAIN writes nothing.
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__codecs"), 0);
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__index"), 0);

  const auto result =
      run("EXPLAIN ANALYZE REFRESH VECTOR INDEX i ON t WHERE ds = 'a'");
  ASSERT_TRUE(result.message.has_value());
  expectRefreshPlans(*result.message);
  EXPECT_THAT(*result.message, testing::HasSubstr("Peak memory"));
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__codecs"), 1);
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__index"), 3);
}

// A partition with more rows than its codec trains on is sampled: its codec
// still has all of its clusters, and every row is indexed.
TEST_F(VectorIndexRefreshTest, sample) {
  createTable();
  run(fmt::format("INSERT INTO t {}", makePoints("a", 1'000)));

  EXPECT_EQ(
      fetchSingleInt64("REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"), 3'000);
  EXPECT_EQ(
      fetchSingleInt64("SELECT vector_count(codec) FROM t__i__codecs"), 3);
  EXPECT_EQ(
      fetchSingleInt64("SELECT vector_count(local_index) FROM t__i__index"),
      3'000);
}

// A table partitioned by several columns gets a codec and local indexes for
// each combination of their values.
TEST_F(VectorIndexRefreshTest, partitionColumns) {
  createTable({"ds", "hr"});
  run(fmt::format(
      "INSERT INTO t "
      "SELECT id, embedding, ds, '01' FROM ({0}) "
      "UNION ALL "
      "SELECT id, embedding, ds, '02' FROM ({0})",
      makePoints("a", 10)));

  EXPECT_EQ(fetchSingleInt64("REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"), 60);
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__codecs"), 2);
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__index"), 6);

  VELOX_ASSERT_THROW(
      run("REFRESH VECTOR INDEX i ON t WHERE hr = '01'"),
      "Partition already exists");
  EXPECT_EQ(fetchSingleInt64("SELECT count(*) FROM t__i__codecs"), 2);
}

// The index holds only valid embeddings: rows with a NULL key, or an embedding
// that is NULL, of another dimension, with a NULL or non-finite element, or all
// zeros, are skipped, as are rows with a NULL partition value.
TEST_F(VectorIndexRefreshTest, invalidRows) {
  createTable();

  // Partition 'a' has 30 valid rows and every kind of invalid one. Partition
  // 'b' has only invalid rows: the index covers it, and a search of it finds
  // nothing.
  const auto invalidRows = [](std::string_view ds) {
    return fmt::format(
        "SELECT CAST(a AS BIGINT), CAST(b AS ARRAY(REAL)), '{}' "
        "FROM (VALUES "
        "   (NULL, ARRAY[1e0, 0e0]), "
        "   (1, NULL), "
        "   (1, ARRAY[1e0]), "
        "   (1, ARRAY[1e0, 0e0, 0e0]), "
        "   (1, ARRAY[0e0, 0e0]), "
        "   (1, ARRAY[1e0, NULL]), "
        "   (1, ARRAY[nan(), 1e0]), "
        "   (1, ARRAY[infinity(), 1e0])) AS v(a, b)",
        ds);
  };
  // An invalid row is skipped whatever its partition value, NULL included.
  run(fmt::format(
      "INSERT INTO t {} UNION ALL {} UNION ALL {} UNION ALL "
      "SELECT BIGINT '1', CAST(NULL AS ARRAY(REAL)), CAST(NULL AS VARCHAR)",
      makePoints("a", 10),
      invalidRows("a"),
      invalidRows("b")));
  EXPECT_EQ(
      fetchSingleInt64(
          "REFRESH VECTOR INDEX i ON t WHERE ds IN ('a', 'b') OR ds IS NULL"),
      30);
  EXPECT_EQ(
      fetchSingleInt64(
          "SELECT count(*) FROM t__i__codecs WHERE ds = 'b' AND codec IS NULL"),
      1);
  EXPECT_EQ(
      fetchSingleInt64(
          "SELECT count(*) FROM t__i__index "
          "WHERE ds = 'b' AND local_index IS NULL"),
      1);
  EXPECT_EQ(
      fetchSingleInt64(
          "SELECT count(*) FROM TABLE(vector_search("
          "queries => TABLE(SELECT ARRAY[REAL '1', REAL '0'] AS embedding), "
          "corpus => TABLE(SELECT * FROM t WHERE ds = 'b'), "
          "embeddings => DESCRIPTOR(embedding), metric => 'cosine', k => 1))"),
      0);

  run("DELETE FROM t WHERE ds IS NULL");
  run("INSERT INTO t SELECT BIGINT '1', ARRAY[REAL '1', REAL '0'], "
      "CAST(NULL AS VARCHAR)");
  EXPECT_EQ(
      fetchSingleInt64("REFRESH VECTOR INDEX i ON t WHERE ds IS NULL"), 0);
}

} // namespace
} // namespace axiom::sql
