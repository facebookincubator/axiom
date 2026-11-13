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

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>

#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"
#include "axiom/sql/presto/CentroidPlanner.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace axiom::sql {
namespace {

using namespace facebook::velox;
using presto::CentroidPlanner;
namespace lp = facebook::axiom::logical_plan;

// A mistake in a planner's result.
enum class Mistake {
  kNone,
  kMissingId,
  kBigintPartition,
  kIntegerIds,
  kDoubleCentroids,
  kTooManyCentroids,
  kIdGap,
  kShortCentroids,
  kZeroCentroid,
  kNullId,
  kNullCentroid,
};

// Plans each partition's first min(rows, K) sampled rows as its centroids,
// with the sample row - 1 as the cluster ID, under names of its own. With a
// mistake, the result has that mistake.
class FirstSampledRows : public CentroidPlanner {
 public:
  explicit FirstSampledRows(Mistake mistake = Mistake::kNone)
      : mistake_{mistake} {}

  lp::PlanBuilder plan(
      const facebook::axiom::connector::VectorIndexProperties& properties,
      const std::function<lp::PlanBuilder()>& makeSample) const override {
    auto rows = makeSample();
    const auto columns = rows.findOrAssignOutputNames();
    const auto numPartitions = columns.size() - 3;
    const auto& sampleRow = columns[numPartitions + 2];
    int64_t limit = properties.numClusters;
    if (mistake_ == Mistake::kTooManyCentroids) {
      ++limit;
    }

    std::vector<lp::ExprApi> projections;
    projections.reserve(numPartitions + 2);
    for (size_t i = 0; i < numPartitions; ++i) {
      projections.push_back(columns[i].toCol().as(fmt::format("part{}", i)));
    }
    if (mistake_ == Mistake::kBigintPartition) {
      projections.front() =
          lp::Call("length", columns.front().toCol()).as("part0");
    }
    auto id = (sampleRow.toCol() - lp::Lit(int64_t{1})).as("cluster");
    if (mistake_ == Mistake::kIntegerIds) {
      id = lp::Cast(INTEGER(), sampleRow.toCol()).as("cluster");
    } else if (mistake_ == Mistake::kNullId) {
      // The highest cluster ID is NULL.
      id = lp::Call(
               "if",
               sampleRow.toCol() == lp::Lit(limit),
               lp::NullLit(BIGINT()),
               sampleRow.toCol() - lp::Lit(int64_t{1}))
               .as("cluster");
    }
    const auto embedding = columns[numPartitions + 1].toCol();
    auto centroid = embedding.as("centroid");
    if (mistake_ == Mistake::kDoubleCentroids) {
      centroid = lp::Cast(ARRAY(DOUBLE()), embedding).as("centroid");
    } else if (mistake_ == Mistake::kShortCentroids) {
      centroid =
          lp::Call("slice", embedding, lp::Lit(1), lp::Lit(1)).as("centroid");
    } else if (mistake_ == Mistake::kZeroCentroid) {
      centroid =
          lp::Call("transform", embedding, lp::Lambda({"x"}, lp::Lit(0.0f)))
              .as("centroid");
    } else if (mistake_ == Mistake::kNullCentroid) {
      centroid = lp::Call(
                     "if",
                     sampleRow.toCol() == lp::Lit(int64_t{1}),
                     lp::NullLit(ARRAY(REAL())),
                     embedding)
                     .as("centroid");
    }
    if (mistake_ != Mistake::kMissingId) {
      projections.push_back(id);
    }
    projections.push_back(centroid);
    auto kept = sampleRow.toCol() <= lp::Lit(limit);
    if (mistake_ == Mistake::kIdGap) {
      // Cluster IDs 0 and 2, without 1.
      kept = kept && sampleRow.toCol() != lp::Lit(int64_t{2});
    }
    return rows.filter(kept).project(projections);
  }

 private:
  const Mistake mistake_;
};

// Drives REFRESH VECTOR INDEX through the CLI over a Hive connector backed by
// a temporary directory, with FirstSampledRows as the centroid planner.
class VectorIndexCentroidPlannerTest : public SqlQueryRunnerTestBase {
 protected:
  // Creates the table 't' of points, (id, embedding, ds), partitioned by ds,
  // with the index 'i' of 3 clusters over the embeddings, keyed by the id.
  void SetUp() override {
    dataPath_ = exec::test::TempDirectoryPath::create();
    useMistake(Mistake::kNone);
    run("CREATE TABLE t ("
        "   id BIGINT, "
        "   embedding ARRAY(REAL), "
        "   ds VARCHAR, "
        "   VECTOR INDEX i (embedding) KEY (id) WITH ("
        "       distance_metric = 'cosine', "
        "       index_type = 'flat', "
        "       num_clusters = 3, "
        "       dimension = 2"
        "   )"
        ") "
        "WITH (partitioned_by = ARRAY['ds'])");
  }

  // Replaces runner_ with one over the same data whose planner makes
  // 'mistake'.
  void useMistake(Mistake mistake) {
    centroidPlanner_ = std::make_shared<FirstSampledRows>(mistake);
    runner_ = makeLocalHiveRunner(dataPath_->getPath());
  }

  // Returns VALUES rows of six points of distinct directions, with ids 1 to 6,
  // in partition 'ds'. The rows are in order of id, or in the opposite order if
  // 'reversed'.
  static std::string pointRows(std::string_view ds, bool reversed) {
    const std::vector<std::pair<std::string_view, std::string_view>> embeddings{
        {"1", "0"},
        {"0", "1"},
        {"-1", "0"},
        {"0", "-1"},
        {"0.6", "0.8"},
        {"-0.6", "-0.8"},
    };
    std::vector<std::string> rows;
    rows.reserve(embeddings.size());
    for (size_t i = 0; i < embeddings.size(); ++i) {
      const auto& [x, y] = embeddings[i];
      rows.push_back(
          fmt::format(
              "({}, ARRAY[REAL '{}', REAL '{}'], '{}')", i + 1, x, y, ds));
    }
    if (reversed) {
      std::reverse(rows.begin(), rows.end());
    }
    return fmt::format("{}", fmt::join(rows, ", "));
  }

  // Returns VALUES rows in partition 'ds' that REFRESH skips: a NULL embedding,
  // one of another dimension and an all-zero one.
  static std::string invalidRows(std::string_view ds) {
    return fmt::format(
        "(101, CAST(NULL AS ARRAY(REAL)), '{0}'), "
        "(102, ARRAY[REAL '1'], '{0}'), "
        "(103, ARRAY[REAL '0', REAL '0'], '{0}')",
        ds);
  }

  // Returns the train plan EXPLAIN (TYPE LOGICAL) shows for 'refresh'.
  std::string trainPlan(std::string_view refresh) {
    const auto result = run(fmt::format("EXPLAIN (TYPE LOGICAL) {}", refresh));
    VELOX_CHECK(result.message.has_value());
    return result.message->substr(0, result.message->find("Build plan:"));
  }

  // Returns the cluster ids of the local indexes of partition 'ds', sorted and
  // joined by commas.
  std::string clusterIds(std::string_view ds) {
    return fetchSingleValue<std::string>(fmt::format(
        "SELECT array_join(array_sort(array_agg(cluster_id)), ',') "
        "FROM t__i__index "
        "WHERE ds = '{}'",
        ds));
  }

  // Returns the rows of partition 'ds' whose embedding is a centroid of the
  // partition's codec, as "id:cluster_id" pairs, sorted and joined by commas.
  std::string centroidRows(std::string_view ds) {
    return fetchSingleValue<std::string>(fmt::format(
        "SELECT array_join("
        "   array_sort(array_agg(CAST(q AS VARCHAR) || ':' || "
        "       CAST(cluster_id AS VARCHAR))), ',') "
        "FROM TABLE(vector_search("
        "   queries => TABLE(SELECT id AS q, embedding FROM t WHERE ds = '{0}'), "
        "   corpus => TABLE(SELECT * FROM \"t$i$centroids\" WHERE ds = '{0}'), "
        "   embeddings => DESCRIPTOR(embedding, centroid), "
        "   metric => 'cosine', "
        "   k => 1, "
        "   score => DESCRIPTOR(score))) "
        "WHERE score > 0.9999",
        ds));
  }

  std::shared_ptr<exec::test::TempDirectoryPath> dataPath_;
  // Runs a query on 4 workers with 4 drivers each.
  const SqlQueryRunner::RunOptions distributed_{
      .numWorkers = 4,
      .numDrivers = 4,
  };
};

// REFRESH packs the planner's centroids into each partition's codec under the
// planner's IDs, then builds and searches the index as usual. Only valid rows
// reach the planner, and a partition without any gets a NULL codec.
TEST_F(VectorIndexCentroidPlannerTest, refresh) {
  run(fmt::format(
      "INSERT INTO t VALUES "
      "(1, ARRAY[REAL '1', REAL '0'], 'a'), "
      "(1, ARRAY[REAL '1', REAL '0'], 'b'), "
      "(2, ARRAY[REAL '0', REAL '1'], 'b'), "
      "(3, ARRAY[REAL '-1', REAL '0'], 'b'), "
      "{}, {}, {}",
      invalidRows("a"),
      pointRows("c", /*reversed=*/false),
      invalidRows("d")));

  const std::string refresh =
      "REFRESH VECTOR INDEX i ON t WHERE ds IN ('a', 'b', 'c', 'd')";
  EXPECT_THAT(
      trainPlan(refresh),
      testing::AllOf(
          testing::HasSubstr("vector_index_pack"),
          testing::Not(testing::HasSubstr("vector_index_train"))));
  EXPECT_EQ(fetchSingleInt64(refresh, distributed_), 10);

  // A partition has min(valid rows, num_clusters) centroids, with IDs from
  // zero. Each centroid's own row is in its cluster, so every ID has a local
  // index.
  for (const auto& [ds, numRows, numCentroids, ids] : {
           std::tuple{"a", 1, 1, "0"},
           std::tuple{"b", 3, 3, "0,1,2"},
           std::tuple{"c", 6, 3, "0,1,2"},
       }) {
    SCOPED_TRACE(ds);
    EXPECT_EQ(
        fetchSingleInt64(
            fmt::format(
                "SELECT vector_count(codec) FROM t__i__codecs WHERE ds = '{}'",
                ds)),
        numCentroids);
    EXPECT_EQ(
        fetchSingleInt64(
            fmt::format(
                "SELECT vector_count(local_index) FROM t__i__index WHERE ds = '{}'",
                ds)),
        numRows);
    EXPECT_EQ(clusterIds(ds), ids);
  }
  EXPECT_EQ(
      fetchSingleInt64(
          "SELECT count(*) FROM t__i__codecs WHERE ds = 'd' AND codec IS NULL"),
      1);

  // Probing only the query's nearest cluster finds the point in its direction.
  EXPECT_EQ(
      fetchSingleInt64(
          "SELECT id "
          "FROM TABLE(vector_search("
          "   query => ARRAY[REAL '0', REAL '1'], "
          "   corpus => TABLE(SELECT * FROM t WHERE ds = 'c'), "
          "   embeddings => DESCRIPTOR(embedding), "
          "   metric => 'cosine', "
          "   k => 1, "
          "   hints => MAP(ARRAY['num_probes'], ARRAY['1'])))"),
      2);
}

// The planner's sample of a partition is numbered in an order that depends
// only on the rows' keys: identical partitions get the same centroids under
// the same IDs, whatever the order their rows are stored in and the number of
// workers and drivers.
TEST_F(VectorIndexCentroidPlannerTest, sampleOrder) {
  run(fmt::format(
      "INSERT INTO t VALUES {}, {}",
      pointRows("a", /*reversed=*/false),
      pointRows("b", /*reversed=*/true)));
  const SqlQueryRunner::RunOptions singleNode{.numWorkers = 1, .numDrivers = 1};
  EXPECT_EQ(
      fetchSingleInt64(
          "REFRESH VECTOR INDEX i ON t WHERE ds = 'a'", singleNode),
      6);
  EXPECT_EQ(
      fetchSingleInt64(
          "REFRESH VECTOR INDEX i ON t WHERE ds = 'b'", distributed_),
      6);

  const auto centroids = centroidRows("a");
  EXPECT_EQ(std::count(centroids.begin(), centroids.end(), ':'), 3)
      << centroids;
  EXPECT_EQ(centroidRows("b"), centroids);
}

// A runner without a planner trains each codec with vector_index_train. The
// v1 optimizer can't run a planner's plans, so a v1 runner takes no planner.
TEST_F(VectorIndexCentroidPlannerTest, withoutPlanner) {
  run(fmt::format(
      "INSERT INTO t VALUES {}", pointRows("a", /*reversed=*/false)));
  centroidPlanner_ = nullptr;
  for (const bool useV2 : {true, false}) {
    SCOPED_TRACE(useV2 ? "v2" : "v1");
    useV2_ = useV2;
    runner_ = makeLocalHiveRunner(dataPath_->getPath());
    EXPECT_THAT(
        trainPlan("REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"),
        testing::AllOf(
            testing::HasSubstr("vector_index_train"),
            testing::Not(testing::HasSubstr("vector_index_pack"))));
  }

  centroidPlanner_ = std::make_shared<FirstSampledRows>();
  VELOX_ASSERT_THROW(
      makeLocalHiveRunner(dataPath_->getPath()),
      "A centroid planner requires the v2 optimizer");
}

// The planner returns, by position, the partition columns with their types,
// then BIGINT cluster IDs and ARRAY(REAL) centroids, which REFRESH checks while
// planning. vector_index_pack checks the cluster IDs and the centroids against
// the index when the refresh runs, and fails on a NULL cluster ID or centroid.
TEST_F(VectorIndexCentroidPlannerTest, invalidCentroids) {
  run(fmt::format(
      "INSERT INTO t VALUES {}", pointRows("a", /*reversed=*/false)));
  const std::string refresh = "REFRESH VECTOR INDEX i ON t WHERE ds = 'a'";
  for (
      const auto& [mistake, error] : {
          std::pair{
              Mistake::kMissingId,
              "Centroid planner must return the partition columns, a cluster ID and a centroid"},
          std::pair{
              Mistake::kBigintPartition,
              "Centroid planner changed a partition column's type"},
          std::pair{
              Mistake::kIntegerIds,
              "Centroid planner must return BIGINT cluster IDs"},
          std::pair{
              Mistake::kDoubleCentroids,
              "Centroid planner must return ARRAY(REAL) centroids"},
      }) {
    SCOPED_TRACE(error);
    useMistake(mistake);
    VELOX_ASSERT_THROW(
        run(fmt::format("EXPLAIN (TYPE LOGICAL) {}", refresh)), error);
  }

  for (const auto& [mistake, error] : {
           std::pair{
               Mistake::kTooManyCentroids,
               "Cluster id must be less than the number of clusters"},
           std::pair{
               Mistake::kIdGap,
               "Cluster ids must be in [0, number of clusters) range"},
           std::pair{
               Mistake::kShortCentroids,
               "Cluster centroid must have the given dimension"},
           std::pair{
               Mistake::kZeroCentroid,
               "Cluster centroid must not be all zeros"},
           std::pair{Mistake::kNullId, "Cluster id must not be null"},
           std::pair{
               Mistake::kNullCentroid, "Cluster centroid must not be null"},
       }) {
    SCOPED_TRACE(error);
    useMistake(mistake);
    VELOX_ASSERT_THROW(run(refresh), error);
  }
}

} // namespace
} // namespace axiom::sql
