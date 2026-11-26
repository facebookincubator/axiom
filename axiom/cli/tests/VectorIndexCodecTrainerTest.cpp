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
#include <gtest/gtest.h>
#include <algorithm>

#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"
#include "axiom/sql/presto/ColumnExprs.h"
#include "axiom/sql/presto/VectorIndexCodecTrainer.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace axiom::sql {
namespace {

using namespace facebook::velox;
using presto::ColumnExprs;
using presto::VectorIndexCodecTrainer;
using presto::VectorIndexSample;
namespace lp = facebook::axiom::logical_plan;

// A small spherical k-means over about kPointsPerCluster * K rows of each
// partition. It seeds each partition's clusters with its K rows of smallest
// sample hash, which it finds among the smaller sample of about
// kCandidatesPerCluster * K rows. It then moves each centroid to the
// normalized sum of its cluster's points kNumIterations times. The points stay
// spread over the workers: each update joins them with their partition's
// centroids and sums them by cluster.
class SphericalKMeans : public VectorIndexCodecTrainer {
 public:
  lp::PlanBuilder plan(const VectorIndexSample& sample) const override {
    const auto numPartitionColumns = sample.partitionColumns->size();
    std::vector<std::string> partitions;
    partitions.reserve(numPartitionColumns);
    for (size_t i = 0; i < numPartitionColumns; ++i) {
      partitions.push_back(fmt::format("part{}", i));
    }
    const auto keys = ColumnExprs::of(partitions);

    // 'rows', a sample, with its columns named by position. The key is unused.
    const auto namePoints = [&](lp::PlanBuilder rows) {
      const auto columns = rows.findOrAssignOutputNames();
      std::vector<lp::ExprApi> named;
      named.reserve(numPartitionColumns + 3);
      for (size_t i = 0; i < numPartitionColumns; ++i) {
        named.push_back(columns[i].toCol().as(partitions[i]));
      }
      named.push_back(columns[numPartitionColumns + 1].toCol().as("embedding"));
      named.push_back(
          columns[numPartitionColumns + 2].toCol().as("sample_hash"));
      named.push_back(columns[numPartitionColumns + 3].toCol().as("num_rows"));
      return rows.project(named);
    };

    // The candidates are the smaller sample of about numCandidates rows of
    // each partition. A partition of at most numCandidates rows has all of its
    // valid rows as candidates, so it gets a seed per valid row, up to K. A
    // larger one fails the refresh when fewer than K of its candidates are
    // valid.
    const int64_t numClusters = sample.properties.numClusters;
    const int64_t numCandidates = kCandidatesPerCluster * numClusters;
    const auto clusterId = lp::Call(
        "if",
        lp::Col("num_rows") > lp::Lit(numCandidates) &&
            lp::Col("num_candidates") < lp::Lit(numClusters),
        lp::Cast(
            BIGINT(),
            lp::Call(
                "fail",
                ColumnExprs::message(
                    "Too few valid rows to seed the clusters of partition: ",
                    partitions))),
        lp::Col("seed_number") - lp::Lit(int64_t{1}));
    auto seeds =
        namePoints(sample.makeSample(numCandidates))
            .aggregate(
                keys,
                {lp::Call(
                     "min_by",
                     lp::Col("embedding"),
                     lp::Col("sample_hash"),
                     lp::Lit(numClusters))
                     .as("seeds"),
                 lp::Call("count").as("num_candidates"),
                 lp::Call("arbitrary", lp::Col("num_rows")).as("num_rows")})
            .unnest(
                {lp::Col("seeds").unnestAs("seed")},
                lp::Ordinality().as("seed_number"))
            .project(
                ColumnExprs::concat(
                    keys,
                    {clusterId.as("cluster_id"),
                     lp::Col("seed").as("centroid")}));

    const auto state = [&] {
      return lp::PlanBuilder(sample.context)
          .fixedPointState("centroids", seeds);
    };

    // Each point's nearest centroid of its partition: the cluster ID of the
    // greatest cosine similarity in the partition's map from cluster ID to
    // centroid.
    auto centroidMaps =
        state()
            .aggregate(
                keys,
                {lp::Call("map_agg", lp::Col("cluster_id"), lp::Col("centroid"))
                     .as("centroids")})
            .project(
                ColumnExprs::concat(
                    ColumnExprs::renamed(partitions, "m_"),
                    lp::Col("centroids")));
    const auto similarities = lp::Call(
        "transform_values",
        lp::Col("centroids"),
        lp::Lambda(
            {"id", "c"},
            lp::Call(
                "cosine_similarity",
                lp::Cast(ARRAY(DOUBLE()), lp::Col("embedding")),
                lp::Cast(ARRAY(DOUBLE()), lp::Col("c")))));
    const auto nearest = lp::Call(
        "element_at",
        lp::Call(
            "map_keys",
            lp::Call("map_top_n", similarities, lp::Lit(int64_t{1}))),
        lp::Lit(int64_t{1}));

    // Each cluster's new centroid: the normalized sum of its points.
    auto means =
        namePoints(sample.makeSample(kPointsPerCluster * numClusters))
            .join(
                centroidMaps,
                *ColumnExprs::equal(partitions, "m_"),
                lp::JoinType::kInner)
            .project(
                ColumnExprs::concat(
                    keys, {nearest.as("cluster_id"), lp::Col("embedding")}))
            .aggregate(
                ColumnExprs::concat(keys, lp::Col("cluster_id")),
                {lp::Call(
                     "vector_sum",
                     lp::Cast(ARRAY(DOUBLE()), lp::Col("embedding")))
                     .as("total")})
            .project(
                ColumnExprs::concat(
                    ColumnExprs::renamed(partitions, "n_"),
                    {lp::Col("cluster_id").as("n_cluster_id"),
                     lp::Cast(
                         ARRAY(REAL()),
                         lp::Call(
                             "array_normalize", lp::Col("total"), lp::Lit(2.0)))
                         .as("n_centroid")}));

    // A cluster without points keeps its centroid.
    auto step =
        state()
            .join(
                means,
                *ColumnExprs::equal(partitions, "n_") &&
                    lp::Col("cluster_id") == lp::Col("n_cluster_id"),
                lp::JoinType::kLeft)
            .project(
                ColumnExprs::concat(
                    keys,
                    {lp::Col("cluster_id"),
                     lp::Call(
                         "coalesce", lp::Col("n_centroid"), lp::Col("centroid"))
                         .as("centroid")}));

    auto keyColumns = partitions;
    keyColumns.emplace_back("cluster_id");
    return seeds.iterate("centroids", step, kNumIterations, keyColumns);
  }

 private:
  static constexpr int64_t kPointsPerCluster{256};
  static constexpr int64_t kCandidatesPerCluster{4};
  static constexpr int32_t kNumIterations{10};
};

// A mistake in a trainer's result that only running the refresh finds.
enum class Mistake {
  kIdOutOfRange,
  kIdGap,
  kShortCentroid,
  kZeroCentroid,
  kNullId,
  kNullCentroid,
  kNoCentroids,
};

// Returns the centroids of SphericalKMeans with a mistake.
class MistakenKMeans : public VectorIndexCodecTrainer {
 public:
  explicit MistakenKMeans(Mistake mistake) : mistake_{mistake} {}

  lp::PlanBuilder plan(const VectorIndexSample& sample) const override {
    auto centroids = SphericalKMeans().plan(sample);
    const auto columns = centroids.findOrAssignOutputNames();
    const auto numPartitionColumns = sample.partitionColumns->size();
    auto id = columns[numPartitionColumns].toCol();
    auto centroid = columns.back().toCol();
    const auto first = id == lp::Lit(int64_t{0});
    switch (mistake_) {
      case Mistake::kIdOutOfRange:
        // IDs 1 to K.
        id = id + lp::Lit(int64_t{1});
        break;
      case Mistake::kIdGap:
        // IDs 0 and 2, without 1.
        centroids.filter(id != lp::Lit(int64_t{1}));
        break;
      case Mistake::kShortCentroid:
        centroid = lp::Call("slice", centroid, lp::Lit(1), lp::Lit(1));
        break;
      case Mistake::kZeroCentroid:
        centroid =
            lp::Call("transform", centroid, lp::Lambda({"x"}, lp::Lit(0.0f)));
        break;
      case Mistake::kNullId:
        id = lp::Call("if", first, lp::NullLit(BIGINT()), id);
        break;
      case Mistake::kNullCentroid:
        centroid = lp::Call("if", first, lp::NullLit(ARRAY(REAL())), centroid);
        break;
      case Mistake::kNoCentroids:
        centroids.filter(lp::Lit(false));
        break;
    }

    std::vector<lp::ExprApi> projections;
    projections.reserve(columns.size());
    for (size_t i = 0; i < numPartitionColumns; ++i) {
      projections.push_back(columns[i].toCol());
    }
    projections.push_back(id.as("cluster_id"));
    projections.push_back(centroid.as("centroid"));
    return centroids.project(projections);
  }

 private:
  const Mistake mistake_;
};

// Plans as many centroids for each partition as its row count, up to K, so
// that the codec's size shows the row count the partition's sample carries.
// It reads only the row count, which every sampled row carries, from a sample
// of K rows, which holds all the valid rows of a partition of at most K.
class RowCountCentroids : public VectorIndexCodecTrainer {
 public:
  lp::PlanBuilder plan(const VectorIndexSample& sample) const override {
    auto rows = sample.makeSample(sample.properties.numClusters);
    const auto columns = rows.findOrAssignOutputNames();
    const auto numPartitionColumns = sample.partitionColumns->size();
    std::vector<lp::ExprApi> partitions;
    partitions.reserve(numPartitionColumns);
    for (size_t i = 0; i < numPartitionColumns; ++i) {
      partitions.push_back(columns[i].toCol());
    }
    const auto ordinal = lp::Col("ordinal");
    auto output = partitions;
    output.push_back((ordinal - lp::Lit(int64_t{1})).as("cluster"));
    output.push_back(
        lp::Call("array_constructor", lp::Lit(1.0f), lp::Cast(REAL(), ordinal))
            .as("centroid"));
    return rows
        .aggregate(
            partitions,
            {lp::Call("arbitrary", columns.back().toCol()).as("num_rows")})
        .unnest({lp::Call(
                     "sequence",
                     lp::Lit(int64_t{1}),
                     lp::Call(
                         "least",
                         lp::Col("num_rows"),
                         lp::Lit(sample.properties.numClusters)))
                     .unnestAs("ordinal")})
        .project(output);
  }
};

// Drives REFRESH VECTOR INDEX through the CLI over a Hive connector backed by
// a temporary directory, with SphericalKMeans as the codec trainer.
class VectorIndexCodecTrainerTest : public SqlQueryRunnerTestBase {
 protected:
  // Creates the table 't' of points, (id, embedding, ds), partitioned by ds,
  // with the index 'i' of 3 clusters over the embeddings, keyed by the id.
  void SetUp() override {
    dataPath_ = exec::test::TempDirectoryPath::create();
    useTrainer(std::make_shared<SphericalKMeans>());
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

  // Replaces runner_ with one over the same data with 'trainer'.
  void useTrainer(std::shared_ptr<const VectorIndexCodecTrainer> trainer) {
    vectorIndexCodecTrainer_ = std::move(trainer);
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

  // Inserts into partition 'ds' 15 points with ids 1 to 15, in 3 clusters
  // around the directions of 0, 120 and 240 degrees. A cluster's points are at
  // -20, -10, 0, 10 and 20 degrees from its direction, so its normalized sum
  // is its direction. In order of sample hash, the points take turns between
  // the clusters, starting at -20 degrees: the 3 points of smallest hash,
  // which seed SphericalKMeans, are one per cluster, 20 degrees off.
  void insertClusters(std::string_view ds) {
    run(fmt::format(
        "INSERT INTO t "
        "SELECT id, ARRAY[CAST(cos(a) AS REAL), CAST(sin(a) AS REAL)], '{}' "
        "FROM ("
        "   SELECT id, radians(120 * (r % 3) + 10 * (r / 3) - 20) AS a "
        "   FROM ("
        "       SELECT id, row_number() OVER ("
        "           ORDER BY from_big_endian_64(xxhash64(to_big_endian_64(id)))"
        "       ) - 1 AS r "
        "       FROM UNNEST(sequence(1, 15)) AS u(id)))",
        ds));
  }

  // Returns the number of centroids in the codec of partition 'ds'.
  int64_t numCentroids(std::string_view ds) {
    return fetchSingleInt64(
        fmt::format(
            "SELECT vector_count(codec) FROM t__i__codecs WHERE ds = '{}'",
            ds));
  }

  // Returns each point of partition 'ds' with its nearest centroid of the
  // partition's codec and their cosine similarity, rounded to 4 places, as
  // "id:cluster_id:similarity" triples, sorted and joined by commas.
  std::string nearestCentroids(std::string_view ds) {
    return fetchSingleValue<std::string>(fmt::format(
        "SELECT array_join(array_sort(array_agg("
        "   CAST(q AS VARCHAR) || ':' || CAST(cluster_id AS VARCHAR) || ':' || "
        "   CAST(round(score, 4) AS VARCHAR))), ',') "
        "FROM TABLE(vector_search("
        "   queries => TABLE(SELECT id AS q, embedding FROM t WHERE ds = '{0}'), "
        "   corpus => TABLE(SELECT * FROM \"t$i$centroids\" WHERE ds = '{0}'), "
        "   embeddings => DESCRIPTOR(embedding, centroid), "
        "   metric => 'cosine', "
        "   k => 1, "
        "   score => DESCRIPTOR(score)))",
        ds));
  }

  std::shared_ptr<exec::test::TempDirectoryPath> dataPath_;
  // Runs a query on 4 workers with 4 drivers each.
  const SqlQueryRunner::RunOptions distributed_{
      .numWorkers = 4,
      .numDrivers = 4,
  };
};

// What REFRESH does with a trainer: it trains with the trainer instead of
// vector_index_train, gives it only valid rows, and packs its centroids into
// each partition's codec under its IDs. With SphericalKMeans, a partition with
// n valid rows gets min(n, K) centroids with IDs 0 to min(n, K) - 1, a
// partition without valid rows gets a NULL codec, and the index answers
// searches. A trainer may fail the refresh for a partition it can't train,
// naming the partition.
TEST_F(VectorIndexCodecTrainerTest, refresh) {
  run(fmt::format(
      "INSERT INTO t VALUES "
      "(1, ARRAY[REAL '1', REAL '0'], 'a'), "
      "(1, ARRAY[REAL '1', REAL '0'], 'b'), "
      "(2, ARRAY[REAL '0', REAL '1'], 'b'), "
      "(3, ARRAY[REAL '-1', REAL '0'], 'b'), "
      "{}, {}",
      invalidRows("a"),
      invalidRows("d")));
  insertClusters("c");
  EXPECT_EQ(
      fetchSingleInt64(
          "REFRESH VECTOR INDEX i ON t WHERE ds IN ('a', 'b', 'c', 'd')",
          distributed_),
      19);

  // A partition has min(valid rows, num_clusters) centroids, with IDs from
  // zero, and a local index per cluster.
  for (const auto& [ds, numRows, numClusters, ids] : {
           std::tuple{"a", 1, 1, "0"},
           std::tuple{"b", 3, 3, "0,1,2"},
           std::tuple{"c", 15, 3, "0,1,2"},
       }) {
    SCOPED_TRACE(ds);
    EXPECT_EQ(numCentroids(ds), numClusters);
    EXPECT_EQ(
        fetchSingleInt64(
            fmt::format(
                "SELECT vector_count(local_index) FROM t__i__index WHERE ds = '{}'",
                ds)),
        numRows);
    EXPECT_EQ(
        fetchSingleValue<std::string>(fmt::format(
            "SELECT array_join(array_sort(array_agg(cluster_id)), ',') "
            "FROM t__i__index "
            "WHERE ds = '{}'",
            ds)),
        ids);
  }
  EXPECT_EQ(
      fetchSingleInt64(
          "SELECT count(*) FROM t__i__codecs WHERE ds = 'd' AND codec IS NULL"),
      1);

  // The seeds of 'c' are 20 degrees off its clusters' directions, and the
  // updates move each centroid to its cluster's direction, so each cluster's
  // local index holds its 5 points.
  EXPECT_EQ(
      fetchSingleInt64(
          "SELECT count(DISTINCT cluster_id) "
          "FROM TABLE(vector_search("
          "   queries => TABLE("
          "       SELECT * "
          "       FROM (VALUES "
          "           (0, ARRAY[REAL '1', REAL '0']), "
          "           (120, ARRAY[REAL '-0.5', REAL '0.8660254']), "
          "           (240, ARRAY[REAL '-0.5', REAL '-0.8660254'])) "
          "           AS d(direction, embedding)), "
          "   corpus => TABLE(SELECT * FROM \"t$i$centroids\" WHERE ds = 'c'), "
          "   embeddings => DESCRIPTOR(embedding, centroid), "
          "   metric => 'cosine', "
          "   k => 1, "
          "   score => DESCRIPTOR(score))) "
          "WHERE score > 0.9999"),
      3);
  EXPECT_EQ(
      fetchSingleValue<std::string>("SELECT array_join(array_agg(n), ',') "
                                    "FROM ("
                                    "   SELECT vector_count(local_index) AS n "
                                    "   FROM t__i__index "
                                    "   WHERE ds = 'c' "
                                    "   GROUP BY cluster_id)"),
      "5,5,5");

  // Probing only the query's nearest cluster finds the point in its direction.
  EXPECT_EQ(
      fetchSingleInt64(
          "SELECT id "
          "FROM TABLE(vector_search("
          "   query => ARRAY[REAL '0', REAL '1'], "
          "   corpus => TABLE(SELECT * FROM t WHERE ds = 'b'), "
          "   embeddings => DESCRIPTOR(embedding), "
          "   metric => 'cosine', "
          "   k => 1, "
          "   hints => MAP(ARRAY['num_probes'], ARRAY['1'])))"),
      2);

  // SphericalKMeans seeds a partition of more than 4 * K rows, valid or not,
  // from about 4 * K of them, and fails the refresh when fewer than K of those
  // are valid: here 2 of 13 rows are valid.
  std::vector<std::string> rows{
      "(1, ARRAY[REAL '1', REAL '0'], 'e')",
      "(2, ARRAY[REAL '0', REAL '1'], 'e')",
  };
  for (int32_t id = 3; id <= 13; ++id) {
    rows.push_back(fmt::format("({}, CAST(NULL AS ARRAY(REAL)), 'e')", id));
  }
  run(fmt::format("INSERT INTO t VALUES {}", fmt::join(rows, ", ")));
  VELOX_ASSERT_THROW(
      run("REFRESH VECTOR INDEX i ON t WHERE ds = 'e'"),
      "Too few valid rows to seed the clusters of partition: e");
}

// A trainer that reads only the documented columns gets the same sample, and
// so the same centroids, on any number of workers and drivers, whatever order
// the rows are stored in. Two partitions of the same rows, stored in opposite
// orders and refreshed on one driver and on 4 workers of 4 drivers, get the
// same centroids under the same IDs.
TEST_F(VectorIndexCodecTrainerTest, sampleOrder) {
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
  EXPECT_EQ(numCentroids("a"), 3);
  EXPECT_EQ(nearestCentroids("a"), nearestCentroids("b"));
}

// The sample carries each partition's row count, valid or not, which REFRESH
// reads from the connector's metadata. A condition the metadata can't
// evaluate fails the refresh.
TEST_F(VectorIndexCodecTrainerTest, rowCount) {
  // A partition 'a' of 1 valid and 1 invalid row, a partition 'b' of 3 valid
  // rows, and 'cc' and 'dd' of the same rows.
  const auto rows = [](std::string_view mixed, std::string_view valid) {
    return fmt::format(
        "(1, ARRAY[REAL '1', REAL '0'], '{0}'), "
        "(2, ARRAY[REAL '0', REAL '0'], '{0}'), "
        "(1, ARRAY[REAL '1', REAL '0'], '{1}'), "
        "(2, ARRAY[REAL '0', REAL '1'], '{1}'), "
        "(3, ARRAY[REAL '-1', REAL '0'], '{1}')",
        mixed,
        valid);
  };
  run(fmt::format(
      "INSERT INTO t VALUES {}, {}", rows("a", "b"), rows("cc", "dd")));
  useTrainer(std::make_shared<RowCountCentroids>());

  run("REFRESH VECTOR INDEX i ON t WHERE ds IN ('a', 'b')");
  EXPECT_EQ(numCentroids("a"), 2);
  EXPECT_EQ(numCentroids("b"), 3);

  VELOX_ASSERT_THROW(
      run("REFRESH VECTOR INDEX i ON t WHERE length(ds) = 2"),
      "Aggregate cannot be answered from metadata and has no fallback");
}

// Which check catches each mistake in a trainer's result that planning can't
// see. vector_index_pack checks the values when the refresh runs: IDs below K
// and without gaps, centroids of length D that are not all zeros, and no NULL
// ID or centroid. A partition with valid rows but no centroids gets a NULL
// codec, which fails the build, since the build finds the partition's rows.
TEST_F(VectorIndexCodecTrainerTest, invalidCentroids) {
  run(fmt::format(
      "INSERT INTO t VALUES {}", pointRows("a", /*reversed=*/false)));

  useTrainer(std::make_shared<MistakenKMeans>(Mistake::kIdOutOfRange));
  VELOX_ASSERT_THROW(
      run("REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"),
      "Cluster id must be less than the number of clusters");

  useTrainer(std::make_shared<MistakenKMeans>(Mistake::kIdGap));
  VELOX_ASSERT_THROW(
      run("REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"),
      "Cluster ids must be in [0, number of clusters) range");

  useTrainer(std::make_shared<MistakenKMeans>(Mistake::kShortCentroid));
  VELOX_ASSERT_THROW(
      run("REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"),
      "Cluster centroid must have the given dimension");

  useTrainer(std::make_shared<MistakenKMeans>(Mistake::kZeroCentroid));
  VELOX_ASSERT_THROW(
      run("REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"),
      "Cluster centroid must not be all zeros");

  useTrainer(std::make_shared<MistakenKMeans>(Mistake::kNullId));
  VELOX_ASSERT_THROW(
      run("REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"),
      "Cluster id must not be null");

  useTrainer(std::make_shared<MistakenKMeans>(Mistake::kNullCentroid));
  VELOX_ASSERT_THROW(
      run("REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"),
      "Cluster centroid must not be null");

  useTrainer(std::make_shared<MistakenKMeans>(Mistake::kNoCentroids));
  VELOX_ASSERT_THROW(
      run("REFRESH VECTOR INDEX i ON t WHERE ds = 'a'"),
      "No codec for partition: a");
}

} // namespace
} // namespace axiom::sql
