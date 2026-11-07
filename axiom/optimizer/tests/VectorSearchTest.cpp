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

#include <folly/String.h>
#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

// Product search:
// - 'queries' holds what to search for: a 'name', an 'embedding' and the day
//   'ds' to search.
// - 'products' holds products, of ids 'id', embeddings 'embedding' and titles
//   'title', with a partition per day 'ds'. Its vector index 'pi' indexes the
//   embeddings, keyed by the id.
//
// A search of 'products' reads the index's codec table, 'products__pi__codecs',
// and its index table, 'products__pi__index'.
class VectorSearchTest : public test::QueryTestBase {
 protected:
  // The partitions of a partitioned table.
  static inline const std::vector<Variant> kPartitions{
      Variant::row({Variant("2026-01-01")}),
      Variant::row({Variant("2026-01-02")}),
  };

  // The properties of a vector index.
  static inline const folly::F14FastMap<std::string, Variant> kIndexProperties{
      {"distance_metric", Variant("cosine")},
      {"index_type", Variant("flat")},
      {"num_clusters", Variant(int64_t{4})},
      {"dimension", Variant(int64_t{2})},
  };

  void SetUp() override {
    QueryTestBase::SetUp();
    testConnector_->addTable(
        "queries",
        ROW({
            {"name", VARCHAR()},
            {"embedding", ARRAY(REAL())},
            {"ds", VARCHAR()},
        }));
    testConnector_
        ->addPartitionedTable(
            "products",
            ROW({
                {"id", BIGINT()},
                {"embedding", ARRAY(REAL())},
                {"title", VARCHAR()},
                {"ds", VARCHAR()},
            }),
            {"ds"})
        ->setDiscreteValues({"ds"}, kPartitions);
    addVectorIndex("products", "pi", "id");
  }

  // Adds the vector index 'name' of 'table' over its 'embedding', keyed by
  // 'keyColumn'. The index covers the table's partitions, kPartitions, and its
  // index table is large.
  void addVectorIndex(
      const std::string& table,
      const std::string& name,
      const std::string& keyColumn) {
    const auto index = testConnector_->addVectorIndex(
        table,
        {.name = name,
         .column = "embedding",
         .key = {keyColumn},
         .properties = kIndexProperties});
    testConnector_->findTable(index->indexTable)
        ->setDiscreteValues({"ds"}, kPartitions)
        .setStats(/*numRows=*/1'000'000, {});
  }

  // The hints of a search, by name.
  using Hints = std::vector<std::pair<std::string, std::string>>;

  // Returns a query of 'columns' of a search through 'corpus', a table or a
  // query, for the 2 rows nearest to each query, with their score as 'score'.
  static std::string searchQuery(
      std::string_view columns,
      std::string_view corpus,
      const Hints& hints) {
    std::vector<std::string> names;
    std::vector<std::string> values;
    for (const auto& [name, value] : hints) {
      names.push_back(fmt::format("'{}'", name));
      values.push_back(fmt::format("'{}'", value));
    }
    return fmt::format(
        "SELECT {} "
        "FROM TABLE(vector_search("
        "   queries => TABLE(SELECT name, embedding FROM queries), "
        "   corpus => TABLE({}), embeddings => DESCRIPTOR(embedding), "
        "   metric => 'cosine', "
        "   k => 2, "
        "   score => DESCRIPTOR(score)"
        "   {}" // hints
        "))",
        columns,
        corpus,
        hints.empty() ? ""
                      : fmt::format(
                            ", hints => MAP(ARRAY[{}], ARRAY[{}])",
                            folly::join(", ", names),
                            folly::join(", ", values)));
  }

  // Matches the search of 'products' through the index 'index' for the 2
  // nearest per query: each query, filtered by 'queryFilter' if set, is
  // assigned to 'numProbes' clusters of each day's codec, whose local indexes
  // give its nearest products, of which the 2 nearest are kept. Both storage
  // tables read only the day 'ds', if set. With 'distributed', each worker
  // keeps the 2 nearest of its matches, which are then repartitioned on the
  // query before the 2 nearest of all are kept. Names the output 'name',
  // 'query_id', 'id' and 'score'.
  core::PlanMatcherBuilder matchSearch(
      const std::optional<std::string>& queryFilter,
      const std::optional<std::string>& ds,
      int64_t numProbes,
      std::string_view index = "pi",
      bool distributed = false) {
    auto matcher = matchScan("queries");
    matcher.aliases({"name", "embedding"})
        .filterIf(queryFilter.has_value(), queryFilter.value_or(""))
        .assignUniqueId("query_id")
        .vectorSearchJoin(
            matchScan(fmt::format("products__{}__codecs", index))
                .aliases({"codec", "codec_id", "ds"})
                .filterIf(
                    ds.has_value(), fmt::format("ds = '{}'", ds.value_or("")))
                .broadcastIf(distributed),
            core::JoinType::kInner,
            {},
            "embedding",
            "codec",
            numProbes)
        .aliases(
            {"name", "embedding", "query_id", "codec_id", "ds", "cluster_id"})
        .shuffleIf(distributed, {"ds", "codec_id", "cluster_id"})
        .vectorSearchJoin(
            matchScan(fmt::format("products__{}__index", index))
                .aliases(
                    {"index_codec_id",
                     "index_cluster_id",
                     "local_index",
                     "index_ds"})
                .filterIf(
                    ds.has_value(),
                    fmt::format("index_ds = '{}'", ds.value_or("")))
                .shuffleIf(
                    distributed,
                    {"index_ds", "index_codec_id", "index_cluster_id"}),
            core::JoinType::kInner,
            {
                "ds = index_ds",
                "codec_id = index_codec_id",
                "cluster_id = index_cluster_id",
            },
            "embedding",
            "local_index",
            2)
        .aliases({"name", "query_id", "id", "score"});
    if (distributed) {
      matcher.partialTopNRowNumber({"query_id"}, {"score DESC"}, 2)
          .shuffle({"query_id"})
          .localPartition({"query_id"});
    }
    matcher.topNRowNumber(
        {"query_id"}, {"score DESC"}, 2, /*rankColumn=*/false);
    return matcher;
  }
};

// A search through an index assigns each query to clusters of each day's
// codec, searches their local indexes, and keeps the nearest products of each
// query. A filter on the day reads only that day of both storage tables.
// Without a num_probes hint, a query is assigned to the default number of
// clusters: 1 of the index's 4.
TEST_F(VectorSearchTest, search) {
  for (const auto& [hints, numProbes] :
       {std::pair<Hints, int64_t>{{{"num_probes", "3"}}, 3}, {{}, 1}}) {
    SCOPED_TRACE(numProbes);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(searchQuery(
            "name, id, ds, score",
            "SELECT * FROM products WHERE ds = '2026-01-01'",
            hints))),
        matchSearch(
            /*queryFilter=*/std::nullopt, "2026-01-01", numProbes)
            .project({"name", "id", "'2026-01-01'", "score"})
            .build());
  }

  // The same filter written above a projection of the products.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(searchQuery(
          "name, id",
          "SELECT * "
          "FROM (SELECT id, embedding, ds AS day FROM products) "
          "WHERE day = '2026-01-01'",
          {{"num_probes", "3"}}))),
      matchSearch(
          /*queryFilter=*/std::nullopt, "2026-01-01", 3)
          .project({"name", "id"})
          .build());
}

// The codecs go to every worker, where the queries are assigned to clusters.
// The assignments and the local indexes meet repartitioned on the day, the
// codec and the cluster. Each query's nearest products are kept per worker,
// then across workers.
TEST_F(VectorSearchTest, placement) {
  const auto plan = parseSelect(searchQuery(
      "name, id",
      "SELECT * FROM products WHERE ds = '2026-01-01'",
      {{"num_probes", "3"}}));
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(plan),
      matchSearch(
          /*queryFilter=*/std::nullopt, "2026-01-01", 3)
          .project({"name", "id"})
          .build());
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(plan).plan,
      matchSearch(
          /*queryFilter=*/std::nullopt,
          "2026-01-01",
          3,
          "pi",
          /*distributed=*/true)
          .project({"name", "id"})
          .gather()
          .build());
}

// A search of an index's centroids is one search of the codecs, whose primary
// key is the day and the codec id. A query that matches at most one codec gets
// that codec's nearest clusters. Otherwise the search keeps its nearest
// clusters across the codecs it matches.
TEST_F(VectorSearchTest, centroids) {
  const auto searchCentroids = [&](lp::JoinType joinType,
                                   const std::string& condition) {
    lp::PlanBuilder plan(makeContext());
    return plan.tableScan("queries")
        .as("q")
        .vectorSearch(
            plan.newPlan().tableScan("products$pi$centroids").as("p"),
            condition,
            joinType,
            {
                .probeEmbedding = "embedding",
                .buildEmbedding = "centroid",
                .k = 1,
                .scoreName = "score",
            })
        .project({"name", "cluster_id", "p.ds"})
        .build();
  };

  // The codecs, filtered by 'filter' if set.
  const auto matchCodecs = [](const std::optional<std::string>& filter) {
    auto codecs = matchScan("products__pi__codecs");
    if (filter.has_value()) {
      codecs.aliases({"codec", "products_ds", "codec_id"}).filter(*filter);
    } else {
      codecs.aliases({"codec", "products_ds"});
    }
    return codecs.project({"codec", "products_ds"});
  };

  // A search that matches each query with at most one codec: that codec's
  // nearest clusters are the result.
  const auto matchOneCodec = [&](core::PlanMatcherBuilder queries,
                                 const std::optional<std::string>& codecFilter,
                                 core::JoinType joinType,
                                 const std::vector<std::string>& keys) {
    return queries
        .vectorSearchJoin(
            matchCodecs(codecFilter), joinType, keys, "embedding", "codec", 1)
        .aliases({"name", "products_ds", "cluster_id"})
        .project({"name", "cluster_id", "products_ds"})
        .build();
  };

  // Keyed by day, on one codec id: one codec. REFRESH runs this left search.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(searchCentroids(
          lp::JoinType::kLeft, "q.ds = p.ds AND p.codec_id = 7")),
      matchOneCodec(
          matchScan("queries"),
          "codec_id = 7",
          core::JoinType::kLeft,
          {"ds = products_ds"}));

  // Conditions on the queries' columns and on the codecs' day filter the
  // inputs.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(searchCentroids(
          lp::JoinType::kInner,
          "q.ds = p.ds AND q.name LIKE 'red%' AND p.ds > '2026-01-01' AND "
          "p.codec_id = 7")),
      matchOneCodec(
          matchScan("queries").filter("name LIKE 'red%'"),
          "codec_id = 7 AND products_ds > '2026-01-01'",
          core::JoinType::kInner,
          {"ds = products_ds"}));

  // One day and one codec id: one codec.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(searchCentroids(
          lp::JoinType::kInner, "p.ds = '2026-01-01' AND p.codec_id = 7")),
      matchOneCodec(
          matchScan("queries"),
          "codec_id = 7 AND products_ds = '2026-01-01'",
          core::JoinType::kInner,
          {}));

  // A search that may match a query with several codecs: each query gets an
  // id, and the search keeps its nearest cluster across the codecs.
  const auto matchNearestAcrossCodecs =
      [&](const std::optional<std::string>& codecFilter,
          core::JoinType joinType,
          const std::vector<std::string>& keys) {
        return matchScan("queries")
            .assignUniqueId("query_id")
            .vectorSearchJoin(
                matchCodecs(codecFilter),
                joinType,
                keys,
                "embedding",
                "codec",
                1)
            .aliases({"name", "query_id", "products_ds", "cluster_id", "score"})
            .topNRowNumber(
                {"query_id"}, {"score DESC"}, 1, /*rankColumn=*/false)
            .project({"name", "cluster_id", "products_ds"})
            .build();
      };

  // One codec id and any day: a codec per day.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(searchCentroids(lp::JoinType::kInner, "p.codec_id = 7")),
      matchNearestAcrossCodecs("codec_id = 7", core::JoinType::kInner, {}));

  // Keyed by day, any codec id: a day may have several codecs.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(searchCentroids(lp::JoinType::kInner, "q.ds = p.ds")),
      matchNearestAcrossCodecs(/*codecFilter=*/std::nullopt,
                               core::JoinType::kInner,
                               {"ds = products_ds"}));

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(searchCentroids(lp::JoinType::kLeft, "q.ds = p.ds")),
      matchNearestAcrossCodecs(/*codecFilter=*/std::nullopt,
                               core::JoinType::kLeft,
                               {"ds = products_ds"}));
}

// A deterministic filter on the queries' columns applies to the queries before
// the search. A filter on the product id, or a nondeterministic one, applies
// to the search's results.
TEST_F(VectorSearchTest, filters) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(
          searchQuery("name, id", "products", {{"num_probes", "3"}}) +
          " WHERE name LIKE 'red%' AND id < 4")),
      matchSearch(
          "name LIKE 'red%'",
          /*ds=*/std::nullopt,
          3)
          .filter("id < 4")
          .project({"name", "id"})
          .build());
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(
          searchQuery("name, id", "products", {{"num_probes", "3"}}) +
          " WHERE rand() < 0.5")),
      matchSearch(
          /*queryFilter=*/std::nullopt,
          /*ds=*/std::nullopt,
          3)
          .filter("rand() < 0.5")
          .project({"name", "id"})
          .build());

  // The search's own condition on one input filters that input: on the
  // products, it reads only that day of both storage tables; on the queries,
  // it applies before the search.
  const lp::PlanBuilder plans(makeContext());
  const auto searchWithCondition = [&](const std::string& condition) {
    return toSingleNodePlan(
        plans.newPlan()
            .tableScan("queries")
            .as("q")
            .vectorSearch(
                plans.newPlan().tableScan("products").as("p"),
                condition,
                lp::JoinType::kInner,
                {.probeEmbedding = "embedding",
                 .buildEmbedding = "embedding",
                 .k = 2,
                 .scoreName = "score"})
            .project({"name", "id", "score"})
            .build());
  };
  AXIOM_ASSERT_PLAN(
      searchWithCondition("p.ds = '2026-01-01'"),
      matchSearch(/*queryFilter=*/std::nullopt, "2026-01-01", /*numProbes=*/1)
          .project({"name", "id", "score"})
          .build());
  AXIOM_ASSERT_PLAN(
      searchWithCondition("q.name LIKE 'red%'"),
      matchSearch("name LIKE 'red%'", /*ds=*/std::nullopt, /*numProbes=*/1)
          .project({"name", "id", "score"})
          .build());
}

// A limit applies to the search's results.
TEST_F(VectorSearchTest, limit) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(
          searchQuery("name, id", "products", {{"num_probes", "3"}}) +
          " LIMIT 3")),
      matchSearch(
          /*queryFilter=*/std::nullopt,
          /*ds=*/std::nullopt,
          3)
          .project({"name", "id"})
          .finalLimit(0, 3)
          .aliases({"name", "id"})
          .project({"name", "id"})
          .build());
}

TEST_F(VectorSearchTest, errors) {
  const lp::PlanBuilder plans(makeContext());
  const lp::PlanBuilder::VectorSearch rows{
      .probeEmbedding = "embedding",
      .buildEmbedding = "embedding",
      .k = 2,
      .scoreName = "score"};
  // Searches 'products' for each query, read as 'q', with 'condition' and
  // 'joinType'.
  auto search = [&](const lp::PlanBuilder& products,
                    const std::string& condition,
                    lp::JoinType joinType,
                    const lp::PlanBuilder::VectorSearch& options) {
    return toSingleNodePlan(
        plans.newPlan()
            .tableScan("queries")
            .as("q")
            .vectorSearch(products, condition, joinType, options)
            .project({"name", "score"})
            .build());
  };
  auto products = plans.newPlan().tableScan("products").as("p");

  // A search reads a column through a vector index on it with its metric, the
  // one the index hint names if set.
  VELOX_ASSERT_THROW(
      toSingleNodePlan(
          parseSelect(searchQuery("*", "SELECT embedding FROM queries", {}))),
      "Vector search requires a vector index on \"default\".\"queries\".embedding using cosine");
  VELOX_ASSERT_THROW(
      toSingleNodePlan(parseSelect(
          searchQuery("name, id", "products", {{"index", "no_such_index"}}))),
      "Vector index hint names no index of the table: no_such_index");
  testConnector_->addTable(
      "two_embeddings",
      ROW({
          {"id", BIGINT()},
          {"embedding", ARRAY(REAL())},
          {"other_embedding", ARRAY(REAL())},
      }));
  testConnector_->addVectorIndex(
      "two_embeddings",
      {.name = "other_index",
       .column = "other_embedding",
       .key = {"id"},
       .properties = kIndexProperties});
  VELOX_ASSERT_THROW(
      toSingleNodePlan(parseSelect(searchQuery(
          "name, id", "two_embeddings", {{"index", "other_index"}}))),
      "Vector index hint must name an index on the searched column: other_index on \"default\".\"two_embeddings\"");

  // A search fails when the index does not cover a partition it reads: an
  // index with no partitions recorded covers none.
  testConnector_
      ->addPartitionedTable(
          "t",
          ROW({
              {"id", BIGINT()},
              {"embedding", ARRAY(REAL())},
              {"ds", VARCHAR()},
          }),
          {"ds"})
      ->setDiscreteValues({"ds"}, kPartitions);
  testConnector_->addVectorIndex(
      "t",
      {.name = "i",
       .column = "embedding",
       .key = {"id"},
       .properties = kIndexProperties});
  VELOX_ASSERT_THROW(
      toSingleNodePlan(parseSelect(searchQuery("name, id", "t", {}))),
      "Vector index does not cover a partition the search reads: i of \"default\".\"t\", partition (ds=2026-01-01)");

  // A search reads a column of a table, optionally filtered on its partition
  // columns.
  const auto searchCorpus = [&](std::string_view corpus) {
    return toSingleNodePlan(parseSelect(searchQuery("name, id", corpus, {})));
  };
  VELOX_ASSERT_THROW(
      searchCorpus("SELECT * FROM products LIMIT 10"),
      "A vector search must search a table, optionally filtered on its partition columns");
  VELOX_ASSERT_THROW(
      searchCorpus(
          "SELECT id, transform(embedding, x -> x + x) AS embedding, ds "
          "FROM products"),
      "A vector search must search a column of its table: \"default\".\"products\"");

  // A search through an index is an inner join without keys.
  VELOX_ASSERT_THROW(
      search(products, "", lp::JoinType::kLeft, rows),
      "supports only an INNER join");
  VELOX_ASSERT_THROW(
      search(products, "q.ds = p.ds", lp::JoinType::kInner, rows),
      "does not support keys yet");

  // A condition is equalities between the inputs and conditions on the build
  // columns.
  VELOX_ASSERT_THROW(
      search(products, "q.ds < p.ds", lp::JoinType::kInner, rows),
      "Vector search condition supports only equalities between probe and build expressions");

  // A filter reads only the day, and the search returns only what the index
  // holds: the id and the day.
  VELOX_ASSERT_THROW(
      searchCorpus("SELECT * FROM products WHERE id > 3"),
      "A vector search may filter its table only on partition columns");
  VELOX_ASSERT_THROW(
      searchCorpus(
          "SELECT * FROM products WHERE ds = '2026-01-01' AND rand() < 0.5"),
      "A vector search may filter its table only by deterministic conditions");
  for (const auto* column : {"title", "p.embedding"}) {
    SCOPED_TRACE(column);
    VELOX_ASSERT_THROW(
        toSingleNodePlan(
            plans.newPlan()
                .tableScan("queries")
                .as("q")
                .vectorSearch(products, "", lp::JoinType::kInner, rows)
                .project({"name", column})
                .build()),
        "Reading a column a vector index does not return is not supported yet");
  }

  // A key named like a column of the storage tables is still the key: a search
  // returns it from the matches, and a filter on it is rejected as on any
  // column other than a partition column.
  testConnector_
      ->addPartitionedTable(
          "named_like_storage",
          ROW({
              {"codec_id", BIGINT()},
              {"embedding", ARRAY(REAL())},
              {"ds", VARCHAR()},
          }),
          {"ds"})
      ->setDiscreteValues({"ds"}, kPartitions);
  addVectorIndex("named_like_storage", "named_like_storage_index", "codec_id");
  VELOX_ASSERT_THROW(
      toSingleNodePlan(parseSelect(searchQuery(
          "codec_id",
          "SELECT * FROM named_like_storage WHERE codec_id = 42",
          {}))),
      "A vector search may filter its table only on partition columns");
  EXPECT_NO_THROW(toSingleNodePlan(
      parseSelect(searchQuery("codec_id", "named_like_storage", {}))));

  // A search of the centroids is keyed only by the day and the codec id.
  const lp::PlanBuilder::VectorSearch centroids{
      .probeEmbedding = "embedding",
      .buildEmbedding = "centroid",
      .k = 1,
      .scoreName = "score"};
  const auto centroidsTable =
      plans.newPlan().tableScan("products$pi$centroids").as("p");
  VELOX_ASSERT_THROW(
      search(
          centroidsTable,
          "length(q.name) = p.cluster_id",
          lp::JoinType::kInner,
          centroids),
      "supports keys only on its partition columns and codec_id");
}

// A search reads the index the hint names. Without a hint, it reads the only
// index on the searched column with the search's metric, and fails when there
// is more than one.
TEST_F(VectorSearchTest, indexHint) {
  addVectorIndex("products", "another_index", "id");
  for (const auto* index : {"pi", "another_index"}) {
    SCOPED_TRACE(index);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(
            searchQuery("name, id", "products", {{"index", index}}))),
        matchSearch(
            /*queryFilter=*/std::nullopt,
            /*ds=*/std::nullopt,
            /*numProbes=*/1,
            index)
            .project({"name", "id"})
            .build());
  }
  VELOX_ASSERT_THROW(
      toSingleNodePlan(parseSelect(searchQuery("name, id", "products", {}))),
      "Found multiple matching vector indexes. Use the index hint to specify which one to use: \"default\".\"products\".embedding using cosine, indexes: pi, another_index");
}

} // namespace
} // namespace facebook::axiom::optimizer
