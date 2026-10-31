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

#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

// 't' holds the queries: an id 'a', an embedding 'b' and a partition 'c'. 'u'
// holds the vectors to index: a partition 'd', an id 'e' and an embedding 'f'.
class VectorSearchTest : public test::QueryTestBase {
 protected:
  void SetUp() override {
    QueryTestBase::SetUp();

    // Two partitions of two vectors each.
    testConnector_
        ->addTable(
            "u", ROW({"d", "e", "f"}, {VARCHAR(), BIGINT(), ARRAY(REAL())}))
        ->addData(makeRowVector({
            makeFlatVector<std::string>({"x", "x", "y", "y"}),
            makeFlatVector<int64_t>({1, 2, 3, 4}),
            makeArrayVector<float>({{1, 0}, {0, 1}, {1, 1}, {-1, 1}}),
        }));

    // Each query is nearest to one vector of its partition. Partition 'z' has
    // no vectors.
    testConnector_
        ->addTable(
            "t", ROW({"a", "b", "c"}, {BIGINT(), ARRAY(REAL()), VARCHAR()}))
        ->addData(makeRowVector({
            makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
            makeArrayVector<float>(
                {{0.9, 0.1}, {0.2, 0.8}, {0.5, 0.6}, {-0.9, 1}, {1, 0}}),
            makeFlatVector<std::string>({"x", "x", "y", "y", "z"}),
        }));
  }

  // Searches each query of 't' that 'queryFilter' keeps for the nearest vector
  // in each index it matches. 'indexKeys' group the vectors of 'u' that
  // 'vectorFilter' keeps into indexes, and a query matches the indexes whose
  // 'buildKeys' equal its 'probeKeys'. Each result has the query's columns and
  // the vector's 'id'.
  lp::PlanBuilder search(
      lp::JoinType joinType,
      const std::vector<std::string>& indexKeys,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::string& queryFilter,
      const std::string& vectorFilter) {
    auto context = makeContext();
    return lp::PlanBuilder(context)
        .tableScan("t")
        .filter(queryFilter)
        .vectorSearch(
            lp::PlanBuilder(context)
                .tableScan("u")
                .filter(vectorFilter)
                .aggregate(
                    indexKeys,
                    {"vector_index_build(f, e, 'flat', 'cosine', "
                     "cast(map() as map(varchar, varchar))) AS g"}),
            {.joinType = joinType,
             .probeKeys = probeKeys,
             .buildKeys = buildKeys,
             .probeEmbedding = "b",
             .buildIndex = "g",
             .metric = common::vectorindex::VectorIndexMetric::kCosine,
             .k = 1,
             .idName = "id",
             .scoreName = "score"});
  }

  // Searches every query in the index of its partition.
  lp::PlanBuilder searchInPartition(lp::JoinType joinType) {
    return search(joinType, {"d"}, {"c"}, {"d"}, "true", "true");
  }

  // Searches every query in each half of its partition: two indexes per
  // partition, each holding the vectors of one parity of id.
  lp::LogicalPlanNodePtr nearestInHalves() {
    return search(
               lp::JoinType::kInner,
               {"d", "e % 2 AS h"},
               {"c"},
               {"d"},
               "true",
               "true")
        .project({"a", "id"})
        .build();
  }

  // The results of 'nearestInHalves': each query finds both vectors of its
  // partition, one in each half.
  RowVectorPtr inHalves() {
    return makeRowVector({
        makeFlatVector<int64_t>({1, 1, 2, 2, 3, 3, 4, 4}),
        makeFlatVector<int64_t>({1, 2, 1, 2, 3, 4, 3, 4}),
    });
  }

  // Matches a search of the queries 'probe' produces in the indexes 'build'
  // produces.
  static core::PlanMatcherBuilder matchSearch(
      core::PlanMatcherBuilder probe,
      core::PlanMatcherBuilder build,
      core::JoinType joinType,
      const std::vector<std::string>& keys) {
    return probe.vectorSearchJoin(
        std::move(build), joinType, keys, "b", "g", /*k=*/1);
  }

  // Matches the single-node plan of 'searchInPartition' up to the search.
  core::PlanMatcherBuilder matchSearchInPartition(core::JoinType joinType) {
    return matchSearch(
        matchScan("t"),
        matchScan("u").singleAggregation(),
        joinType,
        {"c = d"});
  }
};

TEST_F(VectorSearchTest, results) {
  checkSame(
      searchInPartition(lp::JoinType::kInner).project({"a", "id"}).build(),
      {makeRowVector({
          makeFlatVector<int64_t>({1, 2, 3, 4}),
          makeFlatVector<int64_t>({1, 2, 3, 4}),
      })});

  // A query whose partition has no index keeps its row, with a null id.
  checkSame(
      searchInPartition(lp::JoinType::kLeft).project({"a", "id"}).build(),
      {makeRowVector({
          makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
          makeNullableFlatVector<int64_t>({1, 2, 3, 4, std::nullopt}),
      })});

  // One index over all vectors serves every query.
  checkSame(
      search(lp::JoinType::kInner, {}, {}, {}, "true", "true")
          .project({"a", "id"})
          .build(),
      {makeRowVector({
          makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
          makeFlatVector<int64_t>({1, 2, 3, 4, 1}),
      })});

  checkSame(nearestInHalves(), {inHalves()});
}

// Where the search runs:
// - Indexes partitioned on the keys meet the queries repartitioned on them.
// - One index for all queries goes to every worker, and the queries stay
//   where they were scanned.
// - Indexes partitioned on other columns go to every worker when they fit a
//   broadcast. Otherwise they and the queries are repartitioned on the keys.
TEST_F(VectorSearchTest, placement) {
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(
          searchInPartition(lp::JoinType::kInner).project({"a", "id"}).build())
          .plan,
      matchSearch(
          matchScan("t").shuffle({"c"}),
          matchScan("u")
              .partialAggregation()
              .shuffle({"d"})
              .localPartition({"d"})
              .finalAggregation(),
          core::JoinType::kInner,
          {"c = d"})
          .gather()
          .build());

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(search(lp::JoinType::kInner, {}, {}, {}, "true", "true")
                    .project({"a", "id"})
                    .build())
          .plan,
      matchSearch(
          matchScan("t"),
          matchScan("u")
              .partialAggregation()
              .gather()
              .localGather()
              .finalAggregation()
              .broadcast(),
          core::JoinType::kInner,
          {})
          .gather()
          .build());

  auto halves = [&] {
    return matchScan("u")
        .project({"d", "e % 2 AS h", "f", "e"})
        .partialAggregation()
        .shuffle({"d", "h"})
        .localPartition({"d", "h"})
        .finalAggregation()
        .project({"d", "g"});
  };

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(nearestInHalves()).plan,
      matchSearch(
          matchScan("t"),
          halves().broadcast(),
          core::JoinType::kInner,
          {"c = d"})
          .gather()
          .build());

  optimizerOptions_.broadcastSizeLimit = 0;
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(nearestInHalves()).plan,
      matchSearch(
          matchScan("t").shuffle({"c"}),
          halves().shuffle({"d"}),
          core::JoinType::kInner,
          {"c = d"})
          .gather()
          .build());
  checkSame(nearestInHalves(), {inHalves()});
}

// A deterministic filter on the queries' columns applies to the queries before
// the search. A filter on the id applies to the search's results, as does a
// filter on the queries' columns that is not deterministic: before the search,
// it would keep or drop all the results of a query together.
TEST_F(VectorSearchTest, filters) {
  for (const auto joinType : {lp::JoinType::kInner, lp::JoinType::kLeft}) {
    SCOPED_TRACE(lp::JoinTypeName::toName(joinType));
    const auto searchJoinType = joinType == lp::JoinType::kInner
        ? core::JoinType::kInner
        : core::JoinType::kLeft;

    auto logicalPlan = searchInPartition(joinType)
                           .filter("a > 1 AND id < 4")
                           .project({"a", "id"})
                           .build();
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchSearch(
            matchScan("t").filter("a > 1"),
            matchScan("u").singleAggregation(),
            searchJoinType,
            {"c = d"})
            .filter("id < 4")
            .build());
    checkSame(
        logicalPlan,
        {makeRowVector({
            makeFlatVector<int64_t>({2, 3}),
            makeFlatVector<int64_t>({2, 3}),
        })});

    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(searchInPartition(joinType)
                             .filter("rand() < 0.5")
                             .project({"a", "id"})
                             .build()),
        matchSearchInPartition(searchJoinType).filter("rand() < 0.5").build());
  }
}

// No queries give no results. No vectors give no results from an inner search,
// and every query with a null id from a left one.
TEST_F(VectorSearchTest, emptyInput) {
  for (const auto joinType : {lp::JoinType::kInner, lp::JoinType::kLeft}) {
    SCOPED_TRACE(lp::JoinTypeName::toName(joinType));
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(search(joinType, {"d"}, {"c"}, {"d"}, "false", "true")
                             .project({"a", "id"})
                             .build()),
        core::PlanMatcherBuilder().values().build());
  }

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          search(lp::JoinType::kInner, {"d"}, {"c"}, {"d"}, "true", "false")
              .project({"a", "id"})
              .build()),
      core::PlanMatcherBuilder().values().build());
  checkSame(
      search(lp::JoinType::kLeft, {"d"}, {"c"}, {"d"}, "true", "false")
          .project({"a", "id"})
          .build(),
      {makeRowVector({
          makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
          makeNullConstant(TypeKind::BIGINT, 5),
      })});
}

// A limit on the results applies above the search: a query gives as many
// results as the indexes it matches return.
TEST_F(VectorSearchTest, limit) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(searchInPartition(lp::JoinType::kInner)
                           .limit(2)
                           .project({"a", "id"})
                           .build()),
      matchSearchInPartition(core::JoinType::kInner).finalLimit(0, 2).build());
}

} // namespace
} // namespace facebook::axiom::optimizer
