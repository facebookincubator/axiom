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
#include "axiom/sql/presto/KMeans.h"

#include "axiom/sql/presto/ColumnExprs.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;
using facebook::velox::REAL;
using facebook::velox::common::vectorindex::VectorIndexMetric;

namespace {

constexpr std::string_view kState{"kmeans_centroids"};
constexpr int32_t kMaxIterations{20};
// A centroid that moves less than this, in one minus cosine similarity, has
// stopped.
constexpr float kTolerance{1e-6};

// Returns 'vector' scaled to length 1:
//   transform(vector, x -> x / CAST(l2_norm(vector) AS REAL))
lp::ExprApi normalize(const lp::ExprApi& vector) {
  return lp::Call(
      "transform",
      vector,
      lp::Lambda(
          {"x"}, lp::Col("x") / lp::Cast(REAL(), lp::Call("l2_norm", vector))));
}

} // namespace

// static
lp::PlanBuilder KMeans::expand(
    const lp::PlanBuilder& points,
    const std::vector<std::string>& partitionColumns,
    const std::string& embedding,
    const std::string& sequence,
    int64_t numClusters,
    VectorIndexMetric metric) {
  VELOX_CHECK(
      metric == VectorIndexMetric::kCosine,
      "KMeans does not support metric: {}",
      facebook::velox::common::vectorindex::VectorIndexMetricName::toName(
          metric));
  const auto partitionKeys = ColumnExprs::of(partitionColumns);

  auto anchor = lp::PlanBuilder(points)
                    .filter(lp::Col(sequence) <= lp::Lit(numClusters))
                    .project(
                        ColumnExprs::concat(
                            partitionKeys,
                            {
                                (lp::Col(sequence) - lp::Lit<int64_t>(1))
                                    .as("cluster_id"),
                                normalize(lp::Col(embedding)).as("centroid"),
                                lp::Lit(0.0f).as("movement"),
                            }));

  const auto state = [&] {
    return points.newPlan().fixedPointState(std::string(kState), anchor);
  };

  // Each partition's centroids as an array of (cluster_id, centroid):
  //   array_agg(row_constructor(cluster_id, centroid)) AS centers
  auto centers = state()
                     .aggregate(
                         partitionKeys,
                         {lp::Call(
                              "array_agg",
                              lp::Call(
                                  "row_constructor",
                                  lp::Col("cluster_id"),
                                  lp::Col("centroid")))
                              .as("centers")})
                     .project(
                         ColumnExprs::concat(
                             ColumnExprs::renamed(partitionColumns, "c_"),
                             {lp::Col("centers")}));

  // The nearest center to each point, the lowest cluster on a tie, so the
  // result does not depend on the order of 'centers':
  //   array_max_by(
  //       centers,
  //       center -> row_constructor(
  //           cosine_similarity(point, center[2]), negate(center[1])))[1]
  auto nearest = lp::Call(
      "subscript",
      lp::Call(
          "array_max_by",
          lp::Col("centers"),
          lp::Lambda(
              {"center"},
              lp::Call(
                  "row_constructor",
                  lp::Call(
                      "cosine_similarity",
                      lp::Col("point"),
                      lp::Call(
                          "subscript", lp::Col("center"), lp::Lit(int64_t{2}))),
                  lp::Call(
                      "negate",
                      lp::Call(
                          "subscript",
                          lp::Col("center"),
                          lp::Lit(int64_t{1})))))),
      lp::Lit(int64_t{1}));

  // A cluster whose points sum to zero has no direction, so it gets no new
  // centroid and keeps its old one:
  //   if(l2_norm(total) > 0, normalize(total)) AS new_centroid
  const auto newCentroid = lp::Call(
      "if",
      lp::Call("l2_norm", lp::Col("total")) > lp::Lit(0.0),
      normalize(lp::Col("total")));

  auto newCentroids =
      lp::PlanBuilder(points)
          .project(
              ColumnExprs::concat(
                  partitionKeys, {normalize(lp::Col(embedding)).as("point")}))
          .join(
              centers,
              ColumnExprs::equal(partitionColumns, "c_"),
              lp::JoinType::kInner)
          .project(
              ColumnExprs::concat(
                  partitionKeys, {lp::Col("point"), nearest.as("cluster_id")}))
          .aggregate(
              ColumnExprs::concat(partitionKeys, {lp::Col("cluster_id")}),
              {lp::Call("vector_sum", lp::Col("point")).as("total")})
          .project(
              ColumnExprs::concat(
                  ColumnExprs::renamed(partitionColumns, "n_"),
                  {
                      lp::Col("cluster_id").as("n_cluster_id"),
                      newCentroid.as("new_centroid"),
                  }));

  // A cluster no point chose keeps its centroid, and each centroid records how
  // far it moved:
  //   coalesce(new_centroid, centroid) AS centroid,
  //   coalesce(1 - cosine_similarity(centroid, new_centroid), 0) AS movement
  auto stepCondition = ColumnExprs::equal(partitionColumns, "n_");
  auto sameCluster = lp::Col("cluster_id") == lp::Col("n_cluster_id");
  auto step = state()
                  .join(
                      newCentroids,
                      stepCondition.has_value() ? *stepCondition && sameCluster
                                                : sameCluster,
                      lp::JoinType::kLeft)
                  .project(
                      ColumnExprs::concat(
                          partitionKeys,
                          {
                              lp::Col("cluster_id"),
                              lp::Call(
                                  "coalesce",
                                  lp::Col("new_centroid"),
                                  lp::Col("centroid"))
                                  .as("centroid"),
                              lp::Call(
                                  "coalesce",
                                  lp::Lit(1.0f) -
                                      lp::Cast(
                                          REAL(),
                                          lp::Call(
                                              "cosine_similarity",
                                              lp::Col("centroid"),
                                              lp::Col("new_centroid"))),
                                  lp::Lit(0.0f))
                                  .as("movement"),
                          }));

  // The largest move of any centroid; zero when there are none:
  //   coalesce(max(movement), 0) <= kTolerance AS converged
  auto convergence =
      state()
          .aggregate({}, {lp::Call("max", lp::Col("movement")).as("moved")})
          .project({(lp::Call("coalesce", lp::Col("moved"), lp::Lit(0.0f)) <=
                     lp::Lit(kTolerance))
                        .as("converged")});

  auto keyColumns = partitionColumns;
  keyColumns.emplace_back("cluster_id");
  return anchor
      .iterateUntil(
          kState,
          step,
          convergence,
          kMaxIterations,
          /*failAtBound=*/false,
          keyColumns)
      .project(
          ColumnExprs::concat(
              partitionKeys, {lp::Col("cluster_id"), lp::Col("centroid")}));
}

} // namespace axiom::sql::presto
