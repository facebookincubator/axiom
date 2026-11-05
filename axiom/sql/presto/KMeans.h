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
#pragma once

#include <string>
#include <vector>

#include "axiom/logical_plan/PlanBuilder.h"
#include "velox/common/vectorindex/VectorIndexMetric.h"

namespace axiom::sql::presto {

/// The k-means loop that trains a vector index's codecs: a plan that clusters
/// each partition's points into 'numClusters' clusters.
///
/// Example:
///
///   auto centroids = KMeans::expand(
///       points, {"ds"}, "embedding", "sequence", 1000, kCosine);
///   // centroids: (ds, cluster_id, centroid)
///
/// - Seeds: each partition's points with 'sequence' 1 to 'numClusters', as
///   clusters 0 to 'numClusters' - 1.
/// - Step: assigns each point to the nearest centroid of its own partition,
///   then averages each cluster's points. A cluster with no points keeps its
///   centroid.
/// - Stops when no centroid moves more than a tolerance, or after a fixed
///   number of iterations.
///
/// Supports only kCosine, for which the k-means is spherical: points and
/// centroids are normalized to length 1, and a point goes to the centroid with
/// the largest cosine similarity, the lowest cluster_id on a tie.
class KMeans {
 public:
  /// Returns a plan of the centroids: the partition columns, 'cluster_id', a
  /// BIGINT, and 'centroid', an ARRAY(REAL). 'points' has 'partitionColumns',
  /// 'embedding', an ARRAY(REAL), and 'sequence', a BIGINT that numbers each
  /// partition's points from 1. The loop reads it for the seeds and for the
  /// points it assigns. The names of 'points' must not collide with the names
  /// of the loop's own columns, such as 'cluster_id', 'centroid' and names
  /// that start with "c_" or "n_".
  static facebook::axiom::logical_plan::PlanBuilder expand(
      const facebook::axiom::logical_plan::PlanBuilder& points,
      const std::vector<std::string>& partitionColumns,
      const std::string& embedding,
      const std::string& sequence,
      int64_t numClusters,
      facebook::velox::common::vectorindex::VectorIndexMetric metric);
};

} // namespace axiom::sql::presto
