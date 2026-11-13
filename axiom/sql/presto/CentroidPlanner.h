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

#include <functional>

#include "axiom/connectors/VectorIndex.h"
#include "axiom/logical_plan/PlanBuilder.h"

namespace axiom::sql::presto {

/// Plans the centroids of each partition of a vector index for REFRESH VECTOR
/// INDEX. An application gives a planner to the parser session of each query
/// (see ParserSession) when its engine runs the plans the planner returns.
/// Axiom samples the rows, packs the centroids into each partition's codec
/// with vector_index_pack, and builds the local indexes.
///
/// The planner is shared by every query of the application, and several
/// queries may call it at once.
///
/// A planner is written for Presto, even though no SQL text reaches it: the
/// plans it returns call Presto's functions by name, ExprApi's operators call
/// Presto's (a + b calls plus), and the plan builders apply Presto's implicit
/// casts. The application registers the functions the plans call. A planner
/// builds expressions with ExprApi, without lp::Sql, which parses DuckDB's
/// syntax whatever the frontend.
///
/// Example: a planner that keeps each partition's first K sampled rows as its
/// centroids, and a parser session that gives it to a query:
///
///   class FirstRows : public CentroidPlanner {
///    public:
///     lp::PlanBuilder plan(
///         const VectorIndexProperties& properties,
///         const std::function<lp::PlanBuilder()>& makeSample) const override {
///       auto rows = makeSample();
///       const auto columns = rows.findOrAssignOutputNames();
///       const auto numPartitions = columns.size() - 3;
///       const auto sampleRow = columns[numPartitions + 2].toCol();
///       std::vector<lp::ExprApi> output;
///       for (size_t i = 0; i < numPartitions; ++i) {
///         output.push_back(columns[i].toCol());
///       }
///       output.push_back((sampleRow - lp::Lit(int64_t{1})).as("cluster_id"));
///       output.push_back(columns[numPartitions + 1].toCol().as("centroid"));
///       return rows.filter(sampleRow <= lp::Lit(properties.numClusters))
///           .project(output);
///     }
///   };
///
///   auto session = std::make_shared<ParserSession>(
///       context, statsWriter, properties, options,
///       std::make_shared<FirstRows>());
class CentroidPlanner {
 public:
  virtual ~CentroidPlanner() = default;

  /// Returns the centroids of each partition of the sample that 'makeSample'
  /// plans, for an index with 'properties': its number of clusters K, its
  /// dimension D and its metric.
  ///
  /// 'makeSample' is valid only during the call. It plans the sample anew on
  /// each call, since a logical plan is a tree and a planner may read the
  /// sample more than once. Every plan the planner builds comes from
  /// makeSample(), or from its builder via PlanBuilder::newPlan(), so that
  /// names stay unique within REFRESH's plan. The sample's columns, in order:
  ///  - the partition columns of the index's table, with their types, all
  ///    non-null, for the partitions being refreshed;
  ///  - the key (BIGINT), non-null;
  ///  - the embedding (ARRAY(REAL)), of length D, with non-null, finite
  ///    elements and a non-zero norm;
  ///  - the sample row (BIGINT), which numbers each partition's rows from 1
  ///    in an order that depends only on their keys, the same on any number of
  ///    workers and drivers.
  ///
  /// The partition columns are all but the last three. A partition has at most
  /// 256 * K rows. The planner refers to the columns by the names the returned
  /// plan gives them.
  ///
  /// The returned plan's columns, in order:
  ///  - the partition columns, with their types;
  ///  - the cluster ID (BIGINT);
  ///  - the centroid (ARRAY(REAL)).
  ///
  /// For each partition with n sampled rows, it returns min(n, K) rows, with
  /// cluster IDs 0 to min(n, K) - 1, each once, and centroids of length D with
  /// finite elements and a non-zero norm. A partition without sampled rows
  /// has no rows, and gets a NULL codec.
  ///
  /// Axiom checks the column count and types while planning. vector_index_pack
  /// checks the cluster IDs and centroids at run time, and fails on a NULL
  /// cluster ID or centroid.
  virtual facebook::axiom::logical_plan::PlanBuilder plan(
      const facebook::axiom::connector::VectorIndexProperties& properties,
      const std::function<facebook::axiom::logical_plan::PlanBuilder()>&
          makeSample) const = 0;
};

} // namespace axiom::sql::presto
