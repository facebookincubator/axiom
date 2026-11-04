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

#include "axiom/optimizer/Builder.h"
#include "axiom/optimizer/Node.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/Schema.h"
#include "velox/core/ExpressionEvaluator.h"

namespace facebook::axiom::optimizer {

/// Plans a search of embeddings through a vector index of the table its build
/// side reads (see connector::Table::vectorIndexes()): the index on the
/// searched column with the search's metric that the index hint names, or else
/// the first by name. The index is stored as a codec table, with the centroids
/// of each partition's clusters, and an index table, with the local index of
/// each cluster. The search assigns each probe row to its nearest clusters,
/// searches their local indexes, and keeps the 'k' nearest rows:
///
///   VectorSearch(probe, build=Filter(ds = 'x', Scan(t)), embedding, k)
///
/// becomes
///
///   Filter: row_number <= k, by query_id ordered by score DESC
///     VectorSearch: (partitions, codec_id, cluster_id) =, local_index, k
///       VectorSearch: codec, k = numProbes
///         AssignUniqueId query_id
///           probe
///         Filter: ds = 'x' / Scan <codec table>
///       Filter: ds = 'x' / Scan <index table>
///
/// The search returns the table's key and partition columns of each match. It
/// fails when the index does not cover a partition the filters select (see
/// connector::TableLayout::co_checkVectorIndexCoverage()).
class VectorIndexSearch {
 public:
  /// Returns the plan of 'search', a search of embeddings, over the storage
  /// tables of its index. Its build side is a scan of a table, optionally
  /// filtered, under projections. 'probe' produces the probe's rows. The plan
  /// serves filters on the table's partition columns: it reads only the
  /// partitions they select. Throws a user error for a condition on other
  /// columns, which would need a filtered search inside a local index, for
  /// keys, and for reading a column other than the key and the partition
  /// columns, which are not supported yet.
  static NodeCP plan(
      const VectorSearch& search,
      NodeCP probe,
      Builder& builder,
      const Schema& schema,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator);
};

} // namespace facebook::axiom::optimizer
