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

/// What REFRESH VECTOR INDEX gives a codec trainer for one index: the index's
/// properties, its partition columns and its sample. Valid only during
/// VectorIndexCodecTrainer::plan.
struct VectorIndexSample {
  /// The index's properties: its number of clusters K, its dimension D and its
  /// metric.
  facebook::axiom::connector::VectorIndexProperties properties;

  /// The partition columns of the index's table, with their types. Each row of
  /// a sample, and of the trainer's result, starts with them, in this order.
  facebook::velox::RowTypePtr partitionColumns;

  /// Plans the valid rows among each partition's about 'numRows' rows of
  /// smallest sample hash, in no particular order, the same rows on any number
  /// of workers and drivers. A partition of at most 'numRows' rows keeps all
  /// of its valid rows. A smaller 'numRows' keeps a subset of a larger one's
  /// rows, so a trainer can seed its clusters from fewer rows than it trains
  /// on without knowing how REFRESH samples.
  ///
  /// Plans anew on each call, since a logical plan is a tree and a trainer may
  /// read the sample more than once. Every plan the trainer builds comes from
  /// makeSample, or from its builder via PlanBuilder::newPlan(), so that names
  /// stay unique within REFRESH's plan. The columns, in order:
  ///  - the partition columns, all non-null, for the partitions being
  ///    refreshed;
  ///  - the key (BIGINT), non-null;
  ///  - the embedding (ARRAY(REAL)), of length D, with non-null, finite
  ///    elements and a non-zero norm;
  ///  - the sample hash (BIGINT),
  ///    from_big_endian_64(xxhash64(to_big_endian_64(key))), which depends
  ///    only on the key and is uniform over BIGINT;
  ///  - the partition's row count (BIGINT): its number of rows in the
  ///    refresh's input, valid or not.
  ///
  /// REFRESH reads the row count from the connector's metadata, so a trainer
  /// uses it rather than counting the sample. The trainer refers to the
  /// columns by the names the returned plan gives them.
  std::function<facebook::axiom::logical_plan::PlanBuilder(int64_t numRows)>
      makeSample;
};

/// Trains the codecs of a vector index for REFRESH VECTOR INDEX: plans the
/// centroids of each partition, which Axiom packs into the partition's codec
/// with vector_index_pack. An application gives a trainer to the parser session
/// of each query (see ParserSession) when its engine runs the plans the trainer
/// returns. Axiom samples the rows and builds the local indexes.
///
/// The trainer is shared by every query of the application, and several
/// queries may call it at once.
///
/// A trainer is written for Presto, even though no SQL text reaches it: the
/// plans it returns call Presto's functions by name, ExprApi's operators call
/// Presto's (a + b calls plus), and the plan builders apply Presto's implicit
/// casts. The application registers the functions the plans call. A trainer
/// builds expressions with ExprApi, without lp::Sql, which parses DuckDB's
/// syntax whatever the frontend.
///
/// axiom/cli/tests/VectorIndexCodecTrainerTest.cpp has a small spherical
/// k-means trainer that uses each part of the contract below.
class VectorIndexCodecTrainer {
 public:
  virtual ~VectorIndexCodecTrainer() = default;

  /// Returns the centroids of each partition of 'sample'.
  ///
  /// The returned plan's columns, in order:
  ///  - the partition columns, with their types;
  ///  - the cluster ID (BIGINT);
  ///  - the centroid (ARRAY(REAL)).
  ///
  /// For each partition with valid rows, it returns m rows for some m from 1
  /// to K, with cluster IDs 0 to m - 1, each once, and centroids of length D
  /// with finite elements and a non-zero norm. A partition without valid rows
  /// has no rows, and gets a NULL codec. A trainer may instead fail the
  /// refresh for a partition it can't train, naming the partition.
  ///
  /// Axiom checks the column count and types while planning. vector_index_pack
  /// checks the cluster IDs and centroids at run time, and fails on a NULL
  /// cluster ID or centroid. A partition with valid rows but no centroids
  /// fails the refresh when the build finds its rows.
  virtual facebook::axiom::logical_plan::PlanBuilder plan(
      const VectorIndexSample& sample) const = 0;
};

} // namespace axiom::sql::presto
