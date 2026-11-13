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
#include <string>
#include <vector>

#include "axiom/connectors/VectorIndex.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/SqlStatement.h"

namespace axiom::sql::presto {

class CentroidPlanner;

/// The plans of REFRESH VECTOR INDEX (see RefreshVectorIndexStatement).
///
/// Example:
///
///   auto plans = VectorIndexRefresh::plan(
///       table, index, connectorId, makeQuery, addCondition,
///       session->centroidPlanner().get());
///   // Inside the index write, run plans.trainPlan, then plans.buildPlan,
///   // each with the write's codec id as the value of ?1.
class VectorIndexRefresh {
 public:
  using Plans = RefreshVectorIndexStatement::Plans;

  /// Returns the plans that build 'index' of 'table', of the connector
  /// 'connectorId', from the rows of the query 'makeQuery' returns. The query
  /// has, by position, the index's key, its embedding, then its partition
  /// columns. The plans read the query several times, and a logical plan is a
  /// tree, so each call of 'makeQuery' must plan it again, with new names.
  ///
  /// 'addCondition' adds the query's condition on the partition columns to a
  /// scan of a storage table of the index, which has the same partition
  /// columns, so the plans read only the refreshed partitions of the index.
  ///
  /// The index holds only valid rows. A row is skipped when its key is NULL, or
  /// its embedding is NULL, of another length than the index's dimension, has
  /// a NULL or non-finite element, has a zero norm, or has a NULL partition
  /// value. A partition the index covers fails the refresh, for now.
  ///
  /// Each partition's codec is trained with vector_index_train, or packed with
  /// vector_index_pack from the centroids of 'planner' if set.
  ///
  /// For now, the index has at least one partition column.
  static Plans plan(
      const facebook::axiom::SchemaTableName& table,
      const facebook::axiom::connector::VectorIndex& index,
      const std::string& connectorId,
      const std::function<facebook::axiom::logical_plan::PlanBuilder()>&
          makeQuery,
      const std::function<facebook::axiom::logical_plan::PlanBuilder(
          facebook::axiom::logical_plan::PlanBuilder storageScan)>&
          addCondition,
      const CentroidPlanner* planner);
};

} // namespace axiom::sql::presto
