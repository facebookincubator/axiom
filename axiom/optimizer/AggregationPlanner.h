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

#include "axiom/optimizer/Cost.h"
#include "axiom/optimizer/DerivedTable.h"
#include "axiom/optimizer/RelationOp.h"

namespace facebook::axiom::optimizer {

struct PlanState;

/// Repartitions 'plan' by 'desiredKeys' if needed. Adds a gather if
/// desiredKeys is empty. Adds a shuffle if existing partition keys are not a
/// subset of desiredKeys. Does nothing if data is already gathered or
/// partitioned correctly. Returns the possibly repartitioned plan and cost. If
/// no shuffle is added, the returned cost is 0. When a Repartition is added,
/// snapshots 'state.currentGroupedLeaves_' onto
/// Optimization::repartitionGroupedLeaves_ and resets the map per the new
/// Repartition's distribution kind.
std::pair<RelationOpPtr, PlanCost> maybeRepartition(
    const RelationOpPtr& plan,
    ExprVector desiredKeys,
    PlanState& state);

/// Returns the positions in 'keys' of expressions that match 'op's partition
/// keys, in partition-key order. Returns an empty vector if any partition key
/// is missing from 'keys'.
std::vector<uint32_t> joinKeyPartition(
    const RelationOpPtr& op,
    const ExprVector& keys);

/// Plans aggregation operators for a derived table.
class AggregationPlanner {
 public:
  AggregationPlanner(
      bool isSingleWorker,
      bool isSingleDriver,
      bool alwaysPlanPartialAggregation);

  /// Plans the aggregation for a derived table in the main optimization path.
  /// Chooses between single-step and split (partial + final) plans based on
  /// cost and eligibility. Also applies eligible transformations to distinct
  /// aggregations.
  void plan(DerivedTableCP dt, RelationOpPtr& plan, PlanState& state) const;

  /// Plans a single-step aggregation for a derived table in the subquery path
  /// in ToGraph. No tracking of cost.
  static RelationOpPtr planSingle(DerivedTableCP dt, RelationOpPtr& input);

 private:
  // Repartitions plan by partition keys if the current partition keys don't
  // already cover them. Returns the plan and cost of repartitioning. If no
  // repartition is added, the returned cost is 0.
  std::pair<RelationOpPtr, PlanCost> repartitionForAgg(
      const RelationOpPtr& plan,
      const ColumnVector& partitionKeys,
      PlanState& state) const;

  // Creates a two-phase (partial + final) aggregation plan with repartitioning
  // by groupingKeys in between.
  std::pair<RelationOpPtr, PlanCost> makeSplitAggregationPlan(
      RelationOpPtr plan,
      const ExprVector& groupingKeys,
      const AggregateVector& aggregates,
      const ColumnVector& intermediateColumns,
      const ColumnVector& outputColumns,
      QGVector<int32_t> globalGroupingSets,
      ColumnCP groupId,
      PlanState& state) const;

  // Creates a single-phase aggregation plan. Repartitions by groupingKeys,
  // except for global grouping sets, which are gathered onto a single task.
  std::pair<RelationOpPtr, PlanCost> makeSingleAggregationPlan(
      RelationOpPtr plan,
      const ExprVector& groupingKeys,
      const AggregateVector& aggregates,
      const ColumnVector& intermediateColumns,
      const ColumnVector& outputColumns,
      QGVector<int32_t> globalGroupingSets,
      ColumnCP groupId,
      PlanState& state) const;

  // Builds the distributed aggregation, choosing between a split (partial +
  // final) and a single-step plan based on the shape of 'aggregates' and the
  // cost.
  //   1. If any of 'aggregates' has ORDER BY or is DISTINCT, generates a
  //   single-step plan.
  //   2. Else if 'globalGroupingSets' is non-empty, generates a split plan.
  //   3. Otherwise compares split and single by cost.
  // For non-grouping-sets aggregation, set 'globalGroupingSets' and 'groupId'
  // to {} and nullptr.
  std::pair<RelationOpPtr, PlanCost> makeSplitOrSingleAggregationPlan(
      const RelationOpPtr& plan,
      const ExprVector& groupingKeys,
      const AggregateVector& aggregates,
      const ColumnVector& intermediateColumns,
      const ColumnVector& outputColumns,
      QGVector<int32_t> globalGroupingSets,
      ColumnCP groupId,
      PlanState& state) const;

  // Returns a plan for distinct aggregations with its cost. Distinct
  // aggregations are handled through Distinct-to-GroupBy or
  // Distinct-to-MarkDistinct transformations. For non-grouping-sets
  // aggregation, set 'globalGroupingSets' and 'groupId' to {} and nullptr.
  std::pair<RelationOpPtr, PlanCost> makeDistinctAggregation(
      RelationOpPtr plan,
      const ExprVector& groupingKeys,
      const AggregateVector& aggregates,
      AggregationPlanCP aggPlan,
      QGVector<int32_t> globalGroupingSets,
      ColumnCP groupId,
      PlanState& state) const;

  // Transforms distinct aggregation into a two-level GROUP BY + non-distinct
  // aggregation plan.
  std::pair<RelationOpPtr, PlanCost> makeDistinctToGroupByPlan(
      RelationOpPtr plan,
      const ExprVector& groupingKeys,
      const ExprVector& distinctArgs,
      const AggregateVector& aggregates,
      AggregationPlanCP aggPlan,
      QGVector<int32_t> globalGroupingSets,
      ColumnCP groupId,
      PlanState& state) const;

  // Transforms distinct aggregations into a plan with MarkDistinct and
  // non-distinct masked aggregations.
  std::pair<RelationOpPtr, PlanCost> makeDistinctToMarkDistinctPlan(
      RelationOpPtr plan,
      const ExprVector& groupingKeys,
      const AggregateVector& aggregates,
      AggregationPlanCP aggPlan,
      QGVector<int32_t> globalGroupingSets,
      ColumnCP groupId,
      PlanState& state) const;

  // Handles aggregation with grouping sets (ROLLUP, CUBE, GROUPING SETS).
  // Creates a GroupId node followed by cost-based split or single aggregation.
  void addGroupingSetsAggregation(
      AggregationPlanCP aggPlan,
      RelationOpPtr& plan,
      const ExprVector& groupingKeys,
      const AggregateVector& aggregates,
      PlanState& state) const;

  bool isSingleWorker_;
  bool isSingleDriver_;
  bool alwaysPlanPartialAggregation_;
};

} // namespace facebook::axiom::optimizer
