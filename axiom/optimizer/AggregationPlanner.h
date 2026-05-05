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
/// no shuffle is added, the returned cost is 0.
std::pair<RelationOpPtr, PlanCost> maybeRepartition(
    const RelationOpPtr& plan,
    ExprVector desiredKeys);

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
      const ColumnVector& partitionKeys) const;

  // Creates a two-phase (partial + final) aggregation plan with repartitioning
  // by groupingKeys in between.
  std::pair<RelationOpPtr, PlanCost> makeSplitAggregationPlan(
      RelationOpPtr plan,
      const ExprVector& groupingKeys,
      const AggregateVector& aggregates,
      const ColumnVector& intermediateColumns,
      const ColumnVector& outputColumns) const;

  // Creates a single-phase aggregation plan following repartitioning by
  // groupingKeys.
  std::pair<RelationOpPtr, PlanCost> makeSingleAggregationPlan(
      RelationOpPtr plan,
      const ExprVector& groupingKeys,
      const AggregateVector& aggregates,
      const ColumnVector& intermediateColumns,
      const ColumnVector& outputColumns) const;

  // Chooses between split and single aggregation plans based on cost.
  std::pair<RelationOpPtr, PlanCost> makeSplitOrSingleAggregationPlan(
      const RelationOpPtr& plan,
      const ExprVector& groupingKeys,
      const AggregateVector& aggregates,
      const ColumnVector& intermediateColumns,
      const ColumnVector& outputColumns) const;

  // Transforms distinct aggregation into a two-level GROUP BY + non-distinct
  // aggregation plan.
  std::pair<RelationOpPtr, PlanCost> makeDistinctToGroupByPlan(
      RelationOpPtr plan,
      const ExprVector& groupingKeys,
      const ExprVector& distinctArgs,
      const AggregateVector& aggregates,
      AggregationPlanCP aggPlan,
      bool hasOrderBy) const;

  // Transforms distinct aggregations into a plan with MarkDistinct and
  // non-distinct masked aggregations.
  std::pair<RelationOpPtr, PlanCost> makeDistinctToMarkDistinctPlan(
      RelationOpPtr plan,
      const ExprVector& groupingKeys,
      const AggregateVector& aggregates,
      AggregationPlanCP aggPlan,
      bool hasOrderBy) const;

  bool isSingleWorker_;
  bool isSingleDriver_;
  bool alwaysPlanPartialAggregation_;
};

} // namespace facebook::axiom::optimizer
