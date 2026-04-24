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

#include "axiom/optimizer/AggregationPlanner.h"

#include <optional>

#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/PrecomputeProjection.h"

namespace facebook::axiom::optimizer {

namespace {

// Flattens aggregate expressions by resolving args, conditions, and order keys
// through the precompute projection.
AggregateVector flattenAggregates(
    const AggregateVector& aggregates,
    PrecomputeProjection& precompute) {
  AggregateVector flatAggregates;
  flatAggregates.reserve(aggregates.size());

  for (const auto& agg : aggregates) {
    ExprCP condition = nullptr;
    if (agg->condition()) {
      condition = precompute.toColumn(agg->condition());
    }
    ExprVector args;
    args.reserve(agg->args().size());
    for (const auto& arg : agg->args()) {
      if (arg->is(PlanType::kLambdaExpr)) {
        args.push_back(arg);
      } else {
        args.push_back(precompute.toColumn(arg, nullptr, true));
      }
    }
    auto orderKeys = precompute.toColumns(agg->orderKeys());
    flatAggregates.emplace_back(
        make<Aggregate>(
            agg->name(),
            agg->value(),
            std::move(args),
            agg->functions(),
            agg->isDistinct(),
            condition,
            agg->intermediateType(),
            std::move(orderKeys),
            agg->orderTypes()));
  }

  return flatAggregates;
}

// Computes the pre-grouped keys for streaming aggregation based on input's
// orderKeys and clusterKeys. For orderKeys, returns the longest prefix that
// is also a grouping key (order guarantees break at the first non-grouping
// key). For clusterKeys, returns any subset that are grouping keys (clustering
// only requires contiguous values, not ordering). Returns whichever set is
// larger to maximize streaming benefit.
ExprVector computePreGroupedKeys(
    const RelationOp& input,
    const ExprVector& groupingKeys) {
  if (groupingKeys.empty()) {
    return {};
  }

  auto isGroupingKey = [&](ExprCP key) {
    for (const auto& groupingKey : groupingKeys) {
      if (key->sameOrEqual(*groupingKey)) {
        return true;
      }
    }
    return false;
  };

  // Check orderKeys - find the longest prefix that is in groupingKeys.
  // For ordered data, we can only use a prefix because the order guarantee
  // breaks once we hit a key not in groupingKeys.
  ExprVector fromOrderKeys;
  const auto& orderKeys = input.distribution().orderKeys();
  for (const auto& key : orderKeys) {
    if (isGroupingKey(key)) {
      fromOrderKeys.push_back(key);
    } else {
      break; // Must be a prefix for ordered data.
    }
  }

  // Check clusterKeys - any subset of groupingKeys works.
  // Clustering doesn't require prefix matching since rows with the same
  // cluster key values are contiguous regardless of other columns.
  ExprVector fromClusterKeys;
  const auto& clusterKeys = input.distribution().clusterKeys();
  for (const auto& key : clusterKeys) {
    if (isGroupingKey(key)) {
      fromClusterKeys.push_back(key);
    }
  }

  // Return the larger subset for maximum streaming benefit.
  // When equal, prefer orderKeys. Ideally we'd choose the set that produces
  // the smallest hash table, but we lack data to make that determination.
  return fromOrderKeys.size() >= fromClusterKeys.size() ? fromOrderKeys
                                                        : fromClusterKeys;
}

// Returns the union of column expressions from groupingKeys and args,
// skipping literals. Throws on unexpected expression types other than column
// and literal.
ExprVector unionColumnArgs(
    const ExprVector& groupingKeys,
    const ExprVector& args) {
  ExprVector keys = groupingKeys;
  auto keySet = PlanObjectSet::fromObjects(keys);
  for (const auto* arg : args) {
    if (arg->is(PlanType::kLiteralExpr)) {
      continue;
    }
    VELOX_CHECK(
        arg->is(PlanType::kColumnExpr),
        "Expected column or literal expression: {}",
        arg->toString());
    if (!keySet.contains(arg)) {
      keySet.add(arg);
      keys.push_back(arg);
    }
  }
  return keys;
}

// Returns the common distinct column arguments if all aggregates are DISTINCT
// with the same column arguments and no filters. Returns std::nullopt if not
// eligible.
std::optional<ExprVector> getCommonDistinctArgs(
    const AggregateVector& aggregates) {
  VELOX_CHECK(!aggregates.empty());

  for (const auto* agg : aggregates) {
    if (!agg->isDistinct()) {
      return std::nullopt;
    }
    if (agg->condition() != nullptr) {
      return std::nullopt;
    }
  }

  // Check same column args across all aggregates (as a set, using pointer
  // equality since exprs are deduplicated).
  auto commonDistinctArgs = unionColumnArgs({}, aggregates[0]->args());
  auto commonColumnArgSet = PlanObjectSet::fromObjects(commonDistinctArgs);
  PlanObjectSet currentColumnArgSet;
  for (size_t i = 1; i < aggregates.size(); ++i) {
    currentColumnArgSet.clear();
    currentColumnArgSet.unionObjects(
        unionColumnArgs({}, aggregates[i]->args()));
    if (currentColumnArgSet != commonColumnArgSet) {
      return std::nullopt;
    }
  }

  return commonDistinctArgs;
}

// Returns a copy of aggregates with isDistinct set to false.
AggregateVector dropDistinctFromAggregates(const AggregateVector& aggregates) {
  AggregateVector result;
  result.reserve(aggregates.size());
  for (const auto* aggregate : aggregates) {
    result.push_back(aggregate->dropDistinct());
  }
  return result;
}

// Returns true if no distinct aggregate has a filter.
bool canMakeMarkDistinctPlan(const AggregateVector& aggregates) {
  return !std::any_of(
      aggregates.begin(), aggregates.end(), [](const auto* agg) {
        return agg->isDistinct() && agg->condition() != nullptr;
      });
}

// Result of adding MarkDistinct nodes to a plan.
struct MarkDistinctResult {
  RelationOpPtr plan;
  AggregateVector newAggregates;
  PlanCost cost;
};

// Adds MarkDistinct nodes for each unique set of distinct arguments and
// rewrites distinct aggregates to masked non-distinct aggregates.
MarkDistinctResult addMarkDistinctNodes(
    RelationOpPtr plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    bool isSingleWorker) {
  PlanCost totalCost;
  std::unordered_map<PlanObjectSet, ColumnCP> markers;

  AggregateVector newAggregates;
  newAggregates.reserve(aggregates.size());
  auto groupingKeySet = PlanObjectSet::fromObjects(groupingKeys);
  for (const auto* aggregate : aggregates) {
    if (!aggregate->isDistinct()) {
      newAggregates.push_back(aggregate);
      continue;
    }

    auto markDistinctKeys = unionColumnArgs(groupingKeys, aggregate->args());
    auto markDistinctKeySet = PlanObjectSet::fromObjects(markDistinctKeys);
    if (markDistinctKeySet == groupingKeySet) {
      // All distinct args are literals or already in grouping keys.
      // MarkDistinct would be redundant since the aggregation's GROUP BY
      // already partitions by these keys. Keep the aggregate unchanged with
      // DISTINCT.
      newAggregates.push_back(aggregate);
      continue;
    }
    auto [it, inserted] = markers.try_emplace(markDistinctKeySet, nullptr);
    if (inserted) {
      auto markerName = fmt::format("__m{}", markers.size() - 1);
      it->second = make<Column>(
          toName(markerName),
          /*relation=*/nullptr,
          Value{toType(velox::BOOLEAN()), 2});

      if (!isSingleWorker) {
        auto [repartitioned, repartitionCost] =
            maybeRepartition(plan, ExprVector(markDistinctKeys));
        plan = repartitioned;
        totalCost.add(repartitionCost);
      }

      auto markDistinct =
          make<MarkDistinct>(plan, it->second, std::move(markDistinctKeys));
      totalCost.add(*markDistinct);
      plan = markDistinct;
    }

    newAggregates.push_back(aggregate->replaceDistinctWithMarker(it->second));
  }

  return {plan, std::move(newAggregates), totalCost};
}

} // namespace

std::pair<RelationOpPtr, PlanCost> maybeRepartition(
    const RelationOpPtr& plan,
    ExprVector desiredKeys) {
  if (plan->distribution().isGather()) {
    return {plan, {}};
  }

  PlanCost cost;
  if (desiredKeys.empty()) {
    auto* gather =
        make<Repartition>(plan, Distribution::gather(), plan->columns());
    cost.add(*gather);
    return {gather, cost};
  }

  // Check if existing partition keys are a subset of desired keys. If
  // partition keys are empty or contain columns not in desired keys, shuffle.
  // Uses sameOrEqual to account for equivalence classes from join predicates.
  bool shuffle = plan->distribution().partitionKeys().empty();
  if (!shuffle) {
    const auto& existingKeys = plan->distribution().partitionKeys();
    shuffle = std::any_of(
        existingKeys.begin(),
        existingKeys.end(),
        [&desiredKeys](const auto& key) {
          return std::none_of(
              desiredKeys.begin(), desiredKeys.end(), [&key](ExprCP desired) {
                return desired->sameOrEqual(*key);
              });
        });
  }

  if (shuffle) {
    Distribution distribution{
        plan->distribution().distributionType(), std::move(desiredKeys)};
    auto* repartition =
        make<Repartition>(plan, std::move(distribution), plan->columns());
    cost.add(*repartition);
    return {repartition, cost};
  }

  return {plan, {}};
}

std::pair<RelationOpPtr, PlanCost> AggregationPlanner::repartitionForAgg(
    const RelationOpPtr& plan,
    const ColumnVector& partitionKeys) const {
  if (isSingleWorker_ || plan->distribution().isGather()) {
    return {plan, {}};
  }

  ExprVector keyExprs(partitionKeys.begin(), partitionKeys.end());
  return maybeRepartition(plan, std::move(keyExprs));
}

std::pair<RelationOpPtr, PlanCost> AggregationPlanner::makeSplitAggregationPlan(
    RelationOpPtr plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    const ColumnVector& intermediateColumns,
    const ColumnVector& outputColumns) const {
  PlanCost splitAggCost;

  plan = make<Aggregation>(
      plan,
      groupingKeys,
      /*preGroupedKeys*/ ExprVector{},
      aggregates,
      velox::core::AggregationNode::Step::kPartial,
      intermediateColumns);
  splitAggCost.add(*plan);

  ColumnVector partitionKeys(
      intermediateColumns.begin(),
      intermediateColumns.begin() + groupingKeys.size());

  PlanCost repartitionCost;
  std::tie(plan, repartitionCost) = repartitionForAgg(plan, partitionKeys);
  splitAggCost.add(repartitionCost);

  ExprVector finalGroupingKeys(partitionKeys.begin(), partitionKeys.end());
  auto* splitAggPlan = make<Aggregation>(
      plan,
      std::move(finalGroupingKeys),
      /*preGroupedKeys*/ ExprVector{},
      aggregates,
      velox::core::AggregationNode::Step::kFinal,
      outputColumns);
  splitAggCost.add(*splitAggPlan);

  return {splitAggPlan, splitAggCost};
}

std::pair<RelationOpPtr, PlanCost>
AggregationPlanner::makeSingleAggregationPlan(
    RelationOpPtr plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    const ColumnVector& intermediateColumns,
    const ColumnVector& outputColumns) const {
  PlanCost singleAggCost;

  ColumnVector partitionKeys(
      intermediateColumns.begin(),
      intermediateColumns.begin() + groupingKeys.size());
  PlanCost repartitionCost;
  std::tie(plan, repartitionCost) = repartitionForAgg(plan, partitionKeys);
  singleAggCost.add(repartitionCost);

  auto* singleAgg = make<Aggregation>(
      plan,
      groupingKeys,
      /*preGroupedKeys*/ ExprVector{},
      aggregates,
      velox::core::AggregationNode::Step::kSingle,
      outputColumns);
  singleAggCost.add(*singleAgg);

  return {singleAgg, singleAggCost};
}

std::pair<RelationOpPtr, PlanCost>
AggregationPlanner::makeSplitOrSingleAggregationPlan(
    const RelationOpPtr& plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    const ColumnVector& intermediateColumns,
    const ColumnVector& outputColumns) const {
  const auto& [splitAggPlan, splitAggCost] = makeSplitAggregationPlan(
      plan, groupingKeys, aggregates, intermediateColumns, outputColumns);

  if (groupingKeys.empty() || alwaysPlanPartialAggregation_) {
    return {splitAggPlan, splitAggCost};
  }

  const auto& [singleAgg, singleAggCost] = makeSingleAggregationPlan(
      plan, groupingKeys, aggregates, intermediateColumns, outputColumns);

  if (singleAggCost.cost < splitAggCost.cost) {
    return {singleAgg, singleAggCost};
  }
  return {splitAggPlan, splitAggCost};
}

std::pair<RelationOpPtr, PlanCost>
AggregationPlanner::makeDistinctToGroupByPlan(
    RelationOpPtr plan,
    const ExprVector& groupingKeys,
    const ExprVector& distinctArgs,
    const AggregateVector& aggregates,
    AggregationPlanCP aggPlan,
    bool hasOrderBy) const {
  // Build inner GROUP BY keys: groupingKeys union distinctArgs. We put
  // groupingKeys at the beginning, followed by distinctArgs not appearing in
  // groupingKeys.
  ExprVector innerKeys = groupingKeys;
  PlanObjectSet innerKeySet = PlanObjectSet::fromObjects(groupingKeys);
  for (const auto* arg : distinctArgs) {
    if (!innerKeySet.contains(arg)) {
      innerKeySet.add(arg);
      innerKeys.push_back(arg);
    }
  }

  // Make output columns of inner aggregation.
  ColumnVector innerColumns;
  innerColumns.reserve(innerKeys.size());
  for (const auto* key : innerKeys) {
    const auto* keyColumn = key->as<Column>();
    VELOX_CHECK_NOT_NULL(keyColumn);
    innerColumns.push_back(keyColumn);
  }

  PlanCost totalCost;
  const auto& [innerAgg, innerAggCost] = makeSplitOrSingleAggregationPlan(
      plan, innerKeys, AggregateVector{}, innerColumns, innerColumns);
  totalCost.add(innerAggCost);
  plan = innerAgg;

  // Make non-distinct aggregation calls for the outer level.
  auto nonDistinctAggregates = dropDistinctFromAggregates(aggregates);

  if (hasOrderBy) {
    const auto& [outerPlan, outerCost] = makeSingleAggregationPlan(
        plan,
        groupingKeys,
        nonDistinctAggregates,
        aggPlan->intermediateColumns(),
        aggPlan->columns());
    plan = outerPlan;
    totalCost.add(outerCost);
  } else {
    const auto& [outerPlan, outerCost] = makeSplitOrSingleAggregationPlan(
        plan,
        groupingKeys,
        nonDistinctAggregates,
        aggPlan->intermediateColumns(),
        aggPlan->columns());
    plan = outerPlan;
    totalCost.add(outerCost);
  }

  return {plan, totalCost};
}

std::pair<RelationOpPtr, PlanCost>
AggregationPlanner::makeDistinctToMarkDistinctPlan(
    RelationOpPtr plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    AggregationPlanCP aggPlan,
    bool hasOrderBy) const {
  auto [markedPlan, newAggregates, markCost] =
      addMarkDistinctNodes(plan, groupingKeys, aggregates, isSingleWorker_);

  PlanCost totalCost;
  totalCost.add(markCost);

  // Use single-step aggregation if any aggregate has ORDER BY or if any
  // aggregate still has isDistinct. Partial aggregation does not support
  // DISTINCT or ORDER BY.
  const auto hasRemainingDistinct = std::any_of(
      newAggregates.begin(), newAggregates.end(), [](const auto* agg) {
        return agg->isDistinct();
      });
  if (hasOrderBy || hasRemainingDistinct) {
    const auto& [aggregation, aggCost] = makeSingleAggregationPlan(
        markedPlan,
        groupingKeys,
        newAggregates,
        aggPlan->intermediateColumns(),
        aggPlan->columns());
    totalCost.add(aggCost);
    return {aggregation, totalCost};
  }

  const auto& [aggregation, aggCost] = makeSplitOrSingleAggregationPlan(
      markedPlan,
      groupingKeys,
      newAggregates,
      aggPlan->intermediateColumns(),
      aggPlan->columns());
  totalCost.add(aggCost);
  return {aggregation, totalCost};
}

AggregationPlanner::AggregationPlanner(
    bool isSingleWorker,
    bool isSingleDriver,
    bool alwaysPlanPartialAggregation)
    : isSingleWorker_{isSingleWorker},
      isSingleDriver_{isSingleDriver},
      alwaysPlanPartialAggregation_{alwaysPlanPartialAggregation} {}

RelationOpPtr AggregationPlanner::planSingle(
    DerivedTableCP dt,
    RelationOpPtr& input) {
  VELOX_CHECK_NOT_NULL(dt->aggregation);
  const auto* aggPlan = dt->aggregation;

  PrecomputeProjection precompute(input, dt, /*projectAllInputs=*/false);
  auto groupingKeys =
      precompute.toColumns(aggPlan->groupingKeys(), &aggPlan->columns());
  auto aggregates = flattenAggregates(aggPlan->aggregates(), precompute);

  return make<Aggregation>(
      std::move(precompute).maybeProject(),
      std::move(groupingKeys),
      /*preGroupedKeys*/ ExprVector{},
      std::move(aggregates),
      velox::core::AggregationNode::Step::kSingle,
      aggPlan->columns());
}

void AggregationPlanner::plan(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
  VELOX_CHECK_NOT_NULL(dt->aggregation);
  const auto* aggPlan = dt->aggregation;

  PrecomputeProjection precompute(plan, dt, /*projectAllInputs=*/false);
  auto groupingKeys =
      precompute.toColumns(aggPlan->groupingKeys(), &aggPlan->columns());
  auto aggregates = flattenAggregates(aggPlan->aggregates(), precompute);

  auto projected = std::move(precompute).maybeProject();
  if (projected != plan) {
    state.addCost(*projected);
  }
  plan = std::move(projected);
  state.place(aggPlan);

  auto preGroupedKeys = computePreGroupedKeys(*plan, groupingKeys);
  if ((isSingleWorker_ && isSingleDriver_) || !preGroupedKeys.empty()) {
    auto* singleAgg = make<Aggregation>(
        plan,
        std::move(groupingKeys),
        std::move(preGroupedKeys),
        std::move(aggregates),
        velox::core::AggregationNode::Step::kSingle,
        aggPlan->columns());

    state.addCost(*singleAgg);
    plan = singleAgg;
    return;
  }

  const auto hasDistinct =
      std::any_of(aggregates.begin(), aggregates.end(), [](const auto& agg) {
        return agg->isDistinct();
      });
  const auto hasOrderBy =
      std::any_of(aggregates.begin(), aggregates.end(), [](const auto& agg) {
        return !agg->orderKeys().empty();
      });
  if (hasDistinct) {
    auto distinctArgs = getCommonDistinctArgs(aggregates);
    if (distinctArgs.has_value()) {
      auto [result, cost] = makeDistinctToGroupByPlan(
          plan, groupingKeys, *distinctArgs, aggregates, aggPlan, hasOrderBy);
      plan = std::move(result);
      state.cost.add(cost);
      return;
    }

    if (canMakeMarkDistinctPlan(aggregates)) {
      auto [result, cost] = makeDistinctToMarkDistinctPlan(
          plan, groupingKeys, aggregates, aggPlan, hasOrderBy);
      plan = std::move(result);
      state.cost.add(cost);
      return;
    }

    VELOX_USER_FAIL("Distinct aggregation with FILTER is not supported");
  }

  // Check if any aggregate has ORDER BY keys. If so, we must use single-step
  // aggregation because partial aggregation cannot preserve global ordering.
  if (hasOrderBy) {
    const auto& [singleAgg, singleAggCost] = makeSingleAggregationPlan(
        plan,
        groupingKeys,
        aggregates,
        aggPlan->intermediateColumns(),
        aggPlan->columns());
    plan = singleAgg;
    state.cost.add(singleAggCost);
    return;
  }

  const auto& [selectedPlan, selectedCost] = makeSplitOrSingleAggregationPlan(
      plan,
      groupingKeys,
      aggregates,
      aggPlan->intermediateColumns(),
      aggPlan->columns());
  plan = selectedPlan;
  state.cost.add(selectedCost);
}

} // namespace facebook::axiom::optimizer
