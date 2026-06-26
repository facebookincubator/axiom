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

#include <folly/container/F14Map.h>

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

// Result of adding MarkDistinct nodes to a plan.
struct MarkDistinctResult {
  RelationOpPtr plan;
  AggregateVector newAggregates;
  PlanCost cost;
};

// Per-key-set group of distinct aggregates sharing the same MarkDistinct node.
// markers[0] is the no-mask marker, markers[i+1] is the per-mask marker for
// masks[i].
struct MarkDistinctGroup {
  ExprVector markDistinctKeys;
  ColumnVector markers;
  ColumnVector masks;
  // Maps filter condition to its marker column. nullptr means unfiltered.
  folly::F14FastMap<ExprCP, ColumnCP> filterToMarker;
};

// Adds MarkDistinct nodes for each unique set of distinct arguments and
// rewrites distinct aggregates to masked non-distinct aggregates. Handles
// both filtered and unfiltered distinct aggregates via multi-mask MarkDistinct.
MarkDistinctResult addMarkDistinctNodes(
    RelationOpPtr plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    bool isSingleWorker,
    PlanState& state) {
  PlanCost totalCost;
  auto groupingKeySet = PlanObjectSet::fromObjects(groupingKeys);
  size_t markerCounter = 0;

  auto makeMarker = [&]() {
    auto markerName = fmt::format("__m{}", markerCounter++);
    return make<Column>(
        toName(markerName),
        /*relation=*/nullptr,
        Value{toType(velox::BOOLEAN()), 2});
  };

  // Phase 1: Collect — group distinct aggregates by key set, allocate markers,
  // and remember which marker each aggregate will reference in Phase 3.
  folly::F14VectorMap<PlanObjectSet, MarkDistinctGroup> groups;
  folly::F14FastMap<const Aggregate*, ColumnCP> aggregateToMarker;
  for (const auto* aggregate : aggregates) {
    if (!aggregate->isDistinct()) {
      continue;
    }
    auto markDistinctKeys = unionColumnArgs(groupingKeys, aggregate->args());
    auto keySet = PlanObjectSet::fromObjects(markDistinctKeys);
    if (keySet == groupingKeySet) {
      continue;
    }
    auto [groupIt, isNewGroup] = groups.try_emplace(std::move(keySet));
    auto& group = groupIt->second;
    if (isNewGroup) {
      group.markDistinctKeys = std::move(markDistinctKeys);
      // Reserve the no-mask marker name first so it has the smallest counter
      // value in the group; per-mask markers follow as filters are seen.
      auto* noMaskMarker = makeMarker();
      group.markers.push_back(noMaskMarker);
      group.filterToMarker[nullptr] = noMaskMarker;
    }

    auto* filter = aggregate->condition();
    auto [markerIt, markerInserted] =
        group.filterToMarker.try_emplace(filter, nullptr);
    if (markerInserted) {
      auto* marker = makeMarker();
      markerIt->second = marker;
      group.markers.push_back(marker);
      // Filter conditions are projected to Column refs upstream; this cast
      // cannot return null.
      auto* maskColumn = filter->as<Column>();
      VELOX_CHECK_NOT_NULL(
          maskColumn,
          "MarkDistinct mask must be a Column after flattenAggregates; got {}",
          filter->toString());
      group.masks.push_back(maskColumn);
    }
    aggregateToMarker[aggregate] = markerIt->second;
  }

  // Phase 2: Build a MarkDistinct per key set. F14VectorMap iterates in LIFO;
  // use rbegin/rend for insertion order.
  for (auto it = groups.rbegin(); it != groups.rend(); ++it) {
    auto& group = it->second;

    if (!isSingleWorker) {
      auto [repartitioned, repartitionCost] =
          maybeRepartition(plan, ExprVector(group.markDistinctKeys), state);
      plan = repartitioned;
      totalCost.add(repartitionCost);
    }

    plan = make<MarkDistinct>(
        plan,
        std::move(group.markers),
        std::move(group.markDistinctKeys),
        std::move(group.masks));
    totalCost.add(*plan);
  }

  // Phase 3: Rewrite distinct aggregates via the Phase-1 lookup.
  AggregateVector newAggregates;
  newAggregates.reserve(aggregates.size());
  for (const auto* aggregate : aggregates) {
    auto it = aggregateToMarker.find(aggregate);
    if (it == aggregateToMarker.end()) {
      newAggregates.push_back(aggregate);
      continue;
    }
    newAggregates.push_back(
        aggregate->replaceDistinctAndFilterByMarker(it->second));
  }

  return {std::move(plan), std::move(newAggregates), totalCost};
}

// Collects unique column references from aggregate functions (args, ORDER BY
// keys, and filter conditions). Skips literals since they are constants that
// don't need to flow through GroupId.
ExprVector collectAggregationInputs(const AggregateVector& aggregates) {
  PlanObjectSet seen;
  ExprVector inputs;
  auto maybeAdd = [&](ExprCP expr) {
    if (expr->is(PlanType::kColumnExpr) && !seen.contains(expr)) {
      seen.add(expr);
      inputs.push_back(expr);
    }
  };
  for (const auto* agg : aggregates) {
    for (const auto* arg : agg->args()) {
      maybeAdd(arg);
    }
    for (const auto* key : agg->orderKeys()) {
      maybeAdd(key);
    }
    if (agg->condition()) {
      maybeAdd(agg->condition());
    }
  }
  return inputs;
}

// Constructs a GroupId node from the aggregation plan's grouping sets,
// precomputed grouping keys, and aggregate inputs.
GroupId* makeGroupIdNode(
    RelationOpPtr& plan,
    AggregationPlanCP aggPlan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates) {
  const auto numKeys = groupingKeys.size();
  ColumnVector outputKeys;
  outputKeys.reserve(numKeys);
  for (size_t i = 0; i < numKeys; ++i) {
    outputKeys.push_back(aggPlan->columns()[i]);
  }

  ColumnVector inputKeys;
  inputKeys.reserve(numKeys);
  for (const auto* key : groupingKeys) {
    inputKeys.push_back(key->as<Column>());
  }

  return make<GroupId>(
      plan,
      std::move(inputKeys),
      collectAggregationInputs(aggregates),
      aggPlan->groupingSets(),
      std::move(outputKeys),
      aggPlan->groupId());
}

} // namespace

std::vector<uint32_t> joinKeyPartition(
    const RelationOpPtr& op,
    const ExprVector& keys) {
  const auto& partitionKeys = op->distribution().partitionKeys();
  std::vector<uint32_t> positions;
  positions.reserve(partitionKeys.size());
  for (const auto& partitionKey : partitionKeys) {
    bool found = false;
    for (uint32_t j = 0; j < keys.size(); ++j) {
      if (keys[j]->sameOrEqual(*partitionKey)) {
        positions.push_back(j);
        found = true;
        break;
      }
    }
    if (!found) {
      return {};
    }
  }
  return positions;
}

std::pair<RelationOpPtr, PlanCost> maybeRepartition(
    const RelationOpPtr& plan,
    ExprVector desiredKeys,
    PlanState& state) {
  if (plan->distribution().isGather()) {
    return {plan, {}};
  }

  PlanCost cost;
  if (desiredKeys.empty()) {
    auto* gather =
        make<Repartition>(plan, Distribution::gather(), plan->columns());
    cost.add(*gather);
    commitGroupedLeavesForRepartition(state, gather);
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
    Distribution distribution{/*partitionType=*/nullptr,
                              std::move(desiredKeys)};
    auto* repartition =
        make<Repartition>(plan, std::move(distribution), plan->columns());
    cost.add(*repartition);
    commitGroupedLeavesForRepartition(state, repartition);
    return {repartition, cost};
  }

  return {plan, {}};
}

std::pair<RelationOpPtr, PlanCost> AggregationPlanner::repartitionForAgg(
    const RelationOpPtr& plan,
    const ColumnVector& partitionKeys,
    PlanState& state) const {
  if (isSingleWorker_ || plan->distribution().isGather()) {
    return {plan, {}};
  }

  ExprVector keyExprs(partitionKeys.begin(), partitionKeys.end());
  return maybeRepartition(plan, std::move(keyExprs), state);
}

std::pair<RelationOpPtr, PlanCost> AggregationPlanner::makeSplitAggregationPlan(
    RelationOpPtr plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    const ColumnVector& intermediateColumns,
    const ColumnVector& outputColumns,
    QGVector<int32_t> globalGroupingSets,
    ColumnCP groupId,
    PlanState& state) const {
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
  std::tie(plan, repartitionCost) =
      repartitionForAgg(plan, partitionKeys, state);
  splitAggCost.add(repartitionCost);

  ExprVector finalGroupingKeys(partitionKeys.begin(), partitionKeys.end());
  auto* splitAggPlan = make<Aggregation>(
      plan,
      std::move(finalGroupingKeys),
      /*preGroupedKeys*/ ExprVector{},
      aggregates,
      velox::core::AggregationNode::Step::kFinal,
      outputColumns,
      std::move(globalGroupingSets),
      groupId);
  splitAggCost.add(*splitAggPlan);

  return {splitAggPlan, splitAggCost};
}

std::pair<RelationOpPtr, PlanCost>
AggregationPlanner::makeSingleAggregationPlan(
    RelationOpPtr plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    const ColumnVector& intermediateColumns,
    const ColumnVector& outputColumns,
    QGVector<int32_t> globalGroupingSets,
    ColumnCP groupId,
    PlanState& state) const {
  PlanCost singleAggCost;

  PlanCost repartitionCost;
  if (!globalGroupingSets.empty()) {
    // Gather a global-grouping-set single-step aggregation to one task so its
    // default row is emitted once. A single worker already runs on one task.
    if (!isSingleWorker_) {
      std::tie(plan, repartitionCost) =
          maybeRepartition(plan, /*desiredKeys=*/{}, state);
    }
  } else {
    ColumnVector partitionKeys(
        intermediateColumns.begin(),
        intermediateColumns.begin() + groupingKeys.size());
    std::tie(plan, repartitionCost) =
        repartitionForAgg(plan, partitionKeys, state);
  }
  singleAggCost.add(repartitionCost);

  auto* singleAgg = make<Aggregation>(
      plan,
      groupingKeys,
      /*preGroupedKeys*/ ExprVector{},
      aggregates,
      velox::core::AggregationNode::Step::kSingle,
      outputColumns,
      std::move(globalGroupingSets),
      groupId);
  singleAggCost.add(*singleAgg);

  return {singleAgg, singleAggCost};
}

std::pair<RelationOpPtr, PlanCost>
AggregationPlanner::makeSplitOrSingleAggregationPlan(
    const RelationOpPtr& plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    const ColumnVector& intermediateColumns,
    const ColumnVector& outputColumns,
    QGVector<int32_t> globalGroupingSets,
    ColumnCP groupId,
    PlanState& state) const {
  const bool requiresSingleStep =
      std::any_of(aggregates.begin(), aggregates.end(), [](const auto* agg) {
        return agg->isDistinct() || !agg->orderKeys().empty();
      });

  // ORDER BY / DISTINCT aggregates cannot be split, so use single-step even
  // with global grouping sets.
  if (requiresSingleStep) {
    return makeSingleAggregationPlan(
        plan,
        groupingKeys,
        aggregates,
        intermediateColumns,
        outputColumns,
        std::move(globalGroupingSets),
        groupId,
        state);
  }

  if (!globalGroupingSets.empty()) {
    return makeSplitAggregationPlan(
        plan,
        groupingKeys,
        aggregates,
        intermediateColumns,
        outputColumns,
        std::move(globalGroupingSets),
        groupId,
        state);
  }

  // Otherwise pick the cheaper of split and single-step plans by cost.
  // Both alternatives may insert Repartitions via maybeRepartition, which
  // mutates state.currentGroupedLeaves_ via commitGroupedLeavesForRepartition.
  // Snapshot+restore around each alternative so the second one starts from
  // the same map the first did, and the chosen alternative's mutations are
  // re-applied at the end.
  auto savedMap = state.currentGroupedLeaves();
  auto [splitAggPlan, splitAggCost] = makeSplitAggregationPlan(
      plan,
      groupingKeys,
      aggregates,
      intermediateColumns,
      outputColumns,
      globalGroupingSets,
      groupId,
      state);
  auto splitMap = std::move(state.mutableCurrentGroupedLeaves());

  if (groupingKeys.empty() || alwaysPlanPartialAggregation_) {
    state.mutableCurrentGroupedLeaves() = std::move(splitMap);
    return {std::move(splitAggPlan), splitAggCost};
  }

  state.mutableCurrentGroupedLeaves() = savedMap;
  auto [singleAgg, singleAggCost] = makeSingleAggregationPlan(
      plan,
      groupingKeys,
      aggregates,
      intermediateColumns,
      outputColumns,
      std::move(globalGroupingSets),
      groupId,
      state);

  // Decide by cost when both costs are known. When a cost is unknown (a missing
  // statistic), lessThan is false, so we default to the split (partial+final)
  // plan.
  if (lessThan(singleAggCost.cost, splitAggCost.cost)) {
    return {std::move(singleAgg), singleAggCost};
  }
  state.mutableCurrentGroupedLeaves() = std::move(splitMap);
  return {std::move(splitAggPlan), splitAggCost};
}

std::pair<RelationOpPtr, PlanCost> AggregationPlanner::makeDistinctAggregation(
    RelationOpPtr plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    AggregationPlanCP aggPlan,
    QGVector<int32_t> globalGroupingSets,
    ColumnCP groupId,
    PlanState& state) const {
  // When a groupId column is supplied, the caller must have already added it to
  // groupingKeys.
  VELOX_DCHECK(
      groupId == nullptr ||
          std::find(groupingKeys.begin(), groupingKeys.end(), groupId) !=
              groupingKeys.end(),
      "groupId must be one of the groupingKeys when present");

  if (auto distinctArgs = getCommonDistinctArgs(aggregates)) {
    return makeDistinctToGroupByPlan(
        std::move(plan),
        groupingKeys,
        *distinctArgs,
        aggregates,
        aggPlan,
        std::move(globalGroupingSets),
        groupId,
        state);
  }

  return makeDistinctToMarkDistinctPlan(
      std::move(plan),
      groupingKeys,
      aggregates,
      aggPlan,
      std::move(globalGroupingSets),
      groupId,
      state);
}

std::pair<RelationOpPtr, PlanCost>
AggregationPlanner::makeDistinctToGroupByPlan(
    RelationOpPtr plan,
    const ExprVector& groupingKeys,
    const ExprVector& distinctArgs,
    const AggregateVector& aggregates,
    AggregationPlanCP aggPlan,
    QGVector<int32_t> globalGroupingSets,
    ColumnCP groupId,
    PlanState& state) const {
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
  // Inner level deduplication only, so it does not apply grouping-set behavior;
  // the groupId column (already in groupingKeys) acts as a plain grouping key.
  auto [innerAgg, innerAggCost] = makeSplitOrSingleAggregationPlan(
      plan,
      innerKeys,
      AggregateVector{},
      innerColumns,
      innerColumns,
      /*globalGroupingSets=*/{},
      /*groupId=*/nullptr,
      state);
  totalCost.add(innerAggCost);
  plan = std::move(innerAgg);

  // Make non-distinct aggregation calls for the outer level. The outer
  // aggregation is the result-producing node, so it carries grouping-set
  // semantics.
  auto nonDistinctAggregates = dropDistinctFromAggregates(aggregates);

  auto [outerPlan, outerCost] = makeSplitOrSingleAggregationPlan(
      plan,
      groupingKeys,
      nonDistinctAggregates,
      aggPlan->intermediateColumns(),
      aggPlan->columns(),
      std::move(globalGroupingSets),
      groupId,
      state);
  plan = std::move(outerPlan);
  totalCost.add(outerCost);

  return {plan, totalCost};
}

std::pair<RelationOpPtr, PlanCost>
AggregationPlanner::makeDistinctToMarkDistinctPlan(
    RelationOpPtr plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    AggregationPlanCP aggPlan,
    QGVector<int32_t> globalGroupingSets,
    ColumnCP groupId,
    PlanState& state) const {
  auto [markedPlan, newAggregates, markCost] = addMarkDistinctNodes(
      std::move(plan), groupingKeys, aggregates, isSingleWorker_, state);

  PlanCost totalCost;
  totalCost.add(markCost);

  auto [aggregation, aggCost] = makeSplitOrSingleAggregationPlan(
      markedPlan,
      groupingKeys,
      newAggregates,
      aggPlan->intermediateColumns(),
      aggPlan->columns(),
      std::move(globalGroupingSets),
      groupId,
      state);
  totalCost.add(aggCost);
  return {std::move(aggregation), totalCost};
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

void AggregationPlanner::addGroupingSetsAggregation(
    AggregationPlanCP aggPlan,
    RelationOpPtr& plan,
    const ExprVector& groupingKeys,
    const AggregateVector& aggregates,
    PlanState& state) const {
  auto* groupIdNode = makeGroupIdNode(plan, aggPlan, groupingKeys, aggregates);
  state.addCost(*groupIdNode);
  plan = groupIdNode;

  auto globalGroupingSets = aggPlan->globalGroupingSets();

  // Only pass groupId to the Aggregation when there are global (empty)
  // grouping sets. Velox uses this pair to produce default output rows for
  // empty inputs; without global sets, the groupId column is just a regular
  // grouping key.
  ColumnCP aggGroupId =
      globalGroupingSets.empty() ? nullptr : aggPlan->groupId();

  const auto numKeys = groupingKeys.size();
  ExprVector aggGroupingKeys;
  aggGroupingKeys.reserve(numKeys + 1);
  for (size_t i = 0; i < numKeys; ++i) {
    aggGroupingKeys.push_back(groupIdNode->columns()[i]);
  }
  aggGroupingKeys.push_back(aggPlan->groupId());

  if (isSingleWorker_ && isSingleDriver_) {
    auto [agg, cost] = makeSingleAggregationPlan(
        plan,
        aggGroupingKeys,
        aggregates,
        aggPlan->intermediateColumns(),
        aggPlan->columns(),
        std::move(globalGroupingSets),
        aggGroupId,
        state);
    state.cost.add(cost);
    plan = std::move(agg);
    return;
  }

  const auto hasDistinct =
      std::any_of(aggregates.begin(), aggregates.end(), [](const auto* agg) {
        return agg->isDistinct();
      });
  if (hasDistinct) {
    auto [agg, cost] = makeDistinctAggregation(
        plan,
        aggGroupingKeys,
        aggregates,
        aggPlan,
        std::move(globalGroupingSets),
        aggGroupId,
        state);
    state.cost.add(cost);
    plan = std::move(agg);
    return;
  }

  auto [agg, cost] = makeSplitOrSingleAggregationPlan(
      plan,
      aggGroupingKeys,
      aggregates,
      aggPlan->intermediateColumns(),
      aggPlan->columns(),
      std::move(globalGroupingSets),
      aggGroupId,
      state);
  state.cost.add(cost);
  plan = std::move(agg);
}

void AggregationPlanner::plan(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
  VELOX_CHECK_NOT_NULL(dt->aggregation);
  const auto* aggPlan = dt->aggregation;

  const bool isBucketedAggregation = !alwaysPlanPartialAggregation_ &&
      !isSingleWorker_ && !aggPlan->groupingKeys().empty() &&
      plan->distribution().partitionType() != nullptr &&
      !joinKeyPartition(plan, aggPlan->groupingKeys()).empty();

  PrecomputeProjection precompute(plan, dt, /*projectAllInputs=*/false);
  // When grouping sets are present, don't pass aliases for grouping keys.
  // This lets them pass through PrecomputeProjection without renaming, so
  // aggregate args that reference the same column get their own pass-through
  // entry.
  auto groupingKeys = precompute.toColumns(
      aggPlan->groupingKeys(),
      aggPlan->hasGroupingSets() ? nullptr : &aggPlan->columns());
  auto aggregates = flattenAggregates(aggPlan->aggregates(), precompute);

  plan = std::move(precompute).maybeProject();
  state.place(aggPlan);

  // ORDER BY aggregates force single-step because partial aggregation cannot
  // preserve ORDER BY semantics.
  const auto hasOrderBy =
      std::any_of(aggregates.begin(), aggregates.end(), [](const auto& agg) {
        return !agg->orderKeys().empty();
      });

  const auto hasDistinct =
      std::any_of(aggregates.begin(), aggregates.end(), [](const auto& agg) {
        return agg->isDistinct();
      });

  if (aggPlan->hasGroupingSets()) {
    addGroupingSetsAggregation(aggPlan, plan, groupingKeys, aggregates, state);
    return;
  }

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

  if (isBucketedAggregation && !hasDistinct && !hasOrderBy) {
    auto* singleAgg = make<Aggregation>(
        plan,
        std::move(groupingKeys),
        /*preGroupedKeys=*/ExprVector{},
        std::move(aggregates),
        velox::core::AggregationNode::Step::kSingle,
        aggPlan->columns());
    state.addCost(*singleAgg);
    plan = singleAgg;
    return;
  }

  if (hasDistinct) {
    auto [result, cost] = makeDistinctAggregation(
        plan,
        groupingKeys,
        aggregates,
        aggPlan,
        /*globalGroupingSets=*/{},
        /*groupId=*/nullptr,
        state);
    plan = std::move(result);
    state.cost.add(cost);
    return;
  }

  const auto& [selectedPlan, selectedCost] = makeSplitOrSingleAggregationPlan(
      plan,
      groupingKeys,
      aggregates,
      aggPlan->intermediateColumns(),
      aggPlan->columns(),
      /*globalGroupingSets=*/{},
      /*groupId=*/nullptr,
      state);
  plan = selectedPlan;
  state.cost.add(selectedCost);
}

} // namespace facebook::axiom::optimizer
