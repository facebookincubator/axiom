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

#include "axiom/optimizer/v2/PlanPhysicalPass.h"

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/CostModel.h"
#include "axiom/optimizer/v2/DPhyp.h"
#include "axiom/optimizer/v2/EstimateProvider.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/ExprSimplifier.h"
#include "axiom/optimizer/v2/FallbackJoinPlanner.h"
#include "axiom/optimizer/v2/GroupedRead.h"
#include "axiom/optimizer/v2/HypergraphBuilder.h"
#include "axiom/optimizer/v2/JoinCluster.h"
#include "axiom/optimizer/v2/JoinPartitioning.h"
#include "axiom/optimizer/v2/JoinTreeEmitter.h"
#include "axiom/optimizer/v2/NodeRewriter.h"
#include "axiom/optimizer/v2/PhysicalJoin.h"
#include "axiom/optimizer/v2/PrecomputeProjections.h"
#include "axiom/optimizer/v2/VeloxExprImporter.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// A counting semi join is its own mirror, so a cluster may put either operand
// on the build side. That rests on the shape INTERSECT ALL lowering gives it:
// the keys cover both inputs' columns, so the exchanged orientation emits the
// same values.
void checkCountingSemiJoinExchangeable(const Join* join) {
  VELOX_CHECK(
      PlanObjectSet::fromObjects(join->leftKeys())
          .containsAll(join->left()->outputColumns()),
      "A counting semi join's keys must cover its probe columns: {}",
      join->left()->toString());
  VELOX_CHECK(
      PlanObjectSet::fromObjects(join->rightKeys())
          .containsAll(join->right()->outputColumns()),
      "A counting semi join's keys must cover its build columns: {}",
      join->right()->toString());
}

// Reorder limit: inner / LEFT / RIGHT / FULL equi-joins,
// plus filtering semijoin (kLeftSemiFilter), antijoin (kAnti),
// mark-preserving semijoin (kLeftSemiProject), and counting semijoin
// (kCountingLeftSemiFilter).
bool isClusterable(const Join* join) {
  using velox::core::JoinType;
  if (join->leftKeys().empty()) {
    return false;
  }
  const auto kind = join->joinType();
  if (kind == JoinType::kCountingLeftSemiFilter) {
    checkCountingSemiJoinExchangeable(join);
    return true;
  }
  return kind == JoinType::kInner || kind == JoinType::kLeft ||
      kind == JoinType::kRight || kind == JoinType::kFull ||
      kind == JoinType::kLeftSemiFilter || kind == JoinType::kAnti ||
      kind == JoinType::kLeftSemiProject;
}

// True when a Filter over `node` stands between joins the cluster enumerates
// together. The emitter applies a conjunct at a join, never at a leaf, so a
// predicate taken from a Filter over a single leaf would move up to the first
// join above, away from the scan pushdown placed it on.
// `dissolveCrossJoins` and `opaqueJoins` say which joins the cluster takes in:
// a bare keyless inner join is descended through only when the first is set,
// and a join named by the second always stays a leaf.
bool separatesJoins(
    NodeCP node,
    bool dissolveCrossJoins,
    const folly::F14FastSet<const Join*>& opaqueJoins) {
  if (node->is(NodeType::kFilter)) {
    return separatesJoins(
        node->as<Filter>()->input(), dissolveCrossJoins, opaqueJoins);
  }
  if (node->is(NodeType::kUnnest)) {
    return separatesJoins(
        node->as<Unnest>()->input(), dissolveCrossJoins, opaqueJoins);
  }
  if (!node->is(NodeType::kJoin)) {
    return false;
  }
  const auto* join = node->as<Join>();
  if (isClusterable(join) && !opaqueJoins.contains(join)) {
    return true;
  }
  // A dissolved cross join puts its children in the cluster, so a Filter over
  // one still stands between whatever they contribute.
  return dissolveCrossJoins && join->isInner() && join->leftKeys().empty() &&
      join->filter().empty();
}

// Appends each non-literal column reference in 'args' to 'keys' if not already
// present, building a MarkDistinct key set as `groupingKeys U
// aggregate.args()`. Literals contribute nothing.
ExprVector unionColumnArgs(const ExprVector& keys, const ExprVector& args) {
  ExprVector merged = keys;
  PlanObjectSet seen = PlanObjectSet::fromObjects(keys);
  for (ExprCP arg : args) {
    if (arg->is(PlanType::kLiteralExpr)) {
      continue;
    }
    VELOX_CHECK(
        arg->is(PlanType::kColumnExpr),
        "Expected column or literal aggregate arg: {}",
        arg->toString());
    if (seen.contains(arg)) {
      continue;
    }
    seen.add(arg);
    merged.push_back(arg);
  }
  return merged;
}

// True when every aggregate shares a single distinct signature: all DISTINCT,
// no FILTER, no ORDER BY, and the same column arguments. Velox then dedups them
// in one native distinct aggregation pass (aggregates keep `distinct=true`).
// Any other mix needs MarkDistinct.
bool canUseNativeDistinct(const AggregateCallVector& aggregates) {
  std::optional<PlanObjectSet> commonArgs;
  for (const auto* aggregate : aggregates) {
    if (!aggregate->isDistinct() || aggregate->condition() != nullptr ||
        !aggregate->orderKeys().empty()) {
      return false;
    }
    PlanObjectSet columnArgs;
    for (ExprCP arg : aggregate->args()) {
      if (!arg->is(PlanType::kLiteralExpr)) {
        columnArgs.add(arg);
      }
    }
    if (!commonArgs.has_value()) {
      commonArgs = std::move(columnArgs);
    } else if (columnArgs != *commonArgs) {
      return false;
    }
  }
  return true;
}

// One MarkDistinct group: all distinct aggregates whose key set
// `(groupingKeys U args)` equals 'keys'. Within a group, each unique FILTER
// condition gets its own per-mask marker; aggregates with no FILTER share
// `markers[0]` (the no-mask marker).
struct MarkDistinctGroup {
  ExprVector keys;
  ColumnVector markers;
  ColumnVector masks;
  // Maps each FILTER condition to the marker that records first occurrence
  // among rows where that condition is true. `nullptr` keys the no-mask
  // marker (`markers[0]`).
  folly::F14FastMap<ExprCP, ColumnCP> filterToMarker;
};

struct DistinctExpansion {
  NodeCP input;
  AggregateCallVector aggregates;
};

// Rewrites each aggregate call to read columns: Velox takes a field for an
// argument, a FILTER mask and an ORDER BY key, and a MarkDistinct key set is
// built from the args, so they must be columns before the lowering runs.
// 'precompute' supplies one column per distinct expression, so two aggregates
// reading the same one share it.
//
// A kFinal aggregate's args name the partial's intermediate results, not
// expressions over the input, so they are left alone.
AggregateCallVector precomputeAggregateArgs(
    const AggregateCallVector& aggregates,
    AggregateStep step,
    PrecomputeProjections& precompute,
    Builder& builder) {
  if (step == AggregateStep::kFinal) {
    return aggregates;
  }

  AggregateCallVector result;
  result.reserve(aggregates.size());
  for (const auto* aggregate : aggregates) {
    ExprVector args;
    args.reserve(aggregate->args().size());
    for (ExprCP arg : aggregate->args()) {
      args.push_back(
          precompute.toColumn(arg, /*alias=*/nullptr, /*allowConstant=*/true));
    }
    ExprCP condition = aggregate->condition() != nullptr
        ? precompute.toColumn(aggregate->condition())
        : nullptr;
    ExprVector orderKeys;
    orderKeys.reserve(aggregate->orderKeys().size());
    for (ExprCP key : aggregate->orderKeys()) {
      orderKeys.push_back(precompute.toColumn(key));
    }
    result.push_back(builder.makeAggregate(
        aggregate->name(),
        aggregate->value(),
        std::move(args),
        aggregate->functions(),
        aggregate->isDistinct(),
        condition,
        aggregate->intermediateType(),
        std::move(orderKeys),
        aggregate->orderTypes()));
  }
  return result;
}

// Lowers DISTINCT aggregates. When they share a single distinct signature (see
// `canUseNativeDistinct`) the aggregates are left as-is for Velox's native
// distinct aggregation. Otherwise each unique `(groupingKeys U args)` set gets
// a `MarkDistinct` and its aggregates are rewritten as non-distinct with the
// marker as their FILTER. Distinct aggregates whose args are all grouping keys
// are redundant (GROUP BY already dedups) and keep a native distinct flag
// without a marker.
//
// Grouping-set lowering already ran in translate, so the group-id column (if
// any) is one of the grouping keys and dedup is per grouping set.
DistinctExpansion expandDistinct(
    NodeCP input,
    const ExprVector& groupingKeys,
    const AggregateCallVector& aggregates,
    Builder& builder,
    int32_t numDrivers) {
  if (aggregates.empty() || canUseNativeDistinct(aggregates)) {
    return {input, aggregates};
  }

  PlanObjectSet groupingKeySet = PlanObjectSet::fromObjects(groupingKeys);
  folly::F14VectorMap<PlanObjectSet, MarkDistinctGroup> groups;
  folly::F14FastMap<const optimizer::Aggregate*, ColumnCP> aggregateToMarker;

  bool anyDistinct = false;
  for (const auto* aggregate : aggregates) {
    if (!aggregate->isDistinct()) {
      continue;
    }
    anyDistinct = true;

    ExprVector keys = unionColumnArgs(groupingKeys, aggregate->args());
    PlanObjectSet keySet = PlanObjectSet::fromObjects(keys);
    if (keySet == groupingKeySet) {
      continue;
    }

    auto [groupIt, isNewGroup] = groups.try_emplace(keySet);
    auto& group = groupIt->second;
    if (isNewGroup) {
      group.keys = std::move(keys);
      group.markers.push_back(Column::createBoolean("mark"));
      group.filterToMarker[nullptr] = group.markers.back();
    }

    ExprCP filter = aggregate->condition();
    auto [filterIt, isNewFilter] =
        group.filterToMarker.try_emplace(filter, nullptr);
    if (isNewFilter) {
      group.markers.push_back(Column::createBoolean("mark"));
      filterIt->second = group.markers.back();

      ColumnCP maskColumn = filter->as<Column>();
      VELOX_CHECK_NOT_NULL(
          maskColumn,
          "MarkDistinct mask must be a Column reference; got: {}",
          filter->toString());
      group.masks.push_back(maskColumn);
    }
    aggregateToMarker[aggregate] = filterIt->second;
  }

  if (!anyDistinct) {
    return {input, aggregates};
  }

  NodeCP currentInput = input;
  // F14VectorMap iterates in LIFO; reverse to keep insertion order so the
  // first encountered key set sits closest to the original input.
  for (auto it = groups.rbegin(); it != groups.rend(); ++it) {
    auto& group = it->second;
    // Each distinct key must reach one driver so it receives exactly one
    // first-row marker.
    if (numDrivers > 1 &&
        !currentInput->physicalProperties().coLocates(
            PropertyScope::kDriver, group.keys)) {
      Partitioning partitioning =
          Partitioning::hash(PropertyScope::kDriver, group.keys);
      const Placement placement = Exchange::placementAfter(
          currentInput, partitioning, /*oneDriverPerTask=*/numDrivers == 1);
      currentInput = builder.make<Exchange>(
          {currentInput, std::move(partitioning), placement});
    }
    ColumnVector outputColumns;
    outputColumns.reserve(
        currentInput->outputColumns().size() + group.markers.size());
    appendAll(outputColumns, currentInput->outputColumns());
    appendAll(outputColumns, group.markers);

    currentInput = builder.make<MarkDistinct>({
        currentInput,
        group.markers,
        group.keys,
        group.masks,
        std::move(outputColumns),
    });
  }

  AggregateCallVector newAggregates;
  newAggregates.reserve(aggregates.size());
  for (const auto* aggregate : aggregates) {
    if (auto it = aggregateToMarker.find(aggregate);
        it != aggregateToMarker.end()) {
      newAggregates.push_back(
          aggregate->replaceDistinctAndFilterByMarker(it->second));
    } else {
      // Either non-distinct, or distinct whose args are all in `groupingKeys`
      // (per-group dedup is implicit; Velox handles `distinct=true` natively
      // for the trivial case).
      newAggregates.push_back(aggregate);
    }
  }
  return {currentInput, std::move(newAggregates)};
}

// Walks a cluster's subtree, taking each node into the cluster or ending the
// branch with it as a leaf. `preserved` is false once the walk has entered an
// input a join above does not preserve, where a node that decides which rows
// that join sees cannot move.
class ClusterCollector {
 public:
  // `dissolveCrossJoins` and `opaqueJoins` carry the same meaning they have in
  // `separatesJoins`, and vary independently: the first collection dissolves
  // cross joins with nothing opaque, and the re-collect that stops dissolving
  // them may still have nothing opaque.
  // `stopAtStateFree` keeps each subtree that reads no recursive state a
  // single leaf, so a fixed point's step can replicate it whole.
  ClusterCollector(
      JoinCluster& cluster,
      bool dissolveCrossJoins,
      const folly::F14FastSet<const Join*>& opaqueJoins,
      bool stopAtStateFree)
      : cluster_{cluster},
        dissolveCrossJoins_{dissolveCrossJoins},
        opaqueJoins_{opaqueJoins},
        stopAtStateFree_{stopAtStateFree} {}

  void collect(NodeCP node, bool preserved, bool unnestMovable) {
    if (stopAtStateFree_ && node->requiredStates().empty()) {
      cluster_.leaves.push_back(node);
      return;
    }
    switch (node->nodeType()) {
      case NodeType::kJoin:
        if (collectJoin(node->as<Join>(), preserved, unnestMovable)) {
          return;
        }
        break;
      case NodeType::kFilter:
        if (collectFilter(node->as<Filter>(), preserved, unnestMovable)) {
          return;
        }
        break;
      case NodeType::kUnnest:
        collectUnnest(node->as<Unnest>(), preserved, unnestMovable);
        return;
      default:
        break;
    }
    cluster_.leaves.push_back(node);
  }

 private:
  // These return true when they took the node into the cluster.

  bool collectJoin(JoinCP join, bool preserved, bool unnestMovable) {
    if (isClusterable(join) && !opaqueJoins_.contains(join)) {
      cluster_.joins.push_back(join);
      const auto sides = Join::preservedSides(join->joinType());
      bool leftUnnestMovable = sides.left;
      bool rightUnnestMovable = sides.right;
      if (join->joinType() == velox::core::JoinType::kCountingLeftSemiFilter) {
        leftUnnestMovable = false;
        rightUnnestMovable = false;
      }
      collect(
          join->left(),
          preserved && sides.left,
          unnestMovable && leftUnnestMovable);
      collect(
          join->right(),
          preserved && sides.right,
          unnestMovable && rightUnnestMovable);
      return true;
    }
    // A bare keyless inner join (no keys, no filter) is a comma-join cross
    // product. Descend through it so its children join the cluster as
    // separate relations, letting an equi-predicate elsewhere reconnect
    // them (a spurious cross product that the join graph can avoid). A
    // keyless join that carries a filter is a theta or decorrelated-subquery
    // join; leave it an opaque leaf so its semantics are preserved.
    if (dissolveCrossJoins_ && join->isInner() && join->leftKeys().empty() &&
        join->filter().empty()) {
      collect(join->left(), preserved, unnestMovable);
      collect(join->right(), preserved, unnestMovable);
      return true;
    }
    return false;
  }

  bool collectFilter(FilterCP filter, bool preserved, bool unnestMovable) {
    // A Filter emits its input's columns, so descending through it leaves the
    // relations unchanged. Three things keep it a leaf: a non-deterministic
    // predicate must run where it was written; a Filter inside an input the
    // join above does not preserve decides which rows that join sees, so it
    // cannot move above the join; a Filter separating no joins is already
    // where pushdown put it.
    const bool deterministic = std::none_of(
        filter->predicates().begin(),
        filter->predicates().end(),
        [](ExprCP predicate) { return predicate->containsNonDeterministic(); });
    if (!deterministic || !preserved ||
        !separatesJoins(filter->input(), dissolveCrossJoins_, opaqueJoins_)) {
      return false;
    }
    appendAll(cluster_.filterPredicates, filter->predicates());
    collect(filter->input(), preserved, unnestMovable);
    return true;
  }

  void collectUnnest(UnnestCP unnest, bool preserved, bool unnestMovable) {
    const bool deterministic = std::none_of(
        unnest->unnestExpressions().begin(),
        unnest->unnestExpressions().end(),
        [](ExprCP expression) {
          return expression->containsNonDeterministic();
        });
    // A column-free input can contain operators that need physical planning.
    // Keep the full subtree inside a leaf of the cluster.
    if (!deterministic || unnest->input()->outputColumns().empty()) {
      cluster_.leaves.push_back(unnest);
      return;
    }
    collect(unnest->input(), preserved, unnestMovable);
    // Preserve JoinCluster's post-order invariant.
    cluster_.unnests.push_back(unnest);
    if (!unnestMovable) {
      cluster_.unnestBarriers.push_back(unnest);
    }
  }

  JoinCluster& cluster_;
  const bool dissolveCrossJoins_;
  const folly::F14FastSet<const Join*>& opaqueJoins_;
  const bool stopAtStateFree_;
};

// Removes Unnests whose placement is unconstrained from the graph relation
// list. Walk consumers before producers so a constrained consumer constrains
// every Unnest that supplies one of its expressions.
std::vector<UnnestCP> takeFreeUnnests(JoinCluster& cluster) {
  PlanObjectSet constrainedColumns;
  for (JoinCP join : cluster.joins) {
    constrainedColumns.unionColumns(join->leftKeys());
    constrainedColumns.unionColumns(join->rightKeys());
    constrainedColumns.unionColumns(join->filter());
  }
  constrainedColumns.unionColumns(cluster.filterPredicates);

  const folly::F14FastSet<UnnestCP> barriers{
      cluster.unnestBarriers.begin(), cluster.unnestBarriers.end()};
  folly::F14FastSet<UnnestCP> constrained;
  for (auto it = cluster.unnests.rbegin(); it != cluster.unnests.rend(); ++it) {
    UnnestCP unnest = *it;
    if (barriers.contains(unnest) ||
        constrainedColumns.hasIntersection(unnest->generatedColumns())) {
      constrained.insert(unnest);
      constrainedColumns.unionColumns(unnest->unnestExpressions());
    }
  }

  std::vector<UnnestCP> free;
  std::vector<UnnestCP> remaining;
  free.reserve(cluster.unnests.size());
  remaining.reserve(cluster.unnests.size());
  for (UnnestCP unnest : cluster.unnests) {
    if (constrained.contains(unnest)) {
      remaining.push_back(unnest);
    } else {
      free.push_back(unnest);
      cluster.freeUnnestColumns.unionSet(unnest->generatedColumns());
    }
  }
  cluster.unnests = std::move(remaining);
  return free;
}

struct CollectedJoinCluster {
  // Relations and operators retained in the hypergraph.
  JoinCluster cluster;
  // Deterministic Unnests reconstructed above the emitted join tree.
  std::vector<UnnestCP> freeUnnests;

  // Returns whether every retained relation has a RelationSet bit.
  bool fitsRelationSet() const {
    return cluster.leaves.size() + cluster.unnests.size() <=
        RelationSet::kMaxRelations;
  }
};

CollectedJoinCluster collectJoinCluster(
    JoinCP root,
    bool dissolveCrossJoins,
    const folly::F14FastSet<const Join*>& opaqueJoins,
    bool stopAtStateFree) {
  JoinCluster cluster;
  cluster.root = root;
  ClusterCollector{cluster, dissolveCrossJoins, opaqueJoins, stopAtStateFree}
      .collect(root, /*preserved=*/true, /*unnestMovable=*/true);
  std::vector<UnnestCP> freeUnnests = takeFreeUnnests(cluster);
  return {std::move(cluster), std::move(freeUnnests)};
}

// Partitions relations into sets closed under every edge's total eligibility.
// DPhyp enumerates each set independently and emission joins the sets with
// cross products.
std::vector<RelationSet> connectedComponents(const JoinHypergraph& graph) {
  const size_t numRelations = graph.relations().size();
  std::vector<int32_t> parent(numRelations);
  std::iota(parent.begin(), parent.end(), 0);
  auto find = [&](int32_t id) {
    while (parent[id] != id) {
      parent[id] = parent[parent[id]];
      id = parent[id];
    }
    return id;
  };
  for (const auto& edge : graph.edges()) {
    int32_t representative{-1};
    edge.totalEligibility().forEach([&](int32_t id) {
      if (representative < 0) {
        representative = id;
      } else {
        parent[find(id)] = find(representative);
      }
    });
  }

  std::vector<RelationSet> components;
  std::vector<int32_t> rootToComponent(numRelations, -1);
  for (const auto& relation : graph.relations()) {
    const int32_t root = find(relation.id());
    if (rootToComponent[root] < 0) {
      rootToComponent[root] = static_cast<int32_t>(components.size());
      components.emplace_back();
    }
    components[rootToComponent[root]].add(relation.id());
  }
  return components;
}

// One Unnest removed from the hypergraph and reconstructed above its emitted
// join tree. `replicatedColumns` is the schema required immediately below the
// Unnest; `outputColumns` is the schema it must expose to the next step.
struct FreeUnnestStep {
  // Original logical Unnest supplying expressions and generated columns.
  UnnestCP unnest;
  // Columns passed through from the preceding node.
  ColumnVector replicatedColumns;
  // Schema exposed to the next reconstruction step.
  ColumnVector outputColumns;
};

// Schema contract between graph emission and reconstruction of free Unnests.
// `graphOutputColumns` is the exact schema demanded from the join graph.
// `steps` stays in the collector's producer-before-consumer order, so applying
// it from front to back rebuilds dependent Unnests correctly.
struct FreeUnnestPlacement {
  // Schema required from the emitted join graph.
  ColumnVector graphOutputColumns;
  // Unnests to rebuild in producer-before-consumer order.
  std::vector<FreeUnnestStep> steps;
};

FreeUnnestPlacement planFreeUnnests(
    const std::vector<UnnestCP>& unnests,
    const ColumnVector& rootOutputColumns) {
  ColumnVector required = rootOutputColumns;
  PlanObjectSet requiredSet = PlanObjectSet::fromObjects(required);
  std::vector<FreeUnnestStep> steps(unnests.size());

  for (size_t i = unnests.size(); i > 0; --i) {
    UnnestCP unnest = unnests[i - 1];
    const PlanObjectSet produced = unnest->generatedColumns();
    ColumnVector replicated;
    for (ColumnCP column : required) {
      if (!produced.contains(column)) {
        replicated.push_back(column);
      }
    }

    ColumnVector output = replicated;
    for (ColumnCP column : unnest->outputColumns()) {
      if (produced.contains(column) &&
          (requiredSet.contains(column) ||
           column == unnest->ordinalityColumn() ||
           column == unnest->markerColumn())) {
        output.push_back(column);
      }
    }
    steps[i - 1] = {unnest, replicated, std::move(output)};

    required = std::move(replicated);
    requiredSet = PlanObjectSet::fromObjects(required);
    for (ExprCP expression : unnest->unnestExpressions()) {
      expression->columns().forEach<Column>([&](ColumnCP column) {
        if (!requiredSet.contains(column)) {
          requiredSet.add(column);
          required.push_back(column);
        }
      });
    }
  }
  return {std::move(required), std::move(steps)};
}

NodeCP placeFreeUnnests(
    NodeCP input,
    const FreeUnnestPlacement& placement,
    const ColumnVector& rootOutputColumns,
    Builder& builder,
    ExprSimplifier& simplifier) {
  for (const auto& step : placement.steps) {
    input = PhysicalJoin::makeUnnest(
        {input,
         step.unnest->unnestExpressions(),
         step.replicatedColumns,
         step.unnest->unnestColumns(),
         step.unnest->ordinalityColumn(),
         step.unnest->markerColumn(),
         step.outputColumns},
        builder,
        simplifier);
  }
  if (input->outputColumns() != rootOutputColumns) {
    input = builder.make<Project>(
        {input, toExprs(rootOutputColumns), rootOutputColumns});
  }
  return input;
}

// Keeps a join opaque when another join predicate reads a fresh value it
// produces. A Filter predicate remains guarded by the producing outer edge's
// eligibility and can participate in reordering. A semi-project mark has no
// source relation, so any predicate that reads it keeps its producer opaque.
folly::F14FastSet<const Join*> valueProducersReadInCluster(
    const JoinCluster& cluster) {
  PlanObjectSet joinPredicateColumns;
  for (JoinCP join : cluster.joins) {
    auto add = [&](const ExprVector& exprs) {
      for (ExprCP expr : exprs) {
        joinPredicateColumns.unionSet(expr->columns());
      }
    };
    add(join->leftKeys());
    add(join->rightKeys());
    add(join->filter());
  }
  // A predicate the cluster took from a Filter resolves against the same
  // relations, so a mark it reads also keeps its producer opaque.
  PlanObjectSet predicateColumns = joinPredicateColumns;
  for (ExprCP predicate : cluster.filterPredicates) {
    predicateColumns.unionSet(predicate->columns());
  }

  folly::F14FastSet<const Join*> opaqueJoins;
  for (JoinCP join : cluster.joins) {
    if (join->isLeftSemiProject() &&
        predicateColumns.contains(join->markColumn())) {
      opaqueJoins.insert(join);
      continue;
    }
    for (size_t i = 0; i < join->outputColumns().size(); ++i) {
      if (join->outputColumns()[i] != join->sourceColumns()[i] &&
          joinPredicateColumns.contains(join->outputColumns()[i])) {
        opaqueJoins.insert(join);
        break;
      }
    }
  }
  return opaqueJoins;
}

// True if some relation appears in an edge's TES but in no edge's left/right
// key endpoints. Such a relation is connected only by correlation (e.g. an
// outer table referenced inside a decorrelated subquery), so DPhyp cannot grow
// a subgraph to include it. Dissolving a cross join that strands such a
// relation produces an unplannable graph.
bool hasEndpointStrandedRelation(const JoinHypergraph& graph) {
  RelationSet endpoints;
  RelationSet tesUnion;
  for (const auto& edge : graph.edges()) {
    endpoints.unionSet(edge.leftEndpoints());
    endpoints.unionSet(edge.rightEndpoints());
    tesUnion.unionSet(edge.totalEligibility());
  }
  tesUnion.except(endpoints);
  return !tesUnion.empty();
}

// What a consumer needs of an input's partitioning.
enum class Alignment {
  // Rows that agree on the keys share a partition. The partitioning may be on
  // any subset of them, in any order. Enough for an aggregation: every group
  // lands whole on one task.
  kCoLocated,

  // Partitioned on exactly these keys, in this order. A join and a write need
  // this: each compares its keys against another side's, so key i of one must
  // be hashed the same as key i of the other, which a subset does not give.
  kExactKeys,
};

// Physical distribution and ordering required by one connector writer.
struct WriterRequirements {
  // Expressions that choose a writer partition.
  ExprVector partitionKeys;
  // Native connector partitioning within one worker.
  std::shared_ptr<const connector::PartitionType> partitionType;
  // Expressions that order rows within each writer.
  ExprVector orderKeys;
  // Direction and null placement corresponding to `orderKeys`.
  OrderTypeVector orderTypes;
};

OrderTypeVector toOrderTypes(std::span<const connector::SortOrder> sortOrders) {
  OrderTypeVector orderTypes;
  orderTypes.reserve(sortOrders.size());
  for (const auto& order : sortOrders) {
    orderTypes.push_back(
        order.isAscending ? (order.isNullsFirst ? OrderType::kAscNullsFirst
                                                : OrderType::kAscNullsLast)
                          : (order.isNullsFirst ? OrderType::kDescNullsFirst
                                                : OrderType::kDescNullsLast));
  }
  return orderTypes;
}

// True when 'partitioning' meets 'alignment' on 'keys'.
bool satisfies(
    const Partitioning& partitioning,
    const ExprVector& keys,
    Alignment alignment) {
  switch (alignment) {
    case Alignment::kCoLocated:
      return partitioning.coLocates(keys);
    case Alignment::kExactKeys:
      return partitioning.isBucketedOn(keys);
  }
  VELOX_UNREACHABLE();
}

// How rows run on one task or several, with one driver per task or several.
Placement placementOf(bool oneTask, bool oneDriverPerTask) {
  if (oneTask) {
    return oneDriverPerTask ? Placement::kSingleThread : Placement::kSingleTask;
  }
  return oneDriverPerTask ? Placement::kOneDriverPerTask
                          : Placement::kDistributed;
}

// A partitioned loop's body runs as a chain of plans, each driven by the
// exchange that links it to the plan before, and a join is driven by its left
// input. Puts on the left each join input that reads the exchange.
class ExchangeProbeRewriter : public NodeRewriter<> {
 public:
  explicit ExchangeProbeRewriter(Builder& builder) : NodeRewriter(builder) {}

  using NodeRewriter<>::rewrite;

  // True when a join reads the exchange only on its right and cannot exchange
  // its inputs: the body then cannot run as a chain.
  bool unswappable() const {
    return unswappable_;
  }

 protected:
  NodeCP rewriteJoin(const Join* node, NoContext& context) override {
    const auto* join = NodeRewriter<>::rewriteJoin(node, context)->as<Join>();
    if (!FixedPoint::movesState(join->right()) ||
        FixedPoint::movesState(join->left())) {
      return join;
    }
    if (!Join::canSwapInputs(join->joinType())) {
      unswappable_ = true;
      return join;
    }
    Join::Key key{
        join->left(),
        join->right(),
        join->joinType(),
        join->leftKeys(),
        join->rightKeys(),
        join->filter(),
        join->nullAware(),
        join->nullAsValue(),
        join->outputColumns(),
        join->sourceColumns()};
    key.swapInputs();
    key.buildIsLoopInvariant = Join::canBuildLoopInvariant(
        key.joinType, key.rightKeys, key.nullAsValue, key.left, key.right);
    return builder().make<Join>(key);
  }

 private:
  bool unswappable_{false};
};

class PhysicalPlanRewriter : public NodeRewriter<> {
 public:
  // 'oneTask' says whether the plan runs on one task. A fixed point's step
  // plans within a task, so its 'numWorkers' is 1 whatever the plan's task
  // count; its leaves run on 'numDrivers' drivers per task. 'replicate', when
  // set, plans each subtree that reads no recursive state: a fixed point's
  // step uses it to place such inputs where the loop needs them.
  // 'statePartitioning' says where a fixed point's state lives, which every
  // read of the state in its step carries. 'gatherOntoOneDriver' puts the rows
  // of each gather between workers on one driver: the gather starts the last
  // plan of a fixed point's convergence, which Velox runs on one driver.
  PhysicalPlanRewriter(
      Builder& builder,
      ExprSimplifier& simplifier,
      EstimateProvider& estimateProvider,
      const OptimizerSession& session,
      const OptimizerOptions& options,
      int32_t numWorkers,
      int32_t numDrivers,
      bool oneTask,
      std::function<NodeCP(NodeCP)> replicate = nullptr,
      Partitioning statePartitioning = {},
      bool gatherOntoOneDriver = false)
      : NodeRewriter(builder),
        session_{session},
        options_{options},
        numWorkers_{numWorkers},
        numDrivers_{numDrivers},
        oneTask_{oneTask},
        replicate_{std::move(replicate)},
        statePartitioning_{std::move(statePartitioning)},
        gatherOntoOneDriver_{gatherOntoOneDriver},
        exprFactory_{builder},
        simplifier_{simplifier},
        estimateProvider_{estimateProvider} {}

  // Re-exposes the rewrite(NodeCP) overload hidden by the override below.
  using NodeRewriter<>::rewrite;

  // Rewriting a node is a pure function of the node, so descend each node
  // once.
  NodeCP rewrite(NodeCP node, NoContext& context) override {
    const auto it = rewrittenNodes_.find(node);
    if (it != rewrittenNodes_.end()) {
      return it->second;
    }
    NodeCP rewritten = replicate_ != nullptr && node->requiredStates().empty()
        ? replicate_(node)
        : NodeRewriter<>::rewrite(node, context);
    rewrittenNodes_.emplace(node, rewritten);
    return rewritten;
  }

  NodeCP rewriteProject(const Project* node, NoContext& context) override {
    NodeCP input = rewrite(node->input(), context);
    if (input == node->input()) {
      return node;
    }
    return PrecomputeProjections::makeProject(
        input, node->exprs(), node->outputColumns(), builder(), simplifier_);
  }

 protected:
  NodeCP rewriteRepeat(const Repeat* /*node*/, NoContext& /*context*/)
      override {
    VELOX_UNREACHABLE("Repeat is lowered before physical planning");
  }

  NodeCP rewriteScan(const Scan* node, NoContext& /*context*/) override {
    return builder().make<Scan>(
        {.baseTable = node->baseTable(),
         .outputColumns = node->outputColumns(),
         .scanHandle = node->scanHandle(),
         .groupedPartitionType = node->groupedPartitionType(),
         .placement = placementOf(
             oneTask_ || node->baseTable()->layout()->runsOnCoordinator(),
             numDrivers_ == 1)});
  }

  NodeCP rewriteValues(const Values* node, NoContext& /*context*/) override {
    return builder().make<Values>(
        {.source = node->source(),
         .rows = node->rows(),
         .outputColumns = node->outputColumns(),
         .channels = node->channels(),
         .placement = placementOf(/*oneTask=*/true, numDrivers_ == 1)});
  }

  NodeCP rewriteWorkingTable(const WorkingTable* node, NoContext& /*context*/)
      override {
    return builder().make<WorkingTable>(
        {.name = node->name(),
         .stateColumns = node->stateColumns(),
         .outputColumns = node->outputColumns(),
         .readMode = node->readMode(),
         .partitioning = statePartitioning_,
         .placement = placementOf(
             oneTask_ || statePartitioning_.is(PartitionKind::kGather),
             numDrivers_ == 1)});
  }

  // With several workers, the loop is one partitioned loop when it cannot run
  // as independent loops or its step joins the state with a subtree too large
  // to broadcast, and the state has keys to partition on (see
  // `planPartitionedLoop`).
  //
  // Otherwise each task that runs the anchor loops over its own anchor rows,
  // with every subtree of the step that reads no recursive state placed whole
  // on each of those tasks. Each loop stops when its own delta is empty, so the
  // loops together compute the fixed point when each row of the step derives
  // from one row of the loop state and the loop has no convergence, which
  // decides on the whole state; otherwise the anchor is gathered and the loop
  // runs on one task.
  NodeCP rewriteFixedPoint(const FixedPoint* node, NoContext& context)
      override {
    NodeCP anchor = rewrite(node->anchor(), context);
    estimateProvider_.estimateLoopState(*node, anchor);

    if (numWorkers_ > 1 &&
        (!isRowLocal(node->step()) || node->convergence() != nullptr ||
         joinsInputTooLargeToBroadcast(node->step()))) {
      ExprFactory::ExprSubstitution readToState;
      WorkingTable::forEachRead(
          node->step(), node->name(), [&](const WorkingTable& read) {
            for (size_t i = 0; i < read.outputColumns().size(); ++i) {
              readToState.emplace(
                  read.outputColumns()[i], read.stateColumns()[i]);
            }
          });
      PlanObjectSet readColumns;
      for (const auto& [readColumn, stateColumn] : readToState) {
        readColumns.add(readColumn);
      }
      NodeCP partitionedLoop = nullptr;
      if (auto keys = firstStateKeys(node->step(), readColumns)) {
        partitionedLoop = planPartitionedLoop(
            node, anchor, exprFactory_.replace(*keys, readToState));
      }
      NodeCP replicatedLoop = planReplicatedLoop(node, anchor);
      if (partitionedLoop != nullptr && replicatedLoop != nullptr) {
        const auto partitionedBytes = bytesPerIteration(partitionedLoop);
        const auto replicatedBytes = bytesPerIteration(replicatedLoop);
        return partitionedBytes.has_value() && replicatedBytes.has_value() &&
                *replicatedBytes < *partitionedBytes
            ? replicatedLoop
            : partitionedLoop;
      }
      if (partitionedLoop != nullptr) {
        return partitionedLoop;
      }
      if (replicatedLoop != nullptr) {
        return replicatedLoop;
      }
    }

    std::function<NodeCP(NodeCP)> replicate;
    Partitioning statePartitioning;
    if (numWorkers_ > 1) {
      if (!isRowLocal(node->step()) || node->convergence() != nullptr) {
        anchor = ensureGathered(anchor);
      }
      const bool gathered = isGathered(anchor);
      replicate = [this, gathered](NodeCP input) {
        NodeCP planned = rewrite(input);
        // Inlined into the step, the input runs on the step's drivers but is
        // placed for this plan's, which states less than is true.
        if (gathered && isGathered(planned)) {
          return planned;
        }
        return exchange(
            {planned,
             gathered ? Partitioning::gather(PropertyScope::kGlobal)
                      : Partitioning::globalBroadcast()});
      };
      if (gathered) {
        statePartitioning = Partitioning::gather(PropertyScope::kGlobal);
      }
    }

    PhysicalPlanRewriter taskLocal{
        builder(),
        simplifier_,
        estimateProvider_,
        session_,
        options_,
        /*numWorkers=*/1,
        numDrivers_,
        oneTask_,
        replicate,
        statePartitioning};
    NodeCP step = taskLocal.rewrite(node->step());
    NodeCP convergence = nullptr;
    if (node->convergence() != nullptr) {
      // The convergence of a loop on one task is one plan, which Velox runs on
      // one driver.
      PhysicalPlanRewriter oneDriver{
          builder(),
          simplifier_,
          estimateProvider_,
          session_,
          options_,
          /*numWorkers=*/1,
          /*numDrivers=*/1,
          oneTask_,
          std::move(replicate),
          std::move(statePartitioning)};
      convergence = oneDriver.rewrite(node->convergence());
    }
    if (anchor == node->anchor() && step == node->step() &&
        convergence == node->convergence()) {
      return node;
    }
    return builder().make<FixedPoint>({
        .anchor = anchor,
        .step = step,
        .name = node->name(),
        .outputColumns = node->outputColumns(),
        .sourceColumns = node->sourceColumns(),
        .maxIterations = node->maxIterations(),
        .failAtBound = node->failAtBound(),
        .mode = node->mode(),
        .convergence = convergence,
    });
  }

  // Plans 'node' as one loop over all workers, whose step is planned with the
  // query's workers. 'keys' are the step's first keys, expressions over the
  // state's columns (see `firstStateKeys`). The anchor is repartitioned on them
  // so the first iteration finds its rows in place. When every key is a state
  // column, the state stays hash-partitioned on them: the step's output is
  // repartitioned on them for the next iteration. Otherwise the state read
  // claims no partitioning and the step repartitions what it needs. Returns
  // null when the planned step would move the state other than by hash: the
  // loop's body then has no single width.
  NodeCP planPartitionedLoop(
      const FixedPoint* node,
      NodeCP anchor,
      const ExprVector& keys) {
    // Rewrites the keys over the columns of a branch whose columns line up
    // with the state's.
    const auto keysOver = [&](const ColumnVector& branchColumns) {
      ExprFactory::ExprSubstitution substitution;
      for (size_t i = 0; i < branchColumns.size(); ++i) {
        substitution.emplace(node->outputColumns()[i], branchColumns[i]);
      }
      return exprFactory_.replace(keys, substitution);
    };
    anchor = ensurePartitionedOn(anchor, keysOver(node->sourceColumns()));

    const bool keyedState = std::ranges::all_of(
        keys, [](ExprCP key) { return key->is(PlanType::kColumnExpr); });
    const Partitioning statePartitioning = keyedState
        ? Partitioning::hash(PropertyScope::kGlobal, keys)
        : Partitioning{};
    PhysicalPlanRewriter partitioned{
        builder(),
        simplifier_,
        estimateProvider_,
        session_,
        options_,
        numWorkers_,
        numDrivers_,
        oneTask_,
        [this](NodeCP input) { return rewrite(input); },
        statePartitioning};
    NodeCP step = partitioned.rewrite(node->step());
    if (keyedState) {
      step = ensurePartitionedOn(step, keysOver(step->outputColumns()));
    }
    if (!movesStateByHash(step)) {
      return nullptr;
    }

    // The convergence is gathered, so the loop's workers stop on one answer.
    // Velox gathers only in the last link of a convergence's chain, and runs
    // the plan after it on one driver.
    PhysicalPlanRewriter partitionedConvergence{
        builder(),
        simplifier_,
        estimateProvider_,
        session_,
        options_,
        numWorkers_,
        numDrivers_,
        oneTask_,
        [this](NodeCP input) { return rewrite(input); },
        statePartitioning,
        /*gatherOntoOneDriver=*/true};
    ExchangeProbeRewriter probeRewriter{builder()};
    step = probeRewriter.rewrite(step);
    NodeCP convergence = probeRewriter.rewrite(
        partitionedConvergence.ensureGathered(partitionedConvergence.rewrite(
            node->convergence() != nullptr ? node->convergence()
                                           : emptyDeltaCheck(node))));
    if (probeRewriter.unswappable()) {
      return nullptr;
    }

    return builder().make<FixedPoint>({
        .anchor = anchor,
        .step = step,
        .name = node->name(),
        .outputColumns = node->outputColumns(),
        .sourceColumns = node->sourceColumns(),
        .maxIterations = node->maxIterations(),
        .failAtBound = node->failAtBound(),
        .mode = node->mode(),
        .convergence = convergence,
    });
  }

  // Plans 'node' as one loop over all workers with its state replicated on
  // every worker. The anchor is broadcast, every read of the state holds all of
  // it, and the step is planned with the query's workers; its output is
  // broadcast for the next iteration unless it is replicated already. The
  // convergence is planned over the state with no exchange: worker 0 decides on
  // its own replica. Returns null when the loop appends to its state, the state
  // or another input the step broadcasts does not fit `broadcast_size_limit`,
  // the planned step moves no rows between workers, so that every worker would
  // repeat the same work, or the convergence moves the state.
  NodeCP planReplicatedLoop(const FixedPoint* node, NodeCP anchor) {
    if (node->mode() != FixedPointMode::kReplace || !broadcastFits(node)) {
      return nullptr;
    }
    anchor = exchange({anchor, Partitioning::globalBroadcast()});

    PhysicalPlanRewriter replicated{
        builder(),
        simplifier_,
        estimateProvider_,
        session_,
        options_,
        numWorkers_,
        numDrivers_,
        oneTask_,
        [this](NodeCP input) { return rewrite(input); },
        Partitioning::globalBroadcast()};
    NodeCP step = replicated.rewrite(node->step());
    if (!step->physicalProperties().globalPartition.is(
            PartitionKind::kBroadcast)) {
      step = exchange({step, Partitioning::globalBroadcast()});
    }
    // A convergence that moves no state is one plan, which Velox runs on one
    // driver.
    PhysicalPlanRewriter oneDriver{
        builder(),
        simplifier_,
        estimateProvider_,
        session_,
        options_,
        numWorkers_,
        /*numDrivers=*/1,
        oneTask_,
        [this](NodeCP input) { return rewrite(input); },
        Partitioning::globalBroadcast()};
    NodeCP convergence = oneDriver.rewrite(
        node->convergence() != nullptr ? node->convergence()
                                       : emptyDeltaCheck(node));
    if (!FixedPoint::movesState(step) || !broadcastsFit(step) ||
        FixedPoint::movesState(convergence)) {
      return nullptr;
    }
    ExchangeProbeRewriter probeRewriter{builder()};
    step = probeRewriter.rewrite(step);
    if (probeRewriter.unswappable()) {
      return nullptr;
    }

    return builder().make<FixedPoint>({
        .anchor = anchor,
        .step = step,
        .name = node->name(),
        .outputColumns = node->outputColumns(),
        .sourceColumns = node->sourceColumns(),
        .maxIterations = node->maxIterations(),
        .failAtBound = node->failAtBound(),
        .mode = node->mode(),
        .convergence = convergence,
    });
  }

  // Returns the bytes the step and the convergence of 'loop', a placed fixed
  // point, move between workers in each iteration: each exchange between
  // workers of rows derived from the state moves its input's estimated size,
  // once per worker for a broadcast. Exchanges of inputs that read no state
  // run once and are left out. Unknown when an exchanged input's size is.
  std::optional<float> bytesPerIteration(NodeCP loop) {
    std::optional<float> total = 0;
    std::function<void(NodeCP)> visit = [&](NodeCP node) {
      if (node->is(NodeType::kExchange)) {
        const auto* exchange = node->as<Exchange>();
        if (!exchange->partitioning().isDriver() &&
            !exchange->input()->requiredStates().empty()) {
          const auto size = estimatedSize(exchange->input());
          if (!size.has_value() || !total.has_value()) {
            total = std::nullopt;
          } else {
            *total += exchange->partitioning().is(PartitionKind::kBroadcast)
                ? *size * numWorkers_
                : *size;
          }
        }
      }
      for (NodeCP input : node->inputs()) {
        visit(input);
      }
    };
    const auto* fixedPoint = loop->as<FixedPoint>();
    visit(fixedPoint->step());
    visit(fixedPoint->convergence());
    return total;
  }

  // True when every broadcast exchange in 'node' copies an input that fits
  // `broadcast_size_limit`.
  bool broadcastsFit(NodeCP node) {
    if (node->is(NodeType::kExchange)) {
      const auto* exchange = node->as<Exchange>();
      if (exchange->partitioning().is(PartitionKind::kBroadcast) &&
          !broadcastFits(exchange->input())) {
        return false;
      }
    }
    return std::ranges::all_of(
        node->inputs(), [&](NodeCP input) { return broadcastsFit(input); });
  }

  // Returns a check over 'node''s latest delta that is true when the delta has
  // no rows: `count(*) = 0`. Planned with the loop's workers, it counts each
  // worker's rows and adds up the counts on one task.
  NodeCP emptyDeltaCheck(const FixedPoint* node) {
    const auto& functionNames = builder().functionNames();
    VELOX_USER_CHECK_NOT_NULL(
        functionNames.count,
        "A partitioned fixed point needs a count aggregate function");
    VELOX_USER_CHECK_NOT_NULL(
        functionNames.equality,
        "A partitioned fixed point needs an equality function");

    ColumnVector deltaColumns;
    deltaColumns.reserve(node->outputColumns().size());
    for (ColumnCP column : node->outputColumns()) {
      deltaColumns.push_back(Column::createWithUnknownValue(column));
    }
    NodeCP delta = builder().make<WorkingTable>(
        {.name = node->name(),
         .stateColumns = node->outputColumns(),
         .outputColumns = std::move(deltaColumns),
         .readMode = WorkingTableReadMode::kLatestDelta});
    const TypeCP bigint = toType(velox::BIGINT());
    const Value oneRow{bigint, 1};
    ColumnCP numRows = Column::create("num_rows", oneRow);
    NodeCP count = builder().make<Aggregate>(
        {.input = delta,
         .aggregates = {builder().makeAggregate(
             functionNames.count,
             oneRow,
             /*args=*/{},
             FunctionSet{},
             /*isDistinct=*/false,
             /*condition=*/nullptr,
             /*intermediateType=*/bigint,
             /*orderKeys=*/{},
             /*orderTypes=*/{})},
         .outputColumns = {numRows}});
    return builder().make<Project>(
        {count,
         {exprFactory_.makeEq(
             numRows,
             builder().makeLiteral(velox::Variant(int64_t{0}), bigint))},
         {Column::createBoolean("converged")}});
  }

  // Returns 'input', with its own columns, hash-partitioned on 'keys',
  // expressions over its columns.
  NodeCP ensurePartitionedOn(NodeCP input, const ExprVector& keys) {
    if (input->physicalProperties().globalPartition.sameClassAs(
            Partitioning::hash(PropertyScope::kGlobal, keys))) {
      return input;
    }
    auto [keyed, columnKeys] = PrecomputeProjections::materializeKeys(
        input, keys, builder(), simplifier_);
    NodeCP partitioned = partition(keyed, columnKeys);
    if (partitioned->outputColumns() == input->outputColumns()) {
      return partitioned;
    }
    return builder().make<Project>(
        {partitioned, toExprs(input->outputColumns()), input->outputColumns()});
  }

  // Returns the keys of the first operation above the state read that needs
  // its input partitioned: a join's keys on the state's side, or an
  // aggregation's grouping keys. The anchor is placed by the same keys, so
  // they must read only 'readColumns', the columns of the state's reads;
  // returns nullopt when one reads a column the step computes, such as an
  // unnest's output.
  static std::optional<ExprVector> firstStateKeys(
      NodeCP node,
      const PlanObjectSet& readColumns) {
    NodeCP stateInput = nullptr;
    for (NodeCP input : node->inputs()) {
      if (!input->requiredStates().empty()) {
        stateInput = input;
      }
    }
    if (stateInput == nullptr) {
      return std::nullopt;
    }
    if (auto keys = firstStateKeys(stateInput, readColumns)) {
      return keys;
    }

    const ExprVector* keys = nullptr;
    if (node->is(NodeType::kJoin)) {
      const auto* join = node->as<Join>();
      keys =
          stateInput == join->left() ? &join->leftKeys() : &join->rightKeys();
    } else if (node->is(NodeType::kAggregate)) {
      keys = &node->as<Aggregate>()->groupingKeys();
    }
    if (keys == nullptr || keys->empty() ||
        !readColumns.containsColumns(*keys)) {
      return std::nullopt;
    }
    return *keys;
  }

  // True when 'node' joins the recursive state with a subtree that reads no
  // state and does not fit the broadcast limit, as this plan places that
  // subtree.
  bool joinsInputTooLargeToBroadcast(NodeCP node) {
    for (NodeCP input : node->inputs()) {
      if (input->requiredStates().empty()) {
        if (node->is(NodeType::kJoin) && !broadcastFits(rewrite(input))) {
          return true;
        }
      } else if (joinsInputTooLargeToBroadcast(input)) {
        return true;
      }
    }
    return false;
  }

  // True when every exchange between workers in 'node' that moves rows
  // derived from the state is a standard hash repartition.
  static bool movesStateByHash(NodeCP node) {
    if (node->requiredStates().empty()) {
      return true;
    }
    if (node->is(NodeType::kExchange)) {
      const auto& partitioning = node->as<Exchange>()->partitioning();
      if (!partitioning.isDriver() &&
          (!partitioning.is(PartitionKind::kPartitioned) ||
           partitioning.partitionType != nullptr)) {
        return false;
      }
    }
    return std::ranges::all_of(node->inputs(), movesStateByHash);
  }

  // Returns output bytes from a point cardinality estimate, or nullopt when the
  // estimate is unavailable.
  std::optional<float> estimatedSize(NodeCP node) {
    const auto cardinality = estimateProvider_.estimate(node).cardinality;
    if (!cardinality.has_value()) {
      return std::nullopt;
    }
    return *cardinality * std::max<float>(1, byteSize(node->outputColumns()));
  }

  // Chooses the build side of inner, left, and right joins. In a loop's step,
  // a loop-invariant input goes on the build side (see
  // `Join::loopInvariantSide`). Otherwise the smaller input does when both
  // inputs have point estimates, and the written orientation stays when not. A
  // distributed keyless left or right join instead puts its non-preserved input
  // on the build side so that input can be broadcast.
  Join::Key chooseBuildSide(Join::Key join) {
    if (numWorkers_ > 1 && join.leftKeys.empty()) {
      if (join.joinType == velox::core::JoinType::kLeft) {
        return join;
      }
      if (join.joinType == velox::core::JoinType::kRight) {
        join.swapInputs();
        return join;
      }
    }

    const Join::Sides loopInvariant = Join::loopInvariantSide(
        join.joinType,
        !join.leftKeys.empty(),
        join.nullAsValue,
        {.left = !join.left->requiredStates().empty(),
         .right = !join.right->requiredStates().empty()});
    if (loopInvariant.right) {
      return join;
    }
    if (loopInvariant.left) {
      join.swapInputs();
      return join;
    }

    if (join.joinType != velox::core::JoinType::kInner &&
        join.joinType != velox::core::JoinType::kLeft &&
        join.joinType != velox::core::JoinType::kRight) {
      return join;
    }

    const auto leftSize = estimatedSize(join.left);
    const auto rightSize = estimatedSize(join.right);
    if (leftSize.has_value() && rightSize.has_value() &&
        *leftSize < *rightSize) {
      join.swapInputs();
    }
    return join;
  }

  // Returns whether 'node' fits the per-worker broadcast limit.
  bool broadcastFits(NodeCP node) {
    return CostModel::broadcastSizeIfFits(
               estimateProvider_.estimate(node).cardinality,
               byteSize(node->outputColumns()),
               options_.broadcastSizeLimit)
        .has_value();
  }

  // Returns whether copying 'build' to every task of the probe's stage moves
  // fewer bytes than repartitioning 'probe'.
  bool broadcastMovesLess(NodeCP probe, NodeCP build) {
    if (!broadcastFits(build)) {
      return false;
    }
    const auto partitionBytes = estimatedSize(probe);
    const auto buildBytes = estimatedSize(build);
    return partitionBytes.has_value() && buildBytes.has_value() &&
        *buildBytes *
            probe->physicalProperties().globalPartition.numStageTasks(
                numWorkers_, options_.hashStageTasks(numWorkers_)) <
        *partitionBytes;
  }

  // Rewrites both inputs and copies the join properties.
  Join::Key rewriteJoinInputs(const Join* node, NoContext& context) {
    return {
        .left = rewrite(node->left(), context),
        .right = rewrite(node->right(), context),
        .joinType = node->joinType(),
        .leftKeys = node->leftKeys(),
        .rightKeys = node->rightKeys(),
        .filter = node->filter(),
        .nullAware = node->nullAware(),
        .nullAsValue = node->nullAsValue(),
        .outputColumns = node->outputColumns(),
        .sourceColumns = node->sourceColumns(),
    };
  }

  NodeCP makeDriverPhysicalJoin(Join::Key join) {
    // Counting joins maintain per-key build counters per driver, so equal
    // probe keys must reach one driver.
    if (velox::core::isCountingJoin(join.joinType)) {
      std::tie(join.left, join.leftKeys) =
          ensureDriverCoLocated(join.left, join.leftKeys);
    }
    return PhysicalJoin::makeJoin(std::move(join), builder(), simplifier_);
  }

  // Adds the exchanges required by a join outside DPhyp and translates it.
  NodeCP makePhysicalJoin(Join::Key join) {
    if (numWorkers_ > 1 &&
        (isReplicated(join.left) || isReplicated(join.right))) {
      return makeDriverPhysicalJoin(placeReplicatedJoin(std::move(join)));
    }
    if (numWorkers_ > 1) {
      if (!join.leftKeys.empty()) {
        // An already partitioned side can co-locate the join without a full
        // shuffle. Not for null-aware anti/semi: an existence side confines a
        // null key to one partition, so it must shuffle-replicate.
        if (join.nullAware ||
            !coPartitionJoinSides(
                join.left,
                join.right,
                join.leftKeys,
                join.rightKeys,
                Join::canBroadcastBuild(join.joinType))) {
          std::tie(join.left, join.leftKeys) =
              PrecomputeProjections::materializeKeys(
                  join.left, join.leftKeys, builder(), simplifier_);
          std::tie(join.right, join.rightKeys) =
              PrecomputeProjections::materializeKeys(
                  join.right, join.rightKeys, builder(), simplifier_);
          const bool useBroadcast = Join::canBroadcastBuild(join.joinType) &&
              broadcastFits(join.right);
          if (useBroadcast) {
            join.right = replicate(join.right, join.left);
          } else {
            // A partitioned null-aware anti/semi join sends null keys from its
            // existence side to every probe partition.
            const bool rightIsBuild = Join::canBroadcastBuild(join.joinType);
            join.left = partition(
                join.left, join.leftKeys, join.nullAware && !rightIsBuild);
            join.right = partition(
                join.right, join.rightKeys, join.nullAware && rightIsBuild);
          }
        }
      } else {
        if (Join::canBroadcastBuild(join.joinType)) {
          join.right = replicate(join.right, join.left);
        } else {
          join.left = ensureGathered(join.left);
          join.right = ensureGathered(join.right);
        }
      }
    }
    return makeDriverPhysicalJoin(std::move(join));
  }

  // Places a join with an input replicated on every worker. A replicated
  // build joins a probe where the probe's rows are, when the join preserves no
  // build rows, and a replicated probe joins a scattered build where the
  // build's rows are, when the join allows it (see
  // `Join::canBroadcastProbe`). Otherwise the other input is broadcast too
  // and the join runs on each worker over both copies.
  Join::Key placeReplicatedJoin(Join::Key join) {
    const bool leftReplicated = isReplicated(join.left);
    const bool rightReplicated = isReplicated(join.right);
    if ((leftReplicated && rightReplicated) ||
        (rightReplicated && Join::canBroadcastBuild(join.joinType)) ||
        (leftReplicated && Join::canBroadcastProbe(join.joinType))) {
      return join;
    }
    NodeCP& scattered = leftReplicated ? join.right : join.left;
    scattered = exchange({scattered, Partitioning::globalBroadcast()});
    return join;
  }

  // True when every task holds all of 'input'.
  static bool isReplicated(NodeCP input) {
    return input->physicalProperties().globalPartition.is(
        PartitionKind::kBroadcast);
  }

  // Keeps the written join tree while choosing the build side automatically.
  NodeCP rewriteFallbackJoin(const Join* node, NoContext& context) {
    return makePhysicalJoin(chooseBuildSide(rewriteJoinInputs(node, context)));
  }

  // Plans a graph with DPhyp, falling back to written-order planning when
  // enumeration does not produce a root.
  NodeCP planJoinGraph(
      const JoinHypergraph& graph,
      const std::vector<RelationSet>& components,
      const ColumnVector& outputColumns) {
    DefaultCostModel costModel{estimateProvider_};
    DPhyp dphyp{
        graph,
        costModel,
        builder(),
        options_.dphypEnumerationBudget,
        numWorkers_,
        options_.hashStageTasks(numWorkers_),
        options_.broadcastSizeLimit};
    std::vector<MemoOpCP> roots;
    if (components.size() == 1) {
      if (MemoOpCP root = dphyp.enumerate(); root != nullptr) {
        roots.push_back(root);
      }
    } else {
      roots = dphyp.enumerate(components);
    }

    const auto joinFactory = [&](Join::Key join) {
      return makeDriverPhysicalJoin(std::move(join));
    };
    if (roots.size() == 1) {
      return JoinTreeEmitter::emit(
          roots.front(),
          graph,
          outputColumns,
          builder(),
          simplifier_,
          numWorkers_,
          /*oneDriverPerTask=*/numDrivers_ == 1,
          joinFactory);
    }
    if (!roots.empty()) {
      const auto crossJoinFactory = [&](Join::Key join) {
        return makePhysicalJoin(std::move(join));
      };
      return JoinTreeEmitter::emitComponents(
          roots,
          graph,
          outputColumns,
          builder(),
          simplifier_,
          numWorkers_,
          /*oneDriverPerTask=*/numDrivers_ == 1,
          joinFactory,
          crossJoinFactory);
    }

    FallbackJoinPlanner fallbackPlanner{graph};
    roots = fallbackPlanner.build(components);
    const auto fallbackJoinFactory = [&](Join::Key join) {
      return makePhysicalJoin(chooseBuildSide(std::move(join)));
    };
    if (roots.size() == 1) {
      return JoinTreeEmitter::emit(
          roots.front(),
          graph,
          outputColumns,
          builder(),
          simplifier_,
          numWorkers_,
          /*oneDriverPerTask=*/numDrivers_ == 1,
          fallbackJoinFactory);
    }
    if (roots.empty()) {
      return nullptr;
    }
    // 'fallbackJoinFactory' adds distribution to component and cross joins.
    return JoinTreeEmitter::emitComponents(
        roots,
        graph,
        outputColumns,
        builder(),
        simplifier_,
        /*numWorkers=*/1,
        /*oneDriverPerTask=*/numDrivers_ == 1,
        fallbackJoinFactory,
        fallbackJoinFactory);
  }

  NodeCP rewriteJoin(const Join* node, NoContext& context) override {
    // Syntactic mode keeps every join in query order, including its input
    // orientation.
    if (options_.syntacticJoinOrder) {
      return makePhysicalJoin(rewriteJoinInputs(node, context));
    }
    if (!isClusterable(node)) {
      return rewriteFallbackJoin(node, context);
    }

    const bool stopAtStateFree = replicate_ != nullptr;
    CollectedJoinCluster collected = collectJoinCluster(
        node,
        /*dissolveCrossJoins=*/true,
        /*opaqueJoins=*/{},
        stopAtStateFree);
    if (collected.cluster.joins.empty()) {
      return rewriteFallbackJoin(node, context);
    }

    const folly::F14FastSet<const Join*> opaqueJoins =
        valueProducersReadInCluster(collected.cluster);
    if (!opaqueJoins.empty()) {
      collected = collectJoinCluster(
          node, /*dissolveCrossJoins=*/true, opaqueJoins, stopAtStateFree);
    }

    // A RelationSet holds `kMaxRelations` relations, so a larger cluster has
    // no hypergraph to enumerate over. Keep this join tree and let the
    // recursion re-examine what is below it: joins come off the top until the
    // rest fits, and that part is enumerated under the usual budget.
    if (!collected.fitsRelationSet()) {
      return rewriteFallbackJoin(node, context);
    }

    std::vector<NodeCP> rewrittenLeaves;
    auto buildGraph = [&]() {
      rewrittenLeaves.clear();
      rewrittenLeaves.reserve(collected.cluster.leaves.size());
      for (NodeCP leaf : collected.cluster.leaves) {
        rewrittenLeaves.push_back(rewrite(leaf, context));
      }
      return HypergraphBuilder::build(
          collected.cluster, rewrittenLeaves, estimateProvider_);
    };

    JoinHypergraph graph = buildGraph();

    // Dissolving a cross join can strand a correlation-only relation (in an
    // edge's TES but no edge's key endpoints), which DPhyp cannot assemble.
    // Redo without dissolving so that relation stays bundled with its
    // cross-join partner. Clusters that benefit from dissolution have no
    // stranded relation and keep the dissolved form.
    if (hasEndpointStrandedRelation(graph)) {
      collected = collectJoinCluster(
          node, /*dissolveCrossJoins=*/false, opaqueJoins, stopAtStateFree);
      if (!collected.fitsRelationSet()) {
        return rewriteFallbackJoin(node, context);
      }
      graph = buildGraph();
    }

    const std::vector<RelationSet> components = connectedComponents(graph);

    const FreeUnnestPlacement freePlacement =
        planFreeUnnests(collected.freeUnnests, node->outputColumns());
    graph.setTargetColumns(
        PlanObjectSet::fromObjects(freePlacement.graphOutputColumns));
    NodeCP joinTree =
        planJoinGraph(graph, components, freePlacement.graphOutputColumns);
    if (joinTree == nullptr) {
      return rewriteFallbackJoin(node, context);
    }
    return placeFreeUnnests(
        joinTree, freePlacement, node->outputColumns(), builder(), simplifier_);
  }

  // An exchange by 'key', with the placement of the rows after it, which run
  // one driver per task when 'oneDriverPerTask'.
  NodeCP exchange(Exchange::Key key, bool oneDriverPerTask) {
    key.placement =
        Exchange::placementAfter(key.input, key.partitioning, oneDriverPerTask);
    return builder().make<Exchange>(std::move(key));
  }

  // An exchange by 'key' whose rows run this rewriter's drivers per task, or
  // one driver after a gather between workers with 'gatherOntoOneDriver_'.
  NodeCP exchange(Exchange::Key key) {
    const bool oneDriverPerTask = numDrivers_ == 1 ||
        (gatherOntoOneDriver_ && key.partitioning.isGlobal() &&
         key.partitioning.is(PartitionKind::kGather));
    return exchange(std::move(key), oneDriverPerTask);
  }

  // Remote exchanges that unconditionally establish a partitioning on 'input'.
  NodeCP gather(NodeCP input) {
    return exchange({input, Partitioning::gather(PropertyScope::kGlobal)});
  }

  // An order-preserving gather: merges the sorted per-task streams (the input
  // must already be sorted on 'orderKeys') onto one task, lowering to a Velox
  // MergeExchange.
  NodeCP gatherMerge(
      NodeCP input,
      const ExprVector& orderKeys,
      const OrderTypeVector& orderTypes) {
    return exchange(
        {input,
         Partitioning::gatherMerge(
             PropertyScope::kGlobal, orderKeys, orderTypes)});
  }

  NodeCP partition(
      NodeCP input,
      const ExprVector& keys,
      bool replicateNullsAndAny = false) {
    return exchange(
        {input,
         Partitioning::hash(
             PropertyScope::kGlobal, keys, replicateNullsAndAny)});
  }

  // Gives every task of the stage that runs 'probe' a full copy of 'build'.
  NodeCP replicate(NodeCP build, NodeCP probe) {
    const Partitioning replicated = Partitioning::globalReplicatedTo(
        probe->physicalProperties().globalPartition);
    if (build->physicalProperties().globalPartition.sameClassAs(replicated)) {
      return build;
    }
    return exchange({build, replicated});
  }

  // Repartitions 'input' on 'keys' using 'targetType' (a connector
  // partitioning), so it co-locates with a side already bucketed that way.
  NodeCP partitionTo(
      NodeCP input,
      const ExprVector& keys,
      const connector::PartitionType* targetType) {
    return exchange(
        {input,
         Partitioning::connectorHash(
             PropertyScope::kGlobal, keys, targetType)});
  }

  // Follows single-input nodes down to a scan and returns the bucketing its
  // table affords, or null if the chain forks or ends elsewhere. A rough
  // answer on purpose: it does not check that those nodes preserve
  // partitioning, so it can name a bucketing `groupedRead` would decline to
  // build. Used only to skip building a subtree that could not pair anyway —
  // a wrong yes costs a build that is discarded, a wrong no costs a grouped
  // read, and neither changes results.
  static const connector::PartitionType* leafStorageBucketing(NodeCP node) {
    while (!node->is(NodeType::kScan)) {
      if (node->inputs().size() != 1) {
        return nullptr;
      }
      node = node->inputs()[0];
    }
    return node->as<Scan>()->storageBucketing().partitionType;
  }

  // Describes a side partitioned on join keys.
  struct PartitioningOffer {
    // Input plan providing the partitioning.
    NodeCP node;

    // The input's partitioning expressed in join-key positions.
    JoinPartitioning partitioning;
  };

  // The side itself when already partitioned on some of 'keys', else a grouped
  // read of its table by them; nullopt when neither exists. A repeated join key
  // uses its first position. Two offers match only when both report the same
  // positions, whose key pairs the join equates. Choosing the first of several
  // equal keys can miss an optimization but cannot misalign matching rows.
  std::optional<PartitioningOffer> partitioningOffer(
      NodeCP side,
      const ExprVector& keys) {
    NodeCP node = side->physicalProperties().globalPartition.coLocates(keys)
        ? side
        : groupedRead(side, keys, Alignment::kCoLocated);
    if (node == nullptr) {
      return std::nullopt;
    }
    auto partitioning = JoinPartitioning::from(
        node->physicalProperties().globalPartition, keys);
    if (!partitioning.has_value()) {
      return std::nullopt;
    }
    return PartitioningOffer{node, std::move(*partitioning)};
  }

  // Co-locates a keyed join on an input's existing partitioning. Matching
  // offers are kept. Otherwise, one offer is kept and the other side is moved
  // onto it, preferring connector bucketing over plain hash. Returns false only
  // when neither side is already partitioned or can be read grouped.
  // Updates 'left'/'right' and, where a side is repartitioned to the other's
  // partitioning, 'leftKeys'/'rightKeys': that shuffle needs column keys, so
  // an expression key is computed first and the join reads that column.
  bool coPartitionJoinSides(
      NodeCP& left,
      NodeCP& right,
      ExprVector& leftKeys,
      ExprVector& rightKeys,
      bool canBroadcastBuild) {
    const auto leftOffer = partitioningOffer(left, leftKeys);

    // Building the right side's offer is wasted when its table's bucketing
    // could not meet the left's anyway.
    const auto* rightStorage = leafStorageBucketing(right);
    // Skip only when the right side's table is known not to meet the left's.
    // A null answer means unknown — its subtree is not a single table — and
    // then it must be built and judged on its own partitioning.
    const bool worthBuilding = !leftOffer.has_value() ||
        leftOffer->partitioning.partitionType == nullptr ||
        rightStorage == nullptr ||
        leftOffer->partitioning.partitionType->copartition(*rightStorage) !=
            nullptr;
    const auto rightOffer =
        worthBuilding ? partitioningOffer(right, rightKeys) : std::nullopt;

    // Repartitions one side onto an offer using the corresponding join keys.
    const auto alignTo = [&](NodeCP& side,
                             ExprVector& keys,
                             const PartitioningOffer& offer) {
      const ExprVector shuffleKeys = offer.partitioning.correspondingKeys(keys);
      auto [keyed, columnKeys] = PrecomputeProjections::materializeKeys(
          side, shuffleKeys, builder(), simplifier_);
      side = offer.partitioning.partitionType == nullptr
          ? partition(keyed, columnKeys)
          : partitionTo(keyed, columnKeys, offer.partitioning.partitionType);
    };

    if (leftOffer.has_value() && rightOffer.has_value()) {
      if (leftOffer->partitioning.coPartitionsWith(rightOffer->partitioning)) {
        left = leftOffer->node;
        right = rightOffer->node;
        return true;
      }
    }
    const bool keepLeft = leftOffer.has_value() &&
        (!rightOffer.has_value() ||
         leftOffer->partitioning.partitionType != nullptr ||
         rightOffer->partitioning.partitionType == nullptr);
    if (keepLeft) {
      left = leftOffer->node;
      alignTo(right, rightKeys, *leftOffer);
      return true;
    }
    if (rightOffer.has_value()) {
      if (canBroadcastBuild && broadcastMovesLess(left, right)) {
        return false;
      }
      right = rightOffer->node;
      alignTo(left, leftKeys, *rightOffer);
      return true;
    }
    return false;
  }

  NodeCP arbitrary(NodeCP input) {
    return exchange({input, Partitioning::arbitrary(PropertyScope::kGlobal)});
  }

  NodeCP driverPartition(NodeCP input, const ExprVector& keys) {
    return exchange(
        {input,
         keys.empty() ? Partitioning::gather(PropertyScope::kDriver)
                      : Partitioning::hash(PropertyScope::kDriver, keys)});
  }

  NodeCP driverGatherMerge(
      NodeCP input,
      const ExprVector& orderKeys,
      const OrderTypeVector& orderTypes) {
    return exchange(
        {input,
         Partitioning::gatherMerge(
             PropertyScope::kDriver, orderKeys, orderTypes)});
  }

  // Co-locates rows on one driver and returns any materialized key columns.
  // Existing placement proofs pass through; empty keys otherwise gather.
  std::pair<NodeCP, ExprVector> ensureDriverCoLocated(
      NodeCP input,
      const ExprVector& keys,
      const ColumnVector& keyAliases = {}) {
    if (numDrivers_ == 1 ||
        input->physicalProperties().coLocates(PropertyScope::kDriver, keys)) {
      return {input, keys};
    }
    if (keys.empty()) {
      return {driverPartition(input, keys), keys};
    }
    auto [keyed, columnKeys] = PrecomputeProjections::materializeKeys(
        input, keys, builder(), simplifier_, keyAliases);
    return {driverPartition(keyed, columnKeys), columnKeys};
  }

  NodeCP broadcast(NodeCP input) {
    return exchange({input, Partitioning::globalBroadcast()});
  }

  // True when each output row of 'node' derives from one row of the recursive
  // state it reads, so 'node' over a union of state shards is the union of
  // 'node' over each shard.
  static bool isRowLocal(NodeCP node) {
    if (node->requiredStates().empty()) {
      return true;
    }
    switch (node->nodeType()) {
      case NodeType::kWorkingTable:
        return true;
      case NodeType::kFilter:
      case NodeType::kProject:
      case NodeType::kUnnest:
        return isRowLocal(node->inputs()[0]);
      case NodeType::kJoin: {
        const auto* join = node->as<Join>();
        const bool stateOnLeft = !join->left()->requiredStates().empty();
        // A join of two reads of the state combines two of its rows.
        if (stateOnLeft && !join->right()->requiredStates().empty()) {
          return false;
        }
        switch (join->joinType()) {
          case velox::core::JoinType::kInner:
            break;
          case velox::core::JoinType::kLeft:
          case velox::core::JoinType::kLeftSemiFilter:
          case velox::core::JoinType::kLeftSemiProject:
          case velox::core::JoinType::kAnti:
            if (!stateOnLeft) {
              return false;
            }
            break;
          case velox::core::JoinType::kRight:
          case velox::core::JoinType::kRightSemiFilter:
          case velox::core::JoinType::kRightSemiProject:
          case velox::core::JoinType::kRightAnti:
            if (stateOnLeft) {
              return false;
            }
            break;
          default:
            return false;
        }
        return isRowLocal(stateOnLeft ? join->left() : join->right());
      }
      default:
        return false;
    }
  }

  // True when 'input' already produces all rows on one task.
  static bool isGathered(NodeCP input) {
    return input->physicalProperties().globalPartition.is(
        PartitionKind::kGather);
  }

  // True when each task that runs 'input' holds all of its rows: gathered on
  // one task, or replicated on every task. An operator over such an input runs
  // on each of those tasks over all the rows, with no exchange.
  static bool holdsAllRows(NodeCP input) {
    const auto& partition = input->physicalProperties().globalPartition;
    return partition.is(PartitionKind::kGather) ||
        partition.is(PartitionKind::kBroadcast);
  }

  // Ensures all rows reach one task: reuse when 'input' is already gathered,
  // else a remote gather exchange. A no-op at a single worker, where one task
  // already holds all rows.
  NodeCP ensureGathered(NodeCP input) {
    if (numWorkers_ == 1 || holdsAllRows(input)) {
      return input;
    }
    return gather(input);
  }

  // Ensures every group of equal 'keys' is co-located on one task so a grouping
  // / partition / per-key-assertion consumer can run per group on one task:
  // reuse when 'input' is already gathered (one task co-locates any keys) or
  // already co-located on 'keys', else a remote hash exchange. Empty 'keys' is
  // one global group, satisfied by a gather. A no-op at a single worker.
  // Returns the co-located input and the keys it is co-located on: a shuffle
  // needs column keys, so an expression key is computed into one here, and the
  // consumer reads that column rather than computing the value again.
  // 'keyAliases', when set, names any key this materializes; see
  // PrecomputeProjections::materializeKeys.
  std::pair<NodeCP, ExprVector> ensureCoLocated(
      NodeCP input,
      const ExprVector& keys,
      const ColumnVector& keyAliases = {}) {
    if (numWorkers_ == 1 || holdsAllRows(input)) {
      return {input, keys};
    }

    if (keys.empty()) {
      return {gather(input), keys};
    }

    if (input->physicalProperties().globalPartition.coLocates(keys)) {
      return {input, keys};
    }

    // Reading the input by its storage bucketing co-locates the keys without
    // moving a row, so it beats a shuffle whenever it is available.
    if (NodeCP grouped = groupedRead(input, keys, Alignment::kCoLocated)) {
      return {grouped, keys};
    }

    auto [keyed, columnKeys] = PrecomputeProjections::materializeKeys(
        input, keys, builder(), simplifier_, keyAliases);
    return {partition(keyed, columnKeys), columnKeys};
  }

  // True when 'input' is already sorted on a prefix matching 'keys' and
  // 'orders'.
  static bool hasOrder(
      NodeCP input,
      const ExprVector& keys,
      const OrderTypeVector& orders) {
    if (keys.empty()) {
      return true;
    }
    const auto& driverOrder = input->physicalProperties().driverOrder;
    if (driverOrder.empty() ||
        driverOrder.front().kind != DriverOrderKind::kSorted ||
        driverOrder.front().columns.size() < keys.size()) {
      return false;
    }
    for (size_t i = 0; i < keys.size(); ++i) {
      if (!keys[i]->is(PlanType::kColumnExpr) ||
          !keys[i]->sameOrEqual(*driverOrder.front().columns[i]) ||
          orders[i] != driverOrder.front().orders[i]) {
        return false;
      }
    }
    return true;
  }

  // Returns 'node's output columns with the leading input-column prefix
  // replaced by 'newInput's columns, for a node whose output is its input's
  // columns followed by what it appends (Window, TopNRowNumber).
  ColumnVector outputFollowingInput(const Node* node, NodeCP newInput) {
    ColumnVector result{newInput->outputColumns()};
    const auto& oldOutput = node->outputColumns();
    for (size_t i = node->inputs()[0]->outputColumns().size();
         i < oldOutput.size();
         ++i) {
      result.push_back(oldOutput[i]);
    }
    return result;
  }

  NodeCP rewriteAggregate(const Aggregate* node, NoContext& context) override {
    return distributeAggregate(node, rewrite(node->input(), context));
  }

  // Returns 'input' read grouped when that co-locates 'keys' across tasks, and
  // whether a remote exchange must still co-locate them.
  std::pair<NodeCP, bool> coLocateByGroupedRead(
      NodeCP input,
      const ExprVector& keys) {
    if (numWorkers_ == 1 || !needsShuffle(input, keys)) {
      return {input, false};
    }
    if (NodeCP grouped = groupedRead(input, keys, Alignment::kCoLocated)) {
      return {grouped, false};
    }
    return {input, true};
  }

  // Distributes 'node' over 'input', its physically planned input.
  NodeCP distributeAggregate(const Aggregate* node, NodeCP input) {
    if (isSplittableAggregate(node)) {
      // Remote two-stage: the input must shuffle across workers to co-locate
      // its groups, so the partial reduces rows before that remote exchange.
      bool needsRemoteExchange{false};
      std::tie(input, needsRemoteExchange) =
          coLocateByGroupedRead(input, node->groupingKeys());
      if (needsRemoteExchange) {
        return rewriteAggregateSplit(node, input, /*remoteExchange=*/true);
      }
      // A single stage is safe when input properties already prove each group
      // stays on one driver.
      if (numDrivers_ > 1 &&
          !input->physicalProperties().coLocates(
              PropertyScope::kDriver, node->groupingKeys())) {
        return rewriteAggregateSplit(node, input, /*remoteExchange=*/false);
      }
    }
    // A global () grouping set emits a default row over empty input; a
    // single-stage aggregate must gather (empty keys) so that row is produced
    // once, not once per worker.
    const ExprVector keys = node->globalGroupingSets().empty()
        ? node->groupingKeys()
        : ExprVector{};
    // A grouping key is published under `outputColumns`, positionally, and
    // consumers read it by that column. Materializing it under a fresh name
    // would leave them referencing a column the aggregate no longer outputs.
    const ColumnVector keyAliases{
        node->outputColumns().begin(),
        node->outputColumns().begin() + keys.size()};
    auto [coLocatedInput, coLocatedKeys] =
        ensureCoLocated(input, keys, keyAliases);
    ExprFactory::ExprSubstitution materialized;
    for (size_t i = 0; i < keys.size(); ++i) {
      if (coLocatedKeys[i] != keys[i]) {
        materialized.emplace(keys[i], coLocatedKeys[i]);
      }
    }
    ExprVector groupingKeys{node->groupingKeys()};
    if (!materialized.empty()) {
      groupingKeys = exprFactory_.replace(groupingKeys, materialized);
    }

    // Every position the aggregate reads becomes a column here rather than in
    // a later pass: a grouping key is also part of each MarkDistinct key set,
    // and a Project inserted above the MarkDistinct chain would separate it
    // from the aggregate that reads its markers. Narrowing, so an input column
    // that only fed a lifted expression stops here.
    PrecomputeProjections precompute{
        coLocatedInput,
        builder(),
        simplifier_,
        /*projectAllInputs=*/false};
    for (size_t i = 0; i < groupingKeys.size(); ++i) {
      groupingKeys[i] =
          precompute.toColumn(groupingKeys[i], node->outputColumns()[i]);
    }
    AggregateCallVector precomputedAggregates = precomputeAggregateArgs(
        node->aggregates(), node->step(), precompute, builder());
    coLocatedInput = std::move(precompute).node();

    // A distinct aggregate never splits, so this is the only place the
    // MarkDistinct chain can land: between the co-located input and the
    // aggregate, above whatever exchange brought the groups together.
    auto [distinctInput, aggregates] = expandDistinct(
        coLocatedInput,
        groupingKeys,
        precomputedAggregates,
        builder(),
        numDrivers_);
    const ColumnVector groupingAliases{
        node->outputColumns().begin(),
        node->outputColumns().begin() + groupingKeys.size()};
    std::tie(distinctInput, groupingKeys) =
        ensureDriverCoLocated(distinctInput, groupingKeys, groupingAliases);

    return builder().make<Aggregate>(
        {.input = distinctInput,
         .groupingKeys = groupingKeys,
         .aggregates = std::move(aggregates),
         .outputColumns = node->outputColumns(),
         .step = node->step(),
         .groupId = node->groupId(),
         .globalGroupingSets = node->globalGroupingSets()});
  }

  // True when 'node' is a DISTINCT: an aggregate with grouping keys only.
  static bool isDistinct(NodeCP node) {
    if (!node->is(NodeType::kAggregate)) {
      return false;
    }
    const auto* aggregate = node->as<Aggregate>();
    return aggregate->aggregates().empty() &&
        !aggregate->groupingKeys().empty();
  }

  // True when 'node' can two-stage into partial + final, independent of whether
  // the exchange between them is remote (numWorkers > 1) or local (numDrivers >
  // 1) — the caller gates on that. Excluded: a DISTINCT aggregate (a per-task
  // partial under-dedups and the final would over-count); and an ordered
  // aggregate (the partial sees only a task-local order). GROUPING SETS
  // aggregates split like any other — by this point they are already GroupId +
  // a plain aggregate keyed on the group-id column.
  bool isSplittableAggregate(const Aggregate* node) const {
    for (const auto* aggregate : node->aggregates()) {
      if (aggregate->isDistinct() || !aggregate->orderKeys().empty()) {
        return false;
      }
    }
    return true;
  }

  // Returns 'input' with every scan under it read one bucket-group at a time,
  // when that makes the result meet 'alignment' on 'keys'; null otherwise. The
  // subtree is rebuilt only when the answer is yes.
  NodeCP
  groupedRead(NodeCP input, const ExprVector& keys, Alignment alignment) {
    if (numWorkers_ == 1 || keys.empty()) {
      return nullptr;
    }
    if (!satisfies(
            GroupedRead::partitioning(input, numWorkers_, builder()),
            keys,
            alignment)) {
      return nullptr;
    }
    return GroupedRead::rewrite(input, numWorkers_, builder());
  }

  // True when 'input' is not already arranged so that rows agreeing on 'keys'
  // share a task: a gathered input co-locates any keys; a global aggregate (no
  // keys) needs a gather unless already gathered; otherwise the input must be
  // partitioned on the keys. Callers that can avoid the shuffle by reading the
  // source grouped ask `groupedRead` after this returns true.
  bool needsShuffle(NodeCP input, const ExprVector& keys) const {
    if (holdsAllRows(input)) {
      return false;
    }
    if (keys.empty()) {
      return true;
    }
    return !input->physicalProperties().globalPartition.coLocates(keys);
  }

  // Lowers an aggregate to partial → exchange → final so the remote shuffle
  // carries the per-task partials (one row per group per task) rather than the
  // whole input. The partial pre-aggregates per task and emits the grouping
  // keys followed by one intermediate accumulator per aggregate; the exchange
  // partitions those partials on the grouping keys (gather when there are none
  // — the global case); the final combines them into the result.
  //
  // Every eligible aggregate two-stages, reducing or not. A non-reducing
  // aggregate (e.g. array_agg) gains nothing and pays an extra hash pass; not
  // splitting it needs a reducing/non-reducing classification that does not yet
  // exist, so that pessimization is deferred.
  NodeCP rewriteAggregateSplit(
      const Aggregate* node,
      NodeCP input,
      bool remoteExchange) {
    const size_t numKeys = node->groupingKeys().size();
    const auto& finalColumns = node->outputColumns();

    // The partial's output reuses the grouping-key columns and adds a fresh
    // intermediate-typed accumulator column per aggregate, positionally aligned
    // (keys first, then one accumulator per aggregate in aggregate order) so
    // the final reads input column 'numKeys + i' as the i-th accumulator.
    ColumnVector partialColumns;
    partialColumns.reserve(finalColumns.size());
    partialColumns.insert(
        partialColumns.end(),
        finalColumns.begin(),
        finalColumns.begin() + numKeys);
    for (size_t i = 0; i < node->aggregates().size(); ++i) {
      ColumnCP finalColumn = finalColumns[numKeys + i];
      partialColumns.push_back(
          Column::create(
              finalColumn->outputName(),
              Value{
                  node->aggregates()[i]->intermediateType(),
                  finalColumn->value().cardinality}));
    }

    // The partial reads the grouping keys and aggregate args, so it takes them
    // as columns like any other operator. Its output column for a key is the
    // final's, which the alias keeps.
    PrecomputeProjections precompute{
        input, builder(), simplifier_, /*projectAllInputs=*/false};
    ExprVector partialKeys;
    partialKeys.reserve(numKeys);
    for (size_t i = 0; i < numKeys; ++i) {
      partialKeys.push_back(
          precompute.toColumn(node->groupingKeys()[i], finalColumns[i]));
    }
    AggregateCallVector partialAggregates = precomputeAggregateArgs(
        node->aggregates(), AggregateStep::kPartial, precompute, builder());
    input = std::move(precompute).node();

    NodeCP partial = builder().make<Aggregate>(
        {.input = input,
         .groupingKeys = std::move(partialKeys),
         .aggregates = partialAggregates,
         .outputColumns = std::move(partialColumns),
         .step = AggregateStep::kPartial,
         .groupId = node->groupId(),
         .globalGroupingSets = node->globalGroupingSets()});

    // The final groups the exchanged partials by their output grouping-key
    // columns (the partial already evaluated any compound grouping
    // expression), so its keys are those plain columns, not the original
    // expressions.
    ExprVector finalKeys;
    finalKeys.reserve(numKeys);
    for (size_t i = 0; i < numKeys; ++i) {
      finalKeys.push_back(finalColumns[i]);
    }

    // A FILTER is applied by the partial, and its mask column is not among the
    // partial's outputs, so the final must not carry one.
    AggregateCallVector finalAggregates;
    finalAggregates.reserve(node->aggregates().size());
    for (const auto* aggregate : node->aggregates()) {
      finalAggregates.push_back(
          aggregate->condition() == nullptr
              ? aggregate
              : builder().makeAggregate(
                    aggregate->name(),
                    aggregate->value(),
                    ExprVector{aggregate->args()},
                    aggregate->functions(),
                    aggregate->isDistinct(),
                    /*condition=*/nullptr,
                    aggregate->intermediateType(),
                    ExprVector{aggregate->orderKeys()},
                    aggregate->orderTypes()));
    }

    NodeCP finalInput = !remoteExchange ? partial
        : finalKeys.empty()             ? gather(partial)
                                        : partition(partial, finalKeys);
    std::tie(finalInput, finalKeys) =
        ensureDriverCoLocated(finalInput, finalKeys);

    return builder().make<Aggregate>(
        {.input = finalInput,
         .groupingKeys = finalKeys,
         .aggregates = std::move(finalAggregates),
         .outputColumns = finalColumns,
         .step = AggregateStep::kFinal,
         .groupId = node->groupId(),
         .globalGroupingSets = node->globalGroupingSets()});
  }

  // Distributes a window: its input must be partitioned on the PARTITION BY
  // keys so each partition is computed on one task (an unpartitioned window
  // gathers to one task). Runs bottom-up, so the input is already physically
  // planned.
  NodeCP rewriteWindow(const Window* node, NoContext& context) override {
    auto [input, partitionKeys] =
        ensureCoLocated(rewrite(node->input(), context), node->partitionKeys());
    std::tie(input, partitionKeys) =
        ensureDriverCoLocated(input, partitionKeys);
    if (input == node->input()) {
      return node;
    }
    return builder().make<Window>(
        {input,
         node->functions(),
         partitionKeys,
         node->orderKeys(),
         node->orderTypes(),
         outputFollowingInput(node, input)});
  }

  // Distributes a row numbering: like a window, its input must be partitioned
  // on the PARTITION BY keys (gather when none) so each partition is numbered
  // on one task.
  NodeCP rewriteRowNumber(const RowNumber* node, NoContext& context) override {
    auto [input, partitionKeys] =
        ensureCoLocated(rewrite(node->input(), context), node->partitionKeys());
    std::tie(input, partitionKeys) =
        ensureDriverCoLocated(input, partitionKeys);
    if (input == node->input()) {
      return node;
    }
    return builder().make<RowNumber>(
        {input,
         partitionKeys,
         node->limit(),
         node->rankColumn(),
         outputFollowingInput(node, input)});
  }

  // Distributes a per-partition top-n (row_number / rank): its input must be
  // partitioned on the PARTITION BY keys (gather when none). The rows each
  // driver ranks within the limit among its own rows still hold a top-n of the
  // whole partition: for rank and dense_rank, every row within the limit is
  // within it among its driver's rows, and row_number keeps a valid choice of
  // tied rows. So when an exchange co-locates the keys, each driver first drops
  // the rows outside its limit and the node runs as the final step.
  NodeCP rewriteTopNRowNumber(const TopNRowNumber* node, NoContext& context)
      override {
    const auto& keys = node->partitionKeys();
    auto [input, needsRemoteExchange] =
        coLocateByGroupedRead(rewrite(node->input(), context), keys);
    const bool needsDriverExchange = numDrivers_ > 1 &&
        !input->physicalProperties().coLocates(PropertyScope::kDriver, keys);
    if (needsRemoteExchange || needsDriverExchange) {
      input = builder().make<TopNRowNumber>(
          {input,
           node->rankFunction(),
           keys,
           node->orderKeys(),
           node->orderTypes(),
           node->limit(),
           /*rankColumn=*/nullptr,
           input->outputColumns()});
    }
    auto [coLocated, partitionKeys] = ensureCoLocated(input, keys);
    std::tie(coLocated, partitionKeys) =
        ensureDriverCoLocated(coLocated, partitionKeys);
    if (coLocated == node->input()) {
      return node;
    }
    return builder().make<TopNRowNumber>(
        {coLocated,
         node->rankFunction(),
         partitionKeys,
         node->orderKeys(),
         node->orderTypes(),
         node->limit(),
         node->rankColumn(),
         outputFollowingInput(node, coLocated)});
  }

  // Distributes a global ORDER BY at both scopes. Multiple drivers sort in
  // parallel and merge to one stream per task. Multiple tasks merge those
  // streams globally; an existing driver or task gather skips its merge.
  NodeCP rewriteSort(const Sort* node, NoContext& context) override {
    NodeCP input = rewrite(node->input(), context);
    const bool needsDriverMerge = numDrivers_ > 1 &&
        !input->physicalProperties().coLocates(
            PropertyScope::kDriver, /*keys=*/{});
    NodeCP sorted = !needsDriverMerge && input == node->input()
        ? node
        : builder().make<Sort>(
              {input, node->orderKeys(), node->orderTypes(), needsDriverMerge});
    if (needsDriverMerge) {
      sorted = driverGatherMerge(sorted, node->orderKeys(), node->orderTypes());
    }
    if (numWorkers_ == 1 || holdsAllRows(input)) {
      return sorted;
    }
    return gatherMerge(sorted, node->orderKeys(), node->orderTypes());
  }

  // An Unnest outside any join cluster.
  NodeCP rewriteUnnest(const Unnest* node, NoContext& context) override {
    return PhysicalJoin::makeUnnest(
        {rewrite(node->input(), context),
         node->unnestExpressions(),
         node->replicatedColumns(),
         node->unnestColumns(),
         node->ordinalityColumn(),
         node->markerColumn(),
         node->outputColumns()},
        builder(),
        simplifier_);
  }

  // Distributes an EnforceSingleRow (scalar-subquery single-row assertion): it
  // must see all rows on one task to assert the global count, so gather.
  NodeCP rewriteEnforceSingleRow(
      const EnforceSingleRow* node,
      NoContext& context) override {
    NodeCP input = ensureGathered(rewrite(node->input(), context));
    std::tie(input, std::ignore) = ensureDriverCoLocated(input, {});
    if (input == node->input()) {
      return node;
    }
    return builder().make<EnforceSingleRow>({input, node->outputColumns()});
  }

  // EnforceDistinct asserts at most one row per distinct key across all input.
  // A per-task check only sees duplicates that share a task, so the input must
  // be co-located on the distinct keys.
  NodeCP rewriteEnforceDistinct(const EnforceDistinct* node, NoContext& context)
      override {
    auto [input, distinctKeys] =
        ensureCoLocated(rewrite(node->input(), context), node->distinctKeys());
    std::tie(input, distinctKeys) = ensureDriverCoLocated(input, distinctKeys);
    if (input == node->input()) {
      return node;
    }
    return builder().make<EnforceDistinct>(
        {input, distinctKeys, node->errorMessage()});
  }

  // Distributes a bounded LIMIT at both scopes. Each driver keeps
  // offset+count rows before one exact Limit per task; each task keeps the same
  // bound before the global gather and final exact Limit. An exact Limit runs
  // on one driver, so an unbounded path needs no explicit driver gather.
  //
  // A bounded LIMIT over a DISTINCT whose input does not already co-locate the
  // distinct keys at both scopes is planned by `distributeDistinctLimit`.
  NodeCP rewriteLimit(const Limit* node, NoContext& context) override {
    const auto* distinct =
        isDistinct(node->input()) ? node->input()->as<Aggregate>() : nullptr;
    NodeCP input;
    if (distinct != nullptr) {
      NodeCP distinctInput = rewrite(distinct->input(), context);
      const auto& keys = distinct->groupingKeys();
      const bool taskCoLocated =
          numWorkers_ == 1 || !needsShuffle(distinctInput, keys);
      const bool driverCoLocated = numDrivers_ == 1 ||
          distinctInput->physicalProperties().coLocates(
              PropertyScope::kDriver, keys);
      if (node->isBounded() && !(taskCoLocated && driverCoLocated)) {
        return distributeDistinctLimit(node, distinct, distinctInput);
      }
      input = distributeAggregate(distinct, distinctInput);
    } else {
      input = rewrite(node->input(), context);
    }

    const bool needsGlobalGather = numWorkers_ > 1 && !holdsAllRows(input);
    if (needsGlobalGather && !node->isBounded()) {
      return builder().make<Limit>(
          {gather(input), node->offset(), node->count()});
    }

    const int64_t taskOffset = needsGlobalGather ? 0 : node->offset();
    const int64_t taskCount =
        needsGlobalGather ? node->offsetPlusCount() : node->count();
    NodeCP taskLimit;
    if (numDrivers_ > 1 && node->isBounded() &&
        !input->physicalProperties().coLocates(
            PropertyScope::kDriver, /*keys=*/{})) {
      NodeCP partial = builder().make<Limit>(
          {input,
           /*offset=*/0,
           node->offsetPlusCount(),
           /*partial=*/true});
      taskLimit = builder().make<Limit>(
          {driverPartition(partial, {}), taskOffset, taskCount});
    } else {
      taskLimit = builder().make<Limit>({input, taskOffset, taskCount});
    }
    if (!needsGlobalGather) {
      return taskLimit;
    }
    return builder().make<Limit>(
        {gather(taskLimit), node->offset(), node->count()});
  }

  // Plans a bounded LIMIT over 'distinct', a DISTINCT over 'input'. Each driver
  // deduplicates its own rows and keeps the first offset + count keys, so at
  // most that many rows per driver reach one driver, where a single aggregate
  // deduplicates across drivers and tasks before the exact Limit.
  NodeCP distributeDistinctLimit(
      const Limit* node,
      const Aggregate* distinct,
      NodeCP input) {
    const size_t numKeys = distinct->groupingKeys().size();
    const auto& outputColumns = distinct->outputColumns();

    // Both aggregates publish each key under the DISTINCT's output column.
    PrecomputeProjections precompute{
        input, builder(), simplifier_, /*projectAllInputs=*/false};
    ExprVector driverKeys;
    driverKeys.reserve(numKeys);
    for (size_t i = 0; i < numKeys; ++i) {
      driverKeys.push_back(
          precompute.toColumn(distinct->groupingKeys()[i], outputColumns[i]));
    }
    NodeCP result = builder().make<Aggregate>(
        {.input = std::move(precompute).node(),
         .groupingKeys = std::move(driverKeys),
         .outputColumns = outputColumns,
         .groupId = distinct->groupId(),
         .globalGroupingSets = distinct->globalGroupingSets()});
    result = builder().make<Limit>(
        {result, /*offset=*/0, node->offsetPlusCount(), /*partial=*/true});
    result = ensureGathered(result);
    std::tie(result, std::ignore) = ensureDriverCoLocated(result, {});

    result = builder().make<Aggregate>(
        {.input = result,
         .groupingKeys =
             ExprVector{outputColumns.begin(), outputColumns.begin() + numKeys},
         .outputColumns = outputColumns,
         .groupId = distinct->groupId(),
         .globalGroupingSets = distinct->globalGroupingSets()});
    return builder().make<Limit>({result, node->offset(), node->count()});
  }

  // Distributes a bounded ORDER BY at both scopes. Drivers compute partial
  // TopN and merge to one ordered stream per task. Tasks retain offset+count
  // rows before the global merge, and the final Limit applies offset/count.
  NodeCP rewriteTopN(const TopN* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    const bool needsGlobalGather = numWorkers_ > 1 && !holdsAllRows(newInput);
    const int64_t taskOffset = needsGlobalGather ? 0 : node->offset();
    const int64_t taskCount =
        needsGlobalGather ? node->offsetPlusCount() : node->count();
    NodeCP localTopN;
    if (numDrivers_ > 1 &&
        !newInput->physicalProperties().coLocates(
            PropertyScope::kDriver, /*keys=*/{})) {
      NodeCP partial = builder().make<TopN>(
          {newInput,
           node->orderKeys(),
           node->orderTypes(),
           /*offset=*/0,
           node->offsetPlusCount(),
           /*partial=*/true});
      localTopN = builder().make<Limit>(
          {driverGatherMerge(partial, node->orderKeys(), node->orderTypes()),
           taskOffset,
           taskCount});
    } else {
      localTopN = builder().make<TopN>(
          {newInput,
           node->orderKeys(),
           node->orderTypes(),
           taskOffset,
           taskCount});
    }
    if (!needsGlobalGather) {
      return localTopN;
    }
    return builder().make<Limit>(
        {gatherMerge(localTopN, node->orderKeys(), node->orderTypes()),
         node->offset(),
         node->count()});
  }

  // Distributes a UNION ALL: at numWorkers>1 isolate each single-task leg
  // (global aggregate, order/limit, Values) behind a remote exchange, so it
  // does not share a fragment with a parallel leg -- which would replicate it
  // across that fragment's tasks. Parallel legs (scans, distincts) stay
  // un-isolated and co-locate in the union fragment, keeping the union parallel
  // rather than funneling every row through one task. Arbitrary partitioning
  // suffices for the isolated legs -- the union only concatenates; a downstream
  // operator that needs a partitioning establishes it on the union's output
  // itself.
  NodeCP rewriteUnionAll(const UnionAll* node, NoContext& context) override {
    NodeVector newInputs;
    newInputs.reserve(node->inputs().size());
    bool changed = false;
    for (NodeCP input : node->inputs()) {
      NodeCP newInput = rewrite(input, context);
      changed = changed || newInput != input;
      newInputs.push_back(newInput);
    }

    if (numWorkers_ > 1) {
      // A leg replicated on every worker unions with the others only where
      // they are on every worker too, so the others are broadcast. A loop over
      // a replicated state checks that each broadcast fits.
      if (std::ranges::any_of(newInputs, isReplicated) &&
          !std::ranges::all_of(newInputs, isReplicated)) {
        for (NodeCP& input : newInputs) {
          if (!isReplicated(input)) {
            input = exchange({input, Partitioning::globalBroadcast()});
          }
        }
      }

      // Keep the legs un-shuffled when they co-bucket: the union derives a
      // bucketed partitioning iff every leg is bucketed on leg columns mapping
      // to the same output columns with copartitionable types, and then all
      // legs share one grouped fragment. Otherwise each leg distributes
      // independently (arbitrary / round-robin).
      NodeCP coalesced = builder().make<UnionAll>(
          {newInputs, node->legColumns(), node->outputColumns()});
      const auto& partition = coalesced->physicalProperties().globalPartition;
      // No isolating exchange is needed when the legs co-bucket into one
      // grouped fragment, nor when every leg is single-task (gathered) — then
      // the union itself runs as a single task. See DistributedExecution.md
      // section 1.
      if (partition.is(PartitionKind::kPartitioned) ||
          partition.is(PartitionKind::kGather) ||
          partition.is(PartitionKind::kBroadcast)) {
        return coalesced;
      }

      // Mixed legs. Parallel legs (scans, distincts) co-locate in the union
      // fragment and run in parallel. The single-task (gathered) legs (Values,
      // global aggregate, order/limit) are grouped into one sub-union and
      // isolated behind a single arbitrary exchange, so they run once and feed
      // the parallel union -- rather than sharing the parallel fragment (which
      // would replicate them across its tasks) or each spawning its own
      // fragment.
      NodeVector parallelLegs;
      QGVector<ColumnVector> parallelLegColumns;
      NodeVector singleTaskLegs;
      QGVector<ColumnVector> singleTaskLegColumns;
      for (size_t i = 0; i < newInputs.size(); ++i) {
        if (newInputs[i]->physicalProperties().globalPartition.is(
                PartitionKind::kGather)) {
          singleTaskLegs.push_back(newInputs[i]);
          singleTaskLegColumns.push_back(node->legColumns()[i]);
        } else {
          parallelLegs.push_back(newInputs[i]);
          parallelLegColumns.push_back(node->legColumns()[i]);
        }
      }

      NodeVector unionInputs = std::move(parallelLegs);
      QGVector<ColumnVector> unionLegColumns = std::move(parallelLegColumns);
      if (!singleTaskLegs.empty()) {
        NodeCP grouped;
        ColumnVector groupedColumns;
        if (singleTaskLegs.size() == 1) {
          grouped = singleTaskLegs.front();
          groupedColumns = singleTaskLegColumns.front();
        } else {
          groupedColumns = node->outputColumns();
          grouped = builder().make<UnionAll>(
              {std::move(singleTaskLegs),
               std::move(singleTaskLegColumns),
               node->outputColumns()});
        }
        unionInputs.push_back(arbitrary(grouped));
        unionLegColumns.push_back(std::move(groupedColumns));
      }
      return builder().make<UnionAll>(
          {std::move(unionInputs),
           std::move(unionLegColumns),
           node->outputColumns()});
    }

    if (!changed) {
      return node;
    }
    return builder().make<UnionAll>(
        {std::move(newInputs), node->legColumns(), node->outputColumns()});
  }

  // Describes a DELETE after its target scan handle is final.
  connector::DeleteLayout
  deleteLayout(const TableWrite& write, const Scan& scan, bool exact) {
    const auto& table = *write.table();
    const auto& connectorId = table.layouts().front()->connectorId();
    const auto& connectorContext = session_.context();
    VELOX_CHECK_NOT_NULL(
        connectorContext, "Optimizer session requires a context");
    auto metadata = session_.connectorMetadata(connectorId);

    VELOX_CHECK_NOT_NULL(
        scan.scanHandle(),
        "Delete target scan reached physical planning without a handle");

    return metadata->deleteLayout(
        connectorContext->sessionFor(connectorId),
        scan.scanHandle()->tableHandle,
        exact);
  }

  // Adds the columns consumed by a DELETE writer and resolves its physical
  // requirements against those columns.
  std::pair<NodeCP, WriterRequirements> prepareDeleteInput(
      NodeCP input,
      const ColumnVector& rowIdColumns,
      const connector::DeleteLayout& deleteLayout) {
    folly::F14FastMap<std::string_view, ColumnCP> rowIdByName;
    folly::F14FastMap<std::string_view, ColumnCP> writerColumnByName;
    ExprVector projections;
    ColumnVector writerColumns;
    const size_t numWriterColumns =
        rowIdColumns.size() + deleteLayout.computedColumns().size();
    projections.reserve(numWriterColumns);
    writerColumns.reserve(numWriterColumns);
    for (ColumnCP column : rowIdColumns) {
      VELOX_CHECK(
          std::ranges::find(input->outputColumns(), column) !=
              input->outputColumns().end(),
          "Delete input does not produce a row-id column: {}",
          column->schemaName());
      rowIdByName.emplace(column->schemaName(), column);
      writerColumnByName.emplace(column->schemaName(), column);
      projections.push_back(column);
      writerColumns.push_back(column);
    }
    VeloxExprImporter exprImporter(builder(), rowIdByName);
    for (const auto& computed : deleteLayout.computedColumns()) {
      VELOX_CHECK(
          !writerColumnByName.contains(computed.name),
          "Computed delete column duplicates a writer column: {}",
          computed.name);
      ExprCP expr = exprImporter.importExpr(computed.expr);
      ColumnCP column = Column::createForSymbol(
          toName(computed.name), Value(toType(computed.expr->type())));
      writerColumnByName.emplace(computed.name, column);
      projections.push_back(expr);
      writerColumns.push_back(column);
    }
    if (writerColumns != input->outputColumns()) {
      input = builder().make<Project>(
          {input, std::move(projections), writerColumns});
    }
    const auto resolveKeys = [&](const std::vector<std::string>& names) {
      ExprVector keys;
      keys.reserve(names.size());
      for (const auto& name : names) {
        const auto it = writerColumnByName.find(name);
        VELOX_CHECK(
            it != writerColumnByName.end(),
            "Delete key is not a column the writer receives: {}",
            name);
        keys.push_back(it->second);
      }
      return keys;
    };

    ExprVector partitionKeys = resolveKeys(deleteLayout.shuffleKeys());
    if (const auto& partitionType = deleteLayout.partitionType()) {
      const auto& keyTypes = partitionType->keyTypes();
      for (size_t i = 0; i < partitionKeys.size(); ++i) {
        VELOX_CHECK(
            partitionKeys[i]->value().type == toType(keyTypes[i]),
            "Delete shuffle key type does not match the partition type: {}",
            deleteLayout.shuffleKeys()[i]);
      }
    }

    return {
        input,
        WriterRequirements{
            .partitionKeys = std::move(partitionKeys),
            .partitionType = deleteLayout.partitionType(),
            .orderKeys = resolveKeys(deleteLayout.sortKeys()),
            .orderTypes = toOrderTypes(deleteLayout.sortOrders()),
        }};
  }

  // Resolves target-layout column names into INSERT input expressions.
  WriterRequirements resolveInsertRequirements(const TableWrite& write) {
    const auto* layout = write.table()->layouts().front();
    const auto& schema = write.table()->type();
    const auto resolveColumns =
        [&](const std::vector<const connector::Column*>& columns) {
          ExprVector expressions;
          expressions.reserve(columns.size());
          for (const auto* column : columns) {
            expressions.push_back(
                write.columnExprs().at(schema->getChildIdx(column->name())));
          }
          return expressions;
        };

    return WriterRequirements{
        .partitionKeys = resolveColumns(layout->partitionColumns()),
        .partitionType = layout->partitionType(),
        .orderKeys = {},
        .orderTypes = {},
    };
  }

  // Adds the shared global, driver, and per-writer ordering topology.
  NodeCP applyWriterRequirements(
      NodeCP input,
      const WriterRequirements& requirements) {
    if (!requirements.partitionKeys.empty()) {
      VELOX_CHECK_NOT_NULL(requirements.partitionType);
      const auto* nativeType = requirements.partitionType.get();
      const auto* globalType =
          queryCtx()->scaledPartitionType(nativeType, numWorkers_);
      if (numWorkers_ > 1 &&
          !input->physicalProperties()
               .globalPartition.satisfiesWritePartitioning(
                   requirements.partitionKeys, *globalType)) {
        if (NodeCP grouped = groupedRead(
                input, requirements.partitionKeys, Alignment::kExactKeys)) {
          if (grouped->physicalProperties()
                  .globalPartition.satisfiesWritePartitioning(
                      requirements.partitionKeys, *globalType)) {
            input = grouped;
          }
        }
      }
      if (numWorkers_ > 1 &&
          !input->physicalProperties()
               .globalPartition.satisfiesWritePartitioning(
                   requirements.partitionKeys, *globalType)) {
        input = partitionTo(input, requirements.partitionKeys, globalType);
      }
      if (numDrivers_ > 1 &&
          !input->physicalProperties()
               .driverPartition.satisfiesWritePartitioning(
                   requirements.partitionKeys, *nativeType)) {
        input = exchange(
            {input,
             Partitioning::connectorHash(
                 PropertyScope::kDriver,
                 requirements.partitionKeys,
                 nativeType)});
      }
    }

    if (!requirements.orderKeys.empty() &&
        !hasOrder(input, requirements.orderKeys, requirements.orderTypes)) {
      // Each writer driver consumes its own sorted stream; there is no local
      // merge because combining drivers would violate writer partitioning.
      input = builder().make<Sort>(
          {input,
           requirements.orderKeys,
           requirements.orderTypes,
           /*partial=*/true});
    }
    return input;
  }

  // Rewrites a DELETE after its source has been physically planned.
  NodeCP rewriteDelete(const TableWrite& write, NodeCP input) {
    if (input->is(NodeType::kValues) &&
        input->as<Values>()->cardinality() == 0) {
      if (input == write.input()) {
        return &write;
      }
      return builder().make<TableWrite>(
          {input,
           write.table(),
           write.kind(),
           write.columnExprs(),
           write.rowIdColumns(),
           write.deleteTarget(),
           /*deleteLayout=*/nullptr});
    }

    const ScanCP targetScan =
        TableWrite::findTargetScan(input, write.deleteTarget());
    VELOX_CHECK_NOT_NULL(
        targetScan,
        "A delete reached physical planning without its target scan");
    const bool exact = input == targetScan;
    auto layout = deleteLayout(write, *targetScan, exact);
    ExprVector columnExprs;
    if (!layout.hasWriter()) {
      VELOX_CHECK(
          exact,
          "Connector returned a metadata-only DELETE layout for rows another "
          "operator narrows");
    } else {
      auto [writerInput, requirements] =
          prepareDeleteInput(input, write.rowIdColumns(), layout);
      columnExprs.assign(
          writerInput->outputColumns().begin(),
          writerInput->outputColumns().end());
      input = applyWriterRequirements(writerInput, requirements);
    }

    return builder().make<TableWrite>(
        {input,
         write.table(),
         write.kind(),
         std::move(columnExprs),
         write.rowIdColumns(),
         write.deleteTarget(),
         builder().takeDeleteLayout(std::move(layout))});
  }

  // A CREATE or INSERT into a bucketed/partitioned layout needs each partition
  // on one worker, else workers race to create the same bucket file.
  // Repartition unless the input is already compatibly bucketed or runs on one
  // worker.
  NodeCP rewriteTableWrite(const TableWrite* node, NoContext& context)
      override {
    NodeCP newInput = rewrite(node->input(), context);
    if (node->kind() == connector::WriteKind::kDelete) {
      return rewriteDelete(*node, newInput);
    }

    newInput =
        applyWriterRequirements(newInput, resolveInsertRequirements(*node));
    if (newInput == node->input()) {
      return node;
    }
    return builder().make<TableWrite>(
        {newInput,
         node->table(),
         node->kind(),
         node->columnExprs(),
         node->rowIdColumns(),
         node->deleteTarget(),
         node->deleteLayout()});
  }

 private:
  const OptimizerSession& session_;
  const OptimizerOptions& options_;
  // A per-plan property, not an OptimizerOptions field, so it is held
  // separately.
  const int32_t numWorkers_;
  const int32_t numDrivers_;
  // True when the plan runs on one task.
  const bool oneTask_;
  const std::function<NodeCP(NodeCP)> replicate_;
  const Partitioning statePartitioning_;
  // True when a gather between workers starts a plan that runs on one driver.
  const bool gatherOntoOneDriver_;
  ExprFactory exprFactory_;
  ExprSimplifier& simplifier_;

  // Owned by the caller; the step rewriters share it.
  EstimateProvider& estimateProvider_;

  // Keys stay valid for the query: Builder owns interned nodes.
  folly::F14FastMap<NodeCP, NodeCP> rewrittenNodes_;
};

} // namespace

NodeCP PlanPhysicalPass::run(
    NodeCP root,
    Builder& builder,
    velox::core::ExpressionEvaluator& evaluator,
    EstimateProvider& estimateProvider,
    const OptimizerSession& session,
    const OptimizerOptions& options,
    int32_t numWorkers,
    int32_t numDrivers) {
  VELOX_USER_CHECK_GE(numWorkers, 1, "numWorkers must be at least 1");
  VELOX_USER_CHECK_GE(numDrivers, 1, "numDrivers must be at least 1");

  ExprSimplifier simplifier{builder, evaluator};
  PhysicalPlanRewriter rewriter{
      builder,
      simplifier,
      estimateProvider,
      session,
      options,
      numWorkers,
      numDrivers,
      /*oneTask=*/numWorkers == 1};
  return rewriter.rewrite(root);
}

} // namespace facebook::axiom::optimizer::v2
