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
        ? precompute.toColumn(
              aggregate->condition(), /*alias=*/nullptr, /*allowConstant=*/true)
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
        !currentInput->physicalProperties().driverCoLocates(group.keys)) {
      currentInput = builder.make<Exchange>(
          {currentInput,
           Partitioning::hash(PropertyScope::kDriver, group.keys)});
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
  ClusterCollector(
      JoinCluster& cluster,
      bool dissolveCrossJoins,
      const folly::F14FastSet<const Join*>& opaqueJoins)
      : cluster_{cluster},
        dissolveCrossJoins_{dissolveCrossJoins},
        opaqueJoins_{opaqueJoins} {}

  void collect(NodeCP node, bool preserved, bool unnestMovable) {
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
  // These return true when they took the node into the cluster; an Unnest
  // always joins it.

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
    if (!deterministic) {
      cluster_.leaves.push_back(unnest);
      return;
    }
    // An Unnest of a constant, as in UNNEST(ARRAY[1, 2]), reads a subtree that
    // produces no columns. No predicate can reference it, so it is not a
    // relation of the cluster; the Unnest emits it as its own input.
    NodeCP input = unnest->input();
    if (!input->outputColumns().empty()) {
      collect(input, preserved, unnestMovable);
    }
    // Preserve JoinCluster's post-order invariant.
    cluster_.unnests.push_back(unnest);
    if (!unnestMovable) {
      cluster_.unnestBarriers.push_back(unnest);
    }
  }

  JoinCluster& cluster_;
  const bool dissolveCrossJoins_;
  const folly::F14FastSet<const Join*>& opaqueJoins_;
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
    // A constant-input Unnest is the only row source on its side of a join.
    // It cannot be removed from the graph even when no predicate reads the
    // columns it produces.
    if (unnest->input()->outputColumns().empty() || barriers.contains(unnest) ||
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
    const folly::F14FastSet<const Join*>& opaqueJoins) {
  JoinCluster cluster;
  cluster.root = root;
  ClusterCollector{cluster, dissolveCrossJoins, opaqueJoins}.collect(
      root, /*preserved=*/true, /*unnestMovable=*/true);
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

class PhysicalPlanRewriter : public NodeRewriter<> {
 public:
  PhysicalPlanRewriter(
      Builder& builder,
      ExprSimplifier& simplifier,
      const OptimizerOptions& options,
      int32_t numWorkers,
      int32_t numDrivers)
      : NodeRewriter(builder),
        options_{options},
        numWorkers_{numWorkers},
        numDrivers_{numDrivers},
        exprFactory_{builder},
        simplifier_{simplifier} {}

  // Re-exposes the rewrite(NodeCP) overload hidden by the override below.
  using NodeRewriter<>::rewrite;

  // Rewriting a node is a pure function of the node, so descend each node
  // once.
  NodeCP rewrite(NodeCP node, NoContext& context) override {
    const auto it = rewrittenNodes_.find(node);
    if (it != rewrittenNodes_.end()) {
      return it->second;
    }
    NodeCP rewritten = NodeRewriter<>::rewrite(node, context);
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
  NodeCP rewriteFixedPoint(const FixedPoint* node, NoContext& context)
      override {
    NodeCP anchor = rewrite(node->anchor(), context);

    static constexpr int32_t kRecursiveNumDrivers = 1;
    PhysicalPlanRewriter singleThreaded{
        builder(),
        simplifier_,
        options_,
        /*numWorkers=*/1,
        /*numDrivers=*/kRecursiveNumDrivers};
    NodeCP step = singleThreaded.rewrite(node->step());
    NodeCP convergence = singleThreaded.rewrite(node->convergence());
    if (anchor == node->anchor() && step == node->step() &&
        convergence == node->convergence() &&
        node->recursiveNumDrivers() == kRecursiveNumDrivers) {
      return node;
    }
    return builder().make<FixedPoint>({
        .anchor = anchor,
        .step = step,
        .convergence = convergence,
        .name = node->name(),
        .outputColumns = node->outputColumns(),
        .sourceColumns = node->sourceColumns(),
        .maxIterations = node->maxIterations(),
        .recursiveNumDrivers = kRecursiveNumDrivers,
    });
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

  // Chooses the build side of inner, left, and right joins when both inputs
  // have point estimates. Otherwise keeps the written orientation.
  // A distributed keyless left or right join instead puts its non-preserved
  // input on the build side so that input can be broadcast.
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
    if (numDrivers_ > 1 && velox::core::isCountingJoin(join.joinType)) {
      std::tie(join.left, join.leftKeys) =
          ensureDriverCoLocated(join.left, join.leftKeys);
    }
    return PhysicalJoin::makeJoin(std::move(join), builder(), simplifier_);
  }

  // Adds the exchanges required by a join outside DPhyp and translates it.
  NodeCP makePhysicalJoin(Join::Key join) {
    if (numWorkers_ > 1) {
      if (!join.leftKeys.empty()) {
        // An already partitioned side can co-locate the join without a full
        // shuffle. Not for null-aware anti/semi: an existence side confines a
        // null key to one partition, so it must shuffle-replicate.
        if (join.nullAware ||
            !coPartitionJoinSides(
                join.left, join.right, join.leftKeys, join.rightKeys)) {
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

    CollectedJoinCluster collected = collectJoinCluster(
        node, /*dissolveCrossJoins=*/true, /*opaqueJoins=*/{});
    if (collected.cluster.joins.empty()) {
      return rewriteFallbackJoin(node, context);
    }

    const folly::F14FastSet<const Join*> opaqueJoins =
        valueProducersReadInCluster(collected.cluster);
    if (!opaqueJoins.empty()) {
      collected =
          collectJoinCluster(node, /*dissolveCrossJoins=*/true, opaqueJoins);
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
      collected =
          collectJoinCluster(node, /*dissolveCrossJoins=*/false, opaqueJoins);
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

  // Remote exchanges that unconditionally establish a partitioning on 'input'.
  NodeCP gather(NodeCP input) {
    return builder().make<Exchange>(
        {input, Partitioning::gather(PropertyScope::kGlobal)});
  }

  // An order-preserving gather: merges the sorted per-task streams (the input
  // must already be sorted on 'orderKeys') onto one task, lowering to a Velox
  // MergeExchange.
  NodeCP gatherMerge(
      NodeCP input,
      const ExprVector& orderKeys,
      const OrderTypeVector& orderTypes) {
    return builder().make<Exchange>(
        {input,
         Partitioning::gatherMerge(
             PropertyScope::kGlobal, orderKeys, orderTypes)});
  }

  NodeCP partition(
      NodeCP input,
      const ExprVector& keys,
      bool replicateNullsAndAny = false) {
    return builder().make<Exchange>(
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
    return builder().make<Exchange>({build, replicated});
  }

  // Repartitions 'input' on 'keys' using 'targetType' (a connector
  // partitioning), so it co-locates with a side already bucketed that way.
  NodeCP partitionTo(
      NodeCP input,
      const ExprVector& keys,
      const connector::PartitionType* targetType) {
    return builder().make<Exchange>(
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
      ExprVector& rightKeys) {
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
      right = rightOffer->node;
      alignTo(left, leftKeys, *rightOffer);
      return true;
    }
    return false;
  }

  NodeCP arbitrary(NodeCP input) {
    return builder().make<Exchange>({input, Partitioning::globalArbitrary()});
  }

  NodeCP driverPartition(NodeCP input, const ExprVector& keys) {
    return builder().make<Exchange>(
        {input,
         keys.empty() ? Partitioning::gather(PropertyScope::kDriver)
                      : Partitioning::hash(PropertyScope::kDriver, keys)});
  }

  NodeCP driverGatherMerge(
      NodeCP input,
      const ExprVector& orderKeys,
      const OrderTypeVector& orderTypes) {
    return builder().make<Exchange>(
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
    if (numDrivers_ == 1 || input->physicalProperties().driverCoLocates(keys)) {
      return {input, keys};
    }
    if (keys.empty()) {
      return {driverPartition(input, keys), keys};
    }
    auto [keyed, columnKeys] = PrecomputeProjections::materializeKeys(
        input, keys, builder(), simplifier_, keyAliases);
    return {driverPartition(keyed, columnKeys), columnKeys};
  }

  // True when 'input' already produces all rows on one task.
  static bool isGathered(NodeCP input) {
    return input->physicalProperties().globalPartition.is(
        PartitionKind::kGather);
  }

  // Ensures all rows reach one task: reuse when 'input' is already gathered,
  // else a remote gather exchange. A no-op at a single worker, where one task
  // already holds all rows.
  NodeCP ensureGathered(NodeCP input) {
    if (numWorkers_ == 1 || isGathered(input)) {
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
    if (numWorkers_ == 1 || isGathered(input)) {
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

  // Distributes 'node' over 'input', its physically planned input.
  NodeCP distributeAggregate(const Aggregate* node, NodeCP input) {
    if (isSplittableAggregate(node)) {
      // Remote two-stage: the input must shuffle across workers to co-locate
      // its groups, so the partial reduces rows before that remote exchange.
      if (numWorkers_ > 1 && needsShuffle(input, node->groupingKeys())) {
        // Reading the input grouped co-locates the groups without a shuffle.
        if (NodeCP grouped = groupedRead(
                input, node->groupingKeys(), Alignment::kCoLocated)) {
          input = grouped;
        } else {
          return rewriteAggregateSplit(
              node,
              input,
              /*remoteExchange=*/true,
              /*partialLimit=*/std::nullopt);
        }
      }
      // A single stage is safe when input properties already prove each group
      // stays on one driver.
      if (numDrivers_ > 1 &&
          !input->physicalProperties().driverCoLocates(node->groupingKeys())) {
        return rewriteAggregateSplit(
            node,
            input,
            /*remoteExchange=*/false,
            /*partialLimit=*/std::nullopt);
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
    if (isGathered(input)) {
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
  //
  // With 'partialLimit', exact per-driver deduplication precedes the partial
  // limit, and a global gather precedes the final deduplication.
  NodeCP rewriteAggregateSplit(
      const Aggregate* node,
      NodeCP input,
      bool remoteExchange,
      std::optional<int64_t> partialLimit) {
    const size_t numKeys = node->groupingKeys().size();
    const auto& finalColumns = node->outputColumns();
    VELOX_CHECK(
        !partialLimit || node->aggregates().empty(),
        "A partial aggregate limit is only valid for DISTINCT");

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
    AggregateCallVector partialAggregates;
    if (!partialLimit) {
      partialAggregates = precomputeAggregateArgs(
          node->aggregates(), AggregateStep::kPartial, precompute, builder());
    }
    input = std::move(precompute).node();

    NodeCP partial;
    if (partialLimit) {
      ColumnCP marker = Column::createBoolean("mark");
      ColumnVector markedColumns = input->outputColumns();
      markedColumns.push_back(marker);
      partial = builder().make<MarkDistinct>(
          {input,
           ColumnVector{marker},
           partialKeys,
           /*masks=*/{},
           std::move(markedColumns)});
      partial = builder().make<Filter>({partial, ExprVector{marker}});
      partial = builder().make<Project>(
          {partial, std::move(partialKeys), std::move(partialColumns)});
    } else {
      partial = builder().make<Aggregate>(
          {.input = input,
           .groupingKeys = std::move(partialKeys),
           .aggregates = std::move(partialAggregates),
           .outputColumns = std::move(partialColumns),
           .step = AggregateStep::kPartial,
           .groupId = node->groupId(),
           .globalGroupingSets = node->globalGroupingSets()});
    }

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

    if (partialLimit) {
      partial = builder().make<Limit>(
          {partial, /*offset=*/0, *partialLimit, /*partial=*/true});
    }
    const bool gatherForPartialLimit =
        partialLimit && numWorkers_ > 1 && !isGathered(partial);
    NodeCP finalInput = gatherForPartialLimit ? gather(partial)
        : !remoteExchange                     ? partial
        : finalKeys.empty()                   ? gather(partial)
                                              : partition(partial, finalKeys);
    if (numDrivers_ > 1) {
      if (partialLimit) {
        finalInput = ensureDriverCoLocated(finalInput, {}).first;
      } else {
        std::tie(finalInput, finalKeys) =
            ensureDriverCoLocated(finalInput, finalKeys);
      }
    }

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

  // Distributes a per-partition top-n (row_number / rank): like a window, its
  // input must be partitioned on the PARTITION BY keys (gather when none).
  NodeCP rewriteTopNRowNumber(const TopNRowNumber* node, NoContext& context)
      override {
    auto [input, partitionKeys] =
        ensureCoLocated(rewrite(node->input(), context), node->partitionKeys());
    std::tie(input, partitionKeys) =
        ensureDriverCoLocated(input, partitionKeys);
    if (input == node->input()) {
      return node;
    }
    return builder().make<TopNRowNumber>(
        {input,
         node->rankFunction(),
         partitionKeys,
         node->orderKeys(),
         node->orderTypes(),
         node->limit(),
         node->rankColumn(),
         outputFollowingInput(node, input)});
  }

  // Distributes a global ORDER BY (Sort). At numWorkers>1 each task sorts its
  // own rows and an order-preserving merge gather (MergeExchange) combines the
  // sorted streams onto one task, so the sort parallelizes across tasks instead
  // of running on a single gathered task. A single worker, or an input already
  // gathered onto one task, sorts in one pass. Runs bottom-up, so the input is
  // already physically planned.
  NodeCP rewriteSort(const Sort* node, NoContext& context) override {
    NodeCP input = rewrite(node->input(), context);
    const bool needsDriverMerge = numDrivers_ > 1 &&
        !input->physicalProperties().driverCoLocates(/*keys=*/{});
    NodeCP sorted = !needsDriverMerge && input == node->input()
        ? node
        : builder().make<Sort>(
              {input, node->orderKeys(), node->orderTypes(), needsDriverMerge});
    if (needsDriverMerge) {
      sorted = driverGatherMerge(sorted, node->orderKeys(), node->orderTypes());
    }
    if (numWorkers_ == 1 || isGathered(input)) {
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

  // Distributes a LIMIT: at numWorkers>1 a per-task partial keeps the first
  // offset+count rows, the gather brings them to one task, and the full Limit
  // applies offset/count there.
  //
  // Over a DISTINCT, exact per-driver deduplication carries the partial Limit;
  // final deduplication runs after the gather.
  NodeCP rewriteLimit(const Limit* node, NoContext& context) override {
    const auto* distinct =
        isDistinct(node->input()) ? node->input()->as<Aggregate>() : nullptr;
    NodeCP input;
    if (distinct != nullptr) {
      NodeCP distinctInput = rewrite(distinct->input(), context);
      const bool remoteExchange = numWorkers_ > 1 &&
          needsShuffle(distinctInput, distinct->groupingKeys());
      if (node->isBounded() && (remoteExchange || numDrivers_ > 1)) {
        NodeCP split = rewriteAggregateSplit(
            distinct, distinctInput, remoteExchange, node->offsetPlusCount());
        return builder().make<Limit>({split, node->offset(), node->count()});
      }
      input = distributeAggregate(distinct, distinctInput);
    } else {
      input = rewrite(node->input(), context);
    }

    const bool needsGlobalGather = numWorkers_ > 1 && !isGathered(input);
    const bool partialReduces = node->isBounded();
    if (needsGlobalGather && !partialReduces) {
      return builder().make<Limit>(
          {gather(input), node->offset(), node->count()});
    }

    const int64_t taskOffset = needsGlobalGather ? 0 : node->offset();
    const int64_t taskCount =
        needsGlobalGather ? node->offsetPlusCount() : node->count();
    NodeCP taskLimit;
    if (numDrivers_ > 1 && partialReduces &&
        !input->physicalProperties().driverCoLocates(/*keys=*/{})) {
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

  // Distributes a bounded ORDER BY (TopN, an ORDER BY + LIMIT): at numWorkers>1
  // a per-task partial keeps its own top offset+count rows, and an
  // order-preserving merge gather combines those sorted streams, so the one
  // task only has to drop rows outside offset/count rather than sort again.
  NodeCP rewriteTopN(const TopN* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    const bool needsGlobalGather = numWorkers_ > 1 && !isGathered(newInput);
    const int64_t taskOffset = needsGlobalGather ? 0 : node->offset();
    const int64_t taskCount =
        needsGlobalGather ? node->offsetPlusCount() : node->count();
    NodeCP localTopN;
    if (numDrivers_ > 1 &&
        !newInput->physicalProperties().driverCoLocates(/*keys=*/{})) {
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
          partition.is(PartitionKind::kGather)) {
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

  // A write to a bucketed/partitioned layout needs each partition (bucket) on a
  // single worker, else workers race to create the same bucket file.
  // Repartition the input on the target's partition columns using the target's
  // connector partitioning, unless the input is already compatibly bucketed (a
  // collocated write), the query runs on one worker, or the write is a delete,
  // which writes no columns and so has no key to shuffle on.
  NodeCP rewriteTableWrite(const TableWrite* node, NoContext& context)
      override {
    NodeCP newInput = rewrite(node->input(), context);
    const auto* layout = node->table()->layouts().front();
    const auto& partitionColumns = layout->partitionColumns();
    if (node->kind() != connector::WriteKind::kDelete &&
        !partitionColumns.empty() && (numWorkers_ > 1 || numDrivers_ > 1)) {
      const auto& schema = node->table()->type();
      ExprVector keys;
      keys.reserve(partitionColumns.size());
      for (const auto* partitionColumn : partitionColumns) {
        keys.push_back(node->columnExprs().at(
            schema->getChildIdx(partitionColumn->name())));
      }
      if (numWorkers_ > 1) {
        // Coarsened here, not at emit: the exchange's partition count and the
        // writer fragment's task count are the same decision.
        const auto* taskPartitionType = queryCtx()->scaledPartitionType(
            layout->partitionType().get(), numWorkers_);
        // Reading the source by its own bucketing can deliver rows already
        // grouped the way the target is written, which saves the shuffle.
        if (!newInput->physicalProperties()
                 .globalPartition.satisfiesWritePartitioning(
                     keys, *taskPartitionType)) {
          if (NodeCP grouped =
                  groupedRead(newInput, keys, Alignment::kExactKeys)) {
            if (grouped->physicalProperties()
                    .globalPartition.satisfiesWritePartitioning(
                        keys, *taskPartitionType)) {
              newInput = grouped;
            }
          }
        }
        if (!newInput->physicalProperties()
                 .globalPartition.satisfiesWritePartitioning(
                     keys, *taskPartitionType)) {
          newInput = partitionTo(newInput, keys, taskPartitionType);
        }
      }
      // Keep the native layout type here. Connector local specs derive the
      // bucket-to-driver map from the runtime driver count; the scaled task
      // type already embeds the bucket-to-task map.
      if (numDrivers_ > 1 &&
          !newInput->physicalProperties()
               .driverPartition.satisfiesWritePartitioning(
                   keys, *layout->partitionType())) {
        newInput = builder().make<Exchange>(
            {newInput,
             Partitioning::connectorHash(
                 PropertyScope::kDriver, keys, layout->partitionType().get())});
      }
    }
    if (newInput == node->input()) {
      return node;
    }
    return builder().make<TableWrite>(
        {newInput, node->table(), node->kind(), node->columnExprs()});
  }

 private:
  const OptimizerOptions& options_;
  // A per-plan property, not an OptimizerOptions field, so it is held
  // separately.
  const int32_t numWorkers_;
  const int32_t numDrivers_;
  ExprFactory exprFactory_;
  ExprSimplifier& simplifier_;

  // Shared across all clusters of this query so a leaf (or hash-consed
  // duplicate) subtree is estimated once.
  EstimateProvider estimateProvider_;

  // Keys stay valid for the query: Builder owns interned nodes.
  folly::F14FastMap<NodeCP, NodeCP> rewrittenNodes_;
};

} // namespace

NodeCP PlanPhysicalPass::run(
    NodeCP root,
    Builder& builder,
    velox::core::ExpressionEvaluator& evaluator,
    const OptimizerOptions& options,
    int32_t numWorkers,
    int32_t numDrivers) {
  VELOX_USER_CHECK_GE(numWorkers, 1, "numWorkers must be at least 1");
  VELOX_USER_CHECK_GE(numDrivers, 1, "numDrivers must be at least 1");

  ExprSimplifier simplifier{builder, evaluator};
  PhysicalPlanRewriter rewriter{
      builder, simplifier, options, numWorkers, numDrivers};
  return rewriter.rewrite(root);
}

} // namespace facebook::axiom::optimizer::v2
