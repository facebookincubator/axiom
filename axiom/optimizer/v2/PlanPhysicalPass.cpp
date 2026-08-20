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

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/v2/CostModel.h"
#include "axiom/optimizer/v2/DPhyp.h"
#include "axiom/optimizer/v2/EstimateProvider.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/HypergraphBuilder.h"
#include "axiom/optimizer/v2/JoinCluster.h"
#include "axiom/optimizer/v2/JoinTreeEmitter.h"
#include "axiom/optimizer/v2/NodeRewriter.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// True if every expression in 'keys' references at least one column.
bool allKeysReferenceColumns(const ExprVector& keys) {
  for (ExprCP key : keys) {
    if (key->columns().empty()) {
      return false;
    }
  }
  return true;
}

// Reorder limit: inner / LEFT / RIGHT / FULL equi-joins,
// plus filtering semijoin (kLeftSemiFilter), antijoin (kAnti), and
// mark-preserving semijoin (kLeftSemiProject).
bool isClusterable(const Join* join) {
  using velox::core::JoinType;
  if (join->leftKeys().empty()) {
    return false;
  }
  // A hyperedge needs a non-empty relation set on both sides: the edge's
  // left/right relation sets are the cluster leaves referenced by the
  // left/right keys (HypergraphBuilder::unionExpressionRelations). A
  // degenerate equi-key that references no column (e.g. the `1 = 1` a
  // `1 IN (SELECT 1)` decorrelates to) would yield an empty side and trip
  // the JoinEdge invariant, so such a join stays an opaque cluster leaf.
  if (!allKeysReferenceColumns(join->leftKeys()) ||
      !allKeysReferenceColumns(join->rightKeys())) {
    return false;
  }
  const auto kind = join->joinType();
  return kind == JoinType::kInner || kind == JoinType::kLeft ||
      kind == JoinType::kRight || kind == JoinType::kFull ||
      kind == JoinType::kLeftSemiFilter || kind == JoinType::kAnti ||
      kind == JoinType::kLeftSemiProject;
}

void collectCluster(
    NodeCP node,
    JoinCluster& cluster,
    bool dissolveCrossJoins) {
  if (node->is(NodeType::kJoin)) {
    const auto* join = node->as<Join>();
    if (isClusterable(join)) {
      cluster.joins.push_back(join);
      collectCluster(join->left(), cluster, dissolveCrossJoins);
      collectCluster(join->right(), cluster, dissolveCrossJoins);
      return;
    }
    // A bare keyless inner join (no keys, no filter) is a comma-join cross
    // product. Descend through it so its children join the cluster as
    // separate relations, letting an equi-predicate elsewhere reconnect
    // them (a spurious cross product that the join graph can avoid). A
    // keyless join that carries a filter is a theta or decorrelated-subquery
    // join; leave it an opaque leaf so its semantics are preserved.
    if (dissolveCrossJoins && join->isInner() && join->leftKeys().empty() &&
        join->filter().empty()) {
      collectCluster(join->left(), cluster, dissolveCrossJoins);
      collectCluster(join->right(), cluster, dissolveCrossJoins);
      return;
    }
  }
  if (node->is(NodeType::kUnnest)) {
    const auto* unnest = node->as<Unnest>();
    // An Unnest of a constant, as in UNNEST(ARRAY[1, 2]), reads a subtree that
    // produces no columns. No predicate can reference it, so it is not a
    // relation of the cluster; the Unnest emits it as its own input.
    NodeCP input = unnest->input();
    if (!input->outputColumns().empty()) {
      collectCluster(input, cluster, dissolveCrossJoins);
    }
    // Preserve JoinCluster's post-order invariant.
    cluster.unnests.push_back(unnest);
    return;
  }
  cluster.leaves.push_back(node);
}

// True if some relation appears in an edge's TES but in no edge's left/right
// endpoints. Such a relation is connected only by correlation (e.g. an outer
// table referenced inside a decorrelated subquery), so DPhyp — which grows
// subgraphs along edge endpoints — cannot assemble it. Dissolving a cross
// join that strands such a relation produces an unplannable graph.
bool hasEndpointStrandedRelation(const JoinHypergraph& graph) {
  RelationSet endpoints;
  RelationSet tesUnion;
  for (size_t i = 0; i < graph.edges().size(); ++i) {
    endpoints.unionSet(graph.edges()[i].left());
    endpoints.unionSet(graph.edges()[i].right());
    tesUnion.unionSet(graph.tes()[i]);
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

// True when regrouping the scans under 'node' can make it bucketed. Mirrors
// GroupedScanRewriter's two stopping rules -- only a scan of a bucketed table
// contributes, and nothing past an exchange does -- but reads the tree instead
// of rebuilding it, so asking costs no allocation.
bool hasRegroupableScan(NodeCP node, folly::F14FastMap<NodeCP, bool>& answers) {
  if (node->is(NodeType::kExchange)) {
    return false;
  }
  const auto it = answers.find(node);
  if (it != answers.end()) {
    return it->second;
  }
  bool answer;
  if (node->is(NodeType::kScan)) {
    answer = node->as<Scan>()->storageBucketing().partitionType != nullptr;
  } else if (node->is(NodeType::kUnionAll)) {
    // A union is bucketed only when every leg is.
    answer = std::ranges::all_of(node->inputs(), [&](NodeCP leg) {
      return hasRegroupableScan(leg, answers);
    });
  } else {
    answer = std::ranges::any_of(node->inputs(), [&](NodeCP input) {
      return hasRegroupableScan(input, answers);
    });
  }
  answers.emplace(node, answer);
  return answer;
}

// Nodes are interned, so one subtree can hang off several parents -- and the
// same node can be two legs of one union. Answers are remembered per node so a
// diamond is walked once and a repeated leg reports what it is.
bool hasRegroupableScan(NodeCP node) {
  folly::F14FastMap<NodeCP, bool> answers;
  return hasRegroupableScan(node, answers);
}

// Rewrites a subtree so every scan of a bucketed table is read one bucket-group
// at a time. Nodes above are rebuilt by the base rewriter, which re-derives
// their partitioning from the new inputs.
class GroupedScanRewriter : public NodeRewriter<> {
 public:
  GroupedScanRewriter(Builder& builder, int32_t numWorkers)
      : NodeRewriter<>(builder), numWorkers_{numWorkers} {}

  // True when at least one scan was read grouped.
  bool regrouped() const {
    return regrouped_;
  }

 protected:
  // Past an exchange the rows are redistributed, so how the source was read
  // cannot help this consumer; leave that subtree alone.
  NodeCP rewriteExchange(const Exchange* node, NoContext& /*context*/)
      override {
    return node;
  }

  // A union is bucketed only when every leg is, so one ungroupable leg forfeits
  // it for all of them. Checking that first keeps the groupable legs from being
  // rebuilt only to be thrown away.
  NodeCP rewriteUnionAll(const UnionAll* node, NoContext& context) override {
    for (NodeCP leg : node->inputs()) {
      if (!hasRegroupableScan(leg)) {
        return node;
      }
    }
    return NodeRewriter<>::rewriteUnionAll(node, context);
  }

  NodeCP rewriteScan(const Scan* node, NoContext& /*context*/) override {
    // Whether this bucketing is any use to the consumer is not decided here —
    // the keys it asked for may belong to another leg or another side of a
    // join. A table with no bucketing at all is left alone, so a subtree that
    // could never be grouped rebuilds to itself and allocates nothing.
    const Partitioning available = node->storageBucketing();
    if (available.partitionType == nullptr) {
      return node;
    }
    // Coarsened to the worker count here so the plan carries the group count
    // it will run with, and two scans that scale alike stay one node.
    NodeCP grouped = builder().make<Scan>(Scan::Key{
        .baseTable = node->baseTable(),
        .outputColumns = node->outputColumns(),
        .scanHandle = node->scanHandle(),
        .groupedPartitionType = queryCtx()->scaledPartitionType(
            available.partitionType, numWorkers_)});
    // A scan already read this way interns back to itself, and then this
    // rewrite changed nothing.
    if (grouped != node) {
      regrouped_ = true;
    }
    return grouped;
  }

 private:
  const int32_t numWorkers_;
  bool regrouped_{false};
};

class PhysicalPlanRewriter : public NodeRewriter<> {
 public:
  PhysicalPlanRewriter(
      Builder& builder,
      const OptimizerOptions& options,
      int32_t numWorkers,
      int32_t numDrivers)
      : NodeRewriter(builder),
        options_{options},
        numWorkers_{numWorkers},
        numDrivers_{numDrivers},
        exprFactory_{builder} {}

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

 protected:
  // Rebuilds a join that DPhyp does not plan (non-clusterable cross / theta /
  // decorrelated-subquery joins, and the syntactic-order / uncostable
  // fallbacks) with its children rewritten, giving it a valid distributed input
  // combination at numWorkers>1:
  //   - keyed: co-partition both sides on the join keys (semi/anti
  //   correctness),
  //   - keyless with a broadcastable build: broadcast the build (cross /
  //   theta),
  //   - keyless, build not broadcastable (right / full): gather both sides.
  NodeCP rewriteUnclusteredJoin(const Join* node, NoContext& context) {
    NodeCP newLeft = rewrite(node->left(), context);
    NodeCP newRight = rewrite(node->right(), context);
    ExprVector leftKeys{node->leftKeys()};
    ExprVector rightKeys{node->rightKeys()};
    if (numWorkers_ > 1) {
      if (!node->leftKeys().empty()) {
        // A null-aware anti/semi join (NOT IN / IN) needs the existence side's
        // null keys on every probe partition. canBroadcastBuild is true exactly
        // when the right side is the non-preserved (existence) side.
        const bool nullAware = node->nullAware();
        const bool rightIsBuild = canBroadcastBuild(node->joinType());
        // A bucketed side co-locates the join with no full shuffle. Not for
        // null-aware anti/semi: a bucketed existence side confines a null key
        // to one bucket, so it must shuffle-replicate.
        if (nullAware ||
            !coBucketJoinSides(newLeft, newRight, leftKeys, rightKeys)) {
          std::tie(newLeft, leftKeys) =
              builder().materializeKeys(newLeft, leftKeys);
          std::tie(newRight, rightKeys) =
              builder().materializeKeys(newRight, rightKeys);
          newLeft = partition(newLeft, leftKeys, nullAware && !rightIsBuild);
          newRight = partition(newRight, rightKeys, nullAware && rightIsBuild);
        }
      } else if (canBroadcastBuild(node->joinType())) {
        newRight = broadcast(newRight);
      } else {
        newLeft = ensureGathered(newLeft);
        newRight = ensureGathered(newRight);
      }
    }
    if (newLeft == node->left() && newRight == node->right()) {
      return node;
    }
    return builder().make<Join>(Join::Key{
        .left = newLeft,
        .right = newRight,
        .joinType = node->joinType(),
        .leftKeys = leftKeys,
        .rightKeys = rightKeys,
        .filter = node->filter(),
        .nullAware = node->nullAware(),
        .nullAsValue = node->nullAsValue(),
        .outputColumns = node->outputColumns()});
  }

  NodeCP rewriteJoin(const Join* node, NoContext& context) override {
    // Syntactic mode keeps every join in query order: rebuild the subtree
    // as written, with no clustering or cost-based reordering.
    if (options_.syntacticJoinOrder || !isClusterable(node)) {
      return rewriteUnclusteredJoin(node, context);
    }

    JoinCluster cluster;
    cluster.root = node;
    collectCluster(node, cluster, /*dissolveCrossJoins=*/true);
    if (cluster.joins.empty()) {
      return rewriteUnclusteredJoin(node, context);
    }

    std::vector<NodeCP> rewrittenLeaves;
    auto buildGraph = [&]() {
      rewrittenLeaves.clear();
      rewrittenLeaves.reserve(cluster.leaves.size());
      for (NodeCP leaf : cluster.leaves) {
        rewrittenLeaves.push_back(rewrite(leaf, context));
      }
      return HypergraphBuilder::build(
          cluster, rewrittenLeaves, estimateProvider_);
    };

    JoinHypergraph graph = buildGraph();

    // Dissolving a cross join can strand a correlation-only relation (in an
    // edge's TES but no edge's endpoints), which DPhyp cannot assemble. Redo
    // without dissolving so that relation stays bundled with its cross-join
    // partner. Clusters that benefit from dissolution have no stranded
    // relation and keep the dissolved form.
    if (hasEndpointStrandedRelation(graph)) {
      cluster = JoinCluster{};
      cluster.root = node;
      collectCluster(node, cluster, /*dissolveCrossJoins=*/false);
      graph = buildGraph();
    }

    // Partition the cluster's relations into components, each closed under
    // every edge's TES: DPhyp can only assemble a relation set once all
    // relations in a crossing edge's TES are present, so two relations tied
    // by any edge's TES must land in one component. A dissolved keyless
    // cross join with no predicate connecting its sides yields separate
    // components, combined later with cross products. Union-find over TES.
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
    for (const RelationSet& tes : graph.tes()) {
      int32_t representative = -1;
      tes.forEach([&](int32_t id) {
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

    graph.setTargetColumns(PlanObjectSet::fromObjects(node->outputColumns()));

    DefaultCostModel costModel{estimateProvider_};
    DPhyp dphyp{
        graph,
        costModel,
        options_.dphypEnumerationBudget,
        numWorkers_,
        options_.broadcastSizeLimit};
    if (components.size() == 1) {
      MemoOpCP root = dphyp.enumerate();
      // No costable plan (a relation or join key lacked stats): keep the
      // cluster in query order.
      if (root == nullptr) {
        return rewriteUnclusteredJoin(node, context);
      }
      return JoinTreeEmitter::emit(
          root, graph, node->outputColumns(), builder());
    }
    const std::vector<MemoOpCP> roots = dphyp.enumerate(components);
    if (roots.empty()) {
      return rewriteUnclusteredJoin(node, context);
    }
    return JoinTreeEmitter::emitComponents(
        roots, graph, node->outputColumns(), builder(), numWorkers_);
  }

  // Remote exchanges that unconditionally establish a partitioning on 'input'.
  NodeCP gather(NodeCP input) {
    return builder().make<Exchange>({input, Partitioning::globalGather()});
  }

  // An order-preserving gather: merges the sorted per-task streams (the input
  // must already be sorted on 'orderKeys') onto one task, lowering to a Velox
  // MergeExchange.
  NodeCP gatherMerge(
      NodeCP input,
      const ExprVector& orderKeys,
      const OrderTypeVector& orderTypes) {
    return builder().make<Exchange>(
        {input, Partitioning::globalGatherMerge(orderKeys, orderTypes)});
  }

  NodeCP partition(
      NodeCP input,
      const ExprVector& keys,
      bool replicateNullsAndAny = false) {
    return builder().make<Exchange>(
        {input, Partitioning::globalHash(keys, replicateNullsAndAny)});
  }

  NodeCP broadcast(NodeCP input) {
    return builder().make<Exchange>({input, Partitioning::globalBroadcast()});
  }

  // Repartitions 'input' on 'keys' using 'targetType' (a connector
  // partitioning), so it co-locates with a side already bucketed that way.
  NodeCP partitionTo(
      NodeCP input,
      const ExprVector& keys,
      const connector::PartitionType* targetType) {
    Partitioning partitioning = Partitioning::globalHash(keys);
    partitioning.partitionType = targetType;
    return builder().make<Exchange>({input, std::move(partitioning)});
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

  // Where 'node's bucket columns sit in 'keys', or nullopt when it is not
  // bucketed or a bucket column is not among them. A join pairs leftKeys[i]
  // with rightKeys[i], so equal positions on both sides mean their bucketings
  // line up column for column, whatever order the query wrote the keys in.
  //
  // A key repeated in 'keys' reports its first position. Since a pair is taken
  // only when both sides report the same positions, and the join equates the
  // keys at those positions, the two bucketings still agree value for value;
  // picking the first of several equal keys can only cost a pairing, never
  // make a wrong one.
  static std::optional<std::vector<size_t>> bucketKeyPositions(
      NodeCP node,
      const ExprVector& keys) {
    if (node == nullptr) {
      return std::nullopt;
    }
    const auto& partition = node->physicalProperties().globalPartition;
    if (partition.partitionType == nullptr || partition.keys.empty()) {
      return std::nullopt;
    }
    std::vector<size_t> positions;
    positions.reserve(partition.keys.size());
    for (ExprCP partitionKey : partition.keys) {
      const auto it = std::find_if(keys.begin(), keys.end(), [&](ExprCP key) {
        return key->sameOrEqual(*partitionKey);
      });
      if (it == keys.end()) {
        return std::nullopt;
      }
      positions.push_back(it - keys.begin());
    }
    return positions;
  }

  // The connector bucketing 'node' produces, or null when it has none. A null
  // 'node' is the ordinary case of a side with no grouped read to offer.
  static const connector::PartitionType* bucketingAt(NodeCP node) {
    return node == nullptr
        ? nullptr
        : node->physicalProperties().globalPartition.partitionType;
  }

  // Co-locates a keyed join on the sides' bucketing instead of shuffling both.
  // A side qualifies when it is already bucketed on its keys or its table can
  // be read grouped by them. Both qualify and copartition: keep both. One
  // qualifies: align the other to its connector partitioning. Neither, or two
  // that do not copartition: returns false and the caller shuffles.
  // Updates 'left'/'right' and, where a side is repartitioned to the other's
  // bucketing, 'leftKeys'/'rightKeys': that shuffle needs column keys, so an
  // expression key is computed first and the join reads that column.
  bool coBucketJoinSides(
      NodeCP& left,
      NodeCP& right,
      ExprVector& leftKeys,
      ExprVector& rightKeys) {
    // What a side can offer: itself when already bucketed on keys the join
    // uses, else a grouped read of its table by them, else nothing. Which of
    // the join's keys, and in what order, is settled below — a side's bucketing
    // follows its table, not the order the query wrote the join in.
    const auto offer = [&](NodeCP side, const ExprVector& keys) -> NodeCP {
      return side->physicalProperties().globalPartition.coLocates(keys)
          ? side
          : groupedRead(side, keys, Alignment::kCoLocated);
    };

    const NodeCP leftOffer = offer(left, leftKeys);
    const auto leftAt = bucketKeyPositions(leftOffer, leftKeys);

    // Building the right side's offer is wasted when its table's bucketing
    // could not meet the left's anyway.
    const auto* leftType = bucketingAt(leftOffer);
    const auto* rightStorage = leafStorageBucketing(right);
    // Skip only when the right side's table is known not to meet the left's.
    // A null answer means unknown — its subtree is not a single table — and
    // then it must be built and judged on its own partitioning.
    const bool worthBuilding = leftType == nullptr || rightStorage == nullptr ||
        leftType->copartition(*rightStorage) != nullptr;
    const NodeCP rightOffer = worthBuilding ? offer(right, rightKeys) : nullptr;
    const auto rightAt = bucketKeyPositions(rightOffer, rightKeys);
    const auto* rightType = bucketingAt(rightOffer);

    // Repartitions the side that has nothing to offer onto the other's
    // bucketing, on the keys that correspond to the other's bucket columns.
    // That shuffle needs column keys, so an expression key is computed first
    // and the join then reads that column.
    const auto alignTo = [&](NodeCP& side,
                             ExprVector& keys,
                             const std::vector<size_t>& at,
                             const connector::PartitionType* target) {
      ExprVector shuffleKeys;
      shuffleKeys.reserve(at.size());
      for (const size_t position : at) {
        shuffleKeys.push_back(keys[position]);
      }
      auto [keyed, columnKeys] = builder().materializeKeys(side, shuffleKeys);
      side = partitionTo(keyed, columnKeys, target);
    };

    if (leftAt.has_value() && rightAt.has_value()) {
      // Equal positions mean bucket column i of one side joins bucket column i
      // of the other; unequal ones would send matching rows to different tasks.
      // Both sides are kept only when the partitioning they agree on runs at
      // the width they already run at; otherwise the caller shuffles.
      const auto* folded = queryCtx()->copartitionedType(leftType, rightType);
      if (*leftAt != *rightAt || folded == nullptr ||
          folded->numPartitions() != leftType->numPartitions() ||
          folded->numPartitions() != rightType->numPartitions()) {
        return false;
      }
      left = leftOffer;
      right = rightOffer;
      return true;
    }
    if (leftAt.has_value()) {
      left = leftOffer;
      alignTo(right, rightKeys, *leftAt, leftType);
      return true;
    }
    if (rightAt.has_value()) {
      right = rightOffer;
      alignTo(left, leftKeys, *rightAt, rightType);
      return true;
    }
    return false;
  }

  NodeCP arbitrary(NodeCP input) {
    return builder().make<Exchange>({input, Partitioning::globalArbitrary()});
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
  // Builder::materializeKeys.
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

    auto [keyed, columnKeys] =
        builder().materializeKeys(input, keys, keyAliases);
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
    NodeCP input = rewrite(node->input(), context);
    if (isSplittableAggregate(node)) {
      // Remote two-stage: the input must shuffle across workers to co-locate
      // its groups, so the partial reduces rows before that remote exchange.
      if (numWorkers_ > 1 && needsShuffle(input, node->groupingKeys())) {
        // Reading the input grouped co-locates the groups without a shuffle.
        if (NodeCP grouped = groupedRead(
                input, node->groupingKeys(), Alignment::kCoLocated)) {
          input = grouped;
        } else {
          return rewriteAggregateSplit(node, input, /*remoteExchange=*/true);
        }
      }
      // Local two-stage: the input is already co-located (e.g. a bucketed
      // scan's grouped fragment), but at numDrivers > 1 a local exchange still
      // brings each group to one driver, so the partial reduces rows before it.
      // At numDrivers == 1 there is no exchange, so a single stage is optimal.
      // The local exchange itself is not materialized here — emit inserts it at
      // numDrivers > 1 (local exchanges are implicit).
      if (numDrivers_ > 1) {
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
    if (coLocatedInput == node->input()) {
      return node;
    }
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
    return builder().make<Aggregate>(Aggregate::Key{
        .input = coLocatedInput,
        .groupingKeys = groupingKeys,
        .aggregates = node->aggregates(),
        .outputColumns = node->outputColumns(),
        .step = node->step(),
        .groupId = node->groupId(),
        .globalGroupingSets = node->globalGroupingSets()});
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
  // when that makes the result meet 'alignment' on 'keys'; null otherwise.
  //
  // Which operators carry a scan's bucketing upward is not decided here: the
  // subtree is rebuilt and each node derives its own partitioning, so a join
  // keeps its probe's, a union keeps what its legs agree on, and anything that
  // redistributes keeps nothing. The answer is then read off the rebuilt root.
  //
  // Nodes are interned, so rebuilding one over unchanged inputs returns the
  // node itself; only a scan that is actually regrouped, and its ancestors,
  // are new.
  NodeCP
  groupedRead(NodeCP input, const ExprVector& keys, Alignment alignment) {
    if (numWorkers_ == 1 || keys.empty()) {
      return nullptr;
    }

    GroupedScanRewriter rewriter{builder(), numWorkers_};
    NoContext context;
    NodeCP grouped = rewriter.rewrite(input, context);
    if (!rewriter.regrouped()) {
      return nullptr;
    }
    return satisfies(
               grouped->physicalProperties().globalPartition, keys, alignment)
        ? grouped
        : nullptr;
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

    NodeCP partial = builder().make<Aggregate>(Aggregate::Key{
        .input = input,
        .groupingKeys = node->groupingKeys(),
        .aggregates = node->aggregates(),
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

    // Without a remote exchange the partial and final share one fragment; the
    // final's local repartition (added at emit for numDrivers > 1) co-locates
    // each group's partials on one driver.
    NodeCP finalInput = !remoteExchange ? partial
        : finalKeys.empty()             ? gather(partial)
                                        : partition(partial, finalKeys);

    return builder().make<Aggregate>(Aggregate::Key{
        .input = finalInput,
        .groupingKeys = finalKeys,
        .aggregates = node->aggregates(),
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
    if (numWorkers_ == 1 || isGathered(input)) {
      if (input == node->input()) {
        return node;
      }
      return builder().make<Sort>(
          {input, node->orderKeys(), node->orderTypes()});
    }
    auto [sortInput, orderKeys] =
        builder().materializeKeys(input, node->orderKeys());
    NodeCP partialSort =
        builder().make<Sort>({sortInput, orderKeys, node->orderTypes()});
    return gatherMerge(partialSort, orderKeys, node->orderTypes());
  }

  // Distributes an EnforceSingleRow (scalar-subquery single-row assertion): it
  // must see all rows on one task to assert the global count, so gather.
  NodeCP rewriteEnforceSingleRow(
      const EnforceSingleRow* node,
      NoContext& context) override {
    NodeCP input = ensureGathered(rewrite(node->input(), context));
    if (input == node->input()) {
      return node;
    }
    return builder().make<EnforceSingleRow>({input});
  }

  // EnforceDistinct asserts at most one row per distinct key across all input.
  // A per-task check only sees duplicates that share a task, so the input must
  // be co-located on the distinct keys.
  NodeCP rewriteEnforceDistinct(const EnforceDistinct* node, NoContext& context)
      override {
    auto [input, distinctKeys] =
        ensureCoLocated(rewrite(node->input(), context), node->distinctKeys());
    if (input == node->input()) {
      return node;
    }
    return builder().make<EnforceDistinct>(
        {input, distinctKeys, node->errorMessage()});
  }

  // Distributes a LIMIT: at numWorkers>1 a per-task partial keeps the first
  // offset+count rows, the gather brings them to one task, and the full Limit
  // applies offset/count there.
  NodeCP rewriteLimit(const Limit* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    if (numWorkers_ == 1 || isGathered(newInput)) {
      if (newInput == node->input()) {
        return node;
      }
      return builder().make<Limit>({newInput, node->offset(), node->count()});
    }

    // A partial keeps the first offset + count rows of each task; with no
    // count that is every row, so there is nothing to reduce before the gather.
    if (node->offsetPlusCount() == std::numeric_limits<int64_t>::max()) {
      return builder().make<Limit>(
          {gather(newInput), node->offset(), node->count()});
    }

    NodeCP partial = builder().make<Limit>(
        {newInput, /*offset=*/0, node->offsetPlusCount()});
    return builder().make<Limit>(
        {gather(partial), node->offset(), node->count()});
  }

  // Distributes a bounded ORDER BY (TopN, an ORDER BY + LIMIT): at numWorkers>1
  // a per-task partial keeps its own top offset+count rows, and an
  // order-preserving merge gather combines those sorted streams, so the one
  // task only has to drop rows outside offset/count rather than sort again.
  NodeCP rewriteTopN(const TopN* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    if (numWorkers_ == 1 || isGathered(newInput)) {
      if (newInput == node->input()) {
        return node;
      }
      return builder().make<TopN>(
          {newInput,
           node->orderKeys(),
           node->orderTypes(),
           node->offset(),
           node->count()});
    }

    auto [topNInput, orderKeys] =
        builder().materializeKeys(newInput, node->orderKeys());
    NodeCP partial = builder().make<TopN>(
        {topNInput,
         orderKeys,
         node->orderTypes(),
         /*offset=*/0,
         node->offsetPlusCount()});
    return builder().make<Limit>(
        {gatherMerge(partial, orderKeys, node->orderTypes()),
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
  // collocated write) or the query runs on one worker. The within-worker split
  // is added at emit.
  NodeCP rewriteTableWrite(const TableWrite* node, NoContext& context)
      override {
    NodeCP newInput = rewrite(node->input(), context);
    // A partition key computed for the shuffle is written from that column
    // rather than evaluated a second time.
    ExprFactory::ExprSubstitution materialized;
    if (numWorkers_ > 1) {
      const auto* layout = node->table()->layouts().front();
      const auto& partitionColumns = layout->partitionColumns();
      if (!partitionColumns.empty()) {
        // Coarsened here, not at emit: the exchange's partition count and the
        // writer fragment's task count are the same decision.
        const auto* targetType = queryCtx()->scaledPartitionType(
            layout->partitionType().get(), numWorkers_);
        const auto& schema = node->table()->type();
        ExprVector keys;
        keys.reserve(partitionColumns.size());
        for (const auto* partitionColumn : partitionColumns) {
          keys.push_back(node->columnExprs().at(
              schema->getChildIdx(partitionColumn->name())));
        }
        // Reading the source by its own bucketing can deliver rows already
        // grouped the way the target is written, which saves the shuffle.
        if (!newInput->physicalProperties()
                 .globalPartition.isBucketedCompatibleWith(keys, *targetType)) {
          if (NodeCP grouped =
                  groupedRead(newInput, keys, Alignment::kExactKeys)) {
            if (grouped->physicalProperties()
                    .globalPartition.isBucketedCompatibleWith(
                        keys, *targetType)) {
              newInput = grouped;
            }
          }
        }
        if (!newInput->physicalProperties()
                 .globalPartition.isBucketedCompatibleWith(keys, *targetType)) {
          auto [keyed, columnKeys] = builder().materializeKeys(newInput, keys);
          newInput = partitionTo(keyed, columnKeys, targetType);
          for (size_t i = 0; i < keys.size(); ++i) {
            if (columnKeys[i] != keys[i]) {
              materialized.emplace(keys[i], columnKeys[i]);
            }
          }
        }
      }
    }
    if (newInput == node->input()) {
      return node;
    }
    ExprVector columnExprs{node->columnExprs()};
    if (!materialized.empty()) {
      columnExprs = exprFactory_.replace(columnExprs, materialized);
    }
    return builder().make<TableWrite>(
        {newInput, node->table(), node->kind(), std::move(columnExprs)});
  }

 private:
  const OptimizerOptions& options_;
  // A per-plan property, not an OptimizerOptions field, so it is held
  // separately.
  const int32_t numWorkers_;
  const int32_t numDrivers_;
  ExprFactory exprFactory_;

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
    const OptimizerOptions& options,
    int32_t numWorkers,
    int32_t numDrivers) {
  PhysicalPlanRewriter rewriter{builder, options, numWorkers, numDrivers};
  return rewriter.rewrite(root);
}

} // namespace facebook::axiom::optimizer::v2
