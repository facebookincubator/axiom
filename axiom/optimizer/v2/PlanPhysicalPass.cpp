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

#include <cstdint>
#include <numeric>
#include <vector>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/v2/CostModel.h"
#include "axiom/optimizer/v2/DPhyp.h"
#include "axiom/optimizer/v2/EstimateProvider.h"
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
    cluster.unnests.push_back(unnest);
    collectCluster(unnest->input(), cluster, dissolveCrossJoins);
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
        numDrivers_{numDrivers} {}

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
            !coBucketJoinSides(
                newLeft, newRight, node->leftKeys(), node->rightKeys())) {
          newLeft =
              partition(newLeft, node->leftKeys(), nullAware && !rightIsBuild);
          newRight =
              partition(newRight, node->rightKeys(), nullAware && rightIsBuild);
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
        .leftKeys = node->leftKeys(),
        .rightKeys = node->rightKeys(),
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

  // Co-locates a keyed join without shuffling the bucketed side(s): keeps both
  // when both are already bucketed on the keys and copartitionable, else aligns
  // the unbucketed side to the bucketed one's connector partitioning. Returns
  // true and updates 'left'/'right' when co-location applies; false when
  // neither side is bucketed, or both are bucketed but not copartitionable — in
  // either case the caller falls back to the standard shuffle.
  bool coBucketJoinSides(
      NodeCP& left,
      NodeCP& right,
      const ExprVector& leftKeys,
      const ExprVector& rightKeys) {
    const auto& leftPart = left->physicalProperties().globalPartition;
    const auto& rightPart = right->physicalProperties().globalPartition;
    const bool leftBucketed = leftPart.isBucketedOn(leftKeys);
    const bool rightBucketed = rightPart.isBucketedOn(rightKeys);
    if (leftBucketed && rightBucketed) {
      return leftPart.partitionType->copartition(*rightPart.partitionType) !=
          nullptr;
    }
    if (leftBucketed) {
      right = partitionTo(right, rightKeys, leftPart.partitionType);
      return true;
    }
    if (rightBucketed) {
      left = partitionTo(left, leftKeys, rightPart.partitionType);
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

  // True when every expression in 'sub' is also in 'super'.
  static bool isSubset(const ExprVector& sub, const ExprVector& super) {
    const auto superSet = PlanObjectSet::fromObjects(super);
    for (ExprCP expr : sub) {
      if (!superSet.contains(expr)) {
        return false;
      }
    }
    return true;
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
  NodeCP ensureCoLocated(NodeCP input, const ExprVector& keys) {
    if (numWorkers_ == 1 || isGathered(input)) {
      return input;
    }

    if (keys.empty()) {
      return gather(input);
    }

    if (input->physicalProperties().globalPartition.coLocates(keys)) {
      return input;
    }

    return partition(input, keys);
  }

  NodeCP rewriteAggregate(const Aggregate* node, NoContext& context) override {
    NodeCP input = rewrite(node->input(), context);
    if (isSplittableAggregate(node)) {
      // Remote two-stage: the input must shuffle across workers to co-locate
      // its groups, so the partial reduces rows before that remote exchange.
      if (numWorkers_ > 1 && needsShuffle(input, node->groupingKeys())) {
        return rewriteAggregateSplit(node, input, /*remoteExchange=*/true);
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
    return rewriteAggregateCoLocated(node, input, /*requiredCoLocation=*/{});
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

  // True when co-locating 'input's groups on 'keys' requires a shuffle — i.e.
  // when the single-stage path's `ensureCoLocated` would add one. Mirrors its
  // conditions: a gathered input already co-locates any keys; a global
  // aggregate (no keys) needs a gather unless already gathered; otherwise a
  // shuffle is needed unless the input is already partitioned on the keys.
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

  // Rewrites an aggregate, co-locating its input on 'requiredCoLocation' when
  // that is a non-empty subset of the grouping keys: such a subset still lands
  // every group on one task, and choosing it lets a consumer that needs the
  // coarser partition reuse it without a reshuffle. Otherwise co-locates on the
  // grouping keys (empty for a global aggregate, which gathers).
  NodeCP rewriteAggregateCoLocated(
      const Aggregate* node,
      NodeCP newInput,
      const ExprVector& requiredCoLocation) {
    ExprVector keys;
    // A global () grouping set emits a default row over empty input; a
    // single-stage aggregate must gather (empty keys) so that row is produced
    // once, not once per worker.
    if (node->globalGroupingSets().empty()) {
      keys = !requiredCoLocation.empty() &&
              isSubset(requiredCoLocation, node->groupingKeys())
          ? requiredCoLocation
          : node->groupingKeys();
    }
    NodeCP input = ensureCoLocated(newInput, keys);
    if (input == node->input()) {
      return node;
    }
    return builder().make<Aggregate>(Aggregate::Key{
        .input = input,
        .groupingKeys = node->groupingKeys(),
        .aggregates = node->aggregates(),
        .outputColumns = node->outputColumns(),
        .step = node->step(),
        .groupId = node->groupId(),
        .globalGroupingSets = node->globalGroupingSets()});
  }

  // Distributes a window: its input must be partitioned on the PARTITION BY
  // keys so each partition is computed on one task (an unpartitioned window
  // gathers to one task). Runs bottom-up, so the input is already physically
  // planned.
  NodeCP rewriteWindow(const Window* node, NoContext& context) override {
    NodeCP input =
        ensureCoLocated(rewrite(node->input(), context), node->partitionKeys());
    if (input == node->input()) {
      return node;
    }
    return builder().make<Window>(
        {input,
         node->functions(),
         node->partitionKeys(),
         node->orderKeys(),
         node->orderTypes(),
         node->outputColumns()});
  }

  // Distributes a per-partition top-n (row_number / rank): like a window, its
  // input must be partitioned on the PARTITION BY keys (gather when none).
  NodeCP rewriteTopNRowNumber(const TopNRowNumber* node, NoContext& context)
      override {
    NodeCP input =
        ensureCoLocated(rewrite(node->input(), context), node->partitionKeys());
    if (input == node->input()) {
      return node;
    }
    return builder().make<TopNRowNumber>(
        {input,
         node->rankFunction(),
         node->partitionKeys(),
         node->orderKeys(),
         node->orderTypes(),
         node->limit(),
         node->rankColumn(),
         node->outputColumns()});
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
    NodeCP partialSort =
        builder().make<Sort>({input, node->orderKeys(), node->orderTypes()});
    return gatherMerge(partialSort, node->orderKeys(), node->orderTypes());
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

  // Rewrites 'node' so its output is co-located on 'keys', pushing the
  // requirement into a producer that can satisfy it by its own partition choice
  // — an aggregate co-locating on a subset of its grouping keys — so no
  // reshuffle is added above it. Other producers fall back to a co-locating
  // exchange.
  NodeCP
  rewriteCoLocatedOn(NodeCP node, const ExprVector& keys, NoContext& context) {
    if (numWorkers_ > 1 && node->is(NodeType::kAggregate)) {
      const auto* aggregate = node->as<Aggregate>();
      return ensureCoLocated(
          rewriteAggregateCoLocated(
              aggregate, rewrite(aggregate->input(), context), keys),
          keys);
    }
    return ensureCoLocated(rewrite(node, context), keys);
  }

  // EnforceDistinct asserts at most one row per distinct key across all input.
  // A per-task check only sees duplicates that share a task, so the input must
  // be co-located on the distinct keys.
  NodeCP rewriteEnforceDistinct(const EnforceDistinct* node, NoContext& context)
      override {
    NodeCP input =
        rewriteCoLocatedOn(node->input(), node->distinctKeys(), context);
    if (input == node->input()) {
      return node;
    }
    return builder().make<EnforceDistinct>(
        {input, node->distinctKeys(), node->errorMessage()});
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

    NodeCP partial = builder().make<Limit>(
        {newInput, /*offset=*/0, node->offsetPlusCount()});
    return builder().make<Limit>(
        {gather(partial), node->offset(), node->count()});
  }

  // Distributes a bounded ORDER BY (TopN, an ORDER BY + LIMIT): at numWorkers>1
  // a per-task partial keeps its own top offset+count rows, the gather brings
  // them to one task, and the full TopN produces the global top there.
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

    NodeCP partial = builder().make<TopN>(
        {newInput,
         node->orderKeys(),
         node->orderTypes(),
         /*offset=*/0,
         node->offset() + node->count()});
    return builder().make<TopN>(
        {gather(partial),
         node->orderKeys(),
         node->orderTypes(),
         node->offset(),
         node->count()});
  }

  // Distributes a UNION ALL: at numWorkers>1 isolate each leg behind a remote
  // exchange so a single-task leg (global aggregate, order/limit, Values) does
  // not pin the union to one task and never shares a fragment with a parallel
  // leg. Arbitrary partitioning
  // suffices — the union only concatenates; a downstream operator that needs a
  // partitioning establishes it on the union's output itself.
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
      for (NodeCP& newInput : newInputs) {
        newInput = arbitrary(newInput);
      }
      changed = true;
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
    if (numWorkers_ > 1) {
      const auto* layout = node->table()->layouts().front();
      const auto& partitionColumns = layout->partitionColumns();
      if (!partitionColumns.empty()) {
        const auto* targetType = layout->partitionType().get();
        const auto& schema = node->table()->type();
        ExprVector keys;
        keys.reserve(partitionColumns.size());
        for (const auto* partitionColumn : partitionColumns) {
          keys.push_back(node->columnExprs().at(
              schema->getChildIdx(partitionColumn->name())));
        }
        const auto& partition = newInput->physicalProperties().globalPartition;
        if (!partition.isBucketedCompatibleWith(keys, *targetType)) {
          newInput = partitionTo(newInput, keys, targetType);
        }
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

  // Shared across all clusters of this query so a leaf (or hash-consed
  // duplicate) subtree is estimated once.
  EstimateProvider estimateProvider_;
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
