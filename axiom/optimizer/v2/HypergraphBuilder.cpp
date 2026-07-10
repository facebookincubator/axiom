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

#include "axiom/optimizer/v2/HypergraphBuilder.h"

#include <map>
#include <set>

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "axiom/optimizer/EstimateMath.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/AlgebraicProperties.h"
#include "axiom/optimizer/v2/Cost.h"
#include "axiom/optimizer/v2/EstimateProvider.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// Cluster leaves whose columns `expression` references.
RelationSet expressionRelations(
    ExprCP expression,
    const folly::F14FastMap<ColumnCP, int8_t>& columnToLeaf) {
  RelationSet result;
  expression->columns().forEach<Column>([&](ColumnCP column) {
    const auto it = columnToLeaf.find(column);
    VELOX_CHECK(
        it != columnToLeaf.end(),
        "Expression references a column not produced by any cluster leaf: {}",
        column->toString());
    result.add(it->second);
  });
  return result;
}

RelationSet unionExpressionRelations(
    const ExprVector& expressions,
    const folly::F14FastMap<ColumnCP, int8_t>& columnToLeaf) {
  RelationSet result;
  for (ExprCP expression : expressions) {
    result.unionSet(expressionRelations(expression, columnToLeaf));
  }
  return result;
}

// Per-Join leaf covers of left and right operands, with the
// kRight-to-kLeft normalization baked in: for an IR `kRight`
// join, `left`/`right` are swapped and `joinType` is `kLeft`.
// The rest of the pipeline never sees `kRight`.
struct JoinLeaves {
  RelationSet left;
  RelationSet right;
  velox::core::JoinType joinType;
};

// Returns the subtree leaves of `node`; populates `leaves[J]` for
// every Join `J` reached during the walk. For an IR `kRight` join,
// `leaves[J]` records the swapped operands and `joinType = kLeft`.
// Unnest nodes are not in `leafIds`; descend into `input()`.
RelationSet populateJoinLeaves(
    NodeCP node,
    const folly::F14FastMap<NodeCP, int8_t>& leafIds,
    folly::F14FastMap<JoinCP, JoinLeaves>& leaves) {
  const auto it = leafIds.find(node);
  if (it != leafIds.end()) {
    return RelationSet::singleton(it->second);
  }
  if (node->is(NodeType::kUnnest)) {
    return populateJoinLeaves(node->as<Unnest>()->input(), leafIds, leaves);
  }
  const auto* join = node->as<Join>();
  const RelationSet leftLeaves =
      populateJoinLeaves(join->left(), leafIds, leaves);
  const RelationSet rightLeaves =
      populateJoinLeaves(join->right(), leafIds, leaves);
  // Normalize right-form joins to their left form by swapping operands.
  // kRight, kRightSemiFilter and kRightSemiProject each map to the
  // matching left-form type; the rest of the pipeline never sees a
  // right-form join.
  switch (join->joinType()) {
    case velox::core::JoinType::kRight:
      leaves[join] = JoinLeaves{
          .left = rightLeaves,
          .right = leftLeaves,
          .joinType = velox::core::JoinType::kLeft};
      break;
    case velox::core::JoinType::kRightSemiFilter:
      leaves[join] = JoinLeaves{
          .left = rightLeaves,
          .right = leftLeaves,
          .joinType = velox::core::JoinType::kLeftSemiFilter};
      break;
    case velox::core::JoinType::kRightSemiProject:
      leaves[join] = JoinLeaves{
          .left = rightLeaves,
          .right = leftLeaves,
          .joinType = velox::core::JoinType::kLeftSemiProject};
      break;
    default:
      leaves[join] = JoinLeaves{
          .left = leftLeaves,
          .right = rightLeaves,
          .joinType = join->joinType()};
  }
  RelationSet subtree{leftLeaves};
  subtree.unionSet(rightLeaves);
  return subtree;
}

// Returns the leaves an ancestor join must add to its TES to keep
// unsound reshapes blocked over `node`'s descendant joins. `node`
// is in the ancestor's LEFT subtree iff `leftSide` is true.
// `parentType` is the (already-kRight-normalized) join type of
// the ancestor.
RelationSet tesExpansion(
    NodeCP node,
    velox::core::JoinType parentType,
    const RelationSet& parentSes,
    bool leftSide,
    const folly::F14FastMap<NodeCP, int8_t>& leafIds,
    const folly::F14FastMap<JoinCP, JoinLeaves>& leaves) {
  if (leafIds.contains(node)) {
    return RelationSet{};
  }
  if (node->is(NodeType::kUnnest)) {
    return tesExpansion(
        node->as<Unnest>()->input(),
        parentType,
        parentSes,
        leftSide,
        leafIds,
        leaves);
  }

  const auto* childJoin = node->as<Join>();
  const auto& childLeaves = leaves.at(childJoin);

  // CD-A: for a LEFT-subtree descendant the M/N table is keyed
  // `(child, parent)`. For a RIGHT-subtree descendant the roles
  // flip to `(parent, child)`. `childLeaves.joinType` is already
  // normalized (never kRight).
  auto props = leftSide
      ? AlgebraicProperties::derive(childLeaves.joinType, parentType)
      : AlgebraicProperties::derive(parentType, childLeaves.joinType);

  // When the parent's referenced relations overlap a child
  // operand that the equivalence's syntactic precondition
  // forbids, the equivalence breaks; downgrade that property.
  // Skip for inner-over-inner where commutativity makes the
  // equivalences hold regardless of predicate placement.
  const bool involvesOuter =
      childLeaves.joinType != velox::core::JoinType::kInner ||
      parentType != velox::core::JoinType::kInner;
  if (involvesOuter) {
    if (leftSide) {
      if (parentSes.hasIntersection(childLeaves.left)) {
        props.associative = false;
      }
      if (parentSes.hasIntersection(childLeaves.right)) {
        props.leftAsscom = false;
      }
    } else {
      if (parentSes.hasIntersection(childLeaves.right)) {
        props.associative = false;
      }
      if (parentSes.hasIntersection(childLeaves.left)) {
        props.rightAsscom = false;
      }
    }
  }

  RelationSet expansion;
  if (leftSide) {
    if (!props.leftAsscom) {
      expansion.unionSet(childLeaves.right);
    }
    if (!props.associative) {
      expansion.unionSet(childLeaves.left);
    }
  } else {
    if (!props.rightAsscom) {
      expansion.unionSet(childLeaves.left);
    }
    if (!props.associative) {
      expansion.unionSet(childLeaves.right);
    }
  }
  expansion.unionSet(tesExpansion(
      childJoin->left(), parentType, parentSes, leftSide, leafIds, leaves));
  expansion.unionSet(tesExpansion(
      childJoin->right(), parentType, parentSes, leftSide, leafIds, leaves));
  return expansion;
}

std::pair<int8_t, int8_t> orderedPair(int8_t a, int8_t b) {
  return a < b ? std::make_pair(a, b) : std::make_pair(b, a);
}

// Adds equi-join edges implied by transitivity of inner equality. Given a chain
// of inner column equalities (a = b, b = c, ...), every pair (a = c) also
// holds, so a derived edge connects relations the written predicates leave only
// transitively linked. DPhyp can then attach a sub-join to either end of an
// equality chain — e.g. the nation/region filter to customer rather than only
// supplier in TPC-H q5 — instead of being forced through the single written
// path. Only inner equalities participate; an outer join's null-padding breaks
// the transitivity, and those keys are skipped.
//
// The equality classes are recorded on the shared `Column::equivalence`, so the
// cost model can later read them back (via `Column::equivalence`) to count a
// transitive equality's selectivity once across same-class edges at a join.
void addTransitiveInnerEdges(
    JoinHypergraph& graph,
    const JoinCluster& cluster,
    const folly::F14FastMap<ColumnCP, int8_t>& columnToLeaf) {
  std::vector<ColumnCP> equatedColumns;
  folly::F14FastSet<ColumnCP> seenColumns;
  for (JoinCP join : cluster.joins) {
    if (!join->isInner()) {
      continue;
    }
    for (size_t i = 0; i < join->leftKeys().size(); ++i) {
      ExprCP leftKey = join->leftKeys()[i];
      ExprCP rightKey = join->rightKeys()[i];
      if (leftKey->isColumn() && rightKey->isColumn()) {
        const auto left = leftKey->as<Column>();
        const auto right = rightKey->as<Column>();
        if (seenColumns.insert(left).second) {
          equatedColumns.push_back(left);
        }
        if (seenColumns.insert(right).second) {
          equatedColumns.push_back(right);
        }
      }
    }
  }

  // Relation pairs already joined by a single-relation inner equi-edge; a
  // derived edge for such a pair would duplicate the equality and double-count
  // its selectivity, so it is skipped.
  std::set<std::pair<int8_t, int8_t>> connected;
  for (const auto& edge : graph.edges()) {
    if (edge.joinType() == velox::core::JoinType::kInner &&
        edge.left().size() == 1 && edge.right().size() == 1) {
      connected.insert(orderedPair(
          static_cast<int8_t>(edge.left().min()),
          static_cast<int8_t>(edge.right().min())));
    }
  }

  // Group the equality classes, keeping one representative column per relation.
  // `classOrder` records first-seen order (from the deterministic
  // `equatedColumns`) so edges are added in a stable order: `classReps` is
  // hashed on a pointer, whose iteration order is non-deterministic, and the
  // edge-addition order fixes edge indices, which feed DPhyp's candidate
  // tiebreak.
  folly::F14FastMap<EquivalenceP, std::map<int8_t, ColumnCP>> classReps;
  std::vector<EquivalenceP> classOrder;
  for (ColumnCP column : equatedColumns) {
    const auto leafIt = columnToLeaf.find(column);
    if (leafIt != columnToLeaf.end()) {
      auto [it, inserted] = classReps.try_emplace(column->equivalence());
      if (inserted) {
        classOrder.push_back(column->equivalence());
      }
      it->second.emplace(leafIt->second, column);
    }
  }

  // Connect every relation pair within each class that is not already joined.
  for (EquivalenceP root : classOrder) {
    const std::map<int8_t, ColumnCP>& repByRelation = classReps.at(root);
    for (auto a = repByRelation.begin(); a != repByRelation.end(); ++a) {
      for (auto b = std::next(a); b != repByRelation.end(); ++b) {
        if (!connected.insert(orderedPair(a->first, b->first)).second) {
          continue;
        }
        const RelationSet leftSet = RelationSet::singleton(a->first);
        const RelationSet rightSet = RelationSet::singleton(b->first);
        const ExprVector leftKeys{a->second};
        const ExprVector rightKeys{b->second};
        RelationSet tes{leftSet};
        tes.unionSet(rightSet);
        graph.addEdge(
            JoinEdge{
                leftSet,
                rightSet,
                leftKeys,
                rightKeys,
                ExprVector{},
                velox::core::JoinType::kInner,
                /*nullAware=*/false,
                /*nullAsValue=*/false},
            tes);
      }
    }
  }
}

} // namespace

JoinHypergraph HypergraphBuilder::build(
    const JoinCluster& cluster,
    const std::vector<NodeCP>& rewrittenLeaves,
    EstimateProvider& estimateProvider) {
  VELOX_CHECK_LE(
      cluster.leaves.size(),
      RelationSet::kMaxRelations,
      "Cluster has more leaves than RelationSet can represent");
  VELOX_CHECK_EQ(
      cluster.leaves.size(),
      rewrittenLeaves.size(),
      "rewrittenLeaves must align 1:1 with cluster.leaves");

  JoinHypergraph graph;
  folly::F14FastMap<ColumnCP, int8_t> columnToLeaf;
  folly::F14FastMap<NodeCP, int8_t> leafIds;
  for (size_t i = 0; i < cluster.leaves.size(); ++i) {
    NodeCP original = cluster.leaves[i];
    NodeCP rewritten = rewrittenLeaves[i];
    PlanObjectSet columns;
    for (const auto* column : rewritten->outputColumns()) {
      columns.add(column);
    }
    const int8_t id = graph.addRelation(
        rewritten,
        estimateProvider.estimate(rewritten).cardinality,
        std::move(columns));
    VELOX_CHECK(leafIds.emplace(original, id).second, "Duplicate cluster leaf");
    if (rewritten != original) {
      VELOX_CHECK(
          leafIds.emplace(rewritten, id).second,
          "Duplicate rewritten cluster leaf");
    }
    for (const auto* column : rewritten->outputColumns()) {
      columnToLeaf.emplace(column, id);
    }
  }

  // Admit Unnest relations and extend the column-resolution map so the
  // Unnest's produced columns resolve to the Unnest's relation id.
  // Innermost first, so a dependent Unnest sees the Unnest feeding its
  // input already registered.
  folly::F14FastMap<UnnestCP, int8_t> unnestIds;
  for (UnnestCP unnest : cluster.unnests) {
    PlanObjectSet producedColumns;
    for (const auto& columnsForExpr : unnest->unnestColumns()) {
      for (const auto* column : columnsForExpr) {
        producedColumns.add(column);
      }
    }
    if (unnest->ordinalityColumn() != nullptr) {
      producedColumns.add(unnest->ordinalityColumn());
    }
    const std::optional<float> cardinality =
        mul(estimateProvider.estimate(unnest->input()).cardinality,
            kDefaultUnnestFanout);
    const int8_t id =
        graph.addUnnestRelation(unnest, cardinality, producedColumns);
    unnestIds.emplace(unnest, id);
    for (const auto& columnsForExpr : unnest->unnestColumns()) {
      for (const auto* column : columnsForExpr) {
        columnToLeaf.emplace(column, id);
      }
    }
    if (unnest->ordinalityColumn() != nullptr) {
      columnToLeaf.emplace(unnest->ordinalityColumn(), id);
    }
  }

  // Build the directed cross-join-unnest edge for each Unnest, and record
  // its input relations (the relation ids its input subtree contributes,
  // resolved via the now-complete columnToLeaf) for the outer-join barrier
  // applied in the cluster-join loop.
  folly::F14FastMap<int8_t, RelationSet> unnestInputRelations;
  for (UnnestCP unnest : cluster.unnests) {
    const int8_t id = unnestIds.at(unnest);
    RelationSet inputRelations;
    for (const auto* column : unnest->input()->outputColumns()) {
      const auto it = columnToLeaf.find(column);
      if (it != columnToLeaf.end()) {
        inputRelations.add(it->second);
      }
    }
    VELOX_CHECK(
        !inputRelations.empty(),
        "Unnest input subtree must contribute at least one cluster relation");
    unnestInputRelations.emplace(id, inputRelations);

    RelationSet tes{inputRelations};
    tes.add(id);
    graph.addEdge(
        JoinEdge{
            inputRelations,
            RelationSet::singleton(id),
            unnest->unnestExpressions()},
        tes);
  }

  folly::F14FastMap<JoinCP, JoinLeaves> joinLeaves;
  populateJoinLeaves(cluster.root, leafIds, joinLeaves);

  for (JoinCP join : cluster.joins) {
    const auto& leaves = joinLeaves.at(join);
    if (join->isInner()) {
      // Group keys by endpoint relation-pair and emit one edge per group.
      // Keys sharing an endpoint (a composite key) stay on one edge so
      // their combined selectivity is preserved; keys with different
      // endpoints become separate edges, so two relations are not forced
      // co-located by a single hyperedge.
      std::map<std::pair<uint64_t, uint64_t>, std::vector<size_t>>
          keysByEndpoint;
      for (size_t i = 0; i < join->leftKeys().size(); ++i) {
        const RelationSet leftKeyRelations{
            expressionRelations(join->leftKeys()[i], columnToLeaf)};
        const RelationSet rightKeyRelations{
            expressionRelations(join->rightKeys()[i], columnToLeaf)};
        keysByEndpoint[{leftKeyRelations.bits(), rightKeyRelations.bits()}]
            .push_back(i);
      }

      for (const auto& [endpoint, keyIndices] : keysByEndpoint) {
        ExprVector leftKeys;
        ExprVector rightKeys;
        for (size_t i : keyIndices) {
          leftKeys.push_back(join->leftKeys()[i]);
          rightKeys.push_back(join->rightKeys()[i]);
        }
        const RelationSet leftSet{
            unionExpressionRelations(leftKeys, columnToLeaf)};
        const RelationSet rightSet{
            unionExpressionRelations(rightKeys, columnToLeaf)};
        RelationSet ses{leftSet};
        ses.unionSet(rightSet);
        RelationSet tes{ses};
        tes.unionSet(tesExpansion(
            join->left(),
            join->joinType(),
            ses,
            /*leftSide=*/true,
            leafIds,
            joinLeaves));
        tes.unionSet(tesExpansion(
            join->right(),
            join->joinType(),
            ses,
            /*leftSide=*/false,
            leafIds,
            joinLeaves));

        // Consumer edges that reference an Unnest produced column must
        // wait until the Unnest's input relations are in cover, so the
        // cross-join-unnest fires before any consumer joins Unnest with
        // its other side.
        for (const auto& [unnestId, inputRelations] : unnestInputRelations) {
          if (ses.contains(unnestId)) {
            tes.unionSet(inputRelations);
          }
        }

        graph.addEdge(
            JoinEdge{
                leftSet,
                rightSet,
                leftKeys,
                rightKeys,
                ExprVector{},
                join->joinType(),
                /*nullAware=*/false,
                /*nullAsValue=*/false},
            tes);
      }

      for (ExprCP conjunct : join->filter()) {
        graph.addFilterConjunct(
            {conjunct, expressionRelations(conjunct, columnToLeaf)});
      }
    } else {
      // Swap keys when the IR join is a right-form join so the edge's
      // leftKeys reference `leaves.left` (already right-normalized to the
      // matching left form in populateJoinLeaves).
      const bool flip = join->joinType() == velox::core::JoinType::kRight ||
          join->joinType() == velox::core::JoinType::kRightSemiFilter ||
          join->joinType() == velox::core::JoinType::kRightSemiProject;
      const ExprVector& edgeLeftKeys =
          flip ? join->rightKeys() : join->leftKeys();
      const ExprVector& edgeRightKeys =
          flip ? join->leftKeys() : join->rightKeys();
      NodeCP normalizedLeftChild = flip ? join->right() : join->left();
      NodeCP normalizedRightChild = flip ? join->left() : join->right();

      const RelationSet leftSet{
          unionExpressionRelations(edgeLeftKeys, columnToLeaf)};
      const RelationSet rightSet{
          unionExpressionRelations(edgeRightKeys, columnToLeaf)};
      RelationSet ses{leftSet};
      ses.unionSet(rightSet);
      for (ExprCP conjunct : join->filter()) {
        ses.unionSet(expressionRelations(conjunct, columnToLeaf));
      }
      RelationSet tes{ses};
      tes.unionSet(tesExpansion(
          normalizedLeftChild,
          leaves.joinType,
          ses,
          /*leftSide=*/true,
          leafIds,
          joinLeaves));
      tes.unionSet(tesExpansion(
          normalizedRightChild,
          leaves.joinType,
          ses,
          /*leftSide=*/false,
          leafIds,
          joinLeaves));

      // Unnest barrier: if an Unnest's input subtree contributes a
      // relation on this outer join's null-padded side, the
      // cross-join-unnest must fire before the outer join. Otherwise
      // null-padding flows into UNNEST and rows drop. Expand TES to
      // include the Unnest relation id so DPhyp pins the order.
      RelationSet joinRelations{leaves.left};
      joinRelations.unionSet(leaves.right);
      for (const auto& [unnestId, inputRelations] : unnestInputRelations) {
        // An outer join inside the Unnest's input subtree is already
        // ordered before the Unnest by the directed cross-join-unnest
        // edge. The barrier would demand the reverse order, which no
        // plan can satisfy when this join is the only edge co-locating
        // the input relations.
        if (joinRelations.isSubset(inputRelations)) {
          continue;
        }
        RelationSet nullPaddedSide;
        if (leaves.joinType == velox::core::JoinType::kLeft) {
          nullPaddedSide = leaves.right;
        } else if (leaves.joinType == velox::core::JoinType::kFull) {
          nullPaddedSide = leaves.left;
          nullPaddedSide.unionSet(leaves.right);
        }
        if (nullPaddedSide.hasIntersection(inputRelations)) {
          tes.add(unnestId);
        }
      }

      // kLeftSemiProject preserves the left side and appends a mark; the
      // Join (and its Apply origin) build outputColumns as
      // `left.outputColumns ++ mark`, so the mark is the last output
      // column. Emit reads it back via JoinEdge::markColumn to thread the
      // mark through reordering. The filtering forms (kLeftSemiFilter /
      // kAnti) carry no mark.
      ColumnCP markColumn = nullptr;
      if (leaves.joinType == velox::core::JoinType::kLeftSemiProject) {
        VELOX_CHECK(
            !join->outputColumns().empty(),
            "kLeftSemiProject join must output its mark column");
        markColumn = join->outputColumns().back();
      }

      graph.addEdge(
          JoinEdge{
              leftSet,
              rightSet,
              edgeLeftKeys,
              edgeRightKeys,
              join->filter(),
              leaves.joinType,
              join->nullAware(),
              join->nullAsValue(),
              markColumn},
          tes);
    }
  }

  addTransitiveInnerEdges(graph, cluster, columnToLeaf);

  return graph;
}

} // namespace facebook::axiom::optimizer::v2
