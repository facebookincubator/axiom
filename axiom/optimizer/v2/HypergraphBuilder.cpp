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

// The relations an edge's side reads: those its 'keys' name, or 'operand' --
// the whole side -- when a key names none, as the constant side of
// `1 IN (SELECT ...)` does. The operand is a superset of what any key of that
// side can read, and a larger edge side only forbids reorderings, so
// substituting it cannot admit a wrong one. It does cost plan space: the join
// is then pinned above everything its operand covers.
RelationSet keyRelationsOrOperand(
    const ExprVector& keys,
    const folly::F14FastMap<ColumnCP, int8_t>& columnToLeaf,
    const RelationSet& operand) {
  RelationSet fromKeys = unionExpressionRelations(keys, columnToLeaf);
  if (!fromKeys.empty()) {
    return fromKeys;
  }
  VELOX_CHECK(
      !operand.empty(),
      "A join edge side must read some relation, but neither its keys nor its "
      "operand do");
  return operand;
}

// Records each Join's normalized operands. `leftLeaves` and `rightLeaves`
// contain leaf-relation ids used by the algebraic TES rules; a constant-input
// Unnest is itself a leaf. `leftRelations` and `rightRelations` additionally
// contain dependent Unnest relation ids used to split the completed TES into
// hyperedge sides. Right-form joins are stored in their equivalent left form.
struct JoinInputs {
  RelationSet leftLeaves;
  RelationSet rightLeaves;
  RelationSet leftRelations;
  RelationSet rightRelations;
  velox::core::JoinType joinType{velox::core::JoinType::kInner};
};

// Tracks leaf relations and complete hypergraph-relation coverage for a
// subtree.
struct SubtreeRelations {
  RelationSet leaves;
  RelationSet allRelations;
};

// Returns the leaf relations and all hypergraph relations below `node`, and
// records the normalized operands of every Join reached during the walk.
SubtreeRelations populateJoinInputs(
    NodeCP node,
    const folly::F14FastMap<NodeCP, int8_t>& leafIds,
    const folly::F14FastMap<UnnestCP, int8_t>& unnestIds,
    folly::F14FastMap<JoinCP, JoinInputs>& inputs) {
  const auto it = leafIds.find(node);
  if (it != leafIds.end()) {
    const RelationSet relation = RelationSet::singleton(it->second);
    return {relation, relation};
  }
  if (node->is(NodeType::kUnnest)) {
    const auto* unnest = node->as<Unnest>();
    NodeCP input = unnest->input();
    const int8_t unnestId = unnestIds.at(unnest);
    // The cluster does not contain the input of an Unnest of a constant, so
    // the Unnest's own relation is all its subtree contributes.
    if (input->outputColumns().empty()) {
      const RelationSet relation = RelationSet::singleton(unnestId);
      return {relation, relation};
    }
    SubtreeRelations subtree =
        populateJoinInputs(input, leafIds, unnestIds, inputs);
    subtree.allRelations.add(unnestId);
    return subtree;
  }
  const auto* join = node->as<Join>();
  const SubtreeRelations left =
      populateJoinInputs(join->left(), leafIds, unnestIds, inputs);
  const SubtreeRelations right =
      populateJoinInputs(join->right(), leafIds, unnestIds, inputs);
  // Normalize right-form joins to their left form by swapping operands.
  // kRight, kRightSemiFilter and kRightSemiProject each map to the
  // matching left-form type; the rest of the pipeline never sees a
  // right-form join.
  switch (join->joinType()) {
    case velox::core::JoinType::kRight:
      inputs[join] = JoinInputs{
          .leftLeaves = right.leaves,
          .rightLeaves = left.leaves,
          .leftRelations = right.allRelations,
          .rightRelations = left.allRelations,
          .joinType = velox::core::JoinType::kLeft};
      break;
    case velox::core::JoinType::kRightSemiFilter:
      inputs[join] = JoinInputs{
          .leftLeaves = right.leaves,
          .rightLeaves = left.leaves,
          .leftRelations = right.allRelations,
          .rightRelations = left.allRelations,
          .joinType = velox::core::JoinType::kLeftSemiFilter};
      break;
    case velox::core::JoinType::kRightSemiProject:
      inputs[join] = JoinInputs{
          .leftLeaves = right.leaves,
          .rightLeaves = left.leaves,
          .leftRelations = right.allRelations,
          .rightRelations = left.allRelations,
          .joinType = velox::core::JoinType::kLeftSemiProject};
      break;
    default:
      inputs[join] = JoinInputs{
          .leftLeaves = left.leaves,
          .rightLeaves = right.leaves,
          .leftRelations = left.allRelations,
          .rightRelations = right.allRelations,
          .joinType = join->joinType()};
  }
  SubtreeRelations subtree{left};
  subtree.leaves.unionSet(right.leaves);
  subtree.allRelations.unionSet(right.allRelations);
  return subtree;
}

// Splits an operator's total TES between its normalized input covers.
std::pair<RelationSet, RelationSet> splitTes(
    const RelationSet& tes,
    const RelationSet& normalizedLeftRelations,
    const RelationSet& normalizedRightRelations) {
  VELOX_CHECK(
      !normalizedLeftRelations.hasIntersection(normalizedRightRelations),
      "Normalized join inputs must be disjoint: left={}, right={}",
      normalizedLeftRelations.bits(),
      normalizedRightRelations.bits());
  RelationSet inputRelations{normalizedLeftRelations};
  inputRelations.unionSet(normalizedRightRelations);
  VELOX_CHECK(
      tes.isSubset(inputRelations),
      "TES must be contained in the normalized join inputs: tes={}, inputs={}",
      tes.bits(),
      inputRelations.bits());

  RelationSet leftTes{tes};
  leftTes.intersect(normalizedLeftRelations);
  RelationSet rightTes{tes};
  rightTes.intersect(normalizedRightRelations);
  return {leftTes, rightTes};
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
    const folly::F14FastMap<JoinCP, JoinInputs>& inputs) {
  if (leafIds.contains(node)) {
    return RelationSet{};
  }
  if (node->is(NodeType::kUnnest)) {
    // The cluster does not contain the input of an Unnest of a constant.
    NodeCP input = node->onlyInput();
    if (input->outputColumns().empty()) {
      return RelationSet{};
    }
    return tesExpansion(
        input, parentType, parentSes, leftSide, leafIds, inputs);
  }

  const auto* childJoin = node->as<Join>();
  const auto& childInputs = inputs.at(childJoin);

  // CD-A: for a LEFT-subtree descendant the M/N table is keyed
  // `(child, parent)`. For a RIGHT-subtree descendant the roles
  // flip to `(parent, child)`. `childInputs.joinType` is already
  // normalized (never kRight).
  auto props = leftSide
      ? AlgebraicProperties::derive(childInputs.joinType, parentType)
      : AlgebraicProperties::derive(parentType, childInputs.joinType);

  // When the parent's referenced relations overlap a child
  // operand that the equivalence's syntactic precondition
  // forbids, the equivalence breaks; downgrade that property.
  // Skip for inner-over-inner where commutativity makes the
  // equivalences hold regardless of predicate placement.
  const bool involvesOuter =
      childInputs.joinType != velox::core::JoinType::kInner ||
      parentType != velox::core::JoinType::kInner;
  if (involvesOuter) {
    if (leftSide) {
      if (parentSes.hasIntersection(childInputs.leftLeaves)) {
        props.associative = false;
      }
      if (parentSes.hasIntersection(childInputs.rightLeaves)) {
        props.leftAsscom = false;
      }
    } else {
      if (parentSes.hasIntersection(childInputs.rightLeaves)) {
        props.associative = false;
      }
      if (parentSes.hasIntersection(childInputs.leftLeaves)) {
        props.rightAsscom = false;
      }
    }
  }

  RelationSet expansion;
  if (leftSide) {
    if (!props.leftAsscom) {
      expansion.unionSet(childInputs.rightLeaves);
    }
    if (!props.associative) {
      expansion.unionSet(childInputs.leftLeaves);
    }
  } else {
    if (!props.rightAsscom) {
      expansion.unionSet(childInputs.leftLeaves);
    }
    if (!props.associative) {
      expansion.unionSet(childInputs.rightLeaves);
    }
  }
  expansion.unionSet(tesExpansion(
      childJoin->left(), parentType, parentSes, leftSide, leafIds, inputs));
  expansion.unionSet(tesExpansion(
      childJoin->right(), parentType, parentSes, leftSide, leafIds, inputs));
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
// One representative equality per class and relation pair completes the
// relation-level closure. Written predicates remain on their original edges.
void addTransitiveInnerEdges(
    JoinHypergraph& graph,
    const folly::F14FastMap<ColumnCP, int8_t>& columnToLeaf) {
  using RelationPair = std::pair<int8_t, int8_t>;
  folly::F14FastMap<EquivalenceP, folly::F14FastSet<RelationPair>>
      coveredRelationPairs;
  // Keep one representative per relation and preserve first-seen class order.
  // Derived edge indices participate in DPhyp's candidate tiebreak, so they
  // must not depend on pointer-keyed hash iteration.
  folly::F14FastMap<EquivalenceP, std::map<int8_t, ColumnCP>> classReps;
  std::vector<EquivalenceP> classOrder;

  // Record written coverage per equality class. An edge for another class on
  // the same relation pair does not cover this class.
  for (const auto& edge : graph.edges()) {
    if (edge.joinType() != velox::core::JoinType::kInner) {
      continue;
    }
    for (size_t keyIndex = 0; keyIndex < edge.leftKeys().size(); ++keyIndex) {
      const ExprCP leftKey = edge.leftKeys()[keyIndex];
      const ExprCP rightKey = edge.rightKeys()[keyIndex];
      if (leftKey == nullptr || rightKey == nullptr || !leftKey->isColumn() ||
          !rightKey->isColumn()) {
        continue;
      }

      const ColumnCP leftColumn = leftKey->as<Column>();
      const ColumnCP rightColumn = rightKey->as<Column>();
      const EquivalenceP equivalence = leftColumn->equivalence();
      VELOX_CHECK_NOT_NULL(
          equivalence,
          "Inner equi-join column has no equivalence class: {}",
          leftColumn->toString());
      VELOX_CHECK(
          rightColumn->equivalence() == equivalence,
          "Inner equi-join columns have different equivalence classes: {}, {}",
          leftColumn->toString(),
          rightColumn->toString());

      const auto leftRelation = columnToLeaf.find(leftColumn);
      const auto rightRelation = columnToLeaf.find(rightColumn);
      if (leftRelation == columnToLeaf.end() ||
          rightRelation == columnToLeaf.end()) {
        continue;
      }
      VELOX_DCHECK_NE(leftRelation->second, rightRelation->second);

      auto [classIt, inserted] = classReps.try_emplace(equivalence);
      if (inserted) {
        classOrder.push_back(equivalence);
      }
      classIt->second.try_emplace(leftRelation->second, leftColumn);
      classIt->second.try_emplace(rightRelation->second, rightColumn);
      coveredRelationPairs[equivalence].insert(
          orderedPair(leftRelation->second, rightRelation->second));
    }
  }

  struct DerivedEdgeSpec {
    int8_t leftRelation;
    int8_t rightRelation;
    ExprVector leftKeys;
    ExprVector rightKeys;
  };
  std::vector<DerivedEdgeSpec> derivedEdges;
  folly::F14FastMap<RelationPair, size_t> derivedEdgeByRelations;

  for (EquivalenceP equivalence : classOrder) {
    const auto& reps = classReps.at(equivalence);
    const auto& coveredPairs = coveredRelationPairs.at(equivalence);
    for (auto left = reps.begin(); left != reps.end(); ++left) {
      auto right = left;
      for (++right; right != reps.end(); ++right) {
        const RelationPair relationPair =
            orderedPair(left->first, right->first);
        if (coveredPairs.contains(relationPair)) {
          continue;
        }
        auto [it, inserted] = derivedEdgeByRelations.try_emplace(
            relationPair, derivedEdges.size());
        if (inserted) {
          derivedEdges.push_back(
              DerivedEdgeSpec{
                  .leftRelation = relationPair.first,
                  .rightRelation = relationPair.second,
              });
        }
        DerivedEdgeSpec& derivedEdge = derivedEdges.at(it->second);
        derivedEdge.leftKeys.push_back(left->second);
        derivedEdge.rightKeys.push_back(right->second);
      }
    }
  }

  for (DerivedEdgeSpec& derivedEdge : derivedEdges) {
    const RelationSet left = RelationSet::singleton(derivedEdge.leftRelation);
    const RelationSet right = RelationSet::singleton(derivedEdge.rightRelation);
    graph.addEdge(
        JoinEdge{
            left,
            right,
            left,
            right,
            std::move(derivedEdge.leftKeys),
            std::move(derivedEdge.rightKeys),
            ExprVector{},
            velox::core::JoinType::kInner,
            /*nullAware=*/false,
            /*nullAsValue=*/false});
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

  // Admit Unnest relations in post-order so input Unnests receive lower ids
  // than the Unnests that depend on them. Extend the column-resolution map so
  // each Unnest's produced columns resolve to its relation id.
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

  // Builds each Unnest edge and records the relation ids referenced by the
  // Unnest input's output columns. These data dependencies determine whether
  // an outer join must precede or follow the Unnest; they are distinct from the
  // complete structural subtree membership recorded in `joinInputs` below.
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
    unnestInputRelations.emplace(id, inputRelations);

    // Nothing of the cluster precedes an Unnest of a constant, so it has no
    // edge; the cluster's own joins connect it.
    if (unnest->input()->outputColumns().empty()) {
      continue;
    }

    VELOX_CHECK(
        !inputRelations.empty(),
        "Unnest input subtree must contribute at least one cluster relation");

    graph.addEdge(JoinEdge::unnest(inputRelations, RelationSet::singleton(id)));
  }

  folly::F14FastMap<JoinCP, JoinInputs> joinInputs;
  populateJoinInputs(cluster.root, leafIds, unnestIds, joinInputs);

  for (JoinCP join : cluster.joins) {
    const auto& inputs = joinInputs.at(join);
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
            keyRelationsOrOperand(leftKeys, columnToLeaf, inputs.leftLeaves)};
        const RelationSet rightSet{
            keyRelationsOrOperand(rightKeys, columnToLeaf, inputs.rightLeaves)};
        RelationSet ses{leftSet};
        ses.unionSet(rightSet);
        RelationSet tes{ses};
        tes.unionSet(tesExpansion(
            join->left(),
            join->joinType(),
            ses,
            /*leftSide=*/true,
            leafIds,
            joinInputs));
        tes.unionSet(tesExpansion(
            join->right(),
            join->joinType(),
            ses,
            /*leftSide=*/false,
            leafIds,
            joinInputs));
        auto [leftTes, rightTes] =
            splitTes(tes, inputs.leftRelations, inputs.rightRelations);

        graph.addEdge(
            JoinEdge{
                leftSet,
                rightSet,
                leftTes,
                rightTes,
                leftKeys,
                rightKeys,
                ExprVector{},
                join->joinType(),
                /*nullAware=*/false,
                /*nullAsValue=*/false});
      }

      for (ExprCP conjunct : join->filter()) {
        graph.addFilterConjunct(
            {conjunct, expressionRelations(conjunct, columnToLeaf)});
      }
    } else {
      // Swap keys when the IR join is a right-form join so the edge's
      // leftKeys reference the normalized left input.
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
          keyRelationsOrOperand(edgeLeftKeys, columnToLeaf, inputs.leftLeaves)};
      const RelationSet rightSet{keyRelationsOrOperand(
          edgeRightKeys, columnToLeaf, inputs.rightLeaves)};
      RelationSet ses{leftSet};
      ses.unionSet(rightSet);
      for (ExprCP conjunct : join->filter()) {
        ses.unionSet(expressionRelations(conjunct, columnToLeaf));
      }
      RelationSet tes{ses};
      tes.unionSet(tesExpansion(
          normalizedLeftChild,
          inputs.joinType,
          ses,
          /*leftSide=*/true,
          leafIds,
          joinInputs));
      tes.unionSet(tesExpansion(
          normalizedRightChild,
          inputs.joinType,
          ses,
          /*leftSide=*/false,
          leafIds,
          joinInputs));

      // `populateJoinInputs` already applies the same right-form operand swap
      // as the normalized child selection above.
      const RelationSet& normalizedLeftRelations = inputs.leftRelations;
      const RelationSet& normalizedRightRelations = inputs.rightRelations;

      // Outer-join barrier: if an Unnest's input subtree contributes a relation
      // on this outer join's null-padded side, the unnest edge must fire before
      // the outer join. Otherwise null-padding flows into UNNEST and rows drop.
      // Expand TES to include the Unnest relation id so DPhyp pins the order.
      RelationSet joinRelations{inputs.leftLeaves};
      joinRelations.unionSet(inputs.rightLeaves);
      for (const auto& [unnestId, inputRelations] : unnestInputRelations) {
        // An outer join inside the Unnest's input subtree is already ordered
        // before the Unnest by the unnest edge. The barrier would demand the
        // reverse order, which no plan can satisfy when this join is the only
        // edge co-locating the input relations.
        if (joinRelations.isSubset(inputRelations)) {
          continue;
        }
        RelationSet nullPaddedSide;
        if (inputs.joinType == velox::core::JoinType::kLeft) {
          nullPaddedSide = inputs.rightLeaves;
        } else if (inputs.joinType == velox::core::JoinType::kFull) {
          nullPaddedSide = inputs.leftLeaves;
          nullPaddedSide.unionSet(inputs.rightLeaves);
        }
        if (nullPaddedSide.hasIntersection(inputRelations)) {
          VELOX_CHECK(
              normalizedLeftRelations.contains(unnestId) ||
                  normalizedRightRelations.contains(unnestId),
              "Unnest must belong to one normalized join side: {}",
              static_cast<int32_t>(unnestId));
          tes.add(unnestId);
        }
      }

      auto [leftTes, rightTes] =
          splitTes(tes, normalizedLeftRelations, normalizedRightRelations);

      // kLeftSemiProject preserves the left side and appends a mark; the
      // Join (and its Apply origin) build outputColumns as
      // `left.outputColumns ++ mark`, so the mark is the last output
      // column. Emit reads it back via JoinEdge::markColumn to thread the
      // mark through reordering. The filtering forms (kLeftSemiFilter /
      // kAnti) carry no mark.
      ColumnCP markColumn =
          inputs.joinType == velox::core::JoinType::kLeftSemiProject
          ? join->markColumn()
          : nullptr;

      graph.addEdge(
          JoinEdge{
              leftSet,
              rightSet,
              leftTes,
              rightTes,
              edgeLeftKeys,
              edgeRightKeys,
              join->filter(),
              inputs.joinType,
              join->nullAware(),
              join->nullAsValue(),
              markColumn});
    }
  }

  addTransitiveInnerEdges(graph, columnToLeaf);

  return graph;
}

} // namespace facebook::axiom::optimizer::v2
