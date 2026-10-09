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
// contain dependent Unnest relation ids. They split the completed TES into
// hyperedge sides and decide which Unnests must run below the join.
// Right-form joins are stored in their equivalent left form.
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

// Returns the leaf relations and all hypergraph relations below `node`,
// records the normalized operands of every Join reached during the walk in
// `inputs`, and records in `unnestSubtrees` what each Unnest's input subtree
// contributes. That set excludes the Unnest's own relation id, so testing a
// join's relations against it asks whether the join lies below the Unnest.
// Every Unnest of the cluster gets an entry, an Unnest of a constant an empty
// one: the cluster holds no part of its input.
SubtreeRelations populateJoinInputs(
    NodeCP node,
    const folly::F14FastMap<NodeCP, int8_t>& leafIds,
    const folly::F14FastMap<UnnestCP, int8_t>& unnestIds,
    folly::F14FastMap<JoinCP, JoinInputs>& inputs,
    folly::F14FastMap<int8_t, RelationSet>& unnestSubtrees) {
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
      unnestSubtrees[unnestId] = RelationSet{};
      const RelationSet relation = RelationSet::singleton(unnestId);
      return {relation, relation};
    }
    SubtreeRelations subtree =
        populateJoinInputs(input, leafIds, unnestIds, inputs, unnestSubtrees);
    unnestSubtrees[unnestId] = subtree.allRelations;
    subtree.allRelations.add(unnestId);
    return subtree;
  }

  if (node->is(NodeType::kFilter)) {
    // A Filter the cluster descended through contributes no relation of its
    // own; its predicates became graph conjuncts.
    return populateJoinInputs(
        node->as<Filter>()->input(),
        leafIds,
        unnestIds,
        inputs,
        unnestSubtrees);
  }

  const auto* join = node->as<Join>();
  const SubtreeRelations left = populateJoinInputs(
      join->left(), leafIds, unnestIds, inputs, unnestSubtrees);
  const SubtreeRelations right = populateJoinInputs(
      join->right(), leafIds, unnestIds, inputs, unnestSubtrees);

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

  if (node->is(NodeType::kFilter)) {
    // A Filter contributes no join to reshape over; its predicates are graph
    // conjuncts, eligible by their own relation sets.
    return tesExpansion(
        node->as<Filter>()->input(),
        parentType,
        parentSes,
        leftSide,
        leafIds,
        inputs);
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
            /*nullAsValue=*/false,
            /*markColumn=*/nullptr,
            /*outputColumns=*/{},
            /*sourceColumns=*/{}});
  }
}

// The TES of an edge: its SES plus what each side's shape forbids reordering
// past. 'left' and 'right' are the join's operands in normalized order.
RelationSet expandTes(
    const RelationSet& ses,
    NodeCP left,
    NodeCP right,
    velox::core::JoinType joinType,
    const folly::F14FastMap<NodeCP, int8_t>& leafIds,
    const folly::F14FastMap<JoinCP, JoinInputs>& joinInputs) {
  RelationSet tes{ses};
  tes.unionSet(tesExpansion(
      left, joinType, ses, /*leftSide=*/true, leafIds, joinInputs));
  tes.unionSet(tesExpansion(
      right, joinType, ses, /*leftSide=*/false, leafIds, joinInputs));
  return tes;
}

// Admits an Unnest relation per Unnest in 'cluster', in post-order so an input
// Unnest receives a lower id than one depending on it, and extends
// 'columnToLeaf' so each Unnest's produced columns resolve to its relation id.
folly::F14FastMap<UnnestCP, int8_t> addUnnestRelations(
    const JoinCluster& cluster,
    EstimateProvider& estimateProvider,
    JoinHypergraph& graph,
    folly::F14FastMap<ColumnCP, int8_t>& columnToLeaf) {
  folly::F14FastMap<UnnestCP, int8_t> unnestIds;
  for (UnnestCP unnest : cluster.unnests) {
    PlanObjectSet producedColumns;
    for (const auto& columnsForExpr : unnest->unnestColumns()) {
      producedColumns.unionObjects(columnsForExpr);
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
  return unnestIds;
}

// Adds an edge per Unnest, from the relations its input's columns read to the
// Unnest itself, and returns those input relations by Unnest id: the Unnest's
// data dependencies, which decide whether an outer join must precede or
// follow it.
folly::F14FastMap<int8_t, RelationSet> addUnnestEdges(
    const JoinCluster& cluster,
    const folly::F14FastMap<UnnestCP, int8_t>& unnestIds,
    const folly::F14FastMap<ColumnCP, int8_t>& columnToLeaf,
    JoinHypergraph& graph) {
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
  return unnestInputRelations;
}

// Adds to 'tes' the Unnests this non-inner join must not be reordered past.
//
// An Unnest reading an array from the join's null-padded side has to run
// before the join. After it, the Unnest reads the padded row's NULLs. A NULL
// array unnests to no rows and drops the row the outer join preserved. An
// array built from NULLs, such as one holding `e` and `e + 1`, repeats the
// row. Adding the Unnest's relation id to the TES is what pins that order for
// DPhyp.
//
// An Unnest in a semi or anti join's existence side also has to run first.
// Below the join its rows collapse to presence or absence; above the join they
// would instead duplicate or drop rows from the preserved side.
//
// A counting semi join implements multiset intersection, so an Unnest on either
// side must run first: its row count is part of the join result.
//
// A join contained in the Unnest's input subtree is one the query unnests the
// result of, so its own order is join-then-unnest and the barrier, which
// demands the reverse, does not apply to it. Containment is structural: a
// projection can drop the columns of a relation the join reads, which is why
// the Unnest's data dependencies do not answer this.
void addUnnestBarriers(
    const JoinInputs& inputs,
    const folly::F14FastMap<int8_t, RelationSet>& unnestInputRelations,
    const folly::F14FastMap<int8_t, RelationSet>& unnestSubtrees,
    RelationSet& tes) {
  RelationSet nullPaddedSide;
  bool rightSideDeterminesExistence{false};
  bool bothSidesDetermineMultiplicity{false};
  if (inputs.joinType == velox::core::JoinType::kLeft) {
    nullPaddedSide = inputs.rightRelations;
  } else if (inputs.joinType == velox::core::JoinType::kFull) {
    nullPaddedSide = inputs.leftRelations;
    nullPaddedSide.unionSet(inputs.rightRelations);
  } else if (
      inputs.joinType == velox::core::JoinType::kLeftSemiFilter ||
      inputs.joinType == velox::core::JoinType::kLeftSemiProject ||
      inputs.joinType == velox::core::JoinType::kAnti) {
    rightSideDeterminesExistence = true;
  } else if (
      inputs.joinType == velox::core::JoinType::kCountingLeftSemiFilter) {
    bothSidesDetermineMultiplicity = true;
  } else {
    return;
  }

  RelationSet joinRelations{inputs.leftRelations};
  joinRelations.unionSet(inputs.rightRelations);

  for (const auto& [unnestId, inputRelations] : unnestInputRelations) {
    if (joinRelations.isSubset(unnestSubtrees.at(unnestId))) {
      continue;
    }
    const bool readsNullPaddedSide =
        nullPaddedSide.hasIntersection(inputRelations);
    const bool determinesExistence = rightSideDeterminesExistence &&
        inputs.rightRelations.contains(unnestId);
    const bool determinesMultiplicity = bothSidesDetermineMultiplicity &&
        (inputs.leftRelations.contains(unnestId) ||
         inputs.rightRelations.contains(unnestId));
    if (!readsNullPaddedSide && !determinesExistence &&
        !determinesMultiplicity) {
      continue;
    }
    VELOX_CHECK(
        inputs.leftRelations.contains(unnestId) ||
            inputs.rightRelations.contains(unnestId),
        "Unnest must belong to one normalized join side: {}",
        unnestId);
    tes.add(unnestId);
  }
}

// The graph under construction and the maps edge construction resolves
// against: which leaf a column comes from, which relation a node is, and what
// each join's inputs contribute.
struct EdgeBuilder {
  JoinHypergraph& graph;
  const folly::F14FastMap<ColumnCP, int8_t>& columnToLeaf;
  const folly::F14FastMap<NodeCP, int8_t>& leafIds;
  const folly::F14FastMap<JoinCP, JoinInputs>& joinInputs;
  const folly::F14FastMap<int8_t, RelationSet>& unnestInputRelations;
  const folly::F14FastMap<int8_t, RelationSet>& unnestSubtrees;

  // Adds the edges of an inner join, and its filter as graph conjuncts.
  void addInner(JoinCP join, const JoinInputs& inputs) {
    // Group keys by endpoint relation-pair and emit one edge per group.
    // Keys sharing an endpoint (a composite key) stay on one edge so
    // their combined selectivity is preserved; keys with different
    // endpoints become separate edges, so two relations are not forced
    // co-located by a single hyperedge.
    std::map<std::pair<uint64_t, uint64_t>, std::vector<size_t>> keysByEndpoint;
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
      const RelationSet tes = expandTes(
          ses,
          join->left(),
          join->right(),
          join->joinType(),
          leafIds,
          joinInputs);
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
              /*nullAsValue=*/false,
              /*markColumn=*/nullptr,
              /*outputColumns=*/{},
              /*sourceColumns=*/{}});
    }

    for (ExprCP conjunct : join->filter()) {
      graph.addFilterConjunct(
          {conjunct, expressionRelations(conjunct, columnToLeaf)});
    }
  }

  // Adds the single edge of a non-inner join, normalized to left form.
  void addOuter(JoinCP join, const JoinInputs& inputs) {
    VELOX_CHECK(
        inputs.joinType == velox::core::JoinType::kLeft ||
            inputs.joinType == velox::core::JoinType::kFull ||
            inputs.joinType == velox::core::JoinType::kLeftSemiFilter ||
            inputs.joinType == velox::core::JoinType::kLeftSemiProject ||
            inputs.joinType == velox::core::JoinType::kAnti ||
            inputs.joinType == velox::core::JoinType::kCountingLeftSemiFilter,
        "Unsupported join type in join cluster: {}",
        join->joinTypeName());

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
    const RelationSet rightSet{
        keyRelationsOrOperand(edgeRightKeys, columnToLeaf, inputs.rightLeaves)};
    RelationSet ses{leftSet};
    ses.unionSet(rightSet);
    for (ExprCP conjunct : join->filter()) {
      ses.unionSet(expressionRelations(conjunct, columnToLeaf));
    }
    RelationSet tes = expandTes(
        ses,
        normalizedLeftChild,
        normalizedRightChild,
        inputs.joinType,
        leafIds,
        joinInputs);

    addUnnestBarriers(inputs, unnestInputRelations, unnestSubtrees, tes);

    // `populateJoinInputs` already applies the same right-form operand swap
    // as the normalized child selection above, so these are in normalized
    // order.
    auto [leftTes, rightTes] =
        splitTes(tes, inputs.leftRelations, inputs.rightRelations);

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
            markColumn,
            join->outputColumns(),
            join->sourceColumns()});
  }
};

// Adds one relation per cluster leaf, keyed so that both the original and the
// rewritten pointer resolve to it, and records which leaf each column comes
// from.
void addLeafRelations(
    const JoinCluster& cluster,
    const std::vector<NodeCP>& rewrittenLeaves,
    EstimateProvider& estimateProvider,
    JoinHypergraph& graph,
    folly::F14FastMap<ColumnCP, int8_t>& columnToLeaf,
    folly::F14FastMap<NodeCP, int8_t>& leafIds) {
  for (size_t i = 0; i < cluster.leaves.size(); ++i) {
    NodeCP original = cluster.leaves[i];
    NodeCP rewritten = rewrittenLeaves[i];
    PlanObjectSet columns =
        PlanObjectSet::fromObjects(rewritten->outputColumns());
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
}

// Resolves a fresh join output to the relation supplying its value, so an
// Unnest whose input reads the output depends on that relation. Runs after the
// Unnest columns are registered, so an output padded from one resolves too.
void addJoinOutputRelations(
    const JoinCluster& cluster,
    folly::F14FastMap<ColumnCP, int8_t>& columnToLeaf) {
  bool changed;
  do {
    changed = false;
    for (JoinCP join : cluster.joins) {
      for (size_t i = 0; i < join->outputColumns().size(); ++i) {
        const auto source = columnToLeaf.find(join->sourceColumns()[i]);
        if (source != columnToLeaf.end()) {
          const int8_t sourceRelation = source->second;
          changed |=
              columnToLeaf.emplace(join->outputColumns()[i], sourceRelation)
                  .second;
        }
      }
    }
  } while (changed);
}

// Adds the cluster's Filter predicates as graph conjuncts. A fresh output of
// an outer join exists once that join has run. Its source can itself be a
// fresh output of a join below, so a predicate that reads one takes the
// eligibility of every join on that chain and the relation the chain starts
// from.
void addClusterConjuncts(
    const ExprVector& predicates,
    const folly::F14FastMap<ColumnCP, int8_t>& columnToLeaf,
    JoinHypergraph& graph) {
  struct FreshOutput {
    RelationSet eligibility;
    ColumnCP source;
  };
  folly::F14FastMap<ColumnCP, FreshOutput> freshOutputs;
  for (const auto& edge : graph.edges()) {
    for (size_t i = 0; i < edge.outputColumns().size(); ++i) {
      if (edge.outputColumns()[i] != edge.sourceColumns()[i]) {
        freshOutputs.emplace(
            edge.outputColumns()[i],
            FreshOutput{edge.totalEligibility(), edge.sourceColumns()[i]});
      }
    }
  }
  for (ExprCP predicate : predicates) {
    RelationSet relations;
    predicate->columns().forEach<Column>([&](ColumnCP column) {
      ColumnCP source = column;
      for (auto it = freshOutputs.find(source); it != freshOutputs.end();
           it = freshOutputs.find(source)) {
        relations.unionSet(it->second.eligibility);
        source = it->second.source;
      }
      relations.unionSet(expressionRelations(source, columnToLeaf));
    });
    graph.addFilterConjunct({predicate, relations});
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
  addLeafRelations(
      cluster, rewrittenLeaves, estimateProvider, graph, columnToLeaf, leafIds);
  folly::F14FastMap<UnnestCP, int8_t> unnestIds =
      addUnnestRelations(cluster, estimateProvider, graph, columnToLeaf);
  addJoinOutputRelations(cluster, columnToLeaf);

  folly::F14FastMap<int8_t, RelationSet> unnestInputRelations =
      addUnnestEdges(cluster, unnestIds, columnToLeaf, graph);

  folly::F14FastMap<JoinCP, JoinInputs> joinInputs;
  folly::F14FastMap<int8_t, RelationSet> unnestSubtrees;
  populateJoinInputs(
      cluster.root, leafIds, unnestIds, joinInputs, unnestSubtrees);

  EdgeBuilder edges{
      graph,
      columnToLeaf,
      leafIds,
      joinInputs,
      unnestInputRelations,
      unnestSubtrees};
  for (JoinCP join : cluster.joins) {
    const auto& inputs = joinInputs.at(join);
    if (join->isInner()) {
      edges.addInner(join, inputs);
    } else {
      edges.addOuter(join, inputs);
    }
  }

  addTransitiveInnerEdges(graph, columnToLeaf);

  addClusterConjuncts(cluster.filterPredicates, columnToLeaf, graph);

  return graph;
}

} // namespace facebook::axiom::optimizer::v2
