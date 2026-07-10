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

#include "axiom/optimizer/v2/JoinTreeEmitter.h"

#include <algorithm>
#include <numeric>
#include <optional>

#include "axiom/optimizer/EstimateMath.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// An intermediate join emits the columns a consumer above `cover` demands —
// `graph.coverOutputColumns(cover)`, which collapses each within-cover
// equi-group to one representative — exactly the set the cost model charges
// for, so the executed plan matches its estimated width. Left-then-right order
// is preserved. A column produced by no relation in `cover` (e.g. a semijoin
// mark synthesized below) is outside the demand universe and is always kept.
ColumnVector coverNarrowedColumns(
    const JoinHypergraph& graph,
    const RelationSet& cover,
    NodeCP left,
    NodeCP right) {
  const PlanObjectSet needed = graph.coverOutputColumns(cover);
  const PlanObjectSet coverColumns = graph.coverColumns(cover);
  ColumnVector columns;
  for (NodeCP side : {left, right}) {
    for (ColumnCP column : side->outputColumns()) {
      if (!coverColumns.contains(column) || needed.contains(column)) {
        columns.push_back(column);
      }
    }
  }
  return columns;
}

// Returns each not-yet-placed conjunct whose required relations are
// a subset of `cover`. Marks the returned conjuncts in `fired`.
ExprVector takeReadyConjuncts(
    const JoinHypergraph& graph,
    RelationSet cover,
    std::vector<bool>& fired) {
  ExprVector ready;
  const auto& conjuncts = graph.filterConjuncts();
  for (size_t i = 0; i < conjuncts.size(); ++i) {
    if (fired[i]) {
      continue;
    }
    if (conjuncts[i].relations.isSubset(cover)) {
      ready.push_back(conjuncts[i].expr);
      fired[i] = true;
    }
  }
  return ready;
}

struct EmitState {
  const JoinHypergraph& graph;
  Builder& builder;
  ExprFactory exprs;
  std::vector<bool> fired;
};

NodeCP emitOp(MemoOpCP op, EmitState& state);

// A join's children each emit one representative per equivalence group, so a
// column equated and collapsed in a child is gone from that child's output.
// `mergedChildReps` maps every such column to the surviving representative
// across both child covers; `remapToReps` rewrites key/filter expressions to
// reference the survivor, keeping the emitted node's column references valid.
folly::F14FastMap<ColumnCP, ColumnCP> mergedChildReps(
    const JoinOp* join,
    EmitState& state) {
  auto reps = state.graph.coverColumnReps(join->left->cover());
  auto right = state.graph.coverColumnReps(join->right->cover());
  reps.insert(right.begin(), right.end());
  return reps;
}

ExprVector remapToReps(
    const ExprVector& exprs,
    const folly::F14FastMap<ColumnCP, ColumnCP>& reps,
    EmitState& state) {
  ColumnVector sources;
  ExprVector targets;
  for (const auto& [column, rep] : reps) {
    if (rep != column) {
      sources.push_back(column);
      targets.push_back(rep);
    }
  }
  if (sources.empty()) {
    return exprs;
  }
  return state.exprs.substitute(exprs, sources, targets);
}

NodeCP emitLeaf(const LeafOp* leaf, EmitState& state) {
  return state.graph.relation(leaf->relationId).node();
}

// Builds a new `Unnest` IR node from a JoinOp wrapping a directed
// cross-join-unnest edge. DPhyp's orientation enforcement guarantees
// `join->left` is the input-side plan and `join->right` is the
// singleton Unnest leaf. `unnestExpressions`, `unnestColumns`, and
// `ordinalityColumn` come from the original Unnest IR node stored on
// the Unnest relation. `replicatedColumns` conservatively forwards
// every input column.
NodeCP buildUnnest(const JoinOp* join, NodeCP leftNode, EmitState& state) {
  const auto& edge = state.graph.edges()[join->edgeIndex];
  const int8_t unnestRelId = edge.right().min();
  const auto* origUnnest =
      state.graph.relation(unnestRelId).node()->as<Unnest>();

  ColumnVector replicatedColumns{leftNode->outputColumns()};

  ColumnVector outputColumns{replicatedColumns};
  for (const auto& perExpr : origUnnest->unnestColumns()) {
    for (const auto* column : perExpr) {
      outputColumns.push_back(column);
    }
  }
  if (origUnnest->ordinalityColumn() != nullptr) {
    outputColumns.push_back(origUnnest->ordinalityColumn());
  }

  // A column collapsed in the input is gone from `leftNode`'s output; point the
  // unnest arguments at its surviving representative.
  ExprVector unnestExpressions = remapToReps(
      origUnnest->unnestExpressions(),
      state.graph.coverColumnReps(join->left->cover()),
      state);

  return state.builder.make<Unnest>(Unnest::Key{
      leftNode,
      std::move(unnestExpressions),
      std::move(replicatedColumns),
      origUnnest->unnestColumns(),
      origUnnest->ordinalityColumn(),
      std::move(outputColumns)});
}

// Narrows `outputColumns` for semi/anti joins, which emit only the semi'd
// side's columns (plus the mark for the project forms); inner/outer emit both
// sides. Keys off the orientation-resolved `joinType`, so a left-form and its
// build-side-flipped right form (operands swapped) yield identical output
// columns. Returns std::nullopt for inner/outer (caller's columns are correct).
std::optional<ColumnVector> narrowSemiAntiOutput(
    velox::core::JoinType joinType,
    NodeCP leftNode,
    NodeCP rightNode,
    ColumnCP markColumn) {
  using velox::core::JoinType;
  switch (joinType) {
    case JoinType::kLeftSemiFilter:
    case JoinType::kAnti:
      // Semi'd side is the probe (left) input.
      return ColumnVector{leftNode->outputColumns()};
    case JoinType::kRightSemiFilter:
      // Flipped semijoin: semi'd side is the build (right) input.
      return ColumnVector{rightNode->outputColumns()};
    case JoinType::kLeftSemiProject: {
      ColumnVector columns{leftNode->outputColumns()};
      if (markColumn != nullptr) {
        columns.push_back(markColumn);
      }
      return columns;
    }
    case JoinType::kRightSemiProject: {
      ColumnVector columns{rightNode->outputColumns()};
      if (markColumn != nullptr) {
        columns.push_back(markColumn);
      }
      return columns;
    }
    default:
      return std::nullopt;
  }
}

NodeCP buildJoin(
    const JoinOp* join,
    NodeCP leftNode,
    NodeCP rightNode,
    ColumnVector outputColumns,
    EmitState& state) {
  const auto& edge = state.graph.edges()[join->edgeIndex];
  const bool isInner = edge.joinType() == velox::core::JoinType::kInner;

  if (auto narrowed = narrowSemiAntiOutput(
          join->joinType, leftNode, rightNode, edge.markColumn())) {
    outputColumns = std::move(*narrowed);
  }

  // If DPhyp chose the swapped orientation, edge.left() covers
  // join->right; swap keys.
  ExprVector leftKeys{edge.leftKeys()};
  ExprVector rightKeys{edge.rightKeys()};
  if (edge.left().isSubset(join->right->cover())) {
    std::swap(leftKeys, rightKeys);
  }

  // Conjoin the keys of any additional inner edges this join applies (a
  // cyclic join graph can have several edges crossing one partition). Each
  // is orientation-corrected independently. Inner edges carry no filter
  // (their non-equi conjuncts live in the hypergraph's filter pool).
  for (size_t extraIndex : join->extraEdges) {
    const auto& extra = state.graph.edges()[extraIndex];
    ExprVector extraLeftKeys{extra.leftKeys()};
    ExprVector extraRightKeys{extra.rightKeys()};
    if (extra.left().isSubset(join->right->cover())) {
      std::swap(extraLeftKeys, extraRightKeys);
    }
    appendAll(leftKeys, extraLeftKeys);
    appendAll(rightKeys, extraRightKeys);
  }

  ExprVector filter;
  if (isInner) {
    filter = takeReadyConjuncts(state.graph, join->cover(), state.fired);
  } else {
    filter = ExprVector{edge.filter()};
  }

  // A key/filter may reference a column a child collapsed into its
  // representative; rewrite it to the survivor present in the child's output.
  const auto reps = mergedChildReps(join, state);
  leftKeys = remapToReps(leftKeys, reps, state);
  rightKeys = remapToReps(rightKeys, reps, state);
  filter = remapToReps(filter, reps, state);

  return state.builder.make<Join>(Join::Key{
      leftNode,
      rightNode,
      join->joinType,
      std::move(leftKeys),
      std::move(rightKeys),
      std::move(filter),
      edge.nullAware(),
      edge.nullAsValue(),
      std::move(outputColumns)});
}

// Lowers an antijoin played in its reversed (build-on-the-preserved-side)
// orientation. There is no `kAnti` build-side flip in Velox, so synthesize
// it: a kRightSemiProject (probe = `probeNode`, the edge's right side; build
// = `buildNode`, the preserved left side) emits each build row plus a mark
// for a probe match; `Filter(not mark)` keeps the unmatched rows (the
// antijoin result); a Project drops the mark, restoring the antijoin schema.
// The mark is fresh and never escapes this subtree.
//
// TODO: Replace this synthesis (and the reversedAnti orientation marker)
// with a plain kRightAnti relabel once Velox adds that join type:
// https://github.com/facebookincubator/velox/issues/17815.
NodeCP buildReversedAnti(
    const JoinOp* join,
    NodeCP probeNode,
    NodeCP buildNode,
    EmitState& state) {
  const auto& edge = state.graph.edges()[join->edgeIndex];

  const ColumnVector antiOutput{buildNode->outputColumns()};
  ColumnCP mark = Column::createBoolean("mark");
  ColumnVector joinOutput{antiOutput};
  joinOutput.push_back(mark);

  // edge.leftKeys reference the preserved (build) side, rightKeys the probe
  // side. The IR Join's leftKeys must reference its left (probe) input. A key
  // referencing a column collapsed in a child is rewritten to the survivor.
  const auto reps = mergedChildReps(join, state);
  NodeCP rightSemiProject = state.builder.make<Join>(Join::Key{
      probeNode,
      buildNode,
      velox::core::JoinType::kRightSemiProject,
      remapToReps(ExprVector{edge.rightKeys()}, reps, state),
      remapToReps(ExprVector{edge.leftKeys()}, reps, state),
      remapToReps(ExprVector{edge.filter()}, reps, state),
      edge.nullAware(),
      edge.nullAsValue(),
      std::move(joinOutput)});

  ExprFactory exprFactory{state.builder};
  NodeCP filtered = state.builder.make<Filter>(
      Filter::Key{rightSemiProject, ExprVector{exprFactory.makeNot(mark)}});

  // Project away the mark, restoring the antijoin's output schema. Every
  // entry is a pass-through of the preserved-side column.
  ExprVector projectExprs;
  projectExprs.reserve(antiOutput.size());
  for (ColumnCP column : antiOutput) {
    projectExprs.push_back(column);
  }
  return state.builder.make<Project>(
      Project::Key{filtered, std::move(projectExprs), antiOutput});
}

NodeCP emitOp(MemoOpCP op, EmitState& state) {
  switch (op->kind()) {
    case MemoOpKind::kLeaf:
      return emitLeaf(op->as<LeafOp>(), state);
    case MemoOpKind::kJoin: {
      const auto* join = op->as<JoinOp>();
      const auto& edge = state.graph.edges()[join->edgeIndex];
      NodeCP leftNode = emitOp(join->left, state);
      if (edge.isUnnest()) {
        return buildUnnest(join, leftNode, state);
      }
      NodeCP rightNode = emitOp(join->right, state);
      if (join->reversedAnti) {
        return buildReversedAnti(join, leftNode, rightNode, state);
      }
      return buildJoin(
          join,
          leftNode,
          rightNode,
          coverNarrowedColumns(state.graph, join->cover(), leftNode, rightNode),
          state);
    }
    case MemoOpKind::kExchange: {
      const auto* exchange = op->as<ExchangeOp>();
      NodeCP inputNode = emitOp(exchange->input, state);
      return state.builder.make<Exchange>(
          Exchange::Key{inputNode, exchange->outputPartitioning()});
    }
  }
  VELOX_UNREACHABLE();
}

// Emits the cluster root. The root join carries exactly `rootOutputColumns`
// unless an equivalence collapse dropped a target column in favor of its
// representative; then the join emits representatives and a Project
// re-materializes each target name (and restores output order).
NodeCP buildRoot(
    const JoinOp* join,
    NodeCP leftNode,
    NodeCP rightNode,
    const ColumnVector& rootOutputColumns,
    EmitState& state) {
  const auto rootReps = state.graph.coverColumnReps(join->cover());
  bool collapsed = false;
  for (ColumnCP target : rootOutputColumns) {
    const auto it = rootReps.find(target);
    if (it != rootReps.end() && it->second != target) {
      collapsed = true;
      break;
    }
  }
  if (!collapsed) {
    return buildJoin(
        join, leftNode, rightNode, ColumnVector{rootOutputColumns}, state);
  }

  NodeCP joinNode = buildJoin(
      join,
      leftNode,
      rightNode,
      coverNarrowedColumns(state.graph, join->cover(), leftNode, rightNode),
      state);
  ExprVector exprs;
  exprs.reserve(rootOutputColumns.size());
  for (ColumnCP target : rootOutputColumns) {
    const auto it = rootReps.find(target);
    exprs.push_back(it != rootReps.end() ? it->second : target);
  }
  return state.builder.make<Project>(Project::Key{
      joinNode, std::move(exprs), ColumnVector{rootOutputColumns}});
}

} // namespace

NodeCP JoinTreeEmitter::emit(
    MemoOpCP root,
    const JoinHypergraph& graph,
    const ColumnVector& rootOutputColumns,
    Builder& builder) {
  VELOX_CHECK_NOT_NULL(root);
  EmitState state{
      graph,
      builder,
      ExprFactory{builder},
      std::vector<bool>(graph.filterConjuncts().size(), false)};
  NodeCP result{nullptr};
  switch (root->kind()) {
    case MemoOpKind::kLeaf:
      result = emitLeaf(root->as<LeafOp>(), state);
      break;
    case MemoOpKind::kJoin: {
      const auto* join = root->as<JoinOp>();
      const auto& edge = state.graph.edges()[join->edgeIndex];
      NodeCP leftNode = emitOp(join->left, state);
      if (edge.isUnnest()) {
        result = buildUnnest(join, leftNode, state);
      } else {
        NodeCP rightNode = emitOp(join->right, state);
        if (join->reversedAnti) {
          result = buildReversedAnti(join, leftNode, rightNode, state);
        } else {
          result =
              buildRoot(join, leftNode, rightNode, rootOutputColumns, state);
        }
      }
      break;
    }
    case MemoOpKind::kExchange:
      // The chosen join-tree root is never an exchange: a final gather is added
      // by the fragment splitter, not enumerated into the memo.
      VELOX_UNREACHABLE("Join-tree root cannot be an exchange");
  }
  VELOX_CHECK_NOT_NULL(result);
  const size_t unplaced =
      std::count(state.fired.begin(), state.fired.end(), false);
  VELOX_CHECK_EQ(
      unplaced,
      0,
      "Filter conjuncts were not all placed; required relations exceed the root cover");
  return result;
}

NodeCP JoinTreeEmitter::emitComponents(
    const std::vector<MemoOpCP>& componentRoots,
    const JoinHypergraph& graph,
    const ColumnVector& rootOutputColumns,
    Builder& builder,
    int32_t numWorkers) {
  VELOX_CHECK_GE(
      componentRoots.size(),
      2,
      "emitComponents requires at least two components");
  EmitState state{
      graph,
      builder,
      ExprFactory{builder},
      std::vector<bool>(graph.filterConjuncts().size(), false)};

  // Emit each component subtree first, sharing one `fired` vector so a
  // cross-component conjunct is placed once, at a fold below.
  std::vector<NodeCP> nodes;
  nodes.reserve(componentRoots.size());
  for (MemoOpCP root : componentRoots) {
    nodes.push_back(emitOp(root, state));
  }

  // Largest component drives as the bottom-left probe; the rest join as
  // builds in ascending cardinality.
  std::vector<size_t> order(componentRoots.size());
  std::iota(order.begin(), order.end(), 0);
  std::sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs) {
    // Descending by cardinality. A known cardinality sorts before an unknown
    // one (an unknown is never the larger); ties — including two unknowns —
    // break deterministically on the component index.
    const std::optional<float> lhsCard = componentRoots[lhs]->cost.cardinality;
    const std::optional<float> rhsCard = componentRoots[rhs]->cost.cardinality;
    if (lessThan(rhsCard, lhsCard)) {
      return true;
    }
    if (lessThan(lhsCard, rhsCard)) {
      return false;
    }
    if (lhsCard.has_value() != rhsCard.has_value()) {
      return lhsCard.has_value();
    }
    return lhs < rhs;
  });

  // A target collapsed to a representative within a component must be
  // re-materialized by name; the final fold then emits representatives and a
  // Project restores the target names. With no such collapse the final fold
  // emits `rootOutputColumns` directly and no Project is needed.
  RelationSet fullCover;
  for (MemoOpCP root : componentRoots) {
    fullCover.unionSet(root->cover());
  }
  const auto rootReps = graph.coverColumnReps(fullCover);
  bool collapsed = false;
  for (ColumnCP target : rootOutputColumns) {
    const auto it = rootReps.find(target);
    if (it != rootReps.end() && it->second != target) {
      collapsed = true;
      break;
    }
  }

  NodeCP result = nodes[order[0]];
  RelationSet cover{componentRoots[order[0]]->cover()};
  for (size_t i = 1; i < order.size(); ++i) {
    NodeCP build = nodes[order[i]];
    // A cross product is keyless: broadcast the build so the probe keeps its
    // partitioning and a single-task (Values / global-aggregate) or scan build
    // is isolated in its own fragment instead of co-locating with the probe.
    if (numWorkers > 1) {
      build = builder.make<Exchange>(
          Exchange::Key{build, Partitioning::globalBroadcast()});
    }
    cover.unionSet(componentRoots[order[i]]->cover());
    const bool isLast = (i + 1 == order.size());
    ColumnVector columns = (isLast && !collapsed)
        ? ColumnVector{rootOutputColumns}
        : coverNarrowedColumns(state.graph, cover, result, build);
    const auto reps = graph.coverColumnReps(cover);
    result = builder.make<Join>(Join::Key{
        result,
        build,
        velox::core::JoinType::kInner,
        /*leftKeys=*/ExprVector{},
        /*rightKeys=*/ExprVector{},
        remapToReps(
            takeReadyConjuncts(state.graph, cover, state.fired), reps, state),
        /*nullAware=*/false,
        /*nullAsValue=*/false,
        std::move(columns)});
  }

  if (collapsed) {
    ExprVector projectExprs;
    projectExprs.reserve(rootOutputColumns.size());
    for (ColumnCP target : rootOutputColumns) {
      const auto it = rootReps.find(target);
      projectExprs.push_back(it != rootReps.end() ? it->second : target);
    }
    result = builder.make<Project>(Project::Key{
        result, std::move(projectExprs), ColumnVector{rootOutputColumns}});
  }

  const size_t unplaced =
      std::count(state.fired.begin(), state.fired.end(), false);
  VELOX_CHECK_EQ(
      unplaced,
      0,
      "Filter conjuncts were not all placed across cross-product components");
  return result;
}

} // namespace facebook::axiom::optimizer::v2
