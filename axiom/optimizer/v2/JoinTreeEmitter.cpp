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

// Appends the columns of `side` that a node over `cover` emits: those `needed`
// demands, plus any column produced by no relation in `cover` (e.g. a semijoin
// mark synthesized below, or a key a shuffle materialized), which is outside
// the demand universe and is always kept.
void appendNarrowed(
    ColumnVector& columns,
    NodeCP side,
    const PlanObjectSet& needed,
    const PlanObjectSet& coverColumns) {
  for (ColumnCP column : side->outputColumns()) {
    if (!coverColumns.contains(column) || needed.contains(column)) {
      columns.push_back(column);
    }
  }
}

// An intermediate join emits the columns a consumer above `cover` demands —
// `graph.coverOutputColumns(cover)`, which collapses each within-cover
// equi-group to one representative — exactly the set the cost model charges
// for, so the executed plan matches its estimated width. The one exception is
// a join carrying residual edges, which keeps both columns of each so the
// filter above it can read them. Left-then-right order is preserved. A column
// produced by no relation in `cover` (e.g. a semijoin mark synthesized below,
// or a key a shuffle materialized) is outside the demand universe and is always
// kept.
ColumnVector coverNarrowedColumns(
    const JoinHypergraph& graph,
    const RelationSet& cover,
    NodeCP left,
    NodeCP right) {
  const PlanObjectSet needed = graph.coverOutputColumns(cover);
  const PlanObjectSet coverColumns = graph.coverColumns(cover);
  ColumnVector columns;
  appendNarrowed(columns, left, needed, coverColumns);
  appendNarrowed(columns, right, needed, coverColumns);
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
  EmitState(const JoinHypergraph& graph, Builder& builder)
      : graph{graph},
        builder{builder},
        exprs{builder},
        fired(graph.filterConjuncts().size(), false) {}

  const JoinHypergraph& graph;
  Builder& builder;
  ExprFactory exprs;
  std::vector<bool> fired;
};

// A subtree's root node together with the key expressions its shuffles
// materialized into columns. A consumer rewrites its own keys and filter
// through `materialized`, so it reads the column the shuffle already computed
// instead of evaluating the same expression a second time. An entry is carried
// upward only while its column stays in the emitting node's output.
struct Emitted {
  NodeCP node;
  ExprFactory::ExprSubstitution materialized;
};

Emitted emitOp(MemoOpCP op, EmitState& state);

// A join's children each emit one representative per equivalence group, so a
// column equated and collapsed in a child is gone from that child's output.
// `mergedChildReps` maps every such column to the surviving representative
// across both child covers.
folly::F14FastMap<ColumnCP, ColumnCP> mergedChildReps(
    const JoinOp* join,
    EmitState& state) {
  auto reps = state.graph.coverColumnReps(join->left->cover());
  auto right = state.graph.coverColumnReps(join->right->cover());
  reps.insert(right.begin(), right.end());
  return reps;
}

// The collapsed entries of `reps` as a substitution, so an expression
// referencing a collapsed column is rewritten to the survivor present in the
// child's output.
ExprFactory::ExprSubstitution collapsedColumns(
    const folly::F14FastMap<ColumnCP, ColumnCP>& reps) {
  ExprFactory::ExprSubstitution substitution;
  for (const auto& [column, rep] : reps) {
    if (rep != column) {
      substitution.emplace(column, rep);
    }
  }
  return substitution;
}

// Returns the union of two substitutions. A key expression reads columns of one
// cover only and sibling covers are disjoint, so the same `ExprCP` cannot be
// materialized on two sides.
ExprFactory::ExprSubstitution merge(
    ExprFactory::ExprSubstitution into,
    const ExprFactory::ExprSubstitution& from) {
  for (const auto& [expr, column] : from) {
    const bool inserted = into.emplace(expr, column).second;
    VELOX_CHECK(
        inserted, "Two inputs materialized the same key: {}", expr->toString());
  }
  return into;
}

ExprVector rewrite(
    const ExprVector& exprs,
    const ExprFactory::ExprSubstitution& substitution,
    EmitState& state) {
  if (substitution.empty()) {
    return exprs;
  }
  return state.exprs.replace(exprs, substitution);
}

// Drops entries whose column `node` does not emit: a semi/anti join keeps only
// one side's columns, so a key the other side materialized is unreadable above.
void retainVisible(ExprFactory::ExprSubstitution& substitution, NodeCP node) {
  if (substitution.empty()) {
    return;
  }
  folly::F14FastSet<ExprCP> visible;
  visible.reserve(node->outputColumns().size());
  for (ColumnCP column : node->outputColumns()) {
    visible.insert(column);
  }
  for (auto it = substitution.begin(); it != substitution.end();) {
    it = visible.contains(it->second) ? std::next(it) : substitution.erase(it);
  }
}

// True when an equivalence collapse replaced one of `targets` by a
// representative, so the emitting node cannot carry the target name itself.
bool hasCollapsedTarget(
    const folly::F14FastMap<ColumnCP, ColumnCP>& reps,
    const ColumnVector& targets) {
  for (ColumnCP target : targets) {
    const auto it = reps.find(target);
    if (it != reps.end() && it->second != target) {
      return true;
    }
  }
  return false;
}

// Wraps `node` in a Project that re-materializes each target name from its
// representative, restoring the demanded output schema and order.
NodeCP restoreTargets(
    NodeCP node,
    const folly::F14FastMap<ColumnCP, ColumnCP>& reps,
    const ColumnVector& targets,
    EmitState& state) {
  ExprVector exprs;
  exprs.reserve(targets.size());
  for (ColumnCP target : targets) {
    const auto it = reps.find(target);
    exprs.push_back(it != reps.end() ? it->second : target);
  }
  return state.builder.make<Project>(
      {node, std::move(exprs), ColumnVector{targets}});
}

// The equalities of residual inner edges. Equality is symmetric, so the edges'
// orientation does not matter here. Inner edges carry no filter (`JoinEdge`
// enforces that), so the keys are the whole predicate.
ExprVector residualEdgeEqualities(
    const std::vector<size_t>& residualEdges,
    const ExprFactory::ExprSubstitution& substitution,
    EmitState& state) {
  ExprVector predicates;
  for (size_t index : residualEdges) {
    const auto& edge = state.graph.edges()[index];
    const auto& leftKeys = edge.leftKeys();
    const auto& rightKeys = edge.rightKeys();
    VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());
    for (size_t i = 0; i < leftKeys.size(); ++i) {
      predicates.push_back(state.exprs.makeEq(leftKeys[i], rightKeys[i]));
    }
  }
  return rewrite(predicates, substitution, state);
}

void checkAllConjunctsPlaced(const EmitState& state) {
  const size_t unplaced =
      std::count(state.fired.begin(), state.fired.end(), false);
  VELOX_CHECK_EQ(
      unplaced,
      0,
      "Filter conjuncts were not all placed; required relations exceed the emitted cover");
}

NodeCP emitLeaf(const LeafOp* leaf, EmitState& state) {
  NodeCP node = state.graph.relation(leaf->relationId).node();
  // The relation's node reads the table ungrouped. When the plan that won reads
  // it one bucket-group at a time, that is a different read, so it is a
  // different scan.
  const auto* partitionType = leaf->outputPartitioning().partitionType;
  if (partitionType != nullptr && node->is(NodeType::kScan)) {
    const auto* scan = node->as<Scan>();
    if (scan->groupedPartitionType() != partitionType) {
      return state.builder.make<Scan>(
          {.baseTable = scan->baseTable(),
           .outputColumns = scan->outputColumns(),
           .scanHandle = scan->scanHandle(),
           .groupedPartitionType = partitionType});
    }
  }
  return node;
}

// Builds a new `Unnest` IR node over the plan `unnest` expands.
// `unnestExpressions`, `unnestColumns`, and `ordinalityColumn` come from the
// original Unnest IR node stored on the Unnest relation.
Emitted buildUnnest(const UnnestOp* unnest, Emitted input, EmitState& state) {
  const auto& edge = state.graph.edges()[unnest->edgeIndex];
  const int8_t unnestRelId = edge.rightEndpoints().min();
  const auto* origUnnest =
      state.graph.relation(unnestRelId).node()->as<Unnest>();

  const auto substitution = merge(
      collapsedColumns(state.graph.coverColumnReps(unnest->input->cover())),
      input.materialized);
  ExprVector unnestExpressions =
      rewrite(origUnnest->unnestExpressions(), substitution, state);

  // Inner edges that crossed alongside the Unnest constrain what it produced,
  // so they filter its output: both sides of each equality are columns of this
  // node. Conjuncts from the filter pool that this cover makes ready are
  // applied here too — after the expansion, since one may read an unnested
  // column.
  ExprVector predicates = rewrite(
      takeReadyConjuncts(state.graph, unnest->cover(), state.fired),
      substitution,
      state);
  appendAll(
      predicates,
      residualEdgeEqualities(unnest->residualEdges, substitution, state));

  // The expansion replicates the columns a consumer above the cover demands,
  // plus those the predicates above read: the Filter sits on this node's
  // output, so its inputs have to survive the expansion even when nothing
  // above wants them.
  PlanObjectSet needed = state.graph.coverOutputColumns(unnest->cover());
  needed.unionColumns(predicates);
  ColumnVector replicatedColumns;
  appendNarrowed(
      replicatedColumns,
      input.node,
      needed,
      state.graph.coverColumns(unnest->cover()));

  ColumnVector outputColumns{replicatedColumns};
  for (const auto& perExpr : origUnnest->unnestColumns()) {
    for (const auto* column : perExpr) {
      outputColumns.push_back(column);
    }
  }
  if (origUnnest->ordinalityColumn() != nullptr) {
    outputColumns.push_back(origUnnest->ordinalityColumn());
  }
  if (origUnnest->isOuter()) {
    outputColumns.push_back(origUnnest->markerColumn());
  }

  NodeCP node = state.builder.make<Unnest>(
      {input.node,
       std::move(unnestExpressions),
       std::move(replicatedColumns),
       origUnnest->unnestColumns(),
       origUnnest->ordinalityColumn(),
       origUnnest->markerColumn(),
       std::move(outputColumns)});

  if (!predicates.empty()) {
    node = state.builder.make<Filter>({node, std::move(predicates)});
  }

  retainVisible(input.materialized, node);
  return {node, std::move(input.materialized)};
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
    // A counting semi join is its own mirror, so the semi'd side is the probe
    // in either orientation.
    case JoinType::kCountingLeftSemiFilter:
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

// Produces the demanded columns above a node that emitted the other member of
// an equated pair, binding each absent column to the equal one that is
// present. The keys are oriented: `leftKeys` is the emitted (probe) side.
NodeCP projectDemanded(
    NodeCP node,
    const ColumnVector& demanded,
    const ExprVector& leftKeys,
    const ExprVector& rightKeys,
    EmitState& state) {
  const auto emitted = PlanObjectSet::fromObjects(node->outputColumns());
  if (emitted.containsAll(demanded)) {
    return node;
  }
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());

  ExprVector exprs;
  exprs.reserve(demanded.size());
  for (ColumnCP column : demanded) {
    ExprCP replacement = column;
    if (!emitted.contains(column)) {
      for (size_t i = 0; i < rightKeys.size(); ++i) {
        if (rightKeys[i] == column) {
          replacement = leftKeys[i];
          break;
        }
      }
      VELOX_CHECK(
          replacement != column,
          "Demanded column is absent from the join output and is not equated "
          "to one that is present: {}",
          column->toString());
      VELOX_CHECK(
          replacement->isColumn(),
          "Replacement for a demanded column is not a column: {}",
          column->toString());
      VELOX_CHECK(
          emitted.contains(replacement->as<Column>()),
          "Replacement for a demanded column is not emitted by the join: {}",
          column->toString());
    }
    exprs.push_back(replacement);
  }
  return state.builder.make<Project>(
      {node, std::move(exprs), ColumnVector{demanded}});
}

Emitted buildJoin(
    const JoinOp* join,
    const Emitted& left,
    const Emitted& right,
    ColumnVector outputColumns,
    EmitState& state) {
  const auto& edge = state.graph.edges()[join->edgeIndex];
  const bool isInner = edge.joinType() == velox::core::JoinType::kInner;

  // True when the cover may have collapsed a demanded column onto the build
  // side, leaving the probe-only output short of it.
  const bool mayNeedProject =
      join->joinType == velox::core::JoinType::kCountingLeftSemiFilter;
  ColumnVector demanded;
  if (mayNeedProject) {
    demanded = outputColumns;
  }
  if (auto narrowed = narrowSemiAntiOutput(
          join->joinType, left.node, right.node, edge.markColumn())) {
    outputColumns = std::move(*narrowed);
  }

  // If DPhyp chose the swapped orientation, the left eligibility side covers
  // join->right; swap keys.
  ExprVector leftKeys{edge.leftKeys()};
  ExprVector rightKeys{edge.rightKeys()};
  if (edge.leftEligibility().isSubset(join->right->cover())) {
    std::swap(leftKeys, rightKeys);
  }

  // Additional inner edges this join applies (a cyclic join graph can have
  // several edges crossing one partition), each orientation-corrected
  // independently. `extraEdges` are guaranteed to split across the two
  // children, so they conjoin into the join keys. Residual edges are kept
  // separately.
  VELOX_DCHECK(isInner || join->extraEdges.empty());
  for (size_t extraIndex : join->extraEdges) {
    const auto& extra = state.graph.edges()[extraIndex];
    const bool forward =
        extra.leftEligibility().isSubset(join->left->cover()) &&
        extra.rightEligibility().isSubset(join->right->cover());
    ExprVector extraLeftKeys{extra.leftKeys()};
    ExprVector extraRightKeys{extra.rightKeys()};
    if (!forward) {
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

  auto materialized = merge(left.materialized, right.materialized);
  const auto substitution =
      merge(collapsedColumns(mergedChildReps(join, state)), materialized);
  leftKeys = rewrite(leftKeys, substitution, state);
  rightKeys = rewrite(rightKeys, substitution, state);
  filter = rewrite(filter, substitution, state);
  // A non-inner join applies its edge filter as the join condition. Conjuncts
  // newly eligible at this cover and residual edge equalities run above the
  // join; putting them in a non-inner condition would null-pad rejected rows
  // instead of dropping them.
  ExprVector aboveJoin =
      residualEdgeEqualities(join->residualEdges, substitution, state);
  if (!isInner) {
    appendAll(
        aboveJoin,
        rewrite(
            takeReadyConjuncts(state.graph, join->cover(), state.fired),
            substitution,
            state));
  }

  // The cover collapses the equated columns of a residual edge to one
  // representative, so the narrowed output carries only that one. The filter
  // reads both, and both are emitted by the children — the collapse spans the
  // two child covers, so neither child applied it — so keep them here.
  if (!aboveJoin.empty()) {
    PlanObjectSet extraColumns;
    extraColumns.unionColumns(aboveJoin);
    PlanObjectSet present;
    for (ColumnCP column : outputColumns) {
      present.add(column);
    }
    for (NodeCP side : {left.node, right.node}) {
      for (ColumnCP column : side->outputColumns()) {
        if (extraColumns.contains(column) && !present.contains(column)) {
          outputColumns.push_back(column);
          present.add(column);
        }
      }
    }
  }

  NodeCP node = state.builder.make<Join>(
      {left.node,
       right.node,
       join->joinType,
       // The projection below reads these, so they outlive the Join.
       leftKeys,
       rightKeys,
       std::move(filter),
       edge.nullAware(),
       edge.nullAsValue(),
       std::move(outputColumns)});
  if (!aboveJoin.empty()) {
    node = state.builder.make<Filter>({node, std::move(aboveJoin)});
  }
  // A probe-only join emits its probe side, so a demanded column that the
  // equated class placed on the other side is absent. Both hold the same value
  // on every emitted row, so bind it to its partner through the keys.
  if (mayNeedProject) {
    node = projectDemanded(node, demanded, leftKeys, rightKeys, state);
  }
  retainVisible(materialized, node);
  return {node, std::move(materialized)};
}

// Lowers an antijoin played in its reversed (build-on-the-preserved-side)
// orientation. There is no `kAnti` build-side flip in Velox, so synthesize
// it: a kRightSemiProject (probe = `probe`, the edge's right side; build =
// `build`, the preserved left side) emits each build row plus a mark for a
// probe match; `Filter(not mark)` keeps the unmatched rows (the antijoin
// result); a Project drops the mark, restoring the antijoin schema. The mark is
// fresh and never escapes this subtree.
//
// TODO: Replace this synthesis (and the reversedAnti orientation marker)
// with a plain kRightAnti relabel once Velox adds that join type:
// https://github.com/facebookincubator/velox/issues/17815.
Emitted buildReversedAnti(
    const JoinOp* join,
    const Emitted& probe,
    const Emitted& build,
    EmitState& state) {
  const auto& edge = state.graph.edges()[join->edgeIndex];

  const ColumnVector antiOutput{build.node->outputColumns()};
  ColumnCP mark = Column::createBoolean("mark");
  ColumnVector joinOutput{antiOutput};
  joinOutput.push_back(mark);

  // edge.leftKeys reference the preserved (build) side, rightKeys the probe
  // side. The IR Join's leftKeys must reference its left (probe) input.
  auto materialized = merge(probe.materialized, build.materialized);
  const auto substitution =
      merge(collapsedColumns(mergedChildReps(join, state)), materialized);
  NodeCP rightSemiProject = state.builder.make<Join>(
      {probe.node,
       build.node,
       velox::core::JoinType::kRightSemiProject,
       rewrite(ExprVector{edge.rightKeys()}, substitution, state),
       rewrite(ExprVector{edge.leftKeys()}, substitution, state),
       rewrite(ExprVector{edge.filter()}, substitution, state),
       edge.nullAware(),
       edge.nullAsValue(),
       std::move(joinOutput)});

  NodeCP filtered = state.builder.make<Filter>(
      {rightSemiProject, ExprVector{state.exprs.makeNot(mark)}});

  // Project away the mark, restoring the antijoin's output schema. Every
  // entry is a pass-through of the preserved-side column.
  ExprVector projectExprs;
  projectExprs.reserve(antiOutput.size());
  for (ColumnCP column : antiOutput) {
    projectExprs.push_back(column);
  }
  NodeCP node = state.builder.make<Project>(
      {filtered, std::move(projectExprs), antiOutput});
  retainVisible(materialized, node);
  return {node, std::move(materialized)};
}

// Re-materializes the demanded names on a cluster root whose equivalence
// collapse replaced some of them by a representative. Callers test
// `hasCollapsedTarget` first.
Emitted restoreRootTargets(
    Emitted result,
    const folly::F14FastMap<ColumnCP, ColumnCP>& rootReps,
    const ColumnVector& rootOutputColumns,
    EmitState& state) {
  result.node = restoreTargets(result.node, rootReps, rootOutputColumns, state);
  retainVisible(result.materialized, result.node);
  return result;
}

// Emits a join op and everything below it. `rootOutputColumns` is non-null only
// for the cluster root, whose output must be exactly those columns; an
// equivalence collapse that dropped a target in favor of its representative is
// undone by a Project on top.
Emitted emitJoin(
    const JoinOp* join,
    EmitState& state,
    const ColumnVector* rootOutputColumns) {
  Emitted left = emitOp(join->left, state);
  Emitted right = emitOp(join->right, state);
  if (join->reversedAnti) {
    return buildReversedAnti(join, left, right, state);
  }

  if (rootOutputColumns == nullptr) {
    return buildJoin(
        join,
        left,
        right,
        coverNarrowedColumns(state.graph, join->cover(), left.node, right.node),
        state);
  }

  const auto rootReps = state.graph.coverColumnReps(join->cover());
  if (!hasCollapsedTarget(rootReps, *rootOutputColumns)) {
    return buildJoin(
        join, left, right, ColumnVector{*rootOutputColumns}, state);
  }

  return restoreRootTargets(
      buildJoin(
          join,
          left,
          right,
          coverNarrowedColumns(
              state.graph, join->cover(), left.node, right.node),
          state),
      rootReps,
      *rootOutputColumns,
      state);
}

// Emits an Unnest op and everything below it. `rootOutputColumns` as in
// `emitJoin`.
Emitted emitUnnest(
    const UnnestOp* unnest,
    EmitState& state,
    const ColumnVector* rootOutputColumns) {
  Emitted result = buildUnnest(unnest, emitOp(unnest->input, state), state);
  if (rootOutputColumns == nullptr) {
    return result;
  }
  const auto rootReps = state.graph.coverColumnReps(unnest->cover());
  if (!hasCollapsedTarget(rootReps, *rootOutputColumns)) {
    return result;
  }
  return restoreRootTargets(
      std::move(result), rootReps, *rootOutputColumns, state);
}

// A shuffle partitions on columns of the row it shuffles, so an expression key
// is computed on the producer side and recorded, letting the consumer above
// read that column instead of evaluating the expression again.
Emitted emitExchange(const ExchangeOp* exchange, EmitState& state) {
  Emitted input = emitOp(exchange->input, state);
  Partitioning partitioning = exchange->outputPartitioning();
  auto [keyed, columnKeys] =
      state.builder.materializeKeys(input.node, partitioning.keys);
  for (size_t i = 0; i < partitioning.keys.size(); ++i) {
    if (columnKeys[i] == partitioning.keys[i]) {
      continue;
    }
    const bool inserted =
        input.materialized.emplace(partitioning.keys[i], columnKeys[i]).second;
    VELOX_CHECK(
        inserted,
        "Key already materialized below: {}",
        partitioning.keys[i]->toString());
  }
  partitioning.keys = std::move(columnKeys);
  NodeCP node = state.builder.make<Exchange>({keyed, std::move(partitioning)});
  return {node, std::move(input.materialized)};
}

Emitted emitOp(MemoOpCP op, EmitState& state) {
  switch (op->kind()) {
    case MemoOpKind::kLeaf:
      return {emitLeaf(op->as<LeafOp>(), state), {}};
    case MemoOpKind::kJoin:
      return emitJoin(op->as<JoinOp>(), state, /*rootOutputColumns=*/nullptr);
    case MemoOpKind::kUnnest:
      return emitUnnest(
          op->as<UnnestOp>(), state, /*rootOutputColumns=*/nullptr);
    case MemoOpKind::kExchange:
      return emitExchange(op->as<ExchangeOp>(), state);
  }
  VELOX_UNREACHABLE();
}

} // namespace

NodeCP JoinTreeEmitter::emit(
    MemoOpCP root,
    const JoinHypergraph& graph,
    const ColumnVector& rootOutputColumns,
    Builder& builder) {
  VELOX_CHECK_NOT_NULL(root);
  EmitState state{graph, builder};
  NodeCP result{nullptr};
  switch (root->kind()) {
    case MemoOpKind::kLeaf:
      result = emitLeaf(root->as<LeafOp>(), state);
      break;
    case MemoOpKind::kJoin:
      result = emitJoin(root->as<JoinOp>(), state, &rootOutputColumns).node;
      break;
    case MemoOpKind::kUnnest:
      result = emitUnnest(root->as<UnnestOp>(), state, &rootOutputColumns).node;
      break;
    case MemoOpKind::kExchange:
      // The chosen join-tree root is never an exchange: a final gather is added
      // by the fragment splitter, not enumerated into the memo.
      VELOX_UNREACHABLE("Join-tree root cannot be an exchange");
  }
  VELOX_CHECK_NOT_NULL(result);
  checkAllConjunctsPlaced(state);
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
  EmitState state{graph, builder};

  // Emit each component subtree first, sharing one `fired` vector so a
  // cross-component conjunct is placed once, at a fold below.
  std::vector<Emitted> emitted;
  emitted.reserve(componentRoots.size());
  for (MemoOpCP root : componentRoots) {
    emitted.push_back(emitOp(root, state));
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
  const bool collapsed = hasCollapsedTarget(rootReps, rootOutputColumns);

  NodeCP result = emitted[order[0]].node;
  RelationSet cover{componentRoots[order[0]]->cover()};
  // Grows with the fold, so a conjunct is rewritten only through what is
  // already below it.
  auto materialized = emitted[order[0]].materialized;
  for (size_t i = 1; i < order.size(); ++i) {
    NodeCP build = emitted[order[i]].node;
    materialized =
        merge(std::move(materialized), emitted[order[i]].materialized);
    // A cross product is keyless: broadcast the build so the probe keeps its
    // partitioning and a single-task (Values / global-aggregate) or scan build
    // is isolated in its own fragment instead of co-locating with the probe.
    if (numWorkers > 1) {
      build = builder.make<Exchange>({build, Partitioning::globalBroadcast()});
    }
    cover.unionSet(componentRoots[order[i]]->cover());
    const bool isLast = (i + 1 == order.size());
    ColumnVector columns = (isLast && !collapsed)
        ? ColumnVector{rootOutputColumns}
        : coverNarrowedColumns(state.graph, cover, result, build);
    const auto substitution =
        merge(collapsedColumns(graph.coverColumnReps(cover)), materialized);
    result = builder.make<Join>(
        {result,
         build,
         velox::core::JoinType::kInner,
         /*leftKeys=*/ExprVector{},
         /*rightKeys=*/ExprVector{},
         rewrite(
             takeReadyConjuncts(state.graph, cover, state.fired),
             substitution,
             state),
         /*nullAware=*/false,
         /*nullAsValue=*/false,
         std::move(columns)});
  }

  if (collapsed) {
    result = restoreTargets(result, rootReps, rootOutputColumns, state);
  }

  checkAllConjunctsPlaced(state);
  return result;
}

} // namespace facebook::axiom::optimizer::v2
