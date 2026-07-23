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

#include "axiom/optimizer/v2/PushdownAndPrunePass.h"

#include <limits>
#include <optional>

#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/ExprSimplifier.h"
#include "axiom/optimizer/v2/JoinCondition.h"
#include "axiom/optimizer/v2/NodeRewriter.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

// Per-call state for the pushdown visitor.
struct PushdownContext {
  // Conjuncts collected from the path above this node that the visitor
  // will try to push further down.
  ExprVector pending;

  // Columns the consumer above this node actually reads. Used to narrow
  // each node's `outputColumns` (and to drop unused exprs / aggregates
  // / window functions / leg columns). Seeded at the root with
  // `root->outputColumns()` so the root's output schema is preserved.
  // Includes columns referenced by `pending` (they must survive to where
  // a conjunct lands).
  PlanObjectSet required;

  // Subset of `required` demanded by consumers strictly above this node,
  // i.e. `required` without the columns folded in for `pending`. A
  // kLeftSemiProject mark in this set is live above and must not be
  // fused away. Conservatively defaults to `required` on paths that do
  // not refine it, which only blocks (never wrongly enables) fusion.
  PlanObjectSet requiredAbove;

  // `Column*`s whose values are guaranteed non-NULL in the current
  // subtree (derived from default-null-behavior equi-keys of an
  // ancestor inner join). Never inserted into the plan.
  PlanObjectSet nonNullColumns;
};

// Returns true if `conjunct` references at least one column in
// `columns` and contains no non-default-null-behavior sub-expression.
// Conservative — a true result implies null-rejection; a false result
// does not imply the opposite.
bool isNullRejecting(ExprCP conjunct, const PlanObjectSet& columns) {
  return conjunct->columns().hasIntersection(columns) &&
      !conjunct->functions().contains(FunctionSet::kNonDefaultNullBehavior);
}

// Returns the demoted join kind if a pending conjunct or an
// ancestor-proven non-NULL column rejects nulls on the
// null-padded side(s), otherwise `joinType` unchanged.
velox::core::JoinType demoteOuterToInner(
    velox::core::JoinType joinType,
    const ExprVector& pending,
    const PlanObjectSet& nonNullColumns,
    const PlanObjectSet& leftColumns,
    const PlanObjectSet& rightColumns) {
  auto anyRejects = [&](const PlanObjectSet& columns) {
    for (ExprCP conjunct : pending) {
      if (isNullRejecting(conjunct, columns)) {
        return true;
      }
    }
    return columns.hasIntersection(nonNullColumns);
  };
  switch (joinType) {
    case velox::core::JoinType::kLeft:
      return anyRejects(rightColumns) ? velox::core::JoinType::kInner
                                      : joinType;
    case velox::core::JoinType::kRight:
      return anyRejects(leftColumns) ? velox::core::JoinType::kInner : joinType;
    case velox::core::JoinType::kFull: {
      const bool rejectsLeft = anyRejects(leftColumns);
      const bool rejectsRight = anyRejects(rightColumns);
      if (rejectsLeft && rejectsRight) {
        return velox::core::JoinType::kInner;
      }
      if (rejectsLeft) {
        return velox::core::JoinType::kRight;
      }
      if (rejectsRight) {
        return velox::core::JoinType::kLeft;
      }
      return joinType;
    }
    case velox::core::JoinType::kInner:
    case velox::core::JoinType::kLeftSemiFilter:
    case velox::core::JoinType::kCountingLeftSemiFilter:
    case velox::core::JoinType::kLeftSemiProject:
    case velox::core::JoinType::kRightSemiFilter:
    case velox::core::JoinType::kRightSemiProject:
    case velox::core::JoinType::kRightAnti:
    case velox::core::JoinType::kAnti:
    case velox::core::JoinType::kCountingAnti:
      return joinType;
    case velox::core::JoinType::kNumJoinTypes:
      break;
  }
  VELOX_UNREACHABLE();
}

// Result of fusing a consuming mark filter into a kLeftSemiProject
// join. `joinType` is the filtering type the join switches to; `conjunct`
// is the pending conjunct that gets consumed (removed from `pending`).
struct MarkFusion {
  velox::core::JoinType joinType;
  ExprCP conjunct;
};

// Returns true if `expr` is syntactically `not(arg)`.
bool isNot(ExprCP expr, ExprCP& arg) {
  if (!expr->is(PlanType::kCallExpr)) {
    return false;
  }
  const Call* call = expr->as<Call>();
  if (call->name() != toName(FunctionRegistry::instance()->negation()) ||
      call->args().size() != 1) {
    return false;
  }
  arg = call->args()[0];
  return true;
}

// Attempts to fuse a consuming mark filter on a `kLeftSemiProject` join
// into a filtering `kLeftSemiFilter` / `kAnti`. Returns the fusion when a
// pending conjunct is syntactically exactly the mark (→ kLeftSemiFilter)
// or exactly `not(mark)` (→ kAnti), and the mark is dead above except for
// that conjunct. Returns nullopt otherwise (leaving the join a
// kLeftSemiProject).
//
// Correctness preconditions, all required:
//  - Shape: the conjunct is exactly `mark` or `not(mark)`; a conjunct
//    that merely references the mark (`mark IS NULL`, `mark OR x`,
//    `coalesce(mark, false)`) is not a semi/anti and must not fuse.
//  - Three-valued mark: the kLeftSemiProject mark is three-valued for
//    null-aware IN. `Filter(not(mark))` keeps exactly `mark = false`
//    rows, which is correct NOT IN by 3VL. The match is against the raw
//    conjunct, preserving the negated nullable mark's null behavior.
//  - Liveness: the mark must not be required by any consumer other than
//    the fused conjunct. The mark is a fresh column produced only by this
//    join, so its consumers are pending conjuncts (the fused one plus any
//    others, checked here) and consumers strictly above carried in
//    `requiredAbove`. A mark live above is not fused: dropping it would
//    leave a dangling output-column demand.
std::optional<MarkFusion> fuseMarkFilter(
    JoinCP node,
    const ExprVector& pending,
    const PlanObjectSet& requiredAbove) {
  if (node->joinType() != velox::core::JoinType::kLeftSemiProject) {
    return std::nullopt;
  }
  // The mark is the last output column of a kLeftSemiProject join.
  VELOX_CHECK(
      !node->outputColumns().empty(),
      "kLeftSemiProject join must produce a mark column");
  ColumnCP mark = node->outputColumns().back();

  // A consumer strictly above keeps the mark live; fusion drops it.
  if (requiredAbove.contains(mark)) {
    return std::nullopt;
  }

  ExprCP fused{nullptr};
  velox::core::JoinType fusedType{};
  for (ExprCP conjunct : pending) {
    if (conjunct == mark) {
      fused = conjunct;
      fusedType = velox::core::JoinType::kLeftSemiFilter;
      break;
    }
    ExprCP negated{nullptr};
    if (isNot(conjunct, negated) && negated == mark) {
      fused = conjunct;
      fusedType = velox::core::JoinType::kAnti;
      break;
    }
  }
  if (fused == nullptr) {
    return std::nullopt;
  }

  // Any other pending conjunct that references the mark keeps it live.
  for (ExprCP conjunct : pending) {
    if (conjunct != fused && conjunct->columns().contains(mark)) {
      return std::nullopt;
    }
  }
  return MarkFusion{fusedType, fused};
}

// Returns true if a `leftOnly` conjunct that arrived from above the join
// may be pushed to the left input of a join of `joinType`. An above-join
// conjunct is an extra restriction on the join output; this differs from a
// conjunct in the join's own match condition (see joinFilterTarget).
bool canPushLeft(velox::core::JoinType joinType, bool leftOnly) {
  if (!leftOnly) {
    return false;
  }
  switch (joinType) {
    case velox::core::JoinType::kInner:
    case velox::core::JoinType::kLeft:
    case velox::core::JoinType::kLeftSemiFilter:
    case velox::core::JoinType::kCountingLeftSemiFilter:
    case velox::core::JoinType::kLeftSemiProject:
    case velox::core::JoinType::kAnti:
    case velox::core::JoinType::kCountingAnti:
      return true;
    case velox::core::JoinType::kRight:
    case velox::core::JoinType::kFull:
    case velox::core::JoinType::kRightSemiFilter:
    case velox::core::JoinType::kRightSemiProject:
    case velox::core::JoinType::kRightAnti:
      return false;
    case velox::core::JoinType::kNumJoinTypes:
      break;
  }
  VELOX_UNREACHABLE();
}

// Returns the cross-side `Column == Column` equi-pairs reachable on
// `node` — either explicitly via `leftKeys`/`rightKeys` or as
// `eq(Column, Column)` conjuncts in the join's filter. The returned
// pair has equal-length `first` (left columns) and `second` (right
// columns) vectors.
std::pair<ColumnVector, ColumnVector> collectEquiColumnPairs(
    JoinCP node,
    const PlanObjectSet& leftColumns,
    const PlanObjectSet& rightColumns) {
  ColumnVector leftKeys;
  ColumnVector rightKeys;
  for (size_t i = 0; i < node->leftKeys().size(); ++i) {
    ExprCP leftKey = node->leftKeys()[i];
    ExprCP rightKey = node->rightKeys()[i];
    if (leftKey->is(PlanType::kColumnExpr) &&
        rightKey->is(PlanType::kColumnExpr)) {
      leftKeys.push_back(leftKey->as<Column>());
      rightKeys.push_back(rightKey->as<Column>());
    }
  }
  if (node->filter().empty()) {
    return {std::move(leftKeys), std::move(rightKeys)};
  }

  const Name equalityName = toName(FunctionRegistry::instance()->equality());
  for (ExprCP conjunct : node->filter()) {
    if (!conjunct->is(PlanType::kCallExpr)) {
      continue;
    }
    const Call* call = conjunct->as<Call>();
    if (call->name() != equalityName) {
      continue;
    }
    ExprCP first = call->args()[0];
    ExprCP second = call->args()[1];
    if (!first->is(PlanType::kColumnExpr) ||
        !second->is(PlanType::kColumnExpr)) {
      continue;
    }
    ColumnCP firstColumn = first->as<Column>();
    ColumnCP secondColumn = second->as<Column>();
    if (leftColumns.contains(firstColumn) &&
        rightColumns.contains(secondColumn)) {
      leftKeys.push_back(firstColumn);
      rightKeys.push_back(secondColumn);
    } else if (
        leftColumns.contains(secondColumn) &&
        rightColumns.contains(firstColumn)) {
      leftKeys.push_back(secondColumn);
      rightKeys.push_back(firstColumn);
    }
  }
  return {std::move(leftKeys), std::move(rightKeys)};
}

// Returns per-side necessary-condition filters derived from `orExpr`:
// for each side, an OR of the per-disjunct AND-chunks of conjuncts
// referencing only that side's columns. Returns nullptr for a side
// when any disjunct contributes no conjunct on that side (the per-
// side OR would be trivially true), and returns nullptr for both
// sides when `orExpr` is not an OR call or contains non-deterministic
// expressions.
std::pair<ExprCP, ExprCP> deriveSideFiltersFromOr(
    ExprCP orExpr,
    const PlanObjectSet& leftColumns,
    const PlanObjectSet& rightColumns,
    ExprFactory& factory) {
  if (!orExpr->is(PlanType::kCallExpr) ||
      orExpr->as<Call>()->name() != SpecialFormCallNames::kOr) {
    return {nullptr, nullptr};
  }
  if (orExpr->containsNonDeterministic()) {
    return {nullptr, nullptr};
  }
  ExprVector disjuncts = ExprFactory::flattenOr(orExpr);
  ExprVector leftSideDisjuncts;
  ExprVector rightSideDisjuncts;
  bool leftValid = true;
  bool rightValid = true;
  for (ExprCP disjunct : disjuncts) {
    ExprVector conjuncts = ExprFactory::flattenAnd(disjunct);
    ExprVector leftOnly;
    ExprVector rightOnly;
    for (ExprCP conjunct : conjuncts) {
      const auto& columns = conjunct->columns();
      if (columns.empty()) {
        continue;
      }
      if (columns.isSubset(leftColumns)) {
        leftOnly.push_back(conjunct);
      }
      if (columns.isSubset(rightColumns)) {
        rightOnly.push_back(conjunct);
      }
    }
    if (leftOnly.empty()) {
      leftValid = false;
    } else if (leftValid) {
      leftSideDisjuncts.push_back(factory.andAll(leftOnly));
    }
    if (rightOnly.empty()) {
      rightValid = false;
    } else if (rightValid) {
      rightSideDisjuncts.push_back(factory.andAll(rightOnly));
    }
    if (!leftValid && !rightValid) {
      return {nullptr, nullptr};
    }
  }
  return {
      leftValid ? factory.orAll(leftSideDisjuncts) : nullptr,
      rightValid ? factory.orAll(rightSideDisjuncts) : nullptr};
}

// Splits `expressions` by whether each one's column set overlaps
// `columns`. Returns `{noOverlap, overlap}`.
std::pair<ExprVector, ExprVector> partition(
    const ExprVector& expressions,
    const PlanObjectSet& columns) {
  ExprVector noOverlap;
  ExprVector overlap;
  for (ExprCP expression : expressions) {
    if (expression->columns().hasIntersection(columns)) {
      overlap.push_back(expression);
    } else {
      noOverlap.push_back(expression);
    }
  }
  return {std::move(noOverlap), std::move(overlap)};
}

// Mirror of canPushLeft for the right input (from-above conjuncts).
bool canPushRight(velox::core::JoinType joinType, bool rightOnly) {
  if (!rightOnly) {
    return false;
  }
  switch (joinType) {
    case velox::core::JoinType::kInner:
    case velox::core::JoinType::kRight:
    case velox::core::JoinType::kRightSemiFilter:
    case velox::core::JoinType::kRightSemiProject:
    case velox::core::JoinType::kRightAnti:
      return true;
    case velox::core::JoinType::kLeft:
    case velox::core::JoinType::kFull:
    case velox::core::JoinType::kLeftSemiFilter:
    case velox::core::JoinType::kCountingLeftSemiFilter:
    case velox::core::JoinType::kLeftSemiProject:
    case velox::core::JoinType::kAnti:
    case velox::core::JoinType::kCountingAnti:
      return false;
    case velox::core::JoinType::kNumJoinTypes:
      break;
  }
  VELOX_UNREACHABLE();
}

// Where a conjunct from a join's own match condition (its filter) that
// references only one input may move.
enum class FilterTarget { kKeep, kLeft, kRight };

// Returns the target for a single-input match conjunct. This differs from
// canPushLeft/canPushRight, which govern conjuncts arriving from above the
// join (an extra restriction on the output). A match conjunct may move to:
//  - kInner: either referenced input.
//  - kLeft / kRight: only the non-preserved input; pushing the preserved
//    input would drop rows that must be NULL-padded.
//  - semi / anti: only the existence-check input. Pre-filtering the match
//    rows preserves which output rows have (semi) or lack (anti) a match.
//    The output side is row-preserving for anti / project / counting, so
//    pushing a conjunct into it would drop rows that must survive.
//  - kFull: neither.
FilterTarget joinFilterTarget(
    velox::core::JoinType joinType,
    bool leftOnly,
    bool rightOnly) {
  switch (joinType) {
    case velox::core::JoinType::kInner:
      if (leftOnly) {
        return FilterTarget::kLeft;
      }
      return rightOnly ? FilterTarget::kRight : FilterTarget::kKeep;
    case velox::core::JoinType::kLeft:
      return rightOnly ? FilterTarget::kRight : FilterTarget::kKeep;
    case velox::core::JoinType::kRight:
      return leftOnly ? FilterTarget::kLeft : FilterTarget::kKeep;
    case velox::core::JoinType::kFull:
      return FilterTarget::kKeep;
    // Existence-check input is the right side for these.
    case velox::core::JoinType::kLeftSemiFilter:
    case velox::core::JoinType::kCountingLeftSemiFilter:
    case velox::core::JoinType::kLeftSemiProject:
    case velox::core::JoinType::kAnti:
    case velox::core::JoinType::kCountingAnti:
      return rightOnly ? FilterTarget::kRight : FilterTarget::kKeep;
    // ...and the left side for right-semi and right-anti.
    case velox::core::JoinType::kRightSemiFilter:
    case velox::core::JoinType::kRightSemiProject:
    case velox::core::JoinType::kRightAnti:
      return leftOnly ? FilterTarget::kLeft : FilterTarget::kKeep;
    case velox::core::JoinType::kNumJoinTypes:
      break;
  }
  VELOX_UNREACHABLE();
}

// A recognized `Filter(rankColumn <op> n)` over a single-ranking-function
// `Window`, to be fused into a `TopNRowNumber`.
struct RankFusion {
  // The consumed predicate conjunct.
  ExprCP predicate;
  // The window function's output column (the rank value).
  ColumnCP rankColumn;
  velox::core::TopNRowNumberNode::RankFunction rankFunction;
  int32_t limit;
};

// Detects a single ranking function (row_number / rank / dense_rank) bounded by
// a per-partition row limit, returning the fusion when one of the 'pending'
// conjuncts bounds the rank column.
std::optional<RankFusion> detectRankFusion(
    const Window* node,
    const ExprVector& pending,
    const FunctionNames& names) {
  if (node->functions().size() != 1 || node->orderKeys().empty()) {
    return std::nullopt;
  }
  const ExprCP call = node->functions()[0].call;
  if (!call->is(PlanType::kCallExpr)) {
    return std::nullopt;
  }
  const Name functionName = call->as<Call>()->name();
  velox::core::TopNRowNumberNode::RankFunction rankFunction;
  if (functionName == names.rowNumber) {
    rankFunction = velox::core::TopNRowNumberNode::RankFunction::kRowNumber;
  } else if (functionName == names.rank) {
    rankFunction = velox::core::TopNRowNumberNode::RankFunction::kRank;
  } else if (functionName == names.denseRank) {
    rankFunction = velox::core::TopNRowNumberNode::RankFunction::kDenseRank;
  } else {
    return std::nullopt;
  }

  const size_t numInputColumns = node->input()->outputColumns().size();
  const ColumnCP rankColumn = node->outputColumns()[numInputColumns];

  for (ExprCP conjunct : pending) {
    if (!conjunct->is(PlanType::kCallExpr)) {
      continue;
    }
    const Call* comparison = conjunct->as<Call>();
    if (comparison->args().size() != 2 || comparison->args()[0] != rankColumn ||
        !comparison->args()[1]->is(PlanType::kLiteralExpr)) {
      continue;
    }
    const int64_t bound =
        integerValue(&comparison->args()[1]->as<Literal>()->literal());
    const Name name = comparison->name();
    int64_t limit;
    if (name == names.lte && bound > 0) {
      limit = bound;
    } else if (name == names.lt && bound > 1) {
      limit = bound - 1;
    } else if (name == names.equality && bound == 1) {
      limit = 1;
    } else {
      continue;
    }
    if (limit > std::numeric_limits<int32_t>::max()) {
      continue;
    }
    return RankFusion{
        conjunct, rankColumn, rankFunction, static_cast<int32_t>(limit)};
  }
  return std::nullopt;
}

// Top-down filter pushdown visitor. Every node kind is overridden
// explicitly so each kind decides whether and how to propagate pending
// conjuncts.
class Pushdown : public NodeRewriter<PushdownContext> {
 public:
  Pushdown(Builder& builder, velox::core::ExpressionEvaluator& evaluator)
      : NodeRewriter(builder),
        exprs_(builder),
        simplifier_(builder, evaluator) {}

 protected:
  // Filter conjuncts (flattened across AND trees) join `pending`; the
  // Filter node itself is dropped.
  NodeCP rewriteFilter(const Filter* node, PushdownContext& context) override {
    for (ExprCP predicate : node->predicates()) {
      ExprFactory::flattenAnd(predicate, context.pending);
    }
    return rewrite(node->input(), context);
  }

  // Project: substitute pending output-column references through aliases
  // whose backing expression is deterministic. Conjuncts that reference a
  // non-deterministic output are blocked above the Project.
  NodeCP rewriteProject(const Project* node, PushdownContext& context)
      override {
    PlanObjectSet blockedOutputs;
    for (size_t i = 0; i < node->exprs().size(); ++i) {
      if (node->exprs()[i]->functions().contains(
              FunctionSet::kNonDeterministic)) {
        blockedOutputs.add(node->outputColumns()[i]);
      }
    }

    auto [substitutable, blocked] = partition(context.pending, blockedOutputs);
    ExprVector pushable;
    pushable.reserve(substitutable.size());
    for (ExprCP conjunct : substitutable) {
      ExprCP substituted =
          exprs_.substitute(conjunct, node->outputColumns(), node->exprs());
      if (simplifier_.simplifyFilter(substituted, pushable)) {
        return makeEmptyValues(node);
      }
    }

    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);
    ExprVector survivingExprs;
    ColumnVector survivingOutputs;
    survivingExprs.reserve(node->exprs().size());
    survivingOutputs.reserve(node->outputColumns().size());
    for (size_t i = 0; i < node->exprs().size(); ++i) {
      if (outputsKept.contains(node->outputColumns()[i])) {
        survivingExprs.push_back(node->exprs()[i]);
        survivingOutputs.push_back(node->outputColumns()[i]);
      }
    }

    PushdownContext childContext;
    childContext.pending = std::move(pushable);
    childContext.required.unionColumns(survivingExprs);
    // Demand from above the child excludes the conjuncts pushed into
    // `pending`; it is exactly what the surviving projections read.
    childContext.requiredAbove = childContext.required;
    childContext.required.unionColumns(childContext.pending);
    childContext.nonNullColumns = context.nonNullColumns;
    NodeCP newInput = rewrite(node->input(), childContext);

    // Inline a child Project: a Project directly over another Project collapses
    // into one by substituting this Project's expressions through the child's
    // output->expression map. Skipped when the child has a non-deterministic
    // expression, since a parent that references such an output more than once
    // would evaluate it multiple times.
    //
    // TODO: still inline the deterministic outputs when only some are
    // non-deterministic — isolate the non-deterministic ones in a separate
    // Project below and fold the rest.
    if (newInput->is(NodeType::kProject)) {
      const auto* childProject = newInput->as<Project>();
      if (childProject->isDeterministic()) {
        survivingExprs = exprs_.substitute(
            survivingExprs,
            childProject->outputColumns(),
            childProject->exprs());
        newInput = childProject->input();
      }
    }

    // Drop the Project when its surviving outputs are a pure pass-through of
    // the (rewritten) input's output columns. Pushdown can make a Project
    // identity by pruning columns below it — e.g. a semi-project join fused
    // to a semi-filter no longer emits the mark this Project dropped. An
    // empty-output Project is never identity: it eliminates all columns (the
    // semi-join EXISTS gate), which the input does not guarantee on its own.
    const ColumnVector& inputColumns = newInput->outputColumns();
    bool isIdentity{
        !survivingOutputs.empty() &&
        survivingOutputs.size() == inputColumns.size()};
    for (size_t i = 0; isIdentity && i < survivingOutputs.size(); ++i) {
      isIdentity = survivingOutputs[i] == inputColumns[i] &&
          survivingExprs[i] == inputColumns[i];
    }

    NodeCP newProject;
    if (isIdentity) {
      newProject = newInput;
    } else if (
        newInput == node->input() &&
        survivingExprs.size() == node->exprs().size()) {
      newProject = node;
    } else {
      newProject = builder().make<Project>(
          {newInput, std::move(survivingExprs), std::move(survivingOutputs)});
    }
    return maybeWrapFilter(newProject, std::move(blocked));
  }

  // Aggregate: conjuncts referencing only grouping-key outputs push below
  // (substituted to the grouping-key expressions); conjuncts referencing
  // any aggregate output stay above as a HAVING-equivalent Filter.
  //
  // An aggregate that emits a row on empty input — a global aggregate (no
  // grouping keys), or one with a global (empty) grouping set — blocks
  // pushdown entirely: the aggregate manufactures that row regardless of its
  // input, so a filter pushed below it can't suppress the row it was meant to
  // remove. A predicate on a key that GroupId NULLs for some set is handled by
  // the GroupId barrier (see rewriteGroupId), not here.
  //
  // Grouping keys stay; dropping one would re-shape the groups.
  NodeCP rewriteAggregate(const Aggregate* node, PushdownContext& context)
      override {
    const bool emitsRowOnEmptyInput =
        node->groupingKeys().empty() || !node->globalGroupingSets().empty();
    if (emitsRowOnEmptyInput) {
      PushdownContext childContext;
      childContext.required = context.required;
      childContext.required.unionColumns(context.pending);
      childContext.required.unionColumns(node->groupingKeys());
      for (const auto* aggregate : node->aggregates()) {
        childContext.required.unionColumns(aggregate);
      }
      childContext.requiredAbove = childContext.required;
      childContext.nonNullColumns = context.nonNullColumns;
      NodeCP newInput = rewrite(node->input(), childContext);
      NodeCP newAggregate = (newInput == node->input())
          ? static_cast<NodeCP>(node)
          : builder().make<Aggregate>(Aggregate::Key{
                .input = newInput,
                .groupingKeys = node->groupingKeys(),
                .aggregates = node->aggregates(),
                .outputColumns = node->outputColumns(),
                .step = node->step(),
                .groupId = node->groupId(),
                .globalGroupingSets = node->globalGroupingSets()});
      return maybeWrapFilter(newAggregate, std::move(context.pending));
    }

    const size_t numKeys = node->groupingKeys().size();
    ColumnVector groupingKeyOutputs(
        node->outputColumns().begin(), node->outputColumns().begin() + numKeys);
    PlanObjectSet aggregateOutputs;
    for (size_t i = numKeys; i < node->outputColumns().size(); ++i) {
      aggregateOutputs.add(node->outputColumns()[i]);
    }

    auto [substitutable, blocked] =
        partition(context.pending, aggregateOutputs);
    ExprVector pushable;
    pushable.reserve(substitutable.size());
    for (ExprCP conjunct : substitutable) {
      ExprCP substituted =
          exprs_.substitute(conjunct, groupingKeyOutputs, node->groupingKeys());
      if (simplifier_.simplifyFilter(substituted, pushable)) {
        return makeEmptyValues(node);
      }
    }

    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);
    AggregateCallVector survivingAggregates;
    ColumnVector survivingOutputs(groupingKeyOutputs);
    survivingAggregates.reserve(node->aggregates().size());
    survivingOutputs.reserve(node->outputColumns().size());
    for (size_t i = 0; i < node->aggregates().size(); ++i) {
      const ColumnCP outputColumn = node->outputColumns()[numKeys + i];
      if (outputsKept.contains(outputColumn)) {
        survivingAggregates.push_back(node->aggregates()[i]);
        survivingOutputs.push_back(outputColumn);
      }
    }

    PushdownContext childContext;
    childContext.pending = std::move(pushable);
    childContext.required.unionColumns(node->groupingKeys());
    for (const auto* aggregate : survivingAggregates) {
      childContext.required.unionColumns(aggregate);
    }
    childContext.requiredAbove = childContext.required;
    childContext.required.unionColumns(childContext.pending);
    childContext.nonNullColumns = context.nonNullColumns;
    NodeCP newInput = rewrite(node->input(), childContext);

    const bool unchanged = newInput == node->input() &&
        survivingAggregates.size() == node->aggregates().size();
    NodeCP newAggregate = unchanged
        ? static_cast<NodeCP>(node)
        : builder().make<Aggregate>(Aggregate::Key{
              .input = newInput,
              .groupingKeys = node->groupingKeys(),
              .aggregates = std::move(survivingAggregates),
              .outputColumns = std::move(survivingOutputs),
              .step = node->step(),
              .groupId = node->groupId(),
              .globalGroupingSets = node->globalGroupingSets()});
    return maybeWrapFilter(newAggregate, std::move(blocked));
  }

  // Join: demote outer to inner when possible, then route each pending
  // conjunct to one of: left input, right input, join filter, or
  // stay-above.
  NodeCP rewriteJoin(const Join* node, PushdownContext& context) override {
    PlanObjectSet leftColumns =
        PlanObjectSet::fromObjects(node->left()->outputColumns());
    PlanObjectSet rightColumns =
        PlanObjectSet::fromObjects(node->right()->outputColumns());

    // Fuse a consuming mark filter on a kLeftSemiProject into a filtering
    // kLeftSemiFilter / kAnti, dropping the mark. kLeftSemiFilter is never
    // null-aware; kAnti carries the node's flag (true for NOT IN).
    ColumnCP fusedMark{nullptr};
    bool fusedNullAware{false};
    velox::core::JoinType newKind;
    if (auto fusion =
            fuseMarkFilter(node, context.pending, context.requiredAbove)) {
      newKind = fusion->joinType;
      fusedMark = node->outputColumns().back();
      fusedNullAware =
          newKind == velox::core::JoinType::kAnti && node->nullAware();
      ExprVector remaining;
      remaining.reserve(context.pending.size() - 1);
      for (ExprCP conjunct : context.pending) {
        if (conjunct != fusion->conjunct) {
          remaining.push_back(conjunct);
        }
      }
      context.pending = std::move(remaining);
    } else {
      newKind = demoteOuterToInner(
          node->joinType(),
          context.pending,
          context.nonNullColumns,
          leftColumns,
          rightColumns);
    }

    // Inner-join equi-key pairs let pending conjuncts cross to the
    // other side. If derivation produces a literal-false conjunct, the
    // join can't yield any rows.
    if (newKind == velox::core::JoinType::kInner) {
      if (deriveTransitive(node, leftColumns, rightColumns, context.pending)) {
        return makeEmptyValues(node);
      }
    }

    ExprVector leftPending;
    ExprVector rightPending;
    ExprVector inFilter;
    ExprVector above;
    for (ExprCP conjunct : context.pending) {
      const auto& columns = conjunct->columns();
      // Pushable to a side only if every referenced column lives on
      // that side's input. The `!empty()` guard excludes constant
      // predicates — `isSubset` is vacuously true on an empty set, so
      // without it a constant would push to both sides.
      const bool leftOnly = !columns.empty() && columns.isSubset(leftColumns);
      const bool rightOnly = !columns.empty() && columns.isSubset(rightColumns);

      if (canPushLeft(newKind, leftOnly)) {
        leftPending.push_back(conjunct);
      } else if (canPushRight(newKind, rightOnly)) {
        rightPending.push_back(conjunct);
      } else if (newKind == velox::core::JoinType::kInner) {
        inFilter.push_back(conjunct);
      } else {
        above.push_back(conjunct);
      }
    }
    context.pending.clear();

    // Redistribute the join's own match conjuncts: a conjunct referencing
    // only one input moves into that input when sound for the kind (see
    // joinFilterTarget); the rest stay as the join filter.
    ExprVector keptFilter;
    keptFilter.reserve(node->filter().size());
    for (ExprCP conjunct : node->filter()) {
      const auto& columns = conjunct->columns();
      const bool leftOnly = !columns.empty() && columns.isSubset(leftColumns);
      const bool rightOnly = !columns.empty() && columns.isSubset(rightColumns);
      switch (joinFilterTarget(newKind, leftOnly, rightOnly)) {
        case FilterTarget::kLeft:
          leftPending.push_back(conjunct);
          break;
        case FilterTarget::kRight:
          rightPending.push_back(conjunct);
          break;
        case FilterTarget::kKeep:
          keptFilter.push_back(conjunct);
          break;
      }
    }

    // An OR conjunct spanning both sides yields per-side derived
    // pre-filters; the original OR stays on the join. Push a derived
    // filter only to the non-preserved side: pushing into the
    // preserved side would drop rows that should be NULL-padded.
    const bool pushLeftSide = newKind == velox::core::JoinType::kInner ||
        newKind == velox::core::JoinType::kRight;
    const bool pushRightSide = newKind == velox::core::JoinType::kInner ||
        newKind == velox::core::JoinType::kLeft;
    if (pushLeftSide || pushRightSide) {
      ExprFactory factory(builder());
      auto deriveAndPush = [&](ExprCP conjunct) {
        auto [leftSide, rightSide] = deriveSideFiltersFromOr(
            conjunct, leftColumns, rightColumns, factory);
        if (pushLeftSide && leftSide != nullptr) {
          leftPending.push_back(leftSide);
        }
        if (pushRightSide && rightSide != nullptr) {
          rightPending.push_back(rightSide);
        }
      };
      for (ExprCP conjunct : keptFilter) {
        deriveAndPush(conjunct);
      }
      for (ExprCP conjunct : inFilter) {
        deriveAndPush(conjunct);
      }
    }

    ExprVector newLeftKeys = node->leftKeys();
    ExprVector newRightKeys = node->rightKeys();
    if (!inFilter.empty()) {
      JoinCondition::Split split =
          JoinCondition::splitEquiKeys(inFilter, leftColumns, rightColumns);
      appendAll(newLeftKeys, split.leftKeys);
      appendAll(newRightKeys, split.rightKeys);
      inFilter = std::move(split.residual);
    }

    ExprVector newFilter = std::move(keptFilter);
    appendAll(newFilter, inFilter);

    // The join's own output is what consumers above demand plus columns
    // read by a Filter fused above it; keys and filter columns are
    // demanded of the children via `sideRequired`.
    PlanObjectSet outputsKept = context.requiredAbove;
    outputsKept.unionColumns(above);
    ColumnVector newOutputColumns;
    newOutputColumns.reserve(node->outputColumns().size());
    for (ColumnCP column : node->outputColumns()) {
      // Drop the fused mark: its only consumer was the conjunct that just
      // became the filtering join's semantics.
      if (column != fusedMark && outputsKept.contains(column)) {
        newOutputColumns.push_back(column);
      }
    }

    PlanObjectSet sideRequired = PlanObjectSet::fromObjects(newOutputColumns);
    sideRequired.unionColumns(newLeftKeys);
    sideRequired.unionColumns(newRightKeys);
    sideRequired.unionColumns(newFilter);

    // For inner joins, expose columns proven non-NULL by this
    // join's equi-keys as hints to descendants so a deeper outer
    // can be demoted. A key side contributes its columns only when
    // its expression has default null behavior — otherwise null
    // values can leak through it.
    PlanObjectSet childNonNullColumns = context.nonNullColumns;
    if (newKind == velox::core::JoinType::kInner) {
      auto addIfDefaultNull = [&](ExprCP key) {
        if (!key->functions().contains(FunctionSet::kNonDefaultNullBehavior)) {
          childNonNullColumns.unionColumns(key);
        }
      };
      for (size_t i = 0; i < newLeftKeys.size(); ++i) {
        addIfDefaultNull(newLeftKeys[i]);
        addIfDefaultNull(newRightKeys[i]);
      }
    }

    PushdownContext leftContext;
    leftContext.pending = std::move(leftPending);
    leftContext.required = sideRequired;
    // Demand from above each side excludes the conjuncts pushed into that
    // side's `pending`; it is exactly `sideRequired` (this join's kept
    // outputs, keys, and filter columns).
    leftContext.requiredAbove = sideRequired;
    leftContext.required.unionColumns(leftContext.pending);
    leftContext.nonNullColumns = childNonNullColumns;
    PushdownContext rightContext;
    rightContext.pending = std::move(rightPending);
    rightContext.required = sideRequired;
    rightContext.requiredAbove = sideRequired;
    rightContext.required.unionColumns(rightContext.pending);
    rightContext.nonNullColumns = std::move(childNonNullColumns);

    NodeCP newLeft = rewrite(node->left(), leftContext);
    NodeCP newRight = rewrite(node->right(), rightContext);

    NodeCP newJoin = (newLeft == node->left() && newRight == node->right() &&
                      newFilter.size() == node->filter().size() &&
                      newKind == node->joinType() &&
                      newLeftKeys.size() == node->leftKeys().size() &&
                      newOutputColumns.size() == node->outputColumns().size())
        ? static_cast<NodeCP>(node)
        : builder().make<Join>(
              {newLeft,
               newRight,
               newKind,
               std::move(newLeftKeys),
               std::move(newRightKeys),
               std::move(newFilter),
               fusedMark != nullptr ? fusedNullAware : node->nullAware(),
               node->nullAsValue(),
               std::move(newOutputColumns)});
    return maybeWrapFilter(newJoin, std::move(above));
  }

  // Scan: append pending conjuncts to `Scan.filters`; Emit decides
  // what the connector absorbs. `outputColumns` is the strict consumer-
  // required set; Emit folds in any columns referenced by retained
  // filters and projects them away above the scan.
  NodeCP rewriteScan(const Scan* node, PushdownContext& context) override {
    ExprVector mergedFilters = node->filters();
    appendAll(mergedFilters, context.pending);
    context.pending.clear();

    ColumnVector survivingOutputs;
    survivingOutputs.reserve(node->outputColumns().size());
    for (ColumnCP column : node->outputColumns()) {
      if (context.requiredAbove.contains(column)) {
        survivingOutputs.push_back(column);
      }
    }

    const bool unchanged = mergedFilters.size() == node->filters().size() &&
        survivingOutputs.size() == node->outputColumns().size();
    if (unchanged) {
      return node;
    }
    return builder().make<Scan>(
        {node->baseTable(),
         std::move(survivingOutputs),
         std::move(mergedFilters)});
  }

  NodeCP rewriteValues(const Values* node, PushdownContext& context) override {
    // Keep every column the output needs (`required`) plus every column a
    // pending conjunct reads: maybeWrapFilter re-materializes those conjuncts
    // as a Filter over this Values, so their columns must survive.
    PlanObjectSet keep = context.required;
    keep.unionColumns(context.pending);

    ColumnVector survivingOutputs;
    QGVector<velox::column_index_t> survivingChannels;
    const auto& outputColumns = node->outputColumns();
    const auto& channels = node->channels();
    for (size_t i = 0; i < outputColumns.size(); ++i) {
      if (keep.contains(outputColumns[i])) {
        survivingOutputs.push_back(outputColumns[i]);
        survivingChannels.push_back(channels[i]);
      }
    }

    NodeCP result = node;
    if (survivingOutputs.size() != outputColumns.size()) {
      result = builder().make<Values>(
          {node->source(),
           node->rows(),
           std::move(survivingOutputs),
           std::move(survivingChannels)});
    }
    return maybeWrapFilter(result, std::move(context.pending));
  }

  // Internal nodes — recurse with empty pending via `blockAt`:
  NodeCP rewriteLimit(const Limit* node, PushdownContext& context) override {
    return blockAt(context, [&](PushdownContext& empty) {
      return NodeRewriter::rewriteLimit(node, empty);
    });
  }

  // Sort: pending passes through unchanged — order has no row-set
  // effect. The sort keys must survive in the child's outputs.
  NodeCP rewriteSort(const Sort* node, PushdownContext& context) override {
    context.required.unionColumns(node->orderKeys());
    context.requiredAbove.unionColumns(node->orderKeys());
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder().make<Sort>(
        {newInput, node->orderKeys(), node->orderTypes()});
  }

  // TopN: a filter barrier like Limit (its bound depends on the row set), and
  // its order keys must survive in the child like Sort.
  NodeCP rewriteTopN(const TopN* node, PushdownContext& context) override {
    return blockAt(context, [&](PushdownContext& empty) {
      empty.required.unionColumns(node->orderKeys());
      empty.requiredAbove.unionColumns(node->orderKeys());
      return NodeRewriter::rewriteTopN(node, empty);
    });
  }

  // GroupId is a barrier: a conjunct on a key GroupId NULLs for some set can't
  // push below it. The input must supply the key expressions GroupId replicates
  // on and the columns the aggregates read (passed through).
  NodeCP rewriteGroupId(const GroupId* node, PushdownContext& context)
      override {
    return blockAt(context, [&](PushdownContext& empty) {
      empty.required.unionColumns(node->groupingKeys());
      empty.required.unionColumns(node->aggregationInputs());
      empty.requiredAbove.unionColumns(node->groupingKeys());
      empty.requiredAbove.unionColumns(node->aggregationInputs());
      return NodeRewriter::rewriteGroupId(node, empty);
    });
  }

  // MarkDistinct: conjuncts referencing only input columns push below.
  // Conjuncts referencing any output marker stay above — the marker
  // depends on the full input set, so pre-filtering changes which rows
  // get marked.
  NodeCP rewriteMarkDistinct(const MarkDistinct* node, PushdownContext& context)
      override {
    const PlanObjectSet markers = PlanObjectSet::fromObjects(node->markers());
    auto [pushable, blocked] = partition(context.pending, markers);
    // Drop the whole MarkDistinct when no consumer reads any marker.
    // Partial-drop is unsafe: the constructor pairs markers with masks
    // and requires the pairing.
    PlanObjectSet markersKept = context.required;
    markersKept.unionColumns(blocked);
    bool anyMarkerNeeded = false;
    for (ColumnCP marker : node->markers()) {
      if (markersKept.contains(marker)) {
        anyMarkerNeeded = true;
        break;
      }
    }
    PushdownContext childContext =
        makeChildContext(std::move(pushable), context);
    childContext.required.unionColumns(blocked);
    if (!anyMarkerNeeded) {
      childContext.requiredAbove = childContext.required;
      NodeCP newInput = rewrite(node->input(), childContext);
      return maybeWrapFilter(newInput, std::move(blocked));
    }
    childContext.required.unionColumns(node->distinctKeys());
    childContext.required.unionObjects(node->masks());
    childContext.requiredAbove = childContext.required;
    NodeCP newInput = rewrite(node->input(), childContext);
    NodeCP newNode = (newInput == node->input()) ? static_cast<NodeCP>(node)
                                                 : builder().make<MarkDistinct>(
                                                       {newInput,
                                                        node->markers(),
                                                        node->distinctKeys(),
                                                        node->masks(),
                                                        node->outputColumns()});
    return maybeWrapFilter(newNode, std::move(blocked));
  }

  // Unnest: conjuncts referencing only replicated (pre-unnest) columns
  // push below. Conjuncts that touch any unnest-produced column or the
  // ordinality column stay above — those don't exist on the input.
  NodeCP rewriteUnnest(const Unnest* node, PushdownContext& context) override {
    PlanObjectSet outputOnlyColumns;
    for (const ColumnVector& perExpression : node->unnestColumns()) {
      outputOnlyColumns.unionObjects(perExpression);
    }
    if (node->ordinalityColumn() != nullptr) {
      outputOnlyColumns.add(node->ordinalityColumn());
    }
    auto [pushable, blocked] = partition(context.pending, outputOnlyColumns);

    // Columns this Unnest must still produce: those the consumer requires plus
    // the columns read by conjuncts that stay above.
    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);

    // Drop the replicated (pass-through) columns and ordinality column the
    // consumer no longer needs. These must leave the replicated/ordinality
    // fields and the output set together: Unnest requires both to appear in
    // outputColumns. Columns still read by pushed-down conjuncts remain
    // required of the input via 'pushable'.
    ColumnVector survivingReplicated;
    survivingReplicated.reserve(node->replicatedColumns().size());
    for (ColumnCP column : node->replicatedColumns()) {
      if (outputsKept.contains(column)) {
        survivingReplicated.push_back(column);
      }
    }
    ColumnCP survivingOrdinality = node->ordinalityColumn() != nullptr &&
            outputsKept.contains(node->ordinalityColumn())
        ? node->ordinalityColumn()
        : nullptr;

    PushdownContext childContext =
        makeChildContext(std::move(pushable), context);
    childContext.required.unionColumns(node->unnestExpressions());
    childContext.required.unionObjects(survivingReplicated);
    childContext.required.unionColumns(blocked);
    childContext.requiredAbove = childContext.required;
    NodeCP newInput = rewrite(node->input(), childContext);

    // Unnest accepts a subset of structured-field outputs as
    // `outputColumns`; drop entries the consumer doesn't need.
    ColumnVector survivingOutputs;
    survivingOutputs.reserve(node->outputColumns().size());
    for (ColumnCP column : node->outputColumns()) {
      if (outputsKept.contains(column)) {
        survivingOutputs.push_back(column);
      }
    }

    const bool unchanged = newInput == node->input() &&
        survivingOutputs.size() == node->outputColumns().size();
    NodeCP newNode = unchanged ? static_cast<NodeCP>(node)
                               : builder().make<Unnest>(
                                     {newInput,
                                      node->unnestExpressions(),
                                      std::move(survivingReplicated),
                                      node->unnestColumns(),
                                      survivingOrdinality,
                                      std::move(survivingOutputs)});
    return maybeWrapFilter(newNode, std::move(blocked));
  }

  // UnionAll: each leg receives the same conjuncts rewritten in terms
  // of that leg's input columns.
  NodeCP rewriteUnionAll(const UnionAll* node, PushdownContext& context)
      override {
    // Positions to keep: outputColumns[i] required by the consumer.
    std::vector<size_t> keptPositions;
    keptPositions.reserve(node->outputColumns().size());
    for (size_t i = 0; i < node->outputColumns().size(); ++i) {
      if (context.required.contains(node->outputColumns()[i])) {
        keptPositions.push_back(i);
      }
    }
    ColumnVector newOutputColumns;
    newOutputColumns.reserve(keptPositions.size());
    for (size_t i : keptPositions) {
      newOutputColumns.push_back(node->outputColumns()[i]);
    }

    NodeVector newInputs;
    newInputs.reserve(node->inputs().size());
    QGVector<ColumnVector> newLegColumns;
    newLegColumns.reserve(node->inputs().size());
    bool changed = keptPositions.size() != node->outputColumns().size();
    for (size_t legIndex = 0; legIndex < node->inputs().size(); ++legIndex) {
      const ColumnVector& legColumns = node->legColumns()[legIndex];
      const ExprVector legAsExprs(legColumns.begin(), legColumns.end());
      ExprVector legPending =
          exprs_.substitute(context.pending, node->outputColumns(), legAsExprs);
      ColumnVector newLegCols;
      newLegCols.reserve(keptPositions.size());
      for (size_t i : keptPositions) {
        newLegCols.push_back(legColumns[i]);
      }
      newLegColumns.push_back(newLegCols);

      PushdownContext legContext;
      legContext.pending = std::move(legPending);
      legContext.required.unionObjects(newLegCols);
      legContext.required.unionColumns(legContext.pending);
      legContext.requiredAbove = legContext.required;
      legContext.nonNullColumns = context.nonNullColumns;
      NodeCP newLeg = rewrite(node->inputs()[legIndex], legContext);
      changed |= (newLeg != node->inputs()[legIndex]);
      newInputs.push_back(newLeg);
    }
    context.pending.clear();
    if (!changed) {
      return node;
    }
    return builder().make<UnionAll>(
        {std::move(newInputs),
         std::move(newLegColumns),
         std::move(newOutputColumns)});
  }

  // Window: conjuncts whose columns are all direct partition-key
  // columns push below — within a partition those values are
  // constant, so pre/post-filter are equivalent. All others stay
  // above.
  NodeCP rewriteWindow(const Window* node, PushdownContext& context) override {
    // A `rankColumn <op> n` predicate over a single ranking function fuses the
    // Window into a TopNRowNumber, consuming that predicate.
    const std::optional<RankFusion> fusion =
        detectRankFusion(node, context.pending, builder().functionNames());

    PlanObjectSet partitionKeyColumns;
    for (ExprCP key : node->partitionKeys()) {
      if (key->is(PlanType::kColumnExpr)) {
        partitionKeyColumns.add(key->as<Column>());
      }
    }

    // Conjuncts referencing only partition keys push below; the rest stay above
    // (or, for the fused ranking predicate, are consumed).
    ExprVector pushable;
    ExprVector blocked;
    for (ExprCP conjunct : context.pending) {
      if (fusion && conjunct == fusion->predicate) {
        continue;
      }
      const auto& columns = conjunct->columns();
      // Exclude constant predicates: `isSubset` is vacuously true on an
      // empty set, so without `!empty()` a constant would push.
      if (!columns.empty() && columns.isSubset(partitionKeyColumns)) {
        pushable.push_back(conjunct);
      } else {
        blocked.push_back(conjunct);
      }
    }

    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);
    const size_t numInputCols = node->input()->outputColumns().size();

    PushdownContext childContext;
    childContext.pending = std::move(pushable);
    childContext.required = context.required;
    childContext.required.unionColumns(node->partitionKeys());
    childContext.required.unionColumns(node->orderKeys());
    // Window/TopNRowNumber emit every input column; the child must keep
    // producing them all.
    childContext.required.unionObjects(node->input()->outputColumns());
    childContext.required.unionColumns(blocked);
    childContext.nonNullColumns = context.nonNullColumns;

    if (fusion) {
      childContext.required.unionColumns(childContext.pending);
      childContext.requiredAbove = childContext.required;
      NodeCP newInput = rewrite(node->input(), childContext);
      // Emit the rank column only when a consumer above still needs it.
      const ColumnCP rankColumn = outputsKept.contains(fusion->rankColumn)
          ? fusion->rankColumn
          : nullptr;
      ColumnVector newOutputColumns;
      appendAll(newOutputColumns, newInput->outputColumns());
      if (rankColumn != nullptr) {
        newOutputColumns.push_back(rankColumn);
      }
      NodeCP topN = builder().make<TopNRowNumber>(
          {newInput,
           fusion->rankFunction,
           node->partitionKeys(),
           node->orderKeys(),
           node->orderTypes(),
           fusion->limit,
           rankColumn,
           std::move(newOutputColumns)});
      return maybeWrapFilter(topN, std::move(blocked));
    }

    WindowFunctions survivingFunctions;
    ColumnVector survivingFunctionOutputs;
    for (size_t i = 0; i < node->functions().size(); ++i) {
      const ColumnCP funcOutput = node->outputColumns()[numInputCols + i];
      if (outputsKept.contains(funcOutput)) {
        survivingFunctions.push_back(node->functions()[i]);
        survivingFunctionOutputs.push_back(funcOutput);
      }
    }
    for (const auto& function : survivingFunctions) {
      childContext.required.unionColumns(function.call);
    }
    childContext.required.unionColumns(childContext.pending);
    childContext.requiredAbove = childContext.required;
    NodeCP newInput = rewrite(node->input(), childContext);

    ColumnVector newOutputColumns;
    newOutputColumns.reserve(numInputCols + survivingFunctions.size());
    appendAll(newOutputColumns, newInput->outputColumns());
    appendAll(newOutputColumns, survivingFunctionOutputs);

    const bool unchanged = newInput == node->input() &&
        survivingFunctions.size() == node->functions().size();
    NodeCP newWindow = unchanged ? static_cast<NodeCP>(node)
                                 : builder().make<Window>(
                                       {newInput,
                                        std::move(survivingFunctions),
                                        node->partitionKeys(),
                                        node->orderKeys(),
                                        node->orderTypes(),
                                        std::move(newOutputColumns)});
    return maybeWrapFilter(newWindow, std::move(blocked));
  }

  NodeCP rewriteApply(const Apply* /*node*/, PushdownContext& /*context*/)
      override {
    VELOX_FAIL("Apply must be removed by decorrelate before pushdown");
  }

  NodeCP rewriteEnforceSingleRow(
      const EnforceSingleRow* node,
      PushdownContext& context) override {
    return blockAt(context, [&](PushdownContext& empty) {
      return NodeRewriter::rewriteEnforceSingleRow(node, empty);
    });
  }

  // AssignUniqueId: conjuncts on input columns push freely — the id
  // sequence regenerates over the surviving rows. Conjuncts on the id
  // column stay above. If no consumer reads the id, drop the node.
  NodeCP rewriteAssignUniqueId(
      const AssignUniqueId* node,
      PushdownContext& context) override {
    const PlanObjectSet idColumn = PlanObjectSet::single(node->idColumn());
    auto [pushable, blocked] = partition(context.pending, idColumn);
    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);

    PushdownContext childContext =
        makeChildContext(std::move(pushable), context);
    childContext.required.unionColumns(blocked);
    childContext.requiredAbove = childContext.required;
    if (!outputsKept.contains(node->idColumn())) {
      NodeCP newInput = rewrite(node->input(), childContext);
      return maybeWrapFilter(newInput, std::move(blocked));
    }

    NodeCP newInput = rewrite(node->input(), childContext);
    NodeCP newNode = (newInput == node->input())
        ? static_cast<NodeCP>(node)
        : builder().make<AssignUniqueId>({newInput, node->idColumn()});
    return maybeWrapFilter(newNode, std::move(blocked));
  }

  NodeCP rewriteEnforceDistinct(
      const EnforceDistinct* node,
      PushdownContext& context) override {
    PushdownContext childContext;
    childContext.required = context.required;
    childContext.required.unionColumns(context.pending);
    childContext.required.unionColumns(node->distinctKeys());
    childContext.requiredAbove = childContext.required;
    childContext.nonNullColumns = context.nonNullColumns;
    NodeCP newInput = rewrite(node->input(), childContext);
    NodeCP newNode = (newInput == node->input())
        ? static_cast<NodeCP>(node)
        : builder().make<EnforceDistinct>(
              {newInput, node->distinctKeys(), node->errorMessage()});
    return maybeWrapFilter(newNode, std::move(context.pending));
  }

  // A write is the plan root: nothing consumes its row-count output, so the
  // only demand on its input is the columns the write's value expressions read.
  // Recurse with exactly those required, or the input scan gets pruned to
  // nothing. There is no pending predicate to route — a root carries none.
  NodeCP rewriteTableWrite(const TableWrite* node, PushdownContext& context)
      override {
    VELOX_DCHECK(context.pending.empty());
    PushdownContext child;
    child.required.unionColumns(node->columnExprs());
    child.requiredAbove = child.required;
    child.nonNullColumns = context.nonNullColumns;
    return NodeRewriter::rewriteTableWrite(node, child);
  }

 private:
  // Builds a child `PushdownContext` whose required column set is
  // `parent.required` plus the columns referenced by any conjunct in
  // `pending`. Callers augment the result with the node's own column
  // reads before recursing.
  PushdownContext makeChildContext(
      ExprVector pending,
      const PushdownContext& parent) {
    PushdownContext child;
    child.pending = std::move(pending);
    child.required = parent.required;
    child.required.unionColumns(child.pending);
    // Conservative: callers that push a fusable mark down refine this.
    child.requiredAbove = parent.required;
    child.nonNullColumns = parent.nonNullColumns;
    return child;
  }

  // Invokes `recurse` with an empty-pending context — letting the
  // subtree rewrite under its own clean pending state, while still
  // propagating `required` — then wraps the caller's `context.pending`
  // as a Filter above the recursed result.
  template <typename Recurse>
  NodeCP blockAt(PushdownContext& context, Recurse recurse) {
    PushdownContext empty;
    empty.required = context.required;
    empty.required.unionColumns(context.pending);
    // Blocked conjuncts wrap as a Filter above the recursed subtree, so
    // everything is "above" it.
    empty.requiredAbove = empty.required;
    empty.nonNullColumns = context.nonNullColumns;
    NodeCP recursed = recurse(empty);
    return maybeWrapFilter(recursed, std::move(context.pending));
  }

  // Routes each pending conjunct to either the input (pushable) or a
  // Filter above the rebuilt node (blocked), based on whether the
  // conjunct touches any column in `outputOnlyColumns` — the set of
  // columns the node produces that don't exist on its input. `rebuild`
  // builds a new node from a rewritten input. Child required carries
  // every input-side column read by blocked conjuncts or pushable.
  template <typename SingleInputNode, typename Rebuild>
  NodeCP splitOnOutputColumns(
      const SingleInputNode* node,
      const PushdownContext& parent,
      const PlanObjectSet& outputOnlyColumns,
      Rebuild rebuild) {
    auto [pushable, blocked] = partition(parent.pending, outputOnlyColumns);
    PushdownContext childContext =
        makeChildContext(std::move(pushable), parent);
    childContext.required.unionColumns(blocked);
    childContext.requiredAbove = childContext.required;
    NodeCP newInput = rewrite(node->input(), childContext);
    NodeCP newNode = (newInput == node->input()) ? static_cast<NodeCP>(node)
                                                 : rebuild(newInput);
    return maybeWrapFilter(newNode, std::move(blocked));
  }

  NodeCP maybeWrapFilter(NodeCP body, ExprVector conjuncts) {
    if (conjuncts.empty()) {
      return body;
    }
    return builder().make<Filter>({body, std::move(conjuncts)});
  }

  // Builds an empty `Values` node with the same output schema as `node`.
  // Used to short-circuit a subtree when pushdown discovers a predicate
  // that folds to literal `false`.
  NodeCP makeEmptyValues(NodeCP node) {
    return builder().makeEmptyValues(node->outputColumns());
  }

  // Derives new pending conjuncts via each cross-side
  // `Column == Column` equi-pair: a conjunct that references one side
  // gets duplicated with that side's column substituted for the other
  // side's equivalent column, exposing a push to the other side.
  // Returns true if any derived conjunct simplifies to literal `false`,
  // signalling the caller to short-circuit the join to an empty result.
  bool deriveTransitive(
      JoinCP node,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      ExprVector& pending) {
    auto [leftKeys, rightKeys] =
        collectEquiColumnPairs(node, leftColumns, rightColumns);
    if (leftKeys.empty()) {
      return false;
    }

    const ExprVector leftAsExprs(leftKeys.begin(), leftKeys.end());
    const ExprVector rightAsExprs(rightKeys.begin(), rightKeys.end());

    ExprVector derived;
    for (ExprCP conjunct : pending) {
      ExprCP leftSubstituted =
          exprs_.substitute(conjunct, rightKeys, leftAsExprs);
      if (leftSubstituted != conjunct &&
          simplifier_.simplifyFilter(leftSubstituted, derived)) {
        return true;
      }
      ExprCP rightSubstituted =
          exprs_.substitute(conjunct, leftKeys, rightAsExprs);
      if (rightSubstituted != conjunct &&
          simplifier_.simplifyFilter(rightSubstituted, derived)) {
        return true;
      }
    }
    appendAll(pending, derived);
    return false;
  }

  ExprFactory exprs_;
  ExprSimplifier simplifier_;
};

} // namespace

NodeCP PushdownAndPrunePass::run(
    NodeCP root,
    Builder& builder,
    velox::core::ExpressionEvaluator& evaluator) {
  Pushdown pass{builder, evaluator};
  PushdownContext context;
  context.required = PlanObjectSet::fromObjects(root->outputColumns());
  context.requiredAbove = context.required;
  return pass.rewrite(root, context);
}

} // namespace facebook::axiom::optimizer::v2
