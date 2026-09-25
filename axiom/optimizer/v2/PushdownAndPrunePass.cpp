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

#include "axiom/optimizer/v2/PrecomputeProjections.h"

#include "axiom/optimizer/ToSubfield.h"
#include "axiom/optimizer/v2/ColumnAccess.h"
#include "axiom/optimizer/v2/JoinFilterRewriter.h"

#include "axiom/optimizer/v2/ScanHandle.h"

#include <limits>
#include <optional>

#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/ExprSimplifier.h"
#include "axiom/optimizer/v2/ImpliedFilters.h"
#include "axiom/optimizer/v2/JoinCondition.h"
#include "axiom/optimizer/v2/NodeRewriter.h"

#include <folly/container/F14Set.h>

namespace facebook::axiom::optimizer::v2 {
namespace {

// Per-call state for the pushdown visitor.
struct PushdownContext {
  // In collection mode, the visitor records predicates guaranteed by this
  // subtree without changing the plan or consulting a connector.
  bool collectingFilters{false};
  PlanObjectSet requestedFilterColumns;
  ExprVector collectedFilters;

  // True if replacing multiple rows that are identical after pruning with one
  // cannot affect consumers above. Distinct emitted rows and the difference
  // between zero and one row remain significant.
  bool mayDropDuplicates{false};

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

  // Per-partition row cap implied by a `Limit` or `TopN` directly above a
  // ranking `Window`: it keeps at most `offset + count` rows overall, so no
  // partition can contribute more than that many. Set only for that immediate
  // parent-child pair.
  std::optional<int32_t> rankLimit;

  // Set when the consumer lists its own output columns, so a `Scan` below may
  // leave the columns only a refused conjunct reads in its output rather than
  // adding a `Project` to drop them.
  bool consumerDropsExtraColumns{false};

  // `Column*`s whose values are guaranteed non-NULL in the current
  // subtree (derived from default-null-behavior equi-keys of an
  // ancestor inner join). Never inserted into the plan.
  PlanObjectSet nonNullColumns;

  // Expression identities established by joins in this subtree. A parent
  // applies them while rebuilding on the way back up.
  ExprFactory::ExprSubstitution outputRewrites;
};

// Drops rewrites whose expressions cannot be evaluated from 'outputColumns'.
void retainVisibleRewrites(
    ExprFactory::ExprSubstitution& rewrites,
    const ColumnVector& outputColumns) {
  const auto outputSet = PlanObjectSet::fromObjects(outputColumns);
  for (auto it = rewrites.begin(); it != rewrites.end();) {
    if (!outputSet.containsColumns(it->first) ||
        !outputSet.containsColumns(it->second)) {
      it = rewrites.erase(it);
    } else {
      ++it;
    }
  }
}

// Adds 'from' to 'into'. The same expression may be discovered through more
// than one child only when both discoveries choose the same replacement.
void mergeRewrites(
    ExprFactory::ExprSubstitution& into,
    const ExprFactory::ExprSubstitution& from) {
  for (const auto& [source, target] : from) {
    const auto [it, inserted] = into.emplace(source, target);
    VELOX_CHECK(
        inserted || it->second == target,
        "Conflicting rewrites for expression: {}",
        source->toString());
  }
}

// Repeats replacement because rebuilding a parent expression can expose the
// same identity again at a different nesting level.
ExprCP applyRewrites(
    ExprFactory& exprs,
    ExprCP expression,
    const ExprFactory::ExprSubstitution& rewrites) {
  folly::F14FastSet<ExprCP> visited;
  while (visited.insert(expression).second) {
    ExprCP rewritten = exprs.replace(expression, rewrites);
    if (rewritten == expression) {
      return expression;
    }
    expression = rewritten;
  }
  VELOX_FAIL("Join-key expression rewrites did not converge");
}

ExprVector applyRewrites(
    ExprFactory& exprs,
    const ExprVector& expressions,
    const ExprFactory::ExprSubstitution& rewrites) {
  ExprVector rewritten;
  rewritten.reserve(expressions.size());
  for (ExprCP expression : expressions) {
    rewritten.push_back(applyRewrites(exprs, expression, rewrites));
  }
  return rewritten;
}

// Rebuilds an aggregate call whose inputs contain a rewrite source.
optimizer::AggregateCP applyRewrites(
    ExprFactory& exprs,
    Builder& builder,
    optimizer::AggregateCP aggregate,
    const ExprFactory::ExprSubstitution& rewrites) {
  ExprVector arguments = applyRewrites(exprs, aggregate->args(), rewrites);
  ExprCP condition = applyRewrites(exprs, aggregate->condition(), rewrites);
  ExprVector orderKeys = applyRewrites(exprs, aggregate->orderKeys(), rewrites);
  optimizer::AggregateCP fallback = aggregate->fallback();
  if (fallback != nullptr) {
    fallback = applyRewrites(exprs, builder, fallback, rewrites);
  }
  if (arguments == aggregate->args() && condition == aggregate->condition() &&
      orderKeys == aggregate->orderKeys() &&
      fallback == aggregate->fallback()) {
    return aggregate;
  }
  FunctionSet functions = Call::unionArgFunctions(FunctionSet{}, arguments);
  if (aggregate->functions().contains(
          FunctionSet::kIgnoreDuplicatesAggregate)) {
    functions = functions | FunctionSet::kIgnoreDuplicatesAggregate;
  }
  if (aggregate->functions().contains(FunctionSet::kOrderSensitiveAggregate)) {
    functions = functions | FunctionSet::kOrderSensitiveAggregate;
  }
  return builder.makeAggregate(
      aggregate->name(),
      aggregate->value(),
      std::move(arguments),
      functions,
      aggregate->isDistinct(),
      condition,
      aggregate->intermediateType(),
      std::move(orderKeys),
      aggregate->orderTypes(),
      aggregate->specialKind(),
      fallback);
}

AggregateCallVector applyRewrites(
    ExprFactory& exprs,
    Builder& builder,
    const AggregateCallVector& aggregates,
    const ExprFactory::ExprSubstitution& rewrites) {
  AggregateCallVector rewritten;
  rewritten.reserve(aggregates.size());
  for (const auto* aggregate : aggregates) {
    rewritten.push_back(applyRewrites(exprs, builder, aggregate, rewrites));
  }
  return rewritten;
}

// Moves the child's identities upward when all referenced columns remain
// visible in the rebuilt output.
NodeCP propagateVisibleRewrites(
    PushdownContext& parent,
    PushdownContext& child,
    NodeCP output) {
  parent.outputRewrites = std::move(child.outputRewrites);
  retainVisibleRewrites(parent.outputRewrites, output->outputColumns());
  return output;
}

bool hasNonDefaultNullBehavior(ExprCP expression) {
  return expression->containsFunction(FunctionSet::kNonDefaultNullBehavior);
}

// Returns true if `conjunct` references at least one column in
// `columns` and contains no non-default-null-behavior sub-expression.
// Conservative — a true result implies null-rejection; a false result
// does not imply the opposite.
bool isNullRejecting(ExprCP conjunct, const PlanObjectSet& columns) {
  return conjunct->columns().hasIntersection(columns) &&
      !hasNonDefaultNullBehavior(conjunct);
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
      // A predicate that rejects nulls on one input eliminates the rows in
      // which that input is null-padded, i.e. the other input's unmatched
      // rows. So only the referenced input stays row-preserving: left ->
      // LEFT, right -> RIGHT, both -> INNER.
      const bool rejectsLeft = anyRejects(leftColumns);
      const bool rejectsRight = anyRejects(rightColumns);
      if (rejectsLeft && rejectsRight) {
        return velox::core::JoinType::kInner;
      }
      if (rejectsLeft) {
        return velox::core::JoinType::kLeft;
      }
      if (rejectsRight) {
        return velox::core::JoinType::kRight;
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
  ColumnCP mark = node->markColumn();
  VELOX_CHECK_NOT_NULL(mark);

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
  return leftOnly && Join::preservedSides(joinType).left;
}

// Returns the cross-side `Column == Column` equi-pairs reachable on
// `node` — either explicitly via `leftKeys`/`rightKeys` or as
// `eq(Column, Column)` conjuncts in the join's filter or in
// `extraConjuncts`, which a caller uses to include the pending conjuncts: a
// `WHERE` equality reaches this pass as a pending conjunct and only becomes
// the join's condition once the pass routes it there. Pairs are distinct;
// the same equality may be written more than once. The returned pair has
// equal-length `first` (left columns) and `second` (right columns) vectors.
std::pair<ColumnVector, ColumnVector> collectEquiColumnPairs(
    JoinCP node,
    const PlanObjectSet& leftColumns,
    const PlanObjectSet& rightColumns,
    const ExprVector& extraConjuncts = {}) {
  ColumnVector leftKeys;
  ColumnVector rightKeys;

  auto addPair = [&](ColumnCP left, ColumnCP right) {
    for (size_t i = 0; i < leftKeys.size(); ++i) {
      if (leftKeys[i] == left && rightKeys[i] == right) {
        return;
      }
    }
    leftKeys.push_back(left);
    rightKeys.push_back(right);
  };

  for (size_t i = 0; i < node->leftKeys().size(); ++i) {
    ExprCP leftKey = node->leftKeys()[i];
    ExprCP rightKey = node->rightKeys()[i];
    if (leftKey->is(PlanType::kColumnExpr) &&
        rightKey->is(PlanType::kColumnExpr)) {
      addPair(leftKey->as<Column>(), rightKey->as<Column>());
    }
  }

  const Name equalityName = toName(FunctionRegistry::instance()->equality());
  const auto addEqualityPair = [&](ExprCP conjunct) {
    if (!conjunct->is(PlanType::kCallExpr)) {
      return;
    }
    const Call* call = conjunct->as<Call>();
    if (call->name() != equalityName) {
      return;
    }
    ExprCP first = call->args()[0];
    ExprCP second = call->args()[1];
    if (!first->is(PlanType::kColumnExpr) ||
        !second->is(PlanType::kColumnExpr)) {
      return;
    }
    ColumnCP firstColumn = first->as<Column>();
    ColumnCP secondColumn = second->as<Column>();
    if (leftColumns.contains(firstColumn) &&
        rightColumns.contains(secondColumn)) {
      addPair(firstColumn, secondColumn);
    } else if (
        leftColumns.contains(secondColumn) &&
        rightColumns.contains(firstColumn)) {
      addPair(secondColumn, firstColumn);
    }
  };

  for (ExprCP conjunct : node->filter()) {
    addEqualityPair(conjunct);
  }
  for (ExprCP conjunct : extraConjuncts) {
    addEqualityPair(conjunct);
  }
  return {std::move(leftKeys), std::move(rightKeys)};
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

// A nondeterministic conjunct may not cross a node whose output row count
// differs from its input's: one evaluation on either side of the node stands
// in for a different number of evaluations on the other, which admits a
// different set of rows.
void blockNondeterministic(ExprVector& pushable, ExprVector& blocked) {
  std::erase_if(pushable, [&](ExprCP conjunct) {
    if (!conjunct->containsNonDeterministic()) {
      return false;
    }
    blocked.push_back(conjunct);
    return true;
  });
}

// Mirror of canPushLeft for the right input (from-above conjuncts).
bool canPushRight(velox::core::JoinType joinType, bool rightOnly) {
  return rightOnly && Join::preservedSides(joinType).right;
}

// Where a conjunct from a join's own match condition (its filter) that
// references only one input may move.
enum class FilterTarget { kKeep, kLeft, kRight };

// Describes safe propagation directions and whether every output row matched
// a row from the other input.
struct JoinFilterPropagation {
  bool leftToRight{false};
  bool rightToLeft{false};
  bool emitsOnlyMatchedRows{false};
};

// Returns the directions in which a predicate guaranteed by one input can
// restrict rows read from the other input.
JoinFilterPropagation filterPropagation(
    velox::core::JoinType joinType,
    bool nullAware) {
  using JoinType = velox::core::JoinType;
  switch (joinType) {
    case JoinType::kInner:
    case JoinType::kLeftSemiFilter:
    case JoinType::kCountingLeftSemiFilter:
    case JoinType::kRightSemiFilter:
      return {
          .leftToRight = true,
          .rightToLeft = true,
          .emitsOnlyMatchedRows = true,
      };
    case JoinType::kLeft:
      return {.leftToRight = true};
    case JoinType::kRight:
      return {.rightToLeft = true};
    // A NULL on the matching input affects the projected mark or anti result,
    // even when its key is outside a translated filter's range.
    case JoinType::kLeftSemiProject:
      return {.leftToRight = !nullAware};
    case JoinType::kRightSemiProject:
      return {.rightToLeft = !nullAware};
    case JoinType::kAnti:
      return {.leftToRight = !nullAware};
    case JoinType::kCountingAnti:
      return {.leftToRight = true};
    case JoinType::kRightAnti:
      return {.rightToLeft = !nullAware};
    case JoinType::kFull:
      return {};
    case JoinType::kNumJoinTypes:
      break;
  }
  VELOX_UNREACHABLE();
}

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

// A single-ranking-function `Window` that a specialized ranking node can
// replace, with the per-partition row cap that applies to it.
struct RankFusion {
  // The conjunct consumed as the cap, or null when the cap came from a bound
  // above the Window or there is no cap.
  ExprCP consumedPredicate;
  // The part of a consumed predicate that does not constrain the upper bound.
  ExprCP remainingPredicate;
  // The window function's output column (the rank value).
  ColumnCP rankColumn;
  velox::core::TopNRowNumberNode::RankFunction rankFunction;
  // Per-partition row cap; absent only for an unordered row_number with no
  // bound.
  std::optional<int32_t> limit;
};

std::optional<RankFusion> rankFusionFromPredicate(
    ExprCP predicate,
    ColumnCP rankColumn,
    velox::core::TopNRowNumberNode::RankFunction rankFunction,
    const FunctionNames& names,
    ExprFactory& exprs) {
  if (!predicate->is(PlanType::kCallExpr)) {
    return std::nullopt;
  }
  const Call* comparison = predicate->as<Call>();
  const auto& args = comparison->args();
  if (args.empty() || args[0] != rankColumn) {
    return std::nullopt;
  }

  const Name name = comparison->name();
  ExprCP remainingPredicate{nullptr};
  int64_t bound;
  if (name == names.between && args.size() == 3 &&
      args[1]->is(PlanType::kLiteralExpr) &&
      args[2]->is(PlanType::kLiteralExpr)) {
    const int64_t lower = integerValue(&args[1]->as<Literal>()->literal());
    bound = integerValue(&args[2]->as<Literal>()->literal());
    if (lower > 1) {
      remainingPredicate = exprs.makeGreaterThanOrEqual(rankColumn, args[1]);
    }
  } else if (args.size() == 2 && args[1]->is(PlanType::kLiteralExpr)) {
    bound = integerValue(&args[1]->as<Literal>()->literal());
  } else {
    return std::nullopt;
  }

  int64_t limit;
  if ((name == names.lte || name == names.between) && bound > 0) {
    limit = bound;
  } else if (name == names.lt && bound > 1) {
    limit = bound - 1;
  } else if (name == names.equality && bound == 1) {
    limit = 1;
  } else {
    return std::nullopt;
  }
  if (limit > std::numeric_limits<int32_t>::max()) {
    return std::nullopt;
  }
  return RankFusion{
      predicate,
      remainingPredicate,
      rankColumn,
      rankFunction,
      static_cast<int32_t>(limit)};
}

// Returns the specialization for a single ranking function (row_number / rank /
// dense_rank), taking its cap from a 'pending' conjunct on the rank column or
// from 'rankLimit'.
std::optional<RankFusion> detectRankFusion(
    const Window* node,
    const ExprVector& pending,
    std::optional<int32_t> rankLimit,
    const FunctionNames& names,
    ExprFactory& exprs) {
  if (node->functions().size() != 1) {
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

  // An unordered rank / dense_rank ties every row of a partition at rank 1,
  // which no specialized node computes.
  const bool ordered = !node->orderKeys().empty();
  if (!ordered &&
      rankFunction !=
          velox::core::TopNRowNumberNode::RankFunction::kRowNumber) {
    return std::nullopt;
  }

  for (ExprCP conjunct : pending) {
    if (auto fusion = rankFusionFromPredicate(
            conjunct, rankColumn, rankFunction, names, exprs)) {
      return fusion;
    }
  }

  if (ordered) {
    // A `Limit` above bounds every partition just as a rank predicate would.
    if (rankLimit.has_value()) {
      return RankFusion{/*consumedPredicate=*/nullptr,
                        /*remainingPredicate=*/nullptr,
                        rankColumn,
                        rankFunction,
                        rankLimit};
    }
    // An unbounded ordered ranking still has to sort each partition.
    return std::nullopt;
  }
  // An unordered row_number numbers rows as they arrive, so it specializes with
  // or without a bound.
  return RankFusion{/*consumedPredicate=*/nullptr,
                    /*remainingPredicate=*/nullptr,
                    rankColumn,
                    rankFunction,
                    /*limit=*/std::nullopt};
}

// With no ORDER BY every row of a partition ties, so `rank` and `dense_rank`
// are 1 for every row. Returns that column when 'node' computes such a
// constant ranking, else null.
ColumnCP constantRankColumn(const Window* node, const FunctionNames& names) {
  if (!node->orderKeys().empty() || node->functions().size() != 1) {
    return nullptr;
  }
  const ExprCP call = node->functions()[0].call;
  if (!call->is(PlanType::kCallExpr)) {
    return nullptr;
  }
  const Name name = call->as<Call>()->name();
  if (name != names.rank && name != names.denseRank) {
    return nullptr;
  }
  return node->outputColumns()[node->input()->outputColumns().size()];
}

// True when 'conjunct' compares 'rankColumn' with a literal and holds for a
// rank of 1 — with the rank constant, the predicate selects every row. Only
// (column, literal) order is matched: `Builder` canonicalizes a reversible
// comparison to put the literal second.
bool holdsAtRankOne(
    ExprCP conjunct,
    ColumnCP rankColumn,
    const FunctionNames& names) {
  if (!conjunct->is(PlanType::kCallExpr)) {
    return false;
  }
  const Call* comparison = conjunct->as<Call>();
  if (comparison->args().size() != 2 || comparison->args()[0] != rankColumn ||
      !comparison->args()[1]->is(PlanType::kLiteralExpr)) {
    return false;
  }
  const int64_t bound =
      integerValue(&comparison->args()[1]->as<Literal>()->literal());
  const Name name = comparison->name();
  if (name == names.equality) {
    return bound == 1;
  }
  if (name == names.lt) {
    return 1 < bound;
  }
  if (name == names.lte) {
    return 1 <= bound;
  }
  if (name == names.gt) {
    return 1 > bound;
  }
  if (name == names.gte) {
    return 1 >= bound;
  }
  return false;
}

// Top-down filter pushdown visitor. Every node kind is overridden
// explicitly so each kind decides whether and how to propagate pending
// conjuncts.

class Pushdown : public NodeRewriter<PushdownContext> {
 public:
  Pushdown(
      Builder& builder,
      velox::core::ExpressionEvaluator& evaluator,
      const OptimizerSession& session,
      PushdownAndPrunePass::ConnectorPushdown connectorPushdown,
      const ColumnVector& outputColumns)
      : NodeRewriter(builder),
        exprs_(builder),
        evaluator_(evaluator),
        session_(session),
        connectorPushdown_(connectorPushdown),
        simplifier_(builder, evaluator) {
    // The query returns these, so they are read whole however narrowly an
    // expression below reads them.
    for (ColumnCP column : outputColumns) {
      access_.add(column);
    }
  }

  using NodeRewriter::rewrite;

  // Records what each node reads on the way down, so that a node producing a
  // column is reached after the consumers whose paths it extends, and a Scan
  // sees the finished access when it negotiates with its connector.
  NodeCP rewrite(NodeCP node, PushdownContext& context) override {
    if (context.collectingFilters) {
      collectFilters(node, context);
      return node;
    }
    access_.add(*node);
    return narrowed(NodeRewriter::rewrite(node, context), context);
  }

 protected:
  // Predicates guaranteed by each join input and by the join output.
  struct CollectedJoinFilters {
    ExprVector leftInput;
    ExprVector rightInput;
    ExprVector output;
  };

  static void appendDistinct(
      ExprVector& destination,
      const ExprVector& source) {
    PlanObjectSet seen = PlanObjectSet::fromObjects(destination);
    for (ExprCP expression : source) {
      if (!seen.contains(expression)) {
        destination.push_back(expression);
        seen.add(expression);
      }
    }
  }

  static void appendDistinct(ExprVector& destination, ExprCP expression) {
    if (std::find(destination.begin(), destination.end(), expression) ==
        destination.end()) {
      destination.push_back(expression);
    }
  }

  // Runs one read-only collection walk and returns its response.
  ExprVector collectGuaranteedFilters(NodeCP node, PlanObjectSet requested) {
    PushdownContext context;
    context.collectingFilters = true;
    context.requestedFilterColumns = std::move(requested);
    rewrite(node, context);
    return std::move(context.collectedFilters);
  }

  void collectThroughInput(NodeCP input, PushdownContext& context) {
    PlanObjectSet requested;
    for (ColumnCP column : input->outputColumns()) {
      if (context.requestedFilterColumns.contains(column)) {
        requested.add(column);
      }
    }
    context.collectedFilters =
        collectGuaranteedFilters(input, std::move(requested));
  }

  // Produces every requested output-column combination for a filter whose
  // input columns may each have multiple aliases.
  static std::vector<ExprFactory::ExprSubstitution> aliasSubstitutions(
      ExprCP filter,
      const folly::F14FastMap<ColumnCP, ColumnVector>& outputAliases) {
    std::vector<ExprFactory::ExprSubstitution> substitutions(1);
    filter->columns().forEach<Column>([&](ColumnCP inputColumn) {
      const auto aliasIt = outputAliases.find(inputColumn);
      VELOX_DCHECK(aliasIt != outputAliases.end());

      auto partial = std::move(substitutions);
      substitutions.clear();
      substitutions.reserve(partial.size() * aliasIt->second.size());
      for (const auto& mapping : partial) {
        for (ColumnCP outputColumn : aliasIt->second) {
          auto extended = mapping;
          extended.emplace(inputColumn, outputColumn);
          substitutions.push_back(std::move(extended));
        }
      }
    });
    return substitutions;
  }

  // Maps predicates through direct column aliases. General expressions are
  // not invertible, so they form a conservative collection barrier.
  void collectThroughMapping(
      NodeCP input,
      const ExprVector& inputExpressions,
      const ColumnVector& outputColumns,
      PushdownContext& context) {
    folly::F14FastMap<ColumnCP, ColumnVector> outputAliases;
    PlanObjectSet requestedInputs;
    for (size_t i = 0; i < outputColumns.size(); ++i) {
      if (!context.requestedFilterColumns.contains(outputColumns[i]) ||
          !inputExpressions[i]->is(PlanType::kColumnExpr)) {
        continue;
      }
      ColumnCP inputColumn = inputExpressions[i]->as<Column>();
      requestedInputs.add(inputColumn);
      outputAliases[inputColumn].push_back(outputColumns[i]);
    }

    for (ExprCP filter :
         collectGuaranteedFilters(input, std::move(requestedInputs))) {
      for (const auto& mapping : aliasSubstitutions(filter, outputAliases)) {
        appendDistinct(
            context.collectedFilters, exprs_.replace(filter, mapping));
      }
    }
  }

  // Returns only predicates present on every union leg after mapping each
  // leg's columns to the union outputs.
  void collectFromUnion(const UnionAll* node, PushdownContext& context) {
    ExprVector common;
    bool firstInput{true};
    for (size_t i = 0; i < node->inputs().size(); ++i) {
      ExprVector inputExpressions(
          node->legColumns()[i].begin(), node->legColumns()[i].end());
      PushdownContext inputContext;
      inputContext.collectingFilters = true;
      inputContext.requestedFilterColumns = context.requestedFilterColumns;
      collectThroughMapping(
          node->inputs()[i],
          inputExpressions,
          node->outputColumns(),
          inputContext);
      if (firstInput) {
        common = std::move(inputContext.collectedFilters);
        firstInput = false;
      } else {
        const PlanObjectSet present =
            PlanObjectSet::fromObjects(inputContext.collectedFilters);
        std::erase_if(
            common, [&](ExprCP filter) { return !present.contains(filter); });
      }
    }
    context.collectedFilters = std::move(common);
  }

  // Collects predicates guaranteed to hold for every row emitted by `node`.
  void collectFilters(NodeCP node, PushdownContext& context) {
    switch (node->nodeType()) {
      case NodeType::kScan:
      case NodeType::kValues:
      case NodeType::kWorkingTable:
        return;
      case NodeType::kFilter: {
        const auto* filter = node->as<Filter>();
        collectThroughInput(filter->input(), context);
        for (ExprCP predicate : filter->predicates()) {
          ExprVector conjuncts;
          ExprFactory::flattenAnd(predicate, conjuncts);
          for (ExprCP conjunct : conjuncts) {
            if (!conjunct->containsNonDeterministic() &&
                !conjunct->columns().empty() &&
                conjunct->columns().isSubset(context.requestedFilterColumns)) {
              appendDistinct(context.collectedFilters, conjunct);
            }
          }
        }
        return;
      }
      case NodeType::kProject: {
        const auto* project = node->as<Project>();
        collectThroughMapping(
            project->input(),
            project->exprs(),
            project->outputColumns(),
            context);
        return;
      }
      case NodeType::kAggregate: {
        const auto* aggregate = node->as<Aggregate>();
        if (aggregate->groupingKeys().empty() ||
            !aggregate->globalGroupingSets().empty()) {
          return;
        }
        const size_t numKeys = aggregate->groupingKeys().size();
        collectThroughMapping(
            aggregate->input(),
            aggregate->groupingKeys(),
            ColumnVector(
                aggregate->outputColumns().begin(),
                aggregate->outputColumns().begin() + numKeys),
            context);
        return;
      }
      case NodeType::kUnionAll:
        collectFromUnion(node->as<UnionAll>(), context);
        return;
      case NodeType::kJoin: {
        const auto& filters = joinFilters(node->as<Join>());
        context.collectedFilters = filters.output;
        std::erase_if(context.collectedFilters, [&](ExprCP filter) {
          return !filter->columns().isSubset(context.requestedFilterColumns);
        });
        return;
      }
      case NodeType::kLimit:
      case NodeType::kSort:
      case NodeType::kTopN:
      case NodeType::kMarkDistinct:
      case NodeType::kUnnest:
      case NodeType::kWindow:
      case NodeType::kInference:
      case NodeType::kRowNumber:
      case NodeType::kTopNRowNumber:
      case NodeType::kAssignUniqueId:
      case NodeType::kEnforceDistinct:
      case NodeType::kExchange:
        collectThroughInput(node->inputs().front(), context);
        return;
      case NodeType::kGroupId:
      case NodeType::kApply:
      case NodeType::kEnforceSingleRow:
      case NodeType::kTableWrite:
      case NodeType::kFixedPoint:
        return;
    }
    VELOX_UNREACHABLE();
  }

  // Places repeated source columns in separate maps so every equivalent
  // target receives a derived filter.
  static std::vector<ExprFactory::ExprSubstitution> makeSubstitutions(
      const ColumnVector& sources,
      const ColumnVector& targets) {
    std::vector<ExprFactory::ExprSubstitution> result;
    folly::F14FastMap<ColumnCP, size_t> nextSubstitution;
    for (size_t i = 0; i < sources.size(); ++i) {
      const size_t index = nextSubstitution[sources[i]]++;
      if (index == result.size()) {
        result.emplace_back();
      }
      result[index].emplace(sources[i], targets[i]);
    }
    return result;
  }

  // Substitutes equivalent join-key columns into deterministic filters.
  // Returns true if a derived filter simplifies to false.
  bool deriveFilters(
      const ExprVector& filters,
      const ColumnVector& sources,
      const ColumnVector& targets,
      ExprVector& derived) {
    const auto substitutions = makeSubstitutions(sources, targets);
    for (ExprCP filter : filters) {
      if (filter->containsNonDeterministic()) {
        continue;
      }
      for (const auto& mapping : substitutions) {
        ExprCP substituted = exprs_.replace(filter, mapping);
        if (substituted != filter && !isSelfEquality(substituted) &&
            simplifier_.simplifyFilter(substituted, derived)) {
          return true;
        }
      }
    }
    return false;
  }

  // Appends derived filters that can be evaluated entirely by one input.
  void deriveForInput(
      const ExprVector& filters,
      const ColumnVector& sourceKeys,
      const ColumnVector& targetKeys,
      const PlanObjectSet& targetColumns,
      ExprVector& targetFilters) {
    ExprVector derived;
    if (deriveFilters(filters, sourceKeys, targetKeys, derived)) {
      appendDistinct(targetFilters, builder().makeBoolean(false));
      return;
    }
    std::erase_if(derived, [&](ExprCP filter) {
      return filter->columns().empty() ||
          !filter->columns().isSubset(targetColumns);
    });
    appendDistinct(targetFilters, derived);
  }

  // Caches predicates independently of a caller's requested columns. Map
  // presence records completion even when all three result vectors are empty.
  const CollectedJoinFilters& joinFilters(JoinCP node) {
    const auto found = collectedJoinFilters_.find(node);
    if (found != collectedJoinFilters_.end()) {
      return found->second;
    }

    const PlanObjectSet leftColumns =
        PlanObjectSet::fromObjects(node->left()->outputColumns());
    const PlanObjectSet rightColumns =
        PlanObjectSet::fromObjects(node->right()->outputColumns());
    auto [leftKeys, rightKeys] =
        collectEquiColumnPairs(node, leftColumns, rightColumns);

    CollectedJoinFilters collected;
    collected.leftInput = collectGuaranteedFilters(node->left(), leftColumns);
    collected.rightInput =
        collectGuaranteedFilters(node->right(), rightColumns);

    const auto preserved = Join::preservedSides(node->joinType());
    if (preserved.left) {
      appendDistinct(collected.output, collected.leftInput);
    }
    if (preserved.right) {
      appendDistinct(collected.output, collected.rightInput);
    }
    if (filterPropagation(node->joinType(), node->nullAware())
            .emitsOnlyMatchedRows) {
      if (preserved.right) {
        deriveForInput(
            collected.leftInput,
            leftKeys,
            rightKeys,
            rightColumns,
            collected.output);
      }
      if (preserved.left) {
        deriveForInput(
            collected.rightInput,
            rightKeys,
            leftKeys,
            leftColumns,
            collected.output);
      }
      for (ExprCP filter : node->filter()) {
        if (!filter->containsNonDeterministic() && !filter->columns().empty()) {
          appendDistinct(collected.output, filter);
        }
      }
    }

    return collectedJoinFilters_.emplace(node, std::move(collected))
        .first->second;
  }

  static void appendInputFilters(
      const ExprVector& filters,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      ExprVector& leftFilters,
      ExprVector& rightFilters) {
    for (ExprCP filter : filters) {
      if (filter->columns().empty()) {
        continue;
      }
      if (filter->columns().isSubset(leftColumns)) {
        appendDistinct(leftFilters, filter);
      } else if (filter->columns().isSubset(rightColumns)) {
        appendDistinct(rightFilters, filter);
      }
    }
  }

  // Propagates filters only toward an input whose rows the join may discard.
  void propagateAcrossJoin(
      JoinCP node,
      velox::core::JoinType joinType,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      const ExprVector& pending,
      ExprVector& leftPending,
      ExprVector& rightPending) {
    const auto propagation = filterPropagation(joinType, node->nullAware());
    if (!propagation.leftToRight && !propagation.rightToLeft) {
      return;
    }

    auto [leftKeys, rightKeys] =
        collectEquiColumnPairs(node, leftColumns, rightColumns, pending);
    if (leftKeys.empty()) {
      return;
    }

    const auto& collected = joinFilters(node);
    ExprVector leftFilters = collected.leftInput;
    ExprVector rightFilters = collected.rightInput;
    if (joinType != velox::core::JoinType::kInner) {
      appendInputFilters(
          pending, leftColumns, rightColumns, leftFilters, rightFilters);
    }
    appendInputFilters(
        node->filter(), leftColumns, rightColumns, leftFilters, rightFilters);

    if (propagation.rightToLeft) {
      deriveForInput(
          rightFilters, rightKeys, leftKeys, leftColumns, leftPending);
    }
    if (propagation.leftToRight) {
      deriveForInput(
          leftFilters, leftKeys, rightKeys, rightColumns, rightPending);
    }
  }

  // Adds the `Project` that `Node::emitsInputColumns` describes, here rather
  // than at the root, so the column stays out of everything in between.
  NodeCP narrowed(NodeCP node, const PushdownContext& context) {
    if (!node->emitsInputColumns()) {
      return node;
    }
    ColumnVector keep;
    for (ColumnCP column : node->outputColumns()) {
      if (context.required.contains(column)) {
        keep.push_back(column);
      }
    }
    // Nothing required reads this node, so the rows themselves are what a
    // consumer wants -- a semijoin's existence check, say. Narrowing to no
    // columns at all is not a plan Velox can run.
    if (keep.empty() || keep.size() == node->outputColumns().size()) {
      return node;
    }
    ExprVector exprs(keep.begin(), keep.end());
    return PrecomputeProjections::makeProject(
        node, std::move(exprs), keep, builder(), simplifier_);
  }

  // Filter conjuncts (flattened across AND trees) join `pending`; the
  // Filter node itself is dropped.
  NodeCP rewriteFilter(const Filter* node, PushdownContext& context) override {
    for (ExprCP predicate : node->predicates()) {
      ExprFactory::flattenAnd(predicate, context.pending);
    }
    // Forwards 'context' unchanged: this Filter dissolves into 'pending', so
    // whatever consumes the result is the consumer this node had.
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

    // A conjunct pushed below this Project reads the expressions it was
    // substituted into, and the projection producing them may not survive.
    access_.addAll(pushable);

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

    // Recorded here, not on the way down: an expression this pass prunes must
    // not contribute the paths it reads.
    for (size_t i = 0; i < survivingExprs.size(); ++i) {
      access_.addProducing(survivingExprs[i], survivingOutputs[i]);
    }

    PushdownContext childContext;
    childContext.pending = std::move(pushable);
    childContext.required.unionColumns(survivingExprs);
    // Demand from above the child excludes the conjuncts pushed into
    // `pending`; it is exactly what the surviving projections read.
    childContext.requiredAbove = childContext.required;
    childContext.required.unionColumns(childContext.pending);
    childContext.nonNullColumns = context.nonNullColumns;
    childContext.consumerDropsExtraColumns = true;
    childContext.mayDropDuplicates = context.mayDropDuplicates;
    for (ExprCP expr : survivingExprs) {
      if (expr->containsNonDeterministic()) {
        childContext.mayDropDuplicates = false;
        break;
      }
    }
    NodeCP newInput = rewrite(node->input(), childContext);
    applyOutputRewrites(childContext, survivingExprs, blocked);

    NodeCP newProject = PrecomputeProjections::makeProject(
        newInput,
        std::move(survivingExprs),
        std::move(survivingOutputs),
        builder(),
        simplifier_);
    const auto* project = newProject->as<Project>();

    // Drop the Project when its surviving outputs are a pure pass-through of
    // the (rewritten) input's output columns. Pushdown can make a Project
    // identity by pruning columns below it — e.g. a semi-project join fused
    // to a semi-filter no longer emits the mark this Project dropped. An
    // empty-output Project is never identity: it eliminates all columns (the
    // semi-join EXISTS gate), which the input does not guarantee on its own.
    const ColumnVector& inputColumns = project->input()->outputColumns();
    const auto& projectExprs = project->exprs();
    const auto& projectOutputs = project->outputColumns();
    bool isIdentity{
        !projectOutputs.empty() &&
        projectOutputs.size() == inputColumns.size()};
    for (size_t i = 0; isIdentity && i < projectOutputs.size(); ++i) {
      isIdentity = projectOutputs[i] == inputColumns[i] &&
          projectExprs[i] == inputColumns[i];
    }

    if (isIdentity) {
      newProject = project->input();
    }
    return propagateVisibleRewrites(
        context, childContext, maybeWrapFilter(newProject, std::move(blocked)));
  }

  // Aggregate: conjuncts referencing only grouping-key outputs push below
  // (substituted to the grouping-key expressions); conjuncts referencing
  // any aggregate output stay above as a HAVING-equivalent Filter, as do
  // nondeterministic conjuncts, since a group collapses rows.
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
      for (const auto* aggregate : node->aggregates()) {
        access_.add(aggregate);
      }

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
      AggregateCallVector aggregates = node->aggregates();
      applyOutputRewrites(childContext, aggregates);
      NodeCP newAggregate =
          (newInput == node->input() && aggregates == node->aggregates())
          ? static_cast<NodeCP>(node)
          : builder().make<Aggregate>(
                {.input = newInput,
                 .groupingKeys = node->groupingKeys(),
                 .aggregates = std::move(aggregates),
                 .outputColumns = node->outputColumns(),
                 .step = node->step(),
                 .groupId = node->groupId(),
                 .globalGroupingSets = node->globalGroupingSets()});
      return propagateVisibleRewrites(
          context,
          childContext,
          maybeWrapFilter(newAggregate, std::move(context.pending)));
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
    blockNondeterministic(substitutable, blocked);

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

    for (const auto* aggregate : survivingAggregates) {
      access_.add(aggregate);
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

    ExprVector groupingKeys = node->groupingKeys();
    applyOutputRewrites(childContext, groupingKeys, survivingAggregates);

    const bool unchanged = newInput == node->input() &&
        survivingAggregates == node->aggregates() &&
        groupingKeys == node->groupingKeys();
    NodeCP newAggregate = unchanged
        ? static_cast<NodeCP>(node)
        : builder().make<Aggregate>(
              {.input = newInput,
               .groupingKeys = std::move(groupingKeys),
               .aggregates = std::move(survivingAggregates),
               .outputColumns = std::move(survivingOutputs),
               .step = node->step(),
               .groupId = node->groupId(),
               .globalGroupingSets = node->globalGroupingSets()});
    return propagateVisibleRewrites(
        context,
        childContext,
        maybeWrapFilter(newAggregate, std::move(blocked)));
  }

  // True if a conjunct of 'conjuncts' is statically false, so no row passes.
  bool neverMatches(const ExprVector& conjuncts) {
    ExprVector kept;
    for (ExprCP conjunct : conjuncts) {
      kept.clear();
      if (simplifier_.simplifyFilter(conjunct, kept)) {
        return true;
      }
    }
    return false;
  }

  // A join condition no row satisfies decides the join's answer without a
  // join: an inner join produces nothing, and an outer join produces its
  // preserved side with the other side's columns NULL.
  //
  // Only a written ON clause reaches here as a false condition. A subquery
  // whose body is empty is a different shape, still joined against.
  NodeCP simplifyNeverMatchingJoin(const Join* node, PushdownContext& context) {
    if (!neverMatches(node->filter())) {
      return nullptr;
    }

    NodeCP preserved{nullptr};
    switch (node->joinType()) {
      case velox::core::JoinType::kInner:
        return makeEmptyValues(node);
      case velox::core::JoinType::kLeft:
        preserved = node->left();
        break;
      case velox::core::JoinType::kRight:
        preserved = node->right();
        break;
      default:
        // kFull preserves both sides, which is a union rather than one node.
        // A written ON clause reaches the semi and anti kinds as the body of
        // a subquery rather than as a join condition, so a false one does not
        // arrive here.
        return nullptr;
    }

    // The preserved side is asked only for the columns it passes through;
    // the rest of the join's output is NULL.
    const PlanObjectSet preservedColumns =
        PlanObjectSet::fromObjects(preserved->outputColumns());
    ExprVector expressions;
    expressions.reserve(node->outputColumns().size());
    PlanObjectSet kept;
    for (ColumnCP column : node->outputColumns()) {
      if (preservedColumns.contains(column)) {
        expressions.push_back(column);
        kept.add(column);
      } else {
        expressions.push_back(builder().makeNull(column->value().type));
      }
    }

    PushdownContext preservedContext;
    preservedContext.required = std::move(kept);
    preservedContext.requiredAbove = preservedContext.required;
    preservedContext.nonNullColumns = context.nonNullColumns;

    // The conjuncts waiting above the join read its output columns, some of
    // them from the side that is gone. They stay above the Project, which is
    // where the join's output now comes from.
    ExprVector above = std::move(context.pending);
    context.pending.clear();

    NodeCP newPreserved = rewrite(preserved, preservedContext);
    applyOutputRewrites(preservedContext, expressions, above);
    NodeCP project = builder().make<Project>(
        {newPreserved, std::move(expressions), node->outputColumns()});
    return propagateVisibleRewrites(
        context, preservedContext, maybeWrapFilter(project, std::move(above)));
  }

  // True if nothing reads 'mark': neither a consumer above nor a conjunct
  // still looking for a home here.
  static bool markIsDead(ColumnCP mark, const PushdownContext& context) {
    if (context.requiredAbove.contains(mark)) {
      return false;
    }
    return std::none_of(
        context.pending.begin(), context.pending.end(), [&](ExprCP conjunct) {
          return conjunct->columns().contains(mark);
        });
  }

  // Join: demote outer to inner when possible, then route each pending
  // conjunct to one of: left input, right input, join filter, or
  // stay-above. The join's own filter conjuncts are redistributed by the same
  // rules. Neither moves a nondeterministic conjunct into an input, which
  // would evaluate it once for rows the join then multiplies.
  NodeCP rewriteJoin(const Join* node, PushdownContext& context) override {
    if (NodeCP simplified = simplifyNeverMatchingJoin(node, context)) {
      return simplified;
    }

    // A kLeftSemiProject keeps every left row and adds a mark, so with the
    // mark read by nobody the join has no effect and its left input stands in
    // its place.
    if (velox::core::isLeftSemiProjectJoin(node->joinType()) &&
        markIsDead(node->markColumn(), context)) {
      return rewrite(node->left(), context);
    }

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
      fusedMark = node->markColumn();
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

    ExprVector leftPending;
    ExprVector rightPending;
    ExprVector inFilter;
    ExprVector above;

    propagateAcrossJoin(
        node,
        newKind,
        leftColumns,
        rightColumns,
        context.pending,
        leftPending,
        rightPending);

    if (newKind == velox::core::JoinType::kInner) {
      if (NodeCP replacement = rewriteConstantInputJoin(
              node,
              leftColumns,
              rightColumns,
              context,
              leftPending,
              rightPending)) {
        return replacement;
      }
      // Inner-join equi-key pairs let pending conjuncts cross to the
      // other side. If derivation produces a literal-false conjunct, the
      // join can't yield any rows.
      if (deriveTransitive(node, leftColumns, rightColumns, context.pending)) {
        return makeEmptyValues(node);
      }
    }

    for (ExprCP conjunct : context.pending) {
      const auto& columns = conjunct->columns();
      // Pushable to a side only if every referenced column lives on
      // that side's input. The `!empty()` guard excludes constant
      // predicates — `isSubset` is vacuously true on an empty set, so
      // without it a constant would push to both sides.
      const bool leftOnly = !columns.empty() && columns.isSubset(leftColumns);
      const bool rightOnly = !columns.empty() && columns.isSubset(rightColumns);

      // A nondeterministic conjunct must not move into an input: one evaluation
      // per input row would then decide every output row that row produces.
      if (conjunct->containsNonDeterministic() && (leftOnly || rightOnly)) {
        above.push_back(conjunct);
      } else if (canPushLeft(newKind, leftOnly)) {
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
      // Staying on the join is where a filter conjunct already is, so unlike
      // the loop above this needs no test for which inputs it references.
      if (conjunct->containsNonDeterministic()) {
        keptFilter.push_back(conjunct);
        continue;
      }
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
      ExprVector filters = keptFilter;
      appendAll(filters, inFilter);
      ExprFactory factory(builder());
      auto [leftFilters, rightFilters] = ImpliedFilters::deriveForJoinInputs(
          filters, leftColumns, rightColumns, factory);
      if (pushLeftSide) {
        appendAll(leftPending, leftFilters);
      }
      if (pushRightSide) {
        appendAll(rightPending, rightFilters);
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
        if (!hasNonDefaultNullBehavior(key)) {
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

    const auto allDeterministic = [](const ExprVector& expressions) {
      return std::ranges::none_of(expressions, [](ExprCP expression) {
        return expression->containsNonDeterministic();
      });
    };
    if (allDeterministic(newLeftKeys) && allDeterministic(newRightKeys) &&
        allDeterministic(newFilter)) {
      switch (newKind) {
        case velox::core::JoinType::kLeftSemiFilter:
        case velox::core::JoinType::kLeftSemiProject:
        case velox::core::JoinType::kAnti:
          rightContext.mayDropDuplicates = true;
          break;
        case velox::core::JoinType::kRightSemiFilter:
        case velox::core::JoinType::kRightSemiProject:
        case velox::core::JoinType::kRightAnti:
          leftContext.mayDropDuplicates = true;
          break;
        default:
          break;
      }
    }

    NodeCP newLeft = rewrite(node->left(), leftContext);
    NodeCP newRight = rewrite(node->right(), rightContext);

    ExprFactory::ExprSubstitution outputRewrites =
        std::move(leftContext.outputRewrites);
    mergeRewrites(outputRewrites, rightContext.outputRewrites);
    if (!outputRewrites.empty()) {
      newLeftKeys = applyRewrites(exprs_, newLeftKeys, outputRewrites);
      newRightKeys = applyRewrites(exprs_, newRightKeys, outputRewrites);
      newFilter = applyRewrites(exprs_, newFilter, outputRewrites);
    }
    addJoinKeyRewrites(newKind, newLeftKeys, newRightKeys, outputRewrites);
    if (!outputRewrites.empty()) {
      newFilter = applyRewrites(exprs_, newFilter, outputRewrites);
      above = applyRewrites(exprs_, above, outputRewrites);
    }

    // A cross join's filter is evaluated per pair of rows, so the parts of it
    // reading one side move into that side (see `JoinFilterRewriter`). Running
    // here, before distribution is chosen, the moved value is what gets
    // broadcast rather than the columns it reads.
    if (newLeftKeys.empty() && !newFilter.empty()) {
      PrecomputeProjections leftPrecompute{
          newLeft, builder(), simplifier_, /*projectAllInputs=*/false};
      PrecomputeProjections rightPrecompute{
          newRight, builder(), simplifier_, /*projectAllInputs=*/false};
      const auto leftColumns =
          PlanObjectSet::fromObjects(newLeft->outputColumns());
      const auto rightColumns =
          PlanObjectSet::fromObjects(newRight->outputColumns());

      // Each side keeps what the join emits before the moved expressions, so a
      // projection lists its passthrough columns first.
      for (ColumnCP column : newOutputColumns) {
        if (leftColumns.contains(column)) {
          leftPrecompute.toColumn(column);
        } else if (rightColumns.contains(column)) {
          rightPrecompute.toColumn(column);
        }
      }

      JoinFilterRewriter rewriter{
          leftPrecompute,
          rightPrecompute,
          leftColumns,
          rightColumns,
          builder()};
      newFilter = rewriter.rewrite(newFilter);
      newLeft = std::move(leftPrecompute).node();
      newRight = std::move(rightPrecompute).node();
    }

    NodeCP newJoin =
        (newLeft == node->left() && newRight == node->right() &&
         newFilter == node->filter() && newKind == node->joinType() &&
         newLeftKeys == node->leftKeys() && newRightKeys == node->rightKeys() &&
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
    NodeCP result = maybeWrapFilter(newJoin, std::move(above));
    retainVisibleRewrites(outputRewrites, result->outputColumns());
    context.outputRewrites = std::move(outputRewrites);
    return result;
  }

  // Scan: pending conjuncts reach the table, and the connector decides which
  // of them it will evaluate. The ones it rejects become a Filter over the
  // Scan, and the columns they read join the Scan's output.
  NodeCP rewriteScan(const Scan* node, PushdownContext& context) override {
    ExprVector filters = std::move(context.pending);
    context.pending.clear();
    PlanObjectSet impliedFilterSet;
    if (connectorPushdown_ == PushdownAndPrunePass::ConnectorPushdown::kOffer) {
      ExprVector impliedFilters;
      for (ExprCP derivedFilter :
           ImpliedFilters::deriveForColumns(filters, exprs_)) {
        if (simplifier_.simplifyFilter(derivedFilter, impliedFilters)) {
          return makeEmptyValues(node);
        }
      }

      PlanObjectSet offeredFilterSet = PlanObjectSet::fromObjects(filters);
      std::erase_if(impliedFilters, [&](ExprCP filter) {
        if (offeredFilterSet.contains(filter)) {
          return true;
        }
        offeredFilterSet.add(filter);
        return false;
      });
      impliedFilterSet.unionObjects(impliedFilters);
      appendAll(filters, impliedFilters);
    }

    ColumnVector survivingOutputs;
    survivingOutputs.reserve(node->outputColumns().size());
    for (ColumnCP column : node->outputColumns()) {
      if (context.requiredAbove.contains(column)) {
        survivingOutputs.push_back(column);
      }
    }

    ExprVector rejected;
    const ScanHandle* handle =
        negotiate(*node->baseTable(), survivingOutputs, filters, rejected);
    // Implied filters are optional pushdown opportunities because the
    // original filters that imply them remain in the plan.
    std::erase_if(rejected, [&](ExprCP filter) {
      return impliedFilterSet.contains(filter);
    });
    if (rejected.empty()) {
      return builder().make<Scan>(
          {node->baseTable(), std::move(survivingOutputs), handle});
    }

    // The rejected conjuncts are evaluated above the scan, so the scan must
    // read what they reference.
    PlanObjectSet rejectedColumns;
    rejectedColumns.unionColumns(rejected);
    ColumnVector scanOutputs = survivingOutputs;
    for (ColumnCP column : node->baseTable()->columns) {
      if (rejectedColumns.contains(column) &&
          std::find(scanOutputs.begin(), scanOutputs.end(), column) ==
              scanOutputs.end()) {
        scanOutputs.push_back(column);
      }
    }
    // The Filter this rewrite puts above the scan reads only columns the scan
    // produces.
    const PlanObjectSet scanOutputSet = PlanObjectSet::fromObjects(scanOutputs);
    rejectedColumns.forEach<Column>([&](ColumnCP column) {
      VELOX_CHECK(
          scanOutputSet.contains(column),
          "A rejected filter reads a column the scan does not produce: {}",
          column->name());
    });

    const bool readsExtraColumns = scanOutputs.size() > survivingOutputs.size();
    NodeCP scan = builder().make<Scan>(
        {node->baseTable(), std::move(scanOutputs), handle});
    NodeCP filter = builder().make<Filter>({scan, std::move(rejected)});
    if (!readsExtraColumns || context.consumerDropsExtraColumns) {
      return filter;
    }
    // The columns only the rejected conjuncts read stop here: nothing above
    // asked for them, and carrying them would widen every operator between
    // this scan and the query's output.
    ExprVector passThrough(survivingOutputs.begin(), survivingOutputs.end());
    return builder().make<Project>(
        {filter, std::move(passThrough), std::move(survivingOutputs)});
  }

  // Offers 'filters' to the connector for 'baseTable' read as 'outputColumns'
  // and returns the resulting handle, appending the conjuncts the connector
  // rejected to 'rejected'. Returns null, rejecting everything, when the
  // caller asked for no connector pushdown.
  const ScanHandle* negotiate(
      const BaseTable& baseTable,
      const ColumnVector& outputColumns,
      const ExprVector& filters,
      ExprVector& rejected) {
    if (connectorPushdown_ == PushdownAndPrunePass::ConnectorPushdown::kSkip) {
      appendAll(rejected, filters);
      return nullptr;
    }
    const auto it = negotiatedByBaseTableId_.find(baseTable.id());
    if (it != negotiatedByBaseTableId_.end()) {
      // A rewrite that duplicated a subtree can present one Scan twice. The
      // connector is asked once, so the second visit must be offering the same
      // predicates and reading no more columns; otherwise its predicates would
      // be silently dropped or it would read a column with no handle. The
      // paths are not compared: access accumulates as the walk descends, so
      // the second visit legitimately sees a superset of what the handle was
      // built for.
      VELOX_CHECK(
          it->second.filters == filters,
          "Two reads of one table offer the connector different filters: {}",
          baseTable.schemaTable->name());
      for (ColumnCP column : outputColumns) {
        VELOX_CHECK(
            it->second.handle->columnHandles.contains(column),
            "Two reads of one table read different columns: {}.{}",
            baseTable.schemaTable->name(),
            column->name());
      }
      appendAll(rejected, it->second.rejected);
      return it->second.handle;
    }
    ExprVector rejectedHere;
    const ScanHandle* handle = builder().takeScanHandle(
        ScanHandle::build(
            baseTable,
            outputColumns,
            filters,
            [this](ColumnCP column) {
              return toSubfields(
                  column->name(),
                  access_.subfieldsOf(column),
                  /*mapKeysAsFields=*/false);
            },
            session_,
            evaluator_,
            rejectedHere));
    appendAll(rejected, rejectedHere);
    negotiatedByBaseTableId_.emplace(
        baseTable.id(), Negotiated{handle, filters, std::move(rejectedHere)});
    return handle;
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
    return blockAt(context, [&](PushdownContext& empty) -> NodeCP {
      const int64_t cap = node->offsetPlusCount();
      if (node->input()->is(NodeType::kWindow) &&
          cap <= std::numeric_limits<int32_t>::max()) {
        empty.rankLimit = static_cast<int32_t>(cap);
      }
      NodeCP newInput = rewrite(node->input(), empty);

      // A row_number over one partition, capped at the same count, already
      // emits exactly the rows this limit would keep.
      if (node->offset() == 0 && newInput->is(NodeType::kTopNRowNumber)) {
        const auto* ranking = newInput->as<TopNRowNumber>();
        if (ranking->partitionKeys().empty() &&
            ranking->limit() == node->count() &&
            ranking->rankFunction() ==
                velox::core::TopNRowNumberNode::RankFunction::kRowNumber) {
          return newInput;
        }
      }

      if (newInput == node->input()) {
        return static_cast<NodeCP>(node);
      }
      return builder().make<Limit>({newInput, node->offset(), node->count()});
    });
  }

  // Sort: pending passes through unchanged — order has no row-set
  // effect. The sort keys must survive in the child's outputs.
  NodeCP rewriteSort(const Sort* node, PushdownContext& context) override {
    // With no column of this node read above, its rows are indistinguishable
    // and their order cannot be observed: `SELECT 1 FROM t ORDER BY x` is
    // `SELECT 1 FROM t`. Dropping the Sort also frees its keys to be pruned.
    if (!context.requiredAbove.containsAny(node->outputColumns())) {
      return rewrite(node->input(), context);
    }
    context.required.unionColumns(node->orderKeys());
    context.requiredAbove.unionColumns(node->orderKeys());
    // A Sort outputs its input's columns, so it is not the consumer the flag
    // describes.
    context.consumerDropsExtraColumns = false;
    NodeCP newInput = rewrite(node->input(), context);
    ExprVector orderKeys = node->orderKeys();
    applyOutputRewrites(context, orderKeys);
    if (newInput == node->input() && orderKeys == node->orderKeys()) {
      return node;
    }
    return builder().make<Sort>(
        {newInput, std::move(orderKeys), node->orderTypes()});
  }

  // Returns true when 'node' already emits rows in the order 'keys' / 'types'
  // ask for. A Window over a single partition sorts its output, and a
  // pass-through Project above it keeps that order. Pass a rewritten node: a
  // Window specialized into a ranking node does not order its output.
  bool emitsInOrder(
      NodeCP node,
      const ExprVector& keys,
      const OrderTypeVector& types) {
    ExprVector mapped(keys);
    while (node->is(NodeType::kProject)) {
      const auto* project = node->as<Project>();
      const auto& outputs = project->outputColumns();
      for (ExprCP& key : mapped) {
        const auto it = std::find(outputs.begin(), outputs.end(), key);
        if (it == outputs.end()) {
          return false;
        }
        key = project->exprs()[it - outputs.begin()];
      }
      node = project->input();
    }

    if (!node->is(NodeType::kWindow)) {
      return false;
    }
    const auto* window = node->as<Window>();
    return window->partitionKeys().empty() &&
        window->orderKeys().size() >= mapped.size() &&
        std::equal(mapped.begin(), mapped.end(), window->orderKeys().begin()) &&
        std::equal(types.begin(), types.end(), window->orderTypes().begin());
  }

  // TopN: a filter barrier like Limit (its bound depends on the row set), and
  // its order keys must survive in the child like Sort.
  NodeCP rewriteTopN(const TopN* node, PushdownContext& context) override {
    // As in rewriteSort, with no column read above only the row count the
    // TopN keeps is observable, which a Limit keeps as well.
    const bool rowsUnread =
        !context.requiredAbove.containsAny(node->outputColumns());
    return blockAt(context, [&](PushdownContext& empty) -> NodeCP {
      if (rowsUnread) {
        return builder().make<Limit>(
            {rewrite(node->input(), empty), node->offset(), node->count()});
      }
      empty.required.unionColumns(node->orderKeys());
      empty.requiredAbove.unionColumns(node->orderKeys());

      // Sorting by what the window below ranks on means a row past rank
      // 'offset + count' in its partition has that many rows before it here
      // too, so it cannot survive this TopN: the window can cap partitions
      // there.
      const int64_t cap = node->offsetPlusCount();
      const bool ordersAgree = node->input()->is(NodeType::kWindow) &&
          node->orderKeys() == node->input()->as<Window>()->orderKeys() &&
          node->orderTypes() == node->input()->as<Window>()->orderTypes();
      if (ordersAgree && cap <= std::numeric_limits<int32_t>::max()) {
        empty.rankLimit = static_cast<int32_t>(cap);
      }

      NodeCP newInput = rewrite(node->input(), empty);
      ExprVector orderKeys = node->orderKeys();
      applyOutputRewrites(empty, orderKeys);

      // A Window over a single partition emits its rows in order-key order, so
      // a TopN asking for that same order only has to count rows.
      if (emitsInOrder(newInput, orderKeys, node->orderTypes())) {
        return builder().make<Limit>({newInput, node->offset(), node->count()});
      }

      // The cap only reduces the rows the TopN sees; the TopN itself stays,
      // because a ranking node emits each partition greatest-rank first rather
      // than in order-key order. It can go once
      // https://github.com/facebookincubator/velox/issues/18494 makes a ranking
      // node's output order match a Window's.
      if (newInput == node->input() && orderKeys == node->orderKeys()) {
        return static_cast<NodeCP>(node);
      }
      return builder().make<TopN>(
          {newInput,
           std::move(orderKeys),
           node->orderTypes(),
           node->offset(),
           node->count()});
    });
  }

  // GroupId is a barrier: a conjunct on a key GroupId NULLs for some set can't
  // push below it. The input must supply the key expressions GroupId replicates
  // on and the columns the aggregates read (passed through).
  NodeCP rewriteGroupId(const GroupId* node, PushdownContext& context)
      override {
    return blockAt(context, [&](PushdownContext& empty) -> NodeCP {
      empty.required.unionColumns(node->groupingKeys());
      empty.required.unionColumns(node->aggregationInputs());
      empty.requiredAbove.unionColumns(node->groupingKeys());
      empty.requiredAbove.unionColumns(node->aggregationInputs());
      NodeCP newInput = rewrite(node->input(), empty);
      ExprVector groupingKeys = node->groupingKeys();
      ExprVector aggregationInputs = node->aggregationInputs();
      applyOutputRewrites(empty, groupingKeys, aggregationInputs);

      // Grouping sets can null the two sides of an identity independently.
      empty.outputRewrites.clear();
      if (newInput == node->input() && groupingKeys == node->groupingKeys() &&
          aggregationInputs == node->aggregationInputs()) {
        return static_cast<NodeCP>(node);
      }
      return builder().make<GroupId>(
          {newInput,
           std::move(groupingKeys),
           std::move(aggregationInputs),
           node->groupingSets(),
           node->groupingKeyColumns(),
           node->groupId(),
           node->outputColumns()});
    });
  }

  NodeCP rewriteMarkDistinct(
      const MarkDistinct* /*node*/,
      PushdownContext& /*context*/) override {
    VELOX_FAIL("MarkDistinct is added by physical planning, after pushdown");
  }

  // Unnest: conjuncts referencing only replicated (pre-unnest) columns
  // push below. Conjuncts that touch any unnest-produced column or the
  // ordinality column stay above — those don't exist on the input. So do
  // nondeterministic conjuncts, since one input row yields many output rows.
  NodeCP rewriteUnnest(const Unnest* node, PushdownContext& context) override {
    PlanObjectSet outputOnlyColumns;
    for (const ColumnVector& perExpression : node->unnestColumns()) {
      outputOnlyColumns.unionObjects(perExpression);
    }
    if (node->ordinalityColumn() != nullptr) {
      outputOnlyColumns.add(node->ordinalityColumn());
    }
    if (node->isOuter()) {
      outputOnlyColumns.add(node->markerColumn());
    }
    auto [pushable, blocked] = partition(context.pending, outputOnlyColumns);
    blockNondeterministic(pushable, blocked);

    // Columns this Unnest must still produce: those the consumer requires plus
    // the columns read by conjuncts that stay above. The marker keeps the rows
    // of an empty unnested value rather than carrying a value of its own, so
    // dropping it would drop those rows; keep it whether or not it is read.
    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);
    if (node->isOuter()) {
      outputsKept.add(node->markerColumn());
    }

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

    ColumnCP survivingMarker = node->markerColumn();

    const bool collapseDuplicates = context.mayDropDuplicates &&
        builder().functionNames().cardinality != nullptr &&
        builder().functionNames().lte != nullptr && !node->isOuter() &&
        blocked.empty() && !outputsKept.hasIntersection(outputOnlyColumns);

    PushdownContext childContext =
        makeChildContext(std::move(pushable), context);
    childContext.required.unionColumns(node->unnestExpressions());
    childContext.required.unionObjects(survivingReplicated);
    childContext.required.unionColumns(blocked);
    childContext.requiredAbove = childContext.required;
    childContext.mayDropDuplicates = collapseDuplicates;
    // This collapse still evaluates the collection once per input row. A
    // child collapse could remove input rows and change its evaluation count.
    for (ExprCP expr : node->unnestExpressions()) {
      if (expr->containsNonDeterministic()) {
        childContext.mayDropDuplicates = false;
        break;
      }
    }
    NodeCP newInput = rewrite(node->input(), childContext);
    ExprVector unnestExpressions = node->unnestExpressions();
    applyOutputRewrites(childContext, unnestExpressions, blocked);

    if (collapseDuplicates) {
      const auto* one = builder().makeLiteral(
          velox::Variant(int64_t{1}), toType(velox::BIGINT()));
      ExprVector nonEmpty;
      nonEmpty.reserve(unnestExpressions.size());
      // UNNEST pads shorter collections to the longest one, so an input row
      // produces output exactly when at least one collection is non-empty.
      for (ExprCP expression : unnestExpressions) {
        nonEmpty.push_back(exprs_.makeLessThanOrEqual(
            one, exprs_.makeCardinality(expression)));
      }
      NodeCP filtered = builder().make<Filter>(
          {newInput, ExprVector{exprs_.orAll(nonEmpty)}});
      return propagateVisibleRewrites(
          context, childContext, narrowed(filtered, context));
    }

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
        unnestExpressions == node->unnestExpressions() &&
        survivingOutputs.size() == node->outputColumns().size();
    NodeCP newNode = unchanged ? static_cast<NodeCP>(node)
                               : builder().make<Unnest>(
                                     {newInput,
                                      std::move(unnestExpressions),
                                      std::move(survivingReplicated),
                                      node->unnestColumns(),
                                      survivingOrdinality,
                                      survivingMarker,
                                      std::move(survivingOutputs)});
    return propagateVisibleRewrites(
        context, childContext, maybeWrapFilter(newNode, std::move(blocked)));
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
    context.outputRewrites.clear();
    if (!changed) {
      return node;
    }
    return builder().make<UnionAll>(
        {std::move(newInputs),
         std::move(newLegColumns),
         std::move(newOutputColumns)});
  }

  // Inference: every conjunct stays above, since the common case reads the
  // call's result.
  NodeCP rewriteInference(const Inference* node, PushdownContext& context)
      override {
    return blockAt(context, [&](PushdownContext& child) -> NodeCP {
      // The input supplies what the call reads; the result is produced here.
      child.required.unionColumns(node->call());
      child.requiredAbove = child.required;
      NodeCP newInput = rewrite(node->input(), child);

      ColumnVector outputColumns = newInput->outputColumns();
      outputColumns.push_back(node->result());
      return builder().make<Inference>(
          {newInput, node->call(), node->result(), std::move(outputColumns)});
    });
  }

  // Window: conjuncts whose columns are all direct partition-key
  // columns push below — within a partition those values are
  // constant, so a deterministic conjunct keeps or drops the partition whole.
  // All others stay above, as do nondeterministic conjuncts, which would
  // instead thin a partition and change what the window functions read.
  // A single ranking function then specializes into the cheapest node
  // that computes it; anything else stays a Window with unread functions
  // pruned.
  NodeCP rewriteWindow(const Window* node, PushdownContext& context) override {
    const std::optional<RankFusion> fusion = detectRankFusion(
        node,
        context.pending,
        context.rankLimit,
        builder().functionNames(),
        exprs_);

    PlanObjectSet partitionKeyColumns;
    for (ExprCP key : node->partitionKeys()) {
      if (key->is(PlanType::kColumnExpr)) {
        partitionKeyColumns.add(key->as<Column>());
      }
    }

    // Conjuncts referencing only partition keys push below; the rest stay above
    // (or, for a bound the ranking node absorbs, are consumed).
    const ColumnCP constantRank =
        constantRankColumn(node, builder().functionNames());
    ExprVector pushable;
    ExprVector blocked;
    for (ExprCP conjunct : context.pending) {
      if (fusion && conjunct == fusion->consumedPredicate) {
        if (fusion->remainingPredicate != nullptr) {
          blocked.push_back(fusion->remainingPredicate);
        }
        continue;
      }
      // A predicate that every row satisfies selects nothing away.
      if (constantRank != nullptr &&
          holdsAtRankOne(conjunct, constantRank, builder().functionNames())) {
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
    blockNondeterministic(pushable, blocked);

    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);

    PushdownContext childContext;
    childContext.pending = std::move(pushable);
    childContext.required = context.required;
    childContext.required.unionColumns(node->partitionKeys());
    childContext.required.unionColumns(node->orderKeys());
    childContext.required.unionColumns(blocked);
    childContext.nonNullColumns = context.nonNullColumns;

    NodeCP result;
    if (fusion) {
      result = specializeRanking(
          node, *fusion, childContext, outputsKept, std::move(blocked));
    } else {
      result = pruneWindowFunctions(
          node, childContext, outputsKept, std::move(blocked));
    }
    return propagateVisibleRewrites(context, childContext, result);
  }

  // Replaces a single-ranking-function Window with the node that computes it
  // most cheaply: RowNumber when the rows need no ordering, TopNRowNumber when
  // an ordered ranking is bounded by a per-partition limit.
  NodeCP specializeRanking(
      const Window* node,
      const RankFusion& fusion,
      PushdownContext& childContext,
      const PlanObjectSet& outputsKept,
      ExprVector blocked) {
    childContext.required.unionColumns(childContext.pending);
    childContext.requiredAbove = childContext.required;
    NodeCP newInput = dropColumnsForWindow(
        rewrite(node->input(), childContext), childContext.required);
    ExprVector partitionKeys = node->partitionKeys();
    ExprVector orderKeys = node->orderKeys();
    applyOutputRewrites(childContext, partitionKeys, orderKeys, blocked);

    // Emit the rank column only when a consumer above still needs it.
    const ColumnCP rankColumn =
        outputsKept.contains(fusion.rankColumn) ? fusion.rankColumn : nullptr;
    ColumnVector newOutputColumns;
    appendAll(newOutputColumns, newInput->outputColumns());
    if (rankColumn != nullptr) {
      newOutputColumns.push_back(rankColumn);
    }

    // With no rank column to emit and no limit, the node would pass its input
    // through unchanged.
    if (node->orderKeys().empty() && rankColumn == nullptr &&
        !fusion.limit.has_value()) {
      return maybeWrapFilter(newInput, std::move(blocked));
    }

    NodeCP ranking;
    if (orderKeys.empty()) {
      ranking = builder().make<RowNumber>(
          {newInput,
           std::move(partitionKeys),
           fusion.limit,
           rankColumn,
           std::move(newOutputColumns)});
    } else {
      ranking = builder().make<TopNRowNumber>(
          {newInput,
           fusion.rankFunction,
           std::move(partitionKeys),
           std::move(orderKeys),
           node->orderTypes(),
           fusion.limit.value(),
           rankColumn,
           std::move(newOutputColumns)});
    }
    return maybeWrapFilter(ranking, std::move(blocked));
  }

  // Velox's window operators pass every input column through, so a column
  // that only an operator below the window reads would ride through it. Adds
  // a Project that keeps just 'required' when the input has more.
  NodeCP dropColumnsForWindow(NodeCP input, const PlanObjectSet& required) {
    ExprVector exprs;
    ColumnVector columns;
    for (ColumnCP column : input->outputColumns()) {
      if (required.contains(column)) {
        exprs.push_back(column);
        columns.push_back(column);
      }
    }

    if (columns.size() == input->outputColumns().size()) {
      return input;
    }

    return builder().make<Project>(
        {input, std::move(exprs), std::move(columns)});
  }

  // Keeps the Window, dropping the functions no consumer reads.
  NodeCP pruneWindowFunctions(
      const Window* node,
      PushdownContext& childContext,
      const PlanObjectSet& outputsKept,
      ExprVector blocked) {
    const size_t numInputColumns = node->input()->outputColumns().size();

    WindowFunctions survivingFunctions;
    ColumnVector survivingFunctionOutputs;
    for (size_t i = 0; i < node->functions().size(); ++i) {
      const ColumnCP funcOutput = node->outputColumns()[numInputColumns + i];
      if (outputsKept.contains(funcOutput)) {
        survivingFunctions.push_back(node->functions()[i]);
        survivingFunctionOutputs.push_back(funcOutput);
      }
    }
    for (const auto& function : survivingFunctions) {
      childContext.required.unionColumns(function.call);
      if (function.frame.startValue != nullptr) {
        childContext.required.unionColumns(function.frame.startValue);
      }
      if (function.frame.endValue != nullptr) {
        childContext.required.unionColumns(function.frame.endValue);
      }
    }
    childContext.required.unionColumns(childContext.pending);
    childContext.requiredAbove = childContext.required;
    NodeCP newInput = dropColumnsForWindow(
        rewrite(node->input(), childContext), childContext.required);

    ExprVector partitionKeys = node->partitionKeys();
    ExprVector orderKeys = node->orderKeys();
    applyOutputRewrites(
        childContext, partitionKeys, orderKeys, blocked, survivingFunctions);

    // With every function pruned the node computes nothing and emits its
    // input's columns.
    if (survivingFunctions.empty()) {
      return maybeWrapFilter(newInput, std::move(blocked));
    }

    ColumnVector newOutputColumns;
    newOutputColumns.reserve(numInputColumns + survivingFunctions.size());
    appendAll(newOutputColumns, newInput->outputColumns());
    appendAll(newOutputColumns, survivingFunctionOutputs);

    const bool unchanged = newInput == node->input() &&
        survivingFunctions == node->functions() &&
        partitionKeys == node->partitionKeys() &&
        orderKeys == node->orderKeys();
    NodeCP newWindow = unchanged ? static_cast<NodeCP>(node)
                                 : builder().make<Window>(
                                       {newInput,
                                        std::move(survivingFunctions),
                                        std::move(partitionKeys),
                                        std::move(orderKeys),
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
      applyOutputRewrites(childContext, blocked);
      return propagateVisibleRewrites(
          context, childContext, maybeWrapFilter(newInput, std::move(blocked)));
    }

    NodeCP newInput = rewrite(node->input(), childContext);
    applyOutputRewrites(childContext, blocked);
    NodeCP newNode = (newInput == node->input())
        ? static_cast<NodeCP>(node)
        : builder().make<AssignUniqueId>({newInput, node->idColumn()});
    return propagateVisibleRewrites(
        context, childContext, maybeWrapFilter(newNode, std::move(blocked)));
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
    ExprVector distinctKeys = node->distinctKeys();
    ExprVector pending = std::move(context.pending);
    applyOutputRewrites(childContext, distinctKeys, pending);
    NodeCP newNode =
        (newInput == node->input() && distinctKeys == node->distinctKeys())
        ? static_cast<NodeCP>(node)
        : builder().make<EnforceDistinct>(
              {newInput, std::move(distinctKeys), node->errorMessage()});
    return propagateVisibleRewrites(
        context, childContext, maybeWrapFilter(newNode, std::move(pending)));
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
    // A delete reads no column values: the connector's handle says which rows
    // go. An insert writes its input columns, so only a delete can take a
    // wider input.
    child.consumerDropsExtraColumns =
        node->kind() == connector::WriteKind::kDelete;
    return NodeRewriter::rewriteTableWrite(node, child);
  }

  // Pending predicates must remain above the fixed point because pushing them
  // into either branch can change the rows accumulated across iterations.
  // TODO: Push iteration-invariant predicates into the anchor and step.
  NodeCP rewriteFixedPoint(const FixedPoint* node, PushdownContext& context)
      override {
    // Filtering seed rows is sound only when `p(step(x))` implies `p(x)`;
    // otherwise, a rejected seed can still produce matching descendants.
    // The step and convergence subtrees read the recursive state through the
    // same columns the anchor produces, so their paths must be recorded before
    // the anchor's scan negotiates; the descent reaches them only afterwards.
    access_.addSubtree(*node->step());
    access_.addSubtree(*node->convergence());

    PushdownContext anchorContext;
    anchorContext.required.unionObjects(node->outputColumns());
    anchorContext.requiredAbove = anchorContext.required;
    NodeCP newAnchor = rewrite(node->anchor(), anchorContext);
    VELOX_CHECK(
        std::ranges::equal(newAnchor->outputColumns(), node->outputColumns()),
        "FixedPoint anchor output columns must retain pointer identity after pushdown");

    // Required columns use step output identities because they may differ from
    // anchor output identities. The working table carries no outer null
    // guarantees, so `nonNullColumns` remains empty.
    PushdownContext stepContext;
    stepContext.required.unionObjects(node->step()->outputColumns());
    stepContext.requiredAbove = stepContext.required;
    NodeCP newStep = rewrite(node->step(), stepContext);

    // Convergence is an independent Boolean subplan. Seed only its root output;
    // each operator adds its expression dependencies while WorkingTable keeps
    // the complete recursive-state schema.
    PushdownContext convergenceContext;
    convergenceContext.required.unionObjects(
        node->convergence()->outputColumns());
    convergenceContext.requiredAbove = convergenceContext.required;
    NodeCP newConvergence = rewrite(node->convergence(), convergenceContext);

    NodeCP fixedPoint = node;
    if (newAnchor != node->anchor() || newStep != node->step() ||
        newConvergence != node->convergence()) {
      fixedPoint = builder().make<FixedPoint>({
          .anchor = newAnchor,
          .step = newStep,
          .convergence = newConvergence,
          .name = node->name(),
          .outputColumns = node->outputColumns(),
          .maxIterations = node->maxIterations(),
          .recursiveNumDrivers = node->recursiveNumDrivers(),
      });
    }
    return maybeWrapFilter(fixedPoint, std::move(context.pending));
  }

  NodeCP rewriteWorkingTable(const WorkingTable* node, PushdownContext& context)
      override {
    return maybeWrapFilter(node, std::move(context.pending));
  }

 private:
  // Adds the COALESCE identities established by a join. Inner and one-sided
  // outer joins select one key; full joins canonicalize the operand order.
  // A key that can be null-padded must evaluate to NULL on a null input.
  void addJoinKeyRewrites(
      velox::core::JoinType joinType,
      const ExprVector& leftKeys,
      const ExprVector& rightKeys,
      ExprFactory::ExprSubstitution& rewrites) {
    VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());
    const auto addRewrite = [&](ExprCP source, ExprCP target) {
      if (source == target) {
        return;
      }
      const auto [it, inserted] = rewrites.emplace(source, target);
      VELOX_CHECK(
          inserted || it->second == target,
          "Conflicting rewrites for expression: {}",
          source->toString());
    };

    if (joinType == velox::core::JoinType::kFull) {
      for (size_t i = 0; i < leftKeys.size(); ++i) {
        ExprCP leftKey = leftKeys[i];
        ExprCP rightKey = rightKeys[i];
        if (hasNonDefaultNullBehavior(leftKey) ||
            hasNonDefaultNullBehavior(rightKey)) {
          continue;
        }
        ExprCP canonical = builder().canonicalizeCoalesce(leftKey, rightKey);
        addRewrite(exprs_.makeCoalesce(leftKey, rightKey), canonical);
        addRewrite(exprs_.makeCoalesce(rightKey, leftKey), canonical);
      }
      return;
    }

    if (joinType != velox::core::JoinType::kInner &&
        joinType != velox::core::JoinType::kLeft &&
        joinType != velox::core::JoinType::kRight) {
      return;
    }
    const bool selectLeft = joinType != velox::core::JoinType::kRight;
    for (size_t i = 0; i < leftKeys.size(); ++i) {
      ExprCP leftKey = leftKeys[i];
      ExprCP rightKey = rightKeys[i];

      ExprCP discarded = selectLeft ? rightKey : leftKey;
      if (joinType != velox::core::JoinType::kInner &&
          hasNonDefaultNullBehavior(discarded)) {
        continue;
      }

      ExprCP selected = selectLeft ? leftKey : rightKey;
      addRewrite(exprs_.makeCoalesce(leftKey, rightKey), selected);
      addRewrite(exprs_.makeCoalesce(rightKey, leftKey), selected);
    }
  }

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

  void applyOutputRewrites(
      const ExprFactory::ExprSubstitution& rewrites,
      ExprCP& expression) {
    expression = applyRewrites(exprs_, expression, rewrites);
  }

  void applyOutputRewrites(
      const ExprFactory::ExprSubstitution& rewrites,
      ExprVector& expressions) {
    expressions = applyRewrites(exprs_, expressions, rewrites);
  }

  void applyOutputRewrites(
      const ExprFactory::ExprSubstitution& rewrites,
      AggregateCallVector& aggregates) {
    aggregates = applyRewrites(exprs_, builder(), aggregates, rewrites);
  }

  void applyOutputRewrites(
      const ExprFactory::ExprSubstitution& rewrites,
      WindowFunctions& functions) {
    for (auto& function : functions) {
      applyOutputRewrites(rewrites, function.call);
      applyOutputRewrites(rewrites, function.frame.startValue);
      applyOutputRewrites(rewrites, function.frame.endValue);
    }
  }

  template <typename... Rewritable>
  void applyOutputRewrites(
      const PushdownContext& child,
      Rewritable&... expressions) {
    if (child.outputRewrites.empty()) {
      return;
    }
    (applyOutputRewrites(child.outputRewrites, expressions), ...);
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
    ExprVector blocked = std::move(context.pending);
    applyOutputRewrites(empty, blocked);
    return propagateVisibleRewrites(
        context, empty, maybeWrapFilter(recursed, std::move(blocked)));
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

  // Substituting a cross-side equality by the join's own equi-pair maps both
  // of its arguments to the same expression, e.g. `t.x = u.x` becomes
  // `t.x = t.x`. That says only that the key is not null, which an equi-join
  // already guarantees, so it adds nothing.
  bool isSelfEquality(ExprCP expr) {
    if (!expr->is(PlanType::kCallExpr)) {
      return false;
    }
    const auto* call = expr->as<Call>();
    return call->name() == builder().functionNames().equality &&
        call->args().size() == 2 && call->args()[0] == call->args()[1];
  }

  struct ConstantJoinInput {
    // Null unless exactly one input is a constant `Values`.
    const Values* values{nullptr};
    bool onLeft{false};
  };

  // An inner join against a constant `Values` restricts its other input to the
  // values that `Values` holds. Three cases:
  //   - no rows: nothing joins, so the result is empty.
  //   - one row: every condition is pinned to a constant, so the join becomes
  //     a Project of those constants over a Filter on the other input.
  //   - several rows: the join stays, and each key gains `key IN (values)` on
  //     the other input.
  // Returns the replacement for the first two cases. Returns nullptr for the
  // third, having left its filters in the pending of the input that is not
  // constant, and when 'node' has no constant input.
  NodeCP rewriteConstantInputJoin(
      JoinCP node,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      PushdownContext& context,
      ExprVector& leftPending,
      ExprVector& rightPending) {
    const ConstantJoinInput side = constantSide(node);
    if (side.values == nullptr) {
      return nullptr;
    }
    if (side.values->cardinality() == 0) {
      return makeEmptyValues(node);
    }

    if (side.values->cardinality() > 1) {
      return restrictOtherInput(
                 node,
                 side,
                 leftColumns,
                 rightColumns,
                 context,
                 side.onLeft ? rightPending : leftPending)
          ? makeEmptyValues(node)
          : nullptr;
    }

    ExprFactory::ExprSubstitution constants;
    for (size_t i = 0; i < side.values->outputColumns().size(); ++i) {
      ColumnCP column = side.values->outputColumns()[i];
      constants.emplace(
          column,
          builder().makeLiteral(
              velox::Variant(side.values->valueAt(0, i)),
              column->value().type));
    }

    // The join's own conditions vanish with it, so each becomes a filter on
    // the other input. Conjuncts above the join need no such care: the Project
    // below still produces the `Values` columns, now constant, and pushdown
    // substitutes them on the way down.
    ExprVector filters;
    filters.reserve(node->leftKeys().size() + node->filter().size());

    // True if 'conjunct' is always false, leaving the join empty.
    const auto restate = [&](ExprCP conjunct) {
      return simplifier_.simplifyFilter(
          exprs_.replace(conjunct, constants), filters);
    };

    for (size_t i = 0; i < node->leftKeys().size(); ++i) {
      if (restate(exprs_.makeEq(node->leftKeys()[i], node->rightKeys()[i]))) {
        return makeEmptyValues(node);
      }
    }
    for (ExprCP conjunct : node->filter()) {
      if (restate(conjunct)) {
        return makeEmptyValues(node);
      }
    }

    NodeCP input = side.onLeft ? node->right() : node->left();
    if (!filters.empty()) {
      input = builder().make<Filter>({input, std::move(filters)});
    }

    // The `Values` columns the join output carries become constants; without
    // any, the other input already produces what the join did.
    ExprVector exprs;
    ColumnVector outputColumns;
    exprs.reserve(node->outputColumns().size());
    outputColumns.reserve(node->outputColumns().size());
    bool readsConstant = false;
    for (ColumnCP column : node->outputColumns()) {
      const auto constant = constants.find(column);
      readsConstant |= constant != constants.end();
      exprs.push_back(constant == constants.end() ? column : constant->second);
      outputColumns.push_back(column);
    }
    if (!readsConstant) {
      return rewrite(input, context);
    }
    return rewrite(
        builder().make<Project>(
            {input, std::move(exprs), std::move(outputColumns)}),
        context);
  }

  // Adds `key IN (values)` to 'otherInputPending' for each equi-key of 'node'
  // whose key column comes from the constant input. Derived per key, so with
  // several keys the filters admit combinations no row of the constant input
  // has. That is sound, because the join still rejects them. Returns true if a
  // derived filter can never hold, so no row joins.
  //
  // The filters go straight to the other input rather than through
  // 'context.pending', which crosses to both sides: read off the constant
  // input, they always hold on it.
  bool restrictOtherInput(
      JoinCP node,
      const ConstantJoinInput& side,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      PushdownContext& context,
      ExprVector& otherInputPending) {
    auto [leftKeys, rightKeys] = collectEquiColumnPairs(
        node, leftColumns, rightColumns, context.pending);

    ExprVector derived;
    for (size_t i = 0; i < leftKeys.size(); ++i) {
      ColumnCP valuesKey = side.onLeft ? leftKeys[i] : rightKeys[i];
      ColumnCP probeKey = side.onLeft ? rightKeys[i] : leftKeys[i];
      if (simplifier_.simplifyFilter(
              makeKeyFilter(*side.values, valuesKey, probeKey), derived)) {
        return true;
      }
    }
    appendAll(otherInputPending, derived);
    return false;
  }

  // The join's constant `Values` input, when exactly one input is constant.
  static ConstantJoinInput constantSide(JoinCP node) {
    const Values* left = constantInput(node->left());
    const Values* right = constantInput(node->right());
    if ((left == nullptr) == (right == nullptr)) {
      return {};
    }
    return left != nullptr ? ConstantJoinInput{left, true}
                           : ConstantJoinInput{right, false};
  }

  // Returns 'node' as a `Values` holding folded rows, or nullptr.
  static const Values* constantInput(NodeCP node) {
    if (!node->is(NodeType::kValues)) {
      return nullptr;
    }
    const auto* values = node->as<Values>();
    return values->rows() != nullptr ? values : nullptr;
  }

  // Returns `probeKey = v` over the single value 'values' holds for
  // 'valuesKey', or `probeKey IN (v...)` over its distinct values.
  ExprCP
  makeKeyFilter(const Values& values, ColumnCP valuesKey, ColumnCP probeKey) {
    const ColumnVector& outputColumns = values.outputColumns();
    const auto it =
        std::find(outputColumns.begin(), outputColumns.end(), valuesKey);
    VELOX_CHECK(
        it != outputColumns.end(),
        "Join key is not a column of the input it reads: {}",
        valuesKey->toString());
    const size_t column = it - outputColumns.begin();

    const TypeCP type = valuesKey->value().type;
    ExprVector distinct;
    folly::F14FastSet<ExprCP> seen;
    for (size_t row = 0; row < values.cardinality(); ++row) {
      ExprCP literal = builder().makeLiteral(
          velox::Variant(values.valueAt(row, column)), type);
      if (seen.insert(literal).second) {
        distinct.push_back(literal);
      }
    }

    if (distinct.size() == 1) {
      return exprs_.makeEq(probeKey, distinct[0]);
    }
    return exprs_.makeIn(probeKey, std::move(distinct));
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
    ExprVector derived;
    if (deriveFilters(pending, rightKeys, leftKeys, derived) ||
        deriveFilters(pending, leftKeys, rightKeys, derived)) {
      return true;
    }
    appendDistinct(pending, derived);
    return false;
  }

  folly::F14FastMap<JoinCP, CollectedJoinFilters> collectedJoinFilters_;

  ExprFactory exprs_;
  velox::core::ExpressionEvaluator& evaluator_;
  const OptimizerSession& session_;
  const PushdownAndPrunePass::ConnectorPushdown connectorPushdown_;

  // Which parts of each column the plan reads, accumulated as the rewrite
  // descends.
  ColumnAccess access_;

  ExprSimplifier simplifier_;

  // Outcome of one negotiation with the connector.
  struct Negotiated {
    const ScanHandle* handle;
    // The conjuncts offered, and of those the ones the connector rejected,
    // which the plan applies itself.
    ExprVector filters;
    ExprVector rejected;
  };

  // One negotiation per base table, by base-table id.
  folly::F14FastMap<int32_t, Negotiated> negotiatedByBaseTableId_;
};

} // namespace

NodeCP PushdownAndPrunePass::run(
    NodeCP root,
    const ColumnVector& outputColumns,
    Builder& builder,
    velox::core::ExpressionEvaluator& evaluator,
    const OptimizerSession& session,
    ConnectorPushdown connectorPushdown) {
  Pushdown pass{builder, evaluator, session, connectorPushdown, outputColumns};
  PushdownContext context;
  context.required = PlanObjectSet::fromObjects(outputColumns);
  context.requiredAbove = context.required;
  return pass.rewrite(root, context);
}

} // namespace facebook::axiom::optimizer::v2
