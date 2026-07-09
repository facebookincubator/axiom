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

#include "axiom/optimizer/v2/DecorrelatePass.h"

#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/v2/AggregateRecovery.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/JoinCondition.h"
#include "axiom/optimizer/v2/NodeExpressions.h"
#include "axiom/optimizer/v2/NodeRewriter.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// Returns the subset of `inputColumns` referenced anywhere inside
// `body`'s tree (in expressions on body's nodes and recursive
// sub-nodes). Refs at the Apply boundary (caller's `Apply.filter`)
// are NOT included — those become `Join.filter` at terminus and
// don't block iteration; only refs INSIDE body block terminus.
//
// Recompute (not incremental): a peel may have absorbed body's only
// occurrence of an outer col (drop the ref) or only one of several
// (ref still in body via other occurrences). Subtracting per-peel
// would be wrong; only this walk gives the right answer.
//
// TODO: cache per-node correlation sets to avoid re-walking the full
// body on every iteration. Optimization, not correctness.
ColumnVector recomputeCorrelations(
    NodeCP body,
    const ColumnVector& inputColumns) {
  PlanObjectSet inputSet = PlanObjectSet::fromObjects(inputColumns);
  PlanObjectSet found;

  auto walk = [&](auto& self, NodeCP node) -> void {
    forEachExpressionInNode(node, [&](ExprCP expr) {
      expr->columns().forEach<Column>([&](const Column* column) {
        if (inputSet.contains(column)) {
          found.add(column);
        }
      });
    });
    for (NodeCP child : node->inputs()) {
      self(self, child);
    }
  };
  walk(walk, body);

  ColumnVector result;
  for (ColumnCP column : inputColumns) {
    if (found.contains(column)) {
      result.push_back(column);
    }
  }
  return result;
}

// True when a kLeftSemiProject Apply's body Aggregate can be dropped
// without changing the mark: grouping keys are non-empty and neither
// the accumulated filter nor `inBodyKey` references any aggregate-
// result column.
bool canElideBodyAggregate(
    ApplyCP node,
    AggregateCP aggregate,
    const ExprVector& accumulatedFilter) {
  const size_t numGroupingKeys = aggregate->groupingKeys().size();
  if (numGroupingKeys == 0) {
    return false;
  }

  if (accumulatedFilter.empty() && node->inBodyKey() == nullptr) {
    return true;
  }

  PlanObjectSet aggregateResultColumns;
  for (size_t i = numGroupingKeys; i < aggregate->outputColumns().size(); ++i) {
    aggregateResultColumns.add(aggregate->outputColumns()[i]);
  }

  for (ExprCP conjunct : accumulatedFilter) {
    if (conjunct->columns().hasIntersection(aggregateResultColumns)) {
      return false;
    }
  }

  if (node->inBodyKey() != nullptr &&
      node->inBodyKey()->columns().hasIntersection(aggregateResultColumns)) {
    return false;
  }
  return true;
}

// Appends each element of `src` to `dst` unless `dst` already
// contains it.
template <typename Dst, typename Src>
void appendUnique(Dst& dst, const Src& src) {
  PlanObjectSet seen = PlanObjectSet::fromObjects(dst);
  for (auto&& element : src) {
    if (!seen.contains(element)) {
      seen.add(element);
      dst.push_back(element);
    }
  }
}

// Returns nullptr if `expr` is null; otherwise delegates to
// `ExprFactory::substitute`.
ExprCP substituteOrNull(
    ExprFactory& exprFactory,
    ExprCP expr,
    const ColumnVector& sources,
    const ExprVector& targets) {
  return expr == nullptr ? nullptr
                         : exprFactory.substitute(expr, sources, targets);
}

// Decorrelate pass implementation. The per-Apply loop:
// recompute `correlationColumns` from body; if empty, hit terminus;
// otherwise dispatch a peel rule by body's outermost operator and
// continue.
class Decorrelator : public NodeRewriter<> {
 public:
  explicit Decorrelator(Builder& builder)
      : NodeRewriter(builder), exprFactory_(builder) {}

 protected:
  NodeCP rewriteApply(ApplyCP node, NoContext& context) override {
    // Bottom-up: nested Apply nodes inside input / body get
    // decorrelated first via the base rewriter's dispatch.
    NodeCP input = rewrite(node->input(), context);
    NodeCP body = rewrite(node->body(), context);

    ExprVector accumulatedFilter = node->filter();

    // Recompute correlations from body before each iteration;
    // terminus when empty.
    while (true) {
      ColumnVector correlations =
          recomputeCorrelations(body, input->outputColumns());

      if (correlations.empty()) {
        return terminus(node, input, body, accumulatedFilter);
      }

      // Dispatch by body's outermost operator.
      if (body->is(NodeType::kFilter)) {
        // Filter peel: absorb all predicates into Apply.filter; body
        // shrinks to Filter's child.
        const Filter* filterBody = body->as<Filter>();
        appendAll(accumulatedFilter, filterBody->predicates());
        body = filterBody->input();
        continue;
      }

      if (body->is(NodeType::kProject)) {
        // Project peel returns the fully-decorrelated subtree (it
        // builds the inner Apply and recurses). Driver loop ends here.
        if (node->isLeftSemiProject()) {
          return projectPeelSemi(node, input, body, accumulatedFilter);
        }
        return projectPeelLeft(node, input, body, accumulatedFilter);
      }

      if (body->is(NodeType::kAggregate)) {
        // Aggregate peel returns the fully-decorrelated subtree.
        return aggregatePeel(node, input, body, accumulatedFilter);
      }

      if (body->is(NodeType::kLimit)) {
        return limitPeel(node, input, body, accumulatedFilter);
      }

      if (body->is(NodeType::kJoin)) {
        return joinPeel(node, input, body, accumulatedFilter);
      }

      if (body->is(NodeType::kAssignUniqueId)) {
        return assignUniqueIdPeel(node, input, body, accumulatedFilter);
      }

      VELOX_NYI(
          "Correlated reference inside a {} branch is not supported yet",
          body->nodeType());
    }
  }

 private:
  // Project peel (Rule A): Project lifts above Apply. Apply's body
  // becomes Project's child; the lifted Project above the decorrelated
  // inner Apply reproduces the original output schema using Project's
  // original exprs (which may reference L cols, now visible above the
  // inner Apply per the relaxed contract).
  //
  // Recurses on the inner Apply so the driver continues peeling whatever
  // remains in body (more Filters / Projects below, or terminus).
  NodeCP projectPeelLeft(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      ExprVector accumulatedFilter) {
    const Project* projectBody = body->as<Project>();
    NodeCP newBody = projectBody->input();

    // If accumulatedFilter references any of Project's output cols,
    // substitute those refs with Project's corresponding exprs. After
    // Project peels, body becomes newBody, and Project's outputs no
    // longer exist below — but their values are computed by Project's
    // exprs, which reference newBody cols (and possibly outer cols) —
    // all visible in the inner Apply's outputColumns. Pass-through
    // Project entries (where expr == output col) substitute to
    // themselves — no-op.
    ExprVector newAccumulatedFilter = exprFactory_.substitute(
        accumulatedFilter, projectBody->outputColumns(), projectBody->exprs());

    // Build inner Apply with body = Project's child. Inner Apply
    // inherits the outer's includeMarker. Body cols that alias input
    // cols by identity collapse to a single slot.
    ColumnVector innerOutputColumns;
    innerOutputColumns.reserve(
        input->outputColumns().size() + newBody->outputColumns().size() + 1);
    appendAll(innerOutputColumns, input->outputColumns());
    appendUnique(innerOutputColumns, newBody->outputColumns());
    innerOutputColumns.push_back(node->includeMarker());

    // Recompute correlations for the inner body. Project peel may have
    // lifted outer-column refs out of body (into the lifted Project
    // above), shrinking the correlation set.
    ColumnVector innerCorrelations =
        recomputeCorrelations(newBody, input->outputColumns());

    NodeCP innerApply = builder().make<Apply>(Apply::Key{
        input,
        newBody,
        std::move(innerCorrelations),
        node->kind(),
        std::move(newAccumulatedFilter),
        node->enforceSingleRow(),
        node->markColumn(),
        node->inLhs(),
        node->inBodyKey(),
        node->includeMarker(),
        std::move(innerOutputColumns),
    });

    // Recurse: continue peeling whatever's in newBody, or hit terminus.
    NodeCP decorrelatedInner = rewrite(innerApply);

    // Build lifted Project. Its exprs reproduce the original Apply's
    // output schema:
    //   input.cols → pass-through.
    //   Project's original output cols → IF(_include, expr, NULL) so
    //     non-default-null exprs evaluate to NULL on pad rows post-Join.
    //     Skip cols that already appear in input — they collapse to the
    //     same slot.
    //   includeMarker → pass-through.
    ExprVector liftedExprs;
    liftedExprs.reserve(
        input->outputColumns().size() + projectBody->exprs().size() + 1);
    appendAll(liftedExprs, input->outputColumns());
    PlanObjectSet liftedSeen =
        PlanObjectSet::fromObjects(input->outputColumns());
    ColumnCP includeMarker = node->includeMarker();
    const auto& projectOutputs = projectBody->outputColumns();
    for (size_t i = 0; i < projectBody->exprs().size(); ++i) {
      ColumnCP outputColumn = projectOutputs[i];
      if (liftedSeen.contains(outputColumn)) {
        continue;
      }
      liftedSeen.add(outputColumn);
      ExprCP expr = projectBody->exprs()[i];
      const Literal* nullLiteral = builder().makeNull(expr->value().type);
      liftedExprs.push_back(
          exprFactory_.makeIf(includeMarker, expr, nullLiteral));
    }
    liftedExprs.push_back(includeMarker);

    return builder().make<Project>(Project::Key{
        decorrelatedInner,
        std::move(liftedExprs),
        node->outputColumns(),
    });
  }

  // Project peel (Rule B) for kLeftSemiProject. Project dissolves
  // into the mark predicate: substitute refs to Project's output cols
  // in `inBodyKey` (and `Apply.filter`) with Project's corresponding
  // exprs. Body becomes Project's child; no Project lives above Apply
  // (kSemi's output doesn't carry body cols, so there's nothing for a
  // lifted Project to feed). Recurses on the inner Apply.
  NodeCP projectPeelSemi(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      ExprVector accumulatedFilter) {
    const Project* projectBody = body->as<Project>();
    NodeCP newBody = projectBody->input();

    // Substitution: Project's output cols → Project's exprs.
    // (Pass-through entries have expr == output col, so they substitute
    // to themselves — no-op.)
    ExprCP newInBodyKey = substituteOrNull(
        exprFactory_,
        node->inBodyKey(),
        projectBody->outputColumns(),
        projectBody->exprs());
    ExprVector newAccumulatedFilter = exprFactory_.substitute(
        accumulatedFilter, projectBody->outputColumns(), projectBody->exprs());

    // Recompute inner correlations against the new body.
    ColumnVector innerCorrelations =
        recomputeCorrelations(newBody, input->outputColumns());

    // Inner Apply.outputColumns for kLeftSemiProject:
    //   input.cols ++ markColumn (per Apply contract).
    ColumnVector innerOutputColumns;
    innerOutputColumns.reserve(input->outputColumns().size() + 1);
    appendAll(innerOutputColumns, input->outputColumns());
    innerOutputColumns.push_back(node->markColumn());

    NodeCP innerApply = builder().make<Apply>(Apply::Key{
        input,
        newBody,
        std::move(innerCorrelations),
        velox::core::JoinType::kLeftSemiProject,
        newAccumulatedFilter,
        /*enforceSingleRow=*/false,
        node->markColumn(),
        node->inLhs(),
        newInBodyKey,
        /*includeMarker=*/nullptr,
        std::move(innerOutputColumns),
    });

    return rewrite(innerApply);
  }

  // Peels a Limit body operator. Handles all kind × count
  // combinations; kSemi+IN with count >= 1 and non-zero Limit.offset
  // are NYI.
  NodeCP limitPeel(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      ExprVector accumulatedFilter) {
    LimitCP limitBody = body->as<Limit>();
    VELOX_USER_CHECK_EQ(
        limitBody->offset(),
        0,
        "Decorrelate: Limit with non-zero offset in correlated body "
        "not yet supported");
    // Translate rewrites `LIMIT 0` to `Values(empty)` before this pass,
    // so decorrelate never sees a zero-count Limit body.
    const int64_t count{limitBody->count()};
    VELOX_CHECK_GE(count, 1);
    const bool isInSubquery =
        node->isLeftSemiProject() && node->inLhs() != nullptr;

    if (node->isLeftSemiProject() && !isInSubquery) {
      return limitPeelDropForExists(node, input, limitBody, accumulatedFilter);
    }
    if (isInSubquery) {
      VELOX_NYI(
          "Decorrelate: kLeftSemiProject IN over Limit with count >= 1 "
          "not yet implemented (LIMIT must precede the IN equi check)");
    }
    VELOX_USER_CHECK(
        node->isLeft(),
        "Decorrelate Limit peel: unexpected Apply kind {}",
        node->kind());
    return limitPeelLeftWindowed(
        node, input, limitBody, count, accumulatedFilter);
  }

  // kSemi+EXISTS+count>=1: drop Limit, recurse on body = Limit.input.
  // Existence is unchanged when truncating to first N for N>=1.
  NodeCP limitPeelDropForExists(
      ApplyCP node,
      NodeCP input,
      LimitCP limitBody,
      ExprVector accumulatedFilter) {
    NodeCP newBody = limitBody->input();
    ColumnVector innerOutputColumns;
    innerOutputColumns.reserve(input->outputColumns().size() + 1);
    appendAll(innerOutputColumns, input->outputColumns());
    innerOutputColumns.push_back(node->markColumn());
    ColumnVector innerCorrelations =
        recomputeCorrelations(newBody, input->outputColumns());
    NodeCP innerApply = builder().make<Apply>(Apply::Key{
        input,
        newBody,
        std::move(innerCorrelations),
        node->kind(),
        std::move(accumulatedFilter),
        /*enforceSingleRow=*/false,
        node->markColumn(),
        node->inLhs(),
        node->inBodyKey(),
        /*includeMarker=*/nullptr,
        std::move(innerOutputColumns),
    });
    return rewrite(innerApply);
  }

  // kLeft+count>=1: per-outer LIMIT via Window+Filter. Tags input with
  // a per-outer `rn`, decorrelates `Apply(taggedInput, body, kLeft)`,
  // then keeps the first `count` body rows per `rn`. When the outer
  // Apply has enforceSingleRow=true, EnforceDistinct on `rn` asserts
  // <=1 row per outer.
  //
  // If `body` is a Sort, its ORDER BY is lifted into the row_number
  // window's OVER clause so the per-outer LIMIT picks the
  // sort-deterministic first `count` rows per partition.
  NodeCP limitPeelLeftWindowed(
      ApplyCP node,
      NodeCP input,
      LimitCP limitBody,
      int64_t count,
      ExprVector accumulatedFilter) {
    ColumnCP rowNumberPartition = makeIdColumn();
    NodeCP taggedInput = builder().make<AssignUniqueId>(
        AssignUniqueId::Key{input, rowNumberPartition});

    NodeCP newBody = limitBody->input();
    ExprVector windowOrderKeys;
    OrderTypeVector windowOrderTypes;
    if (newBody->is(NodeType::kSort)) {
      SortCP sortBody = newBody->as<Sort>();
      windowOrderKeys = sortBody->orderKeys();
      windowOrderTypes = sortBody->orderTypes();
      newBody = sortBody->input();
    }
    ColumnCP innerIncludeMarker = makeIncludeColumn();

    ColumnVector innerOutputColumns;
    innerOutputColumns.reserve(
        taggedInput->outputColumns().size() + newBody->outputColumns().size() +
        1);
    appendAll(innerOutputColumns, taggedInput->outputColumns());
    appendUnique(innerOutputColumns, newBody->outputColumns());
    innerOutputColumns.push_back(innerIncludeMarker);

    ColumnVector innerCorrelations =
        recomputeCorrelations(newBody, taggedInput->outputColumns());

    NodeCP innerApply = builder().make<Apply>(Apply::Key{
        taggedInput,
        newBody,
        std::move(innerCorrelations),
        velox::core::JoinType::kLeft,
        std::move(accumulatedFilter),
        /*enforceSingleRow=*/false,
        /*markColumn=*/nullptr,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr,
        innerIncludeMarker,
        std::move(innerOutputColumns),
    });
    NodeCP decorrelatedInner = rewrite(innerApply);

    ColumnCP rowNumberColumn = makeIdColumn("__limit_rn");
    NodeCP windowed = addRowNumberWindow(
        decorrelatedInner,
        rowNumberPartition,
        rowNumberColumn,
        std::move(windowOrderKeys),
        std::move(windowOrderTypes));

    const Literal* countLiteral =
        builder().makeLiteral(velox::Variant(count), toType(velox::BIGINT()));
    NodeCP filtered = builder().make<Filter>(Filter::Key{
        windowed,
        ExprVector{
            exprFactory_.makeLessThanOrEqual(rowNumberColumn, countLiteral)},
    });

    // Trivially holds for count=1; for count>=2 the EnforceDistinct
    // fires when an outer has >1 matches.
    NodeCP enforced = node->enforceSingleRow()
        ? enforceScalarSingleRow(filtered, rowNumberPartition)
        : filtered;

    // Outer Apply's includeMarker is sourced from the inner Apply's
    // includeMarker so per-outer LIMIT preserves the real-vs-pad signal.
    ExprVector finalExprs;
    finalExprs.reserve(node->outputColumns().size());
    for (ColumnCP outputColumn : node->outputColumns()) {
      if (outputColumn == node->includeMarker()) {
        finalExprs.push_back(innerIncludeMarker);
      } else {
        finalExprs.push_back(outputColumn);
      }
    }
    return builder().make<Project>(Project::Key{
        enforced,
        std::move(finalExprs),
        node->outputColumns(),
    });
  }

  // A `row_number()` window function over the default running frame,
  // writing into `rowNumberColumn`.
  WindowFunction rowNumberWindowFunction(ColumnCP rowNumberColumn) {
    const auto& rowNumberName = FunctionRegistry::instance()->rowNumber();
    VELOX_USER_CHECK(
        rowNumberName.has_value(),
        "Decorrelate requires row_number registered via "
        "FunctionRegistry::registerRowNumber");
    ExprCP call = builder().makeCall(
        toName(*rowNumberName),
        rowNumberColumn->value(),
        ExprVector{},
        FunctionSet{} | FunctionSet::kNonDeterministic |
            FunctionSet::kNonDefaultNullBehavior);
    return WindowFunction{call, Frame::toCurrentRow(), /*ignoreNulls=*/false};
  }

  // Wraps 'input' in a Window that emits `row_number() OVER
  // (PARTITION BY partition [ORDER BY orderKeys])` into
  // 'rowNumberColumn'. Empty 'orderKeys' yields an unordered
  // row_number assignment.
  NodeCP addRowNumberWindow(
      NodeCP input,
      ColumnCP partition,
      ColumnCP rowNumberColumn,
      ExprVector orderKeys,
      OrderTypeVector orderTypes) {
    WindowFunctions functions;
    functions.push_back(rowNumberWindowFunction(rowNumberColumn));

    ColumnVector outputs;
    outputs.reserve(input->outputColumns().size() + 1);
    appendAll(outputs, input->outputColumns());
    outputs.push_back(rowNumberColumn);

    return builder().make<Window>(Window::Key{
        input,
        std::move(functions),
        ExprVector{partition},
        std::move(orderKeys),
        std::move(orderTypes),
        std::move(outputs),
    });
  }

  // Peels a Join body operator. Serializes `Apply(L, Join(A, B, ...))`
  // into a chain `Apply(Apply(L, A), B)`. Most kind / correlation
  // combinations dispatched out to helpers; kFull and several mixed
  // cases are NYI loud pending further design.
  NodeCP joinPeel(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      ExprVector accumulatedFilter) {
    JoinCP joinBody = body->as<Join>();

    ExprVector joinPredicate = flattenJoinPredicate(joinBody);

    if (joinBody->isFull()) {
      VELOX_NYI(
          "Decorrelate: Apply over kFull Join in body is not yet "
          "supported (tree-only execution can't preserve both sides' "
          "unmatched rows without DAG support)");
    }

    if (node->isLeft()) {
      return joinPeelLeft(
          node,
          input,
          joinBody,
          std::move(joinPredicate),
          std::move(accumulatedFilter));
    }

    if (node->isLeftSemiProject()) {
      return joinPeelSemi(
          node,
          input,
          joinBody,
          std::move(joinPredicate),
          std::move(accumulatedFilter));
    }

    VELOX_FAIL(
        "Decorrelate joinPeel: unexpected outer Apply kind {}", node->kind());
  }

  // Flattens Join's split form (equi-pairs as `eq(leftKeys[i],
  // rightKeys[i])` plus `filter`) into a single conjunct vector.
  ExprVector flattenJoinPredicate(JoinCP joinBody) {
    ExprVector conjuncts;
    conjuncts.reserve(joinBody->leftKeys().size() + joinBody->filter().size());
    for (size_t i = 0; i < joinBody->leftKeys().size(); ++i) {
      conjuncts.push_back(exprFactory_.makeEq(
          joinBody->leftKeys()[i], joinBody->rightKeys()[i]));
    }
    appendAll(conjuncts, joinBody->filter());
    return conjuncts;
  }

  // Outer Apply is kLeft (scalar). Supports kInner cross-join (no
  // predicate) and kLeft with predicate. kInner with a Join-node
  // predicate (requires per-rn pad-collapse over matches) and other
  // kinds NYI loud.
  NodeCP joinPeelLeft(
      ApplyCP node,
      NodeCP input,
      JoinCP joinBody,
      ExprVector joinPredicate,
      ExprVector accumulatedFilter) {
    if (!joinBody->isInner() && !joinBody->isLeft()) {
      VELOX_NYI(
          "Decorrelate joinPeel: outer kLeft over Join.kind={} not yet "
          "implemented",
          joinBody->joinType());
    }
    if (joinBody->isInner() && !joinPredicate.empty()) {
      VELOX_NYI(
          "Decorrelate joinPeel: outer kLeft over kInner Join with "
          "predicate not yet implemented (needs per-rn pad-collapse)");
    }

    NodeCP leftSide = joinBody->left();
    NodeCP rightSide = joinBody->right();
    ExprVector applyBFilter;
    if (joinBody->isLeft()) {
      applyBFilter = std::move(joinPredicate);
    }
    appendAll(applyBFilter, accumulatedFilter);

    if (joinBody->isInner()) {
      return joinPeelLeftInner(
          node, input, leftSide, rightSide, std::move(applyBFilter));
    }

    // Body is kLeft. Chain: applyA over A, then applyB over B with
    // applyA's output as input. joinPredicate becomes applyB.filter
    // (the LEFT JOIN's ON condition); kLeft pad on no match preserves
    // the body LEFT JOIN's semantics.

    // applyA: input = L, body = leftSide.
    ColumnCP applyAIncludeMarker = makeIncludeColumn();
    ColumnVector applyAOutputs;
    applyAOutputs.reserve(
        input->outputColumns().size() + leftSide->outputColumns().size() + 1);
    appendAll(applyAOutputs, input->outputColumns());
    appendUnique(applyAOutputs, leftSide->outputColumns());
    applyAOutputs.push_back(applyAIncludeMarker);

    ColumnVector applyACorrelations =
        recomputeCorrelations(leftSide, input->outputColumns());

    NodeCP applyA = builder().make<Apply>(Apply::Key{
        input,
        leftSide,
        std::move(applyACorrelations),
        velox::core::JoinType::kLeft,
        /*filter=*/ExprVector{},
        node->enforceSingleRow(),
        /*markColumn=*/nullptr,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr,
        applyAIncludeMarker,
        std::move(applyAOutputs),
    });

    // applyB: input = applyA, body = rightSide.
    ColumnCP applyBIncludeMarker = makeIncludeColumn();
    ColumnVector applyBOutputs;
    applyBOutputs.reserve(
        applyA->outputColumns().size() + rightSide->outputColumns().size() + 1);
    appendAll(applyBOutputs, applyA->outputColumns());
    appendUnique(applyBOutputs, rightSide->outputColumns());
    applyBOutputs.push_back(applyBIncludeMarker);

    ColumnVector applyBCorrelations =
        recomputeCorrelations(rightSide, applyA->outputColumns());

    NodeCP applyB = builder().make<Apply>(Apply::Key{
        applyA,
        rightSide,
        std::move(applyBCorrelations),
        velox::core::JoinType::kLeft,
        std::move(applyBFilter),
        node->enforceSingleRow(),
        /*markColumn=*/nullptr,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr,
        applyBIncludeMarker,
        std::move(applyBOutputs),
    });

    NodeCP decorrelatedChain = rewrite(applyB);

    // Final Project: shape to outer Apply's outputColumns. Body is
    // kLeft, so the inner LEFT JOIN's right-side pad rows are legitimate
    // body output (left preserved with NULL right cols) and stay
    // included; applyA's marker alone gates inclusion.
    ExprCP includeMarker = applyAIncludeMarker;
    ExprVector finalExprs;
    finalExprs.reserve(node->outputColumns().size());
    for (ColumnCP outputColumn : node->outputColumns()) {
      if (outputColumn == node->includeMarker()) {
        finalExprs.push_back(includeMarker);
      } else {
        finalExprs.push_back(outputColumn);
      }
    }
    return builder().make<Project>(Project::Key{
        decorrelatedChain,
        std::move(finalExprs),
        node->outputColumns(),
    });
  }

  // Outer kLeft over a body kInner cross-join. The leg cascade uses
  // kLeft legs so outers and left rows survive, but that over-produces
  // pad rows when one side is empty and the other has >1 rows, which
  // would duplicate the outer. Restore `outer LEFT JOIN (A x B)`
  // semantics with a per-rn pad-collapse: keep every real (a, b) row
  // and, for an outer with no match, keep exactly one pad row. See
  // Decorrelate-join-rules.md §"INNER pad-row drop".
  NodeCP joinPeelLeftInner(
      ApplyCP node,
      NodeCP input,
      NodeCP leftSide,
      NodeCP rightSide,
      ExprVector applyBFilter) {
    ColumnCP rowId = makeIdColumn();
    NodeCP taggedInput =
        builder().make<AssignUniqueId>(AssignUniqueId::Key{input, rowId});

    // Legs never enforce single row: an empty side makes the cross join
    // empty (a valid 0-row scalar), which a per-leg check would misread
    // as the other side's rows. ESR is applied once after the collapse.
    ColumnCP markA = makeIncludeColumn();
    ColumnVector applyAOutputs;
    applyAOutputs.reserve(
        taggedInput->outputColumns().size() + leftSide->outputColumns().size() +
        1);
    appendAll(applyAOutputs, taggedInput->outputColumns());
    appendUnique(applyAOutputs, leftSide->outputColumns());
    applyAOutputs.push_back(markA);
    NodeCP applyA = builder().make<Apply>(Apply::Key{
        taggedInput,
        leftSide,
        recomputeCorrelations(leftSide, taggedInput->outputColumns()),
        velox::core::JoinType::kLeft,
        /*filter=*/ExprVector{},
        /*enforceSingleRow=*/false,
        /*markColumn=*/nullptr,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr,
        markA,
        std::move(applyAOutputs),
    });

    ColumnCP markB = makeIncludeColumn();
    ColumnVector applyBOutputs;
    applyBOutputs.reserve(
        applyA->outputColumns().size() + rightSide->outputColumns().size() + 1);
    appendAll(applyBOutputs, applyA->outputColumns());
    appendUnique(applyBOutputs, rightSide->outputColumns());
    applyBOutputs.push_back(markB);
    NodeCP applyB = builder().make<Apply>(Apply::Key{
        applyA,
        rightSide,
        recomputeCorrelations(rightSide, applyA->outputColumns()),
        velox::core::JoinType::kLeft,
        std::move(applyBFilter),
        /*enforceSingleRow=*/false,
        /*markColumn=*/nullptr,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr,
        markB,
        std::move(applyBOutputs),
    });

    NodeCP chain = rewrite(applyB);

    // A real body row requires both sides to match. Leg markers are
    // `true` on a match and NULL on a pad, so fold to a clean boolean
    // (NULL → false) before the window: `bool_or` over all-NULL markers
    // would otherwise yield NULL and drop the pad row. Materialize it
    // as a column so the window and filter can reference it.
    ColumnCP matched = makeIncludeColumn();
    NodeCP marked = appendColumn(
        chain,
        matched,
        exprFactory_.makeCoalesce(
            exprFactory_.makeAnd(markA, markB), builder().makeBoolean(false)));

    // Per-outer window: `anyMatch` over the whole partition, and a
    // `padOrdinal` row_number to designate the single pad row to keep.
    ColumnCP anyMatch = Column::createBoolean("__any_match");
    ColumnCP padOrdinal = makeIdColumn("__pad_rn");
    NodeCP windowed =
        addPadCollapseWindow(marked, rowId, matched, anyMatch, padOrdinal);

    // Keep real rows; for a match-less outer keep only its first row (a
    // pad) and drop the duplicate pads.
    ExprCP one = builder().makeLiteral(
        velox::Variant(static_cast<int64_t>(1)), toType(velox::BIGINT()));
    ExprCP keep = exprFactory_.makeOr(
        matched,
        exprFactory_.makeAnd(
            exprFactory_.makeNot(anyMatch),
            exprFactory_.makeEq(padOrdinal, one)));
    NodeCP filtered =
        builder().make<Filter>(Filter::Key{windowed, ExprVector{keep}});

    NodeCP enforced = node->enforceSingleRow()
        ? enforceScalarSingleRow(filtered, rowId)
        : filtered;

    // Shape to the outer Apply's contract. The kept pad row carries
    // real left-side values (gated only by applyA's marker), so body
    // columns must be NULLed when this outer had no match; otherwise a
    // left-only scalar would leak the pad's value. Outer columns are
    // valid on a pad and pass through; the matched marker becomes the
    // outer includeMarker. rowId, leg markers, and window columns drop
    // here.
    PlanObjectSet outerColumns =
        PlanObjectSet::fromObjects(input->outputColumns());
    ExprVector finalExprs;
    finalExprs.reserve(node->outputColumns().size());
    for (ColumnCP outputColumn : node->outputColumns()) {
      if (outputColumn == node->includeMarker()) {
        finalExprs.push_back(matched);
      } else if (outerColumns.contains(outputColumn)) {
        finalExprs.push_back(outputColumn);
      } else {
        finalExprs.push_back(exprFactory_.makeIf(
            matched,
            outputColumn,
            builder().makeNull(outputColumn->value().type)));
      }
    }
    return builder().make<Project>(Project::Key{
        enforced,
        std::move(finalExprs),
        node->outputColumns(),
    });
  }

  // Returns a Project that passes `input`'s columns through unchanged
  // and appends `column` computed as `expr`.
  NodeCP appendColumn(NodeCP input, ColumnCP column, ExprCP expr) {
    const auto& inputColumns = input->outputColumns();

    ExprVector exprs;
    ColumnVector outputs;
    exprs.reserve(inputColumns.size() + 1);
    outputs.reserve(inputColumns.size() + 1);
    appendAll(exprs, inputColumns);
    appendAll(outputs, inputColumns);
    exprs.push_back(expr);
    outputs.push_back(column);
    return builder().make<Project>(
        Project::Key{input, std::move(exprs), std::move(outputs)});
  }

  // Window over PARTITION BY `partition` computing `anyMatch =
  // bool_or(marker)` across the whole partition and `padOrdinal =
  // row_number()` (running frame; used only to pick one pad row).
  NodeCP addPadCollapseWindow(
      NodeCP input,
      ColumnCP partition,
      ColumnCP marker,
      ColumnCP anyMatch,
      ColumnCP padOrdinal) {
    const auto& boolOrName = FunctionRegistry::instance()->boolOr();
    VELOX_USER_CHECK(
        boolOrName.has_value(),
        "Decorrelate kInner cross-join recovery requires bool_or "
        "registered via FunctionRegistry::registerBoolOr");

    // bool_or yields a BOOLEAN (two distinct values).
    ExprCP boolOrCall = builder().makeCall(
        toName(*boolOrName),
        Value(toType(velox::BOOLEAN()), /*cardinality=*/2),
        ExprVector{marker},
        marker->functions() | FunctionSet::kNonDeterministic |
            FunctionSet::kNonDefaultNullBehavior);

    WindowFunctions functions;
    functions.push_back(
        WindowFunction{
            boolOrCall, Frame::wholePartition(), /*ignoreNulls=*/false});
    functions.push_back(rowNumberWindowFunction(padOrdinal));

    ColumnVector outputs;
    outputs.reserve(input->outputColumns().size() + 2);
    appendAll(outputs, input->outputColumns());
    outputs.push_back(anyMatch);
    outputs.push_back(padOrdinal);

    return builder().make<Window>(Window::Key{
        input,
        std::move(functions),
        ExprVector{partition},
        /*orderKeys=*/{},
        /*orderTypes=*/{},
        std::move(outputs),
    });
  }

  // Outer Apply is kLeftSemiProject. Currently supports EXISTS shape
  // (inLhs == nullptr) over kInner cross-join body; mark = markA AND
  // markB. IN shape and other Join kinds NYI loud.
  NodeCP joinPeelSemi(
      ApplyCP node,
      NodeCP input,
      JoinCP joinBody,
      ExprVector joinPredicate,
      ExprVector accumulatedFilter) {
    if (node->inLhs() != nullptr) {
      VELOX_NYI(
          "Decorrelate joinPeel: outer kLeftSemiProject IN over Join "
          "body not yet implemented");
    }
    if (!joinBody->isInner() || !joinPredicate.empty()) {
      VELOX_NYI(
          "Decorrelate joinPeel: outer kLeftSemiProject EXISTS over "
          "Join.kind={} with predicate {} not yet implemented",
          joinBody->joinType(),
          joinPredicate.empty() ? "empty" : "non-empty");
    }

    // Cross-join EXISTS: mark = (∃a) AND (∃b).
    NodeCP leftSide = joinBody->left();
    NodeCP rightSide = joinBody->right();

    ColumnCP markA = Column::createBoolean("_join_chain_markA");
    ColumnVector applyAOutputs;
    applyAOutputs.reserve(input->outputColumns().size() + 1);
    appendAll(applyAOutputs, input->outputColumns());
    applyAOutputs.push_back(markA);
    ColumnVector applyACorrelations =
        recomputeCorrelations(leftSide, input->outputColumns());
    NodeCP applyA = builder().make<Apply>(Apply::Key{
        input,
        leftSide,
        std::move(applyACorrelations),
        velox::core::JoinType::kLeftSemiProject,
        /*filter=*/ExprVector{},
        /*enforceSingleRow=*/false,
        markA,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr,
        /*includeMarker=*/nullptr,
        std::move(applyAOutputs),
    });

    ColumnCP markB = Column::createBoolean("_join_chain_markB");
    ColumnVector applyBOutputs;
    applyBOutputs.reserve(applyA->outputColumns().size() + 1);
    appendAll(applyBOutputs, applyA->outputColumns());
    applyBOutputs.push_back(markB);
    ColumnVector applyBCorrelations =
        recomputeCorrelations(rightSide, applyA->outputColumns());
    NodeCP applyB = builder().make<Apply>(Apply::Key{
        applyA,
        rightSide,
        std::move(applyBCorrelations),
        velox::core::JoinType::kLeftSemiProject,
        /*filter=*/std::move(accumulatedFilter),
        /*enforceSingleRow=*/false,
        markB,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr,
        /*includeMarker=*/nullptr,
        std::move(applyBOutputs),
    });

    NodeCP decorrelatedChain = rewrite(applyB);

    // Final Project: markColumn = markA AND markB.
    ExprCP combinedMark = exprFactory_.makeAnd(markA, markB);
    ExprVector finalExprs;
    finalExprs.reserve(node->outputColumns().size());
    for (ColumnCP outputColumn : node->outputColumns()) {
      if (outputColumn == node->markColumn()) {
        finalExprs.push_back(combinedMark);
      } else {
        finalExprs.push_back(outputColumn);
      }
    }
    return builder().make<Project>(Project::Key{
        decorrelatedChain,
        std::move(finalExprs),
        node->outputColumns(),
    });
  }

  // Peels an AssignUniqueId body operator by lifting it above Apply.
  // Body = `AssignUniqueId(child, idColumn)`. The inner Apply
  // decorrelates `child`; an `AssignUniqueId` placed above re-supplies
  // `idColumn` to the result. ID values differ from the original (per
  // Apply-output row instead of per child row), but the column's
  // identity and schema position are preserved — consumers that rely
  // only on uniqueness (the common case for ESR-style enforcement)
  // are unaffected.
  NodeCP assignUniqueIdPeel(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      ExprVector accumulatedFilter) {
    AssignUniqueIdCP assignUniqueIdBody = body->as<AssignUniqueId>();
    NodeCP newBody = assignUniqueIdBody->input();
    ColumnCP idColumn = assignUniqueIdBody->idColumn();

    ColumnCP innerIncludeMarker =
        node->isLeft() ? makeIncludeColumn() : nullptr;

    ColumnVector innerOutputColumns;
    innerOutputColumns.reserve(
        input->outputColumns().size() +
        (node->isLeftSemiProject() ? 1 : newBody->outputColumns().size() + 1));
    appendAll(innerOutputColumns, input->outputColumns());
    if (node->isLeftSemiProject()) {
      innerOutputColumns.push_back(node->markColumn());
    } else {
      PlanObjectSet innerSeen =
          PlanObjectSet::fromObjects(input->outputColumns());
      for (ColumnCP column : newBody->outputColumns()) {
        if (!innerSeen.contains(column)) {
          innerSeen.add(column);
          innerOutputColumns.push_back(column);
        }
      }
      innerOutputColumns.push_back(innerIncludeMarker);
    }

    ColumnVector innerCorrelations =
        recomputeCorrelations(newBody, input->outputColumns());

    NodeCP innerApply = builder().make<Apply>(Apply::Key{
        input,
        newBody,
        std::move(innerCorrelations),
        node->kind(),
        std::move(accumulatedFilter),
        node->enforceSingleRow(),
        node->markColumn(),
        node->inLhs(),
        node->inBodyKey(),
        innerIncludeMarker,
        std::move(innerOutputColumns),
    });
    NodeCP decorrelatedInner = rewrite(innerApply);

    NodeCP withId = builder().make<AssignUniqueId>(
        AssignUniqueId::Key{decorrelatedInner, idColumn});

    // Final Project reorders `withId`'s cols to match
    // `node->outputColumns()` and aliases the inner includeMarker to
    // the outer's (kLeft only).
    ExprVector finalExprs;
    finalExprs.reserve(node->outputColumns().size());
    for (ColumnCP outputColumn : node->outputColumns()) {
      if (node->isLeft() && outputColumn == node->includeMarker()) {
        finalExprs.push_back(innerIncludeMarker);
      } else {
        finalExprs.push_back(outputColumn);
      }
    }
    return builder().make<Project>(Project::Key{
        withId,
        std::move(finalExprs),
        node->outputColumns(),
    });
  }

  // Aggregate peel (Rule A) for kLeft (Cases 1, 2, 3).
  //
  // Shape:
  //   - Project (strip rn, COALESCE empty-input aggs, reorder)
  //     - Aggregate (groupingKeys = [rn, gby],
  //                  aggregates = user aggs FILTER (_include)
  //                            ++ arbitrary(L.col) for each L.col)
  //       - Apply (kLeft, filter = F_pre)  ← recurses
  //         - body  = Project(agg.input, [..., _include := true])
  //         - input = AssignUniqueId(L) → tagged_L
  //
  // In scope: kind = kLeft; gby empty or non-empty; F_post empty
  // (accFilter doesn't reference Agg's aggregate-result output cols —
  // refs to gby output cols are fine, they're pass-through in lifted
  // Agg).
  //
  // COALESCE for non-NULL empty-input aggregates: applied in final
  // Project. The lifted Agg's aggregate-result output uses a fresh
  // Column* (slot-identity invariant); the original aggregate output
  // Column* is reused at the final Project's output position so parent
  // expressions resolve.
  NodeCP aggregatePeel(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      ExprVector accumulatedFilter) {
    AggregateCP aggregate = body->as<Aggregate>();

    if (node->isLeftSemiProject()) {
      return aggregatePeelSemi(node, input, body, accumulatedFilter);
    }
    if (!node->isLeft()) {
      VELOX_NYI(
          "Decorrelate: Aggregate peel for kind={} not yet supported",
          node->kind());
    }
    if (aggregate->input()->is(NodeType::kGroupId)) {
      VELOX_NYI("Decorrelate: Aggregate peel over GROUPING SETS NYI");
    }

    const size_t numGroupingKeys = aggregate->groupingKeys().size();
    auto [filterPreConjuncts, filterPostConjuncts] =
        splitFilterPreAndPost(accumulatedFilter, aggregate, numGroupingKeys);

    // Equi-correlated scalar aggregate with no boundary pre-filter, no inner
    // GROUP BY, and aggregates that don't reference the outer: group the body
    // once by the correlation key and LEFT JOIN it back to the outer,
    // aggregating once per key instead of once per outer row. With no inner
    // GROUP BY the grouped body has one row per correlation key, so the
    // join-back yields one row per outer. Other correlations use the general
    // shape below.
    if (filterPreConjuncts.empty() && numGroupingKeys == 0 &&
        !aggregateArgsReferenceOuter(aggregate, input->outputColumns())) {
      if (auto correlation =
              liftEquiCorrelation(aggregate->input(), input->outputColumns())) {
        return aggregatePeelEqui(
            node,
            input,
            aggregate,
            std::move(*correlation),
            filterPostConjuncts);
      }
    }

    // Case 4 NYI guard: kLeft + non-empty F_post + non-empty gby.
    // The IF-in-outer-Project shape is correct only when the lifted
    // Aggregate produces exactly one row per outer (empty gby). With
    // non-empty gby it produces N rows per outer; IF-in-Project would
    // emit all N rows with selective NULLs while the original kLeft
    // Apply over Aggregate with HAVING-style filter drops failing rows
    // and pads ONCE if all fail. Wrong row counts.
    if (!filterPostConjuncts.empty() && numGroupingKeys > 0) {
      VELOX_NYI(
          "Decorrelate Case 4 (kLeft, Apply.filter non-null with F_post, "
          "Agg.gby non-empty) is not yet implemented");
    }

    // Validator: reject aggregates whose args reference ONLY outer
    // correlation columns (e.g., `max(t.a)`, `count(t.a)`). These are
    // "pure-outer" aggregates: SQL semantics demand outer-scope
    // collapse (one output row per outer scope), but the lifted shape
    // produces per-(rn, gby) results — wrong row count and wrong
    // values. A separate rewrite is required (tracked as the
    // pure-outer aggregate semantics work); until it lands we decline
    // here.
    //
    // Aggregates with literal-only args (e.g., `count(1)`) are NOT
    // pure-outer — no column refs at all, and the lifted shape handles
    // them correctly via FILTER(_include). The check is "ARGS
    // REFERENCE AT LEAST ONE COLUMN AND EVERY REFERENCED COLUMN IS
    // OUTER".
    AggregateRecovery::validateAggregateArgs(
        aggregate->aggregates(), node->correlationColumns());
    AggregateRecovery recovery(builder(), exprFactory_);

    // Build the recovered chain bottom-up.
    auto innerApply = buildAggregateInnerApply(
        input, aggregate, std::move(filterPreConjuncts));
    NodeCP decorrelatedInner = rewrite(innerApply.apply);

    auto wraps = buildAggregateWraps(aggregate, numGroupingKeys);
    NodeCP liftedAggregate = buildLiftedAggregate(
        input,
        aggregate,
        recovery,
        innerApply.rowIdColumn,
        innerApply.includeMarker,
        wraps,
        numGroupingKeys,
        decorrelatedInner);

    // With an inner GROUP BY the lifted aggregate groups by (rowId, gby keys),
    // so it can emit several rows per outer row; grouping on the rowId alone no
    // longer guarantees one row per outer. A scalar subquery must raise on the
    // first outer with more than one group, so assert uniqueness on the rowId.
    if (node->enforceSingleRow() && numGroupingKeys > 0) {
      liftedAggregate =
          enforceScalarSingleRow(liftedAggregate, innerApply.rowIdColumn);
    }

    ExprCP filterPostSubstituted = buildFilterPostSubstituted(
        filterPostConjuncts, aggregate, wraps, numGroupingKeys);

    return buildAggregateFinalProject(
        node,
        input,
        aggregate,
        wraps,
        filterPostSubstituted,
        liftedAggregate,
        numGroupingKeys);
  }

  // True if any aggregate's arguments reference an outer (input) column.
  // Such aggregates need the outer columns visible below the aggregation,
  // which the grouped join-back form cannot provide.
  static bool aggregateArgsReferenceOuter(
      AggregateCP aggregate,
      const ColumnVector& outerColumns) {
    for (const auto* call : aggregate->aggregates()) {
      for (ExprCP argument : call->args()) {
        if (argument->columns().containsAny(outerColumns)) {
          return true;
        }
      }
    }
    return false;
  }

  // Equi-correlation lifted from an Aggregate's input for the grouped
  // join-back decorrelation.
  struct EquiCorrelation {
    // The aggregate's input with the correlation predicates removed.
    NodeCP cleanBody;
    // Equi keys: `leftKeys` over the outer input, `rightKeys` over
    // `cleanBody`.
    ExprVector leftKeys;
    ExprVector rightKeys;
  };

  // Lifts the equi-correlation out of an Aggregate's input. Succeeds only
  // when the correlation sits in a Filter chain above a correlation-free
  // subtree and every correlation conjunct is an equality partitioning
  // cleanly into an outer-side and a body-side expression; returns nullopt
  // otherwise.
  std::optional<EquiCorrelation> liftEquiCorrelation(
      NodeCP aggregateInput,
      const ColumnVector& inputColumns) {
    ExprVector correlation;
    ExprVector localPredicates;
    NodeCP base = aggregateInput;
    while (base->is(NodeType::kFilter)) {
      const Filter* filter = base->as<Filter>();
      for (ExprCP conjunct : filter->predicates()) {
        (conjunct->columns().containsAny(inputColumns) ? correlation
                                                       : localPredicates)
            .push_back(conjunct);
      }
      base = filter->input();
    }

    // The subtree below the lifted Filters must be correlation-free, and
    // there must be at least one correlation conjunct to lift.
    if (correlation.empty() ||
        !recomputeCorrelations(base, inputColumns).empty()) {
      return std::nullopt;
    }

    JoinCondition::Split split = JoinCondition::splitEquiKeys(
        correlation,
        PlanObjectSet::fromObjects(inputColumns),
        PlanObjectSet::fromObjects(base->outputColumns()));
    if (!split.residual.empty()) {
      return std::nullopt;
    }

    NodeCP cleanBody = localPredicates.empty()
        ? base
        : builder().make<Filter>(Filter::Key{base, std::move(localPredicates)});
    return EquiCorrelation{
        cleanBody, std::move(split.leftKeys), std::move(split.rightKeys)};
  }

  // Decorrelates an equi-correlated kLeft Aggregate Apply (no inner GROUP
  // BY) by grouping the (correlation-free) body once by the correlation
  // key, then LEFT JOINing the result back to the outer on the correlation
  // equi keys. Empty-input aggregates receive their empty value on outer
  // rows with no match via the COALESCE in `wraps`; a post-aggregation
  // filter is applied at the final Project.
  NodeCP aggregatePeelEqui(
      ApplyCP node,
      NodeCP input,
      AggregateCP aggregate,
      EquiCorrelation correlation,
      const ExprVector& filterPostConjuncts) {
    std::vector<AggregateWrap> wraps =
        buildAggregateWraps(aggregate, /*numGroupingKeys=*/0);

    // The correlation keys become the new Aggregate's grouping keys and the
    // join-back's right keys: reuse the body column for a plain column, mint
    // a fresh column for a computed key.
    ColumnVector rightKeyColumns;
    rightKeyColumns.reserve(correlation.rightKeys.size());
    for (ExprCP key : correlation.rightKeys) {
      rightKeyColumns.push_back(
          key->is(PlanType::kColumnExpr)
              ? key->as<Column>()
              : Column::create("__groupingKey", key->value()));
    }

    // Aggregate output: [correlation key columns, raw aggregate outputs].
    ColumnVector aggregateOutputColumns;
    aggregateOutputColumns.reserve(rightKeyColumns.size() + wraps.size());
    appendAll(aggregateOutputColumns, rightKeyColumns);
    for (const auto& wrap : wraps) {
      aggregateOutputColumns.push_back(wrap.liftedOutput);
    }

    AggregateCallVector aggregates = aggregate->aggregates();
    NodeCP groupedBody = builder().make<Aggregate>(Aggregate::Key{
        correlation.cleanBody,
        std::move(correlation.rightKeys),
        std::move(aggregates),
        std::move(aggregateOutputColumns),
    });

    // LEFT JOIN the grouped body back to the outer on the correlation equi
    // keys. The output keeps the outer columns and the (raw) aggregate
    // outputs; the correlation key columns are internal and dropped.
    ColumnVector joinOutput;
    joinOutput.reserve(input->outputColumns().size() + wraps.size());
    appendAll(joinOutput, input->outputColumns());
    for (const auto& wrap : wraps) {
      joinOutput.push_back(wrap.liftedOutput);
    }

    ExprVector rightKeyExprs;
    appendAll(rightKeyExprs, rightKeyColumns);

    NodeCP join = builder().make<Join>(Join::Key{
        input,
        groupedBody,
        velox::core::JoinType::kLeft,
        std::move(correlation.leftKeys),
        std::move(rightKeyExprs),
        /*filter=*/{},
        /*nullAware=*/false,
        /*nullAsValue=*/false,
        std::move(joinOutput),
    });

    ExprCP filterPostSubstituted = buildFilterPostSubstituted(
        filterPostConjuncts, aggregate, wraps, /*numGroupingKeys=*/0);

    return buildAggregateFinalProject(
        node,
        input,
        aggregate,
        wraps,
        filterPostSubstituted,
        join,
        /*numGroupingKeys=*/0);
  }

  // For each aggregate in the original Aggregate, decides what the
  // lifted Aggregate should emit (`liftedOutput`) and what the final
  // Project should produce at the original output position
  // (`finalExpression`). Empty-input value resolution via the
  // FunctionRegistry: aggregates whose empty value is non-NULL (count,
  // count_if, etc.) need COALESCE; others pass through.
  //
  // Slot-identity invariant: a Column* must carry the same value
  // across all output positions. For COALESCE-needing aggregates,
  // the lifted Aggregate's raw output
  // (NULL for pad rows) and the final Project's COALESCE output
  // (non-NULL for pad rows) must occupy DIFFERENT Column*s. We
  // allocate a fresh Column* for the lifted raw output and reuse the
  // original output Column* at the final Project — parent expressions
  // referencing the original Column* see the coalesced value.
  struct AggregateWrap {
    // What the lifted Aggregate emits for this aggregate. Either the
    // original output Column* (no COALESCE needed) or a fresh Column*.
    ColumnCP liftedOutput;
    // The expression at the final Project's output position. Either
    // the original Column* itself (pass-through) or a COALESCE Call.
    ExprCP finalExpression;
  };

  std::vector<AggregateWrap> buildAggregateWraps(
      AggregateCP aggregate,
      size_t numGroupingKeys) {
    std::vector<AggregateWrap> wraps;
    wraps.reserve(aggregate->aggregates().size());
    const auto* registry = FunctionRegistry::instance();
    for (size_t i = 0; i < aggregate->aggregates().size(); ++i) {
      const auto* aggregateCall = aggregate->aggregates()[i];
      // outputColumns positional layout: [gby_cols, agg_result_cols].
      ColumnCP originalOutput = aggregate->outputColumns()[numGroupingKeys + i];

      std::vector<const velox::Type*> argumentTypes;
      argumentTypes.reserve(aggregateCall->args().size());
      for (ExprCP argument : aggregateCall->args()) {
        argumentTypes.push_back(argument->value().type);
      }
      velox::Variant emptyValue = registry->aggregateResultForEmptyInput(
          aggregateCall->name(), argumentTypes);

      if (emptyValue.isNull()) {
        wraps.push_back({originalOutput, originalOutput});
      } else {
        ColumnCP rawOutput =
            Column::create(originalOutput->name(), originalOutput->value());
        const Literal* emptyLiteral = builder().makeLiteral(
            std::move(emptyValue), originalOutput->value().type);
        ExprCP coalesceExpression =
            exprFactory_.makeCoalesce(rawOutput, emptyLiteral);
        wraps.push_back({rawOutput, coalesceExpression});
      }
    }
    return wraps;
  }

  // Splits the accumulated Apply.filter into F_pre (no refs to
  // aggregate-result columns; applied pre-aggregation as Join.filter)
  // and F_post (refs aggregate-result columns; applied post-
  // aggregation via the final Project's IF wrap per Case 3).
  //
  // Refs to grouping-key columns are treated as F_pre — those columns
  // pass through the lifted Aggregate unchanged and are valid pre- or
  // post-aggregation.
  std::pair<ExprVector, ExprVector> splitFilterPreAndPost(
      const ExprVector& accumulatedFilter,
      AggregateCP aggregate,
      size_t numGroupingKeys) {
    if (accumulatedFilter.empty()) {
      return {ExprVector{}, ExprVector{}};
    }
    PlanObjectSet aggregateResultColumns;
    for (size_t i = numGroupingKeys; i < aggregate->outputColumns().size();
         ++i) {
      aggregateResultColumns.add(aggregate->outputColumns()[i]);
    }
    ExprVector pre;
    ExprVector post;
    for (ExprCP conjunct : accumulatedFilter) {
      bool referencesAggregateResult = false;
      conjunct->columns().forEach<Column>([&](const Column* column) {
        if (aggregateResultColumns.contains(column)) {
          referencesAggregateResult = true;
        }
      });
      (referencesAggregateResult ? post : pre).push_back(conjunct);
    }
    return {std::move(pre), std::move(post)};
  }

  // Result of the inner-Apply construction step of Aggregate peel.
  struct InnerApplyResult {
    NodeCP apply;
    ColumnCP rowIdColumn;
    ColumnCP includeMarker;
  };

  // Builds the inner Apply (kLeft, filter = F_pre) over
  // AssignUniqueId(input) with body = aggregate's input. The Apply's
  // includeMarker is consumed by the lifted Aggregate's FILTER.
  // The driver recurses on this inner Apply to handle any remaining
  // peels in the Aggregate's input chain. enforceSingleRow is false:
  // the lifted Aggregate above guarantees ≤1 row per row-id by
  // grouping on it, so no separate per-row assertion is needed.
  InnerApplyResult buildAggregateInnerApply(
      NodeCP input,
      AggregateCP aggregate,
      ExprVector filterPre) {
    ColumnCP rowIdColumn = makeIdColumn();
    NodeCP taggedInput =
        builder().make<AssignUniqueId>(AssignUniqueId::Key{input, rowIdColumn});

    NodeCP body = aggregate->input();

    // Fresh includeMarker per inner Apply.
    ColumnCP includeMarker = makeIncludeColumn();

    ColumnVector innerOutputColumns;
    innerOutputColumns.reserve(
        taggedInput->outputColumns().size() + body->outputColumns().size() + 1);
    appendAll(innerOutputColumns, taggedInput->outputColumns());
    PlanObjectSet bodySeen =
        PlanObjectSet::fromObjects(taggedInput->outputColumns());
    for (ColumnCP column : body->outputColumns()) {
      if (!bodySeen.contains(column)) {
        bodySeen.add(column);
        innerOutputColumns.push_back(column);
      }
    }
    innerOutputColumns.push_back(includeMarker);

    ColumnVector innerCorrelations =
        recomputeCorrelations(body, taggedInput->outputColumns());

    NodeCP applyNode = builder().make<Apply>(Apply::Key{
        taggedInput,
        body,
        std::move(innerCorrelations),
        velox::core::JoinType::kLeft,
        std::move(filterPre),
        /*enforceSingleRow=*/false,
        /*markColumn=*/nullptr,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr,
        includeMarker,
        std::move(innerOutputColumns),
    });
    return {applyNode, rowIdColumn, includeMarker};
  }

  // Builds the lifted Aggregate above the decorrelated inner Apply.
  //
  // groupingKeys = [rowId, original gby...]: each (rowId, gby_values)
  // tuple corresponds to one outer row's worth of body rows grouped by
  // the original gby. The outer-input columns (L.cols) are NOT in
  // groupingKeys (they would inflate cardinality with no benefit);
  // they ride out via `arbitrary(L.col)` aggregates instead, without
  // the _include FILTER (pad rows have real L.col values that we want
  // preserved).
  //
  // aggregates = (a) original aggregates with `count(*)` rewritten and
  // the `_include` marker AND-folded into every aggregate's FILTER
  // condition; plus (b) `arbitrary(L.col)` for each outer-input
  // column.
  NodeCP buildLiftedAggregate(
      NodeCP input,
      AggregateCP aggregate,
      AggregateRecovery& recovery,
      ColumnCP rowIdColumn,
      ColumnCP includeMarker,
      const std::vector<AggregateWrap>& wraps,
      size_t numGroupingKeys,
      NodeCP decorrelatedInner) {
    AggregateCallVector liftedAggregates =
        recovery.rewriteCountStar(aggregate->aggregates(), includeMarker);
    liftedAggregates =
        recovery.addFilterCondition(liftedAggregates, includeMarker);
    for (ColumnCP outerColumn : input->outputColumns()) {
      liftedAggregates.push_back(makeArbitrary(outerColumn));
    }

    ExprVector groupingKeys;
    groupingKeys.reserve(1 + numGroupingKeys);
    groupingKeys.push_back(rowIdColumn);
    appendAll(groupingKeys, aggregate->groupingKeys());

    // outputColumns positional contract:
    //   [groupingKeys (rowId, gby_cols),
    //    user aggregate outputs (wraps[i].liftedOutput),
    //    arbitrary(L.col) outputs (reuse L.col Column*s)]
    ColumnVector liftedOutputColumns;
    liftedOutputColumns.reserve(
        1 + numGroupingKeys + wraps.size() + input->outputColumns().size());
    liftedOutputColumns.push_back(rowIdColumn);
    for (size_t i = 0; i < numGroupingKeys; ++i) {
      liftedOutputColumns.push_back(aggregate->outputColumns()[i]);
    }
    for (const auto& wrap : wraps) {
      liftedOutputColumns.push_back(wrap.liftedOutput);
    }
    appendAll(liftedOutputColumns, input->outputColumns());

    return builder().make<Aggregate>(Aggregate::Key{
        decorrelatedInner,
        std::move(groupingKeys),
        std::move(liftedAggregates),
        std::move(liftedOutputColumns),
    });
  }

  // Substitutes each aggregate-result column reference in F_post
  // conjuncts with the corresponding `c_coal` expression
  // (= wraps[i].finalExpression). Ensures F_post evaluates over the
  // COALESCE-corrected per-outer values rather than the raw
  // aggregate results.
  ExprCP buildFilterPostSubstituted(
      const ExprVector& filterPostConjuncts,
      AggregateCP aggregate,
      const std::vector<AggregateWrap>& wraps,
      size_t numGroupingKeys) {
    if (filterPostConjuncts.empty()) {
      return nullptr;
    }
    ColumnVector substitutionSource;
    ExprVector substitutionTarget;
    substitutionSource.reserve(wraps.size());
    substitutionTarget.reserve(wraps.size());
    for (size_t i = 0; i < wraps.size(); ++i) {
      substitutionSource.push_back(
          aggregate->outputColumns()[numGroupingKeys + i]);
      substitutionTarget.push_back(wraps[i].finalExpression);
    }
    ExprCP combined = exprFactory_.andAll(filterPostConjuncts);
    return exprFactory_.substitute(
        combined, substitutionSource, substitutionTarget);
  }

  // Final Project: shapes `child`'s output to node->outputColumns()
  // (= L.cols ++ gby_cols ++ aggregate_results ++ includeMarker).
  //   - L.cols: pass-through from the input columns (the outer side of
  //     the join-back, or the lifted `arbitrary` outputs).
  //   - gby_cols: pass-through from the grouping-key outputs.
  //   - aggregate_results: each `wrap.finalExpression` (pass-through
  //     or COALESCE), additionally wrapped in IF(F_post_subst, ...,
  //     NULL) when a post-aggregation filter is present. The IF preserves
  //     the outer row and nulls the aggregate values when the filter
  //     fails — kLeft semantics for HAVING-style predicates.
  NodeCP buildAggregateFinalProject(
      ApplyCP node,
      NodeCP input,
      AggregateCP aggregate,
      const std::vector<AggregateWrap>& wraps,
      ExprCP filterPostSubstituted,
      NodeCP child,
      size_t numGroupingKeys) {
    ExprVector finalExpressions;
    finalExpressions.reserve(node->outputColumns().size());
    appendAll(finalExpressions, input->outputColumns());
    for (size_t i = 0; i < numGroupingKeys; ++i) {
      finalExpressions.push_back(aggregate->outputColumns()[i]);
    }
    for (size_t i = 0; i < wraps.size(); ++i) {
      ExprCP expression = wraps[i].finalExpression;
      if (filterPostSubstituted != nullptr) {
        ColumnCP originalOutput =
            aggregate->outputColumns()[numGroupingKeys + i];
        TypeCP type = originalOutput->value().type;
        expression = exprFactory_.makeIf(
            filterPostSubstituted, expression, builder().makeNull(type));
      }
      finalExpressions.push_back(expression);
    }
    // includeMarker is `true` for every output row: this level emits one
    // row per outer (grouped by the per-outer id, or one match per outer
    // from the join-back), so there are no pad rows to exclude.
    finalExpressions.push_back(builder().makeBoolean(true));
    return builder().make<Project>(Project::Key{
        child,
        std::move(finalExpressions),
        node->outputColumns(),
    });
  }

  // Builds the terminus shape from a fully-peeled Apply.
  NodeCP terminus(ApplyCP apply, NodeCP input, NodeCP body, ExprVector filter) {
    switch (apply->kind()) {
      case velox::core::JoinType::kLeft:
        return terminusLeft(apply, input, body, filter);
      case velox::core::JoinType::kLeftSemiProject:
        return terminusSemi(apply, input, body, filter);
      default:
        VELOX_NYI("Decorrelate: unexpected Apply.kind {}", apply->kind());
    }
  }

  // Adds `_include := true` to body. After LEFT JOIN it reads `true`
  // on real body rows and NULL on pad rows.
  // Wraps body in a Project exposing body cols absent from `excluded`
  // plus `_include := true`. After LEFT JOIN above this, the marker
  // reads `true` on real body rows and NULL on pad rows. Excluding
  // input cols keeps the join's right side disjoint from its left.
  NodeCP addIncludeMarkerToBody(
      NodeCP body,
      ColumnCP includeMarker,
      const ColumnVector& excluded) {
    PlanObjectSet excludedSet = PlanObjectSet::fromObjects(excluded);
    ExprVector exprs;
    ColumnVector outputColumns;
    exprs.reserve(body->outputColumns().size() + 1);
    outputColumns.reserve(body->outputColumns().size() + 1);
    for (ColumnCP column : body->outputColumns()) {
      if (!excludedSet.contains(column)) {
        exprs.push_back(column);
        outputColumns.push_back(column);
      }
    }
    exprs.push_back(builder().makeBoolean(true));
    outputColumns.push_back(includeMarker);

    return builder().make<Project>(
        Project::Key{body, std::move(exprs), std::move(outputColumns)});
  }

  NodeCP
  terminusLeft(ApplyCP apply, NodeCP input, NodeCP body, ExprVector filter) {
    NodeCP markedBody = addIncludeMarkerToBody(
        body, apply->includeMarker(), input->outputColumns());

    if (!apply->enforceSingleRow()) {
      // Plain LEFT JOIN terminus: no per-outer cardinality assertion.
      JoinCondition::Split split = JoinCondition::splitEquiKeys(
          filter,
          PlanObjectSet::fromObjects(input->outputColumns()),
          PlanObjectSet::fromObjects(markedBody->outputColumns()));
      return builder().make<Join>(Join::Key{
          input,
          markedBody,
          velox::core::JoinType::kLeft,
          std::move(split.leftKeys),
          std::move(split.rightKeys),
          std::move(split.residual),
          /*nullAware=*/false,
          /*nullAsValue=*/false,
          apply->outputColumns(),
      });
    }

    // ESR=true: wrap LEFT JOIN in EnforceDistinct on the per-row id
    // (from AssignUniqueId on input) so cardinality > 1 per outer
    // raises at runtime. Final Project strips the id so the output
    // matches Apply's contract.

    // Tag input rows with a fresh BIGINT id column.
    ColumnCP idColumn = makeIdColumn();
    NodeCP taggedInput =
        builder().make<AssignUniqueId>(AssignUniqueId::Key{input, idColumn});

    // Build LEFT JOIN; output carries taggedInput.cols ++ markedBody.cols
    // (i.e., input.cols + idColumn + body.cols + includeMarker).
    ColumnVector joinOutput;
    joinOutput.reserve(
        taggedInput->outputColumns().size() +
        markedBody->outputColumns().size());
    appendAll(joinOutput, taggedInput->outputColumns());
    appendAll(joinOutput, markedBody->outputColumns());

    JoinCondition::Split split = JoinCondition::splitEquiKeys(
        filter,
        PlanObjectSet::fromObjects(taggedInput->outputColumns()),
        PlanObjectSet::fromObjects(markedBody->outputColumns()));
    NodeCP join = builder().make<Join>(Join::Key{
        taggedInput,
        markedBody,
        velox::core::JoinType::kLeft,
        std::move(split.leftKeys),
        std::move(split.rightKeys),
        std::move(split.residual),
        /*nullAware=*/false,
        /*nullAsValue=*/false,
        std::move(joinOutput),
    });

    NodeCP enforced = enforceScalarSingleRow(join, idColumn);

    // Strip the id column. Final outputColumns match Apply's contract
    // (input.cols ++ body.cols ++ includeMarker). EnforceDistinct
    // passes through (incl. the id col); the Project here is the
    // column-prune that removes the id from the public output schema.
    ExprVector finalExprs;
    finalExprs.reserve(apply->outputColumns().size());
    appendAll(finalExprs, apply->outputColumns());
    return builder().make<Project>(Project::Key{
        enforced,
        std::move(finalExprs),
        apply->outputColumns(),
    });
  }

  // Aggregate peel for kLeftSemiProject:
  //   - body aggregate elidable → drop it, recurse against its input
  //   - inLhs != nullptr → Rule B-IN  (Cases 5b/6b/7b/8b)
  //   - inLhs == nullptr → Rule B-EXISTS (Cases 5a/6a/7a/8a)
  NodeCP aggregatePeelSemi(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      ExprVector accumulatedFilter) {
    AggregateCP aggregate = body->as<Aggregate>();

    if (aggregate->input()->is(NodeType::kGroupId)) {
      VELOX_NYI(
          "Decorrelate: Aggregate peel for kLeftSemiProject over GROUPING "
          "SETS NYI");
    }

    if (canElideBodyAggregate(node, aggregate, accumulatedFilter)) {
      return aggregatePeelSemiDropAggregate(
          node, input, aggregate, accumulatedFilter);
    }

    if (node->inLhs() != nullptr) {
      return aggregatePeelSemiIn(node, input, body, accumulatedFilter);
    }
    return aggregatePeelSemiExists(node, input, body, accumulatedFilter);
  }

  // Drops the body Aggregate and rewrites the Apply against its input.
  // Caller must have verified via `canElideBodyAggregate` that no
  // aggregate-result column is referenced.
  NodeCP aggregatePeelSemiDropAggregate(
      ApplyCP node,
      NodeCP input,
      AggregateCP aggregate,
      ExprVector accumulatedFilter) {
    ColumnVector substitutionSource =
        prefix(aggregate->outputColumns(), aggregate->groupingKeys().size());

    ExprVector newFilter = exprFactory_.substitute(
        accumulatedFilter, substitutionSource, aggregate->groupingKeys());
    ExprCP newInBodyKey = substituteOrNull(
        exprFactory_,
        node->inBodyKey(),
        substitutionSource,
        aggregate->groupingKeys());

    NodeCP newBody = aggregate->input();
    ColumnVector innerCorrelations =
        recomputeCorrelations(newBody, input->outputColumns());

    ColumnVector innerOutputColumns;
    innerOutputColumns.reserve(input->outputColumns().size() + 1);
    appendAll(innerOutputColumns, input->outputColumns());
    innerOutputColumns.push_back(node->markColumn());

    NodeCP newApply = builder().make<Apply>(Apply::Key{
        input,
        newBody,
        std::move(innerCorrelations),
        node->kind(),
        std::move(newFilter),
        node->enforceSingleRow(),
        node->markColumn(),
        node->inLhs(),
        newInBodyKey,
        /*includeMarker=*/nullptr,
        std::move(innerOutputColumns),
    });
    return rewrite(newApply);
  }

  // Aggregate peel for kLeftSemiProject in the EXISTS branch (Rule
  // B-EXISTS, Cases 5a/6a/7a/8a).
  //
  // Three branches, dispatched by (F_post presence, gby presence):
  //
  // - **Case 5a** (no F_post, empty gby): mark = TRUE per outer.
  //   Body is elided — scalar aggregates always emit one row, so
  //   EXISTS is unconditionally TRUE. Matches strict SQL; same
  //   trade-off as Translate's EXISTS-over-scalar-agg fold, which
  //   catches the common case earlier.
  //
  // - **Case 6a** (no F_post, non-empty gby): single Aggregate per
  //   `[rn]` with `bool_or(_include)` collapsing across gby groups.
  //   Drops the body's user aggregates (kSemi doesn't expose them).
  //   For unmatched outer (pad-only): bool_or({NULL}) → NULL →
  //   COALESCE → false. Matches strict SQL: gby on empty input → 0
  //   groups → EXISTS = false.
  //
  // - **Cases 7a / 8a** (F_post present): unified Stage 1 / Stage 2
  //   shape paralleling `aggregatePeelSemiIn`, with the equi-pair
  //   dropped from `combined` and the mark CASE reduced to two-state
  //   (no null-aware tri-state — EXISTS has no equi-pair to source
  //   NULL from). The `combined` bit is `COALESCE(F_post_subst,
  //   false)`. Path 1 (empty gby = 7a) emits one row per outer; mark
  //   = combined. Path 2 (non-empty gby = 8a) collapses gby groups
  //   per outer via `bool_or(combined) FILTER(_include)`.
  NodeCP aggregatePeelSemiExists(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      ExprVector accumulatedFilter) {
    AggregateCP aggregate = body->as<Aggregate>();
    const size_t numGroupingKeys = aggregate->groupingKeys().size();

    auto [filterPreConjuncts, filterPostConjuncts] =
        splitFilterPreAndPost(accumulatedFilter, aggregate, numGroupingKeys);

    // Case 5a: scalar agg with no HAVING. Per strict SQL, mark = TRUE
    // for every outer regardless of body content (scalar aggs always
    // emit one row → EXISTS sees that row → TRUE). The body is elided
    // — same trade-off as Translate's EXISTS-over-scalar-agg fold,
    // which catches the common case earlier. This handles the rare
    // shapes Translate's fold misses (e.g., bodies with a Sort/Window
    // operator above the Aggregate that breaks Translate's walker).
    if (filterPostConjuncts.empty() && numGroupingKeys == 0) {
      ExprVector finalExprs;
      finalExprs.reserve(input->outputColumns().size() + 1);
      appendAll(finalExprs, input->outputColumns());
      finalExprs.push_back(builder().makeBoolean(true));
      return builder().make<Project>(Project::Key{
          input,
          std::move(finalExprs),
          node->outputColumns(),
      });
    }

    AggregateRecovery recovery(builder(), exprFactory_);
    auto innerApply = buildAggregateInnerApply(
        input, aggregate, std::move(filterPreConjuncts));
    NodeCP decorrelatedInner = rewrite(innerApply.apply);

    // Case 6a: non-empty gby with no HAVING. Single Aggregate per
    // [rn] with `bool_or(_include)` collapsing across gby groups.
    // Drops the body's user aggregates entirely — kSemi doesn't
    // expose them, and without F_post nothing else references them.
    if (filterPostConjuncts.empty()) {
      ColumnCP rawMark = Column::create(
          node->markColumn()->name(), node->markColumn()->value());

      AggregateCallVector aggregates;
      aggregates.reserve(1 + input->outputColumns().size());
      aggregates.push_back(recovery.makeBoolOr(innerApply.includeMarker));
      for (ColumnCP outerColumn : input->outputColumns()) {
        aggregates.push_back(makeArbitrary(outerColumn));
      }

      ColumnVector outputs;
      outputs.reserve(2 + input->outputColumns().size());
      outputs.push_back(innerApply.rowIdColumn);
      outputs.push_back(rawMark);
      appendAll(outputs, input->outputColumns());

      NodeCP agg = builder().make<Aggregate>(Aggregate::Key{
          decorrelatedInner,
          ExprVector{innerApply.rowIdColumn},
          std::move(aggregates),
          std::move(outputs),
      });

      ExprCP markExpr =
          exprFactory_.makeCoalesce(rawMark, builder().makeBoolean(false));

      ExprVector finalExprs;
      finalExprs.reserve(input->outputColumns().size() + 1);
      appendAll(finalExprs, input->outputColumns());
      finalExprs.push_back(markExpr);
      return builder().make<Project>(Project::Key{
          agg,
          std::move(finalExprs),
          node->outputColumns(),
      });
    }

    // Cases 7a / 8a: F_post present. Stage 1 keeps user aggregates so
    // F_post can reference them (via wraps substitution). The
    // validator only applies here — 5a / 6a don't compute the user
    // aggregates at all, so the pure-outer-args concern is moot.
    AggregateRecovery::validateAggregateArgs(
        aggregate->aggregates(), node->correlationColumns());

    auto wraps = buildAggregateWraps(aggregate, numGroupingKeys);

    AggregateCallVector stage1Aggregates = recovery.rewriteCountStar(
        aggregate->aggregates(), innerApply.includeMarker);
    stage1Aggregates =
        recovery.addFilterCondition(stage1Aggregates, innerApply.includeMarker);
    for (ColumnCP outerColumn : input->outputColumns()) {
      stage1Aggregates.push_back(makeArbitrary(outerColumn));
    }
    ColumnCP padPresentColumn = nullptr;
    if (numGroupingKeys > 0) {
      padPresentColumn = Column::create(
          innerApply.includeMarker->name(), innerApply.includeMarker->value());
      stage1Aggregates.push_back(makeArbitrary(innerApply.includeMarker));
    }

    ExprVector stage1GroupingKeys;
    stage1GroupingKeys.reserve(1 + numGroupingKeys);
    stage1GroupingKeys.push_back(innerApply.rowIdColumn);
    appendAll(stage1GroupingKeys, aggregate->groupingKeys());

    ColumnVector stage1OutputColumns;
    stage1OutputColumns.reserve(
        1 + numGroupingKeys + wraps.size() + input->outputColumns().size() +
        (padPresentColumn != nullptr ? 1 : 0));
    stage1OutputColumns.push_back(innerApply.rowIdColumn);
    for (size_t i = 0; i < numGroupingKeys; ++i) {
      stage1OutputColumns.push_back(aggregate->outputColumns()[i]);
    }
    for (const auto& wrap : wraps) {
      stage1OutputColumns.push_back(wrap.liftedOutput);
    }
    appendAll(stage1OutputColumns, input->outputColumns());
    if (padPresentColumn != nullptr) {
      stage1OutputColumns.push_back(padPresentColumn);
    }

    NodeCP stage1 = builder().make<Aggregate>(Aggregate::Key{
        decorrelatedInner,
        std::move(stage1GroupingKeys),
        std::move(stage1Aggregates),
        std::move(stage1OutputColumns),
    });

    const Literal* falseLiteral = builder().makeBoolean(false);

    // combined: TRUE if no F_post, else COALESCE(F_post_subst, false).
    ExprCP combinedExpr = builder().makeBoolean(true);
    if (!filterPostConjuncts.empty()) {
      ColumnVector substitutionSource;
      ExprVector substitutionTarget;
      substitutionSource.reserve(wraps.size());
      substitutionTarget.reserve(wraps.size());
      for (size_t i = 0; i < wraps.size(); ++i) {
        substitutionSource.push_back(
            aggregate->outputColumns()[numGroupingKeys + i]);
        substitutionTarget.push_back(wraps[i].finalExpression);
      }
      ExprCP filterPostCombined = exprFactory_.andAll(filterPostConjuncts);
      ExprCP filterPostSubst = exprFactory_.substitute(
          filterPostCombined, substitutionSource, substitutionTarget);
      combinedExpr = exprFactory_.makeCoalesce(filterPostSubst, falseLiteral);
    }

    NodeCP finalInput;
    ExprCP markExpr;
    if (numGroupingKeys == 0) {
      // Path 1 — empty gby.
      markExpr = combinedExpr;
      finalInput = stage1;
    } else {
      // Path 2 — non-empty gby.
      ColumnCP combinedColumn =
          Column::create("_combined", combinedExpr->value());

      ExprVector stage15Expressions;
      ColumnVector stage15Outputs;
      const size_t stage15Width = 3 + input->outputColumns().size();
      stage15Expressions.reserve(stage15Width);
      stage15Outputs.reserve(stage15Width);
      stage15Expressions.push_back(innerApply.rowIdColumn);
      stage15Outputs.push_back(innerApply.rowIdColumn);
      stage15Expressions.push_back(padPresentColumn);
      stage15Outputs.push_back(padPresentColumn);
      for (ColumnCP outerColumn : input->outputColumns()) {
        stage15Expressions.push_back(outerColumn);
        stage15Outputs.push_back(outerColumn);
      }
      stage15Expressions.push_back(combinedExpr);
      stage15Outputs.push_back(combinedColumn);

      NodeCP stage15 = builder().make<Project>(Project::Key{
          stage1,
          std::move(stage15Expressions),
          std::move(stage15Outputs),
      });

      ColumnCP hasTrueColumn = Column::createBoolean("_has_true");

      AggregateCallVector boolOrs;
      boolOrs.reserve(1);
      boolOrs.push_back(recovery.makeBoolOr(combinedColumn));
      boolOrs = recovery.addFilterCondition(boolOrs, padPresentColumn);

      AggregateCallVector stage2Aggregates;
      stage2Aggregates.reserve(1 + input->outputColumns().size());
      appendAll(stage2Aggregates, boolOrs);
      for (ColumnCP outerColumn : input->outputColumns()) {
        stage2Aggregates.push_back(makeArbitrary(outerColumn));
      }

      ColumnVector stage2Outputs;
      stage2Outputs.reserve(2 + input->outputColumns().size());
      stage2Outputs.push_back(innerApply.rowIdColumn);
      stage2Outputs.push_back(hasTrueColumn);
      appendAll(stage2Outputs, input->outputColumns());

      NodeCP stage2 = builder().make<Aggregate>(Aggregate::Key{
          stage15,
          ExprVector{innerApply.rowIdColumn},
          std::move(stage2Aggregates),
          std::move(stage2Outputs),
      });

      markExpr = exprFactory_.makeCoalesce(hasTrueColumn, falseLiteral);
      finalInput = stage2;
    }

    ExprVector finalExpressions;
    finalExpressions.reserve(input->outputColumns().size() + 1);
    appendAll(finalExpressions, input->outputColumns());
    finalExpressions.push_back(markExpr);

    return builder().make<Project>(Project::Key{
        finalInput,
        std::move(finalExpressions),
        node->outputColumns(),
    });
  }

  // Aggregate peel for kLeftSemiProject in the IN-pair branch (Rule
  // B-IN, Cases 5b/6b/7b/8b).
  //
  // Two paths, branched on `aggregate->groupingKeys().empty()`. Both
  // share Stage 1 — a lifted Aggregate that runs the body's user aggs
  // FILTER(_include) and carries the outer columns via arbitrary().
  // The IN equi pair becomes a per-row `combined` bit:
  //
  //   combined := [if F_post present: COALESCE(F_post_subst, false) AND]
  //               eq(inLhs, inBodyKey_subst)
  //
  // F_post coalesces to FALSE — a HAVING that fails or returns NULL
  // means the row does not contribute to the IN match.
  //
  // Path 1 (empty gby): Stage 1 emits one row per outer. Mark is
  // computed inline in the final Project from `combined` alone — no
  // pad-row short-circuit needed, because Stage 1's FILTER(_include)
  // makes user aggs evaluate over the empty set for unmatched outers,
  // so `inBodyKey_subst` already carries the SQL "agg over empty"
  // value (NULL for max, 0 for count, etc.) and the IN equi test runs
  // against that.
  //
  // Path 2 (non-empty gby): Stage 1 emits multiple rows per outer
  // (one per gby group). A Stage 1.5 Project computes `combined` per
  // group. Stage 2 collapses across groups per outer using two
  // bool_ors — `has_true = bool_or(combined)` and
  // `has_null = bool_or(combined IS NULL)` — both with
  // `FILTER(_include)` to discard the pad-row group. The mark CASE
  // then maps (has_true, has_null) to the tri-state IN result.
  //
  // `nullAware = false`: wrap the final mark with `COALESCE(mark,
  // false)` to collapse the NULL state to false.
  NodeCP aggregatePeelSemiIn(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      ExprVector accumulatedFilter) {
    AggregateCP aggregate = body->as<Aggregate>();
    const size_t numGroupingKeys = aggregate->groupingKeys().size();

    auto [filterPreConjuncts, filterPostConjuncts] =
        splitFilterPreAndPost(accumulatedFilter, aggregate, numGroupingKeys);

    // Validator (same as Rule A): pure-outer aggregates need
    // outer-scope-collapse semantics not modeled here.
    AggregateRecovery::validateAggregateArgs(
        aggregate->aggregates(), node->correlationColumns());

    auto wraps = buildAggregateWraps(aggregate, numGroupingKeys);

    AggregateRecovery recovery(builder(), exprFactory_);
    auto innerApply = buildAggregateInnerApply(
        input, aggregate, std::move(filterPreConjuncts));
    NodeCP decorrelatedInner = rewrite(innerApply.apply);

    // Stage 1 Aggregate:
    //   groupingKeys = [rowId, original gby...]
    //   aggregates   = user aggs FILTER(_include)
    //                + arbitrary(L.col) for each outer col
    //                + arbitrary(_include) AS padPresent   [Path 2 only]
    AggregateCallVector stage1Aggregates = recovery.rewriteCountStar(
        aggregate->aggregates(), innerApply.includeMarker);
    stage1Aggregates =
        recovery.addFilterCondition(stage1Aggregates, innerApply.includeMarker);
    for (ColumnCP outerColumn : input->outputColumns()) {
      stage1Aggregates.push_back(makeArbitrary(outerColumn));
    }
    // Pad-present marker: needed only by Path 2's Stage 2 FILTER.
    // For Path 1 the mark CASE consumes `inBodyKey_subst` directly,
    // which already encodes the unmatched-outer case via FILTER(_include)
    // on the user aggs.
    ColumnCP padPresentColumn = nullptr;
    if (numGroupingKeys > 0) {
      padPresentColumn = Column::create(
          innerApply.includeMarker->name(), innerApply.includeMarker->value());
      stage1Aggregates.push_back(makeArbitrary(innerApply.includeMarker));
    }

    ExprVector stage1GroupingKeys;
    stage1GroupingKeys.reserve(1 + numGroupingKeys);
    stage1GroupingKeys.push_back(innerApply.rowIdColumn);
    appendAll(stage1GroupingKeys, aggregate->groupingKeys());

    ColumnVector stage1OutputColumns;
    stage1OutputColumns.reserve(
        1 + numGroupingKeys + wraps.size() + input->outputColumns().size() +
        (padPresentColumn != nullptr ? 1 : 0));
    stage1OutputColumns.push_back(innerApply.rowIdColumn);
    for (size_t i = 0; i < numGroupingKeys; ++i) {
      stage1OutputColumns.push_back(aggregate->outputColumns()[i]);
    }
    for (const auto& wrap : wraps) {
      stage1OutputColumns.push_back(wrap.liftedOutput);
    }
    appendAll(stage1OutputColumns, input->outputColumns());
    if (padPresentColumn != nullptr) {
      stage1OutputColumns.push_back(padPresentColumn);
    }

    NodeCP stage1 = builder().make<Aggregate>(Aggregate::Key{
        decorrelatedInner,
        std::move(stage1GroupingKeys),
        std::move(stage1Aggregates),
        std::move(stage1OutputColumns),
    });

    // Substitute agg-result columns in `inBodyKey` and `F_post` with
    // their post-COALESCE expressions so the mark evaluates over the
    // SQL "agg over empty" values when applicable. Same mechanism as
    // Rule A's F_post substitution.
    ColumnVector substitutionSource;
    ExprVector substitutionTarget;
    substitutionSource.reserve(wraps.size());
    substitutionTarget.reserve(wraps.size());
    for (size_t i = 0; i < wraps.size(); ++i) {
      substitutionSource.push_back(
          aggregate->outputColumns()[numGroupingKeys + i]);
      substitutionTarget.push_back(wraps[i].finalExpression);
    }
    ExprCP inBodyKeySubst = exprFactory_.substitute(
        node->inBodyKey(), substitutionSource, substitutionTarget);

    const Literal* falseLiteral = builder().makeBoolean(false);

    ExprCP equiPair = exprFactory_.makeEq(node->inLhs(), inBodyKeySubst);
    ExprCP combinedExpr = equiPair;
    if (!filterPostConjuncts.empty()) {
      ExprCP filterPostCombined = exprFactory_.andAll(filterPostConjuncts);
      ExprCP filterPostSubst = exprFactory_.substitute(
          filterPostCombined, substitutionSource, substitutionTarget);
      ExprCP havingCoalesced =
          exprFactory_.makeCoalesce(filterPostSubst, falseLiteral);
      combinedExpr = exprFactory_.makeAnd(havingCoalesced, equiPair);
    }

    const Literal* trueLiteral = builder().makeBoolean(true);
    const Literal* nullBoolLiteral =
        builder().makeNull(toType(velox::BOOLEAN()));

    NodeCP finalInput;
    ExprCP markExpr;
    if (numGroupingKeys == 0) {
      // ===== Path 1 — empty gby =====
      //
      // Single Aggregate; mark inline:
      //   IF(combined, true, IF(combined IS NULL, NULL, false))
      ExprCP combinedIsNull = exprFactory_.makeIsNull(combinedExpr);
      markExpr = exprFactory_.makeIf(
          combinedExpr,
          trueLiteral,
          exprFactory_.makeIf(combinedIsNull, nullBoolLiteral, falseLiteral));
      finalInput = stage1;
    } else {
      // ===== Path 2 — non-empty gby =====
      //
      // Stage 1.5 Project: per-(rn, gby) combined bit + carry-through
      // (rowId, padPresent, L.cols). Stage 2 aggregates over [rowId].
      ColumnCP combinedColumn =
          Column::create("_combined", combinedExpr->value());

      ExprVector stage15Expressions;
      ColumnVector stage15Outputs;
      const size_t stage15Width = 3 + input->outputColumns().size();
      stage15Expressions.reserve(stage15Width);
      stage15Outputs.reserve(stage15Width);

      stage15Expressions.push_back(innerApply.rowIdColumn);
      stage15Outputs.push_back(innerApply.rowIdColumn);
      stage15Expressions.push_back(padPresentColumn);
      stage15Outputs.push_back(padPresentColumn);
      for (ColumnCP outerColumn : input->outputColumns()) {
        stage15Expressions.push_back(outerColumn);
        stage15Outputs.push_back(outerColumn);
      }
      stage15Expressions.push_back(combinedExpr);
      stage15Outputs.push_back(combinedColumn);

      NodeCP stage15 = builder().make<Project>(Project::Key{
          stage1,
          std::move(stage15Expressions),
          std::move(stage15Outputs),
      });

      // Stage 2: bool_or(combined) and bool_or(combined IS NULL), both
      // FILTER(_include); + arbitrary(L.cols).
      ColumnCP hasTrueColumn = Column::createBoolean("_has_true");
      ColumnCP hasNullColumn = Column::createBoolean("_has_null");

      AggregateCallVector boolOrs;
      boolOrs.reserve(2);
      boolOrs.push_back(recovery.makeBoolOr(combinedColumn));
      boolOrs.push_back(
          recovery.makeBoolOr(exprFactory_.makeIsNull(combinedColumn)));
      boolOrs = recovery.addFilterCondition(boolOrs, padPresentColumn);

      AggregateCallVector stage2Aggregates;
      stage2Aggregates.reserve(2 + input->outputColumns().size());
      appendAll(stage2Aggregates, boolOrs);
      for (ColumnCP outerColumn : input->outputColumns()) {
        stage2Aggregates.push_back(makeArbitrary(outerColumn));
      }

      ColumnVector stage2Outputs;
      stage2Outputs.reserve(3 + input->outputColumns().size());
      stage2Outputs.push_back(innerApply.rowIdColumn);
      stage2Outputs.push_back(hasTrueColumn);
      stage2Outputs.push_back(hasNullColumn);
      appendAll(stage2Outputs, input->outputColumns());

      NodeCP stage2 = builder().make<Aggregate>(Aggregate::Key{
          stage15,
          ExprVector{innerApply.rowIdColumn},
          std::move(stage2Aggregates),
          std::move(stage2Outputs),
      });

      // Mark CASE: IF(COALESCE(has_true, false), true,
      //               IF(has_null, NULL, false))
      ExprCP hasTrueCoalesced =
          exprFactory_.makeCoalesce(hasTrueColumn, falseLiteral);
      markExpr = exprFactory_.makeIf(
          hasTrueCoalesced,
          trueLiteral,
          exprFactory_.makeIf(hasNullColumn, nullBoolLiteral, falseLiteral));
      finalInput = stage2;
    }

    // `nullAware = false`: collapse the NULL mark state to false.
    if (!node->nullAware()) {
      markExpr = exprFactory_.makeCoalesce(markExpr, falseLiteral);
    }

    // Final Project: input.cols ++ markColumn.
    ExprVector finalExpressions;
    finalExpressions.reserve(input->outputColumns().size() + 1);
    appendAll(finalExpressions, input->outputColumns());
    finalExpressions.push_back(markExpr);

    return builder().make<Project>(Project::Key{
        finalInput,
        std::move(finalExpressions),
        node->outputColumns(),
    });
  }

  // Terminus for kLeftSemiProject. Builds a Join with
  // joinType=kLeftSemiProject; the markColumn flows through Apply's
  // outputColumns to the Join's outputColumns. For IN, the
  // (inLhs, inBodyKey) pair seeds the Join's equi-keys; any equi
  // conjuncts in `filter` that partition cleanly left/right become
  // additional equi-keys, the rest stays in the Join's filter.
  NodeCP
  terminusSemi(ApplyCP apply, NodeCP input, NodeCP body, ExprVector filter) {
    ExprVector leftKeys;
    ExprVector rightKeys;
    if (apply->inLhs() != nullptr) {
      // Equi-keys must each resolve on a single join side. When the
      // body key references outer columns, demote the IN equality
      // into the join filter; `nullAware=true` preserves null-aware
      // semi-project semantics.
      const bool bodyKeyHasOuter =
          apply->inBodyKey()->columns().containsAny(input->outputColumns());
      if (bodyKeyHasOuter) {
        filter.push_back(
            exprFactory_.makeEq(apply->inLhs(), apply->inBodyKey()));
      } else {
        leftKeys.push_back(apply->inLhs());
        rightKeys.push_back(apply->inBodyKey());
      }
    }
    JoinCondition::Split split = JoinCondition::splitEquiKeys(
        filter,
        PlanObjectSet::fromObjects(input->outputColumns()),
        PlanObjectSet::fromObjects(body->outputColumns()));
    // Null-aware semantics apply only to the IN equality. Keep
    // correlation equi-conditions in the residual so they execute
    // under standard `=`.
    if (apply->nullAware()) {
      for (size_t i = 0; i < split.leftKeys.size(); ++i) {
        split.residual.push_back(
            exprFactory_.makeEq(split.leftKeys[i], split.rightKeys[i]));
      }
    } else {
      appendAll(leftKeys, split.leftKeys);
      appendAll(rightKeys, split.rightKeys);
    }
    return builder().make<Join>(Join::Key{
        input,
        body,
        velox::core::JoinType::kLeftSemiProject,
        std::move(leftKeys),
        std::move(rightKeys),
        std::move(split.residual),
        apply->nullAware(),
        /*nullAsValue=*/false,
        apply->outputColumns(),
    });
  }

  // Creates a fresh BIGINT column intended to hold a per-row distinct
  // value (AssignUniqueId output, Window row_number output, ...). NDV
  // is set to max to convey "unique"; the planner treats this as
  // each-row-distinct.
  static ColumnCP makeIdColumn(std::string_view name = "__rownum") {
    Value value(toType(velox::BIGINT()), std::numeric_limits<float>::max());
    return Column::create(name, value);
  }

  // Creates a fresh `_include` BOOLEAN column for an Apply's
  // includeMarker. See `Apply::Key::includeMarker`.
  static ColumnCP makeIncludeColumn() {
    return Column::createBoolean("_include");
  }

  // Wraps `input` in an `EnforceDistinct` that asserts at most one row
  // per `perOuterId`, raising SQL's "scalar subquery returned multiple
  // rows" error otherwise.
  NodeCP enforceScalarSingleRow(NodeCP input, ColumnCP perOuterId) {
    return builder().make<EnforceDistinct>(EnforceDistinct::Key{
        input,
        ExprVector{perOuterId},
        toName("Scalar sub-query has returned multiple rows"),
    });
  }

  // Constructs an `arbitrary(arg)` aggregate. Used for outer-column
  // carry-through in the Aggregate peel: each outer column contributes
  // a constant value within an `rowId` group (one row per outer), so
  // `arbitrary` picks that value. Output type matches the argument's
  // type; intermediate accumulator type same; output NDV (number of
  // distinct values) inherits the argument's NDV — `arbitrary(x)`
  // ranges over x's distinct values, not a single value.
  const optimizer::Aggregate* makeArbitrary(ExprCP argument) {
    const auto& arbitraryName = FunctionRegistry::instance()->arbitrary();
    VELOX_USER_CHECK(
        arbitraryName.has_value(),
        "Decorrelate outer-column carry-through requires arbitrary "
        "registered via FunctionRegistry::registerArbitrary");
    const std::string& nameStr = *arbitraryName;

    ExprVector arguments{argument};
    Name aggregateName = toName(nameStr);
    // Output value mirrors the argument: same type, same NDV.
    Value value(argument->value().type, argument->value().cardinality);

    const auto& metadata = velox::exec::getAggregateFunctionMetadata(nameStr);
    FunctionSet functions = Call::unionArgFunctions(FunctionSet{}, arguments);
    if (metadata.ignoreDuplicates) {
      functions = functions | FunctionSet::kIgnoreDuplicatesAggregate;
    }
    if (metadata.orderSensitive) {
      functions = functions | FunctionSet::kOrderSensitiveAggregate;
    }
    // For `arbitrary`, intermediate accumulator type equals the input
    // type (it just holds one value).
    TypeCP intermediateType = argument->value().type;

    return builder().makeAggregate(
        aggregateName,
        value,
        std::move(arguments),
        functions,
        /*isDistinct=*/false,
        /*condition=*/nullptr,
        intermediateType,
        /*orderKeys=*/{},
        /*orderTypes=*/{});
  }

  ExprFactory exprFactory_;
};

} // namespace

NodeCP DecorrelatePass::run(NodeCP root, Builder& builder) {
  Decorrelator rewriter(builder);
  return rewriter.rewrite(root);
}

} // namespace facebook::axiom::optimizer::v2
