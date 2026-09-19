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

// True when a Sort sits somewhere in the chain of single-input nodes below
// 'node'. Such a Sort establishes an order the caller cannot see.
bool hasSortBelow(NodeCP node) {
  for (NodeCP below = node; below->inputs().size() == 1;
       below = below->inputs()[0]) {
    if (below->is(NodeType::kSort)) {
      return true;
    }
  }
  return false;
}

// Returns 'node' as a Project when it emits columns of its input unchanged,
// dropping some but computing and renaming none, and nullptr otherwise. A
// Project emitting nothing is not pass-through: it keeps only the rows.
const Project* asPassThroughProject(NodeCP node) {
  if (!node->is(NodeType::kProject)) {
    return nullptr;
  }
  const Project* project = node->as<Project>();
  if (project->exprs().empty()) {
    return nullptr;
  }
  for (size_t i = 0; i < project->exprs().size(); ++i) {
    if (project->exprs()[i] != project->outputColumns()[i]) {
      return nullptr;
    }
  }
  return project;
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

// True if `expr` evaluates to NULL on a pad row of a kLeft Apply,
// where every column of `bodyColumns` is NULL. A default-null
// function returns NULL for a NULL argument, so an expression built
// only from those and reading at least one body column is NULL there
// already and needs no includeMarker guard.
bool isNullOnPadRows(ExprCP expr, const PlanObjectSet& bodyColumns) {
  return expr->columns().hasIntersection(bodyColumns) &&
      !expr->containsFunction(FunctionSet::kNonDefaultNullBehavior);
}

// True if `node` is a `Values` with one row and no columns — what the FROM of
// a subquery that selects only from an UNNEST lowers to. Joining with it
// neither adds columns nor changes cardinality.
bool isSingleEmptyRowValues(NodeCP node) {
  if (!node->is(NodeType::kValues)) {
    return false;
  }
  const Values* values = node->as<Values>();
  return values->outputColumns().empty() && values->cardinality() == 1;
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
        return projectPeelNonSemi(node, input, body, accumulatedFilter);
      }

      if (body->is(NodeType::kAggregate)) {
        // Aggregate peel returns the fully-decorrelated subtree.
        return aggregatePeel(node, input, body, accumulatedFilter);
      }

      if (body->is(NodeType::kLimit)) {
        return limitPeel(node, input, body, accumulatedFilter);
      }

      if (body->is(NodeType::kJoin)) {
        if (auto lifted = liftCorrelationAboveJoin(
                body->as<Join>(), input->outputColumns())) {
          appendAll(accumulatedFilter, lifted->predicates);
          body = lifted->body;
          continue;
        }
        return joinPeel(node, input, body, accumulatedFilter);
      }

      if (body->is(NodeType::kAssignUniqueId)) {
        return assignUniqueIdPeel(node, input, body, accumulatedFilter);
      }

      if (body->is(NodeType::kUnnest)) {
        return unnestPeel(node, input, body, accumulatedFilter);
      }

      VELOX_NYI(
          "Correlated reference inside a {} branch is not supported yet",
          body->nodeType());
    }
  }

 private:
  // Project peel (Rule A) for non-semi kinds (kLeft, kInner): Project
  // lifts above Apply. Apply's body becomes Project's child; the lifted
  // Project above the decorrelated inner Apply reproduces the original
  // output schema using Project's original exprs (which may reference L
  // cols, now visible above the inner Apply per the relaxed contract).
  //
  // kLeft carries pad rows and the includeMarker; kInner has neither.
  //
  // Recurses on the inner Apply so the driver continues peeling whatever
  // remains in body (more Filters / Projects below, or terminus).
  NodeCP projectPeelNonSemi(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      ExprVector accumulatedFilter) {
    const bool hasMarker = node->isLeft();
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
    if (hasMarker) {
      innerOutputColumns.push_back(node->includeMarker());
    }

    // Recompute correlations for the inner body. Project peel may have
    // lifted outer-column refs out of body (into the lifted Project
    // above), shrinking the correlation set.
    ColumnVector innerCorrelations =
        recomputeCorrelations(newBody, input->outputColumns());

    NodeCP innerApply = builder().make<Apply>({
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

    // Build lifted Project reproducing the original Apply output schema:
    // input cols pass through; each Project output col is recomputed from its
    // original expr (per-kind handling below); cols already present in input
    // collapse to the same slot.
    // Only columns the body produces are NULL on a pad row. A body
    // output that is an outer column keeps its value there.
    PlanObjectSet bodyColumns =
        PlanObjectSet::fromObjects(newBody->outputColumns());
    bodyColumns.eraseAll(input->outputColumns());

    ExprVector liftedExprs;
    liftedExprs.reserve(
        input->outputColumns().size() + projectBody->exprs().size() + 1);
    appendAll(liftedExprs, input->outputColumns());
    PlanObjectSet liftedSeen =
        PlanObjectSet::fromObjects(input->outputColumns());
    const auto& projectOutputs = projectBody->outputColumns();
    for (size_t i = 0; i < projectBody->exprs().size(); ++i) {
      ColumnCP outputColumn = projectOutputs[i];
      if (liftedSeen.contains(outputColumn)) {
        continue;
      }
      liftedSeen.add(outputColumn);
      ExprCP expr = projectBody->exprs()[i];
      if (hasMarker && !isNullOnPadRows(expr, bodyColumns)) {
        // kLeft: NULL out exprs a pad row would otherwise give a value.
        const Literal* nullLiteral = builder().makeNull(expr->value().type);
        liftedExprs.push_back(
            exprFactory_.makeIf(node->includeMarker(), expr, nullLiteral));
      } else {
        liftedExprs.push_back(expr);
      }
    }
    if (hasMarker) {
      liftedExprs.push_back(node->includeMarker());
    }

    return builder().make<Project>({
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

    NodeCP innerApply = builder().make<Apply>({
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
    if (node->isInner()) {
      VELOX_NYI(
          "Decorrelate: INNER LATERAL over a Limit body is not yet supported");
    }
    VELOX_USER_CHECK(
        node->isLeft(),
        "Decorrelate Limit peel: unexpected Apply kind {}",
        node->kind());
    if (auto equi =
            limitPeelEqui(node, input, limitBody, count, accumulatedFilter)) {
      return *equi;
    }
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
    NodeCP innerApply = builder().make<Apply>({
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

  // The ORDER BY 'node' establishes, and 'node' without it.
  struct BodyOrder {
    // 'node' with the Sort removed, or 'node' itself where there was none.
    NodeCP body;
    // The Sort's keys, empty where there was no Sort.
    ExprVector orderKeys;
    OrderTypeVector orderTypes;
  };

  // Takes the ORDER BY off 'node' so a row_number can carry it. Returns
  // nullopt where a Sort sits below 'node' instead: its order decides which
  // rows a LIMIT keeps, and a ranking that cannot see it would pick others.
  static std::optional<BodyOrder> takeSortOrder(NodeCP node) {
    if (node->is(NodeType::kSort)) {
      const Sort* sort = node->as<Sort>();
      return BodyOrder{sort->input(), sort->orderKeys(), sort->orderTypes()};
    }
    if (hasSortBelow(node)) {
      return std::nullopt;
    }
    return BodyOrder{node, {}, {}};
  }

  // Replaces the Sort in 'node' with a ranking over 'partitionKeys', keeping
  // only the first 'count' rows of each partition. Descends through Projects
  // that only pass columns through, so the ranking lands where the Sort's keys
  // are still in scope. With no Sort at all the rows are ranked as they
  // arrive, which is what an unordered LIMIT asks for. Returns nullopt when a
  // Sort sits below where the descent reaches, since ranking would then pick
  // rows in an order that Sort was there to decide.
  std::optional<NodeCP> rankPerPartition(
      NodeCP node,
      const ExprVector& partitionKeys,
      int64_t count) {
    if (const Project* project = asPassThroughProject(node)) {
      auto input = rankPerPartition(project->input(), partitionKeys, count);
      if (!input.has_value()) {
        return std::nullopt;
      }
      return builder().make<Project>(
          {*input, project->exprs(), project->outputColumns()});
    }

    auto order = takeSortOrder(node);
    if (!order.has_value()) {
      return std::nullopt;
    }

    ColumnCP rowNumberColumn = makeIdColumn("__limit_rn");
    NodeCP windowed = addRowNumberWindow(
        order->body,
        partitionKeys,
        rowNumberColumn,
        std::move(order->orderKeys),
        std::move(order->orderTypes));

    const Literal* countLiteral =
        builder().makeLiteral(velox::Variant(count), toType(velox::BIGINT()));
    return builder().make<Filter>(
        {windowed,
         ExprVector{
             exprFactory_.makeLessThanOrEqual(rowNumberColumn, countLiteral)}});
  }

  // What a join-back produced.
  struct JoinBack {
    // The LEFT JOIN, with the outer tagged and the scalar bound asserted
    // where the caller asked for them.
    NodeCP node;

    // Reads NULL on an outer the body had no row for, or nullptr where the
    // caller asked for no marker.
    ColumnCP includeMarker;
  };

  // LEFT JOINs a correlation-free 'body' back to 'input' on the lifted equi
  // keys, so the body is read once rather than once per outer row. The join
  // publishes the outer columns and 'bodyColumns'.
  //
  // An outer the body has no row for is padded here, and 'markPads' asks for
  // a marker the pad reads NULL: sourcing it from the body is what makes the
  // pad visible above, where a literal would claim the outer had a row.
  //
  // 'enforceSingleRow' tags the outer rows and asserts one row each over the
  // join's output, for a body that can match an outer more than once.
  JoinBack joinBodyBack(
      NodeCP input,
      NodeCP body,
      const ExprVector& leftKeys,
      const ExprVector& rightKeys,
      const ColumnVector& bodyColumns,
      bool markPads,
      bool enforceSingleRow) {
    ColumnCP includeMarker = nullptr;
    if (markPads) {
      includeMarker = makeIncludeColumn();
      body =
          addIncludeMarkerToBody(body, includeMarker, input->outputColumns());
    }

    NodeCP joinLeft = input;
    ColumnCP rowId = nullptr;
    if (enforceSingleRow) {
      rowId = makeIdColumn();
      joinLeft = tagOuterRows(input, rowId);
    }

    ColumnVector joinOutput;
    joinOutput.reserve(input->outputColumns().size() + bodyColumns.size() + 2);
    appendAll(joinOutput, input->outputColumns());
    if (rowId != nullptr) {
      joinOutput.push_back(rowId);
    }
    appendUnique(joinOutput, bodyColumns);
    if (includeMarker != nullptr) {
      joinOutput.push_back(includeMarker);
    }

    NodeCP join = builder().make<Join>({
        joinLeft,
        body,
        velox::core::JoinType::kLeft,
        leftKeys,
        rightKeys,
        /*filter=*/{},
        /*nullAware=*/false,
        /*nullAsValue=*/false,
        std::move(joinOutput),
    });

    return JoinBack{
        rowId != nullptr ? enforceScalarSingleRow(join, rowId) : join,
        includeMarker};
  }

  // kLeft over a Limit whose correlation is an equality: rank the body once,
  // partitioned by the correlation key, and LEFT JOIN it back to the outer.
  // The body is read once rather than once per outer row. An outer matching
  // nothing reads NULL, which is what a scalar subquery over no rows means.
  //
  // Returns nullopt for the correlations and counts this shape cannot serve,
  // leaving them to the per-outer form.
  std::optional<NodeCP> limitPeelEqui(
      ApplyCP node,
      NodeCP input,
      LimitCP limitBody,
      int64_t count,
      const ExprVector& accumulatedFilter) {
    // A filter above the body would have to be applied after the join-back,
    // and more than one row per outer would still need the scalar bound
    // asserted; both are left to the per-outer form.
    if (!accumulatedFilter.empty() || (count > 1 && node->enforceSingleRow())) {
      return std::nullopt;
    }

    auto correlation =
        liftEquiCorrelation(limitBody->input(), input->outputColumns());
    if (!correlation.has_value()) {
      return std::nullopt;
    }

    auto rankedBody =
        rankPerPartition(correlation->cleanBody, correlation->rightKeys, count);
    if (!rankedBody.has_value()) {
      return std::nullopt;
    }

    // Ranking to `count` rows per key already holds any scalar bound this
    // shape accepts, so the join needs no assertion of its own.
    JoinBack back = joinBodyBack(
        input,
        *rankedBody,
        correlation->leftKeys,
        correlation->rightKeys,
        limitBody->outputColumns(),
        /*markPads=*/true,
        /*enforceSingleRow=*/false);

    return projectApplyOutput(node, back.node, back.includeMarker);
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
    NodeCP taggedInput =
        builder().make<AssignUniqueId>({input, rowNumberPartition});

    // Translating `SELECT x ... ORDER BY y` materializes y for the Sort and
    // prunes it above, so the Sort sits under Projects that only drop columns.
    NodeCP beneathLimit = limitBody->input();
    while (const Project* project = asPassThroughProject(beneathLimit)) {
      beneathLimit = project->input();
    }

    auto order = takeSortOrder(beneathLimit);
    if (!order.has_value()) {
      VELOX_NYI(
          "Decorrelate Limit peel: a Sort under {} does not reach the "
          "window's ordering",
          beneathLimit->nodeType());
    }
    // Where there was no Sort the Projects descended through above still
    // belong to the body.
    NodeCP newBody =
        order->orderKeys.empty() ? limitBody->input() : order->body;
    ExprVector windowOrderKeys = std::move(order->orderKeys);
    OrderTypeVector windowOrderTypes = std::move(order->orderTypes);
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

    NodeCP innerApply = builder().make<Apply>({
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
        ExprVector{rowNumberPartition},
        rowNumberColumn,
        std::move(windowOrderKeys),
        std::move(windowOrderTypes));

    const Literal* countLiteral =
        builder().makeLiteral(velox::Variant(count), toType(velox::BIGINT()));
    NodeCP filtered = builder().make<Filter>({
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
    return projectApplyOutput(node, enforced, innerIncludeMarker);
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

  // `bool_or(source)` over the whole partition, reading true when any row of
  // the partition has it.
  WindowFunction boolOrWindowFunction(ColumnCP source) {
    const auto& boolOrName = FunctionRegistry::instance()->boolOr();
    VELOX_USER_CHECK(
        boolOrName.has_value(),
        "Decorrelate requires bool_or registered via "
        "FunctionRegistry::registerBoolOr");

    // bool_or yields a BOOLEAN (two distinct values).
    ExprCP call = builder().makeCall(
        toName(*boolOrName),
        Value(toType(velox::BOOLEAN()), /*cardinality=*/2),
        ExprVector{source},
        source->functions() | FunctionSet::kNonDeterministic |
            FunctionSet::kNonDefaultNullBehavior);
    return WindowFunction{call, Frame::wholePartition(), /*ignoreNulls=*/false};
  }

  // Shapes 'child' to the outer Apply's output columns, writing 'marker' where
  // the includeMarker goes and passing every other column through.
  NodeCP projectApplyOutput(ApplyCP node, NodeCP child, ExprCP marker) {
    ExprVector finalExprs;
    finalExprs.reserve(node->outputColumns().size());
    for (ColumnCP outputColumn : node->outputColumns()) {
      finalExprs.push_back(
          outputColumn == node->includeMarker() ? marker : outputColumn);
    }
    return builder().make<Project>(
        {child, std::move(finalExprs), node->outputColumns()});
  }

  // Wraps 'input' in a Window that emits `row_number() OVER
  // (PARTITION BY partitionKeys [ORDER BY orderKeys])` into
  // 'rowNumberColumn'. Empty 'orderKeys' yields an unordered
  // row_number assignment.
  NodeCP addRowNumberWindow(
      NodeCP input,
      ExprVector partitionKeys,
      ColumnCP rowNumberColumn,
      ExprVector orderKeys,
      OrderTypeVector orderTypes) {
    WindowFunctions functions;
    functions.push_back(rowNumberWindowFunction(rowNumberColumn));

    ColumnVector outputs;
    outputs.reserve(input->outputColumns().size() + 1);
    appendAll(outputs, input->outputColumns());
    outputs.push_back(rowNumberColumn);

    return builder().make<Window>({
        input,
        std::move(functions),
        std::move(partitionKeys),
        std::move(orderKeys),
        std::move(orderTypes),
        std::move(outputs),
    });
  }

  // What a lift leaves behind.
  struct LiftedJoinFilter {
    // The Join with the lifted conjuncts removed from its sides.
    NodeCP body;
    // The lifted conjuncts, to be applied above the Join.
    ExprVector predicates;
  };

  // A correlated predicate the body Join keeps on a preserved side filters
  // that side's rows, so it selects the same rows above the Join. Lifting it
  // there can leave the body uncorrelated, which the driver then hands to the
  // terminus without peeling the Join at all. A predicate on a null-supplying
  // side decides which rows pad and does not lift.
  std::optional<LiftedJoinFilter> liftCorrelationAboveJoin(
      JoinCP join,
      const ColumnVector& outerColumns) {
    // A conjunct only lifts if it still resolves above the Join, which keeps
    // only the columns its output names.
    PlanObjectSet visibleAbove =
        PlanObjectSet::fromObjects(join->outputColumns());
    visibleAbove.unionSet(PlanObjectSet::fromObjects(outerColumns));

    ExprVector lifted;
    auto liftFrom = [&](NodeCP side) {
      if (!side->is(NodeType::kFilter)) {
        return side;
      }
      const Filter* filter = side->as<Filter>();
      ExprVector correlated;
      ExprVector kept;
      for (ExprCP conjunct : filter->predicates()) {
        const bool lifts = conjunct->columns().containsAny(outerColumns) &&
            conjunct->columns().isSubset(visibleAbove);
        (lifts ? correlated : kept).push_back(conjunct);
      }
      if (correlated.empty()) {
        return side;
      }
      appendAll(lifted, correlated);
      return kept.empty()
          ? filter->input()
          : builder().make<Filter>({filter->input(), std::move(kept)});
    };

    const bool leftIsPreserved =
        join->isInner() || join->isLeft() || join->isLeftSemiProject();
    NodeCP left = leftIsPreserved ? liftFrom(join->left()) : join->left();
    NodeCP right = join->isInner() ? liftFrom(join->right()) : join->right();

    if (lifted.empty()) {
      return std::nullopt;
    }

    // Lifting is predicate pushdown in reverse, so it only pays when it
    // leaves nothing correlated behind: the body then needs no peel at all.
    // A body still correlated elsewhere keeps its filter where it is.
    if (!recomputeCorrelations(left, outerColumns).empty() ||
        !recomputeCorrelations(right, outerColumns).empty()) {
      return std::nullopt;
    }

    return LiftedJoinFilter{
        builder().make<Join>({
            left,
            right,
            join->joinType(),
            join->leftKeys(),
            join->rightKeys(),
            join->filter(),
            join->nullAware(),
            join->nullAsValue(),
            join->outputColumns(),
        }),
        std::move(lifted)};
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

    if (node->isInner()) {
      VELOX_NYI(
          "Decorrelate: INNER LATERAL over a Join body is not yet supported");
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

  // Outer Apply is kLeft (scalar). Supports a kLeftSemiProject body and a
  // kLeft or kInner body, with or without a predicate. The remaining kinds
  // NYI loud.
  NodeCP joinPeelLeft(
      ApplyCP node,
      NodeCP input,
      JoinCP joinBody,
      ExprVector joinPredicate,
      ExprVector accumulatedFilter) {
    if (joinBody->joinType() == velox::core::JoinType::kLeftSemiProject) {
      return joinPeelLeftSemiProject(
          node, input, joinBody, std::move(accumulatedFilter));
    }

    if (!joinBody->isInner() && !joinBody->isLeft()) {
      VELOX_NYI(
          "Decorrelate joinPeel: outer kLeft over this body Join kind is not "
          "yet implemented: {}",
          joinBody->joinTypeName());
    }

    NodeCP leftSide = joinBody->left();
    NodeCP rightSide = joinBody->right();

    if (joinBody->isInner()) {
      // A row the join predicate rejects is not a body row, and neither is
      // one the accumulated filter rejects, so both ride on applyB and the
      // collapse drops what they reject.
      ExprVector applyBFilter = std::move(joinPredicate);
      appendAll(applyBFilter, accumulatedFilter);
      return joinPeelLeftInner(
          node, input, leftSide, rightSide, std::move(applyBFilter));
    }

    return joinPeelLeftOuter(
        node,
        input,
        leftSide,
        rightSide,
        std::move(joinPredicate),
        std::move(accumulatedFilter));
  }

  // Outer kLeft over a body kLeft Join. A row the join predicate rejects is
  // still a body row, the left side preserved with NULL right columns, so
  // only the predicate rides on applyB. A row the accumulated filter rejects
  // is not a body row, so the filter decides which rows the per-rn
  // pad-collapse keeps, and an outer left with none keeps one pad.
  NodeCP joinPeelLeftOuter(
      ApplyCP node,
      NodeCP input,
      NodeCP leftSide,
      NodeCP rightSide,
      ExprVector joinPredicate,
      ExprVector accumulatedFilter) {
    ColumnCP rowId = makeIdColumn();
    NodeCP taggedInput = tagOuterRows(input, rowId);

    // The accumulated filter can cut a multi-row body down to one, so the
    // scalar bound is checked once after the collapse, not per leg.
    ColumnCP markA = makeIncludeColumn();
    NodeCP applyA = makeLeftLeg(
        taggedInput,
        leftSide,
        /*filter=*/ExprVector{},
        /*enforceSingleRow=*/false,
        markA);

    NodeCP applyB = makeLeftLeg(
        applyA,
        rightSide,
        std::move(joinPredicate),
        /*enforceSingleRow=*/false,
        makeIncludeColumn());

    NodeCP chain = rewrite(applyB);

    ExprCP matched = accumulatedFilter.empty()
        ? markA
        : exprFactory_.makeAnd(markA, exprFactory_.andAll(accumulatedFilter));
    return collapsePadRows(node, input, chain, rowId, matched);
  }

  // Outer kLeft over a body kLeftSemiProject: the body emits A's rows plus a
  // mark saying whether any B row matched. applyA carries the outer scalar
  // Apply's kLeft pad semantics over A, and applyB tests B for existence and
  // writes the body's own mark.
  //
  // A filter reading that mark selects among body rows, and an outer whose
  // every body row it rejects must still emit one NULL-padded row.
  NodeCP joinPeelLeftSemiProject(
      ApplyCP node,
      NodeCP input,
      JoinCP joinBody,
      ExprVector accumulatedFilter) {
    NodeCP leftSide = joinBody->left();
    NodeCP rightSide = joinBody->right();
    ColumnCP mark = joinBody->markColumn();
    VELOX_CHECK_NOT_NULL(mark);

    ExprVector markFilter;
    ExprVector rowFilter;
    for (ExprCP conjunct : accumulatedFilter) {
      if (conjunct->columns().contains(mark)) {
        markFilter.push_back(conjunct);
      } else {
        rowFilter.push_back(conjunct);
      }
    }

    // A null-aware body carries the IN equality as its single equi-pair, and
    // applyB must keep the two sides apart to stay null-aware.
    ExprCP inLhs = nullptr;
    ExprCP inBodyKey = nullptr;
    ExprVector applyBFilter;
    if (joinBody->nullAware()) {
      // A body whose IN key reads outer columns carries the equality in its
      // filter instead, leaving nothing to hand applyB as the null-aware pair.
      if (joinBody->leftKeys().size() != 1) {
        VELOX_NYI(
            "Decorrelate joinPeel: outer kLeft over a null-aware "
            "kLeftSemiProject Join without a single equi-pair is not yet "
            "implemented");
      }
      inLhs = joinBody->leftKeys()[0];
      inBodyKey = joinBody->rightKeys()[0];
      appendAll(applyBFilter, joinBody->filter());
    } else {
      applyBFilter = flattenJoinPredicate(joinBody);
    }

    // Collapsing keeps the rows the mark filter accepts and one pad row per
    // outer that has none, which needs a per-outer id and defers the
    // cardinality assertion until after the collapse.
    const bool collapse = !markFilter.empty();
    ColumnCP rowId = collapse ? makeIdColumn() : nullptr;
    NodeCP chainInput = collapse ? tagOuterRows(input, rowId) : input;

    ColumnCP applyAIncludeMarker = makeIncludeColumn();
    NodeCP applyA = makeLeftLeg(
        chainInput,
        leftSide,
        std::move(rowFilter),
        collapse ? false : node->enforceSingleRow(),
        applyAIncludeMarker);

    // Semi projection is one row in, one row out, so A's rows are neither
    // multiplied nor dropped by the existence test.
    NodeCP applyB = makeSemiLeg(
        applyA, rightSide, std::move(applyBFilter), mark, inLhs, inBodyKey);

    NodeCP chain = rewrite(applyB);

    if (!collapse) {
      // Without a mark filter the mark is a value on an A row rather than a
      // reason to keep or drop it, so applyA's marker alone gates inclusion.
      return projectApplyOutput(node, chain, applyAIncludeMarker);
    }

    // A body row counts when A matched and the mark filter accepts it.
    ExprCP matchedExpr = markFilter.empty()
        ? applyAIncludeMarker
        : exprFactory_.makeAnd(
              applyAIncludeMarker, exprFactory_.andAll(markFilter));
    return collapsePadRows(node, input, chain, rowId, matchedExpr);
  }

  // Outer kLeft over a body kInner Join. The leg cascade uses kLeft legs
  // so outers and left rows survive, but that over-produces pad rows: from
  // an empty side, and from a row applyB's filter rejects. Restore
  // `outer LEFT JOIN (A JOIN B)` semantics with a per-rn pad-collapse:
  // keep every real (a, b) row and, for an outer with no match, keep
  // exactly one pad row. See
  // Decorrelate-join-rules.md §"INNER pad-row drop".
  NodeCP joinPeelLeftInner(
      ApplyCP node,
      NodeCP input,
      NodeCP leftSide,
      NodeCP rightSide,
      ExprVector applyBFilter) {
    ColumnCP rowId = makeIdColumn();
    NodeCP taggedInput = tagOuterRows(input, rowId);

    // Legs never enforce single row: an empty side makes the cross join
    // empty (a valid 0-row scalar), which a per-leg check would misread
    // as the other side's rows. ESR is applied once after the collapse.
    ColumnCP markA = makeIncludeColumn();
    NodeCP applyA = makeLeftLeg(
        taggedInput,
        leftSide,
        /*filter=*/ExprVector{},
        /*enforceSingleRow=*/false,
        markA);

    ColumnCP markB = makeIncludeColumn();
    NodeCP applyB = makeLeftLeg(
        applyA,
        rightSide,
        std::move(applyBFilter),
        /*enforceSingleRow=*/false,
        markB);

    NodeCP chain = rewrite(applyB);

    // A real body row requires both sides to match.
    return collapsePadRows(
        node, input, chain, rowId, exprFactory_.makeAnd(markA, markB));
  }

  // Reduces 'chain' to the outer Apply's contract: every row 'matchedExpr'
  // accepts survives, and an outer with no such row keeps exactly one
  // NULL-padded row. 'rowId', from an AssignUniqueId over 'input', identifies
  // the outer a row belongs to.
  NodeCP collapsePadRows(
      ApplyCP node,
      NodeCP input,
      NodeCP chain,
      ColumnCP rowId,
      ExprCP matchedExpr) {
    PerOuterMatch perOuter = markPerOuter(chain, rowId, matchedExpr);
    ColumnCP matched = perOuter.matched;

    // Keep real rows; for a match-less outer keep only its first row (a
    // pad) and drop the duplicate pads.
    ExprCP keep = exprFactory_.makeOr(
        matched,
        exprFactory_.makeAnd(
            exprFactory_.makeNot(perOuter.anyMatch),
            isFirstRowOfOuter(perOuter.padOrdinal)));
    NodeCP filtered = builder().make<Filter>({perOuter.node, ExprVector{keep}});

    NodeCP enforced = node->enforceSingleRow()
        ? enforceScalarSingleRow(filtered, rowId)
        : filtered;

    // The kept pad row carries real body values, so body columns must be
    // NULLed when this outer had no match; otherwise the scalar would leak
    // the pad's value. Outer columns are valid on a pad and pass through;
    // the matched marker becomes the outer includeMarker. rowId, leg
    // markers, and window columns drop here.
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
    return builder().make<Project>({
        enforced,
        std::move(finalExprs),
        node->outputColumns(),
    });
  }

  // Tags each 'input' row with a fresh id, so a later window can group a
  // chain's rows by the outer they came from.
  NodeCP tagOuterRows(NodeCP input, ColumnCP rowId) {
    return builder().make<AssignUniqueId>({input, rowId});
  }

  // A fresh BOOLEAN column for a kLeftSemiProject leg's mark. Each leg of a
  // chain names its own, so a plan dump tells them apart.
  static ColumnCP makeMarkColumn(std::string_view name) {
    return Column::createBoolean(name);
  }

  // A kLeft Apply leg over 'body': every 'input' row survives, padded when
  // the body has no row for it. A null 'marker' creates a private marker that
  // remains available while the Apply is decorrelated.
  NodeCP makeLeftLeg(
      NodeCP input,
      NodeCP body,
      ExprVector filter,
      bool enforceSingleRow,
      ColumnCP marker) {
    ColumnCP includeMarker = marker ? marker : makeIncludeColumn();
    ColumnVector outputs;
    outputs.reserve(
        input->outputColumns().size() + body->outputColumns().size() + 1);
    appendAll(outputs, input->outputColumns());
    appendUnique(outputs, body->outputColumns());
    outputs.push_back(includeMarker);

    return builder().make<Apply>({
        input,
        body,
        recomputeCorrelations(body, input->outputColumns()),
        velox::core::JoinType::kLeft,
        std::move(filter),
        enforceSingleRow,
        /*markColumn=*/nullptr,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr,
        includeMarker,
        std::move(outputs),
    });
  }

  // A kLeftSemiProject Apply leg over 'body': one row out per 'input' row,
  // with 'mark' saying whether 'body' had a row the filter accepts. 'inLhs'
  // and 'inBodyKey' carry an IN equality, which makes the leg null-aware.
  NodeCP makeSemiLeg(
      NodeCP input,
      NodeCP body,
      ExprVector filter,
      ColumnCP mark,
      ExprCP inLhs,
      ExprCP inBodyKey) {
    ColumnVector outputs;
    outputs.reserve(input->outputColumns().size() + 1);
    appendAll(outputs, input->outputColumns());
    outputs.push_back(mark);

    return builder().make<Apply>({
        input,
        body,
        recomputeCorrelations(body, input->outputColumns()),
        velox::core::JoinType::kLeftSemiProject,
        std::move(filter),
        /*enforceSingleRow=*/false,
        mark,
        inLhs,
        inBodyKey,
        /*includeMarker=*/nullptr,
        std::move(outputs),
    });
  }

  // A chain's rows seen per outer: 'matched' says whether the row counts,
  // 'anyMatch' whether any row of the same outer does, and 'padOrdinal'
  // numbers an outer's rows so exactly one can be singled out.
  struct PerOuterMatch {
    NodeCP node;
    ColumnCP matched;
    ColumnCP anyMatch;
    ColumnCP padOrdinal;
  };

  // Leg markers read `true` on a match and NULL on a pad, so 'matchedExpr'
  // folds to a clean boolean before the window: `bool_or` over all-NULL
  // markers would yield NULL rather than false.
  PerOuterMatch markPerOuter(NodeCP chain, ColumnCP rowId, ExprCP matchedExpr) {
    ColumnCP matched = makeIncludeColumn();
    NodeCP marked = appendColumn(
        chain,
        matched,
        exprFactory_.makeCoalesce(matchedExpr, builder().makeBoolean(false)));

    ColumnCP anyMatch = Column::createBoolean("__any_match");
    ColumnCP padOrdinal = makeIdColumn("__pad_rn");
    return {
        addPadCollapseWindow(marked, rowId, matched, anyMatch, padOrdinal),
        matched,
        anyMatch,
        padOrdinal};
  }

  // Groups rows by outer id while preserving the outer columns.
  NodeCP aggregatePerOuter(
      ApplyCP node,
      NodeCP rows,
      ColumnCP rowId,
      AggregateCallVector aggregates,
      const ColumnVector& aggregateOutputs) {
    VELOX_CHECK_EQ(aggregates.size(), aggregateOutputs.size());

    PlanObjectSet rowColumns =
        PlanObjectSet::fromObjects(rows->outputColumns());
    ColumnVector outerColumns;
    for (ColumnCP outputColumn : node->outputColumns()) {
      if (outputColumn == node->markColumn()) {
        continue;
      }
      VELOX_CHECK(
          rowColumns.contains(outputColumn),
          "kLeftSemiProject Apply must output only outer columns and its mark");
      aggregates.push_back(makeArbitrary(outputColumn));
      outerColumns.push_back(outputColumn);
    }

    ColumnVector outputs;
    outputs.reserve(1 + aggregateOutputs.size() + outerColumns.size());
    outputs.push_back(rowId);
    appendAll(outputs, aggregateOutputs);
    appendAll(outputs, outerColumns);
    return builder().make<Aggregate>(
        {.input = rows,
         .groupingKeys = ExprVector{rowId},
         .aggregates = std::move(aggregates),
         .outputColumns = std::move(outputs),
         .step = AggregateStep::kSingle,
         .groupId = nullptr,
         .globalGroupingSets = {}});
  }

  // Projects the outer Apply's schema with `mark` in its mark column.
  NodeCP projectApplyMark(ApplyCP node, NodeCP input, ExprCP mark) {
    ExprVector finalExprs;
    finalExprs.reserve(node->outputColumns().size());
    for (ColumnCP outputColumn : node->outputColumns()) {
      if (outputColumn == node->markColumn()) {
        finalExprs.push_back(mark);
      } else {
        finalExprs.push_back(outputColumn);
      }
    }
    return builder().make<Project>({
        input,
        std::move(finalExprs),
        node->outputColumns(),
    });
  }

  // Reduces rows to one row per outer and writes whether any input row
  // satisfied `matchedExpr` into the outer Apply's mark column.
  NodeCP collapseToSemiMark(
      ApplyCP node,
      NodeCP rows,
      ColumnCP rowId,
      ExprCP matchedExpr) {
    ColumnCP matched = makeIncludeColumn();
    NodeCP marked = appendColumn(
        rows,
        matched,
        exprFactory_.makeCoalesce(matchedExpr, builder().makeBoolean(false)));

    AggregateRecovery recovery(builder(), exprFactory_);
    ColumnCP anyMatch = Column::createBoolean("__any_match");
    NodeCP collapsed = aggregatePerOuter(
        node,
        marked,
        rowId,
        AggregateCallVector{recovery.makeBoolOr(matched)},
        ColumnVector{anyMatch});
    return projectApplyMark(node, collapsed, anyMatch);
  }

  // Reduces row-level IN results to one three-valued mark per outer row. A
  // qualified true comparison makes the result true. Otherwise, a qualified
  // unknown comparison makes it NULL, and no qualifying comparison makes it
  // false, including the synthetic row for an empty body.
  NodeCP collapseToInMark(
      ApplyCP node,
      NodeCP rows,
      ColumnCP rowId,
      ExprCP qualifies,
      ExprCP comparison) {
    const Literal* falseLiteral = builder().makeBoolean(false);
    ColumnCP matchRow = Column::createBoolean("__in_match");
    ColumnCP unknownRow = Column::createBoolean("__in_unknown");
    NodeCP compared = appendColumns(
        rows,
        ColumnVector{matchRow, unknownRow},
        ExprVector{
            exprFactory_.makeCoalesce(
                exprFactory_.makeAnd(qualifies, comparison), falseLiteral),
            exprFactory_.makeCoalesce(
                exprFactory_.makeAnd(
                    qualifies, exprFactory_.makeIsNull(comparison)),
                falseLiteral)});

    ColumnCP anyMatch = Column::createBoolean("__any_match");
    ColumnCP anyUnknown = Column::createBoolean("__any_unknown");
    AggregateRecovery recovery(builder(), exprFactory_);
    NodeCP collapsed = aggregatePerOuter(
        node,
        compared,
        rowId,
        AggregateCallVector{
            recovery.makeBoolOr(matchRow), recovery.makeBoolOr(unknownRow)},
        ColumnVector{anyMatch, anyUnknown});

    ExprCP mark = exprFactory_.makeSwitch(
        {{anyMatch, builder().makeBoolean(true)},
         {anyUnknown, builder().makeNull(toType(velox::BOOLEAN()))}},
        falseLiteral);
    return projectApplyMark(node, collapsed, mark);
  }

  // Holds the state needed to evaluate the right side of a join body.
  struct SemiJoinChainHead {
    // The kLeft Apply over the body's left input.
    NodeCP leftApply;
    // Groups all chain rows that came from the same outer row.
    ColumnCP outerRowId;
    // Distinguishes real left-input rows from padding.
    ColumnCP leftIncludeMarker;
  };

  // Groups join-chain filters by the latest leg needed to evaluate them.
  struct JoinChainFilterSplit {
    // Conjuncts that read only outer and left-side columns.
    ExprVector leftAndOuter;
    // Conjuncts that read a right-side output or semi-project mark.
    ExprVector rightDependent;
  };

  // Separates conjuncts that can ride on the left leg from those that require
  // the right leg.
  JoinChainFilterSplit splitJoinChainFilters(
      const ExprVector& filter,
      JoinCP joinBody) {
    PlanObjectSet rightColumns =
        PlanObjectSet::fromObjects(joinBody->right()->outputColumns());
    if (joinBody->isLeftSemiProject()) {
      ColumnCP bodyMark = joinBody->markColumn();
      VELOX_CHECK_NOT_NULL(bodyMark);
      rightColumns.add(bodyMark);
    }

    JoinChainFilterSplit split;
    for (ExprCP conjunct : filter) {
      if (conjunct->columns().hasIntersection(rightColumns)) {
        split.rightDependent.push_back(conjunct);
      } else {
        split.leftAndOuter.push_back(conjunct);
      }
    }
    return split;
  }

  // Starts a semi-project join chain while retaining outers with no left row.
  SemiJoinChainHead
  makeSemiJoinChainHead(NodeCP input, NodeCP leftSide, ExprVector leftFilter) {
    ColumnCP outerRowId = makeIdColumn();
    NodeCP taggedInput = tagOuterRows(input, outerRowId);
    ColumnCP leftIncludeMarker = makeIncludeColumn();
    return {
        makeLeftLeg(
            taggedInput,
            leftSide,
            std::move(leftFilter),
            /*enforceSingleRow=*/false,
            leftIncludeMarker),
        outerRowId,
        leftIncludeMarker,
    };
  }

  // Reads `padOrdinal = 1`, which picks one row of each outer.
  ExprCP isFirstRowOfOuter(ColumnCP padOrdinal) {
    return exprFactory_.makeEq(
        padOrdinal,
        builder().makeLiteral(
            velox::Variant(static_cast<int64_t>(1)), toType(velox::BIGINT())));
  }

  // Returns a Project that passes `input`'s columns through unchanged
  // and appends `columns`, computed as the matching entry of `newExprs`.
  NodeCP appendColumns(
      NodeCP input,
      const ColumnVector& columns,
      const ExprVector& newExprs) {
    VELOX_CHECK_EQ(columns.size(), newExprs.size());
    const auto& inputColumns = input->outputColumns();

    ExprVector exprs;
    ColumnVector outputs;
    exprs.reserve(inputColumns.size() + columns.size());
    outputs.reserve(inputColumns.size() + columns.size());
    appendAll(exprs, inputColumns);
    appendAll(outputs, inputColumns);
    appendAll(exprs, newExprs);
    appendAll(outputs, columns);
    return builder().make<Project>(
        {input, std::move(exprs), std::move(outputs)});
  }

  NodeCP appendColumn(NodeCP input, ColumnCP column, ExprCP expr) {
    return appendColumns(input, ColumnVector{column}, ExprVector{expr});
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
    WindowFunctions functions;
    functions.push_back(boolOrWindowFunction(marker));
    functions.push_back(rowNumberWindowFunction(padOrdinal));

    ColumnVector outputs;
    outputs.reserve(input->outputColumns().size() + 2);
    appendAll(outputs, input->outputColumns());
    outputs.push_back(anyMatch);
    outputs.push_back(padOrdinal);

    return builder().make<Window>({
        input,
        std::move(functions),
        ExprVector{partition},
        /*orderKeys=*/{},
        /*orderTypes=*/{},
        std::move(outputs),
    });
  }

  // Outer Apply is kLeftSemiProject. Supports IN over a kInner body and EXISTS
  // over a kInner, kLeft, or non-null-aware kLeftSemiProject body. With
  // nothing pairing the two sides of an inner Join the EXISTS mark is markA
  // AND markB; otherwise it is a property of the pair. Other Join kinds NYI
  // loud.
  NodeCP joinPeelSemi(
      ApplyCP node,
      NodeCP input,
      JoinCP joinBody,
      ExprVector joinPredicate,
      ExprVector accumulatedFilter) {
    if (node->inLhs() != nullptr) {
      return joinPeelSemiIn(
          node, input, joinBody, std::move(joinPredicate), accumulatedFilter);
    }
    if (joinBody->isLeft() || joinBody->isLeftSemiProject()) {
      return joinPeelSemiWithGuaranteedLeftOutput(
          node,
          input,
          joinBody,
          std::move(joinPredicate),
          std::move(accumulatedFilter));
    }
    if (!joinBody->isInner()) {
      VELOX_NYI(
          "Decorrelate joinPeel: outer kLeftSemiProject EXISTS requires a "
          "kInner, kLeft, or kLeftSemiProject body: joinType={}",
          joinBody->joinTypeName());
    }

    // A predicate ties the two sides together, so existence is not the
    // conjunction of each side's own. Anything accumulated above the body join
    // counts: a single-side conjunct is pushed into its side before this pass,
    // so what remains here reads both. The chain below could not test it
    // anyway, its first leg being a semi join that forwards the outer's
    // columns but not the body side's.
    if (!joinPredicate.empty() || !accumulatedFilter.empty()) {
      return joinPeelSemiInnerWithPredicate(
          node,
          input,
          joinBody,
          std::move(joinPredicate),
          std::move(accumulatedFilter));
    }

    // Cross-join EXISTS: mark = (∃a) AND (∃b).
    NodeCP leftSide = joinBody->left();
    NodeCP rightSide = joinBody->right();

    ColumnCP markA = makeMarkColumn("_join_chain_markA");
    NodeCP applyA = makeSemiLeg(
        input,
        leftSide,
        /*filter=*/ExprVector{},
        markA,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr);

    ColumnCP markB = makeMarkColumn("_join_chain_markB");
    NodeCP applyB = makeSemiLeg(
        applyA,
        rightSide,
        std::move(accumulatedFilter),
        markB,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr);

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
    return builder().make<Project>({
        decorrelatedChain,
        std::move(finalExprs),
        node->outputColumns(),
    });
  }

  // Evaluates IN against the rows of an inner Join body, retaining one pad
  // row per outer so an empty body produces false.
  NodeCP joinPeelSemiIn(
      ApplyCP node,
      NodeCP input,
      JoinCP joinBody,
      ExprVector joinPredicate,
      const ExprVector& accumulatedFilter) {
    if (!joinBody->isInner()) {
      VELOX_NYI(
          "Decorrelate joinPeel: outer kLeftSemiProject IN requires a "
          "kInner body: joinType={}",
          joinBody->joinTypeName());
    }

    // applyB resolves the IN key against its body, the Join's right input. A
    // key reading anything else resolves on the input side, where the pair is
    // demoted to a filter and the leg loses the null-awareness the mark's
    // unknown case depends on.
    PlanObjectSet rightColumns =
        PlanObjectSet::fromObjects(joinBody->right()->outputColumns());
    if (!rightColumns.containsColumns(node->inBodyKey())) {
      VELOX_NYI(
          "Decorrelate joinPeel: outer kLeftSemiProject IN over a Join body "
          "requires a body key reading only the Join's right input");
    }

    JoinChainFilterSplit filterSplit =
        splitJoinChainFilters(accumulatedFilter, joinBody);
    SemiJoinChainHead chain = makeSemiJoinChainHead(
        input, joinBody->left(), std::move(filterSplit.leftAndOuter));
    ExprVector applyBFilter = std::move(joinPredicate);
    appendAll(applyBFilter, filterSplit.rightDependent);

    ColumnCP inMark = makeMarkColumn("_join_chain_in");
    NodeCP applyB = makeSemiLeg(
        chain.leftApply,
        joinBody->right(),
        std::move(applyBFilter),
        inMark,
        node->inLhs(),
        node->inBodyKey());

    return collapseToInMark(
        node,
        rewrite(applyB),
        chain.outerRowId,
        chain.leftIncludeMarker,
        inMark);
  }

  // Handles a body that produces at least one output row for every left input
  // row. Keep an empty left side long enough to produce a false outer mark,
  // then reduce to one row per outer.
  NodeCP joinPeelSemiWithGuaranteedLeftOutput(
      ApplyCP node,
      NodeCP input,
      JoinCP joinBody,
      ExprVector joinPredicate,
      const ExprVector& accumulatedFilter) {
    JoinChainFilterSplit filterSplit =
        splitJoinChainFilters(accumulatedFilter, joinBody);
    SemiJoinChainHead chain = makeSemiJoinChainHead(
        input, joinBody->left(), std::move(filterSplit.leftAndOuter));

    if (filterSplit.rightDependent.empty()) {
      // Both supported joins emit at least one row per left row. Their
      // predicate only chooses right-side matches or computes the body mark,
      // neither of which affects EXISTS when the remaining filters read only
      // outer and left columns.
      return collapseToSemiMark(
          node,
          rewrite(chain.leftApply),
          chain.outerRowId,
          chain.leftIncludeMarker);
    }

    NodeCP applyB;
    if (joinBody->isLeft()) {
      applyB = makeLeftLeg(
          chain.leftApply,
          joinBody->right(),
          std::move(joinPredicate),
          /*enforceSingleRow=*/false,
          /*marker=*/nullptr);
    } else {
      // A semi-project body emits one row for every left row and exposes its
      // match result through the body's mark.
      ColumnCP bodyMark = joinBody->markColumn();
      VELOX_CHECK_NOT_NULL(bodyMark);
      if (joinBody->nullAware()) {
        VELOX_NYI(
            "Decorrelate joinPeel: outer kLeftSemiProject EXISTS over a "
            "null-aware kLeftSemiProject Join is not yet implemented");
      }
      applyB = makeSemiLeg(
          chain.leftApply,
          joinBody->right(),
          std::move(joinPredicate),
          bodyMark,
          /*inLhs=*/nullptr,
          /*inBodyKey=*/nullptr);
    }

    // Evaluating the post-join filter in the mark keeps the pad row that
    // produces false when no body row qualifies.
    ExprCP matchedExpr = exprFactory_.makeAnd(
        chain.leftIncludeMarker,
        exprFactory_.andAll(filterSplit.rightDependent));

    return collapseToSemiMark(
        node, rewrite(applyB), chain.outerRowId, matchedExpr);
  }

  // Outer kLeftSemiProject EXISTS over a body kInner Join where some predicate
  // pairs the two sides, whether it came from the join itself or from a filter
  // above it. The mark asks whether the outer has any (a, b) pair the
  // predicate accepts, which a per-outer `bool_or` over the chain's rows
  // answers. The chain then collapses to the one row per outer the outer
  // Apply's contract calls for.
  NodeCP joinPeelSemiInnerWithPredicate(
      ApplyCP node,
      NodeCP input,
      JoinCP joinBody,
      ExprVector joinPredicate,
      const ExprVector& accumulatedFilter) {
    JoinChainFilterSplit filterSplit =
        splitJoinChainFilters(accumulatedFilter, joinBody);
    SemiJoinChainHead chain = makeSemiJoinChainHead(
        input, joinBody->left(), std::move(filterSplit.leftAndOuter));

    ExprVector applyBFilter = std::move(joinPredicate);
    appendAll(applyBFilter, filterSplit.rightDependent);

    ColumnCP markB = makeMarkColumn("_join_chain_markB");
    NodeCP applyB = makeSemiLeg(
        chain.leftApply,
        joinBody->right(),
        std::move(applyBFilter),
        markB,
        /*inLhs=*/nullptr,
        /*inBodyKey=*/nullptr);

    // A pad row of applyA reads its include marker as NULL, and applyB's
    // existence test over its NULL columns reads false, so padding cannot
    // count as a match.
    return collapseToSemiMark(
        node,
        rewrite(applyB),
        chain.outerRowId,
        exprFactory_.makeAnd(chain.leftIncludeMarker, markB));
  }

  // Rebuilds `unnestBody` over `input`, replicating every column `input`
  // produces. A non-null `marker` makes it an outer Unnest, which keeps an
  // input row whose unnested value is empty.
  NodeCP
  liftUnnestOver(const Unnest* unnestBody, NodeCP input, ColumnCP marker) {
    ColumnVector replicated = input->outputColumns();
    ColumnVector outputs = replicated;
    for (const auto& columns : unnestBody->unnestColumns()) {
      appendAll(outputs, columns);
    }
    if (unnestBody->withOrdinality()) {
      outputs.push_back(unnestBody->ordinalityColumn());
    }
    if (marker != nullptr) {
      outputs.push_back(marker);
    }

    return builder().make<Unnest>({
        input,
        unnestBody->unnestExpressions(),
        std::move(replicated),
        unnestBody->unnestColumns(),
        unnestBody->ordinalityColumn(),
        marker,
        std::move(outputs),
    });
  }

  // Unnest peel. An Unnest whose expressions read outer columns produces
  // rows per outer row, which is what an Apply already means, so the Unnest
  // lifts above the Apply and replicates the outer columns. Conjuncts
  // reading a column the Unnest produces cannot go below the lift and stay
  // above it.
  //
  // Only kInner lifts this way, since a plain Unnest drops an outer whose
  // array is empty. kLeftSemiProject keeps such an outer with an outer Unnest
  // and reduces per outer; see `unnestPeelSemi`. kLeft is not implemented.
  NodeCP unnestPeel(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      ExprVector accumulatedFilter) {
    if (node->isLeftSemiProject()) {
      return unnestPeelSemi(node, input, body, accumulatedFilter);
    }
    if (!node->isInner()) {
      VELOX_NYI(
          "Decorrelate unnestPeel: a {} Apply over an Unnest body is not yet "
          "implemented",
          node->isLeft() ? "kLeft" : "semi-filter or anti");
    }
    const Unnest* unnestBody = body->as<Unnest>();
    NodeCP newBody = unnestBody->input();

    PlanObjectSet unnestedColumns;
    for (const auto& columns : unnestBody->unnestColumns()) {
      unnestedColumns.unionObjects(columns);
    }
    if (unnestBody->withOrdinality()) {
      unnestedColumns.add(unnestBody->ordinalityColumn());
    }

    ExprVector innerFilter;
    ExprVector liftedFilter;
    for (ExprCP conjunct : accumulatedFilter) {
      if (conjunct->columns().hasIntersection(unnestedColumns)) {
        liftedFilter.push_back(conjunct);
      } else {
        innerFilter.push_back(conjunct);
      }
    }

    ColumnVector innerOutputColumns;
    innerOutputColumns.reserve(
        input->outputColumns().size() + newBody->outputColumns().size());
    appendAll(innerOutputColumns, input->outputColumns());
    appendUnique(innerOutputColumns, newBody->outputColumns());

    NodeCP innerApply = builder().make<Apply>({
        input,
        newBody,
        recomputeCorrelations(newBody, input->outputColumns()),
        node->kind(),
        std::move(innerFilter),
        node->enforceSingleRow(),
        node->markColumn(),
        node->inLhs(),
        node->inBodyKey(),
        node->includeMarker(),
        std::move(innerOutputColumns),
    });

    NodeCP decorrelatedInner = rewrite(innerApply);

    NodeCP lifted = liftUnnestOver(
        unnestBody, decorrelatedInner, unnestBody->markerColumn());
    const ColumnVector unnestOutputs = lifted->outputColumns();

    if (!liftedFilter.empty()) {
      lifted = builder().make<Filter>({lifted, std::move(liftedFilter)});
    }

    // The lift carries every column the inner Apply produced, in its own
    // order; the Apply's schema may differ in either.
    if (unnestOutputs == node->outputColumns()) {
      return lifted;
    }
    ExprVector finalExprs;
    appendAll(finalExprs, node->outputColumns());
    return builder().make<Project>({
        lifted,
        std::move(finalExprs),
        node->outputColumns(),
    });
  }

  // Unnest peel for kLeftSemiProject: `EXISTS (SELECT ... FROM UNNEST(a) ...)`
  // asks whether any element of the outer row's array passes the filter.
  //
  // The lift is an outer Unnest, which keeps an outer whose array is empty and
  // says so in its marker, so every outer reaches the answer. A row counts
  // when it came from a real element and the filter accepts it; `bool_or` over
  // an outer's rows turns that into the mark, and one row per outer survives.
  NodeCP unnestPeelSemi(
      ApplyCP node,
      NodeCP input,
      NodeCP body,
      const ExprVector& accumulatedFilter) {
    VELOX_CHECK(
        !node->enforceSingleRow(),
        "A kLeftSemiProject Apply does not assert a single row");

    const Unnest* unnestBody = body->as<Unnest>();
    if (!isSingleEmptyRowValues(unnestBody->input())) {
      VELOX_NYI(
          "Decorrelate unnestPeel: EXISTS over an Unnest of a relation is not "
          "yet implemented; only over the subquery's own row is");
    }

    ColumnCP rowId = makeIdColumn();
    NodeCP tagged = tagOuterRows(input, rowId);

    ColumnCP marker = makeIncludeColumn();
    NodeCP expanded = liftUnnestOver(unnestBody, tagged, marker);

    // A row of `expanded` is an element of this outer's array when the marker
    // says it came from a value and the subquery's own predicates accept it.
    ExprCP qualifies = marker;
    for (ExprCP conjunct : accumulatedFilter) {
      qualifies = exprFactory_.makeAnd(qualifies, conjunct);
    }

    if (!node->nullAware()) {
      return collapseToSemiMark(node, expanded, rowId, qualifies);
    }

    return collapseToInMark(
        node,
        expanded,
        rowId,
        qualifies,
        exprFactory_.makeEq(node->inLhs(), node->inBodyKey()));
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
    if (node->isInner()) {
      VELOX_NYI(
          "Decorrelate: INNER LATERAL over an AssignUniqueId body is not "
          "yet supported");
    }
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

    NodeCP innerApply = builder().make<Apply>({
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

    NodeCP withId =
        builder().make<AssignUniqueId>({decorrelatedInner, idColumn});

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
    return builder().make<Project>({
        withId,
        std::move(finalExprs),
        node->outputColumns(),
    });
  }

  // Aggregate peel (Rule A) for kLeft: the body aggregates once per outer
  // row.
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
  // A non-empty gby leaves an outer one row per group, and F_post drops the
  // groups it rejects, so an outer it rejects every group of needs a pad row
  // kept in their place. That collapse sits below the Project. Which pad it
  // keeps is arbitrary, since they carry the same values, so the row_number
  // picking one needs no ORDER BY:
  //
  //   - Project
  //     - Filter (accepted, or the first row of an outer with none)
  //       - Window (row_number and any-match, both over the outer)
  //         - Project (rowId, real-group marker AND F_post)
  //           - Aggregate (as above)
  //
  // The other two combinations drop no row and go straight from the Aggregate
  // to the Project, which NULLs the values of a row that does not count: over
  // an empty gby the aggregate gives an outer one row, and with no F_post
  // every group is either real or the one pad standing for an outer's
  // absence.
  //
  // In scope: kind = kLeft; gby empty or non-empty; F_post empty or
  // non-empty.
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
    if (node->isInner()) {
      VELOX_NYI(
          "Decorrelate: INNER LATERAL over an Aggregate body is not yet "
          "supported");
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

    // Equi-correlated scalar aggregate with no boundary pre-filter and
    // aggregates that don't reference the outer: group the body once by the
    // correlation key and LEFT JOIN it back to the outer, aggregating once
    // per key instead of once per outer row. Other correlations use the
    // general shape below.
    // With an inner GROUP BY the post-aggregation filter runs below the
    // join-back, where the outer columns are not visible, so one that reads
    // them takes the general shape instead. Without one it runs above the
    // join, where they are, and reads whatever it likes.
    const bool filterPostIsBodyLocal = numGroupingKeys == 0 ||
        !readsOuter(filterPostConjuncts, input->outputColumns());

    if (filterPreConjuncts.empty() && filterPostIsBodyLocal &&
        !readsOuter(aggregate, input->outputColumns())) {
      if (auto correlation =
              liftEquiCorrelation(aggregate->input(), input->outputColumns())) {
        return aggregatePeelEqui(
            node,
            input,
            aggregate,
            std::move(*correlation),
            filterPostConjuncts,
            numGroupingKeys);
      }
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

    auto wraps = buildAggregateWraps(
        aggregate, numGroupingKeys, /*coalesceEmptyInput=*/false);

    // A grouped body that produced no rows for an outer has no groups, so its
    // pad group is not a body row however its aggregates read.
    ColumnCP realGroupMarker =
        numGroupingKeys > 0 ? makeIncludeColumn() : nullptr;
    NodeCP liftedAggregate = buildLiftedAggregate(
        input,
        aggregate,
        recovery,
        innerApply.rowIdColumn,
        innerApply.includeMarker,
        wraps,
        numGroupingKeys,
        decorrelatedInner,
        realGroupMarker);

    // With an inner GROUP BY the lifted aggregate groups by (rowId, gby keys),
    // so it can emit several rows per outer row; grouping on the rowId alone no
    // longer guarantees one row per outer.
    if (numGroupingKeys > 0) {
      if (filterPostConjuncts.empty()) {
        // An outer's groups are all real, or are the one pad group standing
        // for its absence, so no row needs dropping: the pad's values are
        // NULLed at the final Project and the scalar bound asserted over the
        // groups.
        NodeCP enforced = node->enforceSingleRow()
            ? enforceScalarSingleRow(liftedAggregate, innerApply.rowIdColumn)
            : liftedAggregate;
        return buildAggregateFinalProject(
            node,
            input,
            aggregate,
            wraps,
            /*valueIsLive=*/realGroupMarker,
            enforced,
            numGroupingKeys,
            /*includeMarkerValue=*/realGroupMarker);
      }

      // F_post drops the groups it rejects rather than NULLing them, and an
      // outer left with none keeps one pad. Which rows survive decides the row
      // count, so the scalar bound is asserted inside the collapse, after it.
      //
      // F_post reads the aggregate's own output columns, which the Project
      // below publishes, so it is applied as written rather than substituted.
      return collapsePadRows(
          node,
          input,
          buildAggregateRowIdProject(
              input,
              aggregate,
              wraps,
              liftedAggregate,
              numGroupingKeys,
              innerApply.rowIdColumn,
              realGroupMarker),
          innerApply.rowIdColumn,
          exprFactory_.makeAnd(
              realGroupMarker, exprFactory_.andAll(filterPostConjuncts)));
    }

    // Every outer has a group here, holding either its body rows or the pad
    // that stands for none, so every row counts for its outer. F_post runs at
    // the final Project, over the lifted Aggregate's own outputs, so it is
    // substituted onto those.
    return buildAggregateFinalProject(
        node,
        input,
        aggregate,
        wraps,
        buildFilterPostSubstituted(
            filterPostConjuncts, aggregate, wraps, numGroupingKeys),
        liftedAggregate,
        numGroupingKeys,
        builder().makeBoolean(true));
  }

  // For each aggregate in the original Aggregate, decides what the
  // lifted Aggregate should emit (`liftedOutput`) and what the final
  // Project should produce at the original output position
  // (`finalExpression`). Empty-input value resolution via the
  // FunctionRegistry: aggregates whose empty value is non-NULL (count,
  // count_if, etc.) need COALESCE; others pass through.
  //
  // 'coalesceEmptyInput' restores an aggregate's empty-input value on an outer
  // row that reaches the final Project with no group behind it. Two shapes do
  // not need it: one where the lifted Aggregate groups by the outer row id, so
  // an outer with no matches still forms a group and a masked aggregate over
  // it already returns that value; and one with an inner GROUP BY, where an
  // outer with no group read no rows at all and the scalar is NULL.
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
      size_t numGroupingKeys,
      bool coalesceEmptyInput) {
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

      if (!coalesceEmptyInput || emptyValue.isNull()) {
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
    NodeCP taggedInput = builder().make<AssignUniqueId>({input, rowIdColumn});

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

    NodeCP applyNode = builder().make<Apply>({
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
      NodeCP decorrelatedInner,
      ColumnCP realGroupMarker) {
    AggregateCallVector liftedAggregates =
        recovery.rewriteCountStar(aggregate->aggregates(), includeMarker);
    liftedAggregates =
        recovery.addFilterCondition(liftedAggregates, includeMarker);
    for (ColumnCP outerColumn : input->outputColumns()) {
      liftedAggregates.push_back(makeArbitrary(outerColumn));
    }
    // An outer with no body rows contributes one pad row, so a group holds
    // either only real rows or only that pad: the marker is constant per
    // group and 'arbitrary' reads it exactly.
    if (realGroupMarker != nullptr) {
      liftedAggregates.push_back(makeArbitrary(includeMarker));
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
    if (realGroupMarker != nullptr) {
      liftedOutputColumns.push_back(realGroupMarker);
    }

    return builder().make<Aggregate>({
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

  // Projects the lifted Aggregate onto the outer Apply's output columns and
  // keeps 'rowId', which identifies the outer a row belongs to, and
  // 'realGroupMarker', which the collapse below reads. Writes no
  // includeMarker: which rows count is decided by the filter that runs next.
  //
  // Only the inner-GROUP BY shape collapses, so 'realGroupMarker' is always a
  // column here, never null.
  NodeCP buildAggregateRowIdProject(
      NodeCP input,
      AggregateCP aggregate,
      const std::vector<AggregateWrap>& wraps,
      NodeCP child,
      size_t numGroupingKeys,
      ColumnCP rowId,
      ColumnCP realGroupMarker) {
    ExprVector expressions;
    ColumnVector outputColumns;
    appendAll(expressions, input->outputColumns());
    appendAll(outputColumns, input->outputColumns());
    for (size_t i = 0; i < numGroupingKeys; ++i) {
      expressions.push_back(aggregate->outputColumns()[i]);
      outputColumns.push_back(aggregate->outputColumns()[i]);
    }
    for (size_t i = 0; i < wraps.size(); ++i) {
      expressions.push_back(wraps[i].finalExpression);
      outputColumns.push_back(aggregate->outputColumns()[numGroupingKeys + i]);
    }
    expressions.push_back(rowId);
    outputColumns.push_back(rowId);
    expressions.push_back(realGroupMarker);
    outputColumns.push_back(realGroupMarker);
    return builder().make<Project>({
        child,
        std::move(expressions),
        std::move(outputColumns),
    });
  }

  // True if any of 'expressions' reads an outer (input) column. Such an
  // expression can only be evaluated where the outer columns are visible,
  // which a join-back's body side is not: it reads the body alone.
  static bool readsOuter(
      const ExprVector& expressions,
      const ColumnVector& outerColumns) {
    return std::any_of(
        expressions.begin(), expressions.end(), [&](ExprCP expression) {
          return expression->columns().containsAny(outerColumns);
        });
  }

  // True if any expression a Node carries reads an outer column. Asking the
  // Node rather than the members a caller happens to think of covers every
  // place one can hide — an Aggregate's grouping keys, and each aggregate's
  // arguments, FILTER mask and order keys.
  static bool readsOuter(NodeCP node, const ColumnVector& outerColumns) {
    bool readsOuterColumn = false;
    forEachExpressionInNode(node, [&](ExprCP expression) {
      readsOuterColumn =
          readsOuterColumn || expression->columns().containsAny(outerColumns);
    });
    return readsOuterColumn;
  }

  // Equi-correlation lifted from a correlated body, for the join-back
  // decorrelations.
  struct EquiCorrelation {
    // The body with the correlation predicates removed.
    NodeCP cleanBody;
    // Equi keys: `leftKeys` over the outer input, `rightKeys` over
    // `cleanBody`.
    ExprVector leftKeys;
    ExprVector rightKeys;
  };

  // Removes conjuncts reading 'inputColumns' from the Filters of 'node',
  // collecting them in 'correlation', and returns what is left. Descends
  // through Sort and through Projects that only pass columns through, since
  // translating a query leaves those between the operators that carry
  // predicates. A lifted conjunct becomes a join key, so a rebuilt Project
  // also carries the columns its body side reads, which that Project may
  // have pruned.
  NodeCP liftCorrelationFromBody(
      NodeCP node,
      const ColumnVector& inputColumns,
      ExprVector& correlation) {
    if (node->is(NodeType::kFilter)) {
      const Filter* filter = node->as<Filter>();
      ExprVector kept;
      for (ExprCP conjunct : filter->predicates()) {
        (conjunct->columns().containsAny(inputColumns) ? correlation : kept)
            .push_back(conjunct);
      }
      // Classify before descending, so a Project below carries the columns
      // these conjuncts read, which it may have pruned.
      NodeCP input =
          liftCorrelationFromBody(filter->input(), inputColumns, correlation);
      if (input == filter->input() &&
          kept.size() == filter->predicates().size()) {
        return node;
      }
      return kept.empty() ? input
                          : builder().make<Filter>({input, std::move(kept)});
    }

    if (const Project* project = asPassThroughProject(node)) {
      NodeCP input =
          liftCorrelationFromBody(project->input(), inputColumns, correlation);
      if (input == project->input()) {
        return node;
      }
      // The lifted predicate becomes a join key, so the columns it reads on
      // the body side have to reach the join. A Project that pruned them is
      // rebuilt carrying them.
      ExprVector exprs = project->exprs();
      ColumnVector outputColumns = project->outputColumns();
      PlanObjectSet present = PlanObjectSet::fromObjects(outputColumns);
      PlanObjectSet available =
          PlanObjectSet::fromObjects(input->outputColumns());
      for (ExprCP conjunct : correlation) {
        conjunct->columns().forEach<Column>([&](const Column* column) {
          if (available.contains(column) && !present.contains(column)) {
            present.add(column);
            exprs.push_back(column);
            outputColumns.push_back(column);
          }
        });
      }
      return builder().make<Project>(
          {input, std::move(exprs), std::move(outputColumns)});
    }

    // A Sort neither adds nor drops rows, so a predicate below it selects the
    // same rows as one above it.
    if (node->is(NodeType::kSort)) {
      const Sort* sort = node->as<Sort>();
      NodeCP input =
          liftCorrelationFromBody(sort->input(), inputColumns, correlation);
      if (input == sort->input()) {
        return node;
      }
      return builder().make<Sort>(
          {input, sort->orderKeys(), sort->orderTypes()});
    }

    return node;
  }

  // Lifts the equi-correlation out of 'body'. Succeeds only when every
  // correlation conjunct is an equality partitioning cleanly into an
  // outer-side and a body-side expression, and what remains after removing
  // them is correlation-free; returns nullopt otherwise.
  std::optional<EquiCorrelation> liftEquiCorrelation(
      NodeCP body,
      const ColumnVector& inputColumns) {
    ExprVector correlation;
    NodeCP base = liftCorrelationFromBody(body, inputColumns, correlation);

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

    return EquiCorrelation{
        base, std::move(split.leftKeys), std::move(split.rightKeys)};
  }

  // Decorrelates an equi-correlated kLeft Aggregate Apply by grouping the
  // (correlation-free) body once by the correlation key, plus any inner
  // GROUP BY key, then LEFT JOINing the result back to the outer on the
  // correlation equi keys.
  //
  // Without an inner GROUP BY the grouped body has one row per correlation
  // key, empty-input aggregates receive their empty value on an outer with no
  // match via the COALESCE in `wraps`, and a post-aggregation filter reads
  // that value at the final Project. With one, an outer can match several
  // groups: the filter selects among them below the join, an outer left with
  // none reads NULL, and the scalar bound is asserted over the join output.
  NodeCP aggregatePeelEqui(
      ApplyCP node,
      NodeCP input,
      AggregateCP aggregate,
      EquiCorrelation correlation,
      const ExprVector& filterPostConjuncts,
      size_t numGroupingKeys) {
    std::vector<AggregateWrap> wraps = buildAggregateWraps(
        aggregate,
        numGroupingKeys,
        /*coalesceEmptyInput=*/numGroupingKeys == 0);

    // The correlation keys become the new Aggregate's grouping keys and the
    // join-back's right keys: reuse the body column for a plain column, mint
    // a fresh column for a computed key.
    ColumnVector rightKeyColumns;
    rightKeyColumns.reserve(correlation.rightKeys.size());
    for (ExprCP key : correlation.rightKeys) {
      // A key the query also groups by is already published under that
      // grouping key's column; reuse it rather than name the value twice.
      ColumnCP groupingKeyOutput = nullptr;
      for (size_t i = 0; i < numGroupingKeys; ++i) {
        if (aggregate->groupingKeys()[i] == key) {
          groupingKeyOutput = aggregate->outputColumns()[i];
          break;
        }
      }
      if (groupingKeyOutput != nullptr) {
        rightKeyColumns.push_back(groupingKeyOutput);
      } else {
        rightKeyColumns.push_back(
            key->is(PlanType::kColumnExpr)
                ? key->as<Column>()
                : Column::create("__groupingKey", key->value()));
      }
    }

    // Aggregate output: [correlation keys, inner gby keys, raw aggregates]. A
    // correlation key the query also groups by is one key, not two.
    ColumnVector aggregateOutputColumns;
    aggregateOutputColumns.reserve(
        rightKeyColumns.size() + numGroupingKeys + wraps.size());
    appendAll(aggregateOutputColumns, rightKeyColumns);

    ExprVector groupingKeys = std::move(correlation.rightKeys);
    ColumnVector addedGroupingKeys;
    PlanObjectSet keyColumns = PlanObjectSet::fromObjects(rightKeyColumns);
    for (size_t i = 0; i < numGroupingKeys; ++i) {
      ColumnCP groupingKeyOutput = aggregate->outputColumns()[i];
      if (keyColumns.contains(groupingKeyOutput)) {
        continue;
      }
      keyColumns.add(groupingKeyOutput);
      groupingKeys.push_back(aggregate->groupingKeys()[i]);
      addedGroupingKeys.push_back(groupingKeyOutput);
    }
    appendAll(aggregateOutputColumns, addedGroupingKeys);

    for (const auto& wrap : wraps) {
      aggregateOutputColumns.push_back(wrap.liftedOutput);
    }

    AggregateCallVector aggregates = aggregate->aggregates();
    NodeCP groupedBody = builder().make<Aggregate>({
        .input = correlation.cleanBody,
        .groupingKeys = std::move(groupingKeys),
        .aggregates = std::move(aggregates),
        .outputColumns = std::move(aggregateOutputColumns),
    });

    // With an inner GROUP BY the filter selects among an outer's groups, so
    // it runs below the join and a rejected group leaves the outer NULL. A
    // global aggregate gives an outer one row, which the filter must NULL
    // rather than drop, so it runs at the final Project.
    ExprCP filterPostSubstituted = buildFilterPostSubstituted(
        filterPostConjuncts, aggregate, wraps, numGroupingKeys);
    if (numGroupingKeys > 0 && filterPostSubstituted != nullptr) {
      groupedBody = builder().make<Filter>(
          {groupedBody, ExprVector{filterPostSubstituted}});
    }

    // The join publishes the inner gby keys and the raw aggregate outputs;
    // the correlation key columns are internal and dropped. Every grouping
    // key is published, including one shared with a correlation key: the
    // grouped body carries it either way.
    ColumnVector bodyColumns;
    bodyColumns.reserve(numGroupingKeys + wraps.size());
    for (size_t i = 0; i < numGroupingKeys; ++i) {
      bodyColumns.push_back(aggregate->outputColumns()[i]);
    }
    for (const auto& wrap : wraps) {
      bodyColumns.push_back(wrap.liftedOutput);
    }

    ExprVector rightKeyExprs;
    appendAll(rightKeyExprs, rightKeyColumns);

    // Without an inner GROUP BY every outer has a row carrying its
    // aggregate's empty-input value, and that row counts, so no marker is
    // needed. An outer matches several groups only where the query groups by
    // a key the join does not match on.
    JoinBack back = joinBodyBack(
        input,
        groupedBody,
        correlation.leftKeys,
        rightKeyExprs,
        bodyColumns,
        /*markPads=*/numGroupingKeys > 0,
        /*enforceSingleRow=*/!addedGroupingKeys.empty() &&
            node->enforceSingleRow());

    return buildAggregateFinalProject(
        node,
        input,
        aggregate,
        wraps,
        numGroupingKeys == 0 ? filterPostSubstituted : nullptr,
        back.node,
        numGroupingKeys,
        back.includeMarker != nullptr ? static_cast<ExprCP>(back.includeMarker)
                                      : builder().makeBoolean(true));
  }

  // Final Project: shapes `child`'s output to node->outputColumns()
  // (= L.cols ++ gby_cols ++ aggregate_results ++ includeMarker).
  //   - L.cols: pass-through from the input columns (the outer side of
  //     the join-back, or the lifted `arbitrary` outputs).
  //   - gby_cols and aggregate_results: the grouping-key outputs, and each
  //     `wrap.finalExpression` (pass-through or COALESCE), both wrapped in
  //     IF('valueIsLive', ..., NULL) where that is given. 'valueIsLive' says
  //     the body values on a row count for its outer — a substituted F_post
  //     where a HAVING-style predicate selects among an outer's groups, or
  //     the real-group marker where the row may be the pad standing for an
  //     outer that read no rows. Either way the outer row is preserved and
  //     only what the body would have produced is NULLed, which is kLeft
  //     semantics. A grouping key is NULLed with the aggregates, since a key
  //     whose expression is non-NULL over the body's NULLs would otherwise
  //     publish a value for an outer that read no rows.
  //   - includeMarker: 'includeMarkerValue'. Literal `true` where every
  //     outer reaches here with a row of its own, and a marker column
  //     sourced from the body where a join below can pad an outer, so the
  //     pad reads NULL.
  NodeCP buildAggregateFinalProject(
      ApplyCP node,
      NodeCP input,
      AggregateCP aggregate,
      const std::vector<AggregateWrap>& wraps,
      ExprCP valueIsLive,
      NodeCP child,
      size_t numGroupingKeys,
      ExprCP includeMarkerValue) {
    auto nullUnlessLive = [&](ExprCP expression, TypeCP type) {
      return valueIsLive == nullptr
          ? expression
          : exprFactory_.makeIf(
                valueIsLive, expression, builder().makeNull(type));
    };

    ExprVector finalExpressions;
    finalExpressions.reserve(node->outputColumns().size());
    appendAll(finalExpressions, input->outputColumns());
    for (size_t i = 0; i < numGroupingKeys; ++i) {
      ColumnCP groupingKeyOutput = aggregate->outputColumns()[i];
      finalExpressions.push_back(
          nullUnlessLive(groupingKeyOutput, groupingKeyOutput->value().type));
    }
    for (size_t i = 0; i < wraps.size(); ++i) {
      finalExpressions.push_back(nullUnlessLive(
          wraps[i].finalExpression,
          aggregate->outputColumns()[numGroupingKeys + i]->value().type));
    }
    finalExpressions.push_back(includeMarkerValue);
    return builder().make<Project>({
        child,
        std::move(finalExpressions),
        node->outputColumns(),
    });
  }

  // Builds the terminus shape from a fully-peeled Apply.
  NodeCP terminus(ApplyCP apply, NodeCP input, NodeCP body, ExprVector filter) {
    switch (apply->kind()) {
      case velox::core::JoinType::kInner:
        return terminusInner(apply, input, body, filter);
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
        {body, std::move(exprs), std::move(outputColumns)});
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
      return builder().make<Join>({
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
    NodeCP taggedInput = builder().make<AssignUniqueId>({input, idColumn});

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
    NodeCP join = builder().make<Join>({
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
    return builder().make<Project>({
        enforced,
        std::move(finalExprs),
        apply->outputColumns(),
    });
  }

  NodeCP terminusInner(
      ApplyCP apply,
      NodeCP input,
      NodeCP body,
      const ExprVector& filter) {
    // A body that is one empty row contributes nothing to join, so the
    // Apply is its input.
    if (filter.empty() && isSingleEmptyRowValues(body)) {
      return input;
    }

    // Plain INNER JOIN: no cardinality assertion, no include marker. Body and
    // input columns are disjoint (correlation columns live on the Apply, not
    // in body output), so the output is input ++ body with no collision.
    JoinCondition::Split split = JoinCondition::splitEquiKeys(
        filter,
        PlanObjectSet::fromObjects(input->outputColumns()),
        PlanObjectSet::fromObjects(body->outputColumns()));
    return builder().make<Join>({
        input,
        body,
        velox::core::JoinType::kInner,
        std::move(split.leftKeys),
        std::move(split.rightKeys),
        std::move(split.residual),
        /*nullAware=*/false,
        /*nullAsValue=*/false,
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

    NodeCP newApply = builder().make<Apply>({
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
      return builder().make<Project>({
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

      NodeCP agg = builder().make<Aggregate>({
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
      return builder().make<Project>({
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

    auto wraps = buildAggregateWraps(
        aggregate, numGroupingKeys, /*coalesceEmptyInput=*/false);

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

    NodeCP stage1 = builder().make<Aggregate>({
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

      NodeCP stage15 = builder().make<Project>({
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

      NodeCP stage2 = builder().make<Aggregate>({
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

    return builder().make<Project>({
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

    auto wraps = buildAggregateWraps(
        aggregate, numGroupingKeys, /*coalesceEmptyInput=*/false);

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

    NodeCP stage1 = builder().make<Aggregate>({
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

      NodeCP stage15 = builder().make<Project>({
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

      NodeCP stage2 = builder().make<Aggregate>({
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

    return builder().make<Project>({
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
    bool bodyKeyHasOuter = false;
    if (apply->inLhs() != nullptr) {
      // Equi-keys must each resolve on a single join side. When the
      // body key references outer columns, demote the IN equality
      // into the join filter; `nullAware=true` preserves null-aware
      // semi-project semantics.
      bodyKeyHasOuter =
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
    //
    // A correlation repeating the IN equality makes the IN an EXISTS: the body
    // holds only rows equal to the left side, so neither the repeated
    // predicate nor a null-aware key is needed.
    bool repeatsInEquality = false;
    if (apply->nullAware()) {
      for (size_t i = 0; i < split.leftKeys.size(); ++i) {
        if (!bodyKeyHasOuter && split.leftKeys[i] == apply->inLhs() &&
            split.rightKeys[i] == apply->inBodyKey()) {
          repeatsInEquality = true;
          continue;
        }
        split.residual.push_back(
            exprFactory_.makeEq(split.leftKeys[i], split.rightKeys[i]));
      }
    } else {
      appendAll(leftKeys, split.leftKeys);
      appendAll(rightKeys, split.rightKeys);
    }
    return builder().make<Join>({
        input,
        body,
        velox::core::JoinType::kLeftSemiProject,
        std::move(leftKeys),
        std::move(rightKeys),
        std::move(split.residual),
        apply->nullAware() && !repeatsInEquality,
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
    return builder().make<EnforceDistinct>({
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
