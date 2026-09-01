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

#include "axiom/optimizer/v2/PrecomputeProjectionsPass.h"

#include <folly/container/F14Map.h>
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/NodeRewriter.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

// Builds `Project(exprs -> outColumns)` over `input`, folding into `input` when
// it is a deterministic Project (substituting the expressions through the
// child's output->expression map) rather than stacking a second Project. A
// non-deterministic child is left alone, since a fold could evaluate one of its
// outputs more than once.
//
// TODO: still inline the deterministic outputs when only some are
// non-deterministic — isolate the non-deterministic ones in a separate Project
// below and fold the rest.
NodeCP makeProject(
    NodeCP input,
    ExprVector exprs,
    ColumnVector outColumns,
    Builder& builder) {
  if (input->is(NodeType::kProject)) {
    const auto* child = input->as<Project>();
    if (child->isDeterministic()) {
      exprs = ExprFactory(builder).substitute(
          exprs, child->outputColumns(), child->exprs());
      input = child->input();
    }
  }
  return builder.make<Project>(
      {input, std::move(exprs), std::move(outColumns)});
}

// Returns a new ColumnVector with the first `oldPrefixLength` elements
// of `oldOutputColumns` replaced by `newPrefix`.
ColumnVector replacePrefix(
    const ColumnVector& oldOutputColumns,
    size_t oldPrefixLength,
    const ColumnVector& newPrefix) {
  VELOX_DCHECK_LE(oldPrefixLength, oldOutputColumns.size());
  ColumnVector result;
  result.reserve(newPrefix.size() + oldOutputColumns.size() - oldPrefixLength);
  for (ColumnCP column : newPrefix) {
    result.push_back(column);
  }
  for (size_t i = oldPrefixLength; i < oldOutputColumns.size(); ++i) {
    result.push_back(oldOutputColumns[i]);
  }
  return result;
}

// Per-consumer builder that lifts compound sub-expressions into a Project
// inserted between the consumer and its existing input.
class PrecomputeProjections {
 public:
  // When `projectAllInputs` is true (the default, for pass-through consumers
  // like Window/Sort/TopN), the project preserves every input column
  // alongside the lifted ones. When false (for narrowing consumers like
  // Aggregate/Unnest/Join), the project outputs only the columns passed to
  // `toColumn` — so an input column kept solely to feed a lifted expression is
  // dropped instead of passed through. The caller must `toColumn` every column
  // the consumer reads.
  PrecomputeProjections(
      NodeCP input,
      Builder& builder,
      bool projectAllInputs = true);

  // Returns the ExprCP that the consumer should reference in place of
  // 'expr'. Pass-throughs:
  //   - 'expr' is a Column: returned unchanged.
  //   - 'expr' is a Literal and 'allowConstant' is true: returned unchanged.
  // Otherwise lifts 'expr' into a projected column. If 'alias' is
  // non-null, that exact Column is used as the projection's output;
  // otherwise a fresh `__pXX` column is synthesized.
  ExprCP
  toColumn(ExprCP expr, ColumnCP alias = nullptr, bool allowConstant = false);

  // Returns the input unchanged if no projections were added, otherwise the
  // input wrapped in a fresh `Project`. With `projectAllInputs` the project
  // adds the lifted columns alongside all input columns; without it the
  // project outputs only the columns passed to `toColumn`.
  NodeCP node() &&;

 private:
  void addToProject(ExprCP expr, ColumnCP column);

  NodeCP input_;
  Builder& builder_;
  const bool projectAllInputs_;
  ColumnVector outColumns_;
  ExprVector outExprs_;
  folly::F14FastMap<ExprCP, ColumnCP> seen_;
  bool needsProject_{false};
};

PrecomputeProjections::PrecomputeProjections(
    NodeCP input,
    Builder& builder,
    bool projectAllInputs)
    : input_(input), builder_(builder), projectAllInputs_(projectAllInputs) {
  if (!projectAllInputs_) {
    return;
  }
  const auto& inputColumns = input->outputColumns();
  outColumns_.reserve(inputColumns.size());
  outExprs_.reserve(inputColumns.size());
  for (ColumnCP column : inputColumns) {
    addToProject(column, column);
  }
}

ExprCP PrecomputeProjections::toColumn(
    ExprCP expr,
    ColumnCP alias,
    bool allowConstant) {
  if (allowConstant && expr->is(PlanType::kLiteralExpr)) {
    return expr;
  }

  if (expr->is(PlanType::kColumnExpr)) {
    // In narrowing mode the project is not seeded with the input columns, so a
    // referenced passthrough column must be added explicitly. This is not a
    // lifted expression, so it does not by itself require a project.
    if (!projectAllInputs_ && !seen_.contains(expr)) {
      addToProject(expr, expr->as<Column>());
    }
    return expr;
  }

  // Lambdas are consumed by their parent higher-order function directly
  // and cannot be evaluated by a Project node.
  if (expr->is(PlanType::kLambdaExpr)) {
    return expr;
  }

  if (auto it = seen_.find(expr); it != seen_.end()) {
    return it->second;
  }

  if (alias != nullptr) {
    addToProject(expr, alias);
    needsProject_ = true;
    return alias;
  }

  ColumnCP column = Column::create("__p", expr->value());
  addToProject(expr, column);
  needsProject_ = true;
  return column;
}

NodeCP PrecomputeProjections::node() && {
  if (!needsProject_) {
    return input_;
  }
  return makeProject(
      input_, std::move(outExprs_), std::move(outColumns_), builder_);
}

void PrecomputeProjections::addToProject(ExprCP expr, ColumnCP column) {
  VELOX_DCHECK(!seen_.contains(expr));
  seen_.emplace(expr, column);
  outColumns_.emplace_back(column);
  outExprs_.emplace_back(expr);
}

// Moves the single-side parts of a join filter into the inputs. See
// PrecomputeProjectionsPass for why and for the effect on error masking.
//
// A subexpression moves when it reads at least one column, reads no column from
// the other side, and is deterministic. The walk moves the highest such node
// and does not descend into it, so a subexpression moves as a whole rather than
// piecewise.
class JoinFilterRewriter {
 public:
  JoinFilterRewriter(
      PrecomputeProjections& leftPrecompute,
      PrecomputeProjections& rightPrecompute,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      Builder& builder)
      : leftPrecompute_{leftPrecompute},
        rightPrecompute_{rightPrecompute},
        leftColumns_{leftColumns},
        rightColumns_{rightColumns},
        builder_{builder} {}

  // Returns 'filter' with each maximal single-side subexpression replaced by
  // the column it was moved to. Every column the result still reads is added to
  // its side's projection, so the caller need not keep filter columns alive
  // separately.
  ExprVector rewrite(const ExprVector& filter);

 private:
  ExprCP rewrite(ExprCP expr);
  ExprCP rewriteCall(const Call* call);
  ExprCP rewriteField(const Field* field);

  // Computes 'expr' in the input that supplies all of its columns and returns
  // the column it became there. Returns nullptr, leaving 'expr' in the filter,
  // when no single input supplies its columns or 'expr' is non-deterministic.
  ExprCP tryPrecompute(ExprCP expr);

  // Adds 'column' to the projection of the input that produces it.
  void keep(ColumnCP column);

  PrecomputeProjections& leftPrecompute_;
  PrecomputeProjections& rightPrecompute_;
  const PlanObjectSet& leftColumns_;
  const PlanObjectSet& rightColumns_;
  Builder& builder_;
};

ExprVector JoinFilterRewriter::rewrite(const ExprVector& filter) {
  ExprVector result;
  result.reserve(filter.size());
  for (ExprCP conjunct : filter) {
    result.push_back(rewrite(conjunct));
  }
  return result;
}

ExprCP JoinFilterRewriter::rewrite(ExprCP expr) {
  switch (expr->type()) {
    case PlanType::kCallExpr:
      return rewriteCall(expr->as<Call>());
    case PlanType::kFieldExpr:
      return rewriteField(expr->as<Field>());
    case PlanType::kColumnExpr:
      keep(expr->as<Column>());
      return expr;
    case PlanType::kLiteralExpr:
      // No columns to keep and nothing to compute.
      return expr;
    default:
      VELOX_UNREACHABLE(
          "Unexpected expression in a join filter: {}", expr->toString());
  }
}

ExprCP JoinFilterRewriter::rewriteCall(const Call* call) {
  if (ExprCP column = tryPrecompute(call)) {
    return column;
  }

  ExprVector newArgs;
  newArgs.reserve(call->args().size());
  bool changed = false;
  for (ExprCP arg : call->args()) {
    if (arg->is(PlanType::kLambdaExpr)) {
      // A Project cannot evaluate a Lambda on its own, so it stays with the
      // call that binds its arguments. Its columns are the outer ones the body
      // reads -- `Lambda` excludes the bound arguments from that set -- and
      // those still have to reach the join.
      arg->columns().forEach<Column>([&](ColumnCP column) { keep(column); });
      newArgs.push_back(arg);
      continue;
    }
    ExprCP newArg = rewrite(arg);
    changed |= newArg != arg;
    newArgs.push_back(newArg);
  }
  if (!changed) {
    return call;
  }
  return ExprFactory(builder_).rebuildCall(call, std::move(newArgs));
}

ExprCP JoinFilterRewriter::rewriteField(const Field* field) {
  if (ExprCP column = tryPrecompute(field)) {
    return column;
  }
  ExprCP newBase = rewrite(field->base());
  if (newBase == field->base()) {
    return field;
  }
  return ExprFactory(builder_).rebuildField(field, newBase);
}

ExprCP JoinFilterRewriter::tryPrecompute(ExprCP expr) {
  // A non-deterministic expression has to produce a new value per pair.
  if (expr->containsNonDeterministic()) {
    return nullptr;
  }
  // A constant is the same for every row; projecting it would only add a
  // column.
  if (expr->columns().empty()) {
    return nullptr;
  }
  if (leftColumns_.containsColumns(expr)) {
    return leftPrecompute_.toColumn(expr);
  }
  if (rightColumns_.containsColumns(expr)) {
    return rightPrecompute_.toColumn(expr);
  }
  // 'expr' reads both inputs.
  return nullptr;
}

void JoinFilterRewriter::keep(ColumnCP column) {
  if (leftColumns_.contains(column)) {
    leftPrecompute_.toColumn(column);
  } else {
    VELOX_CHECK(
        rightColumns_.contains(column),
        "Join filter reads a column neither input produces: {}",
        column->toString());
    rightPrecompute_.toColumn(column);
  }
}

// Lifts compound expressions at restricted operator positions into a Project
// below the consumer.
class Rewriter : public NodeRewriter<> {
 public:
  using NodeRewriter::NodeRewriter;

 protected:
  NodeCP rewriteAggregate(const Aggregate* aggregate, NoContext& context)
      override;
  NodeCP rewriteWindow(const Window* window, NoContext& context) override;
  NodeCP rewriteRowNumber(const RowNumber* rowNumber, NoContext& context)
      override;
  NodeCP rewriteTopNRowNumber(const TopNRowNumber* topN, NoContext& context)
      override;
  NodeCP rewriteSort(const Sort* sort, NoContext& context) override;
  NodeCP rewriteTopN(const TopN* topN, NoContext& context) override;
  NodeCP rewriteUnnest(const Unnest* unnest, NoContext& context) override;
  NodeCP rewriteJoin(const Join* join, NoContext& context) override;
  NodeCP rewriteUnionAll(const UnionAll* unionAll, NoContext& context) override;
  NodeCP rewriteFixedPoint(const FixedPoint* fixedPoint, NoContext& context)
      override;
  NodeCP rewriteApply(const Apply* /*apply*/, NoContext& /*context*/) override {
    VELOX_UNREACHABLE(
        "Apply must be removed by decorrelate before PrecomputeProjections");
  }
};

NodeCP Rewriter::rewriteAggregate(
    const Aggregate* aggregate,
    NoContext& context) {
  return PrecomputeProjectionsPass::prepareAggregateInputs(
      aggregate, rewrite(aggregate->input(), context), builder());
}

NodeCP Rewriter::rewriteWindow(const Window* window, NoContext& context) {
  NodeCP newInput = rewrite(window->input(), context);
  PrecomputeProjections precompute{newInput, builder()};

  ExprVector newPartitionKeys;
  newPartitionKeys.reserve(window->partitionKeys().size());
  for (ExprCP key : window->partitionKeys()) {
    newPartitionKeys.push_back(precompute.toColumn(key));
  }
  ExprVector newOrderKeys;
  newOrderKeys.reserve(window->orderKeys().size());
  for (ExprCP key : window->orderKeys()) {
    newOrderKeys.push_back(precompute.toColumn(key));
  }

  // A ROWS bound is an offset in rows, which Velox reads as a constant. A
  // RANGE bound is the boundary value for each row, which Velox reads from a
  // column, so it stays a column even when it folds to a literal — as it does
  // when the ORDER BY key is constant.
  auto liftBound = [&](ExprCP value, bool allowConstant) -> ExprCP {
    return value != nullptr
        ? precompute.toColumn(value, /*alias=*/nullptr, allowConstant)
        : nullptr;
  };

  WindowFunctions newFunctions;
  newFunctions.reserve(window->functions().size());
  for (const WindowFunction& windowFunction : window->functions()) {
    const auto* call = windowFunction.call->as<Call>();
    ExprVector newArgs;
    newArgs.reserve(call->args().size());
    for (ExprCP arg : call->args()) {
      newArgs.push_back(
          precompute.toColumn(arg, /*alias=*/nullptr, /*allowConstant=*/true));
    }
    auto* newCall = builder().makeCall(
        call->name(), call->value(), std::move(newArgs), call->functions());
    const Frame& frame = windowFunction.frame;
    const bool allowConstant =
        frame.type == logical_plan::WindowExpr::WindowType::kRows;
    Frame newFrame{
        frame.type,
        frame.startType,
        liftBound(frame.startValue, allowConstant),
        frame.endType,
        liftBound(frame.endValue, allowConstant),
    };
    newFunctions.push_back({newCall, newFrame, windowFunction.ignoreNulls});
  }

  // A Window emits its input's columns followed by its function results, so
  // materializing a frame bound or an order key extends its output too.
  const size_t oldPrefixLength = window->input()->outputColumns().size();
  newInput = std::move(precompute).node();
  ColumnVector newOutputColumns = replacePrefix(
      window->outputColumns(), oldPrefixLength, newInput->outputColumns());
  return builder().make<Window>(
      {newInput,
       std::move(newFunctions),
       std::move(newPartitionKeys),
       std::move(newOrderKeys),
       window->orderTypes(),
       std::move(newOutputColumns)});
}

NodeCP Rewriter::rewriteRowNumber(
    const RowNumber* rowNumber,
    NoContext& context) {
  NodeCP newInput = rewrite(rowNumber->input(), context);
  PrecomputeProjections precompute{newInput, builder()};

  ExprVector newPartitionKeys;
  newPartitionKeys.reserve(rowNumber->partitionKeys().size());
  for (ExprCP key : rowNumber->partitionKeys()) {
    newPartitionKeys.push_back(precompute.toColumn(key));
  }

  // A RowNumber emits its input's columns followed by the row number, so
  // materializing a key extends its output too.
  const size_t oldPrefixLength = rowNumber->input()->outputColumns().size();
  newInput = std::move(precompute).node();
  ColumnVector newOutputColumns = replacePrefix(
      rowNumber->outputColumns(), oldPrefixLength, newInput->outputColumns());
  return builder().make<RowNumber>(
      {newInput,
       std::move(newPartitionKeys),
       rowNumber->limit(),
       rowNumber->rankColumn(),
       std::move(newOutputColumns)});
}

NodeCP Rewriter::rewriteTopNRowNumber(
    const TopNRowNumber* topN,
    NoContext& context) {
  NodeCP newInput = rewrite(topN->input(), context);
  PrecomputeProjections precompute{newInput, builder()};

  ExprVector newPartitionKeys;
  newPartitionKeys.reserve(topN->partitionKeys().size());
  for (ExprCP key : topN->partitionKeys()) {
    newPartitionKeys.push_back(precompute.toColumn(key));
  }
  ExprVector newOrderKeys;
  newOrderKeys.reserve(topN->orderKeys().size());
  for (ExprCP key : topN->orderKeys()) {
    newOrderKeys.push_back(precompute.toColumn(key));
  }

  const size_t oldPrefixLength = topN->input()->outputColumns().size();
  newInput = std::move(precompute).node();
  ColumnVector newOutputColumns = replacePrefix(
      topN->outputColumns(), oldPrefixLength, newInput->outputColumns());
  return builder().make<TopNRowNumber>(
      {newInput,
       topN->rankFunction(),
       std::move(newPartitionKeys),
       std::move(newOrderKeys),
       topN->orderTypes(),
       topN->limit(),
       topN->rankColumn(),
       std::move(newOutputColumns)});
}

NodeCP Rewriter::rewriteSort(const Sort* sort, NoContext& context) {
  NodeCP newInput = rewrite(sort->input(), context);
  PrecomputeProjections precompute{newInput, builder()};
  ExprVector newOrderKeys;
  newOrderKeys.reserve(sort->orderKeys().size());
  for (ExprCP key : sort->orderKeys()) {
    newOrderKeys.push_back(precompute.toColumn(key));
  }
  return builder().make<Sort>(
      {std::move(precompute).node(),
       std::move(newOrderKeys),
       sort->orderTypes()});
}

NodeCP Rewriter::rewriteTopN(const TopN* topN, NoContext& context) {
  NodeCP newInput = rewrite(topN->input(), context);
  PrecomputeProjections precompute{newInput, builder()};
  ExprVector newOrderKeys;
  newOrderKeys.reserve(topN->orderKeys().size());
  for (ExprCP key : topN->orderKeys()) {
    newOrderKeys.push_back(precompute.toColumn(key));
  }
  return builder().make<TopN>(
      {std::move(precompute).node(),
       std::move(newOrderKeys),
       topN->orderTypes(),
       topN->offset(),
       topN->count()});
}

NodeCP Rewriter::rewriteJoin(const Join* join, NoContext& context) {
  NodeCP newLeft = rewrite(join->left(), context);
  NodeCP newRight = rewrite(join->right(), context);

  // A join reads only its keys, filter, and the columns it outputs, so each
  // side's lifting project outputs just those — dropping any input column kept
  // solely to feed a lifted join key.
  PrecomputeProjections leftPrecompute{
      newLeft, builder(), /*projectAllInputs=*/false};
  PrecomputeProjections rightPrecompute{
      newRight, builder(), /*projectAllInputs=*/false};
  ExprVector newLeftKeys;
  newLeftKeys.reserve(join->leftKeys().size());
  for (ExprCP key : join->leftKeys()) {
    newLeftKeys.push_back(leftPrecompute.toColumn(key));
  }
  ExprVector newRightKeys;
  newRightKeys.reserve(join->rightKeys().size());
  for (ExprCP key : join->rightKeys()) {
    newRightKeys.push_back(rightPrecompute.toColumn(key));
  }

  // A key that was lifted is now computed by the input, so the filter must
  // read that column rather than compute the same expression per pair.
  ExprFactory::ExprSubstitution lifted;
  const auto recordLifted = [&](const ExprVector& keys,
                                const ExprVector& newKeys) {
    for (size_t i = 0; i < keys.size(); ++i) {
      if (newKeys[i] != keys[i]) {
        lifted.emplace(keys[i], newKeys[i]);
      }
    }
  };
  recordLifted(join->leftKeys(), newLeftKeys);
  recordLifted(join->rightKeys(), newRightKeys);
  ExprFactory factory{builder()};
  ExprVector joinFilter;
  joinFilter.reserve(join->filter().size());
  for (ExprCP conjunct : join->filter()) {
    joinFilter.push_back(factory.replace(conjunct, lifted));
  }

  // Keep each side's passthrough columns: those it contributes to the join
  // output. A column produced by the join itself (e.g. a semijoin mark) belongs
  // to neither input and is skipped.
  const auto leftColumns = PlanObjectSet::fromObjects(newLeft->outputColumns());
  const auto rightColumns =
      PlanObjectSet::fromObjects(newRight->outputColumns());
  auto keepPassthrough = [&](ColumnCP column) {
    if (leftColumns.contains(column)) {
      leftPrecompute.toColumn(column);
    } else if (rightColumns.contains(column)) {
      rightPrecompute.toColumn(column);
    }
  };
  for (ColumnCP column : join->outputColumns()) {
    keepPassthrough(column);
  }

  ExprVector newFilter;
  if (newLeftKeys.empty()) {
    JoinFilterRewriter rewriter{
        leftPrecompute, rightPrecompute, leftColumns, rightColumns, builder()};
    newFilter = rewriter.rewrite(joinFilter);
  } else {
    newFilter = joinFilter;
    for (ExprCP conjunct : newFilter) {
      conjunct->columns().forEach<Column>(keepPassthrough);
    }
  }

  return builder().make<Join>(
      {std::move(leftPrecompute).node(),
       std::move(rightPrecompute).node(),
       join->joinType(),
       std::move(newLeftKeys),
       std::move(newRightKeys),
       std::move(newFilter),
       join->nullAware(),
       join->nullAsValue(),
       join->outputColumns()});
}

NodeCP Rewriter::rewriteUnnest(const Unnest* unnest, NoContext& context) {
  NodeCP newInput = rewrite(unnest->input(), context);
  // An Unnest reads only its unnest expressions and the columns it replicates,
  // so the lifting project outputs just those — dropping any input column kept
  // solely to feed a lifted unnest expression.
  PrecomputeProjections precompute{
      newInput, builder(), /*projectAllInputs=*/false};
  // Keep the replicated (passthrough) columns first, before the lifted unnest
  // expressions, so the project preserves input column order.
  for (ColumnCP column : unnest->replicatedColumns()) {
    precompute.toColumn(column);
  }
  ExprVector newUnnestExprs;
  newUnnestExprs.reserve(unnest->unnestExpressions().size());
  for (ExprCP expr : unnest->unnestExpressions()) {
    newUnnestExprs.push_back(precompute.toColumn(expr));
  }
  newInput = std::move(precompute).node();
  // Structured fields (replicatedColumns / unnestColumns / ordinalityColumn /
  // markerColumn) are by Column*; precompute preserves Column identity so they
  // stay valid.
  return builder().make<Unnest>(
      {newInput,
       std::move(newUnnestExprs),
       unnest->replicatedColumns(),
       unnest->unnestColumns(),
       unnest->ordinalityColumn(),
       unnest->markerColumn(),
       unnest->outputColumns()});
}

NodeCP Rewriter::rewriteFixedPoint(
    const FixedPoint* fixedPoint,
    NoContext& context) {
  auto restoreSchema = [&](NodeCP branch, const ColumnVector& columns) {
    if (branch->outputColumns() == columns) {
      return branch;
    }
    return makeProject(
        branch, ExprVector{columns.begin(), columns.end()}, columns, builder());
  };

  NodeCP anchor = restoreSchema(
      rewrite(fixedPoint->anchor(), context), fixedPoint->outputColumns());
  NodeCP step = restoreSchema(
      rewrite(fixedPoint->step(), context),
      fixedPoint->step()->outputColumns());
  NodeCP convergence = restoreSchema(
      rewrite(fixedPoint->convergence(), context),
      fixedPoint->convergence()->outputColumns());
  if (anchor == fixedPoint->anchor() && step == fixedPoint->step() &&
      convergence == fixedPoint->convergence()) {
    return fixedPoint;
  }
  return builder().make<FixedPoint>({
      .anchor = anchor,
      .step = step,
      .convergence = convergence,
      .name = fixedPoint->name(),
      .outputColumns = fixedPoint->outputColumns(),
      .maxIterations = fixedPoint->maxIterations(),
      .recursiveNumDrivers = fixedPoint->recursiveNumDrivers(),
  });
}

NodeCP Rewriter::rewriteUnionAll(const UnionAll* unionAll, NoContext& context) {
  // Velox's LocalPartition requires every source to share one output RowType,
  // so each leg must produce the union's output columns (same names, same
  // order). Emit that aligning projection here -- like the pre-projections for
  // aggregates/joins -- rather than at emit, folding it into a deterministic
  // child Project so a coercion leg (e.g. VALUES needing a cast) does not stack
  // two Projects. For a leg already isolated behind a remote exchange, the
  // rename lands on the exchange's consumer side; moving it below the exchange
  // is future work.
  const ColumnVector& outputColumns = unionAll->outputColumns();
  NodeVector newInputs;
  newInputs.reserve(unionAll->inputs().size());
  QGVector<ColumnVector> newLegColumns;
  newLegColumns.reserve(unionAll->inputs().size());
  bool changed = false;
  for (size_t i = 0; i < unionAll->inputs().size(); ++i) {
    NodeCP input = rewrite(unionAll->inputs()[i], context);
    const ColumnVector& legCols = unionAll->legColumns()[i];

    // Already aligned: the leg produces exactly legCols in order and each
    // carries the union's output name (so its type matches too).
    bool aligned = input->outputColumns().size() == legCols.size();
    for (size_t k = 0; aligned && k < legCols.size(); ++k) {
      aligned = input->outputColumns()[k] == legCols[k] &&
          legCols[k]->outputName() == outputColumns[k]->outputName();
    }
    if (aligned) {
      changed |= input != unionAll->inputs()[i];
      newInputs.push_back(input);
      newLegColumns.push_back(legCols);
      continue;
    }

    // Rename legCols to fresh columns carrying the union's output names.
    ExprVector exprs(legCols.begin(), legCols.end());
    ColumnVector projectColumns;
    projectColumns.reserve(outputColumns.size());
    for (ColumnCP column : outputColumns) {
      projectColumns.push_back(
          Column::createForSymbol(
              toName(column->outputName()), column->value()));
    }
    newInputs.push_back(
        makeProject(input, std::move(exprs), projectColumns, builder()));
    newLegColumns.push_back(std::move(projectColumns));
    changed = true;
  }
  if (!changed) {
    return unionAll;
  }
  return builder().make<UnionAll>(
      {std::move(newInputs), std::move(newLegColumns), outputColumns});
}

} // namespace

AggregateCP PrecomputeProjectionsPass::prepareAggregateInputs(
    const Aggregate* aggregate,
    NodeCP rewrittenInput,
    Builder& builder) {
  // An Aggregate reads only its grouping keys and aggregate inputs, so the
  // lifting project outputs just those — dropping any input column kept solely
  // to feed a lifted aggregate expression.
  PrecomputeProjections precompute{
      rewrittenInput, builder, /*projectAllInputs=*/false};

  ExprVector newGroupingKeys;
  newGroupingKeys.reserve(aggregate->groupingKeys().size());
  for (size_t i = 0; i < aggregate->groupingKeys().size(); ++i) {
    // Reuse the existing output column as the projection alias so the
    // Aggregate's outputColumns identity is preserved.
    newGroupingKeys.push_back(precompute.toColumn(
        aggregate->groupingKeys()[i], aggregate->outputColumns()[i]));
  }

  // A kFinal aggregate's args reference the Partial's raw inputs, which are
  // absent at the Final's input (it consumes intermediate accumulators), so
  // leave them untouched rather than precompute them here.
  AggregateCallVector newAggregates;
  if (aggregate->step() == AggregateStep::kFinal) {
    newAggregates = aggregate->aggregates();
  } else {
    newAggregates.reserve(aggregate->aggregates().size());
    for (const auto* call : aggregate->aggregates()) {
      ExprVector newArgs;
      newArgs.reserve(call->args().size());
      for (ExprCP arg : call->args()) {
        newArgs.push_back(precompute.toColumn(
            arg, /*alias=*/nullptr, /*allowConstant=*/true));
      }
      ExprCP newCondition = call->condition() != nullptr
          ? precompute.toColumn(
                call->condition(), /*alias=*/nullptr, /*allowConstant=*/true)
          : nullptr;
      ExprVector newOrderKeys;
      newOrderKeys.reserve(call->orderKeys().size());
      for (ExprCP key : call->orderKeys()) {
        newOrderKeys.push_back(precompute.toColumn(key));
      }
      newAggregates.push_back(builder.makeAggregate(
          call->name(),
          call->value(),
          std::move(newArgs),
          call->functions(),
          call->isDistinct(),
          newCondition,
          call->intermediateType(),
          std::move(newOrderKeys),
          call->orderTypes()));
    }
  }

  return builder.make<Aggregate>(Aggregate::Key{
      .input = std::move(precompute).node(),
      .groupingKeys = std::move(newGroupingKeys),
      .aggregates = std::move(newAggregates),
      .outputColumns = aggregate->outputColumns(),
      .step = aggregate->step(),
      .groupId = aggregate->groupId(),
      .globalGroupingSets = aggregate->globalGroupingSets()});
}

NodeCP PrecomputeProjectionsPass::run(NodeCP node, Builder& builder) {
  return Rewriter{builder}.rewrite(node);
}

} // namespace facebook::axiom::optimizer::v2
