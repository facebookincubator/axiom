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

#include "axiom/optimizer/v2/PrecomputeProjections.h"

#include <folly/container/F14Map.h>
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/NodeRewriter.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

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
  // Physical planning materializes every position an Aggregate reads, so there
  // is nothing to lift here. A kFinal aggregate is exempt: its args name the
  // partial's raw inputs, which its own input does not produce, and emit reads
  // only their types and any lambda among them.
  for (ExprCP key : aggregate->groupingKeys()) {
    VELOX_CHECK(
        key->is(PlanType::kColumnExpr),
        "Aggregate grouping key is not a column: {}",
        key->toString());
  }
  if (aggregate->step() != AggregateStep::kFinal) {
    for (const auto* call : aggregate->aggregates()) {
      for (ExprCP arg : call->args()) {
        VELOX_CHECK(
            arg->is(PlanType::kColumnExpr) || arg->is(PlanType::kLiteralExpr) ||
                arg->is(PlanType::kLambdaExpr),
            "Aggregate argument is not a column: {}",
            arg->toString());
      }
      VELOX_CHECK(
          call->condition() == nullptr ||
              call->condition()->is(PlanType::kColumnExpr),
          "Aggregate FILTER is not a column");
      for (ExprCP key : call->orderKeys()) {
        VELOX_CHECK(
            key->is(PlanType::kColumnExpr),
            "Aggregate ORDER BY key is not a column: {}",
            key->toString());
      }
    }
  }

  NodeCP newInput = rewrite(aggregate->input(), context);
  if (newInput == aggregate->input()) {
    return aggregate;
  }
  return builder().make<Aggregate>(
      {.input = newInput,
       .groupingKeys = aggregate->groupingKeys(),
       .aggregates = aggregate->aggregates(),
       .outputColumns = aggregate->outputColumns(),
       .step = aggregate->step(),
       .groupId = aggregate->groupId(),
       .globalGroupingSets = aggregate->globalGroupingSets()});
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

  // The single-side parts of a keyless join's filter were moved into the
  // inputs by pushdown, so every conjunct here just needs its columns kept.
  ExprVector newFilter = joinFilter;
  for (ExprCP conjunct : newFilter) {
    conjunct->columns().forEach<Column>(keepPassthrough);
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
    return PrecomputeProjections::makeProject(
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
        PrecomputeProjections::makeProject(
            input, std::move(exprs), projectColumns, builder()));
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

NodeCP PrecomputeProjectionsPass::run(NodeCP node, Builder& builder) {
  return Rewriter{builder}.rewrite(node);
}

} // namespace facebook::axiom::optimizer::v2
