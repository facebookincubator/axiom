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
#include "axiom/optimizer/v2/NodeRewriter.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

// Per-consumer builder that lifts compound sub-expressions into a Project
// inserted between the consumer and its existing input.
class PrecomputeProjections {
 public:
  // When `projectAllInputs` is true (the default, for pass-through consumers
  // like Window/Sort/Exchange), the project preserves every input column
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
  // A compound expression materializes into a column named after its id, so the
  // same expression always lifts to the same name. If a column of that name is
  // already in the input — e.g. an exchange below this consumer lifted the same
  // join/grouping key — reuse it instead of projecting a second column with the
  // same name (which would be a duplicate output column).
  const auto name = toName(fmt::format("__p{}", expr->id()));
  for (ColumnCP column : input_->outputColumns()) {
    if (column->name() == name) {
      seen_.emplace(expr, column);
      return column;
    }
  }
  ColumnCP column = make<Column>(name, nullptr, expr->value());
  addToProject(expr, column);
  needsProject_ = true;
  return column;
}

NodeCP PrecomputeProjections::node() && {
  if (!needsProject_) {
    return input_;
  }
  return builder_.make<Project>(
      {input_, std::move(outExprs_), std::move(outColumns_)});
}

void PrecomputeProjections::addToProject(ExprCP expr, ColumnCP column) {
  VELOX_DCHECK(!seen_.contains(expr));
  seen_.emplace(expr, column);
  outColumns_.emplace_back(column);
  outExprs_.emplace_back(expr);
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

// Lifts compound expressions at restricted positions of Aggregate /
// Window / Sort / Unnest into a Project below the consumer.
class Rewriter : public NodeRewriter<> {
 public:
  using NodeRewriter::NodeRewriter;

 protected:
  NodeCP rewriteAggregate(const Aggregate* aggregate, NoContext& context)
      override;
  NodeCP rewriteWindow(const Window* window, NoContext& context) override;
  NodeCP rewriteSort(const Sort* sort, NoContext& context) override;
  NodeCP rewriteUnnest(const Unnest* unnest, NoContext& context) override;
  NodeCP rewriteJoin(const Join* join, NoContext& context) override;
  NodeCP rewriteExchange(const Exchange* exchange, NoContext& context) override;
  NodeCP rewriteApply(const Apply* /*apply*/, NoContext& /*context*/) override {
    VELOX_UNREACHABLE(
        "Apply must be removed by decorrelate before PrecomputeProjections");
  }
};

NodeCP Rewriter::rewriteAggregate(
    const Aggregate* aggregate,
    NoContext& context) {
  NodeCP newInput = rewrite(aggregate->input(), context);
  // An Aggregate reads only its grouping keys and aggregate inputs, so the
  // lifting project outputs just those — dropping any input column kept solely
  // to feed a lifted aggregate expression.
  PrecomputeProjections precompute{
      newInput, builder(), /*projectAllInputs=*/false};

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
      newAggregates.push_back(
          builder().makeAggregate(
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

  return builder().make<Aggregate>(Aggregate::Key{
      .input = std::move(precompute).node(),
      .groupingKeys = std::move(newGroupingKeys),
      .aggregates = std::move(newAggregates),
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

  // Frame bounds must be FieldAccess or Constant in Velox.
  auto liftBound = [&](ExprCP value) -> ExprCP {
    return value != nullptr
        ? precompute.toColumn(value, /*alias=*/nullptr, /*allowConstant=*/true)
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
    Frame newFrame{
        frame.type,
        frame.startType,
        liftBound(frame.startValue),
        frame.endType,
        liftBound(frame.endValue),
    };
    newFunctions.push_back({newCall, newFrame, windowFunction.ignoreNulls});
  }

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

NodeCP Rewriter::rewriteExchange(const Exchange* exchange, NoContext& context) {
  NodeCP newInput = rewrite(exchange->input(), context);
  const Partitioning& partitioning = exchange->partitioning();
  // Only hash partitioning carries keys, and emit requires them to be column
  // references; lift any compound key expression into a projected column.
  if (partitioning.kind != PartitionKind::kPartitioned) {
    if (newInput == exchange->input()) {
      return exchange;
    }
    return builder().make<Exchange>({newInput, partitioning});
  }
  PrecomputeProjections precompute{newInput, builder()};
  ExprVector newKeys;
  newKeys.reserve(partitioning.keys.size());
  for (ExprCP key : partitioning.keys) {
    newKeys.push_back(precompute.toColumn(key));
  }
  Partitioning newPartitioning = partitioning;
  newPartitioning.keys = std::move(newKeys);
  return builder().make<Exchange>(
      {std::move(precompute).node(), std::move(newPartitioning)});
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

  // Keep each side's passthrough columns: those it contributes to the join
  // output or references in the join filter. A column produced by the join
  // itself (e.g. a semijoin mark) belongs to neither input and is skipped.
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
  for (ExprCP conjunct : join->filter()) {
    conjunct->columns().forEach<Column>(keepPassthrough);
  }

  return builder().make<Join>(
      {std::move(leftPrecompute).node(),
       std::move(rightPrecompute).node(),
       join->joinType(),
       std::move(newLeftKeys),
       std::move(newRightKeys),
       join->filter(),
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
  ExprVector newUnnestExprs;
  newUnnestExprs.reserve(unnest->unnestExpressions().size());
  for (ExprCP expr : unnest->unnestExpressions()) {
    newUnnestExprs.push_back(precompute.toColumn(expr));
  }
  // Keep the replicated (passthrough) columns in the project's output.
  for (ColumnCP column : unnest->replicatedColumns()) {
    precompute.toColumn(column);
  }
  newInput = std::move(precompute).node();
  // Structured fields (replicatedColumns / unnestColumns / ordinalityColumn)
  // are by Column*; precompute preserves Column identity so they stay valid.
  return builder().make<Unnest>(
      {newInput,
       std::move(newUnnestExprs),
       unnest->replicatedColumns(),
       unnest->unnestColumns(),
       unnest->ordinalityColumn(),
       unnest->outputColumns()});
}

} // namespace

NodeCP PrecomputeProjectionsPass::run(NodeCP node, Builder& builder) {
  return Rewriter{builder}.rewrite(node);
}

} // namespace facebook::axiom::optimizer::v2
