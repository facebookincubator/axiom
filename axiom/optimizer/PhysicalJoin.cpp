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

#include "axiom/optimizer/PhysicalJoin.h"

#include "axiom/optimizer/ExprFactory.h"
#include "axiom/optimizer/JoinInputRewriter.h"
#include "axiom/optimizer/PrecomputeProjections.h"

namespace facebook::axiom::optimizer {

namespace {

// A projection over a join input. It goes below the input's exchange, if the
// input has one, so it computes each expression once per row before the
// exchange copies or moves the row, and the exchange carries only what the
// join reads. Only the outermost exchange is peeled.
class JoinInputProjection {
 public:
  JoinInputProjection(
      NodeCP input,
      Builder& builder,
      ExprSimplifier& simplifier)
      : input_{input},
        exchange_{
            input->is(NodeType::kExchange) ? input->as<Exchange>() : nullptr},
        belowExchange_{exchange_ != nullptr ? input->onlyInput() : input},
        columns_{PlanObjectSet::fromObjects(belowExchange_->outputColumns())},
        precompute_{
            belowExchange_,
            builder,
            simplifier,
            /*projectAllInputs=*/false} {}

  // Lets a `JoinInputRewriter` move expressions into the projection.
  JoinInputRewriter::Input rewriterInput() {
    return {&precompute_, &columns_};
  }

  // Computes 'expr' in the projection and returns the column holding it.
  ExprCP toColumn(ExprCP expr) {
    return precompute_.toColumn(expr);
  }

  // Adds 'column' to the projection if this input produces it.
  void keep(ColumnCP column) {
    if (columns_.contains(column)) {
      precompute_.toColumn(column);
    }
  }

  // Returns the input with the projection under its exchange. The exchange
  // partitions on the projected columns, which hold the same values as its
  // original keys.
  NodeCP node(Builder& builder) && {
    if (exchange_ == nullptr) {
      return std::move(precompute_).node();
    }
    Partitioning partitioning = exchange_->partitioning();
    for (ExprCP& partitionKey : partitioning.keys) {
      partitionKey = precompute_.toColumn(partitionKey);
    }
    NodeCP projected = std::move(precompute_).node();
    if (projected == belowExchange_) {
      return input_;
    }
    return builder.make<Exchange>(
        {projected, std::move(partitioning), exchange_->placement()});
  }

 private:
  const NodeCP input_;
  const Exchange* const exchange_;
  const NodeCP belowExchange_;
  const PlanObjectSet columns_;
  PrecomputeProjections precompute_;
};

// Moves the parts of 'filter' that read only an input in 'filterInputs' into
// that input's projection and returns the rewritten filter.
ExprVector moveFilterParts(
    const ExprVector& filter,
    Join::Sides filterInputs,
    JoinInputProjection& left,
    JoinInputProjection& right,
    Builder& builder) {
  std::vector<JoinInputRewriter::Input> inputs;
  if (filterInputs.left) {
    inputs.push_back(left.rewriterInput());
  }
  if (filterInputs.right) {
    inputs.push_back(right.rewriterInput());
  }
  if (inputs.empty()) {
    return filter;
  }
  JoinInputRewriter rewriter{
      std::move(inputs), builder, /*onlyNarrowing=*/false};
  return rewriter.rewrite(filter);
}

// Replaces each expression key of 'key' with a column its side computes, and
// rewrites the filter to read that column.
void liftKeys(
    Join::Key& key,
    JoinInputProjection& left,
    JoinInputProjection& right,
    Builder& builder) {
  ExprFactory::ExprSubstitution lifted;
  const auto lift = [&](ExprVector& keys, JoinInputProjection& side) {
    for (ExprCP& joinKey : keys) {
      ExprCP column = side.toColumn(joinKey);
      if (column != joinKey) {
        lifted.emplace(joinKey, column);
        joinKey = column;
      }
    }
  };
  lift(key.leftKeys, left);
  lift(key.rightKeys, right);
  if (lifted.empty()) {
    return;
  }
  ExprFactory factory{builder};
  for (ExprCP& conjunct : key.filter) {
    conjunct = factory.replace(conjunct, lifted);
  }
}

} // namespace

NodeCP PhysicalJoin::makeJoin(
    Join::Key key,
    Join::Sides filterInputs,
    Builder& builder,
    ExprSimplifier& simplifier) {
  key.sourceColumns = key.effectiveSourceColumns();
  JoinInputProjection left{key.left, builder, simplifier};
  JoinInputProjection right{key.right, builder, simplifier};

  liftKeys(key, left, right, builder);

  // A column the join outputs or its filter reads passes through the side that
  // produces it. A column the join itself produces, such as a semijoin mark,
  // belongs to neither input.
  const auto keep = [&](ColumnCP column) {
    left.keep(column);
    right.keep(column);
  };
  for (ColumnCP column : key.sourceColumns) {
    keep(column);
  }

  key.filter = moveFilterParts(key.filter, filterInputs, left, right, builder);
  for (ExprCP conjunct : key.filter) {
    conjunct->columns().forEach<Column>(keep);
  }

  key.left = std::move(left).node(builder);
  key.right = std::move(right).node(builder);
  key.buildIsLoopInvariant = Join::canBuildLoopInvariant(
      key.joinType, key.rightKeys, key.nullAsValue, key.left, key.right);
  return builder.make<Join>(std::move(key));
}

NodeCP PhysicalJoin::makeUnnest(
    Unnest::Key key,
    Builder& builder,
    ExprSimplifier& simplifier) {
  PrecomputeProjections precompute{
      key.input, builder, simplifier, /*projectAllInputs=*/false};
  // Replicated columns first, so the projection preserves input column order.
  for (ColumnCP column : key.replicatedColumns) {
    precompute.toColumn(column);
  }
  for (ExprCP& unnestExpression : key.unnestExpressions) {
    unnestExpression = precompute.toColumn(unnestExpression);
  }
  key.input = std::move(precompute).node();
  return builder.make<Unnest>(std::move(key));
}

} // namespace facebook::axiom::optimizer
