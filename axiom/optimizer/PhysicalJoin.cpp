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
#include "axiom/optimizer/PrecomputeProjections.h"

namespace facebook::axiom::optimizer {

NodeCP PhysicalJoin::makeJoin(
    Join::Key key,
    Builder& builder,
    ExprSimplifier& simplifier) {
  key.sourceColumns = key.effectiveSourceColumns();
  PrecomputeProjections left{
      key.left, builder, simplifier, /*projectAllInputs=*/false};
  PrecomputeProjections right{
      key.right, builder, simplifier, /*projectAllInputs=*/false};

  ExprFactory::ExprSubstitution lifted;
  const auto liftKeys = [&](ExprVector& keys, PrecomputeProjections& side) {
    for (ExprCP& joinKey : keys) {
      ExprCP column = side.toColumn(joinKey);
      if (column != joinKey) {
        lifted.emplace(joinKey, column);
        joinKey = column;
      }
    }
  };
  liftKeys(key.leftKeys, left);
  liftKeys(key.rightKeys, right);

  if (!lifted.empty()) {
    ExprFactory factory{builder};
    for (ExprCP& conjunct : key.filter) {
      conjunct = factory.replace(conjunct, lifted);
    }
  }

  // A column the join outputs or its filter reads passes through the side that
  // produces it. A column the join itself produces, such as a semijoin mark,
  // belongs to neither input.
  const auto leftColumns =
      PlanObjectSet::fromObjects(key.left->outputColumns());
  const auto rightColumns =
      PlanObjectSet::fromObjects(key.right->outputColumns());
  const auto keep = [&](ColumnCP column) {
    if (leftColumns.contains(column)) {
      left.toColumn(column);
    } else if (rightColumns.contains(column)) {
      right.toColumn(column);
    }
  };
  for (ColumnCP column : key.sourceColumns) {
    keep(column);
  }
  for (ExprCP conjunct : key.filter) {
    conjunct->columns().forEach<Column>(keep);
  }

  key.left = std::move(left).node();
  key.right = std::move(right).node();
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
