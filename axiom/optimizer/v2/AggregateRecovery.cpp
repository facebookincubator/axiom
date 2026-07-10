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

#include "axiom/optimizer/v2/AggregateRecovery.h"

#include "axiom/optimizer/FunctionRegistry.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/type/Type.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

bool isCountStar(optimizer::AggregateCP aggregate) {
  // `COUNT(*)` is the dialect's count aggregate (per FunctionRegistry)
  // invoked with no args. Aggregates with args, or aggregates from a
  // dialect that hasn't registered count, are not the count-bug case.
  const auto& countName = FunctionRegistry::instance()->count();
  return countName.has_value() && aggregate->name() == toName(*countName) &&
      aggregate->args().empty();
}

bool argsAreEntirelyOuter(
    optimizer::AggregateCP aggregate,
    const PlanObjectSet& outerSet) {
  if (aggregate->args().empty()) {
    return false;
  }
  // "Entirely outer" requires at least one referenced column AND every
  // referenced column to be in the outer set. An aggregate whose args
  // are literals only (e.g., `count(1)`) has an empty column set,
  // which would be vacuously a subset of any outer set; we don't want
  // to flag those.
  bool referencesAnyColumn = false;
  for (ExprCP arg : aggregate->args()) {
    if (arg->columns().empty()) {
      continue;
    }
    referencesAnyColumn = true;
    if (!arg->columns().isSubset(outerSet)) {
      return false;
    }
  }
  return referencesAnyColumn;
}

} // namespace

AggregateCallVector AggregateRecovery::rewriteCountStar(
    const AggregateCallVector& aggregates,
    ColumnCP marker) {
  AggregateCallVector rewritten;
  rewritten.reserve(aggregates.size());
  for (const auto* aggregate : aggregates) {
    if (!isCountStar(aggregate)) {
      rewritten.push_back(aggregate);
      continue;
    }
    ExprCP newCondition = aggregate->condition() == nullptr
        ? static_cast<ExprCP>(marker)
        : exprFactory_.makeAnd(aggregate->condition(), marker);
    rewritten.push_back(replaceCondition(aggregate, newCondition));
  }
  return rewritten;
}

AggregateCallVector AggregateRecovery::addFilterCondition(
    const AggregateCallVector& aggregates,
    ExprCP predicate) {
  AggregateCallVector rewritten;
  rewritten.reserve(aggregates.size());
  for (const auto* aggregate : aggregates) {
    ExprCP newCondition = aggregate->condition() == nullptr
        ? predicate
        : exprFactory_.makeAnd(aggregate->condition(), predicate);
    rewritten.push_back(replaceCondition(aggregate, newCondition));
  }
  return rewritten;
}

void AggregateRecovery::validateAggregateArgs(
    const AggregateCallVector& aggregates,
    const ColumnVector& outerCorrelations) {
  PlanObjectSet outerSet = PlanObjectSet::fromObjects(outerCorrelations);
  for (const auto* aggregate : aggregates) {
    VELOX_USER_CHECK(
        !argsAreEntirelyOuter(aggregate, outerSet),
        "Correlated subquery with an aggregate whose arguments reference "
        "only outer columns is not supported yet");
  }
}

optimizer::AggregateCP AggregateRecovery::makeBoolOr(ExprCP arg) {
  VELOX_CHECK_EQ(
      arg->value().type->kind(),
      velox::TypeKind::BOOLEAN,
      "bool_or expects a BOOLEAN argument");

  const auto& boolOrName = FunctionRegistry::instance()->boolOr();
  VELOX_USER_CHECK(
      boolOrName.has_value(),
      "Decorrelate semi recovery requires bool_or registered via "
      "FunctionRegistry::registerBoolOr; the active dialect did not "
      "register it");

  const std::vector<velox::TypePtr> argTypes{velox::BOOLEAN()};
  TypeCP intermediateType =
      toType(velox::exec::resolveIntermediateType(*boolOrName, argTypes));

  const auto& metadata = velox::exec::getAggregateFunctionMetadata(*boolOrName);
  FunctionSet functions = arg->functions();
  if (metadata.ignoreDuplicates) {
    functions = functions | FunctionSet::kIgnoreDuplicatesAggregate;
  }
  if (metadata.orderSensitive) {
    functions = functions | FunctionSet::kOrderSensitiveAggregate;
  }
  return builder_.makeAggregate(
      toName(*boolOrName),
      Value(toType(velox::BOOLEAN()), /*cardinality=*/2),
      ExprVector{arg},
      functions,
      /*isDistinct=*/false,
      /*condition=*/nullptr,
      intermediateType,
      /*orderKeys=*/{},
      /*orderTypes=*/{});
}

optimizer::AggregateCP AggregateRecovery::replaceCondition(
    optimizer::AggregateCP aggregate,
    ExprCP condition) {
  return builder_.makeAggregate(
      aggregate->name(),
      aggregate->value(),
      aggregate->args(),
      aggregate->functions(),
      aggregate->isDistinct(),
      condition,
      aggregate->intermediateType(),
      aggregate->orderKeys(),
      aggregate->orderTypes());
}

} // namespace facebook::axiom::optimizer::v2
