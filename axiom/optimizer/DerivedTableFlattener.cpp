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
#include "axiom/optimizer/DerivedTableFlattener.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer {
namespace {

// Unions function flags from children into 'functions'.
void unionChildFunctions(FunctionSet& functions, const ExprVector& children) {
  for (const auto& child : children) {
    if (child->isFunction()) {
      functions = functions | child->as<Call>()->functions();
    }
  }
}

// Returns the function flags for an Aggregate with new children.
// Aggregate-specific flags (kIgnoreDuplicatesAggregate,
// kOrderSensitiveAggregate) are preserved from the original aggregate.
FunctionSet computeAggregateFunctions(
    const Aggregate* aggregate,
    const ExprVector& newChildren) {
  FunctionSet functions;
  if (aggregate->functions().contains(
          FunctionSet::kIgnoreDuplicatesAggregate)) {
    functions = functions | FunctionSet::kIgnoreDuplicatesAggregate;
  }
  if (aggregate->functions().contains(FunctionSet::kOrderSensitiveAggregate)) {
    functions = functions | FunctionSet::kOrderSensitiveAggregate;
  }
  unionChildFunctions(functions, newChildren);
  return functions;
}

ExprCP replaceInputs(ExprCP expr, const ExprMapping& mapping) {
  if (!expr) {
    return nullptr;
  }

  switch (expr->type()) {
    case PlanType::kColumnExpr: {
      auto it = mapping.find(expr);
      return it != mapping.end() ? it->second : expr;
    }
    case PlanType::kLiteralExpr:
      return expr;
    case PlanType::kCallExpr:
    case PlanType::kAggregateExpr: {
      auto children = expr->children();
      ExprVector newChildren(children.size());
      bool anyChange = false;
      for (auto i = 0; i < children.size(); ++i) {
        newChildren[i] = replaceInputs(children[i]->as<Expr>(), mapping);
        anyChange |= newChildren[i] != children[i];
      }

      if (expr->type() == PlanType::kAggregateExpr) {
        const auto* aggregate = expr->as<Aggregate>();
        auto* newCondition = replaceInputs(aggregate->condition(), mapping);

        ExprVector newOrderKeys;
        newOrderKeys.reserve(aggregate->orderKeys().size());
        for (const auto* orderKey : aggregate->orderKeys()) {
          newOrderKeys.push_back(replaceInputs(orderKey, mapping));
        }

        anyChange |= newCondition != aggregate->condition();
        anyChange |= newOrderKeys != aggregate->orderKeys();

        if (!anyChange) {
          return expr;
        }

        auto functions = computeAggregateFunctions(aggregate, newChildren);
        return make<Aggregate>(
            aggregate->name(),
            aggregate->value(),
            std::move(newChildren),
            std::move(functions),
            aggregate->isDistinct(),
            newCondition,
            aggregate->intermediateType(),
            std::move(newOrderKeys),
            aggregate->orderTypes());
      }

      if (!anyChange) {
        return expr;
      }

      const auto* call = expr->as<Call>();
      return queryCtx()->optimization()->deduppedCall(
          call->name(),
          call->value(),
          std::move(newChildren),
          /*specialForm=*/SpecialFormCallNames::isSpecialForm(call->name()));
    }
    case PlanType::kFieldExpr: {
      auto* field = expr->as<Field>();
      auto* newBase = replaceInputs(field->base(), mapping);
      if (newBase != field->base()) {
        return make<Field>(field->value().type, newBase, field->field());
      }

      return expr;
    }
    case PlanType::kLambdaExpr: {
      auto* lambda = expr->as<Lambda>();
      auto* body = lambda->body();
      auto* newBody = replaceInputs(body, mapping);
      if (body == newBody) {
        return expr;
      }

      return make<Lambda>(lambda->args(), lambda->value().type, newBody);
    }
    case PlanType::kWindowExpr: {
      auto* window = expr->as<WindowFunction>();
      auto children = expr->children();
      ExprVector newArgs(children.size());
      bool anyChange = false;
      for (auto i = 0; i < children.size(); ++i) {
        newArgs[i] = replaceInputs(children[i]->as<Expr>(), mapping);
        anyChange |= newArgs[i] != children[i];
      }

      ExprVector newPartitionKeys;
      newPartitionKeys.reserve(window->partitionKeys().size());
      for (const auto* key : window->partitionKeys()) {
        newPartitionKeys.push_back(replaceInputs(key, mapping));
      }
      anyChange |= newPartitionKeys != window->partitionKeys();

      ExprVector newOrderKeys;
      newOrderKeys.reserve(window->orderKeys().size());
      for (const auto* key : window->orderKeys()) {
        newOrderKeys.push_back(replaceInputs(key, mapping));
      }
      anyChange |= newOrderKeys != window->orderKeys();

      Frame newFrame = window->frame();
      newFrame.startValue = replaceInputs(newFrame.startValue, mapping);
      newFrame.endValue = replaceInputs(newFrame.endValue, mapping);
      anyChange |= newFrame.startValue != window->frame().startValue;
      anyChange |= newFrame.endValue != window->frame().endValue;

      if (!anyChange) {
        return expr;
      }

      FunctionSet functions;
      unionChildFunctions(functions, newArgs);
      return make<WindowFunction>(
          window->name(),
          window->value(),
          std::move(newArgs),
          functions,
          std::move(newPartitionKeys),
          std::move(newOrderKeys),
          window->orderTypes(),
          newFrame,
          window->ignoreNulls());
    }
    default:
      VELOX_UNREACHABLE(
          "Unexpected expression: {} - {}", expr->typeName(), expr->toString());
  }
}

void replaceInputs(ExprVector& exprs, const ExprMapping& mapping) {
  for (auto i = 0; i < exprs.size(); ++i) {
    exprs[i] = replaceInputs(exprs[i], mapping);
  }
}

AggregationPlanCP replaceInputs(
    AggregationPlanCP aggregation,
    const ExprMapping& mapping) {
  if (!aggregation) {
    return nullptr;
  }

  ExprVector newGroupingKeys = aggregation->groupingKeys();
  replaceInputs(newGroupingKeys, mapping);

  AggregateVector newAggregates;
  newAggregates.reserve(aggregation->aggregates().size());
  for (const auto* aggregate : aggregation->aggregates()) {
    newAggregates.push_back(replaceInputs(aggregate, mapping)->as<Aggregate>());
  }

  if (newGroupingKeys == aggregation->groupingKeys() &&
      newAggregates == aggregation->aggregates()) {
    return aggregation;
  }

  return make<AggregationPlan>(
      std::move(newGroupingKeys),
      std::move(newAggregates),
      aggregation->columns(),
      aggregation->intermediateColumns());
}

// Returns a new JoinEdge with columns and expressions remapped via
// 'mapping'. Copies fanouts from the original. Uniqueness is left as
// default (recomputed by guessFanout during finalizeJoins).
JoinEdge* remapJoinColumns(const JoinEdge* join, const ExprMapping& mapping) {
  auto remapColumn = [&](ColumnCP column) -> ColumnCP {
    if (!column) {
      return nullptr;
    }
    auto it = mapping.find(column);
    return it != mapping.end() ? it->second->as<Column>() : column;
  };

  auto remapExprs = [&](const ExprVector& exprs) {
    ExprVector result;
    result.reserve(exprs.size());
    for (auto* expr : exprs) {
      result.push_back(replaceInputs(expr, mapping));
    }
    return result;
  };

  auto remapColumns = [&](const ColumnVector& columns) {
    ColumnVector result;
    result.reserve(columns.size());
    for (auto* column : columns) {
      result.push_back(remapColumn(column));
    }
    return result;
  };

  auto* edge = make<JoinEdge>(
      join->leftTable(),
      join->rightTable(),
      JoinEdge::Spec{
          .filter = remapExprs(join->filter()),
          .joinType = join->joinType(),
          .nullAwareIn = join->isNullAwareIn(),
          .nullAsValue = join->isNullAsValue(),
          .markColumn = remapColumn(join->markColumn()),
          .rowNumberColumn = remapColumn(join->rowNumberColumn()),
          .multipleMatchesError = join->multipleMatchesError(),
          .leftColumns = remapColumns(join->leftColumns()),
          .leftExprs = remapExprs(join->leftExprs()),
          .rightColumns = remapColumns(join->rightColumns()),
          .rightExprs = remapExprs(join->rightExprs()),
          .directed = join->directed(),
      },
      join->originalJoinType());

  for (size_t i = 0; i < join->numKeys(); ++i) {
    edge->addEquality(
        replaceInputs(join->leftKeys()[i], mapping),
        replaceInputs(join->rightKeys()[i], mapping));
  }
  edge->setFanouts(join->lrFanout(), join->rlFanout());
  return edge;
}

} // namespace

ExprCP DerivedTableFlattener::replaceInputs(
    ExprCP expr,
    const ExprMapping& mapping) {
  return ::facebook::axiom::optimizer::replaceInputs(expr, mapping);
}

ExprCP DerivedTableFlattener::replaceInputs(
    ExprCP expr,
    const ColumnVector& source,
    const ExprVector& target) {
  ExprMapping mapping;
  auto size = std::min(source.size(), target.size());
  mapping.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    mapping[source[i]] = target[i];
  }
  return replaceInputs(expr, mapping);
}

ExprCP DerivedTableFlattener::replaceInputs(
    ExprCP expr,
    const ExprVector& source,
    const ColumnVector& target) {
  ExprMapping mapping;
  auto size = std::min(source.size(), target.size());
  mapping.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    mapping[source[i]] = target[i];
  }
  return replaceInputs(expr, mapping);
}

void DerivedTableFlattener::reconstructColumns(
    DerivedTable* dt,
    const DerivedTable* oldDt) {
  // Import the anonymous-namespace replaceInputs overloads (map-based).
  // Without this, the compiler resolves to the public 3-arg overload.
  using ::facebook::axiom::optimizer::replaceInputs;

  // Reconstruct columns with relation_ == oldDt. These reference an object
  // no longer in dt's tableSet. Process bottom-up through the DT layers so
  // that each layer's expressions are rewritten before the next layer's
  // outputs are created.
  ExprMapping mapping;

  // Recreates a column with relation_ == dt. Registers the old->new
  // mapping so that expressions referencing the old column get rewritten.
  auto recreateColumn = [&](ColumnCP column) -> ColumnCP {
    auto* newColumn =
        make<Column>(column->name(), dt, column->value(), column->alias());
    mapping[column] = newColumn;
    return newColumn;
  };

  // Layer 2: Joins — recreate output columns with relation_ == oldDt, then
  // rebuild affected edges.
  bool hasJoinColumns = false;
  for (auto* join : dt->joins) {
    for (auto* column : join->leftColumns()) {
      recreateColumn(column);
      hasJoinColumns = true;
    }
    for (auto* column : join->rightColumns()) {
      recreateColumn(column);
      hasJoinColumns = true;
    }
    if (join->markColumn()) {
      recreateColumn(join->markColumn());
      hasJoinColumns = true;
    }
    if (join->rowNumberColumn()) {
      recreateColumn(join->rowNumberColumn());
      hasJoinColumns = true;
    }
  }
  if (hasJoinColumns) {
    for (auto*& join : dt->joins) {
      join = remapJoinColumns(join, mapping);
    }
  }

  // Layer 3: Filters — rewrite conjuncts referencing remapped join columns.
  if (!mapping.empty()) {
    replaceInputs(dt->conjuncts, mapping);
  }

  // Layer 4: Aggregation — recreate output columns with
  // relation_ == oldDt, rewrite grouping keys and aggregate inputs.
  // Grouping key output columns may reference base tables
  // (relation_ != oldDt) and are left unchanged.
  if (dt->aggregation) {
    auto* rewritten = replaceInputs(dt->aggregation, mapping);
    const auto numMappingsBefore = mapping.size();

    const auto numKeys = rewritten->groupingKeys().size();

    ColumnVector newAggregationColumns;
    newAggregationColumns.reserve(rewritten->columns().size());
    for (auto* column : rewritten->columns()) {
      if (column->relation() == oldDt) {
        newAggregationColumns.push_back(recreateColumn(column));
      } else {
        newAggregationColumns.push_back(column);
      }
    }

    // Grouping key entries in intermediateColumns share the same Column
    // objects as columns (see ToGraph.cpp translateAggregation). Reuse the
    // already-recreated columns for the grouping key prefix. Aggregate
    // entries have distinct Column objects and are recreated independently.
    ColumnVector newIntermediateColumns;
    newIntermediateColumns.reserve(rewritten->intermediateColumns().size());
    for (size_t i = 0; i < rewritten->intermediateColumns().size(); ++i) {
      if (i < numKeys) {
        newIntermediateColumns.push_back(newAggregationColumns[i]);
      } else {
        auto* column = rewritten->intermediateColumns()[i];
        if (column->relation() == oldDt) {
          newIntermediateColumns.push_back(recreateColumn(column));
        } else {
          newIntermediateColumns.push_back(column);
        }
      }
    }

    if (rewritten != dt->aggregation || mapping.size() != numMappingsBefore) {
      dt->aggregation = make<AggregationPlan>(
          rewritten->groupingKeys(),
          rewritten->aggregates(),
          std::move(newAggregationColumns),
          std::move(newIntermediateColumns));
    }
  }

  // Layer 5: Having — rewrite expressions referencing remapped agg columns.
  if (!dt->having.empty() && !mapping.empty()) {
    replaceInputs(dt->having, mapping);
  }

  // Layer 6: Window — recreate output columns with relation_ == oldDt,
  // rewrite window function expressions. Window functions within a DT are
  // independent (no cross-references).
  if (dt->windowPlan) {
    const auto numMappingsBefore = mapping.size();

    WindowFunctionVector newFunctions;
    newFunctions.reserve(dt->windowPlan->functions().size());
    for (const auto* func : dt->windowPlan->functions()) {
      newFunctions.push_back(
          replaceInputs(func, mapping)->as<WindowFunction>());
    }

    ColumnVector newWindowColumns;
    newWindowColumns.reserve(dt->windowPlan->columns().size());

    for (const auto* column : dt->windowPlan->columns()) {
      if (column->relation() == oldDt) {
        newWindowColumns.push_back(recreateColumn(column));
      } else {
        newWindowColumns.push_back(column);
      }
    }

    if (newFunctions != dt->windowPlan->functions() ||
        mapping.size() != numMappingsBefore) {
      dt->windowPlan = make<WindowPlan>(
          std::move(newFunctions),
          std::move(newWindowColumns),
          dt->windowPlan->rankingLimit());
    }
  }

  // Layer 7: Rewrite expressions that reference remapped columns.
  if (!mapping.empty()) {
    replaceInputs(dt->exprs, mapping);
    replaceInputs(dt->orderKeys, mapping);
  }
}

} // namespace facebook::axiom::optimizer
