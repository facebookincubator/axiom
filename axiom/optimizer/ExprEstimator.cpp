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

#include "axiom/optimizer/ExprEstimator.h"

#include <algorithm>

#include <folly/container/F14Set.h>

#include "axiom/optimizer/EstimateMath.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PlanUtils.h"

namespace facebook::axiom::optimizer {

namespace {

// Returns the exact statistics of 'values', all of type 'type': their distinct
// count, bounds, null and true fractions, and the statistics of their children.
Value literalStatistics(
    TypeCP type,
    const std::vector<const velox::Variant*>& values) {
  Value result(type);
  std::vector<const velox::Variant*> nonNull;
  for (const auto* value : values) {
    if (!value->isNull()) {
      nonNull.push_back(value);
    }
  }
  if (!values.empty()) {
    result.nullFraction =
        static_cast<float>(values.size() - nonNull.size()) / values.size();
  }
  result.nullable = nonNull.size() < values.size();
  if (nonNull.empty()) {
    return result;
  }

  folly::F14FastSet<
      const velox::Variant*,
      decltype([](const velox::Variant* value) { return value->hash(); }),
      decltype([](const velox::Variant* left, const velox::Variant* right) {
        return *left == *right;
      })>
      distinct(nonNull.begin(), nonNull.end());
  result.cardinality = distinct.size();

  if (type->isPrimitiveType()) {
    const auto less = [](const velox::Variant* left,
                         const velox::Variant* right) {
      return *left < *right;
    };
    result.min = *std::ranges::min_element(nonNull, less);
    result.max = *std::ranges::max_element(nonNull, less);
  }
  if (type->kind() == velox::TypeKind::BOOLEAN) {
    result.trueFraction =
        static_cast<float>(std::ranges::count_if(
            nonNull,
            [](const velox::Variant* value) { return value->value<bool>(); })) /
        nonNull.size();
  }

  if (!type->isArray() && !type->isMap() && !type->isRow()) {
    return result;
  }
  // The values of each child: an array's elements, a map's keys and values, a
  // row's fields.
  std::vector<std::vector<const velox::Variant*>> childValues(type->size());
  for (const auto* value : nonNull) {
    if (type->isArray()) {
      for (const auto& element : value->array()) {
        childValues[0].push_back(&element);
      }
    } else if (type->isMap()) {
      for (const auto& [key, mapValue] : value->map()) {
        childValues[0].push_back(&key);
        childValues[1].push_back(&mapValue);
      }
    } else {
      for (size_t i = 0; i < type->size(); ++i) {
        childValues[i].push_back(&value->row()[i]);
      }
    }
  }
  std::vector<Value> children;
  children.reserve(type->size());
  for (size_t i = 0; i < type->size(); ++i) {
    children.push_back(
        literalStatistics(type->childAt(i).get(), childValues[i]));
  }
  std::vector<const Value*> pointers;
  for (const Value& child : children) {
    pointers.push_back(&child);
  }
  result.children = Value::makeChildren(type, pointers);
  return result;
}

// Returns the statistics of 'subfield' of 'value' (see
// FunctionMetadata::DrawnFrom). nullptr when unknown.
const Value* valueAt(const Value& value, uint32_t subfield) {
  return subfield == 0 ? &value : value.child(subfield - 1);
}

// Returns the type of 'subfield' of a value of 'type'.
TypeCP typeAt(TypeCP type, uint32_t subfield) {
  return subfield == 0 ? type : type->childAt(subfield - 1).get();
}

// Returns 'value' with the statistics of 'subfield' replaced by 'source''s
// NDV, min, max, size and children. The null and true fractions and the other
// children are kept.
Value narrowAt(const Value& value, uint32_t subfield, const Value& source) {
  if (subfield == 0) {
    Value result = value;
    if (source.cardinality.has_value()) {
      result.cardinality = source.cardinality;
    }
    result.min = source.min;
    result.max = source.max;
    result.avgSizeInBytes = source.avgSizeInBytes;
    result.children = source.children;
    return result;
  }
  const uint32_t index = subfield - 1;
  const Value* known = value.child(index);
  const Value narrowed = narrowAt(
      known != nullptr ? *known : Value(value.type->childAt(index).get()),
      0,
      source);
  std::vector<const Value*> children(value.type->size());
  for (uint32_t i = 0; i < children.size(); ++i) {
    children[i] = i == index ? &narrowed : value.child(i);
  }
  Value result = value;
  result.children = Value::makeChildren(value.type, children);
  return result;
}

// Returns 'estimate', the estimate of argument 'index' of 'call'. A literal's
// children are derived here, when a rule reads them.
Value argument(const Call& call, uint32_t index, Value estimate) {
  Value result = std::move(estimate);
  const ExprCP arg = call.argAt(index);
  if (result.children == nullptr && arg->is(PlanType::kLiteralExpr)) {
    result.children =
        literalStatistics(result.type, {&arg->as<Literal>()->literal()})
            .children;
  }
  return result;
}

// Returns where 'call's result values come from (see
// FunctionMetadata::DrawnFrom).
std::vector<FunctionMetadata::DrawnFrom> drawnFrom(const Call& call) {
  using DrawnFrom = FunctionMetadata::DrawnFrom;
  std::vector<TypeCP> argTypes;
  argTypes.reserve(call.args().size());
  for (const auto* arg : call.args()) {
    argTypes.push_back(arg->value().type);
  }

  // An element of an array or a value of a map is one of its values.
  const auto& names = queryCtx()->functionNames();
  if (call.name() == names.elementAt || call.name() == names.subscript) {
    return {
        argTypes[0]->isArray() ? DrawnFrom::resultValue().drawnFromElement(0)
                               : DrawnFrom::resultValue().drawnFromMapValue(0)};
  }

  const auto* metadata = call.metadata();
  if (metadata == nullptr || !metadata->drawnFrom) {
    return {};
  }
  return metadata->drawnFrom(argTypes);
}

// Returns 'result' narrowed at each subfield whose values 'call' draws from an
// argument. 'argumentEstimate(i)' returns the estimate of argument i.
Value narrowDrawnFrom(
    const Call& call,
    Value result,
    const auto& argumentEstimate) {
  for (const auto& drawn : drawnFrom(call)) {
    VELOX_CHECK_LT(drawn.arg, call.args().size(), "{}", call.name());
    VELOX_CHECK(
        typeAt(result.type, drawn.resultSubfield)
            ->equivalent(*typeAt(
                call.argAt(drawn.arg)->value().type, drawn.argSubfield)),
        "Drawn-from subfields must have the same type: {}",
        call.name());
    const Value source = argument(call, drawn.arg, argumentEstimate(drawn.arg));
    if (const Value* drawnValue = valueAt(source, drawn.argSubfield)) {
      result = narrowAt(result, drawn.resultSubfield, *drawnValue);
    }
  }
  return result;
}

} // namespace

Value ExprEstimator::estimate(ExprCP expr, bool update) {
  const bool isLeaf =
      expr->is(PlanType::kLiteralExpr) || expr->is(PlanType::kColumnExpr);
  if (!update || isLeaf) {
    if (const auto it = constraints_.find(expr); it != constraints_.end()) {
      return it->second;
    }
  }

  Value result = expr->value();
  if (expr->is(PlanType::kFieldExpr)) {
    const auto* field = expr->as<Field>();
    const Value base = estimate(field->base(), update);
    result = base.field(
        field->field() != nullptr
            ? base.type->asRow().getChildIdx(field->field())
            : field->index());
  } else if (expr->is(PlanType::kCallExpr)) {
    result = estimateCall(*expr->as<Call>(), update);
  }

  result = clampCardinality(result);
  constraints_.insert_or_assign(expr, result);
  return result;
}

Value ExprEstimator::estimateCall(const Call& call, bool update) {
  VELOX_CHECK(
      !call.containsFunction(FunctionSet::kAggregate),
      "Expression cannot contain an aggregate function call: {}",
      call.toString());
  std::vector<Value> args;
  args.reserve(call.args().size());
  for (const auto* arg : call.args()) {
    args.push_back(estimate(arg, update));
  }

  // A row field's statistics are the row's child (see Value::field). The
  // index is an INTEGER literal.
  if (call.name() == SpecialFormCallNames::kDereference) {
    return args[0].field(
        static_cast<uint32_t>(integerValue(&call.literalAt(1))));
  }

  // The largest NDV among the inputs; unknown if any input's is unknown. A
  // lambda's inputs are the columns it reads from outside.
  Value result = call.value();
  std::optional<float> maxCardinality = 1.0f;
  for (size_t i = 0; i < args.size(); ++i) {
    if (call.argAt(i)->is(PlanType::kLambdaExpr)) {
      call.argAt(i)->columns().forEach<Column>([&](ColumnCP column) {
        maxCardinality =
            maxOf(maxCardinality, estimate(column, update).cardinality);
      });
    } else {
      maxCardinality = maxOf(maxCardinality, args[i].cardinality);
    }
  }
  result.cardinality = maxCardinality;

  // Field i of a constructed row takes argument i's statistics.
  if (call.isRowConstructor()) {
    std::vector<const Value*> fields;
    fields.reserve(args.size());
    for (const Value& arg : args) {
      fields.push_back(&arg);
    }
    result.children = Value::makeChildren(result.type, fields);
    return result;
  }

  return narrowDrawnFrom(
      call, std::move(result), [&](uint32_t i) { return args[i]; });
}

Value ExprEstimator::estimateAggregate(
    const Aggregate& aggregate,
    std::optional<float> numGroups) {
  return narrowDrawnFrom(
      aggregate, Value(aggregate.value().type, numGroups), [&](uint32_t i) {
        return estimate(aggregate.argAt(i));
      });
}

} // namespace facebook::axiom::optimizer
