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

#include "axiom/optimizer/v2/PlanSubstitutions.h"

#include <stddef.h>
#include <algorithm>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PlanObject.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/ExprSimplifier.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/PrecomputeProjections.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/Variant.h"

#include <folly/container/F14Set.h>
#include <folly/lang/Hint.h>

namespace facebook::axiom::optimizer::v2 {
namespace {

// Drops order keys that repeat an earlier key, preserving the first key's
// order because later occurrences cannot refine it.
void dropDuplicateOrderKeys(
    ExprVector& orderKeys,
    OrderTypeVector& orderTypes) {
  VELOX_DCHECK_EQ(orderKeys.size(), orderTypes.size());
  ExprVector uniqueKeys;
  OrderTypeVector uniqueTypes;
  uniqueKeys.reserve(orderKeys.size());
  uniqueTypes.reserve(orderTypes.size());
  folly::F14FastSet<ExprCP> seen;
  for (size_t i = 0; i < orderKeys.size(); ++i) {
    if (seen.insert(orderKeys[i]).second) {
      uniqueKeys.push_back(orderKeys[i]);
      uniqueTypes.push_back(orderTypes[i]);
    }
  }
  orderKeys = std::move(uniqueKeys);
  orderTypes = std::move(uniqueTypes);
}

} // namespace

void PlanSubstitutions::add(ExprCP source, ExprCP replacement) {
  const auto [it, inserted] = substitutions_.emplace(source, replacement);
  VELOX_CHECK(
      inserted || it->second == replacement,
      "Conflicting substitutions for expression: {}",
      source->toString());
}

void PlanSubstitutions::set(ExprCP source, ExprCP replacement) {
  substitutions_.insert_or_assign(source, replacement);
}

void PlanSubstitutions::setAll(const PlanSubstitutions& other) {
  for (const auto& [source, replacement] : other.substitutions_) {
    set(source, replacement);
  }
}

void PlanSubstitutions::addIfAbsent(ExprCP source, ExprCP replacement) {
  substitutions_.emplace(source, replacement);
}

void PlanSubstitutions::merge(const PlanSubstitutions& other) {
  for (const auto& [source, target] : other.substitutions_) {
    add(source, target);
  }
}

ExprCP PlanSubstitutions::apply(ExprCP expression, ExprFactory& exprs) const {
  folly::F14FastSet<ExprCP> visited;
  while (visited.insert(expression).second) {
    ExprCP rewritten = exprs.replace(expression, substitutions_);
    if (rewritten == expression) {
      return expression;
    }
    expression = rewritten;
  }
  VELOX_FAIL("Expression substitutions did not converge");
}

ExprVector PlanSubstitutions::apply(
    const ExprVector& expressions,
    ExprFactory& exprs) const {
  ExprVector rewritten;
  rewritten.reserve(expressions.size());
  for (ExprCP expression : expressions) {
    rewritten.push_back(apply(expression, exprs));
  }
  return rewritten;
}

optimizer::AggregateCP PlanSubstitutions::apply(
    optimizer::AggregateCP aggregate,
    ExprFactory& exprs,
    Builder& builder) const {
  ExprVector arguments = apply(aggregate->args(), exprs);
  ExprCP condition = apply(aggregate->condition(), exprs);
  ExprVector orderKeys = apply(aggregate->orderKeys(), exprs);
  OrderTypeVector orderTypes = aggregate->orderTypes();
  dropDuplicateOrderKeys(orderKeys, orderTypes);
  optimizer::AggregateCP fallback = aggregate->fallback();
  if (fallback != nullptr) {
    fallback = apply(fallback, exprs, builder);
  }
  if (arguments == aggregate->args() && condition == aggregate->condition() &&
      orderKeys == aggregate->orderKeys() &&
      fallback == aggregate->fallback()) {
    return aggregate;
  }
  FunctionSet functions = Call::unionArgFunctions(FunctionSet{}, arguments);
  if (aggregate->functions().contains(
          FunctionSet::kIgnoreDuplicatesAggregate)) {
    functions = functions | FunctionSet::kIgnoreDuplicatesAggregate;
  }
  if (aggregate->functions().contains(FunctionSet::kOrderSensitiveAggregate)) {
    functions = functions | FunctionSet::kOrderSensitiveAggregate;
  }
  return builder.makeAggregate(
      aggregate->name(),
      aggregate->value(),
      std::move(arguments),
      functions,
      aggregate->isDistinct(),
      condition,
      aggregate->intermediateType(),
      std::move(orderKeys),
      std::move(orderTypes),
      aggregate->specialKind(),
      fallback);
}

AggregateCallVector PlanSubstitutions::apply(
    const AggregateCallVector& aggregates,
    ExprFactory& exprs,
    Builder& builder) const {
  AggregateCallVector rewritten;
  rewritten.reserve(aggregates.size());
  for (const auto* aggregate : aggregates) {
    rewritten.push_back(apply(aggregate, exprs, builder));
  }
  return rewritten;
}

void PlanSubstitutions::retainVisible(
    const ColumnVector& outputColumns,
    ExprFactory& exprs) {
  const auto outputSet = PlanObjectSet::fromObjects(outputColumns);
  ExprFactory::ExprSubstitution visible;
  for (const auto& [source, target] : substitutions_) {
    ExprCP replacement = apply(target, exprs);
    if (outputSet.containsColumns(replacement)) {
      visible.emplace(source, replacement);
    }
  }
  substitutions_ = std::move(visible);
}

NodeCP PlanSubstitutions::restore(
    NodeCP input,
    const ColumnVector& outputColumns,
    ExprFactory& exprs,
    Builder& builder,
    ExprSimplifier& simplifier) const {
  const auto restoreSingleRow = [&](const ExprVector& values) -> NodeCP {
    std::vector<velox::Variant> row;
    row.reserve(outputColumns.size());
    for (ColumnCP output : outputColumns) {
      const auto it = std::find(
          input->outputColumns().begin(), input->outputColumns().end(), output);
      ExprCP replacement = it == input->outputColumns().end()
          ? apply(output, exprs)
          : apply(values[it - input->outputColumns().begin()], exprs);
      replacement = simplifier.simplify(replacement);
      if (!replacement->is(PlanType::kLiteralExpr)) {
        return nullptr;
      }
      row.push_back(replacement->as<Literal>()->literal());
    }
    return builder.makeSingleRowValues(std::move(row), outputColumns);
  };
  if (input->is(NodeType::kProject)) {
    const auto* project = input->as<Project>();
    if (project->input()->is(NodeType::kValues) &&
        project->input()->as<Values>()->cardinality() == 1) {
      if (NodeCP restored = restoreSingleRow(project->exprs())) {
        return restored;
      }
    }
  } else if (
      input->is(NodeType::kValues) && input->as<Values>()->cardinality() == 1) {
    if (NodeCP restored = restoreSingleRow(
            ExprVector{
                input->outputColumns().begin(),
                input->outputColumns().end()})) {
      return restored;
    }
  }
  const auto inputSet = PlanObjectSet::fromObjects(input->outputColumns());
  ExprVector expressions;
  expressions.reserve(outputColumns.size());
  for (ColumnCP output : outputColumns) {
    ExprCP inputExpr = apply(output, exprs);
    VELOX_CHECK(
        inputSet.containsColumns(inputExpr),
        "Cannot restore output column after rewrite: {}",
        output->toString());
    expressions.push_back(inputExpr);
  }
  if (input->outputColumns() == outputColumns &&
      std::equal(
          expressions.begin(), expressions.end(), outputColumns.begin())) {
    return input;
  }
  return PrecomputeProjections::makeProject(
      input, std::move(expressions), outputColumns, builder, simplifier);
}

} // namespace facebook::axiom::optimizer::v2
