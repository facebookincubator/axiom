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

#include "axiom/optimizer/v2/NodeSimplifier.h"

#include <stdint.h>
#include <algorithm>
#include <initializer_list>
#include <utility>

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/container/HeterogeneousAccess.h>
#include <folly/lang/Hint.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/logical_plan/Expr.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PlanObject.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/ExprSimplifier.h"
#include "axiom/optimizer/v2/JoinFilterRewriter.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/NonNullOutput.h"
#include "axiom/optimizer/v2/PlanSubstitutions.h"
#include "axiom/optimizer/v2/PrecomputeProjections.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/TypeKind.h"
#include "velox/type/Variant.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

using SimplifiedNode = NodeSimplifier::SimplifiedNode;

// Returns an empty result whose original layout is 'columns'.
SimplifiedNode emptyResult(ColumnVector columns) {
  return {nullptr, {}, std::move(columns)};
}

// Returns the empty result of a node that passes 'input' through. Its layout is
// the input's columns before any renames: 'input.originalColumns' when set,
// otherwise the columns of the key's input node.
SimplifiedNode emptyPassThroughResult(
    NodeCP keyInput,
    const SimplifiedNode& input) {
  return emptyResult(
      input.originalColumns.empty() ? keyInput->outputColumns()
                                    : input.originalColumns);
}

} // namespace

ExprVector NodeSimplifier::simplifyExpressions(
    const PlanSubstitutions& substitutions,
    const ExprVector& expressions) {
  ExprVector simplified = substitutions.apply(expressions, exprs_);
  for (ExprCP& expression : simplified) {
    expression = simplifier_.simplify(expression);
  }
  return simplified;
}

void NodeSimplifier::simplifyOrderKeys(
    const PlanSubstitutions& substitutions,
    ExprVector& keys,
    OrderTypeVector& types) {
  VELOX_DCHECK_EQ(keys.size(), types.size());
  ExprVector simplifiedKeys;
  OrderTypeVector simplifiedTypes;
  simplifiedKeys.reserve(keys.size());
  simplifiedTypes.reserve(types.size());
  folly::F14FastSet<ExprCP> seen;
  ExprVector rewritten = simplifyExpressions(substitutions, keys);
  for (size_t i = 0; i < keys.size(); ++i) {
    ExprCP key = rewritten[i];
    if (key->is(PlanType::kLiteralExpr) || !seen.insert(key).second) {
      continue;
    }
    simplifiedKeys.push_back(key);
    simplifiedTypes.push_back(types[i]);
  }
  keys = std::move(simplifiedKeys);
  types = std::move(simplifiedTypes);
}

PlanSubstitutions NodeSimplifier::visibleSubstitutions(
    PlanSubstitutions substitutions,
    NodeCP output) {
  substitutions.retainVisible(output->outputColumns(), exprs_);
  return substitutions;
}

ExprVector NodeSimplifier::restoreColumnPositions(
    NodeCP& input,
    const ExprVector& positions,
    const ExprVector& replacements) {
  VELOX_CHECK_EQ(positions.size(), replacements.size());
  ExprVector restoredExpressions(
      input->outputColumns().begin(), input->outputColumns().end());
  ColumnVector restoredOutputs = input->outputColumns();
  PlanObjectSet restoredSet = PlanObjectSet::fromObjects(restoredOutputs);
  ExprVector result;
  result.reserve(positions.size());
  for (size_t i = 0; i < positions.size(); ++i) {
    ExprCP position = positions[i];
    ExprCP replacement = replacements[i];
    if (replacement->isColumn()) {
      result.push_back(replacement);
      continue;
    }
    // A key may already be an expression (including a literal) before input
    // substitutions are applied. Emission precomputes such expression keys;
    // only a replaced column needs its original identity restored here.
    if (!position->isColumn()) {
      result.push_back(replacement);
      continue;
    }
    ColumnCP column = position->as<Column>();
    if (!restoredSet.contains(column)) {
      restoredExpressions.push_back(replacement);
      restoredOutputs.push_back(column);
      restoredSet.add(column);
    }
    result.push_back(column);
  }
  if (restoredOutputs.size() != input->outputColumns().size()) {
    input = PrecomputeProjections::makeProject(
        input,
        std::move(restoredExpressions),
        std::move(restoredOutputs),
        builder_,
        simplifier_);
  }
  return result;
}

ColumnVector NodeSimplifier::restoreColumnPositions(
    NodeCP& input,
    const ColumnVector& positions,
    const PlanSubstitutions& substitutions) {
  ExprVector expressions = toExprs(positions);
  ExprVector replacements = substitutions.apply(expressions, exprs_);
  ExprVector restored =
      restoreColumnPositions(input, expressions, replacements);
  return toColumns(restored);
}

NodeCP NodeSimplifier::restoreExactLayout(
    NodeCP input,
    const ColumnVector& outputColumns,
    const PlanSubstitutions& substitutions) {
  return substitutions.restore(
      input, outputColumns, exprs_, builder_, simplifier_);
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(Values::Key key) {
  const Values* values = builder_.make<Values>(std::move(key));
  if (values->cardinality() == 0) {
    return emptyResult(values->outputColumns());
  }
  if (values->cardinality() != 1 || values->rows() == nullptr) {
    return {values, {}};
  }

  PlanSubstitutions substitutions;
  const ColumnVector outputColumns = values->outputColumns();
  for (size_t i = 0; i < outputColumns.size(); ++i) {
    substitutions.add(
        outputColumns[i],
        builder_.makeLiteral(
            velox::Variant(values->valueAt(0, i)),
            outputColumns[i]->value().type));
  }
  NodeCP output =
      builder_.make<Values>({values->source(), values->rows(), {}, {}});
  return {output, std::move(substitutions)};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(Scan::Key key) {
  return {builder_.make<Scan>(std::move(key)), {}};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(WorkingTable::Key key) {
  return {builder_.make<WorkingTable>(std::move(key)), {}};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    Filter::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    return input;
  }

  ExprVector predicates = input.substitutions.apply(key.predicates, exprs_);
  NonNullOutput nonNullOutput{simplifier_};
  const PlanObjectSet& nonNullColumns = nonNullOutput.get(input.node);
  for (ExprCP& predicate : predicates) {
    predicate = simplifier_.simplify(predicate, nonNullColumns);
  }
  ExprVector kept;
  kept.reserve(predicates.size());
  for (ExprCP predicate : predicates) {
    if (simplifier_.simplifyFilter(predicate, kept)) {
      return emptyPassThroughResult(key.input, input);
    }
  }

  NodeCP output = input.node;
  if (!kept.empty()) {
    output = builder_.make<Filter>({input.node, std::move(kept)});
  }
  input.substitutions =
      visibleSubstitutions(std::move(input.substitutions), output);
  input.node = output;
  return input;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    Project::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    return emptyResult(std::move(key.outputColumns));
  }

  key.input = input.node;
  key.exprs = simplifyExpressions(input.substitutions, key.exprs);
  VELOX_CHECK_EQ(key.exprs.size(), key.outputColumns.size());

  ExprVector keptExpressions;
  ColumnVector keptOutputs;
  // Project expressions already include input substitutions, so their output
  // mappings supersede input mappings for reused column identities.
  PlanSubstitutions outputSubstitutions{std::move(input.substitutions)};
  keptExpressions.reserve(key.exprs.size());
  keptOutputs.reserve(key.outputColumns.size());
  for (size_t i = 0; i < key.exprs.size(); ++i) {
    ExprCP expression = key.exprs[i];
    ColumnCP output = key.outputColumns[i];
    if (expression->is(PlanType::kLiteralExpr)) {
      outputSubstitutions.set(output, expression);
      continue;
    }
    keptExpressions.push_back(expression);
    keptOutputs.push_back(output);
    if (!expression->isColumn()) {
      outputSubstitutions.erase(output);
      outputSubstitutions.addIfAbsent(expression, output);
    }
  }

  bool isCompleteRename = !keptOutputs.empty() &&
      keptOutputs.size() == input.node->outputColumns().size();
  PlanObjectSet renamedInputs;
  for (size_t i = 0; isCompleteRename && i < keptOutputs.size(); ++i) {
    ExprCP expression = keptExpressions[i];
    isCompleteRename = expression->isColumn();
    if (isCompleteRename) {
      renamedInputs.add(expression->as<Column>());
      if (keptOutputs[i] != expression) {
        outputSubstitutions.set(keptOutputs[i], expression);
      }
    }
  }
  isCompleteRename = isCompleteRename &&
      renamedInputs.containsAll(input.node->outputColumns());

  NodeCP output = input.node;
  if (!isCompleteRename) {
    output = PrecomputeProjections::makeProject(
        input.node,
        std::move(keptExpressions),
        std::move(keptOutputs),
        builder_,
        simplifier_);
  }
  outputSubstitutions.retainVisible(output->outputColumns(), exprs_);
  return {output, std::move(outputSubstitutions)};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    Sort::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    return input;
  }
  key.input = input.node;
  const ExprVector originalOrderKeys = key.orderKeys;
  simplifyOrderKeys(input.substitutions, key.orderKeys, key.orderTypes);
  NodeCP output = input.node;
  if (!key.orderKeys.empty()) {
    PrecomputeProjections precompute{input.node, builder_, simplifier_};
    PlanSubstitutions orderSubstitutions;
    for (ExprCP& orderKey : key.orderKeys) {
      ExprCP expression = orderKey;
      orderKey = precompute.toColumn(expression);
      if (expression != orderKey) {
        orderSubstitutions.add(expression, orderKey);
        for (ExprCP original : originalOrderKeys) {
          ExprCP rewritten =
              simplifier_.simplify(input.substitutions.apply(original, exprs_));
          if (rewritten == expression && original != expression) {
            orderSubstitutions.add(original, orderKey);
          }
        }
      }
    }
    key.input = std::move(precompute).node();
    output = builder_.make<Sort>(std::move(key));
    input.substitutions.merge(orderSubstitutions);
  }
  input.substitutions =
      visibleSubstitutions(std::move(input.substitutions), output);
  input.node = output;
  return input;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    Limit::Key key,
    SimplifiedNode input) {
  if (input.empty() || key.count == 0) {
    return emptyPassThroughResult(key.input, input);
  }
  key.input = input.node;
  NodeCP output = builder_.make<Limit>(std::move(key));
  input.substitutions =
      visibleSubstitutions(std::move(input.substitutions), output);
  input.node = output;
  return input;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    TopN::Key key,
    SimplifiedNode input) {
  if (input.empty() || key.count == 0) {
    return emptyPassThroughResult(key.input, input);
  }
  key.input = input.node;
  simplifyOrderKeys(input.substitutions, key.orderKeys, key.orderTypes);
  NodeCP output = key.orderKeys.empty()
      ? static_cast<NodeCP>(
            builder_.make<Limit>({input.node, key.offset, key.count}))
      : static_cast<NodeCP>(builder_.make<TopN>(std::move(key)));
  input.substitutions =
      visibleSubstitutions(std::move(input.substitutions), output);
  input.node = output;
  return input;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    Aggregate::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    if (!key.groupingKeys.empty() && key.globalGroupingSets.empty()) {
      return emptyResult(std::move(key.outputColumns));
    }

    // A global aggregate, including an aggregate with a global grouping set,
    // manufactures its default row(s) over empty input. Keep an explicitly
    // empty input under the Aggregate so Velox produces those rows.
    key.input = builder_.makeEmptyValues(input.outputColumns());
    return {builder_.make<Aggregate>(std::move(key)), {}};
  }

  VELOX_CHECK_EQ(key.step, AggregateStep::kSingle);
  key.input = input.node;
  key.groupingKeys = simplifyExpressions(input.substitutions, key.groupingKeys);
  key.aggregates = input.substitutions.apply(key.aggregates, exprs_, builder_);

  const size_t originalNumKeys = key.groupingKeys.size();
  VELOX_CHECK_GE(key.outputColumns.size(), originalNumKeys);

  PlanSubstitutions outputSubstitutions;
  ExprVector keptKeys;
  ColumnVector keptKeyOutputs;
  folly::F14FastMap<ExprCP, ColumnCP> firstOutputForKey;
  for (size_t i = 0; i < originalNumKeys; ++i) {
    ExprCP groupingKey = key.groupingKeys[i];
    ColumnCP output = key.outputColumns[i];
    if (!keepsGroupingKey(key.groupingKeys, i)) {
      outputSubstitutions.add(output, groupingKey);
      continue;
    }

    const auto [it, inserted] = firstOutputForKey.emplace(groupingKey, output);
    if (!inserted) {
      if (output != it->second) {
        outputSubstitutions.add(output, it->second);
      }
      continue;
    }
    keptKeys.push_back(groupingKey);
    keptKeyOutputs.push_back(output);
  }

  AggregateCallVector keptAggregates;
  for (size_t i = 0; i < key.aggregates.size(); ++i) {
    const auto* aggregate = key.aggregates[i];

    ExprCP condition = aggregate->condition();
    if (condition != nullptr) {
      condition = simplifier_.simplify(condition);
      if (isConstantNull(condition) || isConstantFalse(condition)) {
        outputSubstitutions.add(
            key.outputColumns[originalNumKeys + i],
            builder_.makeLiteral(
                aggregate->emptyInputResult(), aggregate->value().type));
        continue;
      }

      if (isConstantTrue(condition)) {
        condition = nullptr;
      }

      if (condition != aggregate->condition()) {
        aggregate = builder_.makeAggregate(
            aggregate->name(),
            aggregate->value(),
            ExprVector{aggregate->args()},
            aggregate->functions(),
            aggregate->isDistinct(),
            condition,
            aggregate->intermediateType(),
            ExprVector{aggregate->orderKeys()},
            aggregate->orderTypes(),
            aggregate->specialKind(),
            aggregate->fallback());
      }
    }

    keptAggregates.push_back(aggregate);
    keptKeyOutputs.push_back(key.outputColumns[originalNumKeys + i]);
  }
  key.groupingKeys = std::move(keptKeys);
  key.aggregates = std::move(keptAggregates);
  key.outputColumns = std::move(keptKeyOutputs);

  NodeCP output = key.groupingKeys.empty() && key.aggregates.empty() &&
          key.globalGroupingSets.empty()
      ? builder_.makeSingleRowValues({}, ColumnVector{})
      : static_cast<NodeCP>(builder_.make<Aggregate>(std::move(key)));
  input.substitutions.retainVisible(output->outputColumns(), exprs_);
  outputSubstitutions.merge(input.substitutions);
  outputSubstitutions.retainVisible(output->outputColumns(), exprs_);
  return {output, std::move(outputSubstitutions)};
}

// static
bool NodeSimplifier::keepsGroupingKey(
    const ExprVector& groupingKeys,
    size_t index) {
  const auto isLiteral = [](ExprCP key) {
    return key->is(PlanType::kLiteralExpr);
  };
  return !isLiteral(groupingKeys[index]) ||
      (index == 0 && std::ranges::all_of(groupingKeys, isLiteral));
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    GroupId::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    return emptyResult(std::move(key.outputColumns));
  }

  key.input = input.node;
  const size_t originalNumKeys = key.groupingKeys.size();
  VELOX_CHECK_EQ(originalNumKeys, key.groupingKeyColumns.size());
  ExprVector rewrittenKeys =
      simplifyExpressions(input.substitutions, key.groupingKeys);

  PlanSubstitutions outputSubstitutions;
  ExprVector keptKeys;
  ColumnVector keptKeyColumns;
  std::vector<int32_t> oldToNew(originalNumKeys, -1);
  std::vector<bool> nonNullLiteral(originalNumKeys, false);
  for (size_t i = 0; i < originalNumKeys; ++i) {
    ExprCP rewritten = rewrittenKeys[i];
    if (isConstantNull(rewritten)) {
      outputSubstitutions.add(key.groupingKeyColumns[i], rewritten);
      continue;
    }

    oldToNew[i] = keptKeys.size();
    if (rewritten->is(PlanType::kLiteralExpr)) {
      VELOX_CHECK(key.groupingKeys[i]->isColumn());
      nonNullLiteral[i] = true;
      keptKeys.push_back(key.groupingKeys[i]);
    } else {
      keptKeys.push_back(rewritten);
    }
    keptKeyColumns.push_back(key.groupingKeyColumns[i]);
  }

  QGVector<QGVector<int32_t>> groupingSets;
  groupingSets.reserve(key.groupingSets.size());
  std::vector<bool> presentInEverySet(originalNumKeys, true);
  for (const auto& groupingSet : key.groupingSets) {
    std::vector<bool> present(originalNumKeys, false);
    QGVector<int32_t> keptSet;
    for (int32_t index : groupingSet) {
      VELOX_CHECK_GE(index, 0);
      VELOX_CHECK_LT(index, originalNumKeys);
      present[index] = true;
      if (oldToNew[index] >= 0) {
        keptSet.push_back(oldToNew[index]);
      }
    }
    for (size_t i = 0; i < originalNumKeys; ++i) {
      presentInEverySet[i] = presentInEverySet[i] && present[i];
    }
    groupingSets.push_back(std::move(keptSet));
  }

  // Non-NULL constants must remain columns because a set that omits one emits
  // NULL. Restore them immediately below GroupId. They are reportable only
  // when every set contains the key.
  ExprVector keptOriginals;
  ExprVector keptReplacements;
  keptOriginals.reserve(keptKeys.size());
  keptReplacements.reserve(keptKeys.size());
  for (size_t i = 0; i < originalNumKeys; ++i) {
    if (oldToNew[i] >= 0) {
      keptOriginals.push_back(key.groupingKeys[i]);
      keptReplacements.push_back(rewrittenKeys[i]);
    }
    if (nonNullLiteral[i] && presentInEverySet[i]) {
      outputSubstitutions.add(key.groupingKeyColumns[i], rewrittenKeys[i]);
    }
  }
  keptKeys = restoreColumnPositions(key.input, keptOriginals, keptReplacements);

  ExprVector aggregationInputs =
      simplifyExpressions(input.substitutions, key.aggregationInputs);
  ExprVector keptAggregationInputs;
  ColumnVector keptAggregationOutputs;
  keptAggregationInputs.reserve(aggregationInputs.size());
  keptAggregationOutputs.reserve(aggregationInputs.size());
  for (size_t i = 0; i < aggregationInputs.size(); ++i) {
    ExprCP rewritten = aggregationInputs[i];
    ColumnCP originalOutput = key.outputColumns[originalNumKeys + i];
    if (rewritten->is(PlanType::kLiteralExpr)) {
      outputSubstitutions.add(originalOutput, rewritten);
      continue;
    }
    VELOX_CHECK(rewritten->isColumn());
    ColumnCP rewrittenColumn = rewritten->as<Column>();
    keptAggregationInputs.push_back(rewrittenColumn);
    keptAggregationOutputs.push_back(rewrittenColumn);
    if (originalOutput != rewrittenColumn) {
      outputSubstitutions.add(originalOutput, rewrittenColumn);
    }
  }

  key.aggregationInputs = std::move(keptAggregationInputs);
  key.groupingKeys = std::move(keptKeys);
  key.groupingKeyColumns = std::move(keptKeyColumns);
  key.groupingSets = std::move(groupingSets);

  ColumnVector outputColumns = key.groupingKeyColumns;
  appendAll(outputColumns, keptAggregationOutputs);
  outputColumns.push_back(key.groupId);
  key.outputColumns = std::move(outputColumns);

  NodeCP output = builder_.make<GroupId>(std::move(key));
  outputSubstitutions.retainVisible(output->outputColumns(), exprs_);
  return {output, std::move(outputSubstitutions)};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    Window::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    return emptyResult(std::move(key.outputColumns));
  }
  if (key.functions.empty()) {
    return input;
  }

  key.input = input.node;
  key.partitionKeys =
      simplifyExpressions(input.substitutions, key.partitionKeys);
  std::erase_if(key.partitionKeys, [](ExprCP partitionKey) {
    return partitionKey->is(PlanType::kLiteralExpr);
  });

  key.orderKeys = simplifyExpressions(input.substitutions, key.orderKeys);
  const auto isOffsetBound = [](logical_plan::WindowExpr::BoundType bound) {
    return bound == logical_plan::WindowExpr::BoundType::kPreceding ||
        bound == logical_plan::WindowExpr::BoundType::kFollowing;
  };
  for (auto& function : key.functions) {
    function.call = input.substitutions.apply(function.call, exprs_);
    const auto simplifyBound = [&](ExprCP& value,
                                   logical_plan::WindowExpr::BoundType type) {
      if (value == nullptr) {
        return;
      }
      ExprCP replacement =
          simplifier_.simplify(input.substitutions.apply(value, exprs_));
      if (function.frame.type == logical_plan::WindowExpr::WindowType::kRange &&
          isOffsetBound(type)) {
        value = restoreColumnPositions(
            key.input, ExprVector{value}, ExprVector{replacement})[0];
      } else {
        value = replacement;
      }
    };
    simplifyBound(function.frame.startValue, function.frame.startType);
    simplifyBound(function.frame.endValue, function.frame.endType);
  }

  const bool hasRangeOffset =
      std::ranges::any_of(key.functions, [&](const WindowFunction& function) {
        return function.frame.type ==
            logical_plan::WindowExpr::WindowType::kRange &&
            (isOffsetBound(function.frame.startType) ||
             isOffsetBound(function.frame.endType));
      });
  const bool hasConstantOrderKey = key.orderKeys.size() == 1 &&
      key.orderKeys.front()->is(PlanType::kLiteralExpr);
  // Every row is a peer when the sole RANGE key is constant. Offset frames
  // retain their key so Velox validates the offset value.
  if (hasConstantOrderKey && !hasRangeOffset) {
    for (auto& function : key.functions) {
      if (function.frame.type == logical_plan::WindowExpr::WindowType::kRange) {
        function.frame = Frame::wholePartition();
      }
    }
  }
  if (!(hasConstantOrderKey && hasRangeOffset)) {
    simplifyOrderKeys({}, key.orderKeys, key.orderTypes);
  }

  if (key.orderKeys.empty()) {
    for (auto& function : key.functions) {
      if (function.frame.type != logical_plan::WindowExpr::WindowType::kRange) {
        continue;
      }
      if (function.frame.startType ==
          logical_plan::WindowExpr::BoundType::kCurrentRow) {
        function.frame.startType =
            logical_plan::WindowExpr::BoundType::kUnboundedPreceding;
      }
      if (function.frame.endType ==
          logical_plan::WindowExpr::BoundType::kCurrentRow) {
        function.frame.endType =
            logical_plan::WindowExpr::BoundType::kUnboundedFollowing;
      }
    }
  }

  PrecomputeProjections precompute{key.input, builder_, simplifier_};
  for (ExprCP& partitionKey : key.partitionKeys) {
    partitionKey = precompute.toColumn(partitionKey);
  }
  for (ExprCP& orderKey : key.orderKeys) {
    orderKey = precompute.toColumn(orderKey);
  }
  VELOX_CHECK_GE(key.outputColumns.size(), key.functions.size());
  ColumnVector functionOutputs(
      key.outputColumns.end() - key.functions.size(), key.outputColumns.end());
  key.input = std::move(precompute).node();
  key.outputColumns = key.input->outputColumns();
  appendAll(key.outputColumns, functionOutputs);

  NodeCP output = builder_.make<Window>(std::move(key));
  input.substitutions =
      visibleSubstitutions(std::move(input.substitutions), output);
  input.node = output;
  return input;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    Inference::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    return emptyResult(std::move(key.outputColumns));
  }
  key.input = input.node;
  key.call = input.substitutions.apply(key.call, exprs_);
  key.outputColumns = input.node->outputColumns();
  key.outputColumns.push_back(key.result);
  NodeCP output = builder_.make<Inference>(std::move(key));
  input.substitutions =
      visibleSubstitutions(std::move(input.substitutions), output);
  input.node = output;
  return input;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    RowNumber::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    return emptyResult(std::move(key.outputColumns));
  }

  key.input = input.node;
  key.partitionKeys =
      simplifyExpressions(input.substitutions, key.partitionKeys);
  folly::F14FastSet<ExprCP> seen;
  std::erase_if(key.partitionKeys, [&](ExprCP partitionKey) {
    return partitionKey->is(PlanType::kLiteralExpr) ||
        !seen.insert(partitionKey).second;
  });

  key.outputColumns = input.node->outputColumns();
  if (key.rankColumn != nullptr) {
    key.outputColumns.push_back(key.rankColumn);
  }

  NodeCP output = input.node;
  if (key.rankColumn != nullptr || key.limit.has_value()) {
    output = builder_.make<RowNumber>(std::move(key));
  }
  input.substitutions =
      visibleSubstitutions(std::move(input.substitutions), output);
  input.node = output;
  return input;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    TopNRowNumber::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    return emptyResult(std::move(key.outputColumns));
  }

  key.partitionKeys =
      simplifyExpressions(input.substitutions, key.partitionKeys);
  folly::F14FastSet<ExprCP> seenPartitions;
  std::erase_if(key.partitionKeys, [&](ExprCP partitionKey) {
    return partitionKey->is(PlanType::kLiteralExpr) ||
        !seenPartitions.insert(partitionKey).second;
  });
  simplifyOrderKeys(input.substitutions, key.orderKeys, key.orderTypes);

  if (key.orderKeys.empty()) {
    if (key.rankFunction == TopNRowNumber::RankFunction::kRowNumber) {
      NodeCP inputNode = input.node;
      return make(
          RowNumber::Key{
              inputNode,
              std::move(key.partitionKeys),
              key.limit,
              key.rankColumn,
              std::move(key.outputColumns),
          },
          std::move(input));
    }
    if (key.rankColumn != nullptr) {
      input.substitutions.add(
          key.rankColumn,
          builder_.makeLiteral(
              velox::Variant(int64_t{1}), key.rankColumn->value().type));
    }
    input.substitutions =
        visibleSubstitutions(std::move(input.substitutions), input.node);
    return input;
  }

  key.input = input.node;
  key.outputColumns = input.node->outputColumns();
  if (key.rankColumn != nullptr) {
    key.outputColumns.push_back(key.rankColumn);
  }
  NodeCP output = builder_.make<TopNRowNumber>(std::move(key));
  input.substitutions =
      visibleSubstitutions(std::move(input.substitutions), output);
  input.node = output;
  return input;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    Unnest::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    return emptyResult(std::move(key.outputColumns));
  }
  key.input = input.node;
  key.unnestExpressions =
      simplifyExpressions(input.substitutions, key.unnestExpressions);

  PlanSubstitutions outputSubstitutions{input.substitutions};
  folly::F14FastMap<ColumnCP, ExprCP> rewrittenReplicated;
  ColumnVector replicatedColumns;
  PlanObjectSet addedReplicated;
  replicatedColumns.reserve(key.replicatedColumns.size());
  for (ColumnCP column : key.replicatedColumns) {
    ExprCP replacement = input.substitutions.apply(column, exprs_);
    rewrittenReplicated.emplace(column, replacement);
    if (replacement->is(PlanType::kLiteralExpr)) {
      outputSubstitutions.add(column, replacement);
    } else {
      VELOX_CHECK(replacement->isColumn());
      ColumnCP replacementColumn = replacement->as<Column>();
      if (!addedReplicated.contains(replacementColumn)) {
        replicatedColumns.push_back(replacementColumn);
        addedReplicated.add(replacementColumn);
      }
      if (replacementColumn != column) {
        outputSubstitutions.add(column, replacementColumn);
      }
    }
  }
  key.replicatedColumns = std::move(replicatedColumns);

  ColumnVector outputColumns;
  PlanObjectSet addedOutputs;
  outputColumns.reserve(key.outputColumns.size());
  for (ColumnCP column : key.outputColumns) {
    const auto it = rewrittenReplicated.find(column);
    if (it == rewrittenReplicated.end()) {
      if (!addedOutputs.contains(column)) {
        outputColumns.push_back(column);
        addedOutputs.add(column);
      }
    } else if (it->second->isColumn()) {
      ColumnCP replacement = it->second->as<Column>();
      if (!addedOutputs.contains(replacement)) {
        outputColumns.push_back(replacement);
        addedOutputs.add(replacement);
      }
    }
  }
  key.outputColumns = std::move(outputColumns);

  NodeCP cardinalitySource = input.node;
  while (cardinalitySource->is(NodeType::kProject)) {
    cardinalitySource = cardinalitySource->as<Project>()->input();
  }
  if (Values::isSingleRowNoColumns(cardinalitySource)) {
    bool singletonArrays = true;
    PlanSubstitutions singletonSubstitutions;
    for (size_t i = 0; i < key.unnestExpressions.size(); ++i) {
      ExprCP expression = key.unnestExpressions[i];
      if (!expression->is(PlanType::kLiteralExpr) ||
          expression->value().type->kind() != velox::TypeKind::ARRAY ||
          expression->as<Literal>()->literal().isNull() ||
          expression->as<Literal>()->literal().array().size() != 1 ||
          key.unnestColumns[i].size() != 1) {
        singletonArrays = false;
        break;
      }
      ColumnCP output = key.unnestColumns[i].front();
      singletonSubstitutions.add(
          output,
          builder_.makeLiteral(
              velox::Variant(
                  expression->as<Literal>()->literal().array().front()),
              output->value().type));
    }
    if (singletonArrays) {
      if (key.ordinalityColumn != nullptr) {
        singletonSubstitutions.add(
            key.ordinalityColumn,
            builder_.makeLiteral(
                velox::Variant(int64_t{1}),
                key.ordinalityColumn->value().type));
      }
      if (key.markerColumn != nullptr) {
        singletonSubstitutions.add(
            key.markerColumn, builder_.makeBoolean(true));
      }
      outputSubstitutions.merge(singletonSubstitutions);
      outputSubstitutions.retainVisible(input.node->outputColumns(), exprs_);
      return {input.node, std::move(outputSubstitutions)};
    }
  }

  NodeCP output = builder_.make<Unnest>(std::move(key));
  outputSubstitutions.retainVisible(output->outputColumns(), exprs_);
  return {output, std::move(outputSubstitutions)};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    EnforceDistinct::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    return input;
  }

  key.input = input.node;
  ExprVector rewrittenKeys =
      simplifyExpressions(input.substitutions, key.distinctKeys);

  ExprVector keptOriginals;
  ExprVector keptKeys;
  folly::F14FastSet<ExprCP> seen;
  for (size_t i = 0; i < rewrittenKeys.size(); ++i) {
    ExprCP rewritten = rewrittenKeys[i];
    if (!rewritten->is(PlanType::kLiteralExpr) &&
        seen.insert(rewritten).second) {
      keptOriginals.push_back(key.distinctKeys[i]);
      keptKeys.push_back(rewritten);
    }
  }

  // The node needs one key to express the assertion that the whole input has
  // at most one row.
  if (keptKeys.empty()) {
    VELOX_CHECK(!key.distinctKeys.empty());
    keptOriginals.push_back(key.distinctKeys.front());
    keptKeys.push_back(rewrittenKeys.front());
  }
  key.distinctKeys = restoreColumnPositions(key.input, keptOriginals, keptKeys);

  NodeCP output = builder_.make<EnforceDistinct>(std::move(key));
  input.substitutions =
      visibleSubstitutions(std::move(input.substitutions), output);
  input.node = output;
  return input;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    MarkDistinct::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    return emptyResult(std::move(key.outputColumns));
  }

  key.input = input.node;
  const ExprVector rewrittenKeys =
      simplifyExpressions(input.substitutions, key.distinctKeys);
  ExprVector originalKeys;
  ExprVector keptKeys;
  folly::F14FastSet<ExprCP> seen;
  for (size_t i = 0; i < rewrittenKeys.size(); ++i) {
    ExprCP rewritten = rewrittenKeys[i];
    if (rewritten->is(PlanType::kLiteralExpr) ||
        !seen.insert(rewritten).second) {
      continue;
    }
    originalKeys.push_back(key.distinctKeys[i]);
    keptKeys.push_back(rewritten);
  }

  if (keptKeys.empty()) {
    VELOX_CHECK(!rewrittenKeys.empty());
    ExprCP rewritten = rewrittenKeys.front();
    ExprCP original = key.distinctKeys.front();
    if (original->isColumn()) {
      key.distinctKeys =
          restoreColumnPositions(key.input, {original}, {rewritten});
    } else {
      PrecomputeProjections precompute{
          key.input, builder_, simplifier_, /*projectAllInputs=*/true};
      key.distinctKeys = {precompute.toColumn(rewritten)};
      key.input = std::move(precompute).node();
    }
  } else {
    key.distinctKeys =
        restoreColumnPositions(key.input, originalKeys, keptKeys);
  }
  key.masks = restoreColumnPositions(key.input, key.masks, input.substitutions);
  key.outputColumns = key.input->outputColumns();
  appendAll(key.outputColumns, key.markers);

  NodeCP output = builder_.make<MarkDistinct>(std::move(key));
  input.substitutions =
      visibleSubstitutions(std::move(input.substitutions), output);
  input.node = output;
  return input;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    AssignUniqueId::Key key,
    SimplifiedNode input) {
  if (input.empty()) {
    ColumnVector columns = input.outputColumns();
    columns.push_back(key.idColumn);
    return emptyResult(std::move(columns));
  }
  key.input = input.node;
  NodeCP output = builder_.make<AssignUniqueId>(std::move(key));
  input.substitutions =
      visibleSubstitutions(std::move(input.substitutions), output);
  input.node = output;
  return input;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    EnforceSingleRow::Key key,
    SimplifiedNode input) {
  const ColumnVector sourceColumns = input.originalColumns.empty()
      ? key.input->outputColumns()
      : std::move(input.originalColumns);
  VELOX_CHECK_EQ(sourceColumns.size(), key.outputColumns.size());
  PlanSubstitutions outputSubstitutions;
  if (input.empty()) {
    for (ColumnCP output : key.outputColumns) {
      outputSubstitutions.add(
          output,
          builder_.makeLiteral(
              velox::Variant::null(output->value().type->kind()),
              output->value().type));
    }
    return {
        builder_.makeSingleRowValues({}, {}), std::move(outputSubstitutions)};
  }

  ExprVector keptReplacements;
  ColumnVector keptSources;
  ColumnVector keptOutputs;
  for (size_t i = 0; i < sourceColumns.size(); ++i) {
    ExprCP replacement = input.substitutions.apply(sourceColumns[i], exprs_);
    if (isConstantNull(replacement)) {
      outputSubstitutions.add(key.outputColumns[i], replacement);
      continue;
    }
    keptSources.push_back(sourceColumns[i]);
    keptReplacements.push_back(replacement);
    keptOutputs.push_back(key.outputColumns[i]);
  }

  key.input = input.node;
  ExprVector sourceExpressions =
      restoreColumnPositions(key.input, toExprs(keptSources), keptReplacements);
  ColumnVector restoredSources = toColumns(sourceExpressions);
  if (!std::ranges::equal(restoredSources, key.input->outputColumns())) {
    key.input = PrecomputeProjections::makeProject(
        key.input,
        std::move(sourceExpressions),
        restoredSources,
        builder_,
        simplifier_);
  }

  for (size_t i = 0; i < restoredSources.size(); ++i) {
    if (restoredSources[i] != keptSources[i]) {
      ColumnCP output = Column::createForNullExtendedValue(restoredSources[i]);
      outputSubstitutions.add(keptOutputs[i], output);
      keptOutputs[i] = output;
    }
  }
  key.outputColumns = std::move(keptOutputs);
  NodeCP output = builder_.make<EnforceSingleRow>(std::move(key));
  outputSubstitutions.retainVisible(output->outputColumns(), exprs_);
  return {output, std::move(outputSubstitutions)};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    TableWrite::Key key,
    SimplifiedNode input,
    const ColumnVector& originalOutputs) {
  if (input.empty()) {
    key.input = builder_.makeEmptyValues(toColumns(key.columnExprs));
  } else {
    key.input = input.node;
    if (key.kind != connector::WriteKind::kDelete) {
      const ColumnVector writtenColumns = toColumns(key.columnExprs);
      key.input =
          restoreExactLayout(key.input, writtenColumns, input.substitutions);
    }
  }

  SimplifiedNode result{builder_.make<TableWrite>(std::move(key)), {}};
  if (!originalOutputs.empty()) {
    VELOX_CHECK_EQ(originalOutputs.size(), result.node->outputColumns().size());
    for (size_t i = 0; i < originalOutputs.size(); ++i) {
      result.substitutions.add(
          originalOutputs[i], result.node->outputColumns()[i]);
    }
  }
  return result;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    FixedPoint::Key key,
    SimplifiedNode anchor,
    SimplifiedNode step,
    SimplifiedNode convergence) {
  // The loop runs its step at least once, and a step can produce rows from an
  // empty state, so an empty anchor does not make the result empty.
  const ColumnVector stepColumns =
      step.empty() ? step.originalColumns : key.step->outputColumns();
  key.anchor = anchor.node == nullptr
      ? materialize(anchor)
      : restoreExactLayout(
            anchor.node, key.sourceColumns, anchor.substitutions);
  VELOX_USER_CHECK(
      step.empty() || step.node->requiredStates().contains(key.name),
      "FixedPoint step does not depend on the loop's state");
  if (step.empty()) {
    if (key.mode == FixedPointMode::kReplace) {
      return emptyResult(std::move(key.outputColumns));
    }
    VELOX_CHECK_EQ(key.outputColumns.size(), key.sourceColumns.size());
    PlanSubstitutions outputSubstitutions;
    for (size_t i = 0; i < key.outputColumns.size(); ++i) {
      outputSubstitutions.add(key.outputColumns[i], key.sourceColumns[i]);
    }
    return {key.anchor, std::move(outputSubstitutions)};
  }
  key.step = restoreExactLayout(step.node, stepColumns, step.substitutions);
  if (key.convergence != nullptr) {
    VELOX_USER_CHECK(
        !convergence.empty() &&
            convergence.node->requiredStates().contains(key.name),
        "FixedPoint convergence does not depend on the loop's state");
    key.convergence = restoreExactLayout(
        convergence.node,
        key.convergence->outputColumns(),
        convergence.substitutions);
  }
  return {builder_.make<FixedPoint>(key), {}};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    Repeat::Key key,
    SimplifiedNode start,
    SimplifiedNode hop,
    std::optional<SimplifiedNode> end) {
  if (start.empty() || (end.has_value() && end->empty()) ||
      (hop.empty() && key.minHops > 0)) {
    return emptyResult(std::move(key.outputColumns));
  }
  // Without hops, only the walks of zero hops remain.
  if (hop.empty()) {
    key.maxHops = 0;
    hop = {builder_.makeEmptyValues(key.hop->outputColumns()), {}};
  }

  key.startKeys = start.substitutions.apply(key.startKeys, exprs_);
  key.hopSourceKeys = hop.substitutions.apply(key.hopSourceKeys, exprs_);
  key.hopDestinationKeys =
      hop.substitutions.apply(key.hopDestinationKeys, exprs_);
  key.endKeys = (end.has_value() ? end->substitutions : start.substitutions)
                    .apply(key.endKeys, exprs_);

  // An input outputs the columns the Repeat passes on from it and the columns
  // its keys read, restored to their identities where they are missing.
  const auto sideLayout = [&](NodeCP original,
                              const SimplifiedNode& side,
                              std::initializer_list<const ExprVector*> keys) {
    ColumnVector layout = original->outputsAmong(key.effectiveSourceColumns());
    PlanObjectSet keyColumns;
    for (const ExprVector* sideKeys : keys) {
      keyColumns.unionColumns(*sideKeys);
    }
    keyColumns.eraseAll(layout);
    keyColumns.forEach<Column>(
        [&](ColumnCP column) { layout.push_back(column); });
    const auto available =
        PlanObjectSet::fromObjects(side.node->outputColumns());
    const bool complete =
        std::all_of(layout.begin(), layout.end(), [&](ColumnCP column) {
          return available.contains(column);
        });
    return complete ? side.node
                    : restoreExactLayout(side.node, layout, side.substitutions);
  };
  if (end.has_value()) {
    key.start = sideLayout(key.start, start, {&key.startKeys});
    key.end = sideLayout(key.end, *end, {&key.endKeys});
  } else {
    key.start = sideLayout(key.start, start, {&key.startKeys, &key.endKeys});
  }
  key.hop =
      sideLayout(key.hop, hop, {&key.hopSourceKeys, &key.hopDestinationKeys});
  return {builder_.make<Repeat>(key), {}};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    UnionAll::Key key,
    std::vector<SimplifiedNode> inputs) {
  VELOX_CHECK_EQ(key.inputs.size(), inputs.size());
  VELOX_CHECK_EQ(key.legColumns.size(), inputs.size());

  NodeVector survivingInputs;
  QGVector<ColumnVector> survivingLegColumns;
  std::vector<SimplifiedNode> survivingResults;
  survivingInputs.reserve(inputs.size());
  survivingLegColumns.reserve(inputs.size());
  survivingResults.reserve(inputs.size());
  for (size_t i = 0; i < inputs.size(); ++i) {
    // PushdownAndPrune still materializes some known-empty branches as
    // zero-row Values. The nullptr form is NodeSimplifier's final empty
    // representation; accept Values during the caller migration.
    const bool empty = inputs[i].empty() ||
        (inputs[i].node->is(NodeType::kValues) &&
         inputs[i].node->as<Values>()->cardinality() == 0);
    if (empty) {
      continue;
    }
    survivingInputs.push_back(inputs[i].node);
    survivingLegColumns.push_back(std::move(key.legColumns[i]));
    survivingResults.push_back(std::move(inputs[i]));
  }

  if (survivingInputs.empty()) {
    return emptyResult(std::move(key.outputColumns));
  }

  PlanSubstitutions outputSubstitutions;
  std::vector<bool> commonLiteralPositions(key.outputColumns.size(), false);
  for (size_t position = 0; position < key.outputColumns.size(); ++position) {
    ExprCP commonLiteral{nullptr};
    bool allSameLiteral = true;
    for (size_t leg = 0; leg < survivingResults.size(); ++leg) {
      ExprCP replacement = survivingResults[leg].substitutions.apply(
          survivingLegColumns[leg][position], exprs_);
      if (!replacement->is(PlanType::kLiteralExpr) ||
          (commonLiteral != nullptr && replacement != commonLiteral)) {
        allSameLiteral = false;
        break;
      }
      commonLiteral = replacement;
    }
    if (allSameLiteral && commonLiteral != nullptr) {
      outputSubstitutions.add(key.outputColumns[position], commonLiteral);
      commonLiteralPositions[position] = true;
    }
  }

  if (survivingInputs.size() == 1) {
    for (size_t position = 0; position < key.outputColumns.size(); ++position) {
      ExprCP replacement = survivingResults.front().substitutions.apply(
          survivingLegColumns.front()[position], exprs_);
      outputSubstitutions.addIfAbsent(key.outputColumns[position], replacement);
    }
    outputSubstitutions.retainVisible(
        survivingInputs.front()->outputColumns(), exprs_);
    return {survivingInputs.front(), std::move(outputSubstitutions)};
  }

  ColumnVector keptOutputs;
  QGVector<ColumnVector> keptLegColumns(survivingLegColumns.size());
  keptOutputs.reserve(key.outputColumns.size());
  for (size_t position = 0; position < key.outputColumns.size(); ++position) {
    if (commonLiteralPositions[position]) {
      continue;
    }
    keptOutputs.push_back(key.outputColumns[position]);
    for (size_t leg = 0; leg < survivingLegColumns.size(); ++leg) {
      keptLegColumns[leg].push_back(survivingLegColumns[leg][position]);
    }
  }
  key.outputColumns = std::move(keptOutputs);
  survivingLegColumns = std::move(keptLegColumns);

  for (size_t leg = 0; leg < survivingInputs.size(); ++leg) {
    NodeCP input = survivingInputs[leg];
    survivingLegColumns[leg] = restoreColumnPositions(
        input, survivingLegColumns[leg], survivingResults[leg].substitutions);
    survivingInputs[leg] = input;
  }

  NodeCP output = builder_.make<UnionAll>(
      {std::move(survivingInputs),
       std::move(survivingLegColumns),
       std::move(key.outputColumns)});
  outputSubstitutions.retainVisible(output->outputColumns(), exprs_);
  return {output, std::move(outputSubstitutions)};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    IndexLookupJoin::Key key,
    SimplifiedNode probe) {
  if (probe.empty()) {
    return emptyResult(std::move(key.outputColumns));
  }
  key.probe = restoreExactLayout(
      probe.node, key.probe->outputColumns(), probe.substitutions);
  return {builder_.make<IndexLookupJoin>(std::move(key)), {}};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::make(
    Apply::Key key,
    SimplifiedNode input,
    SimplifiedNode body) {
  VELOX_CHECK(
      key.kind == velox::core::JoinType::kInner ||
      key.kind == velox::core::JoinType::kLeft ||
      key.kind == velox::core::JoinType::kLeftSemiProject);
  if (input.empty()) {
    return emptyResult(std::move(key.outputColumns));
  }

  const ColumnVector inputColumns = key.input->outputColumns();
  const ColumnVector bodyColumns =
      body.empty() ? body.originalColumns : key.body->outputColumns();
  const auto inputSet = PlanObjectSet::fromObjects(inputColumns);
  const auto bodySet = PlanObjectSet::fromObjects(bodyColumns);
  const ColumnVector sourceColumns = key.effectiveSourceColumns();
  VELOX_CHECK_EQ(sourceColumns.size(), key.outputColumns.size());

  const auto withoutBody = [&]() -> SimplifiedNode {
    if (key.kind == velox::core::JoinType::kInner) {
      return emptyResult(key.outputColumns);
    }
    PlanSubstitutions substitutions;
    for (size_t i = 0; i < sourceColumns.size(); ++i) {
      ExprCP replacement;
      if (inputSet.contains(sourceColumns[i])) {
        replacement = input.substitutions.apply(sourceColumns[i], exprs_);
      } else {
        const bool isMark =
            key.kind == velox::core::JoinType::kLeftSemiProject &&
            sourceColumns[i] == key.markColumn;
        replacement = isMark ? static_cast<ExprCP>(builder_.makeBoolean(false))
                             : static_cast<ExprCP>(builder_.makeNull(
                                   key.outputColumns[i]->value().type));
      }
      if (key.outputColumns[i] != replacement) {
        substitutions.add(key.outputColumns[i], replacement);
      }
    }
    substitutions.retainVisible(input.node->outputColumns(), exprs_);
    return {input.node, std::move(substitutions)};
  };
  if (body.empty()) {
    return withoutBody();
  }

  PlanSubstitutions allSubstitutions{input.substitutions};
  allSubstitutions.merge(body.substitutions);
  ExprVector filter;
  for (ExprCP conjunct : allSubstitutions.apply(key.filter, exprs_)) {
    if (simplifier_.simplifyFilter(conjunct, filter)) {
      return withoutBody();
    }
  }
  key.filter = std::move(filter);

  key.input = input.node;
  if (!key.correlationColumns.empty()) {
    key.correlationColumns = restoreColumnPositions(
        key.input, key.correlationColumns, input.substitutions);
  }
  if (key.inLhs != nullptr) {
    key.inLhs =
        simplifier_.simplify(input.substitutions.apply(key.inLhs, exprs_));
  }

  key.body = body.node;
  if (key.inBodyKey != nullptr) {
    key.inBodyKey = restoreColumnPositions(
        key.body,
        ExprVector{key.inBodyKey},
        simplifyExpressions(body.substitutions, {key.inBodyKey}))[0];
  }
  key.body = restoreExactLayout(key.body, bodyColumns, body.substitutions);

  PlanSubstitutions outputSubstitutions;
  for (size_t i = 0; i < sourceColumns.size(); ++i) {
    ExprCP replacement{nullptr};
    if (inputSet.contains(sourceColumns[i])) {
      replacement = input.substitutions.apply(sourceColumns[i], exprs_);
    } else if (bodySet.contains(sourceColumns[i])) {
      replacement = body.substitutions.apply(sourceColumns[i], exprs_);
      if (key.kind == velox::core::JoinType::kLeft &&
          !isConstantNull(replacement)) {
        continue;
      }
    }
    if (replacement != nullptr && key.outputColumns[i] != replacement) {
      outputSubstitutions.add(key.outputColumns[i], replacement);
    }
  }

  NodeCP output = builder_.make<Apply>(std::move(key));
  outputSubstitutions.retainVisible(output->outputColumns(), exprs_);
  return {output, std::move(outputSubstitutions)};
}

void NodeSimplifier::precomputeCrossJoinFilter(
    NodeCP& left,
    NodeCP& right,
    ExprVector& filter,
    const ColumnVector& sourceColumns) {
  if (filter.empty()) {
    return;
  }
  PrecomputeProjections leftPrecompute{
      left, builder_, simplifier_, /*projectAllInputs=*/false};
  PrecomputeProjections rightPrecompute{
      right, builder_, simplifier_, /*projectAllInputs=*/false};
  const auto leftColumns = PlanObjectSet::fromObjects(left->outputColumns());
  const auto rightColumns = PlanObjectSet::fromObjects(right->outputColumns());
  PlanObjectSet inputColumns{leftColumns};
  inputColumns.unionSet(rightColumns);
  for (ExprCP conjunct : filter) {
    if (!inputColumns.containsColumns(conjunct)) {
      return;
    }
  }

  for (ColumnCP column : sourceColumns) {
    if (leftColumns.contains(column)) {
      leftPrecompute.toColumn(column);
    } else if (rightColumns.contains(column)) {
      rightPrecompute.toColumn(column);
    }
  }

  JoinFilterRewriter rewriter{
      leftPrecompute, rightPrecompute, leftColumns, rightColumns, builder_};
  filter = rewriter.rewrite(filter);
  left = std::move(leftPrecompute).node();
  right = std::move(rightPrecompute).node();
}

void NodeSimplifier::addJoinKeySubstitutions(
    velox::core::JoinType joinType,
    const ExprVector& leftKeys,
    const ExprVector& rightKeys,
    PlanSubstitutions& substitutions) {
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());
  const auto addRewrite = [&](ExprCP source, ExprCP target) {
    if (source != target) {
      substitutions.add(source, target);
    }
  };

  if (joinType == velox::core::JoinType::kFull) {
    for (size_t i = 0; i < leftKeys.size(); ++i) {
      ExprCP leftKey = leftKeys[i];
      ExprCP rightKey = rightKeys[i];
      if (leftKey->containsNonDefaultNullBehavior() ||
          rightKey->containsNonDefaultNullBehavior()) {
        continue;
      }
      ExprCP canonical = builder_.canonicalizeCoalesce(leftKey, rightKey);
      addRewrite(exprs_.makeCoalesce(leftKey, rightKey), canonical);
      addRewrite(exprs_.makeCoalesce(rightKey, leftKey), canonical);
    }
    return;
  }

  if (joinType != velox::core::JoinType::kInner &&
      joinType != velox::core::JoinType::kLeft &&
      joinType != velox::core::JoinType::kRight) {
    return;
  }
  const bool selectLeft = joinType != velox::core::JoinType::kRight;
  for (size_t i = 0; i < leftKeys.size(); ++i) {
    ExprCP leftKey = leftKeys[i];
    ExprCP rightKey = rightKeys[i];
    if (joinType == velox::core::JoinType::kInner && leftKey->isColumn() &&
        rightKey->isColumn()) {
      substitutions.addIfAbsent(rightKey, leftKey);
      rightKey = substitutions.apply(rightKey, exprs_);
    }
    ExprCP discarded = selectLeft ? rightKey : leftKey;
    if (joinType != velox::core::JoinType::kInner &&
        discarded->containsNonDefaultNullBehavior()) {
      continue;
    }

    ExprCP selected = selectLeft ? leftKey : rightKey;
    addRewrite(exprs_.makeCoalesce(leftKey, rightKey), selected);
    addRewrite(exprs_.makeCoalesce(rightKey, leftKey), selected);
  }
}

void NodeSimplifier::restoreOutputLayout(
    NodeCP& node,
    ColumnVector& outputColumns,
    const PlanSubstitutions& substitutions) {
  ColumnVector rewritten;
  rewritten.reserve(outputColumns.size());
  for (ColumnCP output : outputColumns) {
    ExprCP replacement = substitutions.apply(output, exprs_);
    if (!replacement->isColumn()) {
      node = restoreExactLayout(node, outputColumns, substitutions);
      return;
    }
    rewritten.push_back(replacement->as<Column>());
  }
  outputColumns = std::move(rewritten);
}

} // namespace facebook::axiom::optimizer::v2
