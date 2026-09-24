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

#include "axiom/optimizer/v2/JoinKeyCoalesceNormalizer.h"

#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/NodeRewriter.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

using CoalesceSubstitutions = ExprFactory::ExprSubstitution;

// Caches the rewritten subtree and the substitutions it proves for ancestors.
struct CoalesceRewriteResult {
  // Rewritten subtree.
  NodeCP node;
  // Substitutions valid at the subtree root.
  CoalesceSubstitutions substitutions;
};

// Synthesizes substitutions from each subtree and caches them by original node.
class JoinKeyCoalesceRewriter : public NodeRewriter<CoalesceSubstitutions> {
 public:
  explicit JoinKeyCoalesceRewriter(Builder& builder)
      : NodeRewriter(builder), exprs_(builder) {}

  using NodeRewriter::rewrite;

  NodeCP rewrite(NodeCP node, CoalesceSubstitutions& substitutions) override;

 private:
  NodeCP rewriteFilter(const Filter* node, CoalesceSubstitutions& substitutions)
      override;
  NodeCP rewriteProject(
      const Project* node,
      CoalesceSubstitutions& substitutions) override;
  NodeCP rewriteSort(const Sort* node, CoalesceSubstitutions& substitutions)
      override;
  NodeCP rewriteTopN(const TopN* node, CoalesceSubstitutions& substitutions)
      override;
  NodeCP rewriteAggregate(
      const Aggregate* node,
      CoalesceSubstitutions& substitutions) override;
  NodeCP rewriteGroupId(
      const GroupId* node,
      CoalesceSubstitutions& substitutions) override;
  NodeCP rewriteMarkDistinct(
      const MarkDistinct* node,
      CoalesceSubstitutions& substitutions) override;
  NodeCP rewriteUnnest(const Unnest* node, CoalesceSubstitutions& substitutions)
      override;
  NodeCP rewriteUnionAll(
      const UnionAll* node,
      CoalesceSubstitutions& substitutions) override;
  NodeCP rewriteJoin(const Join* node, CoalesceSubstitutions& substitutions)
      override;
  NodeCP rewriteWindow(const Window* node, CoalesceSubstitutions& substitutions)
      override;
  NodeCP rewriteInference(
      const Inference* node,
      CoalesceSubstitutions& substitutions) override;
  NodeCP rewriteRowNumber(
      const RowNumber* node,
      CoalesceSubstitutions& substitutions) override;
  NodeCP rewriteTopNRowNumber(
      const TopNRowNumber* node,
      CoalesceSubstitutions& substitutions) override;
  NodeCP rewriteApply(const Apply* node, CoalesceSubstitutions& substitutions)
      override;
  NodeCP rewriteEnforceDistinct(
      const EnforceDistinct* node,
      CoalesceSubstitutions& substitutions) override;
  NodeCP rewriteExchange(
      const Exchange* node,
      CoalesceSubstitutions& substitutions) override;
  NodeCP rewriteTableWrite(
      const TableWrite* node,
      CoalesceSubstitutions& substitutions) override;
  NodeCP rewriteFixedPoint(
      const FixedPoint* node,
      CoalesceSubstitutions& substitutions) override;

  ExprCP replaceAll(ExprCP expr, const CoalesceSubstitutions& substitutions);
  ExprVector replaceAll(
      const ExprVector& exprs,
      const CoalesceSubstitutions& substitutions);
  const optimizer::Aggregate* rewriteAggregateCall(
      const optimizer::Aggregate* aggregate,
      const CoalesceSubstitutions& substitutions);
  ExprCP rewriteWindowCall(
      ExprCP expr,
      const CoalesceSubstitutions& substitutions);
  void mergeSubstitutions(
      CoalesceSubstitutions& target,
      const CoalesceSubstitutions& source);
  void invalidateReboundColumns(
      CoalesceSubstitutions& substitutions,
      const PlanObjectSet& reboundColumns);

  ExprFactory exprs_;
  folly::F14FastMap<NodeCP, CoalesceRewriteResult> rewrittenCache_;
};

NodeCP JoinKeyCoalesceRewriter::rewrite(
    NodeCP node,
    CoalesceSubstitutions& substitutions) {
  substitutions.clear();
  if (const auto it = rewrittenCache_.find(node); it != rewrittenCache_.end()) {
    substitutions = it->second.substitutions;
    return it->second.node;
  }

  NodeCP rewritten = NodeRewriter::rewrite(node, substitutions);
  rewrittenCache_.emplace(
      node, CoalesceRewriteResult{rewritten, substitutions});
  return rewritten;
}

ExprCP JoinKeyCoalesceRewriter::replaceAll(
    ExprCP expr,
    const CoalesceSubstitutions& substitutions) {
  if (expr == nullptr || substitutions.empty()) {
    return expr;
  }
  while (true) {
    ExprCP replaced = exprs_.replace(expr, substitutions);
    if (replaced == expr) {
      return expr;
    }
    expr = replaced;
  }
}

ExprVector JoinKeyCoalesceRewriter::replaceAll(
    const ExprVector& exprs,
    const CoalesceSubstitutions& substitutions) {
  ExprVector rewritten;
  rewritten.reserve(exprs.size());
  for (ExprCP expr : exprs) {
    rewritten.push_back(replaceAll(expr, substitutions));
  }
  return rewritten;
}

void JoinKeyCoalesceRewriter::mergeSubstitutions(
    CoalesceSubstitutions& target,
    const CoalesceSubstitutions& source) {
  for (const auto& [expr, replacement] : source) {
    const auto it = target.find(expr);
    if (it == target.end()) {
      target.emplace(expr, replacement);
    } else if (it->second != replacement) {
      // Neither branch's representative is valid after the branches merge.
      target.erase(it);
    }
  }
}

void JoinKeyCoalesceRewriter::invalidateReboundColumns(
    CoalesceSubstitutions& substitutions,
    const PlanObjectSet& reboundColumns) {
  for (auto it = substitutions.begin(); it != substitutions.end();) {
    PlanObjectSet participating = it->first->columns();
    participating.unionSet(it->second->columns());
    if (participating.hasIntersection(reboundColumns)) {
      it = substitutions.erase(it);
    } else {
      ++it;
    }
  }
}

NodeCP JoinKeyCoalesceRewriter::rewriteFilter(
    const Filter* node,
    CoalesceSubstitutions& substitutions) {
  NodeCP input = rewrite(node->input(), substitutions);
  ExprVector predicates = replaceAll(node->predicates(), substitutions);
  if (input == node->input() && predicates == node->predicates()) {
    return node;
  }
  return builder().make<Filter>({
      .input = input,
      .predicates = std::move(predicates),
  });
}

NodeCP JoinKeyCoalesceRewriter::rewriteProject(
    const Project* node,
    CoalesceSubstitutions& substitutions) {
  NodeCP input = rewrite(node->input(), substitutions);
  ExprVector exprs = replaceAll(node->exprs(), substitutions);

  PlanObjectSet reboundColumns;
  for (size_t i = 0; i < exprs.size(); ++i) {
    if (exprs[i] != node->outputColumns()[i]) {
      reboundColumns.add(node->outputColumns()[i]);
    }
  }
  invalidateReboundColumns(substitutions, reboundColumns);

  if (input == node->input() && exprs == node->exprs()) {
    return node;
  }
  // Folding could expose expressions after this node's substitutions have
  // already been applied.
  return builder().make<Project>({
      .input = input,
      .exprs = std::move(exprs),
      .outputColumns = node->outputColumns(),
  });
}

NodeCP JoinKeyCoalesceRewriter::rewriteSort(
    const Sort* node,
    CoalesceSubstitutions& substitutions) {
  NodeCP input = rewrite(node->input(), substitutions);
  ExprVector orderKeys = replaceAll(node->orderKeys(), substitutions);
  if (input == node->input() && orderKeys == node->orderKeys()) {
    return node;
  }
  return builder().make<Sort>({
      .input = input,
      .orderKeys = std::move(orderKeys),
      .orderTypes = node->orderTypes(),
  });
}

NodeCP JoinKeyCoalesceRewriter::rewriteTopN(
    const TopN* node,
    CoalesceSubstitutions& substitutions) {
  NodeCP input = rewrite(node->input(), substitutions);
  ExprVector orderKeys = replaceAll(node->orderKeys(), substitutions);
  if (input == node->input() && orderKeys == node->orderKeys()) {
    return node;
  }
  return builder().make<TopN>({
      .input = input,
      .orderKeys = std::move(orderKeys),
      .orderTypes = node->orderTypes(),
      .offset = node->offset(),
      .count = node->count(),
  });
}

const optimizer::Aggregate* JoinKeyCoalesceRewriter::rewriteAggregateCall(
    const optimizer::Aggregate* aggregate,
    const CoalesceSubstitutions& substitutions) {
  ExprVector arguments = replaceAll(aggregate->args(), substitutions);
  ExprCP condition = replaceAll(aggregate->condition(), substitutions);
  ExprVector orderKeys = replaceAll(aggregate->orderKeys(), substitutions);
  const optimizer::Aggregate* fallback = aggregate->fallback();
  if (fallback != nullptr) {
    fallback = rewriteAggregateCall(fallback, substitutions);
  }
  if (arguments == aggregate->args() && condition == aggregate->condition() &&
      orderKeys == aggregate->orderKeys() &&
      fallback == aggregate->fallback()) {
    return aggregate;
  }

  return exprs_.rebuildAggregateCall(
      aggregate,
      std::move(arguments),
      condition,
      std::move(orderKeys),
      fallback);
}

NodeCP JoinKeyCoalesceRewriter::rewriteAggregate(
    const Aggregate* node,
    CoalesceSubstitutions& substitutions) {
  NodeCP input = rewrite(node->input(), substitutions);
  ExprVector groupingKeys = replaceAll(node->groupingKeys(), substitutions);
  AggregateCallVector aggregates;
  aggregates.reserve(node->aggregates().size());
  for (const optimizer::Aggregate* aggregate : node->aggregates()) {
    aggregates.push_back(rewriteAggregateCall(aggregate, substitutions));
  }

  PlanObjectSet reboundColumns;
  for (size_t i = 0; i < groupingKeys.size(); ++i) {
    if (groupingKeys[i] != node->outputColumns()[i]) {
      reboundColumns.add(node->outputColumns()[i]);
    }
  }
  for (size_t i = groupingKeys.size(); i < node->outputColumns().size(); ++i) {
    reboundColumns.add(node->outputColumns()[i]);
  }
  invalidateReboundColumns(substitutions, reboundColumns);

  if (input == node->input() && groupingKeys == node->groupingKeys() &&
      aggregates == node->aggregates()) {
    return node;
  }
  return builder().make<Aggregate>({
      .input = input,
      .groupingKeys = std::move(groupingKeys),
      .aggregates = std::move(aggregates),
      .outputColumns = node->outputColumns(),
      .step = node->step(),
      .groupId = node->groupId(),
      .globalGroupingSets = node->globalGroupingSets(),
  });
}

NodeCP JoinKeyCoalesceRewriter::rewriteGroupId(
    const GroupId* node,
    CoalesceSubstitutions& substitutions) {
  NodeCP input = rewrite(node->input(), substitutions);
  ExprVector groupingKeys = replaceAll(node->groupingKeys(), substitutions);
  ExprVector aggregationInputs =
      replaceAll(node->aggregationInputs(), substitutions);

  PlanObjectSet reboundColumns =
      PlanObjectSet::fromObjects(node->groupingKeyColumns());
  for (size_t i = 0; i < aggregationInputs.size(); ++i) {
    const size_t outputIndex = node->groupingKeyColumns().size() + i;
    if (aggregationInputs[i] != node->outputColumns()[outputIndex]) {
      reboundColumns.add(node->outputColumns()[outputIndex]);
    }
  }
  reboundColumns.add(node->groupId());
  invalidateReboundColumns(substitutions, reboundColumns);

  if (input == node->input() && groupingKeys == node->groupingKeys() &&
      aggregationInputs == node->aggregationInputs()) {
    return node;
  }
  return builder().make<GroupId>({
      .input = input,
      .groupingKeys = std::move(groupingKeys),
      .aggregationInputs = std::move(aggregationInputs),
      .groupingSets = node->groupingSets(),
      .groupingKeyColumns = node->groupingKeyColumns(),
      .groupId = node->groupId(),
      .outputColumns = node->outputColumns(),
  });
}

NodeCP JoinKeyCoalesceRewriter::rewriteMarkDistinct(
    const MarkDistinct* /*node*/,
    CoalesceSubstitutions& /*substitutions*/) {
  VELOX_UNREACHABLE("MarkDistinct is introduced after join-key coalesce");
}

NodeCP JoinKeyCoalesceRewriter::rewriteUnnest(
    const Unnest* node,
    CoalesceSubstitutions& substitutions) {
  NodeCP input = rewrite(node->input(), substitutions);
  ExprVector unnestExpressions =
      replaceAll(node->unnestExpressions(), substitutions);
  if (input == node->input() &&
      unnestExpressions == node->unnestExpressions()) {
    return node;
  }
  return builder().make<Unnest>({
      .input = input,
      .unnestExpressions = std::move(unnestExpressions),
      .replicatedColumns = node->replicatedColumns(),
      .unnestColumns = node->unnestColumns(),
      .ordinalityColumn = node->ordinalityColumn(),
      .markerColumn = node->markerColumn(),
      .outputColumns = node->outputColumns(),
  });
}

NodeCP JoinKeyCoalesceRewriter::rewriteUnionAll(
    const UnionAll* node,
    CoalesceSubstitutions& /*substitutions*/) {
  NodeVector inputs;
  inputs.reserve(node->inputs().size());
  bool changed = false;
  CoalesceSubstitutions inputContext;
  for (NodeCP input : node->inputs()) {
    NodeCP rewritten = rewrite(input, inputContext);
    changed |= rewritten != input;
    inputs.push_back(rewritten);
  }
  if (!changed) {
    return node;
  }
  return builder().make<UnionAll>({
      .inputs = std::move(inputs),
      .legColumns = node->legColumns(),
      .outputColumns = node->outputColumns(),
  });
}

bool supportsJoinKeyCoalesceRewrite(TypeCP type) {
  if (type->kind() == velox::TypeKind::REAL ||
      type->kind() == velox::TypeKind::DOUBLE ||
      type->providesCustomComparison()) {
    return false;
  }
  for (size_t i = 0; i < type->size(); ++i) {
    if (!supportsJoinKeyCoalesceRewrite(type->childAt(i).get())) {
      return false;
    }
  }
  return true;
}

bool isSideLocalDeterministicKey(ExprCP key, const PlanObjectSet& sideColumns) {
  return !key->columns().empty() && sideColumns.containsColumns(key) &&
      !key->containsNonDeterministic();
}

NodeCP JoinKeyCoalesceRewriter::rewriteJoin(
    const Join* node,
    CoalesceSubstitutions& substitutions) {
  CoalesceSubstitutions leftContext;
  CoalesceSubstitutions rightContext;
  NodeCP left = rewrite(node->left(), leftContext);
  NodeCP right = rewrite(node->right(), rightContext);

  ExprVector leftKeys = replaceAll(node->leftKeys(), leftContext);
  ExprVector rightKeys = replaceAll(node->rightKeys(), rightContext);
  substitutions = leftContext;
  mergeSubstitutions(substitutions, rightContext);
  ExprVector filter = replaceAll(node->filter(), substitutions);

  const bool canRewriteCoalesce =
      node->joinType() == velox::core::JoinType::kInner ||
      node->joinType() == velox::core::JoinType::kLeft ||
      node->joinType() == velox::core::JoinType::kRight;
  if (canRewriteCoalesce) {
    const auto leftColumns = PlanObjectSet::fromObjects(left->outputColumns());
    const auto rightColumns =
        PlanObjectSet::fromObjects(right->outputColumns());
    for (size_t i = 0; i < leftKeys.size(); ++i) {
      ExprCP leftKey = leftKeys[i];
      ExprCP rightKey = rightKeys[i];
      if (!isSideLocalDeterministicKey(leftKey, leftColumns) ||
          !isSideLocalDeterministicKey(rightKey, rightColumns)) {
        continue;
      }
      if (leftKey->value().type != rightKey->value().type ||
          !supportsJoinKeyCoalesceRewrite(leftKey->value().type)) {
        continue;
      }

      ExprCP replacement;
      if (node->joinType() == velox::core::JoinType::kLeft) {
        if (!rightKey->propagatesNullsFrom(rightColumns)) {
          continue;
        }
        replacement = leftKey;
      } else if (node->joinType() == velox::core::JoinType::kRight) {
        if (!leftKey->propagatesNullsFrom(leftColumns)) {
          continue;
        }
        replacement = rightKey;
      } else {
        replacement = leftKey;
      }
      substitutions.insert_or_assign(
          exprs_.makeCoalesce(leftKey, rightKey), replacement);
      substitutions.insert_or_assign(
          exprs_.makeCoalesce(rightKey, leftKey), replacement);
    }
  }

  if (left == node->left() && right == node->right() &&
      leftKeys == node->leftKeys() && rightKeys == node->rightKeys() &&
      filter == node->filter()) {
    return node;
  }
  return builder().make<Join>({
      .left = left,
      .right = right,
      .joinType = node->joinType(),
      .leftKeys = std::move(leftKeys),
      .rightKeys = std::move(rightKeys),
      .filter = std::move(filter),
      .nullAware = node->nullAware(),
      .nullAsValue = node->nullAsValue(),
      .outputColumns = node->outputColumns(),
  });
}

ExprCP JoinKeyCoalesceRewriter::rewriteWindowCall(
    ExprCP expr,
    const CoalesceSubstitutions& substitutions) {
  const auto* call = expr->as<Call>();
  ExprVector arguments = replaceAll(call->args(), substitutions);
  if (arguments == call->args()) {
    return call;
  }
  return exprs_.rebuildWindowCall(call, std::move(arguments));
}

NodeCP JoinKeyCoalesceRewriter::rewriteWindow(
    const Window* node,
    CoalesceSubstitutions& substitutions) {
  NodeCP input = rewrite(node->input(), substitutions);
  WindowFunctions functions;
  functions.reserve(node->functions().size());
  bool functionsChanged = false;
  for (const auto& function : node->functions()) {
    ExprCP call = rewriteWindowCall(function.call, substitutions);
    Frame frame = function.frame;
    frame.startValue = replaceAll(frame.startValue, substitutions);
    frame.endValue = replaceAll(frame.endValue, substitutions);
    functionsChanged |= call != function.call || frame != function.frame;
    functions.push_back({call, frame, function.ignoreNulls});
  }
  ExprVector partitionKeys = replaceAll(node->partitionKeys(), substitutions);
  ExprVector orderKeys = replaceAll(node->orderKeys(), substitutions);
  if (input == node->input() && !functionsChanged &&
      partitionKeys == node->partitionKeys() &&
      orderKeys == node->orderKeys()) {
    return node;
  }
  return builder().make<Window>({
      .input = input,
      .functions = std::move(functions),
      .partitionKeys = std::move(partitionKeys),
      .orderKeys = std::move(orderKeys),
      .orderTypes = node->orderTypes(),
      .outputColumns = node->outputColumns(),
  });
}

NodeCP JoinKeyCoalesceRewriter::rewriteInference(
    const Inference* node,
    CoalesceSubstitutions& substitutions) {
  NodeCP input = rewrite(node->input(), substitutions);
  ExprCP call = replaceAll(node->call(), substitutions);
  if (input == node->input() && call == node->call()) {
    return node;
  }
  ColumnVector outputColumns = input->outputColumns();
  outputColumns.push_back(node->result());
  return builder().make<Inference>({
      .input = input,
      .call = call,
      .result = node->result(),
      .outputColumns = std::move(outputColumns),
  });
}

NodeCP JoinKeyCoalesceRewriter::rewriteRowNumber(
    const RowNumber* /*node*/,
    CoalesceSubstitutions& /*substitutions*/) {
  VELOX_UNREACHABLE("RowNumber is introduced after join-key coalesce");
}

NodeCP JoinKeyCoalesceRewriter::rewriteTopNRowNumber(
    const TopNRowNumber* /*node*/,
    CoalesceSubstitutions& /*substitutions*/) {
  VELOX_UNREACHABLE("TopNRowNumber is introduced after join-key coalesce");
}

NodeCP JoinKeyCoalesceRewriter::rewriteApply(
    const Apply* /*node*/,
    CoalesceSubstitutions& /*substitutions*/) {
  VELOX_UNREACHABLE("Apply is removed before join-key coalesce");
}

NodeCP JoinKeyCoalesceRewriter::rewriteEnforceDistinct(
    const EnforceDistinct* node,
    CoalesceSubstitutions& substitutions) {
  NodeCP input = rewrite(node->input(), substitutions);
  ExprVector distinctKeys = replaceAll(node->distinctKeys(), substitutions);
  if (input == node->input() && distinctKeys == node->distinctKeys()) {
    return node;
  }
  return builder().make<EnforceDistinct>({
      .input = input,
      .distinctKeys = std::move(distinctKeys),
      .errorMessage = node->errorMessage(),
  });
}

NodeCP JoinKeyCoalesceRewriter::rewriteExchange(
    const Exchange* /*node*/,
    CoalesceSubstitutions& /*substitutions*/) {
  VELOX_UNREACHABLE("Exchange is introduced after join-key coalesce");
}

NodeCP JoinKeyCoalesceRewriter::rewriteTableWrite(
    const TableWrite* node,
    CoalesceSubstitutions& substitutions) {
  NodeCP input = rewrite(node->input(), substitutions);
  substitutions.clear();
  if (input == node->input()) {
    return node;
  }
  return builder().make<TableWrite>({
      .input = input,
      .table = node->table(),
      .kind = node->kind(),
      .columnExprs = node->columnExprs(),
  });
}

NodeCP JoinKeyCoalesceRewriter::rewriteFixedPoint(
    const FixedPoint* node,
    CoalesceSubstitutions& /*substitutions*/) {
  CoalesceSubstitutions inputContext;
  NodeCP anchor = rewrite(node->anchor(), inputContext);
  NodeCP step = rewrite(node->step(), inputContext);
  NodeCP convergence = rewrite(node->convergence(), inputContext);
  if (anchor == node->anchor() && step == node->step() &&
      convergence == node->convergence()) {
    return node;
  }
  return builder().make<FixedPoint>({
      .anchor = anchor,
      .step = step,
      .convergence = convergence,
      .name = node->name(),
      .outputColumns = node->outputColumns(),
      .maxIterations = node->maxIterations(),
      .recursiveNumDrivers = node->recursiveNumDrivers(),
  });
}

} // namespace

NodeCP JoinKeyCoalesceNormalizer::normalize(NodeCP root, Builder& builder) {
  CoalesceSubstitutions substitutions;
  return JoinKeyCoalesceRewriter(builder).rewrite(root, substitutions);
}

} // namespace facebook::axiom::optimizer::v2
