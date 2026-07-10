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

#pragma once

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/Node.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer::v2 {

/// Empty context type for stateless rewriters.
struct NoContext {};

/// Visitor base for tree-IR rewrite passes. Default implementations
/// recursively rewrite children and rebuild a node only when a child
/// changed (identity-equal pointer comparison). Subclasses override the
/// per-kind hooks they transform.
///
/// Stateless passes inherit `NodeRewriter<>`. State-bearing passes
/// parameterize on a `TContext` that is threaded by reference through
/// dispatch.
template <typename TContext = NoContext>
class NodeRewriter {
 public:
  explicit NodeRewriter(Builder& builder) : builder_(builder) {}
  virtual ~NodeRewriter() = default;

  /// Dispatches by `node->nodeType()` to the matching `rewriteX` hook.
  NodeCP rewrite(NodeCP node, TContext& context) {
    switch (node->nodeType()) {
      case NodeType::kScan:
        return rewriteScan(node->as<Scan>(), context);
      case NodeType::kValues:
        return rewriteValues(node->as<Values>(), context);
      case NodeType::kFilter:
        return rewriteFilter(node->as<Filter>(), context);
      case NodeType::kProject:
        return rewriteProject(node->as<Project>(), context);
      case NodeType::kLimit:
        return rewriteLimit(node->as<Limit>(), context);
      case NodeType::kSort:
        return rewriteSort(node->as<Sort>(), context);
      case NodeType::kTopN:
        return rewriteTopN(node->as<TopN>(), context);
      case NodeType::kAggregate:
        return rewriteAggregate(node->as<Aggregate>(), context);
      case NodeType::kGroupId:
        return rewriteGroupId(node->as<GroupId>(), context);
      case NodeType::kMarkDistinct:
        return rewriteMarkDistinct(node->as<MarkDistinct>(), context);
      case NodeType::kUnnest:
        return rewriteUnnest(node->as<Unnest>(), context);
      case NodeType::kUnionAll:
        return rewriteUnionAll(node->as<UnionAll>(), context);
      case NodeType::kJoin:
        return rewriteJoin(node->as<Join>(), context);
      case NodeType::kWindow:
        return rewriteWindow(node->as<Window>(), context);
      case NodeType::kTopNRowNumber:
        return rewriteTopNRowNumber(node->as<TopNRowNumber>(), context);
      case NodeType::kApply:
        return rewriteApply(node->as<Apply>(), context);
      case NodeType::kEnforceSingleRow:
        return rewriteEnforceSingleRow(node->as<EnforceSingleRow>(), context);
      case NodeType::kAssignUniqueId:
        return rewriteAssignUniqueId(node->as<AssignUniqueId>(), context);
      case NodeType::kEnforceDistinct:
        return rewriteEnforceDistinct(node->as<EnforceDistinct>(), context);
      case NodeType::kExchange:
        return rewriteExchange(node->as<Exchange>(), context);
      case NodeType::kTableWrite:
        return rewriteTableWrite(node->as<TableWrite>(), context);
    }
    VELOX_UNREACHABLE();
  }

  /// Convenience overload available when `TContext` is `NoContext`.
  NodeCP rewrite(NodeCP node)
    requires std::is_same_v<TContext, NoContext>
  {
    NoContext empty{};
    return rewrite(node, empty);
  }

 protected:
  virtual NodeCP rewriteScan(const Scan* node, TContext& /*context*/) {
    return node;
  }

  virtual NodeCP rewriteValues(const Values* node, TContext& /*context*/) {
    return node;
  }

  virtual NodeCP rewriteFilter(const Filter* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<Filter>({newInput, node->predicates()});
  }

  virtual NodeCP rewriteProject(const Project* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<Project>(
        {newInput, node->exprs(), node->outputColumns()});
  }

  virtual NodeCP rewriteLimit(const Limit* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<Limit>(
        {newInput, node->offset(), node->count()});
  }

  virtual NodeCP rewriteSort(const Sort* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<Sort>(
        {newInput, node->orderKeys(), node->orderTypes()});
  }

  virtual NodeCP rewriteTopN(const TopN* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<TopN>(
        {newInput,
         node->orderKeys(),
         node->orderTypes(),
         node->offset(),
         node->count()});
  }

  virtual NodeCP rewriteAggregate(const Aggregate* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<Aggregate>(Aggregate::Key{
        .input = newInput,
        .groupingKeys = node->groupingKeys(),
        .aggregates = node->aggregates(),
        .outputColumns = node->outputColumns(),
        .step = node->step(),
        .groupId = node->groupId(),
        .globalGroupingSets = node->globalGroupingSets()});
  }

  virtual NodeCP rewriteGroupId(const GroupId* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<GroupId>(
        {newInput,
         node->groupingKeys(),
         node->aggregationInputs(),
         node->groupingSets(),
         node->groupingKeyColumns(),
         node->groupId(),
         node->outputColumns()});
  }

  virtual NodeCP rewriteMarkDistinct(
      const MarkDistinct* node,
      TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<MarkDistinct>(
        {newInput,
         node->markers(),
         node->distinctKeys(),
         node->masks(),
         node->outputColumns()});
  }

  virtual NodeCP rewriteUnnest(const Unnest* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<Unnest>(
        {newInput,
         node->unnestExpressions(),
         node->replicatedColumns(),
         node->unnestColumns(),
         node->ordinalityColumn(),
         node->outputColumns()});
  }

  virtual NodeCP rewriteUnionAll(const UnionAll* node, TContext& context) {
    NodeVector newInputs;
    newInputs.reserve(node->inputs().size());
    bool changed = false;
    for (NodeCP input : node->inputs()) {
      NodeCP newInput = rewrite(input, context);
      changed |= (newInput != input);
      newInputs.push_back(newInput);
    }
    if (!changed) {
      return node;
    }
    // legColumns are by Column*: valid as long as leg rewrites preserve
    // identity of every referenced column. Rewrites that drop or replace a
    // referenced column must remap legColumns explicitly; the UnionAll ctor
    // enforces this.
    return builder_.template make<UnionAll>(
        {std::move(newInputs), node->legColumns(), node->outputColumns()});
  }

  virtual NodeCP rewriteJoin(const Join* node, TContext& context) {
    NodeCP newLeft = rewrite(node->left(), context);
    NodeCP newRight = rewrite(node->right(), context);
    if (newLeft == node->left() && newRight == node->right()) {
      return node;
    }
    return builder_.template make<Join>(
        {newLeft,
         newRight,
         node->joinType(),
         node->leftKeys(),
         node->rightKeys(),
         node->filter(),
         node->nullAware(),
         node->nullAsValue(),
         node->outputColumns()});
  }

  virtual NodeCP rewriteWindow(const Window* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<Window>(
        {newInput,
         node->functions(),
         node->partitionKeys(),
         node->orderKeys(),
         node->orderTypes(),
         node->outputColumns()});
  }

  virtual NodeCP rewriteTopNRowNumber(
      const TopNRowNumber* node,
      TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<TopNRowNumber>(
        {newInput,
         node->rankFunction(),
         node->partitionKeys(),
         node->orderKeys(),
         node->orderTypes(),
         node->limit(),
         node->rankColumn(),
         node->outputColumns()});
  }

  virtual NodeCP rewriteApply(const Apply* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    NodeCP newBody = rewrite(node->body(), context);
    if (newInput == node->input() && newBody == node->body()) {
      return node;
    }
    return builder_.template make<Apply>(
        {newInput,
         newBody,
         node->correlationColumns(),
         node->kind(),
         node->filter(),
         node->enforceSingleRow(),
         node->markColumn(),
         node->inLhs(),
         node->inBodyKey(),
         node->includeMarker(),
         node->outputColumns()});
  }

  virtual NodeCP rewriteEnforceSingleRow(
      const EnforceSingleRow* node,
      TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<EnforceSingleRow>({newInput});
  }

  virtual NodeCP rewriteAssignUniqueId(
      const AssignUniqueId* node,
      TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<AssignUniqueId>({newInput, node->idColumn()});
  }

  virtual NodeCP rewriteEnforceDistinct(
      const EnforceDistinct* node,
      TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<EnforceDistinct>(
        {newInput, node->distinctKeys(), node->errorMessage()});
  }

  virtual NodeCP rewriteExchange(const Exchange* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<Exchange>({newInput, node->partitioning()});
  }

  virtual NodeCP rewriteTableWrite(const TableWrite* node, TContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    if (newInput == node->input()) {
      return node;
    }
    return builder_.template make<TableWrite>(
        {newInput, node->table(), node->kind(), node->columnExprs()});
  }

  Builder& builder() {
    return builder_;
  }

 private:
  Builder& builder_;
};

} // namespace facebook::axiom::optimizer::v2
