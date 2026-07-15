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
/// recursively rewrite children and rebuild the node via `Node::withInputs`
/// only when at least one child changed (identity-equal pointer
/// comparison). Subclasses override the per-kind hooks they transform.
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
      case NodeType::kWorkingTable:
        return rewriteWorkingTable(node->as<WorkingTable>(), context);
      case NodeType::kFixedPoint:
        return rewriteFixedPoint(node->as<FixedPoint>(), context);
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
  // Recurses on every input, then rebuilds `node` via `Node::withInputs`
  // iff at least one child changed. Preserves pointer identity along
  // untouched paths and defers `NodeVector` allocation until the first
  // changed child, so passthrough is alloc-free.
  NodeCP rewriteChildren(NodeCP node, TContext& context) {
    const auto inputs = node->inputs();
    NodeVector newInputs;
    for (size_t i = 0; i < inputs.size(); ++i) {
      NodeCP child = inputs[i];
      NodeCP rewritten = rewrite(child, context);
      if (newInputs.empty() && rewritten == child) {
        continue;
      }
      if (newInputs.empty()) {
        newInputs.reserve(inputs.size());
        for (size_t j = 0; j < i; ++j) {
          newInputs.push_back(inputs[j]);
        }
      }
      newInputs.push_back(rewritten);
    }
    if (newInputs.empty()) {
      return node;
    }
    return node->withInputs(std::move(newInputs), builder_);
  }

  virtual NodeCP rewriteScan(const Scan* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteValues(const Values* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteFilter(const Filter* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteProject(const Project* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteLimit(const Limit* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteSort(const Sort* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteTopN(const TopN* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteAggregate(const Aggregate* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteGroupId(const GroupId* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteMarkDistinct(
      const MarkDistinct* node,
      TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteUnnest(const Unnest* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteUnionAll(const UnionAll* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteJoin(const Join* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteWindow(const Window* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteTopNRowNumber(
      const TopNRowNumber* node,
      TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteApply(const Apply* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteEnforceSingleRow(
      const EnforceSingleRow* node,
      TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteAssignUniqueId(
      const AssignUniqueId* node,
      TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteEnforceDistinct(
      const EnforceDistinct* node,
      TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteExchange(const Exchange* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteTableWrite(const TableWrite* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteWorkingTable(
      const WorkingTable* node,
      TContext& context) {
    return rewriteChildren(node, context);
  }

  virtual NodeCP rewriteFixedPoint(const FixedPoint* node, TContext& context) {
    return rewriteChildren(node, context);
  }

  Builder& builder() {
    return builder_;
  }

 private:
  Builder& builder_;
};

} // namespace facebook::axiom::optimizer::v2
