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

#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2 {

/// Invokes `visit(expression)` for each expression that lives directly
/// on `node` (predicates, projection exprs, aggregate args, join keys,
/// etc.). Does NOT recurse into sub-nodes — callers that need a full
/// tree walk recurse via `node->inputs()` themselves.
///
/// Used by passes that need to inspect every expression on a Node
/// uniformly across all Node kinds (correlation tracking, column-use
/// analysis, etc.). Kept central so per-pass implementations don't
/// each maintain their own switch over `NodeType`.
///
/// For nodes that carry no expressions (Scan, Values, Limit,
/// EnforceSingleRow, AssignUniqueId), no callbacks are invoked.
///
/// For nested `Apply`, only the boundary-level expressions (`filter`,
/// `inLhs`, `inBodyKey`) are visited. `correlationColumns` is NOT
/// visited — those are nested-Apply boundary references to the nested
/// Apply's input cols, not references from the outer caller's
/// perspective. Refs to outer cols inside the nested Apply's body are
/// reached via input-tree recursion.
///
/// `visit` should be callable as `visit(ExprCP)`. `ColumnCP` is
/// passed where appropriate (Unnest's `replicatedColumns`, UnionAll's
/// `legColumns` entries — both are column references); since `Column`
/// derives from `Expr`, the upcast is implicit.
template <typename Visit>
void forEachExpressionInNode(NodeCP node, Visit&& visit) {
  switch (node->nodeType()) {
    case NodeType::kFilter:
      for (ExprCP predicate : node->as<Filter>()->predicates()) {
        visit(predicate);
      }
      break;
    case NodeType::kProject:
      for (ExprCP expression : node->as<Project>()->exprs()) {
        visit(expression);
      }
      break;
    case NodeType::kAggregate: {
      const Aggregate* aggregate = node->as<Aggregate>();
      for (ExprCP groupingKey : aggregate->groupingKeys()) {
        visit(groupingKey);
      }
      for (const auto* aggregateCall : aggregate->aggregates()) {
        for (ExprCP argument : aggregateCall->args()) {
          visit(argument);
        }
        if (aggregateCall->condition() != nullptr) {
          visit(aggregateCall->condition());
        }
        for (ExprCP orderKey : aggregateCall->orderKeys()) {
          visit(orderKey);
        }
      }
      break;
    }
    case NodeType::kSort:
      for (ExprCP orderKey : node->as<Sort>()->orderKeys()) {
        visit(orderKey);
      }
      break;
    case NodeType::kTopN:
      for (ExprCP orderKey : node->as<TopN>()->orderKeys()) {
        visit(orderKey);
      }
      break;
    case NodeType::kJoin: {
      const Join* join = node->as<Join>();
      for (ExprCP key : join->leftKeys()) {
        visit(key);
      }
      for (ExprCP key : join->rightKeys()) {
        visit(key);
      }
      for (ExprCP conjunct : join->filter()) {
        visit(conjunct);
      }
      break;
    }
    case NodeType::kApply: {
      const Apply* apply = node->as<Apply>();
      for (ExprCP conjunct : apply->filter()) {
        visit(conjunct);
      }
      if (apply->inLhs() != nullptr) {
        visit(apply->inLhs());
      }
      if (apply->inBodyKey() != nullptr) {
        visit(apply->inBodyKey());
      }
      // correlationColumns: nested-Apply boundary refs (to nested
      // input cols), not refs from the outer caller's perspective.
      // Refs to outer cols inside the nested body reach the caller via
      // input-tree recursion.
      break;
    }
    case NodeType::kEnforceDistinct:
      for (ExprCP key : node->as<EnforceDistinct>()->distinctKeys()) {
        visit(key);
      }
      break;
    case NodeType::kExchange:
      for (ExprCP key : node->as<Exchange>()->partitioning().keys) {
        visit(key);
      }
      break;
    case NodeType::kWindow: {
      const Window* window = node->as<Window>();
      for (ExprCP key : window->partitionKeys()) {
        visit(key);
      }
      for (ExprCP key : window->orderKeys()) {
        visit(key);
      }
      for (const auto& windowFunction : window->functions()) {
        visit(windowFunction.call);
        if (windowFunction.frame.startValue != nullptr) {
          visit(windowFunction.frame.startValue);
        }
        if (windowFunction.frame.endValue != nullptr) {
          visit(windowFunction.frame.endValue);
        }
      }
      break;
    }
    case NodeType::kTopNRowNumber: {
      const TopNRowNumber* topN = node->as<TopNRowNumber>();
      for (ExprCP key : topN->partitionKeys()) {
        visit(key);
      }
      for (ExprCP key : topN->orderKeys()) {
        visit(key);
      }
      break;
    }
    case NodeType::kUnnest: {
      const Unnest* unnest = node->as<Unnest>();
      for (ExprCP expression : unnest->unnestExpressions()) {
        visit(expression);
      }
      for (ColumnCP column : unnest->replicatedColumns()) {
        visit(column);
      }
      break;
    }
    case NodeType::kUnionAll: {
      const UnionAll* unionAll = node->as<UnionAll>();
      for (const auto& leg : unionAll->legColumns()) {
        for (ColumnCP column : leg) {
          visit(column);
        }
      }
      break;
    }
    case NodeType::kGroupId: {
      const GroupId* groupId = node->as<GroupId>();
      for (ExprCP key : groupId->groupingKeys()) {
        visit(key);
      }
      for (ExprCP input : groupId->aggregationInputs()) {
        visit(input);
      }
      break;
    }
    case NodeType::kMarkDistinct: {
      const MarkDistinct* markDistinct = node->as<MarkDistinct>();
      for (ExprCP key : markDistinct->distinctKeys()) {
        visit(key);
      }
      for (ColumnCP mask : markDistinct->masks()) {
        visit(mask);
      }
      break;
    }
    case NodeType::kTableWrite:
      for (ExprCP expression : node->as<TableWrite>()->columnExprs()) {
        visit(expression);
      }
      break;
    case NodeType::kScan:
    case NodeType::kValues:
    case NodeType::kLimit:
    case NodeType::kEnforceSingleRow:
    case NodeType::kAssignUniqueId:
    case NodeType::kFixedPoint:
    case NodeType::kWorkingTable:
      // No expressions on these nodes.
      break;
  }
}

} // namespace facebook::axiom::optimizer::v2
