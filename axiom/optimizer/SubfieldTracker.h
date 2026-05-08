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

#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PathSet.h"

namespace facebook::axiom::optimizer {

/// Set of accessed subfields given ordinal of output column or function
/// argument.
struct ResultAccess {
  // Key in 'resultPaths' to indicate the path is applied to the function
  // itself, not the ith argument.
  static constexpr int32_t kSelf = -1;
  std::map<int32_t, PathSet> resultPaths;
};

/// PlanNode output columns and function arguments with accessed subfields.
struct PlanSubfields {
  folly::F14FastMap<const logical_plan::LogicalPlanNode*, ResultAccess>
      nodeFields;
  folly::F14FastMap<const logical_plan::Expr*, ResultAccess> argFields;

  /// Return true if 'ordinal' output column of 'node' is accessed.
  bool hasColumn(const logical_plan::LogicalPlanNode* node, int32_t ordinal)
      const;

  /// Return a set of accessed subfields for the result of 'expr'.
  std::optional<PathSet> findSubfields(const logical_plan::Expr* expr) const;

  std::string toString() const;
};

/// Struct for resolving which logical PlanNode or Lambda defines which
/// field for column and subfield tracking.
/// Only one of planNode or callName + callExpr + lambdaOrdinal is set.
struct LogicalContextSource {
  const logical_plan::LogicalPlanNode* planNode{nullptr};
  std::string_view callName;
  const logical_plan::Expr* callExpr{nullptr};
  int32_t lambdaOrdinal{-1};
};

struct MarkFieldsAccessedContext {
  // 1:1 with 'sources'. Either output type of the plan node or signature of a
  // lambda.
  std::span<const velox::RowType* const> rowTypes;
  std::span<const LogicalContextSource> sources;
};

/// Take a pass over logical plan and collect all accessed columns and
/// subfields.
class SubfieldTracker {
 public:
  /// @param tryFoldConstant A function that attempts to constant fold an
  /// expression. Returns nullptr if cannot fold.
  explicit SubfieldTracker(
      std::function<logical_plan::ConstantExprPtr(const logical_plan::ExprPtr&)>
          tryFoldConstant);

  /// Goes over the local plan and collects all accessed columns and subfields.
  /// Reports 'control' and 'payload' columns and subfields separately.
  ///
  /// @param outputOrdinals If non-empty, only these ordinals of the root node
  /// are marked as payload-accessed, enabling pruning of columns that
  /// OutputNode does not export. When empty, all root output columns are
  /// marked.
  std::pair<PlanSubfields, PlanSubfields> markAll(
      const logical_plan::LogicalPlanNode& node,
      std::vector<int32_t> outputOrdinals = {}) && {
    if (outputOrdinals.empty()) {
      markAllSubfields(node, {});
    } else {
      markSelectedSubfields(node, outputOrdinals, {});
    }
    return {controlSubfields_, payloadSubfields_};
  }

  /// If 'step' applied to result of the function of 'metadata'
  /// corresponds to an argument, returns the ordinal of the argument.
  static std::optional<int32_t> stepToArg(
      const Step& step,
      const FunctionMetadata* metadata);

  /// Given a constant array index or map key in 'variant', populates 'step'
  /// with the corresponding field name or integer id. Returns false if
  /// 'variant' is null or has an unsupported type.
  static bool trySetSubscript(Step& step, const velox::Variant& variant);

 private:
  void markSubfields(
      const logical_plan::ExprPtr& expr,
      std::vector<Step>& steps,
      bool isControl,
      const MarkFieldsAccessedContext& context);

  void markFieldAccessed(
      const logical_plan::ProjectNode& project,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl,
      const MarkFieldsAccessedContext& context);

  void markFieldAccessed(
      const logical_plan::UnnestNode& unnest,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl,
      const MarkFieldsAccessedContext& context);

  void markFieldAccessed(
      const logical_plan::AggregateNode& agg,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl,
      const MarkFieldsAccessedContext& context);

  void markFieldAccessed(
      const logical_plan::SetNode& set,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl,
      const MarkFieldsAccessedContext& context);

  void markFieldAccessed(
      const LogicalContextSource& source,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl,
      const MarkFieldsAccessedContext& context);

  void markAllSubfields(
      const logical_plan::LogicalPlanNode& node,
      const MarkFieldsAccessedContext& context);

  void markSelectedSubfields(
      const logical_plan::LogicalPlanNode& node,
      const std::vector<int32_t>& ordinals,
      const MarkFieldsAccessedContext& context);

  void markControl(
      const logical_plan::LogicalPlanNode& node,
      const MarkFieldsAccessedContext& context);

  void markColumnSubfields(
      const logical_plan::LogicalPlanNodePtr& source,
      std::span<const logical_plan::ExprPtr> columns,
      bool isControl,
      const MarkFieldsAccessedContext& context);

  std::function<logical_plan::ConstantExprPtr(const logical_plan::ExprPtr&)>
      tryFoldConstant_;

  Name elementAt_{nullptr};
  Name subscript_{nullptr};
  Name cardinality_{nullptr};

  PlanSubfields controlSubfields_;
  PlanSubfields payloadSubfields_;
};

} // namespace facebook::axiom::optimizer
