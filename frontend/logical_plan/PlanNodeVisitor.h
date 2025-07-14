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

#include "logical_plan/LogicalPlanNode.h" //@manual

namespace facebook::velox::logical_plan {

class PlanNodeVisitorContext {
 public:
  virtual ~PlanNodeVisitorContext() = default;
};

class PlanNodeVisitor {
 public:
  virtual ~PlanNodeVisitor() = default;

  virtual void visit(const ValuesNode& node, PlanNodeVisitorContext& context)
      const = 0;

  virtual void visit(const TableScanNode& node, PlanNodeVisitorContext& context)
      const = 0;

  virtual void visit(const FilterNode& node, PlanNodeVisitorContext& context)
      const = 0;

  virtual void visit(const ProjectNode& node, PlanNodeVisitorContext& context)
      const = 0;

  virtual void visit(const AggregateNode& node, PlanNodeVisitorContext& context)
      const = 0;

  virtual void visit(const JoinNode& node, PlanNodeVisitorContext& context)
      const = 0;

  virtual void visit(const SortNode& node, PlanNodeVisitorContext& context)
      const = 0;

  virtual void visit(const LimitNode& node, PlanNodeVisitorContext& context)
      const = 0;

  virtual void visit(const SetNode& node, PlanNodeVisitorContext& context)
      const = 0;

  virtual void visit(const UnnestNode& node, PlanNodeVisitorContext& context)
      const = 0;

 protected:
  void visitInputs(const LogicalPlanNode& node, PlanNodeVisitorContext& ctx)
      const {
    for (const auto& input : node.inputs()) {
      input->accept(*this, ctx);
    }
  }
};
} // namespace facebook::velox::logical_plan
