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

class NodeVisitorContext {
 public:
  virtual ~NodeVisitorContext() = default;
};

/// Double-dispatch visitor for v2 `Node` subclasses. Mirrors
/// `logical_plan::PlanNodeVisitor`. Each subclass implements `accept` to
/// forward to the matching `visit` overload.
class NodeVisitor {
 public:
  virtual ~NodeVisitor() = default;

  virtual void visit(const Scan& node, NodeVisitorContext& context) const = 0;
  virtual void visit(const Filter& node, NodeVisitorContext& context) const = 0;
  virtual void visit(const Project& node, NodeVisitorContext& context)
      const = 0;
  virtual void visit(const Limit& node, NodeVisitorContext& context) const = 0;
  virtual void visit(const Sort& node, NodeVisitorContext& context) const = 0;
  virtual void visit(const TopN& node, NodeVisitorContext& context) const = 0;
  virtual void visit(const Aggregate& node, NodeVisitorContext& context)
      const = 0;
  virtual void visit(const GroupId& node, NodeVisitorContext& context)
      const = 0;
  virtual void visit(const MarkDistinct& node, NodeVisitorContext& context)
      const = 0;
  virtual void visit(const Values& node, NodeVisitorContext& context) const = 0;
  virtual void visit(const Unnest& node, NodeVisitorContext& context) const = 0;
  virtual void visit(const UnionAll& node, NodeVisitorContext& context)
      const = 0;
  virtual void visit(const Join& node, NodeVisitorContext& context) const = 0;
  virtual void visit(const Window& node, NodeVisitorContext& context) const = 0;
  virtual void visit(const TopNRowNumber& node, NodeVisitorContext& context)
      const = 0;
  virtual void visit(const Apply& node, NodeVisitorContext& context) const = 0;
  virtual void visit(const EnforceSingleRow& node, NodeVisitorContext& context)
      const = 0;
  virtual void visit(const AssignUniqueId& node, NodeVisitorContext& context)
      const = 0;
  virtual void visit(const EnforceDistinct& node, NodeVisitorContext& context)
      const = 0;
  virtual void visit(const Exchange& node, NodeVisitorContext& context)
      const = 0;
  virtual void visit(const TableWrite& node, NodeVisitorContext& context)
      const = 0;

 protected:
  void visitInputs(const Node& node, NodeVisitorContext& context) const {
    for (NodeCP input : node.inputs()) {
      input->accept(*this, context);
    }
  }
};

} // namespace facebook::axiom::optimizer::v2
