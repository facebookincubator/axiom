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

#include "logical_plan/PlanPrinter.h" //@manual
#include "logical_plan/ExprPrinter.h" //@manual

namespace facebook::velox::logical_plan {

namespace {

struct ToTextVisitorContext : public PlanNodeVisitorContext {
  std::stringstream out;
  int32_t indent{0};
};

class ToTextVisitor : public PlanNodeVisitor {
 public:
  void visit(const ValuesNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode(
        "Values", node, fmt::format("{} rows", node.rows().size()), context);
  }

  void visit(const TableScanNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode(
        "TableScan",
        node,
        fmt::format("{}.{}", node.connectorId(), node.tableName()),
        context);
  }

  void visit(const FilterNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode("Filter", node, ExprPrinter::toText(*node.predicate()), context);
  }

  void visit(const ProjectNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& myContext = static_cast<ToTextVisitorContext&>(context);
    myContext.out << makeIndent(myContext.indent) << "- Project:";

    appendOutputType(node, myContext);

    myContext.out << std::endl;

    const auto size = node.names().size();

    const auto indent = makeIndent(myContext.indent + 2);

    for (auto i = 0; i < size; ++i) {
      myContext.out << indent << node.names().at(i)
                    << " := " << ExprPrinter::toText(*node.expressions().at(i))
                    << std::endl;
    }

    appendInputs(node, myContext);
  }

  void visit(const AggregateNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& myContext = static_cast<ToTextVisitorContext&>(context);
    myContext.out << makeIndent(myContext.indent) << "- Aggregate(";

    const auto numKeys = node.groupingKeys().size();
    for (auto i = 0; i < numKeys; ++i) {
      if (i > 0) {
        myContext.out << ", ";
      }
      myContext.out << ExprPrinter::toText(*node.groupingKeys().at(i));
    }

    appendOutputType(node, myContext);

    myContext.out << std::endl;

    const auto size = node.aggregates().size();

    const auto indent = makeIndent(myContext.indent + 2);

    for (auto i = 0; i < size; ++i) {
      myContext.out << indent << node.outputNames().at(numKeys + i)
                    << " := " << ExprPrinter::toText(*node.aggregates().at(i))
                    << std::endl;
    }

    appendInputs(node, myContext);
  }

  void visit(const JoinNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode(
        fmt::format("Join {}", JoinTypeName::toName(node.joinType())),
        node,
        ExprPrinter::toText(*node.condition()),
        context);
  }

  void visit(const SortNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& myContext = static_cast<ToTextVisitorContext&>(context);
    myContext.out << makeIndent(myContext.indent) << "- Sort: ";

    const auto size = node.ordering().size();
    for (auto i = 0; i < size; ++i) {
      if (i > 0) {
        myContext.out << ", ";
      }
      myContext.out << ExprPrinter::toText(*node.ordering().at(i).expression)
                    << " " << node.ordering().at(i).order.toString();
    }

    appendOutputType(node, myContext);
    myContext.out << std::endl;
    appendInputs(node, myContext);
  }

  void visit(const LimitNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode(
        "Limit",
        node,
        node.offset() > 0
            ? fmt::format("{} (offset: {})", node.count(), node.offset())
            : fmt::format("{}", node.count()),
        context);
  }

  void visit(const SetNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode("Set", node, context);
  }

  void visit(const UnnestNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode("Unnest", node, context);
  }

 private:
  static std::string makeIndent(int32_t size) {
    return std::string(size * 2, ' ');
  }

  void appendNode(
      const std::string& name,
      const LogicalPlanNode& node,
      const std::optional<std::string>& details,
      PlanNodeVisitorContext& context) const {
    auto& myContext = static_cast<ToTextVisitorContext&>(context);
    myContext.out << makeIndent(myContext.indent) << "- " << name << ":";

    if (details.has_value()) {
      myContext.out << " " << details.value();
    }

    appendOutputType(node, myContext);

    myContext.out << std::endl;

    appendInputs(node, myContext);
  }

  void appendNode(
      const std::string& name,
      const LogicalPlanNode& node,
      PlanNodeVisitorContext& context) const {
    appendNode(name, node, std::nullopt, context);
  }

  void appendOutputType(
      const LogicalPlanNode& node,
      ToTextVisitorContext& context) const {
    context.out << " -> " << node.outputType()->toString();
  }

  void appendInputs(const LogicalPlanNode& node, ToTextVisitorContext& context)
      const {
    context.indent++;
    PlanNodeVisitor::visitInputs(node, context);
    context.indent--;
  }
};

} // namespace

// static
std::string PlanPrinter::toText(const LogicalPlanNode& root) {
  ToTextVisitorContext context;
  ToTextVisitor visitor;
  root.accept(visitor, context);
  return context.out.str();
}

} // namespace facebook::velox::logical_plan
