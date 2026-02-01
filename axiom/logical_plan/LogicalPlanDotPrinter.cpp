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

#include "axiom/logical_plan/LogicalPlanDotPrinter.h"

#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/logical_plan/ExprVisitor.h"
#include "axiom/logical_plan/PlanNodeVisitor.h"

namespace facebook::axiom::logical_plan {

namespace {

// Color palette for the visualization.
struct ColorPalette {
  // Text and borders.
  std::string_view text = "#1c2915"; // Dark green
  // Edges between nodes.
  std::string_view lines = "#727a6b"; // Gray-green
  // Node headers.
  std::string_view header = "#ccd5c4"; // Light sage
  // Node details.
  std::string_view highlight = "#ede3d9"; // Cream
  // Subquery edges.
  std::string_view subqueryEdge = "#8b4513"; // Saddle brown
};

const ColorPalette kPalette;

std::string escapeHtml(const std::string& s) {
  std::string result;
  result.reserve(s.size());
  for (char c : s) {
    switch (c) {
      case '<':
        result += "&lt;";
        break;
      case '>':
        result += "&gt;";
        break;
      case '&':
        result += "&amp;";
        break;
      case '"':
        result += "&quot;";
        break;
      default:
        result += c;
    }
  }
  return result;
}

constexpr size_t kMaxExprLength = 60;

std::string truncate(const std::string& s, size_t maxLength = kMaxExprLength) {
  if (s.size() <= maxLength) {
    return s;
  }
  return s.substr(0, maxLength - 3) + "...";
}

std::string nodeId(const LogicalPlanNode& node) {
  return fmt::format("node_{}", node.id());
}

void printRow(std::ostream& out, const std::string& text) {
  out << "        <TR><TD ALIGN=\"LEFT\">" << text << "</TD></TR>\n";
}

void printHighlightedRow(std::ostream& out, const std::string& text) {
  out << "        <TR><TD BGCOLOR=\"" << kPalette.highlight
      << "\" ALIGN=\"LEFT\">" << text << "</TD></TR>\n";
}

// Visitor to collect all subqueries from an expression tree.
class SubqueryCollector : public ExprVisitor {
 public:
  struct Context : public ExprVisitorContext {
    std::vector<const SubqueryExpr*> subqueries;
  };

  void visit(const InputReferenceExpr&, ExprVisitorContext&) const override {}

  void visit(const CallExpr& expr, ExprVisitorContext& ctx) const override {
    visitInputs(expr, ctx);
  }

  void visit(const SpecialFormExpr& expr, ExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }

  void visit(const AggregateExpr& expr, ExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
    if (expr.filter() != nullptr) {
      expr.filter()->accept(*this, ctx);
    }
  }

  void visit(const WindowExpr& expr, ExprVisitorContext& ctx) const override {
    visitInputs(expr, ctx);
  }

  void visit(const ConstantExpr&, ExprVisitorContext&) const override {}

  void visit(const LambdaExpr& expr, ExprVisitorContext& ctx) const override {
    if (expr.body() != nullptr) {
      expr.body()->accept(*this, ctx);
    }
  }

  void visit(const SubqueryExpr& expr, ExprVisitorContext& ctx) const override {
    auto& context = static_cast<Context&>(ctx);
    context.subqueries.push_back(&expr);
  }
};

// Collect all subqueries from an expression.
std::vector<const SubqueryExpr*> collectSubqueries(const Expr& expr) {
  SubqueryCollector collector;
  SubqueryCollector::Context ctx;
  expr.accept(collector, ctx);
  return std::move(ctx.subqueries);
}

// Collect all subqueries from a list of expressions.
std::vector<const SubqueryExpr*> collectSubqueries(
    const std::vector<ExprPtr>& exprs) {
  std::vector<const SubqueryExpr*> result;
  SubqueryCollector collector;
  SubqueryCollector::Context ctx;
  for (const auto& expr : exprs) {
    expr->accept(collector, ctx);
  }
  return std::move(ctx.subqueries);
}

class DotPrinterVisitor : public PlanNodeVisitor {
 public:
  struct Context : public PlanNodeVisitorContext {
    std::ostream& out;
    // Counter for unique subquery cluster IDs.
    int subqueryCounter{0};

    explicit Context(std::ostream& o) : out(o) {}
  };

  void visit(const ValuesNode& node, PlanNodeVisitorContext& ctx)
      const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(context.out, node, "Values", kPalette.header);

    printHighlightedRow(
        context.out,
        fmt::format("{} rows", static_cast<int>(node.cardinality())));

    printOutputColumns(context.out, node);
    printNodeEnd(context.out);
  }

  void visit(const TableScanNode& node, PlanNodeVisitorContext& ctx)
      const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(
        context.out,
        node,
        fmt::format("TableScan: {}", escapeHtml(node.table()->name())),
        kPalette.header);

    printOutputColumns(context.out, node);
    printNodeEnd(context.out);
  }

  void visit(const FilterNode& node, PlanNodeVisitorContext& ctx)
      const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(context.out, node, "Filter", kPalette.header);

    printHighlightedRow(
        context.out,
        escapeHtml(truncate(ExprPrinter::toText(*node.predicate()))));

    printNodeEnd(context.out);

    // Collect and render subqueries from the predicate.
    auto subqueries = collectSubqueries(*node.predicate());
    for (const auto* subquery : subqueries) {
      renderSubqueryInCluster(context, node, *subquery->subquery());
    }

    printEdgeToInputs(context.out, node);
    visitInputs(node, ctx);
  }

  void visit(const ProjectNode& node, PlanNodeVisitorContext& ctx)
      const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(context.out, node, "Project", kPalette.header);

    for (size_t i = 0; i < node.names().size(); ++i) {
      const auto exprText = ExprPrinter::toText(*node.expressionAt(i));
      if (exprText != node.names()[i]) {
        printRow(
            context.out,
            fmt::format(
                "{} := {}",
                escapeHtml(node.names()[i]),
                escapeHtml(truncate(exprText))));
      } else {
        printRow(context.out, escapeHtml(node.names()[i]));
      }
    }

    printNodeEnd(context.out);

    // Collect and render subqueries from expressions.
    auto subqueries = collectSubqueries(node.expressions());
    for (const auto* subquery : subqueries) {
      renderSubqueryInCluster(context, node, *subquery->subquery());
    }

    printEdgeToInputs(context.out, node);
    visitInputs(node, ctx);
  }

  void visit(const AggregateNode& node, PlanNodeVisitorContext& ctx)
      const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(context.out, node, "Aggregate", kPalette.header);

    if (!node.groupingKeys().empty()) {
      std::string keys = "GROUP BY: ";
      for (size_t i = 0; i < node.groupingKeys().size(); ++i) {
        if (i > 0) {
          keys += ", ";
        }
        keys +=
            escapeHtml(truncate(ExprPrinter::toText(*node.groupingKeys()[i])));
      }
      printHighlightedRow(context.out, keys);
    }

    const auto numGroupingKeys = node.groupingKeys().size();
    for (size_t i = 0; i < node.aggregates().size(); ++i) {
      const auto& agg = node.aggregateAt(i);
      printRow(
          context.out,
          fmt::format(
              "{} := {}",
              escapeHtml(node.outputNames()[numGroupingKeys + i]),
              escapeHtml(truncate(ExprPrinter::toText(*agg)))));
    }

    printNodeEnd(context.out);
    printEdgeToInputs(context.out, node);
    visitInputs(node, ctx);
  }

  void visit(const JoinNode& node, PlanNodeVisitorContext& ctx) const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(
        context.out,
        node,
        fmt::format("Join {}", JoinTypeName::toName(node.joinType())),
        kPalette.header);

    if (node.condition() != nullptr) {
      printHighlightedRow(
          context.out,
          fmt::format(
              "ON: {}",
              escapeHtml(truncate(ExprPrinter::toText(*node.condition())))));
    }

    printNodeEnd(context.out);

    // Collect and render subqueries from the condition.
    if (node.condition() != nullptr) {
      auto subqueries = collectSubqueries(*node.condition());
      for (const auto* subquery : subqueries) {
        renderSubqueryInCluster(context, node, *subquery->subquery());
      }
    }

    printEdgeToInputs(context.out, node);
    visitInputs(node, ctx);
  }

  void visit(const SortNode& node, PlanNodeVisitorContext& ctx) const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(context.out, node, "Sort", kPalette.header);

    for (const auto& field : node.ordering()) {
      printRow(
          context.out,
          fmt::format(
              "{} {}",
              escapeHtml(truncate(ExprPrinter::toText(*field.expression))),
              field.order.toString()));
    }

    printNodeEnd(context.out);
    printEdgeToInputs(context.out, node);
    visitInputs(node, ctx);
  }

  void visit(const LimitNode& node, PlanNodeVisitorContext& ctx)
      const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(context.out, node, "Limit", kPalette.header);

    if (node.offset() > 0) {
      printHighlightedRow(
          context.out, fmt::format("OFFSET: {}", node.offset()));
    }
    if (!node.noLimit()) {
      printHighlightedRow(context.out, fmt::format("COUNT: {}", node.count()));
    }

    printNodeEnd(context.out);
    printEdgeToInputs(context.out, node);
    visitInputs(node, ctx);
  }

  void visit(const SetNode& node, PlanNodeVisitorContext& ctx) const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(
        context.out,
        node,
        fmt::format("{}", SetOperationName::toName(node.operation())),
        kPalette.header);

    printOutputColumns(context.out, node);
    printNodeEnd(context.out);
    printEdgeToInputs(context.out, node);
    visitInputs(node, ctx);
  }

  void visit(const UnnestNode& node, PlanNodeVisitorContext& ctx)
      const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(context.out, node, "Unnest", kPalette.header);

    for (const auto& expr : node.unnestExpressions()) {
      printHighlightedRow(
          context.out, escapeHtml(truncate(ExprPrinter::toText(*expr))));
    }

    if (node.ordinalityName().has_value()) {
      printRow(
          context.out,
          fmt::format(
              "{} (ordinality)", escapeHtml(node.ordinalityName().value())));
    }

    printNodeEnd(context.out);
    printEdgeToInputs(context.out, node);
    visitInputs(node, ctx);
  }

  void visit(const TableWriteNode& node, PlanNodeVisitorContext& ctx)
      const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(
        context.out,
        node,
        fmt::format(
            "{}: {}",
            connector::WriteKindName::toName(node.writeKind()),
            escapeHtml(node.table()->name())),
        kPalette.header);

    for (const auto& name : node.columnNames()) {
      printRow(context.out, escapeHtml(name));
    }

    printNodeEnd(context.out);
    printEdgeToInputs(context.out, node);
    visitInputs(node, ctx);
  }

  void visit(const SampleNode& node, PlanNodeVisitorContext& ctx)
      const override {
    auto& context = static_cast<Context&>(ctx);
    printNodeStart(
        context.out,
        node,
        fmt::format("Sample ({})", SampleNode::toName(node.sampleMethod())),
        kPalette.header);

    printHighlightedRow(
        context.out,
        fmt::format(
            "{}%",
            escapeHtml(truncate(ExprPrinter::toText(*node.percentage())))));

    printNodeEnd(context.out);
    printEdgeToInputs(context.out, node);
    visitInputs(node, ctx);
  }

 private:
  static void printNodeStart(
      std::ostream& out,
      const LogicalPlanNode& node,
      const std::string& header,
      std::string_view headerColor) {
    out << "  " << nodeId(node) << " [shape=none, margin=0, label=<\n";
    out << "    <TABLE BORDER=\"1\" CELLBORDER=\"0\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\" COLOR=\""
        << kPalette.text << "\">\n";
    out << "      <TR><TD BGCOLOR=\"" << headerColor << "\"><B>"
        << escapeHtml(header) << " [" << node.id() << "]</B></TD></TR>\n";
  }

  static void printNodeEnd(std::ostream& out) {
    out << "    </TABLE>\n";
    out << "  >];\n\n";
  }

  static void printOutputColumns(
      std::ostream& out,
      const LogicalPlanNode& node) {
    const auto& outputType = node.outputType();
    for (size_t i = 0; i < outputType->size(); ++i) {
      printRow(out, escapeHtml(outputType->nameOf(i)));
    }
  }

  static void printEdgeToInputs(
      std::ostream& out,
      const LogicalPlanNode& node) {
    for (const auto& input : node.inputs()) {
      out << "  " << nodeId(node) << " -> " << nodeId(*input) << " [color=\""
          << kPalette.lines << "\", penwidth=2];\n";
    }
    out << "\n";
  }

  void renderSubqueryInCluster(
      Context& context,
      const LogicalPlanNode& parent,
      const LogicalPlanNode& subqueryRoot) const {
    int clusterId = context.subqueryCounter++;

    // Start subgraph cluster for the subquery.
    context.out << "  subgraph cluster_subquery_" << clusterId << " {\n";
    context.out << "    label=\"Subquery\";\n";
    context.out << "    fontname=\"Helvetica\";\n";
    context.out << "    fontsize=10;\n";
    context.out << "    fontcolor=\"" << kPalette.subqueryEdge << "\";\n";
    context.out << "    style=dashed;\n";
    context.out << "    color=\"" << kPalette.subqueryEdge << "\";\n";
    context.out << "    penwidth=2;\n";
    context.out << "    margin=12;\n";
    context.out << "\n";

    // Render the subquery plan tree inside the cluster.
    subqueryRoot.accept(*this, context);

    // End subgraph cluster.
    context.out << "  }\n\n";

    // Use rank=same to position subquery to the right of parent node.
    context.out << "  {rank=same; " << nodeId(parent) << "; "
                << nodeId(subqueryRoot) << ";}\n";

    // Draw edge from parent to subquery root.
    context.out << "  " << nodeId(parent) << " -> " << nodeId(subqueryRoot)
                << " [color=\"" << kPalette.subqueryEdge
                << "\", penwidth=2, style=dashed, constraint=false];\n\n";
  }
};

} // namespace

// static
void LogicalPlanDotPrinter::print(
    const LogicalPlanNode& root,
    std::ostream& out) {
  out << "digraph LogicalPlan {\n";
  out << "  rankdir=TB;\n";
  out << "  newrank=true;\n";
  out << "  splines=spline;\n";
  out << "  nodesep=0.5;\n";
  out << "  ranksep=0.8;\n";
  out << "  node [fontname=\"Helvetica\", fontsize=10, fontcolor=\""
      << kPalette.text << "\"];\n";
  out << "  edge [fontname=\"Helvetica\", fontsize=10];\n";
  out << "\n";

  DotPrinterVisitor visitor;
  DotPrinterVisitor::Context context(out);
  root.accept(visitor, context);

  out << "}\n";
}

} // namespace facebook::axiom::logical_plan
