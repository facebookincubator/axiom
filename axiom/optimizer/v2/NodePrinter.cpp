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

#include "axiom/optimizer/v2/NodePrinter.h"

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ranges>
#include <sstream>
#include "axiom/optimizer/v2/NodeVisitor.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

std::string spaces(size_t depth) {
  return std::string(depth, ' ');
}

std::string formatExpr(ExprCP expr) {
  if (expr == nullptr) {
    return "<null>";
  }
  return expr->toString();
}

std::string formatColumns(const ColumnVector& columns) {
  auto formatted =
      columns | std::views::transform([](ColumnCP c) {
        return fmt::format("{}:{}", c->toString(), c->value().type->toString());
      });
  return fmt::to_string(fmt::join(formatted, ", "));
}

// Templated so any range of Expr-derived pointers (ExprVector,
// AggregateCallVector, etc.) formats the same way.
template <typename Range>
std::string formatExprs(const Range& exprs) {
  auto texts = exprs |
      std::views::transform([](const auto* expr) { return formatExpr(expr); });
  return fmt::to_string(fmt::join(texts, ", "));
}

class Printer : public NodeVisitor {
 public:
  struct Context : public NodeVisitorContext {
    std::stringstream out;
    size_t indent{0};
  };

  // Bumps `ctx.indent` by 2 on construction, restores on destruction.
  class IndentGuard {
   public:
    explicit IndentGuard(Context& ctx) : ctx_(ctx) {
      ctx_.indent += 2;
    }
    ~IndentGuard() {
      ctx_.indent -= 2;
    }
    IndentGuard(const IndentGuard&) = delete;
    IndentGuard& operator=(const IndentGuard&) = delete;

   private:
    Context& ctx_;
  };

  void visit(const Scan& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(
        ctx, node, fmt::format("[{}]", node.baseTable()->schemaTable->name()));
    const auto pad = spaces(ctx.indent + 2);
    for (ExprCP filter : node.filters()) {
      ctx.out << pad << "filter: " << formatExpr(filter) << '\n';
    }
    visitInputs(node, ctx);
  }

  void visit(const Filter& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    const auto pad = spaces(ctx.indent + 2);
    for (ExprCP pred : node.predicates()) {
      ctx.out << pad << "predicate: " << formatExpr(pred) << '\n';
    }
    visitInputs(node, ctx);
  }

  void visit(const Project& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    const auto pad = spaces(ctx.indent + 2);
    const auto& cols = node.outputColumns();
    const auto& exprs = node.exprs();
    for (size_t i = 0; i < cols.size(); ++i) {
      ctx.out << pad << cols[i]->toString() << " := " << formatExpr(exprs[i])
              << '\n';
    }
    visitInputs(node, ctx);
  }

  void visit(const Limit& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node, fmt::format("[{}, {}]", node.offset(), node.count()));
    visitInputs(node, ctx);
  }

  void visit(const Sort& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    ctx.out << spaces(ctx.indent + 2)
            << "orderKeys: " << formatExprs(node.orderKeys()) << '\n';
    visitInputs(node, ctx);
  }

  void visit(const TopN& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node, fmt::format("[{}, {}]", node.offset(), node.count()));
    ctx.out << spaces(ctx.indent + 2)
            << "orderKeys: " << formatExprs(node.orderKeys()) << '\n';
    visitInputs(node, ctx);
  }

  void visit(const Aggregate& node, NodeVisitorContext& context)
      const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    const auto pad = spaces(ctx.indent + 2);
    if (!node.groupingKeys().empty()) {
      ctx.out << pad << "groupingKeys: " << formatExprs(node.groupingKeys())
              << '\n';
    }
    if (!node.aggregates().empty()) {
      ctx.out << pad << "aggregates: " << formatExprs(node.aggregates())
              << '\n';
    }
    if (!node.globalGroupingSets().empty()) {
      ctx.out << pad << "globalGroupingSets: "
              << folly::join(", ", node.globalGroupingSets()) << '\n';
    }
    visitInputs(node, ctx);
  }

  void visit(const GroupId& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    const auto pad = spaces(ctx.indent + 2);
    ctx.out << pad << "groupingKeys: " << formatExprs(node.groupingKeys())
            << '\n';
    if (!node.aggregationInputs().empty()) {
      ctx.out << pad
              << "aggregationInputs: " << formatExprs(node.aggregationInputs())
              << '\n';
    }
    ctx.out << pad << "groupingSets: " << node.groupingSets().size()
            << " set(s)\n";
    visitInputs(node, ctx);
  }

  void visit(const MarkDistinct& node, NodeVisitorContext& context)
      const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    const auto pad = spaces(ctx.indent + 2);
    ctx.out << pad << "distinctKeys: " << formatExprs(node.distinctKeys())
            << '\n';
    if (!node.masks().empty()) {
      ctx.out << pad << "masks: " << formatExprs(node.masks()) << '\n';
    }
    ctx.out << pad << "markers: " << formatExprs(node.markers()) << '\n';
    visitInputs(node, ctx);
  }

  void visit(const Values& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    visitInputs(node, ctx);
  }

  void visit(const Unnest& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    const auto pad = spaces(ctx.indent + 2);
    ctx.out << pad
            << "unnestExpressions: " << formatExprs(node.unnestExpressions())
            << '\n';
    if (node.withOrdinality()) {
      ctx.out << pad << "withOrdinality: true\n";
    }
    visitInputs(node, ctx);
  }

  void visit(const UnionAll& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    const auto pad = spaces(ctx.indent + 2);
    const auto& legColumns = node.legColumns();
    for (size_t i = 0; i < legColumns.size(); ++i) {
      ctx.out << pad << "leg[" << i << "] columns: "
              << formatExprs(
                     ExprVector(legColumns[i].begin(), legColumns[i].end()))
              << '\n';
    }
    visitInputs(node, ctx);
  }

  void visit(const Join& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    std::string details = std::string{"["} + std::string{node.joinTypeName()};
    if (node.nullAware()) {
      details += ", nullAware";
    }
    details += ']';
    header(ctx, node, details);
    const auto pad = spaces(ctx.indent + 2);
    if (!node.leftKeys().empty()) {
      ctx.out << pad << "leftKeys: " << formatExprs(node.leftKeys()) << '\n';
      ctx.out << pad << "rightKeys: " << formatExprs(node.rightKeys()) << '\n';
    }
    if (!node.filter().empty()) {
      ctx.out << pad << "filter: " << formatExprs(node.filter()) << '\n';
    }
    visitInputs(node, ctx);
  }

  void visit(const Window& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    const auto pad = spaces(ctx.indent + 2);
    if (!node.partitionKeys().empty()) {
      ctx.out << pad << "partitionKeys: " << formatExprs(node.partitionKeys())
              << '\n';
    }
    if (!node.orderKeys().empty()) {
      ctx.out << pad << "orderKeys: " << formatExprs(node.orderKeys()) << '\n';
    }
    visitInputs(node, ctx);
  }

  void visit(const TopNRowNumber& node, NodeVisitorContext& context)
      const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node, fmt::format("[limit {}]", node.limit()));
    const auto pad = spaces(ctx.indent + 2);
    if (!node.partitionKeys().empty()) {
      ctx.out << pad << "partitionKeys: " << formatExprs(node.partitionKeys())
              << '\n';
    }
    ctx.out << pad << "orderKeys: " << formatExprs(node.orderKeys()) << '\n';
    visitInputs(node, ctx);
  }

  void visit(const Apply& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    std::string details = std::string{"["} + std::string{node.kindName()};
    if (node.enforceSingleRow()) {
      details += ", enforceSingleRow";
    }
    if (node.nullAware()) {
      details += ", nullAware";
    }
    details += ']';
    header(ctx, node, details);
    const auto pad = spaces(ctx.indent + 2);
    if (!node.correlationColumns().empty()) {
      ctx.out << pad
              << "correlation: " << formatColumns(node.correlationColumns())
              << '\n';
    }
    if (!node.filter().empty()) {
      ctx.out << pad << "filter: " << formatExprs(node.filter()) << '\n';
    }
    if (node.markColumn() != nullptr) {
      ctx.out << pad << "mark: " << node.markColumn()->toString() << '\n';
    }
    if (node.inLhs() != nullptr) {
      ctx.out << pad << "inLhs: " << formatExpr(node.inLhs()) << '\n';
      ctx.out << pad << "inBodyKey: " << formatExpr(node.inBodyKey()) << '\n';
    }
    visitInputs(node, ctx);
  }

  void visit(const EnforceSingleRow& node, NodeVisitorContext& context)
      const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    visitInputs(node, ctx);
  }

  void visit(const AssignUniqueId& node, NodeVisitorContext& context)
      const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    ctx.out << spaces(ctx.indent + 2)
            << "idColumn: " << node.idColumn()->toString() << '\n';
    visitInputs(node, ctx);
  }

  void visit(const EnforceDistinct& node, NodeVisitorContext& context)
      const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    const auto pad = spaces(ctx.indent + 2);
    ctx.out << pad << "distinctKeys: " << formatExprs(node.distinctKeys())
            << '\n';
    if (node.errorMessage() != nullptr) {
      ctx.out << pad << "errorMessage: " << node.errorMessage() << '\n';
    }
    visitInputs(node, ctx);
  }

  void visit(const Exchange& node, NodeVisitorContext& context) const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    ctx.out << spaces(ctx.indent + 2)
            << "partitionKeys: " << formatExprs(node.partitioning().keys)
            << '\n';
    visitInputs(node, ctx);
  }

  void visit(const TableWrite& node, NodeVisitorContext& context)
      const override {
    auto& ctx = static_cast<Context&>(context);
    header(ctx, node);
    ctx.out << spaces(ctx.indent + 2)
            << "columnExprs: " << formatExprs(node.columnExprs()) << '\n';
    visitInputs(node, ctx);
  }

 private:
  void header(Context& ctx, const Node& node, std::string_view extra = {})
      const {
    ctx.out << spaces(ctx.indent) << "- "
            << NodeTypeName::toName(node.nodeType());
    if (!extra.empty()) {
      ctx.out << extra;
    }
    ctx.out << " -> " << formatColumns(node.outputColumns()) << '\n';
  }

  void visitInputs(const Node& node, Context& ctx) const {
    IndentGuard guard(ctx);
    NodeVisitor::visitInputs(node, ctx);
  }
};

} // namespace

std::string NodePrinter::toText(NodeCP root) {
  Printer::Context ctx;
  root->accept(Printer{}, ctx);
  return ctx.out.str();
}

} // namespace facebook::axiom::optimizer::v2
