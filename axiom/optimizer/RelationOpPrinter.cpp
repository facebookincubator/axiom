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

#include "axiom/optimizer/RelationOpPrinter.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/RelationOpVisitor.h"

namespace facebook::axiom::optimizer {

namespace {

class ToTextVisitor : public RelationOpVisitor {
 public:
  struct Context : public RelationOpVisitorContext {
    std::stringstream out;
    size_t indent{0};
  };

  void visit(const TableScan& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);
    printHeader(op, myCtx);

    const auto& table = *op.as<TableScan>()->baseTable;

    const auto indentation = toIndentation(myCtx.indent + 1);
    auto appendLine = [&](const std::string& line) {
      myCtx.out << indentation << line << std::endl;
    };

    appendLine(fmt::format("table: {}", table.schemaTable->name()));
    if (!table.columnFilters.empty()) {
      appendLine(
          fmt::format(
              "single-column filters: {}",
              conjunctsToString(table.columnFilters)));
    }
    if (!table.filter.empty()) {
      appendLine(
          fmt::format(
              "multi-column filters: {}",
              conjunctsToString(table.columnFilters)));
    }
  }

  void visit(const Repartition& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Filter& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);
    printHeader(op, myCtx);
    myCtx.out << toIndentation(myCtx.indent + 2)
              << conjunctsToString(op.exprs()) << std::endl;
    printInput(*op.input(), myCtx);
  }

  void visit(const Project& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);
    if (op.isRedundant()) {
      printHeader(op, myCtx, "(redundant)");
    } else {
      printHeader(op, myCtx);
    }

    for (auto i = 0; i < op.exprs().size(); ++i) {
      myCtx.out << toIndentation(myCtx.indent + 2)
                << op.columns()[i]->toString()
                << " := " << op.exprs()[i]->toString() << std::endl;
    }

    printInput(*op.input(), myCtx);
  }

  void visit(const Join& op, RelationOpVisitorContext& context) const override {
    auto& myCtx = static_cast<Context&>(context);
    printHeader(
        op,
        myCtx,
        fmt::format(
            "{} {}",
            std::string(velox::core::JoinTypeName::toName(op.joinType)),
            std::string(JoinMethodName::toName(op.method))));

    const auto indentation = toIndentation(myCtx.indent + 2);
    auto appendLine = [&](const std::string& line) {
      myCtx.out << indentation << line << std::endl;
    };

    for (auto i = 0; i < op.leftKeys.size(); ++i) {
      appendLine(
          fmt::format(
              "{} = {}",
              op.leftKeys[i]->toString(),
              op.rightKeys[i]->toString()));
    }

    if (!op.filter.empty()) {
      appendLine(conjunctsToString(op.filter));
    }

    printInput(*op.input(), myCtx);
    printInput(*op.right, myCtx);
  }

  void visit(const HashBuild& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Aggregation& op, RelationOpVisitorContext& context)
      const override {
    const auto numGrouppingKeys = op.groupingKeys.size();

    std::stringstream groupingKeys;
    if (numGrouppingKeys > 0) {
      groupingKeys << "(";
      for (auto i = 0; i < numGrouppingKeys; ++i) {
        if (i > 0) {
          groupingKeys << ", ";
        }
        groupingKeys << op.groupingKeys[i]->toString();
      }
      groupingKeys << ")";
    }

    auto& myCtx = static_cast<Context&>(context);
    printHeader(op, myCtx, groupingKeys.str());

    for (auto i = 0; i < op.aggregates.size(); ++i) {
      myCtx.out << toIndentation(myCtx.indent + 2)
                << op.columns()[i + numGrouppingKeys]->toString()
                << " := " << op.aggregates[i]->toString() << std::endl;
    }

    printInput(*op.input(), myCtx);
  }

  void visit(const OrderBy& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const UnionAll& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);
    printHeader(op, myCtx);

    for (auto i = 0; i < op.inputs.size(); ++i) {
      printInput(*op.inputs[i], myCtx);
    }
  }

  void visit(const Limit& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);
    if (op.offset > 0) {
      printHeader(
          op, myCtx, fmt::format("({} offset {})", op.limit, op.offset));
    } else {
      printHeader(op, myCtx, fmt::format("({})", op.limit));
    }

    printInput(*op.input(), myCtx);
  }

  void visit(const Values& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Unnest& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);
    printHeader(op, myCtx);

    for (auto i = 0; i < op.unnestExprs.size(); ++i) {
      myCtx.out << toIndentation(myCtx.indent + 2)
                << op.unnestExprs[i]->toString() << std::endl;
    }

    printInput(*op.input(), myCtx);
  }

  void visit(const TableWrite& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

 private:
  static std::string toIndentation(size_t indent) {
    return std::string(indent * 2, ' ');
  }

  void visitDefault(const RelationOp& op, RelationOpVisitorContext& context)
      const {
    auto& myCtx = static_cast<Context&>(context);
    printHeader(op, myCtx);
    if (op.input() != nullptr) {
      printInput(*op.input(), myCtx);
    }
  }

  void printHeader(
      const RelationOp& op,
      Context& context,
      const std::string& extra = "") const {
    context.out << toIndentation(context.indent)
                << RelTypeName::toName(op.relType());
    if (!extra.empty()) {
      context.out << " " << extra;
    }
    context.out << " -> " << columnsToString(op.columns()) << std::endl;
  }

  void printInput(const RelationOp& op, Context& context) const {
    context.indent++;
    SCOPE_EXIT {
      context.indent--;
    };

    op.accept(*this, context);
  }
};

class OnelineVisitor : public RelationOpVisitor {
 public:
  struct Context : public RelationOpVisitorContext {
    std::stringstream out;
  };

  void visit(const TableScan& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);

    const auto& table = *op.as<TableScan>()->baseTable;
    myCtx.out << table.schemaTable->name();
  }

  void visit(const Repartition& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Filter& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Project& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Join& op, RelationOpVisitorContext& context) const override {
    auto& myCtx = static_cast<Context&>(context);

    myCtx.out << "(";
    op.input()->accept(*this, context);

    myCtx.out << " " << velox::core::JoinTypeName::toName(op.joinType) << " ";

    op.right->accept(*this, context);
    myCtx.out << ")";
  }

  void visit(const HashBuild& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Aggregation& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const OrderBy& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const UnionAll& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Limit& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Values& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Unnest& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const TableWrite& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

 private:
  void visitDefault(const RelationOp& op, RelationOpVisitorContext& context)
      const {
    if (op.input() != nullptr) {
      op.input()->accept(*this, context);
    }
  }
};

} // namespace

// static
std::string RelationOpPrinter::toText(const RelationOp& root) {
  ToTextVisitor::Context context;
  ToTextVisitor visitor;
  root.accept(visitor, context);
  return context.out.str();
}

// static
std::string RelationOpPrinter::toOneline(const RelationOp& root) {
  OnelineVisitor::Context context;
  OnelineVisitor visitor;
  root.accept(visitor, context);
  return context.out.str();
}

} // namespace facebook::axiom::optimizer
