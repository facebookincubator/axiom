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
    const RelationOpToTextOptions& options;

    std::stringstream out;
    size_t indent{0};

    explicit Context(const RelationOpToTextOptions& options)
        : options(options) {}
  };

  void visit(const TableScan& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);
    printHeader(op, myCtx);
    printConstraints(op, myCtx);

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
              "multi-column filters: {}", conjunctsToString(table.filter)));
    }
  }

  void visit(const Repartition& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);
    printHeader(
        op,
        myCtx,
        op.distribution().isBroadcast() ? "(broadcast)"
            : op.distribution().isGather()
            ? "(gather)"
            : fmt::format(
                  "({})", exprsToString(op.distribution().partitionKeys())));

    printConstraints(op, myCtx);
    printInput(*op.input(), myCtx);
  }

  void visit(const Filter& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);
    printHeader(op, myCtx);
    printConstraints(op, myCtx);
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
                << " := " << op.exprs()[i]->toString();
      if (myCtx.options.includeConstraints) {
        myCtx.out << formatConstraint(op, op.columns()[i]->id());
      }
      myCtx.out << std::endl;
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

    printConstraints(op, myCtx);

    const auto indentation = toIndentation(myCtx.indent + 2);
    for (auto i = 0; i < op.leftKeys.size(); ++i) {
      myCtx.out << indentation
                << fmt::format(
                       "{} = {}",
                       op.leftKeys[i]->toString(),
                       op.rightKeys[i]->toString())
                << std::endl;
    }

    if (!op.filter.empty()) {
      myCtx.out << indentation << conjunctsToString(op.filter) << std::endl;
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
                << " := " << op.aggregates[i]->toString();
      if (myCtx.options.includeConstraints) {
        myCtx.out << formatConstraint(
            op, op.columns()[i + numGrouppingKeys]->id());
      }
      myCtx.out << std::endl;
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
    printConstraints(op, myCtx);

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

    printConstraints(op, myCtx);
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

    printConstraints(op, myCtx);

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

  void visit(const EnforceSingleRow& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const AssignUniqueId& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const EnforceDistinct& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Window& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const RowNumber& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const TopNRowNumber& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const GroupId& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);

    std::stringstream groupingSets;
    groupingSets << "(";
    for (size_t i = 0; i < op.groupingSets().size(); ++i) {
      if (i > 0) {
        groupingSets << ", ";
      }
      // Convert indices to output names for readability.
      groupingSets << "[";
      const auto& set = op.groupingSets()[i];
      for (size_t j = 0; j < set.size(); ++j) {
        if (j > 0) {
          groupingSets << ", ";
        }
        groupingSets << op.groupingKeyInfos()[set[j]].output;
      }
      groupingSets << "]";
    }
    groupingSets << ")";

    printHeader(op, myCtx, groupingSets.str());
    printConstraints(op, myCtx);

    const auto indentation = toIndentation(myCtx.indent + 2);
    for (const auto& keyInfo : op.groupingKeyInfos()) {
      myCtx.out << indentation << keyInfo.output
                << " := " << keyInfo.input->toString() << std::endl;
    }
    myCtx.out << indentation
              << "groupIdColumn: " << op.groupIdColumn()->toString()
              << std::endl;

    printInput(*op.input(), myCtx);
  }

 private:
  static std::string toIndentation(size_t indent) {
    return std::string(indent * 2, ' ');
  }

  void visitDefault(const RelationOp& op, RelationOpVisitorContext& context)
      const {
    auto& myCtx = static_cast<Context&>(context);
    printHeader(op, myCtx);
    printConstraints(op, myCtx);
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

    context.out << " [" << std::fixed << std::setprecision(2)
                << op.resultCardinality() << " rows] -> "
                << columnsToString(op.columns()) << std::endl;

    if (context.options.includeCost) {
      context.out << toIndentation(context.indent + 1)
                  << "Estimates: fanout = " << std::fixed
                  << std::setprecision(2) << op.cost().fanout
                  << ", cost = " << std::fixed << std::setprecision(2)
                  << op.cost().totalCost() << std::endl;
    }
  }

  // Formats constraint info for a column, e.g. " BIGINT (ndv=2.00 min=3
  // max=4)". Returns empty string if no constraint found.
  static std::string formatConstraint(const RelationOp& op, int32_t columnId) {
    auto it = op.constraints().find(columnId);
    if (it == op.constraints().end()) {
      return "";
    }

    const auto& value = it->second;
    std::stringstream out;
    out << " " << value.type->toString() << " (ndv=" << std::fixed
        << std::setprecision(2) << value.cardinality;
    if (value.min != nullptr) {
      out << " min=" << *value.min;
    }
    if (value.max != nullptr) {
      out << " max=" << *value.max;
    }
    if (value.nullFraction > 0) {
      out << " nulls=" << std::fixed << std::setprecision(0)
          << (value.nullFraction * 100) << "%";
    }
    out << ")";
    return out.str();
  }

  // Prints column constraints (type, ndv, min, max) in a box-drawing
  // style that visually separates them from operator-specific details:
  //
  //   | Constraints:
  //   |    t3.r_name VARCHAR (ndv=55 min="AFRICA" max="MIDDLE EAST")
  //   |___ t3.r_regionkey BIGINT (ndv=55 min=0 max=4)
  //
  // No-op when includeConstraints is false or no columns have constraints.
  void printConstraints(const RelationOp& op, Context& context) const {
    if (!context.options.includeConstraints) {
      return;
    }
    std::vector<std::string> lines;
    for (auto* column : op.columns()) {
      auto constraint = formatConstraint(op, column->id());
      if (!constraint.empty()) {
        lines.push_back(column->toString() + constraint);
      }
    }
    if (lines.empty()) {
      return;
    }
    const auto indentation = toIndentation(context.indent + 1);
    context.out << indentation << "| Constraints:" << std::endl;
    for (auto i = 0; i < lines.size(); ++i) {
      context.out << indentation;
      if (i + 1 < lines.size()) {
        context.out << "|    ";
      } else {
        context.out << "|___ ";
      }
      context.out << lines[i] << std::endl;
    }
  }

  void printInput(const RelationOp& op, Context& context) const {
    if (context.options.maxDepth.has_value() &&
        context.indent >= *context.options.maxDepth) {
      return;
    }

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
    auto& myCtx = static_cast<Context&>(context);
    myCtx.out << "agg(";
    op.input()->accept(*this, context);
    myCtx.out << ")";
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

  void visit(const EnforceSingleRow& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const AssignUniqueId& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const EnforceDistinct& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const Window& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const RowNumber& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const TopNRowNumber& op, RelationOpVisitorContext& context)
      const override {
    visitDefault(op, context);
  }

  void visit(const GroupId& op, RelationOpVisitorContext& context)
      const override {
    auto& myCtx = static_cast<Context&>(context);
    myCtx.out << "groupid(";
    op.input()->accept(*this, context);
    myCtx.out << ")";
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
std::string RelationOpPrinter::toText(
    const RelationOp& root,
    const RelationOpToTextOptions& options) {
  ToTextVisitor::Context context(options);
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
