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

#include "axiom/pyspark/SparkPlanPrinter.h"

#include <sstream>
#include "axiom/pyspark/SparkVeloxConverter.h"

namespace axiom::collagen {
namespace {

struct SparkPlanPrinterContext : public SparkPlanVisitorContext {
  std::stringstream out;
  int32_t indent{0};
};

std::string_view getGroupTypeString(
    spark::connect::Aggregate::GroupType groupType) {
  switch (groupType) {
    case spark::connect::Aggregate::GROUP_TYPE_GROUPBY:
      return "GROUPBY";
    case spark::connect::Aggregate::GROUP_TYPE_ROLLUP:
      return "ROLLUP";
    case spark::connect::Aggregate::GROUP_TYPE_CUBE:
      return "CUBE";
    case spark::connect::Aggregate::GROUP_TYPE_PIVOT:
      return "PIVOT";
    case spark::connect::Aggregate::GROUP_TYPE_UNSPECIFIED:
      return "UNSPECIFIED";
    default:
      COLLAGEN_NYI(
          "Group type to variant not implemented yet for '{}'", groupType);
  }
}

class SparkPlanPrinter : public SparkPlanVisitor {
 public:
  virtual ~SparkPlanPrinter() = default;

  void visit(
      const spark::connect::Read_NamedTable& namedTable,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- Read[" << context.planId << "]";
    out << "[" << namedTable.unparsed_identifier() << "] ";
    out << "(named table)" << std::endl;

    for (const auto& option : namedTable.options()) {
      out << extraIndent(context) << option.first << " - " << option.second
          << std::endl;
    }
  }

  void visit(
      const spark::connect::LocalRelation& localRelation,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- LocalRelation[" << context.planId << "]" << std::endl;
    out << extraIndent(context);
    out << "Schema: " << localRelation.schema() << std::endl;
    out << extraIndent(context);
    out << "Input bytes: " << localRelation.data().size() << std::endl;
  }

  void visit(
      const spark::connect::Project& project,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- Project[" << context.planId << "]:" << std::endl;

    for (const auto& expression : project.expressions()) {
      out << extraIndent(context);
      SparkPlanVisitor::visit(expression, context);
      out << std::endl;
    }
    addIndent(context);
    SparkPlanVisitor::visit(project, context);
    removeIndent(context);
  }

  void visit(
      const spark::connect::Filter& filter,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- Filter[" << context.planId << "] ";
    SparkPlanPrinter::visit(filter.condition(), context);
    out << std::endl;

    addIndent(context);
    SparkPlanVisitor::visit(filter, context);
    removeIndent(context);
  }

  void visit(
      const spark::connect::Aggregate& aggregate,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- Aggregate[" << context.planId << "]:" << std::endl;

    // Print group type.
    out << extraIndent(context)
        << "Group Type: " << getGroupTypeString(aggregate.group_type())
        << std::endl;

    // Print grouping expressions.
    if (!aggregate.grouping_expressions().empty()) {
      out << extraIndent(context) << "Grouping Expressions:" << std::endl;
      for (const auto& groupingExpr : aggregate.grouping_expressions()) {
        out << extraIndent(context) << "  ";
        SparkPlanPrinter::visit(groupingExpr, context);
        out << std::endl;
      }
    }

    // Print aggregate expressions.
    if (!aggregate.aggregate_expressions().empty()) {
      out << extraIndent(context) << "Aggregate Expressions:" << std::endl;
      for (const auto& aggExpr : aggregate.aggregate_expressions()) {
        out << extraIndent(context) << "  ";
        SparkPlanPrinter::visit(aggExpr, context);
        out << std::endl;
      }
    }

    // Print pivot information if present.
    if (aggregate.has_pivot()) {
      const auto& pivot = aggregate.pivot();
      out << extraIndent(context) << "Pivot:" << std::endl;
      out << extraIndent(context) << "  Column: ";
      SparkPlanPrinter::visit(pivot.col(), context);
      out << std::endl;

      if (!pivot.values().empty()) {
        out << extraIndent(context) << "  Values: ";
        bool first = true;
        for (const auto& value : pivot.values()) {
          if (!first) {
            out << ", ";
          }
          SparkPlanPrinter::visit(value, context);
          first = false;
        }
        out << std::endl;
      }
    }
  }

  void visit(const spark::connect::Join& join, SparkPlanVisitorContext& context)
      override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- Join[" << context.planId << "] (type=" << join.join_type()
        << "): ";
    SparkPlanPrinter::visit(join.join_condition(), context);
    out << std::endl;

    addIndent(context);
    SparkPlanPrinter::visit(join.left(), context);
    SparkPlanPrinter::visit(join.right(), context);
    removeIndent(context);
  }

  void visit(
      const spark::connect::Limit& limit,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- Limit[" << context.planId << "] ";
    out << "(" << limit.limit() << ")";
    out << std::endl;

    addIndent(context);
    SparkPlanVisitor::visit(limit, context);
    removeIndent(context);
  }

  void visit(
      const spark::connect::Offset& offset,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- Offset[" << context.planId << "] ";
    out << "(" << offset.offset() << ")";
    out << std::endl;

    addIndent(context);
    SparkPlanVisitor::visit(offset, context);
    removeIndent(context);
  }

  void visit(const spark::connect::SQL& sql, SparkPlanVisitorContext& context)
      override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- SQL[" << context.planId << "]:" << std::endl;
    out << extraIndent(context) << sql.query() << std::endl;
  }

  void visit(
      const spark::connect::SubqueryAlias& subqueryAlias,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- Alias [" << context.planId << "]: ";
    out << subqueryAlias.alias() << std::endl;

    addIndent(context);
    SparkPlanVisitor::visit(subqueryAlias, context);
    removeIndent(context);
  }

  void visit(const spark::connect::ToDF& toDF, SparkPlanVisitorContext& context)
      override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- ToDF[" << context.planId << "]:" << std::endl;

    bool first = true;
    for (const auto& columnName : toDF.column_names()) {
      if (first) {
        first = false;
        out << extraIndent(context);
      } else {
        out << ", ";
      }
      out << columnName;
    }
    out << std::endl;

    addIndent(context);
    SparkPlanVisitor::visit(toDF, context);
    removeIndent(context);
  }

  void visit(
      const spark::connect::ShowString& showString,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- ShowString[" << context.planId << "]:" << std::endl;
    out << extraIndent(context) << "num_rows: " << showString.num_rows()
        << std::endl;

    addIndent(context);
    SparkPlanVisitor::visit(showString, context);
    removeIndent(context);
  }

  void visit(
      const spark::connect::WriteOperation& writeOperation,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- WriteOperation:";

    if (writeOperation.has_source()) {
      out << " (" << writeOperation.source() << ")";
    }
    out << std::endl;

    // Writing to a path or table.
    out << extraIndent(context);
    switch (writeOperation.save_type_case()) {
      case spark::connect::WriteOperation::kPath:
        out << "path: " << writeOperation.path();
        break;

      case spark::connect::WriteOperation::kTable:
        out << "table: " << writeOperation.table().table_name();
        break;

      case spark::connect::WriteOperation::SAVE_TYPE_NOT_SET:
        out << "(not set)";
        break;
    }
    out << std::endl;

    addIndent(context);
    SparkPlanVisitor::visit(writeOperation, context);
    removeIndent(context);
  }

  void visit(const spark::connect::Sort& sort, SparkPlanVisitorContext& context)
      override {
    auto& out = getOutput(context);
    out << indent(context);
    out << "- Sort[" << context.planId << "]:" << std::endl;

    // Print sort orders
    if (!sort.order().empty()) {
      out << extraIndent(context) << "Order by:" << std::endl;
      for (const auto& sortOrder : sort.order()) {
        out << extraIndent(context) << "  ";

        // Print the expression being sorted
        SparkPlanPrinter::visit(sortOrder.child(), context);

        // Print sort direction
        switch (sortOrder.direction()) {
          case spark::connect::Expression::SortOrder::SORT_DIRECTION_ASCENDING:
            out << " ASC";
            break;
          case spark::connect::Expression::SortOrder::SORT_DIRECTION_DESCENDING:
            out << " DESC";
            break;
          default:
            out << " ASC"; // Default
            break;
        }

        // Print null ordering
        switch (sortOrder.null_ordering()) {
          case spark::connect::Expression::SortOrder::SORT_NULLS_FIRST:
            out << " NULLS FIRST";
            break;
          case spark::connect::Expression::SortOrder::SORT_NULLS_LAST:
            out << " NULLS LAST";
            break;
          default:
            out << " NULLS LAST"; // Default
            break;
        }

        out << std::endl;
      }
    }

    addIndent(context);
    SparkPlanVisitor::visit(sort, context);
    removeIndent(context);
  }

  void visit(
      const spark::connect::Relation& relation,
      SparkPlanVisitorContext& context) override {
    SparkPlanVisitor::visit(relation, context);
  }

  void visit(const spark::connect::Plan& plan, SparkPlanVisitorContext& context)
      override {
    SparkPlanVisitor::visit(plan, context);
  }

  // Expressions:

  void visit(
      const spark::connect::Expression& expression,
      SparkPlanVisitorContext& context) override {
    SparkPlanVisitor::visit(expression, context);
  }

  void visit(
      const spark::connect::Expression::Literal& literal,
      SparkPlanVisitorContext& context) override {
    getOutput(context) << *toVeloxVariant(literal);
  }

  void visit(
      const spark::connect::Expression::UnresolvedAttribute&
          unresolvedAttribute,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << unresolvedAttribute.unparsed_identifier();
    if (unresolvedAttribute.has_plan_id()) {
      out << "[plan_id=" << unresolvedAttribute.plan_id() << "]";
    } else {
      out << "[none]";
    }
  }

  void visit(
      const spark::connect::Expression::UnresolvedFunction& unresolvedFunction,
      SparkPlanVisitorContext& context) override {
    auto& out = getOutput(context);
    out << unresolvedFunction.function_name() << "(";

    bool first = true;
    for (const auto& argument : unresolvedFunction.arguments()) {
      if (first) {
        first = false;
      } else {
        out << ", ";
      }
      visit(argument, context);
    }
    out << ")";
  }

  void visit(
      const spark::connect::Expression::Alias& alias,
      SparkPlanVisitorContext& context) override {
    SparkPlanVisitor::visit(alias.expr(), context);

    auto& out = getOutput(context);
    bool first = true;

    for (const auto& name : alias.name()) {
      if (first) {
        out << " AS ";
        first = false;
      } else {
        out << ", ";
      }
      out << name;
    }
  }

  void visit(
      const spark::connect::Expression::UnresolvedExtractValue&
          unresolvedExtractValue,
      SparkPlanVisitorContext& context) override {
    visit(unresolvedExtractValue.child(), context);

    auto& out = getOutput(context);
    out << "[";
    visit(unresolvedExtractValue.extraction(), context);
    out << "]";
  }

  virtual void visit(
      const spark::connect::Expression::UnresolvedStar& /*unresolvedStar*/,
      SparkPlanVisitorContext& context) override {
    getOutput(context) << "*";
  }

 private:
  std::stringstream& getOutput(SparkPlanVisitorContext& context) const {
    return static_cast<SparkPlanPrinterContext&>(context).out;
  }

  std::string indent(int32_t size) const {
    return std::string(size * 2, ' ');
  }

  std::string indent(const SparkPlanVisitorContext& context) const {
    return indent(static_cast<const SparkPlanPrinterContext&>(context).indent);
  }

  std::string extraIndent(const SparkPlanVisitorContext& context) const {
    return indent(
        static_cast<const SparkPlanPrinterContext&>(context).indent + 2);
  }

  void addIndent(SparkPlanVisitorContext& context) const {
    static_cast<SparkPlanPrinterContext&>(context).indent++;
  }

  void removeIndent(SparkPlanVisitorContext& context) const {
    static_cast<SparkPlanPrinterContext&>(context).indent--;
  }
};

} // namespace

std::string printSparkPlan(const spark::connect::Plan& plan) {
  SparkPlanPrinter printer;
  SparkPlanPrinterContext context;
  printer.visit(plan, context);
  return context.out.str();
}

} // namespace axiom::collagen
