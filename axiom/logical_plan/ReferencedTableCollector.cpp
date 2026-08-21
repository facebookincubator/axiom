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

#include "axiom/logical_plan/ReferencedTableCollector.h"
#include "axiom/logical_plan/ExprVisitor.h"
#include "axiom/logical_plan/PlanNodeVisitor.h"

namespace facebook::axiom::logical_plan {
namespace {

void collectFromPlan(const LogicalPlanNode& plan, ReferencedTables& tables);

// Reaches the plan a subquery expression carries. Every other case is there to
// walk through the expression trees that may hold one.
class SubqueryTableVisitor : public ExprVisitor {
 public:
  struct Context : public ExprVisitorContext {
    explicit Context(ReferencedTables& tables) : tables{tables} {}

    ReferencedTables& tables;
  };

  void visit(const SubqueryExpr& expr, ExprVisitorContext& ctx) const override {
    collectFromPlan(*expr.subquery(), static_cast<Context&>(ctx).tables);
  }

  void visit(const InputReferenceExpr& expr, ExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }

  void visit(const ConstantExpr& expr, ExprVisitorContext& ctx) const override {
    visitInputs(expr, ctx);
  }

  void visit(const CallExpr& expr, ExprVisitorContext& ctx) const override {
    visitInputs(expr, ctx);
  }

  void visit(const SpecialFormExpr& expr, ExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }

  void visit(const AggregateExpr& expr, ExprVisitorContext& ctx)
      const override {
    visitAggregate(expr, ctx);
  }

  void visit(const SpecialFormAggExpr& expr, ExprVisitorContext& ctx)
      const override {
    visitAggregate(expr, ctx);
  }

  void visit(const WindowExpr& expr, ExprVisitorContext& ctx) const override {
    for (const auto& key : expr.partitionKeys()) {
      key->accept(*this, ctx);
    }
    visitOrdering(expr.ordering(), ctx);
    if (expr.frame().startValue != nullptr) {
      expr.frame().startValue->accept(*this, ctx);
    }
    if (expr.frame().endValue != nullptr) {
      expr.frame().endValue->accept(*this, ctx);
    }
    visitInputs(expr, ctx);
  }

  void visit(const LambdaExpr& expr, ExprVisitorContext& ctx) const override {
    expr.body()->accept(*this, ctx);
  }

 private:
  void visitOrdering(
      const std::vector<SortingField>& ordering,
      ExprVisitorContext& ctx) const {
    for (const auto& field : ordering) {
      field.expression->accept(*this, ctx);
    }
  }

  void visitAggregate(const AggregateExpr& expr, ExprVisitorContext& ctx)
      const {
    if (expr.filter() != nullptr) {
      expr.filter()->accept(*this, ctx);
    }
    visitOrdering(expr.ordering(), ctx);
    visitInputs(expr, ctx);
  }
};

class TableVisitor : public PlanNodeVisitor {
 public:
  struct Context : public PlanNodeVisitorContext {
    explicit Context(ReferencedTables& tables) : tables{tables} {}

    ReferencedTables& tables;
  };

  void visit(const ValuesNode& node, PlanNodeVisitorContext& ctx)
      const override {
    if (const auto* rows = std::get_if<ValuesNode::Exprs>(&node.data())) {
      for (const auto& row : *rows) {
        visitExprs(row, ctx);
      }
    }
  }

  void visit(const TableScanNode& node, PlanNodeVisitorContext& ctx)
      const override {
    static_cast<Context&>(ctx).tables.inputTables.insert(
        {.catalogName = node.connectorId(),
         .schemaTableName = node.tableName()});
  }

  void visit(const FilterNode& node, PlanNodeVisitorContext& ctx)
      const override {
    visitExpr(*node.predicate(), ctx);
    visitInputs(node, ctx);
  }

  void visit(const ProjectNode& node, PlanNodeVisitorContext& ctx)
      const override {
    visitExprs(node.expressions(), ctx);
    visitInputs(node, ctx);
  }

  void visit(const AggregateNode& node, PlanNodeVisitorContext& ctx)
      const override {
    visitExprs(node.groupingKeys(), ctx);
    for (const auto& aggregate : node.aggregates()) {
      visitExpr(*aggregate, ctx);
    }
    visitInputs(node, ctx);
  }

  void visit(const JoinNode& node, PlanNodeVisitorContext& ctx) const override {
    visitCondition(node.condition(), ctx);
    visitInputs(node, ctx);
  }

  void visit(const LateralJoinNode& node, PlanNodeVisitorContext& ctx)
      const override {
    visitCondition(node.condition(), ctx);
    visitInputs(node, ctx);
  }

  void visit(const SortNode& node, PlanNodeVisitorContext& ctx) const override {
    for (const auto& field : node.ordering()) {
      visitExpr(*field.expression, ctx);
    }
    visitInputs(node, ctx);
  }

  void visit(const LimitNode& node, PlanNodeVisitorContext& ctx)
      const override {
    visitInputs(node, ctx);
  }

  void visit(const SetNode& node, PlanNodeVisitorContext& ctx) const override {
    visitInputs(node, ctx);
  }

  void visit(const UnnestNode& node, PlanNodeVisitorContext& ctx)
      const override {
    visitExprs(node.unnestExpressions(), ctx);
    visitInputs(node, ctx);
  }

  void visit(const TableWriteNode& node, PlanNodeVisitorContext& ctx)
      const override {
    auto& tables = static_cast<Context&>(ctx).tables;
    CatalogSchemaTableName outputTable{
        .catalogName = node.connectorId(), .schemaTableName = node.tableName()};

    // A node rejects a write as its input, so the only way a second write
    // reaches here is through the plan a subquery expression carries.
    VELOX_USER_CHECK(
        !tables.outputTable.has_value(),
        "Plan cannot write more than one table: {} and {}",
        tables.outputTable.value(),
        outputTable);

    tables.outputTable = std::move(outputTable);
    visitExprs(node.columnExpressions(), ctx);
    visitInputs(node, ctx);
  }

  void visit(const SampleNode& node, PlanNodeVisitorContext& ctx)
      const override {
    visitExpr(*node.percentage(), ctx);
    visitInputs(node, ctx);
  }

  void visit(const OutputNode& node, PlanNodeVisitorContext& ctx)
      const override {
    visitInputs(node, ctx);
  }

  void visit(const FixedPointNode& node, PlanNodeVisitorContext& ctx)
      const override {
    visitInputs(node, ctx);
  }

  void visit(
      const RecursiveReferenceNode& /*node*/,
      PlanNodeVisitorContext& /*ctx*/) const override {
    // Stands for the rows of the enclosing fixed point, not for a table.
  }

 private:
  static void visitCondition(
      const ExprPtr& condition,
      PlanNodeVisitorContext& ctx) {
    if (condition != nullptr) {
      visitExpr(*condition, ctx);
    }
  }

  static void visitExpr(const Expr& expr, PlanNodeVisitorContext& ctx) {
    const SubqueryTableVisitor visitor;
    SubqueryTableVisitor::Context exprContext{
        static_cast<Context&>(ctx).tables};
    expr.accept(visitor, exprContext);
  }

  static void visitExprs(
      const std::vector<ExprPtr>& exprs,
      PlanNodeVisitorContext& ctx) {
    for (const auto& expr : exprs) {
      visitExpr(*expr, ctx);
    }
  }
};

void collectFromPlan(const LogicalPlanNode& plan, ReferencedTables& tables) {
  const TableVisitor visitor;
  TableVisitor::Context context{tables};
  plan.accept(visitor, context);
}

} // namespace

// static
ReferencedTables ReferencedTableCollector::collect(
    const LogicalPlanNode& plan) {
  ReferencedTables tables;
  collectFromPlan(plan, tables);
  return tables;
}

} // namespace facebook::axiom::logical_plan
