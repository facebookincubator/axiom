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

#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/logical_plan/PlanNodeVisitor.h"

namespace facebook::velox::logical_plan {

namespace {

class ToTextVisitor : public PlanNodeVisitor {
 public:
  struct Context : public PlanNodeVisitorContext {
    std::stringstream out;
    int32_t indent{0};
  };

  void visit(const ValuesNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode(
        "Values", node, fmt::format("{} rows", node.cardinality()), context);
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
    auto& myContext = static_cast<Context&>(context);
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
    auto& myContext = static_cast<Context&>(context);
    myContext.out << makeIndent(myContext.indent) << "- Aggregate(";

    const auto numKeys = node.groupingKeys().size();
    for (auto i = 0; i < numKeys; ++i) {
      if (i > 0) {
        myContext.out << ", ";
      }
      myContext.out << ExprPrinter::toText(*node.groupingKeys().at(i));
    }

    myContext.out << ")";

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
        node.condition() != nullptr ? ExprPrinter::toText(*node.condition())
                                    : "",
        context);
  }

  void visit(const SortNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& myContext = static_cast<Context&>(context);
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
    appendNode(SetOperationName::toName(node.operation()), node, context);
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
      std::string_view name,
      const LogicalPlanNode& node,
      const std::optional<std::string>& details,
      PlanNodeVisitorContext& context) const {
    auto& myContext = static_cast<Context&>(context);
    myContext.out << makeIndent(myContext.indent) << "- " << name << ":";

    if (details.has_value()) {
      myContext.out << " " << details.value();
    }

    appendOutputType(node, myContext);

    myContext.out << std::endl;

    appendInputs(node, myContext);
  }

  void appendNode(
      std::string_view name,
      const LogicalPlanNode& node,
      PlanNodeVisitorContext& context) const {
    appendNode(name, node, std::nullopt, context);
  }

  void appendOutputType(const LogicalPlanNode& node, Context& context) const {
    context.out << " -> " << node.outputType()->toString();
  }

  void appendInputs(const LogicalPlanNode& node, Context& context) const {
    context.indent++;
    PlanNodeVisitor::visitInputs(node, context);
    context.indent--;
  }
};

std::string truncate(const std::string& str, size_t maxLength = 50) {
  if (str.size() > maxLength) {
    return str.substr(0, maxLength) + "...";
  }
  return str;
}

class ExprStats : public ExprVisitorContext {
 public:
  std::unordered_map<std::string, int64_t>& functionCounts() {
    return functionCounts_;
  }

  const std::unordered_map<std::string, int64_t>& functionCounts() const {
    return functionCounts_;
  }

  std::unordered_map<std::string, int64_t>& expressionCounts() {
    return expressionCounts_;
  }

  const std::unordered_map<std::string, int64_t>& expressionCounts() const {
    return expressionCounts_;
  }

  std::unordered_map<velox::TypePtr, int64_t>& constantCounts() {
    return constantCounts_;
  }

  const std::unordered_map<velox::TypePtr, int64_t>& constantCounts() const {
    return constantCounts_;
  }

  void merge(const ExprStats& other) {
    for (const auto& [name, count] : other.functionCounts()) {
      functionCounts_[name] += count;
    }

    for (const auto& [name, count] : other.expressionCounts()) {
      expressionCounts_[name] += count;
    }

    for (const auto& [type, count] : other.constantCounts()) {
      constantCounts_[type] += count;
    }
  }

 private:
  std::unordered_map<std::string, int64_t> functionCounts_;
  std::unordered_map<std::string, int64_t> expressionCounts_;
  std::unordered_map<velox::TypePtr, int64_t> constantCounts_;
};

class CollectExprStatsPlanNodeVisitor : public PlanNodeVisitor {
 public:
  struct Context : public PlanNodeVisitorContext {
    ExprStats stats;
  };

  void visit(const ValuesNode& node, PlanNodeVisitorContext& context)
      const override {
    // Nothing to do.
  }

  void visit(const TableScanNode& node, PlanNodeVisitorContext& context)
      const override {
    // Nothing to do.
  }

  void visit(const FilterNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& stats = static_cast<Context&>(context).stats;
    collectExprStats(*node.predicate(), stats);
    visitInputs(node, context);
  }

  void visit(const ProjectNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& stats = static_cast<Context&>(context).stats;
    collectExprStats(node.expressions(), stats);
    visitInputs(node, context);
  }

  void visit(const AggregateNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& stats = static_cast<Context&>(context).stats;
    collectExprStats(node.groupingKeys(), stats);
    for (const auto& agg : node.aggregates()) {
      collectExprStats(*agg, stats);
    }
    visitInputs(node, context);
  }

  void visit(const JoinNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& stats = static_cast<Context&>(context).stats;
    if (node.condition() != nullptr) {
      collectExprStats(*node.condition(), stats);
    }
    visitInputs(node, context);
  }

  void visit(const SortNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& stats = static_cast<Context&>(context).stats;
    for (const auto& sortKey : node.ordering()) {
      collectExprStats(*sortKey.expression, stats);
    }
    visitInputs(node, context);
  }

  void visit(const LimitNode& node, PlanNodeVisitorContext& context)
      const override {
    visitInputs(node, context);
  }

  void visit(const SetNode& node, PlanNodeVisitorContext& context)
      const override {
    visitInputs(node, context);
  }

  void visit(const UnnestNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& stats = static_cast<Context&>(context).stats;
    collectExprStats(node.unnestExpressions(), stats);
    visitInputs(node, context);
  }

 private:
  static void collectExprStats(const Expr& expr, ExprStats& stats);

  static void collectExprStats(
      const std::vector<ExprPtr>& exprs,
      ExprStats& stats);
};

class SummarizeExprVisitor : public ExprVisitor {
 public:
  using Context = ExprStats;

  void visit(const InputReferenceExpr& expr, ExprVisitorContext& ctx)
      const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["field"]++;

    visitInputs(expr, ctx);
  }

  void visit(const CallExpr& expr, ExprVisitorContext& ctx) const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["call"]++;
    myCtx.functionCounts()[expr.name()]++;
    visitInputs(expr, ctx);
  }

  void visit(const SpecialFormExpr& expr, ExprVisitorContext& ctx)
      const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()[std::string(
        SpecialFormName::toName(expr.form()))]++;
    visitInputs(expr, ctx);
  }

  void visit(const AggregateExpr& expr, ExprVisitorContext& ctx)
      const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["aggregate"]++;
    myCtx.functionCounts()[expr.name()]++;
    visitInputs(expr, ctx);
  }

  void visit(const WindowExpr& expr, ExprVisitorContext& ctx) const override {
    VELOX_NYI();
  }

  void visit(const ConstantExpr& expr, ExprVisitorContext& ctx) const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["constant"]++;
    myCtx.constantCounts()[expr.type()]++;
  }

  void visit(const LambdaExpr& expr, ExprVisitorContext& ctx) const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["lambda"]++;
    expr.body()->accept(*this, ctx);
  }

  void visit(const SubqueryExpr& expr, ExprVisitorContext& ctx) const override {
    auto& myCtx = static_cast<Context&>(ctx);
    myCtx.expressionCounts()["subquery"]++;

    CollectExprStatsPlanNodeVisitor visitor;
    CollectExprStatsPlanNodeVisitor::Context collectStatsContext;

    expr.subquery()->accept(visitor, collectStatsContext);

    myCtx.merge(collectStatsContext.stats);
  }
};

// static
void CollectExprStatsPlanNodeVisitor::collectExprStats(
    const Expr& expr,
    ExprStats& stats) {
  SummarizeExprVisitor visitor;
  expr.accept(visitor, stats);
}

// static
void CollectExprStatsPlanNodeVisitor::collectExprStats(
    const std::vector<ExprPtr>& exprs,
    ExprStats& stats) {
  SummarizeExprVisitor visitor;
  for (const auto& expr : exprs) {
    expr->accept(visitor, stats);
  }
}

class SummarizeToTextVisitor : public PlanNodeVisitor {
 public:
  struct Context : public PlanNodeVisitorContext {
    const PlanSummaryOptions options;
    const bool skeletonOnly;

    std::stringstream out;
    int32_t indent{0};

    explicit Context(
        const PlanSummaryOptions& _options,
        bool _skeletonOnly = false)
        : options(_options), skeletonOnly{_skeletonOnly} {}

    void appendExpression(const Expr& expr) {
      out << truncate(ExprPrinter::toText(expr), options.maxLength);
    }
  };

  void visit(const ValuesNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& myContext = static_cast<Context&>(context);
    appendHeader(node, myContext);

    if (myContext.skeletonOnly) {
      return;
    }

    const auto indent = makeIndent(myContext.indent + 3);
    myContext.out << indent << "rows: " << node.cardinality() << std::endl;
  }

  void visit(const TableScanNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& myContext = static_cast<Context&>(context);
    appendHeader(node, myContext);

    const auto indent = makeIndent(myContext.indent + 3);

    myContext.out << indent << "table: " << node.tableName() << std::endl;
    myContext.out << indent << "connector: " << node.connectorId() << std::endl;
  }

  void visit(const FilterNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& myContext = static_cast<Context&>(context);
    appendHeader(node, myContext);

    if (!myContext.skeletonOnly) {
      const auto indent = makeIndent(myContext.indent + 3);
      myContext.out << indent << "predicate: ";
      myContext.appendExpression(*node.predicate());
      myContext.out << std::endl;

      appendExpressions(std::vector<ExprPtr>{node.predicate()}, myContext);
    }

    appendInputs(node, myContext);
  }

  void visit(const ProjectNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& myContext = static_cast<Context&>(context);

    if (myContext.skeletonOnly) {
      node.onlyInput()->accept(*this, context);
      return;
    }

    appendHeader(node, myContext);
    appendExpressions(node.expressions(), myContext);

    const auto indent = makeIndent(myContext.indent + 3);

    // Collect non-identity projections.
    auto [projections, dereferences] = categorizeProjections(node);

    // projections: 4 out of 10
    const size_t numFields = node.outputType()->size();
    if (!projections.empty()) {
      myContext.out << indent << "projections: " << projections.size()
                    << " out of " << numFields << std::endl;
      {
        const auto cnt = std::min(
            myContext.options.project.maxProjections, projections.size());
        appendProjections(
            makeIndent(myContext.indent + 4),
            node,
            projections,
            cnt,
            myContext);
      }
    }

    // dereferences: 2 out of 10
    if (!dereferences.empty()) {
      myContext.out << indent << "dereferences: " << dereferences.size()
                    << " out of " << numFields << std::endl;
      {
        const auto cnt = std::min(
            myContext.options.project.maxDereferences, dereferences.size());
        appendProjections(
            makeIndent(myContext.indent + 4),
            node,
            dereferences,
            cnt,
            myContext);
      }
    }

    appendInputs(node, myContext);
  }

  void visit(const AggregateNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode(node, context);
  }

  void visit(const JoinNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& myContext = static_cast<Context&>(context);
    appendHeader(
        fmt::format(
            "{} {}",
            NodeKindName::toName(node.kind()),
            JoinTypeName::toName(node.joinType())),
        node,
        myContext);

    if (!myContext.skeletonOnly) {
      const auto indent = makeIndent(myContext.indent + 3);
      if (node.condition() != nullptr) {
        myContext.out << indent << "condition: ";
        myContext.appendExpression(*node.condition());
        myContext.out << std::endl;
      }
    }

    appendInputs(node, myContext);
  }

  void visit(const SortNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode(node, context);
  }

  void visit(const LimitNode& node, PlanNodeVisitorContext& context)
      const override {
    auto& myContext = static_cast<Context&>(context);
    appendHeader(node, myContext);

    if (!myContext.skeletonOnly) {
      const auto indent = makeIndent(myContext.indent + 3);
      myContext.out << indent << "limit: " << node.count();
      if (node.offset() > 0) {
        myContext.out << " offset: " << node.offset();
      }
      myContext.out << std::endl;
    }

    appendInputs(node, myContext);
  }

  void visit(const SetNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode(
        std::string(SetOperationName::toName(node.operation())), node, context);
  }

  void visit(const UnnestNode& node, PlanNodeVisitorContext& context)
      const override {
    appendNode(node, context);
  }

 private:
  static std::string makeIndent(int32_t size) {
    return std::string(size * 2, ' ');
  }

  void appendNode(const LogicalPlanNode& node, PlanNodeVisitorContext& context)
      const {
    appendNode(std::string(NodeKindName::toName(node.kind())), node, context);
  }

  void appendNode(
      const std::string& name,
      const LogicalPlanNode& node,
      PlanNodeVisitorContext& context) const {
    auto& myContext = static_cast<Context&>(context);
    appendHeader(name, node, myContext);
    appendInputs(node, myContext);
  }

  // Appends a single line header for the given node:
  //  - <name> [<id>]: N fields: field1 type1, field2 type2, ...
  //
  // Example: - PROJECT [8]: 2 fields: a VARCHAR, b INTEGER
  void appendHeader(const LogicalPlanNode& node, Context& context) const {
    appendHeader(std::string(NodeKindName::toName(node.kind())), node, context);
  }

  void appendHeader(
      const std::string& name,
      const LogicalPlanNode& node,
      Context& context) const {
    context.out << makeIndent(context.indent) << "- " << name << " ["
                << node.id() << "]: "
                << summarizeOutputType(node.outputType(), context.options)
                << std::endl;
  }

  void appendInputs(const LogicalPlanNode& node, Context& context) const {
    context.indent++;
    PlanNodeVisitor::visitInputs(node, context);
    context.indent--;
  }

  void appendExpressions(
      const std::vector<ExprPtr>& expressions,
      Context& context) const {
    const auto indent = makeIndent(context.indent + 3);

    SummarizeExprVisitor::Context exprCtx;
    SummarizeExprVisitor visitor;
    for (const auto& expression : expressions) {
      expression->accept(visitor, exprCtx);
    }

    appendExpressionStats(exprCtx, indent, context);
  }

  // Appends up to 3 lines: expressions, functions, constants.
  //
  // Example:
  //    expressions: AND: 2, OR: 1, call: 4, constant: 4, field: 4
  //    functions: eq: 2, gte: 1, lte: 1
  //    constants: VARCHAR: 4
  static void appendExpressionStats(
      const ExprStats& stats,
      const std::string& indent,
      Context& context) {
    context.out << indent << "expressions: ";
    appendCounts(stats.expressionCounts(), context.out);
    context.out << std::endl;

    if (!stats.functionCounts().empty()) {
      context.out << indent << "functions: ";
      appendCounts(stats.functionCounts(), context.out);
      context.out << std::endl;
    }

    if (!stats.constantCounts().empty()) {
      context.out << indent << "constants: ";
      std::unordered_map<std::string, int64_t> counts;
      for (const auto& [type, count] : stats.constantCounts()) {
        counts[type->toSummaryString(
            {.maxChildren = (uint32_t)context.options.maxChildTypes})] += count;
      }

      appendCounts(counts, context.out);
      context.out << std::endl;
    }
  }

  // @param sortByKey Indicates whether to sort counts by key asc (default) or
  // 'count' desc.
  static void appendCounts(
      const std::unordered_map<std::string, int64_t>& counts,
      std::ostream& text,
      bool sortByKey = true) {
    std::vector<std::pair<std::string, int64_t>> sortedCounts;
    sortedCounts.reserve(counts.size());
    for (const auto& [key, count] : counts) {
      sortedCounts.emplace_back(key, count);
    }

    if (sortByKey) {
      std::sort(
          sortedCounts.begin(),
          sortedCounts.end(),
          [&](const auto& a, const auto& b) { return a.first < b.first; });
    } else {
      std::sort(
          sortedCounts.begin(),
          sortedCounts.end(),
          [&](const auto& a, const auto& b) { return a.second > b.second; });
    }

    bool first = true;
    for (const auto& [key, count] : sortedCounts) {
      if (first) {
        first = false;
      } else {
        text << ", ";
      }
      text << key << ": " << count;
    }
  }

  struct Projections {
    // Non-identity non-dereference projections.
    std::vector<size_t> projections;

    // Struct field access projections.
    std::vector<size_t> dereferences;
  };

  static Projections categorizeProjections(const ProjectNode& node) {
    const size_t numFields = node.outputType()->size();

    std::vector<size_t> projections;
    projections.reserve(numFields);

    std::vector<size_t> dereferences;
    dereferences.reserve(numFields);

    for (auto i = 0; i < numFields; ++i) {
      const auto& expr = node.expressionAt(i);
      if (expr->isInputReference()) {
        // Skip identity projection.
        continue;
      }

      if (expr->isSpecialForm() &&
          expr->asUnchecked<SpecialFormExpr>()->form() ==
              SpecialForm::kDereference) {
        dereferences.push_back(i);
      } else {
        projections.push_back(i);
      }
    }

    return {projections, dereferences};
  }

  static void appendProjections(
      const std::string& indent,
      const ProjectNode& node,
      const std::vector<size_t>& projections,
      size_t cnt,
      Context& context) {
    if (cnt == 0) {
      return;
    }

    const auto& expressions = node.expressions();
    for (auto i = 0; i < cnt; ++i) {
      const auto index = projections[i];
      const auto& expr = expressions.at(index);
      context.out << indent << node.outputType()->nameOf(index) << ": ";
      context.appendExpression(*expr);
      context.out << std::endl;
    }

    if (cnt < projections.size()) {
      context.out << indent << "... " << (projections.size() - cnt) << " more"
                  << std::endl;
    }
  }

  static std::string summarizeOutputType(
      const velox::RowTypePtr& type,
      const PlanSummaryOptions& options) {
    std::ostringstream out;
    out << type->size() << " fields";

    // Include names and types for the first few fields.
    const auto cnt = std::min<size_t>(options.maxOutputFields, type->size());
    if (cnt > 0) {
      out << ": ";
      for (auto i = 0; i < cnt; ++i) {
        if (i > 0) {
          out << ", ";
        }
        out << type->nameOf(i) << " "
            << type->childAt(i)->toSummaryString(
                   {.maxChildren = (uint32_t)options.maxChildTypes});
      }

      if (cnt < type->size()) {
        out << ", ...";

        // TODO Include counts of fields by type kind.
      }
    }

    return out.str();
  }
};

} // namespace

// static
std::string PlanPrinter::toText(const LogicalPlanNode& root) {
  ToTextVisitor::Context context;
  ToTextVisitor visitor;
  root.accept(visitor, context);
  return context.out.str();
}

// static
std::string PlanPrinter::summarizeToText(
    const LogicalPlanNode& root,
    const PlanSummaryOptions& options) {
  SummarizeToTextVisitor::Context context{options};
  SummarizeToTextVisitor visitor;
  root.accept(visitor, context);
  return context.out.str();
}

// static
std::string PlanPrinter::toSkeletonText(const LogicalPlanNode& root) {
  SummarizeToTextVisitor::Context context{
      {.maxOutputFields = 0},
      /* skeleton */ true};
  SummarizeToTextVisitor visitor;
  root.accept(visitor, context);
  return context.out.str();
}

} // namespace facebook::velox::logical_plan
