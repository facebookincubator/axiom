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

#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/logical_plan/ExprVisitor.h"
#include "axiom/logical_plan/PlanPrinter.h"

namespace facebook::axiom::logical_plan {

namespace {

struct ToTextVisitorContext : public ExprVisitorContext {
  std::stringstream out;
};

class ToTextVisitor : public ExprVisitor {
 public:
  void visit(const InputReferenceExpr& expr, ExprVisitorContext& context)
      const override {
    toOut(context) << expr.name();
  }

  void visit(const CallExpr& expr, ExprVisitorContext& context) const override {
    auto& out = toOut(context);

    out << expr.name();
    appendInputs(expr, out, context);
  }

  void visit(const SpecialFormExpr& expr, ExprVisitorContext& context)
      const override {
    auto& out = toOut(context);

    out << SpecialFormName::toName(expr.form());

    if (expr.form() == SpecialForm::kCast ||
        expr.form() == SpecialForm::kTryCast) {
      out << "(";
      expr.inputAt(0)->accept(*this, context);
      out << " AS " << expr.type()->toString() << ")";
    } else {
      appendInputs(expr, out, context);
    }
  }

  void visit(const AggregateExpr& expr, ExprVisitorContext& context)
      const override {
    auto& out = toOut(context);

    out << expr.name();

    out << "(";

    if (expr.isDistinct()) {
      out << "DISTINCT ";
    }

    const auto numInputs = expr.inputs().size();
    for (auto i = 0; i < numInputs; ++i) {
      if (i > 0) {
        out << ", ";
      }
      expr.inputAt(i)->accept(*this, context);
    }

    if (!expr.ordering().empty()) {
      out << " ORDER BY ";
      for (auto i = 0; i < expr.ordering().size(); ++i) {
        if (i > 0) {
          out << ", ";
        }
        expr.ordering()[i].expression->accept(*this, context);
        out << " " << expr.ordering()[i].order.toString();
      }
    }

    out << ")";

    if (expr.filter() != nullptr) {
      out << " FILTER (WHERE ";
      expr.filter()->accept(*this, context);
      out << ")";
    }
  }

  void visit(const WindowExpr& expr, ExprVisitorContext& context)
      const override {
    auto& out = toOut(context);

    out << expr.name();
    appendInputs(expr, out, context);

    if (expr.ignoreNulls()) {
      out << " IGNORE NULLS";
    }

    out << " OVER (";

    if (!expr.partitionKeys().empty()) {
      out << "PARTITION BY ";
      for (auto i = 0; i < expr.partitionKeys().size(); ++i) {
        if (i > 0) {
          out << ", ";
        }
        expr.partitionKeys()[i]->accept(*this, context);
      }
    }

    if (!expr.ordering().empty()) {
      if (!expr.partitionKeys().empty()) {
        out << " ";
      }
      out << "ORDER BY ";
      for (auto i = 0; i < expr.ordering().size(); ++i) {
        if (i > 0) {
          out << ", ";
        }
        expr.ordering()[i].expression->accept(*this, context);
        out << " " << expr.ordering()[i].order.toString();
      }
    }

    const auto& frame = expr.frame();
    if (!expr.partitionKeys().empty() || !expr.ordering().empty()) {
      out << " ";
    }
    out << WindowExpr::toName(frame.type) << " BETWEEN ";

    // Start bound
    if (frame.startValue != nullptr) {
      frame.startValue->accept(*this, context);
      out << " ";
    }
    out << WindowExpr::toName(frame.startType);

    out << " AND ";

    // End bound
    if (frame.endValue != nullptr) {
      frame.endValue->accept(*this, context);
      out << " ";
    }
    out << WindowExpr::toName(frame.endType);

    out << ")";
  }

  void visit(const ConstantExpr& expr, ExprVisitorContext& context)
      const override {
    // TODO Avoid using velox::Variant::toString as we do not control its
    // output.
    toOut(context) << expr.value()->toString(expr.type());
  }

  void visit(const LambdaExpr& expr, ExprVisitorContext& context)
      const override {
    auto& out = toOut(context);

    const auto numArgs = expr.signature()->size();
    for (auto i = 0; i < numArgs; ++i) {
      if (i > 0) {
        out << ", ";
      }

      out << expr.signature()->nameOf(i);
    }

    out << " -> ";

    expr.body()->accept(*this, context);
  }

  void visit(const SubqueryExpr& expr, ExprVisitorContext& context)
      const override {
    auto& out = toOut(context);

    // TODO Produce a single-line subquery text: Aggregate(keys, aggs) ->
    // Filter(condition) -> TableScan(t).
    auto text = PlanPrinter::toText(*expr.subquery());
    std::replace(text.begin(), text.end(), '\n', ' ');
    out << "subquery(" << text << ")";
  }

 private:
  static std::stringstream& toOut(ExprVisitorContext& context) {
    auto& myContext = static_cast<ToTextVisitorContext&>(context);
    return myContext.out;
  }

  void appendInputs(
      const Expr& expr,
      std::stringstream& out,
      ExprVisitorContext& context) const {
    out << "(";
    const auto numInputs = expr.inputs().size();
    for (auto i = 0; i < numInputs; ++i) {
      if (i > 0) {
        out << ", ";
      }

      expr.inputAt(i)->accept(*this, context);
    }
    out << ")";
  }
};

} // namespace

// static
std::string ExprPrinter::toText(const Expr& root) {
  ToTextVisitorContext context;
  ToTextVisitor visitor;
  root.accept(visitor, context);
  return context.out.str();
}

} // namespace facebook::axiom::logical_plan
