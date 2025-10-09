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

#include "axiom/optimizer/PlanUtils.h"
#include <folly/container/F14Map.h>
#include <optimizer/PlanObject.h>
#include <optimizer/QueryGraphContext.h>
#include <algorithm>
#include <unordered_map>
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/RelationOp.h"

namespace facebook::axiom::optimizer {
namespace {

/// Match the input 'value' to the most appropriate unit and return
/// a string value. The units are specified in the 'units' array.
/// unitOffset is used to skip the starting units.
/// unitScale is used to determine the unit.
/// precision is used to set the decimal digits in the final output.
std::string succinctPrint(
    double decimalValue,
    const std::string_view* units,
    int unitsSize,
    int unitOffset,
    double unitScale,
    int precision) {
  std::stringstream out;
  int offset = unitOffset;
  while ((decimalValue / unitScale) >= 1 && offset < (unitsSize - 1)) {
    decimalValue = decimalValue / unitScale;
    offset++;
  }
  if (offset == unitOffset) {
    // Print the default value.
    precision = 0;
  }
  out << std::fixed << std::setprecision(precision) << decimalValue
      << units[offset];
  return out.str();
}

} // namespace

std::string succinctNumber(double value, int32_t precision) {
  static constexpr std::string_view kUnit[] = {
      "n", "u", "m", "", "k", "M", "G", "T", "P"};

  return succinctPrint(
      value * 1e9,
      kUnit,
      sizeof(kUnit) / sizeof(std::string_view),
      0,
      1000,
      precision);
}

namespace {
template <typename T>
int64_t integerValueInner(const velox::Variant* variant) {
  return variant->value<T>();
}
} // namespace

int64_t integerValue(const velox::Variant* variant) {
  switch (variant->kind()) {
    case velox::TypeKind::TINYINT:
      return integerValueInner<int8_t>(variant);
    case velox::TypeKind::SMALLINT:
      return integerValueInner<int16_t>(variant);
    case velox::TypeKind::INTEGER:
      return integerValueInner<int32_t>(variant);
    case velox::TypeKind::BIGINT:
      return integerValueInner<int64_t>(variant);
    default:
      VELOX_FAIL();
  }
}

std::optional<int64_t> maybeIntegerLiteral(
    const logical_plan::ConstantExpr* expr) {
  switch (expr->typeKind()) {
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::BIGINT:
      return integerValue(expr->value().get());
    default:
      return std::nullopt;
  }
}

std::string conjunctsToString(const ExprVector& conjuncts) {
  std::stringstream out;
  for (auto i = 0; i < conjuncts.size(); ++i) {
    out << conjuncts[i]->toString()
        << (i == conjuncts.size() - 1 ? "" : " and ");
  }
  return out.str();
}

class WindowsCollector {
 public:
  using SpecToWindows = std::unordered_map<WindowSpec, WindowVector, WindowSpec::Hasher>;
  SpecToWindows collect(const ExprVector& exprs) {
    for (const auto& expr : exprs) {
      collect(*expr);
    }
    return specToWindows_;
  }

 private:
  void collect(const Expr& expr) {
    if (expr.is(PlanType::kColumnExpr)) {
      return;
    }

    if (expr.is(PlanType::kWindowExpr)) {
      const auto* window = expr.as<Window>();
      specToWindows_[window->spec()].emplace_back(window);
    }

    expr.subexpressions().forEach([&](const PlanObject* subexpr) {
      if (subexpr->isExpr()) {
        collect(*subexpr->as<Expr>());
      }
    });
  }

  SpecToWindows specToWindows_;
};

// Recursively replaces Window expressions with their corresponding columns
ExprCP replaceWindows(
    ExprCP expr,
    const folly::F14FastMap<const Window*, ColumnCP>& windowToColumn) {
  if (expr->is(PlanType::kColumnExpr)) {
    return expr;
  }

  if (expr->is(PlanType::kWindowExpr)) {
    auto it = windowToColumn.find(expr->as<Window>());
    VELOX_CHECK(
        it != windowToColumn.end(),
        "Window expression not found in mapping");
    return it->second;
  }

  // Recursively replace in subexpressions
  ExprVector newArgs;
  bool changed = false;
  expr->subexpressions().forEach([&](const PlanObject* subexpr) {
    if (subexpr->isExpr()) {
      auto newExpr = replaceWindows(subexpr->as<Expr>(), windowToColumn);
      newArgs.push_back(newExpr);
      if (newExpr != subexpr) {
        changed = true;
      }
    }
  });

  if (!changed) {
    return expr;
  }

  if (expr->is(PlanType::kCallExpr)) {
    const auto* call = expr->as<Call>();
    return make<Call>(
        call->name(),
        call->value(),
        std::move(newArgs),
        call->functions());
  }

  // For other expression types, return the original
  // (they shouldn't contain Window expressions in practice)
  return expr;
}

RelationOpPtr makeProjectWithWindows(
    RelationOpPtr input,
    ExprVector projectExprs,
    ColumnVector projectColumns,
    bool isRedundant,
    DerivedTableCP dt) {
  auto specToWindows = WindowsCollector().collect(projectExprs);

  if (specToWindows.empty()) {
    return make<Project>(input, projectExprs, projectColumns, isRedundant);
  }

  RelationOpPtr result = input;
  folly::F14FastMap<const Window*, ColumnCP> windowToColumn;

  ColumnVector allColumns = result->columns();
  for (auto&& [spec, windows] : specToWindows) {

    for (const auto* window : windows) {
      auto* windowColumn = make<Column>(
          toName(fmt::format("__window{}", window->id())),
          dt,
          window->value());

      allColumns.push_back(windowColumn);
      windowToColumn[window] = windowColumn;
    }

    auto* windowOp = make<WindowOp>(
        std::move(result),
        spec.partitionKeys,
        spec.orderKeys,
        spec.orderTypes,
        windows,
        allColumns);

    result = windowOp;
  }

  // Replace Window expressions with their corresponding columns
  ExprVector replacedExprs;
  replacedExprs.reserve(projectExprs.size());
  for (const auto* expr : projectExprs) {
    replacedExprs.emplace_back(replaceWindows(expr, windowToColumn));
  }

  return make<Project>(
      result, std::move(replacedExprs), projectColumns, isRedundant);
}

std::string orderByToString(
    const ExprVector& orderKeys,
    const OrderTypeVector& orderTypes) {
  std::stringstream out;
  for (auto i = 0; i < orderKeys.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << orderKeys[i]->toString() << " "
        << OrderTypeName::toName(orderTypes[i]);
  }
  return out.str();
}

} // namespace facebook::axiom::optimizer
