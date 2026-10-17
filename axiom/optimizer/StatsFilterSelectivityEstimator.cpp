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

#include "axiom/optimizer/StatsFilterSelectivityEstimator.h"

#include <algorithm>

#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/SelectivityEngine.h"
#include "velox/core/Expressions.h"
#include "velox/expression/ScopedVarSetter.h"
#include "velox/type/Filter.h"

namespace facebook::axiom::optimizer {
namespace {

using connector::ColumnStatistics;
using connector::FilterEstimate;
using connector::TypedColumnStatistics;
using velox::core::CallTypedExpr;
using velox::core::ConstantTypedExpr;
using velox::core::FieldAccessTypedExpr;
using velox::core::TypedExprPtr;

// Converts a refined optimizer Value to connector ColumnStatistics for the
// FilterEstimate result.
ColumnStatistics toColumnStatistics(const Value& value) {
  ColumnStatistics stats;
  if (value.cardinality.has_value()) {
    stats.numDistinct = static_cast<int64_t>(*value.cardinality);
  }
  stats.nullPct = value.nullFraction.value_or(0) * 100.0f;
  stats.nonNull = !value.nullable;
  if (value.min != nullptr) {
    stats.min = *value.min;
  }
  if (value.max != nullptr) {
    stats.max = *value.max;
  }
  return stats;
}

// SelectivityEngine policy over velox TypedExpr filters pushed into a
// connector. Reads statistics from a per-column ColumnStatistics map and writes
// back refined per-column Values (kUpdatesConstraints == true) so the connector
// can report both a row count and the narrowed statistics of the surviving
// rows.
class TypedExprPolicy {
 public:
  using Expr = TypedExprPtr;
  using ColumnKey = std::string;
  static constexpr bool kUpdatesConstraints = true;

  explicit TypedExprPolicy(
      const folly::F14FastMap<std::string, TypedColumnStatistics>& columnStats)
      : columnStats_{columnStats} {
    // Pushed-down filters carry velox operator names. Comparisons and
    // is_null/not already match queryCtx()->functionNames() verbatim, but
    // special forms (and/or/in/...) arrive under their velox names and must be
    // mapped to the optimizer's canonical SpecialFormCallNames the engine
    // compares against. Built once from the registry so it covers every special
    // form, stays dialect-agnostic, and makes name() a single lookup per call.
    for (const auto& [form, veloxName] :
         queryCtx()->functionRegistry().specialForms()) {
      specialFormNames_.emplace(
          veloxName, SpecialFormCallNames::toCallName(form));
    }
  }

  Value valueOf(const Expr& expr) const {
    TypeCP type = expr->type().get();
    if (expr->isFieldAccessKind()) {
      const auto& columnName =
          expr->asUnchecked<FieldAccessTypedExpr>()->name();
      // A column refined by an earlier conjunct reflects the narrowed value.
      auto refined = refined_.find(columnName);
      if (refined != refined_.end()) {
        return refined->second;
      }
      auto it = columnStats_.find(columnName);
      if (it != columnStats_.end()) {
        return Value::fromColumnStatistics(type, it->second.stats);
      }
      return Value(type);
    }

    if (expr->isConstantKind()) {
      // A literal is a single value.
      return Value(type, 1.0f);
    }

    return Value(type);
  }

  bool isCall(const Expr& expr) const {
    return expr->isCallKind();
  }

  bool isColumn(const Expr& expr) const {
    return expr->isFieldAccessKind();
  }

  bool isLiteral(const Expr& expr) const {
    return expr->isConstantKind();
  }

  Name name(const Expr& expr) const {
    VELOX_DCHECK(expr->isCallKind());
    const auto& veloxName = expr->asUnchecked<CallTypedExpr>()->name();
    // Special forms (and/or/in/...) map to the optimizer's canonical sentinels;
    // comparisons and is_null/not already match functionNames() once interned,
    // and the engine compares the returned Name by pointer.
    auto it = specialFormNames_.find(veloxName);
    if (it != specialFormNames_.end()) {
      return it->second;
    }
    return toName(veloxName);
  }

  std::span<const Expr> args(const Expr& expr) const {
    return expr->inputs();
  }

  const velox::Variant& literal(const Expr& expr) const {
    VELOX_DCHECK(expr->isConstantKind());
    const auto* constant = expr->asUnchecked<ConstantTypedExpr>();
    if (!constant->hasValueVector()) {
      return constant->value();
    }
    // Complex constants -- notably a folded IN list, in(column, ARRAY[...]) --
    // are held as a value vector. Materialize to a Variant once and hand back a
    // stable reference.
    auto it = materializedLiterals_.find(constant);
    if (it == materializedLiterals_.end()) {
      it = materializedLiterals_
               .emplace(constant, constant->valueVector()->variantAt(0))
               .first;
    }
    return it->second;
  }

  ColumnKey columnKey(const Expr& expr) const {
    if (expr->isFieldAccessKind()) {
      return expr->asUnchecked<FieldAccessTypedExpr>()->name();
    }
    return expr->toString();
  }

  // valueOf() derives a column's Value from its statistics on demand, so there
  // is nothing to pre-seed.
  void seed(const Expr& /*conjunct*/) const {}

  void refine(const Expr& expr, const Value& value) const {
    refined_.insert_or_assign(columnKey(expr), value);
  }

  // Converts the refined Values accumulated during estimation back to
  // ColumnStatistics for the connector's FilteredTableStats.
  folly::F14FastMap<std::string, ColumnStatistics> refinedColumnStats() const {
    folly::F14FastMap<std::string, ColumnStatistics> result;
    result.reserve(refined_.size());
    for (const auto& [name, value] : refined_) {
      result.emplace(name, toColumnStatistics(value));
    }
    return result;
  }

 private:
  const folly::F14FastMap<std::string, TypedColumnStatistics>& columnStats_;
  // velox special-form name -> optimizer sentinel Name, built once at
  // construction (see the constructor).
  folly::F14FastMap<std::string, Name> specialFormNames_;
  // Refined per-column Values, written back as conjuncts are estimated.
  mutable folly::F14FastMap<std::string, Value> refined_;
  // Variants materialized from complex (value-vector) constants, keyed by node.
  // Node-stable: literal() hands out references into this map that the engine
  // holds across later inserts, so the values must not move on rehash.
  mutable folly::F14NodeMap<const ConstantTypedExpr*, velox::Variant>
      materializedLiterals_;
};

} // namespace

StatsFilterSelectivityEstimator::StatsFilterSelectivityEstimator()
    : context_{queryCtx()} {}

connector::FilterEstimate StatsFilterSelectivityEstimator::estimate(
    const std::vector<velox::core::TypedExprPtr>& filters,
    const folly::F14FastMap<std::string, connector::TypedColumnStatistics>&
        columnStats) const {
  std::lock_guard<std::mutex> lock(mutex_);
  velox::ScopedVarSetter contextSetter(&queryCtx(), context_);

  TypedExprPolicy policy{columnStats};
  SelectivityEngine<TypedExprPolicy> engine{policy};
  auto selectivity =
      engine.conjunctsSelectivity(filters, /*updateConstraints=*/true);

  connector::FilterEstimate estimate;
  estimate.selectivity = selectivity.has_value()
      ? std::clamp(selectivity->trueFraction, 0.0, 1.0)
      : Selectivity::kUnknown;
  estimate.columnStats = policy.refinedColumnStats();
  return estimate;
}

connector::FilterEstimate StatsFilterSelectivityEstimator::estimate(
    const folly::F14FastMap<std::string, const velox::common::Filter*>& filters,
    const folly::F14FastMap<std::string, connector::TypedColumnStatistics>&
        columnStats) const {
  std::lock_guard<std::mutex> lock(mutex_);
  velox::ScopedVarSetter contextSetter(&queryCtx(), context_);

  connector::FilterEstimate estimate;
  double selectivity = 1.0;
  for (const auto& [name, filter] : filters) {
    auto it = columnStats.find(name);
    if (it == columnStats.end()) {
      // No statistics for this column; leave it unaccounted rather than guess.
      continue;
    }

    Value value =
        Value::fromColumnStatistics(it->second.type, it->second.stats);
    Value refined = value;
    auto columnSelectivity = commonFilterSelectivity(*filter, value, refined);
    selectivity *= columnSelectivity.has_value()
        ? std::clamp(columnSelectivity->trueFraction, 0.0, 1.0)
        : Selectivity::kUnknown;
    estimate.columnStats.emplace(name, toColumnStatistics(refined));
  }
  estimate.selectivity = std::clamp(selectivity, 0.0, 1.0);
  return estimate;
}

} // namespace facebook::axiom::optimizer
