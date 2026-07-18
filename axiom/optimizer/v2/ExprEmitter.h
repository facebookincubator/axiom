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

#pragma once

#include <folly/container/F14Map.h>

#include "axiom/common/Enums.h"
#include "axiom/optimizer/QueryGraph.h"
#include "velox/core/Expressions.h"

namespace facebook::velox::memory {
class MemoryPool;
}

namespace facebook::axiom::optimizer::v2 {

/// Selects the name a `Column` lowers to in a Velox `FieldAccessTypedExpr`.
enum class ColumnNaming {
  /// `Column::outputName()` (post-rename alias).
  kOutputName,
  /// `Column::name()` (underlying table column name).
  kSchemaName,
};

AXIOM_DECLARE_ENUM_NAME(ColumnNaming);

/// Converts tree-IR expressions (`ExprCP`) into Velox `core::TypedExpr`s.
/// Used by Emit to lower expressions inside Filter, Project, and connector
/// handle construction.
class ExprEmitter {
 public:
  /// 'pool' backs constant vectors materialized during lowering (e.g. the
  /// array constant of an IN list); it must outlive the emitted plan.
  explicit ExprEmitter(velox::memory::MemoryPool* pool) : pool_{pool} {}

  /// Lowers a single expression. Recursive; throws VELOX_NYI for expression
  /// kinds not yet handled.
  velox::core::TypedExprPtr toTypedExpr(
      ExprCP expr,
      ColumnNaming naming = ColumnNaming::kOutputName);

  /// Lowers each expression to a Velox TypedExpr, preserving order.
  std::vector<velox::core::TypedExprPtr> toTypedExprs(
      const ExprVector& exprs,
      ColumnNaming naming = ColumnNaming::kOutputName);

  /// Returns the conjunction of 'predicates'. Calls toTypedExpr() on each
  /// predicate, wraps with an `and` Velox call if more than one.
  /// 'predicates' must be non-empty.
  velox::core::TypedExprPtr makeAnd(
      const ExprVector& predicates,
      ColumnNaming naming = ColumnNaming::kOutputName);

 private:
  // Memoizes each Expr's lowered form for the duration of one conversion, so a
  // shared subexpression DAG lowers in linear rather than exponential time.
  using ExprCache = folly::F14FastMap<ExprCP, velox::core::TypedExprPtr>;

  // Lowers 'expr', reusing 'cache' for already-lowered subexpressions.
  velox::core::TypedExprPtr
  toTypedExpr(ExprCP expr, ColumnNaming naming, ExprCache& cache);

  // Lowers a `Call`. 'args' are the already-lowered arguments.
  velox::core::TypedExprPtr callToTypedExpr(
      const Call* call,
      std::vector<velox::core::TypedExprPtr> args);

  velox::memory::MemoryPool* const pool_;
};

} // namespace facebook::axiom::optimizer::v2
