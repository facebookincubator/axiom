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

#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer::v2 {

/// Normalizes raw join-condition conjuncts into the Join IR's split
/// form. The invariant `Join` callers rely on: any `eq(leftExpr,
/// rightExpr)` conjunct whose sides partition cleanly into one side
/// each lives in `leftKeys` / `rightKeys`, never in `filter`.
class JoinCondition {
 public:
  /// Output of `splitEquiKeys`: paired equi-keys plus the residual
  /// non-equi conjuncts.
  struct Split {
    ExprVector leftKeys;
    ExprVector rightKeys;
    ExprVector residual;
  };

  /// Walks `conjuncts` (typically the flattened AND-conjuncts of a join
  /// condition). For each `eq(lhs, rhs)` whose `lhs` references only
  /// `leftColumns` and `rhs` only `rightColumns` (or vice versa, with
  /// the pair swapped to canonical left/right order), records it as a
  /// `(leftKey, rightKey)` pair. Anything not matching that pattern
  /// stays in `residual`.
  static Split splitEquiKeys(
      const ExprVector& conjuncts,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns);
};

} // namespace facebook::axiom::optimizer::v2
