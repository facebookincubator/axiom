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

#include "axiom/optimizer/ExprFactory.h"

namespace facebook::axiom::optimizer {

/// Derives deterministic filters implied by predicates for individual columns
/// or join inputs. Within a top-level OR, predicates may be nested under any
/// combination of AND and OR: AND contributes every constraint for a group,
/// while OR contributes a group only when every branch constrains it. A
/// predicate belongs to a column group when it references exactly that column,
/// and to a join-input group when all its columns come from that input.
///
/// For example:
///
///     OR
///     |-- AND
///     |   |-- a = 1
///     |   `-- OR
///     |       |-- b = 10
///     |       `-- b = 20
///     `-- AND
///         |-- a = 2
///         `-- b = 30
///
/// implies:
///
///     a = 1 OR a = 2
///     b = 10 OR b = 20 OR b = 30
///
/// A row predicate `(a, b) IN ((1, 10), (2, 20))` similarly implies
/// `a IN (1, 2)` and `b IN (10, 20)`. These filters do not preserve the
/// correlation between fields; callers must retain the original predicate.
///
/// Non-deterministic expressions are excluded from derived filters.
class ImpliedFilters {
 public:
  /// Derives necessary filters for each join input from `filters`.
  static std::pair<ExprVector, ExprVector> deriveForJoinInputs(
      const ExprVector& filters,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      ExprFactory& factory);

  /// Equalities that a join's equi-keys imply between keys of one input, and
  /// the key pairs they make redundant.
  struct KeyEqualities {
    /// Equalities between left keys.
    ExprVector left;
    /// Equalities between right keys.
    ExprVector right;
    /// Positionally aligned with the keys: whether the pair is redundant given
    /// the equalities and the pairs that are not.
    std::vector<bool> redundant;
  };

  /// Derives the equalities that equi-keys imply: a pair that shares a key with
  /// an earlier pair is redundant once their other keys are equal, since
  /// `a = x AND b = x` holds exactly when `a = x AND a = b`. Each redundant
  /// pair contributes that equality. `leftKeys[i]` pairs with `rightKeys[i]`. A
  /// pair with a non-deterministic key implies nothing and stays.
  static KeyEqualities deriveForJoinKeys(
      const ExprVector& leftKeys,
      const ExprVector& rightKeys,
      ExprFactory& factory);

  /// Derives new necessary single-column filters from ORs and row-valued INs
  /// in `filters`.
  static ExprVector deriveForColumns(
      const ExprVector& filters,
      ExprFactory& factory);
};

} // namespace facebook::axiom::optimizer
