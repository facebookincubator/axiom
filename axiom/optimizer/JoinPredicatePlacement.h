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

#include <optional>
#include <utility>

#include "axiom/optimizer/Builder.h"

namespace facebook::axiom::optimizer {

/// Places predicates at the join boundary where their evaluation is valid.
///
/// For an inner join, a filter that reads one side moves to that input, while a
/// cross-side predicate becomes part of the join condition:
///
///   Filter(left.a > 0 AND left.x + right.x > 0)
///     InnerJoin                         InnerJoin(left.x + right.x > 0)
///       left                       ->     Filter(left.a > 0)
///       right                               left
///                                         right
///
/// A left join's own right-only condition can move to the right input because
/// the join still preserves every left row:
///
///   LeftJoin(right.a > 0)               LeftJoin
///     left                         ->     left
///     right                               Filter(right.a > 0)
///                                           right
class JoinPredicatePlacement {
 public:
  /// Predicates and keys grouped by their evaluation boundary.
  struct Routed {
    /// Predicates evaluated by the left input.
    ExprVector leftInputPredicates;
    /// Predicates evaluated by the right input.
    ExprVector rightInputPredicates;
    /// Join keys evaluated by the left input.
    ExprVector leftKeys;
    /// Join keys evaluated by the right input.
    ExprVector rightKeys;
    /// Residual predicates evaluated while matching join rows.
    ExprVector joinPredicates;
    /// Predicates evaluated after the join produces its output.
    ExprVector aboveJoinPredicates;
  };

  /// Safe propagation directions and whether every output row matched.
  struct FilterPropagation {
    /// A predicate guaranteed by the left input may restrict the right input.
    bool leftToRight{false};
    /// A predicate guaranteed by the right input may restrict the left input.
    bool rightToLeft{false};
    /// Every emitted row has a matching row from both inputs.
    bool emitsOnlyMatchedRows{false};
  };

  /// Routes predicates and extracts equi-keys from inner-join predicates. Two
  /// keys of one input paired with the same key of the other imply an equality,
  /// which goes where a join-condition predicate on that input would, and the
  /// key pair it makes redundant is dropped. 'nullAware' is the join's, which
  /// can differ from 'join's when a mark is fused into it.
  static Routed route(
      const Join* join,
      velox::core::JoinType joinType,
      bool nullAware,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      const ExprVector& pending,
      ExprVector leftInputPredicates,
      ExprVector rightInputPredicates,
      Builder& builder);

  /// Returns directions in which an input fact may restrict the other input.
  static FilterPropagation filterPropagation(
      velox::core::JoinType joinType,
      bool nullAware);

  /// Returns the two columns of 'conjunct' if it is `column = column`.
  static std::optional<std::pair<ColumnCP, ColumnCP>> columnEquality(
      ExprCP conjunct);

  /// Returns distinct cross-side column equality pairs from the join's keys,
  /// filter and `extraConjuncts`.
  static std::pair<ColumnVector, ColumnVector> equiColumnPairs(
      JoinCP join,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      const ExprVector& extraConjuncts);

  /// Pairs of equal columns: each of 'sources' with the target at its position.
  struct ColumnPairs {
    ColumnVector sources;
    ColumnVector targets;
  };

  /// Classes of columns a join makes equal that span both inputs. The targets
  /// of a class on an input are its columns there that an equality between the
  /// inputs reads.
  struct EqualColumns {
    /// Pairs each left column of a class with each right target of the class.
    ColumnPairs toRight;
    /// Pairs each right column of a class with each left target of the class.
    ColumnPairs toLeft;
    /// Every column of these classes.
    PlanObjectSet columns;
    /// The conjuncts of 'conjuncts' that formed the classes. The classes hold
    /// only while these are enforced, so they must not be rewritten through the
    /// classes.
    PlanObjectSet equalities;
  };

  /// Groups the columns made equal by the join's column keys and by the
  /// `column = column` conjuncts of its filter, of 'conjuncts' and of
  /// 'guaranteed', the conjuncts its inputs enforce themselves. The columns
  /// are non-null on the join's matched rows unless it matches NULL to NULL.
  static EqualColumns equalColumns(
      JoinCP join,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      const ExprVector& conjuncts,
      const ExprVector& guaranteed);
};

} // namespace facebook::axiom::optimizer
