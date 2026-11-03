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

#include "axiom/optimizer/Node.h"

namespace facebook::axiom::optimizer {

/// Fuses a filter that consumes a semi-project join's mark when nothing else
/// reads the mark.
///
/// A positive mark becomes a filtering semi join:
///
///   Filter(mark)                 LeftSemiFilterJoin
///     LeftSemiProjectJoin   ->     left
///       left                       right
///       right
///
/// A negated three-valued mark preserves NOT IN semantics by becoming a
/// null-aware anti join:
///
///   Filter(not(mark))            NullAwareAntiJoin
///     LeftSemiProjectJoin   ->     left
///       left                       right
///       right
class MarkFilterFusion {
 public:
  /// Selected filtering join semantics and the consumed predicate.
  struct Result {
    /// Filtering join type that replaces the semi-project join.
    velox::core::JoinType joinType;
    /// Pending predicate consumed by the filtering join.
    ExprCP conjunct;
  };

  /// Returns the fused join semantics when the mark is otherwise dead.
  static std::optional<Result> fuse(
      JoinCP join,
      const ExprVector& pending,
      const PlanObjectSet& requiredAbove);
};

} // namespace facebook::axiom::optimizer
