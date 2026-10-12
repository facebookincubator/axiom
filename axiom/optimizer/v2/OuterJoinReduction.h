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

#include "axiom/optimizer/PlanObject.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/PlanSubstitutions.h"
#include "velox/core/PlanNode.h"

namespace facebook::axiom::optimizer::v2 {

/// Derives an effective join type and output identities when predicates reject
/// NULL-padded rows.
///
/// A filter that rejects the right side's padded NULLs turns a left join into
/// an inner join. The right output can then use its source column directly:
///
///   Filter(rightOutput > 0)             Filter(right.b > 0)
///     LeftJoin                            InnerJoin
///       output: left.a, rightOutput  ->     output: left.a, right.b
///       source: left.a, right.b             left
///       left                                right
///       right
class OuterJoinReduction {
 public:
  /// Returns the effective join type after applying null-rejecting facts.
  static velox::core::JoinType reduce(
      velox::core::JoinType joinType,
      const ExprVector& pending,
      const PlanObjectSet& nonNullColumns,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns);

  /// Maps fresh join outputs to the input columns that supply them.
  static PlanSubstitutions outputSources(const Join* join);

  /// Returns output-to-source mappings made valid by `joinType`.
  static PlanSubstitutions reducedOutputs(
      const Join* join,
      velox::core::JoinType joinType,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns);
};

} // namespace facebook::axiom::optimizer::v2
