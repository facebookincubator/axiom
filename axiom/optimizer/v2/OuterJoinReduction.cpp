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

#include "axiom/optimizer/v2/OuterJoinReduction.h"

#include <stddef.h>
#include <vector>

#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/PlanSubstitutions.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

// Conservatively recognizes predicates that cannot hold when any referenced
// column from `columns` is NULL.
bool isNullRejecting(ExprCP conjunct, const PlanObjectSet& columns) {
  return conjunct->columns().hasIntersection(columns) &&
      !conjunct->containsNonDefaultNullBehavior();
}

} // namespace

velox::core::JoinType OuterJoinReduction::reduce(
    velox::core::JoinType joinType,
    const ExprVector& pending,
    const PlanObjectSet& nonNullColumns,
    const PlanObjectSet& leftColumns,
    const PlanObjectSet& rightColumns) {
  const auto anyRejects = [&](const PlanObjectSet& columns) {
    for (ExprCP conjunct : pending) {
      if (isNullRejecting(conjunct, columns)) {
        return true;
      }
    }
    return columns.hasIntersection(nonNullColumns);
  };
  switch (joinType) {
    case velox::core::JoinType::kLeft:
      return anyRejects(rightColumns) ? velox::core::JoinType::kInner
                                      : joinType;
    case velox::core::JoinType::kRight:
      return anyRejects(leftColumns) ? velox::core::JoinType::kInner : joinType;
    case velox::core::JoinType::kFull: {
      // Rejecting one input's NULLs eliminates the other input's unmatched
      // rows. The referenced input remains row-preserving.
      const bool rejectsLeft = anyRejects(leftColumns);
      const bool rejectsRight = anyRejects(rightColumns);
      if (rejectsLeft && rejectsRight) {
        return velox::core::JoinType::kInner;
      }
      if (rejectsLeft) {
        return velox::core::JoinType::kLeft;
      }
      if (rejectsRight) {
        return velox::core::JoinType::kRight;
      }
      return joinType;
    }
    case velox::core::JoinType::kInner:
    case velox::core::JoinType::kLeftSemiFilter:
    case velox::core::JoinType::kCountingLeftSemiFilter:
    case velox::core::JoinType::kLeftSemiProject:
    case velox::core::JoinType::kRightSemiFilter:
    case velox::core::JoinType::kRightSemiProject:
    case velox::core::JoinType::kRightAnti:
    case velox::core::JoinType::kAnti:
    case velox::core::JoinType::kCountingAnti:
      return joinType;
    case velox::core::JoinType::kNumJoinTypes:
      break;
  }
  VELOX_UNREACHABLE();
}

PlanSubstitutions OuterJoinReduction::outputSources(const Join* join) {
  PlanSubstitutions substitutions;
  for (size_t i = 0; i < join->outputColumns().size(); ++i) {
    if (join->outputColumns()[i] != join->sourceColumns()[i]) {
      substitutions.add(join->outputColumns()[i], join->sourceColumns()[i]);
    }
  }
  return substitutions;
}

PlanSubstitutions OuterJoinReduction::reducedOutputs(
    const Join* join,
    velox::core::JoinType joinType,
    const PlanObjectSet& leftColumns,
    const PlanObjectSet& rightColumns) {
  PlanSubstitutions substitutions;
  for (size_t i = 0; i < join->outputColumns().size(); ++i) {
    ColumnCP source = join->sourceColumns()[i];
    const bool originallyPreserved = Join::preservesSource(
        join->joinType(), source, leftColumns, rightColumns);
    const bool nowPreserved =
        Join::preservesSource(joinType, source, leftColumns, rightColumns);
    if (!originallyPreserved && nowPreserved &&
        join->outputColumns()[i] != source) {
      substitutions.add(join->outputColumns()[i], source);
    }
  }
  return substitutions;
}

} // namespace facebook::axiom::optimizer::v2
