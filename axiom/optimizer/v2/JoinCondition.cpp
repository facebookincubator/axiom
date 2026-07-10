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

#include "axiom/optimizer/v2/JoinCondition.h"

#include "axiom/optimizer/FunctionRegistry.h"

namespace facebook::axiom::optimizer::v2 {

JoinCondition::Split JoinCondition::splitEquiKeys(
    const ExprVector& conjuncts,
    const PlanObjectSet& leftColumns,
    const PlanObjectSet& rightColumns) {
  Split result;
  const Name equalityName = toName(FunctionRegistry::instance()->equality());
  for (ExprCP conjunct : conjuncts) {
    if (!conjunct->is(PlanType::kCallExpr)) {
      result.residual.push_back(conjunct);
      continue;
    }
    const auto* call = conjunct->as<Call>();
    if (call->name() != equalityName || call->args().size() != 2) {
      result.residual.push_back(conjunct);
      continue;
    }
    ExprCP lhs = call->args()[0];
    ExprCP rhs = call->args()[1];
    const auto& lhsCols = lhs->columns();
    const auto& rhsCols = rhs->columns();
    if (lhsCols.empty() || rhsCols.empty()) {
      result.residual.push_back(conjunct);
      continue;
    }
    if (lhsCols.isSubset(leftColumns) && rhsCols.isSubset(rightColumns)) {
      result.leftKeys.push_back(lhs);
      result.rightKeys.push_back(rhs);
      continue;
    }
    if (lhsCols.isSubset(rightColumns) && rhsCols.isSubset(leftColumns)) {
      result.leftKeys.push_back(rhs);
      result.rightKeys.push_back(lhs);
      continue;
    }
    result.residual.push_back(conjunct);
  }
  return result;
}

} // namespace facebook::axiom::optimizer::v2
