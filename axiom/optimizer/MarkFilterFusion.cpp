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

#include "axiom/optimizer/MarkFilterFusion.h"

#include "axiom/optimizer/FunctionRegistry.h"

namespace facebook::axiom::optimizer {
namespace {

bool isNot(ExprCP expression, ExprCP& argument) {
  if (!expression->is(PlanType::kCallExpr)) {
    return false;
  }
  const Call* call = expression->as<Call>();
  if (call->name() != toName(queryCtx()->functionRegistry().negation()) ||
      call->args().size() != 1) {
    return false;
  }
  argument = call->args()[0];
  return true;
}

} // namespace

std::optional<MarkFilterFusion::Result> MarkFilterFusion::fuse(
    JoinCP join,
    const ExprVector& pending,
    const PlanObjectSet& requiredAbove) {
  if (join->joinType() != velox::core::JoinType::kLeftSemiProject) {
    return std::nullopt;
  }
  ColumnCP mark = join->markColumn();
  VELOX_CHECK_NOT_NULL(mark);
  if (requiredAbove.contains(mark)) {
    return std::nullopt;
  }

  ExprCP fused{nullptr};
  velox::core::JoinType fusedType{};
  for (ExprCP conjunct : pending) {
    if (conjunct == mark) {
      fused = conjunct;
      fusedType = velox::core::JoinType::kLeftSemiFilter;
      break;
    }
    ExprCP negated{nullptr};
    if (isNot(conjunct, negated) && negated == mark) {
      fused = conjunct;
      fusedType = velox::core::JoinType::kAnti;
      break;
    }
  }
  if (fused == nullptr) {
    return std::nullopt;
  }
  for (ExprCP conjunct : pending) {
    if (conjunct != fused && conjunct->columns().contains(mark)) {
      return std::nullopt;
    }
  }
  return Result{fusedType, fused};
}

} // namespace facebook::axiom::optimizer
