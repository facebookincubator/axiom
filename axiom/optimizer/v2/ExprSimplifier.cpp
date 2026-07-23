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

#include "axiom/optimizer/v2/ExprSimplifier.h"

#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/ExprEmitter.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// If `orExpr` is an OR call and its disjuncts share AND-conjuncts,
// appends those common conjuncts to `common` and sets `*residual` to
// the rewritten OR (or nullptr when a disjunct was fully subsumed and
// the OR is trivially true once `common` holds). Returns true iff
// any factoring was performed; on false, `common` and `*residual`
// are unmodified.
bool tryFactorOr(
    Builder& builder,
    ExprCP orExpr,
    ExprVector& common,
    ExprCP* residual) {
  if (!orExpr->is(PlanType::kCallExpr) ||
      orExpr->as<Call>()->name() != SpecialFormCallNames::kOr) {
    return false;
  }
  ExprVector disjuncts = ExprFactory::flattenOr(orExpr);

  // Dedup disjuncts. Hash-cons guarantees pointer equality for
  // structurally identical exprs.
  folly::F14FastSet<ExprCP> seen;
  ExprVector uniqueDisjuncts;
  uniqueDisjuncts.reserve(disjuncts.size());
  for (ExprCP disjunct : disjuncts) {
    if (seen.insert(disjunct).second) {
      uniqueDisjuncts.push_back(disjunct);
    }
  }
  if (uniqueDisjuncts.size() < 2) {
    return false;
  }

  // Flatten each disjunct into its AND-conjuncts.
  std::vector<ExprVector> perDisjunctConjuncts;
  perDisjunctConjuncts.reserve(uniqueDisjuncts.size());
  for (ExprCP disjunct : uniqueDisjuncts) {
    perDisjunctConjuncts.push_back(ExprFactory::flattenAnd(disjunct));
  }

  // Intersect across all disjuncts.
  folly::F14FastSet<ExprCP> commonSet(
      perDisjunctConjuncts[0].begin(), perDisjunctConjuncts[0].end());
  for (size_t i = 1; i < perDisjunctConjuncts.size(); ++i) {
    folly::F14FastSet<ExprCP> next(
        perDisjunctConjuncts[i].begin(), perDisjunctConjuncts[i].end());
    folly::F14FastSet<ExprCP> intersected;
    for (ExprCP conjunct : commonSet) {
      if (next.contains(conjunct)) {
        intersected.insert(conjunct);
      }
    }
    commonSet = std::move(intersected);
  }
  if (commonSet.empty()) {
    return false;
  }

  // Build residuals; any empty residual means that disjunct is fully
  // subsumed by `common`, so the OR is trivially true given `common`.
  ExprFactory factory(builder);
  ExprVector residualDisjuncts;
  residualDisjuncts.reserve(uniqueDisjuncts.size());
  bool subsumed = false;
  for (auto& conjuncts : perDisjunctConjuncts) {
    ExprVector remaining;
    remaining.reserve(conjuncts.size());
    for (ExprCP conjunct : conjuncts) {
      if (!commonSet.contains(conjunct)) {
        remaining.push_back(conjunct);
      }
    }
    if (remaining.empty()) {
      subsumed = true;
      break;
    }
    residualDisjuncts.push_back(factory.andAll(remaining));
  }

  // Append common in first-disjunct order to keep deterministic output.
  for (ExprCP conjunct : perDisjunctConjuncts[0]) {
    if (commonSet.contains(conjunct)) {
      common.push_back(conjunct);
    }
  }
  *residual = subsumed ? nullptr : factory.orAll(residualDisjuncts);
  return true;
}

} // namespace

ExprCP ExprSimplifier::simplify(ExprCP expr) {
  return tryFoldConstant(expr);
}

bool ExprSimplifier::simplifyFilter(ExprCP predicate, ExprVector& into) {
  ExprVector flattened = ExprFactory::flattenAnd(predicate);
  ExprVector surviving;
  surviving.reserve(flattened.size());
  for (ExprCP conjunct : flattened) {
    ExprCP simplified = simplify(conjunct);
    if (simplified->is(PlanType::kLiteralExpr)) {
      const auto& variant = simplified->as<Literal>()->literal();
      VELOX_CHECK_EQ(
          variant.kind(),
          velox::TypeKind::BOOLEAN,
          "Conjunct simplified to a non-boolean literal");
      if (variant.isNull() || !variant.value<bool>()) {
        return true;
      }
      // Literal `true` — drop.
      continue;
    }
    ExprCP residual = nullptr;
    if (tryFactorOr(builder_, simplified, surviving, &residual)) {
      if (residual != nullptr) {
        surviving.push_back(residual);
      }
      continue;
    }
    surviving.push_back(simplified);
  }
  appendAll(into, surviving);
  return false;
}

ExprCP ExprSimplifier::tryFoldConstant(ExprCP expr) {
  if (expr->is(PlanType::kLiteralExpr)) {
    return expr;
  }

  if (!expr->columns().empty()) {
    return expr;
  }

  try {
    auto typedExpr = ExprEmitter{evaluator_.pool()}.toTypedExpr(expr);
    auto exprSet = evaluator_.compile(typedExpr);
    const auto& first = *exprSet->exprs().front();
    if (!first.isConstant()) {
      return expr;
    }
    const auto& constantExpr =
        static_cast<const velox::exec::ConstantExpr&>(first);
    auto variant = constantExpr.value()->variantAt(0);
    Value value(toType(constantExpr.type()), 1);
    auto* registered = queryCtx()->registerVariant(
        std::make_unique<velox::Variant>(std::move(variant)));
    return builder_.makeLiteral(value, registered);
  } catch (const velox::VeloxException&) {
    // Data-dependent evaluation failure on constant args (e.g.
    // division-by-zero, invalid cast). Leave the call unchanged so the
    // error surfaces at execution.
    return expr;
  }
}

velox::Variant ExprSimplifier::evaluate(ExprCP expr) {
  if (expr->is(PlanType::kLiteralExpr)) {
    return expr->as<Literal>()->literal();
  }

  VELOX_CHECK(
      expr->columns().empty(),
      "Expression to evaluate must not reference columns");

  if (emptyInput_ == nullptr) {
    emptyInput_ = std::make_shared<velox::RowVector>(
        evaluator_.pool(),
        velox::ROW({}),
        /*nulls=*/nullptr,
        /*length=*/1,
        std::vector<velox::VectorPtr>{});
  }

  auto typedExpr = ExprEmitter{evaluator_.pool()}.toTypedExpr(expr);
  auto exprSet = evaluator_.compile(typedExpr);
  velox::SelectivityVector rows(1);
  velox::VectorPtr result;
  evaluator_.evaluate(exprSet.get(), rows, *emptyInput_, result);
  return result->variantAt(0);
}

} // namespace facebook::axiom::optimizer::v2
