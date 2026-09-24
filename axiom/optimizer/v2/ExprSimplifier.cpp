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

#include "axiom/optimizer/FunctionRegistry.h"
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

// Returns the value of `expr` if it is a non-null boolean literal.
std::optional<bool> constantBoolean(ExprCP expr) {
  if (!expr->is(PlanType::kLiteralExpr)) {
    return std::nullopt;
  }
  const auto& variant = expr->as<Literal>()->literal();
  if (variant.isNull() || variant.kind() != velox::TypeKind::BOOLEAN) {
    return std::nullopt;
  }
  return variant.value<bool>();
}

} // namespace

ExprCP ExprSimplifier::simplify(ExprCP expr) {
  return tryFoldCoalesce(tryFoldConjunct(tryFoldConstant(expr)));
}

ExprCP ExprSimplifier::simplifyTree(ExprCP expr) {
  switch (expr->type()) {
    case PlanType::kColumnExpr:
    case PlanType::kLiteralExpr:
      return simplify(expr);
    case PlanType::kCallExpr: {
      const auto* call = expr->as<Call>();
      ExprVector args;
      args.reserve(call->args().size());
      bool changed = false;
      for (ExprCP arg : call->args()) {
        ExprCP simplified = simplifyTree(arg);
        changed |= simplified != arg;
        args.push_back(simplified);
      }
      return simplify(
          changed ? ExprFactory{builder_}.rebuildCall(call, std::move(args))
                  : expr);
    }
    case PlanType::kFieldExpr: {
      const auto* field = expr->as<Field>();
      ExprCP base = simplifyTree(field->base());
      return simplify(
          base == field->base()
              ? expr
              : ExprFactory{builder_}.rebuildField(field, base));
    }
    case PlanType::kLambdaExpr: {
      const auto* lambda = expr->as<Lambda>();
      ExprCP body = simplifyTree(lambda->body());
      // A Lambda is a binding expression, so only its body is simplified.
      return body == lambda->body()
          ? expr
          : make<Lambda>(lambda->args(), lambda->value().type, body);
    }
    default:
      VELOX_NYI(
          "ExprSimplifier::simplifyTree: unsupported expression type {}",
          expr->typeName());
  }
}

ExprCP ExprSimplifier::tryFoldCoalesce(ExprCP expr) {
  if (!expr->is(PlanType::kCallExpr)) {
    return expr;
  }
  const auto* call = expr->as<Call>();
  if (call->name() != SpecialFormCallNames::kCoalesce ||
      call->args().size() != 2 || call->args()[0] != call->args()[1] ||
      call->args()[0]->containsNonDeterministic()) {
    return expr;
  }
  return call->args()[0];
}

ExprCP ExprSimplifier::tryFoldConjunct(ExprCP expr) {
  if (!expr->is(PlanType::kCallExpr)) {
    return expr;
  }
  const auto* call = expr->as<Call>();
  const bool isAnd = call->name() == SpecialFormCallNames::kAnd;
  if (!isAnd && call->name() != SpecialFormCallNames::kOr) {
    return expr;
  }

  ExprVector remaining;
  remaining.reserve(call->args().size());
  for (ExprCP arg : call->args()) {
    const auto value = constantBoolean(arg);
    if (!value.has_value()) {
      remaining.push_back(arg);
    } else if (value.value() != isAnd) {
      // AND is false as soon as one argument is false, even if the others are
      // null or throw; OR is the mirror image.
      return arg;
    }
  }

  if (remaining.size() == call->args().size()) {
    return expr;
  }
  if (remaining.empty()) {
    return builder_.makeLiteral(
        velox::Variant(isAnd), toType(velox::BOOLEAN()));
  }
  if (remaining.size() == 1) {
    return remaining.front();
  }
  const FunctionSet functions = Call::unionArgFunctions(
      functionBits(call->name(), /*specialForm=*/true), remaining);
  // A literal argument is never the one that determined the call's
  // cardinality, so dropping it leaves `value()` valid.
  return builder_.makeCall(
      call->name(), call->value(), std::move(remaining), functions);
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

  // A call with default null behavior (result is null whenever any argument is
  // null) folds to null if any argument is the constant null -- even when its
  // other arguments reference columns. Special forms are given
  // kNonDefaultNullBehavior unconditionally, so they are excluded.
  // `Call::functions()` aggregates the arguments' bits, so the call's own bits
  // are recomputed here.
  if (expr->is(PlanType::kCallExpr)) {
    const auto* call = expr->as<Call>();
    const FunctionSet ownFunctions = functionBits(
        call->name(), SpecialFormCallNames::isSpecialForm(call->name()));
    if (!ownFunctions.contains(FunctionSet::kNonDefaultNullBehavior)) {
      for (ExprCP arg : call->args()) {
        if (arg->is(PlanType::kLiteralExpr) &&
            arg->as<Literal>()->literal().isNull()) {
          return builder_.makeNull(call->value().type);
        }
      }
    }
  }

  if (!expr->columns().empty()) {
    return expr;
  }

  velox::Variant variant;
  velox::TypePtr type;
  try {
    auto typedExpr = ExprEmitter{evaluator_.pool()}.toTypedExpr(expr);
    auto exprSet = evaluator_.compile(typedExpr);
    const auto& first = *exprSet->exprs().front();
    if (!first.isConstant()) {
      return expr;
    }
    const auto& constantExpr =
        static_cast<const velox::exec::ConstantExpr&>(first);
    variant = constantExpr.value()->variantAt(0);
    type = constantExpr.type();
  } catch (const velox::VeloxException&) {
    // Data-dependent evaluation failure on constant args (e.g.
    // division-by-zero, invalid cast). Leave the call unchanged so the
    // error surfaces at execution.
    return expr;
  }

  return builder_.makeLiteral(std::move(variant), toType(type));
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
