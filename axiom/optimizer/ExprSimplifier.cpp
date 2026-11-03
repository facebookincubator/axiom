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

#include "axiom/optimizer/ExprSimplifier.h"

#include "axiom/optimizer/PlanUtils.h"

#include <algorithm>
#include "axiom/optimizer/Domain.h"
#include "axiom/optimizer/ExprEmitter.h"
#include "axiom/optimizer/ExprFactory.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/QueryGraph.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::axiom::optimizer {

namespace {

// Returns the values of IN arguments '[value, list...]' when the list holds one
// literal of the value's type per value, the form ExprFactory::makeIn folds.
// Returns std::nullopt otherwise, including for makeIn's own form, whose list
// is one ARRAY literal.
std::optional<std::vector<velox::Variant>> literalInValues(
    const ExprVector& args) {
  if (args.size() < 2) {
    return std::nullopt;
  }
  std::vector<velox::Variant> values;
  values.reserve(args.size() - 1);
  for (size_t i = 1; i < args.size(); ++i) {
    if (!args[i]->is(PlanType::kLiteralExpr) ||
        !args[i]->value().type->equivalent(*args[0]->value().type)) {
      return std::nullopt;
    }
    values.push_back(args[i]->as<Literal>()->literal());
  }
  return values;
}

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

velox::Variant typedIntegralValue(TypeCP type, int64_t value) {
  switch (type->kind()) {
    case velox::TypeKind::TINYINT:
      return velox::Variant(static_cast<int8_t>(value));
    case velox::TypeKind::SMALLINT:
      return velox::Variant(static_cast<int16_t>(value));
    case velox::TypeKind::INTEGER:
      return velox::Variant(static_cast<int32_t>(value));
    case velox::TypeKind::BIGINT:
      return velox::Variant(value);
    default:
      VELOX_UNREACHABLE();
  }
}

ExprCP makeIntegralLiteral(Builder& builder, ColumnCP column, int64_t value) {
  return builder.makeLiteral(
      typedIntegralValue(column->value().type, value), column->value().type);
}

ExprCP rangeBoundToFilter(
    Builder& builder,
    ExprFactory& factory,
    ColumnCP column,
    const Bound& bound,
    bool isLower,
    int64_t typeLimit) {
  const auto value = bound.value.value<int64_t>();
  if (bound.inclusive && value == typeLimit) {
    return nullptr;
  }
  auto* literal = makeIntegralLiteral(builder, column, value);
  if (isLower) {
    return bound.inclusive ? factory.makeGreaterThanOrEqual(column, literal)
                           : factory.makeGreaterThan(column, literal);
  }
  return bound.inclusive ? factory.makeLessThanOrEqual(column, literal)
                         : factory.makeLessThan(column, literal);
}

ExprCP nonNullDomainToFilter(
    Builder& builder,
    ExprFactory& factory,
    ColumnCP column,
    const Domain& domain,
    const Domain& nonNullValues) {
  const auto& ranges = domain.ranges();
  const bool allSingleValues =
      std::all_of(ranges.begin(), ranges.end(), [](const Range& range) {
        return range.isSingleValue();
      });
  if (allSingleValues) {
    std::vector<velox::Variant> values;
    values.reserve(ranges.size());
    for (const auto& range : ranges) {
      values.push_back(typedIntegralValue(
          column->value().type, range.low()->value.value<int64_t>()));
    }
    return factory.makeIn(column, std::move(values));
  }

  ExprVector disjuncts;
  disjuncts.reserve(ranges.size());
  const auto minimum =
      nonNullValues.ranges().front().low()->value.value<int64_t>();
  const auto maximum =
      nonNullValues.ranges().front().high()->value.value<int64_t>();
  for (const auto& range : ranges) {
    if (range.low().has_value() && range.high().has_value() &&
        range.lowInclusive() && range.highInclusive() &&
        range.low()->value.value<int64_t>() != minimum &&
        range.high()->value.value<int64_t>() != maximum) {
      disjuncts.push_back(factory.makeBetween(
          column,
          makeIntegralLiteral(
              builder, column, range.low()->value.value<int64_t>()),
          makeIntegralLiteral(
              builder, column, range.high()->value.value<int64_t>())));
      continue;
    }

    ExprVector bounds;
    if (range.low().has_value()) {
      if (auto* filter = rangeBoundToFilter(
              builder,
              factory,
              column,
              *range.low(),
              /*isLower=*/true,
              minimum)) {
        bounds.push_back(filter);
      }
    }
    if (range.high().has_value()) {
      if (auto* filter = rangeBoundToFilter(
              builder,
              factory,
              column,
              *range.high(),
              /*isLower=*/false,
              maximum)) {
        bounds.push_back(filter);
      }
    }
    VELOX_CHECK(!bounds.empty());
    disjuncts.push_back(factory.andAll(bounds));
  }
  return factory.orAll(disjuncts);
}

ExprCP domainToFilter(Builder& builder, ColumnCP column, Domain domain) {
  const auto nonNullValues = *integralTypeDomain(*column->value().type);
  const auto allValues = nonNullValues.unite(Domain::onlyNull());
  domain = domain.intersect(allValues);
  if (domain == allValues) {
    return nullptr;
  }

  ExprFactory factory(builder);
  const auto nonNullResult = domain.intersect(Domain::notNull());
  ExprCP nonNullFilter{nullptr};
  if (nonNullResult == nonNullValues) {
    nonNullFilter = factory.makeNot(factory.makeIsNull(column));
  } else if (!nonNullResult.isNone()) {
    const auto excluded = nonNullValues.subtract(nonNullResult);
    if (excluded.ranges().size() < nonNullResult.ranges().size()) {
      nonNullFilter = factory.makeNot(nonNullDomainToFilter(
          builder, factory, column, excluded, nonNullValues));
    } else {
      nonNullFilter = nonNullDomainToFilter(
          builder, factory, column, nonNullResult, nonNullValues);
    }
  }

  if (!domain.nullsAllowed()) {
    return nonNullFilter;
  }
  ExprCP nullFilter = factory.makeIsNull(column);
  return nonNullFilter == nullptr ? nullFilter
                                  : factory.makeOr(nullFilter, nonNullFilter);
}

} // namespace

ExprCP ExprSimplifier::simplify(ExprCP expr) {
  if (const auto it = simplified_.find(expr); it != simplified_.end()) {
    return it->second;
  }

  const ExprCP original = expr;
  if (expr->is(PlanType::kCallExpr)) {
    const auto* call = expr->as<Call>();
    if (call->name() == SpecialFormCallNames::kIf ||
        call->name() == SpecialFormCallNames::kSwitch) {
      expr = simplifyConditional(call);
      expr = tryFoldConjunct(tryFoldConstant(expr));
      simplified_.emplace(original, expr);
      return expr;
    }
    const bool isCoalesce = call->name() == SpecialFormCallNames::kCoalesce;
    ExprVector args;
    args.reserve(call->args().size());
    bool changed = false;
    for (ExprCP arg : call->args()) {
      ExprCP simplified = simplify(arg);
      changed |= simplified != arg;
      // A null argument of coalesce never supplies the result.
      if (isCoalesce && isConstantNull(simplified)) {
        changed = true;
        continue;
      }
      args.push_back(simplified);
    }
    if (isCoalesce && args.size() <= 1) {
      expr = args.empty() ? builder_.makeNull(call->value().type) : args[0];
    } else if (
        auto values = call->name() == SpecialFormCallNames::kIn
            ? literalInValues(args)
            : std::nullopt) {
      // A list of literals takes ExprFactory::makeIn's form.
      expr = ExprFactory(builder_).makeIn(args[0], std::move(*values));
    } else if (changed) {
      expr = ExprFactory(builder_).rebuildCall(call, std::move(args));
    }
  } else if (expr->is(PlanType::kLambdaExpr)) {
    const auto* lambda = expr->as<Lambda>();
    ExprCP body = simplify(lambda->body());
    if (body != lambda->body()) {
      expr = ExprFactory(builder_).rebuildLambda(lambda, body);
    }
  }
  if (ExprCP field = tryFoldRowField(expr)) {
    expr = field;
  } else if (ExprCP fieldEqualities = tryExpandRowEquality(expr)) {
    expr = simplify(fieldEqualities);
  } else {
    expr = tryFoldConjunct(tryFoldConstant(expr));
  }
  simplified_.emplace(original, expr);
  return expr;
}

ExprCP ExprSimplifier::simplifyConditional(const Call* call) {
  ExprVector args;
  args.reserve(call->args().size());
  const size_t numConditionalArgs = call->args().size() / 2 * 2;
  ExprCP elseResult{nullptr};
  for (size_t i = 0; i < numConditionalArgs; i += 2) {
    ExprCP condition = simplify(call->args()[i]);
    if (condition->is(PlanType::kLiteralExpr)) {
      const auto& value = condition->as<Literal>()->literal();
      VELOX_CHECK_EQ(value.kind(), velox::TypeKind::BOOLEAN);
      if (value.isNull() || !value.value<bool>()) {
        continue;
      }

      elseResult = simplify(call->args()[i + 1]);
      break;
    }

    args.push_back(condition);
    args.push_back(simplify(call->args()[i + 1]));
  }

  if (elseResult == nullptr) {
    elseResult = call->args().size() % 2 == 1
        ? simplify(call->args().back())
        : builder_.makeNull(call->value().type);
  }

  if (args.empty()) {
    return elseResult;
  }
  args.push_back(elseResult);
  return ExprFactory(builder_).rebuildCall(call, std::move(args));
}

bool ExprSimplifier::isKnownNonNull(
    ExprCP expr,
    const PlanObjectSet& nonNullColumns) const {
  if (expr->is(PlanType::kColumnExpr)) {
    return nonNullColumns.contains(expr);
  }
  if (expr->is(PlanType::kLiteralExpr)) {
    return !expr->as<Literal>()->literal().isNull();
  }
  if (expr->is(PlanType::kCallExpr) &&
      expr->as<Call>()->name() == SpecialFormCallNames::kCoalesce) {
    return std::ranges::any_of(expr->as<Call>()->args(), [&](ExprCP argument) {
      return isKnownNonNull(argument, nonNullColumns);
    });
  }
  return false;
}

ExprCP ExprSimplifier::simplify(
    ExprCP expr,
    const PlanObjectSet& nonNullColumns) const {
  if (expr->is(PlanType::kFieldExpr)) {
    const auto* field = expr->as<Field>();
    const auto* base = simplify(field->base(), nonNullColumns);
    return base == field->base()
        ? expr
        : ExprFactory(builder_).rebuildField(field, base);
  }
  if (expr->is(PlanType::kLambdaExpr)) {
    const auto* lambda = expr->as<Lambda>();
    const auto* body = simplify(lambda->body(), nonNullColumns);
    return body == lambda->body()
        ? expr
        : ExprFactory(builder_).rebuildLambda(lambda, body);
  }
  if (expr->isNot(PlanType::kCallExpr)) {
    return expr;
  }

  const auto* call = expr->as<Call>();
  ExprVector rewrittenArgs;
  bool changed = false;
  const bool isCoalesce = call->name() == SpecialFormCallNames::kCoalesce;
  for (size_t i = 0; i < call->args().size(); ++i) {
    ExprCP argument = call->args()[i];
    const auto* simplified = simplify(argument, nonNullColumns);
    if (simplified != argument && !changed) {
      changed = true;
      rewrittenArgs.reserve(call->args().size());
      rewrittenArgs.insert(
          rewrittenArgs.end(), call->args().begin(), call->args().begin() + i);
    }
    // A null argument of coalesce never supplies the result.
    if (isCoalesce && isConstantNull(simplified)) {
      if (!changed) {
        changed = true;
        rewrittenArgs.reserve(call->args().size());
        rewrittenArgs.insert(
            rewrittenArgs.end(),
            call->args().begin(),
            call->args().begin() + i);
      }
      continue;
    }
    if (changed) {
      rewrittenArgs.push_back(simplified);
    }
    if (isCoalesce && isKnownNonNull(simplified, nonNullColumns)) {
      if (i == 0) {
        return simplified;
      }
      if (i + 1 < call->args().size() && !changed) {
        changed = true;
        rewrittenArgs.assign(
            call->args().begin(), call->args().begin() + i + 1);
      }
      break;
    }
  }

  if (call->name() == builder_.functionNames().isNull &&
      isKnownNonNull(
          changed ? rewrittenArgs[0] : call->args()[0], nonNullColumns)) {
    return builder_.makeBoolean(false);
  }

  if (call->name() == builder_.functionNames().equality) {
    ExprCP lhs = changed ? rewrittenArgs[0] : call->args()[0];
    ExprCP rhs = changed ? rewrittenArgs[1] : call->args()[1];
    // A complex value can hold NULL elements, which make its equality NULL.
    if (lhs == rhs && lhs->value().type->isPrimitiveType() &&
        !lhs->containsNonDeterministic() &&
        isKnownNonNull(lhs, nonNullColumns)) {
      return builder_.makeBoolean(true);
    }
  }

  if (!changed) {
    return expr;
  }
  if (isCoalesce && rewrittenArgs.size() <= 1) {
    return rewrittenArgs.empty() ? builder_.makeNull(call->value().type)
                                 : rewrittenArgs.front();
  }
  return ExprFactory(builder_).rebuildCall(call, std::move(rewrittenArgs));
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
  // A literal argument is never the one that determined the call's
  // cardinality, so dropping it leaves `value()` valid.
  return ExprFactory(builder_).rebuildCall(call, std::move(remaining));
}

bool ExprSimplifier::simplifyFilter(ExprCP predicate, ExprVector& into) {
  ExprVector flattened = ExprFactory::flattenAnd(predicate);
  ExprVector candidates;
  candidates.reserve(flattened.size());
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
    if (tryFactorOr(builder_, simplified, candidates, &residual)) {
      if (residual != nullptr) {
        candidates.push_back(residual);
      }
      continue;
    }
    candidates.push_back(simplified);
  }

  struct ColumnDomain {
    Domain values;
    size_t outputIndex;
  };
  folly::F14FastMap<ColumnCP, ColumnDomain> domains;
  ExprVector surviving;
  surviving.reserve(candidates.size());
  for (ExprCP candidate : candidates) {
    if (candidate->columns().size() != 1) {
      surviving.push_back(candidate);
      continue;
    }
    const auto* column = candidate->columns().onlyObject<Column>();
    if (!integralTypeDomain(*column->value().type).has_value()) {
      surviving.push_back(candidate);
      continue;
    }
    auto domain = exprToDomain(candidate);
    if (!domain.has_value()) {
      surviving.push_back(candidate);
      continue;
    }

    auto it = domains.find(column);
    if (it == domains.end()) {
      domains.emplace(
          column, ColumnDomain{std::move(*domain), surviving.size()});
      surviving.push_back(nullptr);
    } else {
      it->second.values = it->second.values.intersect(*domain);
    }
  }

  for (const auto& [column, domain] : domains) {
    if (domain.values.isNone()) {
      return true;
    }
    surviving[domain.outputIndex] =
        domainToFilter(builder_, column, domain.values);
  }

  for (ExprCP expr : surviving) {
    if (expr != nullptr) {
      into.push_back(expr);
    }
  }
  return false;
}

ExprCP ExprSimplifier::tryFoldRowField(ExprCP expr) {
  if (!expr->is(PlanType::kCallExpr)) {
    return nullptr;
  }
  const auto* call = expr->as<Call>();
  if (call->name() != SpecialFormCallNames::kDereference ||
      !call->args()[0]->is(PlanType::kCallExpr)) {
    return nullptr;
  }
  const auto* row = call->args()[0]->as<Call>();
  if (!row->isRowConstructor()) {
    return nullptr;
  }
  // A dereference selects its field by an integer literal.
  return row->args()[integerValue(&call->literalAt(1))];
}

ExprCP ExprSimplifier::tryExpandRowEquality(ExprCP expr) {
  if (!expr->is(PlanType::kCallExpr)) {
    return nullptr;
  }
  const auto* call = expr->as<Call>();
  if (call->name() != builder_.functionNames().equality) {
    return nullptr;
  }
  ExprCP lhs = call->args()[0];
  ExprCP rhs = call->args()[1];
  if (!lhs->is(PlanType::kCallExpr) || !rhs->is(PlanType::kLiteralExpr)) {
    return nullptr;
  }

  const auto* rowConstructor = lhs->as<Call>();
  const auto& literal = rhs->as<Literal>()->literal();
  if (!rowConstructor->isRowConstructor() || literal.isNull()) {
    return nullptr;
  }

  const auto& fields = rowConstructor->args();
  const auto& values = literal.row();
  const TypeCP type = rhs->value().type;
  VELOX_CHECK_EQ(fields.size(), values.size());

  ExprFactory factory(builder_);
  ExprVector equalities;
  equalities.reserve(fields.size());
  for (size_t i = 0; i < fields.size(); ++i) {
    equalities.push_back(factory.makeEq(
        fields[i], factory.makeLiteral(values[i], type->childAt(i).get())));
  }
  return factory.andAll(equalities);
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

} // namespace facebook::axiom::optimizer
