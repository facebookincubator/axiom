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
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "velox/common/base/Exceptions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"

namespace facebook::axiom::logical_plan {

namespace {

constexpr auto kNoAlias = std::nullopt;

} // namespace
namespace detail {

velox::core::ExprPtr BinaryCall(
    std::string name,
    velox::core::ExprPtr left,
    velox::core::ExprPtr right) {
  return std::make_shared<const velox::core::CallExpr>(
      std::move(name),
      std::vector{std::move(left), std::move(right)},
      kNoAlias);
}

} // namespace detail

ExprApi ExprApi::operator+(const velox::Variant& value) const {
  return Plus(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator-(const velox::Variant& value) const {
  return Minus(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator*(const velox::Variant& value) const {
  return Multiply(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator/(const velox::Variant& value) const {
  return Divide(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator%(const velox::Variant& value) const {
  return Modulus(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator<(const velox::Variant& value) const {
  return Lt(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator<=(const velox::Variant& value) const {
  return Lte(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator>(const velox::Variant& value) const {
  return Gt(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator>=(const velox::Variant& value) const {
  return Gte(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator&&(const velox::Variant& value) const {
  return And(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator||(const velox::Variant& value) const {
  return Or(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator==(const velox::Variant& value) const {
  return Eq(expr_, Lit(value).expr());
}

ExprApi ExprApi::operator!=(const velox::Variant& value) const {
  return NEq(expr_, Lit(value).expr());
}

ExprApi Col(std::string name) {
  return ExprApi{
      std::make_shared<const velox::core::FieldAccessExpr>(name, kNoAlias),
      std::move(name)};
}

ExprApi Col(std::string name, const ExprApi& input) {
  std::vector<velox::core::ExprPtr> inputs{input.expr()};
  return ExprApi{
      std::make_shared<const velox::core::FieldAccessExpr>(
          name, kNoAlias, std::move(inputs)),
      std::move(name)};
}

ExprApi Cast(velox::TypePtr type, const ExprApi& input) {
  return ExprApi{std::make_shared<const velox::core::CastExpr>(
      type, input.expr(), /* isTryCast */ false, kNoAlias)};
}

ExprApi TryCast(velox::TypePtr type, const ExprApi& input) {
  return ExprApi{std::make_shared<const velox::core::CastExpr>(
      type, input.expr(), /* isTryCast */ true, kNoAlias)};
}

ExprApi Lit(velox::Variant&& val) {
  auto type = val.inferType();
  return ExprApi{std::make_shared<const velox::core::ConstantExpr>(
      std::move(type), std::move(val), kNoAlias)};
}

ExprApi Lit(velox::Variant&& val, velox::TypePtr type) {
  return ExprApi{std::make_shared<const velox::core::ConstantExpr>(
      std::move(type), std::move(val), kNoAlias)};
}

ExprApi Lit(const velox::Variant& val) {
  auto type = val.inferType();
  return ExprApi{std::make_shared<const velox::core::ConstantExpr>(
      std::move(type), val, kNoAlias)};
}

ExprApi Lit(const velox::Variant& val, velox::TypePtr type) {
  return ExprApi{std::make_shared<const velox::core::ConstantExpr>(
      std::move(type), val, kNoAlias)};
}

ExprApi Call(std::string name, const std::vector<ExprApi>& args) {
  std::vector<velox::core::ExprPtr> argExpr;
  argExpr.reserve(args.size());
  for (auto& arg : args) {
    argExpr.push_back(arg.expr());
  }
  return ExprApi{std::make_shared<const velox::core::CallExpr>(
      std::move(name), std::move(argExpr), kNoAlias)};
}

ExprApi Lambda(std::vector<std::string> names, const ExprApi& body) {
  return ExprApi{
      std::make_shared<velox::core::LambdaExpr>(std::move(names), body.expr())};
}

ExprApi Subquery(std::shared_ptr<const LogicalPlanNode> subquery) {
  VELOX_CHECK_NOT_NULL(subquery);
  VELOX_CHECK_LE(1, subquery->outputType()->size());

  const auto& name = subquery->outputType()->nameOf(0);
  const auto expr =
      std::make_shared<velox::core::SubqueryExpr>(std::move(subquery));
  if (name.empty()) {
    return {expr};
  }
  return {expr, name};
}

ExprApi Exists(const ExprApi& input) {
  return ExprApi{std::make_shared<velox::core::CallExpr>(
      "exists", std::vector<velox::core::ExprPtr>{input.expr()}, kNoAlias)};
}

ExprApi MetadataRowCount() {
  return ExprApi{std::make_shared<velox::core::SpecialFormAggCallExpr>(
      SpecialAggregateKind::kMetadataRowCount,
      std::vector<velox::core::ExprPtr>{})};
}

ExprApi MetadataNullCount(const ExprApi& input) {
  return ExprApi{std::make_shared<velox::core::SpecialFormAggCallExpr>(
      SpecialAggregateKind::kMetadataNullCount,
      std::vector<velox::core::ExprPtr>{input.expr()})};
}

ExprApi MetadataNonNullCount(const ExprApi& input) {
  return ExprApi{std::make_shared<velox::core::SpecialFormAggCallExpr>(
      SpecialAggregateKind::kMetadataNonNullCount,
      std::vector<velox::core::ExprPtr>{input.expr()})};
}

ExprApi Sql(const std::string& sql) {
  return ExprApi{velox::parse::DuckSqlExpressionsParser().parseExpr(sql)};
}

ExprApi Ordinality() {
  return Lit(true);
}

ExprApi ExprApi::unnestAs(std::vector<std::string> aliases) const {
  VELOX_USER_CHECK(!aliases.empty(), "UNNEST aliases must not be empty.");
  VELOX_USER_CHECK(
      !expr_->is(velox::core::IExpr::Kind::kWindow),
      "UNNEST cannot be combined with OVER: {}",
      expr_->toString());
  return ExprApi(expr_, alias_, std::move(aliases));
}

ExprApi ExprApi::over(const WindowSpec& windowSpec) const {
  VELOX_USER_CHECK(
      expr_->is(velox::core::IExpr::Kind::kCall),
      "OVER can only be applied to a plain function call: {}",
      expr_->toString());

  auto* call = expr_->as<velox::core::CallExpr>();

  std::vector<velox::core::ExprPtr> partitionKeys;
  partitionKeys.reserve(windowSpec.partitionKeys().size());
  for (const auto& key : windowSpec.partitionKeys()) {
    partitionKeys.push_back(key.expr());
  }

  std::vector<velox::core::SortKey> orderByKeys;
  orderByKeys.reserve(windowSpec.orderByKeys().size());
  for (const auto& key : windowSpec.orderByKeys()) {
    orderByKeys.push_back({key.expr.expr(), key.ascending, key.nullsFirst});
  }

  return ExprApi(
      std::make_shared<velox::core::WindowCallExpr>(
          call->name(),
          call->inputs(),
          std::move(partitionKeys),
          std::move(orderByKeys),
          windowSpec.frame(),
          windowSpec.isIgnoreNulls()),
      alias_);
}

namespace {

// Returns the AggregateCallExpr if the expression is one, or nullptr.
const velox::core::AggregateCallExpr* asAggregate(
    const velox::core::ExprPtr& expr) {
  if (expr->is(velox::core::IExpr::Kind::kAggregate)) {
    return expr->as<velox::core::AggregateCallExpr>();
  }
  return nullptr;
}

} // namespace

ExprApi ExprApi::distinct() const {
  VELOX_USER_CHECK(
      expr_->is(velox::core::IExpr::Kind::kCall) ||
          expr_->is(velox::core::IExpr::Kind::kAggregate),
      "DISTINCT can only be applied to a function call: {}",
      expr_->toString());
  VELOX_USER_CHECK(
      !expr_->is(velox::core::IExpr::Kind::kWindow),
      "DISTINCT cannot be combined with OVER: {}",
      expr_->toString());

  auto* agg = asAggregate(expr_);
  VELOX_USER_CHECK(
      !agg || !agg->isDistinct(),
      "DISTINCT already specified: {}",
      expr_->toString());

  velox::core::ExprPtr filter;
  std::vector<velox::core::SortKey> orderBy;
  if (agg) {
    filter = agg->filter();
    orderBy = agg->orderBy();
  }

  auto* call = expr_->as<velox::core::CallExpr>();
  return ExprApi(
      std::make_shared<velox::core::AggregateCallExpr>(
          call->name(),
          call->inputs(),
          /*distinct=*/true,
          std::move(filter),
          std::move(orderBy)),
      alias_);
}

ExprApi ExprApi::filter(const ExprApi& filterExpr) const {
  VELOX_USER_CHECK(
      expr_->is(velox::core::IExpr::Kind::kCall) ||
          expr_->is(velox::core::IExpr::Kind::kAggregate),
      "FILTER can only be applied to a function call: {}",
      expr_->toString());
  VELOX_USER_CHECK(
      !expr_->is(velox::core::IExpr::Kind::kWindow),
      "FILTER cannot be combined with OVER: {}",
      expr_->toString());

  auto* agg = asAggregate(expr_);
  VELOX_USER_CHECK(
      !agg || agg->filter() == nullptr,
      "FILTER already specified: {}",
      expr_->toString());

  bool distinct = agg ? agg->isDistinct() : false;
  std::vector<velox::core::SortKey> orderBy;
  if (agg) {
    orderBy = agg->orderBy();
  }

  auto* call = expr_->as<velox::core::CallExpr>();
  return ExprApi(
      std::make_shared<velox::core::AggregateCallExpr>(
          call->name(),
          call->inputs(),
          distinct,
          filterExpr.expr(),
          std::move(orderBy)),
      alias_);
}

ExprApi ExprApi::sortBy(const std::vector<SortKey>& keys) const {
  VELOX_USER_CHECK(
      expr_->is(velox::core::IExpr::Kind::kCall) ||
          expr_->is(velox::core::IExpr::Kind::kAggregate),
      "ORDER BY can only be applied to a function call: {}",
      expr_->toString());
  VELOX_USER_CHECK(
      !expr_->is(velox::core::IExpr::Kind::kWindow),
      "ORDER BY cannot be combined with OVER: {}",
      expr_->toString());

  auto* agg = asAggregate(expr_);
  VELOX_USER_CHECK(
      !agg || agg->orderBy().empty(),
      "ORDER BY already specified: {}",
      expr_->toString());

  // Convert SortKey (ExprApi-level) to velox::core::SortKey
  // (IExpr-level).
  std::vector<velox::core::SortKey> sortKeys;
  sortKeys.reserve(keys.size());
  for (const auto& key : keys) {
    sortKeys.push_back({key.expr.expr(), key.ascending, key.nullsFirst});
  }

  bool distinct = agg ? agg->isDistinct() : false;
  velox::core::ExprPtr filter;
  if (agg) {
    filter = agg->filter();
  }

  auto* call = expr_->as<velox::core::CallExpr>();
  return ExprApi(
      std::make_shared<velox::core::AggregateCallExpr>(
          call->name(),
          call->inputs(),
          distinct,
          std::move(filter),
          std::move(sortKeys)),
      alias_);
}

namespace {

velox::core::WindowCallExpr::Frame makeFrame(
    velox::core::WindowCallExpr::WindowType type,
    velox::core::WindowCallExpr::BoundType startType,
    std::optional<ExprApi> startValue,
    velox::core::WindowCallExpr::BoundType endType,
    std::optional<ExprApi> endValue) {
  return {
      type,
      startType,
      startValue ? startValue->expr() : nullptr,
      endType,
      endValue ? endValue->expr() : nullptr,
  };
}

} // namespace

WindowSpec& WindowSpec::rows(
    velox::core::WindowCallExpr::BoundType startType,
    std::optional<ExprApi> startValue,
    velox::core::WindowCallExpr::BoundType endType,
    std::optional<ExprApi> endValue) {
  frame_ = makeFrame(
      velox::core::WindowCallExpr::WindowType::kRows,
      startType,
      std::move(startValue),
      endType,
      std::move(endValue));
  return *this;
}

WindowSpec& WindowSpec::range(
    velox::core::WindowCallExpr::BoundType startType,
    std::optional<ExprApi> startValue,
    velox::core::WindowCallExpr::BoundType endType,
    std::optional<ExprApi> endValue) {
  frame_ = makeFrame(
      velox::core::WindowCallExpr::WindowType::kRange,
      startType,
      std::move(startValue),
      endType,
      std::move(endValue));
  return *this;
}

WindowSpec& WindowSpec::groups(
    velox::core::WindowCallExpr::BoundType startType,
    std::optional<ExprApi> startValue,
    velox::core::WindowCallExpr::BoundType endType,
    std::optional<ExprApi> endValue) {
  frame_ = makeFrame(
      velox::core::WindowCallExpr::WindowType::kGroups,
      startType,
      std::move(startValue),
      endType,
      std::move(endValue));
  return *this;
}

} // namespace facebook::axiom::logical_plan
