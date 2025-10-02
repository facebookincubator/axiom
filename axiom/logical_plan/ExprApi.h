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

#include "velox/parse/IExpr.h"
#include "velox/type/Variant.h"

/// Convenient API for building expression trees "by hand".
///
/// - Col(<name>) - column reference
/// - Lit(<value>) - literal value
/// - Call(<name>, arg1, arg2,...) - function call; supports zero or more
/// argumnts
/// - Cast(<type>, arg) - cast expression
/// - Lambda({<args>}, <body>) - lambda expression
/// - Subquery(<plan>) - correlated (or not) subquery expression
///
/// The API supports basic arithmetic and comparison operations:
/// - Col("a") + Col("b")
/// - Col("a") * 10
/// - Col("a") < 0

namespace facebook::axiom::logical_plan {

class LogicalPlanNode;
using LogicalPlanNodePtr = std::shared_ptr<const LogicalPlanNode>;

} // namespace facebook::axiom::logical_plan
namespace facebook::velox::core {

// A scalar, possibly correlated, subquery expression.
class SubqueryExpr : public core::IExpr {
 public:
  // @param subquery A plan tree that produces a single column.
  explicit SubqueryExpr(axiom::logical_plan::LogicalPlanNodePtr subquery)
      : IExpr(IExpr::Kind::kSubquery, {}), subquery_(std::move(subquery)) {}

  const axiom::logical_plan::LogicalPlanNodePtr& subquery() const {
    return subquery_;
  }

  std::string toString() const override {
    return "<subquery>";
  }

  ExprPtr replaceInputs(std::vector<ExprPtr> newInputs) const override {
    VELOX_CHECK_EQ(newInputs.size(), 0);
    return std::make_shared<SubqueryExpr>(subquery_);
  }

  ExprPtr dropAlias() const final {
    return std::make_shared<SubqueryExpr>(subquery_);
  }

  bool operator==(const IExpr& other) const override {
    if (!other.is(Kind::kSubquery)) {
      return false;
    }

    auto* otherSubquery = other.as<SubqueryExpr>();
    // Compare the subquery plan pointers. Two SubqueryExprs are equal if they
    // reference the same logical plan node.
    return subquery_ == otherSubquery->subquery_ &&
        compareAliasAndInputs(other);
  }

 protected:
  size_t localHash() const override {
    // Hash the pointer to the logical plan node
    return std::hash<axiom::logical_plan::LogicalPlanNodePtr>{}(subquery_);
  }

 private:
  const axiom::logical_plan::LogicalPlanNodePtr subquery_;
};

} // namespace facebook::velox::core
namespace facebook::axiom::logical_plan {
namespace detail {

velox::core::ExprPtr BinaryCall(
    std::string name,
    velox::core::ExprPtr left,
    velox::core::ExprPtr right);

} // namespace detail

#define VELOX_MAKE_BINARY_CALLER(OutName, InName)                         \
  inline velox::core::ExprPtr OutName(                                    \
      velox::core::ExprPtr left, velox::core::ExprPtr right) {            \
    return detail::BinaryCall(InName, std::move(left), std::move(right)); \
  }

VELOX_MAKE_BINARY_CALLER(Plus, "plus")
VELOX_MAKE_BINARY_CALLER(Minus, "minus")
VELOX_MAKE_BINARY_CALLER(Multiply, "multiply")
VELOX_MAKE_BINARY_CALLER(Divide, "divide")
VELOX_MAKE_BINARY_CALLER(Modulus, "mod")
VELOX_MAKE_BINARY_CALLER(Lt, "lt")
VELOX_MAKE_BINARY_CALLER(Gt, "gt")
VELOX_MAKE_BINARY_CALLER(Lte, "lte")
VELOX_MAKE_BINARY_CALLER(Gte, "gte")
VELOX_MAKE_BINARY_CALLER(Eq, "eq")
VELOX_MAKE_BINARY_CALLER(NEq, "neq")
VELOX_MAKE_BINARY_CALLER(And, "and")
VELOX_MAKE_BINARY_CALLER(Or, "or")

#undef VELOX_MAKE_BINARY_CALLER

class ExprApi {
 public:
  /* implicit */ ExprApi(velox::core::ExprPtr expr)
      : expr_{std::move(expr)}, alias_{expr_->alias()} {}

  ExprApi(
      velox::core::ExprPtr expr,
      std::optional<std::string> alias,
      std::vector<std::string> unnestedAliases = {})
      : expr_{std::move(expr)},
        alias_{std::move(alias)},
        unnestedAliases_{std::move(unnestedAliases)} {
    if (alias_.has_value()) {
      VELOX_CHECK(!alias_.value().empty());
    }

    for (const auto& alias : unnestedAliases_) {
      VELOX_CHECK(!alias.empty());
    }
  }

  ExprApi(const ExprApi& other) = default;

  ExprApi(ExprApi&& other) = default;

  ExprApi& operator=(const ExprApi& other) = default;

  ExprApi& operator=(ExprApi&& other) = default;

  const velox::core::ExprPtr& expr() const {
    return expr_;
  }

  ExprApi operator+(const ExprApi& other) const {
    return Plus(expr_, other.expr_);
  }

  ExprApi operator+(const velox::Variant& value) const;

  ExprApi operator-(const ExprApi& other) const {
    return Minus(expr_, other.expr_);
  }

  ExprApi operator-(const velox::Variant& value) const;

  ExprApi operator*(const ExprApi& other) const {
    return Multiply(expr_, other.expr_);
  }

  ExprApi operator*(const velox::Variant& value) const;

  ExprApi operator/(const ExprApi& other) const {
    return Divide(expr_, other.expr_);
  }

  ExprApi operator/(const velox::Variant& value) const;

  ExprApi operator%(const ExprApi& other) const {
    return Modulus(expr_, other.expr_);
  }

  ExprApi operator%(const velox::Variant& value) const;

  ExprApi operator<(const ExprApi& other) const {
    return Lt(expr_, other.expr_);
  }

  ExprApi operator<(const velox::Variant& value) const;

  ExprApi operator<=(const ExprApi& other) const {
    return Lte(expr_, other.expr_);
  }

  ExprApi operator<=(const velox::Variant& value) const;

  ExprApi operator>(const ExprApi& other) const {
    return Gt(expr_, other.expr_);
  }

  ExprApi operator>(const velox::Variant& value) const;

  ExprApi operator>=(const ExprApi& other) const {
    return Gte(expr_, other.expr_);
  }

  ExprApi operator>=(const velox::Variant& value) const;

  ExprApi operator&&(const ExprApi& other) const {
    return And(expr_, other.expr_);
  }

  ExprApi operator&&(const velox::Variant& value) const;

  ExprApi operator||(const ExprApi& other) const {
    return Or(expr_, other.expr_);
  }

  ExprApi operator||(const velox::Variant& value) const;

  ExprApi operator==(const ExprApi& other) const {
    return Eq(expr_, other.expr_);
  }

  ExprApi operator==(const velox::Variant& value) const;

  ExprApi operator!=(const ExprApi& other) const {
    return NEq(expr_, other.expr_);
  }

  ExprApi operator!=(const velox::Variant& value) const;

  // TODO Remove in favor of 'alias'.
  const std::optional<std::string>& name() const {
    return alias_;
  }

  const std::optional<std::string>& alias() const {
    return alias_;
  }

  const std::vector<std::string>& unnestedAliases() const {
    return unnestedAliases_;
  }

  ExprApi as(std::string alias) const {
    return ExprApi(expr_, std::move(alias));
  }

  ExprApi unnestAs(std::vector<std::string> aliases) const {
    return ExprApi(expr_, alias_, aliases);
  }

  template <typename... T>
  ExprApi unnestAs(T... aliases) const {
    return unnestAs(std::vector<std::string>{std::forward<T>(aliases)...});
  }

 private:
  velox::core::ExprPtr expr_;
  std::optional<std::string> alias_;
  std::vector<std::string> unnestedAliases_;
};

ExprApi Lit(velox::Variant&& val);

ExprApi Lit(velox::Variant&& val, velox::TypePtr type);

ExprApi Lit(const velox::Variant& val);

ExprApi Lit(const velox::Variant& val, velox::TypePtr type);

template <typename T>
inline ExprApi Lit(T&& val) {
  return Lit(velox::Variant(std::forward<T>(val)));
}

ExprApi Col(std::string name);

ExprApi Col(std::string name, const ExprApi& input);

ExprApi Call(std::string name, const std::vector<ExprApi>& args);

template <typename... T>
ExprApi Call(std::string name, T... args) {
  return Call(std::move(name), std::vector<ExprApi>{std::forward<T>(args)...});
}

template <typename... T>
ExprApi In(T... args) {
  return Call("in", std::vector<ExprApi>{std::forward<T>(args)...});
}

ExprApi Cast(velox::TypePtr type, const ExprApi& input);

ExprApi TryCast(velox::TypePtr type, const ExprApi& input);

inline ExprApi Cast(velox::TypePtr type, const velox::Variant& value) {
  return Cast(std::move(type), Lit(value));
}

ExprApi Lambda(std::vector<std::string> names, const ExprApi& body);

ExprApi Subquery(std::shared_ptr<const LogicalPlanNode> subquery);

ExprApi Exists(const ExprApi& input);

ExprApi Sql(const std::string& sql);

// SortKey to use with sort operation.
// e.g.
//
//  SortKey(Col("name"))                  -- name ASC
//  SortKey(Col("name"), DESC)            -- name DESC
//  SortKey(Col("name"), ASC_NULLS_LAST)  -- name ASC NULLS LAST

namespace sorting {
// Default is ASC NULLS LAST.
template <bool ascending = true, bool nullsFirst = false>
struct Order {};
} // namespace sorting

constexpr sorting::Order<true> ASC;
constexpr sorting::Order<false> DESC;

constexpr sorting::Order<true, true> ASC_NULLS_FIRST;
constexpr sorting::Order<true, false> ASC_NULLS_LAST;

constexpr sorting::Order<false, true> DESC_NULLS_FIRST;
constexpr sorting::Order<false, false> DESC_NULLS_LAST;

struct SortKey {
  template <bool ascending = true, bool nullsFirst = false>
  explicit SortKey(ExprApi input, sorting::Order<ascending, nullsFirst> = ASC)
      : expr{std::move(input)}, ascending{ascending}, nullsFirst(nullsFirst) {}

  SortKey(ExprApi input, bool ascending, bool nullsFirst)
      : expr{std::move(input)}, ascending{ascending}, nullsFirst(nullsFirst) {}

  const ExprApi expr;
  const bool ascending;
  const bool nullsFirst;
};

} // namespace facebook::axiom::logical_plan
