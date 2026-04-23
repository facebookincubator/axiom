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

#include <memory>
#include <string>
#include <vector>
#include "velox/core/ITypedExpr.h"
#include "velox/type/Type.h"

namespace facebook::velox::core {

/// Structural pattern matcher for typed expression trees (ITypedExpr),
/// analogous to PlanMatcher for plan nodes.
///
/// Replaces the old approach of comparing filter expressions via
/// DuckDB-parsed toString() equality, which is fragile against float
/// roundoff (0.06 - 0.01 stringifies as "0.049999999999999996", not
/// "0.05") and adds an unnecessary DuckDB dependency.
///
/// Matching uses ConstantTypedExpr::operator== for constants (strict
/// type + value equality), dynamic_cast for node-type discrimination,
/// and recursive child matching.
///
/// Factories:
///   col("name")                - column reference
///   constant(value)            - typed literal (int32, int64, double, bool,
///                                string)
///   constant(type, "value")    - typed literal from string (e.g. DATE)
///   call("fn", {args...})      - function call
///   cast(type, input)          - CAST expression with target type
///   any()                      - wildcard (matches anything)
///
/// Fluent operators: eq, neq, lt, lte, gt, gte, between, and_, or_,
/// not_, like.
///
/// Examples (with `using namespace core::em;`):
///
///   // Simple comparison
///   col("a").eq(col("b")).match(actualExpr);
///
///   // Compound TPC-H Q6 filter
///   col("l_shipdate").gte(constant(DATE(), "1994-01-01"))
///       .and_(col("l_shipdate").lt(constant(DATE(), "1995-01-01")))
///       .and_(col("l_discount").between(constant(0.05), constant(0.07)))
///       .and_(col("l_quantity").lt(constant(24.0)))
///       .match(actualExpr);
///
///   // Wildcard for complex constants (e.g. IN-list arrays)
///   call("in", {col("x"), any()}).match(actualExpr);
///
///   // CAST with target type
///   cast(DATE(), col("x")).match(actualExpr);
class ExpressionMatcher {
 public:
  /// Matches any expression (wildcard).
  static ExpressionMatcher any();

  static ExpressionMatcher col(const std::string& name);

  static ExpressionMatcher constant(int64_t value);
  static ExpressionMatcher constant(int32_t value);
  static ExpressionMatcher constant(float value);
  static ExpressionMatcher constant(double value);
  static ExpressionMatcher constant(bool value);
  static ExpressionMatcher constant(const std::string& value);
  static ExpressionMatcher constant(const char* value);

  /// Typed constant factory. Parses the string representation according to
  /// the given type. Currently supports DATE (e.g. "1994-01-01").
  static ExpressionMatcher constant(
      const TypePtr& type,
      const std::string& value);

  static ExpressionMatcher call(
      const std::string& name,
      std::vector<ExpressionMatcher> inputs);

  /// Matches a CAST expression with the given target type and input.
  static ExpressionMatcher cast(TypePtr targetType, ExpressionMatcher input);

  explicit ExpressionMatcher(int32_t value);
  explicit ExpressionMatcher(int64_t value);
  explicit ExpressionMatcher(double value);

  ExpressionMatcher eq(ExpressionMatcher rhs) const;
  ExpressionMatcher neq(ExpressionMatcher rhs) const;
  ExpressionMatcher lt(ExpressionMatcher rhs) const;
  ExpressionMatcher lte(ExpressionMatcher rhs) const;
  ExpressionMatcher gt(ExpressionMatcher rhs) const;
  ExpressionMatcher gte(ExpressionMatcher rhs) const;
  ExpressionMatcher between(ExpressionMatcher low, ExpressionMatcher high)
      const;
  ExpressionMatcher and_(ExpressionMatcher rhs) const;
  ExpressionMatcher or_(ExpressionMatcher rhs) const;
  ExpressionMatcher not_() const;
  ExpressionMatcher like(ExpressionMatcher pattern) const;

  /// Match this pattern against a typed expression tree. Sets gtest
  /// EXPECT failures with diagnostics on mismatch.
  bool match(const TypedExprPtr& actual) const;

 private:
  struct Impl;
  std::shared_ptr<const Impl> impl_;

  explicit ExpressionMatcher(std::shared_ptr<const Impl> impl);

  static bool matchImpl(const Impl& pattern, const ITypedExpr* actual);
  static std::string describePattern(const Impl& pattern);
};

/// Short aliases for use with `using namespace core::em;` in tests.
namespace em {
using ExpressionMatcher = core::ExpressionMatcher;
inline ExpressionMatcher any() {
  return ExpressionMatcher::any();
}
inline ExpressionMatcher col(const std::string& name) {
  return ExpressionMatcher::col(name);
}
inline ExpressionMatcher constant(int64_t v) {
  return ExpressionMatcher::constant(v);
}
inline ExpressionMatcher constant(int32_t v) {
  return ExpressionMatcher::constant(v);
}
inline ExpressionMatcher constant(float v) {
  return ExpressionMatcher::constant(v);
}
inline ExpressionMatcher constant(double v) {
  return ExpressionMatcher::constant(v);
}
inline ExpressionMatcher constant(bool v) {
  return ExpressionMatcher::constant(v);
}
inline ExpressionMatcher constant(const std::string& v) {
  return ExpressionMatcher::constant(v);
}
inline ExpressionMatcher constant(const char* v) {
  return ExpressionMatcher::constant(v);
}
inline ExpressionMatcher constant(const TypePtr& type, const std::string& v) {
  return ExpressionMatcher::constant(type, v);
}
inline ExpressionMatcher call(
    const std::string& name,
    std::vector<ExpressionMatcher> inputs) {
  return ExpressionMatcher::call(name, std::move(inputs));
}
inline ExpressionMatcher cast(TypePtr targetType, ExpressionMatcher input) {
  return ExpressionMatcher::cast(std::move(targetType), std::move(input));
}
} // namespace em

} // namespace facebook::velox::core
