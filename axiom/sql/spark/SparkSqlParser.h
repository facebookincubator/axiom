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

#include <functional>
#include <span>
#include <string>
#include <string_view>

#include "axiom/logical_plan/ExprApi.h"
#include "velox/type/Type.h"

namespace facebook::velox::tz {
class TimeZone;
} // namespace facebook::velox::tz

namespace axiom::sql::spark {

/// Parses Spark SQL, with Spark 3.4's grammar and default settings: keywords
/// are not ANSI, and double quotes delimit strings.
///
///   SparkSqlParser parser(*tz::locateZone("America/Los_Angeles"));
///   auto expr = parser.parseExpression("age + 1 AS next_age", nameResolver);
///   // add(age, 1) aliased next_age.
class SparkSqlParser {
 public:
  /// @param sessionTimeZone The time zone of a TIMESTAMP literal without one.
  explicit SparkSqlParser(const facebook::velox::tz::TimeZone& sessionTimeZone)
      : sessionTimeZone_{&sessionTimeZone} {}

  /// Binds the names of an expression to the caller's plan.
  struct NameResolver {
    /// Returns the expression a name refers to, given its parts: a, or t.a,
    /// or a.f for a field of a struct column.
    std::function<facebook::axiom::logical_plan::ExprApi(
        std::span<const std::string> nameParts)>
        resolveName;

    /// Returns the name of a lambda parameter, given its name in the text.
    /// The name must differ from every name 'resolveName' refers to, so the
    /// parameter cannot hide a column.
    std::function<std::string(std::string_view name)> newLambdaParameterName;
  };

  /// Parses one expression, optionally aliased, as in F.expr() or
  /// DataFrame.selectExpr(). Names other than lambda parameters go to
  /// 'nameResolver'. Operators and functions follow SparkFunctions.
  ///
  /// @throws VeloxUserError on a syntax error, naming its position.
  /// @throws VeloxRuntimeError with kNotImplemented for a construct not
  /// supported yet, such as a subquery or a window function.
  facebook::axiom::logical_plan::ExprApi parseExpression(
      std::string_view sql,
      const NameResolver& nameResolver) const;

  /// Parses a Spark type name, such as 'int', 'decimal(10, 2)' or
  /// 'array<struct<a: string>>', as in Column.cast().
  ///
  /// @throws VeloxUserError on a syntax error or an unknown type.
  /// @throws VeloxRuntimeError with kNotImplemented for a type not supported
  /// yet, such as an interval.
  static facebook::velox::TypePtr parseDataType(std::string_view sql);

 private:
  const facebook::velox::tz::TimeZone* sessionTimeZone_;
};

} // namespace axiom::sql::spark
