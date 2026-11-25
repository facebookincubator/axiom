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
///   SparkSqlParser parser(tz::locateZone("America/Los_Angeles"));
///   auto expr = parser.parseExpression("age + 1 AS next_age");
///   // add(age, 1) aliased next_age.
class SparkSqlParser {
 public:
  /// @param sessionTimeZone The time zone of a TIMESTAMP literal without one.
  /// Not null.
  explicit SparkSqlParser(const facebook::velox::tz::TimeZone* sessionTimeZone);

  /// Parses one expression, optionally aliased, as in F.expr() or
  /// DataFrame.selectExpr(). Returns an unresolved expression: names and
  /// functions are resolved when the expression is added to a plan.
  /// Operators and functions follow SparkFunctions.
  ///
  /// @throws VeloxUserError on a syntax error, naming its position.
  /// @throws VeloxRuntimeError with kNotImplemented for a construct not
  /// supported yet, such as a subquery or a window function.
  facebook::axiom::logical_plan::ExprApi parseExpression(
      std::string_view sql) const;

  /// Parses a Spark type name, such as 'int', 'decimal(10, 2)' or
  /// 'array<struct<a: string>>', as in Column.cast().
  ///
  /// @throws VeloxUserError on a syntax error or an unknown type.
  /// @throws VeloxRuntimeError with kNotImplemented for a type not supported
  /// yet, such as an interval.
  static facebook::velox::TypePtr parseDataType(std::string_view sql);

 private:
  const facebook::velox::tz::TimeZone* const sessionTimeZone_;
};

} // namespace axiom::sql::spark
