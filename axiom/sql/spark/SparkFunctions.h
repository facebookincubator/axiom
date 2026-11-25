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
#include <vector>

#include "axiom/logical_plan/ExprApi.h"

namespace axiom::sql::spark {

/// Builds Axiom calls for Spark function calls, with Spark's semantics. Spark
/// Connect plans and Spark SQL both name operators and functions as Spark
/// does; this maps them to the Velox functions registered for Spark.
///
///   // a + 1 is add(a, 1).
///   auto expr = SparkFunctions::call("+", lp::Col("a"), lp::Lit(1));
class SparkFunctions {
 public:
  /// Returns a call of the Spark function or operator 'name'. Operators are
  /// named as Spark names them: +, ==, <=>, negative, and so on.
  static facebook::axiom::logical_plan::ExprApi call(
      std::string_view name,
      std::vector<facebook::axiom::logical_plan::ExprApi> args);

  /// Same as above, with the arguments listed: call("+", a, b).
  template <typename... T>
  static facebook::axiom::logical_plan::ExprApi call(
      std::string_view name,
      T... args) {
    return call(
        name,
        std::vector<facebook::axiom::logical_plan::ExprApi>{
            std::forward<T>(args)...});
  }
};

} // namespace axiom::sql::spark
