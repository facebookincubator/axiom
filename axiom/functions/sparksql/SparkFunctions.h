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

#include "axiom/optimizer/FunctionRegistry.h"

namespace facebook::axiom::functions::sparksql {

/// Spark function semantics for the Axiom optimizer.
///
/// Matches a Velox runtime that registers the Spark SQL scalar functions, and
/// the Spark SQL aggregate and window functions over the Presto ones, with no
/// prefix:
///
///   velox::functions::sparksql::registerFunctions("");
///   velox::aggregate::prestosql::registerAllAggregateFunctions();
///   velox::functions::aggregate::sparksql::registerAggregateFunctions("");
///   velox::window::prestosql::registerAllWindowFunctions();
///   velox::functions::window::sparksql::registerWindowFunctions("");
///
/// Velox has no Spark count, bool_or, arbitrary, count_if or approx_distinct
/// aggregates; those names are the Presto aggregates.
///
/// Example:
///   auto registry = SparkFunctions::makeRegistry();
class SparkFunctions {
 public:
  /// Registers Spark function semantics in 'registry'.
  static void registerAll(optimizer::FunctionRegistry& registry);

  /// Shortcut for creating a registry and calling registerAll() on it.
  static std::shared_ptr<optimizer::FunctionRegistry> makeRegistry();
};

} // namespace facebook::axiom::functions::sparksql
