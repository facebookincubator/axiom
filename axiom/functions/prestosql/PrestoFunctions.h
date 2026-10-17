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
#include <string_view>

#include "axiom/optimizer/FunctionRegistry.h"

namespace facebook::axiom::functions::prestosql {

/// Presto function semantics for the Axiom optimizer.
///
/// Example:
///   auto registry = PrestoFunctions::makeRegistry();
class PrestoFunctions {
 public:
  /// Registers Presto function semantics in 'registry', each function name
  /// prefixed with 'prefix'. 'prefix' must match the prefix the Presto
  /// functions were registered with in Velox (e.g.
  /// velox::functions::prestosql::registerAllScalarFunctions(prefix)), so the
  /// names the optimizer recognizes are the names Velox executes. Special forms
  /// are registered without the prefix.
  static void registerAll(
      optimizer::FunctionRegistry& registry,
      std::string_view prefix = "");

  /// Shortcut for creating a registry and calling registerAll() on it with no
  /// prefix.
  static std::shared_ptr<optimizer::FunctionRegistry> makeRegistry();
};

} // namespace facebook::axiom::functions::prestosql
