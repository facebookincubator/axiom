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

#include <folly/container/F14Set.h>

namespace facebook::velox::optimizer {

/// Pointer to an arena allocated interned copy of a null terminated string.
/// Used for identifiers. Allows comparing strings by comparing pointers.
using Name = const char*;

/// This class should be created and access via queryCtx().
/// It's needed for optimizer to have access to some hardcoded names.
/// Right now it's names of functions with presto semantics.
///
/// Some members here are functions another variables: the reason is simple,
/// it's function when it's accessed rarely in some specific code that can be
/// disabled, e.g. join sample.
struct BuiltinNames {
  BuiltinNames();

  BuiltinNames(BuiltinNames&&) = delete;
  BuiltinNames(const BuiltinNames&) = delete;
  BuiltinNames& operator=(BuiltinNames&&) = delete;
  BuiltinNames& operator=(const BuiltinNames&) = delete;

  // Returns names of "opposite" function if it's exists, e.g. gt for lt.
  // Opposite means if arguments will be swapped it will be same semantics.
  // If such name doesn't exist returns it's own input, e.g. eq for eq.
  Name reverse(Name op) const;

  /// Returns true for function names that are commutative or
  /// that have oppostite function.
  bool isCanonicalizable(Name name) const {
    return canonicalizable_.contains(name);
  }

  /// These function names used only for FunctionRegistry,
  /// this is why they're not "Name" type but "std::string_view".
  static constexpr std::string_view kTransformValues = "transform_values";
  static constexpr std::string_view kTransform = "transform";
  static constexpr std::string_view kZip = "zip";
  static constexpr std::string_view kRowConstructor = "row_constructor";

  /// These function names used only for JoinSample,
  /// this is why they're not variables but functions.
  Name crc32() const;
  Name bitwiseAnd() const;
  Name bitwiseRightShift() const;
  Name bitwiseXor() const;
  Name hash() const;
  Name mod() const;

  /// These function names used for special subfields access optimizations.
  Name cardinality;
  Name subscript;
  Name elementAt;

  /// These function names used for isCanonicalizable checks.
  Name eq;
  Name neq;
  Name lt;
  Name lte;
  Name gt;
  Name gte;
  Name plus;
  Name multiply;

  /// These function names used for isCanonicalizable checks and
  /// also it's special form callable function names.
  Name _and;
  Name _or;
  Name _cast;
  Name _tryCast;
  Name _try;
  Name _coalesce;
  Name _if;
  Name _switch;
  Name _in;

 private:
  const folly::F14FastSet<Name> canonicalizable_;
};

} // namespace facebook::velox::optimizer
