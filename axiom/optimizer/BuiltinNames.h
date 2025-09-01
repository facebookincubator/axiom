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

struct BuiltinNames {
  BuiltinNames();

  BuiltinNames(BuiltinNames&&) = delete;
  BuiltinNames(const BuiltinNames&) = delete;
  BuiltinNames& operator=(BuiltinNames&&) = delete;
  BuiltinNames& operator=(const BuiltinNames&) = delete;

  Name reverse(Name op) const;

  bool isCanonicalizable(Name name) const {
    return canonicalizable.contains(name);
  }

  // Used for function registry
  static constexpr std::string_view kTransformValues = "transform_values";
  static constexpr std::string_view kTransform = "transform";
  static constexpr std::string_view kZip = "zip";
  static constexpr std::string_view kRowConstructor = "row_constructor";

  // Used for join sample
  Name crc32() const;
  Name bitwiseAnd() const;
  Name bitwiseRightShift() const;
  Name bitwiseXor() const;
  Name hash() const;
  Name mod() const;

  // Used for subfields
  Name cardinality;
  Name subscript;
  Name elementAt;

  // Used for canonicalization
  Name eq;
  Name neq;
  Name lt;
  Name lte;
  Name gt;
  Name gte;
  Name plus;
  Name multiply;

  Name _and;
  Name _or;
  Name _cast;
  Name _tryCast;
  Name _try;
  Name _coalesce;
  Name _if;
  Name _switch;
  Name _in;

  folly::F14FastSet<Name> canonicalizable;
};

} // namespace facebook::velox::optimizer
