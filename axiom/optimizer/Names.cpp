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

#include "axiom/optimizer/Names.h"

namespace facebook::velox::optimizer {

// The names of known functions and special forms are declared
// statically in one translation unit to ensure that all references
// end up pointing to the same string. In this way the constants like kEq
// et al can be compared by pointer to toName("eq") etc., as long as
// the set of interned names in QueryGraphContext is initialized
// with this set.

const char* Names::kEq = "eq";
const char* Names::kLt = "lt";
const char* Names::kLte = "lte";
const char* Names::kGt = "gt";
const char* Names::kGte = "gte";
const char* Names::kPlus = "plus";
const char* Names::kMultiply = "multiply";
const char* Names::kAnd = "and";
const char* Names::kOr = "or";
const char* Names::kCast = "cast";
const char* Names::kTryCast = "trycast";
const char* Names::kTry = "try";
const char* Names::kCoalesce = "coalesce";
const char* Names::kIf = "if";
const char* Names::kSwitch = "switch";
const char* Names::kIn = "in";
const char* Names::kSubscript = "subscript";

std::unordered_set<std::string_view> Names::builtinNames() {
  static std::unordered_set<std::string_view> names = {
      Names::kEq,
      Names::kLt,
      Names::kLte,
      Names::kGt,
      Names::kGte,
      Names::kPlus,
      Names::kMultiply,
      Names::kAnd,
      Names::kOr,
      Names::kCast,
      Names::kTryCast,
      Names::kTry,
      Names::kCoalesce,
      Names::kIf,
      Names::kSwitch,
      Names::kIn,
      Names::kSubscript,
  };

  return names;
}

bool Names::isCanonicalizable(const char* name) {
  return name == kEq || name == kLt || name == kLte || name == kGt ||
      name == kGte || name == kPlus || name == kMultiply || name == kAnd ||
      name == kOr;
}

const char* Names::reverse(const char* name) {
  if (name == kLt) {
    return kGt;
  }
  if (name == kLte) {
    return kGte;
  }
  if (name == kGt) {
    return kLt;
  }
  if (name == kGte) {
    return kLte;
  }
  return name;
}

} // namespace facebook::velox::optimizer
