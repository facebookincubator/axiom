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

#include "axiom/optimizer/v2/AlgebraicProperties.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

using JoinType = velox::core::JoinType;

constexpr int kKindCount = 5;

// Compact ordinal for indexing the algebraic-properties table.
// kInner = 0, kLeft = 1, kFull = 2, semijoin (⋉) = 3, antijoin (▷)
// = 4. The paper's LOP is left-oriented; callers normalize kRight
// and kRightSemi* by swapping operands and looking up the left
// form. kLeftSemiFilter and kLeftSemiProject share the ⋉ row
// (identical reorderability); kAnti maps to ▷.
int kindIndex(JoinType type) {
  switch (type) {
    case JoinType::kInner:
      return 0;
    case JoinType::kLeft:
      return 1;
    case JoinType::kFull:
      return 2;
    case JoinType::kLeftSemiFilter:
    case JoinType::kLeftSemiProject:
      return 3;
    case JoinType::kAnti:
      return 4;
    default:
      VELOX_NYI(
          "Algebraic properties not defined for join type: {}; "
          "callers must normalize kRight/kRightSemi* to their left "
          "forms before lookup.",
          velox::core::JoinTypeName::toName(type));
  }
}

// Indexed [child][parent]. Conditional entries collapsed to
// false; see header.
constexpr AlgebraicProperties kTable[kKindCount][kKindCount] = {
    // child = kInner
    {
        // parent = kInner
        {.associative = true, .leftAsscom = true, .rightAsscom = true},
        // parent = kLeft
        {.associative = true, .leftAsscom = true, .rightAsscom = false},
        // parent = kFull
        {.associative = false, .leftAsscom = false, .rightAsscom = false},
        // parent = ⋉
        {.associative = true, .leftAsscom = true, .rightAsscom = false},
        // parent = ▷
        {.associative = true, .leftAsscom = true, .rightAsscom = false},
    },
    // child = kLeft
    {
        // parent = kInner
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
        // parent = kLeft
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
        // parent = kFull
        {.associative = false, .leftAsscom = false, .rightAsscom = false},
        // parent = ⋉
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
        // parent = ▷
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
    },
    // child = kFull
    {
        // parent = kInner
        {.associative = false, .leftAsscom = false, .rightAsscom = false},
        // parent = kLeft
        {.associative = false, .leftAsscom = false, .rightAsscom = false},
        // parent = kFull
        {.associative = false, .leftAsscom = false, .rightAsscom = false},
        // parent = ⋉
        {.associative = false, .leftAsscom = false, .rightAsscom = false},
        // parent = ▷
        {.associative = false, .leftAsscom = false, .rightAsscom = false},
    },
    // child = ⋉
    {
        // parent = kInner
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
        // parent = kLeft
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
        // parent = kFull
        {.associative = false, .leftAsscom = false, .rightAsscom = false},
        // parent = ⋉
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
        // parent = ▷
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
    },
    // child = ▷
    {
        // parent = kInner
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
        // parent = kLeft
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
        // parent = kFull
        {.associative = false, .leftAsscom = false, .rightAsscom = false},
        // parent = ⋉
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
        // parent = ▷
        {.associative = false, .leftAsscom = true, .rightAsscom = false},
    },
};

} // namespace

AlgebraicProperties AlgebraicProperties::derive(
    JoinType child,
    JoinType parent) {
  return kTable[kindIndex(child)][kindIndex(parent)];
}

} // namespace facebook::axiom::optimizer::v2
