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

#include <array>

#include <fmt/core.h>
#include <gtest/gtest.h>

#include "axiom/optimizer/v2/AlgebraicProperties.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

using JoinType = velox::core::JoinType;

constexpr std::array<JoinType, 3> kSupportedTypes{
    JoinType::kInner,
    JoinType::kLeft,
    JoinType::kFull,
};

std::string pairTrace(JoinType child, JoinType parent) {
  return fmt::format(
      "(child={}, parent={})",
      velox::core::JoinTypeName::toName(child),
      velox::core::JoinTypeName::toName(parent));
}

// Verifies every cell of the (child, parent) table.
TEST(AlgebraicPropertiesTest, table) {
  auto expect = [](JoinType child,
                   JoinType parent,
                   bool associative,
                   bool leftAsscom,
                   bool rightAsscom) {
    SCOPED_TRACE(pairTrace(child, parent));
    const auto actual = AlgebraicProperties::derive(child, parent);
    EXPECT_EQ(actual.associative, associative);
    EXPECT_EQ(actual.leftAsscom, leftAsscom);
    EXPECT_EQ(actual.rightAsscom, rightAsscom);
  };

  // child = kInner
  expect(JoinType::kInner, JoinType::kInner, true, true, true);
  expect(JoinType::kInner, JoinType::kLeft, true, true, false);
  expect(JoinType::kInner, JoinType::kFull, false, false, false);

  // child = kLeft
  expect(JoinType::kLeft, JoinType::kInner, false, true, false);
  expect(JoinType::kLeft, JoinType::kLeft, false, true, false);
  expect(JoinType::kLeft, JoinType::kFull, false, false, false);

  // child = kFull
  expect(JoinType::kFull, JoinType::kInner, false, false, false);
  expect(JoinType::kFull, JoinType::kLeft, false, false, false);
  expect(JoinType::kFull, JoinType::kFull, false, false, false);

  // child = semi/anti
  expect(JoinType::kLeftSemiFilter, JoinType::kInner, false, true, false);
  expect(JoinType::kAnti, JoinType::kInner, false, true, false);
  expect(JoinType::kLeftSemiFilter, JoinType::kLeft, false, true, false);
  expect(JoinType::kAnti, JoinType::kLeft, false, true, false);
  expect(JoinType::kLeftSemiFilter, JoinType::kFull, false, false, false);
  expect(JoinType::kAnti, JoinType::kFull, false, false, false);

  // parent = semi/anti
  expect(JoinType::kInner, JoinType::kLeftSemiFilter, true, true, false);
  expect(JoinType::kInner, JoinType::kAnti, true, true, false);
  expect(JoinType::kLeft, JoinType::kLeftSemiFilter, false, true, false);
  expect(JoinType::kLeft, JoinType::kAnti, false, true, false);
  expect(JoinType::kFull, JoinType::kLeftSemiFilter, false, false, false);
  expect(JoinType::kFull, JoinType::kAnti, false, false, false);

  // semi under semi
  expect(
      JoinType::kLeftSemiFilter, JoinType::kLeftSemiFilter, false, true, false);
  expect(JoinType::kAnti, JoinType::kAnti, false, true, false);
  expect(JoinType::kLeftSemiFilter, JoinType::kAnti, false, true, false);
  expect(JoinType::kAnti, JoinType::kLeftSemiFilter, false, true, false);

  // kLeftSemiProject reorders like kLeftSemiFilter.
  expect(JoinType::kLeftSemiProject, JoinType::kInner, false, true, false);
}

// For a commutative child, associativity implies left-asscom (and
// symmetrically, a commutative parent implies right-asscom). Only
// `kInner` and `kFull` are commutative in this subset.
TEST(AlgebraicPropertiesTest, asscomImpliedByCommAndAssoc) {
  auto isCommutative = [](JoinType type) {
    return type == JoinType::kInner || type == JoinType::kFull;
  };

  for (JoinType child : kSupportedTypes) {
    for (JoinType parent : kSupportedTypes) {
      SCOPED_TRACE(pairTrace(child, parent));
      const auto props = AlgebraicProperties::derive(child, parent);
      if (isCommutative(child) && props.associative) {
        EXPECT_TRUE(props.leftAsscom);
      }
      if (isCommutative(parent) && props.associative) {
        EXPECT_TRUE(props.rightAsscom);
      }
    }
  }
}

// `leftAsscom` and `rightAsscom` are symmetric under argument swap.
TEST(AlgebraicPropertiesTest, asscomSymmetry) {
  for (JoinType lhs : kSupportedTypes) {
    for (JoinType rhs : kSupportedTypes) {
      SCOPED_TRACE(pairTrace(lhs, rhs));
      const auto forward = AlgebraicProperties::derive(lhs, rhs);
      const auto swapped = AlgebraicProperties::derive(rhs, lhs);
      EXPECT_EQ(forward.leftAsscom, swapped.leftAsscom);
      EXPECT_EQ(forward.rightAsscom, swapped.rightAsscom);
    }
  }
}

TEST(AlgebraicPropertiesTest, unsupportedJoinTypeThrows) {
  // kRight and kRightSemi* must be normalized to their left forms +
  // operand swap by callers.
  VELOX_ASSERT_THROW(
      AlgebraicProperties::derive(JoinType::kRight, JoinType::kInner),
      "callers must normalize");
  VELOX_ASSERT_THROW(
      AlgebraicProperties::derive(JoinType::kRightSemiFilter, JoinType::kInner),
      "callers must normalize");
  // Counting variants stay opaque leaves and are not reorderable.
  VELOX_ASSERT_THROW(
      AlgebraicProperties::derive(JoinType::kCountingAnti, JoinType::kInner),
      "Algebraic properties not defined for join type");
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
