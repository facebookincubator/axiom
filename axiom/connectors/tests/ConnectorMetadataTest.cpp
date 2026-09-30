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

#include "axiom/connectors/ConnectorMetadata.h"

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"

namespace facebook::axiom::connector {
namespace {

TEST(DeleteInputTest, computedColumnRequiresName) {
  VELOX_ASSERT_THROW(
      DeleteInput(
          {{"",
            std::make_shared<velox::core::FieldAccessTypedExpr>(
                velox::BIGINT(), "id")}},
          {},
          {},
          {}),
      "Computed delete column requires a name");
}

TEST(DeleteInputTest, computedColumnRequiresExpression) {
  VELOX_ASSERT_THROW(
      DeleteInput({{"route", nullptr}}, {}, {}, {}),
      "Computed delete column requires an expression: route");
}

TEST(DeleteInputTest, shuffleKeyRequiresName) {
  VELOX_ASSERT_THROW(
      DeleteInput({}, {""}, {}, {}), "Delete shuffle key requires a name");
}

TEST(DeleteInputTest, sortKeyRequiresName) {
  VELOX_ASSERT_THROW(
      DeleteInput(
          {}, {}, {""}, {{/*isAscending=*/true, /*isNullsFirst=*/false}}),
      "Delete sort key requires a name");
}

TEST(DeleteInputTest, sortKeysAndOrdersAreOneToOne) {
  VELOX_ASSERT_THROW(
      DeleteInput({}, {}, {"row_number"}, {}),
      "Delete sort keys and sort orders must be one-to-one");
}

} // namespace
} // namespace facebook::axiom::connector
