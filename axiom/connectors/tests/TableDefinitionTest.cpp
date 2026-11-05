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

#include "axiom/connectors/TableDefinition.h"

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox;

namespace facebook::axiom::connector {
namespace {

// Each part of a definition fails against a support that does not include it.
TEST(TableDefinitionTest, check) {
  const TableDefinitionSupport none;
  const auto definition = [](ColumnDefinition column) {
    return TableDefinition{.columns = {std::move(column)}};
  };

  none.check(definition({.name = "a", .type = BIGINT()}));

  VELOX_ASSERT_THROW(
      none.check(
          definition({.name = "a", .type = BIGINT(), .nullable = false})),
      "NOT NULL is not supported by this connector: a");
  VELOX_ASSERT_THROW(
      none.check(definition({.name = "a", .type = BIGINT(), .comment = "x"})),
      "Column comments are not supported by this connector: a");
  VELOX_ASSERT_THROW(
      none.check(definition(
          {.name = "a", .type = BIGINT(), .properties = {{"x", Variant(1)}}})),
      "Column properties are not supported by this connector: a");

  auto withComment = definition({.name = "a", .type = BIGINT()});
  withComment.comment = "x";
  VELOX_ASSERT_THROW(
      none.check(withComment),
      "Table comments are not supported by this connector");

  auto withKey = definition({.name = "a", .type = BIGINT()});
  withKey.primaryKey = {"a"};
  VELOX_ASSERT_THROW(
      none.check(withKey), "PRIMARY KEY is not supported by this connector");

  auto withIndex = definition({.name = "a", .type = ARRAY(REAL())});
  withIndex.vectorIndexes = {{.name = "i", .column = "a"}};
  VELOX_ASSERT_THROW(
      none.check(withIndex),
      "Vector indexes are not supported by this connector: i");
  VELOX_ASSERT_THROW(
      none.check(withIndex.vectorIndexes.front()),
      "Vector indexes are not supported by this connector: i");

  const TableDefinitionSupport all{
      .nullability = true,
      .columnComments = true,
      .columnProperties = true,
      .tableComment = true,
      .primaryKey = true,
      .vectorIndexes = true,
  };
  all.check(withIndex);
  all.check(
      TableDefinition{
          .columns = {{
              .name = "a",
              .type = BIGINT(),
              .nullable = false,
              .comment = "x",
              .properties = {{"x", Variant(1)}},
          }},
          .comment = "x",
          .primaryKey = {"a"},
      });
}

} // namespace
} // namespace facebook::axiom::connector
