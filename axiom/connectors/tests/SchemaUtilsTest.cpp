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

#include <gtest/gtest.h>

#include "axiom/connectors/SchemaUtils.h"

namespace facebook::axiom::connector {
namespace {

TEST(SchemaUtilsTest, basic) {
  {
    TableNameParser parser("table");
    EXPECT_TRUE(parser.valid());
    EXPECT_FALSE(parser.catalog().has_value());
    EXPECT_FALSE(parser.schema().has_value());
    EXPECT_EQ(parser.table(), "table");
  }

  {
    TableNameParser parser("schema.table");
    EXPECT_TRUE(parser.valid());
    EXPECT_FALSE(parser.catalog().has_value());
    EXPECT_EQ(parser.schema(), "schema");
    EXPECT_EQ(parser.table(), "table");
  }

  {
    TableNameParser parser("catalog.schema.table");
    EXPECT_TRUE(parser.valid());
    EXPECT_EQ(parser.catalog(), "catalog");
    EXPECT_EQ(parser.schema(), "schema");
    EXPECT_EQ(parser.table(), "table");
  }
}

TEST(SchemaUtilsTest, invalidInput) {
  {
    TableNameParser parser("");
    EXPECT_FALSE(parser.valid());
  }

  {
    TableNameParser parser("a.b.c.d");
    EXPECT_FALSE(parser.valid());
  }

  {
    TableNameParser parser("...");
    EXPECT_FALSE(parser.valid());
  }

  {
    TableNameParser parser("catalog..table");
    EXPECT_FALSE(parser.valid());
  }

  {
    TableNameParser parser(".schema.table");
    EXPECT_FALSE(parser.valid());
  }

  {
    TableNameParser parser("catalog.schema.");
    EXPECT_FALSE(parser.valid());
  }
}

} // namespace
} // namespace facebook::axiom::connector
