// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>

#include "optimizer/SchemaUtils.h" //@manual

namespace facebook::velox::optimizer {
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
} // namespace facebook::velox::optimizer
