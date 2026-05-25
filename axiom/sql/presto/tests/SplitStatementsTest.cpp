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

#include "axiom/sql/presto/PrestoSqlError.h"
#include "axiom/sql/presto/tests/ExpectPrestoSqlError.h"
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"

namespace axiom::sql::presto::test {
namespace {

class SplitStatementsTest : public PrestoParserTestBase {};

TEST_F(SplitStatementsTest, basicSplit) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1; select 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(SplitStatementsTest, withTrailingSemicolon) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1; select 2;");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(SplitStatementsTest, withWhitespace) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("  select 1  ;  \n  select 2  ;  \n  select 3  ");
  ASSERT_EQ(3, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
  ASSERT_TRUE(statements[2]->isSelect());
}

TEST_F(SplitStatementsTest, withComments) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "-- First query\nselect 1;\n-- Second query\nselect 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(SplitStatementsTest, withMultiByteUtf8Char) {
  // Multi-byte UTF-8 characters in comments, string literals, or
  // between statements must not break statement splitting.
  auto parser = makeParser();

  // 2-byte UTF-8 (U+00F1) in a leading comment.
  {
    auto statements = parser.parseMultiple("-- ñ\nSELECT 1");
    ASSERT_EQ(1, statements.size());
    ASSERT_TRUE(statements[0]->isSelect());
  }

  // 2-byte UTF-8 in a string literal.
  {
    auto statements = parser.parseMultiple("SELECT 'ñ'; SELECT 1");
    ASSERT_EQ(2, statements.size());
    ASSERT_TRUE(statements[0]->isSelect());
    ASSERT_TRUE(statements[1]->isSelect());
  }

  // 3-byte UTF-8 (U+20AC '€') in a comment between statements.
  {
    auto statements = parser.parseMultiple("SELECT 1; -- €\nSELECT 2");
    ASSERT_EQ(2, statements.size());
    ASSERT_TRUE(statements[0]->isSelect());
    ASSERT_TRUE(statements[1]->isSelect());
  }

  // Multi-byte chars across both a comment and two string literals.
  {
    auto statements =
        parser.parseMultiple("-- ñ ñ ñ\nSELECT 'ä' AS x; SELECT 'ö' AS y");
    ASSERT_EQ(2, statements.size());
    ASSERT_TRUE(statements[0]->isSelect());
    ASSERT_TRUE(statements[1]->isSelect());
  }

  // 4-byte UTF-8 (U+1F389 '🎉') in a comment between statements.
  {
    auto statements = parser.parseMultiple("SELECT 1; -- 🎉\nSELECT 2");
    ASSERT_EQ(2, statements.size());
    ASSERT_TRUE(statements[0]->isSelect());
    ASSERT_TRUE(statements[1]->isSelect());
  }
}

TEST_F(SplitStatementsTest, withBlockComments) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("/* First */ select 1; /* Second */ select 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(SplitStatementsTest, withSingleQuotes) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("select 'hello; world'; select 'foo''bar; baz'");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(SplitStatementsTest, withDoubleQuotes) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select 1 as \"col;name\"; select 2 as \"foo\"\"bar; baz\"");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(SplitStatementsTest, emptyAndWhitespaceOnly) {
  // Empty input and whitespace/separator-only input produce no statements.
  auto parser = makeParser();

  ASSERT_EQ(0, parser.parseMultiple("").size());
  ASSERT_EQ(0, parser.parseMultiple("   \n\t  ").size());
  ASSERT_EQ(0, parser.parseMultiple(";;;").size());
  ASSERT_EQ(0, parser.parseMultiple(" ; ; ; ").size());
}

TEST_F(SplitStatementsTest, malformedInput) {
  // Malformed SQL (unterminated string or block comment) must surface as
  // a syntax error, not a hang or silently dropped content.
  auto parser = makeParser();

  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parser.parseMultiple("select '"),
      "Syntax error at 0:7 near ''': mismatched input '''");
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parser.parseMultiple("/* never closed"),
      "Syntax error at 0:0 near '/': mismatched input '/'");
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parser.parseMultiple("select 1; /* never closed"),
      "near '/': mismatched input '/'");
}

TEST_F(SplitStatementsTest, mixedStatements) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select * from nation; "
      "select n_name from nation where n_nationkey = 1; "
      "select 42");
  ASSERT_EQ(3, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
  ASSERT_TRUE(statements[2]->isSelect());
}

TEST_F(SplitStatementsTest, singleStatement) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1");
  ASSERT_EQ(1, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
}

TEST_F(SplitStatementsTest, complexQuery) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select n_nationkey, n_name "
      "from nation "
      "where n_regionkey = 1 "
      "order by n_name; "
      "select count(*) from nation");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

} // namespace
} // namespace axiom::sql::presto::test
