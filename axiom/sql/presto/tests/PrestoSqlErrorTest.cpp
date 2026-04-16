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
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"

namespace axiom::sql::presto::test {

namespace {

class PrestoSqlErrorTest : public PrestoParserTestBase {};

auto hasLocation(size_t line, size_t column, const std::string& token) {
  return testing::AllOf(
      testing::Property(&PrestoSqlError::line, line),
      testing::Property(&PrestoSqlError::column, column),
      testing::Property(&PrestoSqlError::token, token));
}

auto hasLineAndColumn(size_t line, size_t column) {
  return testing::AllOf(
      testing::Property(&PrestoSqlError::line, line),
      testing::Property(&PrestoSqlError::column, column));
}

TEST_F(PrestoSqlErrorTest, basic) {
  auto parser = makeParser();
  // "SELECT * FROM" is missing the table name — error at the EOF token.
  EXPECT_THAT(
      [&]() { parser.parse("SELECT * FROM"); },
      testing::Throws<PrestoSqlError>(hasLocation(0, 13, "<EOF>")));
}

TEST_F(PrestoSqlErrorTest, multiline) {
  auto parser = makeParser();
  // Multiline SQL — error on the second line at column 5 (WHERE at end).
  EXPECT_THAT(
      [&]() {
        parser.parse(
            "SELECT * FROM nation\n"
            "WHERE");
      },
      testing::Throws<PrestoSqlError>(hasLocation(1, 5, "<EOF>")));
}

TEST_F(PrestoSqlErrorTest, midToken) {
  auto parser = makeParser();
  // Extra closing paren — error on the ')' token at position 30.
  EXPECT_THAT(
      [&]() { parser.parse("SELECT * FROM (VALUES 1, 2, 3)) blah..."); },
      testing::Throws<PrestoSqlError>(hasLocation(0, 30, ")")));
}

TEST_F(PrestoSqlErrorTest, unrecognizedChar) {
  auto parser = makeParser();
  // A control character (0x01) is not recognized by the Presto SQL lexer.
  // Error location should still be populated.
  EXPECT_THAT(
      [&]() { parser.parse(std::string("SELECT \x01")); },
      testing::Throws<PrestoSqlError>(
          testing::Property(&PrestoSqlError::line, 0)));
}

TEST_F(PrestoSqlErrorTest, multiByteUtf8) {
  auto parser = makeParser();
  // 'é' is 2 bytes in UTF-8 but 1 codepoint. PrestoSqlError reports
  // ANTLR's character-based column, not byte offsets.
  EXPECT_THAT(
      [&]() { parser.parse("SELECT 'é' AS name FORM"); },
      testing::Throws<PrestoSqlError>(hasLocation(0, 19, "FORM")));
}

TEST_F(PrestoSqlErrorTest, bom) {
  auto parser = makeParser();
  // UTF-8 BOM (3 bytes: EF BB BF) followed by "SELECT * FROM".
  // ANTLR strips the BOM before parsing; line/column are post-BOM.
  std::string sql =
      "\xEF\xBB\xBF"
      "SELECT * FROM";
  EXPECT_THAT(
      [&]() { parser.parse(sql); },
      testing::Throws<PrestoSqlError>(hasLocation(0, 13, "<EOF>")));
}

TEST_F(PrestoSqlErrorTest, multipleStatementsLineAdjustment) {
  auto parser = makeParser();
  // Error in second statement should report line relative to original SQL.
  // "select 1;\n" has one newline, so line 0 in the second statement becomes
  // line 1 in the original SQL.
  EXPECT_THAT(
      [&]() { parser.parseMultiple("select 1;\nSELECT * FROM"); },
      testing::Throws<PrestoSqlError>(hasLocation(1, 13, "<EOF>")));
}

TEST_F(PrestoSqlErrorTest, multipleStatementsMultipleNewlines) {
  auto parser = makeParser();
  // Error in the third statement, preceded by two newlines.
  EXPECT_THAT(
      [&]() { parser.parseMultiple("select 1;\nselect 2;\nSELECT * FROM"); },
      testing::Throws<PrestoSqlError>(hasLocation(2, 13, "<EOF>")));
}

TEST_F(PrestoSqlErrorTest, multipleStatementsColumnAdjustment) {
  auto parser = makeParser();
  // Second statement has leading whitespace on the same line as the semicolon.
  // splitStatements trims it, so ANTLR sees column 0. But in the original SQL,
  // "SELETC" starts at column 12 on line 0.
  EXPECT_THAT(
      [&]() { parser.parseMultiple("select 1;   SELETC 2"); },
      testing::Throws<PrestoSqlError>(hasLineAndColumn(0, 12)));
}

TEST_F(PrestoSqlErrorTest, multipleStatementsFirstStatement) {
  auto parser = makeParser();
  // Error in the first statement — no line/column adjustment needed.
  EXPECT_THAT(
      [&]() { parser.parseMultiple("SELETC 1; select 2"); },
      testing::Throws<PrestoSqlError>(hasLineAndColumn(0, 0)));
}

TEST_F(PrestoSqlErrorTest, multipleStatementsMultiLineSubStatement) {
  auto parser = makeParser();
  // Error on line 1 of the second sub-statement = line 2 in original SQL.
  // Column should NOT be adjusted since it's not line 0 of the sub-statement.
  EXPECT_THAT(
      [&]() { parser.parseMultiple("select 1;\nSELECT * FROM nation\nWHERE"); },
      testing::Throws<PrestoSqlError>(hasLineAndColumn(2, 5)));
}

TEST_F(PrestoSqlErrorTest, multipleStatementsColumnOnNewLine) {
  auto parser = makeParser();
  // Second statement has leading whitespace on a new line.
  // "SELETC" starts at column 2 on line 1.
  EXPECT_THAT(
      [&]() { parser.parseMultiple("select 1;\n  SELETC 2"); },
      testing::Throws<PrestoSqlError>(hasLineAndColumn(1, 2)));
}

TEST_F(PrestoSqlErrorTest, messageForInvalidDecimalTypeArgs) {
  auto parser = makeParser();
  EXPECT_THAT(
      [&]() { parser.parse("CREATE TABLE t (price DECIMAL('abc', 'xyz'))"); },
      testing::Throws<PrestoSqlError>(testing::AllOf(
          testing::Property(&PrestoSqlError::line, 0),
          testing::Property(&PrestoSqlError::column, 30),
          testing::Property(
              &PrestoSqlError::what,
              testing::HasSubstr("mismatched input ''abc''")))));
}

} // namespace
} // namespace axiom::sql::presto::test
