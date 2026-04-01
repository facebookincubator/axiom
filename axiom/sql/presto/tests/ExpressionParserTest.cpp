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

#include <fmt/format.h>
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/types/QDigestRegistration.h"
#include "velox/functions/prestosql/types/QDigestType.h"
#include "velox/functions/prestosql/types/TDigestRegistration.h"
#include "velox/functions/prestosql/types/TDigestType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

namespace {

class ExpressionParserTest : public PrestoParserTestBase {
 protected:
  lp::ExprPtr parseExpr(std::string_view sql) {
    return makeParser().parseExpression(sql, true);
  }

  lp::ExprPtr parseStrictExpr(std::string_view sql) {
    return makeStrictParser().parseExpression(sql, true);
  }

  // Parses 'SELECT <expr> FROM nation' and verifies the project expression
  // matches expectedExpr via toString().
  void testNationExpr(std::string_view expr, std::string_view expectedExpr) {
    SCOPED_TRACE(expr);
    testSelect(
        fmt::format("SELECT {} FROM nation", expr),
        matchScan()
            .project([expectedExpr](const lp::LogicalPlanNodePtr& node) {
              auto& project = *node->as<lp::ProjectNode>();
              ASSERT_EQ(1, project.expressions().size());
              EXPECT_EQ(
                  std::string(expectedExpr),
                  project.expressionAt(0)->toString());
            })
            .output());
  }

  // Parses a decimal literal and verifies its value and type.
  template <typename T>
  void testDecimal(std::string_view sql, T value, const TypePtr& type) {
    SCOPED_TRACE(sql);

    auto expr = parseExpr(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), type->toString());

    auto v = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(v->isNull());
    ASSERT_EQ(v->value<T>(), value);
  }
};

TEST_F(ExpressionParserTest, types) {
  // Verifies that cast and try_cast produce the expected type and are preserved
  // as kCast and kTryCast special forms respectively.
  auto test = [&](std::string_view castArgs, const TypePtr& expectedType) {
    SCOPED_TRACE(castArgs);

    auto castExpr = parseExpr(fmt::format("cast({})", castArgs));
    VELOX_EXPECT_EQ_TYPES(castExpr->type(), expectedType);
    ASSERT_TRUE(castExpr->isSpecialForm());
    ASSERT_EQ(
        castExpr->as<lp::SpecialFormExpr>()->form(), lp::SpecialForm::kCast);

    auto tryCastExpr = parseExpr(fmt::format("try_cast({})", castArgs));
    VELOX_EXPECT_EQ_TYPES(tryCastExpr->type(), expectedType);
    ASSERT_TRUE(tryCastExpr->isSpecialForm());
    ASSERT_EQ(
        tryCastExpr->as<lp::SpecialFormExpr>()->form(),
        lp::SpecialForm::kTryCast);
  };

  test("null as boolean", BOOLEAN());
  test("'1' as tinyint", TINYINT());
  test("null as smallint", SMALLINT());
  test("'2' as int", INTEGER());
  test("null as integer", INTEGER());
  test("'3' as bIgInT", BIGINT());
  test("'2020-01-01' as date", DATE());
  test("null as timestamp", TIMESTAMP());
  test("null as decimal(3, 2)", DECIMAL(3, 2));
  test("null as decimal(33, 10)", DECIMAL(33, 10));

  facebook::velox::registerTDigestType();
  facebook::velox::registerQDigestType();

  test("null as tdigest(double)", TDIGEST(DOUBLE()));
  test("null as qdigest(bigint)", QDIGEST(BIGINT()));
  test("null as qdigest(real)", QDIGEST(REAL()));
  test("null as qdigest(double)", QDIGEST(DOUBLE()));

  // CHAR without length parameter resolves to VARCHAR.
  test("null as char", VARCHAR());

  test("null as int array", ARRAY(INTEGER()));
  test("null as varchar array", ARRAY(VARCHAR()));
  test("null as map(integer, real)", MAP(INTEGER(), REAL()));
  test("null as row(int, double)", ROW({INTEGER(), DOUBLE()}));
  test("null as row(a int, b double)", ROW({"a", "b"}, {INTEGER(), DOUBLE()}));
}

// Verifies that VARCHAR(n) wraps the inner cast in substr(expr, 1, n).
TEST_F(ExpressionParserTest, varcharN) {
  auto testTruncation = [&](std::string_view sql, int64_t expectedMaxLength) {
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);
    VELOX_EXPECT_EQ_TYPES(expr->type(), VARCHAR());
    ASSERT_TRUE(expr->isCall());
    auto* call = expr->as<lp::CallExpr>();
    EXPECT_EQ(call->name(), "substr");
    ASSERT_EQ(call->inputs().size(), 3);
    // First argument is the inner cast.
    ASSERT_TRUE(call->inputAt(0)->isSpecialForm());
    // Second argument: start position = 1.
    auto* startLit = call->inputs()[1]->as<lp::ConstantExpr>();
    ASSERT_NE(startLit, nullptr);
    EXPECT_EQ(startLit->value()->value<int64_t>(), 1);
    // Third argument: max length.
    auto* lengthLit = call->inputs()[2]->as<lp::ConstantExpr>();
    ASSERT_NE(lengthLit, nullptr);
    EXPECT_EQ(lengthLit->value()->value<int64_t>(), expectedMaxLength);
  };

  testTruncation("cast(null as varchar(10))", 10);
  testTruncation("cast(null as varchar(255))", 255);
  testTruncation("try_cast(null as varchar(10))", 10);

  // Nested in complex types — length is dropped, no substr wrapper.
  auto testType = [&](std::string_view castArgs, const TypePtr& expectedType) {
    SCOPED_TRACE(castArgs);
    auto expr = parseExpr(fmt::format("cast({})", castArgs));
    VELOX_EXPECT_EQ_TYPES(expr->type(), expectedType);
    // Verify the expression is a plain cast, not wrapped in substr/rpad.
    ASSERT_TRUE(expr->isSpecialForm());
    ASSERT_FALSE(expr->isCall());
  };
  testType("null as array(varchar(10))", ARRAY(VARCHAR()));
  testType("null as array(char(10))", ARRAY(VARCHAR()));
  testType("null as map(varchar(5), integer)", MAP(VARCHAR(), INTEGER()));

  // Zero length is rejected.
  VELOX_ASSERT_THROW(
      parseExpr("cast(null as varchar(0))"), "Length must be positive");
}

// Verifies that CHAR(n) wraps the inner cast in rpad(expr, n, ' ').
TEST_F(ExpressionParserTest, charN) {
  auto testPadding = [&](std::string_view sql, int64_t expectedLength) {
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);
    VELOX_EXPECT_EQ_TYPES(expr->type(), VARCHAR());
    ASSERT_TRUE(expr->isCall());
    auto* call = expr->as<lp::CallExpr>();
    EXPECT_EQ(call->name(), "rpad");
    ASSERT_EQ(call->inputs().size(), 3);
    // First argument is the inner cast.
    ASSERT_TRUE(call->inputAt(0)->isSpecialForm());
    // Second argument: target length.
    auto* lengthLit = call->inputs()[1]->as<lp::ConstantExpr>();
    ASSERT_NE(lengthLit, nullptr);
    EXPECT_EQ(lengthLit->value()->value<int64_t>(), expectedLength);
    // Third argument: pad character = ' '.
    auto* padLit = call->inputs()[2]->as<lp::ConstantExpr>();
    ASSERT_NE(padLit, nullptr);
    EXPECT_EQ(padLit->value()->value<StringView>(), " ");
  };

  testPadding("cast(null as char(20))", 20);
  testPadding("cast(null as char(5))", 5);
  testPadding("try_cast(null as char(20))", 20);

  // Zero length is rejected.
  VELOX_ASSERT_THROW(
      parseExpr("cast(null as char(0))"), "Length must be positive");
}

// Verifies that VARBINARY(n) resolves to unbounded VARBINARY with no wrapper.
TEST_F(ExpressionParserTest, varbinaryN) {
  auto expr = parseExpr("cast(null as varbinary(100))");
  VELOX_EXPECT_EQ_TYPES(expr->type(), VARBINARY());
  ASSERT_TRUE(expr->isSpecialForm());
  ASSERT_FALSE(expr->isCall());

  // Zero length is rejected.
  VELOX_ASSERT_THROW(
      parseExpr("cast(null as varbinary(0))"), "Length must be positive");
}

TEST_F(ExpressionParserTest, intervalDayTime) {
  auto test = [&](std::string_view sql, int64_t expectedSeconds) {
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), INTERVAL_DAY_TIME()->toString());

    auto value = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(value->isNull());
    ASSERT_EQ(value->value<int64_t>(), expectedSeconds * 1'000);
  };

  test("INTERVAL '2' DAY", 2 * 24 * 60 * 60);
  test("INTERVAL '3' HOUR", 3 * 60 * 60);
  test("INTERVAL '4' MINUTE", 4 * 60);
  test("INTERVAL '5' SECOND", 5);

  test("INTERVAL '' DAY", 0);
  test("INTERVAL '0' HOUR", 0);

  test("INTERVAL '-2' DAY", -2 * 24 * 60 * 60);
  test("INTERVAL '-3' HOUR", -3 * 60 * 60);
  test("INTERVAL '-4' MINUTE", -4 * 60);
  test("INTERVAL '-5' SECOND", -5);
}

TEST_F(ExpressionParserTest, decimal) {
  auto testShort =
      [&](std::string_view sql, int64_t value, const TypePtr& type) {
        testDecimal<int64_t>(sql, value, type);
      };

  auto testLong =
      [&](std::string_view sql, std::string_view value, const TypePtr& type) {
        testDecimal<int128_t>(sql, folly::to<int128_t>(value), type);
      };

  // Short decimals.
  testShort("DECIMAL '1.2'", 12, DECIMAL(2, 1));
  testShort("DECIMAL '-1.23'", -123, DECIMAL(3, 2));
  testShort("DECIMAL '+12.3'", 123, DECIMAL(3, 1));
  testShort("DECIMAL '1.2345'", 12345, DECIMAL(5, 4));
  testShort("DECIMAL '12'", 12, DECIMAL(2, 0));
  testShort("DECIMAL '12.'", 12, DECIMAL(2, 0));
  testShort("DECIMAL '.12'", 12, DECIMAL(2, 2));
  testShort("DECIMAL '000001.2'", 12, DECIMAL(2, 1));
  testShort("DECIMAL '-000001.2'", -12, DECIMAL(2, 1));

  // Long decimals.
  testLong(
      "decimal '11111222223333344444555556666677777888'",
      "11111222223333344444555556666677777888",
      DECIMAL(38, 0));
  testLong(
      "decimal '000000011111222223333344444555556666677777888'",
      "11111222223333344444555556666677777888",
      DECIMAL(38, 0));
  testLong(
      "decimal '11111222223333344444.55'",
      "1111122222333334444455",
      DECIMAL(22, 2));
  testLong(
      "decimal '00000000000000011111222223333344444.55'",
      "1111122222333334444455",
      DECIMAL(22, 2));
  testLong(
      "decimal '-11111.22222333334444455555'",
      "-1111122222333334444455555",
      DECIMAL(25, 20));

  // Zeros.
  testShort("DECIMAL '0'", 0, DECIMAL(1, 0));
  testShort("DECIMAL '00000000000000000000000'", 0, DECIMAL(1, 0));
  testShort("DECIMAL '0.'", 0, DECIMAL(1, 0));
  testShort("DECIMAL '0.0'", 0, DECIMAL(1, 1));
  testShort("DECIMAL '0.000'", 0, DECIMAL(3, 3));
  testShort("DECIMAL '.0'", 0, DECIMAL(1, 1));

  testLong(
      "DECIMAL '0.00000000000000000000000000000000000000'",
      "0",
      DECIMAL(38, 38));
}

TEST_F(ExpressionParserTest, intervalYearMonth) {
  auto test = [&](std::string_view sql, int64_t expected) {
    auto expr = parseExpr(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), INTERVAL_YEAR_MONTH()->toString());

    auto value = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(value->isNull());
    ASSERT_EQ(value->value<int32_t>(), expected);
  };

  test("INTERVAL '2' YEAR", 2 * 12);
  test("INTERVAL '3' MONTH", 3);

  test("INTERVAL '' YEAR", 0);
  test("INTERVAL '0' MONTH", 0);

  test("INTERVAL '-2' YEAR", -2 * 12);
  test("INTERVAL '-3' MONTH", -3);
}

TEST_F(ExpressionParserTest, doubleLiteral) {
  auto test = [&](std::string_view sql, double expected) {
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(expr->type()->toString(), DOUBLE()->toString());

    auto value = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(value->isNull());
    ASSERT_DOUBLE_EQ(value->value<double>(), expected);
  };

  test("1E10", 1e10);
  test("1.5E10", 1.5e10);
  test("1.23E-5", 1.23e-5);
  test(".5E2", 0.5e2);
  test("1E+5", 1e5);
}

TEST_F(ExpressionParserTest, decimalLiteralAsDecimal) {
  // By default, 1.5 is parsed as DOUBLE.
  auto expr = parseExpr("1.5");
  VELOX_EXPECT_EQ_TYPES(expr->type(), DOUBLE());

  // With parseDecimalLiteralAsDouble=false, 1.5 is parsed as DECIMAL(2, 1).
  PrestoParser parser(
      kConnectorId,
      "default",
      ParserOptions{.parseDecimalLiteralAsDouble = false});
  expr = parser.parseExpression("1.5");
  VELOX_EXPECT_EQ_TYPES(expr->type(), DECIMAL(2, 1));

  expr = parser.parseExpression("123.45");
  VELOX_EXPECT_EQ_TYPES(expr->type(), DECIMAL(5, 2));
}

TEST_F(ExpressionParserTest, digitSeparators) {
  // Integer with underscores.
  auto expr = parseExpr("10_000");
  ASSERT_TRUE(expr->isConstant());
  VELOX_EXPECT_EQ_TYPES(expr->type(), INTEGER());
  EXPECT_EQ(expr->as<lp::ConstantExpr>()->value()->value<int32_t>(), 10'000);

  // Double with underscores.
  expr = parseExpr("1_000.5E2");
  ASSERT_TRUE(expr->isConstant());
  ASSERT_DOUBLE_EQ(
      expr->as<lp::ConstantExpr>()->value()->value<double>(), 1000.5e2);

  // Rejected when Friendly SQL is disabled.
  VELOX_ASSERT_THROW(
      parseStrictExpr("10_000"),
      "Underscores in numeric literals require Friendly SQL mode");
}

TEST_F(ExpressionParserTest, binaryLiteral) {
  auto test = [&](std::string_view sql, std::string_view expectedBytes) {
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(*expr->type(), *VARBINARY());

    auto value = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(value->isNull());
    EXPECT_EQ(value->value<TypeKind::VARBINARY>(), expectedBytes);
  };

  test("X'48454C4C4F'", "HELLO");
  test(
      "X'abcdef1234567890ABCDEF'",
      "\xab\xcd\xef\x12\x34\x56\x78\x90\xAB\xCD\xEF");
  test("X'00'", std::string("\x00", 1));
  test("X''", "");

  test("x''", "");
  test("x' '", "");
  test("X'AB CD'", "\xAB\xCD");

  // Odd number of hex digits.
  VELOX_ASSERT_THROW(parseExpr("X'a b c'"), "even number of digits");

  // Non-hexadecimal character.
  VELOX_ASSERT_THROW(parseExpr("X'az'"), "hexadecimal digits");
}

TEST_F(ExpressionParserTest, stringLiteralEscapedQuotes) {
  auto test = [&](std::string_view sql, std::string_view expected) {
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(*expr->type(), *VARCHAR());

    auto value = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(value->isNull());
    EXPECT_EQ(value->value<std::string>(), expected);
  };

  // Simple string without escapes.
  test("'hello'", "hello");

  // Doubled single quote should produce a single quote.
  test("'it''s'", "it's");

  // Multiple escaped quotes.
  test("'it''s a ''test'''", "it's a 'test'");

  // Quote at start.
  test("'''hello'", "'hello");

  // Quote at end.
  test("'hello'''", "hello'");

  // Only escaped quotes.
  test("''''", "'");

  // Empty string.
  test("''", "");
}

TEST_F(ExpressionParserTest, unicodeStringLiteral) {
  auto test = [&](std::string_view sql, std::string_view expected) {
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);

    ASSERT_TRUE(expr->isConstant());
    ASSERT_EQ(*expr->type(), *VARCHAR());

    auto value = expr->as<lp::ConstantExpr>()->value();
    ASSERT_FALSE(value->isNull());
    EXPECT_EQ(value->value<std::string>(), expected);
  };

  // Doubled single quote in unicode string literal.
  test(R"(U&'it''s')", "it's");

  // 4-digit unicode escape: \000A is newline.
  test(R"(U&'hello\000Aworld')", "hello\nworld");

  // 6-digit escape with +.
  test(R"(U&'hello\+00000Aworld')", "hello\nworld");

  // Escaped backslash: \\ produces a single backslash.
  test(R"(U&'hello\\world')", "hello\\world");

  // Custom escape character via UESCAPE.
  test(R"(U&'hello#000Aworld' UESCAPE '#')", "hello\nworld");

  // No escape sequences.
  test(R"(U&'hello world')", "hello world");

  // Multi-byte character: \00E9 is 'é'.
  test(R"(U&'caf\00E9')", "caf\xC3\xA9");

  // Empty string.
  test(R"(U&'')", "");

  // Incomplete 4-digit escape sequence.
  VELOX_ASSERT_THROW(
      parseExpr(R"(U&'hello\6dB\8Bd5')"), "Incomplete escape sequence: 6dB");

  // Incomplete escape at end of string.
  VELOX_ASSERT_THROW(parseExpr(R"(U&'hello\')"), "Incomplete escape sequence");

  // Incomplete 6-digit escape with +.
  VELOX_ASSERT_THROW(parseExpr(R"(U&'hello\+')"), "Incomplete escape sequence");

  // Invalid hex digit.
  VELOX_ASSERT_THROW(
      parseExpr(R"(U&'hello\K6B\8Bd5')"), "Invalid hexadecimal digit: K");

  // Invalid code point (out of range).
  VELOX_ASSERT_THROW(
      parseExpr(R"(U&'hello\+FFFFFD\8Bd5')"),
      "Invalid escaped character: FFFFFD");

  // Surrogate code point (4-digit).
  VELOX_ASSERT_THROW(
      parseExpr(R"(U&'hello\DBFF')"),
      "Invalid escaped character: DBFF. Escaped character is a surrogate");

  // Surrogate code point (6-digit).
  VELOX_ASSERT_THROW(
      parseExpr(R"(U&'hello\+00DBFF')"),
      "Invalid escaped character: 00DBFF. Escaped character is a surrogate");

  // Invalid UESCAPE: multi-character.
  VELOX_ASSERT_THROW(
      parseExpr(R"(U&'hello\8Bd5' UESCAPE '%%')"),
      "Invalid Unicode escape character: %%");

  // Invalid UESCAPE: single quote.
  VELOX_ASSERT_THROW(
      parseExpr("U&'hello\\8Bd5' UESCAPE ''''"),
      "Invalid Unicode escape character: '");

  // Invalid UESCAPE: space.
  VELOX_ASSERT_THROW(
      parseExpr(R"(U&'hello\8Bd5' UESCAPE ' ')"),
      "Invalid Unicode escape character:");

  // Invalid UESCAPE: hex digit.
  VELOX_ASSERT_THROW(
      parseExpr(R"(U&'hello\8Bd5' UESCAPE '1')"),
      "Invalid Unicode escape character: 1");

  // Invalid UESCAPE: plus.
  VELOX_ASSERT_THROW(
      parseExpr(R"(U&'hello\8Bd5' UESCAPE '+')"),
      "Invalid Unicode escape character: +");
}

TEST_F(ExpressionParserTest, timestampLiteral) {
  auto test = [&](std::string_view sql, const TypePtr& expectedType) {
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);

    VELOX_ASSERT_EQ_TYPES(expr->type(), expectedType);
  };

  test("TIMESTAMP '2020-01-01'", TIMESTAMP());
  test("TIMESTAMP '2020-01-01 00:00:00'", TIMESTAMP());
  test("TIMESTAMP '2020-01-01 00:00:00.000'", TIMESTAMP());
  test(
      "TIMESTAMP '2020-01-01 00:00 America/Los_Angeles'",
      TIMESTAMP_WITH_TIME_ZONE());

  VELOX_ASSERT_THROW(
      parseExpr("TIMESTAMP 'foo'"), "Not a valid timestamp literal");
}

// JSON literals (json '...') should be translated as json_parse('...'),
// not CAST('...' AS JSON). CAST wraps the value as a JSON string, while
// json_parse interprets the value as JSON.
TEST_F(ExpressionParserTest, jsonLiteral) {
  auto expr = parseExpr(R"(json '{"a": 1}')");
  ASSERT_TRUE(expr->isCall());
  auto* call = expr->as<lp::CallExpr>();
  ASSERT_EQ(call->name(), "json_parse");
  ASSERT_EQ(call->inputs().size(), 1);
  ASSERT_TRUE(call->inputAt(0)->isConstant());
  ASSERT_EQ(
      call->inputAt(0)
          ->as<lp::ConstantExpr>()
          ->value()
          ->value<TypeKind::VARCHAR>(),
      R"({"a": 1})");
}

TEST_F(ExpressionParserTest, atTimeZone) {
  // AT TIME ZONE translates to at_timezone().
  EXPECT_EQ(
      "at_timezone(from_unixtime(CAST(1700000000 AS DOUBLE), UTC), America/New_York)",
      parseExpr(
          "from_unixtime(1700000000, 'UTC') AT TIME ZONE 'America/New_York'")
          ->toString());
  EXPECT_EQ(
      "date_format(date_trunc(hour, at_timezone(from_unixtime(CAST(1700000000 AS DOUBLE), UTC), GMT)), %Y-%m-%d+%H:00)",
      parseExpr(
          "date_format(date_trunc('hour', from_unixtime(1700000000, 'UTC') AT TIME ZONE 'GMT'), '%Y-%m-%d+%H:00')")
          ->toString());
}

TEST_F(ExpressionParserTest, extract) {
  // EXTRACT(field FROM x) translates to a scalar function call.
  auto test = [&](std::string_view field, const std::string& expectedFunction) {
    auto sql =
        fmt::format("EXTRACT({} FROM TIMESTAMP '2020-06-15 12:30:45')", field);
    SCOPED_TRACE(sql);
    auto expr = parseExpr(sql);
    ASSERT_TRUE(expr->isCall());
    ASSERT_EQ(expr->as<lp::CallExpr>()->name(), expectedFunction);
  };

  test("YEAR", "year");
  test("MONTH", "month");
  test("DAY", "day");
  test("HOUR", "hour");
  test("MINUTE", "minute");
  test("SECOND", "second");
}

TEST_F(ExpressionParserTest, specialDateTimeFunctions) {
  // Special date/time keywords translate to zero-argument function calls.
  EXPECT_EQ("current_date()", parseExpr("CURRENT_DATE")->toString());
  EXPECT_EQ("current_date()", parseExpr("current_date")->toString());
  EXPECT_EQ("current_time()", parseExpr("CURRENT_TIME")->toString());
  EXPECT_EQ("current_timestamp()", parseExpr("CURRENT_TIMESTAMP")->toString());
  EXPECT_EQ("localtime()", parseExpr("LOCALTIME")->toString());
  EXPECT_EQ("localtimestamp()", parseExpr("LOCALTIMESTAMP")->toString());

  // Precision is not supported.
  VELOX_ASSERT_THROW(
      parseExpr("CURRENT_TIME(3)"),
      "Precision for date/time functions is not supported yet");
  VELOX_ASSERT_THROW(
      parseExpr("CURRENT_TIMESTAMP(6)"),
      "Precision for date/time functions is not supported yet");
}

TEST_F(ExpressionParserTest, nullif) {
  // NULLIF(a, b) translates to IF(eq(a, b), null, a).
  EXPECT_EQ(
      "IF(eq(1, 2), CAST(null AS INTEGER), 1)",
      parseExpr("NULLIF(1, 2)")->toString());
  EXPECT_EQ(
      "IF(eq(1, 1), CAST(null AS INTEGER), 1)",
      parseExpr("nullif(1, 1)")->toString());
  EXPECT_EQ(
      "IF(eq(foo, bar), CAST(null AS VARCHAR), foo)",
      parseExpr("NULLIF('foo', 'bar')")->toString());
}

TEST_F(ExpressionParserTest, null) {
  EXPECT_EQ("is_null(1)", parseExpr("1 is null")->toString());
  EXPECT_EQ("is_null(1)", parseExpr("1 IS NULL")->toString());

  EXPECT_EQ("not(is_null(1))", parseExpr("1 is not null")->toString());
  EXPECT_EQ("not(is_null(1))", parseExpr("1 IS NOT NULL")->toString());
}

TEST_F(ExpressionParserTest, unaryArithmetic) {
  EXPECT_EQ("negate(1)", parseExpr("-1")->toString());
  EXPECT_EQ("1", parseExpr("+1")->toString());
}

TEST_F(ExpressionParserTest, distinctFrom) {
  EXPECT_EQ(
      "distinct_from(1, 2)", parseExpr("1 is distinct from 2")->toString());
  EXPECT_EQ(
      "not(distinct_from(1, 2))",
      parseExpr("1 is not distinct from 2")->toString());
}

TEST_F(ExpressionParserTest, ifClause) {
  EXPECT_EQ(
      "IF(gt(1, 2), 100, null)", parseExpr("if (1 > 2, 100)")->toString());
  EXPECT_EQ(
      "IF(gt(1, 2), 100, 200)", parseExpr("if (1 > 2, 100, 200)")->toString());

  testNationExpr(
      "if (n_nationkey between 10 and 13, 'foo')",
      "IF(between(n_nationkey, CAST(10 AS BIGINT), CAST(13 AS BIGINT)), foo, null)");
}

TEST_F(ExpressionParserTest, notBetween) {
  // NOT BETWEEN translates to not(between(x, low, high)).
  testNationExpr(
      "n_nationkey NOT BETWEEN 10 AND 20",
      "not(between(n_nationkey, CAST(10 AS BIGINT), CAST(20 AS BIGINT)))");
}

TEST_F(ExpressionParserTest, switch) {
  // Searched CASE (CASE WHEN cond THEN result).
  EXPECT_EQ(
      "SWITCH(gt(1, 2), 100, gt(3, 4), 200)",
      parseExpr("case when 1 > 2 then 100 when 3 > 4 then 200 end")
          ->toString());
  EXPECT_EQ(
      "SWITCH(gt(1, 2), 100, gt(3, 4), 200, 300)",
      parseExpr("case when 1 > 2 then 100 when 3 > 4 then 200 else 300 end")
          ->toString());

  // NULL condition can never be true. Drop the clause.
  EXPECT_EQ("null", parseExpr("case when null then 1 end")->toString());
  EXPECT_EQ("2", parseExpr("case when null then 1 else 2 end")->toString());
  EXPECT_EQ(
      "SWITCH(gt(1, 2), 100)",
      parseExpr("case when null then 0 when 1 > 2 then 100 end")->toString());

  // Simple CASE (CASE x WHEN v THEN result) desugars to SWITCH(eq(x, v), ...).
  EXPECT_EQ(
      "SWITCH(eq(1, 1), 100, eq(1, 2), 200)",
      parseExpr("case 1 when 1 then 100 when 2 then 200 end")->toString());
  EXPECT_EQ(
      "SWITCH(eq(1, 1), 100, eq(1, 2), 200, 300)",
      parseExpr("case 1 when 1 then 100 when 2 then 200 else 300 end")
          ->toString());
}

TEST_F(ExpressionParserTest, in) {
  EXPECT_EQ("IN(1, 2, 3, 4)", parseExpr("1 in (2,3,4)")->toString());
  EXPECT_EQ("IN(1, 2, 3, 4)", parseExpr("1 IN (2,3,4)")->toString());

  EXPECT_EQ("not(IN(1, 2, 3, 4))", parseExpr("1 not in (2,3,4)")->toString());
  EXPECT_EQ("not(IN(1, 2, 3, 4))", parseExpr("1 NOT IN (2,3,4)")->toString());

  // Coercions.
  testNationExpr(
      "n_nationkey in (1, 2, 3)",
      "IN(n_nationkey, CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT))");

  auto assertInSubquery = [](const lp::ExprPtr& expr) {
    ASSERT_EQ(lp::ExprKind::kSpecialForm, expr->kind());
    auto& in = *expr->as<lp::SpecialFormExpr>();
    ASSERT_EQ(in.form(), lp::SpecialForm::kIn);
    ASSERT_EQ(in.inputs().size(), 2);
    ASSERT_EQ(lp::ExprKind::kInputReference, in.inputAt(0)->kind());
    ASSERT_EQ(lp::ExprKind::kSubquery, in.inputAt(1)->kind());
  };

  // Subquery IN in WHERE clause produces a filter with IN(column, subquery).
  testSelect(
      "SELECT * FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name like 'A%')",
      matchScan()
          .filter([&](const auto& node) {
            auto filter = std::dynamic_pointer_cast<const lp::FilterNode>(node);
            assertInSubquery(filter->predicate());
          })
          .output());

  // Subquery IN in SELECT clause produces a project with IN(column, subquery).
  testSelect(
      "SELECT n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name like 'A%') FROM nation",
      matchScan()
          .project([&](const auto& node) {
            auto project =
                std::dynamic_pointer_cast<const lp::ProjectNode>(node);
            ASSERT_EQ(project->expressions().size(), 1);
            assertInSubquery(project->expressionAt(0));
          })
          .output());
}

TEST_F(ExpressionParserTest, notLike) {
  // NOT LIKE translates to not(like(x, pattern)).
  testNationExpr("n_name NOT LIKE 'A%'", "not(like(n_name, A%))");
}

TEST_F(ExpressionParserTest, likeEscape) {
  // LIKE with ESCAPE translates to a 3-argument like() call.
  testNationExpr("n_name LIKE 'A%' ESCAPE '#'", "like(n_name, A%, #)");
  testNationExpr("n_name NOT LIKE 'A%' ESCAPE '#'", "not(like(n_name, A%, #))");
}

TEST_F(ExpressionParserTest, coalesce) {
  testNationExpr("coalesce(n_name, 'foo')", "COALESCE(n_name, foo)");
  testNationExpr("COALESCE(n_name, 'foo')", "COALESCE(n_name, foo)");

  // Coercions.
  testNationExpr(
      "coalesce(n_regionkey, 1)", "COALESCE(n_regionkey, CAST(1 AS BIGINT))");
}

TEST_F(ExpressionParserTest, concat) {
  // || translates to concat().
  testNationExpr("n_name || n_comment", "concat(n_name, n_comment)");
}

TEST_F(ExpressionParserTest, position) {
  // POSITION(x IN y) translates to strpos(y, x).
  testNationExpr("POSITION('A' IN n_name)", "strpos(n_name, A)");
  testNationExpr("POSITION(n_comment IN n_name)", "strpos(n_name, n_comment)");
}

TEST_F(ExpressionParserTest, substringFrom) {
  // SUBSTRING(x FROM y FOR z) translates to substr(x, y, z).
  testNationExpr("SUBSTRING(n_name FROM 1)", "substr(n_name, 1)");
  testNationExpr("SUBSTRING(n_name FROM 2 FOR 3)", "substr(n_name, 2, 3)");
}

TEST_F(ExpressionParserTest, subscript) {
  EXPECT_EQ(
      "subscript(array_constructor(1, 2, 3), 1)",
      parseExpr("array[1, 2, 3][1]")->toString());
  EXPECT_EQ(
      "DEREFERENCE(row_constructor(1, 2), 1)",
      parseExpr("row(1, 2)[2]")->toString());
}

TEST_F(ExpressionParserTest, dereference) {
  // Named field dereference.
  EXPECT_EQ(
      "DEREFERENCE(CAST(row_constructor(1, 2) AS ROW<a:INTEGER,b:INTEGER>), a)",
      parseExpr("cast(row(1, 2) as row(a int, b int)).a")->toString());

  // Legacy field name dereference.
  EXPECT_EQ(
      "DEREFERENCE(row_constructor(1, 2), 0)",
      parseExpr("row(1, 2).field0")->toString());
  EXPECT_EQ(
      "DEREFERENCE(row_constructor(1, 2), 0)",
      parseExpr("row(1, 2).field000")->toString());
  EXPECT_EQ(
      "DEREFERENCE(row_constructor(1, 2), 1)",
      parseExpr("row(1, 2).field1")->toString());
  EXPECT_EQ(
      "DEREFERENCE(row_constructor(1, 2), 1)",
      parseExpr("row(1, 2).field01")->toString());

  VELOX_ASSERT_THROW(
      parseExpr("row(1, 2).field2"), "Invalid legacy field name: field2");

  VELOX_ASSERT_THROW(
      parseExpr("cast(row(1, 2) as row(a int, b int)).field0"),
      "Cannot access named field using legacy field name: field0 vs. a");

  auto expr = parseExpr(
      R"(cast(json_parse('{"foo": 1, "bar": 2}') as row(foo bigint, "BAR" int)).BAR)");
  VELOX_EXPECT_EQ_TYPES(expr->type(), INTEGER());
}

TEST_F(ExpressionParserTest, methodCall) {
  // Basic method call: expr.func() desugars to func(expr).
  EXPECT_EQ("upper(hello)", parseExpr("'hello'.upper()")->toString());

  // Method call with arguments: expr.func(args) desugars to func(expr, args).
  EXPECT_EQ(
      "substr(hello, 1, 3)", parseExpr("'hello'.substr(1, 3)")->toString());

  // Multi-level chaining: a.f().g() desugars to g(f(a)).
  EXPECT_EQ(
      "upper(trim(hello))", parseExpr("'hello'.trim().upper()")->toString());

  // Chaining on parenthesized expression.
  EXPECT_EQ("abs(plus(1, 2))", parseExpr("(1 + 2).abs()")->toString());

  // Chaining on function call result.
  EXPECT_EQ(
      "substr(upper(hello), 1, 3)",
      parseExpr("upper('hello').substr(1, 3)")->toString());

  // Column-based method call: col.func(args) desugars to func(col, args).
  testNationExpr("n_name.substr(1, 3)", "substr(n_name, 1, 3)");

  // Chaining on column: col.func().func2().
  testNationExpr("n_name.trim().upper()", "upper(trim(n_name))");

  // Dereference without parentheses still works as struct field access.
  EXPECT_EQ(
      "DEREFERENCE(CAST(row_constructor(1, 2) AS ROW<a:INTEGER,b:INTEGER>), a)",
      parseExpr("cast(row(1, 2) as row(a int, b int)).a")->toString());
}

TEST_F(ExpressionParserTest, methodCallNoFriendlySql) {
  // Method call syntax is rejected when Friendly SQL is disabled.
  VELOX_ASSERT_THROW(
      parseStrictExpr("'hello'.upper()"),
      "Method-call syntax requires Friendly SQL mode");
}

TEST_F(ExpressionParserTest, lateralColumnAlias) {
  // Parses 'SELECT <selectList> FROM nation' and verifies each project
  // expression matches the expected toString() output.
  auto testExprs = [&](std::string_view selectList,
                       const std::vector<std::string>& expectedExprs) {
    SCOPED_TRACE(selectList);
    testSelect(
        fmt::format("SELECT {} FROM nation", selectList),
        matchScan()
            .project([&](const lp::LogicalPlanNodePtr& node) {
              auto& project = *node->as<lp::ProjectNode>();
              ASSERT_EQ(expectedExprs.size(), project.expressions().size());
              for (size_t i = 0; i < expectedExprs.size(); ++i) {
                EXPECT_EQ(expectedExprs[i], project.expressionAt(i)->toString())
                    << "at index " << i;
              }
            })
            .output());
  };

  // Basic reuse: j is expanded to (n_regionkey + 1).
  testExprs(
      "n_regionkey + 1 AS j, j + 2 AS k",
      {
          "plus(n_regionkey, CAST(1 AS BIGINT))",
          "plus(plus(n_regionkey, CAST(1 AS BIGINT)), CAST(2 AS BIGINT))",
      });

  // Triple chaining: each alias references the previous.
  testExprs(
      "n_regionkey AS x, x + 1 AS y, y * 2 AS z",
      {
          "n_regionkey",
          "plus(n_regionkey, CAST(1 AS BIGINT))",
          "multiply(plus(n_regionkey, CAST(1 AS BIGINT)), CAST(2 AS BIGINT))",
      });

  // Alias with function call and method-call chaining.
  testExprs(
      "upper(n_name) AS u, u.substr(1, 3) AS s",
      {"upper(n_name)", "substr(upper(n_name), 1, 3)"});
}

TEST_F(ExpressionParserTest, lateralColumnAliasErrors) {
  // Forward reference: j is used before it's defined.
  VELOX_ASSERT_THROW(
      parseSql("SELECT j + 2 AS k, n_regionkey + 1 AS j FROM nation"),
      "Cannot resolve column: j");

  // Self-reference: j is not yet in the alias map when the expression is
  // evaluated, so it resolves as a (non-existent) column reference.
  VELOX_ASSERT_THROW(
      parseSql("SELECT j + 1 AS j FROM nation"), "Cannot resolve column: j");

  // Lateral column aliases are not available when Friendly SQL is disabled.
  VELOX_ASSERT_THROW(
      makeStrictParser().parse(
          "SELECT n_regionkey + 1 AS j, j + 2 AS k FROM nation", true),
      "Cannot resolve column: j");
}

TEST_F(ExpressionParserTest, row) {
  testNationExpr(
      "row(n_regionkey, n_name)", "row_constructor(n_regionkey, n_name)");
}

TEST_F(ExpressionParserTest, namedRow) {
  // Named ROW constructor with literals.
  auto expr = parseExpr("row(1 as x, 'hello' as y)");
  VELOX_EXPECT_EQ_TYPES(expr->type(), ROW({"x", "y"}, {INTEGER(), VARCHAR()}));

  // Single field.
  expr = parseExpr("row(42 as answer)");
  VELOX_EXPECT_EQ_TYPES(expr->type(), ROW({"answer"}, {INTEGER()}));
}

TEST_F(ExpressionParserTest, namedRowNoFriendlySql) {
  // Named ROW constructor is rejected when Friendly SQL is disabled.
  VELOX_ASSERT_THROW(
      parseStrictExpr("row(1 as x, 'hello' as y)"),
      "Named ROW constructor requires Friendly SQL mode");
}

TEST_F(ExpressionParserTest, namedRowDereference) {
  // Field access on named ROW.
  auto expr = parseExpr("row(1 as x, 2 as y).x");
  VELOX_EXPECT_EQ_TYPES(expr->type(), INTEGER());

  expr = parseExpr("row(1 as x, 'hello' as y).y");
  VELOX_EXPECT_EQ_TYPES(expr->type(), VARCHAR());
}

TEST_F(ExpressionParserTest, windowFunction) {
  // row_number() with ORDER BY: SQL standard default frame when ORDER BY is
  // present without explicit frame is RANGE UNBOUNDED PRECEDING to CURRENT ROW.
  testNationExpr(
      "row_number() OVER (ORDER BY n_nationkey)",
      "row_number() OVER (ORDER BY n_nationkey ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");

  // row_number() with PARTITION BY and ORDER BY: same default applies.
  testNationExpr(
      "row_number() OVER (PARTITION BY n_regionkey ORDER BY n_nationkey)",
      "row_number() OVER (PARTITION BY n_regionkey ORDER BY n_nationkey ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");

  // Aggregate function as window function without ORDER BY: default frame is
  // entire partition (UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING).
  testNationExpr(
      "sum(n_nationkey) OVER (PARTITION BY n_regionkey)",
      "sum(n_nationkey) OVER (PARTITION BY n_regionkey RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)");

  // ORDER BY DESC NULLS FIRST with default frame.
  testNationExpr(
      "row_number() OVER (ORDER BY n_nationkey DESC NULLS FIRST)",
      "row_number() OVER (ORDER BY n_nationkey DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");

  // Explicit ROWS frame is preserved.
  testNationExpr(
      "sum(n_nationkey) OVER (ORDER BY n_nationkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
      "sum(n_nationkey) OVER (ORDER BY n_nationkey ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");

  // ROWS frame with bounded preceding and following.
  testNationExpr(
      "sum(n_nationkey) OVER (ORDER BY n_nationkey ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)",
      "sum(n_nationkey) OVER (ORDER BY n_nationkey ASC NULLS LAST ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)");

  // RANGE frame.
  testNationExpr(
      "sum(n_nationkey) OVER (ORDER BY n_nationkey RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)",
      "sum(n_nationkey) OVER (ORDER BY n_nationkey ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)");

  // Multiple window functions in SELECT: first has ORDER BY (default to CURRENT
  // ROW), second has no ORDER BY (default to UNBOUNDED FOLLOWING).
  testSelect(
      "SELECT row_number() OVER (ORDER BY n_nationkey), sum(n_nationkey) OVER (PARTITION BY n_regionkey) FROM nation",
      matchScan()
          .project({
              "row_number() OVER (ORDER BY n_nationkey ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
              "sum(n_nationkey) OVER (PARTITION BY n_regionkey RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
          })
          .output());

  // Mix of scalar and window functions.
  testSelect(
      "SELECT n_name, length(n_name), row_number() OVER (ORDER BY n_nationkey) FROM nation",
      matchScan()
          .project({
              "n_name",
              "length(n_name)",
              "row_number() OVER (ORDER BY n_nationkey ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
          })
          .output());

  // Window function with alias.
  testSelect(
      "SELECT row_number() OVER (ORDER BY n_nationkey) AS row_num FROM nation",
      matchScan()
          .project({
              "row_number() OVER (ORDER BY n_nationkey ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
          })
          .output());

  // Pure window function with coercion: ntile expects BIGINT, integer literal
  // 1 is INTEGER.
  testNationExpr(
      "ntile(1) OVER (ORDER BY n_nationkey)",
      "ntile(CAST(1 AS BIGINT)) OVER (ORDER BY n_nationkey ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");

  // Aggregate as window function with coercion: corr expects (DOUBLE, DOUBLE),
  // columns are BIGINT.
  testNationExpr(
      "corr(n_nationkey, n_regionkey) OVER (ORDER BY n_nationkey)",
      "corr(CAST(n_nationkey AS DOUBLE), CAST(n_regionkey AS DOUBLE)) OVER (ORDER BY n_nationkey ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
}

TEST_F(ExpressionParserTest, lambda) {
  ASSERT_NO_THROW(parseExpr("filter(array[1,2,3], x -> x > 1)"));
  ASSERT_NO_THROW(parseExpr("FILTER(array[1,2,3], x -> x > 1)"));

  ASSERT_NO_THROW(parseExpr("filter(array[], x -> true)"));

  ASSERT_NO_THROW(parseExpr("reduce(array[], map(), (s, x) -> s, s -> 123)"));

  ASSERT_NO_THROW(parseExpr(
      "reduce(array[], map(), (s, x) -> map(array[1], array[2]), s -> 123)"));
}

} // namespace
} // namespace axiom::sql::presto::test
