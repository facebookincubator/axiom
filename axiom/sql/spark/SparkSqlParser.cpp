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

#include "axiom/sql/spark/SparkSqlParser.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cmath>
#include <optional>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/Unicode.h>
#include <folly/container/F14Map.h>

#include "axiom/sql/DecimalDigits.h"
#include "axiom/sql/DepthLimitListener.h"
#include "axiom/sql/SyntaxErrorListener.h"
#include "axiom/sql/UpperCaseInputStream.h"
#include "axiom/sql/spark/SparkFunctions.h"
#include "axiom/sql/spark/grammar/SparkGrammar.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace axiom::sql::spark {
namespace {

namespace lp = facebook::axiom::logical_plan;
using namespace facebook::velox;
using Parser = SqlBaseParser;

std::string toLower(std::string text) {
  folly::toLowerAscii(text);
  return text;
}

// Fails a construct of the grammar that is not translated yet, naming it by
// its text.
[[noreturn]] void notSupported(
    std::string_view construct,
    antlr4::ParserRuleContext* context) {
  VELOX_NYI(
      "Spark SQL {} is not supported yet: {}", construct, context->getText());
}

// Returns the UTF-16 code unit of the 4 hex digits at 'start', or nullopt if
// they are not 4 hex digits.
std::optional<char32_t> hexCodeUnit(std::string_view body, size_t start) {
  if (start + 4 > body.size()) {
    return std::nullopt;
  }
  uint32_t value{0};
  const char* end = body.data() + start + 4;
  const auto [last, error] =
      std::from_chars(body.data() + start, end, value, 16);
  if (error != std::errc() || last != end) {
    return std::nullopt;
  }
  return value;
}

bool isSurrogate(char32_t codeUnit) {
  return codeUnit >= 0xD800 && codeUnit <= 0xDFFF;
}

// Appends the character for the \u escape whose 'u' is at 'position', joining
// a surrogate pair into one character. Returns the position of the last
// character consumed. A lone surrogate becomes '?', as Java encodes it.
size_t appendUnicodeEscape(
    std::string_view body,
    size_t position,
    char32_t codeUnit,
    std::string& text) {
  size_t last = position + 4;
  if (codeUnit >= 0xD800 && codeUnit <= 0xDBFF &&
      body.substr(last + 1, 2) == "\\u") {
    if (const auto low = hexCodeUnit(body, last + 3);
        low.has_value() && *low >= 0xDC00 && *low <= 0xDFFF) {
      folly::appendCodePointToUtf8(
          0x10000 + ((codeUnit - 0xD800) << 10) + (*low - 0xDC00), text);
      return last + 6;
    }
  }
  if (isSurrogate(codeUnit)) {
    text += '?';
  } else {
    folly::appendCodePointToUtf8(codeUnit, text);
  }
  return last;
}

// True if the 3 characters at 'start' are an octal escape: [01][0-7][0-7].
bool isOctalEscape(std::string_view body, size_t start) {
  auto isOctal = [](char character) {
    return character >= '0' && character <= '7';
  };
  return start + 3 <= body.size() &&
      (body[start] == '0' || body[start] == '1') && isOctal(body[start + 1]) &&
      isOctal(body[start + 2]);
}

void appendEscapedCharacter(char escaped, std::string& text) {
  switch (escaped) {
    case '0':
      text += '\0';
      break;
    case 'b':
      text += '\b';
      break;
    case 'n':
      text += '\n';
      break;
    case 'r':
      text += '\r';
      break;
    case 't':
      text += '\t';
      break;
    case 'Z':
      text += '\x1A';
      break;
    case '%':
    case '_':
      // Kept escaped for LIKE patterns.
      text += '\\';
      text += escaped;
      break;
    default:
      text += escaped;
  }
}

// Returns the value of a string literal token, with Spark's escapes applied
// (ParserUtils.unescapeSQLString). A raw string, R'...', keeps its text.
std::string unescapeString(std::string_view token) {
  if (token.front() == 'r' || token.front() == 'R') {
    return std::string(token.substr(2, token.size() - 3));
  }

  const auto body = token.substr(1, token.size() - 2);
  std::string text;
  text.reserve(body.size());
  for (size_t i = 0; i < body.size(); ++i) {
    if (body[i] != '\\' || i + 1 == body.size()) {
      text += body[i];
      continue;
    }
    if (body[i + 1] == 'u') {
      if (const auto codeUnit = hexCodeUnit(body, i + 2)) {
        i = appendUnicodeEscape(body, i + 1, *codeUnit, text);
        continue;
      }
    } else if (isOctalEscape(body, i + 1)) {
      folly::appendCodePointToUtf8(
          ((body[i + 1] - '0') << 6) + ((body[i + 2] - '0') << 3) +
              (body[i + 3] - '0'),
          text);
      i += 3;
      continue;
    }
    appendEscapedCharacter(body[++i], text);
  }
  return text;
}

// Bounds the nesting of the input, so that deeply nested input fails instead
// of overflowing the stack.
constexpr uint32_t kMaxNestingDepth{1'024};

[[noreturn]] void failNestingDepth(const antlr4::Token& start) {
  VELOX_USER_FAIL(
      "Expression exceeds maximum nesting depth at line {}, position {}",
      start.getLine(),
      start.getCharPositionInLine() + 1);
}

// The parser for 'sql' and the objects it reads from. Each member reads from
// the one declared before it.
struct ParserState {
  explicit ParserState(std::string_view sql)
      : input{sql},
        lexer{&input},
        tokens{&lexer},
        parser{&tokens},
        depthLimit{kMaxNestingDepth, failNestingDepth} {
    lexer.removeErrorListeners();
    lexer.addErrorListener(&errors);
    parser.removeErrorListeners();
    parser.addErrorListener(&errors);
    parser.addParseListener(&depthLimit);
  }

  // Fails on the first syntax error, naming its position.
  void checkErrors() const {
    VELOX_USER_CHECK(
        !errors.hasError(),
        "Syntax error at line {}, position {}: {}",
        errors.line() + 1,
        errors.column() + 1,
        errors.firstError());
    VELOX_USER_CHECK(
        !lexer.has_unclosed_bracketed_comment,
        "Syntax error: unclosed bracketed comment");
  }

  UpperCaseInputStream input;
  SqlBaseLexer lexer;
  antlr4::CommonTokenStream tokens;
  SqlBaseParser parser;
  SyntaxErrorListener errors;
  DepthLimitListener depthLimit;
};

std::string toName(Parser::IdentifierContext* context) {
  auto text = context->getText();
  if (text.front() != '`') {
    return text;
  }
  // `a``b` names a`b.
  std::string name;
  for (size_t i = 1; i + 1 < text.size(); ++i) {
    name += text[i];
    if (text[i] == '`') {
      ++i;
    }
  }
  return name;
}

TypePtr toType(Parser::DataTypeContext* context);

TypePtr toPrimitiveType(Parser::PrimitiveDataTypeContext* context) {
  static const folly::F14FastMap<std::string_view, TypePtr> kTypes = {
      {"boolean", BOOLEAN()},
      {"tinyint", TINYINT()},
      {"byte", TINYINT()},
      {"smallint", SMALLINT()},
      {"short", SMALLINT()},
      {"int", INTEGER()},
      {"integer", INTEGER()},
      {"bigint", BIGINT()},
      {"long", BIGINT()},
      {"float", REAL()},
      {"real", REAL()},
      {"double", DOUBLE()},
      {"date", DATE()},
      {"timestamp", TIMESTAMP()},
      {"timestamp_ltz", TIMESTAMP()},
      {"string", VARCHAR()},
      {"binary", VARBINARY()},
      {"void", UNKNOWN()},
  };

  const auto name = toLower(toName(context->identifier()));
  std::vector<int32_t> parameters;
  for (auto* parameter : context->INTEGER_VALUE()) {
    const auto value = folly::tryTo<int32_t>(parameter->getText());
    VELOX_USER_CHECK(
        value.hasValue(),
        "Data type parameter is out of range: {}",
        context->getText());
    parameters.push_back(*value);
  }

  if (name == "decimal" || name == "dec" || name == "numeric") {
    VELOX_USER_CHECK_LE(
        parameters.size(),
        2,
        "Decimal type must have at most 2 parameters: {}",
        context->getText());
    const int32_t precision = parameters.empty() ? 10 : parameters[0];
    const int32_t scale = parameters.size() < 2 ? 0 : parameters[1];
    VELOX_USER_CHECK(
        precision >= 1 && precision <= LongDecimalType::kMaxPrecision,
        "Decimal precision must be between 1 and {}: {}",
        LongDecimalType::kMaxPrecision,
        context->getText());
    VELOX_USER_CHECK_LE(
        scale,
        precision,
        "Decimal scale must not exceed the precision: {}",
        context->getText());
    return DECIMAL(precision, scale);
  }
  if (name == "char" || name == "character" || name == "varchar") {
    VELOX_USER_CHECK_EQ(
        parameters.size(),
        1,
        "Character type must have a length: {}",
        context->getText());
    return VARCHAR();
  }

  auto it = kTypes.find(name);
  VELOX_USER_CHECK(
      it != kTypes.end(), "Data type is not supported: {}", context->getText());
  VELOX_USER_CHECK_EQ(
      parameters.size(),
      0,
      "Data type must not have parameters: {}",
      context->getText());
  return it->second;
}

TypePtr toType(Parser::DataTypeContext* context) {
  if (auto* primitive =
          dynamic_cast<Parser::PrimitiveDataTypeContext*>(context)) {
    return toPrimitiveType(primitive);
  }
  if (auto* complex = dynamic_cast<Parser::ComplexDataTypeContext*>(context)) {
    switch (complex->complex->getType()) {
      case Parser::ARRAY:
        return ARRAY(toType(complex->dataType(0)));
      case Parser::MAP:
        return MAP(toType(complex->dataType(0)), toType(complex->dataType(1)));
      default: {
        std::vector<std::string> names;
        std::vector<TypePtr> types;
        if (auto* fields = complex->complexColTypeList()) {
          for (auto* field : fields->complexColType()) {
            names.push_back(toName(field->identifier()));
            types.push_back(toType(field->dataType()));
          }
        }
        return ROW(std::move(names), std::move(types));
      }
    }
  }
  notSupported("data type", context);
}

// Returns an integer literal of type T, failing if the value does not fit.
template <typename T>
lp::ExprApi integerLiteral(std::string_view digits, std::string_view text) {
  const auto value = folly::tryTo<T>(digits);
  VELOX_USER_CHECK(
      value.hasValue(), "Numeric literal does not fit in its type: {}", text);
  return lp::Lit(*value);
}

// Returns a floating-point literal of type T, failing if the value overflows.
template <typename T>
lp::ExprApi floatingPointLiteral(
    std::string_view digits,
    std::string_view text) {
  const auto value = folly::tryTo<T>(digits);
  VELOX_USER_CHECK(
      value.hasValue() && std::isfinite(*value),
      "Numeric literal does not fit in its type: {}",
      text);
  return lp::Lit(*value);
}

lp::ExprApi decimalLiteral(std::string_view digits, std::string_view text) {
  const auto decimal = DecimalDigits::parse(digits);
  VELOX_CHECK(decimal.has_value(), "Invalid decimal literal: {}", text);
  VELOX_USER_CHECK_LE(
      decimal->precision(),
      LongDecimalType::kMaxPrecision,
      "Decimal literal has too many digits: {}",
      text);
  return decimal->toExpr();
}

// Spark's types for numeric literals: an integer is an int if it fits, else
// a bigint, else a decimal; a number with a dot is a decimal; one with an
// exponent is a double; the suffixes Y, S, L, F, D and BD pick tinyint,
// smallint, bigint, float, double and decimal.
lp::ExprApi toNumber(Parser::NumberContext* context) {
  const auto text = context->getText();
  auto withoutSuffix = [&](size_t suffixLength) {
    return std::string_view(text).substr(0, text.size() - suffixLength);
  };

  if (dynamic_cast<Parser::IntegerLiteralContext*>(context) != nullptr) {
    if (const auto value = folly::tryTo<int32_t>(text)) {
      return lp::Lit(*value);
    }
    if (const auto value = folly::tryTo<int64_t>(text)) {
      return lp::Lit(*value);
    }
    return decimalLiteral(text, text);
  }
  if (dynamic_cast<Parser::DecimalLiteralContext*>(context) != nullptr) {
    return decimalLiteral(text, text);
  }
  if (dynamic_cast<Parser::ExponentLiteralContext*>(context) != nullptr) {
    return floatingPointLiteral<double>(text, text);
  }
  if (dynamic_cast<Parser::TinyIntLiteralContext*>(context) != nullptr) {
    return integerLiteral<int8_t>(withoutSuffix(1), text);
  }
  if (dynamic_cast<Parser::SmallIntLiteralContext*>(context) != nullptr) {
    return integerLiteral<int16_t>(withoutSuffix(1), text);
  }
  if (dynamic_cast<Parser::BigIntLiteralContext*>(context) != nullptr) {
    return integerLiteral<int64_t>(withoutSuffix(1), text);
  }
  if (dynamic_cast<Parser::FloatLiteralContext*>(context) != nullptr) {
    return floatingPointLiteral<float>(withoutSuffix(1), text);
  }
  if (dynamic_cast<Parser::DoubleLiteralContext*>(context) != nullptr) {
    return floatingPointLiteral<double>(withoutSuffix(1), text);
  }
  if (dynamic_cast<Parser::BigDecimalLiteralContext*>(context) != nullptr) {
    const auto digits = withoutSuffix(2);
    if (digits.find_first_of("eE") != std::string_view::npos) {
      notSupported("BD literal with an exponent", context);
    }
    return decimalLiteral(digits, text);
  }
  notSupported("number", context);
}

lp::ExprApi toLiteral(Parser::StringLitContext* context) {
  return lp::Lit(unescapeString(context->getText()));
}

// DATE '...', TIMESTAMP '...' and X'...'. A TIMESTAMP without a time zone is
// in the session time zone.
lp::ExprApi toTypedLiteral(
    Parser::TypeConstructorContext* context,
    const tz::TimeZone& sessionTimeZone) {
  const auto type = toLower(toName(context->identifier()));
  auto value = unescapeString(context->stringLit()->getText());
  if (type == "date") {
    const auto days = util::fromDateString(
        value.data(), value.size(), util::ParseMode::kSparkCast);
    VELOX_USER_CHECK(
        days.hasValue(), "Date literal is not valid: {}", context->getText());
    return lp::Lit(Variant(*days), DATE());
  }

  if (type == "timestamp" || type == "timestamp_ltz") {
    const auto parsed = util::fromTimestampWithTimezoneString(
        value.data(), value.size(), util::TimestampParseMode::kSparkCast);
    VELOX_USER_CHECK(
        parsed.hasValue(),
        "Timestamp literal is not valid: {}",
        context->getText());
    return lp::Lit(
        Variant(
            util::fromParsedTimestampWithTimeZone(*parsed, &sessionTimeZone)),
        TIMESTAMP());
  }

  if (type == "timestamp_ntz") {
    notSupported("TIMESTAMP_NTZ literal", context);
  }

  if (type == "x") {
    // An odd number of digits has an implicit leading 0, as in Spark's unhex.
    if (value.size() % 2 == 1) {
      value.insert(0, "0");
    }
    std::string bytes;
    VELOX_USER_CHECK(
        folly::unhexlify(value, bytes),
        "Binary literal must contain only hex digits: {}",
        context->getText());
    return lp::Lit(Variant::binary(std::move(bytes)));
  }

  VELOX_USER_FAIL("Literal type is not supported: {}", context->getText());
}

lp::ExprApi toLiteral(
    Parser::ConstantContext* context,
    const tz::TimeZone& sessionTimeZone) {
  if (dynamic_cast<Parser::NullLiteralContext*>(context) != nullptr) {
    return lp::NullLit(UNKNOWN());
  }

  if (auto* boolean = dynamic_cast<Parser::BooleanLiteralContext*>(context)) {
    return lp::Lit(boolean->booleanValue()->TRUE() != nullptr);
  }

  if (auto* strings = dynamic_cast<Parser::StringLiteralContext*>(context)) {
    // Adjacent string literals concatenate: 'a' 'b' is 'ab'.
    std::string text;
    for (auto* string : strings->stringLit()) {
      text += unescapeString(string->getText());
    }
    return lp::Lit(std::move(text));
  }

  if (auto* typed = dynamic_cast<Parser::TypeConstructorContext*>(context)) {
    return toTypedLiteral(typed, sessionTimeZone);
  }

  if (auto* numeric = dynamic_cast<Parser::NumericLiteralContext*>(context)) {
    return toNumber(numeric->number());
  }

  notSupported("literal", context);
}

// True for a name: a, a.b, a.b.c. Field access on any other expression is a
// subscript, which reads a struct field or a map value.
bool isName(Parser::PrimaryExpressionContext* context) {
  if (dynamic_cast<Parser::ColumnReferenceContext*>(context) != nullptr) {
    return true;
  }
  auto* dereference = dynamic_cast<Parser::DereferenceContext*>(context);
  return dereference != nullptr && isName(dereference->base);
}

// Builds expressions from the parse tree.
class ExpressionBuilder {
 public:
  explicit ExpressionBuilder(const tz::TimeZone& sessionTimeZone)
      : sessionTimeZone_{sessionTimeZone} {}

  lp::ExprApi toExpr(Parser::NamedExpressionContext* context) {
    auto expr = toExpr(context->expression());
    if (context->name != nullptr) {
      return expr.as(toAlias(context->name));
    }
    if (context->identifierList() != nullptr) {
      notSupported("alias with several names", context);
    }
    return expr;
  }

 private:
  static std::string toAlias(Parser::ErrorCapturingIdentifierContext* context) {
    VELOX_USER_CHECK_NULL(
        dynamic_cast<Parser::ErrorIdentContext*>(
            context->errorCapturingIdentifierExtra()),
        "Unquoted identifier must not contain '-': {}",
        context->getText());
    return toName(context->identifier());
  }

  lp::ExprApi toExpr(Parser::ExpressionContext* context) {
    return toExpr(context->booleanExpression());
  }

  lp::ExprApi toExpr(Parser::BooleanExpressionContext* context) {
    if (auto* logicalNot = dynamic_cast<Parser::LogicalNotContext*>(context)) {
      return SparkFunctions::call(
          "not", toExpr(logicalNot->booleanExpression()));
    }
    if (auto* binary = dynamic_cast<Parser::LogicalBinaryContext*>(context)) {
      return SparkFunctions::call(
          toLower(binary->op->getText()),
          toExpr(binary->left),
          toExpr(binary->right));
    }
    if (auto* predicated = dynamic_cast<Parser::PredicatedContext*>(context)) {
      auto value = toExpr(predicated->valueExpression());
      if (auto* predicate = predicated->predicate()) {
        return toPredicate(std::move(value), predicate);
      }
      return value;
    }
    notSupported("expression", context);
  }

  lp::ExprApi toPredicate(
      lp::ExprApi value,
      Parser::PredicateContext* context) {
    const bool negated = context->NOT() != nullptr;
    auto maybeNot = [&](lp::ExprApi expr) {
      return negated ? SparkFunctions::call("not", std::move(expr)) : expr;
    };

    switch (context->kind->getType()) {
      case Parser::BETWEEN:
        return maybeNot(
            SparkFunctions::call(
                "between",
                std::move(value),
                toExpr(context->lower),
                toExpr(context->upper)));
      case Parser::IN: {
        if (context->query() != nullptr) {
          notSupported("IN subquery", context);
        }
        std::vector<lp::ExprApi> args{std::move(value)};
        for (auto* expr : context->expression()) {
          args.push_back(toExpr(expr));
        }
        return maybeNot(SparkFunctions::call("in", std::move(args)));
      }
      case Parser::RLIKE:
        return maybeNot(
            SparkFunctions::call(
                "rlike", std::move(value), toExpr(context->pattern)));
      case Parser::LIKE:
      case Parser::ILIKE: {
        if (context->quantifier != nullptr) {
          notSupported("LIKE ANY, SOME or ALL", context);
        }
        auto pattern = toExpr(context->pattern);
        // ILIKE is LIKE of the lowercased value and pattern.
        if (context->kind->getType() == Parser::ILIKE) {
          value = SparkFunctions::call("lower", std::move(value));
          pattern = SparkFunctions::call("lower", std::move(pattern));
        }
        std::vector<lp::ExprApi> args{std::move(value), std::move(pattern)};
        if (context->escapeChar != nullptr) {
          args.push_back(toLiteral(context->escapeChar));
        }
        return maybeNot(SparkFunctions::call("like", std::move(args)));
      }
      case Parser::NULL_LITERAL:
        return SparkFunctions::call(
            negated ? "isnotnull" : "isnull", std::move(value));
      case Parser::DISTINCT: {
        auto equal = SparkFunctions::call(
            "<=>", std::move(value), toExpr(context->right));
        return negated ? equal : SparkFunctions::call("not", std::move(equal));
      }
      default:
        notSupported("predicate", context);
    }
  }

  lp::ExprApi toExpr(Parser::ValueExpressionContext* context) {
    if (auto* primary =
            dynamic_cast<Parser::ValueExpressionDefaultContext*>(context)) {
      return toExpr(primary->primaryExpression());
    }
    if (auto* unary = dynamic_cast<Parser::ArithmeticUnaryContext*>(context)) {
      auto operand = toExpr(unary->valueExpression());
      switch (unary->op->getType()) {
        case Parser::PLUS:
          return operand;
        case Parser::MINUS:
          return SparkFunctions::call("negative", std::move(operand));
        default:
          return SparkFunctions::call(unary->op->getText(), std::move(operand));
      }
    }
    if (auto* binary =
            dynamic_cast<Parser::ArithmeticBinaryContext*>(context)) {
      // DIV is a keyword; the other operators are symbols.
      return SparkFunctions::call(
          toLower(binary->op->getText()),
          toExpr(binary->left),
          toExpr(binary->right));
    }
    if (auto* comparison = dynamic_cast<Parser::ComparisonContext*>(context)) {
      return SparkFunctions::call(
          comparison->comparisonOperator()->getText(),
          toExpr(comparison->left),
          toExpr(comparison->right));
    }
    notSupported("expression", context);
  }

  lp::ExprApi toExpr(Parser::PrimaryExpressionContext* context) {
    if (auto* constant =
            dynamic_cast<Parser::ConstantDefaultContext*>(context)) {
      return toLiteral(constant->constant(), sessionTimeZone_);
    }
    if (auto* column = dynamic_cast<Parser::ColumnReferenceContext*>(context)) {
      return lp::Col(toName(column->identifier()));
    }
    if (auto* dereference =
            dynamic_cast<Parser::DereferenceContext*>(context)) {
      auto field = toName(dereference->fieldName);
      if (isName(dereference->base)) {
        return lp::Col(field, toExpr(dereference->base));
      }
      return SparkFunctions::call(
          "subscript", toExpr(dereference->base), lp::Lit(field));
    }
    if (auto* function = dynamic_cast<Parser::FunctionCallContext*>(context)) {
      return toCall(function);
    }
    if (auto* parenthesized =
            dynamic_cast<Parser::ParenthesizedExpressionContext*>(context)) {
      return toExpr(parenthesized->expression());
    }
    if (auto* subscript = dynamic_cast<Parser::SubscriptContext*>(context)) {
      return SparkFunctions::call(
          "subscript", toExpr(subscript->value), toExpr(subscript->index));
    }
    if (auto* lambda = dynamic_cast<Parser::LambdaContext*>(context)) {
      std::vector<std::string> parameters;
      for (auto* identifier : lambda->identifier()) {
        parameters.push_back(toName(identifier));
      }
      return lp::Lambda(std::move(parameters), toExpr(lambda->expression()));
    }
    if (auto* cast = dynamic_cast<Parser::CastContext*>(context)) {
      auto type = toType(cast->dataType());
      auto input = toExpr(cast->expression());
      return cast->name->getType() == Parser::TRY_CAST
          ? lp::TryCast(std::move(type), input)
          : lp::Cast(std::move(type), input);
    }
    if (auto* searched = dynamic_cast<Parser::SearchedCaseContext*>(context)) {
      return toCase(
          std::nullopt, searched->whenClause(), searched->elseExpression);
    }
    if (auto* simple = dynamic_cast<Parser::SimpleCaseContext*>(context)) {
      return toCase(
          toExpr(simple->value), simple->whenClause(), simple->elseExpression);
    }
    notSupported("expression", context);
  }

  lp::ExprApi toCall(Parser::FunctionCallContext* context) {
    if (context->nullsOption != nullptr) {
      notSupported("IGNORE NULLS or RESPECT NULLS", context);
    }
    if (context->windowSpec() != nullptr) {
      notSupported("window function", context);
    }

    auto* name = context->functionName();
    std::vector<std::string> nameParts;
    if (name->qualifiedName() != nullptr) {
      for (auto* identifier : name->qualifiedName()->identifier()) {
        nameParts.push_back(toName(identifier));
      }
    } else {
      nameParts.push_back(name->getText());
    }
    const auto functionName =
        toLower(fmt::format("{}", fmt::join(nameParts, ".")));
    const bool distinct = context->setQuantifier() != nullptr &&
        context->setQuantifier()->DISTINCT() != nullptr;

    std::vector<lp::ExprApi> args;
    // count(*) counts rows: Spark rewrites it to count(1).
    if (functionName == "count" && !distinct && context->argument.size() == 1 &&
        context->argument[0]->getText() == "*") {
      args.push_back(lp::Lit(1));
    } else {
      for (auto* argument : context->argument) {
        args.push_back(toExpr(argument));
      }
    }

    auto result = SparkFunctions::call(functionName, std::move(args));
    if (distinct) {
      result = result.distinct();
    }
    if (context->where != nullptr) {
      result = result.filter(toExpr(context->where));
    }
    return result;
  }

  // A searched CASE is a call of when, with each condition followed by its
  // result and the ELSE result last. A simple CASE is a call of case, with
  // the value first, so that the value is evaluated once.
  lp::ExprApi toCase(
      const std::optional<lp::ExprApi>& value,
      const std::vector<Parser::WhenClauseContext*>& whenClauses,
      Parser::ExpressionContext* elseExpression) {
    std::vector<lp::ExprApi> args;
    if (value.has_value()) {
      args.push_back(*value);
    }
    for (auto* whenClause : whenClauses) {
      args.push_back(toExpr(whenClause->condition));
      args.push_back(toExpr(whenClause->result));
    }
    if (elseExpression != nullptr) {
      args.push_back(toExpr(elseExpression));
    }
    return value.has_value() ? lp::Call("case", args)
                             : SparkFunctions::call("when", std::move(args));
  }

  const tz::TimeZone& sessionTimeZone_;
};

} // namespace

SparkSqlParser::SparkSqlParser(const tz::TimeZone* sessionTimeZone)
    : sessionTimeZone_{sessionTimeZone} {
  VELOX_CHECK_NOT_NULL(sessionTimeZone_);
}

lp::ExprApi SparkSqlParser::parseExpression(std::string_view sql) const {
  ParserState state{sql};
  auto* tree = state.parser.singleExpression();
  state.checkErrors();
  return ExpressionBuilder(*sessionTimeZone_).toExpr(tree->namedExpression());
}

TypePtr SparkSqlParser::parseDataType(std::string_view sql) {
  ParserState state{sql};
  auto* tree = state.parser.singleDataType();
  state.checkErrors();
  return toType(tree->dataType());
}

} // namespace axiom::sql::spark
