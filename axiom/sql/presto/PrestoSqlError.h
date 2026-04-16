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

#pragma once

#include <cassert>
#include <exception>
#include <optional>
#include <string>

#include <fmt/format.h>
#include <folly/Likely.h>
#include "axiom/common/Enums.h"

namespace axiom::sql::presto {

/// Classifies the phase where the error originated.
enum class PrestoSqlErrorKind {
  /// Syntax errors from the ANTLR parser or AST builder.
  kSyntax,
  /// Type errors, resolution errors, and other semantic checks.
  kSemantic,
};

AXIOM_DECLARE_ENUM_NAME(PrestoSqlErrorKind);

/// Exception thrown when a Presto SQL query fails parsing, type checking,
/// or name resolution. The `kind()` accessor indicates which phase
/// produced the error.
class PrestoSqlError : public std::exception {
 public:
  PrestoSqlError(
      const std::string& message,
      size_t line,
      size_t column,
      std::optional<std::string> token,
      PrestoSqlErrorKind kind = PrestoSqlErrorKind::kSyntax,
      std::string messageTemplate = "")
      : formatted_(formatWhat(kind, line, column, token, message)),
        messageTemplate_(std::move(messageTemplate)),
        messageLength_(message.size()),
        line_(line),
        column_(column),
        token_(std::move(token)),
        kind_(kind) {}

  /// 0-based line number.
  size_t line() const {
    return line_;
  }

  /// 0-based column number.
  size_t column() const {
    return column_;
  }

  /// Offending token text. Empty when no specific token is available.
  const std::optional<std::string>& token() const {
    return token_;
  }

  /// Raw error description without the line:column prefix.
  std::string_view message() const noexcept {
    return std::string_view(formatted_)
        .substr(formatted_.size() - messageLength_);
  }

  /// Raw format string before argument substitution. Enables programmatic
  /// error categorization (e.g. grouping all "Table not found: {}" errors
  /// regardless of the table name).
  std::string_view messageTemplate() const noexcept {
    return messageTemplate_;
  }

  PrestoSqlErrorKind kind() const noexcept {
    return kind_;
  }

  const char* what() const noexcept override {
    return formatted_.data();
  }

  /// Returns a copy with adjusted line and column.
  PrestoSqlError withOffset(size_t lineOffset, size_t columnOffset) const {
    return PrestoSqlError(
        std::string(message()),
        line_ + lineOffset,
        column_ + columnOffset,
        token_,
        kind_,
        messageTemplate_);
  }

 private:
  static std::string formatWhat(
      PrestoSqlErrorKind kind,
      size_t line,
      size_t column,
      const std::optional<std::string>& token,
      const std::string& message);

  std::string formatted_;
  std::string messageTemplate_;
  size_t messageLength_;
  size_t line_;
  size_t column_;
  std::optional<std::string> token_;
  PrestoSqlErrorKind kind_;
};

/// Throws PrestoSqlError with kSyntax kind if `condition` is false.
/// @param condition  Boolean expression to check.
/// @param location   A raw NodeLocation whose `line` is 1-based (as stored
///                   by AST nodes and returned by getLocation(ctx)). The
///                   macro converts to 0-based internally. Do NOT pass an
///                   already-0-based line value (e.g. from ErrorListener).
/// @param token      Offending token text (string or std::nullopt).
/// @param fmt_str    fmt::format format string.
/// @param ...        fmt::format arguments (may be empty).
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_PRESTO_SYNTAX_CHECK(condition, location, token, fmt_str, ...) \
  do {                                                                      \
    if (FOLLY_UNLIKELY(!(condition))) {                                     \
      assert((location).line > 0 && "Location must have 1-based line");     \
      throw ::axiom::sql::presto::PrestoSqlError(                           \
          fmt::format(fmt_str, ##__VA_ARGS__),                              \
          static_cast<size_t>((location).line - 1),                         \
          static_cast<size_t>(std::max(0, (location).charPosition)),        \
          (token),                                                          \
          ::axiom::sql::presto::PrestoSqlErrorKind::kSyntax,                \
          fmt_str);                                                         \
    }                                                                       \
  } while (0)

/// Unconditionally throws PrestoSqlError with kSyntax kind.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_PRESTO_SYNTAX_FAIL(location, token, fmt_str, ...)       \
  do {                                                                \
    assert((location).line > 0 && "Location must have 1-based line"); \
    throw ::axiom::sql::presto::PrestoSqlError(                       \
        fmt::format(fmt_str, ##__VA_ARGS__),                          \
        static_cast<size_t>((location).line - 1),                     \
        static_cast<size_t>(std::max(0, (location).charPosition)),    \
        (token),                                                      \
        ::axiom::sql::presto::PrestoSqlErrorKind::kSyntax,            \
        fmt_str);                                                     \
  } while (0)

/// Throws PrestoSqlError with kSemantic kind if `condition` is false.
/// Same parameter contract as AXIOM_PRESTO_SYNTAX_CHECK.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_PRESTO_SEMANTIC_CHECK(condition, location, token, fmt_str, ...) \
  do {                                                                        \
    if (FOLLY_UNLIKELY(!(condition))) {                                       \
      assert((location).line > 0 && "Location must have 1-based line");       \
      throw ::axiom::sql::presto::PrestoSqlError(                             \
          fmt::format(fmt_str, ##__VA_ARGS__),                                \
          static_cast<size_t>((location).line - 1),                           \
          static_cast<size_t>(std::max(0, (location).charPosition)),          \
          (token),                                                            \
          ::axiom::sql::presto::PrestoSqlErrorKind::kSemantic,                \
          fmt_str);                                                           \
    }                                                                         \
  } while (0)

/// Unconditionally throws PrestoSqlError with kSemantic kind.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_PRESTO_SEMANTIC_FAIL(location, token, fmt_str, ...)     \
  do {                                                                \
    assert((location).line > 0 && "Location must have 1-based line"); \
    throw ::axiom::sql::presto::PrestoSqlError(                       \
        fmt::format(fmt_str, ##__VA_ARGS__),                          \
        static_cast<size_t>((location).line - 1),                     \
        static_cast<size_t>(std::max(0, (location).charPosition)),    \
        (token),                                                      \
        ::axiom::sql::presto::PrestoSqlErrorKind::kSemantic,          \
        fmt_str);                                                     \
  } while (0)

/// Throws PrestoSqlError with kSemantic kind if val1 != val2.
/// Automatically appends ": {val1} vs {val2}" to the error message.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_PRESTO_SEMANTIC_CHECK_EQ(                                 \
    val1, val2, location, token, fmt_str, ...)                          \
  do {                                                                  \
    const auto _axiom_v1 = (val1);                                      \
    const auto _axiom_v2 = (val2);                                      \
    if (FOLLY_UNLIKELY(_axiom_v1 != _axiom_v2)) {                       \
      assert((location).line > 0 && "Location must have 1-based line"); \
      throw ::axiom::sql::presto::PrestoSqlError(                       \
          fmt::format(fmt_str, ##__VA_ARGS__) +                         \
              fmt::format(": {} vs {}", _axiom_v1, _axiom_v2),          \
          static_cast<size_t>((location).line - 1),                     \
          static_cast<size_t>(std::max(0, (location).charPosition)),    \
          (token),                                                      \
          ::axiom::sql::presto::PrestoSqlErrorKind::kSemantic,          \
          fmt_str);                                                     \
    }                                                                   \
  } while (0)

/// Throws PrestoSqlError with kSemantic kind if val1 > val2.
/// Automatically appends ": {val1} vs {val2}" to the error message.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_PRESTO_SEMANTIC_CHECK_LE(                                 \
    val1, val2, location, token, fmt_str, ...)                          \
  do {                                                                  \
    const auto _axiom_v1 = (val1);                                      \
    const auto _axiom_v2 = (val2);                                      \
    if (FOLLY_UNLIKELY(_axiom_v1 > _axiom_v2)) {                        \
      assert((location).line > 0 && "Location must have 1-based line"); \
      throw ::axiom::sql::presto::PrestoSqlError(                       \
          fmt::format(fmt_str, ##__VA_ARGS__) +                         \
              fmt::format(": {} vs {}", _axiom_v1, _axiom_v2),          \
          static_cast<size_t>((location).line - 1),                     \
          static_cast<size_t>(std::max(0, (location).charPosition)),    \
          (token),                                                      \
          ::axiom::sql::presto::PrestoSqlErrorKind::kSemantic,          \
          fmt_str);                                                     \
    }                                                                   \
  } while (0)

/// Throws PrestoSqlError with kSemantic kind if val1 < val2.
/// Automatically appends ": {val1} vs {val2}" to the error message.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_PRESTO_SEMANTIC_CHECK_GE(                                 \
    val1, val2, location, token, fmt_str, ...)                          \
  do {                                                                  \
    const auto _axiom_v1 = (val1);                                      \
    const auto _axiom_v2 = (val2);                                      \
    if (FOLLY_UNLIKELY(_axiom_v1 < _axiom_v2)) {                        \
      assert((location).line > 0 && "Location must have 1-based line"); \
      throw ::axiom::sql::presto::PrestoSqlError(                       \
          fmt::format(fmt_str, ##__VA_ARGS__) +                         \
              fmt::format(": {} vs {}", _axiom_v1, _axiom_v2),          \
          static_cast<size_t>((location).line - 1),                     \
          static_cast<size_t>(std::max(0, (location).charPosition)),    \
          (token),                                                      \
          ::axiom::sql::presto::PrestoSqlErrorKind::kSemantic,          \
          fmt_str);                                                     \
    }                                                                   \
  } while (0)

} // namespace axiom::sql::presto

AXIOM_ENUM_FORMATTER(axiom::sql::presto::PrestoSqlErrorKind);
