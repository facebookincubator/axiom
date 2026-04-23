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

#include <fmt/format.h>
#include <grpcpp/grpcpp.h> // @manual=//grpc_fb/cpp:grpc
#include <exception>
#include <string>

namespace axiom::collagen {

// Code adapted from Velox Exception.

#define _COLLAGEN_THROW_IMPL(exprStr, exception, errorCode, ...) \
  do {                                                           \
    throw exception(                                             \
        errorCode,                                               \
        ::axiom::collagen::errorMessage(__VA_ARGS__),            \
        exprStr,                                                 \
        __FILE__,                                                \
        __LINE__,                                                \
        __FUNCTION__);                                           \
  } while (0)

#define _COLLAGEN_THROW(exception, ...) \
  _COLLAGEN_THROW_IMPL("", exception, ##__VA_ARGS__)

#define _COLLAGEN_CHECK_AND_THROW_IMPL(                                 \
    expr, exprStr, exception, errorCode, ...)                           \
  do {                                                                  \
    if (UNLIKELY(!(expr))) {                                            \
      _COLLAGEN_THROW_IMPL(exprStr, exception, errorCode, __VA_ARGS__); \
    }                                                                   \
  } while (0)

#define _COLLAGEN_CHECK_IMPL(expr, exprStr, ...) \
  _COLLAGEN_CHECK_AND_THROW_IMPL(                \
      expr,                                      \
      exprStr,                                   \
      ::axiom::collagen::CollagenException,      \
      ::grpc::StatusCode::UNKNOWN,               \
      ##__VA_ARGS__)

#define COLLAGEN_FAIL(...)                  \
  _COLLAGEN_THROW(                          \
      ::axiom::collagen::CollagenException, \
      ::grpc::StatusCode::UNKNOWN,          \
      ##__VA_ARGS__)

#define COLLAGEN_USER_FAIL(...)             \
  _COLLAGEN_THROW(                          \
      ::axiom::collagen::CollagenException, \
      ::grpc::StatusCode::INVALID_ARGUMENT, \
      ##__VA_ARGS__)

#define COLLAGEN_NYI(...)                   \
  _COLLAGEN_THROW(                          \
      ::axiom::collagen::CollagenException, \
      ::grpc::StatusCode::UNIMPLEMENTED,    \
      ##__VA_ARGS__)

#define _COLLAGEN_CHECK_OP_WITH_USER_FMT_HELPER( \
    implmacro, expr1, expr2, op, user_fmt, ...)  \
  implmacro(                                     \
      (expr1)op(expr2),                          \
      #expr1 " " #op " " #expr2,                 \
      "({} vs. {}) " user_fmt,                   \
      expr1,                                     \
      expr2,                                     \
      ##__VA_ARGS__)

#define _COLLAGEN_CHECK_OP_HELPER(implmacro, expr1, expr2, op, ...) \
  do {                                                              \
    if constexpr (FOLLY_PP_DETAIL_NARGS(__VA_ARGS__) > 0) {         \
      _COLLAGEN_CHECK_OP_WITH_USER_FMT_HELPER(                      \
          implmacro, expr1, expr2, op, __VA_ARGS__);                \
    } else {                                                        \
      implmacro(                                                    \
          (expr1)op(expr2),                                         \
          #expr1 " " #op " " #expr2,                                \
          "({} vs. {})",                                            \
          expr1,                                                    \
          expr2);                                                   \
    }                                                               \
  } while (0)

#define _COLLAGEN_CHECK_OP(expr1, expr2, op, ...) \
  _COLLAGEN_CHECK_OP_HELPER(                      \
      _COLLAGEN_CHECK_IMPL, expr1, expr2, op, ##__VA_ARGS__)

// For all below macros, an additional message can be passed using a
// format string and arguments, as with `fmt::format`.
#define COLLAGEN_CHECK(expr, ...) \
  _COLLAGEN_CHECK_IMPL(expr, #expr, ##__VA_ARGS__)
#define COLLAGEN_CHECK_GT(e1, e2, ...) \
  _COLLAGEN_CHECK_OP(e1, e2, >, ##__VA_ARGS__)
#define COLLAGEN_CHECK_GE(e1, e2, ...) \
  _COLLAGEN_CHECK_OP(e1, e2, >=, ##__VA_ARGS__)
#define COLLAGEN_CHECK_LT(e1, e2, ...) \
  _COLLAGEN_CHECK_OP(e1, e2, <, ##__VA_ARGS__)
#define COLLAGEN_CHECK_LE(e1, e2, ...) \
  _COLLAGEN_CHECK_OP(e1, e2, <=, ##__VA_ARGS__)
#define COLLAGEN_CHECK_EQ(e1, e2, ...) \
  _COLLAGEN_CHECK_OP(e1, e2, ==, ##__VA_ARGS__)
#define COLLAGEN_CHECK_NE(e1, e2, ...) \
  _COLLAGEN_CHECK_OP(e1, e2, !=, ##__VA_ARGS__)
#define COLLAGEN_CHECK_NULL(e, ...) COLLAGEN_CHECK(e == nullptr, ##__VA_ARGS__)
#define COLLAGEN_CHECK_NOT_NULL(e, ...) \
  COLLAGEN_CHECK(e != nullptr, ##__VA_ARGS__)

#define COLLAGEN_ARROW_CHECK(expr)                       \
  do {                                                   \
    ::arrow::Status _s = (expr);                         \
    _COLLAGEN_CHECK_IMPL(_s.ok(), #expr, _s.ToString()); \
  } while (false)

#define _COLLAGEN_ARROW_ASSIGN_OR_THROW_IMPL(result_name, lhs, rexpr) \
  auto&& result_name = (rexpr);                                       \
  _COLLAGEN_CHECK_IMPL(                                               \
      result_name.ok(), #rexpr, result_name.status().ToString());     \
  lhs = std::move(result_name).ValueUnsafe();

#define COLLAGEN_CONCAT_IMPL(x, y) x##y
#define COLLAGEN_CONCAT(x, y) COLLAGEN_CONCAT_IMPL(x, y)

#define COLLAGEN_ARROW_ASSIGN_OR_THROW(lhs, rexpr) \
  _COLLAGEN_ARROW_ASSIGN_OR_THROW_IMPL(            \
      COLLAGEN_CONCAT(_error_or_value, __COUNTER__), lhs, rexpr);

class CollagenException : public std::exception {
 public:
  CollagenException(
      grpc::StatusCode statusCode,
      std::string_view errorDescription,
      std::string_view errorExpression,
      std::string_view sourceFile,
      size_t sourceLine,
      std::string_view sourceFunction)
      : statusCode_(statusCode),
        errorMessage_(createErrorMessage(
            errorDescription,
            errorExpression,
            sourceFile,
            sourceLine,
            sourceFunction)) {}

  const char* what() const noexcept override {
    return errorMessage_.data();
  }

  grpc::Status status() const {
    return grpc::Status(statusCode_, what());
  }

 private:
  std::string createErrorMessage(
      std::string_view errorDescription,
      std::string_view errorExpression,
      std::string_view sourceFile,
      size_t sourceLine,
      std::string_view sourceFunction) {
    return fmt::format(
        "CollagenException: {}\n"
        "Status code: {}\n"
        "Expression: {}\n"
        "File: {}\n"
        "Line: {}\n"
        "Function: {}",
        errorDescription,
        statusCode_,
        errorExpression,
        sourceFile,
        sourceLine,
        sourceFunction);
  }

  const grpc::StatusCode statusCode_;
  const std::string errorMessage_;
};

struct StringEmpty {
  operator std::string_view() const {
    return "";
  }
};

inline StringEmpty errorMessage() {
  return {};
}

inline const char* errorMessage(const char* s) {
  return s;
}

inline std::string errorMessage(const std::string& str) {
  return str;
}

template <typename... Args>
std::string errorMessage(fmt::string_view fmt, const Args&... args) {
  return fmt::vformat(fmt, fmt::make_format_args(args...));
}

} // namespace axiom::collagen
