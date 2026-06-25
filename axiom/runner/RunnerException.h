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

#include <exception>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <folly/Likely.h>
#include "axiom/common/Enums.h"

namespace facebook::axiom::runner {

/// Classifies the reason for runner-level failure.
enum class RunnerErrorKind {
  /// Query execution exceeded configured time limit.
  kQueryTimedOut,
  /// Query was cancelled via abort().
  kCancelled,
  /// Generic execution failure.
  kExecutionError,
};

AXIOM_DECLARE_ENUM_NAME(RunnerErrorKind);

/// Exception thrown from Runner implementations for query-level failures
/// that originate at the coordinator / SQL runner layer rather than at
/// individual Velox worker tasks.
///
/// Throw via the macros below rather than constructing directly; they populate
/// messageTemplate from the format string for programmatic categorization:
///   AXIOM_QUERY_TIMEOUT("Query exceeded maximum time limit of {:.2f}s", 5.0);
///   AXIOM_RUNNER_FAIL(RunnerErrorKind::kCancelled, "Query {} cancelled", id);
///   AXIOM_RUNNER_CHECK(ok, "Unexpected state: {}", state);
class RunnerException : public std::exception {
 public:
  /// Constructs from an already-formatted `message` and the raw format-string
  /// `messageTemplate` it was rendered from. The template enables programmatic
  /// categorization regardless of which macro raised the error. Prefer the
  /// AXIOM_QUERY_TIMEOUT / AXIOM_RUNNER_FAIL / AXIOM_RUNNER_CHECK macros over
  /// constructing directly.
  RunnerException(
      RunnerErrorKind kind,
      std::string message,
      std::string messageTemplate);

  RunnerErrorKind kind() const noexcept {
    return kind_;
  }

  /// Raw error description without prefix.
  std::string_view message() const noexcept {
    return message_;
  }

  /// Raw format string before argument substitution for programmatic
  /// categorization.
  std::string_view messageTemplate() const noexcept {
    return messageTemplate_;
  }

  const char* what() const noexcept override {
    return formatted_.c_str();
  }

 private:
  static std::string formatWhat(
      RunnerErrorKind kind,
      std::string_view message) {
    return fmt::format("{}: {}", RunnerErrorKindName::toName(kind), message);
  }

  // Reason for the failure.
  RunnerErrorKind kind_;
  // Raw error description without the kind prefix.
  std::string message_;
  // Format string before argument substitution, for programmatic
  // categorization.
  std::string messageTemplate_;
  // Pre-formatted what() string combining the kind name and message, built once
  // in the constructor.
  std::string formatted_;
};

// In all three macros below, 'fmt_str' must be a string literal format string.
// It is used both to format the message and as the messageTemplate, so callers
// can categorize errors by the raw template regardless of which macro raised
// them.

/// Throws RunnerException if condition is false.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_RUNNER_CHECK(condition, fmt_str, ...)                    \
  do {                                                                 \
    if (FOLLY_UNLIKELY(!(condition))) {                                \
      throw ::facebook::axiom::runner::RunnerException(                \
          ::facebook::axiom::runner::RunnerErrorKind::kExecutionError, \
          fmt::format(fmt_str, ##__VA_ARGS__),                         \
          fmt_str);                                                    \
    }                                                                  \
  } while (0)

/// Unconditionally throws RunnerException with specified kind.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_RUNNER_FAIL(kind, fmt_str, ...)                \
  do {                                                       \
    throw ::facebook::axiom::runner::RunnerException(        \
        kind, fmt::format(fmt_str, ##__VA_ARGS__), fmt_str); \
  } while (0)

/// Throws RunnerException with kQueryTimedOut kind.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_QUERY_TIMEOUT(fmt_str, ...)                           \
  do {                                                              \
    throw ::facebook::axiom::runner::RunnerException(               \
        ::facebook::axiom::runner::RunnerErrorKind::kQueryTimedOut, \
        fmt::format(fmt_str, ##__VA_ARGS__),                        \
        fmt_str);                                                   \
  } while (0)

} // namespace facebook::axiom::runner

// Provide formatter specialization for RunnerErrorKind.
AXIOM_ENUM_FORMATTER(facebook::axiom::runner::RunnerErrorKind);
