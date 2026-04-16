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

namespace axiom::sql::presto {

namespace {
constexpr const char* prefixFor(PrestoSqlErrorKind kind) {
  switch (kind) {
    case PrestoSqlErrorKind::kSyntax:
      return "Syntax error";
    case PrestoSqlErrorKind::kSemantic:
      return "Semantic error";
  }
}

const auto& prestoSqlErrorKindNames() {
  static const folly::F14FastMap<PrestoSqlErrorKind, std::string_view> kNames =
      {
          {PrestoSqlErrorKind::kSyntax, "Syntax"},
          {PrestoSqlErrorKind::kSemantic, "Semantic"},
      };
  return kNames;
}
} // namespace

AXIOM_DEFINE_ENUM_NAME(PrestoSqlErrorKind, prestoSqlErrorKindNames);

/* static */ std::string PrestoSqlError::formatWhat(
    PrestoSqlErrorKind kind,
    size_t line,
    size_t column,
    const std::optional<std::string>& token,
    const std::string& message) {
  auto prefix = prefixFor(kind);
  if (token.has_value() && !token->empty()) {
    return fmt::format(
        "{} at {}:{} near '{}': {}",
        prefix,
        line,
        column,
        token.value(),
        message);
  }
  return fmt::format("{} at {}:{}: {}", prefix, line, column, message);
}

} // namespace axiom::sql::presto
