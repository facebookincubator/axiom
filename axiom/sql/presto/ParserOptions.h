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

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <folly/container/F14Map.h>

#include "velox/common/config/ConfigProvider.h"

namespace axiom::sql::presto {

/// Options controlling SQL parsing behavior.
struct ParserOptions : public facebook::velox::config::ConfigProvider {
  static constexpr std::string_view kFriendlySql = "friendly_sql";
  static constexpr std::string_view kParseDecimalLiteralAsDouble =
      "parse_decimal_literal_as_double";
  static constexpr std::string_view kMaxExpressionDepth =
      "max_expression_depth";
  static constexpr std::string_view kMaxSubqueryDepth = "max_subquery_depth";

  static constexpr bool kFriendlySqlDefault = true;
  static constexpr bool kParseDecimalLiteralAsDoubleDefault = true;
  static constexpr uint32_t kMaxExpressionDepthDefault = 512;
  static constexpr uint32_t kMaxSubqueryDepthDefault = 1024;

  ParserOptions();

  /// Overrides code defaults with values from `configOverrides`.
  explicit ParserOptions(
      const std::unordered_map<std::string, std::string>& configOverrides);

  /// Enables Friendly SQL extensions inspired by DuckDB: named ROW
  /// constructors, trailing commas, FROM-first syntax, etc.
  bool friendlySql{kFriendlySqlDefault};

  /// When true, decimal literals (e.g., 1.5) are parsed as DOUBLE. When
  /// false, they are parsed as DECIMAL with precision and scale inferred
  /// from the literal. Matches Presto's decimal_literal_result_type session
  /// property.
  bool parseDecimalLiteralAsDouble{kParseDecimalLiteralAsDoubleDefault};

  /// Maximum expression nesting depth; deeper expressions are rejected to
  /// avoid a stack overflow.
  uint32_t maxExpressionDepth{kMaxExpressionDepthDefault};

  /// Maximum subquery nesting depth; deeper nesting is rejected to avoid a
  /// stack overflow.
  uint32_t maxSubqueryDepth{kMaxSubqueryDepthDefault};

  /// Constructs options from session property name-value pairs.
  /// Keys are unqualified property names (e.g., "friendly_sql").
  /// Missing keys use struct defaults.
  static ParserOptions from(
      const folly::F14FastMap<std::string, std::string>& properties);

  std::vector<facebook::velox::config::ConfigProperty> properties()
      const override {
    return properties_;
  }

  std::string normalize(std::string_view name, std::string_view value)
      const override;

 private:
  std::vector<facebook::velox::config::ConfigProperty> properties_;
};

} // namespace axiom::sql::presto
