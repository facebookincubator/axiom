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

#include "axiom/sql/presto/ParserOptions.h"

#include <fmt/format.h>
#include <folly/Conv.h>

#include "velox/common/base/Exceptions.h"

namespace axiom::sql::presto {

using facebook::velox::config::ConfigProperty;
using facebook::velox::config::ConfigPropertyType;

namespace {

std::vector<ConfigProperty> buildProperties(
    const std::unordered_map<std::string, std::string>& configOverrides) {
  std::vector<ConfigProperty> properties = {
      {
          std::string(ParserOptions::kFriendlySql),
          ConfigPropertyType::kBoolean,
          fmt::to_string(ParserOptions::kFriendlySqlDefault),
          "Enable Friendly SQL extensions: named ROW constructors, trailing "
          "commas, FROM-first syntax, etc.",
      },
      {
          std::string(ParserOptions::kParseDecimalLiteralAsDouble),
          ConfigPropertyType::kBoolean,
          fmt::to_string(ParserOptions::kParseDecimalLiteralAsDoubleDefault),
          "Parse decimal literals (e.g. 1.5) as DOUBLE. When false, parse as "
          "DECIMAL with inferred precision and scale.",
      },
      {
          std::string(ParserOptions::kMaxExpressionDepth),
          ConfigPropertyType::kInteger,
          fmt::to_string(ParserOptions::kMaxExpressionDepthDefault),
          "Maximum expression nesting depth; deeper expressions are rejected "
          "to avoid a stack overflow.",
      },
      {
          std::string(ParserOptions::kMaxSubqueryDepth),
          ConfigPropertyType::kInteger,
          fmt::to_string(ParserOptions::kMaxSubqueryDepthDefault),
          "Maximum subquery nesting depth; deeper nesting is rejected to avoid "
          "a stack overflow.",
      },
  };

  if (configOverrides.empty()) {
    return properties;
  }

  std::unordered_map<std::string, size_t> propertyIndex;
  for (size_t i = 0; i < properties.size(); ++i) {
    propertyIndex[properties[i].name] = i;
  }
  for (const auto& [key, value] : configOverrides) {
    auto it = propertyIndex.find(key);
    VELOX_USER_CHECK(
        it != propertyIndex.end(),
        "Unknown session property in parser config: {}",
        key);
    properties[it->second].defaultValue = value;
  }

  return properties;
}

} // namespace

ParserOptions::ParserOptions() : properties_(buildProperties({})) {}

ParserOptions::ParserOptions(
    const std::unordered_map<std::string, std::string>& configOverrides)
    : properties_(buildProperties(configOverrides)) {}

std::string ParserOptions::normalize(
    std::string_view /*name*/,
    std::string_view value) const {
  return std::string(value);
}

ParserOptions ParserOptions::from(
    const folly::F14FastMap<std::string, std::string>& properties) {
  auto setBool = [&](std::string_view key, bool& field) {
    auto it = properties.find(key);
    if (it != properties.end()) {
      field = it->second == "true";
    }
  };
  auto setUint32 = [&](std::string_view key, uint32_t& field) {
    auto it = properties.find(key);
    if (it != properties.end()) {
      field = folly::to<uint32_t>(it->second);
    }
  };

  ParserOptions options;
  setBool(kFriendlySql, options.friendlySql);
  setBool(kParseDecimalLiteralAsDouble, options.parseDecimalLiteralAsDouble);
  setUint32(kMaxExpressionDepth, options.maxExpressionDepth);
  setUint32(kMaxSubqueryDepth, options.maxSubqueryDepth);
  return options;
}

} // namespace axiom::sql::presto
