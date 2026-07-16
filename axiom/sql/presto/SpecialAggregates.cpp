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
#include "axiom/sql/presto/SpecialAggregates.h"

#include <array>
#include <cctype>

namespace axiom::sql::presto {

namespace {
// Case-insensitive comparison against an already-lowercase candidate, without
// allocating (this runs for every function call while parsing).
bool equalsLowercase(std::string_view name, std::string_view lowercase) {
  if (name.size() != lowercase.size()) {
    return false;
  }
  for (size_t i = 0; i < name.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(name[i])) != lowercase[i]) {
      return false;
    }
  }
  return true;
}
} // namespace

std::optional<lp::SpecialAggregateKind> specialAggregateKind(
    std::string_view name) {
  static constexpr std::array<
      std::pair<std::string_view, lp::SpecialAggregateKind>,
      3>
      kByName = {{
          {"approx_count_star", lp::SpecialAggregateKind::kMetadataRowCount},
          {"approx_null_count", lp::SpecialAggregateKind::kMetadataNullCount},
          {"approx_non_null_count",
           lp::SpecialAggregateKind::kMetadataNonNullCount},
      }};

  for (const auto& [candidate, kind] : kByName) {
    if (equalsLowercase(name, candidate)) {
      return kind;
    }
  }
  return std::nullopt;
}

} // namespace axiom::sql::presto
