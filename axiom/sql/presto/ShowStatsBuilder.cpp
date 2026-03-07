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

#include "axiom/sql/presto/ShowStatsBuilder.h"

namespace axiom::sql::presto {

using namespace facebook::velox;

namespace {
const auto kNullBigint = Variant::null(TypeKind::BIGINT);
const auto kNullDouble = Variant::null(TypeKind::DOUBLE);
const auto kNullVarchar = Variant::null(TypeKind::VARCHAR);

Variant toVariant(const std::optional<double>& value) {
  return value.has_value() ? Variant(*value) : kNullDouble;
}

Variant toVariant(const std::optional<int64_t>& value) {
  return value.has_value() ? Variant(*value) : kNullBigint;
}

// Formats a Variant value for display. Returns the raw string for VARCHAR,
// otherwise uses type-aware formatting.
std::string formatValue(const Variant& value, const Type& type) {
  if (value.kind() == TypeKind::VARCHAR) {
    return value.value<TypeKind::VARCHAR>();
  }
  return value.toJson(type);
}
} // namespace

ShowStatsBuilder::ShowStatsBuilder(int64_t rowCount) {
  rows_.emplace_back(
      Variant::row({
          Variant(rowCount),
          kNullVarchar,
          kNullDouble,
          kNullBigint,
          kNullBigint,
          kNullVarchar,
          kNullVarchar,
      }));
}

ShowStatsBuilder& ShowStatsBuilder::addColumn(
    std::string_view name,
    const Type& type,
    std::optional<double> nullsFraction,
    std::optional<int64_t> distinctCount,
    std::optional<int64_t> avgLength,
    const Variant* min,
    const Variant* max) {
  auto lowValue = (min != nullptr && !min->isNull())
      ? Variant(formatValue(*min, type))
      : kNullVarchar;
  auto highValue = (max != nullptr && !max->isNull())
      ? Variant(formatValue(*max, type))
      : kNullVarchar;

  rows_.emplace_back(
      Variant::row({
          kNullBigint,
          Variant(std::string(name)),
          toVariant(nullsFraction),
          toVariant(distinctCount),
          toVariant(avgLength),
          lowValue,
          highValue,
      }));
  return *this;
}

const RowTypePtr& ShowStatsBuilder::outputType() {
  static const auto kType = ROW(
      {
          "row_count",
          "column_name",
          "nulls_fraction",
          "distinct_values_count",
          "avg_length",
          "low_value",
          "high_value",
      },
      {
          BIGINT(),
          VARCHAR(),
          DOUBLE(),
          BIGINT(),
          BIGINT(),
          VARCHAR(),
          VARCHAR(),
      });
  return kType;
}

} // namespace axiom::sql::presto
