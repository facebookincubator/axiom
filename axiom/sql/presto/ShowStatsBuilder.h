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

#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace axiom::sql::presto {

/// Builds the result rows for SHOW STATS statements. The constructor adds the
/// summary row with the total row count, then addColumn appends per-column
/// stats rows. The result can be used to construct a VALUES plan or a RowVector
/// via BaseVector::createFromVariants.
///
/// Output schema:
///   row_count (BIGINT) - estimated rows, populated only in the summary row.
///   column_name (VARCHAR) - column name, NULL in the summary row.
///   nulls_fraction (DOUBLE) - fraction of null values.
///   distinct_values_count (BIGINT) - number of distinct values.
///   avg_length (BIGINT) - average length in bytes for varchar and varbinary;
///     average number of elements for arrays and maps.
///   low_value (VARCHAR) - minimum value formatted as string.
///   high_value (VARCHAR) - maximum value formatted as string.
class ShowStatsBuilder {
 public:
  /// @param rowCount Total row count for the summary row.
  explicit ShowStatsBuilder(int64_t rowCount);

  /// Adds a per-column stats row. Min/max Variant values are formatted using
  /// the column type (raw string for VARCHAR, toJson for other types). Null
  /// Variant pointers produce NULL. Returns *this for chaining.
  ShowStatsBuilder& addColumn(
      std::string_view name,
      const facebook::velox::Type& type,
      std::optional<double> nullsFraction,
      std::optional<int64_t> distinctCount,
      std::optional<int64_t> avgLength,
      const facebook::velox::Variant* min,
      const facebook::velox::Variant* max);

  /// Returns the output schema shared by all SHOW STATS variants.
  static const facebook::velox::RowTypePtr& outputType();

  /// Returns the accumulated rows: summary row followed by per-column rows.
  const std::vector<facebook::velox::Variant>& rows() const {
    return rows_;
  }

 private:
  // Summary row followed by per-column stats rows.
  std::vector<facebook::velox::Variant> rows_;
};

} // namespace axiom::sql::presto
