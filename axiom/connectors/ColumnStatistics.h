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

#include "velox/type/Variant.h"

namespace facebook::axiom::connector {

/// Represents statistics of a column. The statistics may represent the column
/// across the table or may be calculated over a sample of a layout of the
/// table. All fields are optional.
struct ColumnStatistics {
  /// The table column's name. For a subfield, the struct member's name or the
  /// map key.
  std::string name;

  /// If true, the column cannot have nulls.
  bool nonNull{false};

  /// Observed percentage of nulls. 0 does not mean that there are no nulls.
  float nullPct{0};

  /// Minimum observed value for comparable scalar column.
  std::optional<velox::Variant> min;

  /// Maximum observed value for a comparable scalar column.
  std::optional<velox::Variant> max;

  /// For string, varbinary, array and map, the maximum observed number of
  /// characters/bytes/elements/key-value pairs.
  std::optional<int32_t> maxLength;

  /// Percentage of values where the next row is > the previous. 50 for a random
  /// distribution, 0 for descending, 100 for ascending.
  std::optional<float> ascendingPct;

  std::optional<float> descendingPct;

  /// Average count of characters/bytes/elements/key-value pairs.
  std::optional<int32_t> avgLength;

  /// Average size of the column's value per row in bytes, including nested
  /// children. Null rows count toward the average. Not populated for
  /// fixed-width types, whose size comes from the type.
  std::optional<int32_t> avgSizeInBytes;

  /// Estimated number of distinct values. Not specified for complex types.
  std::optional<int64_t> numDistinct;

  /// For a BOOLEAN column, the number of true values.
  std::optional<int64_t> numTrue;

  /// Count of non-nulls.
  int64_t numValues{0};

  /// For complex type columns, statistics of children. For array, contains one
  /// element describing the array elements. For struct, has one element for
  /// each member. For map, has an element for keys and one for values. For flat
  /// map, may have one element for each key. In all cases, stats may be
  /// missing.
  std::vector<ColumnStatistics> children;
};

} // namespace facebook::axiom::connector
