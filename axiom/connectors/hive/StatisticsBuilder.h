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

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::axiom::connector {

struct ColumnStatistics;

struct StatisticsBuilderOptions {
  /// Maximum length of strings to track for min/max statistics. If actual min
  /// or max string exceeds this limit, min/max stats are dropped entirely.
  int32_t maxStringLength{100};

  /// Whether to count distinct values. Requires 'allocator' to be set.
  bool countDistincts{false};

  /// Allocator for counting distinct values. Required if 'countDistincts' is
  /// true.
  velox::HashStringAllocator* allocator{nullptr};
};

/// Abstract class for building statistics from samples.
class StatisticsBuilder {
 public:
  virtual ~StatisticsBuilder() = default;

  /// Creates a StatisticsBuilder for the given type. Returns nullptr for
  /// unsupported types. Supported types: SMALLINT, INTEGER, BIGINT, REAL,
  /// DOUBLE, VARCHAR.
  static std::unique_ptr<StatisticsBuilder> create(
      const velox::TypePtr& type,
      const StatisticsBuilderOptions& options);

  /// Adds data from each column of 'data' to the corresponding builder in
  /// 'builders'. Skips null builders. 'builders.size()' must be <=
  /// 'data->childrenSize()'.
  static void updateBuilders(
      const velox::RowVectorPtr& data,
      std::vector<std::unique_ptr<StatisticsBuilder>>& builders);

  virtual const velox::TypePtr& type() const = 0;

  /// Accumulates elements of 'data' into stats. 'data' type must match type().
  /// Note: Batch size matters. Ordering statistics (numAscending,
  /// numDescending, numRepeat) are tracked per batch. For strings, min/max are
  /// checked against maxStringLength per batch; adding data in one large batch
  /// vs multiple smaller batches may produce different results.
  virtual void add(const velox::VectorPtr& data) = 0;

  /// Combines statistics from 'other' into 'this'. Used to aggregate statistics
  /// collected from multiple data batches or partitions. The result is the same
  /// as if all the data was added to a single builder. Both builders must have
  /// the same 'countDistincts' setting.
  virtual void merge(const StatisticsBuilder& other) = 0;

  /// Fills 'result' with the accumulated stats. Does not modify the builder's
  /// state. It is safe to continue adding data or merging after calling this.
  ///
  /// Populates: min, max, nullPct, numDistinct, avgLength (for strings),
  /// ascendingPct, descendingPct.
  ///
  /// Does NOT populate: maxLength.
  virtual void build(ColumnStatistics& result) const = 0;
};

} // namespace facebook::axiom::connector
