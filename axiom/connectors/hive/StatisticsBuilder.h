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

/// Options for StatisticsBuilder.
struct StatisticsBuilderOptions {
  int32_t maxStringLength{100};
  int32_t initialSize{0};
  bool countDistincts{false};
  velox::HashStringAllocator* allocator{nullptr};
};

/// Abstract class for building statistics from samples.
class StatisticsBuilder {
 public:
  virtual ~StatisticsBuilder() = default;

  static std::unique_ptr<StatisticsBuilder> create(
      const velox::TypePtr& type,
      const StatisticsBuilderOptions& opts);

  static void updateBuilders(
      const velox::RowVectorPtr& data,
      std::vector<std::unique_ptr<StatisticsBuilder>>& builders);

  virtual const velox::TypePtr& type() const = 0;

  /// Accumulates elements of 'vector' into stats.
  virtual void add(const velox::VectorPtr& data) = 0;

  /// Merges the statistics of 'other' into 'this'.
  virtual void merge(const StatisticsBuilder& other) = 0;

  /// Fills 'result' with the accumulated stats. Scales up counts by
  /// 'sampleFraction', e.g. 0.1 means 10x.
  virtual void build(ColumnStatistics& result, float sampleFraction = 1) = 0;

  virtual int64_t numAscending() const = 0;
  virtual int64_t numRepeat() const = 0;
  virtual int64_t numDescending() const = 0;
};

} // namespace facebook::axiom::connector
