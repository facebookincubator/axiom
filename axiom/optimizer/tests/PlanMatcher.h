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

#include "velox/core/PlanNode.h"
#include "velox/type/Filter.h"

namespace facebook::velox::core {

class PlanMatcher {
 public:
  virtual ~PlanMatcher() = default;

  struct MatchResult {
    const bool match;

    /// Mapping from an alias specified in the PlanMatcher to the actual symbol
    /// found in the plan.
    const std::unordered_map<std::string, std::string> symbols;

    static MatchResult success(
        std::unordered_map<std::string, std::string> symbols = {}) {
      return MatchResult{true, std::move(symbols)};
    }

    static MatchResult failure() {
      return MatchResult{false, {}};
    }
  };

  bool match(const PlanNodePtr& plan) const {
    return match(plan, {}).match;
  }

  virtual MatchResult match(
      const PlanNodePtr& plan,
      const std::unordered_map<std::string, std::string>& symbols) const = 0;
};

class PlanMatcherBuilder {
 public:
  PlanMatcherBuilder& tableScan();

  PlanMatcherBuilder& tableScan(const std::string& tableName);

  /// @param tableName The name of the table.
  /// @param outputType The list of schema names and types of columns in the
  /// output of the scan node.
  PlanMatcherBuilder& tableScan(
      const std::string& tableName,
      const RowTypePtr& outputType);

  PlanMatcherBuilder& hiveScan(
      const std::string& tableName,
      common::SubfieldFilters subfieldFilters,
      const std::string& remainingFilter = "");

  PlanMatcherBuilder& values();

  PlanMatcherBuilder& values(const TypePtr& type);

  PlanMatcherBuilder& filter();

  PlanMatcherBuilder& filter(const std::string& predicate);

  PlanMatcherBuilder& project();

  PlanMatcherBuilder& project(const std::vector<std::string>& expressions);

  PlanMatcherBuilder& parallelProject();

  PlanMatcherBuilder& parallelProject(
      const std::vector<std::string>& expressions);

  PlanMatcherBuilder& unnest();

  PlanMatcherBuilder& unnest(
      const std::vector<std::string>& replicateExprs,
      const std::vector<std::string>& unnestExprs);

  PlanMatcherBuilder& aggregation();

  PlanMatcherBuilder& singleAggregation();

  PlanMatcherBuilder& singleAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates);

  PlanMatcherBuilder& partialAggregation();

  PlanMatcherBuilder& partialAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates);

  PlanMatcherBuilder& finalAggregation();

  PlanMatcherBuilder& finalAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates);

  PlanMatcherBuilder& hashJoin(
      const std::shared_ptr<PlanMatcher>& rightMatcher);

  PlanMatcherBuilder& hashJoin(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      JoinType joinType);

  PlanMatcherBuilder& localPartition();

  PlanMatcherBuilder& localPartition(
      std::initializer_list<std::shared_ptr<PlanMatcher>> matcher);

  PlanMatcherBuilder& localPartition(
      const std::shared_ptr<PlanMatcher>& matcher) {
    return localPartition({matcher});
  }

  PlanMatcherBuilder& localMerge();

  PlanMatcherBuilder& partitionedOutput();

  PlanMatcherBuilder& exchange();

  PlanMatcherBuilder& mergeExchange();

  PlanMatcherBuilder& limit();

  PlanMatcherBuilder& partialLimit(int64_t offset, int64_t count);

  PlanMatcherBuilder& finalLimit(int64_t offset, int64_t count);

  PlanMatcherBuilder& topN();

  PlanMatcherBuilder& topN(int64_t count);

  PlanMatcherBuilder& orderBy();

  PlanMatcherBuilder& orderBy(const std::vector<std::string>& ordering);

  PlanMatcherBuilder& tableWrite();

  std::shared_ptr<PlanMatcher> build() {
    VELOX_USER_CHECK_NOT_NULL(matcher_, "Cannot build an empty PlanMatcher.");
    return matcher_;
  }

 private:
  std::shared_ptr<PlanMatcher> matcher_;
};

} // namespace facebook::velox::core
