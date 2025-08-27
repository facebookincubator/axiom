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

  virtual bool match(const PlanNodePtr& plan) const = 0;
};

class PlanMatcherBuilder {
 public:
  PlanMatcherBuilder& tableScan();

  PlanMatcherBuilder& tableScan(const std::string& tableName);

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

  PlanMatcherBuilder& aggregation();

  PlanMatcherBuilder& partialAggregation();

  PlanMatcherBuilder& finalAggregation();

  PlanMatcherBuilder& hashJoin(
      const std::shared_ptr<PlanMatcher>& rightMatcher);

  PlanMatcherBuilder& hashJoin(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      JoinType joinType);

  PlanMatcherBuilder& localPartition();

  PlanMatcherBuilder& localPartition(
      const std::shared_ptr<PlanMatcher>& matcher);

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

  std::shared_ptr<PlanMatcher> build() {
    VELOX_USER_CHECK_NOT_NULL(matcher_, "Cannot build an empty PlanMatcher.");
    return matcher_;
  }

 private:
  std::shared_ptr<PlanMatcher> matcher_;
};

} // namespace facebook::velox::core
