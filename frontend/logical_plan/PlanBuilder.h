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

#include "velox/frontend/logical_plan/LogicalPlanNode.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/PlanNodeIdGenerator.h"

namespace facebook::velox::logical_plan {

class PlanBuilder {
 public:
  PlanBuilder()
      : planNodeIdGenerator_(std::make_shared<core::PlanNodeIdGenerator>()) {}

  explicit PlanBuilder(
      std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator)
      : planNodeIdGenerator_{std::move(planNodeIdGenerator)} {
    VELOX_CHECK(planNodeIdGenerator_ != nullptr);
  }

  PlanBuilder& values(const RowTypePtr& rowType, std::vector<Variant> rows);

  PlanBuilder& tableScan(
      const std::string& connectorId,
      const std::string& tableName,
      const std::vector<std::string>& columnNames);

  PlanBuilder& filter(const std::string& predicate);

  PlanBuilder& project(const std::vector<std::string>& projections);

  /// An alias for 'project'.
  PlanBuilder& map(const std::vector<std::string>& projections) {
    return project(projections);
  }

  /// Similar to 'project', but appends 'projections' to the existing columns.
  PlanBuilder& with(const std::vector<std::string>& projections);

  PlanBuilder& aggregate(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates);

  PlanBuilder& join(
      const LogicalPlanNodePtr& right,
      const std::string& condition,
      JoinType joinType);

  PlanBuilder& sort(const std::vector<std::string>& sortingKeys);

  /// An alias for 'sort'.
  PlanBuilder& orderBy(const std::vector<std::string>& sortingKeys) {
    return sort(sortingKeys);
  }

  PlanBuilder& limit(int32_t count) {
    return limit(0, count);
  }

  PlanBuilder& limit(int32_t offset, int32_t count);

  const LogicalPlanNodePtr& build() {
    return node_;
  }

 private:
  std::string nextId() {
    return planNodeIdGenerator_->next();
  }

  const std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator_;
  const parse::ParseOptions parseOptions_;

  LogicalPlanNodePtr node_;
};

} // namespace facebook::velox::logical_plan
