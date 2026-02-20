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

#include "axiom/logical_plan/LogicalPlanNode.h"

namespace facebook::axiom::logical_plan::test {

class LogicalPlanMatcher {
 public:
  virtual ~LogicalPlanMatcher() = default;

  virtual bool match(const LogicalPlanNodePtr& plan) const = 0;
};

class LogicalPlanMatcherBuilder {
 public:
  using OnMatchCallback = std::function<void(const LogicalPlanNodePtr&)>;

  LogicalPlanMatcherBuilder& tableWrite(OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& tableScan(OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& values(OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& values(
      velox::RowTypePtr outputType,
      OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& filter(OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& project(OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& aggregate(OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& unnest(OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& join(
      const std::shared_ptr<LogicalPlanMatcher>& rightMatcher,
      OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& setOperation(
      SetOperation op,
      const std::shared_ptr<LogicalPlanMatcher>& matcher,
      OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& sort(OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& sample(OnMatchCallback onMatch = nullptr);

  LogicalPlanMatcherBuilder& outputColumns(std::vector<std::string> expected);

  std::shared_ptr<LogicalPlanMatcher> build() {
    VELOX_USER_CHECK_NOT_NULL(
        matcher_, "Cannot build an empty LogicalPlanMatcher.");
    return matcher_;
  }

 private:
  std::shared_ptr<LogicalPlanMatcher> matcher_;
};

} // namespace facebook::axiom::logical_plan::test
