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
  LogicalPlanMatcherBuilder& tableWrite();

  LogicalPlanMatcherBuilder& tableScan();

  LogicalPlanMatcherBuilder& values();

  LogicalPlanMatcherBuilder& filter();

  LogicalPlanMatcherBuilder& project();

  LogicalPlanMatcherBuilder& aggregate();

  LogicalPlanMatcherBuilder& unnest();

  LogicalPlanMatcherBuilder& join(
      const std::shared_ptr<LogicalPlanMatcher>& rightMatcher);

  LogicalPlanMatcherBuilder& sort();

  std::shared_ptr<LogicalPlanMatcher> build() {
    VELOX_USER_CHECK_NOT_NULL(
        matcher_, "Cannot build an empty LogicalPlanMatcher.");
    return matcher_;
  }

 private:
  std::shared_ptr<LogicalPlanMatcher> matcher_;
};

} // namespace facebook::axiom::logical_plan::test
