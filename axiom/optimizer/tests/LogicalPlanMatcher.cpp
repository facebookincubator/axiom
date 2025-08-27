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

#include "axiom/optimizer/tests/LogicalPlanMatcher.h"
#include <gtest/gtest.h>

namespace facebook::velox::logical_plan {
namespace {

template <typename T = LogicalPlanNode>
class LogicalPlanMatcherImpl : public LogicalPlanMatcher {
 public:
  LogicalPlanMatcherImpl() = default;

  explicit LogicalPlanMatcherImpl(
      const std::vector<std::shared_ptr<LogicalPlanMatcher>>& inputMatchers)
      : inputMatchers_{inputMatchers} {}

  explicit LogicalPlanMatcherImpl(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher)
      : inputMatchers_{{inputMatcher}} {}

  bool match(const LogicalPlanNodePtr& plan) const override {
    const auto* specificNode = dynamic_cast<const T*>(plan.get());
    EXPECT_TRUE(specificNode != nullptr)
        << "Expected " << folly::demangle(typeid(T).name()) << ", but got "
        << NodeKindName::toName(plan->kind());
    if (::testing::Test::HasNonfatalFailure()) {
      return false;
    }

    EXPECT_EQ(plan->inputs().size(), inputMatchers_.size());
    if (::testing::Test::HasNonfatalFailure()) {
      return false;
    }

    for (auto i = 0; i < inputMatchers_.size(); ++i) {
      EXPECT_TRUE(inputMatchers_[i]->match(plan->inputs()[i]));
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }
    }

    return matchDetails(*specificNode);
  }

 protected:
  virtual bool matchDetails(const T& plan) const {
    return true;
  }

  const std::vector<std::shared_ptr<LogicalPlanMatcher>> inputMatchers_;
};
} // namespace

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::tableScan() {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<TableScanNode>>();
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::filter() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<FilterNode>>(matcher_);
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::project() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<ProjectNode>>(matcher_);
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::aggregate() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<AggregateNode>>(matcher_);
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::unnest() {
  if (matcher_ != nullptr) {
    matcher_ = std::make_shared<LogicalPlanMatcherImpl<UnnestNode>>(matcher_);
  } else {
    matcher_ = std::make_shared<LogicalPlanMatcherImpl<UnnestNode>>();
  }

  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::join(
    const std::shared_ptr<LogicalPlanMatcher>& rightMatcher) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<JoinNode>>(
      std::vector<std::shared_ptr<LogicalPlanMatcher>>{matcher_, rightMatcher});
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::sort() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<SortNode>>(matcher_);
  return *this;
}

} // namespace facebook::velox::logical_plan
