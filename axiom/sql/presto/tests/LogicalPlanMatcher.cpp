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

#include "axiom/sql/presto/tests/LogicalPlanMatcher.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace facebook::axiom::logical_plan::test {
namespace {

template <typename T = LogicalPlanNode>
class LogicalPlanMatcherImpl : public LogicalPlanMatcher {
 public:
  explicit LogicalPlanMatcherImpl(
      std::function<void(const LogicalPlanNodePtr&)> onMatch)
      : onMatch_{std::move(onMatch)} {}

  LogicalPlanMatcherImpl(
      const std::vector<std::shared_ptr<LogicalPlanMatcher>>& inputMatchers,
      std::function<void(const LogicalPlanNodePtr&)> onMatch)
      : inputMatchers_{inputMatchers}, onMatch_{std::move(onMatch)} {}

  LogicalPlanMatcherImpl(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::function<void(const LogicalPlanNodePtr&)> onMatch)
      : inputMatchers_{{inputMatcher}}, onMatch_{std::move(onMatch)} {}

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

    if (!matchDetails(*specificNode)) {
      return false;
    }

    if (onMatch_ != nullptr) {
      onMatch_(plan);
    }

    return true;
  }

 protected:
  virtual bool matchDetails(const T& plan) const {
    return true;
  }

  const std::vector<std::shared_ptr<LogicalPlanMatcher>> inputMatchers_;
  const std::function<void(const LogicalPlanNodePtr&)> onMatch_;
};

class SetMatcher : public LogicalPlanMatcherImpl<SetNode> {
 public:
  SetMatcher(
      SetOperation op,
      const std::shared_ptr<LogicalPlanMatcher>& leftMatcher,
      const std::shared_ptr<LogicalPlanMatcher>& rightMatcher,
      std::function<void(const LogicalPlanNodePtr&)> onMatch)
      : LogicalPlanMatcherImpl<SetNode>(
            {leftMatcher, rightMatcher},
            std::move(onMatch)),
        op_{op} {}

 private:
  bool matchDetails(const SetNode& plan) const override {
    EXPECT_EQ(plan.operation(), op_);
    return !::testing::Test::HasNonfatalFailure();
  }

  SetOperation op_;
};

class ValuesMatcher : public LogicalPlanMatcherImpl<ValuesNode> {
 public:
  ValuesMatcher(
      velox::RowTypePtr outputType,
      std::function<void(const LogicalPlanNodePtr&)> onMatch)
      : LogicalPlanMatcherImpl<ValuesNode>(std::move(onMatch)),
        outputType_{std::move(outputType)} {}

 private:
  bool matchDetails(const ValuesNode& plan) const override {
    const auto& outputType = plan.outputType();

    EXPECT_EQ(velox::TypeKind::ROW, outputType->kind());
    EXPECT_EQ(outputType_->size(), outputType->size());
    EXPECT_TRUE(*outputType_ == *outputType)
        << "Expected " << outputType_->toString() << ", but got "
        << outputType->toString();

    return !::testing::Test::HasNonfatalFailure();
  }

  const velox::RowTypePtr outputType_;
};

/// Wraps another matcher and verifies output column names after matching.
class OutputColumnsMatcher : public LogicalPlanMatcher {
 public:
  OutputColumnsMatcher(
      std::shared_ptr<LogicalPlanMatcher> innerMatcher,
      std::vector<std::string> expectedColumns)
      : innerMatcher_{std::move(innerMatcher)},
        expectedColumns_{std::move(expectedColumns)} {}

  bool match(const LogicalPlanNodePtr& plan) const override {
    if (!innerMatcher_->match(plan)) {
      return false;
    }

    EXPECT_THAT(
        plan->outputType()->names(),
        ::testing::Pointwise(::testing::Eq(), expectedColumns_));
    return !::testing::Test::HasNonfatalFailure();
  }

 private:
  std::shared_ptr<LogicalPlanMatcher> innerMatcher_;
  std::vector<std::string> expectedColumns_;
};
} // namespace

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::tableWrite(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<TableWriteNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::tableScan(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<TableScanNode>>(
      std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::values(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ =
      std::make_shared<LogicalPlanMatcherImpl<ValuesNode>>(std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::values(
    velox::RowTypePtr outputType,
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<ValuesMatcher>(
      std::move(outputType), std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::filter(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<FilterNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::project(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<ProjectNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::aggregate(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<AggregateNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::unnest(
    OnMatchCallback onMatch) {
  if (matcher_ != nullptr) {
    matcher_ = std::make_shared<LogicalPlanMatcherImpl<UnnestNode>>(
        matcher_, std::move(onMatch));
  } else {
    matcher_ = std::make_shared<LogicalPlanMatcherImpl<UnnestNode>>(
        std::move(onMatch));
  }

  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::join(
    const std::shared_ptr<LogicalPlanMatcher>& rightMatcher,
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<JoinNode>>(
      std::vector<std::shared_ptr<LogicalPlanMatcher>>{matcher_, rightMatcher},
      std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::setOperation(
    SetOperation op,
    const std::shared_ptr<LogicalPlanMatcher>& matcher,
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ =
      std::make_shared<SetMatcher>(op, matcher_, matcher, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::sort(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<SortNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::sample(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<SampleNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::outputColumns(
    std::vector<std::string> expected) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ =
      std::make_shared<OutputColumnsMatcher>(matcher_, std::move(expected));
  return *this;
}

} // namespace facebook::axiom::logical_plan::test
