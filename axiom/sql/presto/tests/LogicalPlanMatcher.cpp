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
#include <gtest/gtest.h>
#include <set>

namespace facebook::axiom::logical_plan::test {
namespace {

/// Returns false from the current function if a non-fatal test failure has
/// occurred.
#define AXIOM_RETURN_IF_FAILURE                \
  if (::testing::Test::HasNonfatalFailure()) { \
    return false;                              \
  }

/// Returns false if a non-fatal test failure has occurred, true otherwise.
#define AXIOM_RETURN_RESULT                    \
  if (::testing::Test::HasNonfatalFailure()) { \
    return false;                              \
  }                                            \
  return true;

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
    AXIOM_RETURN_IF_FAILURE;

    EXPECT_EQ(plan->inputs().size(), inputMatchers_.size());
    AXIOM_RETURN_IF_FAILURE;

    for (auto i = 0; i < inputMatchers_.size(); ++i) {
      EXPECT_TRUE(inputMatchers_[i]->match(plan->inputs()[i]));
      AXIOM_RETURN_IF_FAILURE;
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
    AXIOM_RETURN_RESULT
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
    AXIOM_RETURN_RESULT
  }

  const velox::RowTypePtr outputType_;
};

class LimitMatcher : public LogicalPlanMatcherImpl<LimitNode> {
 public:
  LimitMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      int64_t offset,
      int64_t count)
      : LogicalPlanMatcherImpl<LimitNode>(inputMatcher, nullptr),
        offset_{offset},
        count_{count} {}

 private:
  bool matchDetails(const LimitNode& plan) const override {
    EXPECT_EQ(offset_, plan.offset());
    EXPECT_EQ(count_, plan.count());
    AXIOM_RETURN_RESULT
  }

  const int64_t offset_;
  const int64_t count_;
};

class ProjectMatcher : public LogicalPlanMatcherImpl<ProjectNode> {
 public:
  ProjectMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::vector<std::string> expressions)
      : LogicalPlanMatcherImpl<ProjectNode>(inputMatcher, nullptr),
        expressions_{std::move(expressions)} {}

 private:
  bool matchDetails(const ProjectNode& plan) const override {
    EXPECT_EQ(expressions_.size(), plan.expressions().size());
    AXIOM_RETURN_IF_FAILURE;

    for (auto i = 0; i < expressions_.size(); ++i) {
      EXPECT_EQ(expressions_[i], plan.expressionAt(i)->toString())
          << "at index " << i;
      AXIOM_RETURN_IF_FAILURE;
    }
    AXIOM_RETURN_RESULT
  }

  const std::vector<std::string> expressions_;
};

class DistinctMatcher : public LogicalPlanMatcherImpl<AggregateNode> {
 public:
  explicit DistinctMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher)
      : LogicalPlanMatcherImpl<AggregateNode>(inputMatcher, nullptr) {}

 private:
  bool matchDetails(const AggregateNode& plan) const override {
    EXPECT_EQ(0, plan.aggregates().size())
        << "Expected no aggregates for distinct";
    EXPECT_EQ(0, plan.groupingSets().size())
        << "Expected no grouping sets for distinct";
    AXIOM_RETURN_IF_FAILURE;

    std::set<std::string> names;
    for (const auto& key : plan.groupingKeys()) {
      EXPECT_TRUE(key->isInputReference())
          << "Expected grouping key to be an input column, but got "
          << key->toString();
      AXIOM_RETURN_IF_FAILURE;

      auto name = key->as<InputReferenceExpr>()->name();
      EXPECT_TRUE(names.insert(name).second)
          << "Duplicate grouping key: " << name;
      AXIOM_RETURN_IF_FAILURE;
    }
    AXIOM_RETURN_RESULT
  }
};

#undef AXIOM_RETURN_IF_FAILURE
#undef AXIOM_RETURN_RESULT

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

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::project(
    const std::vector<std::string>& expressions) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<ProjectMatcher>(matcher_, expressions);
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::aggregate(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<AggregateNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::distinct() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<DistinctMatcher>(matcher_);
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

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::unionAll(
    const std::shared_ptr<LogicalPlanMatcher>& matcher,
    OnMatchCallback onMatch) {
  return setOperation(SetOperation::kUnionAll, matcher, std::move(onMatch));
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::except(
    const std::shared_ptr<LogicalPlanMatcher>& matcher,
    OnMatchCallback onMatch) {
  return setOperation(SetOperation::kExcept, matcher, std::move(onMatch));
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::intersect(
    const std::shared_ptr<LogicalPlanMatcher>& matcher,
    OnMatchCallback onMatch) {
  return setOperation(SetOperation::kIntersect, matcher, std::move(onMatch));
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::sort(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<SortNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::limit(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<LimitNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::limit(
    int64_t offset,
    int64_t count) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LimitMatcher>(matcher_, offset, count);
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::sample(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<SampleNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

} // namespace facebook::axiom::logical_plan::test
