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
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"

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

// Prints a velox::core::IExpr tree in the same format as lp::ExprPrinter.
// This allows comparing DuckDB-parsed expected expressions against actual
// lp::Expr::toString() output.
std::string toExprString(const velox::core::IExpr& expr) {
  using velox::core::IExpr;
  switch (expr.kind()) {
    case IExpr::Kind::kFieldAccess:
      return expr.as<velox::core::FieldAccessExpr>()->name();
    case IExpr::Kind::kCall: {
      auto* call = expr.as<velox::core::CallExpr>();
      std::string result = call->name() + "(";
      for (size_t i = 0; i < call->inputs().size(); ++i) {
        if (i > 0) {
          result += ", ";
        }
        result += toExprString(*call->inputAt(i));
      }
      return result + ")";
    }
    case IExpr::Kind::kCast: {
      auto* cast = expr.as<velox::core::CastExpr>();
      return fmt::format(
          "{}({} AS {})",
          cast->isTryCast() ? "TRY_CAST" : "CAST",
          toExprString(*cast->input()),
          cast->type()->toString());
    }
    case IExpr::Kind::kConstant: {
      auto* constant = expr.as<velox::core::ConstantExpr>();
      return constant->value().toStringAsVector(constant->type());
    }
    case IExpr::Kind::kInput:
      return "ROW";
    default:
      VELOX_UNREACHABLE(
          "Unsupported IExpr kind in toExprString: {}", expr.toString());
  }
}

// Formats a velox::parse::WindowExpr in the same format as lp::ExprPrinter.
std::string toWindowExprString(const velox::parse::WindowExpr& window) {
  std::string result = toExprString(*window.functionCall->dropAlias());
  result += " OVER (";

  bool needSeparator = false;

  if (!window.partitionBy.empty()) {
    result += "PARTITION BY ";
    for (size_t i = 0; i < window.partitionBy.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += toExprString(*window.partitionBy[i]);
    }
    needSeparator = true;
  }

  if (!window.orderBy.empty()) {
    if (needSeparator) {
      result += " ";
    }
    result += "ORDER BY ";
    for (size_t i = 0; i < window.orderBy.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += toExprString(*window.orderBy[i].expr);
      result += " ";
      result += window.orderBy[i].ascending ? "ASC" : "DESC";
      result += " NULLS ";
      result += window.orderBy[i].nullsFirst ? "FIRST" : "LAST";
    }
    needSeparator = true;
  }

  if (needSeparator) {
    result += " ";
  }
  result += velox::parse::WindowTypeName::toName(window.frame.type);
  result += " BETWEEN ";
  if (window.frame.startValue) {
    result += toExprString(*window.frame.startValue) + " ";
  }
  result += velox::parse::BoundTypeName::toName(window.frame.startType);
  result += " AND ";
  if (window.frame.endValue) {
    result += toExprString(*window.frame.endValue) + " ";
  }
  result += velox::parse::BoundTypeName::toName(window.frame.endType);
  result += ")";

  if (window.ignoreNulls) {
    result += " IGNORE NULLS";
  }
  return result;
}

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

    AXIOM_RETURN_RESULT
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

/// Matches a ProjectNode with the specified expressions. Each expected
/// expression is parsed with DuckDB and printed in a format compatible with
/// lp::ExprPrinter, then compared against expressionAt(i)->toString().
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

    velox::parse::DuckSqlExpressionsParser parser;

    for (auto i = 0; i < expressions_.size(); ++i) {
      const auto& expected = expressions_[i];
      const auto& actual = plan.expressionAt(i);

      auto parsed = parser.parseScalarOrWindowExpr(expected);

      std::string expectedStr;
      if (auto* windowExpr = std::get_if<velox::parse::WindowExpr>(&parsed)) {
        // DuckDB defaults end bound to CURRENT ROW when ORDER BY is absent,
        // but the SQL standard requires UNBOUNDED FOLLOWING. Correct this.
        if (windowExpr->orderBy.empty() &&
            windowExpr->frame.endType == velox::parse::BoundType::kCurrentRow) {
          windowExpr->frame.endType =
              velox::parse::BoundType::kUnboundedFollowing;
        }
        expectedStr = toWindowExprString(*windowExpr);
      } else {
        expectedStr = toExprString(*std::get<velox::core::ExprPtr>(parsed));
      }

      EXPECT_EQ(expectedStr, actual->toString()) << "at index " << i;
      AXIOM_RETURN_IF_FAILURE;
    }
    AXIOM_RETURN_RESULT
  }

  const std::vector<std::string> expressions_;
};

class AggregateMatcher : public LogicalPlanMatcherImpl<AggregateNode> {
 public:
  AggregateMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::vector<std::string> groupingKeys,
      std::vector<std::string> aggregates)
      : LogicalPlanMatcherImpl<AggregateNode>(inputMatcher, nullptr),
        groupingKeys_{std::move(groupingKeys)},
        aggregates_{std::move(aggregates)} {}

 private:
  bool matchDetails(const AggregateNode& plan) const override {
    EXPECT_EQ(groupingKeys_.size(), plan.groupingKeys().size());
    AXIOM_RETURN_IF_FAILURE;

    for (auto i = 0; i < groupingKeys_.size(); ++i) {
      EXPECT_EQ(groupingKeys_[i], plan.groupingKeys()[i]->toString())
          << "at grouping key index " << i;
      AXIOM_RETURN_IF_FAILURE;
    }

    EXPECT_EQ(aggregates_.size(), plan.aggregates().size());
    AXIOM_RETURN_IF_FAILURE;

    for (auto i = 0; i < aggregates_.size(); ++i) {
      EXPECT_EQ(aggregates_[i], plan.aggregateAt(i)->toString())
          << "at aggregate index " << i;
      AXIOM_RETURN_IF_FAILURE;
    }
    AXIOM_RETURN_RESULT
  }

  const std::vector<std::string> groupingKeys_;
  const std::vector<std::string> aggregates_;
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

class OutputNamesMatcher : public LogicalPlanMatcherImpl<OutputNode> {
 public:
  OutputNamesMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::vector<std::string> expectedNames)
      : LogicalPlanMatcherImpl<OutputNode>(inputMatcher, nullptr),
        expectedNames_{std::move(expectedNames)} {}

 private:
  bool matchDetails(const OutputNode& plan) const override {
    const auto& names = plan.outputType()->names();
    EXPECT_EQ(expectedNames_.size(), names.size());
    AXIOM_RETURN_IF_FAILURE;

    for (auto i = 0; i < expectedNames_.size(); ++i) {
      EXPECT_EQ(expectedNames_[i], names[i]) << "at index " << i;
      AXIOM_RETURN_IF_FAILURE;
    }
    AXIOM_RETURN_RESULT
  }

  const std::vector<std::string> expectedNames_;
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

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::tableScan(
    const std::string& tableName) {
  return tableScan([tableName](const LogicalPlanNodePtr& node) {
    auto scan = std::dynamic_pointer_cast<const TableScanNode>(node);
    EXPECT_EQ(scan->tableName(), tableName);
  });
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

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::aggregate(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ =
      std::make_shared<AggregateMatcher>(matcher_, groupingKeys, aggregates);
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

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::output(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<OutputNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::output(
    const std::vector<std::string>& expectedNames) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<OutputNamesMatcher>(matcher_, expectedNames);
  return *this;
}

std::shared_ptr<LogicalPlanMatcher> LogicalPlanMatcherBuilder::build() {
  VELOX_USER_CHECK_NOT_NULL(
      matcher_, "Cannot build an empty LogicalPlanMatcher.");
  return matcher_;
}

} // namespace facebook::axiom::logical_plan::test
