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

// Returns MatchResult::failure() from the current function if a non-fatal
// test failure has occurred.
#define AXIOM_RETURN_IF_FAILURE                        \
  if (::testing::Test::HasNonfatalFailure()) {         \
    return LogicalPlanMatcher::MatchResult::failure(); \
  }

// Returns MatchResult based on non-fatal test failure status, passing through
// symbols on success.
#define AXIOM_RETURN_RESULT(symbols)                   \
  if (::testing::Test::HasNonfatalFailure()) {         \
    return LogicalPlanMatcher::MatchResult::failure(); \
  }                                                    \
  return LogicalPlanMatcher::MatchResult::success(symbols);

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

// Rewrites field access names in an IExpr tree using the given mapping.
// Used for symbol rewriting: replaces alias names with actual column names.
velox::core::ExprPtr rewriteInputNames(
    const velox::core::ExprPtr& expr,
    const std::unordered_map<std::string, std::string>& mapping) {
  using velox::core::IExpr;
  if (expr->is(IExpr::Kind::kFieldAccess)) {
    auto* fieldAccess = expr->as<velox::core::FieldAccessExpr>();
    if (fieldAccess->isRootColumn()) {
      auto it = mapping.find(fieldAccess->name());
      if (it == mapping.end()) {
        return expr;
      }
      return std::make_shared<velox::core::FieldAccessExpr>(
          it->second, fieldAccess->alias());
    }
  }

  std::vector<velox::core::ExprPtr> newInputs;
  for (const auto& input : expr->inputs()) {
    newInputs.push_back(rewriteInputNames(input, mapping));
  }
  return expr->replaceInputs(newInputs);
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

  MatchResult match(
      const LogicalPlanNodePtr& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    const auto* specificNode = dynamic_cast<const T*>(plan.get());
    EXPECT_TRUE(specificNode != nullptr)
        << "Expected " << folly::demangle(typeid(T).name()) << ", but got "
        << NodeKindName::toName(plan->kind());
    AXIOM_RETURN_IF_FAILURE;

    EXPECT_EQ(plan->inputs().size(), inputMatchers_.size());
    AXIOM_RETURN_IF_FAILURE;

    // Match children, collect symbols.
    std::unordered_map<std::string, std::string> childSymbols;
    for (auto i = 0; i < inputMatchers_.size(); ++i) {
      auto result = inputMatchers_[i]->match(plan->inputs()[i], symbols);
      if (!result.match) {
        return MatchResult::failure();
      }
      for (const auto& [alias, actualName] : result.symbols) {
        childSymbols[alias] = actualName;
      }
    }

    auto result = matchDetails(*specificNode, childSymbols);
    if (!result.match) {
      return MatchResult::failure();
    }

    if (onMatch_ != nullptr) {
      onMatch_(plan);
      AXIOM_RETURN_IF_FAILURE;
    }

    return result;
  }

 protected:
  using MatchResult = LogicalPlanMatcher::MatchResult;

  virtual MatchResult matchDetails(
      const T& plan,
      const std::unordered_map<std::string, std::string>& symbols) const {
    return MatchResult::success(symbols);
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
  MatchResult matchDetails(
      const SetNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    EXPECT_EQ(plan.operation(), op_);
    AXIOM_RETURN_RESULT(symbols)
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
  MatchResult matchDetails(
      const ValuesNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    const auto& outputType = plan.outputType();

    EXPECT_EQ(velox::TypeKind::ROW, outputType->kind());
    EXPECT_EQ(outputType_->size(), outputType->size());
    EXPECT_TRUE(*outputType_ == *outputType)
        << "Expected " << outputType_->toString() << ", but got "
        << outputType->toString();
    AXIOM_RETURN_RESULT(symbols)
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
  MatchResult matchDetails(
      const LimitNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    EXPECT_EQ(offset_, plan.offset());
    EXPECT_EQ(count_, plan.count());
    AXIOM_RETURN_RESULT(symbols)
  }

  const int64_t offset_;
  const int64_t count_;
};

// Matches a ProjectNode with the specified expressions. Each expected
// expression is parsed with DuckDB and printed in a format compatible with
// lp::ExprPrinter, then compared against expressionAt(i)->toString().
class ProjectMatcher : public LogicalPlanMatcherImpl<ProjectNode> {
 public:
  ProjectMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::vector<std::string> expressions)
      : LogicalPlanMatcherImpl<ProjectNode>(inputMatcher, nullptr),
        expressions_{std::move(expressions)} {}

 private:
  MatchResult matchDetails(
      const ProjectNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    EXPECT_EQ(expressions_.size(), plan.expressions().size());
    AXIOM_RETURN_IF_FAILURE;

    std::unordered_map<std::string, std::string> newSymbols;
    velox::parse::DuckSqlExpressionsParser parser;

    for (auto i = 0; i < expressions_.size(); ++i) {
      const auto& actual = plan.expressionAt(i);
      auto parsed = parser.parseScalarOrWindowExpr(expressions_[i]);

      if (auto* windowExpr = std::get_if<velox::parse::WindowExpr>(&parsed)) {
        // Capture alias from the window function call.
        if (windowExpr->functionCall->alias()) {
          newSymbols[windowExpr->functionCall->alias().value()] =
              plan.names()[i];
        }

        // DuckDB defaults end bound to CURRENT ROW when ORDER BY is absent,
        // but the SQL standard requires UNBOUNDED FOLLOWING. Correct this.
        if (windowExpr->orderBy.empty() &&
            windowExpr->frame.endType == velox::parse::BoundType::kCurrentRow) {
          windowExpr->frame.endType =
              velox::parse::BoundType::kUnboundedFollowing;
        }

        // Rewrite symbols in window sub-expressions.
        if (!symbols.empty()) {
          windowExpr->functionCall =
              rewriteInputNames(windowExpr->functionCall, symbols);
          for (auto& expr : windowExpr->partitionBy) {
            expr = rewriteInputNames(expr, symbols);
          }
          for (auto& entry : windowExpr->orderBy) {
            entry.expr = rewriteInputNames(entry.expr, symbols);
          }
          if (windowExpr->frame.startValue) {
            windowExpr->frame.startValue =
                rewriteInputNames(windowExpr->frame.startValue, symbols);
          }
          if (windowExpr->frame.endValue) {
            windowExpr->frame.endValue =
                rewriteInputNames(windowExpr->frame.endValue, symbols);
          }
        }

        EXPECT_EQ(toWindowExprString(*windowExpr), actual->toString())
            << "at index " << i;
      } else {
        auto expectedExpr = std::get<velox::core::ExprPtr>(parsed);

        // Capture alias.
        if (expectedExpr->alias()) {
          newSymbols[expectedExpr->alias().value()] = plan.names()[i];
        }

        // Rewrite symbols in expected expression.
        if (!symbols.empty()) {
          expectedExpr = rewriteInputNames(expectedExpr, symbols);
        }

        EXPECT_EQ(toExprString(*expectedExpr->dropAlias()), actual->toString())
            << "at index " << i;
      }
      AXIOM_RETURN_IF_FAILURE;
    }

    return MatchResult::success(newSymbols);
  }

  const std::vector<std::string> expressions_;
};

class DistinctMatcher : public LogicalPlanMatcherImpl<AggregateNode> {
 public:
  explicit DistinctMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher)
      : LogicalPlanMatcherImpl<AggregateNode>(inputMatcher, nullptr) {}

 private:
  MatchResult matchDetails(
      const AggregateNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
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
    AXIOM_RETURN_RESULT(symbols)
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
  MatchResult matchDetails(
      const OutputNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    const auto& names = plan.outputType()->names();
    EXPECT_EQ(expectedNames_.size(), names.size());
    AXIOM_RETURN_IF_FAILURE;

    for (auto i = 0; i < expectedNames_.size(); ++i) {
      EXPECT_EQ(expectedNames_[i], names[i]) << "at index " << i;
      AXIOM_RETURN_IF_FAILURE;
    }
    AXIOM_RETURN_RESULT(symbols)
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
