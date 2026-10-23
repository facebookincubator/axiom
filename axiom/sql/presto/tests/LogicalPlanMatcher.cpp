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
#include <fmt/core.h>
#include <folly/Demangle.h>
#include <folly/FBString.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <optional>
#include <set>
#include <typeinfo>
#include "axiom/common/SchemaTableName.h"
#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/sql/presto/tests/ExprMatcher.h"
#include "velox/common/base/Exceptions.h"
#include "velox/parse/ExprRewriter.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/IExpr.h"
#include "velox/parse/SqlExpressionsParser.h"
#include "velox/type/TypeKind.h"

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

// Corrects DuckDB's wrong default window frame end bound. DuckDB always
// defaults to CURRENT_ROW, but the SQL standard specifies UNBOUNDED_FOLLOWING
// when no ORDER BY is present.
velox::core::ExprPtr correctDuckDbWindowFrame(
    const velox::core::ExprPtr& expr) {
  auto* window = expr->as<velox::core::WindowCallExpr>();
  if (!window->frame().has_value()) {
    return expr;
  }

  auto frame = window->frame().value();
  if (!window->orderByKeys().empty() ||
      frame.endType != velox::core::WindowCallExpr::BoundType::kCurrentRow) {
    return expr;
  }

  frame.endType = velox::core::WindowCallExpr::BoundType::kUnboundedFollowing;
  return std::make_shared<velox::core::WindowCallExpr>(
      window->name(),
      window->inputs(),
      window->partitionKeys(),
      window->orderByKeys(),
      frame,
      window->isIgnoreNulls());
}

// Fills in the SQL standard default window frame when ExprApi has no explicit
// frame (nullopt). The default is RANGE UNBOUNDED PRECEDING to CURRENT ROW
// when ORDER BY is present, or RANGE UNBOUNDED PRECEDING to UNBOUNDED
// FOLLOWING when no ORDER BY is specified.
velox::core::ExprPtr applyDefaultWindowFrame(const velox::core::ExprPtr& expr) {
  auto* window = expr->as<velox::core::WindowCallExpr>();
  if (window->frame().has_value()) {
    return expr;
  }

  velox::core::WindowCallExpr::Frame frame;
  frame.type = velox::core::WindowCallExpr::WindowType::kRange;
  frame.startType = velox::core::WindowCallExpr::BoundType::kUnboundedPreceding;
  frame.endType = window->orderByKeys().empty()
      ? velox::core::WindowCallExpr::BoundType::kUnboundedFollowing
      : velox::core::WindowCallExpr::BoundType::kCurrentRow;

  return std::make_shared<velox::core::WindowCallExpr>(
      window->name(),
      window->inputs(),
      window->partitionKeys(),
      window->orderByKeys(),
      frame,
      window->isIgnoreNulls());
}

// Rewrites field access names in an IExpr tree using the given mapping.
// Used for symbol rewriting: replaces alias names with actual column names.
velox::core::ExprPtr rewriteInputNames(
    const velox::core::ExprPtr& expr,
    const std::unordered_map<std::string, std::string>& mapping) {
  return velox::core::ExprRewriter::rewrite(
      expr, [&](const velox::core::ExprPtr& e) -> velox::core::ExprPtr {
        if (const auto* field =
                velox::core::FieldAccessExpr::tryAsRootColumn(e)) {
          auto it = mapping.find(field->name());
          if (it != mapping.end()) {
            return std::make_shared<velox::core::FieldAccessExpr>(
                it->second, field->alias());
          }
        }
        return e;
      });
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

// Matches a JoinNode and captures aliases for all output columns.
// 'outputAliases' must have one entry per output column. Each alias is mapped
// to the actual column name at that position, so subsequent matchers can
// reference these aliases in expressions.
class JoinMatcher : public LogicalPlanMatcherImpl<JoinNode> {
 public:
  JoinMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& leftMatcher,
      const std::shared_ptr<LogicalPlanMatcher>& rightMatcher,
      std::vector<std::string> outputAliases)
      : LogicalPlanMatcherImpl<JoinNode>({leftMatcher, rightMatcher}, nullptr),
        outputAliases_{std::move(outputAliases)} {}

 private:
  MatchResult matchDetails(
      const JoinNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    const auto& outputType = plan.outputType();

    EXPECT_EQ(outputAliases_.size(), outputType->size())
        << "Expected " << outputAliases_.size() << " output columns, but got "
        << outputType->size();
    AXIOM_RETURN_IF_FAILURE;

    std::unordered_map<std::string, std::string> newSymbols = symbols;
    for (size_t i = 0; i < outputAliases_.size(); ++i) {
      newSymbols[outputAliases_[i]] = outputType->nameOf(i);
    }

    AXIOM_RETURN_RESULT(newSymbols)
  }

  const std::vector<std::string> outputAliases_;
};

class FilterMatcher : public LogicalPlanMatcherImpl<FilterNode> {
 public:
  FilterMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::string expression,
      velox::parse::ParseOptions parseOptions)
      : LogicalPlanMatcherImpl<FilterNode>(inputMatcher, nullptr),
        expression_{std::move(expression)},
        parseOptions_{std::move(parseOptions)} {}

 private:
  MatchResult matchDetails(
      const FilterNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    velox::parse::DuckSqlExpressionsParser parser(parseOptions_);
    auto expected = parser.parseScalarOrWindowExpr(expression_);

    if (!symbols.empty()) {
      expected = rewriteInputNames(expected, symbols);
    }

    ExprMatcher::match(plan.predicate(), expected->dropAlias());
    AXIOM_RETURN_RESULT(symbols)
  }

  const std::string expression_;
  const velox::parse::ParseOptions parseOptions_;
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

class GraphMatchMatcher : public LogicalPlanMatcherImpl<GraphMatchNode> {
 public:
  explicit GraphMatchMatcher(std::vector<std::string> outputs)
      : LogicalPlanMatcherImpl<GraphMatchNode>(nullptr),
        outputs_{std::move(outputs)} {}

 private:
  MatchResult matchDetails(
      const GraphMatchNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    // An expected output may name its column with ' AS alias' so later
    // matchers can reference it.
    std::vector<std::string> expected;
    std::unordered_map<std::string, std::string> newSymbols = symbols;
    for (auto i = 0; i < outputs_.size(); ++i) {
      const auto& output = outputs_[i];
      const auto as = output.find(" AS ");
      expected.push_back(output.substr(0, as));
      if (as != std::string::npos && i < plan.outputNames().size()) {
        newSymbols[output.substr(as + 4)] = plan.outputNames()[i];
      }
    }

    std::vector<std::string> outputs;
    for (const auto& output : plan.outputs()) {
      if (output->is(GraphMatchOutput::Kind::kPathLength)) {
        outputs.push_back(fmt::format("path_length({})", output->pattern()));
      } else {
        outputs.push_back(
            fmt::format(
                "{}.{}",
                output->pattern(),
                output->as<GraphPropertyOutput>()->property()));
      }
    }
    EXPECT_EQ(expected, outputs);
    AXIOM_RETURN_RESULT(newSymbols)
  }

  const std::vector<std::string> outputs_;
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

// Matches a ProjectNode with the specified expressions.
// - String constructor: parses with DuckDB, corrects window frame defaults.
// - ExprApi constructor: uses the expression directly, applies SQL standard
//   default frame when none is specified.
class ProjectMatcher : public LogicalPlanMatcherImpl<ProjectNode> {
 public:
  ProjectMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::vector<std::string> expressions,
      velox::parse::ParseOptions parseOptions)
      : LogicalPlanMatcherImpl<ProjectNode>(inputMatcher, nullptr),
        expressionStrings_{std::move(expressions)},
        parseOptions_{std::move(parseOptions)} {}

  ProjectMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::vector<ExprApi> expressions)
      : LogicalPlanMatcherImpl<ProjectNode>(inputMatcher, nullptr),
        expressionApis_{std::move(expressions)} {}

 private:
  MatchResult matchDetails(
      const ProjectNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    const bool isExprApi = !expressionApis_.empty();
    const auto numExprs =
        isExprApi ? expressionApis_.size() : expressionStrings_.size();
    EXPECT_EQ(numExprs, plan.expressions().size());
    AXIOM_RETURN_IF_FAILURE;

    // Inherit aliases captured below so a name bound at a descendant (e.g. an
    // aggregate output) stays resolvable above this projection.
    std::unordered_map<std::string, std::string> newSymbols = symbols;
    velox::parse::DuckSqlExpressionsParser parser(parseOptions_);

    for (auto i = 0; i < numExprs; ++i) {
      const auto& actual = plan.expressionAt(i);

      velox::core::ExprPtr expectedExpr;
      if (isExprApi) {
        expectedExpr = expressionApis_[i].expr();
      } else {
        expectedExpr = parser.parseScalarOrWindowExpr(expressionStrings_[i]);
      }

      if (expectedExpr->alias()) {
        newSymbols[expectedExpr->alias().value()] = plan.names()[i];
      }

      if (expectedExpr->is(velox::core::IExpr::Kind::kWindow)) {
        expectedExpr = isExprApi ? applyDefaultWindowFrame(expectedExpr)
                                 : correctDuckDbWindowFrame(expectedExpr);
      }

      if (!symbols.empty()) {
        expectedExpr = rewriteInputNames(expectedExpr, symbols);
      }

      SCOPED_TRACE("project expression at index " + std::to_string(i));
      ExprMatcher::match(actual, expectedExpr->dropAlias());
      AXIOM_RETURN_IF_FAILURE;
    }

    return MatchResult::success(newSymbols);
  }

  const std::vector<std::string> expressionStrings_;
  const std::vector<ExprApi> expressionApis_;
  const velox::parse::ParseOptions parseOptions_;
};

class AggregateMatcher : public LogicalPlanMatcherImpl<AggregateNode> {
 public:
  AggregateMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::vector<std::string> groupingKeys,
      std::vector<std::string> aggregates,
      std::vector<std::vector<int32_t>> groupingSets,
      velox::parse::ParseOptions parseOptions)
      : LogicalPlanMatcherImpl<AggregateNode>(inputMatcher, nullptr),
        groupingKeys_{std::move(groupingKeys)},
        aggregates_{std::move(aggregates)},
        groupingSets_{std::move(groupingSets)},
        parseOptions_{std::move(parseOptions)} {}

 private:
  MatchResult matchDetails(
      const AggregateNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    EXPECT_EQ(groupingKeys_.size(), plan.groupingKeys().size());
    AXIOM_RETURN_IF_FAILURE;

    velox::parse::DuckSqlExpressionsParser parser(parseOptions_);

    for (auto i = 0; i < groupingKeys_.size(); ++i) {
      auto expected = parser.parseScalarOrWindowExpr(groupingKeys_[i]);
      if (!symbols.empty()) {
        expected = rewriteInputNames(expected, symbols);
      }

      SCOPED_TRACE("grouping key at index " + std::to_string(i));
      ExprMatcher::match(plan.groupingKeys()[i], expected->dropAlias());
      AXIOM_RETURN_IF_FAILURE;
    }

    EXPECT_EQ(aggregates_.size(), plan.aggregates().size());
    AXIOM_RETURN_IF_FAILURE;

    std::unordered_map<std::string, std::string> newSymbols = symbols;
    auto numGroupingKeys = plan.groupingKeys().size();
    for (auto i = 0; i < aggregates_.size(); ++i) {
      velox::core::ExprPtr parsed = parser.parseAggregateExpr(aggregates_[i]);

      // Capture alias for symbol propagation.
      if (parsed->alias()) {
        newSymbols[parsed->alias().value()] =
            plan.outputType()->nameOf(numGroupingKeys + i);
      }

      if (!symbols.empty()) {
        parsed = rewriteInputNames(parsed, symbols);
      }

      const auto& actual = plan.aggregateAt(i);
      SCOPED_TRACE("aggregate at index " + std::to_string(i));
      ExprMatcher::match(actual, parsed->dropAlias());
      AXIOM_RETURN_IF_FAILURE;
    }

    EXPECT_EQ(groupingSets_.size(), plan.groupingSets().size())
        << "grouping sets count mismatch";
    AXIOM_RETURN_IF_FAILURE;

    for (auto i = 0; i < groupingSets_.size(); ++i) {
      EXPECT_EQ(groupingSets_[i], plan.groupingSets()[i])
          << "at grouping set index " << i;
      AXIOM_RETURN_IF_FAILURE;
    }

    AXIOM_RETURN_RESULT(newSymbols)
  }

  const std::vector<std::string> groupingKeys_;
  const std::vector<std::string> aggregates_;
  const std::vector<std::vector<int32_t>> groupingSets_;
  const velox::parse::ParseOptions parseOptions_;
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

class OutputTypeMatcher : public LogicalPlanMatcherImpl<OutputNode> {
 public:
  OutputTypeMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      velox::RowTypePtr expectedType)
      : LogicalPlanMatcherImpl<OutputNode>(inputMatcher, nullptr),
        expectedType_{std::move(expectedType)} {}

 private:
  MatchResult matchDetails(
      const OutputNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    const auto& type = plan.outputType();
    EXPECT_TRUE(*expectedType_ == *type)
        << "Expected " << expectedType_->toString() << ", but got "
        << type->toString();
    AXIOM_RETURN_IF_FAILURE;
    AXIOM_RETURN_RESULT(symbols)
  }

  const velox::RowTypePtr expectedType_;
};

class SortMatcher : public LogicalPlanMatcherImpl<SortNode> {
 public:
  SortMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::vector<std::string> ordering,
      std::function<void(const LogicalPlanNodePtr&)> onMatch,
      velox::parse::ParseOptions parseOptions)
      : LogicalPlanMatcherImpl<SortNode>(inputMatcher, std::move(onMatch)),
        ordering_{std::move(ordering)},
        parseOptions_{std::move(parseOptions)} {}

 private:
  MatchResult matchDetails(
      const SortNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    const auto& actualOrdering = plan.ordering();
    EXPECT_EQ(actualOrdering.size(), ordering_.size());
    AXIOM_RETURN_IF_FAILURE;

    velox::parse::DuckSqlExpressionsParser parser(parseOptions_);
    for (size_t i = 0; i < ordering_.size(); ++i) {
      auto expected = parser.parseOrderByExpr(ordering_[i]);
      auto expectedExpr = expected.expr;

      if (!symbols.empty()) {
        expectedExpr = rewriteInputNames(expectedExpr, symbols);
      }

      SCOPED_TRACE("ordering at index " + std::to_string(i));
      ExprMatcher::match(
          actualOrdering[i].expression, expectedExpr->dropAlias());
      AXIOM_RETURN_IF_FAILURE;

      EXPECT_EQ(expected.ascending, actualOrdering[i].order.isAscending())
          << "sort direction mismatch at ordering index " << i;
      AXIOM_RETURN_IF_FAILURE;

      EXPECT_EQ(expected.nullsFirst, actualOrdering[i].order.isNullsFirst())
          << "nulls-first mismatch at ordering index " << i;
      AXIOM_RETURN_IF_FAILURE;
    }

    AXIOM_RETURN_RESULT(symbols)
  }

  const std::vector<std::string> ordering_;
  const velox::parse::ParseOptions parseOptions_;
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
    EXPECT_EQ(scan->tableName().table, tableName);
  });
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::graphMatch(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<GraphMatchNode>>(
      std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::graphMatch(
    const std::vector<std::string>& outputs) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<GraphMatchMatcher>(outputs);
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

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::filter(
    const std::string& expression,
    const velox::parse::ParseOptions& options) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<FilterMatcher>(matcher_, expression, options);
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
    const std::vector<std::string>& expressions,
    const velox::parse::ParseOptions& options) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<ProjectMatcher>(matcher_, expressions, options);
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::project(
    std::initializer_list<ExprApi> expressions) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<ProjectMatcher>(
      matcher_, std::vector<ExprApi>(expressions.begin(), expressions.end()));
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
    const std::vector<std::string>& aggregates,
    const std::vector<std::vector<int32_t>>& groupingSets,
    const velox::parse::ParseOptions& options) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<AggregateMatcher>(
      matcher_, groupingKeys, aggregates, groupingSets, options);
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

namespace {
// Matches 'plan', a node's nested plan, starting from the symbols its input
// captured. Symbols from 'plan' overwrite those of the same name.
LogicalPlanMatcher::MatchResult matchNestedPlan(
    const LogicalPlanMatcher& matcher,
    const LogicalPlanNodePtr& plan,
    const std::unordered_map<std::string, std::string>& inputSymbols) {
  auto result = matcher.match(plan, inputSymbols);
  if (!result.match) {
    return LogicalPlanMatcher::MatchResult::failure();
  }
  auto symbols = inputSymbols;
  for (const auto& [alias, actualName] : result.symbols) {
    symbols[alias] = actualName;
  }
  return LogicalPlanMatcher::MatchResult::success(std::move(symbols));
}

// Matches a LateralJoinNode: its input against the left matcher and the plan
// it runs per left row against the right matcher.
class LateralJoinMatcher : public LogicalPlanMatcherImpl<LateralJoinNode> {
 public:
  LateralJoinMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& leftMatcher,
      std::shared_ptr<LogicalPlanMatcher> rightMatcher,
      std::function<void(const LogicalPlanNodePtr&)> onMatch)
      : LogicalPlanMatcherImpl<LateralJoinNode>(
            leftMatcher,
            std::move(onMatch)),
        rightMatcher_{std::move(rightMatcher)} {}

 private:
  MatchResult matchDetails(
      const LateralJoinNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    return matchNestedPlan(*rightMatcher_, plan.right(), symbols);
  }

  const std::shared_ptr<LogicalPlanMatcher> rightMatcher_;
};
} // namespace

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::lateralJoin(
    const std::shared_ptr<LogicalPlanMatcher>& rightMatcher,
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LateralJoinMatcher>(
      matcher_, rightMatcher, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::join(
    const std::shared_ptr<LogicalPlanMatcher>& rightMatcher,
    const std::vector<std::string>& outputAliases) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ =
      std::make_shared<JoinMatcher>(matcher_, rightMatcher, outputAliases);
  return *this;
}

namespace {
// Matches a FixedPointNode: its anchor against the input matcher and its step
// against the step matcher. When 'outputAliases' is non-empty, captures them
// for all output columns.
class FixedPointMatcher : public LogicalPlanMatcherImpl<FixedPointNode> {
 public:
  FixedPointMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& anchorMatcher,
      std::shared_ptr<LogicalPlanMatcher> stepMatcher,
      std::vector<std::string> outputAliases,
      std::function<void(const LogicalPlanNodePtr&)> onMatch)
      : LogicalPlanMatcherImpl<FixedPointNode>(
            anchorMatcher,
            std::move(onMatch)),
        stepMatcher_{std::move(stepMatcher)},
        outputAliases_{std::move(outputAliases)} {}

 private:
  MatchResult matchDetails(
      const FixedPointNode& plan,
      const std::unordered_map<std::string, std::string>& anchorSymbols)
      const override {
    auto stepResult =
        matchNestedPlan(*stepMatcher_, plan.step(), anchorSymbols);
    if (!stepResult.match || outputAliases_.empty()) {
      return stepResult;
    }
    auto symbols = stepResult.symbols;
    const auto& outputType = plan.outputType();
    if (outputAliases_.size() != outputType->size()) {
      ADD_FAILURE() << "Expected " << outputAliases_.size()
                    << " FixedPointNode output columns, got "
                    << outputType->size();
      return MatchResult::failure();
    }
    for (size_t i = 0; i < outputAliases_.size(); ++i) {
      symbols[outputAliases_[i]] = outputType->nameOf(i);
    }
    return MatchResult::success(std::move(symbols));
  }

  const std::shared_ptr<LogicalPlanMatcher> stepMatcher_;
  const std::vector<std::string> outputAliases_;
};

// Matches a RecursiveReferenceNode leaf with an expected name and,
// optionally, captures aliases for all output columns.
class RecursiveReferenceMatcher
    : public LogicalPlanMatcherImpl<RecursiveReferenceNode> {
 public:
  RecursiveReferenceMatcher(
      std::string expectedName,
      std::vector<std::string> outputAliases)
      : LogicalPlanMatcherImpl<RecursiveReferenceNode>(nullptr),
        expectedName_{std::move(expectedName)},
        outputAliases_{std::move(outputAliases)} {}

 private:
  MatchResult matchDetails(
      const RecursiveReferenceNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    if (expectedName_ != plan.name()) {
      ADD_FAILURE() << "Expected RecursiveReferenceNode name '" << expectedName_
                    << "', got '" << plan.name() << "'";
      return MatchResult::failure();
    }
    if (outputAliases_.empty()) {
      return MatchResult::success(symbols);
    }
    const auto& outputType = plan.outputType();
    if (outputAliases_.size() != outputType->size()) {
      ADD_FAILURE() << "Expected " << outputAliases_.size()
                    << " RecursiveReferenceNode output columns, got "
                    << outputType->size();
      return MatchResult::failure();
    }
    auto newSymbols = symbols;
    for (size_t i = 0; i < outputAliases_.size(); ++i) {
      newSymbols[outputAliases_[i]] = outputType->nameOf(i);
    }
    return MatchResult::success(newSymbols);
  }

  const std::string expectedName_;
  const std::vector<std::string> outputAliases_;
};
} // namespace

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::fixedPoint(
    const std::shared_ptr<LogicalPlanMatcher>& stepMatcher,
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<FixedPointMatcher>(
      matcher_,
      stepMatcher,
      /*outputAliases=*/std::vector<std::string>{},
      std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::fixedPoint(
    const std::shared_ptr<LogicalPlanMatcher>& stepMatcher,
    const std::vector<std::string>& outputAliases) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<FixedPointMatcher>(
      matcher_, stepMatcher, outputAliases, /*onMatch=*/nullptr);
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::recursiveRef(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<RecursiveReferenceNode>>(
      std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::recursiveRef(
    const std::string& name) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<RecursiveReferenceMatcher>(
      name, std::vector<std::string>{});
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::recursiveRef(
    const std::string& name,
    const std::vector<std::string>& outputAliases) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<RecursiveReferenceMatcher>(name, outputAliases);
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

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::unionDistinct(
    const std::shared_ptr<LogicalPlanMatcher>& matcher,
    OnMatchCallback onMatch) {
  return setOperation(SetOperation::kUnion, matcher, std::move(onMatch));
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

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::exceptAll(
    const std::shared_ptr<LogicalPlanMatcher>& matcher,
    OnMatchCallback onMatch) {
  return setOperation(SetOperation::kExceptAll, matcher, std::move(onMatch));
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::intersect(
    const std::shared_ptr<LogicalPlanMatcher>& matcher,
    OnMatchCallback onMatch) {
  return setOperation(SetOperation::kIntersect, matcher, std::move(onMatch));
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::intersectAll(
    const std::shared_ptr<LogicalPlanMatcher>& matcher,
    OnMatchCallback onMatch) {
  return setOperation(SetOperation::kIntersectAll, matcher, std::move(onMatch));
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::sort(
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LogicalPlanMatcherImpl<SortNode>>(
      matcher_, std::move(onMatch));
  return *this;
}

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::sort(
    const std::vector<std::string>& ordering,
    OnMatchCallback onMatch,
    const velox::parse::ParseOptions& options) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<SortMatcher>(
      matcher_, ordering, std::move(onMatch), options);
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

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::output(
    const velox::RowTypePtr& expectedType) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<OutputTypeMatcher>(matcher_, expectedType);
  return *this;
}

std::shared_ptr<LogicalPlanMatcher> LogicalPlanMatcherBuilder::build() {
  VELOX_USER_CHECK_NOT_NULL(
      matcher_, "Cannot build an empty LogicalPlanMatcher.");
  return matcher_;
}

} // namespace facebook::axiom::logical_plan::test
