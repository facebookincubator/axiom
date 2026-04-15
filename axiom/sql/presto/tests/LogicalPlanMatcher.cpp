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
#include <algorithm>
#include <set>
#include <unordered_set>
#include "velox/parse/ExprRewriter.h"
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

// Forward declaration for mutual recursion.
std::string toExprString(const velox::core::IExpr& expr);

// Formats a function call as "name(arg1, arg2, ...)". Shared by kCall,
// kAggregate, and the function-call part of kWindow.
std::string callToString(const velox::core::CallExpr& call) {
  auto name = call.name();
  // DuckDB and ExprApi produce lowercase special form names, but
  // ExprResolver converts these to SpecialFormExprs which print as
  // uppercase. "if" is not included because ExprResolver resolves it to
  // "SWITCH".
  static const std::unordered_set<std::string> kSpecialForms = {
      "and",
      "or",
      "switch",
      "coalesce",
      "cast",
      "try_cast",
      "try",
      "dereference",
      "in",
      "exists"};
  if (kSpecialForms.contains(name)) {
    std::transform(name.begin(), name.end(), name.begin(), ::toupper);
  }
  std::string result = name + "(";
  for (size_t i = 0; i < call.inputs().size(); ++i) {
    if (i > 0) {
      result += ", ";
    }
    result += toExprString(*call.inputAt(i));
  }
  return result + ")";
}

// Appends ORDER BY keys to a stream.
void appendOrderBy(
    const std::vector<velox::core::SortKey>& keys,
    std::stringstream& out) {
  out << "ORDER BY ";
  for (size_t i = 0; i < keys.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << toExprString(*keys[i].expr);
    out << (keys[i].ascending ? " ASC" : " DESC");
    out << (keys[i].nullsFirst ? " NULLS FIRST" : " NULLS LAST");
  }
}

// Appends a frame bound to a stream.
void appendBound(
    velox::core::WindowCallExpr::BoundType boundType,
    const velox::core::ExprPtr& value,
    std::stringstream& out) {
  if (value) {
    out << toExprString(*value) << " ";
  }
  out << velox::core::WindowCallExpr::toName(boundType);
}

// Formats a WindowCallExpr as "func(args) OVER (PARTITION BY ... ORDER BY
// ... ROWS/RANGE/GROUPS BETWEEN ... AND ...)".
std::string windowToString(const velox::core::WindowCallExpr& window) {
  std::stringstream out;
  out << callToString(window) << " OVER (";

  bool needSeparator = false;
  if (!window.partitionKeys().empty()) {
    out << "PARTITION BY ";
    for (size_t i = 0; i < window.partitionKeys().size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << toExprString(*window.partitionKeys()[i]);
    }
    needSeparator = true;
  }

  if (!window.orderByKeys().empty()) {
    if (needSeparator) {
      out << " ";
    }
    appendOrderBy(window.orderByKeys(), out);
    needSeparator = true;
  }

  if (window.frame().has_value()) {
    if (needSeparator) {
      out << " ";
    }
    const auto& frame = window.frame().value();
    out << velox::core::WindowCallExpr::toName(frame.type) << " BETWEEN ";
    appendBound(frame.startType, frame.startValue, out);
    out << " AND ";
    appendBound(frame.endType, frame.endValue, out);
  }

  out << ")";
  if (window.isIgnoreNulls()) {
    out << " IGNORE NULLS";
  }
  return out.str();
}

// Formats an AggregateCallExpr as "name(DISTINCT args ORDER BY ...) FILTER
// (WHERE ...)".
std::string aggregateToString(const velox::core::AggregateCallExpr& agg) {
  std::stringstream out;
  out << agg.name() << "(";
  if (agg.isDistinct()) {
    out << "DISTINCT ";
  }
  for (size_t i = 0; i < agg.inputs().size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << toExprString(*agg.inputs()[i]);
  }
  if (!agg.orderBy().empty()) {
    out << " ";
    appendOrderBy(agg.orderBy(), out);
  }
  out << ")";
  if (agg.filter() != nullptr) {
    out << " FILTER (WHERE " << toExprString(*agg.filter()) << ")";
  }
  return out.str();
}

// Prints a velox::core::IExpr tree in the same format as lp::ExprPrinter.
// This allows comparing DuckDB-parsed expected expressions against actual
// lp::Expr::toString() output.
std::string toExprString(const velox::core::IExpr& expr) {
  using velox::core::IExpr;
  switch (expr.kind()) {
    case IExpr::Kind::kFieldAccess:
      return expr.as<velox::core::FieldAccessExpr>()->name();
    case IExpr::Kind::kCall:
      return callToString(*expr.as<velox::core::CallExpr>());
    case IExpr::Kind::kAggregate:
      return aggregateToString(*expr.as<velox::core::AggregateCallExpr>());
    case IExpr::Kind::kWindow:
      return windowToString(*expr.as<velox::core::WindowCallExpr>());
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
        if (auto* field = e->as<velox::core::FieldAccessExpr>()) {
          if (field->isRootColumn()) {
            auto it = mapping.find(field->name());
            if (it != mapping.end()) {
              return std::make_shared<velox::core::FieldAccessExpr>(
                  it->second, field->alias());
            }
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

// Matches a FilterNode with the specified expression. The expected expression
// is parsed with DuckDB and printed in a format compatible with
// lp::ExprPrinter, then compared against the filter expression's toString().
class FilterMatcher : public LogicalPlanMatcherImpl<FilterNode> {
 public:
  FilterMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::string expression)
      : LogicalPlanMatcherImpl<FilterNode>(inputMatcher, nullptr),
        expression_{std::move(expression)} {}

 private:
  MatchResult matchDetails(
      const FilterNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    velox::parse::DuckSqlExpressionsParser parser;
    auto expected = parser.parseScalarOrWindowExpr(expression_);

    if (!symbols.empty()) {
      expected = rewriteInputNames(expected, symbols);
    }

    EXPECT_EQ(
        toExprString(*expected->dropAlias()), plan.predicate()->toString());
    AXIOM_RETURN_RESULT(symbols)
  }

  const std::string expression_;
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

// Matches a ProjectNode with the specified expressions.
// - String constructor: parses with DuckDB, corrects window frame defaults.
// - ExprApi constructor: uses the expression directly, applies SQL standard
//   default frame when none is specified.
// Both paths compare against expressionAt(i)->toString().
class ProjectMatcher : public LogicalPlanMatcherImpl<ProjectNode> {
 public:
  ProjectMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::vector<std::string> expressions)
      : LogicalPlanMatcherImpl<ProjectNode>(inputMatcher, nullptr),
        expressionStrings_{std::move(expressions)} {}

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

    std::unordered_map<std::string, std::string> newSymbols;
    velox::parse::ParseOptions parseOptions;
    parseOptions.correctWindowFrameDefault = true;
    velox::parse::DuckSqlExpressionsParser parser(parseOptions);

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

      EXPECT_EQ(toExprString(*expectedExpr->dropAlias()), actual->toString())
          << "at index " << i;
      AXIOM_RETURN_IF_FAILURE;
    }

    return MatchResult::success(newSymbols);
  }

  const std::vector<std::string> expressionStrings_;
  const std::vector<ExprApi> expressionApis_;
};

class AggregateMatcher : public LogicalPlanMatcherImpl<AggregateNode> {
 public:
  AggregateMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::vector<std::string> groupingKeys,
      std::vector<std::string> aggregates,
      std::vector<std::vector<int32_t>> groupingSets = {})
      : LogicalPlanMatcherImpl<AggregateNode>(inputMatcher, nullptr),
        groupingKeys_{std::move(groupingKeys)},
        aggregates_{std::move(aggregates)},
        groupingSets_{std::move(groupingSets)} {}

 private:
  MatchResult matchDetails(
      const AggregateNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    EXPECT_EQ(groupingKeys_.size(), plan.groupingKeys().size());
    AXIOM_RETURN_IF_FAILURE;

    for (auto i = 0; i < groupingKeys_.size(); ++i) {
      EXPECT_EQ(groupingKeys_[i], plan.groupingKeys()[i]->toString())
          << "at grouping key index " << i;
      AXIOM_RETURN_IF_FAILURE;
    }

    EXPECT_EQ(aggregates_.size(), plan.aggregates().size());
    AXIOM_RETURN_IF_FAILURE;

    std::unordered_map<std::string, std::string> newSymbols = symbols;
    auto numGroupingKeys = plan.groupingKeys().size();

    velox::parse::DuckSqlExpressionsParser parser;
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
      EXPECT_EQ(toExprString(*parsed->dropAlias()), actual->toString())
          << "at aggregate index " << i;
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

class SortMatcher : public LogicalPlanMatcherImpl<SortNode> {
 public:
  SortMatcher(
      const std::shared_ptr<LogicalPlanMatcher>& inputMatcher,
      std::vector<std::string> ordering,
      std::function<void(const LogicalPlanNodePtr&)> onMatch)
      : LogicalPlanMatcherImpl<SortNode>(inputMatcher, std::move(onMatch)),
        ordering_{std::move(ordering)} {}

 private:
  MatchResult matchDetails(
      const SortNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    const auto& actualOrdering = plan.ordering();
    EXPECT_EQ(actualOrdering.size(), ordering_.size());
    AXIOM_RETURN_IF_FAILURE;

    velox::parse::DuckSqlExpressionsParser parser;
    for (size_t i = 0; i < ordering_.size(); ++i) {
      auto expected = parser.parseOrderByExpr(ordering_[i]);
      auto expectedExpr = expected.expr;

      if (!symbols.empty()) {
        expectedExpr = rewriteInputNames(expectedExpr, symbols);
      }

      EXPECT_EQ(
          toExprString(*expectedExpr->dropAlias()),
          actualOrdering[i].expression->toString())
          << "at ordering index " << i;
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
    const std::string& expression) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<FilterMatcher>(matcher_, expression);
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
    const std::vector<std::vector<int32_t>>& groupingSets) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<AggregateMatcher>(
      matcher_, groupingKeys, aggregates, groupingSets);
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

LogicalPlanMatcherBuilder& LogicalPlanMatcherBuilder::join(
    const std::shared_ptr<LogicalPlanMatcher>& rightMatcher,
    const std::vector<std::string>& outputAliases) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ =
      std::make_shared<JoinMatcher>(matcher_, rightMatcher, outputAliases);
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
    OnMatchCallback onMatch) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ =
      std::make_shared<SortMatcher>(matcher_, ordering, std::move(onMatch));
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
