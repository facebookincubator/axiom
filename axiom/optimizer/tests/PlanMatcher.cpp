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

#include "axiom/optimizer/tests/PlanMatcher.h"
#include <gtest/gtest.h>
#include "axiom/runner/MultiFragmentPlan.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/duckdb/conversion/DuckParser.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"

namespace facebook::velox::core {
namespace {

#define AXIOM_TEST_RETURN_IF_FAILURE           \
  if (::testing::Test::HasNonfatalFailure()) { \
    return MatchResult::failure();             \
  }

#define AXIOM_TEST_RETURN                      \
  if (::testing::Test::HasNonfatalFailure()) { \
    return MatchResult::failure();             \
  } else {                                     \
    return MatchResult::success();             \
  }

template <typename T = PlanNode>
class PlanMatcherImpl : public PlanMatcher {
 public:
  PlanMatcherImpl() = default;

  explicit PlanMatcherImpl(
      const std::vector<std::shared_ptr<PlanMatcher>>& sourceMatchers)
      : sourceMatchers_{sourceMatchers} {}

  MatchResult match(
      const PlanNodePtr& plan,
      const std::unordered_map<std::string, std::string>& symbols,
      const DistributedMatchContext* context) const override {
    const auto* specificNode = dynamic_cast<const T*>(plan.get());
    EXPECT_TRUE(specificNode != nullptr)
        << "Expected " << folly::demangle(typeid(T).name()) << ", but got "
        << plan->toString(false, false);
    AXIOM_TEST_RETURN_IF_FAILURE

    EXPECT_EQ(plan->sources().size(), sourceMatchers_.size());
    AXIOM_TEST_RETURN_IF_FAILURE

    std::unordered_map<std::string, std::string> newSymbols;

    for (auto i = 0; i < sourceMatchers_.size(); ++i) {
      auto result =
          sourceMatchers_[i]->match(plan->sources()[i], symbols, context);
      if (!result.match) {
        return MatchResult::failure();
      }

      // TODO Combine symbols from all sources.
      newSymbols = std::move(result.symbols);
    }

    if (sourceMatchers_.size() > 1) {
      // TODO Add support for multiple sources.
      newSymbols.clear();
    }

    return matchDetails(*specificNode, newSymbols);
  }

  int32_t shuffleBoundaryCount() const override {
    int32_t count = 0;
    for (const auto& sourceMatcher : sourceMatchers_) {
      count += sourceMatcher->shuffleBoundaryCount();
    }
    return count;
  }

 protected:
  virtual MatchResult matchDetails(
      const T& plan,
      const std::unordered_map<std::string, std::string>& /* symbols */) const {
    return MatchResult::success();
  }

  const std::vector<std::shared_ptr<PlanMatcher>> sourceMatchers_;
};

velox::core::ExprPtr rewriteInputNames(
    const velox::core::ExprPtr& expr,
    const std::unordered_map<std::string, std::string>& mapping) {
  if (expr->is(IExpr::Kind::kFieldAccess)) {
    auto fieldAccess = expr->as<velox::core::FieldAccessExpr>();
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

class TableScanMatcher : public PlanMatcherImpl<TableScanNode> {
 public:
  explicit TableScanMatcher() : PlanMatcherImpl<TableScanNode>() {}

  explicit TableScanMatcher(
      const std::string& tableName,
      const RowTypePtr& columns = nullptr)
      : PlanMatcherImpl<TableScanNode>(),
        tableName_{tableName},
        columns_{columns} {}

  MatchResult matchDetails(
      const TableScanNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (tableName_.has_value()) {
      EXPECT_EQ(plan.tableHandle()->name(), tableName_.value());
    }

    if (columns_ != nullptr) {
      const auto& outputType = plan.outputType();
      const auto numColumns = outputType->size();

      EXPECT_EQ(numColumns, columns_->size());
      AXIOM_TEST_RETURN_IF_FAILURE

      for (auto i = 0; i < numColumns; ++i) {
        auto name = plan.assignments().at(outputType->nameOf(i))->name();

        EXPECT_EQ(name, columns_->nameOf(i));
        EXPECT_EQ(
            outputType->childAt(i)->toString(),
            columns_->childAt(i)->toString());
      }
    }

    AXIOM_TEST_RETURN
  }

 private:
  const std::optional<std::string> tableName_;
  const RowTypePtr columns_;
};

class HiveScanMatcher : public PlanMatcherImpl<TableScanNode> {
 public:
  HiveScanMatcher(
      const std::string& tableName,
      common::SubfieldFilters subfieldFilters,
      const std::string& remainingFilter)
      : PlanMatcherImpl<TableScanNode>(),
        tableName_{tableName},
        subfieldFilters_{std::move(subfieldFilters)},
        remainingFilter_{remainingFilter} {}

  MatchResult matchDetails(
      const TableScanNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(
        fmt::format("HiveScanMatcher: {}", plan.toString(true, false)));

    const auto* hiveTableHandle =
        dynamic_cast<const connector::hive::HiveTableHandle*>(
            plan.tableHandle().get());
    EXPECT_TRUE(hiveTableHandle != nullptr);
    AXIOM_TEST_RETURN_IF_FAILURE

    EXPECT_EQ(hiveTableHandle->name(), tableName_);
    AXIOM_TEST_RETURN_IF_FAILURE

    const auto& filters = hiveTableHandle->subfieldFilters();
    EXPECT_EQ(filters.size(), subfieldFilters_.size());
    AXIOM_TEST_RETURN_IF_FAILURE

    for (const auto& [name, filter] : filters) {
      EXPECT_TRUE(subfieldFilters_.contains(name))
          << "Expected filter on " << name;
      AXIOM_TEST_RETURN_IF_FAILURE

      const auto& expected = subfieldFilters_.at(name);

      EXPECT_TRUE(filter->testingEquals(*expected))
          << "Expected filter on " << name << ": " << expected->toString()
          << ", but got " << filter->toString();
      AXIOM_TEST_RETURN_IF_FAILURE
    }

    const auto& remainingFilter = hiveTableHandle->remainingFilter();
    if (remainingFilter == nullptr) {
      EXPECT_TRUE(remainingFilter_.empty())
          << "Expected remaining filter: " << remainingFilter_;
    } else if (remainingFilter_.empty()) {
      EXPECT_TRUE(remainingFilter == nullptr)
          << "Expected no remaining filter, but got "
          << remainingFilter->toString();
    } else {
      auto expected =
          parse::DuckSqlExpressionsParser().parseExpr(remainingFilter_);
      EXPECT_EQ(remainingFilter->toString(), expected->toString());
    }

    AXIOM_TEST_RETURN
  }

 private:
  const std::string tableName_;
  const common::SubfieldFilters subfieldFilters_;
  const std::string remainingFilter_;
};

class ValuesMatcher : public PlanMatcherImpl<ValuesNode> {
 public:
  explicit ValuesMatcher(const RowTypePtr& outputType = nullptr)
      : PlanMatcherImpl<ValuesNode>(), outputType_(outputType) {}

  MatchResult matchDetails(
      const ValuesNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (outputType_) {
      EXPECT_TRUE(outputType_->equivalent(*plan.outputType()))
          << "Expected equal output types on ValuesNode, but got '"
          << outputType_->toString() << "' and '"
          << plan.outputType()->toString() << "'.";
    }

    AXIOM_TEST_RETURN
  }

 private:
  const RowTypePtr outputType_;
};

class FilterMatcher : public PlanMatcherImpl<FilterNode> {
 public:
  explicit FilterMatcher(const std::shared_ptr<PlanMatcher>& matcher)
      : PlanMatcherImpl<FilterNode>({matcher}) {}

  FilterMatcher(
      const std::shared_ptr<PlanMatcher>& matcher,
      const std::string& predicate)
      : PlanMatcherImpl<FilterNode>({matcher}), predicate_{predicate} {}

  MatchResult matchDetails(
      const FilterNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (predicate_.has_value()) {
      auto expected =
          parse::DuckSqlExpressionsParser().parseExpr(predicate_.value());
      if (!symbols.empty()) {
        expected = rewriteInputNames(expected, symbols);
      }
      EXPECT_EQ(plan.filter()->toString(), expected->toString());
    }

    AXIOM_TEST_RETURN
  }

 private:
  const std::optional<std::string> predicate_;
};

class ProjectMatcher : public PlanMatcherImpl<ProjectNode> {
 public:
  explicit ProjectMatcher(const std::shared_ptr<PlanMatcher>& matcher)
      : PlanMatcherImpl<ProjectNode>({matcher}) {}

  ProjectMatcher(
      const std::shared_ptr<PlanMatcher>& matcher,
      const std::vector<std::string>& expressions)
      : PlanMatcherImpl<ProjectNode>({matcher}), expressions_{expressions} {}

  MatchResult matchDetails(
      const ProjectNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(plan.toString(true, false));

    std::unordered_map<std::string, std::string> newSymbols;

    if (!expressions_.empty()) {
      EXPECT_EQ(plan.projections().size(), expressions_.size());
      AXIOM_TEST_RETURN_IF_FAILURE

      for (auto i = 0; i < expressions_.size(); ++i) {
        auto expected =
            parse::DuckSqlExpressionsParser().parseExpr(expressions_[i]);
        if (expected->alias()) {
          newSymbols[expected->alias().value()] = plan.names()[i];
        }

        if (!symbols.empty()) {
          expected = rewriteInputNames(expected, symbols);
        }

        EXPECT_EQ(
            plan.projections()[i]->toString(),
            expected->dropAlias()->toString());
      }
      AXIOM_TEST_RETURN_IF_FAILURE
    }

    return MatchResult::success(newSymbols);
  }

 private:
  const std::vector<std::string> expressions_;
};

class ParallelProjectMatcher : public PlanMatcherImpl<ParallelProjectNode> {
 public:
  explicit ParallelProjectMatcher(const std::shared_ptr<PlanMatcher>& matcher)
      : PlanMatcherImpl<ParallelProjectNode>({matcher}) {}

  ParallelProjectMatcher(
      const std::shared_ptr<PlanMatcher>& matcher,
      const std::vector<std::string>& expressions)
      : PlanMatcherImpl<ParallelProjectNode>({matcher}),
        expressions_{expressions} {}

  MatchResult matchDetails(
      const ParallelProjectNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (!expressions_.empty()) {
      EXPECT_EQ(plan.projections().size(), expressions_.size());
      AXIOM_TEST_RETURN_IF_FAILURE

      for (auto i = 0; i < expressions_.size(); ++i) {
        auto expected =
            parse::DuckSqlExpressionsParser().parseExpr(expressions_[i]);
        EXPECT_EQ(plan.projections()[i]->toString(), expected->toString());
      }
      AXIOM_TEST_RETURN_IF_FAILURE
    }

    return MatchResult::success();
  }

 private:
  const std::vector<std::string> expressions_;
};

class UnnestMatcher : public PlanMatcherImpl<UnnestNode> {
 public:
  explicit UnnestMatcher(const std::shared_ptr<PlanMatcher>& matcher)
      : PlanMatcherImpl<UnnestNode>({matcher}) {}

  UnnestMatcher(
      const std::shared_ptr<PlanMatcher>& matcher,
      const std::vector<std::string>& replicateExprs,
      const std::vector<std::string>& unnestExprs,
      const std::optional<std::string>& ordinalityName = std::nullopt)
      : PlanMatcherImpl<UnnestNode>({matcher}),
        replicateExprs_{replicateExprs},
        unnestExprs_{unnestExprs},
        ordinalityName_{ordinalityName} {}

  MatchResult matchDetails(
      const UnnestNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    if (replicateExprs_.has_value()) {
      EXPECT_EQ(plan.replicateVariables().size(), replicateExprs_->size());
      AXIOM_TEST_RETURN_IF_FAILURE

      for (auto i = 0; i < replicateExprs_->size(); ++i) {
        auto expected =
            parse::DuckSqlExpressionsParser().parseExpr((*replicateExprs_)[i]);
        if (!symbols.empty()) {
          expected = rewriteInputNames(expected, symbols);
        }

        EXPECT_EQ(
            plan.replicateVariables()[i]->toString(), expected->toString());
      }
      AXIOM_TEST_RETURN_IF_FAILURE
    }

    if (unnestExprs_.has_value()) {
      EXPECT_EQ(plan.unnestVariables().size(), unnestExprs_->size());
      AXIOM_TEST_RETURN_IF_FAILURE

      for (auto i = 0; i < unnestExprs_->size(); ++i) {
        auto expected =
            parse::DuckSqlExpressionsParser().parseExpr((*unnestExprs_)[i]);
        if (!symbols.empty()) {
          expected = rewriteInputNames(expected, symbols);
        }

        EXPECT_EQ(plan.unnestVariables()[i]->toString(), expected->toString());
      }
      AXIOM_TEST_RETURN_IF_FAILURE
    }

    EXPECT_EQ(plan.ordinalityName(), ordinalityName_);

    AXIOM_TEST_RETURN_IF_FAILURE

    return MatchResult::success();
  }

 private:
  const std::optional<std::vector<std::string>> replicateExprs_;
  const std::optional<std::vector<std::string>> unnestExprs_;
  const std::optional<std::string> ordinalityName_;
};

class LimitMatcher : public PlanMatcherImpl<LimitNode> {
 public:
  explicit LimitMatcher(const std::shared_ptr<PlanMatcher>& matcher)
      : PlanMatcherImpl<LimitNode>({matcher}) {}

  LimitMatcher(
      const std::shared_ptr<PlanMatcher>& matcher,
      int64_t offset,
      int64_t count,
      bool partial)
      : PlanMatcherImpl<LimitNode>({matcher}),
        offset_{offset},
        count_{count},
        partial_{partial} {}

  MatchResult matchDetails(
      const LimitNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (count_.has_value()) {
      EXPECT_EQ(plan.offset(), offset_.value());
      EXPECT_EQ(plan.count(), count_.value());
      EXPECT_EQ(plan.isPartial(), partial_.value());
    }

    AXIOM_TEST_RETURN
  }

 private:
  const std::optional<int64_t> offset_;
  const std::optional<int64_t> count_;
  const std::optional<bool> partial_;
};

class TopNMatcher : public PlanMatcherImpl<TopNNode> {
 public:
  explicit TopNMatcher(const std::shared_ptr<PlanMatcher>& matcher)
      : PlanMatcherImpl<TopNNode>({matcher}) {}

  TopNMatcher(const std::shared_ptr<PlanMatcher>& matcher, int64_t count)
      : PlanMatcherImpl<TopNNode>({matcher}), count_{count} {}

  MatchResult matchDetails(
      const TopNNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (count_.has_value()) {
      EXPECT_EQ(plan.count(), count_.value());
    }

    return MatchResult::success(symbols);
  }

 private:
  const std::optional<int64_t> count_;
};

class OrderByMatcher : public PlanMatcherImpl<OrderByNode> {
 public:
  explicit OrderByMatcher(const std::shared_ptr<PlanMatcher>& matcher)
      : PlanMatcherImpl<OrderByNode>({matcher}) {}

  OrderByMatcher(
      const std::shared_ptr<PlanMatcher>& matcher,
      const std::vector<std::string>& ordering)
      : PlanMatcherImpl<OrderByNode>({matcher}), ordering_{ordering} {}

  MatchResult matchDetails(
      const OrderByNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (!ordering_.empty()) {
      EXPECT_EQ(plan.sortingOrders().size(), ordering_.size());
      AXIOM_TEST_RETURN_IF_FAILURE

      for (auto i = 0; i < ordering_.size(); ++i) {
        auto expected =
            parse::DuckSqlExpressionsParser().parseOrderByExpr(ordering_[i]);
        auto expectedExpr = expected.expr;
        if (!symbols.empty()) {
          expectedExpr = rewriteInputNames(expectedExpr, symbols);
        }

        EXPECT_EQ(plan.sortingKeys()[i]->toString(), expectedExpr->toString());
        EXPECT_EQ(plan.sortingOrders()[i].isAscending(), expected.ascending);
        EXPECT_EQ(plan.sortingOrders()[i].isNullsFirst(), expected.nullsFirst);
        AXIOM_TEST_RETURN_IF_FAILURE
      }
    }

    return MatchResult::success(symbols);
  }

 private:
  const std::vector<std::string> ordering_;
};

class AggregationMatcher : public PlanMatcherImpl<AggregationNode> {
 public:
  explicit AggregationMatcher(const std::shared_ptr<PlanMatcher>& matcher)
      : PlanMatcherImpl<AggregationNode>({matcher}) {}

  AggregationMatcher(
      const std::shared_ptr<PlanMatcher>& matcher,
      AggregationNode::Step step)
      : PlanMatcherImpl<AggregationNode>({matcher}), step_{step} {}

  AggregationMatcher(
      const std::shared_ptr<PlanMatcher>& matcher,
      AggregationNode::Step step,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates)
      : PlanMatcherImpl<AggregationNode>({matcher}),
        step_{step},
        groupingKeys_{groupingKeys},
        aggregates_{aggregates} {
    VELOX_CHECK(!groupingKeys_.empty() || !aggregates_.empty());
  }

  MatchResult matchDetails(
      const AggregationNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (step_.has_value()) {
      EXPECT_EQ(plan.step(), step_.value());
      AXIOM_TEST_RETURN_IF_FAILURE
    }

    std::unordered_map<std::string, std::string> newSymbols = symbols;
    if (!groupingKeys_.empty() || !aggregates_.empty()) {
      // Verify grouping keys.
      EXPECT_EQ(plan.groupingKeys().size(), groupingKeys_.size());
      AXIOM_TEST_RETURN_IF_FAILURE

      for (auto i = 0; i < groupingKeys_.size(); ++i) {
        auto expected =
            parse::DuckSqlExpressionsParser().parseExpr(groupingKeys_[i]);
        EXPECT_EQ(plan.groupingKeys()[i]->toString(), expected->toString());
      }
      AXIOM_TEST_RETURN_IF_FAILURE

      // Verify aggregates.
      EXPECT_EQ(plan.aggregates().size(), aggregates_.size());
      AXIOM_TEST_RETURN_IF_FAILURE

      for (auto i = 0; i < aggregates_.size(); ++i) {
        auto aggregateExpr = duckdb::parseAggregateExpr(aggregates_[i], {});
        auto expected = rewriteInputNames(aggregateExpr.expr, newSymbols);
        if (expected->alias()) {
          newSymbols[expected->alias().value()] = plan.aggregateNames()[i];
        }

        EXPECT_EQ(
            plan.aggregates()[i].call->toString(),
            expected->dropAlias()->toString());

        AXIOM_TEST_RETURN_IF_FAILURE

        auto expectedMask = aggregateExpr.maskExpr;
        const auto& mask = plan.aggregates()[i].mask;
        EXPECT_EQ(mask != nullptr, expectedMask != nullptr);
        AXIOM_TEST_RETURN_IF_FAILURE

        if (expectedMask) {
          if (!symbols.empty()) {
            expectedMask = rewriteInputNames(expectedMask, symbols);
          }
          EXPECT_EQ(mask->toString(), expectedMask->toString())
              << "Mask mismatch for aggregate " << i;
        }

        // Verify ORDER BY.
        const auto& expectedOrderBy = aggregateExpr.orderBy;
        const auto& sortingKeys = plan.aggregates()[i].sortingKeys;
        const auto& sortingOrders = plan.aggregates()[i].sortingOrders;

        EXPECT_EQ(sortingKeys.size(), expectedOrderBy.size())
            << "ORDER BY clause size mismatch for aggregate " << i;
        AXIOM_TEST_RETURN_IF_FAILURE

        for (auto j = 0; j < expectedOrderBy.size(); ++j) {
          auto expectedKey = expectedOrderBy[j].expr;
          if (!symbols.empty()) {
            expectedKey = rewriteInputNames(expectedKey, symbols);
          }

          EXPECT_EQ(sortingKeys[j]->toString(), expectedKey->toString())
              << "ORDER BY key mismatch for aggregate " << i << ", key " << j;
          EXPECT_EQ(
              sortingOrders[j].isAscending(), expectedOrderBy[j].ascending)
              << "ORDER BY ascending mismatch for aggregate " << i << ", key "
              << j;
          EXPECT_EQ(
              sortingOrders[j].isNullsFirst(), expectedOrderBy[j].nullsFirst)
              << "ORDER BY nullsFirst mismatch for aggregate " << i << ", key "
              << j;
        }
      }
      AXIOM_TEST_RETURN_IF_FAILURE
    }

    return MatchResult::success(std::move(newSymbols));
  }

 private:
  const std::optional<AggregationNode::Step> step_;
  const std::vector<std::string> groupingKeys_;
  const std::vector<std::string> aggregates_;
};

class HashJoinMatcher : public PlanMatcherImpl<HashJoinNode> {
 public:
  HashJoinMatcher(
      const std::shared_ptr<PlanMatcher>& left,
      const std::shared_ptr<PlanMatcher>& right)
      : PlanMatcherImpl<HashJoinNode>({left, right}) {}

  HashJoinMatcher(
      const std::shared_ptr<PlanMatcher>& left,
      const std::shared_ptr<PlanMatcher>& right,
      JoinType joinType,
      bool nullAware)
      : PlanMatcherImpl<HashJoinNode>({left, right}),
        joinType_{joinType},
        nullAware_{nullAware} {}

  MatchResult matchDetails(
      const HashJoinNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (joinType_.has_value()) {
      EXPECT_EQ(
          JoinTypeName::toName(plan.joinType()),
          JoinTypeName::toName(joinType_.value()));
    }

    if (nullAware_.has_value()) {
      EXPECT_EQ(plan.isNullAware(), nullAware_.value());
    }

    AXIOM_TEST_RETURN
  }

 private:
  const std::optional<JoinType> joinType_;
  const std::optional<bool> nullAware_;
};

class NestedLoopJoinMatcher : public PlanMatcherImpl<NestedLoopJoinNode> {
 public:
  NestedLoopJoinMatcher(
      const std::shared_ptr<PlanMatcher>& left,
      const std::shared_ptr<PlanMatcher>& right,
      JoinType joinType)
      : PlanMatcherImpl<NestedLoopJoinNode>({left, right}),
        joinType_{joinType} {}

  MatchResult matchDetails(
      const NestedLoopJoinNode& plan,
      const std::unordered_map<std::string, std::string>& symbols)
      const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (joinType_.has_value()) {
      EXPECT_EQ(
          JoinTypeName::toName(plan.joinType()),
          JoinTypeName::toName(joinType_.value()));
    }

    AXIOM_TEST_RETURN
  }

 private:
  const std::optional<JoinType> joinType_;
};

// Marks a shuffle boundary between fragments in a distributed plan.
// When matching a single PlanNodePtr (via match(PlanNodePtr)):
//   - Fails the match (shuffle boundaries require distributed plan matching)
// When matching a MultiFragmentPlan (via match(MultiFragmentPlan)):
//   - Producer side expects PartitionedOutput
//   - Consumer side expects Exchange (or MergeExchange if ordered)
class ShuffleBoundaryMatcher : public PlanMatcher {
 public:
  explicit ShuffleBoundaryMatcher(
      std::shared_ptr<PlanMatcher> producerMatcher,
      bool ordered = false)
      : producerMatcher_(std::move(producerMatcher)), ordered_(ordered) {}

  MatchResult match(
      const PlanNodePtr& plan,
      const std::unordered_map<std::string, std::string>& symbols,
      const DistributedMatchContext* context) const override;

  int32_t shuffleBoundaryCount() const override {
    // Count this shuffle boundary plus any in the producer matcher.
    return 1 + producerMatcher_->shuffleBoundaryCount();
  }

 private:
  const std::shared_ptr<PlanMatcher> producerMatcher_;
  const bool ordered_;
};

// Returns the producer fragment for the given Exchange node, or nullptr if not
// found.
const axiom::runner::ExecutableFragment* findProducerFragment(
    const PlanNodeId& exchangeNodeId,
    const PlanMatcher::DistributedMatchContext& context) {
  for (const auto& inputStage : context.currentFragment->inputStages) {
    if (inputStage.consumerNodeId == exchangeNodeId) {
      auto it = context.taskPrefixToFragmentIndex->find(
          inputStage.producerTaskPrefix);
      if (it != context.taskPrefixToFragmentIndex->end()) {
        return &context.fragments->at(it->second);
      }
      break;
    }
  }
  return nullptr;
}

// Implementation of ShuffleBoundaryMatcher::match.
// When context is set, verifies Exchange and matches producer.
// Otherwise, throws an error.
PlanMatcher::MatchResult ShuffleBoundaryMatcher::match(
    const PlanNodePtr& plan,
    const std::unordered_map<std::string, std::string>& symbols,
    const DistributedMatchContext* context) const {
  VELOX_CHECK_NOT_NULL(
      context,
      "Cannot match PlanMatcher with shuffle boundaries against a single "
      "PlanNodePtr. Use match(MultiFragmentPlan) for distributed plans.");

  // Verify the plan is an Exchange or MergeExchange.
  if (ordered_) {
    EXPECT_TRUE(dynamic_cast<const MergeExchangeNode*>(plan.get()) != nullptr)
        << "Expected MergeExchange at shuffle boundary, but got "
        << plan->toString(false, false);
  } else {
    EXPECT_TRUE(dynamic_cast<const ExchangeNode*>(plan.get()) != nullptr)
        << "Expected Exchange at shuffle boundary, but got "
        << plan->toString(false, false);
  }
  AXIOM_TEST_RETURN_IF_FAILURE

  // Find the producer fragment.
  const auto* producerFragment = findProducerFragment(plan->id(), *context);
  EXPECT_TRUE(producerFragment != nullptr)
      << "Could not find producer fragment for Exchange " << plan->id();
  AXIOM_TEST_RETURN_IF_FAILURE

  // Match the producer fragment (expects PartitionedOutput at root).
  const auto& fragmentPlan = producerFragment->fragment.planNode;

  const auto* partitionedOutput =
      dynamic_cast<const PartitionedOutputNode*>(fragmentPlan.get());
  EXPECT_TRUE(partitionedOutput != nullptr)
      << "Expected PartitionedOutput at fragment root, but got "
      << fragmentPlan->toString(false, false);
  AXIOM_TEST_RETURN_IF_FAILURE

  // Update context for producer fragment matching.
  DistributedMatchContext producerContext{
      context->fragments, producerFragment, context->taskPrefixToFragmentIndex};

  return producerMatcher_->match(
      partitionedOutput->sources()[0], symbols, &producerContext);
}

#undef AXIOM_TEST_RETURN
#undef AXIOM_TEST_RETURN_IF_FAILURE

} // namespace

PlanMatcherBuilder& PlanMatcherBuilder::tableScan() {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<TableScanMatcher>();
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::tableScan(
    const std::string& tableName) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<TableScanMatcher>(tableName);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::tableScan(
    const std::string& tableName,
    const RowTypePtr& outputType) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<TableScanMatcher>(tableName, outputType);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::hiveScan(
    const std::string& tableName,
    common::SubfieldFilters subfieldFilters,
    const std::string& remainingFilter) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<HiveScanMatcher>(
      tableName, std::move(subfieldFilters), remainingFilter);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::values() {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<ValuesMatcher>();
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::values(const RowTypePtr& outputType) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<ValuesMatcher>(outputType);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::filter() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<FilterMatcher>(matcher_);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::filter(const std::string& predicate) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<FilterMatcher>(matcher_, predicate);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::project() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<ProjectMatcher>(matcher_);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::project(
    const std::vector<std::string>& expressions) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<ProjectMatcher>(matcher_, expressions);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::parallelProject() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<ParallelProjectMatcher>(matcher_);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::parallelProject(
    const std::vector<std::string>& expressions) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<ParallelProjectMatcher>(matcher_, expressions);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::unnest() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<PlanMatcherImpl<UnnestNode>>(
      std::vector<std::shared_ptr<PlanMatcher>>{matcher_});
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::unnest(
    const std::vector<std::string>& replicateExprs,
    const std::vector<std::string>& unnestExprs,
    const std::optional<std::string>& ordinalityName) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<UnnestMatcher>(
      matcher_, replicateExprs, unnestExprs, ordinalityName);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::aggregation() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<AggregationMatcher>(matcher_);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::singleAggregation() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<AggregationMatcher>(
      matcher_, AggregationNode::Step::kSingle);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::singleAggregation(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<AggregationMatcher>(
      matcher_, AggregationNode::Step::kSingle, groupingKeys, aggregates);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::partialAggregation() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<AggregationMatcher>(
      matcher_, AggregationNode::Step::kPartial);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::partialAggregation(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<AggregationMatcher>(
      matcher_, AggregationNode::Step::kPartial, groupingKeys, aggregates);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::finalAggregation() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<AggregationMatcher>(
      matcher_, AggregationNode::Step::kFinal);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::finalAggregation(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<AggregationMatcher>(
      matcher_, AggregationNode::Step::kFinal, groupingKeys, aggregates);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::hashJoin(
    const std::shared_ptr<PlanMatcher>& rightMatcher) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<HashJoinMatcher>(matcher_, rightMatcher);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::hashJoin(
    const std::shared_ptr<PlanMatcher>& rightMatcher,
    JoinType joinType,
    bool nullAware) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<HashJoinMatcher>(
      matcher_, rightMatcher, joinType, nullAware);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::nestedLoopJoin(
    const std::shared_ptr<PlanMatcher>& rightMatcher,
    JoinType joinType) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ =
      std::make_shared<NestedLoopJoinMatcher>(matcher_, rightMatcher, joinType);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::localPartition() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<PlanMatcherImpl<LocalPartitionNode>>(
      std::vector<std::shared_ptr<PlanMatcher>>{matcher_});
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::localPartition(
    std::initializer_list<std::shared_ptr<PlanMatcher>> matcher) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  std::vector<std::shared_ptr<PlanMatcher>> matchers{matcher_};
  matchers.insert(matchers.end(), matcher);
  matcher_ = std::make_shared<PlanMatcherImpl<LocalPartitionNode>>(
      std::move(matchers));
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::localMerge() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<PlanMatcherImpl<LocalMergeNode>>(
      std::vector<std::shared_ptr<PlanMatcher>>{matcher_});
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::exchange() {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<PlanMatcherImpl<ExchangeNode>>();
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::shuffle() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ =
      std::make_shared<ShuffleBoundaryMatcher>(matcher_, /*ordered=*/false);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::shuffleMerge() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ =
      std::make_shared<ShuffleBoundaryMatcher>(matcher_, /*ordered=*/true);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::limit() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LimitMatcher>(matcher_);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::partialLimit(
    int64_t offset,
    int64_t count) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LimitMatcher>(matcher_, offset, count, true);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::finalLimit(
    int64_t offset,
    int64_t count) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<LimitMatcher>(matcher_, offset, count, false);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::topN() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<TopNMatcher>(matcher_);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::topN(int64_t count) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<TopNMatcher>(matcher_, count);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::orderBy() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<OrderByMatcher>(matcher_);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::orderBy(
    const std::vector<std::string>& ordering) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<OrderByMatcher>(matcher_, ordering);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::tableWrite() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<PlanMatcherImpl<TableWriteNode>>(
      std::vector<std::shared_ptr<PlanMatcher>>{matcher_});
  return *this;
}

bool PlanMatcher::match(const axiom::runner::MultiFragmentPlan& plan) const {
  const auto& fragments = plan.fragments();
  EXPECT_FALSE(fragments.empty()) << "MultiFragmentPlan has no fragments";
  if (testing::Test::HasNonfatalFailure()) {
    return false;
  }

  // Count shuffle boundaries in the matcher using the virtual method.
  const int32_t numShuffles = this->shuffleBoundaryCount();

  // Expected: N shuffle boundaries = N+1 fragments.
  const int32_t expectedFragments = numShuffles + 1;
  EXPECT_EQ(static_cast<int32_t>(fragments.size()), expectedFragments)
      << "Expected " << expectedFragments << " fragments for " << numShuffles
      << " shuffle boundaries, but got " << fragments.size();
  if (testing::Test::HasNonfatalFailure()) {
    return false;
  }

  // Build mapping from task prefix to fragment index.
  std::unordered_map<std::string, int32_t> taskPrefixToFragmentIndex;
  for (int32_t i = 0; i < fragments.size(); ++i) {
    taskPrefixToFragmentIndex[fragments[i].taskPrefix] = i;
  }

  // The root fragment is the last one by convention.
  const auto& rootFragment = fragments.back();

  // Set up the distributed match context.
  DistributedMatchContext context{
      &fragments, &rootFragment, &taskPrefixToFragmentIndex};

  // Match the root fragment against the matcher.
  return this->match(rootFragment.fragment.planNode, {}, &context).match;
}

} // namespace facebook::velox::core
