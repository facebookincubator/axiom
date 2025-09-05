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
#include "velox/connectors/hive/TableHandle.h"
#include "velox/parse/ExpressionsParser.h"

namespace facebook::velox::core {
namespace {

template <typename T = PlanNode>
class PlanMatcherImpl : public PlanMatcher {
 public:
  PlanMatcherImpl() = default;

  explicit PlanMatcherImpl(
      const std::vector<std::shared_ptr<PlanMatcher>>& sourceMatchers)
      : sourceMatchers_{sourceMatchers} {}

  bool match(const PlanNodePtr& plan) const override {
    const auto* specificNode = dynamic_cast<const T*>(plan.get());
    EXPECT_TRUE(specificNode != nullptr)
        << "Expected " << folly::demangle(typeid(T).name()) << ", but got "
        << plan->toString(false, false);
    if (::testing::Test::HasNonfatalFailure()) {
      return false;
    }

    EXPECT_EQ(plan->sources().size(), sourceMatchers_.size());
    if (::testing::Test::HasNonfatalFailure()) {
      return false;
    }

    for (auto i = 0; i < sourceMatchers_.size(); ++i) {
      if (!sourceMatchers_[i]->match(plan->sources()[i])) {
        return false;
      }
    }

    return matchDetails(*specificNode);
  }

 protected:
  virtual bool matchDetails(const T& plan) const {
    return true;
  }

  const std::vector<std::shared_ptr<PlanMatcher>> sourceMatchers_;
};

class TableScanMatcher : public PlanMatcherImpl<TableScanNode> {
 public:
  explicit TableScanMatcher() : PlanMatcherImpl<TableScanNode>() {}

  explicit TableScanMatcher(
      const std::string& tableName,
      const RowTypePtr& columns = nullptr)
      : PlanMatcherImpl<TableScanNode>(),
        tableName_{tableName},
        columns_{columns} {}

  bool matchDetails(const TableScanNode& plan) const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (tableName_.has_value()) {
      EXPECT_EQ(plan.tableHandle()->name(), tableName_.value());
    }

    if (columns_ != nullptr) {
      const auto& outputType = plan.outputType();
      const auto numColumns = outputType->size();

      EXPECT_EQ(numColumns, columns_->size());
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }

      for (auto i = 0; i < numColumns; ++i) {
        auto name = plan.assignments().at(outputType->nameOf(i))->name();

        EXPECT_EQ(name, columns_->nameOf(i));
        EXPECT_EQ(
            outputType->childAt(i)->toString(),
            columns_->childAt(i)->toString());
      }
    }

    return !::testing::Test::HasNonfatalFailure();
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

  bool matchDetails(const TableScanNode& plan) const override {
    SCOPED_TRACE(
        fmt::format("HiveScanMatcher: {}", plan.toString(true, false)));

    const auto* hiveTableHandle =
        dynamic_cast<const connector::hive::HiveTableHandle*>(
            plan.tableHandle().get());
    EXPECT_TRUE(hiveTableHandle != nullptr);
    if (::testing::Test::HasNonfatalFailure()) {
      return false;
    }

    EXPECT_EQ(hiveTableHandle->name(), tableName_);
    if (::testing::Test::HasNonfatalFailure()) {
      return false;
    }

    const auto& filters = hiveTableHandle->subfieldFilters();
    EXPECT_EQ(filters.size(), subfieldFilters_.size());
    if (::testing::Test::HasNonfatalFailure()) {
      return false;
    }

    for (const auto& [name, filter] : filters) {
      EXPECT_TRUE(subfieldFilters_.contains(name))
          << "Expected filter on " << name;
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }

      const auto& expected = subfieldFilters_.at(name);

      EXPECT_TRUE(filter->testingEquals(*expected))
          << "Expected filter on " << name << ": " << expected->toString()
          << ", but got " << filter->toString();
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }
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
      auto expected = parse::parseExpr(remainingFilter_, {});
      EXPECT_EQ(remainingFilter->toString(), expected->toString());
    }

    if (::testing::Test::HasNonfatalFailure()) {
      return false;
    }

    return true;
  }

 private:
  const std::string tableName_;
  const common::SubfieldFilters subfieldFilters_;
  const std::string remainingFilter_;
};

class ValuesMatcher : public PlanMatcherImpl<ValuesNode> {
 public:
  explicit ValuesMatcher(const TypePtr& type = nullptr)
      : PlanMatcherImpl<ValuesNode>(), type_(type) {}

  bool matchDetails(const ValuesNode& plan) const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (type_) {
      EXPECT_TRUE(type_->equivalent(*plan.outputType()))
          << "Expected equal output types on ValuesNode, but got '"
          << type_->toString() << "', and '" << plan.outputType()->toString()
          << "'.";
    }
    return !::testing::Test::HasNonfatalFailure();
  }

 private:
  const TypePtr type_;
};

class FilterMatcher : public PlanMatcherImpl<FilterNode> {
 public:
  explicit FilterMatcher(const std::shared_ptr<PlanMatcher>& matcher)
      : PlanMatcherImpl<FilterNode>({matcher}) {}

  FilterMatcher(
      const std::shared_ptr<PlanMatcher>& matcher,
      const std::string& predicate)
      : PlanMatcherImpl<FilterNode>({matcher}), predicate_{predicate} {}

  bool matchDetails(const FilterNode& plan) const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (predicate_.has_value()) {
      auto expected = parse::parseExpr(predicate_.value(), {});
      EXPECT_EQ(plan.filter()->toString(), expected->toString());
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }
    }

    return true;
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

  bool matchDetails(const ProjectNode& plan) const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (!expressions_.empty()) {
      EXPECT_EQ(plan.projections().size(), expressions_.size());
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }

      for (auto i = 0; i < expressions_.size(); ++i) {
        auto expected = parse::parseExpr(expressions_[i], {});
        EXPECT_EQ(plan.projections()[i]->toString(), expected->toString());
      }
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }
    }

    return true;
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

  bool matchDetails(const ParallelProjectNode& plan) const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (!expressions_.empty()) {
      EXPECT_EQ(plan.projections().size(), expressions_.size());
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }

      for (auto i = 0; i < expressions_.size(); ++i) {
        auto expected = parse::parseExpr(expressions_[i], {});
        EXPECT_EQ(plan.projections()[i]->toString(), expected->toString());
      }
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }
    }

    return true;
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
      const std::vector<std::string>& unnestExprs)
      : PlanMatcherImpl<UnnestNode>({matcher}),
        replicateExprs_{replicateExprs},
        unnestExprs_{unnestExprs} {}

  bool matchDetails(const UnnestNode& plan) const override {
    if (!replicateExprs_.empty()) {
      EXPECT_EQ(plan.replicateVariables().size(), replicateExprs_.size());
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }

      for (auto i = 0; i < replicateExprs_.size(); ++i) {
        auto expected = parse::parseExpr(replicateExprs_[i], {});
        EXPECT_EQ(
            plan.replicateVariables()[i]->toString(), expected->toString());
      }
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }
    }
    if (!unnestExprs_.empty()) {
      EXPECT_EQ(plan.unnestVariables().size(), unnestExprs_.size());
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }

      for (auto i = 0; i < unnestExprs_.size(); ++i) {
        auto expected = parse::parseExpr(unnestExprs_[i], {});
        EXPECT_EQ(plan.unnestVariables()[i]->toString(), expected->toString());
      }
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }
    }
    return true;
  }

 private:
  const std::vector<std::string> replicateExprs_;
  const std::vector<std::string> unnestExprs_;
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

  bool matchDetails(const LimitNode& plan) const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (count_.has_value()) {
      EXPECT_EQ(plan.offset(), offset_.value());
      EXPECT_EQ(plan.count(), count_.value());
      EXPECT_EQ(plan.isPartial(), partial_.value());
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }
    }

    return true;
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

  bool matchDetails(const TopNNode& plan) const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (count_.has_value()) {
      EXPECT_EQ(plan.count(), count_.value());
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }
    }

    return true;
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

  bool matchDetails(const OrderByNode& plan) const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (!ordering_.empty()) {
      EXPECT_EQ(plan.sortingOrders().size(), ordering_.size());
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }

      for (auto i = 0; i < ordering_.size(); ++i) {
        const auto expected = parse::parseOrderByExpr(ordering_[i]);

        EXPECT_EQ(plan.sortingKeys()[i]->toString(), expected.expr->toString());
        EXPECT_EQ(plan.sortingOrders()[i].isAscending(), expected.ascending);
        EXPECT_EQ(plan.sortingOrders()[i].isNullsFirst(), expected.nullsFirst);
        if (::testing::Test::HasNonfatalFailure()) {
          return false;
        }
      }
    }

    return true;
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

  bool matchDetails(const AggregationNode& plan) const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (step_.has_value()) {
      EXPECT_EQ(plan.step(), step_.value());
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }
    }

    return true;
  }

 private:
  const std::optional<AggregationNode::Step> step_;
};

class HashJoinMatcher : public PlanMatcherImpl<HashJoinNode> {
 public:
  explicit HashJoinMatcher(
      const std::shared_ptr<PlanMatcher>& left,
      const std::shared_ptr<PlanMatcher>& right)
      : PlanMatcherImpl<HashJoinNode>({left, right}) {}

  HashJoinMatcher(
      const std::shared_ptr<PlanMatcher>& left,
      const std::shared_ptr<PlanMatcher>& right,
      JoinType joinType)
      : PlanMatcherImpl<HashJoinNode>({left, right}), joinType_{joinType} {}

  bool matchDetails(const HashJoinNode& plan) const override {
    SCOPED_TRACE(plan.toString(true, false));

    if (joinType_.has_value()) {
      EXPECT_EQ(
          JoinTypeName::toName(plan.joinType()),
          JoinTypeName::toName(joinType_.value()));
      if (::testing::Test::HasNonfatalFailure()) {
        return false;
      }
    }

    return true;
  }

 private:
  const std::optional<JoinType> joinType_;
};

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

PlanMatcherBuilder& PlanMatcherBuilder::values(const TypePtr& type) {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<ValuesMatcher>(type);
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
  matcher_ = std::make_shared<UnnestMatcher>(matcher_);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::unnest(
    const std::vector<std::string>& replicateExprs,
    const std::vector<std::string>& unnestExprs) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ =
      std::make_shared<UnnestMatcher>(matcher_, replicateExprs, unnestExprs);
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

PlanMatcherBuilder& PlanMatcherBuilder::partialAggregation() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<AggregationMatcher>(
      matcher_, AggregationNode::Step::kPartial);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::finalAggregation() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<AggregationMatcher>(
      matcher_, AggregationNode::Step::kFinal);
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
    JoinType joinType) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ =
      std::make_shared<HashJoinMatcher>(matcher_, rightMatcher, joinType);
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::localPartition() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<PlanMatcherImpl<LocalPartitionNode>>(
      std::vector<std::shared_ptr<PlanMatcher>>{matcher_});
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::localPartition(
    const std::shared_ptr<PlanMatcher>& matcher) {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<PlanMatcherImpl<LocalPartitionNode>>(
      std::vector<std::shared_ptr<PlanMatcher>>{matcher_, matcher});
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::localMerge() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<PlanMatcherImpl<LocalMergeNode>>(
      std::vector<std::shared_ptr<PlanMatcher>>{matcher_});
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::partitionedOutput() {
  VELOX_USER_CHECK_NOT_NULL(matcher_);
  matcher_ = std::make_shared<PlanMatcherImpl<PartitionedOutputNode>>(
      std::vector<std::shared_ptr<PlanMatcher>>{matcher_});
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::exchange() {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<PlanMatcherImpl<ExchangeNode>>();
  return *this;
}

PlanMatcherBuilder& PlanMatcherBuilder::mergeExchange() {
  VELOX_USER_CHECK_NULL(matcher_);
  matcher_ = std::make_shared<PlanMatcherImpl<MergeExchangeNode>>();
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

} // namespace facebook::velox::core
