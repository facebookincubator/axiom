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

#include "velox/dwio/dwrf/writer/StatisticsBuilder.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/hive/StatisticsBuilder.h"

namespace facebook::axiom::connector {

namespace {
/// StatisticsBuilder using dwrf::StaticsBuilder
class StatisticsBuilderImpl : public StatisticsBuilder {
 public:
  StatisticsBuilderImpl(
      velox::TypePtr type,
      std::unique_ptr<velox::dwrf::StatisticsBuilder> builder)
      : type_(std::move(type)), builder_(std::move(builder)) {}

  const velox::TypePtr& type() const override {
    return type_;
  }

  void add(const velox::VectorPtr& data) override;

  void merge(const StatisticsBuilder& other) override;

  int64_t numAscending() const override {
    return numAsc_;
  }

  void build(ColumnStatistics& result, float sampleFraction = 1) override;

  int64_t numRepeat() const override {
    return numRepeat_;
  }
  int64_t numDescending() const override {
    return numDesc_;
  }

 private:
  template <typename Builder, typename T>
  void addStats(
      velox::dwrf::StatisticsBuilder* builder,
      const velox::BaseVector& vector);

  velox::TypePtr type_;
  std::unique_ptr<velox::dwrf::StatisticsBuilder> builder_;
  int64_t numAsc_{0};
  int64_t numRepeat_{0};
  int64_t numDesc_{0};
  int64_t numRows_{0};
};
} // namespace

std::unique_ptr<StatisticsBuilder> StatisticsBuilder::create(
    const velox::TypePtr& type,
    const StatisticsBuilderOptions& options) {
  velox::dwrf::StatisticsBuilderOptions dwrfOptions(
      options.maxStringLength,
      options.initialSize,
      options.countDistincts,
      options.allocator);
  switch (type->kind()) {
    case velox::TypeKind::BIGINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::SMALLINT:
      return std::make_unique<StatisticsBuilderImpl>(
          type,
          std::make_unique<velox::dwrf::IntegerStatisticsBuilder>(dwrfOptions));

    case velox::TypeKind::REAL:
    case velox::TypeKind::DOUBLE:
      return std::make_unique<StatisticsBuilderImpl>(
          type,
          std::make_unique<velox::dwrf::DoubleStatisticsBuilder>(dwrfOptions));

    case velox::TypeKind::VARCHAR:
      return std::make_unique<StatisticsBuilderImpl>(
          type,
          std::make_unique<velox::dwrf::StringStatisticsBuilder>(dwrfOptions));

    default:
      return nullptr;
  }
}

template <typename Builder, typename T>
void StatisticsBuilderImpl::addStats(
    velox::dwrf::StatisticsBuilder* builder,
    const velox::BaseVector& vector) {
  auto* typedVector = vector.asUnchecked<velox::SimpleVector<T>>();
  T previous{};
  bool hasPrevious = false;
  for (auto i = 0; i < typedVector->size(); ++i) {
    if (!typedVector->isNullAt(i)) {
      auto value = typedVector->valueAt(i);
      if (hasPrevious) {
        if (value == previous) {
          ++numRepeat_;
        } else if (value > previous) {
          ++numAsc_;
        } else {
          ++numDesc_;
        }
      } else {
        previous = value;
      }

      reinterpret_cast<Builder*>(builder)->addValues(value);
      previous = value;
      hasPrevious = true;
    }
    ++numRows_;
  }
}

void StatisticsBuilderImpl::add(const velox::VectorPtr& data) {
  auto loadData = [](const velox::VectorPtr& data) {
    return velox::BaseVector::loadedVectorShared(data);
  };

  switch (type_->kind()) {
    case velox::TypeKind::SMALLINT:
      addStats<velox::dwrf::IntegerStatisticsBuilder, short>(
          builder_.get(), *loadData(data));
      break;
    case velox::TypeKind::INTEGER:
      addStats<velox::dwrf::IntegerStatisticsBuilder, int32_t>(
          builder_.get(), *loadData(data));
      break;
    case velox::TypeKind::BIGINT:
      addStats<velox::dwrf::IntegerStatisticsBuilder, int64_t>(
          builder_.get(), *loadData(data));
      break;
    case velox::TypeKind::REAL:
      addStats<velox::dwrf::DoubleStatisticsBuilder, float>(
          builder_.get(), *loadData(data));
      break;
    case velox::TypeKind::DOUBLE:
      addStats<velox::dwrf::DoubleStatisticsBuilder, double>(
          builder_.get(), *loadData(data));
      break;
    case velox::TypeKind::VARCHAR:
      addStats<velox::dwrf::StringStatisticsBuilder, velox::StringView>(
          builder_.get(), *loadData(data));
      break;
    default:
      break;
  }
}

void StatisticsBuilder::updateBuilders(
    const velox::RowVectorPtr& row,
    std::vector<std::unique_ptr<StatisticsBuilder>>& builders) {
  for (auto column = 0; column < builders.size(); ++column) {
    if (!builders[column]) {
      continue;
    }
    auto* builder = builders[column].get();
    velox::VectorPtr data = row->childAt(column);
    builder->add(data);
  }
}

void StatisticsBuilderImpl::merge(const StatisticsBuilder& in) {
  auto* other = dynamic_cast<const StatisticsBuilderImpl*>(&in);
  builder_->merge(*other->builder_);
  numAsc_ += other->numAsc_;
  numRepeat_ += other->numRepeat_;
  numDesc_ += other->numDesc_;
  numRows_ += other->numRows_;
}

void StatisticsBuilderImpl::build(
    ColumnStatistics& result,
    float sampleFraction) {
  auto stats = builder_->build();
  auto optNumValues = stats->getNumberOfValues();
  auto numValues = optNumValues.has_value() ? optNumValues.value() : 0;
  if (auto ints = dynamic_cast<velox::dwio::common::IntegerColumnStatistics*>(
          stats.get())) {
    auto min = ints->getMinimum();
    auto max = ints->getMaximum();
    if (min.has_value() && max.has_value()) {
      result.min = velox::Variant(min.value());
      result.max = velox::Variant(max.value());
    }
  } else if (
      auto* dbl = dynamic_cast<velox::dwio::common::DoubleColumnStatistics*>(
          stats.get())) {
    auto min = dbl->getMinimum();
    auto max = dbl->getMaximum();
    if (min.has_value() && max.has_value()) {
      result.min = velox::Variant(min.value());
      result.max = velox::Variant(max.value());
    }
  } else if (
      auto* str = dynamic_cast<velox::dwio::common::StringColumnStatistics*>(
          stats.get())) {
    auto min = str->getMinimum();
    auto max = str->getMaximum();
    if (min.has_value() && max.has_value()) {
      result.min = velox::Variant(min.value());
      result.max = velox::Variant(max.value());
    }
    if (numValues) {
      result.avgLength = str->getTotalLength().value() / numValues;
    }
  }
  if (numRows_) {
    result.nullPct =
        100 * (numRows_ - numValues) / static_cast<float>(numRows_);
  }
  result.numDistinct = stats->numDistinct();
}

} // namespace facebook::axiom::connector
