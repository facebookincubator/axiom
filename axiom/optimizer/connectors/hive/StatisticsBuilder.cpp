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
#include "axiom/optimizer/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/connectors/hive/StatisticsBuilder.h"

namespace facebook::velox::connector {

namespace {
/// StatisticsBuilder using dwrf::StaticsBuilder
class StatisticsBuilderImpl : public StatisticsBuilder {
 public:
  StatisticsBuilderImpl(
      TypePtr type,
      std::unique_ptr<dwrf::StatisticsBuilder> builder)
      : type_(std::move(type)), builder_(std::move(builder)) {}

  const TypePtr& type() const override {
    return type_;
  }

  void add(const VectorPtr& data) override;

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
      const BaseVector& vector);

  TypePtr type_;
  std::unique_ptr<dwrf::StatisticsBuilder> builder_;
  int64_t numAsc_{0};
  int64_t numRepeat_{0};
  int64_t numDesc_{0};
  int64_t numRows_{0};
};
} // namespace

std::unique_ptr<StatisticsBuilder> StatisticsBuilder::create(
    const TypePtr& type,
    const StatisticsBuilderOptions& options) {
  dwrf::StatisticsBuilderOptions dwrfOptions(
      options.maxStringLength,
      options.initialSize,
      options.countDistincts,
      options.allocator);
  switch (type->kind()) {
    case TypeKind::BIGINT:
    case TypeKind::INTEGER:
    case TypeKind::SMALLINT:
      return std::make_unique<StatisticsBuilderImpl>(
          type, std::make_unique<dwrf::IntegerStatisticsBuilder>(dwrfOptions));

    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return std::make_unique<StatisticsBuilderImpl>(
          type, std::make_unique<dwrf::DoubleStatisticsBuilder>(dwrfOptions));

    case TypeKind::VARCHAR:
      return std::make_unique<StatisticsBuilderImpl>(
          type, std::make_unique<dwrf::StringStatisticsBuilder>(dwrfOptions));

    default:
      return nullptr;
  }
}

template <typename Builder, typename T>
void StatisticsBuilderImpl::addStats(
    velox::dwrf::StatisticsBuilder* builder,
    const BaseVector& vector) {
  auto* typedVector = vector.asUnchecked<SimpleVector<T>>();
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

void StatisticsBuilderImpl::add(const VectorPtr& data) {
  auto loadData = [](const VectorPtr& data) {
    return BaseVector::loadedVectorShared(data);
  };

  switch (type_->kind()) {
    case TypeKind::SMALLINT:
      addStats<dwrf::IntegerStatisticsBuilder, short>(
          builder_.get(), *loadData(data));
      break;
    case TypeKind::INTEGER:
      addStats<dwrf::IntegerStatisticsBuilder, int32_t>(
          builder_.get(), *loadData(data));
      break;
    case TypeKind::BIGINT:
      addStats<dwrf::IntegerStatisticsBuilder, int64_t>(
          builder_.get(), *loadData(data));
      break;
    case TypeKind::REAL:
      addStats<dwrf::DoubleStatisticsBuilder, float>(
          builder_.get(), *loadData(data));
      break;
    case TypeKind::DOUBLE:
      addStats<dwrf::DoubleStatisticsBuilder, double>(
          builder_.get(), *loadData(data));
      break;
    case TypeKind::VARCHAR:
      addStats<dwrf::StringStatisticsBuilder, StringView>(
          builder_.get(), *loadData(data));
      break;
    default:
      break;
  }
}

void StatisticsBuilder::updateBuilders(
    const RowVectorPtr& row,
    std::vector<std::unique_ptr<StatisticsBuilder>>& builders) {
  for (auto column = 0; column < builders.size(); ++column) {
    if (!builders[column]) {
      continue;
    }
    auto* builder = builders[column].get();
    VectorPtr data = row->childAt(column);
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
  if (auto ints =
          dynamic_cast<dwio::common::IntegerColumnStatistics*>(stats.get())) {
    auto min = ints->getMinimum();
    auto max = ints->getMaximum();
    if (min.has_value() && max.has_value()) {
      result.min = variant(min.value());
      result.max = variant(max.value());
    }
  } else if (
      auto* dbl =
          dynamic_cast<dwio::common::DoubleColumnStatistics*>(stats.get())) {
    auto min = dbl->getMinimum();
    auto max = dbl->getMaximum();
    if (min.has_value() && max.has_value()) {
      result.min = variant(min.value());
      result.max = variant(max.value());
    }
  } else if (
      auto* str =
          dynamic_cast<dwio::common::StringColumnStatistics*>(stats.get())) {
    auto min = str->getMinimum();
    auto max = str->getMaximum();
    if (min.has_value() && max.has_value()) {
      result.min = variant(min.value());
      result.max = variant(max.value());
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

} // namespace facebook::velox::connector
