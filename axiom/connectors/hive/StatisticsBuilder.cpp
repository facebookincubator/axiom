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
#include "velox/type/Type.h"

namespace facebook::axiom::connector {

namespace {

template <typename T, typename U>
velox::Variant toVariant(U value) {
  return velox::Variant(static_cast<T>(value));
}

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

  void build(ColumnStatistics& result) const override;

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
      /*initialSize=*/std::nullopt,
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
  VELOX_CHECK(
      vector.type()->equivalent(*type_),
      "Type mismatch: {} vs. {}",
      vector.type()->toString(),
      type_->toString());
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

      // TODO: Remove explicit std::string_view cast.
      if constexpr (std::is_same_v<T, velox::StringView>) {
        reinterpret_cast<Builder*>(builder)->addValues(std::string_view(value));
      } else {
        reinterpret_cast<Builder*>(builder)->addValues(value);
      }
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
  VELOX_CHECK_LE(builders.size(), row->childrenSize());
  for (auto i = 0; i < builders.size(); ++i) {
    if (builders[i] != nullptr) {
      builders[i]->add(row->childAt(i));
    }
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

void StatisticsBuilderImpl::build(ColumnStatistics& result) const {
  auto stats = builder_->build();
  auto numValues = stats->getNumberOfValues().value_or(0);

  if (auto ints = dynamic_cast<velox::dwio::common::IntegerColumnStatistics*>(
          stats.get())) {
    if (auto min = ints->getMinimum(), max = ints->getMaximum(); min && max) {
      // IntegerStatisticsBuilder uses int64_t accumulator, but we need to
      // create a Variant with the correct TypeKind for the actual column type.
      switch (type_->kind()) {
        case velox::TypeKind::TINYINT:
          result.min = toVariant<int8_t>(*min);
          result.max = toVariant<int8_t>(*max);
          break;
        case velox::TypeKind::SMALLINT:
          result.min = toVariant<int16_t>(*min);
          result.max = toVariant<int16_t>(*max);
          break;
        case velox::TypeKind::INTEGER:
          result.min = toVariant<int32_t>(*min);
          result.max = toVariant<int32_t>(*max);
          break;
        case velox::TypeKind::BIGINT:
          result.min = velox::Variant(*min);
          result.max = velox::Variant(*max);
          break;
        case velox::TypeKind::HUGEINT:
          result.min = toVariant<velox::int128_t>(*min);
          result.max = toVariant<velox::int128_t>(*max);
          break;
        default:
          // For other types, use int64_t as fallback.
          result.min = velox::Variant(*min);
          result.max = velox::Variant(*max);
          break;
      }
    }
  } else if (
      auto* dbl = dynamic_cast<velox::dwio::common::DoubleColumnStatistics*>(
          stats.get())) {
    if (auto min = dbl->getMinimum(), max = dbl->getMaximum(); min && max) {
      // DoubleStatisticsBuilder uses double accumulator, but REAL columns
      // need float Variants.
      if (type_->kind() == velox::TypeKind::REAL) {
        result.min = toVariant<float>(*min);
        result.max = toVariant<float>(*max);
      } else {
        result.min = velox::Variant(*min);
        result.max = velox::Variant(*max);
      }
    }
  } else if (
      auto* str = dynamic_cast<velox::dwio::common::StringColumnStatistics*>(
          stats.get())) {
    if (auto min = str->getMinimum(), max = str->getMaximum(); min && max) {
      result.min = velox::Variant(*min);
      result.max = velox::Variant(*max);
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

  // Compute ascending/descending percentages from ordering statistics.
  // Note: totalTransitions counts transitions between consecutive non-null
  // values, while numRows_ counts all rows including nulls.
  const auto totalTransitions = numAsc_ + numDesc_ + numRepeat_;
  if (totalTransitions > 0) {
    result.ascendingPct = 100.0f * numAsc_ / totalTransitions;
    result.descendingPct = 100.0f * numDesc_ / totalTransitions;
  }
}

} // namespace facebook::axiom::connector
