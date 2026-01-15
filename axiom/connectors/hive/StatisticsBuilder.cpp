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

/// StatisticsBuilder for ARRAY type
class ArrayStatisticsBuilder : public StatisticsBuilder {
 public:
  explicit ArrayStatisticsBuilder(
      velox::TypePtr type,
      const StatisticsBuilderOptions& options)
      : type_(std::move(type)) {
    auto arrayType = std::dynamic_pointer_cast<const velox::ArrayType>(type_);
    VELOX_CHECK_NOT_NULL(arrayType, "Type must be ARRAY");
    // Disable HLL for scalar array elements - the cardinality of elements
    // across arrays is not useful for query optimization.
    auto elementOptions = options;
    if (arrayType->elementType()->isPrimitiveType()) {
      elementOptions.countDistincts = false;
    }
    elementBuilder_ =
        StatisticsBuilder::create(arrayType->elementType(), elementOptions);
  }

  const velox::TypePtr& type() const override {
    return type_;
  }

  void add(const velox::VectorPtr& data) override;

  void merge(const StatisticsBuilder& other) override;

  int64_t numAscending() const override {
    return 0;
  }

  void build(ColumnStatistics& result, float sampleFraction = 1) override;

  int64_t numRepeat() const override {
    return 0;
  }

  int64_t numDescending() const override {
    return 0;
  }

 private:
  velox::TypePtr type_;
  int64_t totalElements_{0};
  int64_t nullCount_{0};
  int64_t numRows_{0};
  std::unique_ptr<StatisticsBuilder> elementBuilder_;
};

/// StatisticsBuilder for ROW (struct) type
class StructStatisticsBuilder : public StatisticsBuilder {
 public:
  explicit StructStatisticsBuilder(
      velox::TypePtr type,
      const StatisticsBuilderOptions& options)
      : type_(std::move(type)) {
    auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type_);
    VELOX_CHECK_NOT_NULL(rowType, "Type must be ROW");
    for (size_t i = 0; i < rowType->size(); ++i) {
      childBuilders_.push_back(
          StatisticsBuilder::create(rowType->childAt(i), options));
    }
  }

  const velox::TypePtr& type() const override {
    return type_;
  }

  void add(const velox::VectorPtr& data) override;

  void merge(const StatisticsBuilder& other) override;

  int64_t numAscending() const override {
    return 0;
  }

  void build(ColumnStatistics& result, float sampleFraction = 1) override;

  int64_t numRepeat() const override {
    return 0;
  }

  int64_t numDescending() const override {
    return 0;
  }

 private:
  velox::TypePtr type_;
  int64_t nullCount_{0};
  int64_t numRows_{0};
  std::vector<std::unique_ptr<StatisticsBuilder>> childBuilders_;
};

/// StatisticsBuilder for MAP type
class MapStatisticsBuilder : public StatisticsBuilder {
 public:
  explicit MapStatisticsBuilder(
      velox::TypePtr type,
      const StatisticsBuilderOptions& options)
      : type_(std::move(type)) {
    auto mapType = std::dynamic_pointer_cast<const velox::MapType>(type_);
    VELOX_CHECK_NOT_NULL(mapType, "Type must be MAP");
    keyBuilder_ = StatisticsBuilder::create(mapType->keyType(), options);
    // Disable HLL for map values - the cardinality of values across all map
    // entries is not useful for query optimization.
    auto valueOptions = options;
    valueOptions.countDistincts = false;
    valueBuilder_ = StatisticsBuilder::create(mapType->valueType(), valueOptions);
  }

  const velox::TypePtr& type() const override {
    return type_;
  }

  void add(const velox::VectorPtr& data) override;

  void merge(const StatisticsBuilder& other) override;

  int64_t numAscending() const override {
    return 0;
  }

  void build(ColumnStatistics& result, float sampleFraction = 1) override;

  int64_t numRepeat() const override {
    return 0;
  }

  int64_t numDescending() const override {
    return 0;
  }

 private:
  velox::TypePtr type_;
  int64_t totalElements_{0};
  int64_t nullCount_{0};
  int64_t numRows_{0};
  std::unique_ptr<StatisticsBuilder> keyBuilder_;
  std::unique_ptr<StatisticsBuilder> valueBuilder_;
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

    case velox::TypeKind::ARRAY:
      return std::make_unique<ArrayStatisticsBuilder>(type, options);

    case velox::TypeKind::ROW:
      return std::make_unique<StructStatisticsBuilder>(type, options);

    case velox::TypeKind::MAP:
      return std::make_unique<MapStatisticsBuilder>(type, options);

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
      // IntegerStatisticsBuilder uses int64_t accumulator, but we need to
      // create a Variant with the correct TypeKind for the actual column type
      auto minValue = min.value();
      auto maxValue = max.value();
      switch (type_->kind()) {
        case velox::TypeKind::TINYINT:
          result.min = velox::Variant(static_cast<int8_t>(minValue));
          result.max = velox::Variant(static_cast<int8_t>(maxValue));
          break;
        case velox::TypeKind::SMALLINT:
          result.min = velox::Variant(static_cast<int16_t>(minValue));
          result.max = velox::Variant(static_cast<int16_t>(maxValue));
          break;
        case velox::TypeKind::INTEGER:
          result.min = velox::Variant(static_cast<int32_t>(minValue));
          result.max = velox::Variant(static_cast<int32_t>(maxValue));
          break;
        case velox::TypeKind::BIGINT:
          result.min = velox::Variant(minValue);
          result.max = velox::Variant(maxValue);
          break;
        case velox::TypeKind::HUGEINT:
          result.min = velox::Variant(static_cast<velox::int128_t>(minValue));
          result.max = velox::Variant(static_cast<velox::int128_t>(maxValue));
          break;
        default:
          // For other types, use int64_t as fallback
          result.min = velox::Variant(minValue);
          result.max = velox::Variant(maxValue);
          break;
      }
    }
  } else if (
      auto* dbl = dynamic_cast<velox::dwio::common::DoubleColumnStatistics*>(
          stats.get())) {
    auto min = dbl->getMinimum();
    auto max = dbl->getMaximum();
    if (min.has_value() && max.has_value()) {
      // DoubleStatisticsBuilder uses double accumulator, but REAL columns
      // need float Variants
      auto minValue = min.value();
      auto maxValue = max.value();
      if (type_->kind() == velox::TypeKind::REAL) {
        result.min = velox::Variant(static_cast<float>(minValue));
        result.max = velox::Variant(static_cast<float>(maxValue));
      } else {
        result.min = velox::Variant(minValue);
        result.max = velox::Variant(maxValue);
      }
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

// ArrayStatisticsBuilder implementation
void ArrayStatisticsBuilder::add(const velox::VectorPtr& data) {
  VELOX_CHECK(
      data->type()->equivalent(*type_),
      "Type mismatch: {} vs. {}",
      data->type()->toString(),
      type_->toString());

  // Get the size and wrapped vector
  auto size = data->size();
  auto* wrappedVec = data->wrappedVector();

  // Check that the wrapped vector is an ArrayVector
  VELOX_CHECK_EQ(
      wrappedVec->encoding(),
      velox::VectorEncoding::Simple::ARRAY,
      "Wrapped vector must be an ARRAY, got {}",
      velox::VectorEncoding::mapSimpleToName(wrappedVec->encoding()));

  auto* arrayVector = wrappedVec->asUnchecked<velox::ArrayVector>();

  // Loop through each row using wrappedIndex
  for (auto i = 0; i < size; ++i) {
    ++numRows_;

    auto wrappedIdx = data->wrappedIndex(i);

    if (arrayVector->isNullAt(wrappedIdx)) {
      ++nullCount_;
    } else {
      auto arraySize = arrayVector->sizeAt(wrappedIdx);
      totalElements_ += arraySize;
    }
  }

  // Update element builder with all the elements from all arrays
  if (elementBuilder_) {
    auto elements = arrayVector->elements();
    elementBuilder_->add(elements);
  }
}

void ArrayStatisticsBuilder::merge(const StatisticsBuilder& in) {
  auto* other = dynamic_cast<const ArrayStatisticsBuilder*>(&in);
  VELOX_CHECK_NOT_NULL(other, "Can only merge with ArrayStatisticsBuilder");
  VELOX_CHECK(
      type_->equivalent(*other->type_),
      "Type mismatch during merge: {} vs. {}",
      type_->toString(),
      other->type_->toString());

  totalElements_ += other->totalElements_;
  nullCount_ += other->nullCount_;
  numRows_ += other->numRows_;

  if (elementBuilder_ && other->elementBuilder_) {
    elementBuilder_->merge(*other->elementBuilder_);
  }
}

void ArrayStatisticsBuilder::build(
    ColumnStatistics& result,
    float sampleFraction) {
  if (numRows_ > 0) {
    result.nullPct = 100.0f * nullCount_ / numRows_;
  }
  result.numValues = numRows_ - nullCount_;

  if (result.numValues > 0) {
    result.avgLength = totalElements_ / result.numValues;
  }

  // Create child statistics for elements
  if (elementBuilder_) {
    result.children.resize(1);
    result.children[0].name = "element";
    elementBuilder_->build(result.children[0], sampleFraction);
  }
}

// StructStatisticsBuilder implementation
void StructStatisticsBuilder::add(const velox::VectorPtr& data) {
  VELOX_CHECK(
      data->type()->equivalent(*type_),
      "Type mismatch: {} vs. {}",
      data->type()->toString(),
      type_->toString());

  // Get the size and wrapped vector
  auto size = data->size();
  auto* wrappedVec = data->wrappedVector();

  // Check that the wrapped vector is a RowVector
  VELOX_CHECK_EQ(
      wrappedVec->encoding(),
      velox::VectorEncoding::Simple::ROW,
      "Wrapped vector must be a ROW, got {}",
      velox::VectorEncoding::mapSimpleToName(wrappedVec->encoding()));

  auto* rowVector = wrappedVec->asUnchecked<velox::RowVector>();

  // Loop through each row using wrappedIndex
  for (auto i = 0; i < size; ++i) {
    ++numRows_;

    auto wrappedIdx = data->wrappedIndex(i);

    if (rowVector->isNullAt(wrappedIdx)) {
      ++nullCount_;
    }
  }

  // Update child builders for each field
  for (size_t childIdx = 0; childIdx < childBuilders_.size(); ++childIdx) {
    if (childBuilders_[childIdx]) {
      childBuilders_[childIdx]->add(rowVector->childAt(childIdx));
    }
  }
}

void StructStatisticsBuilder::merge(const StatisticsBuilder& in) {
  auto* other = dynamic_cast<const StructStatisticsBuilder*>(&in);
  VELOX_CHECK_NOT_NULL(other, "Can only merge with StructStatisticsBuilder");
  VELOX_CHECK(
      type_->equivalent(*other->type_),
      "Type mismatch during merge: {} vs. {}",
      type_->toString(),
      other->type_->toString());

  nullCount_ += other->nullCount_;
  numRows_ += other->numRows_;

  VELOX_CHECK_EQ(
      childBuilders_.size(),
      other->childBuilders_.size(),
      "Child builder count mismatch");

  for (size_t i = 0; i < childBuilders_.size(); ++i) {
    if (childBuilders_[i] && other->childBuilders_[i]) {
      childBuilders_[i]->merge(*other->childBuilders_[i]);
    }
  }
}

void StructStatisticsBuilder::build(
    ColumnStatistics& result,
    float sampleFraction) {
  if (numRows_ > 0) {
    result.nullPct = 100.0f * nullCount_ / numRows_;
  }
  result.numValues = numRows_ - nullCount_;

  // Create child statistics for each field
  auto rowType = std::dynamic_pointer_cast<const velox::RowType>(type_);
  result.children.resize(childBuilders_.size());
  for (size_t i = 0; i < childBuilders_.size(); ++i) {
    result.children[i].name = rowType->nameOf(i);
    if (childBuilders_[i]) {
      childBuilders_[i]->build(result.children[i], sampleFraction);
    }
  }
}

// MapStatisticsBuilder implementation
void MapStatisticsBuilder::add(const velox::VectorPtr& data) {
  VELOX_CHECK(
      data->type()->equivalent(*type_),
      "Type mismatch: {} vs. {}",
      data->type()->toString(),
      type_->toString());

  // Get the size and wrapped vector
  auto size = data->size();
  auto* wrappedVec = data->wrappedVector();

  // Check that the wrapped vector is a MapVector
  VELOX_CHECK_EQ(
      wrappedVec->encoding(),
      velox::VectorEncoding::Simple::MAP,
      "Wrapped vector must be a MAP, got {}",
      velox::VectorEncoding::mapSimpleToName(wrappedVec->encoding()));

  auto* mapVector = wrappedVec->asUnchecked<velox::MapVector>();

  // Loop through each row using wrappedIndex
  for (auto i = 0; i < size; ++i) {
    ++numRows_;

    auto wrappedIdx = data->wrappedIndex(i);

    if (mapVector->isNullAt(wrappedIdx)) {
      ++nullCount_;
    } else {
      auto mapSize = mapVector->sizeAt(wrappedIdx);
      totalElements_ += mapSize;
    }
  }

  // Update key and value builders with all the keys and values from all maps
  if (keyBuilder_) {
    auto keys = mapVector->mapKeys();
    keyBuilder_->add(keys);
  }

  if (valueBuilder_) {
    auto values = mapVector->mapValues();
    valueBuilder_->add(values);
  }
}

void MapStatisticsBuilder::merge(const StatisticsBuilder& in) {
  auto* other = dynamic_cast<const MapStatisticsBuilder*>(&in);
  VELOX_CHECK_NOT_NULL(other, "Can only merge with MapStatisticsBuilder");
  VELOX_CHECK(
      type_->equivalent(*other->type_),
      "Type mismatch during merge: {} vs. {}",
      type_->toString(),
      other->type_->toString());

  totalElements_ += other->totalElements_;
  nullCount_ += other->nullCount_;
  numRows_ += other->numRows_;

  if (keyBuilder_ && other->keyBuilder_) {
    keyBuilder_->merge(*other->keyBuilder_);
  }
  if (valueBuilder_ && other->valueBuilder_) {
    valueBuilder_->merge(*other->valueBuilder_);
  }
}

void MapStatisticsBuilder::build(
    ColumnStatistics& result,
    float sampleFraction) {
  if (numRows_ > 0) {
    result.nullPct = 100.0f * nullCount_ / numRows_;
  }
  result.numValues = numRows_ - nullCount_;

  if (result.numValues > 0) {
    result.avgLength = totalElements_ / result.numValues;
  }

  // Create child statistics for keys and values
  result.children.resize(2);
  result.children[0].name = "keys";
  if (keyBuilder_) {
    keyBuilder_->build(result.children[0], sampleFraction);
  }

  result.children[1].name = "values";
  if (valueBuilder_) {
    valueBuilder_->build(result.children[1], sampleFraction);
  }
}

} // namespace facebook::axiom::connector
