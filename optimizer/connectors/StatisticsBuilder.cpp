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


namespace facebook::velox::optimizer {

  /// StatisticsBuilder using dwrf::StatisticsBuilder
  class StatisticsBuilderImpl : public StatisticsBuilder {

    void add(const VectorPtr& data) override;

        void merge(const StatisticsBuilder& other) override;


  private:
    std::unique_ptr<dwrf::StatisticsBuilder> builder_;
  };

  
  std::unique_pointer<StatisticsBuilder> StatisticsBuilder::create(


  

template <typename Builder, typename T>
void addStats(
    velox::dwrf::StatisticsBuilder* builder,
    const BaseVector& vector) {

    auto* typedVector = vector.asUnchecked<SimpleVector<T>>();
  for (auto i = 0; i < typedVector->size(); ++i) {
    if (!typedVector->isNullAt(i)) {
      reinterpret_cast<Builder*>(builder)->addValues(typedVector->valueAt(i));
    }
  }
}
StatisticsBuilder::add(const VectorPtr& data) {
        auto loadChild = [](RowVectorPtr data, int32_t column) {
          data->childAt(column) =
              BaseVector::loadedVectorShared(data->childAt(column));
        };
        switch (rowType()->childAt(column)->kind()) {
          case TypeKind::SMALLINT:
            loadChild(data, column);
            addStats<dwrf::IntegerStatisticsBuilder, short>(
                builder, *data->childAt(column));
            break;
          case TypeKind::INTEGER:
            loadChild(data, column);
            addStats<dwrf::IntegerStatisticsBuilder, int32_t>(
                builder, *data->childAt(column));
            break;
          case TypeKind::BIGINT:
            loadChild(data, column);
            addStats<dwrf::IntegerStatisticsBuilder, int64_t>(
                builder, *data->childAt(column));
            break;
          case TypeKind::REAL:
            loadChild(data, column);
            addStats<dwrf::DoubleStatisticsBuilder, float>(
                builder, *data->childAt(column));
            break;
          case TypeKind::DOUBLE:
            loadChild(data, column);
            addStats<dwrf::DoubleStatisticsBuilder, double>(
                builder, *data->childAt(column));
            break;
          case TypeKind::VARCHAR:
            loadChild(data, column);
            addStats<dwrf::StringStatisticsBuilder, StringView>(
                builder, *data->childAt(column));
            break;

          default:
            break;
        }
 }

void StatisticsBuilder::merge(const StatisticsBuilder& other) {
}

  void updateStatsBuilders(const RowVectorPtr& data, std::vector<std::unique_ptr<StatisticsBuilder>>& builders) {
    for (auto column = 0; column < builders.size(); ++column) {
        if (!builders[column]) {
          continue;
        }
        auto* builder = builders[column].get();
	builder->add(data);
      }
  }



  
}
