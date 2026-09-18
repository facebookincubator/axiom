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

#include "axiom/optimizer/v2/EstimateProvider.h"
#include <gmock/gmock.h>
#include "axiom/optimizer/v2/tests/UnitTestBase.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

using ::testing::Contains;
using ::testing::Key;
using ::testing::Not;
using ::testing::SizeIs;

class EstimateProviderTest : public UnitTestBase {};

TEST_F(EstimateProviderTest, projectPreservesStatsAcrossColumnAliases) {
  const auto* type = optimizer::queryCtx()->toType(velox::BIGINT());
  ColumnCP inputColumn = optimizer::Column::create("input", Value(type, 25));

  std::vector<velox::Variant> rows;
  rows.reserve(100);
  for (int64_t value = 0; value < 100; ++value) {
    rows.push_back(velox::Variant::row({velox::Variant(value)}));
  }
  NodeCP values = builder_->makeValues(
      /*source=*/nullptr,
      registerVariant(velox::Variant::array(std::move(rows))),
      ColumnVector{inputColumn});

  ColumnCP groupedColumn = optimizer::Column::create("grouped", Value(type));
  NodeCP aggregate = builder_->make<Aggregate>(Aggregate::Key{
      .input = values,
      .groupingKeys = ExprVector{inputColumn},
      .aggregates = {},
      .outputColumns = ColumnVector{groupedColumn},
  });

  ColumnCP aliasColumn = optimizer::Column::create("alias", Value(type));
  ColumnCP computedColumn = optimizer::Column::create("computed", Value(type));
  NodeCP project = builder_->make<Project>(Project::Key{
      .input = aggregate,
      .exprs =
          ExprVector{
              groupedColumn,
              builder_->makeLiteral(velox::Variant(int64_t{1}), type),
          },
      .outputColumns = ColumnVector{aliasColumn, computedColumn},
  });

  EstimateProvider estimates;
  const auto& estimate = estimates.estimate(project);

  EXPECT_EQ(estimate.cardinality, 25);
  EXPECT_THAT(estimate.constraints, SizeIs(1));
  EXPECT_THAT(estimate.constraints, Contains(Key(aliasColumn->id())));
  EXPECT_THAT(estimate.constraints, Not(Contains(Key(groupedColumn->id()))));
  EXPECT_THAT(estimate.constraints, Not(Contains(Key(computedColumn->id()))));
  EXPECT_EQ(estimate.constraints.at(aliasColumn->id()).cardinality, 25);
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
