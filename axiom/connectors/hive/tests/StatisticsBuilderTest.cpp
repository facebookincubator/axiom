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

#include "axiom/connectors/hive/StatisticsBuilder.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace facebook::axiom::connector {
namespace {

class StatisticsBuilderTest : public ::testing::Test,
                              public test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

 protected:
  std::unique_ptr<StatisticsBuilder> makeBuilder(
      const TypePtr& type,
      const StatisticsBuilderOptions& options = {}) {
    auto builder = StatisticsBuilder::create(type, options);
    EXPECT_NE(builder, nullptr);
    return builder;
  }

  template <typename T>
  std::unique_ptr<StatisticsBuilder> makeBuilder(
      const std::vector<T>& data,
      const StatisticsBuilderOptions& options = {}) {
    auto builder = makeBuilder(CppToType<T>::create(), options);
    if (!data.empty()) {
      builder->add(makeFlatVector<T>(data));
    }
    return builder;
  }

  template <typename T>
  void assertMinMax(
      const ColumnStatistics& stats,
      TypeKind expectedKind,
      const T& expectedMin,
      const T& expectedMax) {
    ASSERT_TRUE(stats.min.has_value());
    ASSERT_TRUE(stats.max.has_value());
    EXPECT_EQ(stats.min->kind(), expectedKind);
    EXPECT_EQ(stats.max->kind(), expectedKind);
    EXPECT_EQ(stats.min->value<T>(), expectedMin);
    EXPECT_EQ(stats.max->value<T>(), expectedMax);
  }

  void assertMinMax(
      const ColumnStatistics& stats,
      double expectedMin,
      double expectedMax) {
    ASSERT_TRUE(stats.min.has_value());
    ASSERT_TRUE(stats.max.has_value());
    EXPECT_EQ(stats.min->kind(), TypeKind::DOUBLE);
    EXPECT_EQ(stats.max->kind(), TypeKind::DOUBLE);
    EXPECT_DOUBLE_EQ(stats.min->value<double>(), expectedMin);
    EXPECT_DOUBLE_EQ(stats.max->value<double>(), expectedMax);
  }

  void assertMinMaxFloat(
      const ColumnStatistics& stats,
      float expectedMin,
      float expectedMax) {
    ASSERT_TRUE(stats.min.has_value());
    ASSERT_TRUE(stats.max.has_value());
    EXPECT_EQ(stats.min->kind(), TypeKind::REAL);
    EXPECT_EQ(stats.max->kind(), TypeKind::REAL);
    EXPECT_FLOAT_EQ(stats.min->value<float>(), expectedMin);
    EXPECT_FLOAT_EQ(stats.max->value<float>(), expectedMax);
  }

  void assertNoMinMax(const ColumnStatistics& stats) {
    EXPECT_FALSE(stats.min.has_value());
    EXPECT_FALSE(stats.max.has_value());
  }

  void assertMinMax(
      const ColumnStatistics& stats,
      const std::string& expectedMin,
      const std::string& expectedMax) {
    assertMinMax<std::string>(
        stats, TypeKind::VARCHAR, expectedMin, expectedMax);
  }

  void assertOrderingPct(
      const ColumnStatistics& stats,
      float expectedAscendingPct,
      float expectedDescendingPct) {
    ASSERT_TRUE(stats.ascendingPct.has_value());
    ASSERT_TRUE(stats.descendingPct.has_value());
    EXPECT_FLOAT_EQ(stats.ascendingPct.value(), expectedAscendingPct);
    EXPECT_FLOAT_EQ(stats.descendingPct.value(), expectedDescendingPct);
  }

  void assertNoOrderingPct(const ColumnStatistics& stats) {
    EXPECT_FALSE(stats.ascendingPct.has_value());
    EXPECT_FALSE(stats.descendingPct.has_value());
  }

  // Returns a pair of ColumnStatistics: {singleBatch, multiBatch}.
  // 'singleBatch' stats are from adding all data in a single batch.
  // 'multiBatch' stats are from adding data one vector at a time.
  template <typename T>
  std::pair<ColumnStatistics, ColumnStatistics> makeSingleAndMultiBatch(
      const std::vector<std::vector<T>>& data,
      const StatisticsBuilderOptions& options = {}) {
    // Single batch: flatten all data into one vector.
    std::vector<T> flattened;
    for (const auto& v : data) {
      flattened.insert(flattened.end(), v.begin(), v.end());
    }
    auto singleBatch = makeBuilder(CppToType<T>::create(), options);
    singleBatch->add(makeFlatVector<T>(flattened));

    // Multi batch: add each vector as a separate batch.
    auto multiBatch = makeBuilder(CppToType<T>::create(), options);
    for (const auto& v : data) {
      multiBatch->add(makeFlatVector<T>(v));
    }

    ColumnStatistics single;
    singleBatch->build(single);

    ColumnStatistics multi;
    multiBatch->build(multi);

    return {single, multi};
  }
};

TEST_F(StatisticsBuilderTest, create) {
  StatisticsBuilderOptions options;

  // Supported types.
  std::vector<TypePtr> supportedTypes = {
      SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE(), VARCHAR(), DATE()};
  for (const auto& type : supportedTypes) {
    SCOPED_TRACE(type->toString());
    auto builder = StatisticsBuilder::create(type, options);
    ASSERT_NE(builder, nullptr);
    EXPECT_TRUE(type->equivalent(*builder->type()));
  }

  // Unsupported types.
  std::vector<TypePtr> unsupportedTypes = {
      TINYINT(),
      BOOLEAN(),
      ARRAY(BIGINT()),
      MAP(VARCHAR(), BIGINT()),
      ROW({{"a", BIGINT()}})};
  for (const auto& type : unsupportedTypes) {
    SCOPED_TRACE(type->toString());
    auto builder = StatisticsBuilder::create(type, options);
    EXPECT_EQ(builder, nullptr);
  }
}

TEST_F(StatisticsBuilderTest, addIntegerAscending) {
  auto builder = makeBuilder(BIGINT());
  builder->add(makeFlatVector<int64_t>({1, 2, 3, 4, 5}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, TypeKind::BIGINT, 1L, 5L);
  EXPECT_EQ(stats.nullPct, 0.0f);
  assertOrderingPct(stats, 100.0f, 0.0f);
}

TEST_F(StatisticsBuilderTest, addIntegerDescending) {
  auto builder = makeBuilder(BIGINT());
  builder->add(makeFlatVector<int64_t>({5, 4, 3, 2, 1}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, TypeKind::BIGINT, 1L, 5L);
  assertOrderingPct(stats, 0.0f, 100.0f);
}

TEST_F(StatisticsBuilderTest, addIntegerRepeating) {
  auto builder = makeBuilder(BIGINT());
  builder->add(makeFlatVector<int64_t>({5, 5, 5, 5, 5}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, TypeKind::BIGINT, 5L, 5L);
  assertOrderingPct(stats, 0.0f, 0.0f);
}

TEST_F(StatisticsBuilderTest, addIntegerMixed) {
  auto builder = makeBuilder(BIGINT());
  // 4 transitions: 1->3 (asc), 3->2 (desc), 2->2 (repeat), 2->5 (asc)
  builder->add(makeFlatVector<int64_t>({1, 3, 2, 2, 5}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, TypeKind::BIGINT, 1L, 5L);
  // 2 asc / 4 total = 50%, 1 desc / 4 total = 25%
  assertOrderingPct(stats, 50.0f, 25.0f);
}

TEST_F(StatisticsBuilderTest, addIntegerWithNulls) {
  auto builder = makeBuilder(BIGINT());
  builder->add(
      makeNullableFlatVector<int64_t>({1, std::nullopt, 3, std::nullopt, 5}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, TypeKind::BIGINT, 1L, 5L);
  EXPECT_EQ(stats.nullPct, 40.0f);
  assertOrderingPct(stats, 100.0f, 0.0f);
}

// Test that SMALLINT columns produce Variant with correct TypeKind.
TEST_F(StatisticsBuilderTest, addSmallintVariantType) {
  auto builder = makeBuilder(SMALLINT());
  builder->add(makeFlatVector<int16_t>({10, 20, 30}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(
      stats,
      TypeKind::SMALLINT,
      static_cast<int16_t>(10),
      static_cast<int16_t>(30));
}

// Test that INTEGER columns produce Variant with correct TypeKind.
TEST_F(StatisticsBuilderTest, addIntegerVariantType) {
  auto builder = makeBuilder(INTEGER());
  builder->add(makeFlatVector<int32_t>({100, 200, 300}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, TypeKind::INTEGER, 100, 300);
}

// Test that DOUBLE columns produce Variant with correct TypeKind.
TEST_F(StatisticsBuilderTest, addDoubleVariantType) {
  auto builder = makeBuilder(DOUBLE());
  builder->add(makeFlatVector<double>({1.5, 2.5, 3.5, 4.5}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, 1.5, 4.5);
  assertOrderingPct(stats, 100.0f, 0.0f);
}

// Test that REAL columns produce Variant with correct TypeKind (float, not
// double).
TEST_F(StatisticsBuilderTest, addRealVariantType) {
  auto builder = makeBuilder(REAL());
  builder->add(makeFlatVector<float>({1.0f, 2.0f, 3.0f}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMaxFloat(stats, 1.0f, 3.0f);
}

TEST_F(StatisticsBuilderTest, addVarchar) {
  auto builder = makeBuilder<std::string>({"a", "bb", "ccc"});

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, "a", "ccc");
  EXPECT_EQ(stats.nullPct, 0.0f);
  ASSERT_TRUE(stats.avgLength.has_value());
  // Strings of length 1, 2, 3. Average = (1 + 2 + 3) / 3 = 2.
  EXPECT_EQ(stats.avgLength.value(), 2);
  // maxLength is not populated by StatisticsBuilder.
  EXPECT_FALSE(stats.maxLength.has_value());
  assertOrderingPct(stats, 100.0f, 0.0f);
}

TEST_F(StatisticsBuilderTest, addVarcharDescending) {
  auto builder = makeBuilder<std::string>({"cherry", "banana", "apple"});

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, "apple", "cherry");
  EXPECT_EQ(stats.nullPct, 0.0f);
  ASSERT_TRUE(stats.avgLength.has_value());
  // Strings of length 6, 6, 5. Average = (6 + 6 + 5) / 3 = 5 (truncated).
  EXPECT_EQ(stats.avgLength.value(), 5);
  assertOrderingPct(stats, 0.0f, 100.0f);
}

TEST_F(StatisticsBuilderTest, addVarcharWithEmptyStrings) {
  auto builder = makeBuilder<std::string>({"", "abcd"});

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, "", "abcd");
  EXPECT_EQ(stats.nullPct, 0.0f);
  ASSERT_TRUE(stats.avgLength.has_value());
  // Empty string has length 0. Average = (0 + 4) / 2 = 2.
  EXPECT_EQ(stats.avgLength.value(), 2);
  assertOrderingPct(stats, 100.0f, 0.0f);
}

TEST_F(StatisticsBuilderTest, addVarcharWithNulls) {
  auto builder = makeBuilder(VARCHAR());
  builder->add(
      makeNullableFlatVector<std::string>({"ab", std::nullopt, "abcd"}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, "ab", "abcd");
  EXPECT_FLOAT_EQ(stats.nullPct, 100.0f / 3);
  ASSERT_TRUE(stats.avgLength.has_value());
  // Nulls are excluded from avg calculation. Average = (2 + 4) / 2 = 3.
  EXPECT_EQ(stats.avgLength.value(), 3);
  assertOrderingPct(stats, 100.0f, 0.0f);
}

TEST_F(StatisticsBuilderTest, addMultipleBatches) {
  auto builder = makeBuilder(BIGINT());

  builder->add(makeFlatVector<int64_t>({1, 2, 3}));
  builder->add(makeFlatVector<int64_t>({4, 5, 6}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, TypeKind::BIGINT, 1L, 6L);
}

TEST_F(StatisticsBuilderTest, merge) {
  auto a = makeBuilder<int64_t>({1, 2, 3});
  auto b = makeBuilder<int64_t>({10, 20, 30});

  a->merge(*b);

  ColumnStatistics stats;
  a->build(stats);

  assertMinMax(stats, TypeKind::BIGINT, 1L, 30L);
  assertOrderingPct(stats, 100.0f, 0.0f);
}

TEST_F(StatisticsBuilderTest, mergeDouble) {
  auto a = makeBuilder<double>({1.0, 2.0});
  auto b = makeBuilder<double>({100.0, 200.0});

  a->merge(*b);

  ColumnStatistics stats;
  a->build(stats);

  assertMinMax(stats, 1.0, 200.0);
}

TEST_F(StatisticsBuilderTest, mergeVarchar) {
  auto a = makeBuilder<std::string>({"ab", "abcd"});
  auto b = makeBuilder<std::string>({"abcdef"});

  a->merge(*b);

  ColumnStatistics stats;
  a->build(stats);

  assertMinMax(stats, "ab", "abcdef");
  EXPECT_EQ(stats.nullPct, 0.0f);
  ASSERT_TRUE(stats.avgLength.has_value());
  // Builder a: length 2, 4. Builder b: length 6. Average = (2 + 4 + 6) / 3 = 4.
  EXPECT_EQ(stats.avgLength.value(), 4);
  assertOrderingPct(stats, 100.0f, 0.0f);
}

TEST_F(StatisticsBuilderTest, mergeVarcharWithLongStrings) {
  StatisticsBuilderOptions options{.maxStringLength = 5};

  // Merge builder with min/max into builder without min/max (due to long
  // strings).
  {
    auto a = makeBuilder<std::string>({"aa", "bb"}, options); // min/max present
    auto b =
        makeBuilder<std::string>({"long_string"}, options); // min/max dropped

    a->merge(*b);

    ColumnStatistics stats;
    a->build(stats);

    assertNoMinMax(stats);
  }

  // Merge builder without min/max into builder with min/max.
  {
    auto a =
        makeBuilder<std::string>({"long_string"}, options); // min/max dropped
    auto b = makeBuilder<std::string>({"aa", "bb"}, options); // min/max present

    a->merge(*b);

    ColumnStatistics stats;
    a->build(stats);

    assertNoMinMax(stats);
  }

  // Both builders have min/max dropped due to long strings.
  {
    auto a = makeBuilder<std::string>({"long_string_1"}, options);
    auto b = makeBuilder<std::string>({"long_string_2"}, options);

    a->merge(*b);

    ColumnStatistics stats;
    a->build(stats);

    assertNoMinMax(stats);
  }
}

TEST_F(StatisticsBuilderTest, mergeWithEmpty) {
  // Merge non-empty into empty.
  {
    auto a = makeBuilder(BIGINT());
    auto b = makeBuilder<int64_t>({10, 20, 30});

    a->merge(*b);

    ColumnStatistics stats;
    a->build(stats);

    assertMinMax(stats, TypeKind::BIGINT, 10L, 30L);
  }

  // Merge empty into non-empty.
  {
    auto a = makeBuilder<int64_t>({1, 2, 3});
    auto b = makeBuilder(BIGINT());

    a->merge(*b);

    ColumnStatistics stats;
    a->build(stats);

    assertMinMax(stats, TypeKind::BIGINT, 1L, 3L);
  }

  // Merge two empty builders.
  {
    auto a = makeBuilder(BIGINT());
    auto b = makeBuilder(BIGINT());

    a->merge(*b);

    ColumnStatistics stats;
    a->build(stats);

    assertNoMinMax(stats);
  }
}

TEST_F(StatisticsBuilderTest, updateBuilders) {
  std::vector<std::unique_ptr<StatisticsBuilder>> builders;
  builders.push_back(makeBuilder(BIGINT()));
  builders.push_back(makeBuilder(DOUBLE()));
  builders.push_back(makeBuilder(VARCHAR()));

  auto c0 = makeFlatVector<int64_t>({1, 2, 3});
  auto c1 = makeFlatVector<double>({1.5, 2.5, 3.5});
  auto c2 = makeFlatVector<std::string>({"a", "b", "c"});

  auto row = makeRowVector({c0, c1, c2});
  StatisticsBuilder::updateBuilders(row, builders);

  std::vector<ColumnStatistics> stats(builders.size());
  for (auto i = 0; i < builders.size(); ++i) {
    builders[i]->build(stats[i]);
  }

  assertMinMax(stats[0], TypeKind::BIGINT, 1L, 3L);
  assertMinMax(stats[1], 1.5, 3.5);
  assertMinMax(stats[2], "a", "c");
}

TEST_F(StatisticsBuilderTest, updateBuildersWithNullBuilder) {
  std::vector<std::unique_ptr<StatisticsBuilder>> builders;
  builders.push_back(makeBuilder(BIGINT()));
  builders.push_back(nullptr);
  builders.push_back(makeBuilder(VARCHAR()));

  auto c0 = makeFlatVector<int64_t>({1, 2, 3});
  auto c1 = makeFlatVector<double>({1.5, 2.5, 3.5});
  auto c2 = makeFlatVector<std::string>({"a", "b", "c"});

  auto row = makeRowVector({c0, c1, c2});

  EXPECT_NO_THROW(StatisticsBuilder::updateBuilders(row, builders));

  std::vector<ColumnStatistics> stats(builders.size());
  builders[0]->build(stats[0]);
  builders[2]->build(stats[2]);

  assertMinMax(stats[0], TypeKind::BIGINT, 1L, 3L);
  assertMinMax(stats[2], "a", "c");
}

TEST_F(StatisticsBuilderTest, countDistincts) {
  HashStringAllocator allocator(pool());
  StatisticsBuilderOptions options{
      .countDistincts = true, .allocator = &allocator};
  auto builder = makeBuilder(BIGINT(), options);
  builder->add(makeFlatVector<int64_t>({1, 2, 2, 3, 3, 3, 4, 4, 4, 4}));

  ColumnStatistics stats;
  builder->build(stats);

  ASSERT_TRUE(stats.numDistinct.has_value());
  EXPECT_EQ(stats.numDistinct.value(), 4);
}

TEST_F(StatisticsBuilderTest, maxStringLength) {
  StatisticsBuilderOptions options{.maxStringLength = 5};

  auto buildStats = [&](const std::vector<std::string>& data) {
    auto builder = makeBuilder(VARCHAR(), options);
    builder->add(makeFlatVector<std::string>(data));

    ColumnStatistics stats;
    builder->build(stats);
    return stats;
  };

  // Only short strings - min/max are recorded.
  assertMinMax(buildStats({"abc", "def", "xyz"}), "abc", "xyz");

  // Only long strings - min/max are not recorded.
  assertNoMinMax(
      buildStats({"this_is_long", "another_long", "also_very_long"}));

  // Mix: min and max are both short - min/max are recorded.
  assertMinMax(buildStats({"aaa", "middle_long_string", "zzz"}), "aaa", "zzz");

  // Mix: min and max are both long - min/max are not recorded.
  assertNoMinMax(buildStats({"aaa_long", "mmm", "zzz_long"}));

  // Mix: min is long, max is short - min/max are not recorded.
  assertNoMinMax(buildStats({"aaa_long", "mmm", "zzz"}));

  // Mix: min is short, max is long - min/max are not recorded.
  assertNoMinMax(buildStats({"aaa", "mmm", "zzz_long"}));
}

TEST_F(StatisticsBuilderTest, emptyVector) {
  auto builder = makeBuilder(BIGINT());
  builder->add(makeFlatVector<int64_t>({}));

  ColumnStatistics stats;
  builder->build(stats);

  assertNoMinMax(stats);
  assertNoOrderingPct(stats);
}

TEST_F(StatisticsBuilderTest, singleElement) {
  auto builder = makeBuilder(BIGINT());
  builder->add(makeFlatVector<int64_t>({42}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, TypeKind::BIGINT, 42L, 42L);
  // Single element has no transitions.
  assertNoOrderingPct(stats);
}

TEST_F(StatisticsBuilderTest, allNulls) {
  auto builder = makeBuilder(BIGINT());
  builder->add(
      makeNullableFlatVector<int64_t>(
          {std::nullopt, std::nullopt, std::nullopt}));

  ColumnStatistics stats;
  builder->build(stats);

  assertNoMinMax(stats);
  EXPECT_EQ(stats.nullPct, 100.0f);
  // No non-null values means no transitions.
  assertNoOrderingPct(stats);
}

TEST_F(StatisticsBuilderTest, negativeIntegers) {
  auto builder = makeBuilder(BIGINT());
  builder->add(makeFlatVector<int64_t>({-100, -50, 0, 50, 100}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, TypeKind::BIGINT, -100L, 100L);
  assertOrderingPct(stats, 100.0f, 0.0f);
}

TEST_F(StatisticsBuilderTest, negativeDoubles) {
  auto builder = makeBuilder(DOUBLE());
  builder->add(makeFlatVector<double>({-1.5, -0.5, 0.0, 0.5, 1.5}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, -1.5, 1.5);
}

TEST_F(StatisticsBuilderTest, largeValues) {
  auto builder = makeBuilder(BIGINT());
  const int64_t minVal = std::numeric_limits<int64_t>::min();
  const int64_t maxVal = std::numeric_limits<int64_t>::max();
  builder->add(makeFlatVector<int64_t>({minVal, 0, maxVal}));

  ColumnStatistics stats;
  builder->build(stats);

  assertMinMax(stats, TypeKind::BIGINT, minVal, maxVal);
}

// Verifies that adding data in a single batch vs multiple batches produces the
// same min/max/nullPct.
TEST_F(StatisticsBuilderTest, singleVsMultiBatchEquivalence) {
  auto [single, multi] =
      makeSingleAndMultiBatch<int64_t>({{1, 5, 3}, {10, 2, 8}});

  EXPECT_EQ(single.min, multi.min);
  EXPECT_EQ(single.max, multi.max);
  EXPECT_EQ(single.nullPct, multi.nullPct);
}

// Ordering statistics differ between single batch and multiple batches because
// the implementation doesn't track cross-batch boundaries.
TEST_F(StatisticsBuilderTest, singleVsMultiBatchOrderingStats) {
  // Data: {1, 2, 3} followed by {4, 5, 6}
  // Single batch: 1->2->3->4->5->6 = 5 ascending transitions
  // Multi batch: (1->2->3) + (4->5->6) = 2 + 2 = 4 ascending (boundary not
  // counted)
  auto [single, multi] =
      makeSingleAndMultiBatch<int64_t>({{1, 2, 3}, {4, 5, 6}});

  // Both are 100% ascending, but the count of transitions differs.
  assertOrderingPct(single, 100.0f, 0.0f);
  assertOrderingPct(multi, 100.0f, 0.0f);

  // Min/max should still match.
  EXPECT_EQ(single.min, multi.min);
  EXPECT_EQ(single.max, multi.max);
}

// With maxStringLength, single batch vs multiple batches can produce different
// results. Single batch checks global min/max against the limit, while multiple
// batches check each batch's min/max independently.
TEST_F(StatisticsBuilderTest, singleVsMultiBatchStringLength) {
  StatisticsBuilderOptions options{.maxStringLength = 5};

  // Single batch: ["aa", "long_string", "zz"]
  // Global min="aa" (short), max="zz" (short) -> stats KEPT
  auto [single, multi] = makeSingleAndMultiBatch<std::string>(
      {{"aa", "long_string"}, {"zz"}}, options);

  // Single batch keeps min/max because global min="aa" and max="zz" are short.
  assertMinMax(single, "aa", "zz");

  // Multi batch: the behavior depends on how the implementation handles
  // per-batch min/max checking. Document the actual behavior.
  EXPECT_EQ(single.min.has_value(), multi.min.has_value());
  EXPECT_EQ(single.max.has_value(), multi.max.has_value());
}

} // namespace
} // namespace facebook::axiom::connector
