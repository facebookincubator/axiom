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
#include "axiom/logical_plan/PlanBuilder.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/sql/presto/tests/LogicalPlanMatcher.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;

namespace facebook::axiom::logical_plan {
namespace {

class PlanBuilderTest : public testing::Test {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

 protected:
  PlanBuilder makeEmptyValues(
      PlanBuilder::Context& context,
      const std::vector<TypePtr>& types) {
    std::vector<std::string> names;
    names.reserve(types.size());
    for (size_t i = 0; i < types.size(); ++i) {
      names.push_back(fmt::format("c{}", i));
    }
    return PlanBuilder(context).values(
        ROW(std::move(names), types), ValuesNode::Variants{});
  }
};

TEST_F(PlanBuilderTest, outputNames) {
  auto builder = PlanBuilder()
                     .values(
                         ROW({"a"}, {BIGINT()}),
                         std::vector<Variant>{Variant::row({123LL})})
                     .project({"a + 1", "a + 2 as b"});

  EXPECT_EQ(2, builder.numOutput());
  EXPECT_EQ("expr", builder.findOrAssignOutputNameAt(0));
  EXPECT_EQ("b", builder.findOrAssignOutputNameAt(1));

  builder.with({"b * 2"});

  EXPECT_EQ(3, builder.numOutput());

  const auto outputNames = builder.findOrAssignOutputNames();
  EXPECT_EQ(3, outputNames.size());
  EXPECT_EQ("expr", outputNames[0]);
  EXPECT_EQ("b", outputNames[1]);
  EXPECT_EQ("expr_0", outputNames[2]);
}

TEST_F(PlanBuilderTest, duplicateAliasAllowed) {
  PlanBuilder::Context context;

  auto makeBuilder = [&context]() {
    return PlanBuilder(
        context,
        /*enableCoercions=*/false,
        /*allowDuplicateAliases=*/true);
  };

  // Project: duplicate aliases get unique physical names.
  {
    auto builder =
        makeBuilder()
            .values(ROW({"a", "b"}, BIGINT()), ValuesNode::Variants{})
            .with({"a as x", "b as x"});

    auto names = builder.findOrAssignOutputNames();
    EXPECT_EQ(4, names.size());
    EXPECT_EQ("x", names[2]);
    EXPECT_TRUE(names[3].starts_with("x_"));
  }

  // Lookup on duplicate name fails.
  {
    auto builder =
        makeBuilder()
            .values(ROW({"a", "b"}, BIGINT()), ValuesNode::Variants{})
            .with({"a as x", "b as x"});

    VELOX_ASSERT_THROW(builder.project({"x"}), "Cannot resolve");
  }

  // Unnest: duplicate names allowed.
  {
    auto builder = makeBuilder()
                       .values(
                           ROW({"a", "arr"}, {BIGINT(), ARRAY(BIGINT())}),
                           ValuesNode::Variants{})
                       .unnest({Col("arr").unnestAs("a")});

    auto names = builder.findOrAssignOutputNames();
    EXPECT_EQ(3, names.size());
    EXPECT_TRUE(names[0].starts_with("a"));
    EXPECT_TRUE(names[2].starts_with("a"));
  }

  // Aggregate: duplicate aliases get unique physical names.
  {
    auto builder =
        makeBuilder()
            .values(ROW({"a", "b"}, BIGINT()), ValuesNode::Variants{})
            .aggregate({}, {"sum(a) as x", "sum(b) as x"});

    auto names = builder.findOrAssignOutputNames();
    EXPECT_EQ(2, names.size());
    EXPECT_TRUE(names[0].starts_with("x"));
    EXPECT_TRUE(names[1].starts_with("x"));
    EXPECT_NE(names[0], names[1]);
  }
}

TEST_F(PlanBuilderTest, duplicateAliasThrows) {
  // Duplicate aliases throw by default in project/aggregate operations.
  VELOX_ASSERT_THROW(
      PlanBuilder()
          .values(ROW({"a", "b"}, BIGINT()), ValuesNode::Variants{})
          .with({"a as x", "b as x"}),
      "Duplicate name: x");
}

TEST_F(PlanBuilderTest, setOperationTypeCoercion) {
  auto startMatcher = [] { return test::LogicalPlanMatcherBuilder().values(); };

  // (INTEGER, REAL) + (BIGINT, DOUBLE) -> (BIGINT, DOUBLE)
  {
    PlanBuilder::Context context;
    auto plan = PlanBuilder(context, /*enableCoercions=*/true)
                    .setOperation(
                        SetOperation::kUnionAll,
                        {
                            makeEmptyValues(context, {INTEGER(), REAL()}),
                            makeEmptyValues(context, {BIGINT(), DOUBLE()}),
                        })
                    .build();

    EXPECT_EQ(*plan->outputType(), *ROW({"c0", "c1"}, {BIGINT(), DOUBLE()}));

    auto matcher =
        startMatcher()
            .project()
            .setOperation(SetOperation::kUnionAll, startMatcher().build())
            .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString();
  }

  // Same types stay the same. No project nodes needed.
  {
    PlanBuilder::Context context;
    auto plan = PlanBuilder(context, /*enableCoercions=*/true)
                    .setOperation(
                        SetOperation::kUnionAll,
                        {
                            makeEmptyValues(context, {BIGINT()}),
                            makeEmptyValues(context, {BIGINT()}),
                        })
                    .build();

    EXPECT_EQ(*plan->outputType(), *ROW({"c0"}, {BIGINT()}));

    auto matcher =
        startMatcher()
            .setOperation(SetOperation::kUnionAll, startMatcher().build())
            .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString();
  }

  // Incompatible types fail.
  {
    PlanBuilder::Context context;
    VELOX_ASSERT_THROW(
        PlanBuilder(context, /*enableCoercions=*/true)
            .setOperation(
                SetOperation::kUnionAll,
                {
                    makeEmptyValues(context, {VARCHAR()}),
                    makeEmptyValues(context, {INTEGER()}),
                })
            .build(),
        "Output schemas of all inputs to a Set operation must match");
  }

  // Mismatched types fail when coercions are disabled.
  {
    PlanBuilder::Context context;
    VELOX_ASSERT_THROW(
        PlanBuilder(context, /*enableCoercions=*/false)
            .setOperation(
                SetOperation::kUnionAll,
                {
                    makeEmptyValues(context, {INTEGER()}),
                    makeEmptyValues(context, {BIGINT()}),
                })
            .build(),
        "Output schemas of all inputs to a Set operation must match");
  }
}

TEST_F(PlanBuilderTest, groupingSetsEmptyAggregatesAndKeys) {
  auto rowType = ROW({"a", "b"}, INTEGER());
  std::vector<Variant> data{
      Variant::row({1, 10}),
  };

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .values(rowType, data)
          .aggregate({{}}, {}, "$grouping_set_id")
          .build(),
      "Aggregation node must specify at least one aggregate or grouping key");
}

TEST_F(PlanBuilderTest, groupingSetsOutOfBoundIndices) {
  auto rowType = ROW({"a", "b", "c"}, INTEGER());
  std::vector<Variant> data{
      Variant::row({1, 10, 100}),
  };

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .values(rowType, data)
          .aggregate(
              {"a", "b"},
              {{0, 1}, {0, 2}},
              {"sum(c) as total"},
              {},
              "$grouping_set_id")
          .build(),
      "Grouping set index 2 is out of bounds");
}

TEST_F(PlanBuilderTest, groupingSetsDuplicateKeys) {
  auto rowType = ROW({"a", "b", "c"}, INTEGER());
  std::vector<Variant> data{
      Variant::row({1, 10, 100}),
  };

  VELOX_ASSERT_THROW(
      PlanBuilder()
          .values(rowType, data)
          .aggregate(
              {"a", "a"}, {{0, 1}}, {"sum(c) as total"}, {}, "$grouping_set_id")
          .build(),
      "Duplicate grouping key");
}

TEST_F(PlanBuilderTest, groupingSetsWithIndices) {
  auto rowType = ROW({"a", "b", "c", "d"}, INTEGER());
  std::vector<Variant> data{
      Variant::row({1, 10, 100, 1000}),
  };

  auto plan = PlanBuilder()
                  .values(rowType, data)
                  .aggregate(
                      {"a", "b", "c"},
                      {{0, 1}, {0, 1, 2}},
                      {"sum(d) as total"},
                      {},
                      "$grouping_set_id")
                  .build();

  EXPECT_THAT(
      plan->outputType()->names(),
      testing::ElementsAre("a", "b", "c", "total", "$grouping_set_id"));

  auto aggNode = std::dynamic_pointer_cast<const AggregateNode>(plan);
  ASSERT_NE(aggNode, nullptr);
  EXPECT_THAT(
      aggNode->groupingSets(),
      testing::ElementsAre(
          std::vector<int32_t>{0, 1}, std::vector<int32_t>{0, 1, 2}));
}

TEST_F(PlanBuilderTest, rollup) {
  auto rowType = ROW({"a", "b", "c"}, INTEGER());
  std::vector<Variant> data{
      Variant::row({1, 10, 100}),
  };

  auto plan = PlanBuilder()
                  .values(rowType, data)
                  .rollup({"a", "b"}, {"sum(c) as total"}, "$grouping_set_id")
                  .build();

  // Output should have: a, b (grouping keys), total, $grouping_set_id
  EXPECT_THAT(
      plan->outputType()->names(),
      testing::ElementsAre("a", "b", "total", "$grouping_set_id"));

  // Verify ROLLUP(a, b) expands to: [[0,1], [0], []]
  auto aggNode = std::dynamic_pointer_cast<const AggregateNode>(plan);
  ASSERT_NE(aggNode, nullptr);
  EXPECT_THAT(
      aggNode->groupingSets(),
      testing::ElementsAre(
          std::vector<int32_t>{0, 1},
          std::vector<int32_t>{0},
          std::vector<int32_t>{}));
}

TEST_F(PlanBuilderTest, cube) {
  auto rowType = ROW({"a", "b", "c"}, INTEGER());
  std::vector<Variant> data{
      Variant::row({1, 10, 100}),
  };

  auto plan = PlanBuilder()
                  .values(rowType, data)
                  .cube({"a", "b"}, {"sum(c) as total"}, "$grouping_set_id")
                  .build();

  // Output should have: a, b (grouping keys), total, $grouping_set_id
  EXPECT_THAT(
      plan->outputType()->names(),
      testing::ElementsAre("a", "b", "total", "$grouping_set_id"));

  // Verify CUBE(a, b) expands to: [[0,1], [0], [1], []]
  auto aggNode = std::dynamic_pointer_cast<const AggregateNode>(plan);
  ASSERT_NE(aggNode, nullptr);
  EXPECT_THAT(
      aggNode->groupingSets(),
      testing::ElementsAre(
          std::vector<int32_t>{0, 1},
          std::vector<int32_t>{0},
          std::vector<int32_t>{1},
          std::vector<int32_t>{}));
}

TEST_F(PlanBuilderTest, cubeThreeKeys) {
  auto rowType = ROW({"a", "b", "c", "d"}, INTEGER());
  std::vector<Variant> data{
      Variant::row({1, 10, 100, 1000}),
  };

  auto plan =
      PlanBuilder()
          .values(rowType, data)
          .cube({"a", "b", "c"}, {"sum(d) as total"}, "$grouping_set_id")
          .build();

  // Verify CUBE(a, b, c) expands to 2^3 = 8 grouping sets.
  auto aggNode = std::dynamic_pointer_cast<const AggregateNode>(plan);
  ASSERT_NE(aggNode, nullptr);
  EXPECT_THAT(
      aggNode->groupingSets(),
      testing::ElementsAre(
          std::vector<int32_t>{0, 1, 2},
          std::vector<int32_t>{0, 1},
          std::vector<int32_t>{0, 2},
          std::vector<int32_t>{0},
          std::vector<int32_t>{1, 2},
          std::vector<int32_t>{1},
          std::vector<int32_t>{2},
          std::vector<int32_t>{}));
}

} // namespace
} // namespace facebook::axiom::logical_plan
