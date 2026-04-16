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

TEST_F(PlanBuilderTest, anonymousOutputNames) {
  auto builder = PlanBuilder()
                     .values(
                         ROW({"a"}, {BIGINT()}),
                         std::vector<Variant>{Variant::row({123LL})})
                     .project({"a + 1", "a + 2 as b"});

  using Name = PlanBuilder::OutputColumnName;

  EXPECT_EQ(2, builder.numOutput());
  EXPECT_EQ((Name{std::nullopt, "expr"}), builder.findOrAssignOutputNameAt(0));
  EXPECT_EQ((Name{std::nullopt, "b"}), builder.findOrAssignOutputNameAt(1));

  builder.with({"b * 2"});

  EXPECT_EQ(3, builder.numOutput());

  EXPECT_THAT(
      builder.findOrAssignOutputNames(),
      testing::ElementsAre(
          Name{std::nullopt, "expr"},
          Name{std::nullopt, "b"},
          Name{std::nullopt, "expr_0"}));
}

// Verifies that findOrAssignOutputNames returns aliases for all ambiguous
// column names from a join where both sides have the same columns.
TEST_F(PlanBuilderTest, ambiguousOutputNamesFullOverlap) {
  PlanBuilder::Context context;

  auto buildValues = [&](const std::string& alias) {
    return PlanBuilder(
               context,
               /*enableCoercions=*/false,
               /*allowAmbiguousOutputNames=*/true)
        .values(ROW({"a", "b"}, BIGINT()), ValuesNode::Variants{})
        .as(alias);
  };

  auto builder =
      buildValues("t").join(buildValues("u"), "t.a = u.a", JoinType::kInner);

  using Name = PlanBuilder::OutputColumnName;
  auto names = builder.findOrAssignOutputNames();
  EXPECT_THAT(
      names,
      testing::ElementsAre(
          Name{"t", "a"}, Name{"t", "b"}, Name{"u", "a"}, Name{"u", "b"}));

  ASSERT_NO_THROW(builder.project({
      names[0].toCol(),
      names[1].toCol(),
      names[2].toCol(),
      names[3].toCol(),
  }));
}

// Verifies that findOrAssignOutputNames returns aliases only for ambiguous
// columns when tables partially overlap.
TEST_F(PlanBuilderTest, ambiguousOutputNamesPartialOverlap) {
  PlanBuilder::Context context;

  auto buildValues = [&](const std::string& alias) {
    return PlanBuilder(
               context,
               /*enableCoercions=*/false,
               /*allowAmbiguousOutputNames=*/true)
        .values(ROW({"a", "b"}, BIGINT()), ValuesNode::Variants{})
        .as(alias);
  };

  // Rename "b" to "c" on the left side so only "a" overlaps.
  auto builder = buildValues("t")
                     .project({"a", "b as c"})
                     .as("t")
                     .join(buildValues("u"), "t.a = u.a", JoinType::kInner);

  auto names = builder.findOrAssignOutputNames();

  using Name = PlanBuilder::OutputColumnName;

  // "a" is ambiguous — both tables have it. "c" and "b" are unique.
  EXPECT_THAT(
      names,
      testing::ElementsAre(
          Name{"t", "a"},
          Name{std::nullopt, "c"},
          Name{"u", "a"},
          Name{std::nullopt, "b"}));

  ASSERT_NO_THROW(builder.project({
      names[0].toCol(),
      names[1].toCol(),
      names[2].toCol(),
      names[3].toCol(),
  }));
}

TEST_F(PlanBuilderTest, duplicateAliasAllowed) {
  PlanBuilder::Context context;

  auto makeBuilder = [&context]() {
    return PlanBuilder(
        context,
        /*enableCoercions=*/false,
        /*allowAmbiguousOutputNames=*/true);
  };

  // Project: duplicate aliases get unique physical names.
  {
    auto builder =
        makeBuilder()
            .values(ROW({"a", "b"}, BIGINT()), ValuesNode::Variants{})
            .with({"a as x", "b as x"});

    auto names = builder.findOrAssignOutputNames();
    EXPECT_EQ(4, names.size());
    EXPECT_EQ("x", names[2].name);
    EXPECT_TRUE(names[3].name.starts_with("x_"));
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
    EXPECT_TRUE(names[0].name.starts_with("a"));
    EXPECT_TRUE(names[2].name.starts_with("a"));
  }

  // Unnest: ordinality alias duplicates an unnest alias.
  {
    auto builder =
        makeBuilder()
            .values(ROW({"a"}, ARRAY(BIGINT())), ValuesNode::Variants{})
            .unnest({Col("a").unnestAs("x")}, Ordinality().as("x"));

    auto names = builder.findOrAssignOutputNames();
    EXPECT_EQ(3, names.size());
    EXPECT_TRUE(names[1].name.starts_with("x"));
    EXPECT_TRUE(names[2].name.starts_with("x"));
    EXPECT_NE(names[1].name, names[2].name);
  }

  // Aggregate: duplicate aliases get unique physical names.
  {
    auto builder =
        makeBuilder()
            .values(ROW({"a", "b"}, BIGINT()), ValuesNode::Variants{})
            .aggregate({}, {"sum(a) as x", "sum(b) as x"});

    auto names = builder.findOrAssignOutputNames();
    EXPECT_EQ(2, names.size());
    EXPECT_TRUE(names[0].name.starts_with("x"));
    EXPECT_TRUE(names[1].name.starts_with("x"));
    EXPECT_NE(names[0].name, names[1].name);
  }
}

TEST_F(PlanBuilderTest, emptyAliasAllowed) {
  PlanBuilder::Context context;

  auto makeValues = [&context](const RowTypePtr& rowType) {
    return PlanBuilder(
               context,
               /*enableCoercions=*/false,
               /*allowAmbiguousOutputNames=*/true)
        .values(rowType, ValuesNode::Variants{});
  };

  auto assertOutputNames = [](PlanBuilder builder,
                              const std::vector<std::string>& expected) {
    auto plan = builder.build();
    EXPECT_THAT(
        plan->outputType()->names(), testing::ElementsAreArray(expected));
  };

  // Unnest: empty aliases for array.
  assertOutputNames(
      makeValues(ROW("a", ARRAY(BIGINT()))).unnest({Col("a").unnestAs("")}),
      {"a", ""});

  // Unnest: empty aliases for map.
  assertOutputNames(
      makeValues(ROW("m", MAP(BIGINT(), BIGINT())))
          .unnest({Col("m").unnestAs("", "")}),
      {"m", "", ""});

  // Unnest: empty ordinality alias.
  assertOutputNames(
      makeValues(ROW("a", ARRAY(BIGINT())))
          .unnest({Col("a").unnestAs("elem")}, Ordinality().as("")),
      {"a", "elem", ""});
}

TEST_F(PlanBuilderTest, duplicateAliasThrows) {
  // Duplicate aliases throw by default in project/aggregate operations.
  VELOX_ASSERT_THROW(
      PlanBuilder()
          .values(ROW({"a", "b"}, BIGINT()), ValuesNode::Variants{})
          .with({"a as x", "b as x"}),
      "Duplicate name: x");
}

TEST_F(PlanBuilderTest, unnestOrdinality) {
  auto assertOutputNames = [](PlanBuilder builder,
                              const std::vector<std::string>& expected) {
    auto plan = builder.build();
    EXPECT_THAT(
        plan->outputType()->names(), testing::ElementsAreArray(expected));
  };

  auto makeValues = [] {
    return PlanBuilder().values(
        ROW("a", ARRAY(BIGINT())), ValuesNode::Variants{});
  };

  // Without alias.
  assertOutputNames(
      makeValues().unnest({Col("a").unnestAs("elem")}, Ordinality()),
      {"a", "elem", "ordinality"});

  // With alias.
  assertOutputNames(
      makeValues().unnest({Col("a").unnestAs("elem")}, Ordinality().as("ord")),
      {"a", "elem", "ord"});
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

TEST_F(PlanBuilderTest, joinUsingTypeCoercion) {
  auto startMatcher = [] { return test::LogicalPlanMatcherBuilder().values(); };

  auto makeBuilders = [](PlanBuilder::Context& context,
                         const velox::RowTypePtr& leftType,
                         const velox::RowTypePtr& rightType,
                         bool enableCoercions = true) {
    auto left = PlanBuilder(context, enableCoercions)
                    .values(leftType, ValuesNode::Variants{});
    auto right = PlanBuilder(context).values(rightType, ValuesNode::Variants{});
    return std::make_pair(std::move(left), std::move(right));
  };

  // INTEGER vs BIGINT -> BIGINT.
  {
    PlanBuilder::Context context;
    auto [left, right] = makeBuilders(
        context,
        ROW({"c0", "c1"}, {INTEGER(), VARCHAR()}),
        ROW({"c0", "c1"}, {BIGINT(), VARCHAR()}));

    auto plan = left.joinUsing(right, {"c0"}, JoinType::kInner).build();

    VELOX_EXPECT_EQ_TYPES(
        plan->outputType(),
        ROW({"c0", "c1", "c1_1"}, {BIGINT(), VARCHAR(), VARCHAR()}));

    auto matcher =
        startMatcher().join(startMatcher().build()).project().build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString();
  }

  // Same types on both sides.
  {
    PlanBuilder::Context context;
    auto [left, right] = makeBuilders(
        context,
        ROW({"c0", "c1"}, {BIGINT(), VARCHAR()}),
        ROW({"c0", "c1"}, {BIGINT(), VARCHAR()}));

    auto plan = left.joinUsing(right, {"c0"}, JoinType::kInner).build();

    VELOX_EXPECT_EQ_TYPES(
        plan->outputType(),
        ROW({"c0", "c1", "c1_1"}, {BIGINT(), VARCHAR(), VARCHAR()}));

    auto matcher =
        startMatcher().join(startMatcher().build()).project().build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString();
  }

  // Incompatible types (VARCHAR vs INTEGER) fail even with coercions enabled.
  {
    PlanBuilder::Context context;
    auto [left, right] = makeBuilders(
        context,
        ROW({"c0", "c1"}, {VARCHAR(), BIGINT()}),
        ROW({"c0", "c1"}, {INTEGER(), BIGINT()}));

    VELOX_ASSERT_THROW(
        left.joinUsing(right, {"c0"}, JoinType::kInner),
        "USING column has incompatible types");
  }

  // Mismatched types fail when coercions are disabled.
  {
    PlanBuilder::Context context;
    auto [left, right] = makeBuilders(
        context,
        ROW({"c0", "c1"}, {INTEGER(), VARCHAR()}),
        ROW({"c0", "c1"}, {BIGINT(), VARCHAR()}),
        /*enableCoercions=*/false);

    VELOX_ASSERT_THROW(
        left.joinUsing(right, {"c0"}, JoinType::kInner),
        "USING column has different types");
  }

  // RIGHT JOIN with coercion.
  {
    PlanBuilder::Context context;
    auto [left, right] = makeBuilders(
        context,
        ROW({"c0", "c1"}, {INTEGER(), VARCHAR()}),
        ROW({"c0", "c1"}, {BIGINT(), VARCHAR()}));

    auto plan = left.joinUsing(right, {"c0"}, JoinType::kRight).build();

    VELOX_EXPECT_EQ_TYPES(
        plan->outputType(),
        ROW({"c0", "c1", "c1_1"}, {BIGINT(), VARCHAR(), VARCHAR()}));

    auto matcher =
        startMatcher().join(startMatcher().build()).project().project().build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString();
  }

  // FULL OUTER JOIN with coercion.
  {
    PlanBuilder::Context context;
    auto [left, right] = makeBuilders(
        context,
        ROW({"c0", "c1"}, {INTEGER(), VARCHAR()}),
        ROW({"c0", "c1"}, {BIGINT(), VARCHAR()}));

    auto plan = left.joinUsing(right, {"c0"}, JoinType::kFull).build();

    VELOX_EXPECT_EQ_TYPES(
        plan->outputType(),
        ROW({"c0", "c1", "c1_1"}, {BIGINT(), VARCHAR(), VARCHAR()}));

    auto matcher =
        startMatcher().join(startMatcher().build()).project().project().build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString();
  }

  // Multiple USING columns with mixed match/mismatch.
  {
    PlanBuilder::Context context;
    auto [left, right] = makeBuilders(
        context,
        ROW({"c0", "c1", "c2"}, {INTEGER(), VARCHAR(), REAL()}),
        ROW({"c0", "c1", "c2"}, {BIGINT(), VARCHAR(), DOUBLE()}));

    auto plan = left.joinUsing(right, {"c0", "c1"}, JoinType::kInner).build();

    VELOX_EXPECT_EQ_TYPES(
        plan->outputType(),
        ROW({"c0", "c1", "c2", "c2_2"},
            {BIGINT(), VARCHAR(), REAL(), DOUBLE()}));

    auto matcher =
        startMatcher().join(startMatcher().build()).project().build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString();
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
          .aggregate(
              std::vector<std::vector<std::string>>{{}},
              std::vector<std::string>{},
              "$grouping_set_id")
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
              {"a", "a"}, {{0, 1}}, {"sum(c) as total"}, "$grouping_set_id")
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

namespace {
LogicalPlanNodePtr buildValues(
    const std::vector<std::string>& columns,
    const std::vector<std::vector<std::string>>& rows,
    bool enableCoercions = true) {
  PlanBuilder::Context context;
  return PlanBuilder(context, enableCoercions).values(columns, rows).build();
}
} // namespace

TEST_F(PlanBuilderTest, valuesTypeCoercion) {
  // Scalar: BIGINT + INTEGER → BIGINT, with nulls.
  EXPECT_EQ(
      *buildValues(
           {"a", "b"},
           {{"CAST(123 AS bigint)", "CAST(1 AS integer)"},
            {"CAST(null AS integer)", "CAST(null AS bigint)"}})
           ->outputType(),
      *ROW({"a", "b"}, {BIGINT(), BIGINT()}));

  // Progressive widening: INTEGER → BIGINT → DOUBLE.
  EXPECT_EQ(
      *buildValues(
           {"x"},
           {{"CAST(1 AS integer)"},
            {"CAST(2 AS bigint)"},
            {"CAST(3.0 AS double)"}})
           ->outputType(),
      *ROW({"x"}, {DOUBLE()}));

  // Complex type with mixed-direction coercion per child.
  // MAP(INTEGER, DOUBLE) + MAP(BIGINT, REAL): key widens INT→BIGINT,
  // value widens REAL→DOUBLE. leastCommonSuperType picks direction per child.
  EXPECT_EQ(
      *buildValues(
           {"x"},
           {{"MAP(ARRAY[CAST(1 AS integer)], ARRAY[CAST(1.0 AS double)])"},
            {"MAP(ARRAY[CAST(2 AS bigint)], ARRAY[CAST(2.0 AS real)])"}})
           ->outputType(),
      *ROW({"x"}, {MAP(BIGINT(), DOUBLE())}));
}

TEST_F(PlanBuilderTest, valuesTypeCoercionErrors) {
  // Incompatible scalar types.
  VELOX_ASSERT_THROW(
      buildValues({"x"}, {{"true"}, {"CAST(123 AS bigint)"}}),
      "Incompatible types in VALUES row 2, column 1: "
      "expected BOOLEAN, got BIGINT");

  // Incompatible null types.
  VELOX_ASSERT_THROW(
      buildValues({"x"}, {{"CAST(null AS boolean)"}, {"CAST(null AS bigint)"}}),
      "Incompatible types in VALUES row 2, column 1: "
      "expected BOOLEAN, got BIGINT");

  // Incompatible complex types.
  VELOX_ASSERT_THROW(
      buildValues(
          {"x"}, {{"ARRAY[CAST(1 AS integer)]"}, {"MAP(ARRAY[1], ARRAY[2])"}}),
      "Incompatible types in VALUES row 2, column 1: "
      "expected ARRAY<INTEGER>, got MAP<INTEGER,INTEGER>");

  // Coercions disabled.
  VELOX_ASSERT_THROW(
      buildValues(
          {"x"},
          {{"CAST(null AS boolean)"}, {"CAST(null AS bigint)"}},
          /*enableCoercions=*/false),
      "All values should have equivalent types: BIGINT vs. ROW<x:BOOLEAN>");
}

} // namespace
} // namespace facebook::axiom::logical_plan
