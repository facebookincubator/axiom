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
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class NonDeterministicTest : public test::QueryTestBase {
 protected:
  // A fixed-size source with the shared schema. The row count must stay above
  // the limits used by the limit/top-N tests so those boundaries are not
  // optimized away.
  RowVectorPtr makeSource() {
    constexpr int32_t kNumRows = 64;
    return makeRowVector(
        {"k", "v", "s"},
        {makeFlatVector<int64_t>(std::vector<int64_t>(kNumRows)),
         makeFlatVector<int64_t>(std::vector<int64_t>(kNumRows)),
         makeFlatVector<std::string>(std::vector<std::string>(kNumRows))});
  }

  // Registers a table with the shared schema used by every scan-based test.
  void addTable(const std::string& name) {
    testConnector_->addTable(
        name, ROW({"k", "v", "s"}, {BIGINT(), BIGINT(), VARCHAR()}));
  }
};

// A non-deterministic projection referenced more than once is computed once and
// reused: all references see the same value.
TEST_F(NonDeterministicTest, reusedProjectionComputedOnce) {
  auto logicalPlan =
      lp::PlanBuilder()
          .values({makeSource()})
          .map({"concat(cast(uuid() as varchar), '-suffix') as y"})
          .map({"y as a", "y as b", "upper(y) as c"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchValues()
          .project({"concat(cast(uuid() as varchar), '-suffix') as y"})
          .project({"y as a", "y as b", "upper(y) as c"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value compared against itself within a single expression
// is one draw: (r = r) is always true.
TEST_F(NonDeterministicTest, ndSelfComparisonInProjection) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"rand() as r"})
                         .map({"(r = r) as b"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchValues().project({"rand() as r"}).project({"r = r as b"}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Multiple references to a non-deterministic value inside a single filter
// predicate are one draw: r <> r is never true, so no rows pass.
TEST_F(NonDeterministicTest, ndSelfComparisonInFilter) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"rand() as r"})
                         .filter("r <> r")
                         .map({"1 as one"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({"rand() as r"})
                     .filter("r <> r")
                     .project({"1 as one"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Subfield form of the single-expression self-comparison: seq[1] = seq[1] reads
// the same element of one materialized array, so it is always true.
TEST_F(NonDeterministicTest, subfieldSelfComparisonInProjection) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"shuffle(sequence(1, k + 5)) as seq"})
                         .map({"(seq[1] = seq[1]) as b"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({"shuffle(sequence(1, k + 5)) as seq"})
                     .project({"seq[1] = seq[1] as b"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Two distinct occurrences of a non-deterministic function are independent and
// must each be evaluated.
TEST_F(NonDeterministicTest, distinctOccurrencesStayDistinct) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"uuid() as a", "uuid() as b"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues().project({"uuid() as a", "uuid() as b"}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A deterministic projection referenced more than once is still inlined, not
// materialized into a separate projection.
TEST_F(NonDeterministicTest, deterministicProjectionNotMaterialized) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"k + 1 as x"})
                         .map({"x as p", "x * 2 as q"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchValues().project({"k + 1 as p", "(k + 1) * 2 as q"}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value accessed through a subfield more than once is
// computed once and reused.
TEST_F(NonDeterministicTest, reusedSubfieldComputedOnce) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"shuffle(sequence(1, 100)) as seq"})
                         .map({"seq[1] as a1", "seq[1] as a2"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({R"(shuffle("any"()) as g)"})
                     .project({"g[1] as a1", "g[1] as a2"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A field defined directly as a subfield over a non-deterministic function,
// then subfielded again and reused. The outer subfield must be computed once
// over the shared base.
TEST_F(NonDeterministicTest, nestedSubfieldOverFunction) {
  auto logicalPlan =
      lp::PlanBuilder()
          .values({makeSource()})
          .map({"shuffle(array[array[1,2,3], array[4,5,6]])[1] as field1"})
          .map({"field1[1] as a", "field1[1] as b"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({R"(shuffle("any"())[1] as field1)"})
                     .project({"field1[1] as a", "field1[1] as b"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value reused inside a UNION ALL leg is computed once per
// leg.
TEST_F(NonDeterministicTest, reusedAcrossUnionLeg) {
  lp::PlanBuilder::Context ctx;
  auto leftLeg = lp::PlanBuilder(ctx)
                     .values({makeSource()})
                     .map({"uuid() as u"})
                     .map({"u as a", "u as b"});
  auto rightLeg = lp::PlanBuilder(ctx)
                      .values({makeSource()})
                      .map({"uuid() as u"})
                      .map({"u as a", "u as b"});

  auto logicalPlan = leftLeg.unionAll(rightLeg).build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto leg = [this]() {
    return matchValues()
        .project({"uuid() as u"})
        .project({"u as a", "u as b"})
        .build();
  };
  auto matcher = matchValues()
                     .project({"uuid() as u"})
                     .project({"u as a", "u as b"})
                     .localPartition(leg())
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value referenced more than once inside a single filter
// predicate is computed once.
TEST_F(NonDeterministicTest, reusedInFilterComputedOnce) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"cast(uuid() as varchar) as u", "k"})
                         .filter("u >= '0' and u <= 'zzzzz'")
                         .map({"k"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({"cast(uuid() as varchar) as u", "k"})
                     .filter("u >= '0' and u <= 'zzzzz'")
                     .project({"k"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value referenced by both a filter and a downstream
// projection is computed once and shared across both.
TEST_F(NonDeterministicTest, reusedAcrossFilterAndProjection) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"cast(uuid() as varchar) as u"})
                         .filter("u <= 'zzzzz'")
                         .map({"u as a", "u as b"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({"cast(uuid() as varchar) as u"})
                     .filter("u <= 'zzzzz'")
                     .project({"u as a", "u as b"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A deterministic filter on a base column pushes below the non-deterministic
// materialization; the reused source is still computed once.
TEST_F(NonDeterministicTest, deterministicFilterPushedUnderNDMaterialization) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"k + 5 as k"})
                         .map({"shuffle(sequence(1, k)) as seq", "k"})
                         .filter("k > 0")
                         .map({"seq[1] as a", "seq[1] as b"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .filter("k + 5 > 0")
                     .project({"shuffle(sequence(1, k + 5)) as seq"})
                     .project({"seq[1] as a", "seq[1] as b"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic array reused inside a UNION ALL leg via subfield is
// computed once per leg.
TEST_F(NonDeterministicTest, reusedSubfieldInUnionLeg) {
  lp::PlanBuilder::Context ctx;
  auto leftLeg = lp::PlanBuilder(ctx)
                     .values({makeSource()})
                     .map({"shuffle(sequence(1, k + 10)) as seq"})
                     .map({"seq[1] as a", "seq[1] as b"});
  auto rightLeg = lp::PlanBuilder(ctx)
                      .values({makeSource()})
                      .map({"shuffle(sequence(1, k + 10)) as seq"})
                      .map({"seq[1] as a", "seq[1] as b"});
  auto logicalPlan = leftLeg.unionAll(rightLeg).build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto leg = [this]() {
    return matchValues()
        .project({R"(shuffle("any"()) as g)"})
        .project({"g[1] as a", "g[1] as b"})
        .build();
  };
  auto matcher = matchValues()
                     .project({R"(shuffle("any"()) as g)"})
                     .project({"g[1] as a", "g[1] as b"})
                     .localPartition(leg())
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value computed in the same projection as an aggregate
// output sits at post-aggregation scope.
TEST_F(NonDeterministicTest, reusedNonDeterministicWithAggregateOutput) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"k % 3 as g", "k"})
                         .aggregate({"g"}, {"max(k) as mx"})
                         .map({"uuid() as u", "mx"})
                         .map({"u as a", "u as b", "mx"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({"k % 3 as g", "k"})
                     .singleAggregation({"g"}, {"max(k) as mx"})
                     .project({"uuid() as u", "mx"})
                     .project({"u as a", "u as b", "mx"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A pre-aggregation non-deterministic source reused across several aggregate
// arguments is computed once.
TEST_F(NonDeterministicTest, reusedNonDeterministicInAggregateArgs) {
  auto logicalPlan =
      lp::PlanBuilder()
          .values({makeSource()})
          .map({"rand() as r"})
          .aggregate({}, {"sum(r) as seq", "count(r) as c", "max(r) as mx"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({"rand() as r"})
                     .singleAggregation(
                         {}, {"sum(r) as seq", "count(r) as c", "max(r) as mx"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A pre-window non-deterministic source reused across two window functions is
// computed once.
TEST_F(NonDeterministicTest, reusedNonDeterministicInWindowArgs) {
  addTable("t");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto logicalPlan = lp::PlanBuilder(ctx)
                         .tableScan("t", {"k"})
                         .map({"rand() as r", "k"})
                         .map(
                             {"max(r) over (partition by k order by k) as mx",
                              "min(r) over (partition by k order by k) as mn"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .project({"rand() as r", "k"})
                     .window(
                         {"max(r) OVER (PARTITION BY k) as mx",
                          "min(r) OVER (PARTITION BY k) as mn"})
                     .project({"mx", "mn"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value used as a grouping key and reused in
// post-aggregation projection is materialized once.
TEST_F(NonDeterministicTest, reusedNonDeterministicAsGroupingKey) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"uuid() as u"})
                         .aggregate({"u"}, {"count(1) as c"})
                         .map({"u as a", "u as b", "c"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({"uuid() as u"})
                     .singleAggregation({"u"}, {"count(1) as c"})
                     .project({"u as a", "u as b", "c"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A reused non-deterministic value used as an equi-join key and in post-join
// projection must be materialized once on the join table.
TEST_F(NonDeterministicTest, reusedNonDeterministicAsJoinKey) {
  addTable("t_left");
  addTable("t_right");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto logicalPlan = lp::PlanBuilder(ctx)
                         .tableScan("t_left", {"k"})
                         .map({"cast(uuid() as varchar) as u"})
                         .join(
                             lp::PlanBuilder(ctx).tableScan("t_right", {"s"}),
                             "u = s",
                             lp::JoinType::kInner)
                         .map({"u as a", "u as b"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto buildSide = matchScan("t_right").build();
  auto matcher = matchScan("t_left")
                     .project({"cast(uuid() as varchar) as u"})
                     .hashJoinInner(buildSide, {.keys = {{"u = s"}}})
                     .project({"u as a", "u as b"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value read only by an equi-join key and a post-join
// filter must stay materialized.
TEST_F(NonDeterministicTest, reusedInJoinKeyAndFilter) {
  addTable("t_left");
  addTable("t_right");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto logicalPlan = lp::PlanBuilder(ctx)
                         .tableScan("t_left", {"k"})
                         .map({"cast(uuid() as varchar) as u"})
                         .join(
                             lp::PlanBuilder(ctx).tableScan("t_right", {"s"}),
                             "u = s",
                             lp::JoinType::kInner)
                         .filter("u > '0'")
                         .map({"s"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto buildSide = matchScan("t_right").build();
  auto matcher = matchScan("t_left")
                     .project({"cast(uuid() as varchar) as u"})
                     .filter("u > '0'")
                     .hashJoinInner(buildSide, {.keys = {{"u = s"}}})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value on the null-supplying side of a LEFT JOIN must stay
// materialized.
TEST_F(NonDeterministicTest, reusedInLeftJoinFilter) {
  addTable("t_left");
  addTable("t_right");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto logicalPlan = lp::PlanBuilder(ctx)
                         .tableScan("t_left", {"k"})
                         .join(
                             lp::PlanBuilder(ctx)
                                 .tableScan("t_right", {"v"})
                                 .map({"cast(uuid() as varchar) as u", "v"}),
                             "k = v and u > '0'",
                             lp::JoinType::kLeft)
                         .map({"k as lk", "u as uu"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto buildSide = matchScan("t_right")
                       .project({"cast(uuid() as varchar) as u", "v"})
                       .build();
  auto matcher = matchScan("t_left")
                     .hashJoin(
                         buildSide,
                         core::JoinType::kLeft,
                         {.keys = {{"k = v"}}, .filter = "u > '0'"})
                     .project({"k as lk", "u as uu"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A reused non-deterministic value in a filter, on a query that also carries a
// window partitioned/ordered by base columns: the value is materialized once
// below the filter, and the window still partitions by the base column.
TEST_F(NonDeterministicTest, reusedInFilterWithBaseColumnPassThrough) {
  addTable("t");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto logicalPlan =
      lp::PlanBuilder(ctx)
          .tableScan("t", {"k"})
          .map({"cast(uuid() as varchar) as u", "k"})
          .filter("u >= '0' and u <= 'zzzzz'")
          .map(
              {"row_number() over (partition by k order by k) as rn",
               "u as uu"})
          .build();
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .project({"cast(uuid() as varchar) as u", "k"})
                     .filter("u >= '0' and u <= 'zzzzz'")
                     .rowNumber({"k"})
                     .project({R"("any"())", "u as uu"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value computed in the same projection as a window
// function output. The value is materialized once in the projection above the
// window (not pulled below it).
TEST_F(NonDeterministicTest, reusedNonDeterministicWithWindowOutput) {
  addTable("t");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto logicalPlan =
      lp::PlanBuilder(ctx)
          .tableScan("t", {"k"})
          .map(
              {"row_number() over (partition by k order by k) as m",
               "uuid() as u"})
          .map({"u as a", "u as b", "m"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  // RowNumber (the window) sits directly over the scan; the uuid is computed
  // once in the projection above it, then the outer projection reuses that one
  // column as both a and b.
  auto matcher = matchScan("t")
                     .rowNumber({"k"})
                     .project({R"("any"())", "uuid()"})
                     .aliases({"m", "u"})
                     .project({"u as a", "u as b", "m"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value read by a window function argument and a filter
// must stay materialized.
TEST_F(NonDeterministicTest, reusedInWindowArgAndFilter) {
  addTable("t");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};

  auto logicalPlan = lp::PlanBuilder(ctx)
                         .tableScan("t", {"k", "v"})
                         .map({"cast(uuid() as varchar) as u", "k", "v"})
                         .filter("u > '0'")
                         .map({"max(u) over (partition by k order by v) as m"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .project({"cast(uuid() as varchar) as u", "k", "v"})
                     .filter("u > '0'")
                     .window({"max(u) OVER (PARTITION BY k ORDER BY v) as m"})
                     .project({"m"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A filter on a reused non-deterministic value over a table scan is not pushed
// down.
TEST_F(NonDeterministicTest, reusedAcrossPushedDownFilterTableScan) {
  addTable("t");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};

  auto logicalPlan = lp::PlanBuilder(ctx)
                         .tableScan("t", {"k"})
                         .map({"shuffle(sequence(1, k + 10)) as seq"})
                         .map({"seq[1] as x", "seq[1] as y"})
                         .filter("x > -1")
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .project({"shuffle(sequence(1, k + 10)) as g"})
                     .filter("g[1] > -1")
                     .project({"g[1] as x", "g[1] as y"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic filter whose value is NOT reused is pushed to table scan.
TEST_F(NonDeterministicTest, nonReusedFilterStillPushesToScan) {
  addTable("t");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};

  auto logicalPlan = lp::PlanBuilder(ctx)
                         .tableScan("t", {"k"})
                         .map({"rand() as r", "k"})
                         .filter("r > -1.0")
                         .map({"k"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t").filter("rand() > -1.0").build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic value read through a SINGLE subfield is demand 1, so the
// filter is pushed to the scan rather than kept materialized.
TEST_F(NonDeterministicTest, singleUseSubfieldFilterPushesToScan) {
  addTable("t");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};

  auto logicalPlan = lp::PlanBuilder(ctx)
                         .tableScan("t", {"k"})
                         .map({"shuffle(sequence(1, k + 10)) as seq", "k"})
                         .filter("seq[1] > -1")
                         .map({"k"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchScan("t").filter("shuffle(sequence(1, k + 10))[1] > -1").build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A per-row non-deterministic value referenced from inside a lambda body must
// stay materialized.
TEST_F(NonDeterministicTest, lambdaCapturedNonDeterministicStaysMaterialized) {
  auto logicalPlan =
      lp::PlanBuilder()
          .values({makeSource()})
          .map({"k", "rand() as r"})
          .filter(
              "cardinality(array_distinct(transform(sequence(1, k + 2), x -> r))) = 1")
          .map({"k"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchValues()
          .project({"k", "rand() as r"})
          .filter(
              "cardinality(array_distinct(transform(sequence(1, k + 2), x -> r))) = 1")
          .project({"k"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A reused non-deterministic filter over a pure UNION ALL (the other
// allowNondeterministic branch). The filter is not pushed into the legs.
TEST_F(NonDeterministicTest, reusedFilterOverUnionAll) {
  addTable("t_left");
  addTable("t_right");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};

  auto left = lp::PlanBuilder(ctx).tableScan("t_left", {"k"});
  auto right = lp::PlanBuilder(ctx).tableScan("t_right", {"k"});
  auto logicalPlan = left.unionAll(right)
                         .map({"shuffle(sequence(1, k + 10)) as arr"})
                         .map({"arr[1] as p", "arr[1] as q"})
                         .filter("p > -1")
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  // Union of the two scans, then the generator materialized once (g) above the
  // union, referenced by the filter and both outputs.
  auto rightLeg = matchScan("t_right").project({"k_0 as k"}).build();
  auto matcher = matchScan("t_left")
                     .localPartition(rightLeg)
                     .project({"shuffle(sequence(1, k + 10)) as g"})
                     .filter("g[1] > -1")
                     .project({"g[1] as p", "g[1] as q"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A field defined as a subfield over another field over a function, reused: the
// whole chain is materialized once and shared.
TEST_F(NonDeterministicTest, chainedFieldOfFieldOverFunction) {
  auto logicalPlan =
      lp::PlanBuilder()
          .values({makeSource()})
          .map({"shuffle(array[array[1,2,3], array[4,5,6]]) as f0"})
          .map({"f0[1] as field1"})
          .map({"field1[2] as field2"})
          .map({"field2 as a", "field2 as b"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({R"(shuffle("any"()) as g)"})
                     .project({"g[1][2] as a", "g[1][2] as b"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A function-defined field subfielded with multiple steps, reused: the full
// subscript chain is computed once and shared.
TEST_F(NonDeterministicTest, multiStepNestedSubfield) {
  auto logicalPlan =
      lp::PlanBuilder()
          .values({makeSource()})
          .map(
              {"shuffle(array[array[array[1,2],array[3,4]], array[array[5,6],array[7,8]]]) as f0"})
          .map({"f0[1][1] as field1"})
          .map({"field1[1] as a", "field1[1] as b"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({R"(shuffle("any"()) as g)"})
                     .project({"g[1][1][1] as a", "g[1][1][1] as b"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A reused non-deterministic value referenced from an ORDER BY and the
// projection is computed once below the OrderBy.
TEST_F(NonDeterministicTest, reusedInOrderBy) {
  addTable("t");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto logicalPlan = lp::PlanBuilder(ctx)
                         .tableScan("t", {"k"})
                         .map({"shuffle(sequence(1, k + 10))[1] as u"})
                         .orderBy({"u"})
                         .map({"u as a", "u as b"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .project({"shuffle(sequence(1, k + 10))[1] as u"})
                     .orderBy({"u ASC NULLS LAST"})
                     .project({"u as a", "u as b"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A reused non-deterministic value with an ORDER BY on base column with no
// LIMIT. The query must pass through the order by key column.
TEST_F(NonDeterministicTest, reusedNonDeterministicWithOrderByNoLimit) {
  addTable("t");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto logicalPlan = lp::PlanBuilder(ctx)
                         .tableScan("t", {"k"})
                         .map({"uuid() as u", "k"})
                         .orderBy({"k"})
                         .map({"u as a", "u as b"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .project({"uuid() as u", "k"})
                     .orderBy({"k ASC NULLS LAST"})
                     .project({"u as a", "u as b"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic projection over a subquery that carries a LIMIT. The
// LIMIT is a cardinality barrier at the child's layer.
TEST_F(NonDeterministicTest, ndOverLimitKeepsBoundary) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .limit(3)
                         .map({"uuid() as u"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues().limit().project({"uuid() as u"}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic projection over a subquery that carries ORDER BY ...
// LIMIT (top-N). The non-deterministic materialization must be above the top-N.
TEST_F(NonDeterministicTest, ndOverOrderByLimitKeepsBoundary) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .orderBy({"k"})
                         .limit(3)
                         .map({"uuid() as u", "k"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  // The order-by + limit collapses to a TopN barrier below the
  // non-deterministic projection.
  auto matcher = matchValues().topN(3).project({"uuid() as u", "k"}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// An inner non-deterministic value (u) reused both standalone (a) and nested
// inside another reused expression (v). The
// inner generator must be materialized one layer deeper so v references the
// single u column rather than recomputing it.
TEST_F(NonDeterministicTest, nestedCrossPathReuse) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"cast(uuid() as varchar) as u"})
                         .map({"u as uu", "concat(u, 'x') as v"})
                         .map({"uu as a", "v as b", "v as c"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchValues()
          .project({"cast(uuid() as varchar) as u"})
          .project({"u as a", "concat(u, 'x') as b", "concat(u, 'x') as c"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A non-deterministic source read via a subfield (m) feeds the argument of a
// second non-deterministic source (arr); both are reused. The two must be
// stacked -- the inner shuffle deepest, arr above it referencing m -- so each
// shuffle is evaluated once.
TEST_F(NonDeterministicTest, sourceInSource) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"shuffle(sequence(1, 10))[1] as m"})
                         .map({"m as mm", "shuffle(sequence(1, m + 1)) as arr"})
                         .map({"mm as a", "arr[1] as b", "arr[1] as c"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({R"(shuffle("any"())[1] as m)"})
                     .project({"m", "shuffle(sequence(1, m + 1)) as arr"})
                     .project({"m as a", "arr[1] as b", "arr[1] as c"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Non-determinism inside a lambda body, reused. The projection is materialized
// once and shared.
TEST_F(NonDeterministicTest, reusedLambdaWithNonDeterministic) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"transform(sequence(1, 3), x -> rand()) as a"})
                         .map({"a as p", "a as q"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({R"(transform("any"(), x -> rand()) as a)"})
                     .project({"a as p", "a as q"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A single-use of non-deterministic lambda needs no shared materialization.
TEST_F(NonDeterministicTest, singleUseNonDeterministicLambda) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .map({"transform(sequence(1, 3), x -> rand()) as a"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({R"(transform("any"(), x -> rand()) as a)"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Two parallel non-deterministic projections. Each is materialized once
// independently.
TEST_F(NonDeterministicTest, lambdaTrappedAndPerRowSource) {
  auto logicalPlan =
      lp::PlanBuilder()
          .values({makeSource()})
          .map({"uuid() as u", "transform(sequence(1, 3), x -> rand()) as a"})
          .map({"u as u1", "u as u2", "a as a1", "a as a2"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchValues()
          .project({"uuid() as u", R"(transform("any"(), x -> rand()) as a)"})
          .project({"u as u1", "u as u2", "a as a1", "a as a2"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A UNION ALL where a filter on a constant-valued column eliminates one leg.
// The surviving leg has a reused non-deterministic value materialized once at a
// kept boundary.
TEST_F(NonDeterministicTest, unionCollapsePreservesSingleDraw) {
  lp::PlanBuilder::Context ctx;
  auto leftLeg = lp::PlanBuilder(ctx)
                     .values({makeSource()})
                     .map({"cast(uuid() as varchar) as u", "1 as tag"})
                     .map({"u as a", "u as b", "tag"});
  auto rightLeg = lp::PlanBuilder(ctx)
                      .values({makeSource()})
                      .map({"'x' as a", "'x' as b", "2 as tag"});

  auto logicalPlan =
      leftLeg.unionAll(rightLeg).filter("tag = 1").map({"a", "b"}).build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .project({"cast(uuid() as varchar) as u"})
                     .project({"u as a", "u as b"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A reused non-deterministic value in a subquery that is joined with another
// table. The scalar layer must materialize the non-deterministic value BELOW
// the join.
TEST_F(NonDeterministicTest, reusedInJoinedSubquery) {
  lp::PlanBuilder::Context ctx;
  auto logicalPlan = lp::PlanBuilder(ctx)
                         .values({makeSource()})
                         .map({"k", "uuid() as r"})
                         .crossJoin(lp::PlanBuilder(ctx).values({makeSource()}))
                         .map({"r as a", "r as b"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  // The non-deterministic projection must be materialized below the join,
  // scoped to the left table, not above the join.
  auto rightSide = matchValues().build();
  auto matcher = matchValues()
                     .project({"uuid() as r"})
                     .nestedLoopJoin(rightSide)
                     .project({"r as a", "r as b"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Two reused non-deterministic sources from different sides of a join are each
// materialized once, both below the join.
TEST_F(NonDeterministicTest, reusedSourcesFromBothJoinSides) {
  lp::PlanBuilder::Context ctx;
  auto logicalPlan =
      lp::PlanBuilder(ctx)
          .values({makeSource()})
          .map({"uuid() as left_r"})
          .crossJoin(
              lp::PlanBuilder(ctx)
                  .values({makeSource()})
                  .map({"uuid() as right_r"}))
          .map({"left_r as a", "left_r as b", "right_r as c", "right_r as d"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto rightSide = matchValues().project({"uuid() as right_r"}).build();
  auto matcher =
      matchValues()
          .project({"uuid() as left_r"})
          .nestedLoopJoin(rightSide)
          .project(
              {"left_r as a", "left_r as b", "right_r as c", "right_r as d"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// The window's argument is an inline non-deterministic expression. The LIMIT
// sits below the window and must stay below: hoisting it above the window
// would make count() aggregate over all rows instead of the 3 kept rows.
TEST_F(NonDeterministicTest, ndInlineWindowArgOverLimit) {
  auto logicalPlan = lp::PlanBuilder()
                         .values({makeSource()})
                         .limit(3)
                         .map({"count(rand()) over (partition by k) as c"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchValues()
                     .limit()
                     .project({"k", "rand()"})
                     .window()
                     .project({"c"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A window whose argument references another window's output, in a projection
// that is also non-deterministic. The two windows must be in separate Window
// nodes.
TEST_F(NonDeterministicTest, ndWindowOnWindow) {
  addTable("t");
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto logicalPlan =
      lp::PlanBuilder(ctx)
          .tableScan("t", {"k", "v"})
          .map({"row_number() over (partition by k order by v) as w", "k"})
          .map({"sum(w) over (partition by k order by k) as s", "rand() as r"})
          .build();

  core::PlanNodePtr plan;
  ASSERT_NO_THROW({ plan = toSingleNodePlan(logicalPlan); });

  auto matcher =
      matchScan("t")
          .window({"row_number() OVER (PARTITION BY k ORDER BY v) as w"})
          .project({"w", "k"})
          .window({"sum(w) OVER (PARTITION BY k) as s"})
          .project({"s", "rand()"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
