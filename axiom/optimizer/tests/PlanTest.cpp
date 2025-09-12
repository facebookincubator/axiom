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

#include <folly/init/Init.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/ParquetTpchTest.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/tests/utils/DfFunctions.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class PlanTest : public test::QueryTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  static void SetUpTestCase() {
    std::string path;
    if (FLAGS_data_path.empty()) {
      gTempDirectory = exec::test::TempDirectoryPath::create();
      path = gTempDirectory->getPath();
      test::ParquetTpchTest::createTables(path);
    } else {
      path = FLAGS_data_path;
      if (FLAGS_create_dataset) {
        test::ParquetTpchTest::createTables(path);
      }
    }

    LocalRunnerTestBase::testDataPath_ = path;
    LocalRunnerTestBase::localFileFormat_ = "parquet";
    LocalRunnerTestBase::SetUpTestCase();

    test::registerDfFunctions();
  }

  static void TearDownTestCase() {
    LocalRunnerTestBase::TearDownTestCase();
    gTempDirectory.reset();
  }

  void SetUp() override {
    QueryTestBase::SetUp();

    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    connector::registerConnector(testConnector_);
  }

  void TearDown() override {
    connector::unregisterConnector(kTestConnectorId);

    QueryTestBase::TearDown();
  }

  void checkSame(
      const lp::LogicalPlanNodePtr& planNode,
      core::PlanNodePtr referencePlan,
      const axiom::runner::MultiFragmentPlan::Options& options = {
          .numWorkers = 4,
          .numDrivers = 4}) {
    VELOX_CHECK_NOT_NULL(planNode);
    VELOX_CHECK_NOT_NULL(referencePlan);

    auto fragmentedPlan = planVelox(planNode, options);
    auto referenceResult = assertSame(referencePlan, fragmentedPlan);

    if (options.numWorkers != 1) {
      auto singleNodePlan = planVelox(
          planNode, {.numWorkers = 1, .numDrivers = options.numDrivers});
      auto singleNodeResult = runFragmentedPlan(singleNodePlan);

      exec::test::assertEqualResults(
          referenceResult.results, singleNodeResult.results);

      if (options.numDrivers != 1) {
        auto singleThreadPlan =
            planVelox(planNode, {.numWorkers = 1, .numDrivers = 1});
        auto singleThreadResult = runFragmentedPlan(singleThreadPlan);

        exec::test::assertEqualResults(
            referenceResult.results, singleThreadResult.results);
      }
    }
  }

  core::PlanNodePtr toSingleNodePlan(
      const lp::LogicalPlanNodePtr& logicalPlan,
      int32_t numDrivers = 1) {
    schema_ = std::make_shared<optimizer::SchemaResolver>();

    auto plan =
        planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = numDrivers})
            .plan;

    EXPECT_EQ(1, plan->fragments().size());
    return plan->fragments().at(0).fragment.planNode;
  }

  static std::shared_ptr<exec::test::TempDirectoryPath> gTempDirectory;

  std::shared_ptr<connector::TestConnector> testConnector_;
};

// static
std::shared_ptr<exec::test::TempDirectoryPath> PlanTest::gTempDirectory =
    nullptr;

auto gte(const std::string& name, int64_t n) {
  return common::test::singleSubfieldFilter(name, exec::greaterThanOrEqual(n));
}

auto lte(const std::string& name, int64_t n) {
  return common::test::singleSubfieldFilter(name, exec::lessThanOrEqual(n));
}

auto between(const std::string& name, int64_t min, int64_t max) {
  return common::test::singleSubfieldFilter(name, exec::between(min, max));
}

auto gt(const std::string& name, double d) {
  return common::test::singleSubfieldFilter(name, exec::greaterThanDouble(d));
}

auto lt(const std::string& name, double d) {
  return common::test::singleSubfieldFilter(name, exec::lessThanDouble(d));
}

// TODO Move this test into its own file.
TEST_F(PlanTest, queryGraph) {
  TypePtr row1 = ROW({{"c1", ROW({{"c1a", INTEGER()}})}, {"c2", DOUBLE()}});
  TypePtr row2 = row1 =
      ROW({{"c1", ROW({{"c1a", INTEGER()}})}, {"c2", DOUBLE()}});
  TypePtr largeRow = ROW(
      {{"c1", ROW({{"c1a", INTEGER()}})},
       {"c2", DOUBLE()},
       {"m1", MAP(INTEGER(), ARRAY(INTEGER()))}});
  TypePtr differentNames =
      ROW({{"different", ROW({{"c1a", INTEGER()}})}, {"c2", DOUBLE()}});

  auto allocator = std::make_unique<HashStringAllocator>(pool_.get());
  auto context = std::make_unique<QueryGraphContext>(*allocator);
  queryCtx() = context.get();

  SCOPE_EXIT {
    queryCtx() = nullptr;
  };

  auto* dedupRow1 = toType(row1);
  auto* dedupRow2 = toType(row2);
  auto* dedupLargeRow = toType(largeRow);
  auto* dedupDifferentNames = toType(differentNames);

  // dedupped complex types make a copy.
  EXPECT_NE(row1.get(), dedupRow1);

  // Identical types get equal pointers.
  EXPECT_EQ(dedupRow1, dedupRow2);

  // Different names differentiate types.
  EXPECT_NE(dedupDifferentNames, dedupRow1);

  // Shared complex substructure makes equal pointers.
  EXPECT_EQ(dedupRow1->childAt(0).get(), dedupLargeRow->childAt(0).get());

  // Identical child types with different names get equal pointers.
  EXPECT_EQ(dedupRow1->childAt(0).get(), dedupDifferentNames->childAt(0).get());

  auto* path = make<Path>()
                   ->subscript("field")
                   ->subscript(123)
                   ->field("f1")
                   ->cardinality();
  auto interned = queryCtx()->toPath(path);
  EXPECT_EQ(interned, path);
  auto* path2 = make<Path>()
                    ->subscript("field")
                    ->subscript(123)
                    ->field("f1")
                    ->cardinality();
  auto interned2 = queryCtx()->toPath(path2);
  EXPECT_EQ(interned2, interned);
}

TEST_F(PlanTest, agg) {
  testConnector_->createTable(
      "numbers", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), VARCHAR()}));

  auto logicalPlan = lp::PlanBuilder()
                         .tableScan(kTestConnectorId, "numbers", {"a", "b"})
                         .aggregate({"a"}, {"sum(b)"})
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        core::PlanMatcherBuilder().tableScan().singleAggregation().build();

    ASSERT_TRUE(matcher->match(plan));
  }
  {
    auto plan = toSingleNodePlan(logicalPlan, 2);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .partialAggregation()
                       .localPartition()
                       .finalAggregation()
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }
}

// Verify that optimizer can handle connectors that do not support filter
// pushdown.
TEST_F(PlanTest, rejectedFilters) {
  const auto mapType = MAP(BIGINT(), DOUBLE());
  testConnector_->createTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), mapType}));

  auto scan = [&]() {
    lp::PlanBuilder::Context ctx(
        kTestConnectorId, getQueryCtx(), test::resolveDfFunction);
    return lp::PlanBuilder(ctx, /* enableCoersions */ true).tableScan("t");
  };

  // SELECT a + 2 FROM t WHERE a > 10.
  {
    auto logicalPlan = scan().filter("a > 10").map({"a + 2"});

    auto plan = toSingleNodePlan(logicalPlan.build());

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("t", ROW("a", BIGINT()))
                       .filter("a > 10")
                       .project()
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  // SELECT a + 2 FROM t WHERE b > 10.
  {
    auto logicalPlan = scan().filter("b > 10").map({"a + 2"});

    auto plan = toSingleNodePlan(logicalPlan.build());

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("t", ROW({"a", "b"}, {BIGINT(), DOUBLE()}))
                       .filter("b > 10")
                       .project()
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  optimizerOptions_.pushdownSubfields = true;

  // SELECT * FROM t WHERE c.x > 10.
  {
    auto logicalPlan =
        scan()
            .map(
                {"make_row_from_map(c, array[1,2,3], array['x', 'y', 'z']) as c"})
            .filter("c.x > 10")
            .map({"c.x + 1", "c.y + 2", "c.z + 3"})
            .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("t", ROW("c", mapType))
                       .filter()
                       .project() // project top-level columns c.x, c.y, c.z
                       .project() // project c.x + 1, c.y + 2, c.z + 3
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  // SELECT 1 FROM t WHERE c.x > 10.
  // -- Filter uses column that's not otherwise used.
  {
    auto logicalPlan =
        scan()
            .map(
                {"make_row_from_map(c, array[1, 2, 3], array['x', 'y', 'z']) as c"})
            .filter("c.x > 10")
            .map({"1"})
            .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("t", ROW("c", mapType))
                       .filter("c[1] > 10")
                       .project({"1"})
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  // SELECT c.y + 1 FROM t WHERE c.x > 10.
  // -- Filter uses subfield that's not otherwise used.
  {
    auto logicalPlan =
        scan()
            .map(
                {"make_row_from_map(c, array[1, 2, 3], array['x', 'y', 'z']) as c"})
            .filter("c.x > 10")
            .map({"c.y + 1"})
            .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("t", ROW("c", mapType))
                       .filter("c[1] > 10")
                       .project() // project top-level column c.y
                       .project() // project c.y + 1
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }
}

TEST_F(PlanTest, specialFormConstantFold) {
  testConnector_->createTable(
      "numbers", ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), BIGINT()}));

  struct TestCase {
    std::string expression;
    std::optional<std::string> expectedExpression;
  };

  std::vector<TestCase> filterTestCases = {
      {"1 in (1, 2, 3)", std::nullopt},
      {"if(2 > 1, true, false)", std::nullopt},
      {"true or 2 < 1", std::nullopt},
      {"cast(1 as BOOLEAN)", std::nullopt},
      {"try_cast(1 as BOOLEAN)", std::nullopt},
      {"coalesce(cast(null as boolean), true)", std::nullopt},
      {"case when 2 > 1 then true else false end", std::nullopt},
      {"if(1 > 2, 3 + 4, 4 + 5) > 8", std::nullopt},
      {"1 in (2, 3)", "false"},
      {"if(2 < 1, true, false)", "false"},
      {"1 > 2 or false", "false"},
      {"cast(0 as BOOLEAN)", "false"},
      {"try_cast(1 as BIGINT) > 4", "false"},
      {"try(10 / 1) > 4", std::nullopt},
      {"coalesce(cast(null as boolean), false)", "false"},
      {"case when 1 > 2 then true else false end", "false"},
      {"try_cast('a' as BIGINT) > 4", "null"},
      {"if(a > b, 1 + 2, c) > b + 3", "if(a > b, 3, c) > b + 3"},
      {"if(a > b, 1 + 2, 3 + 4) > b + 3", "if(a > b, 3, 7) > b + 3"},
  };

  for (const auto& testCase : filterTestCases) {
    SCOPED_TRACE("Filter: " + testCase.expression);
    auto logicalPlan = lp::PlanBuilder()
                           .tableScan(kTestConnectorId, "numbers")
                           .filter(testCase.expression)
                           .map({"a + 2"})
                           .build();

    auto matcher = testCase.expectedExpression.has_value()
        ? core::PlanMatcherBuilder()
              .tableScan()
              .filter(testCase.expectedExpression.value())
              .project()
              .build()
        : core::PlanMatcherBuilder().tableScan().project().build();

    auto plan = toSingleNodePlan(logicalPlan);
    ASSERT_TRUE(matcher->match(plan));
  }

  std::vector<TestCase> prjectTestCases = {
      {"if(2 > 1, 1, 0)", "1"},
      {"if(2 < 1, 1, 0)", "0"},
      {"cast(1 as BOOLEAN)", "true"},
      {"cast(0 as BOOLEAN)", "false"},
      {"try_cast(1 as BOOLEAN)", "true"},
      {"coalesce(cast(null as bigint), 42)", "42"},
      {"case when 2 > 1 then 100 else 200 end", "100"},
      {"case when 1 > 2 then 100 else 200 end", "200"},
      {"1 + 2", "3"},
      {"10 - 5", "5"},
      {"3 * 4", "12"},
      {"15 / 3", "5"},
      {"2 + 3 * 4", "14"},
      {"(2 + 3) * 4", "20"},
      {"if(a > b, 1 + 2, c)", "if(a > b, 3, c)"},
      {"if(a > b, 1 + 2, 3 + 4)", "if(a > b, 3, 7)"},
      {"case when a > 0 then 5 + 5 else b + 1 end",
       "case when a > 0 then 10 else b + 1 end"},
      {"try(10 / 1)", "10"},
      {"try_cast(1 as BIGINT)", "1"},
      {"coalesce(1 + 1, a)", "coalesce(2, a)"},
      {"coalesce(cast(null as bigint), 5 * 2)", "10"},
  };

  for (const auto& testCase : prjectTestCases) {
    SCOPED_TRACE("Expression: " + testCase.expression);
    auto logicalPlan = lp::PlanBuilder()
                           .tableScan(kTestConnectorId, "numbers")
                           .project({testCase.expression, "a", "b"})
                           .build();

    ASSERT_TRUE(testCase.expectedExpression.has_value());
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .project({testCase.expectedExpression.value(), "a", "b"})
                       .build();

    auto plan = toSingleNodePlan(logicalPlan);
    ASSERT_TRUE(matcher->match(plan));
  }
}

TEST_F(PlanTest, inList) {
  testConnector_->createTable(
      "numbers", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), VARCHAR()}));

  auto scan = [&]() {
    lp::PlanBuilder::Context context(kTestConnectorId, getQueryCtx());
    return lp::PlanBuilder(context).tableScan("numbers");
  };

  auto scanMatcher = [&]() { return core::PlanMatcherBuilder().tableScan(); };

  {
    auto logicalPlan = scan().filter("1 in (1, 2, 3)").map({"a + 2"}).build();

    auto matcher = scanMatcher().project().build();

    auto plan = toSingleNodePlan(logicalPlan);
    ASSERT_TRUE(matcher->match(plan));
  }
  {
    auto logicalPlan = scan().filter("4 in (1, 2, 3)").map({"a + 2"}).build();

    auto matcher = scanMatcher().filter("false").project().build();

    auto plan = toSingleNodePlan(logicalPlan);
    ASSERT_TRUE(matcher->match(plan));
  }
  {
    auto logicalPlan =
        scan().filter("a in (1, 2, 3) and b > 1.2").map({"a + 2"}).build();

    auto matcher =
        scanMatcher().filter("a in (1, 2, 3) and b > 1.2").project().build();

    auto plan = toSingleNodePlan(logicalPlan);
    ASSERT_TRUE(matcher->match(plan));
  }
}

TEST_F(PlanTest, multipleConnectors) {
  auto extraConnector = std::make_shared<connector::TestConnector>("extra");
  connector::registerConnector(extraConnector);
  SCOPE_EXIT {
    connector::unregisterConnector("extra");
  };

  testConnector_->createTable("table1", ROW({"a"}, {BIGINT()}));
  extraConnector->createTable("table2", ROW({"b"}, {BIGINT()}));

  lp::PlanBuilder::Context context(kTestConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("table1")
          .join(
              lp::PlanBuilder(context).tableScan("extra", "table2"),
              "a = b",
              lp::JoinType::kInner)
          .build();
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      core::PlanMatcherBuilder()
          .tableScan("table1")
          .hashJoin(core::PlanMatcherBuilder().tableScan("table2").build())
          .build();

  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(PlanTest, filterToJoinEdge) {
  auto nationType = ROW({"n_regionkey"}, {BIGINT()});
  auto regionType = ROW({"r_regionkey"}, {BIGINT()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = connector::getConnector(connectorId);

  lp::PlanBuilder::Context context;
  auto logicalPlan = lp::PlanBuilder(context)
                         .tableScan(connectorId, "nation", nationType->names())
                         .crossJoin(lp::PlanBuilder(context).tableScan(
                             connectorId, "region", regionType->names()))
                         .filter("n_regionkey + 1 = r_regionkey + 1")
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
                       .project()
                       .hashJoin(core::PlanMatcherBuilder()
                                     .tableScan("nation")
                                     .project()
                                     .build())
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto referencePlan = exec::test::PlanBuilder(planNodeIdGenerator)
                           .tableScan("nation", nationType)
                           .nestedLoopJoin(
                               exec::test::PlanBuilder(planNodeIdGenerator)
                                   .tableScan("region", regionType)
                                   .planNode(),
                               {"n_regionkey", "r_regionkey"},
                               core::JoinType::kInner)
                           .filter("n_regionkey + 1 = r_regionkey + 1")
                           .planNode();

  checkSame(logicalPlan, referencePlan);

  // Add non-deterministic always-true filters to both branches of the join and
  // after the join.
  logicalPlan =
      lp::PlanBuilder(context)
          .tableScan(connectorId, "nation", nationType->names())
          .filter("rand() < 2.0")
          .crossJoin(lp::PlanBuilder(context)
                         .tableScan(connectorId, "region", regionType->names())
                         .filter("rand() < 3.0"))
          .filter("n_regionkey + 1 = r_regionkey + 1 and rand() < 4.0")
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       // TODO Why is this filter not pushed down into scan?
                       .filter("rand() < 2.0")
                       .project()
                       .hashJoin(core::PlanMatcherBuilder()
                                     .tableScan("region")
                                     .filter("rand() < 3.0")
                                     .project()
                                     .build())
                       .filter("rand() < 4.0")
                       .project()
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  checkSame(logicalPlan, referencePlan);
}

TEST_F(PlanTest, filterImport) {
  auto ordersType = ROW({"o_custkey", "o_totalprice"}, {BIGINT(), DOUBLE()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = connector::getConnector(connectorId);

  auto logicalPlan = lp::PlanBuilder()
                         .tableScan(connectorId, "orders", ordersType->names())
                         .aggregate({"o_custkey"}, {"sum(o_totalprice) as a0"})
                         .filter("o_custkey < 100 and a0 > 200.0")
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("orders")
                       .singleAggregation()
                       .filter("a0 > 200.0")
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("orders", ordersType)
          .singleAggregation({"o_custkey"}, {"sum(o_totalprice)"})
          .filter("o_custkey < 100 and a0 > 200.0")
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(PlanTest, filterBreakup) {
  const char* filterText =
      "        (\n"
      "                l_partkey = p_partkey\n"
      "                and p_brand = 'Brand#12'\n"
      "                and p_container like 'SM%'\n"
      "                and l_quantity >= 1.0 and l_quantity <= 1.0 + 10.0\n"
      "                and p_size between 1::int and 5::int\n"
      "                and l_shipmode in ('AIR', 'AIR REG')\n"
      "                and l_shipinstruct = 'DELIVER IN PERSON'\n"
      "        )\n"
      "        or\n"
      "        (\n"
      "                p_partkey = l_partkey\n"
      "                and p_brand = 'Brand#23'\n"
      "                and p_container like 'MED%'\n"
      "                and l_quantity >= 10.0 and l_quantity <= 10.0 + 10.0\n"
      "                and p_size between 1::int and 10::int\n"
      "                and l_shipmode in ('AIR', 'AIR REG')\n"
      "                and l_shipinstruct = 'DELIVER IN PERSON'\n"
      "        )\n"
      "        or\n"
      "        (\n"
      "                p_partkey = l_partkey\n"
      "                and p_brand = 'Brand#34'\n"
      "                and p_container like 'LG%'\n"
      "                and l_quantity >= 20.0 and l_quantity <= 20.0 + 10.0\n"
      "                and p_size between 1::int and 15::int\n"
      "                and l_shipmode in ('AIR', 'AIR REG')\n"
      "                and l_shipinstruct = 'DELIVER IN PERSON'\n"
      "        )\n";

  auto lineitemType = ROW(
      {{"l_partkey", BIGINT()},
       {"l_shipmode", VARCHAR()},
       {"l_shipinstruct", VARCHAR()},
       {"l_extendedprice", DOUBLE()},
       {"l_discount", DOUBLE()},
       {"l_quantity", DOUBLE()}});

  auto partType = ROW(
      {{"p_partkey", BIGINT()},
       {"p_brand", VARCHAR()},
       {"p_container", VARCHAR()},
       {"p_size", INTEGER()}});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = connector::getConnector(connectorId);

  lp::PlanBuilder::Context context;
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan(connectorId, "lineitem", lineitemType->names())
          .crossJoin(lp::PlanBuilder(context).tableScan(
              connectorId, "part", partType->names()))
          .filter(filterText)
          .project({"l_extendedprice * (1.0 - l_discount) as part_revenue"})
          .aggregate({}, {"sum(part_revenue)"})
          .build();

  {
    // Expect the per table filters to be extracted from the OR.
    // TODO Verify remaining filters.
    auto lineitemFilters =
        common::test::SubfieldFiltersBuilder()
            .add("l_shipinstruct", exec::equal("DELIVER IN PERSON"))
            .add(
                "l_shipmode",
                exec::in(std::vector<std::string>{"AIR", "AIR REG"}))
            .build();

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder()
            .hiveScan(
                "lineitem",
                std::move(lineitemFilters),
                // TODO Fix this plan. Compact the filter to between(1, 30) and
                // push down as subfield filter.
                "\"or\"(l_quantity >= 20.0 AND l_quantity <= 30.0, "
                "   \"or\"(l_quantity >= 1.0 AND l_quantity <= 11.0, "
                "          l_quantity >= 10.0 AND l_quantity <= 20.0))")
            .hashJoin(
                core::PlanMatcherBuilder()
                    .hiveScan(
                        "part",
                        {},
                        "\"or\"(\"and\"(p_size between 1 and 15, (p_brand = 'Brand#34' AND p_container LIKE 'LG%')), "
                        "   \"or\"(\"and\"(p_size between 1 and 5, (p_brand = 'Brand#12' AND p_container LIKE 'SM%')), "
                        "          \"and\"(p_size between 1 and 10, (p_brand = 'Brand#23' AND p_container LIKE 'MED%'))))")
                    .build())
            .filter()
            .project()
            .singleAggregation()
            .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  auto referenceBuilder = std::make_unique<exec::test::TpchQueryBuilder>(
      dwio::common::FileFormat::PARQUET);
  referenceBuilder->initialize(LocalRunnerTestBase::testDataPath_);

  auto referencePlan = referenceBuilder->getQueryPlan(19).plan;

  checkSame(logicalPlan, referencePlan);
}

TEST_F(PlanTest, unionAll) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = connector::getConnector(connectorId);

  const std::vector<std::string>& names = nationType->names();

  lp::PlanBuilder::Context ctx;
  auto t1 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey < 11");
  auto t2 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey > 13");

  auto logicalPlan = t1.unionAll(t2)
                         .project({"n_regionkey + 1 as rk"})
                         .filter("rk % 3 = 1")
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder()
            .hiveScan(
                "nation", lte("n_nationkey", 10), "(n_regionkey + 1) % 3 = 1")
            .localPartition(core::PlanMatcherBuilder()
                                .hiveScan(
                                    "nation",
                                    gte("n_nationkey", 14),
                                    "(n_regionkey + 1) % 3 = 1")
                                .project()
                                .build())
            .project()
            .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan = exec::test::PlanBuilder(pool_.get())
                           .tableScan("nation", nationType)
                           .filter("n_nationkey < 11 or n_nationkey > 13")
                           .project({"n_regionkey + 1 as rk"})
                           .filter("rk % 3 = 1")
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(PlanTest, unionJoin) {
  auto partType = ROW({"p_partkey", "p_retailprice"}, {BIGINT(), DOUBLE()});
  auto partSuppType = ROW({"ps_partkey", "ps_availqty"}, {BIGINT(), INTEGER()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = connector::getConnector(connectorId);

  lp::PlanBuilder::Context ctx;
  auto ps1 =
      lp::PlanBuilder(ctx)
          .tableScan(connectorId, "partsupp", {"ps_partkey", "ps_availqty"})
          .filter("ps_availqty < 1000::int")
          .project({"ps_partkey"});

  auto ps2 =
      lp::PlanBuilder(ctx)
          .tableScan(connectorId, "partsupp", {"ps_partkey", "ps_availqty"})
          .filter("ps_availqty > 2000::int")
          .project({"ps_partkey"});

  auto ps3 =
      lp::PlanBuilder(ctx)
          .tableScan(connectorId, "partsupp", {"ps_partkey", "ps_availqty"})
          .filter("ps_availqty between 1200::int and 1400::int")
          .project({"ps_partkey"});

  // The shape of the partsupp union is ps1 union all (ps2 union all ps3). We
  // verify that a stack of multiple set ops works.
  auto psu2 = ps2.unionAll(ps3);

  auto p1 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "part", {"p_partkey", "p_retailprice"})
                .filter("p_retailprice < 1100.0");

  auto p2 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "part", {"p_partkey", "p_retailprice"})
                .filter("p_retailprice > 1200.0");

  auto logicalPlan =
      ps1.unionAll(psu2)
          .join(p1.unionAll(p2), "ps_partkey = p_partkey", lp::JoinType::kInner)
          .aggregate({}, {"sum(1)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder()
            .hiveScan("partsupp", lte("ps_availqty", 999))
            .localPartition(
                core::PlanMatcherBuilder()
                    .hiveScan("partsupp", gte("ps_availqty", 2001))
                    .project()
                    .localPartition(
                        core::PlanMatcherBuilder()
                            .hiveScan(
                                "partsupp", between("ps_availqty", 1200, 1400))
                            .project()
                            .build())
                    .build())
            .hashJoin(
                core::PlanMatcherBuilder()
                    .hiveScan("part", lt("p_retailprice", 1100.0))
                    .localPartition(
                        core::PlanMatcherBuilder()
                            .hiveScan("part", gt("p_retailprice", 1200.0))
                            .project()
                            .build())
                    .build(),
                core::JoinType::kInner)
            .project()
            .singleAggregation()
            .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  auto idGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto referencePlan =
      exec::test::PlanBuilder(idGenerator)
          .tableScan("partsupp", partSuppType)
          .filter(
              "ps_availqty < 1000::int or ps_availqty > 2000::int "
              "or ps_availqty between 1200::int and 1400::int")
          .hashJoin(
              {"ps_partkey"},
              {"p_partkey"},
              exec::test::PlanBuilder(idGenerator)
                  .tableScan("part", partType)
                  .filter("p_retailprice < 1100.0 or p_retailprice > 1200.0")
                  .planNode(),
              "",
              {"p_partkey"})
          .project({"p_partkey"})
          .localPartition({})
          .singleAggregation({}, {"sum(1)"})
          .planNode();

  // Skip distributed run. Problem with local exchange source with
  // multiple inputs.
  checkSame(logicalPlan, referencePlan, {.numWorkers = 1, .numDrivers = 4});
}

TEST_F(PlanTest, intersect) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = connector::getConnector(connectorId);

  const std::vector<std::string>& names = nationType->names();

  lp::PlanBuilder::Context ctx;
  auto t1 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey < 21")
                .project({"n_nationkey", "n_regionkey"});
  auto t2 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey > 11")
                .project({"n_nationkey", "n_regionkey"});
  auto t3 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey > 12")
                .project({"n_nationkey", "n_regionkey"});

  auto logicalPlan =
      lp::PlanBuilder(ctx)
          .setOperation(lp::SetOperation::kIntersect, {t1, t2, t3})
          .project({"n_regionkey + 1 as rk"})
          .filter("rk % 3 = 1")
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       // TODO Fix this plan to push down (n_regionkey + 1) % 3
                       // = 1 to all branches of 'intersect'.
                       .hiveScan("nation", gte("n_nationkey", 13))
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .hiveScan("nation", gte("n_nationkey", 12))
                               .hashJoin(
                                   core::PlanMatcherBuilder()
                                       .hiveScan(
                                           "nation",
                                           lte("n_nationkey", 20),
                                           "(n_regionkey + 1) % 3 = 1")
                                       .build(),
                                   core::JoinType::kRightSemiFilter)
                               .build(),
                           core::JoinType::kRightSemiFilter)
                       .singleAggregation()
                       .project()
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan = exec::test::PlanBuilder(pool_.get())
                           .tableScan("nation", nationType)
                           .filter("n_nationkey > 12 and n_nationkey < 21")
                           .project({"n_regionkey + 1 as rk"})
                           .filter("rk % 3 = 1")
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(PlanTest, except) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = connector::getConnector(connectorId);

  const std::vector<std::string>& names = nationType->names();

  lp::PlanBuilder::Context ctx;
  auto t1 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey < 21")
                .project({"n_nationkey", "n_regionkey"});
  auto t2 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey > 16")
                .project({"n_nationkey", "n_regionkey"});
  auto t3 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey <= 5")
                .project({"n_nationkey", "n_regionkey"});

  auto logicalPlan = lp::PlanBuilder(ctx)
                         .setOperation(lp::SetOperation::kExcept, {t1, t2, t3})
                         .project({"n_nationkey", "n_regionkey + 1 as rk"})
                         .filter("rk % 3 = 1")
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder()
            .hiveScan(
                "nation", lte("n_nationkey", 20), "(n_regionkey + 1) % 3 = 1")
            .hashJoin(
                core::PlanMatcherBuilder()
                    // TODO Fix this plan to push down (n_regionkey + 1) % 3 = 1
                    // to all branches of 'except'.
                    .hiveScan("nation", gte("n_nationkey", 17))
                    .build(),
                core::JoinType::kAnti)
            .hashJoin(
                core::PlanMatcherBuilder()
                    .hiveScan("nation", lte("n_nationkey", 5))
                    .build(),
                core::JoinType::kAnti)
            .singleAggregation()
            .project()
            .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan = exec::test::PlanBuilder(pool_.get())
                           .tableScan("nation", nationType)
                           .filter("n_nationkey > 5 and n_nationkey <= 16")
                           .project({"n_nationkey", "n_regionkey + 1 as rk"})
                           .filter("rk % 3 = 1")
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

// Tests that value nodes can have complex literal types.
TEST_F(PlanTest, valuesComplex) {
  auto rowVector = makeRowVector({
      makeArrayVector<StringView>({{"nation1.0", "nation1.1"}, {"nation2"}}),
      makeMapVectorFromJson<int32_t, int64_t>({"{1: 10, 2: 20}", "{3: 30}"}),
      makeRowVector({
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector<StringView>({"n1", "n2"}),
      }),
  });

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = connector::getConnector(connectorId);

  lp::PlanBuilder::Context ctx{connectorId};
  auto logicalPlan = lp::PlanBuilder(ctx).values({rowVector}).build();
  auto plan = toSingleNodePlan(logicalPlan);

  auto expectedType = ROW({
      ARRAY(VARCHAR()),
      MAP(INTEGER(), BIGINT()),
      ROW({BIGINT(), VARCHAR()}),
  });

  auto matcher = core::PlanMatcherBuilder().values(expectedType).build();
  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(PlanTest, values) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const std::vector<std::string>& names = nationType->names();

  auto rowVector = makeRowVector(
      names,
      {
          makeFlatVector<int64_t>({10, 20, 30}),
          makeFlatVector<int64_t>({1, 2, 3}),
          makeFlatVector<StringView>({"nation1", "nation2", "nation3"}),
          makeFlatVector<StringView>({"comment1", "comment2", "comment3"}),
      });

  lp::PlanBuilder::Context ctx{exec::test::kHiveConnectorId};

  {
    auto makeLogicalPlan = [&](const std::string& filter) {
      return lp::PlanBuilder(ctx)
          .values({rowVector})
          .filter(filter)
          .project({"n_nationkey", "n_regionkey"});
    };

    auto logicalPlanExcept =
        lp::PlanBuilder(ctx)
            .setOperation(
                lp::SetOperation::kExcept,
                {
                    makeLogicalPlan("n_nationkey < 21"),
                    makeLogicalPlan("n_nationkey > 16"),
                    makeLogicalPlan("n_nationkey <= 5"),
                })
            .project({"n_nationkey", "n_regionkey + 1 as rk"})
            .filter("rk in (1, 2, 4, 5)")
            .build();

    auto referencePlanExcept =
        exec::test::PlanBuilder(pool_.get())
            .values({rowVector})
            .filter("n_nationkey > 5 and n_nationkey <= 16")
            .project({"n_nationkey", "n_regionkey + 1 as rk"})
            .filter("rk in (1, 2, 4, 5)")
            .planNode();

    checkSame(logicalPlanExcept, referencePlanExcept);
  }

  // In this test, we verify that the optimizer can handle
  // combinations of logical plans with different leaf types (table scan vs
  // values) and that it can generate the correct physical plan for each
  // combination.
  // We check following cases:
  // 1. t1 join t2
  // 2. t1 join (t2 join t3)
  // 3. t1 join (t2 join (t3 join t4))
  // t* can be either table scan or values.
  // We don't check produced plan, only that it results in the same rows as
  // correct exection plan.

  auto makeLogicalPlan = [&](uint8_t leafType,
                             const std::string& filter,
                             const std::string& alias) {
    auto plan = lp::PlanBuilder(ctx);
    if (leafType == 0) {
      plan.tableScan("nation", names);
    } else {
      plan.values({rowVector});
    }
    return plan.filter(filter).project({
        fmt::format("n_nationkey AS {}1", alias),
        fmt::format("n_regionkey AS {}2", alias),
        fmt::format("n_comment AS {}3", alias),
    });
  };

  auto idGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto makePhysicalPlan =
      [&](int leafType, const std::string& filter, const std::string& alias) {
        auto plan = exec::test::PlanBuilder(idGenerator, pool_.get());
        if (leafType == 0) {
          plan.tableScan("nation", nationType);
        } else {
          plan.values({rowVector});
        }
        return plan.filter(filter).project({
            fmt::format("n_nationkey AS {}1", alias),
            fmt::format("n_regionkey AS {}2", alias),
            fmt::format("n_comment AS {}3", alias),
        });
      };

  auto numJoins = 1;
  auto numCombinations = 1 << (numJoins + 1);
  for (int32_t i = 0; i < numCombinations; ++i) {
    const int leafType1 = bits::isBitSet(&i, 0);
    const int leafType2 = bits::isBitSet(&i, 1);

    SCOPED_TRACE(fmt::format("Join: {} x {}", leafType1, leafType2));

    auto logicalPlan =
        makeLogicalPlan(leafType1, "n_regionkey < 3", "x")
            .join(
                makeLogicalPlan(leafType2, "n_regionkey > 1", "y"),
                "x2 = y2",
                lp::JoinType::kInner)
            .project({"x1", "x2", "y3"})
            .build();

    auto referencePlan =
        makePhysicalPlan(leafType1, "n_regionkey < 3", "x")
            .hashJoin(
                {"x2"},
                {"y2"},
                makePhysicalPlan(leafType2, "n_regionkey > 1", "y").planNode(),
                "",
                {"x1", "x2", "y3"})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }

  numJoins = 2;
  numCombinations = 1 << (numJoins + 1);
  for (int32_t i = 0; i < numCombinations; ++i) {
    const int leafType1 = bits::isBitSet(&i, 0);
    const int leafType2 = bits::isBitSet(&i, 1);
    const int leafType3 = bits::isBitSet(&i, 2);

    SCOPED_TRACE(
        fmt::format("Join: {} x {} x {}", leafType1, leafType2, leafType3));

    auto logicalPlan =
        makeLogicalPlan(leafType1, "n_regionkey < 3", "x")
            .join(
                makeLogicalPlan(leafType2, "n_regionkey < 3", "y")
                    .join(
                        makeLogicalPlan(leafType3, "n_regionkey > 1", "z"),
                        "y2 = z2",
                        lp::JoinType::kInner),
                "x2 = y2",
                lp::JoinType::kInner)
            .project({"x1", "x2", "y1", "z3"})
            .build();

    auto referencePlan =
        makePhysicalPlan(leafType1, "n_regionkey < 3", "x")
            .hashJoin(
                {"x2"},
                {"y2"},
                makePhysicalPlan(leafType2, "n_regionkey < 3", "y")
                    .hashJoin(
                        {"y2"},
                        {"z2"},
                        makePhysicalPlan(leafType3, "n_regionkey > 1", "z")
                            .planNode(),
                        "",
                        {"y1", "y2", "z3"})
                    .planNode(),
                "",
                {"x1", "x2", "y1", "z3"})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }

  numJoins = 3;
  numCombinations = 1 << (numJoins + 1);
  for (int32_t i = 0; i < numCombinations; ++i) {
    const int leafType1 = bits::isBitSet(&i, 0);
    const int leafType2 = bits::isBitSet(&i, 1);
    const int leafType3 = bits::isBitSet(&i, 2);
    const int leafType4 = bits::isBitSet(&i, 3);

    SCOPED_TRACE(fmt::format(
        "Join: {} x {} x {} x {}", leafType1, leafType2, leafType3, leafType4));

    auto logicalPlan =
        makeLogicalPlan(leafType1, "n_regionkey < 3", "x")
            .join(
                makeLogicalPlan(leafType2, "n_regionkey < 3", "y")
                    .join(
                        makeLogicalPlan(leafType3, "n_regionkey < 3", "z")
                            .join(
                                makeLogicalPlan(
                                    leafType4, "n_regionkey > 1", "w"),
                                "z2 == w2",
                                lp::JoinType::kInner)
                            .project({"z1", "z2", "w3"}),
                        "y2 == z2",
                        lp::JoinType::kInner)
                    .project({"y1", "y2", "z1", "w3"}),
                "x2 = y2",
                lp::JoinType::kInner)
            .project({"x1", "x2", "y1", "z1", "w3"})
            .build();

    auto referencePlan =
        makePhysicalPlan(leafType1, "n_regionkey < 3", "x")
            .hashJoin(
                {"x2"},
                {"y2"},
                makePhysicalPlan(leafType2, "n_regionkey < 3", "y")
                    .hashJoin(
                        {"y2"},
                        {"z2"},
                        makePhysicalPlan(leafType3, "n_regionkey < 3", "z")
                            .hashJoin(
                                {"z2"},
                                {"w2"},
                                makePhysicalPlan(
                                    leafType4, "n_regionkey > 1", "w")
                                    .planNode(),
                                "",
                                {"z1", "z2", "w3"})
                            .planNode(),
                        "",
                        {"y1", "y2", "z1", "w3"})
                    .planNode(),
                "",
                {"x1", "x2", "y1", "z1", "w3"})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }
}

TEST_F(PlanTest, limitBeforeProject) {
  testConnector_->createTable("t", ROW({"a", "b"}, {INTEGER(), INTEGER()}));
  {
    auto logicalPlan = lp::PlanBuilder{}
                           .tableScan(kTestConnectorId, "t", {"a", "b"})
                           .limit(10)
                           .project({"a + b as c"})
                           .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        core::PlanMatcherBuilder{}.tableScan().limit().project().build();

    EXPECT_TRUE(matcher->match(plan));
  }
  {
    auto logicalPlan = lp::PlanBuilder{}
                           .tableScan(kTestConnectorId, "t", {"a", "b"})
                           .limit(10'000)
                           .project({"a + b as c"})
                           .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        core::PlanMatcherBuilder{}.tableScan().project().limit().build();

    EXPECT_TRUE(matcher->match(plan));
  }
}

TEST_F(PlanTest, limitAfterOrderBy) {
  testConnector_->createTable("t", ROW({"a", "b"}, {INTEGER(), INTEGER()}));
  {
    auto logicalPlan = lp::PlanBuilder{}
                           .tableScan(kTestConnectorId, "t", {"a", "b"})
                           .project({"a + b as c"})
                           .orderBy({"c"})
                           .limit(10)
                           .build();

    auto plan = toSingleNodePlan(logicalPlan);

    // Extra projection is a bug:
    // https://github.com/facebookexperimental/verax/issues/357
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan()
                       .project({"a", "b", "a + b"})
                       .topN(10)
                       .project({"a + b"})
                       .build();

    EXPECT_TRUE(matcher->match(plan));
  }
  {
    auto logicalPlan = lp::PlanBuilder{}
                           .tableScan(kTestConnectorId, "t", {"a", "b"})
                           .project({"a + b as c"})
                           .orderBy({"c"})
                           .limit(10'000)
                           .build();

    auto plan = toSingleNodePlan(logicalPlan);

    // Extra projection is a bug:
    // https://github.com/facebookexperimental/verax/issues/357
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan()
                       .project({"a", "b", "a + b"})
                       .topN(10'000)
                       .project({"a + b"})
                       .build();

    EXPECT_TRUE(matcher->match(plan));
  }
}

TEST_F(PlanTest, parallelCse) {
  testConnector_->createTable(
      "t", ROW({"a", "b", "c"}, {INTEGER(), INTEGER(), INTEGER()}));

  auto logicalPlan =
      lp::PlanBuilder(/* allowCoersions */ true)
          .tableScan(kTestConnectorId, "t", {"a", "b", "c"})
          .map({"a + b + c as x"})
          .map({
              "contains(array[1], cast(if(cast(x as real) < 0, ceil(cast(x as real)), floor(cast(x as real))) as int)) as a",
          })
          .build();

  optimizerOptions_.parallelProjectWidth = 2;
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .parallelProject()
                     .project()
                     .build();

  EXPECT_TRUE(matcher->match(plan));

  logicalPlan =
      lp::PlanBuilder(/* allowCoersions */ true)
          .tableScan(kTestConnectorId, "t", {"a", "b", "c"})
          .with({"a + b as ab"})
          .with({"ab + c as x"})
          .map(
              {"contains(array[1], cast(if(cast(x as real) < 0, ceil(cast(x as real)), floor(cast(x as real))) as int)) as a",
               "ab"})
          .build();
  plan = toSingleNodePlan(logicalPlan);

  matcher = core::PlanMatcherBuilder()
                .tableScan()
                .parallelProject({"a + b", "c"})
                .parallelProject()
                .project()
                .build();

  EXPECT_TRUE(matcher->match(plan));
}

TEST_F(PlanTest, lastProjection) {
  testConnector_->createTable(
      "numbers", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), VARCHAR()}));

  auto logicalPlan = lp::PlanBuilder{}
                         .tableScan(kTestConnectorId, "numbers", {"a", "b"})
                         .aggregate({"a", "b"}, {"count(1)"})
                         .project({"a"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .singleAggregation()
                     .project({"a"})
                     .build();

  ASSERT_TRUE(matcher->match(plan));
}

} // namespace
} // namespace facebook::axiom::optimizer

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
