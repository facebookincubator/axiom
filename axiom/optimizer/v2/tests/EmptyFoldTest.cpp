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

#include <gtest/gtest.h>
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/v2/EmitPass.h"
#include "axiom/optimizer/v2/FoldEmptyInputsPass.h"
#include "axiom/optimizer/v2/Optimize.h"
#include "velox/expression/Expr.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

using namespace facebook::velox;

class EmptyFoldTest : public optimizer::test::QueryTestBase {
 public:
  EmptyFoldTest() {
    useV2_ = true;
  }

 protected:
  struct TestCase {
    const char* name;
    const char* query;
    std::shared_ptr<core::PlanMatcher> matcher;
  };

  void addSingleColumnTables() {
    testConnector_->addTable("t", ROW({"a"}, BIGINT()));
    testConnector_->addTable("u", ROW({"x"}, BIGINT()));
  }

  void addKnownEmptyJoinTables() {
    testConnector_->addTable("t", ROW({"a"}, BIGINT()));
    testConnector_->addTable("empty_u", ROW({"x"}, BIGINT()))
        ->setKnownEmpty(true);
  }

  void initializeV2Context() {
    allocator_ = std::make_unique<HashStringAllocator>(&optimizerPool());
    v2Context_ = std::make_unique<QueryGraphContext>(*allocator_);
    queryCtx() = v2Context_.get();
    builder_ = std::make_unique<Builder>();
    veloxQueryCtx_ = getQueryCtx();
    evaluator_ = std::make_unique<exec::SimpleExpressionEvaluator>(
        veloxQueryCtx_.get(), &optimizerPool());
    session_ = std::make_unique<OptimizerSession>(
        veloxQueryCtx_->queryId(),
        "test",
        optimizerOptions_,
        connector::ConnectorProperties{});
    options_.queryId = veloxQueryCtx_->queryId();
    options_.numWorkers = 1;
    options_.numDrivers = 1;
  }

  void resetV2Context() {
    session_.reset();
    evaluator_.reset();
    veloxQueryCtx_.reset();
    builder_.reset();
    queryCtx() = nullptr;
    v2Context_.reset();
    allocator_.reset();
  }

  void TearDown() override {
    resetV2Context();
    QueryTestBase::TearDown();
  }

  core::PlanNodePtr emitFoldedPlan(
      NodeCP node,
      const ColumnVector& outputColumns) {
    std::vector<std::string> outputNames;
    outputNames.reserve(outputColumns.size());
    for (ColumnCP column : outputColumns) {
      outputNames.push_back(column->outputName());
    }

    ScanHandleCache scanHandles;

    auto emitted = EmitPass::run(
        FoldEmptyInputsPass::run(node, *builder_),
        outputColumns,
        outputNames,
        *session_,
        *evaluator_,
        scanHandles,
        options_);
    VELOX_CHECK_EQ(emitted.fragments.size(), 1);
    return emitted.fragments.front().fragment.planNode;
  }

  std::unique_ptr<HashStringAllocator> allocator_;
  std::unique_ptr<QueryGraphContext> v2Context_;
  std::unique_ptr<Builder> builder_;
  std::shared_ptr<core::QueryCtx> veloxQueryCtx_;
  std::unique_ptr<exec::SimpleExpressionEvaluator> evaluator_;
  std::unique_ptr<OptimizerSession> session_;
  MultiFragmentPlan::Options options_;
};

TEST_F(EmptyFoldTest, zeroLimitOverEmpty) {
  addSingleColumnTables();

  const std::vector<TestCase> testCases{
      {
          "equi",
          "SELECT * FROM t, (SELECT * FROM u LIMIT 0) s WHERE a = x",
          core::PlanMatcherBuilder{}.values().build(),
      },
      {
          "cross",
          "SELECT * FROM t CROSS JOIN (SELECT * FROM u LIMIT 0) s",
          core::PlanMatcherBuilder{}.values().build(),
      },
      {
          "unaryOperators",
          "SELECT a + 1 FROM (SELECT * FROM t LIMIT 0) s "
          "WHERE a > 10 ORDER BY 1 LIMIT 5",
          core::PlanMatcherBuilder{}.values().build(),
      },
      {
          "window",
          "SELECT row_number() OVER (ORDER BY a) FROM "
          "(SELECT * FROM t LIMIT 0) s",
          core::PlanMatcherBuilder{}.values().build(),
      },
      {
          "groupBy",
          "SELECT a, count(*) FROM (SELECT * FROM t LIMIT 0) s GROUP BY a",
          core::PlanMatcherBuilder{}.values().build(),
      },
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    SCOPED_TRACE(testCase.query);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(testCase.query, kTestConnectorId)),
        testCase.matcher);
  }
}

TEST_F(EmptyFoldTest, emptyScan) {
  testConnector_->addTable("empty_t", ROW({"a"}, BIGINT()))
      ->setKnownEmpty(true);

  const auto query = "SELECT a FROM empty_t";
  SCOPED_TRACE(query);
  const auto matcher = core::PlanMatcherBuilder{}.values().build();
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(query, kTestConnectorId)), matcher);
}

TEST_F(EmptyFoldTest, notFoldableScans) {
  int numEstimateStatsCalls{0};
  auto table = testConnector_->addTable("not_foldable_t", ROW({"a"}, BIGINT()));
  table->setFilteredStats(connector::FilteredTableStats{.numRows = 0});

  // A zero-row estimate without an exact empty guarantee is not foldable.
  {
    const auto query = "SELECT a FROM not_foldable_t";
    SCOPED_TRACE("zeroRowStatsWithoutKnownEmpty");
    SCOPED_TRACE(query);
    table->setKnownEmpty(false);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(query, kTestConnectorId)),
        matchScan("not_foldable_t").build());
  }

  // Disabling filtered stats prevents connector-proven emptiness from folding.
  {
    const auto query = "SELECT a FROM not_foldable_t";
    SCOPED_TRACE("filteredStatsDisabled");
    SCOPED_TRACE(query);
    table->setKnownEmpty(true);
    optimizerOptions_.useFilteredTableStats = false;
    testConnector_->setOnEstimateStats(
        [&](const auto&) { ++numEstimateStatsCalls; });
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(query, kTestConnectorId)),
        matchScan("not_foldable_t").build());
  }

  EXPECT_EQ(numEstimateStatsCalls, 0);
}

TEST_F(EmptyFoldTest, nonInnerJoinsOverEmpty) {
  addKnownEmptyJoinTables();

  const std::vector<TestCase> sqlTestCases{
      {
          "leftEmptyRight",
          "SELECT t.a, empty_u.x FROM t LEFT JOIN empty_u "
          "ON t.a = empty_u.x",
          matchScan("t").project({"a", "null"}).build(),
      },
      {
          "rightEmptyLeft",
          "SELECT empty_u.x, t.a FROM empty_u RIGHT JOIN t "
          "ON empty_u.x = t.a",
          matchScan("t").project({"null", "a"}).build(),
      },
      {
          "fullEmptyRight",
          "SELECT t.a, empty_u.x FROM t FULL JOIN empty_u "
          "ON t.a = empty_u.x",
          matchScan("t").project({"a", "null"}).build(),
      },
      {
          "leftEmptyPreservedSide",
          "SELECT empty_u.x, t.a FROM empty_u LEFT JOIN t "
          "ON empty_u.x = t.a",
          matchValues().build(),
      },
      {
          "rightEmptyPreservedSide",
          "SELECT t.a, empty_u.x FROM t RIGHT JOIN empty_u "
          "ON t.a = empty_u.x",
          matchValues().build(),
      },
      {
          "fullBothEmpty",
          "SELECT e1.x, e2.x FROM empty_u e1 FULL JOIN empty_u e2 "
          "ON e1.x = e2.x",
          matchValues()
              .aliases({"e1_x", "e2_x"})
              .project({"e1_x", "e2_x"})
              .build(),
      },
      {
          "semiEmptyFilteringSide",
          "SELECT a FROM t WHERE EXISTS "
          "(SELECT 1 FROM empty_u WHERE empty_u.x = t.a)",
          matchValues().build(),
      },
      {
          "antiEmptyFilteringSide",
          "SELECT a FROM t WHERE NOT EXISTS "
          "(SELECT 1 FROM empty_u WHERE empty_u.x = t.a)",
          matchScan("t").build(),
      },
      {
          "countingLeftSemiFilter",
          "SELECT a FROM t INTERSECT ALL SELECT x FROM empty_u",
          matchValues().build(),
      },
      {
          "leftSemiProject",
          "SELECT a IN (SELECT x FROM empty_u) FROM t",
          matchScan("t").project({"false"}).build(),
      },
      {
          "countingAnti",
          "SELECT a FROM t EXCEPT ALL SELECT x FROM empty_u",
          matchScan("t").build(),
      },
  };

  for (const auto& testCase : sqlTestCases) {
    SCOPED_TRACE(testCase.name);
    SCOPED_TRACE(testCase.query);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(testCase.query, kTestConnectorId)),
        testCase.matcher);
  }

  // These join forms are optimizer IR variants that are not produced by the
  // SQL queries above, so build them directly.
  struct IrTestCase {
    const char* name;
    core::JoinType joinType;
    bool hasMarkColumn;
  };

  const std::vector<IrTestCase> irTestCases{
      {
          "rightSemiFilter",
          core::JoinType::kRightSemiFilter,
          false,
      },
      {
          "rightSemiProject",
          core::JoinType::kRightSemiProject,
          true,
      },
  };

  initializeV2Context();

  auto makeNonEmptyValues = [&](ColumnCP column) {
    std::vector<Variant> values{static_cast<int64_t>(1)};
    std::vector<Variant> rows;
    rows.push_back(Variant::row(std::move(values)));
    return builder_->makeValues(
        /*source=*/nullptr,
        registerVariant(Variant::array(std::move(rows))),
        {column});
  };

  for (const auto& testCase : irTestCases) {
    SCOPED_TRACE(testCase.name);
    ColumnCP leftColumn =
        Column::create("left", Value(queryCtx()->toType(BIGINT())));
    ColumnCP rightColumn =
        Column::create("r", Value(queryCtx()->toType(BIGINT())));
    ColumnVector outputColumns{rightColumn};
    if (testCase.hasMarkColumn) {
      outputColumns.push_back(
          Column::create("mark", Value(queryCtx()->toType(BOOLEAN()))));
    }

    NodeCP left = builder_->makeEmptyValues({leftColumn});
    NodeCP right = makeNonEmptyValues(rightColumn);

    const auto plan = emitFoldedPlan(
        builder_->make<Join>(
            {left,
             right,
             testCase.joinType,
             ExprVector{},
             ExprVector{},
             ExprVector{},
             /*nullAware=*/false,
             /*nullAsValue=*/false,
             outputColumns}),
        outputColumns);
    const auto matcher = testCase.hasMarkColumn
        ? matchValues().project({rightColumn->outputName(), "false"}).build()
        : matchValues().build();
    EXPECT_TRUE(matcher->match(plan)) << plan->toString(true, true);
  }
}

TEST_F(EmptyFoldTest, aggregationsOverEmpty) {
  testConnector_->addTable("empty_t", ROW({"a"}, BIGINT()))
      ->setKnownEmpty(true);

  const std::vector<TestCase> testCases{
      {
          "globalCount",
          "SELECT count(*) FROM empty_t",
          core::PlanMatcherBuilder{}
              .values()
              .singleAggregation({}, {"count(*)"})
              .build(),
      },
      {
          "globalSum",
          "SELECT sum(a) FROM empty_t",
          core::PlanMatcherBuilder{}
              .values()
              .singleAggregation({}, {"sum(a)"})
              .build(),
      },
      {
          "globalExpressionSum",
          "SELECT sum(a + 1) FROM empty_t",
          core::PlanMatcherBuilder{}
              .values()
              .aliases({"a"})
              .project({"a + 1 as p"})
              .singleAggregation({}, {"sum(p)"})
              .build(),
      },
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    SCOPED_TRACE(testCase.query);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(testCase.query, kTestConnectorId)),
        testCase.matcher);
  }

  {
    const auto query = "SELECT a, count(*) FROM empty_t GROUP BY a";
    SCOPED_TRACE("estimateQueryStats");
    SCOPED_TRACE(query);
    auto logicalPlan = parseSelect(query, kTestConnectorId);
    initializeV2Context();
    connector::SchemaResolver schemaResolver{
        connector::ConnectorMetadataRegistry::global()};
    const auto stats =
        Optimizer(*logicalPlan, schemaResolver, *session_, *evaluator_)
            .estimateQueryStats();
    ASSERT_TRUE(stats.cardinality.has_value());
    EXPECT_EQ(*stats.cardinality, 0);
  }
}

TEST_F(EmptyFoldTest, groupingSetsOverEmpty) {
  testConnector_->addTable("empty_t", ROW({"a", "b"}, {BIGINT(), BIGINT()}))
      ->setKnownEmpty(true);

  const std::vector<TestCase> testCases{
      {
          "withoutGlobalSet",
          "SELECT a, b, count(*) FROM empty_t "
          "GROUP BY GROUPING SETS ((a), (b))",
          matchValues()
              .aliases({"a", "b", "gid", "count"})
              .project({"a", "b", "count"})
              .build(),
      },
      {
          "withGlobalSet",
          "SELECT a, count(*) FROM empty_t GROUP BY GROUPING SETS ((a), ())",
          core::PlanMatcherBuilder{}
              .values()
              .singleAggregation()
              .aliases({"a", "gid", "count"})
              .project({"a", "count"})
              .build(),
      },
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    SCOPED_TRACE(testCase.query);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(testCase.query, kTestConnectorId)),
        testCase.matcher);
  }
}

TEST_F(EmptyFoldTest, unnestOverEmpty) {
  testConnector_->addTable("empty_t", ROW({"items"}, {ARRAY(BIGINT())}))
      ->setKnownEmpty(true);

  const auto query =
      "SELECT item FROM empty_t CROSS JOIN UNNEST(items) AS u(item)";
  SCOPED_TRACE(query);
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(query, kTestConnectorId)),
      matchValues().build());
}

TEST_F(EmptyFoldTest, unionsOverEmpty) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BIGINT()}));
  testConnector_->addTable("u", ROW({"x"}, BIGINT()));
  testConnector_
      ->addTable(
          "empty_u", ROW({"x", "y", "z"}, {BIGINT(), BIGINT(), BIGINT()}))
      ->setKnownEmpty(true);
  testConnector_->addTable("v", ROW({"y"}, BIGINT()));

  const std::vector<TestCase> testCases{
      {
          "unionAllEmptySecondInput",
          "SELECT a FROM t "
          "UNION ALL "
          "SELECT x FROM (SELECT * FROM u LIMIT 0) empty_u",
          matchScan("t").project({"a"}).build(),
      },
      {
          "unionAllEmptyFirstInput",
          "SELECT a FROM (SELECT * FROM t LIMIT 0) empty_t "
          "UNION ALL "
          "SELECT x FROM u",
          matchScan("u").project({"x"}).build(),
      },
      {
          "unionAllInputsEmpty",
          "SELECT a FROM (SELECT * FROM t LIMIT 0) empty_t "
          "UNION ALL "
          "SELECT x FROM (SELECT * FROM u LIMIT 0) empty_u",
          matchValues().build(),
      },
      {
          "unionAllRebuildsAfterDroppingEmpty",
          "SELECT a FROM t "
          "UNION ALL "
          "SELECT x FROM empty_u "
          "UNION ALL "
          "SELECT y FROM v",
          matchScan("t")
              .project({"a"})
              .localPartition(
                  {matchScan("v").aliases({"y"}).project({"y"}).build()})
              .build(),
      },
      {
          "unionAllOneNonEmptyRemains",
          "SELECT b, a, b FROM t "
          "UNION ALL "
          "SELECT x, y, z FROM empty_u",
          matchScan("t").project({"b", "a", "b"}).build(),
      },
      {
          "unionDistinctOneNonEmptyRemains",
          "SELECT a FROM (SELECT * FROM t LIMIT 0) empty_t "
          "UNION "
          "SELECT x FROM u",
          matchScan("u").project({"x"}).singleAggregation().build(),
      },
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    SCOPED_TRACE(testCase.query);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(testCase.query, kTestConnectorId)),
        testCase.matcher);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
