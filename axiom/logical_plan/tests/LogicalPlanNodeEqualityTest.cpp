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

#include "axiom/logical_plan/LogicalPlanNode.h"

#include <gtest/gtest.h>
#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook::velox;

namespace facebook::axiom::logical_plan {
namespace {

class LogicalPlanNodeEqualityTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  test::VectorMaker maker_{pool_.get()};
};

TEST_F(LogicalPlanNodeEqualityTest, valuesNodeEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto makeNode = [&rows] {
    return PlanBuilder().values({"x", "y"}, rows).planNode();
  };

  EXPECT_EQ(*makeNode(), *makeNode());

  // Different id — still equal because id_ is not compared.
  PlanBuilder::Context ctx;
  ctx.planNodeIdGenerator->next();
  auto differentId = PlanBuilder(ctx).values({"x", "y"}, rows).planNode();
  EXPECT_EQ(*makeNode(), *differentId);

  // Different data.
  std::vector<std::vector<std::string>> differentRows = {
      {"99", "10"}, {"2", "20"}};
  auto differentData =
      PlanBuilder().values({"x", "y"}, differentRows).planNode();
  EXPECT_NE(*makeNode(), *differentData);

  // Different cardinality.
  std::vector<std::vector<std::string>> moreRowsData = {
      {"1", "10"}, {"2", "20"}, {"3", "30"}};
  auto moreRows = PlanBuilder().values({"x", "y"}, moreRowsData).planNode();
  EXPECT_NE(*makeNode(), *moreRows);

  // Variant-based ValuesNode equality.
  auto makeVariantValues = [] {
    return PlanBuilder()
        .values(ROW("a", BIGINT()), {Variant::row({Variant(1L)})})
        .planNode();
  };
  EXPECT_EQ(*makeVariantValues(), *makeVariantValues());

  auto variantValuesDifferent =
      PlanBuilder()
          .values(ROW("a", BIGINT()), {Variant::row({Variant(99L)})})
          .planNode();
  EXPECT_NE(*makeVariantValues(), *variantValuesDifferent);

  // Vector-based ValuesNode equality.
  auto makeVectorValues = [this] {
    auto vec = maker_.rowVector(
        {"a"}, {maker_.flatVectorNullable<int64_t>({1, std::nullopt, 3})});
    return PlanBuilder().values({vec}).planNode();
  };
  EXPECT_EQ(*makeVectorValues(), *makeVectorValues());

  auto vectorDifferent =
      maker_.rowVector({"a"}, {maker_.flatVector<int64_t>({99})});
  auto valuesNodeDifferent = PlanBuilder().values({vectorDifferent}).planNode();
  EXPECT_NE(*makeVectorValues(), *valuesNodeDifferent);
}

TEST_F(LogicalPlanNodeEqualityTest, tableScanNodeEquality) {
  auto type = ROW({"a", "b"}, {BIGINT(), VARCHAR()});
  auto makeScan = [&](std::string connectorId = "hive",
                      std::string tableName = "t",
                      std::vector<std::string> columns = {"col_a", "col_b"}) {
    return std::make_shared<TableScanNode>(
        "1",
        type,
        std::move(connectorId),
        SchemaTableName{"default", std::move(tableName)},
        std::move(columns));
  };

  EXPECT_EQ(*makeScan(), *makeScan());

  // Different connector id.
  EXPECT_NE(*makeScan(), *makeScan("iceberg"));

  // Different table name.
  EXPECT_NE(*makeScan(), *makeScan("hive", "other"));

  // Different column names.
  EXPECT_NE(*makeScan(), *makeScan("hive", "t", {"col_x", "col_y"}));
}

TEST_F(LogicalPlanNodeEqualityTest, filterNodeEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto makeNode = [&rows] {
    return PlanBuilder().values({"x", "y"}, rows).filter("x > 0").planNode();
  };

  EXPECT_EQ(*makeNode(), *makeNode());

  // Different predicate.
  auto filterDifferent =
      PlanBuilder().values({"x", "y"}, rows).filter("x > 5").planNode();
  EXPECT_NE(*makeNode(), *filterDifferent);

  // Different input.
  std::vector<std::vector<std::string>> differentRows = {
      {"99", "10"}, {"2", "20"}};
  auto filterDifferentInput = PlanBuilder()
                                  .values({"x", "y"}, differentRows)
                                  .filter("x > 0")
                                  .planNode();
  EXPECT_NE(*makeNode(), *filterDifferentInput);
}

TEST_F(LogicalPlanNodeEqualityTest, projectNodeEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto makeNode = [&rows] {
    return PlanBuilder()
        .values({"x", "y"}, rows)
        .project({"x + y as result"})
        .planNode();
  };

  EXPECT_EQ(*makeNode(), *makeNode());

  // Different expression.
  auto projectDifferent = PlanBuilder()
                              .values({"x", "y"}, rows)
                              .project({"x - y as result"})
                              .planNode();
  EXPECT_NE(*makeNode(), *projectDifferent);

  // Different names.
  auto projectDifferentName = PlanBuilder()
                                  .values({"x", "y"}, rows)
                                  .project({"x + y as other"})
                                  .planNode();
  EXPECT_NE(*makeNode(), *projectDifferentName);
}

TEST_F(LogicalPlanNodeEqualityTest, aggregateNodeEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto makeNode = [&rows] {
    return PlanBuilder()
        .values({"x", "y"}, rows)
        .aggregate({"x"}, {"sum(y) as total"})
        .planNode();
  };

  EXPECT_EQ(*makeNode(), *makeNode());

  // Different aggregate function.
  auto aggregateDifferent = PlanBuilder()
                                .values({"x", "y"}, rows)
                                .aggregate({"x"}, {"avg(y) as total"})
                                .planNode();
  EXPECT_NE(*makeNode(), *aggregateDifferent);

  // Different output names.
  auto aggregateDifferentNames = PlanBuilder()
                                     .values({"x", "y"}, rows)
                                     .aggregate({"x"}, {"sum(y) as value"})
                                     .planNode();
  EXPECT_NE(*makeNode(), *aggregateDifferentNames);
}

TEST_F(LogicalPlanNodeEqualityTest, joinNodeEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  std::vector<std::vector<std::string>> rightRows = {{"1", "2"}};

  auto makeJoin = [&rows, &rightRows] {
    return PlanBuilder()
        .values({"x", "y"}, rows)
        .join(
            PlanBuilder().values({"a", "b"}, rightRows),
            "x = a",
            JoinType::kInner)
        .planNode();
  };

  EXPECT_EQ(*makeJoin(), *makeJoin());

  // Different join type.
  auto joinLeft = PlanBuilder()
                      .values({"x", "y"}, rows)
                      .join(
                          PlanBuilder().values({"a", "b"}, rightRows),
                          "x = a",
                          JoinType::kLeft)
                      .planNode();
  EXPECT_NE(*makeJoin(), *joinLeft);

  // No condition (cross join) vs with condition.
  auto makeCrossJoin = [&rows, &rightRows] {
    return PlanBuilder()
        .values({"x", "y"}, rows)
        .crossJoin(PlanBuilder().values({"a", "b"}, rightRows))
        .planNode();
  };
  EXPECT_NE(*makeJoin(), *makeCrossJoin());

  // Two cross joins are equal.
  EXPECT_EQ(*makeCrossJoin(), *makeCrossJoin());
}

TEST_F(LogicalPlanNodeEqualityTest, sortNodeEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto makeNode = [&rows] {
    return PlanBuilder().values({"x", "y"}, rows).sort({"x"}).planNode();
  };

  EXPECT_EQ(*makeNode(), *makeNode());

  // Different sort order.
  auto sortDesc =
      PlanBuilder().values({"x", "y"}, rows).sort({"x DESC"}).planNode();
  EXPECT_NE(*makeNode(), *sortDesc);

  // Different sort key.
  auto sortY = PlanBuilder().values({"x", "y"}, rows).sort({"y"}).planNode();
  EXPECT_NE(*makeNode(), *sortY);
}

TEST_F(LogicalPlanNodeEqualityTest, limitNodeEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto makeNode = [&rows] {
    return PlanBuilder().values({"x", "y"}, rows).limit(0, 10).planNode();
  };

  EXPECT_EQ(*makeNode(), *makeNode());

  // Different count.
  auto limitDifferentCount =
      PlanBuilder().values({"x", "y"}, rows).limit(0, 20).planNode();
  EXPECT_NE(*makeNode(), *limitDifferentCount);

  // Different offset.
  auto limitDifferentOffset =
      PlanBuilder().values({"x", "y"}, rows).limit(5, 10).planNode();
  EXPECT_NE(*makeNode(), *limitDifferentOffset);
}

TEST_F(LogicalPlanNodeEqualityTest, setNodeEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto makeNode = [&rows] {
    return PlanBuilder()
        .values({"x", "y"}, rows)
        .unionAll(PlanBuilder().values({"x", "y"}, rows))
        .planNode();
  };

  EXPECT_EQ(*makeNode(), *makeNode());

  // Different operation.
  auto setIntersect = PlanBuilder()
                          .values({"x", "y"}, rows)
                          .intersect(PlanBuilder().values({"x", "y"}, rows))
                          .planNode();
  EXPECT_NE(*makeNode(), *setIntersect);
}

TEST_F(LogicalPlanNodeEqualityTest, unnestNodeEquality) {
  auto makeUnnest = [] {
    return PlanBuilder()
        .values(ROW("arr", ARRAY(BIGINT())), std::vector<Variant>{})
        .unnest({Col("arr").unnestAs("elem")}, Ordinality().as("ord"))
        .planNode();
  };

  EXPECT_EQ(*makeUnnest(), *makeUnnest());

  // Different ordinality.
  auto unnestNoOrd =
      PlanBuilder()
          .values(ROW("arr", ARRAY(BIGINT())), std::vector<Variant>{})
          .unnest({Col("arr").unnestAs("elem")})
          .planNode();
  EXPECT_NE(*makeUnnest(), *unnestNoOrd);

  // Different unnested names.
  auto unnestDifferentNames =
      PlanBuilder()
          .values(ROW("arr", ARRAY(BIGINT())), std::vector<Variant>{})
          .unnest({Col("arr").unnestAs("val")}, Ordinality().as("ord"))
          .planNode();
  EXPECT_NE(*makeUnnest(), *unnestDifferentNames);
}

TEST_F(LogicalPlanNodeEqualityTest, tableWriteNodeEquality) {
  auto type = ROW({"x", "y"}, {BIGINT(), BIGINT()});
  auto makeInput = [&type] {
    return std::make_shared<ValuesNode>("1", type, ValuesNode::Variants{});
  };
  auto xRef = std::make_shared<InputReferenceExpr>(BIGINT(), "x");
  auto yRef = std::make_shared<InputReferenceExpr>(BIGINT(), "y");

  auto makeNode =
      [&](WriteKind kind = WriteKind::kInsert,
          std::string tableName = "t",
          folly::F14FastMap<std::string, std::string> options = {}) {
        return std::make_shared<TableWriteNode>(
            "2",
            makeInput(),
            "hive",
            SchemaTableName{"default", std::move(tableName)},
            kind,
            std::vector<std::string>{"x", "y"},
            std::vector<ExprPtr>{xRef, yRef},
            std::move(options));
      };

  EXPECT_EQ(*makeNode(), *makeNode());

  // Different write kind.
  EXPECT_NE(*makeNode(), *makeNode(WriteKind::kCreate));

  // Different table.
  EXPECT_NE(*makeNode(), *makeNode(WriteKind::kInsert, "other"));

  // With options vs without.
  EXPECT_NE(
      *makeNode(),
      *makeNode(WriteKind::kInsert, "t", {{"compression", "zstd"}}));
}

TEST_F(LogicalPlanNodeEqualityTest, sampleNodeEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto makeNode = [&rows] {
    return PlanBuilder()
        .values({"x", "y"}, rows)
        .sample(50.0, SampleNode::SampleMethod::kBernoulli)
        .planNode();
  };

  EXPECT_EQ(*makeNode(), *makeNode());

  // Different percentage.
  auto sampleDifferentPct =
      PlanBuilder()
          .values({"x", "y"}, rows)
          .sample(25.0, SampleNode::SampleMethod::kBernoulli)
          .planNode();
  EXPECT_NE(*makeNode(), *sampleDifferentPct);

  // Different method.
  auto sampleSystem = PlanBuilder()
                          .values({"x", "y"}, rows)
                          .sample(50.0, SampleNode::SampleMethod::kSystem)
                          .planNode();
  EXPECT_NE(*makeNode(), *sampleSystem);
}

TEST_F(LogicalPlanNodeEqualityTest, outputNodeEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto makeNode = [&rows] {
    return std::make_shared<OutputNode>(
        "2",
        PlanBuilder().values({"x", "y"}, rows).planNode(),
        std::vector<OutputNode::Entry>{{0, "col_x"}, {1, "col_y"}});
  };

  EXPECT_EQ(*makeNode(), *makeNode());

  // Different entry name.
  auto outputDifferentName = std::make_shared<OutputNode>(
      "2",
      PlanBuilder().values({"x", "y"}, rows).planNode(),
      std::vector<OutputNode::Entry>{{0, "renamed"}, {1, "col_y"}});
  EXPECT_NE(*makeNode(), *outputDifferentName);

  // Different entry index.
  auto outputDifferentIndex = std::make_shared<OutputNode>(
      "2",
      PlanBuilder().values({"x", "y"}, rows).planNode(),
      std::vector<OutputNode::Entry>{{1, "col_x"}, {0, "col_y"}});
  EXPECT_NE(*makeNode(), *outputDifferentIndex);
}

TEST_F(LogicalPlanNodeEqualityTest, crossKindInequality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto values = PlanBuilder().values({"x", "y"}, rows).planNode();
  auto filter =
      PlanBuilder().values({"x", "y"}, rows).filter("x > 0").planNode();

  EXPECT_NE(*values, *filter);
}

TEST_F(LogicalPlanNodeEqualityTest, selfEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto values = PlanBuilder().values({"x", "y"}, rows).planNode();

  EXPECT_EQ(*values, *values);
}

TEST_F(LogicalPlanNodeEqualityTest, deepTreeEquality) {
  std::vector<std::vector<std::string>> rows = {{"1", "10"}, {"2", "20"}};
  auto makeTree = [&rows] {
    return PlanBuilder()
        .values({"x", "y"}, rows)
        .filter("x > 0")
        .project({"x + y as result"})
        .planNode();
  };

  EXPECT_EQ(*makeTree(), *makeTree());

  // Different deep in the tree.
  auto treeDifferentDeep = PlanBuilder()
                               .values({"x", "y"}, rows)
                               .filter("x > 5")
                               .project({"x + y as result"})
                               .planNode();
  EXPECT_NE(*makeTree(), *treeDifferentDeep);
}

} // namespace
} // namespace facebook::axiom::logical_plan
