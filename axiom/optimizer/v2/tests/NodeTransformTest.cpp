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

#include <array>

#include <folly/ScopeGuard.h>

#include "axiom/connectors/SchemaResolver.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/ConstantFold.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/DecorrelatePass.h"
#include "axiom/optimizer/v2/ExpandAggregatePass.h"
#include "axiom/optimizer/v2/LimitAndOrderPass.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/PlanPhysicalPass.h"
#include "axiom/optimizer/v2/PrecomputeProjectionsPass.h"
#include "axiom/optimizer/v2/PushdownAndPrunePass.h"
#include "axiom/optimizer/v2/TranslatePass.h"
#include "axiom/optimizer/v2/tests/UnitTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/expression/Expr.h"
#include "velox/type/Type.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

class NodeTransformTest : public UnitTestBase {
 protected:
  NodeCP emptyLeaf(ColumnVector columns) {
    return builder_->makeEmptyValues(std::move(columns));
  }

  NodeCP emptyLeaf(std::string_view name) {
    return emptyLeaf(ColumnVector{makeColumn(name, velox::BIGINT())});
  }
};

void assertRoundTrip(NodeCP node, Builder& builder) {
  NodeVector inputs{node->inputs().begin(), node->inputs().end()};
  EXPECT_EQ(node->withInputs(std::move(inputs), builder), node)
      << node->nodeTypeName();
  for (NodeCP input : node->inputs()) {
    assertRoundTrip(input, builder);
  }
}

TEST_F(NodeTransformTest, roundTripPreservesEveryKeyField) {
  NodeCP input = emptyLeaf("a");
  ColumnCP a = input->outputColumns().at(0);
  ColumnCP b = makeColumn("b", velox::BIGINT());
  ColumnCP flag = makeColumn("flag", velox::BOOLEAN());
  ColumnCP id = makeColumn("id", velox::BIGINT());
  ColumnCP rank = makeColumn("rank", velox::BIGINT());
  ColumnCP marker = makeColumn("marker", velox::BOOLEAN());
  ColumnCP groupId = makeColumn("group_id", velox::BIGINT());
  ColumnCP windowOutput = makeColumn("window_sum", velox::BIGINT());

  auto* project = builder_->make<Project>(Project::Key{
      .input = input,
      .exprs = ExprVector{a},
      .outputColumns = ColumnVector{a},
  });
  auto* filter = builder_->make<Filter>(Filter::Key{
      .input = project,
      .predicates = ExprVector{builder_->makeBoolean(true)},
  });
  auto* limit = builder_->make<Limit>(Limit::Key{
      .input = filter,
      .offset = 17,
      .count = 23,
  });
  auto* sort = builder_->make<Sort>(Sort::Key{
      .input = limit,
      .orderKeys = ExprVector{a},
      .orderTypes = OrderTypeVector{OrderType::kDescNullsLast},
  });
  auto* topN = builder_->make<TopN>(TopN::Key{
      .input = sort,
      .orderKeys = ExprVector{a},
      .orderTypes = OrderTypeVector{OrderType::kDescNullsLast},
      .offset = 11,
      .count = 29,
  });
  auto* groupIdNode = builder_->make<GroupId>(GroupId::Key{
      .input = topN,
      .groupingKeys = ExprVector{a},
      .aggregationInputs = ExprVector{a},
      .groupingSets = QGVector<QGVector<int32_t>>{{0}, {}},
      .groupingKeyColumns = ColumnVector{b},
      .groupId = groupId,
      .outputColumns = ColumnVector{b, a, groupId},
  });
  auto* aggregate = builder_->make<Aggregate>(Aggregate::Key{
      .input = groupIdNode,
      .groupingKeys = ExprVector{b, groupId},
      .aggregates = {},
      .outputColumns = ColumnVector{b, groupId},
      .step = AggregateStep::kPartial,
      .groupId = groupId,
      .globalGroupingSets = QGVector<int32_t>{1},
  });
  auto* markDistinct = builder_->make<MarkDistinct>(MarkDistinct::Key{
      .input = aggregate,
      .markers = ColumnVector{marker, flag},
      .distinctKeys = ExprVector{b},
      .masks = ColumnVector{flag},
      .outputColumns = ColumnVector{b, groupId, marker, flag},
  });
  WindowFunctions functions;
  functions.push_back(
      WindowFunction{
          .call = builder_->makeCall(
              toName("sum"),
              Value(queryCtx()->toType(velox::BIGINT())),
              ExprVector{b},
              FunctionSet{}),
          .frame = Frame::wholePartition(),
          .ignoreNulls = true,
      });
  auto windowColumns = markDistinct->outputColumns();
  windowColumns.push_back(windowOutput);
  auto* window = builder_->make<Window>(Window::Key{
      .input = markDistinct,
      .functions = std::move(functions),
      .partitionKeys = ExprVector{b},
      .orderKeys = ExprVector{groupId},
      .orderTypes = OrderTypeVector{OrderType::kAscNullsFirst},
      .outputColumns = std::move(windowColumns),
  });
  auto* topNRowNumber = builder_->make<TopNRowNumber>(TopNRowNumber::Key{
      .input = window,
      .rankFunction = TopNRowNumber::RankFunction::kRank,
      .partitionKeys = ExprVector{b},
      .orderKeys = ExprVector{groupId},
      .orderTypes = OrderTypeVector{OrderType::kAscNullsFirst},
      .limit = 7,
      .rankColumn = rank,
      .outputColumns =
          ColumnVector{b, groupId, marker, flag, windowOutput, rank},
  });
  auto* assignUniqueId = builder_->make<AssignUniqueId>(AssignUniqueId::Key{
      .input = topNRowNumber,
      .idColumn = id,
  });
  auto* enforceDistinct = builder_->make<EnforceDistinct>(EnforceDistinct::Key{
      .input = assignUniqueId,
      .distinctKeys = ExprVector{id},
      .errorMessage = toName("duplicate scalar row"),
  });
  auto* enforceSingleRow = builder_->make<EnforceSingleRow>(
      EnforceSingleRow::Key{.input = enforceDistinct});
  auto* exchange = builder_->make<Exchange>(Exchange::Key{
      .input = enforceSingleRow,
      .partitioning =
          Partitioning{
              .kind = PartitionKind::kGather,
              .scope = PropertyScope::kGlobal,
          },
  });

  assertRoundTrip(exchange, *builder_);
}

TEST_F(NodeTransformTest, rejectsWrongArity) {
  NodeCP input = emptyLeaf("a");
  auto* filter = builder_->make<Filter>(Filter::Key{
      .input = input,
      .predicates = ExprVector{builder_->makeBoolean(true)},
  });

  VELOX_ASSERT_THROW(
      filter->withInputs({}, *builder_),
      "Replacement input count must match existing input count: Filter");
  VELOX_ASSERT_THROW(
      input->withInputs({input}, *builder_),
      "Replacement input count must match existing input count: Values");
}

TEST_F(NodeTransformTest, rejectsMissingOrExtraOutputColumns) {
  ColumnCP a = makeColumn("a", velox::BIGINT());
  ColumnCP b = makeColumn("b", velox::BIGINT());
  NodeCP input = emptyLeaf(ColumnVector{a});
  auto* filter = builder_->make<Filter>(Filter::Key{
      .input = input,
      .predicates = ExprVector{builder_->makeBoolean(true)},
  });

  VELOX_ASSERT_THROW(
      filter->withInputs({emptyLeaf(ColumnVector{})}, *builder_),
      "Replacement input must have the same output columns at index 0: Filter");
  VELOX_ASSERT_THROW(
      filter->withInputs({emptyLeaf(ColumnVector{a, b})}, *builder_),
      "Replacement input must have the same output columns at index 0: Filter");
}

TEST_F(NodeTransformTest, acceptsSameOutputColumnSetInDifferentOrder) {
  ColumnCP a = makeColumn("a", velox::BIGINT());
  ColumnCP b = makeColumn("b", velox::BIGINT());
  NodeCP input = emptyLeaf(ColumnVector{a, b});
  auto* filter = builder_->make<Filter>(Filter::Key{
      .input = input,
      .predicates = ExprVector{builder_->makeBoolean(true)},
  });
  NodeCP replacement = emptyLeaf(ColumnVector{b, a});

  EXPECT_EQ(
      filter->withInputs({replacement}, *builder_)->inputs()[0], replacement);
}

class OptimizerPlanRoundTripTest : public optimizer::test::QueryTestBase {
 protected:
  void configureTestConnector() override {
    testConnector_->addTpchTables();
    testConnector_->addTable(
        "arrays",
        velox::ROW(
            {"a", "b", "items"},
            {velox::BIGINT(), velox::BIGINT(), velox::ARRAY(velox::BIGINT())}));
    testConnector_->addTable(
        "write_target",
        velox::ROW({"a", "b"}, {velox::BIGINT(), velox::BIGINT()}));
  }
};

void assertRoundTripAndCollect(
    NodeCP node,
    Builder& builder,
    std::array<bool, 21>& seen) {
  const auto index = static_cast<size_t>(node->nodeType());
  ASSERT_LT(index, seen.size());
  seen[index] = true;
  NodeVector inputs{node->inputs().begin(), node->inputs().end()};
  EXPECT_EQ(node->withInputs(std::move(inputs), builder), node)
      << node->nodeTypeName();
  for (NodeCP input : node->inputs()) {
    assertRoundTripAndCollect(input, builder, seen);
  }
}

TEST_F(OptimizerPlanRoundTripTest, optimizerProducedPlansCoverEveryNodeKind) {
  const std::vector<logical_plan::LogicalPlanNodePtr> plans{
      parseSelect(
          "SELECT n_name FROM nation WHERE n_regionkey > 1 "
          "ORDER BY n_name LIMIT 5",
          kTestConnectorId),
      parseSelect(
          "SELECT n_name, row_number() OVER (ORDER BY n_name) AS rn "
          "FROM nation",
          kTestConnectorId),
      parseSelect(
          "SELECT * FROM ("
          "SELECT n_name, row_number() OVER (ORDER BY n_name) AS rn "
          "FROM nation) WHERE rn <= 10",
          kTestConnectorId),
      parseSelect(
          "SELECT count(DISTINCT n_regionkey), sum(DISTINCT n_nationkey) "
          "FROM nation",
          kTestConnectorId),
      parseSelect(
          "SELECT a, b, count(*) FROM arrays "
          "GROUP BY GROUPING SETS ((a), (b), ())",
          kTestConnectorId),
      parseSelect(
          "SELECT a, item FROM arrays CROSS JOIN UNNEST(items) AS u(item)",
          kTestConnectorId),
      parseSelect(
          "SELECT n_nationkey FROM nation UNION ALL "
          "SELECT r_regionkey FROM region",
          kTestConnectorId),
      parseSelect(
          "SELECT n_name, r_name FROM nation JOIN region "
          "ON n_regionkey = r_regionkey",
          kTestConnectorId),
      parseSelect(
          "SELECT n_name, "
          "n_regionkey IN (SELECT r_regionkey FROM region) AS present "
          "FROM nation",
          kTestConnectorId),
      parseSelect(
          "SELECT (SELECT r_name FROM region WHERE r_regionkey = 1) "
          "FROM nation LIMIT 1",
          kTestConnectorId),
      parseSelect(
          "SELECT (SELECT r_name FROM region "
          "WHERE r_regionkey = n_regionkey) FROM nation",
          kTestConnectorId),
      parseSelect("SELECT 1", kTestConnectorId),
      logical_plan::PlanBuilder(
          logical_plan::PlanBuilder::Context{kTestConnectorId, kDefaultSchema})
          .tableScan("arrays", {"a", "b"})
          .tableWrite(
              "write_target", logical_plan::WriteKind::kInsert, {"a", "b"})
          .build(),
  };

  auto veloxQueryCtx = getQueryCtx();
  velox::HashStringAllocator allocator(&optimizerPool());
  optimizer::QueryGraphContext context(allocator);
  optimizer::queryCtx() = &context;
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };
  velox::exec::SimpleExpressionEvaluator evaluator(
      veloxQueryCtx.get(), &optimizerPool());
  connector::SchemaResolver resolver{
      connector::ConnectorMetadataRegistry::global()};
  optimizer::Schema schema(resolver);
  optimizer::OptimizerSession session(
      veloxQueryCtx->queryId(),
      "test",
      optimizerOptions_,
      connectorSessionProperties_);
  optimizer::ConstantPlanRunner constantPlanRunner(veloxQueryCtx);
  Builder builder;
  std::array<bool, 21> seen{};

  for (const auto& plan : plans) {
    auto translated = TranslatePass::run(
        *plan, schema, evaluator, builder, session, constantPlanRunner);
    assertRoundTripAndCollect(translated.root, builder, seen);
    NodeCP decorrelated = DecorrelatePass::run(translated.root, builder);
    assertRoundTripAndCollect(decorrelated, builder, seen);
    NodeCP limited = LimitAndOrderPass::run(decorrelated, builder);
    assertRoundTripAndCollect(limited, builder, seen);
    NodeCP pushed = PushdownAndPrunePass::run(limited, builder, evaluator);
    assertRoundTripAndCollect(pushed, builder, seen);
    NodeCP physical = PlanPhysicalPass::run(
        pushed, builder, optimizerOptions_, /*numWorkers=*/4, /*numDrivers=*/4);
    assertRoundTripAndCollect(physical, builder, seen);
    NodeCP precomputed = PrecomputeProjectionsPass::run(physical, builder);
    assertRoundTripAndCollect(precomputed, builder, seen);
    NodeCP expanded = ExpandAggregatePass::run(precomputed, builder);
    assertRoundTripAndCollect(expanded, builder, seen);
  }

  for (size_t i = 0; i < seen.size(); ++i) {
    EXPECT_TRUE(seen[i]) << NodeTypeName::toName(static_cast<NodeType>(i));
  }
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
