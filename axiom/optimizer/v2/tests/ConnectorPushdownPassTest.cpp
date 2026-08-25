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

#include <atomic>
#include <functional>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/ConnectorPushdownPass.h"
#include "axiom/optimizer/v2/EstimateProvider.h"
#include "axiom/optimizer/v2/Node.h"
#include "folly/coro/CurrentExecutor.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/expression/Expr.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;
using connector::PushdownRoot;

NodeCP findFirstDescendantOfType(NodeCP root, NodeType target) {
  if (root->nodeType() == target) {
    return root;
  }
  for (NodeCP input : root->inputs()) {
    if (auto* found = findFirstDescendantOfType(input, target)) {
      return found;
    }
  }
  return nullptr;
}

NodeCP requireFirstDescendantOfType(NodeCP root, NodeType target) {
  NodeCP found = findFirstDescendantOfType(root, target);
  VELOX_CHECK_NOT_NULL(found);
  return found;
}

velox::RowTypePtr rowTypeOfOutputs(NodeCP node) {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  const auto& columns = node->outputColumns();
  names.reserve(columns.size());
  types.reserve(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    ColumnCP column = columns[i];
    names.push_back("output_" + std::to_string(i));
    types.push_back(queryCtx()->toTypePtr(column->value().type));
  }
  return velox::ROW(std::move(names), std::move(types));
}

bool containsScanFromConnector(const Node* node, std::string_view connectorId) {
  if (node == nullptr) {
    return false;
  }
  if (node->is(NodeType::kScan)) {
    return node->as<Scan>()->baseTable()->schemaTable->connectorId() ==
        connectorId;
  }
  for (NodeCP input : node->inputs()) {
    if (containsScanFromConnector(input, connectorId)) {
      return true;
    }
  }
  return false;
}

struct QueryContextScope {
  explicit QueryContextScope(optimizer::QueryGraphContext& context)
      : previous{optimizer::queryCtx()} {
    optimizer::queryCtx() = &context;
  }

  ~QueryContextScope() {
    optimizer::queryCtx() = previous;
  }

  optimizer::QueryGraphContext* previous;
};

struct PlanTestContext {
  explicit PlanTestContext(std::string_view poolName)
      : pool{velox::memory::memoryManager()->addLeafPool(
            std::string(poolName))},
        allocator{pool.get()},
        graphContext{allocator},
        queryContextScope{graphContext},
        resolver{connector::ConnectorMetadataRegistry::global()},
        schema{resolver} {}

  std::shared_ptr<velox::memory::MemoryPool> pool;
  velox::HashStringAllocator allocator;
  optimizer::QueryGraphContext graphContext;
  QueryContextScope queryContextScope;
  connector::SchemaResolver resolver;
  optimizer::Schema schema;
  Builder builder;
};

class ConnectorPushdownPassTest : public optimizer::test::QueryTestBase {
 protected:
  ConnectorPushdownPassTest() {
    useV2_ = true;
  }

  void SetUp() override {
    QueryTestBase::SetUp();
    testMetadata_ = dynamic_cast<connector::TestConnectorMetadata*>(
        testConnector_->metadata().get());
    VELOX_CHECK_NOT_NULL(testMetadata_);
  }

  struct ScopedConnectorRegistration {
    ScopedConnectorRegistration(
        std::string connectorId,
        std::shared_ptr<connector::TestConnector> connector,
        connector::TestConnectorMetadata* metadata)
        : connectorId{std::move(connectorId)},
          connector{std::move(connector)},
          metadata{metadata} {}

    ScopedConnectorRegistration& operator=(ScopedConnectorRegistration&&) =
        delete;
    ScopedConnectorRegistration(const ScopedConnectorRegistration&) = delete;
    ScopedConnectorRegistration& operator=(const ScopedConnectorRegistration&) =
        delete;

    ~ScopedConnectorRegistration() {
      if (connector == nullptr) {
        return;
      }
      connector::ConnectorMetadataRegistry::global().erase(connectorId);
      velox::connector::ConnectorRegistry::global().erase(connectorId);
    }

    std::string connectorId;
    std::shared_ptr<connector::TestConnector> connector;
    connector::TestConnectorMetadata* metadata;
  };

  ScopedConnectorRegistration registerScopedConnector(std::string_view id) {
    auto connector =
        std::make_shared<connector::TestConnector>(std::string(id));
    auto* metadata = dynamic_cast<connector::TestConnectorMetadata*>(
        connector->metadata().get());
    VELOX_CHECK_NOT_NULL(metadata);
    velox::connector::ConnectorRegistry::global().insert(
        connector->connectorId(), connector);
    connector::ConnectorMetadataRegistry::global().insert(
        std::string(id), connector->metadata());
    return {std::string(id), std::move(connector), metadata};
  }

  NodeCP makeLimitedScan(
      optimizer::Schema& schema,
      Builder& builder,
      std::string_view connectorId,
      std::string_view tableName,
      int64_t count) {
    const auto* schemaTable =
        schema.findTable(connectorId, {kDefaultSchema, std::string(tableName)});
    VELOX_CHECK_NOT_NULL(schemaTable);
    auto* baseTable = make<BaseTable>();
    baseTable->cname = optimizer::queryCtx()->newName("t");
    baseTable->schemaTable = schemaTable;
    baseTable->filteredCardinality = schemaTable->cardinality;
    const auto* schemaColumn = schemaTable->columns.begin()->second;
    auto* column = make<Column>(
        schemaColumn->name(),
        baseTable,
        schemaColumn->value(),
        optimizer::queryCtx()->newName("c"),
        schemaColumn->name());
    baseTable->columns.push_back(column);
    const auto* scan = builder.make<Scan>(Scan::Key{
        .baseTable = baseTable,
        .outputColumns = optimizer::ColumnVector{column},
    });
    return builder.make<Limit>(Limit::Key{
        .input = scan,
        .offset = 0,
        .count = count,
    });
  }

  NodeCP makeUnionAll(Builder& builder, NodeVector inputs) {
    QGVector<optimizer::ColumnVector> legColumns;
    legColumns.reserve(inputs.size());
    for (NodeCP input : inputs) {
      legColumns.push_back(
          optimizer::ColumnVector{input->outputColumns().front()});
    }
    const auto* firstOutput = inputs.front()->outputColumns().front();
    return builder.make<UnionAll>(UnionAll::Key{
        .inputs = std::move(inputs),
        .legColumns = std::move(legColumns),
        .outputColumns = optimizer::ColumnVector{optimizer::Column::create(
            "u", firstOutput->value())},
    });
  }

  NodeCP runPass(
      NodeCP root,
      Builder& builder,
      const optimizer::Schema& schema,
      const connector::SchemaResolver& resolver) {
    velox::exec::SimpleExpressionEvaluator evaluator(
        getQueryCtx().get(), &optimizerPool());
    const OptimizerSession session{
        getQueryCtx()->queryId(),
        "test",
        optimizerOptions_,
        connectorSessionProperties_};
    return ConnectorPushdownPass::run(
        root, builder, schema, resolver, session, evaluator);
  }

  logical_plan::LogicalPlanNodePtr aggregatePlan(
      std::string_view tableName = "t",
      std::string_view aggregate = "sum(b)") {
    return lp::PlanBuilder(context_)
        .tableScan(std::string(tableName))
        .aggregate({"a"}, {std::string(aggregate)})
        .build();
  }

  logical_plan::LogicalPlanNodePtr recursivePlan() {
    auto anchor = lp::PlanBuilder(context_).tableScan("seed").aggregate(
        {}, {"max(a) as n"});
    auto step = lp::PlanBuilder(context_)
                    .recursiveRef("counter", anchor)
                    .filter("n > 0")
                    .project({"n - 1"})
                    .planNode();
    return anchor.fixedPoint("counter", step).build();
  }

  void setWholeSubtreeReplacement(connector::TablePtr replacement) {
    testMetadata_->setPushdownMatcher(
        [replacement = std::move(replacement)](const Node& subtree) {
          return std::vector<PushdownRoot>{{&subtree, replacement}};
        });
  }

  void expectConcurrentOffers(
      std::initializer_list<connector::TestConnectorMetadata*> metadata,
      size_t expectedCalls,
      const std::function<void()>& run,
      std::function<std::vector<PushdownRoot>(
          connector::TestConnectorMetadata*,
          const Node&)> respond = {}) {
    std::atomic<size_t> numCalls{0};
    std::atomic<size_t> numInFlight{0};
    std::atomic<bool> callsOverlapped{false};
    for (auto* connectorMetadata : metadata) {
      connectorMetadata->setAsyncPushdownMatcher(
          [&, connectorMetadata](
              connector::ConnectorSessionPtr, const Node& subtree)
              -> folly::coro::Task<std::vector<PushdownRoot>> {
            ++numCalls;
            if (numInFlight.fetch_add(1) != 0) {
              callsOverlapped.store(true);
            }
            co_await folly::coro::co_reschedule_on_current_executor;
            --numInFlight;
            co_return respond ? respond(connectorMetadata, subtree)
                              : std::vector<PushdownRoot>{};
          });
    }

    run();

    EXPECT_EQ(numCalls.load(), expectedCalls);
    EXPECT_TRUE(callsOverlapped.load());
  }

  lp::PlanBuilder::Context context_{
      std::string(kTestConnectorId),
      kDefaultSchema};

  connector::TestConnectorMetadata* testMetadata_{nullptr};
};

TEST_F(ConnectorPushdownPassTest, replacesAndExecutesByPosition) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  auto virtualTable =
      testConnector_->addTable("u", ROW({"group_key", "total"}, BIGINT()));
  virtualTable->addData(makeRowVector({
      makeFlatVector<int64_t>({7}),
      makeFlatVector<int64_t>({100}),
  }));

  auto logicalPlan = aggregatePlan("t", "sum(b) as s");
  setWholeSubtreeReplacement(virtualTable);

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan), matchScan("u").project().build());

  checkSame(
      logicalPlan,
      {makeRowVector({
          makeFlatVector<int64_t>({7}),
          makeFlatVector<int64_t>({100}),
      })});
}

TEST_F(ConnectorPushdownPassTest, replacementStatsChooseQueryWidth) {
  auto source = testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  source->setStats(10'000, {});
  auto virtualTable =
      testConnector_->addTable("small_result", ROW({"key", "total"}, BIGINT()));
  virtualTable->setStats(1, {});

  auto logicalPlan = aggregatePlan();
  setWholeSubtreeReplacement(virtualTable);

  OptimizerOptions options;
  options.smallQueryMaxScanRows = 10;
  options.smallQueryNumWorkers = 1;
  const auto result =
      planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 2}, options);
  EXPECT_EQ(result.plan->options().numWorkers, 1);
}

TEST_F(ConnectorPushdownPassTest, replacementStatsReachDownstreamJoin) {
  auto source = testConnector_->addTable("source", ROW("k", BIGINT()));
  source->setStats(1'000, {{"k", {.numDistinct = 1'000}}});
  auto build = testConnector_->addTable("build", ROW("k", BIGINT()));
  build->setStats(1'000, {{"k", {.numDistinct = 100}}});
  auto replacement =
      testConnector_->addTable("replacement", ROW("k", BIGINT()));
  replacement->setStats(1'000, {{"k", {.numDistinct = 10}}});

  PlanTestContext planContext{"replacement_stats"};
  NodeCP sourceRoot = makeLimitedScan(
      planContext.schema,
      planContext.builder,
      kTestConnectorId,
      "source",
      1'000);
  setWholeSubtreeReplacement(replacement);
  NodeCP rewritten = runPass(
      sourceRoot,
      planContext.builder,
      planContext.schema,
      planContext.resolver);
  ASSERT_TRUE(rewritten->is(NodeType::kProject));

  NodeCP buildRoot = makeLimitedScan(
      planContext.schema,
      planContext.builder,
      kTestConnectorId,
      "build",
      1'000);
  ColumnVector outputColumns = rewritten->outputColumns();
  outputColumns.push_back(buildRoot->outputColumns().front());
  NodeCP join = planContext.builder.make<Join>(Join::Key{
      .left = rewritten,
      .right = buildRoot,
      .joinType = velox::core::JoinType::kInner,
      .leftKeys = {rewritten->outputColumns().front()},
      .rightKeys = {buildRoot->outputColumns().front()},
      .filter = {},
      .nullAware = false,
      .nullAsValue = false,
      .outputColumns = std::move(outputColumns),
  });

  EstimateProvider estimates;
  EXPECT_EQ(estimates.estimate(join).cardinality, 10'000);
}

TEST_F(ConnectorPushdownPassTest, usesSchemaResolverRegistry) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  auto scopedPool = velox::memory::memoryManager()
                        ->addRootPool("scoped_connector_pushdown")
                        ->addAggregateChild("tables");
  auto scopedConnector = std::make_shared<connector::TestConnector>(
      kTestConnectorId, nullptr, std::move(scopedPool));
  scopedConnector->addTable("t", ROW({"a", "b"}, BIGINT()));
  auto scopedRegistry = connector::ConnectorMetadataRegistry::create(
      &connector::ConnectorMetadataRegistry::global());
  scopedRegistry->insert(kTestConnectorId, scopedConnector->metadata());
  connector::SchemaResolver resolver{*scopedRegistry};

  std::atomic<size_t> globalCalls{0};
  testMetadata_->setPushdownMatcher([&](const Node&) {
    ++globalCalls;
    return std::vector<PushdownRoot>{};
  });
  std::atomic<size_t> scopedCalls{0};
  scopedConnector->metadata()->setPushdownMatcher([&](const Node&) {
    ++scopedCalls;
    return std::vector<PushdownRoot>{};
  });

  auto logicalPlan = aggregatePlan();
  planVelox(logicalPlan, resolver, {.numWorkers = 1, .numDrivers = 1});

  EXPECT_EQ(scopedCalls.load(), 1);
  EXPECT_EQ(globalCalls.load(), 0);
}

TEST_F(ConnectorPushdownPassTest, passesConnectorSession) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  setConnectorSession(kTestConnectorId, "pushdown_mode", "enabled");

  testMetadata_->setAsyncPushdownMatcher(
      [](connector::ConnectorSessionPtr session,
         const Node&) -> folly::coro::Task<std::vector<PushdownRoot>> {
        EXPECT_EQ(session->property("pushdown_mode"), "enabled");
        co_return std::vector<PushdownRoot>{};
      });

  toSingleNodePlan(aggregatePlan());
}

TEST_F(ConnectorPushdownPassTest, negotiatesSharedDagSubtreeOnce) {
  auto probe = registerScopedConnector("probe");
  testConnector_->addTable("t", ROW({"a"}, BIGINT()));
  probe.connector->addTable("p", ROW({"b"}, BIGINT()));

  PlanTestContext planContext{"shared_dag"};
  NodeCP shared = makeLimitedScan(
      planContext.schema, planContext.builder, kTestConnectorId, "t", 10);
  NodeCP other = makeLimitedScan(
      planContext.schema,
      planContext.builder,
      probe.connector->connectorId(),
      "p",
      10);
  NodeCP root = makeUnionAll(planContext.builder, {shared, shared, other});

  const auto virtualTable =
      testConnector_->addTable("virt_shared", rowTypeOfOutputs(shared));
  std::atomic<size_t> numOffers{0};
  testMetadata_->setPushdownMatcher([&](const Node& subtree) {
    ++numOffers;
    return std::vector<PushdownRoot>{{&subtree, virtualTable}};
  });
  probe.metadata->setPushdownMatcher(
      [](const Node&) { return std::vector<PushdownRoot>{}; });

  NodeCP rewritten = runPass(
      root, planContext.builder, planContext.schema, planContext.resolver);

  EXPECT_EQ(numOffers.load(), 1);
  ASSERT_TRUE(rewritten->is(NodeType::kUnionAll));
  EXPECT_EQ(rewritten->inputs()[0], rewritten->inputs()[1]);
  EXPECT_TRUE(rewritten->inputs()[0]->is(NodeType::kProject));
  EXPECT_TRUE(rewritten->inputs()[0]->inputs()[0]->is(NodeType::kScan));
}

TEST_F(ConnectorPushdownPassTest, offersMaximalCutpointsPerDagOccurrence) {
  auto probe = registerScopedConnector("probe");
  testConnector_->addTable("t", ROW({"a"}, BIGINT()));
  probe.connector->addTable("p", ROW({"b"}, BIGINT()));

  PlanTestContext planContext{"overlapping_dag"};
  NodeCP shared = makeLimitedScan(
      planContext.schema, planContext.builder, kTestConnectorId, "t", 10);
  NodeCP maximal = planContext.builder.make<Limit>(Limit::Key{
      .input = shared,
      .offset = 0,
      .count = 5,
  });
  NodeCP other = makeLimitedScan(
      planContext.schema,
      planContext.builder,
      probe.connector->connectorId(),
      "p",
      10);
  NodeCP mixed = makeUnionAll(planContext.builder, {shared, other});
  NodeCP root = makeUnionAll(planContext.builder, {maximal, mixed});

  std::atomic<bool> sawMaximal{false};
  std::atomic<bool> sawShared{false};
  testMetadata_->setPushdownMatcher([&](const Node& subtree) {
    if (&subtree == maximal) {
      sawMaximal.store(true);
    } else {
      VELOX_CHECK(&subtree == shared);
      sawShared.store(true);
    }
    return std::vector<PushdownRoot>{};
  });
  probe.metadata->setPushdownMatcher(
      [](const Node&) { return std::vector<PushdownRoot>{}; });

  EXPECT_EQ(
      runPass(
          root, planContext.builder, planContext.schema, planContext.resolver),
      root);
  EXPECT_TRUE(sawMaximal.load());
  EXPECT_TRUE(sawShared.load());
}

TEST_F(ConnectorPushdownPassTest, scopesSharedDescendantReplacementToOffer) {
  auto probe = registerScopedConnector("probe");
  testConnector_->addTable("t", ROW({"a"}, BIGINT()));
  probe.connector->addTable("p", ROW({"b"}, BIGINT()));

  PlanTestContext planContext{"scoped_dag"};
  NodeCP shared = makeLimitedScan(
      planContext.schema, planContext.builder, kTestConnectorId, "t", 10);
  NodeCP maximal = planContext.builder.make<Limit>(Limit::Key{
      .input = shared,
      .offset = 0,
      .count = 5,
  });
  NodeCP other = makeLimitedScan(
      planContext.schema,
      planContext.builder,
      probe.connector->connectorId(),
      "p",
      10);
  NodeCP mixed = makeUnionAll(planContext.builder, {shared, other});
  NodeCP root = makeUnionAll(planContext.builder, {maximal, mixed});

  const auto virtualTable =
      testConnector_->addTable("virt_shared", rowTypeOfOutputs(shared));
  std::atomic<size_t> numOffers{0};
  testMetadata_->setPushdownMatcher([&](const Node& subtree) {
    ++numOffers;
    if (&subtree == shared) {
      return std::vector<PushdownRoot>{};
    }
    VELOX_CHECK(&subtree == maximal);
    return std::vector<PushdownRoot>{{shared, virtualTable}};
  });

  NodeCP rewritten = runPass(
      root, planContext.builder, planContext.schema, planContext.resolver);

  ASSERT_TRUE(rewritten->is(NodeType::kUnionAll));
  EXPECT_EQ(numOffers.load(), 2);
  EXPECT_NE(rewritten->inputs()[0], maximal);
  EXPECT_EQ(rewritten->inputs()[1]->inputs()[0], shared);
}

TEST_F(
    ConnectorPushdownPassTest,
    replacesIncomparableRootsWithSharedDescendant) {
  auto probe = registerScopedConnector("probe");
  testConnector_->addTable("t", ROW({"a"}, BIGINT()));
  probe.connector->addTable("p", ROW({"b"}, BIGINT()));

  PlanTestContext planContext{"diamond_dag"};
  NodeCP shared = makeLimitedScan(
      planContext.schema, planContext.builder, kTestConnectorId, "t", 10);
  NodeCP left = planContext.builder.make<Limit>(Limit::Key{
      .input = shared,
      .offset = 0,
      .count = 5,
  });
  NodeCP right = planContext.builder.make<Limit>(Limit::Key{
      .input = shared,
      .offset = 1,
      .count = 5,
  });
  NodeCP other = makeLimitedScan(
      planContext.schema,
      planContext.builder,
      probe.connector->connectorId(),
      "p",
      10);
  NodeCP root = makeUnionAll(
      planContext.builder,
      {makeUnionAll(planContext.builder, {left, other}),
       makeUnionAll(planContext.builder, {right, other})});

  auto leftReplacement =
      testConnector_->addTable("virt_left", rowTypeOfOutputs(left));
  auto rightReplacement =
      testConnector_->addTable("virt_right", rowTypeOfOutputs(right));
  testMetadata_->setPushdownMatcher([&](const Node& subtree) {
    VELOX_CHECK(&subtree == left || &subtree == right);
    return std::vector<PushdownRoot>{{
        &subtree,
        &subtree == left ? leftReplacement : rightReplacement,
    }};
  });

  NodeCP rewritten = runPass(
      root, planContext.builder, planContext.schema, planContext.resolver);
  NodeCP rewrittenLeft = rewritten->inputs()[0]->inputs()[0];
  NodeCP rewrittenRight = rewritten->inputs()[1]->inputs()[0];
  ASSERT_TRUE(rewrittenLeft->is(NodeType::kProject));
  ASSERT_TRUE(rewrittenRight->is(NodeType::kProject));
  ASSERT_TRUE(rewrittenLeft->inputs()[0]->is(NodeType::kScan));
  ASSERT_TRUE(rewrittenRight->inputs()[0]->is(NodeType::kScan));
  EXPECT_EQ(
      rewrittenLeft->inputs()[0]
          ->as<Scan>()
          ->baseTable()
          ->schemaTable->connectorTable,
      leftReplacement.get());
  EXPECT_EQ(
      rewrittenRight->inputs()[0]
          ->as<Scan>()
          ->baseTable()
          ->schemaTable->connectorTable,
      rightReplacement.get());
}

TEST_F(ConnectorPushdownPassTest, replacesBuildSideOfCrossConnectorJoin) {
  auto probe = registerScopedConnector("probe");

  const auto probeSchema = ROW({"a", "b"}, BIGINT());
  const auto buildSchema = ROW({"c", "d"}, BIGINT());
  probe.connector->addTable("p", probeSchema);
  testConnector_->addTable("q", buildSchema);
  auto replacement =
      testConnector_->addTable("virt_q", ROW({"key", "total"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan(
                             std::string(probe.connector->connectorId()),
                             kDefaultSchema,
                             std::string("p"))
                         .join(
                             lp::PlanBuilder(context_).tableScan("q").aggregate(
                                 {"c"}, {"sum(d)"}),
                             "a = c",
                             lp::JoinType::kInner)
                         .build();

  bool testMatcherCalled = false;
  bool testSubtreeContainsTestScan = false;
  bool testSubtreeContainsProbeScan = false;
  testMetadata_->setPushdownMatcher([&, replacement](const Node& subtree) {
    testMatcherCalled = true;
    testSubtreeContainsTestScan =
        containsScanFromConnector(&subtree, kTestConnectorId);
    testSubtreeContainsProbeScan =
        containsScanFromConnector(&subtree, probe.connector->connectorId());
    const auto* aggregate =
        requireFirstDescendantOfType(&subtree, NodeType::kAggregate);
    return std::vector<PushdownRoot>{{aggregate, replacement}};
  });
  bool probeCalled = false;
  probe.metadata->setPushdownMatcher([&](const Node&) {
    probeCalled = true;
    return std::vector<PushdownRoot>{};
  });

  auto plan = toSingleNodePlan(logicalPlan);
  auto buildMatcher = matchScan("virt_q").project();
  auto matcher = matchScan("p").hashJoin(buildMatcher).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_TRUE(testMatcherCalled);
  EXPECT_TRUE(testSubtreeContainsTestScan);
  EXPECT_FALSE(testSubtreeContainsProbeScan);
  EXPECT_FALSE(probeCalled);
}

TEST_F(ConnectorPushdownPassTest, executesJoinWithDuplicateSourceNames) {
  testConnector_->addTable("left_table", ROW({"k", "a"}, BIGINT()));
  testConnector_->addTable("right_table", ROW({"k", "a"}, BIGINT()));
  auto virtualTable = testConnector_->addTable(
      "virtual_join", ROW({"left_value", "right_value"}, BIGINT()));
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({10}),
      makeFlatVector<int64_t>({20}),
  });
  virtualTable->addData(expected);

  auto logicalPlan = parseSelect(
      "SELECT l.a AS left_a, r.a AS right_a "
      "FROM left_table l JOIN right_table r ON l.k = r.k",
      kTestConnectorId);
  testMetadata_->setPushdownMatcher([virtualTable = std::move(virtualTable)](
                                        const Node& subtree) {
    const auto* join = requireFirstDescendantOfType(&subtree, NodeType::kJoin);
    return std::vector<PushdownRoot>{{join, virtualTable}};
  });

  checkSame(logicalPlan, {expected});
}

TEST_F(ConnectorPushdownPassTest, negotiatesIndependentConnectorsConcurrently) {
  auto probe = registerScopedConnector("probe");

  probe.connector->addTable("p", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("q", ROW({"c", "d"}, BIGINT()));
  auto probeReplacement =
      probe.connector->addTable("virt_p", ROW({"key", "total"}, BIGINT()));
  auto testReplacement =
      testConnector_->addTable("virt_q", ROW({"key", "total"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan(
                             std::string(probe.connector->connectorId()),
                             kDefaultSchema,
                             std::string("p"))
                         .aggregate({"a"}, {"sum(b) as sb"})
                         .join(
                             lp::PlanBuilder(context_).tableScan("q").aggregate(
                                 {"c"}, {"sum(d) as sd"}),
                             "a = c",
                             lp::JoinType::kInner)
                         .build();

  expectConcurrentOffers(
      {probe.metadata, testMetadata_},
      2,
      [&] {
        AXIOM_ASSERT_PLAN(
            toSingleNodePlan(logicalPlan),
            matchScan("virt_p")
                .project()
                .hashJoin(matchScan("virt_q").project())
                .build());
      },
      [&](connector::TestConnectorMetadata* metadata, const Node& subtree) {
        const auto* aggregate =
            requireFirstDescendantOfType(&subtree, NodeType::kAggregate);
        return std::vector<PushdownRoot>{{
            aggregate,
            metadata == probe.metadata ? probeReplacement : testReplacement,
        }};
      });
}

TEST_F(ConnectorPushdownPassTest, negotiatesSameConnectorOffersConcurrently) {
  auto probe = registerScopedConnector("probe");
  testConnector_->addTable("left", ROW({"a"}, BIGINT()));
  testConnector_->addTable("right", ROW({"b"}, BIGINT()));
  probe.connector->addTable("p", ROW({"c"}, BIGINT()));

  PlanTestContext planContext{"concurrent_offers"};
  NodeCP left = makeLimitedScan(
      planContext.schema, planContext.builder, kTestConnectorId, "left", 10);
  NodeCP right = makeLimitedScan(
      planContext.schema, planContext.builder, kTestConnectorId, "right", 10);
  NodeCP other = makeLimitedScan(
      planContext.schema,
      planContext.builder,
      probe.connector->connectorId(),
      "p",
      10);
  NodeCP root = makeUnionAll(planContext.builder, {left, right, other});

  probe.metadata->setAsyncPushdownMatcher(
      [](connector::ConnectorSessionPtr,
         const Node&) -> folly::coro::Task<std::vector<PushdownRoot>> {
        co_return std::vector<PushdownRoot>{};
      });

  expectConcurrentOffers({testMetadata_}, 2, [&] {
    runPass(
        root, planContext.builder, planContext.schema, planContext.resolver);
  });
}

TEST_F(ConnectorPushdownPassTest, emptyMatcherResultIsNoOp) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  auto logicalPlan = aggregatePlan();

  testMetadata_->setPushdownMatcher(
      [](const Node&) { return std::vector<PushdownRoot>{}; });

  auto plan = toSingleNodePlan(logicalPlan);
  auto matcher = matchScan("t").singleAggregation({"a"}, {"sum(b)"}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(ConnectorPushdownPassTest, bareScanRootFails) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  auto replacement =
      testConnector_->addTable("u", ROW({"first", "second"}, BIGINT()));
  auto logicalPlan = aggregatePlan();

  testMetadata_->setPushdownMatcher([replacement](const Node& subtree) {
    const auto* scan = requireFirstDescendantOfType(&subtree, NodeType::kScan);
    return std::vector<PushdownRoot>{{scan, replacement}};
  });

  VELOX_ASSERT_THROW(toSingleNodePlan(logicalPlan), "cannot be a Scan");
}

TEST_F(ConnectorPushdownPassTest, ineligiblePlansSkipMatcher) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  size_t numCalls{0};
  testMetadata_->setPushdownMatcher([&](const Node&) {
    ++numCalls;
    return std::vector<PushdownRoot>{};
  });

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(lp::PlanBuilder(context_).tableScan("t").build()),
      matchScan("t").build());

  auto valuesPlan =
      lp::PlanBuilder(context_)
          .values(ROW("a", BIGINT()), {velox::Variant::row({int64_t{1}})})
          .build();
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(valuesPlan), matchValues().project().build());
  EXPECT_EQ(numCalls, 0);
}

TEST_F(ConnectorPushdownPassTest, tableWriteOffersOnlyItsInput) {
  testConnector_->addTable("source", ROW({"a"}, BIGINT()));
  const auto target = testConnector_->addTable("target", ROW({"a"}, BIGINT()));

  PlanTestContext planContext{"table_write"};
  NodeCP input = makeLimitedScan(
      planContext.schema, planContext.builder, kTestConnectorId, "source", 10);
  NodeCP root = planContext.builder.make<TableWrite>(TableWrite::Key{
      .input = input,
      .table = target.get(),
      .kind = connector::WriteKind::kInsert,
      .columnExprs = optimizer::ExprVector{input->outputColumns().front()},
  });

  std::atomic<NodeCP> offered{nullptr};
  testMetadata_->setPushdownMatcher([&](const Node& subtree) {
    offered.store(&subtree);
    return std::vector<PushdownRoot>{};
  });

  EXPECT_EQ(
      runPass(
          root, planContext.builder, planContext.schema, planContext.resolver),
      root);
  EXPECT_EQ(offered.load(), input);
}

TEST_F(ConnectorPushdownPassTest, replacesRecursiveCteAnchor) {
  testConnector_->addTable("seed", ROW({"a"}, BIGINT()));
  auto replacement =
      testConnector_->addTable("virt_seed", ROW("value", BIGINT()));

  auto logicalPlan = recursivePlan();

  testMetadata_->setPushdownMatcher([replacement](const Node& subtree) {
    const auto* fixedPoint = subtree.as<FixedPoint>();
    return std::vector<PushdownRoot>{{fixedPoint->anchor(), replacement}};
  });

  auto matcher =
      core::PlanMatcherBuilder()
          .fixedPoint(
              core::FixedPointMatch("counter")
                  .outputState(
                      /*append=*/true, matchScan("virt_seed").project())
                  .plan(
                      core::PlanMatcherBuilder()
                          .stateSource("counter", /*delta=*/true)
                          .aliases({"n"})
                          .filter("n > 0")
                          .project({"n - 1"}))
                  .convergeOnEmpty())
          .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_F(ConnectorPushdownPassTest, validatesRecursiveCteRoots) {
  testConnector_->addTable("seed", ROW({"a"}, BIGINT()));
  auto replacement =
      testConnector_->addTable("virt_recursive", ROW("value", BIGINT()));
  auto logicalPlan = recursivePlan();

  testMetadata_->setPushdownMatcher([replacement](const Node& subtree) {
    const auto* fixedPoint = subtree.as<FixedPoint>();
    return std::vector<PushdownRoot>{{fixedPoint->step(), replacement}};
  });
  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan), "depend on unbound recursive state");

  setWholeSubtreeReplacement(replacement);
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("virt_recursive").project().build());
}

TEST_F(ConnectorPushdownPassTest, rejectsConflictingRoots) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  auto firstTable =
      testConnector_->addTable("first", ROW({"key", "total"}, BIGINT()));
  auto secondTable =
      testConnector_->addTable("second", ROW({"key", "total"}, BIGINT()));
  auto outerTable = testConnector_->addTable("outer", ROW("value", BIGINT()));

  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .aggregate({"a"}, {"sum(b)"})
                         .project({"a + 1"})
                         .build();

  testMetadata_->setPushdownMatcher(
      [firstTable, secondTable](const Node& subtree) {
        const auto* aggregate =
            requireFirstDescendantOfType(&subtree, NodeType::kAggregate);
        return std::vector<PushdownRoot>{
            {aggregate, firstTable},
            {aggregate, secondTable},
        };
      });

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan), "Pushdown root was returned twice");

  testMetadata_->setPushdownMatcher(
      [outerTable, firstTable](const Node& subtree) {
        const auto* aggregate =
            requireFirstDescendantOfType(&subtree, NodeType::kAggregate);
        return std::vector<PushdownRoot>{
            {&subtree, outerTable},
            {aggregate, firstTable},
        };
      });

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan), "Pushdown roots cannot be nested");
}

TEST_F(ConnectorPushdownPassTest, replacesStrictDescendantOfOfferedSubtree) {
  const auto schema = ROW({"a", "b"}, BIGINT());
  testConnector_->addTable("t", schema);
  auto replacement =
      testConnector_->addTable("virt_agg", ROW({"key", "total"}, BIGINT()));

  auto planWithFilterDependingOnAggregate =
      lp::PlanBuilder(context_)
          .tableScan("t")
          .aggregate({"a"}, {"sum(b) as s"})
          .filter("s > 10")
          .build();

  testMetadata_->setPushdownMatcher([replacement](const Node& subtree) {
    const auto* aggregate =
        requireFirstDescendantOfType(&subtree, NodeType::kAggregate);
    EXPECT_NE(&subtree, aggregate);
    return std::vector<PushdownRoot>{{aggregate, replacement}};
  });

  auto plan = toSingleNodePlan(planWithFilterDependingOnAggregate);
  auto matcher = matchScan("virt_agg").project().filter("s > 10").build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(ConnectorPushdownPassTest, replacesMultipleDisjointRootsInOneResponse) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"c", "d"}, BIGINT()));
  auto leftReplacement =
      testConnector_->addTable("virt_left", ROW({"key", "total"}, BIGINT()));
  auto rightReplacement =
      testConnector_->addTable("virt_right", ROW({"key", "total"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .aggregate({"a"}, {"sum(b) as sb"})
                         .join(
                             lp::PlanBuilder(context_).tableScan("u").aggregate(
                                 {"c"}, {"sum(d) as sd"}),
                             "a = c",
                             lp::JoinType::kInner)
                         .build();

  testMetadata_->setPushdownMatcher(
      [leftReplacement, rightReplacement](const Node& subtree) {
        const auto inputs = subtree.inputs();
        const auto* leftAgg =
            requireFirstDescendantOfType(inputs[0], NodeType::kAggregate);
        const auto* rightAgg =
            requireFirstDescendantOfType(inputs[1], NodeType::kAggregate);
        return std::vector<PushdownRoot>{
            {leftAgg, leftReplacement},
            {rightAgg, rightReplacement},
        };
      });

  auto plan = toSingleNodePlan(logicalPlan);
  auto buildMatcher = matchScan("virt_right").project();
  auto matcher =
      matchScan("virt_left").project().hashJoin(buildMatcher).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(ConnectorPushdownPassTest, rootOutsideOfferedSubtreeFails) {
  auto probe = registerScopedConnector("probe");
  testConnector_->addTable("t", ROW("a", BIGINT()));
  probe.connector->addTable("p", ROW("b", BIGINT()));

  PlanTestContext planContext{"outside_offer"};
  NodeCP local = makeLimitedScan(
      planContext.schema, planContext.builder, kTestConnectorId, "t", 10);
  NodeCP outside = makeLimitedScan(
      planContext.schema,
      planContext.builder,
      probe.connector->connectorId(),
      "p",
      10);
  NodeCP root = makeUnionAll(planContext.builder, {local, outside});
  auto stolen = testConnector_->addTable("stolen", rowTypeOfOutputs(outside));
  testMetadata_->setPushdownMatcher(
      [outside, stolen = std::move(stolen)](const Node&) {
        return std::vector<PushdownRoot>{{outside, stolen}};
      });

  VELOX_ASSERT_THROW(
      runPass(
          root, planContext.builder, planContext.schema, planContext.resolver),
      "outside the offered subtree");
}

TEST_F(ConnectorPushdownPassTest, rejectsInvalidVirtualTables) {
  auto probe = registerScopedConnector("probe");
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  auto logicalPlan = aggregatePlan("t", "sum(b) as s");

  const auto expectRejected = [&](std::string_view label,
                                  connector::TablePtr replacement,
                                  const char* message) {
    SCOPED_TRACE(label);
    setWholeSubtreeReplacement(std::move(replacement));
    VELOX_ASSERT_THROW(toSingleNodePlan(logicalPlan), message);
  };

  expectRejected(
      "foreign connector",
      probe.connector->addTable("foreign", ROW({"key", "total"}, BIGINT())),
      "belongs to a different connector");

  auto notScannable = testConnector_->addTable(
      "not_scannable", ROW({"key", "total"}, BIGINT()));
  notScannable->prependLayoutForTest({"key", "total"}, false);
  expectRejected(
      "unscannable layout",
      std::move(notScannable),
      "first layout does not support scans");

  auto missingLayoutColumn = testConnector_->addTable(
      "missing_layout_column", ROW({"key", "total"}, BIGINT()));
  missingLayoutColumn->prependLayoutForTest({"key"}, true);
  expectRejected(
      "incomplete layout",
      std::move(missingLayoutColumn),
      "first layout is missing a visible column");

  expectRejected(
      "wrong arity",
      testConnector_->addTable("wrong_arity", ROW("value", BIGINT())),
      "must have one visible column per root output");
  expectRejected(
      "wrong type",
      testConnector_->addTable(
          "wrong_type", ROW({"first", "second"}, VARCHAR())),
      "column type does not match root output");
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
