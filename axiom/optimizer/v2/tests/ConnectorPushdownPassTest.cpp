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

#include <folly/coro/Baton.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/v2/Node.h"
#include "folly/coro/CurrentExecutor.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/ConnectorRegistry.h"

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
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({7}),
      makeFlatVector<int64_t>({100}),
  });
  virtualTable->addData(expected);

  auto logicalPlan = aggregatePlan("t", "sum(b) as s");
  setWholeSubtreeReplacement(virtualTable);

  checkSame(logicalPlan, {expected});
}

TEST_F(ConnectorPushdownPassTest, replacementStatsSelectSingleWorker) {
  auto source = testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  source->setStats(10'000, {});
  auto virtualTable =
      testConnector_->addTable("small_result", ROW({"key", "total"}, BIGINT()));
  virtualTable->setStats(1, {});

  auto logicalPlan = aggregatePlan();
  setWholeSubtreeReplacement(virtualTable);

  // The source exceeds the small-query threshold, but its replacement does
  // not. Worker selection must therefore use the replacement's row count.
  OptimizerOptions options;
  options.smallQueryMaxScanRows = 10;
  options.smallQueryNumWorkers = 1;
  const auto result = planVelox(
      logicalPlan,
      {.maxRemotePartitions = 4, .maxLocalPartitions = 2},
      options);
  EXPECT_EQ(result.plan->options().maxRemotePartitions, 1);
}

TEST_F(ConnectorPushdownPassTest, replacementStatsReachDownstreamJoin) {
  auto buildConnector = registerScopedConnector("build");
  auto source = testConnector_->addTable("source", ROW("k", BIGINT()));
  source->setStats(1'000, {{"k", {.numDistinct = 1'000}}});
  auto build =
      buildConnector.connector->addTable("build", ROW("build_k", BIGINT()));
  build->setStats(1'000, {{"build_k", {.numDistinct = 100}}});
  auto replacement =
      testConnector_->addTable("replacement", ROW("k", BIGINT()));
  replacement->setStats(1'000, {{"k", {.numDistinct = 10}}});

  auto logicalPlan =
      lp::PlanBuilder(context_)
          .tableScan("source")
          .limit(1'000)
          .join(
              lp::PlanBuilder(context_)
                  .tableScan(
                      std::string(buildConnector.connector->connectorId()),
                      kDefaultSchema,
                      std::string("build"))
                  .limit(1'000),
              "k = build_k",
              lp::JoinType::kInner)
          .build();
  setWholeSubtreeReplacement(replacement);

  // The replacement has 10 distinct join keys and the build has 100, so the
  // downstream join estimate is 1'000 * 1'000 / 100 = 10'000 rows.
  const auto result = planVelox(
      logicalPlan, {.maxRemotePartitions = 1, .maxLocalPartitions = 1});
  const auto& root = result.plan->fragments().back().fragment.planNode;
  const auto estimate = result.prediction.find(root->id());
  ASSERT_NE(estimate, result.prediction.end());
  EXPECT_EQ(estimate->second.cardinality, 10'000);
}

TEST_F(ConnectorPushdownPassTest, usesSchemaResolverRegistry) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  auto scopedPool = velox::memory::memoryManager()
                        ->addRootPool("scoped_connector_pushdown")
                        ->addAggregateChild("tables");
  auto scopedConnector = std::make_shared<connector::TestConnector>(
      "scoped-layout", nullptr, std::move(scopedPool));
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
  setConnectorSession(kTestConnectorId, "pushdown_mode", "scoped");
  scopedConnector->metadata()->setAsyncPushdownMatcher(
      [&](connector::ConnectorSessionPtr session,
          const Node&) -> folly::coro::Task<std::vector<PushdownRoot>> {
        ++scopedCalls;
        EXPECT_EQ(session->property("pushdown_mode"), "scoped");
        co_return std::vector<PushdownRoot>{};
      });

  auto logicalPlan = aggregatePlan();
  planVelox(
      logicalPlan,
      resolver,
      {.maxRemotePartitions = 1, .maxLocalPartitions = 1});

  EXPECT_EQ(scopedCalls.load(), 1);
  EXPECT_EQ(globalCalls.load(), 0);
}

TEST_F(ConnectorPushdownPassTest, passesConnectorSession) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  setConnectorSession(kTestConnectorId, "pushdown_mode", "enabled");

  std::atomic<size_t> numCalls{0};
  testMetadata_->setAsyncPushdownMatcher(
      [&](connector::ConnectorSessionPtr session,
          const Node&) -> folly::coro::Task<std::vector<PushdownRoot>> {
        ++numCalls;
        EXPECT_EQ(session->property("pushdown_mode"), "enabled");
        co_return std::vector<PushdownRoot>{};
      });

  toSingleNodePlan(aggregatePlan());
  EXPECT_EQ(numCalls.load(), 1);
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

  testMetadata_->setPushdownMatcher([&, replacement](const Node& subtree) {
    EXPECT_TRUE(containsScanFromConnector(&subtree, kTestConnectorId));
    EXPECT_FALSE(
        containsScanFromConnector(&subtree, probe.connector->connectorId()));
    const auto* aggregate =
        requireFirstDescendantOfType(&subtree, NodeType::kAggregate);
    return std::vector<PushdownRoot>{{aggregate, replacement}};
  });
  bool probeCalled = false;
  probe.metadata->setPushdownMatcher([&](const Node&) {
    probeCalled = true;
    return std::vector<PushdownRoot>{};
  });

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("p").hashJoin(matchScan("virt_q").project()).build());

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
  auto otherConnector = registerScopedConnector("other");
  testConnector_->addTable("left", ROW({"a"}, BIGINT()));
  testConnector_->addTable("right", ROW({"b"}, BIGINT()));
  otherConnector.connector->addTable("u", ROW({"c"}, BIGINT()));

  // The other connector keeps the two local inputs as separate offers.
  auto logicalPlan =
      lp::PlanBuilder(context_)
          .setOperation(
              lp::SetOperation::kUnionAll,
              {
                  lp::PlanBuilder(context_).tableScan("left").limit(10),
                  lp::PlanBuilder(context_).tableScan("right").limit(10),
                  lp::PlanBuilder(context_)
                      .tableScan(
                          std::string(otherConnector.connector->connectorId()),
                          kDefaultSchema,
                          std::string("u"))
                      .limit(10),
              })
          .build();
  expectConcurrentOffers(
      {testMetadata_}, 2, [&] { toSingleNodePlan(logicalPlan); });
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

  toSingleNodePlan(lp::PlanBuilder(context_).tableScan("t").build());
  toSingleNodePlan(
      lp::PlanBuilder(context_)
          .values(ROW("a", BIGINT()), {velox::Variant::row({int64_t{1}})})
          .build());
  EXPECT_EQ(numCalls, 0);
}

TEST_F(ConnectorPushdownPassTest, tableWriteOffersOnlyItsInput) {
  testConnector_->addTable("source", ROW({"a"}, BIGINT()));
  testConnector_->addTable("target", ROW({"a"}, BIGINT()));
  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("source")
                         .limit(10)
                         .tableWrite("target", lp::WriteKind::kInsert, {"a"})
                         .build();

  size_t numCalls{0};
  testMetadata_->setPushdownMatcher([&](const Node& subtree) {
    ++numCalls;
    EXPECT_EQ(subtree.nodeType(), NodeType::kLimit);
    return std::vector<PushdownRoot>{};
  });

  toSingleNodePlan(logicalPlan);
  EXPECT_EQ(numCalls, 1);
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

  // Replacing only the recursive step would leave its state reference
  // unbound. Replacing the whole fixed point removes that reference safely.
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
  auto otherConnector = registerScopedConnector("other");
  testConnector_->addTable("t", ROW("a", BIGINT()));
  otherConnector.connector->addTable("u", ROW("b", BIGINT()));

  auto logicalPlan =
      lp::PlanBuilder(context_)
          .tableScan("t")
          .limit(10)
          .unionAll(
              lp::PlanBuilder(context_)
                  .tableScan(
                      std::string(otherConnector.connector->connectorId()),
                      kDefaultSchema,
                      std::string("u"))
                  .limit(10))
          .build();
  auto stolen = testConnector_->addTable("stolen", ROW("value", BIGINT()));
  folly::coro::Baton outsideReady;
  std::atomic<NodeCP> outside{nullptr};
  otherConnector.metadata->setAsyncPushdownMatcher(
      [&](connector::ConnectorSessionPtr,
          const Node& subtree) -> folly::coro::Task<std::vector<PushdownRoot>> {
        outside.store(&subtree);
        outsideReady.post();
        co_return std::vector<PushdownRoot>{};
      });
  testMetadata_->setAsyncPushdownMatcher(
      [&, stolen = std::move(stolen)](
          connector::ConnectorSessionPtr,
          const Node&) -> folly::coro::Task<std::vector<PushdownRoot>> {
        co_await outsideReady;
        co_return std::vector<PushdownRoot>{{outside.load(), stolen}};
      });

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan), "outside the offered subtree");
}

TEST_F(ConnectorPushdownPassTest, rejectsInvalidVirtualTables) {
  auto foreignConnector = registerScopedConnector("foreign");
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
      foreignConnector.connector->addTable(
          "foreign", ROW({"key", "total"}, BIGINT())),
      "belongs to a different connector");

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
