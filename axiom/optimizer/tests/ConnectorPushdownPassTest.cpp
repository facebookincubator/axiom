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
#include "axiom/connectors/tests/TestConnectorContext.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "axiom/sql/presto/SqlStatement.h"
#include "folly/coro/CurrentExecutor.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/ConnectorRegistry.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;
using namespace facebook::axiom::optimizer::v2;
using connector::PushdownRoot;

NodeCP requireNodeOfType(NodeCP root, NodeType target) {
  NodeCP found = Node::findFirstNode(
      root, [target](NodeCP node) { return node->is(target); });
  VELOX_CHECK_NOT_NULL(found);
  return found;
}

bool containsScanFromConnector(const Node* node, std::string_view connectorId) {
  return Node::findFirstNode(node, [&](NodeCP candidate) {
           return candidate->is(NodeType::kScan) &&
               candidate->as<Scan>()->baseTable()->schemaTable->connectorId() ==
               connectorId;
         }) != nullptr;
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

    ScopedConnectorRegistration(ScopedConnectorRegistration&&) = default;
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
    return parseSelect(
        std::string{"SELECT a, "} + std::string{aggregate} + " FROM " +
            std::string{tableName} + " GROUP BY a",
        kTestConnectorId);
  }

  logical_plan::LogicalPlanNodePtr recursivePlan() {
    return parseSelect(
        "WITH RECURSIVE counter(n) AS ("
        "SELECT max(a) FROM seed "
        "UNION ALL SELECT n - 1 FROM counter WHERE n > 0) "
        "SELECT n FROM counter",
        kTestConnectorId);
  }

  logical_plan::LogicalPlanNodePtr parseInsert(std::string_view sql) {
    ::axiom::sql::presto::PrestoParser parser(
        kTestConnectorId,
        kDefaultSchema,
        std::make_shared<::axiom::sql::presto::ParserSession>(
            connector::makeTestContext("test"),
            connector::makeTestStatWriter(),
            connector::Properties{},
            ::axiom::sql::presto::ParserOptions{}));
    auto statement = parser.parse(sql);
    VELOX_CHECK(statement->isInsert());
    return statement->as<::axiom::sql::presto::InsertStatement>()->plan();
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

  connector::TestConnectorMetadata* testMetadata_{nullptr};
};

TEST_F(ConnectorPushdownPassTest, positionalOutput) {
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

TEST_F(ConnectorPushdownPassTest, workerSelection) {
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

TEST_F(ConnectorPushdownPassTest, schemaResolver) {
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

TEST_F(ConnectorPushdownPassTest, connectorSession) {
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

TEST_F(ConnectorPushdownPassTest, crossConnectorJoin) {
  auto probe = registerScopedConnector("probe");

  const auto probeSchema = ROW({"a", "b"}, BIGINT());
  const auto buildSchema = ROW({"c", "d"}, BIGINT());
  probe.connector->addTable("p", probeSchema);
  testConnector_->addTable("q", buildSchema);
  auto replacement =
      testConnector_->addTable("virt_q", ROW({"key", "total"}, BIGINT()));

  auto logicalPlan = parseSelect(
      "SELECT p.a, p.b, q.c, q.sd "
      "FROM probe.default.p p "
      "JOIN (SELECT c, sum(d) AS sd FROM q GROUP BY c) q ON p.a = q.c",
      kTestConnectorId);

  testMetadata_->setPushdownMatcher([&, replacement](const Node& subtree) {
    EXPECT_TRUE(containsScanFromConnector(&subtree, kTestConnectorId));
    EXPECT_FALSE(
        containsScanFromConnector(&subtree, probe.connector->connectorId()));
    const auto* aggregate = requireNodeOfType(&subtree, NodeType::kAggregate);
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

TEST_F(ConnectorPushdownPassTest, duplicateSourceNames) {
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
  testMetadata_->setPushdownMatcher(
      [virtualTable = std::move(virtualTable)](const Node& subtree) {
        const auto* join = requireNodeOfType(&subtree, NodeType::kJoin);
        return std::vector<PushdownRoot>{{join, virtualTable}};
      });

  checkSame(logicalPlan, {expected});
}

TEST_F(ConnectorPushdownPassTest, independentConnectors) {
  auto probe = registerScopedConnector("probe");

  probe.connector->addTable("p", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("q", ROW({"c", "d"}, BIGINT()));
  auto probeReplacement =
      probe.connector->addTable("virt_p", ROW({"key", "total"}, BIGINT()));
  auto testReplacement =
      testConnector_->addTable("virt_q", ROW({"key", "total"}, BIGINT()));

  auto logicalPlan = parseSelect(
      "SELECT p.a, p.sb, q.c, q.sd "
      "FROM (SELECT a, sum(b) AS sb FROM probe.default.p GROUP BY a) p "
      "JOIN (SELECT c, sum(d) AS sd FROM q GROUP BY c) q ON p.a = q.c",
      kTestConnectorId);

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
            requireNodeOfType(&subtree, NodeType::kAggregate);
        return std::vector<PushdownRoot>{{
            aggregate,
            metadata == probe.metadata ? probeReplacement : testReplacement,
        }};
      });
}

TEST_F(ConnectorPushdownPassTest, concurrentOffers) {
  auto otherConnector = registerScopedConnector("other");
  testConnector_->addTable("left_input", ROW({"a"}, BIGINT()));
  testConnector_->addTable("right_input", ROW({"b"}, BIGINT()));
  otherConnector.connector->addTable("u", ROW({"c"}, BIGINT()));

  // The other connector keeps the two local inputs as separate offers.
  auto logicalPlan = parseSelect(
      "(SELECT a FROM left_input LIMIT 10) "
      "UNION ALL (SELECT b FROM right_input LIMIT 10) "
      "UNION ALL (SELECT c FROM other.default.u LIMIT 10)",
      kTestConnectorId);
  expectConcurrentOffers(
      {testMetadata_}, 2, [&] { toSingleNodePlan(logicalPlan); });
}

TEST_F(ConnectorPushdownPassTest, bareScan) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  auto replacement =
      testConnector_->addTable("u", ROW({"first", "second"}, BIGINT()));
  auto logicalPlan = aggregatePlan();

  testMetadata_->setPushdownMatcher([replacement](const Node& subtree) {
    const auto* scan = requireNodeOfType(&subtree, NodeType::kScan);
    return std::vector<PushdownRoot>{{scan, replacement}};
  });

  VELOX_ASSERT_THROW(toSingleNodePlan(logicalPlan), "cannot be a Scan");
}

TEST_F(ConnectorPushdownPassTest, ineligiblePlans) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  size_t numCalls{0};
  testMetadata_->setPushdownMatcher([&](const Node&) {
    ++numCalls;
    return std::vector<PushdownRoot>{};
  });

  toSingleNodePlan(parseSelect("SELECT * FROM t", kTestConnectorId));
  toSingleNodePlan(parseSelect("SELECT 1 AS a", kTestConnectorId));
  EXPECT_EQ(numCalls, 0);
}

TEST_F(ConnectorPushdownPassTest, tableWrite) {
  testConnector_->addTable("source", ROW({"a"}, BIGINT()));
  testConnector_->addTable("target", ROW({"a"}, BIGINT()));
  auto logicalPlan =
      parseInsert("INSERT INTO target SELECT a FROM source LIMIT 10");

  size_t numCalls{0};
  testMetadata_->setPushdownMatcher([&](const Node& subtree) {
    ++numCalls;
    EXPECT_EQ(subtree.nodeType(), NodeType::kLimit);
    return std::vector<PushdownRoot>{};
  });

  toSingleNodePlan(logicalPlan);
  EXPECT_EQ(numCalls, 1);
}

TEST_F(ConnectorPushdownPassTest, recursiveAnchor) {
  testConnector_->addTable("seed", ROW({"a"}, BIGINT()));
  auto replacement =
      testConnector_->addTable("virt_seed", ROW("value", BIGINT()));

  auto logicalPlan = recursivePlan();

  testMetadata_->setPushdownMatcher([replacement](const Node& subtree) {
    const auto* fixedPoint = subtree.as<FixedPoint>();
    return std::vector<PushdownRoot>{{fixedPoint->anchor(), replacement}};
  });

  auto matcher = core::PlanMatcherBuilder()
                     .fixedPoint(
                         core::FixedPointMatch("counter")
                             .outputState(
                                 /*append=*/true,
                                 matchScan("virt_seed")
                                     .aliases({"value"})
                                     .project({"value"}))
                             .plan(
                                 core::PlanMatcherBuilder()
                                     .stateSource("counter", /*delta=*/true)
                                     .aliases({"n"})
                                     .filter("n > 0")
                                     .project({"n - 1"}))
                             .convergeOnEmpty())
                     .aliases({"n"})
                     .project({"n"})
                     .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_F(ConnectorPushdownPassTest, recursiveRoots) {
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

TEST_F(ConnectorPushdownPassTest, conflictingRoots) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  auto firstTable =
      testConnector_->addTable("first", ROW({"key", "total"}, BIGINT()));
  auto secondTable =
      testConnector_->addTable("second", ROW({"key", "total"}, BIGINT()));
  auto outerTable = testConnector_->addTable("outer", ROW("value", BIGINT()));

  auto logicalPlan = parseSelect(
      "SELECT a + 1 FROM (SELECT a, sum(b) FROM t GROUP BY a)",
      kTestConnectorId);

  testMetadata_->setPushdownMatcher([firstTable,
                                     secondTable](const Node& subtree) {
    const auto* aggregate = requireNodeOfType(&subtree, NodeType::kAggregate);
    return std::vector<PushdownRoot>{
        {aggregate, firstTable},
        {aggregate, secondTable},
    };
  });

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan), "Pushdown root was returned twice");

  testMetadata_->setPushdownMatcher([outerTable,
                                     firstTable](const Node& subtree) {
    const auto* aggregate = requireNodeOfType(&subtree, NodeType::kAggregate);
    return std::vector<PushdownRoot>{
        {&subtree, outerTable},
        {aggregate, firstTable},
    };
  });

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan), "Pushdown roots cannot be nested");
}

TEST_F(ConnectorPushdownPassTest, strictDescendant) {
  const auto schema = ROW({"a", "b"}, BIGINT());
  testConnector_->addTable("t", schema);
  auto replacement =
      testConnector_->addTable("virt_agg", ROW({"key", "total"}, BIGINT()));

  auto planWithFilterDependingOnAggregate = parseSelect(
      "SELECT a, s FROM (SELECT a, sum(b) AS s FROM t GROUP BY a) "
      "WHERE s > 10",
      kTestConnectorId);

  testMetadata_->setPushdownMatcher([replacement](const Node& subtree) {
    const auto* aggregate = requireNodeOfType(&subtree, NodeType::kAggregate);
    EXPECT_NE(&subtree, aggregate);
    return std::vector<PushdownRoot>{{aggregate, replacement}};
  });

  auto plan = toSingleNodePlan(planWithFilterDependingOnAggregate);
  auto matcher = matchScan("virt_agg").project().filter("s > 10").build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(ConnectorPushdownPassTest, disjointRoots) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"c", "d"}, BIGINT()));
  auto leftReplacement =
      testConnector_->addTable("virt_left", ROW({"key", "total"}, BIGINT()));
  auto rightReplacement =
      testConnector_->addTable("virt_right", ROW({"key", "total"}, BIGINT()));

  auto logicalPlan = parseSelect(
      "SELECT l.a, l.sb, r.c, r.sd "
      "FROM (SELECT a, sum(b) AS sb FROM t GROUP BY a) l "
      "JOIN (SELECT c, sum(d) AS sd FROM u GROUP BY c) r ON l.a = r.c",
      kTestConnectorId);

  testMetadata_->setPushdownMatcher([leftReplacement,
                                     rightReplacement](const Node& subtree) {
    const auto inputs = subtree.inputs();
    const auto* leftAgg = requireNodeOfType(inputs[0], NodeType::kAggregate);
    const auto* rightAgg = requireNodeOfType(inputs[1], NodeType::kAggregate);
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

TEST_F(ConnectorPushdownPassTest, outsideRoot) {
  auto otherConnector = registerScopedConnector("other");
  testConnector_->addTable("t", ROW("a", BIGINT()));
  otherConnector.connector->addTable("u", ROW("b", BIGINT()));

  auto logicalPlan = parseSelect(
      "(SELECT a FROM t LIMIT 10) "
      "UNION ALL (SELECT b FROM other.default.u LIMIT 10)",
      kTestConnectorId);
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

TEST_F(ConnectorPushdownPassTest, invalidVirtualTables) {
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
} // namespace facebook::axiom::optimizer::test
