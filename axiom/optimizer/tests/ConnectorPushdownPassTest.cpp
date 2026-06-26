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
#include "axiom/optimizer/ConnectorPushdownPass.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "folly/coro/BlockingWait.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;

namespace facebook::axiom::optimizer {
namespace {

namespace lp = logical_plan;
using connector::PushdownRoot;
using PushdownMatcher = connector::TestConnectorMetadata::PushdownMatcher;

// Returns the first PushdownRoot in 'roots' whose 'root' equals 'node',
// or nullptr if none.
const PushdownRoot* findRoot(
    const std::vector<PushdownRoot>& roots,
    const lp::LogicalPlanNode* node) {
  for (const auto& root : roots) {
    if (root.root == node) {
      return &root;
    }
  }
  return nullptr;
}

class ConnectorPushdownPassTest : public testing::Test {
 protected:
  static constexpr auto kConnectorA = "connector_a";
  static constexpr auto kConnectorB = "connector_b";

  // A registered connector together with its TestConnectorMetadata,
  // returned by 'addConnector'.
  struct RegisteredConnector {
    std::shared_ptr<connector::TestConnector> connector;
    connector::TestConnectorMetadata* metadata;
  };

  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    functions::prestosql::registerAllScalarFunctions();
  }

  void SetUp() override {
    auto a = addConnector(kConnectorA);
    connectorA_ = std::move(a.connector);
    metadataA_ = a.metadata;
    auto b = addConnector(kConnectorB);
    connectorB_ = std::move(b.connector);
    metadataB_ = b.metadata;

    const auto schema = ROW({"a", "b"}, BIGINT());
    connectorA_->addTable("t", schema);
    connectorB_->addTable("u", schema);
  }

  void TearDown() override {
    removeConnector(kConnectorA);
    removeConnector(kConnectorB);
    connectorA_.reset();
    connectorB_.reset();
    metadataA_ = nullptr;
    metadataB_ = nullptr;
  }

  // Creates a TestConnector under 'connectorId', inserts it into the
  // connector and metadata registries, and returns it together with
  // its typed metadata so callers can wire matchers without per-call
  // dynamic_cast.
  static RegisteredConnector addConnector(std::string_view connectorId) {
    auto connector =
        std::make_shared<connector::TestConnector>(std::string(connectorId));
    auto* metadata = dynamic_cast<connector::TestConnectorMetadata*>(
        connector->metadata().get());
    VELOX_CHECK_NOT_NULL(metadata);
    velox::connector::ConnectorRegistry::global().insert(
        connector->connectorId(), connector);
    connector::ConnectorMetadataRegistry::global().insert(
        std::string(connectorId), connector->metadata());
    return {std::move(connector), metadata};
  }

  static void removeConnector(std::string_view connectorId) {
    connector::ConnectorMetadataRegistry::global().erase(
        std::string(connectorId));
    velox::connector::ConnectorRegistry::global().erase(
        std::string(connectorId));
  }

  // Registers a fresh TestTable on 'conn' under 'label' and returns it
  // as a connector::TablePtr suitable for `PushdownRoot::table`.
  connector::TablePtr makePushdownTable(
      connector::TestConnector* conn,
      std::string_view label) {
    static const auto kSchema = ROW({"a", "b"}, BIGINT());
    return conn->addTable(std::string(label), kSchema);
  }

  static PushdownMatcher onePushdownRoot(
      const lp::LogicalPlanNode* root,
      connector::TablePtr table) {
    return [root, table](const lp::LogicalPlanNode&) {
      return std::vector<PushdownRoot>{{root, table}};
    };
  }

  // Synchronously drives the pushdown pass over 'plan'.
  static std::vector<PushdownRoot> collectRoots(
      const lp::LogicalPlanNode& plan) {
    return folly::coro::blockingWait(collectConnectorPushdownRoots(plan));
  }

  // Builds a scan over 'tableName' on 'connectorId' in the fixture's
  // default schema. Threads the shared 'context_' so node IDs and
  // column names stay globally unique across legs of the same plan.
  // Exists because PlanBuilder::tableScan(connectorId, schema, table)
  // overload-resolves ambiguously when the table-name arg is a string
  // literal; this helper picks the right overload and absorbs the
  // std::string conversions.
  lp::PlanBuilder scanOn(
      std::string_view connectorId,
      std::string_view tableName,
      std::string_view schema = connector::TestConnector::kDefaultSchema) {
    return lp::PlanBuilder(context_).tableScan(
        std::string(connectorId), std::string(schema), std::string(tableName));
  }

  // Shared across all builders within one test so plan-node IDs and
  // column names stay globally unique. Default connector is A; reach
  // other connectors via 'scanOn'.
  lp::PlanBuilder::Context context_{
      std::string(kConnectorA),
      std::string(connector::TestConnector::kDefaultSchema)};
  std::shared_ptr<connector::TestConnector> connectorA_;
  std::shared_ptr<connector::TestConnector> connectorB_;
  connector::TestConnectorMetadata* metadataA_{nullptr};
  connector::TestConnectorMetadata* metadataB_{nullptr};
};

TEST_F(ConnectorPushdownPassTest, absorbsRoot) {
  // Plan root is a Filter (a non-scan node) so the connector is
  // executing real work above the scan.
  auto plan =
      lp::PlanBuilder(context_).tableScan("t").filter("a > 0").planNode();
  auto table = makePushdownTable(connectorA_.get(), "v");
  metadataA_->setPushdownMatcher(onePushdownRoot(plan.get(), table));

  auto roots = collectRoots(*plan);
  ASSERT_THAT(roots, testing::SizeIs(1));
  EXPECT_EQ(roots[0].root, plan.get());
  EXPECT_EQ(roots[0].table.get(), table.get());
}

TEST_F(ConnectorPushdownPassTest, absorbsDeepSubtree) {
  // Inner Filter is the absorbed root (non-scan, executes work above
  // the scan).
  const lp::LogicalPlanNode* innerFilter = nullptr;
  auto plan = lp::PlanBuilder(context_)
                  .tableScan("t")
                  .filter("a > 0")
                  .capturePlanNode(&innerFilter)
                  .filter("b > 0")
                  .planNode();
  auto table = makePushdownTable(connectorA_.get(), "v");
  metadataA_->setPushdownMatcher(onePushdownRoot(innerFilter, table));

  auto roots = collectRoots(*plan);
  ASSERT_THAT(roots, testing::SizeIs(1));
  EXPECT_EQ(roots[0].root, innerFilter);
  EXPECT_EQ(roots[0].table.get(), table.get());
}

TEST_F(ConnectorPushdownPassTest, crossConnectorRoots) {
  // Each leg is a Filter over its scan — a real non-scan root the
  // connector can absorb.
  const lp::LogicalPlanNode* filterA = nullptr;
  const lp::LogicalPlanNode* filterB = nullptr;
  auto plan = lp::PlanBuilder(context_)
                  .tableScan("t")
                  .filter("a > 0")
                  .capturePlanNode(&filterA)
                  .unionAll(scanOn(kConnectorB, "u")
                                .filter("a > 0")
                                .capturePlanNode(&filterB))
                  .planNode();
  auto tableA = makePushdownTable(connectorA_.get(), "v");
  auto tableB = makePushdownTable(connectorB_.get(), "w");

  metadataA_->setPushdownMatcher(onePushdownRoot(filterA, tableA));
  metadataB_->setPushdownMatcher(onePushdownRoot(filterB, tableB));

  auto roots = collectRoots(*plan);
  ASSERT_THAT(roots, testing::SizeIs(2));
  const auto* foundA = findRoot(roots, filterA);
  const auto* foundB = findRoot(roots, filterB);
  ASSERT_NE(foundA, nullptr);
  ASSERT_NE(foundB, nullptr);
  EXPECT_EQ(foundA->table.get(), tableA.get());
  EXPECT_EQ(foundB->table.get(), tableB.get());
}

TEST_F(ConnectorPushdownPassTest, notSupportedSkipped) {
  // No matcher installed -> connector is opted out of pushdown and the
  // pass returns no roots.
  auto plan = lp::PlanBuilder(context_).tableScan("t").planNode();
  EXPECT_THAT(collectRoots(*plan), testing::IsEmpty());
}

TEST_F(ConnectorPushdownPassTest, noPushdownRootsReturnsEmpty) {
  auto plan = lp::PlanBuilder(context_).tableScan("t").planNode();
  metadataA_->setPushdownMatcher(
      [](const lp::LogicalPlanNode&) { return std::vector<PushdownRoot>{}; });
  EXPECT_THAT(collectRoots(*plan), testing::IsEmpty());
}

TEST_F(ConnectorPushdownPassTest, nestedRootsFails) {
  const lp::LogicalPlanNode* innerFilter = nullptr;
  auto plan = lp::PlanBuilder(context_)
                  .tableScan("t")
                  .filter("a > 0")
                  .capturePlanNode(&innerFilter)
                  .filter("b > 0")
                  .planNode();
  auto outer = makePushdownTable(connectorA_.get(), "v");
  auto inner = makePushdownTable(connectorA_.get(), "w");

  metadataA_->setPushdownMatcher(
      [innerFilter, outer, inner](const lp::LogicalPlanNode& subtree) {
        return std::vector<PushdownRoot>{
            {&subtree, outer},
            {innerFilter, inner},
        };
      });

  VELOX_ASSERT_THROW(collectRoots(*plan), "overlapping pushdown roots");
}

TEST_F(ConnectorPushdownPassTest, pushdownRootOutsideSubtreeFails) {
  // Each connector receives only the maximal subtree referencing it.
  // A connector that returns a root pointing outside that subtree is
  // malformed; the pass must reject it.
  auto plan = lp::PlanBuilder(context_)
                  .tableScan("t")
                  .unionAll(scanOn(kConnectorB, "u"))
                  .planNode();
  auto stolen = makePushdownTable(connectorA_.get(), "v");

  // Connector A's matcher returns the UnionAll root (not within A's
  // subtree, which is just the scan). Connector B does not install a
  // matcher, so it stays opted out.
  metadataA_->setPushdownMatcher(
      [planPtr = plan.get(), stolen](const lp::LogicalPlanNode&) {
        return std::vector<PushdownRoot>{{planPtr, stolen}};
      });

  VELOX_ASSERT_THROW(collectRoots(*plan), "outside the subtree it was given");
}

TEST_F(ConnectorPushdownPassTest, barePushdownRootFails) {
  // A bare TableScanNode root is a no-op pushdown: the scan already
  // runs on the connector. The pass rejects it as a connector bug.
  auto plan = lp::PlanBuilder(context_).tableScan("t").planNode();
  auto table = makePushdownTable(connectorA_.get(), "v");
  metadataA_->setPushdownMatcher(onePushdownRoot(plan.get(), table));

  VELOX_ASSERT_THROW(collectRoots(*plan), "bare TableScanNode");
}

TEST_F(ConnectorPushdownPassTest, federatedPlanScopedToOwnSubtree) {
  const lp::LogicalPlanNode* scanANode = nullptr;
  const lp::LogicalPlanNode* scanBNode = nullptr;
  auto plan =
      lp::PlanBuilder(context_)
          .tableScan("t")
          .capturePlanNode(&scanANode)
          .unionAll(scanOn(kConnectorB, "u").capturePlanNode(&scanBNode))
          .planNode();

  // Record which subtree each connector received.
  const lp::LogicalPlanNode* seenByA = nullptr;
  const lp::LogicalPlanNode* seenByB = nullptr;
  metadataA_->setPushdownMatcher(
      [&seenByA](const lp::LogicalPlanNode& subtree) {
        seenByA = &subtree;
        return std::vector<PushdownRoot>{};
      });
  metadataB_->setPushdownMatcher(
      [&seenByB](const lp::LogicalPlanNode& subtree) {
        seenByB = &subtree;
        return std::vector<PushdownRoot>{};
      });

  collectRoots(*plan);
  EXPECT_EQ(seenByA, scanANode);
  EXPECT_EQ(seenByB, scanBNode);
}

TEST_F(ConnectorPushdownPassTest, singleConnectorUnionAbsorbedAtUnion) {
  // Both children of the Union belong to connector A. The maximal
  // single-connector subtree is the Union itself, not the individual
  // scans — connector A receives the Union.
  auto plan = lp::PlanBuilder(context_)
                  .tableScan("t")
                  .unionAll(lp::PlanBuilder(context_).tableScan("t"))
                  .planNode();

  const lp::LogicalPlanNode* seenByA = nullptr;
  metadataA_->setPushdownMatcher(
      [&seenByA](const lp::LogicalPlanNode& subtree) {
        seenByA = &subtree;
        return std::vector<PushdownRoot>{};
      });

  collectRoots(*plan);
  EXPECT_EQ(seenByA, plan.get());
}

TEST_F(ConnectorPushdownPassTest, noTableScansReturnsEmpty) {
  // No TableScanNodes in the plan — no connectors to ask, even though
  // a matcher is registered.
  bool matcherCalled = false;
  metadataA_->setPushdownMatcher([&matcherCalled](const lp::LogicalPlanNode&) {
    matcherCalled = true;
    return std::vector<PushdownRoot>{};
  });

  auto plan = lp::PlanBuilder(context_).values({"x"}, {{"1"}}).planNode();
  EXPECT_THAT(collectRoots(*plan), testing::IsEmpty());
  EXPECT_FALSE(matcherCalled);
}

} // namespace
} // namespace facebook::axiom::optimizer
