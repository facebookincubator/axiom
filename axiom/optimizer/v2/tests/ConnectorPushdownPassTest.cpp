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
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/v2/Node.h"
#include "folly/Function.h"
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

velox::RowTypePtr rowTypeOfOutputs(NodeCP node) {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  const auto& columns = node->outputColumns();
  names.reserve(columns.size());
  types.reserve(columns.size());
  for (ColumnCP column : columns) {
    names.emplace_back(column->name());
    types.push_back(queryCtx()->toTypePtr(column->value().type));
  }
  return velox::ROW(std::move(names), std::move(types));
}

// True iff any `Scan` under `node` belongs to the given connector.
bool subtreeReachesConnector(const Node* node, std::string_view connectorId) {
  if (node == nullptr) {
    return false;
  }
  if (node->is(NodeType::kScan)) {
    return node->as<Scan>()->baseTable()->schemaTable->connectorId() ==
        connectorId;
  }
  for (NodeCP input : node->inputs()) {
    if (subtreeReachesConnector(input, connectorId)) {
      return true;
    }
  }
  return false;
}

// Tests run the full v2 pipeline via `toSingleNodePlan` with `useV2_ = true`.
// The pushdown matcher receives the post-`PushdownAndPrunePass` v2 subtree, so
// tests find target nodes by walking that tree rather than by capturing LP
// nodes at plan-build time.
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

  // Registers a virtual pushdown table whose schema mirrors the outputs
  // of `subtree` 1:1, satisfying `PushdownRoot`'s consumer-column contract.
  connector::TablePtr addPushdownTable(std::string_view label, NodeCP subtree) {
    return testConnector_->addTable(
        std::string(label), rowTypeOfOutputs(subtree));
  }

  struct RegisteredConnector {
    RegisteredConnector(
        std::shared_ptr<connector::TestConnector> conn,
        connector::TestConnectorMetadata* meta,
        folly::Function<void()> unreg)
        : connector{std::move(conn)},
          metadata{meta},
          unregister{std::move(unreg)} {}

    RegisteredConnector(RegisteredConnector&&) = default;
    RegisteredConnector& operator=(RegisteredConnector&&) = default;
    RegisteredConnector(const RegisteredConnector&) = delete;
    RegisteredConnector& operator=(const RegisteredConnector&) = delete;

    ~RegisteredConnector() {
      if (unregister) {
        unregister();
      }
    }

    std::shared_ptr<connector::TestConnector> connector;
    connector::TestConnectorMetadata* metadata;
    folly::Function<void()> unregister;
  };

  // Registers a fresh `TestConnector` under `id` in both the connector and
  // metadata registries. The returned handle owns the deregistration; when
  // it goes out of scope, both registries are cleaned up.
  RegisteredConnector registerConnector(std::string_view id) {
    auto conn = std::make_shared<connector::TestConnector>(std::string(id));
    auto* meta =
        dynamic_cast<connector::TestConnectorMetadata*>(conn->metadata().get());
    VELOX_CHECK_NOT_NULL(meta);
    velox::connector::ConnectorRegistry::global().insert(
        conn->connectorId(), conn);
    connector::ConnectorMetadataRegistry::global().insert(
        std::string(id), conn->metadata());
    return {
        std::move(conn), meta, folly::Function<void()>{[id = std::string(id)] {
          connector::ConnectorMetadataRegistry::global().erase(id);
          velox::connector::ConnectorRegistry::global().erase(id);
        }}};
  }

  lp::PlanBuilder::Context context_{
      std::string(kTestConnectorId),
      kDefaultSchema};

  connector::TestConnectorMetadata* testMetadata_{nullptr};
};

TEST_F(ConnectorPushdownPassTest, replacesAggregate) {
  const auto schema = ROW({"a", "b"}, BIGINT());
  testConnector_->addTable("t", schema);

  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .aggregate({"a"}, {"sum(b)"})
                         .build();

  testMetadata_->setPushdownMatcher([this](const Node& subtree) {
    const auto* aggregate =
        findFirstDescendantOfType(&subtree, NodeType::kAggregate);
    VELOX_CHECK_NOT_NULL(aggregate);
    return std::vector<PushdownRoot>{
        {aggregate, addPushdownTable("u", aggregate)}};
  });

  auto plan = toSingleNodePlan(logicalPlan);
  AXIOM_ASSERT_PLAN(plan, matchScan("u").build());
}

TEST_F(ConnectorPushdownPassTest, replacesBuildSideOfCrossConnectorJoin) {
  auto probe = registerConnector("probe");

  const auto probeSchema = ROW({"a", "b"}, BIGINT());
  const auto buildSchema = ROW({"c", "d"}, BIGINT());
  probe.connector->addTable("p", probeSchema);
  testConnector_->addTable("q", buildSchema);

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

  // Offered subtrees live in the optimizer's `HashStringAllocator`,
  // freed when `planVelox` returns; verify shape inside the matcher.
  bool testMatcherCalled = false;
  bool testSubtreeReachesTest = false;
  bool testSubtreeReachesProbe = false;
  testMetadata_->setPushdownMatcher([&, this](const Node& subtree) {
    testMatcherCalled = true;
    testSubtreeReachesTest =
        subtreeReachesConnector(&subtree, kTestConnectorId);
    testSubtreeReachesProbe =
        subtreeReachesConnector(&subtree, probe.connector->connectorId());
    const auto* aggregate =
        findFirstDescendantOfType(&subtree, NodeType::kAggregate);
    VELOX_CHECK_NOT_NULL(aggregate);
    return std::vector<PushdownRoot>{
        {aggregate, addPushdownTable("virt_q", aggregate)}};
  });
  bool probeCalled = false;
  probe.metadata->setPushdownMatcher([&](const Node&) {
    probeCalled = true;
    return std::vector<PushdownRoot>{};
  });

  auto plan = toSingleNodePlan(logicalPlan);
  auto buildMatcher = matchScan("virt_q").build();
  auto matcher = matchScan("p").hashJoin(buildMatcher).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_TRUE(testMatcherCalled);
  EXPECT_TRUE(testSubtreeReachesTest);
  EXPECT_FALSE(testSubtreeReachesProbe);
  EXPECT_FALSE(probeCalled);
}

TEST_F(ConnectorPushdownPassTest, replacesBothSidesOfCrossConnectorJoin) {
  auto probe = registerConnector("probe");

  probe.connector->addTable("p", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("q", ROW({"c", "d"}, BIGINT()));

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

  probe.metadata->setPushdownMatcher([&](const Node& subtree) {
    const auto* aggregate =
        findFirstDescendantOfType(&subtree, NodeType::kAggregate);
    VELOX_CHECK_NOT_NULL(aggregate);
    return std::vector<PushdownRoot>{
        {aggregate,
         probe.connector->addTable("virt_p", rowTypeOfOutputs(aggregate))}};
  });
  testMetadata_->setPushdownMatcher([this](const Node& subtree) {
    const auto* aggregate =
        findFirstDescendantOfType(&subtree, NodeType::kAggregate);
    VELOX_CHECK_NOT_NULL(aggregate);
    return std::vector<PushdownRoot>{
        {aggregate, addPushdownTable("virt_q", aggregate)}};
  });

  auto plan = toSingleNodePlan(logicalPlan);
  auto matcher =
      matchScan("virt_p").hashJoin(matchScan("virt_q").build()).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(ConnectorPushdownPassTest, emptyMatcherResultIsNoOp) {
  const auto schema = ROW({"a", "b"}, BIGINT());
  testConnector_->addTable("t", schema);

  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .aggregate({"a"}, {"sum(b)"})
                         .build();

  testMetadata_->setPushdownMatcher(
      [](const Node&) { return std::vector<PushdownRoot>{}; });

  auto plan = toSingleNodePlan(logicalPlan);
  auto matcher = matchScan("t").singleAggregation({"a"}, {"sum(b)"}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(ConnectorPushdownPassTest, bareScanRootFails) {
  const auto schema = ROW({"a", "b"}, BIGINT());
  testConnector_->addTable("t", schema);

  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .aggregate({"a"}, {"sum(b)"})
                         .build();

  testMetadata_->setPushdownMatcher([this](const Node& subtree) {
    const auto* scan = findFirstDescendantOfType(&subtree, NodeType::kScan);
    VELOX_CHECK_NOT_NULL(scan);
    return std::vector<PushdownRoot>{{scan, addPushdownTable("u", scan)}};
  });

  VELOX_ASSERT_THROW(toSingleNodePlan(logicalPlan), "bare Scan node");
}

TEST_F(ConnectorPushdownPassTest, bareScanPlanSkipsMatcher) {
  const auto schema = ROW({"a", "b"}, BIGINT());
  testConnector_->addTable("t", schema);

  auto logicalPlan = lp::PlanBuilder(context_).tableScan("t").build();

  bool matcherCalled = false;
  testMetadata_->setPushdownMatcher([&](const Node&) {
    matcherCalled = true;
    return std::vector<PushdownRoot>{};
  });

  auto plan = toSingleNodePlan(logicalPlan);
  AXIOM_ASSERT_PLAN(plan, matchScan("t").build());
  EXPECT_FALSE(matcherCalled);
}

TEST_F(ConnectorPushdownPassTest, overlappingRootsFails) {
  const auto schema = ROW({"a", "b"}, BIGINT());
  testConnector_->addTable("t", schema);

  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .aggregate({"a"}, {"sum(b)"})
                         .project({"a + 1"})
                         .build();

  testMetadata_->setPushdownMatcher([this](const Node& subtree) {
    const auto* aggregate =
        findFirstDescendantOfType(&subtree, NodeType::kAggregate);
    VELOX_CHECK_NOT_NULL(aggregate);
    auto outerTable = addPushdownTable("outer", &subtree);
    auto innerTable = addPushdownTable("inner", aggregate);
    return std::vector<PushdownRoot>{
        {&subtree, std::move(outerTable)},
        {aggregate, std::move(innerTable)},
    };
  });

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan), "overlapping pushdown roots");
}

TEST_F(ConnectorPushdownPassTest, replacesStrictDescendantOfOfferedSubtree) {
  const auto schema = ROW({"a", "b"}, BIGINT());
  testConnector_->addTable("t", schema);

  // Filter references the aggregate output, so it stays above the Aggregate.
  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .aggregate({"a"}, {"sum(b) as s"})
                         .filter("s > 10")
                         .build();

  testMetadata_->setPushdownMatcher([this](const Node& subtree) {
    const auto* aggregate =
        findFirstDescendantOfType(&subtree, NodeType::kAggregate);
    VELOX_CHECK_NOT_NULL(aggregate);
    EXPECT_NE(&subtree, aggregate);
    return std::vector<PushdownRoot>{
        {aggregate, addPushdownTable("virt_agg", aggregate)}};
  });

  auto plan = toSingleNodePlan(logicalPlan);
  auto matcher = matchScan("virt_agg").filter("s > 10").build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(ConnectorPushdownPassTest, replacesMultipleDisjointRootsInOneResponse) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"c", "d"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .aggregate({"a"}, {"sum(b) as sb"})
                         .join(
                             lp::PlanBuilder(context_).tableScan("u").aggregate(
                                 {"c"}, {"sum(d) as sd"}),
                             "a = c",
                             lp::JoinType::kInner)
                         .build();

  testMetadata_->setPushdownMatcher([this](const Node& subtree) {
    const auto inputs = subtree.inputs();
    const auto* leftAgg =
        findFirstDescendantOfType(inputs[0], NodeType::kAggregate);
    const auto* rightAgg =
        findFirstDescendantOfType(inputs[1], NodeType::kAggregate);
    VELOX_CHECK_NOT_NULL(leftAgg);
    VELOX_CHECK_NOT_NULL(rightAgg);
    return std::vector<PushdownRoot>{
        {leftAgg, addPushdownTable("virt_left", leftAgg)},
        {rightAgg, addPushdownTable("virt_right", rightAgg)},
    };
  });

  auto plan = toSingleNodePlan(logicalPlan);
  auto buildMatcher = matchScan("virt_right").build();
  auto matcher = matchScan("virt_left").hashJoin(buildMatcher).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(ConnectorPushdownPassTest, rootOutsideOfferedSubtreeFails) {
  auto probe = registerConnector("probe");

  probe.connector->addTable("p", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("q", ROW({"c", "d"}, BIGINT()));

  auto logicalPlan =
      lp::PlanBuilder(context_)
          .tableScan(
              std::string(probe.connector->connectorId()),
              kDefaultSchema,
              std::string("p"))
          .filter("a > 0")
          .join(
              lp::PlanBuilder(context_).tableScan("q").filter("c > 0"),
              "a = c",
              lp::JoinType::kInner)
          .build();

  // Walk visits the left leg first, so probe's matcher captures before
  // test's matcher can reference it.
  const Node* seenByProbe = nullptr;
  probe.metadata->setPushdownMatcher([&](const Node& subtree) {
    seenByProbe = &subtree;
    return std::vector<PushdownRoot>{};
  });
  testMetadata_->setPushdownMatcher([&, this](const Node&) {
    VELOX_CHECK_NOT_NULL(seenByProbe);
    auto stolen =
        testConnector_->addTable("stolen", rowTypeOfOutputs(seenByProbe));
    return std::vector<PushdownRoot>{{seenByProbe, std::move(stolen)}};
  });

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan), "outside the offered subtree");
}

TEST_F(ConnectorPushdownPassTest, syntheticScanRejectsMissingColumn) {
  const auto schema = ROW({"a", "b"}, BIGINT());
  testConnector_->addTable("t", schema);

  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .aggregate({"a"}, {"sum(b) as s"})
                         .build();

  testMetadata_->setPushdownMatcher([this](const Node& subtree) {
    const auto* aggregate =
        findFirstDescendantOfType(&subtree, NodeType::kAggregate);
    VELOX_CHECK_NOT_NULL(aggregate);
    auto badTable =
        testConnector_->addTable("bad_missing", ROW({"a"}, BIGINT()));
    return std::vector<PushdownRoot>{{aggregate, std::move(badTable)}};
  });

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan),
      "Pushdown virtual table missing column expected by consumer");
}

TEST_F(ConnectorPushdownPassTest, syntheticScanRejectsTypeMismatch) {
  const auto schema = ROW({"a", "b"}, BIGINT());
  testConnector_->addTable("t", schema);

  auto logicalPlan = lp::PlanBuilder(context_)
                         .tableScan("t")
                         .aggregate({"a"}, {"sum(b) as s"})
                         .build();

  testMetadata_->setPushdownMatcher([this](const Node& subtree) {
    const auto* aggregate =
        findFirstDescendantOfType(&subtree, NodeType::kAggregate);
    VELOX_CHECK_NOT_NULL(aggregate);
    std::vector<std::string> names;
    std::vector<velox::TypePtr> types;
    for (ColumnCP column : aggregate->outputColumns()) {
      names.emplace_back(column->name());
      types.push_back(velox::VARCHAR());
    }
    auto badTable = testConnector_->addTable(
        "bad_type", velox::ROW(std::move(names), std::move(types)));
    return std::vector<PushdownRoot>{{aggregate, std::move(badTable)}};
  });

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan),
      "Pushdown virtual table column type mismatch");
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
