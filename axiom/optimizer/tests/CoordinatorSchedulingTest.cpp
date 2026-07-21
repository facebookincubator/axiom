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

#include <gmock/gmock.h>
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/connectors/ConnectorRegistry.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;

class CoordinatorSchedulingTest : public test::QueryTestBase {
 protected:
  static constexpr const char* kSystemConnectorIdForTest = "coordinator_system";
  static constexpr const char* kMetadataConnectorIdForTest =
      "coordinator_metadata";

  void SetUp() override {
    QueryTestBase::SetUp();
    optimizerOptions_.systemConnectorId = kSystemConnectorIdForTest;

    testConnector_->addTable("t", ROW("a", BIGINT()))
        ->setStats(100'000, {{"a", {.numDistinct = 1'000}}});

    systemConnector_ = registerConnector(kSystemConnectorIdForTest);
    systemConnector_->addTable("s", ROW("c", BIGINT()))
        ->setStats(1'000, {{"c", {.numDistinct = 100}}});

    metadataConnector_ = registerConnector(kMetadataConnectorIdForTest);
    metadataConnector_->addTable("m", ROW("d", BIGINT()))
        ->setStats(1'000, {{"d", {.numDistinct = 100}}});
  }

  void TearDown() override {
    if (metadataConnector_) {
      unregisterConnector(kMetadataConnectorIdForTest);
      metadataConnector_.reset();
    }
    if (systemConnector_) {
      unregisterConnector(kSystemConnectorIdForTest);
      systemConnector_.reset();
    }
    QueryTestBase::TearDown();
  }

  // Registers a TestConnector in both global registries before exposing it to
  // the caller, so a throw mid-registration cannot leave a half-registered id.
  static std::shared_ptr<connector::TestConnector> registerConnector(
      const std::string& connectorId) {
    auto connector = std::make_shared<connector::TestConnector>(connectorId);
    velox::connector::ConnectorRegistry::global().insert(
        connectorId, connector);
    connector::ConnectorMetadataRegistry::global().insert(
        connectorId, connector->metadata());
    return connector;
  }

  static void unregisterConnector(const std::string& connectorId) {
    connector::ConnectorMetadataRegistry::global().erase(connectorId);
    velox::connector::ConnectorRegistry::global().erase(connectorId);
  }

  static MultiFragmentPlan::Options distributed() {
    return {.numWorkers = 4, .numDrivers = 4};
  }

  std::shared_ptr<connector::TestConnector> systemConnector_;
  std::shared_ptr<connector::TestConnector> metadataConnector_;
};

// A standalone system scan is one kCoordinator fragment.
TEST_F(CoordinatorSchedulingTest, systemScanUsesCoordinatorFragment) {
  auto plan = planVelox(
      parseSelect("FROM coordinator_system.default.s", kTestConnectorId),
      distributed());

  auto matcher =
      matchScan("s").fragmentType(FragmentType::kCoordinator).build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// A filter and projection over a system scan stay in one kCoordinator fragment.
TEST_F(
    CoordinatorSchedulingTest,
    filterProjectOverSystemScanUsesCoordinatorFragment) {
  auto plan = planVelox(
      parseSelect(
          "SELECT c + 1 FROM coordinator_system.default.s WHERE c < 50",
          kTestConnectorId),
      distributed());

  auto matcher = matchScan("s")
                     .filter("c < 50")
                     .project()
                     .fragmentType(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// ORDER BY over a system scan sorts inline in the kCoordinator fragment with no
// remote merge exchange.
TEST_F(
    CoordinatorSchedulingTest,
    orderByOverSystemScanUsesCoordinatorFragment) {
  auto plan = planVelox(
      parseSelect(
          "SELECT c FROM coordinator_system.default.s ORDER BY c",
          kTestConnectorId),
      distributed());

  auto matcher = matchScan("s")
                     .orderBy()
                     .localMerge()
                     .fragmentType(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// LIMIT over an already-gathered system scan runs inline (partial + local
// gather + final) in the kCoordinator fragment, with no remote gather.
TEST_F(CoordinatorSchedulingTest, limitOverSystemScanUsesCoordinatorFragment) {
  auto plan = planVelox(
      parseSelect(
          "SELECT c FROM coordinator_system.default.s LIMIT 10",
          kTestConnectorId),
      distributed());

  auto matcher = matchScan("s")
                     .localLimit(0, 10)
                     .fragmentType(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// ORDER BY ... LIMIT over a system scan keeps the sort and limit together in
// one kCoordinator fragment (TopN + local merge + final limit), no remote
// merge exchange.
TEST_F(
    CoordinatorSchedulingTest,
    orderByLimitOverSystemScanUsesCoordinatorFragment) {
  auto plan = planVelox(
      parseSelect(
          "SELECT c FROM coordinator_system.default.s ORDER BY c LIMIT 10",
          kTestConnectorId),
      distributed());

  auto matcher = matchScan("s")
                     .topN()
                     .localMerge()
                     .limit()
                     .fragmentType(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// A cardinality-reducing aggregation over a system scan stays with the scan in
// one kCoordinator fragment (partial + local gather + final).
TEST_F(
    CoordinatorSchedulingTest,
    aggregationOverSystemScanUsesCoordinatorFragment) {
  auto plan = planVelox(
      parseSelect(
          "SELECT count(*) FROM coordinator_system.default.s",
          kTestConnectorId),
      distributed());

  auto matcher = matchScan("s")
                     .partialAggregation()
                     .localGather()
                     .finalAggregation()
                     .fragmentType(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// UNION ALL of a system scan with single-task Values co-locates both legs in
// one kCoordinator fragment; the Values leg runs on the coordinator.
TEST_F(CoordinatorSchedulingTest, unionOfSystemScanAndValues) {
  auto plan = planVelox(
      parseSelect(
          "SELECT c AS a FROM coordinator_system.default.s UNION ALL VALUES 1",
          kTestConnectorId),
      distributed());

  auto matcher = matchScan("s")
                     .project()
                     .localPartition(matchValues().project().build())
                     .fragmentType(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// UNION ALL of two system scans co-locates both in one kCoordinator fragment.
TEST_F(CoordinatorSchedulingTest, unionOfTwoSystemScans) {
  auto plan = planVelox(
      parseSelect(
          "SELECT c FROM coordinator_system.default.s "
          "UNION ALL SELECT c FROM coordinator_system.default.s",
          kTestConnectorId),
      distributed());

  auto matcher = matchScan("s")
                     .localPartition(matchScan("s").project().build())
                     .fragmentType(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// UNION ALL of a system scan with a parallel scan isolates the system scan in
// its own kCoordinator fragment behind an arbitrary exchange; the parallel scan
// keeps full parallelism in a kSource fragment feeding a kSingle root.
TEST_F(CoordinatorSchedulingTest, unionOfSystemScanAndScan) {
  auto plan = planVelox(
      parseSelect(
          "SELECT c AS a FROM coordinator_system.default.s UNION ALL FROM t",
          kTestConnectorId),
      distributed());

  auto matcher = matchScan("s")
                     .project()
                     .fragmentType(FragmentType::kCoordinator)
                     .arbitrary()
                     .localPartition(matchScan("t").project().build())
                     .fragmentType(FragmentType::kSource)
                     .gather()
                     .fragmentType(FragmentType::kSingle)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// The system connector id is configurable, not hardcoded: a scan of the
// separately-configured metadata connector is a kCoordinator fragment.
TEST_F(CoordinatorSchedulingTest, systemConnectorIdIsConfigurable) {
  auto options = optimizerOptions_;
  options.systemConnectorId = kMetadataConnectorIdForTest;

  auto plan = planVelox(
      parseSelect("FROM coordinator_metadata.default.m", kTestConnectorId),
      distributed(),
      options);

  auto matcher =
      matchScan("m").fragmentType(FragmentType::kCoordinator).build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// With no system connector configured, a scan of the same table is an ordinary
// parallel kSource fragment gathered into a kSingle root.
TEST_F(CoordinatorSchedulingTest, systemCatalogIsNotHardcoded) {
  auto options = optimizerOptions_;
  options.systemConnectorId.clear();

  auto plan = planVelox(
      parseSelect("FROM coordinator_system.default.s", kTestConnectorId),
      distributed(),
      options);

  auto matcher = matchScan("s")
                     .fragmentType(FragmentType::kSource)
                     .gather()
                     .fragmentType(FragmentType::kSingle)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// Join of a parallel scan with a system scan: the system scan is the broadcast
// build in its own kCoordinator fragment; the probe keeps full parallelism in a
// kSource fragment gathered into a kSingle root.
TEST_F(CoordinatorSchedulingTest, systemScanJoinWithScan) {
  auto plan = planVelox(
      parseSelect(
          "SELECT a FROM t JOIN coordinator_system.default.s ON a = c",
          kTestConnectorId),
      distributed());

  auto matcher = matchScan("t")
                     .hashJoin(matchScan("s")
                                   .fragmentType(FragmentType::kCoordinator)
                                   .broadcast()
                                   .build())
                     .fragmentType(FragmentType::kSource)
                     .gather()
                     .fragmentType(FragmentType::kSingle)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// System scan on the probe side (syntactic join order): the join runs in the
// kCoordinator root fragment with the system scan; the parallel build is
// broadcast from its own kSource fragment.
TEST_F(CoordinatorSchedulingTest, systemScanProbeJoinWithScan) {
  auto options = optimizerOptions_;
  options.syntacticJoinOrder = true;

  auto plan = planVelox(
      parseSelect(
          "SELECT a FROM coordinator_system.default.s JOIN t ON c = a",
          kTestConnectorId),
      distributed(),
      options);

  auto matcher = matchScan("s")
                     .hashJoin(matchScan("t")
                                   .fragmentType(FragmentType::kSource)
                                   .broadcast()
                                   .build())
                     .fragmentType(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// Join of a system scan with single-task Values co-locates in one kCoordinator
// fragment.
TEST_F(CoordinatorSchedulingTest, joinOfSystemScanAndValues) {
  auto plan = planVelox(
      parseSelect(
          "SELECT s.c FROM coordinator_system.default.s AS s "
          "JOIN (VALUES 1) AS v(x) ON s.c = v.x",
          kTestConnectorId),
      distributed());

  auto matcher = matchScan("s")
                     .hashJoin(matchValues().project().build())
                     .fragmentType(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// A self-join of a system scan keeps both scans in one kCoordinator fragment.
TEST_F(CoordinatorSchedulingTest, joinOfTwoSystemScans) {
  auto plan = planVelox(
      parseSelect(
          "SELECT s1.c FROM coordinator_system.default.s AS s1 "
          "JOIN coordinator_system.default.s AS s2 ON s1.c = s2.c",
          kTestConnectorId),
      distributed());

  auto matcher = matchScan("s")
                     .hashJoin(matchScan("s").build())
                     .fragmentType(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
