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
#include "axiom/connectors/system/SystemConnector.h"
#include "axiom/connectors/system/SystemConnectorMetadata.h"
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/connectors/Connector.h"

namespace facebook::axiom {
namespace {

using optimizer::FragmentType;
using velox::core::PlanMatcherBuilder;

// Empty providers: these plan-shape tests only resolve the table layout and
// inspect the produced fragments; they never read query or session data.
class EmptyQueryInfoProvider : public connector::system::QueryInfoProvider {
 public:
  std::vector<connector::system::QueryInfo> getQueryInfos() const override {
    return {};
  }
};

class EmptySessionPropertiesProvider
    : public connector::system::SessionPropertiesProvider {
 public:
  std::vector<connector::system::SessionPropertyInfo> getSessionProperties()
      const override {
    return {};
  }
};

class CoordinatorSchedulingTest : public optimizer::test::QueryTestBase {
 protected:
  static constexpr auto kSystemConnectorId = "testsystem";

  CoordinatorSchedulingTest() {
    useV2_ = true;
  }

  void SetUp() override {
    optimizer::test::QueryTestBase::SetUp();

    systemConnector_ = std::make_shared<connector::system::SystemConnector>(
        kSystemConnectorId, &queryProvider_, &sessionProvider_);
    velox::connector::registerConnector(systemConnector_);

    systemMetadata_ =
        std::make_shared<connector::system::SystemConnectorMetadata>(
            systemConnector_.get());
    connector::ConnectorMetadataRegistry::global().insert(
        kSystemConnectorId, systemMetadata_);
  }

  void TearDown() override {
    connector::ConnectorMetadataRegistry::global().erase(kSystemConnectorId);
    systemMetadata_.reset();

    velox::connector::unregisterConnector(kSystemConnectorId);
    systemConnector_.reset();

    optimizer::test::QueryTestBase::TearDown();
  }

  logical_plan::LogicalPlanNodePtr parseSelect(std::string_view sql) {
    return optimizer::test::QueryTestBase::parseSelect(sql, kSystemConnectorId);
  }

  EmptyQueryInfoProvider queryProvider_;
  EmptySessionPropertiesProvider sessionProvider_;
  std::shared_ptr<connector::system::SystemConnector> systemConnector_;
  std::shared_ptr<connector::system::SystemConnectorMetadata> systemMetadata_;
};

// A scan of a coordinator-only system table is planned as a single coordinator
// fragment, even with multiple workers available.
TEST_F(CoordinatorSchedulingTest, scan) {
  auto logicalPlan = parseSelect("SELECT query_id FROM runtime.queries");
  auto plan = planVelox(logicalPlan).plan;

  auto matcher = PlanMatcherBuilder()
                     .tableScan("runtime.queries")
                     .output(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan, matcher);
}

// A global aggregation over a system table runs entirely on the coordinator:
// the partial/final split is local (across drivers), never a cross-node
// shuffle.
TEST_F(CoordinatorSchedulingTest, globalAggregation) {
  auto logicalPlan = parseSelect("SELECT count(*) FROM runtime.queries");
  auto plan = planVelox(logicalPlan).plan;

  auto matcher = PlanMatcherBuilder()
                     .tableScan("runtime.queries")
                     .partialAggregation()
                     .localGather()
                     .finalAggregation()
                     .output(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan, matcher);
}

// A union of a system table and inline values pools both single-task legs into
// one coordinator fragment.
TEST_F(CoordinatorSchedulingTest, unionWithValues) {
  auto logicalPlan = parseSelect(
      "SELECT query_id FROM runtime.queries "
      "UNION ALL "
      "SELECT c FROM (VALUES ('x')) AS _(c)");
  auto plan = planVelox(logicalPlan).plan;

  auto matcher = PlanMatcherBuilder()
                     .tableScan("runtime.queries")
                     .localPartition(matchValues().project())
                     .output(FragmentType::kCoordinator)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan, matcher);
}

// A union of a system table and an ordinary table isolates the system leg into
// its own coordinator fragment; the ordinary leg keeps parallel workers.
TEST_F(CoordinatorSchedulingTest, unionWithRegular) {
  auto logicalPlan = parseSelect(
      "SELECT query_id FROM runtime.queries "
      "UNION ALL "
      "SELECT n_name FROM test.default.nation");
  auto plan = planVelox(logicalPlan).plan;

  auto matcher = PlanMatcherBuilder()
                     .tableScan("\"default\".\"nation\"")
                     .project()
                     .localPartition(
                         PlanMatcherBuilder()
                             .tableScan("runtime.queries")
                             .arbitrary(FragmentType::kCoordinator))
                     .gather(FragmentType::kSource)
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan, matcher);
}

} // namespace
} // namespace facebook::axiom
