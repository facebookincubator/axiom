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

#include "axiom/optimizer/MultiFragmentPlan.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/VectorStream.h"

#include <gtest/gtest.h>

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
using namespace velox::exec::test;

class MultiFragmentPlanTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  PlanBuilder startValues() {
    return PlanBuilder(idGenerator_)
        .values({BaseVector::create<RowVector>(
            ROW("a", BIGINT()), 1, pool_.get())});
  }

  core::PlanNodePtr values() {
    return startValues().planNode();
  }

  core::PlanNodePtr partitionedOutput(int32_t numPartitions) {
    return startValues().partitionedOutput({"a"}, numPartitions).planNode();
  }

  core::PlanNodePtr exchange() {
    return PlanBuilder(idGenerator_)
        .exchange(
            ROW("a", BIGINT()),
            VectorSerde::kindName(VectorSerde::Kind::kPresto))
        .planNode();
  }

  // Creates a single-stage plan with the given type.
  MultiFragmentPlan makeSingleFragmentPlan(
      velox::core::StageType type,
      std::optional<int32_t> numRemotePartitions = std::nullopt,
      const MultiFragmentPlan::Options& options = defaultOptions()) {
    return MultiFragmentPlan(
        {{.id = 1,
          .type = type,
          .numRemotePartitions = numRemotePartitions,
          .plan = values()}},
        {},
        options);
  }

  static MultiFragmentPlan::Options defaultOptions() {
    return {
        .queryId = "test",
        .maxRemotePartitions = 10,
    };
  }

  std::shared_ptr<core::PlanNodeIdGenerator> idGenerator_ =
      std::make_shared<core::PlanNodeIdGenerator>();

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
};

TEST_F(MultiFragmentPlanTest, validSingleFragment) {
  EXPECT_NO_THROW(makeSingleFragmentPlan(
      velox::core::StageType::kSource,
      std::nullopt,
      MultiFragmentPlan::Options::singleNode()));
}

TEST_F(MultiFragmentPlanTest, validDistributedPlan) {
  auto options = defaultOptions();
  options.remoteOutput = true;

  const auto exchangeNode = exchange();
  const MultiFragmentPlan plan(
      {
          {.id = 1,
           .type = velox::core::StageType::kSource,
           .plan = partitionedOutput(4)},
          {.id = 2,
           .type = velox::core::StageType::kFixed,
           .numRemotePartitions = 4,
           .plan = exchangeNode,
           .inputs = {{exchangeNode->id(), 1}}},
      },
      {},
      options);
  EXPECT_EQ(plan.stagePlan().root().id, 2);
}

TEST_F(MultiFragmentPlanTest, invalidStages) {
  VELOX_ASSERT_USER_THROW(
      makeSingleFragmentPlan(velox::core::StageType::kFixed),
      "kFixed stage must have numRemotePartitions set: 1");
}

TEST_F(MultiFragmentPlanTest, scansOfMissingStage) {
  auto scansOf = [](int32_t stageId) {
    folly::F14FastMap<int32_t, FragmentScans> scans;
    scans[stageId].sampledScans.emplace("0", 50);
    return scans;
  };
  auto makePlan = [&](int32_t stageId) {
    return MultiFragmentPlan(
        {{.id = 1, .type = velox::core::StageType::kSingle, .plan = values()}},
        scansOf(stageId),
        defaultOptions());
  };
  VELOX_ASSERT_THROW(
      makePlan(2), "Scans name a stage that is not in the plan: 2");
  VELOX_ASSERT_THROW(
      makePlan(0), "Scans name a stage that is not in the plan: 0");
  VELOX_ASSERT_THROW(
      MultiFragmentPlan({}, scansOf(1), defaultOptions()),
      "Scans name a stage that is not in the plan: 1");
}

TEST_F(MultiFragmentPlanTest, emptyFragments) {
  const MultiFragmentPlan plan({}, {}, defaultOptions());
  EXPECT_TRUE(plan.empty());
  EXPECT_TRUE(plan.stages().empty());
  VELOX_ASSERT_THROW(plan.stagePlan(), "Plan has no stages");
}

TEST_F(MultiFragmentPlanTest, maxRemotePartitions) {
  VELOX_ASSERT_THROW(
      makeSingleFragmentPlan(velox::core::StageType::kFixed, 20),
      "Fragment numRemotePartitions exceeds maxRemotePartitions");
}

TEST_F(MultiFragmentPlanTest, sourceWithWidthHint) {
  auto options = defaultOptions();
  options.remoteOutput = true;
  EXPECT_NO_THROW(
      makeSingleFragmentPlan(velox::core::StageType::kSource, 5, options));
}

TEST_F(MultiFragmentPlanTest, rootFragmentType) {
  auto options = defaultOptions();
  options.remoteOutput = false;

  VELOX_ASSERT_THROW(
      makeSingleFragmentPlan(velox::core::StageType::kFixed, 4, options),
      "Root fragment must be kSingle or kCoordinator");

  VELOX_ASSERT_THROW(
      makeSingleFragmentPlan(
          velox::core::StageType::kSource, std::nullopt, options),
      "Root fragment must be kSingle or kCoordinator");

  // kSource is allowed as root fragment with maxRemotePartitions == 1.
  EXPECT_NO_THROW(makeSingleFragmentPlan(
      velox::core::StageType::kSource,
      std::nullopt,
      MultiFragmentPlan::Options::singleNode()));

  // kFixed is allowed as root fragment with remoteOutput.
  options.remoteOutput = true;
  EXPECT_NO_THROW(
      makeSingleFragmentPlan(velox::core::StageType::kFixed, 4, options));
}

} // namespace
} // namespace facebook::axiom::optimizer
