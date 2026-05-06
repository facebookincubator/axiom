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

  core::PlanFragment values() {
    return core::PlanFragment(startValues().planNode());
  }

  core::PlanFragment partitionedOutput(int32_t numPartitions) {
    return core::PlanFragment(
        startValues().partitionedOutput({"a"}, numPartitions).planNode());
  }

  core::PlanFragment broadcastOutput() {
    return core::PlanFragment(
        startValues().partitionedOutputBroadcast().planNode());
  }

  core::PlanFragment exchange() {
    return core::PlanFragment(
        PlanBuilder(idGenerator_)
            .exchange(
                ROW("a", BIGINT()),
                VectorSerde::kindName(VectorSerde::Kind::kPresto))
            .planNode());
  }

  // Creates a single-fragment plan with the given type.
  MultiFragmentPlan makeSingleFragmentPlan(
      FragmentType type,
      std::optional<int32_t> width = std::nullopt,
      const MultiFragmentPlan::Options& options = defaultOptions()) {
    ExecutableFragment fragment;
    fragment.taskPrefix = "stage0";
    fragment.type = type;
    fragment.width = width;
    fragment.fragment = values();
    return MultiFragmentPlan({std::move(fragment)}, options);
  }

  static MultiFragmentPlan::Options defaultOptions() {
    return {
        .queryId = "test",
        .numWorkers = 10,
    };
  }

  std::shared_ptr<core::PlanNodeIdGenerator> idGenerator_ =
      std::make_shared<core::PlanNodeIdGenerator>();

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
};

TEST_F(MultiFragmentPlanTest, validSingleFragment) {
  auto plan = makeSingleFragmentPlan(
      FragmentType::kSource,
      std::nullopt,
      MultiFragmentPlan::Options::singleNode());
  EXPECT_NO_THROW(plan.checkConsistency());
}

TEST_F(MultiFragmentPlanTest, validDistributedPlan) {
  auto options = defaultOptions();
  options.remoteOutput = true;

  ExecutableFragment producer;
  producer.taskPrefix = "stage0";
  producer.type = FragmentType::kSource;
  producer.fragment = partitionedOutput(4);

  ExecutableFragment consumer;
  consumer.taskPrefix = "stage1";
  consumer.type = FragmentType::kFixed;
  consumer.width = 4;
  consumer.fragment = exchange();
  consumer.inputStages.emplace_back(consumer.fragment.planNode->id(), "stage0");

  auto plan =
      MultiFragmentPlan({std::move(producer), std::move(consumer)}, options);
  EXPECT_NO_THROW(plan.checkConsistency());
}

TEST_F(MultiFragmentPlanTest, emptyFragments) {
  VELOX_ASSERT_THROW(
      MultiFragmentPlan({}, defaultOptions()).checkConsistency(),
      "Plan must have at least one fragment");
}

TEST_F(MultiFragmentPlanTest, fixedWithoutWidth) {
  VELOX_ASSERT_THROW(
      makeSingleFragmentPlan(FragmentType::kFixed).checkConsistency(),
      "kFixed fragment must have width set");
}

TEST_F(MultiFragmentPlanTest, singleWithWidth) {
  VELOX_ASSERT_THROW(
      makeSingleFragmentPlan(FragmentType::kSingle, 1).checkConsistency(),
      "fragment must not have width set");
}

TEST_F(MultiFragmentPlanTest, coordinatorWithWidth) {
  VELOX_ASSERT_THROW(
      makeSingleFragmentPlan(FragmentType::kCoordinator, 1).checkConsistency(),
      "fragment must not have width set");
}

TEST_F(MultiFragmentPlanTest, invalidWidth) {
  VELOX_ASSERT_THROW(
      makeSingleFragmentPlan(FragmentType::kFixed, 0).checkConsistency(),
      "Fragment width must be positive");

  VELOX_ASSERT_THROW(
      makeSingleFragmentPlan(FragmentType::kFixed, 20).checkConsistency(),
      "Fragment width exceeds numWorkers");
}

TEST_F(MultiFragmentPlanTest, sourceWithWidthHint) {
  auto options = defaultOptions();
  options.remoteOutput = true;
  auto plan = makeSingleFragmentPlan(FragmentType::kSource, 5, options);
  EXPECT_NO_THROW(plan.checkConsistency());
}

TEST_F(MultiFragmentPlanTest, missingProducer) {
  ExecutableFragment fragment;
  fragment.taskPrefix = "stage0";
  fragment.type = FragmentType::kSingle;
  fragment.fragment = exchange();
  fragment.inputStages.emplace_back(
      fragment.fragment.planNode->id(), "nonexistent");

  auto plan = MultiFragmentPlan({std::move(fragment)}, defaultOptions());
  VELOX_ASSERT_THROW(
      plan.checkConsistency(),
      "Producer fragment not found: nonexistent, consumer: stage0");
}

TEST_F(MultiFragmentPlanTest, producerNotPartitionedOutput) {
  ExecutableFragment producer;
  producer.taskPrefix = "stage0";
  producer.type = FragmentType::kSource;
  producer.fragment = values();

  ExecutableFragment consumer;
  consumer.taskPrefix = "stage1";
  consumer.type = FragmentType::kSingle;
  consumer.fragment = exchange();
  consumer.inputStages.emplace_back(consumer.fragment.planNode->id(), "stage0");

  auto plan = MultiFragmentPlan(
      {std::move(producer), std::move(consumer)}, defaultOptions());
  VELOX_ASSERT_THROW(plan.checkConsistency(), "Expected PartitionedOutputNode");
}

TEST_F(MultiFragmentPlanTest, lastFragmentIsProducer) {
  auto options = defaultOptions();
  options.remoteOutput = true;

  ExecutableFragment producer;
  producer.taskPrefix = "stage1";
  producer.type = FragmentType::kFixed;
  producer.width = 4;
  producer.fragment = partitionedOutput(4);

  ExecutableFragment consumer;
  consumer.taskPrefix = "stage0";
  consumer.type = FragmentType::kFixed;
  consumer.width = 4;
  consumer.fragment = exchange();
  consumer.inputStages.emplace_back(consumer.fragment.planNode->id(), "stage1");

  {
    auto plan = MultiFragmentPlan({consumer, producer}, options);
    VELOX_ASSERT_THROW(
        plan.checkConsistency(), "Last fragment cannot be a producer");
  }

  {
    auto plan = MultiFragmentPlan({producer, consumer}, options);
    ASSERT_NO_THROW(plan.checkConsistency());
  }
}

TEST_F(MultiFragmentPlanTest, partitionCountMismatch) {
  auto options = defaultOptions();
  options.remoteOutput = true;

  ExecutableFragment producer;
  producer.taskPrefix = "stage0";
  producer.type = FragmentType::kSource;
  producer.fragment = partitionedOutput(4);

  ExecutableFragment consumer;
  consumer.taskPrefix = "stage1";
  consumer.type = FragmentType::kFixed;
  consumer.width = 8;
  consumer.fragment = exchange();
  consumer.inputStages.emplace_back(consumer.fragment.planNode->id(), "stage0");

  auto plan =
      MultiFragmentPlan({std::move(producer), std::move(consumer)}, options);
  VELOX_ASSERT_THROW(plan.checkConsistency(), "Partition count mismatch");
}

TEST_F(MultiFragmentPlanTest, broadcastSkipsWidthCheck) {
  ExecutableFragment producer;
  producer.taskPrefix = "stage0";
  producer.type = FragmentType::kSource;
  producer.fragment = broadcastOutput();

  ExecutableFragment consumer;
  consumer.taskPrefix = "stage1";
  consumer.type = FragmentType::kSingle;
  consumer.fragment = exchange();
  consumer.inputStages.emplace_back(consumer.fragment.planNode->id(), "stage0");

  auto plan = MultiFragmentPlan(
      {std::move(producer), std::move(consumer)}, defaultOptions());
  EXPECT_NO_THROW(plan.checkConsistency());
}

TEST_F(MultiFragmentPlanTest, lastFragmentType) {
  auto options = defaultOptions();
  options.remoteOutput = false;

  VELOX_ASSERT_THROW(
      makeSingleFragmentPlan(FragmentType::kFixed, 4, options)
          .checkConsistency(),
      "Last fragment must be kSingle or kCoordinator");

  VELOX_ASSERT_THROW(
      makeSingleFragmentPlan(FragmentType::kSource, std::nullopt, options)
          .checkConsistency(),
      "Last fragment must be kSingle or kCoordinator");

  // kSource is allowed as last fragment with numWorkers == 1.
  EXPECT_NO_THROW(makeSingleFragmentPlan(
                      FragmentType::kSource,
                      std::nullopt,
                      MultiFragmentPlan::Options::singleNode())
                      .checkConsistency());

  // kFixed is allowed as last fragment with remoteOutput.
  options.remoteOutput = true;
  EXPECT_NO_THROW(makeSingleFragmentPlan(FragmentType::kFixed, 4, options)
                      .checkConsistency());
}

TEST_F(MultiFragmentPlanTest, orphanFragment) {
  auto options = defaultOptions();
  options.remoteOutput = true;

  ExecutableFragment orphan;
  orphan.taskPrefix = "stage0";
  orphan.type = FragmentType::kSource;
  orphan.fragment = partitionedOutput(4);

  ExecutableFragment output;
  output.taskPrefix = "stage1";
  output.type = FragmentType::kFixed;
  output.width = 4;
  output.fragment = values();

  auto plan =
      MultiFragmentPlan({std::move(orphan), std::move(output)}, options);
  VELOX_ASSERT_THROW(
      plan.checkConsistency(),
      "Non-last fragment must be referenced by exactly one consumer: stage0");
}

TEST_F(MultiFragmentPlanTest, producerReferencedByMultipleConsumers) {
  auto options = defaultOptions();
  options.remoteOutput = true;

  ExecutableFragment producer;
  producer.taskPrefix = "stage0";
  producer.type = FragmentType::kSource;
  producer.fragment = partitionedOutput(4);

  ExecutableFragment consumerA;
  consumerA.taskPrefix = "stage1";
  consumerA.type = FragmentType::kFixed;
  consumerA.width = 4;
  consumerA.fragment = exchange();
  consumerA.inputStages.emplace_back(
      consumerA.fragment.planNode->id(), "stage0");

  ExecutableFragment consumerB;
  consumerB.taskPrefix = "stage2";
  consumerB.type = FragmentType::kFixed;
  consumerB.width = 4;
  consumerB.fragment = exchange();
  consumerB.inputStages.emplace_back(
      consumerB.fragment.planNode->id(), "stage0");

  auto plan = MultiFragmentPlan(
      {std::move(producer), std::move(consumerA), std::move(consumerB)},
      options);
  VELOX_ASSERT_THROW(
      plan.checkConsistency(),
      "Producer fragment referenced by multiple consumers: stage0");
}

} // namespace
} // namespace facebook::axiom::optimizer
