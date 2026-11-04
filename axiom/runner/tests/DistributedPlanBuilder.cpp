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

#include "axiom/runner/tests/DistributedPlanBuilder.h"

#include <folly/container/F14Map.h>

namespace facebook::axiom::runner::test {

DistributedPlanBuilder::DistributedPlanBuilder(
    const optimizer::MultiFragmentPlan::Options& options,
    std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator,
    velox::memory::MemoryPool* pool)
    : PlanBuilder(std::move(planNodeIdGenerator), pool),
      options_(options),
      root_(this) {
  root_->stack_.push_back(this);
  newFragment(options_.maxRemotePartitions);
}

DistributedPlanBuilder::DistributedPlanBuilder(DistributedPlanBuilder& root)
    : PlanBuilder(root.planNodeIdGenerator(), root.pool()),
      options_(root.options_),
      root_(&root) {
  root_->stack_.push_back(this);
  newFragment(options_.maxRemotePartitions);
}

DistributedPlanBuilder::~DistributedPlanBuilder() {
  VELOX_CHECK_EQ(root_->stack_.size(), 1);
}

std::vector<velox::core::Stage> DistributedPlanBuilder::stages() {
  newFragment();
  // Builders that return a shuffle take an id for a stage they never finish,
  // so number the finished stages from 1 again.
  folly::F14FastMap<int32_t, int32_t> newIds;
  for (const auto& stage : fragments_) {
    newIds.emplace(stage.id, static_cast<int32_t>(newIds.size()) + 1);
  }
  for (auto& stage : fragments_) {
    stage.id = newIds.at(stage.id);
    for (auto& input : stage.inputs) {
      input.producerStageId = newIds.at(input.producerStageId);
    }
  }
  return std::move(fragments_);
}

optimizer::MultiFragmentPlanPtr DistributedPlanBuilder::build() {
  return std::make_shared<optimizer::MultiFragmentPlan>(
      stages(),
      folly::F14FastMap<int32_t, optimizer::FragmentScans>{},
      options_);
}

void DistributedPlanBuilder::newFragment(
    std::optional<int32_t> numRemotePartitions) {
  if (current_) {
    current_->plan = std::move(planNode_);
    fragments_.push_back(std::move(*current_));
  }

  current_ = std::make_unique<velox::core::Stage>();
  current_->id = ++root_->fragmentCounter_;
  if (numRemotePartitions.has_value() && numRemotePartitions.value() > 1) {
    current_->type = velox::core::StageType::kFixed;
    current_->numRemotePartitions = numRemotePartitions;
  } else if (numRemotePartitions.has_value()) {
    current_->type = velox::core::StageType::kSingle;
  } else {
    current_->type = velox::core::StageType::kSource;
  }

  planNode_ = nullptr;
}

namespace {

template <typename TNode>
const TNode* as(const velox::core::PlanNodePtr& node) {
  const auto* result = dynamic_cast<const TNode*>(node.get());
  VELOX_CHECK_NOT_NULL(result);

  return result;
}

} // namespace

void DistributedPlanBuilder::addExchange(
    const velox::RowTypePtr& producerType,
    int32_t producerStageId,
    velox::core::Stage& fragment) {
  exchange(
      producerType,
      velox::VectorSerde::kindName(velox::VectorSerde::Kind::kPresto));
  auto* exchange = as<velox::core::ExchangeNode>(planNode_);

  fragment.inputs.push_back(
      velox::core::StageInput{exchange->id(), producerStageId});
}

DistributedPlanBuilder& DistributedPlanBuilder::shufflePartitioned(
    const std::vector<std::string>& partitionKeys,
    int numPartitions,
    bool replicateNullsAndAny,
    const std::vector<std::string>& outputLayout) {
  partitionedOutput(
      partitionKeys, numPartitions, replicateNullsAndAny, outputLayout);
  auto* output = as<velox::core::PartitionedOutputNode>(planNode_);

  const auto producerStageId = current_->id;

  newFragment(numPartitions);

  addExchange(output->outputType(), producerStageId, *current_);
  return *this;
}

void DistributedPlanBuilder::appendFragments(
    std::vector<velox::core::Stage> fragments) {
  for (auto& fragment : fragments) {
    fragments_.emplace_back(std::move(fragment));
  }
}

velox::core::PlanNodePtr DistributedPlanBuilder::shufflePartitionedResult(
    const std::vector<std::string>& partitionKeys,
    int numPartitions,
    bool replicateNullsAndAny,
    const std::vector<std::string>& outputLayout) {
  partitionedOutput(
      partitionKeys, numPartitions, replicateNullsAndAny, outputLayout);
  auto* output = as<velox::core::PartitionedOutputNode>(planNode_);

  const auto producerStageId = current_->id;

  newFragment();

  root_->stack_.pop_back(); // Remove self.

  auto* consumer = root_->stack_.back();
  if (consumer->current_->numRemotePartitions.has_value()) {
    VELOX_CHECK_EQ(
        numPartitions,
        consumer->current_->numRemotePartitions.value(),
        "The consumer numRemotePartitions should match the producer fanout");
  } else {
    consumer->current_->type = numPartitions > 1
        ? velox::core::StageType::kFixed
        : velox::core::StageType::kSingle;
    consumer->current_->numRemotePartitions = numPartitions > 1
        ? std::optional<int32_t>{numPartitions}
        : std::nullopt;
  }

  root_->appendFragments(std::move(fragments_));

  addExchange(output->outputType(), producerStageId, *consumer->current_);
  return std::move(planNode_);
}

velox::core::PlanNodePtr DistributedPlanBuilder::shuffleBroadcastResult() {
  partitionedOutputBroadcast();
  auto* output = as<velox::core::PartitionedOutputNode>(planNode_);

  const auto producerStageId = current_->id;
  auto result = planNode_;
  newFragment();

  VELOX_CHECK_GE(root_->stack_.size(), 2);
  root_->stack_.pop_back(); // Remove self.
  auto* consumer = root_->stack_.back();

  VELOX_CHECK(
      consumer->current_->numRemotePartitions.has_value() ||
      consumer->current_->type == velox::core::StageType::kSingle ||
      consumer->current_->type == velox::core::StageType::kCoordinator);

  root_->appendFragments(std::move(fragments_));

  addExchange(output->outputType(), producerStageId, *consumer->current_);
  return std::move(planNode_);
}

} // namespace facebook::axiom::runner::test
