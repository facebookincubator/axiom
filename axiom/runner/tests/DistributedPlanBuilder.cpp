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
#include "velox/serializers/PrestoSerializer.h"

namespace facebook::axiom::runner::test {

DistributedPlanBuilder::DistributedPlanBuilder(
    const optimizer::MultiFragmentPlan::Options& options,
    std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator,
    velox::memory::MemoryPool* pool)
    : PlanBuilder(std::move(planNodeIdGenerator), pool),
      options_(options),
      root_(this) {
  root_->stack_.push_back(this);
  newFragment(options_.numWorkers);
}

DistributedPlanBuilder::DistributedPlanBuilder(DistributedPlanBuilder& root)
    : PlanBuilder(root.planNodeIdGenerator(), root.pool()),
      options_(root.options_),
      root_(&root) {
  root_->stack_.push_back(this);
  newFragment(options_.numWorkers);
}

DistributedPlanBuilder::~DistributedPlanBuilder() {
  VELOX_CHECK_EQ(root_->stack_.size(), 1);
}

std::vector<optimizer::ExecutableFragment> DistributedPlanBuilder::fragments() {
  newFragment();
  return std::move(fragments_);
}

optimizer::MultiFragmentPlanPtr DistributedPlanBuilder::build() {
  return std::make_shared<optimizer::MultiFragmentPlan>(fragments(), options_);
}

void DistributedPlanBuilder::newFragment(std::optional<int32_t> width) {
  if (current_) {
    current_->fragment = velox::core::PlanFragment(std::move(planNode_));
    fragments_.push_back(std::move(*current_));
  }

  auto taskPrefix =
      fmt::format("{}.{}", options_.queryId, root_->fragmentCounter_++);
  if (width.has_value() && width.value() > 1) {
    current_ = std::make_unique<optimizer::ExecutableFragment>(
        optimizer::ExecutableFragment{
            .taskPrefix = std::move(taskPrefix),
            .type = optimizer::FragmentType::kFixed,
            .width = width,
        });
  } else if (width.has_value()) {
    current_ = std::make_unique<optimizer::ExecutableFragment>(
        optimizer::ExecutableFragment{
            .taskPrefix = std::move(taskPrefix),
            .type = optimizer::FragmentType::kSingle,
        });
  } else {
    current_ = std::make_unique<optimizer::ExecutableFragment>(
        optimizer::ExecutableFragment{
            .taskPrefix = std::move(taskPrefix),
            .type = optimizer::FragmentType::kSource,
        });
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
    const std::string& producerPrefix,
    optimizer::ExecutableFragment& fragment) {
  exchange(producerType, velox::serializer::presto::PrestoVectorSerde::name());
  auto* exchange = as<velox::core::ExchangeNode>(planNode_);

  fragment.inputStages.push_back(
      optimizer::InputStage{exchange->id(), producerPrefix});
}

DistributedPlanBuilder& DistributedPlanBuilder::shufflePartitioned(
    const std::vector<std::string>& partitionKeys,
    int numPartitions,
    bool replicateNullsAndAny,
    const std::vector<std::string>& outputLayout) {
  partitionedOutput(
      partitionKeys, numPartitions, replicateNullsAndAny, outputLayout);
  auto* output = as<velox::core::PartitionedOutputNode>(planNode_);

  const auto producerPrefix = current_->taskPrefix;

  newFragment(numPartitions);

  addExchange(output->outputType(), producerPrefix, *current_);
  return *this;
}

void DistributedPlanBuilder::appendFragments(
    std::vector<optimizer::ExecutableFragment> fragments) {
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

  const auto producerPrefix = current_->taskPrefix;

  newFragment();

  root_->stack_.pop_back(); // Remove self.

  auto* consumer = root_->stack_.back();
  if (consumer->current_->width.has_value()) {
    VELOX_CHECK_EQ(
        numPartitions,
        consumer->current_->width.value(),
        "The consumer width should match the producer fanout");
  } else {
    consumer->current_->type = numPartitions > 1
        ? optimizer::FragmentType::kFixed
        : optimizer::FragmentType::kSingle;
    consumer->current_->width = numPartitions > 1
        ? std::optional<int32_t>{numPartitions}
        : std::nullopt;
  }

  root_->appendFragments(std::move(fragments_));

  addExchange(output->outputType(), producerPrefix, *consumer->current_);
  return std::move(planNode_);
}

velox::core::PlanNodePtr DistributedPlanBuilder::shuffleBroadcastResult() {
  partitionedOutputBroadcast();
  auto* output = as<velox::core::PartitionedOutputNode>(planNode_);

  const auto producerPrefix = current_->taskPrefix;
  auto result = planNode_;
  newFragment();

  VELOX_CHECK_GE(root_->stack_.size(), 2);
  root_->stack_.pop_back(); // Remove self.
  auto* consumer = root_->stack_.back();

  VELOX_CHECK(
      consumer->current_->width.has_value() ||
      consumer->current_->type == optimizer::FragmentType::kSingle ||
      consumer->current_->type == optimizer::FragmentType::kCoordinator);

  root_->appendFragments(std::move(fragments_));

  addExchange(output->outputType(), producerPrefix, *consumer->current_);
  return std::move(planNode_);
}

} // namespace facebook::axiom::runner::test
