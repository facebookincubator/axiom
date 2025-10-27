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

#include "axiom/runner/tests/PrestoQueryReplayRunner.h"
#include "axiom/runner/LocalRunner.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::runner {
namespace {
std::shared_ptr<velox::memory::MemoryPool> makeRootPool(
    const std::string& queryId) {
  static std::atomic_uint64_t poolId{0};
  return velox::memory::memoryManager()->addRootPool(
      fmt::format("{}_{}", queryId, poolId++));
}

std::vector<velox::RowVectorPtr> readCursor(
    std::shared_ptr<LocalRunner>& runner,
    velox::memory::MemoryPool* pool) {
  // We'll check the result after tasks are deleted, so copy the result
  // vectors to 'pool' that has longer lifetime.
  std::vector<velox::RowVectorPtr> result;
  while (auto rows = runner->next()) {
    result.push_back(
        std::dynamic_pointer_cast<velox::RowVector>(
            velox::BaseVector::copy(*rows, pool)));
  }
  return result;
}

const std::string kHiveConnectorId = "test-hive";
const std::string kFinalTaskPrefix = "final";

} // namespace

PrestoQueryReplayRunner::PrestoQueryReplayRunner(
    velox::memory::MemoryPool* pool,
    TaskPrefixExtractor taskPrefixExtractor,
    int32_t width,
    int32_t maxDrivers,
    const std::unordered_map<std::string, std::string>& config,
    const std::unordered_map<std::string, std::string>& hiveConfig)
    : pool_{pool},
      taskPrefixExtractor_{taskPrefixExtractor},
      width_{width},
      maxDrivers_{maxDrivers},
      config_{config},
      hiveConfig_{hiveConfig},
      executor_{std::make_unique<folly::CPUThreadPoolExecutor>(maxDrivers)} {}

std::shared_ptr<velox::core::QueryCtx> PrestoQueryReplayRunner::makeQueryCtx(
    const std::string& queryId,
    const std::shared_ptr<velox::memory::MemoryPool>& rootPool) {
  auto& config = config_;
  auto hiveConfig = hiveConfig_;
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      connectorConfigs;
  connectorConfigs[kHiveConnectorId] =
      std::make_shared<velox::config::ConfigBase>(std::move(hiveConfig));

  return velox::core::QueryCtx::create(
      executor_.get(),
      velox::core::QueryConfig(config),
      std::move(connectorConfigs),
      velox::cache::AsyncDataCache::getInstance(),
      rootPool,
      nullptr,
      queryId);
}

namespace {
std::vector<std::string> getStringListFromJson(const folly::dynamic& json) {
  std::vector<std::string> result;
  result.resize(json.size());
  std::transform(
      json.begin(), json.end(), result.begin(), [](const folly::dynamic& json) {
        return json.getString();
      });
  return result;
}

// Return true if 'node' is a gathering PartitionedOutput node.
bool isGatheringPartition(const velox::core::PlanNodePtr& node) {
  if (auto partitionedOutput =
          std::dynamic_pointer_cast<const velox::core::PartitionedOutputNode>(
              node)) {
    return partitionedOutput->keys().empty() &&
        !partitionedOutput->isBroadcast();
  }
  return false;
}

bool isBroadcastPartition(const velox::core::PlanNodePtr& node) {
  if (auto partitionedOutput =
          std::dynamic_pointer_cast<const velox::core::PartitionedOutputNode>(
              node)) {
    return partitionedOutput->isBroadcast();
  }
  return false;
}

// Return a new plan tree with the same structure as 'plan' but with the
// number of partitions of the root PartitionedOutputNode updated to
// 'numPartitions'. This method throws if the root node of 'plan' is not a
// PartitionedOutputNode or if it's a gathering PartitionedOutputNode.
velox::core::PlanNodePtr updateNumOfPartitions(
    const velox::core::PlanNodePtr& plan,
    int numPartitions) {
  auto partitionedOutput =
      std::dynamic_pointer_cast<const velox::core::PartitionedOutputNode>(plan);
  VELOX_CHECK(partitionedOutput != nullptr);
  if (partitionedOutput->isBroadcast()) {
    return plan;
  }

  VELOX_CHECK(!partitionedOutput->keys().empty());
  return velox::core::PartitionedOutputNode::Builder(*partitionedOutput)
      .numPartitions(numPartitions)
      .build();
}

// Deserialize each json string record in 'serializedJsonRecords' into a
// folly::dynamic and return them as a vector in the same order.
std::vector<folly::dynamic> getJsonRecords(
    const std::vector<std::string>& serializedJsonRecords) {
  std::vector<folly::dynamic> jsonRecords;
  jsonRecords.reserve(serializedJsonRecords.size());
  for (const auto& serializedJsonRecord : serializedJsonRecords) {
    jsonRecords.push_back(folly::parseJson(serializedJsonRecord));
  }
  return jsonRecords;
}

velox::core::PlanNodePtr getDeserializedPlan(
    const folly::dynamic& json,
    velox::memory::MemoryPool* pool) {
  const auto& jsonPlanFragment = json.at("plan_fragment");
  return velox::core::PlanNode::deserialize<velox::core::PlanNode>(
      jsonPlanFragment, pool);
}

struct PlanFragmentInfo {
  velox::core::PlanNodePtr plan;
  folly::F14FastMap<std::string, folly::F14FastSet<std::string>>
      remoteTaskIdMap;
  int32_t numWorkers{0};
};

std::vector<ExecutableFragment> createExecutableFragments(
    const folly::F14FastMap<std::string, PlanFragmentInfo>& planFragments) {
  std::vector<ExecutableFragment> executableFragments;
  for (const auto& [taskPrefix, planFragmentInfo] : planFragments) {
    ExecutableFragment executableFragment{taskPrefix};
    executableFragment.width =
        (planFragmentInfo.numWorkers > 0) ? planFragmentInfo.numWorkers : 1;
    executableFragment.fragment =
        velox::core::PlanFragment{planFragmentInfo.plan};

    std::vector<InputStage> inputStages;
    const auto& remoteTaskIdMap = planFragmentInfo.remoteTaskIdMap;
    for (const auto& [planNodeId, remoteTaskPrefixes] : remoteTaskIdMap) {
      for (const auto& remoteTaskPrefix : remoteTaskPrefixes) {
        inputStages.push_back(InputStage{planNodeId, remoteTaskPrefix});
      }
    }
    executableFragment.inputStages = std::move(inputStages);
    executableFragments.push_back(std::move(executableFragment));
  }
  return executableFragments;
}

} // namespace

std::vector<std::string> PrestoQueryReplayRunner::getTaskPrefixes(
    const std::vector<folly::dynamic>& jsonRecords) {
  std::vector<std::string> taskPrefixes;
  taskPrefixes.reserve(jsonRecords.size());
  for (const auto& json : jsonRecords) {
    taskPrefixes.push_back(
        taskPrefixExtractor_(json.at("task_id").getString()));
  }
  return taskPrefixes;
}

bool isSupportedImpl(const velox::core::PlanNodePtr& node) {
  // We don't support arbitrary partitioning yet.
  if (auto partitionedOutput =
          std::dynamic_pointer_cast<const velox::core::PartitionedOutputNode>(
              node)) {
    if (partitionedOutput->isArbitrary()) {
      return false;
    }
  }
  for (const auto& child : node->sources()) {
    if (!isSupportedImpl(child)) {
      return false;
    }
  }
  return true;
}

bool isSupported(
    const folly::dynamic& jsonPlan,
    const velox::core::PlanNodePtr& plan) {
  // We don't support grouped execution yet.
  if (jsonPlan.at("execution_strategy").getString() != "UNGROUPED") {
    return false;
  }
  return isSupportedImpl(plan);
}

namespace {
std::string findRootTaskPrefix(
    const std::vector<std::string>& taskPrefixes,
    const folly::F14FastMap<std::string, PlanFragmentInfo>& planFragments) {
  folly::F14FastSet<std::string> inputTaskPrefixes;
  for (const auto& [_, planFragmentInfo] : planFragments) {
    for (const auto& [_, remoteTaskPrefixes] :
         planFragmentInfo.remoteTaskIdMap) {
      for (const auto& remoteTaskPrefix : remoteTaskPrefixes) {
        inputTaskPrefixes.insert(remoteTaskPrefix);
      }
    }
  }
  for (const auto& taskPrefix : taskPrefixes) {
    if (inputTaskPrefixes.count(taskPrefix) == 0) {
      return taskPrefix;
    }
  }
  VELOX_UNREACHABLE("No root task found.");
}
} // namespace

MultiFragmentPlanPtr PrestoQueryReplayRunner::deserializeSupportedPlan(
    const std::string& queryId,
    const std::vector<std::string>& serializedPlanFragments) {
  auto jsonRecords = getJsonRecords(serializedPlanFragments);
  auto taskPrefixes = getTaskPrefixes(jsonRecords);
  VELOX_CHECK_EQ(jsonRecords.size(), serializedPlanFragments.size());
  VELOX_CHECK_EQ(taskPrefixes.size(), serializedPlanFragments.size());

  folly::F14FastMap<std::string, PlanFragmentInfo> planFragments;
  for (auto i = 0; i < serializedPlanFragments.size(); ++i) {
    auto& taskPrefix = taskPrefixes[i];
    VELOX_CHECK_EQ(planFragments.count(taskPrefix), 0);

    const auto plan = getDeserializedPlan(jsonRecords[i], pool_);
    if (!isSupported(jsonRecords[i], plan)) {
      return nullptr;
    }

    planFragments[taskPrefix].plan = plan;
  }

  for (auto i = 0; i < serializedPlanFragments.size(); ++i) {
    auto& taskPrefix = taskPrefixes[i];
    auto jsonRemoteTaskIdMaps = jsonRecords[i].at("remote_task_ids");
    folly::F14FastMap<std::string, folly::F14FastSet<std::string>>
        remoteTaskIdMap;
    for (const auto& [planNodeId, remoteTaskIds] :
         jsonRemoteTaskIdMaps.items()) {
      auto remoteTaskIdList = getStringListFromJson(remoteTaskIds);
      folly::F14FastSet<std::string> remoteTaskIdPrefixSet;
      for (const auto& remoteTaskId : remoteTaskIdList) {
        auto remoteTaskPrefix = taskPrefixExtractor_(remoteTaskId);
        const auto [_, inserted] =
            remoteTaskIdPrefixSet.insert(remoteTaskPrefix);

        VELOX_CHECK_GT(planFragments.count(remoteTaskPrefix), 0);
        if (inserted) {
          if (isGatheringPartition(planFragments.at(remoteTaskPrefix).plan)) {
            planFragments[taskPrefix].numWorkers = 1;
          } else {
            planFragments[taskPrefix].numWorkers = width_;
            if (!isBroadcastPartition(planFragments[remoteTaskPrefix].plan)) {
              planFragments[remoteTaskPrefix].plan = updateNumOfPartitions(
                  planFragments[remoteTaskPrefix].plan, width_);
            }
          }
        } else {
          // If remoteTaskPrefix already exists in the remoteTaskIdPrefixSet,
          // the number of workers should have already been updated when we
          // inserted this remote task prefix.
          if (isGatheringPartition(planFragments.at(remoteTaskPrefix).plan)) {
            VELOX_CHECK_EQ(planFragments[taskPrefix].numWorkers, 1);
          } else {
            VELOX_CHECK_EQ(planFragments[taskPrefix].numWorkers, width_);
          }
        }
      }
      remoteTaskIdMap[planNodeId.getString()] =
          std::move(remoteTaskIdPrefixSet);
    }
    planFragments[taskPrefix].remoteTaskIdMap = remoteTaskIdMap;
  }

  // If the root task ends with a PartitionedOutputNode, we need to add a
  // final gathering stage to collect the results.
  auto rootTaskPrefix = findRootTaskPrefix(taskPrefixes, planFragments);
  auto& rootPlanFragment = planFragments[rootTaskPrefix];
  if (auto partitionedOutput =
          std::dynamic_pointer_cast<const velox::core::PartitionedOutputNode>(
              rootPlanFragment.plan)) {
    VELOX_CHECK(
        partitionedOutput->keys().empty() && !partitionedOutput->isBroadcast());
    // Use a large plan node id to avoid conflicts with the existing plan node
    // ids.
    velox::core::PlanNodeId id;
    planFragments[kFinalTaskPrefix].plan =
        velox::exec::test::PlanBuilder(
            std::make_shared<velox::core::PlanNodeIdGenerator>(100000))
            .exchange(
                partitionedOutput->outputType(), partitionedOutput->serdeKind())
            .capturePlanNodeId(id)
            .planNode();
    planFragments[kFinalTaskPrefix].numWorkers = 1;
    planFragments[kFinalTaskPrefix].remoteTaskIdMap[id] = {rootTaskPrefix};
  }

  auto executableFragments = createExecutableFragments(planFragments);

  MultiFragmentPlan::Options options{queryId, width_, maxDrivers_};
  return std::make_shared<MultiFragmentPlan>(
      std::move(executableFragments), std::move(options));
}

folly::F14FastMap<velox::core::PlanNodeId, std::vector<ConnectorSplitPtr>>
PrestoQueryReplayRunner::deserializeConnectorSplits(
    const std::vector<std::string>& serializedSplits) {
  folly::F14FastMap<velox::core::PlanNodeId, std::vector<ConnectorSplitPtr>>
      nodeSplitsMap;
  for (auto& serializedSplit : serializedSplits) {
    auto json = folly::parseJson(serializedSplit);
    VELOX_CHECK(json.isObject());
    if (json.empty()) {
      continue;
    }

    for (auto& [key, value] : json.items()) {
      auto planNodeId = key.asString();
      VELOX_CHECK(value.isArray());
      std::vector<ConnectorSplitPtr> nodeSplits;
      for (auto& split : value) {
        nodeSplits.push_back(
            velox::connector::hive::HiveConnectorSplit::create(split));
      }
      nodeSplitsMap[planNodeId].insert(
          nodeSplitsMap[planNodeId].end(),
          nodeSplits.begin(),
          nodeSplits.end());
    }
  }
  return nodeSplitsMap;
}

std::pair<
    std::optional<std::vector<velox::RowVectorPtr>>,
    PrestoQueryReplayRunner::Status>
PrestoQueryReplayRunner::run(
    const std::string& queryId,
    const std::vector<std::string>& serializedPlanFragments,
    const std::vector<std::string>& serializedConnectorSplits) {
  auto queryRootPool = makeRootPool(queryId);
  auto multiFragmentPlan =
      deserializeSupportedPlan(queryId, serializedPlanFragments);
  if (multiFragmentPlan == nullptr) {
    return std::make_pair(std::nullopt, Status::kUnsupported);
  }

  auto nodeSplitMap = deserializeConnectorSplits(serializedConnectorSplits);
  auto localRunner = std::make_shared<LocalRunner>(
      std::move(multiFragmentPlan),
      FinishWrite{},
      makeQueryCtx(queryId, queryRootPool),
      std::make_shared<runner::SimpleSplitSourceFactory>(nodeSplitMap));

  std::vector<velox::RowVectorPtr> result;
  try {
    result = readCursor(localRunner, pool_);
    localRunner->waitForCompletion(kWaitTimeoutUs);
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to run query " << queryId << ": " << e.what();
    return std::make_pair(std::nullopt, Status::kError);
  }
  return std::make_pair(result, Status::kSuccess);
}

} // namespace facebook::axiom::runner
