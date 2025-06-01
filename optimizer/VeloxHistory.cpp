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

#include "optimizer/VeloxHistory.h" //@manual
#include "velox/exec/Operator.h"
#include "velox/exec/TaskStats.h"

namespace facebook::velox::optimizer {

using namespace facebook::velox::exec;
using namespace facebook::velox::runner;

void VeloxHistory::recordJoinSample(
    const std::string& key,
    float lr,
    float rl) {}

std::pair<float, float> VeloxHistory::sampleJoin(JoinEdge* edge) {
  auto keyPair = edge->sampleKey();
  
  if (keyPair.first.empty()) {
    return std::make_pair(0, 0);
  }
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = joinSamples_.find(keyPair.first);
    if (it != joinSamples_.end()) {
      if (keyPair.second) {
	return std::make_pair(it->second.second, it->second.first);
      }
      return it->second;
    }
  }
  std::pair<float, float> pair;
  if (keyPair.second) {
    pair = optimizer::sampleJoin(edge->rightTable()->as<BaseTable>()->schemaTable,  edge->rightKeys(), edge->leftTable()->as<BaseTable>()->schemaTable, edge->leftKeys());
  } else {
    pair = optimizer::sampleJoin(edge->leftTable()->as<BaseTable>()->schemaTable, edge->leftKeys(), edge->rightTable()->as<BaseTable>()->schemaTable,  edge->rightKeys());
  }
  {
    std::lock_guard<std::mutex> l(mutex_);
    joinSamples_[keyPair.first] = pair;
  }
  if (keyPair.second) {
    return std::make_pair(pair.second, pair.first);
  }
  return pair;
}

NodePrediction* VeloxHistory::getHistory(const std::string key) {
  return nullptr;
}

void VeloxHistory::setHistory(const std::string& key, NodePrediction history) {}

bool VeloxHistory::setLeafSelectivity(BaseTable& table, RowTypePtr scanType) {
  auto optimization = queryCtx()->optimization();
  auto handlePair = optimization->leafHandle(table.id());
  auto handle = handlePair.first;
  auto string = handle->toString();
  {
    auto it = leafSelectivities_.find(string);
    if (it != leafSelectivities_.end()) {
      std::lock_guard<std::mutex> l(mutex_);
      table.filterSelectivity = it->second;
      return true;
    }
  }
  auto* runnerTable = table.schemaTable->connectorTable;
  if (!runnerTable) {
    // If there is no physical table to go to: Assume 1/10 if any filters.
    if (table.columnFilters.empty() && table.filter.empty()) {
      table.filterSelectivity = 1;
    } else {
      table.filterSelectivity = 0.1;
    }
    return false;
  }

  auto sample = runnerTable->layouts()[0]->sample(
      handlePair.first, 1, handlePair.second, scanType);
  table.filterSelectivity =
      static_cast<float>(sample.second) / (sample.first + 1);
  recordLeafSelectivity(string, table.filterSelectivity, false);
  return true;
}

void VeloxHistory::recordVeloxExecution(
					const PlanAndStats& plan,
    const std::vector<velox::exec::TaskStats>& stats) {
  std::unordered_map<std::string, const velox::exec::OperatorStats*> map;
  for (auto& task : stats) {
    for (auto& pipeline : task.pipelineStats) {
      for (auto& op : pipeline.operatorStats) {
        map[op.planNodeId] = &op;
      }
    }
  }
  for (auto& fragment : plan.plan->fragments()) {
    for (auto& scan : fragment.scans) {
      auto scanStats = map[scan->id()];
      std::string handle = scan->tableHandle()->toString();
      recordLeafSelectivity(
          handle,
          scanStats->outputPositions /
              std::max<float>(1, scanStats->rawInputPositions),
          true);
    }
  }
}

} // namespace facebook::velox::optimizer
