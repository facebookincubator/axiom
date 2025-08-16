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

#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/optimizer/Plan.h"

#include <iostream>

DEFINE_double(
    cardinality_warning_threshold,
    5,
    "Log a warning if cardinality estimate is more than this many times off. 0 means no warnings.");

using namespace facebook::velox::exec;

namespace facebook::velox::optimizer {

void VeloxHistory::recordJoinSample(
    const std::string& key,
    float lr,
    float rl) {}

std::pair<float, float> VeloxHistory::sampleJoin(JoinEdge* edge) {
  const auto& options = queryCtx()->optimization()->options();
  if (!options.sampleJoins) {
    return {0, 0};
  }

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

  const bool trace = (options.traceFlags & OptimizerOptions::kSample) != 0;

  std::pair<float, float> pair;
  uint64_t start = getCurrentTimeMicro();
  if (keyPair.second) {
    pair = optimizer::sampleJoin(
        edge->rightTable()->as<BaseTable>()->schemaTable,
        edge->rightKeys(),
        edge->leftTable()->as<BaseTable>()->schemaTable,
        edge->leftKeys());
  } else {
    pair = optimizer::sampleJoin(
        edge->leftTable()->as<BaseTable>()->schemaTable,
        edge->leftKeys(),
        edge->rightTable()->as<BaseTable>()->schemaTable,
        edge->rightKeys());
  }
  {
    std::lock_guard<std::mutex> l(mutex_);
    joinSamples_[keyPair.first] = pair;
  }
  if (trace) {
    std::cout << "Sample join " << keyPair.first << ": " << pair.first << " :"
              << pair.second
              << " time=" << succinctMicros(getCurrentTimeMicro() - start)
              << std::endl;
  }
  if (keyPair.second) {
    return std::make_pair(pair.second, pair.first);
  }
  return pair;
}

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
  bool trace =
      (optimization->options().traceFlags & OptimizerOptions::kSample) != 0;
  uint64_t start = getCurrentTimeMicro();
  auto sample = runnerTable->layouts()[0]->sample(
      handlePair.first, 1, handlePair.second, scanType);
  table.filterSelectivity =
      static_cast<float>(sample.second) / (sample.first + 1);
  if (trace) {
    std::cout << "Sampled scan " << string << "= " << table.filterSelectivity
              << " time= " << succinctMicros(getCurrentTimeMicro() - start)
              << std::endl;
  }
  recordLeafSelectivity(string, table.filterSelectivity, false);
  return true;
}

std::shared_ptr<const core::TableScanNode> findScan(
    const core::PlanNodeId& id,
    const axiom::runner::MultiFragmentPlanPtr& plan) {
  for (auto& fragment : plan->fragments()) {
    for (auto& scan : fragment.scans) {
      if (scan->id() == id) {
        return scan;
      }
    }
  }
  return nullptr;
}

void logPrediction(const std::string& message) {
  if (FLAGS_cardinality_warning_threshold != 0) {
    LOG(WARNING) << message;
  }
}

void predictionWarnings(
    const PlanAndStats& plan,
    const core::PlanNodeId& id,
    int64_t actualRows,
    float predictedRows) {
  if (actualRows == 0 && predictedRows == 0) {
    return;
  }

  if (std::isnan(predictedRows)) {
    return;
  }

  std::string historyKey;
  auto it = plan.history.find(id);
  if (it != plan.history.end()) {
    historyKey = it->second;
  }
  if (actualRows == 0 || predictedRows == 0) {
    logPrediction(fmt::format(
        "Node {} actual={} predicted={} key={}",
        id,
        actualRows,
        predictedRows,
        historyKey));
  } else {
    float ratio =
        static_cast<float>(actualRows) / static_cast<float>(predictedRows);
    auto threshold = FLAGS_cardinality_warning_threshold;
    if (ratio < 1 / threshold || ratio > threshold) {
      logPrediction(fmt::format(
          "Node {} actual={} predicted={} key={}",
          id,
          actualRows,
          predictedRows,
          historyKey));
    }
  }
}

void VeloxHistory::recordVeloxExecution(
    const PlanAndStats& plan,
    const std::vector<velox::exec::TaskStats>& stats) {
  for (auto& task : stats) {
    for (auto& pipeline : task.pipelineStats) {
      for (auto& op : pipeline.operatorStats) {
        if (op.operatorType == "HashBuild") {
          // Build has same PlanNodeId as probe and never has
          // output. Build cardinality is recorded as the output of
          // the previous node.
          continue;
        }
        auto it = plan.prediction.find(op.planNodeId);
        auto keyIt = plan.history.find(op.planNodeId);
        if (keyIt == plan.history.end()) {
          continue;
        }
        uint64_t actualRows;
        {
          std::lock_guard<std::mutex> l(mutex_);
          actualRows = op.outputPositions;
          planHistory_[keyIt->second] =
              NodePrediction{.cardinality = static_cast<float>(actualRows)};
        }
        if (op.operatorType == "TableScanOperator") {
          auto scan = findScan(op.planNodeId, plan.plan);
          if (scan) {
            std::string handle = scan->tableHandle()->toString();
            recordLeafSelectivity(
                handle,
                op.outputPositions / std::max<float>(1, op.rawInputPositions),
                true);
          }
        }
        if (it != plan.prediction.end()) {
          auto predictedRows = it->second.cardinality;
          predictionWarnings(plan, op.planNodeId, actualRows, predictedRows);
        }
      }
    }
  }
}

folly::dynamic VeloxHistory::serialize() {
  folly::dynamic obj = folly::dynamic::object();
  auto leafArray = folly::dynamic::array();
  for (auto& pair : leafSelectivities_) {
    folly::dynamic leaf = folly::dynamic::object();
    leaf["key"] = pair.first;
    leaf["value"] = pair.second;
    leafArray.push_back(leaf);
  }
  obj["leaves"] = leafArray;
  auto joinArray = folly::dynamic::array();
  for (auto& pair : joinSamples_) {
    folly::dynamic join = folly::dynamic::object();
    join["key"] = pair.first;
    join["lr"] = pair.second.first;
    join["rl"] = pair.second.second;
    joinArray.push_back(join);
  }
  obj["joins"] = joinArray;
  auto planArray = folly::dynamic::array();
  for (auto& pair : planHistory_) {
    folly::dynamic plan = folly::dynamic::object();
    plan["key"] = pair.first;
    plan["card"] = pair.second.cardinality;
    planArray.push_back(plan);
  }
  obj["plans"] = planArray;
  return obj;
}

void VeloxHistory::update(folly::dynamic& serialized) {
  for (auto& pair : serialized["leaves"]) {
    leafSelectivities_[pair["key"].asString()] =
        atof(pair["value"].asString().c_str());
  }
  for (auto& pair : serialized["joins"]) {
    joinSamples_[pair["key"].asString()] = std::make_pair<float, float>(
        atof(pair["lr"].asString().c_str()),
        atof(pair["rl"].asString().c_str()));
  }
  for (auto& pair : serialized["plans"]) {
    planHistory_[pair["key"].asString()] = NodePrediction{
        .cardinality =
            static_cast<float>(atof(pair["card"].asString().c_str()))};
  }
}

} // namespace facebook::velox::optimizer
