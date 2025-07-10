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

#pragma once

#include "optimizer/RelationOp.h" //@manual

namespace facebook::velox::optimizer {

/// Record the history data for a tracked PlanNode.
struct NodePrediction {
  /// Result cardinality for the top node of the recorded plan.
  float cardinality;
  ///  Peak total memory for the top node.
  float peakMemory{0};
};

/// Interface to historical query cost and cardinality
/// information. There is one long lived instance per
/// process. Public functions are thread safe since multiple
/// concurrent Optimizations may access and update the same History.
class History {
 public:
  virtual ~History() = default;

  /// Returns a historical cost for 'op' or nullopt ir if no data. Matches the
  /// full 'op' tree to history. The exactness of the match depends on the
  /// History implementation. Implementations may extrapolate  from approximate
  /// matches. A Cost from this will be used over a non-historical cost if
  /// available. Not const since some extrapolated data can be kept.
  virtual std::optional<Cost> findCost(RelationOp& op) = 0;

  /// Records that the cost and cardinality of 'op' was 'cost' as observed from
  /// execution.
  virtual void recordCost(const RelationOp& op, Cost cost) = 0;

  /// Sets 'filterSelectivity' of 'baseTable' from historical data. Considers
  /// filters only and does not return a cost since the cost depends on the
  /// columns extracted. This is used first for coming up with join orders. The
  /// plan candidates are then made and findCost() is used to access historical
  /// cost and plan cardinality.
  virtual bool setLeafSelectivity(
      BaseTable& baseTable,
      RowTypePtr scanType) = 0;

  virtual void recordJoinSample(const std::string& key, float lr, float rl) = 0;

  virtual std::pair<float, float> sampleJoin(JoinEdge* edge) = 0;

  /// Returns the history record for the plan fragment represented in 'key'.
  /// nullptr if not recorded.
  virtual NodePrediction* getHistory(const std::string key) = 0;

  virtual void setHistory(const std::string& key, NodePrediction history) = 0;

  virtual void recordLeafSelectivity(
      const std::string& handle,
      float selectivity,
      bool overwrite = true) {
    std::lock_guard<std::mutex> l(mutex_);
    if (!overwrite &&
        leafSelectivities_.find(handle) != leafSelectivities_.end()) {
      return;
    }
    leafSelectivities_[handle] = selectivity;
  }

  virtual folly::dynamic serialize() = 0;

  virtual void update(folly::dynamic& serialized) = 0;

  void saveToFile(const std::string& path);

  void updateFromFile(const std::string& path);

 protected:
  // serializes access to all data members.
  std::mutex mutex_;

  /// Memo for selectivity keyed on ConnectorTableHandle::toString().
  /// Values between 0 and 1.
  std::unordered_map<std::string, float> leafSelectivities_;
};

float shuffleCost(const ColumnVector& columns);

float shuffleCost(const ExprVector& columns);

/// Returns cost of 'expr' for one row, excluding cost of subexpressions.
float selfCost(ExprCP expr);

/// Returns the per row cost of 'expr' and its subexpressions, excluding
/// 'notCounting', which represents already computed subtrees of 'expr'.
float costWithChildren(ExprCP expr, const PlanObjectSet& notCounting);

/// Samples the join of 'left' and 'right' on 'leftKeys' and
/// 'rightKeys'. Returns the number of hits on the right for one row
/// of left and the number of hits on the left for one row on the
/// right.
std::pair<float, float> sampleJoin(
    SchemaTableCP left,
    const ExprVector& leftKeys,
    SchemaTableCP right,
    const ExprVector& rightColumns);

} // namespace facebook::velox::optimizer
