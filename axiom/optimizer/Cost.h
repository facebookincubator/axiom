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

#include "axiom/optimizer/RelationOp.h"

namespace facebook::axiom::optimizer {

/// Record the history data for a tracked PlanNode.
struct NodePrediction {
  /// Result cardinality for the top node of the recorded plan.
  float cardinality;
  ///  Peak total memory for the top node.
  float peakMemory{0};
  /// CPU estimate in optimizer internal units.
  float cpu{0};
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
      const velox::RowTypePtr& scanType) = 0;

  virtual void recordJoinSample(std::string_view key, float lr, float rl) = 0;

  virtual std::pair<float, float> sampleJoin(JoinEdge* edge) = 0;

  virtual void recordLeafSelectivity(
      std::string_view handle,
      float selectivity,
      bool overwrite = true) {
    std::lock_guard<std::mutex> l(mutex_);
    if (!overwrite && leafSelectivities_.contains(handle)) {
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
  folly::F14FastMap<std::string, float> leafSelectivities_;
};

/// Collection of per operation costs for a target system.  The base
/// unit is the time to memcpy a cache line in a large memcpy on one
/// core. This is ~6GB/s, so ~10ns. Other times are expressed as
/// multiples of that.
struct Costs {
  /// Average clock of 2GHz
  static constexpr float kClocksPerUnit = 20;

  static float byteShuffleCost() {
    return 12; // ~500MB/s
  }

  /// Returns the latency in clocks based on working set size and access
  /// size, accounting for L1/L2/L3 cache hierarchy and memory latency.
  static float cacheMissClocks(float workingSet, float accessBytes);

  /// Approximation of cost of the hash table access in hash
  /// probe/build/aggregation. Relative to expected cache misses.
  static float hashTableCost(float cardinality) {
    return cacheMissClocks(8 * cardinality, 1) / kClocksPerUnit;
  }

  /// Approximation of the cost of accessing a hash table row in join or
  /// aggregation. Relative to expected cache misses.
  static float hashRowCost(float cardinality, int32_t rowBytes) {
    return cacheMissClocks(cardinality * rowBytes, rowBytes) / kClocksPerUnit;
  }

  static float hashBuildCost(float cardinality, int32_t rowBytes) {
    // One probe per row, one write per row, 1 read for partitioning build, 1
    // read for insert, estimated 1 row for colision in insert.
    return hashTableCost(cardinality) + 5 * hashRowCost(cardinality, rowBytes);
  }

  static constexpr float kKeyCompareCost =
      0.5; // ~10 instructions to find, decode and an compare

  static constexpr float kColumnRowCost = 5;
  static constexpr float kColumnByteCost = 0.1;

  /// Cost of hash function on one column.
  static constexpr float kHashColumnCost = 0.3;

  /// Cost of getting a column from a hash table
  static constexpr float kHashExtractColumnCost = 0.3;

  /// Cost of sum/min/max. A little more than getting a value from the a hash
  /// table.
  static constexpr float kSimpleAggregateCost = kHashExtractColumnCost * 1.5;

  /// Bytes of overhead for a hash table row: ~12 bytes for the table and ~12
  /// bytes for the row.
  static constexpr float kHashRowBytes = 24;

  /// Minimal cost of calling a filter function, e.g. comparing two numeric
  /// exprss.
  static constexpr float kMinimumFilterCost = 2;

  // Multiplier to apply to shuffle byte volume to get CPU cost. A
  // complete cost model will need to consider the count of
  // destinations, number of partition keys etc.
  static constexpr float kByteShuffleCost = 0.3;
};

/// Returns shuffle cost for a single row. Depends on the number of types of
/// columns.
float shuffleCost(const ColumnVector& columns);

/// Returns shuffle cost for a single row produced by specified expressions.
/// Depends on the number of result types of the expressions.
float shuffleCost(const ExprVector& exprs);

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
    const ExprVector& rightKeys);

} // namespace facebook::axiom::optimizer
