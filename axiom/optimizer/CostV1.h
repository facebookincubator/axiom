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

#include "axiom/optimizer/Cost.h"
#include "axiom/optimizer/RelationOp.h"
#include "velox/connectors/Connector.h"

namespace facebook::axiom::optimizer {

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

  /// Estimates and sets 'filteredCardinality' on 'baseTable'. Also narrows
  /// column Value constraints (cardinality, min, max, trueFraction,
  /// nullFraction, nullable) based on the table's filters. Estimates
  /// selectivity from column statistics (min/max, NDV, null fraction),
  /// optionally refined by sampling the table's layout.
  ///
  /// Must be called at most once per base table. Calling multiple times
  /// compounds constraint narrowing, producing incorrect estimates.
  ///
  /// 'tableHandle' is the connector-level representation of the table's current
  /// filters.
  virtual void estimateLeafSelectivity(
      BaseTable& baseTable,
      const velox::connector::ConnectorTableHandlePtr& tableHandle) = 0;

  virtual void recordJoinSample(std::string_view key, float lr, float rl) = 0;

  /// Estimates join fanout by sampling actual data. Returns {rlFanout,
  /// lrFanout}: the average number of matching rows on each side for one row
  /// on the other side. Returns {0, 0} if sampling is not possible.
  virtual std::pair<float, float> sampleJoin(JoinEdge* edge) = 0;

  virtual folly::dynamic serialize() = 0;

  virtual void update(folly::dynamic& serialized) = 0;

  void saveToFile(const std::string& path);

  void updateFromFile(const std::string& path);

 protected:
  // Records a sampled leaf selectivity value for the given handle.
  void recordSampledLeafSelectivity(
      std::string_view handle,
      float selectivity,
      bool overwrite = true);

  // Returns the sampled selectivity for the given handle, or std::nullopt if
  // not cached.
  std::optional<float> findSampledLeafSelectivity(std::string_view handle);

  // Returns the sampled leaf selectivities map for serialization.
  const folly::F14FastMap<std::string, float>& sampledLeafSelectivities()
      const {
    return sampledLeafSelectivities_;
  }

  // Serializes access to all data members.
  std::mutex mutex_;

 private:
  // Cache for sampled selectivity values keyed on
  // ConnectorTableHandle::toString(). Values between 0 and 1. Only populated
  // when sampling is enabled.
  folly::F14FastMap<std::string, float> sampledLeafSelectivities_;
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
