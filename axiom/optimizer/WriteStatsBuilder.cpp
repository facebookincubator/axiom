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

#include "axiom/optimizer/WriteStatsBuilder.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "velox/exec/AggregateFunctionRegistry.h"

namespace facebook::axiom::optimizer {
WriteStatsBuilder::WriteStatsBuilder(
    const connector::Table& table,
    const velox::RowTypePtr& inputType,
    const connector::ConnectorWriteHandle& writeHandle,
    int32_t numDrivers,
    int32_t numWorkers) {
  const auto& registry = queryCtx()->functionRegistry();
  const auto& statsAggs = registry.statsAggregates();
  const auto& countName = registry.count();
  if (!statsAggs.has_value() || !countName.has_value()) {
    return;
  }

  needsMerge_ = numDrivers > 1 || numWorkers > 1;
  needsFinalMerge_ = numWorkers > 1;

  auto request = writeHandle.statisticsRequest(table.type());
  request.addDependencies();
  if (request.statistics.empty()) {
    return;
  }
  velox::core::AggregationNode::Aggregate rowCount;
  rowCount.call =
      std::make_shared<velox::core::CallTypedExpr>(velox::BIGINT(), *countName);
  aggregates_.push_back(std::move(rowCount));
  aggregateNames_.push_back("$row_count");

  // The write input matches the table's columns by position, not by name. Map
  // each table column name to its input column once, then look up statistic
  // columns and grouping keys by name.
  std::unordered_map<std::string, velox::core::FieldAccessTypedExprPtr> fields;
  for (auto i = 0; i < table.type()->size(); ++i) {
    fields.emplace(
        table.type()->nameOf(i),
        std::make_shared<velox::core::FieldAccessTypedExpr>(
            inputType->childAt(i), inputType->nameOf(i)));
  }
  std::vector<bool> collected;
  collected.reserve(request.statistics.size());
  for (const auto& stat : request.statistics) {
    const auto& input = fields.at(stat.column);
    auto aggregate = registry.statisticAggregate(stat, input->type());
    if (!aggregate.has_value()) {
      collected.push_back(false);
      continue;
    }
    std::vector<velox::core::TypedExprPtr> inputs{input};
    for (size_t i = 0; i < stat.arguments.size(); ++i) {
      inputs.push_back(
          std::make_shared<velox::core::ConstantTypedExpr>(
              aggregate->argumentTypes[i + 1], stat.arguments[i]));
    }
    velox::core::AggregationNode::Aggregate agg;
    agg.call = std::make_shared<velox::core::CallTypedExpr>(
        aggregate->resultType, aggregate->name, inputs);
    agg.rawInputTypes = std::move(aggregate->argumentTypes);
    aggregates_.push_back(std::move(agg));
    aggregateNames_.push_back(fmt::format("$stat_{}", aggregateNames_.size()));
    collected.push_back(true);
  }
  // If only the row count is left, collect no statistics at all.
  if (aggregates_.size() == 1) {
    aggregates_.clear();
    aggregateNames_.clear();
    return;
  }
  for (const auto& name : request.groupingColumns) {
    groupingKeys_.push_back(fields.at(name));
  }
  statsMapping_.request = std::move(request);
  statsMapping_.collected = std::move(collected);

  hasStats_ = true;
}

velox::core::ColumnStatsSpec WriteStatsBuilder::writeSpec() const {
  VELOX_CHECK(hasStats_);
  auto step = needsMerge_ ? velox::core::AggregationNode::Step::kPartial
                          : velox::core::AggregationNode::Step::kSingle;

  auto aggregates = aggregates_;
  if (needsMerge_) {
    for (auto& aggregate : aggregates) {
      auto intermediateType = velox::exec::resolveIntermediateType(
          aggregate.call->name(), aggregate.rawInputTypes);
      if (aggregate.call->inputs().empty()) {
        aggregate.call = std::make_shared<velox::core::CallTypedExpr>(
            intermediateType, aggregate.call->name());
      } else {
        aggregate.call = std::make_shared<velox::core::CallTypedExpr>(
            intermediateType, aggregate.call->name(), aggregate.call->inputs());
      }
    }
  }

  return {groupingKeys_, step, aggregateNames_, std::move(aggregates)};
}

velox::core::ColumnStatsSpec WriteStatsBuilder::localMergeSpec(
    const velox::RowTypePtr& sourceType) const {
  VELOX_CHECK(needsMerge());
  if (needsFinalMerge_) {
    return buildMergeSpec(
        velox::core::AggregationNode::Step::kIntermediate,
        sourceType,
        writeSpec().aggregates);
  }

  return buildMergeSpec(
      velox::core::AggregationNode::Step::kFinal, sourceType, aggregates_);
}

velox::core::ColumnStatsSpec WriteStatsBuilder::finalMergeSpec(
    const velox::RowTypePtr& sourceType) const {
  VELOX_CHECK(needsFinalMerge());
  return buildMergeSpec(
      velox::core::AggregationNode::Step::kFinal, sourceType, aggregates_);
}

velox::core::ColumnStatsSpec WriteStatsBuilder::buildMergeSpec(
    velox::core::AggregationNode::Step step,
    const velox::RowTypePtr& sourceType,
    const std::vector<velox::core::AggregationNode::Aggregate>&
        outputAggregates) const {
  std::vector<velox::core::AggregationNode::Aggregate> aggregates;
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    auto aggregate = aggregates_[i];
    std::vector<velox::core::TypedExprPtr> inputs{
        std::make_shared<velox::core::FieldAccessTypedExpr>(
            sourceType->findChild(aggregateNames_[i]), aggregateNames_[i])};
    const auto& originalInputs = aggregates_[i].call->inputs();
    // Constant arguments are the same at every aggregation step, so pass them
    // again.
    for (size_t arg = 1; arg < originalInputs.size(); ++arg) {
      inputs.push_back(originalInputs[arg]);
    }
    aggregate.call = std::make_shared<velox::core::CallTypedExpr>(
        outputAggregates[i].call->type(),
        aggregate.call->name(),
        std::move(inputs));
    aggregates.push_back(std::move(aggregate));
  }

  return {groupingKeys_, step, aggregateNames_, std::move(aggregates)};
}

} // namespace facebook::axiom::optimizer
