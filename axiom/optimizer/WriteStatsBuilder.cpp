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
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/expression/SignatureBinder.h"

namespace facebook::axiom::optimizer {
namespace {

using AggregateSignatures =
    std::vector<velox::exec::AggregateFunctionSignaturePtr>;

AggregateSignatures getAggregateSignatures(const std::string& functionName) {
  auto signatures = velox::exec::getAggregateFunctionSignatures(functionName);
  VELOX_CHECK(
      signatures.has_value(),
      "Aggregate function is not registered: {}",
      functionName);
  return std::move(*signatures);
}

bool supportsAggregateInputType(
    const AggregateSignatures& signatures,
    const velox::TypePtr& inputType) {
  const std::vector<velox::TypePtr> inputTypes{inputType};
  for (const auto& signature : signatures) {
    velox::exec::SignatureBinder binder(
        *signature, inputTypes, velox::TypeCoercer::defaults());
    if (binder.tryBind() &&
        binder.tryResolveType(signature->intermediateType()) != nullptr) {
      return true;
    }
  }
  return false;
}

} // namespace

WriteStatsBuilder::WriteStatsBuilder(
    const connector::Table& table,
    const velox::RowTypePtr& inputType,
    const connector::ConnectorWriteHandle& writeHandle,
    int32_t numDrivers,
    int32_t numWorkers) {
  // A delete's writer receives row identity, not the table's columns.
  if (writeHandle.deleteInput() != nullptr) {
    return;
  }

  // Statistics are read positionally, so the input's columns are the table's.
  VELOX_CHECK_EQ(
      inputType->size(),
      table.type()->size(),
      "A write's input is the table's columns, in order");

  const auto* registry = FunctionRegistry::instance();
  const auto& statsAggs = registry->statsAggregates();
  const auto& countName = registry->count();
  if (!statsAggs.has_value() || !countName.has_value()) {
    return;
  }

  needsMerge_ = numDrivers > 1 || numWorkers > 1;
  needsFinalMerge_ = numWorkers > 1;

  const auto minSignatures = getAggregateSignatures(statsAggs->min);
  const auto maxSignatures = getAggregateSignatures(statsAggs->max);
  const auto countIfSignatures = getAggregateSignatures(statsAggs->countIf);
  const auto approxDistinctSignatures =
      getAggregateSignatures(statsAggs->approxDistinct);

  // When there are grouping keys, add a count(*) aggregate (no input
  // columns) as the first stats channel to provide exact per-group row
  // counts.
  const bool hasGroupingKeys = !writeHandle.statsGroupingKeys().empty();
  if (hasGroupingKeys) {
    static constexpr auto kRowCount = "$row_count";

    velox::core::AggregationNode::Aggregate agg;
    agg.call = std::make_shared<velox::core::CallTypedExpr>(
        velox::BIGINT(), *countName);
    aggregates_.push_back(std::move(agg));
    aggregateNames_.push_back(kRowCount);
  }

  for (velox::column_index_t i = 0; i < inputType->size(); ++i) {
    const auto& colType = inputType->childAt(i);

    auto inputField = std::make_shared<velox::core::FieldAccessTypedExpr>(
        colType, inputType->nameOf(i));

    ColumnStatMapping mapping;
    mapping.columnName = table.type()->nameOf(i);

    auto addAggregate = [&](const std::string& funcName,
                            const velox::TypePtr& returnType) {
      velox::core::AggregationNode::Aggregate agg;
      agg.call = std::make_shared<velox::core::CallTypedExpr>(
          returnType, funcName, inputField);
      agg.rawInputTypes = {colType};
      aggregates_.push_back(std::move(agg));

      aggregateNames_.push_back(
          fmt::format("{}.{}", table.type()->nameOf(i), funcName));
    };

    auto addAggregateIfSupported = [&](const AggregateSignatures& signatures,
                                       ColumnStatField field,
                                       const std::string& functionName,
                                       const velox::TypePtr& returnType) {
      if (supportsAggregateInputType(signatures, colType)) {
        mapping.fields.push_back(field);
        addAggregate(functionName, returnType);
      }
    };

    mapping.fields.push_back(ColumnStatField::kCount);
    addAggregate(*countName, velox::BIGINT());

    switch (colType->kind()) {
      case velox::TypeKind::BOOLEAN:
        addAggregateIfSupported(
            countIfSignatures,
            ColumnStatField::kCountIf,
            statsAggs->countIf,
            velox::BIGINT());
        break;

      case velox::TypeKind::TINYINT:
      case velox::TypeKind::SMALLINT:
      case velox::TypeKind::INTEGER:
      case velox::TypeKind::BIGINT:
      case velox::TypeKind::REAL:
      case velox::TypeKind::DOUBLE:
      case velox::TypeKind::TIMESTAMP:
      case velox::TypeKind::VARCHAR:
      case velox::TypeKind::VARBINARY:
        addAggregateIfSupported(
            minSignatures, ColumnStatField::kMin, statsAggs->min, colType);
        addAggregateIfSupported(
            maxSignatures, ColumnStatField::kMax, statsAggs->max, colType);
        addAggregateIfSupported(
            approxDistinctSignatures,
            ColumnStatField::kApproxDistinct,
            statsAggs->approxDistinct,
            velox::BIGINT());
        break;

      default:
        break;
    }

    statsMapping_.columns.push_back(std::move(mapping));
  }

  if (aggregates_.empty() || (hasGroupingKeys && aggregates_.size() == 1)) {
    // No per-column stats (or only $row_count with no column stats).
    aggregates_.clear();
    aggregateNames_.clear();
    statsMapping_.columns.clear();
    return;
  }

  // Build grouping keys. The write handle specifies which table columns to
  // group by (e.g. partition columns). Column names come from the table
  // schema (e.g. "pk") but inputType uses write input names from the query
  // output (e.g. "expr_0"). Look up by position in the table schema to
  // find the matching input column.
  const auto& groupingKeyNames = writeHandle.statsGroupingKeys();
  for (const auto& keyName : groupingKeyNames) {
    auto idx = table.type()->getChildIdx(keyName);
    groupingKeys_.push_back(
        std::make_shared<velox::core::FieldAccessTypedExpr>(
            inputType->childAt(idx), inputType->nameOf(idx)));
  }

  statsMapping_.numGroupingKeys =
      static_cast<uint32_t>(groupingKeyNames.size());
  statsMapping_.groupingKeyNames = groupingKeyNames;

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
            intermediateType,
            aggregate.call->name(),
            aggregate.call->inputs()[0]);
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
    aggregate.call = std::make_shared<velox::core::CallTypedExpr>(
        outputAggregates[i].call->type(),
        aggregate.call->name(),
        std::make_shared<velox::core::FieldAccessTypedExpr>(
            sourceType->findChild(aggregateNames_[i]), aggregateNames_[i]));
    aggregates.push_back(std::move(aggregate));
  }

  return {groupingKeys_, step, aggregateNames_, std::move(aggregates)};
}

} // namespace facebook::axiom::optimizer
