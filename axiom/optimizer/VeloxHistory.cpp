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
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/Schema.h"
#include "velox/common/base/AsyncSource.h"
#include "velox/type/Subfield.h"

#include <iostream>

DEFINE_double(
    cardinality_warning_threshold,
    5,
    "Log a warning if cardinality estimate is more than this many times off. 0 means no warnings.");

namespace facebook::axiom::optimizer {

namespace {

/// Compares two Variants. Returns true if left < right.
/// Both variants must be of the same typeKind.
/// Only supports numbers and strings. Returns false for other types.
bool variantLessThan(const velox::Variant& left, const velox::Variant& right) {
  if (left.kind() != right.kind()) {
    return false;
  }

  switch (left.kind()) {
    case velox::TypeKind::BOOLEAN:
      return !left.value<bool>() && right.value<bool>();
    case velox::TypeKind::TINYINT:
      return left.value<int8_t>() < right.value<int8_t>();
    case velox::TypeKind::SMALLINT:
      return left.value<int16_t>() < right.value<int16_t>();
    case velox::TypeKind::INTEGER:
      return left.value<int32_t>() < right.value<int32_t>();
    case velox::TypeKind::BIGINT:
      return left.value<int64_t>() < right.value<int64_t>();
    case velox::TypeKind::REAL:
      return left.value<float>() < right.value<float>();
    case velox::TypeKind::DOUBLE:
      return left.value<double>() < right.value<double>();
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY:
      return left.value<velox::StringView>() < right.value<velox::StringView>();
    case velox::TypeKind::TIMESTAMP:
      return left.value<velox::Timestamp>() < right.value<velox::Timestamp>();
    default:
      // Unsupported types for comparison
      return false;
  }
}

/// Returns true if the variant type supports min/max comparison.
bool supportsMinMaxComparison(velox::TypeKind kind) {
  switch (kind) {
    case velox::TypeKind::BOOLEAN:
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::BIGINT:
    case velox::TypeKind::REAL:
    case velox::TypeKind::DOUBLE:
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY:
    case velox::TypeKind::TIMESTAMP:
      return true;
    default:
      return false;
  }
}

/// Registers an optional variant with type conversion to the target TypeKind.
/// For numeric types, casts the variant to the requested type.
/// For string/binary types, keeps them as-is.
/// Returns nullptr if the optional has no value.
const velox::Variant* registerOptionalVariantWithType(
    const std::optional<velox::Variant>& opt,
    velox::TypeKind targetKind) {
  if (!opt.has_value()) {
    return nullptr;
  }

  const auto& variant = opt.value();
  auto sourceKind = variant.kind();

  // If types match, register as-is
  if (sourceKind == targetKind) {
    return registerVariant(variant);
  }

  // Handle string and binary types - no conversion needed
  if (targetKind == velox::TypeKind::VARCHAR ||
      targetKind == velox::TypeKind::VARBINARY) {
    VELOX_CHECK(
        sourceKind == velox::TypeKind::VARCHAR ||
            sourceKind == velox::TypeKind::VARBINARY,
        "Cannot convert {} to {}",
        sourceKind,
        targetKind);
    return registerVariant(variant);
  }

  // For numeric types, perform casting
  // First, convert source to either int64_t or double
  double doubleValue = 0;
  int64_t intValue = 0;
  bool isFloatingPoint = false;

  switch (sourceKind) {
    case velox::TypeKind::TINYINT:
      intValue = variant.value<int8_t>();
      break;
    case velox::TypeKind::SMALLINT:
      intValue = variant.value<int16_t>();
      break;
    case velox::TypeKind::INTEGER:
      intValue = variant.value<int32_t>();
      break;
    case velox::TypeKind::BIGINT:
      intValue = variant.value<int64_t>();
      break;
    case velox::TypeKind::REAL:
      doubleValue = variant.value<float>();
      isFloatingPoint = true;
      break;
    case velox::TypeKind::DOUBLE:
      doubleValue = variant.value<double>();
      isFloatingPoint = true;
      break;
    default:
      VELOX_FAIL(
          "Unsupported variant type for numeric conversion: {}", sourceKind);
  }

  // Now cast to the target type
  switch (targetKind) {
    case velox::TypeKind::TINYINT:
      return registerVariant(
          static_cast<int8_t>(isFloatingPoint ? doubleValue : intValue));
    case velox::TypeKind::SMALLINT:
      return registerVariant(
          static_cast<int16_t>(isFloatingPoint ? doubleValue : intValue));
    case velox::TypeKind::INTEGER:
      return registerVariant(
          static_cast<int32_t>(isFloatingPoint ? doubleValue : intValue));
    case velox::TypeKind::BIGINT:
      return registerVariant(
          static_cast<int64_t>(isFloatingPoint ? doubleValue : intValue));
    case velox::TypeKind::REAL:
      return registerVariant(
          static_cast<float>(isFloatingPoint ? doubleValue : intValue));
    case velox::TypeKind::DOUBLE:
      return registerVariant(
          static_cast<double>(isFloatingPoint ? doubleValue : intValue));
    default:
      VELOX_FAIL(
          "Unsupported target type for variant conversion: {}", targetKind);
  }
}

} // namespace

Value toValue(const connector::ColumnStatistics& stats, const Value& value) {
  Value result = value;

  // Set cardinality from numDistinct if available
  if (stats.numDistinct.has_value()) {
    const_cast<float&>(result.cardinality) =
        static_cast<float>(stats.numDistinct.value());
  }

  // Set min and max using registerOptionalVariantWithType to ensure
  // the variant type matches the column type
  const_cast<const velox::Variant*&>(result.min) =
      registerOptionalVariantWithType(stats.min, value.type->kind());
  const_cast<const velox::Variant*&>(result.max) =
      registerOptionalVariantWithType(stats.max, value.type->kind());

  // Set nullFraction from nullPct (converting percentage to fraction)
  const_cast<float&>(result.nullFraction) = stats.nullPct / 100.0f;

  return result;
}

void VeloxHistory::setBaseTableValues(
    const ConstraintMap& constraints,
    BaseTable& table) {
  // Update the Values in columns of BaseTable to correspond to the constraints
  // returned by conjunctsSelectivity
  for (const auto& [columnId, constrainedValue] : constraints) {
    // Find the column by ID and update its value
    for (auto* column : table.columns) {
      if (column->id() == columnId) {
        auto& value = const_cast<Value&>(column->value());
        // Update individual const fields using const_cast
        const_cast<float&>(value.cardinality) = constrainedValue.cardinality;
        const_cast<const velox::Variant*&>(value.min) = constrainedValue.min;
        const_cast<const velox::Variant*&>(value.max) = constrainedValue.max;
        const_cast<float&>(value.trueFraction) = constrainedValue.trueFraction;
        const_cast<float&>(value.nullFraction) = constrainedValue.nullFraction;
        const_cast<bool&>(value.nullable) = constrainedValue.nullable;
        break;
      }
    }
  }
}

void VeloxHistory::recordJoinSample(std::string_view key, float lr, float rl) {}

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
    std::lock_guard<std::recursive_mutex> l(mutex_);
    auto it = joinSamples_.find(keyPair.first);
    if (it != joinSamples_.end()) {
      if (keyPair.second) {
        return std::make_pair(it->second.second, it->second.first);
      }
      return it->second;
    }
  }

  auto rightTable = edge->rightTable()->as<BaseTable>()->schemaTable;
  auto leftTable = edge->leftTable()->as<BaseTable>()->schemaTable;

  std::pair<float, float> pair;
  uint64_t start = velox::getCurrentTimeMicro();
  if (keyPair.second) {
    pair = optimizer::sampleJoin(
        rightTable, edge->rightKeys(), leftTable, edge->leftKeys());
  } else {
    pair = optimizer::sampleJoin(
        leftTable, edge->leftKeys(), rightTable, edge->rightKeys());
  }

  {
    std::lock_guard<std::recursive_mutex> l(mutex_);
    joinSamples_[keyPair.first] = pair;
  }

  const bool trace = (options.traceFlags & OptimizerOptions::kSample) != 0;
  if (trace) {
    std::cout << "Sample join " << keyPair.first << ": " << pair.first << " :"
              << pair.second << " time="
              << velox::succinctMicros(velox::getCurrentTimeMicro() - start)
              << std::endl;
  }
  if (keyPair.second) {
    return std::make_pair(pair.second, pair.first);
  }
  return pair;
}

int64_t VeloxHistory::combineStatistics(
    const velox::connector::ConnectorTableHandlePtr& handle,
    float pct,
    const std::vector<velox::common::Subfield>& fields,
    std::vector<connector::ColumnStatistics>* statistics) const {
  // Get metadata and split manager
  auto* metadata =
      connector::ConnectorMetadata::metadata(handle->connectorId());
  auto* splitManager = metadata->splitManager();
  VELOX_CHECK_NOT_NULL(splitManager, "Split manager not found");

  // List all partitions
  auto partitions = splitManager->listPartitions(nullptr, handle);
  if (partitions.empty()) {
    return 0;
  }

  // Select partitions to sample
  std::vector<connector::PartitionHandlePtr> partitionsToSample;

  // Always include first partition
  partitionsToSample.push_back(partitions[0]);

  // Always include last partition if there's more than one
  if (partitions.size() > 1) {
    partitionsToSample.push_back(partitions.back());
  }

  // Sample a fraction of remaining partitions based on hash
  if (partitions.size() > 2) {
    for (size_t i = 1; i < partitions.size() - 1; ++i) {
      auto hashValue = folly::hasher<std::string>()(partitions[i]->partition());
      if (hashValue % 100 < static_cast<size_t>(pct)) {
        partitionsToSample.push_back(partitions[i]);
      }
    }
  }

  if (partitionsToSample.empty()) {
    return 0;
  }

  // Extract column names from subfields (only top-level columns)
  std::vector<std::string> columnNames;
  columnNames.reserve(fields.size());
  for (const auto& subfield : fields) {
    const auto& path = subfield.path();
    if (!path.empty()) {
      if (auto* nestedField =
              path[0]->as<velox::common::Subfield::NestedField>()) {
        columnNames.push_back(nestedField->name());
      }
    }
  }

  // Get partition statistics
  auto partitionStats =
      splitManager->getPartitionStatistics(partitionsToSample, columnNames);

  if (partitionStats.empty() || !statistics) {
    return 0;
  }

  // Initialize combined statistics for each column
  statistics->clear();
  statistics->resize(columnNames.size());

  int64_t totalRows = 0;
  int64_t sampledRows = 0;

  // Initialize statistics structures
  for (size_t colIdx = 0; colIdx < columnNames.size(); ++colIdx) {
    (*statistics)[colIdx].name = columnNames[colIdx];
  }

  // Combine statistics from all sampled partitions
  for (const auto& partitionStat : partitionStats) {
    if (!partitionStat) {
      continue;
    }

    sampledRows += partitionStat->numRows;

    // Process each column
    for (size_t colIdx = 0; colIdx < partitionStat->columnStatistics.size() &&
         colIdx < statistics->size();
         ++colIdx) {
      const auto& partColStat = partitionStat->columnStatistics[colIdx];
      auto& combinedStat = (*statistics)[colIdx];

      // numDistinct: take max
      if (partColStat.numDistinct.has_value()) {
        if (!combinedStat.numDistinct.has_value()) {
          combinedStat.numDistinct = partColStat.numDistinct.value();
        } else {
          combinedStat.numDistinct = std::max(
              combinedStat.numDistinct.value(),
              partColStat.numDistinct.value());
        }
      }

      // min: take minimum
      if (partColStat.min.has_value()) {
        if (!combinedStat.min.has_value()) {
          combinedStat.min = partColStat.min;
        } else if (
            supportsMinMaxComparison(partColStat.min.value().kind()) &&
            variantLessThan(
                partColStat.min.value(), combinedStat.min.value())) {
          combinedStat.min = partColStat.min;
        }
      }

      // max: take maximum
      if (partColStat.max.has_value()) {
        if (!combinedStat.max.has_value()) {
          combinedStat.max = partColStat.max;
        } else if (
            supportsMinMaxComparison(partColStat.max.value().kind()) &&
            variantLessThan(
                combinedStat.max.value(), partColStat.max.value())) {
          combinedStat.max = partColStat.max;
        }
      }

      // maxLength: take max
      if (partColStat.maxLength.has_value()) {
        if (!combinedStat.maxLength.has_value()) {
          combinedStat.maxLength = partColStat.maxLength.value();
        } else {
          combinedStat.maxLength = std::max(
              combinedStat.maxLength.value(), partColStat.maxLength.value());
        }
      }

      // Accumulate weighted values for averaging
      if (partitionStat->numRows > 0) {
        // nullPct: weighted average
        combinedStat.nullPct += partColStat.nullPct * partitionStat->numRows;

        // avgLength: weighted average
        if (partColStat.avgLength.has_value()) {
          if (!combinedStat.avgLength.has_value()) {
            combinedStat.avgLength = 0;
          }
          combinedStat.avgLength = combinedStat.avgLength.value() +
              partColStat.avgLength.value() * partitionStat->numRows;
        }
      }
    }
  }

  // Finalize weighted averages
  if (sampledRows > 0) {
    for (auto& stat : *statistics) {
      stat.nullPct /= sampledRows;
      if (stat.avgLength.has_value()) {
        stat.avgLength = stat.avgLength.value() / sampledRows;
      }
    }
  }

  // Estimate total rows by scaling based on sampling ratio
  if (!partitionsToSample.empty()) {
    float samplingRatio = static_cast<float>(partitions.size()) /
        static_cast<float>(partitionsToSample.size());
    totalRows = static_cast<int64_t>(sampledRows * samplingRatio);
  }

  return totalRows;
}

bool VeloxHistory::setLeafSelectivityDiscrete(
    BaseTable& table,
    const velox::RowTypePtr& scanType) {
  auto* runnerTable = table.schemaTable->connectorTable;
  if (!runnerTable) {
    return false;
  }

  auto [tableHandle, filters] =
      queryCtx()->optimization()->leafHandle(table.id());

  // Get the first layout's discrete predicate columns
  const auto& layouts = runnerTable->layouts();
  if (layouts.empty()) {
    return false;
  }

  const auto& discreteColumns = layouts[0]->discretePredicateColumns();
  if (discreteColumns.empty()) {
    return false;
  }

  // Create a set of discrete column names for quick lookup
  folly::F14FastSet<std::string_view> discreteColumnNames;
  for (auto* col : discreteColumns) {
    discreteColumnNames.insert(col->name());
  }

  // Step 1: Convert control column names to Subfields for combineStatistics
  auto controlNames = table.controlColumnNames();
  std::vector<velox::common::Subfield> subfields;
  subfields.reserve(controlNames.size());
  for (auto name : controlNames) {
    subfields.emplace_back(std::string(name));
  }

  // Step 2: Call combineStatistics to get statistics and estimated total rows
  std::vector<connector::ColumnStatistics> statistics;
  int64_t estimatedTotalRows =
      combineStatistics(tableHandle, 0.1f, subfields, &statistics);

  // Set firstLayoutScanCardinality to the estimated total number of rows in
  // the selected partitions
  if (estimatedTotalRows > 0) {
    table.firstLayoutScanCardinality = static_cast<float>(estimatedTotalRows);
  }

  // Step 3: Set the Value in optimizer::Columns of the BaseTable according to
  // ColumnStatistics
  for (size_t i = 0; i < controlNames.size() && i < statistics.size(); ++i) {
    // Find the column by name
    for (auto* column : table.columns) {
      if (column->name() == controlNames[i]) {
        auto& value = const_cast<Value&>(column->value());
        value = toValue(statistics[i], value);
        break;
      }
    }
  }

  // Step 4: Make a filters list excluding filters on discrete predicate columns
  ExprVector nonDiscreteFilters;
  auto allFilters = table.allFilters();
  for (auto* filter : allFilters) {
    // Check if the filter's left side (first argument) is a discrete column
    bool isDiscreteFilter = false;
    if (auto* call = filter->as<Call>()) {
      if (!call->args().empty()) {
        if (auto* leftCol = call->args()[0]->as<Column>()) {
          if (discreteColumnNames.contains(leftCol->name())) {
            isDiscreteFilter = true;
          }
        }
      }
    }
    if (!isDiscreteFilter) {
      nonDiscreteFilters.push_back(filter);
    }
  }

  // Step 5: Call conjunctsSelectivity on the non-discrete filters
  float selectivity = 1.0f;
  if (!nonDiscreteFilters.empty()) {
    PlanState state(*queryCtx()->optimization(), nullptr);
    ConstraintMap constraints;
    auto result =
        conjunctsSelectivity(state, nonDiscreteFilters, true, constraints);
    selectivity = result.trueFraction;

    // Step 6: Update the Values according to constraints from
    // conjunctsSelectivity
    setBaseTableValues(constraints, table);
  }

  // Step 7: If sampling is enabled, check cache and potentially sample
  auto options = queryCtx()->optimization()->options();
  if (options.sampleFilters) {
    const auto handleString = tableHandle->toString();
    {
      auto it = leafSelectivities_.find(handleString);
      if (it != leafSelectivities_.end()) {
        // Use cached selectivity
        table.filterSelectivity = it->second;
        return true;
      }
    }

    // Sample the table to get actual selectivity
    const uint64_t start = velox::getCurrentTimeMicro();
    auto sample =
        runnerTable->layouts()[0]->sample(tableHandle, 1, filters, scanType);
    VELOX_CHECK_GE(sample.first, 0);
    VELOX_CHECK_GE(sample.first, sample.second);

    if (sample.first == 0) {
      table.filterSelectivity = 1;
    } else {
      // Replace selectivity from conjunctsSelectivity with sampled selectivity
      // but keep the column constraints from conjunctsSelectivity
      table.filterSelectivity = std::max<float>(0.9f, sample.second) /
          static_cast<float>(sample.first);
    }
    recordLeafSelectivity(handleString, table.filterSelectivity, false);

    bool trace = (options.traceFlags & OptimizerOptions::kSample) != 0;
    if (trace) {
      std::cout << "Sampled scan (discrete) " << handleString << "= "
                << table.filterSelectivity << " time= "
                << velox::succinctMicros(velox::getCurrentTimeMicro() - start)
                << std::endl;
    }
  } else {
    // Use selectivity from conjunctsSelectivity
    table.filterSelectivity = selectivity;
  }

  return true;
}

void VeloxHistory::setLeafSelectivityNoDiscrete(
    BaseTable& table,
    const velox::RowTypePtr& scanType) {
  auto options = queryCtx()->optimization()->options();
  auto [tableHandle, filters] =
      queryCtx()->optimization()->leafHandle(table.id());
  const auto handleString = tableHandle->toString();

  // Set firstLayoutScanCardinality to the schema table cardinality
  table.firstLayoutScanCardinality = table.schemaTable->cardinality;

  // If cardinality is <= 1, get the actual row count from partition statistics
  if (table.firstLayoutScanCardinality <= 1) {
    auto* runnerTable = table.schemaTable->connectorTable;
    if (runnerTable) {
      auto* metadata =
          connector::ConnectorMetadata::metadata(tableHandle->connectorId());
      auto* splitManager = metadata->splitManager();
      if (splitManager) {
        // List partitions - expect a single partition for unpartitioned tables
        auto partitions = splitManager->listPartitions(nullptr, tableHandle);
        if (!partitions.empty()) {
          // Get partition statistics for the single partition
          std::vector<std::string> columns; // Empty columns list is fine
          auto partitionStats =
              splitManager->getPartitionStatistics(partitions, columns);
          if (!partitionStats.empty() && partitionStats[0]) {
            int64_t numRows = partitionStats[0]->numRows;
            if (numRows > 0) {
              table.firstLayoutScanCardinality = static_cast<float>(numRows);
            }
          }
        }
      }
    }
  }

  // Step 1: Reset the Value in the columns of the BaseTable to correspond to
  // the Value in the columns of the SchemaTable
  for (auto* column : table.columns) {
    // Only reset top level columns.
    if (column->path()) {
      continue;
    }
    if (auto* schemaCol = column->schemaColumn()) {
      auto& value = const_cast<Value&>(column->value());
      value = schemaCol->value();
    }
  }

  // Step 2: Check if sampling should be done
  auto* runnerTable = table.schemaTable->connectorTable;
  bool shouldSample =
      runnerTable && (options.sampleFilters || options.sampleFiltersIfNoStats);

  std::optional<float> sampledSelectivity;

  if (shouldSample) {
    // Check for cached selectivity
    {
      auto it = leafSelectivities_.find(handleString);
      if (it != leafSelectivities_.end()) {
        sampledSelectivity = it->second;
      }
    }

    // If no cached selectivity, perform sampling
    if (!sampledSelectivity.has_value()) {
      const uint64_t start = velox::getCurrentTimeMicro();
      auto sample =
          runnerTable->layouts()[0]->sample(tableHandle, 1, filters, scanType);
      VELOX_CHECK_GE(sample.first, 0);
      VELOX_CHECK_GE(sample.first, sample.second);

      if (sample.first == 0) {
        sampledSelectivity = 1.0f;
      } else {
        // When finding no hits, do not make a selectivity of 0 because this
        // makes /0 or *0 and *0 is 0, which makes any subsequent operations 0
        // regardless of cost. So as not to underflow, count non-existent as 0.9
        // rows.
        sampledSelectivity = std::max<float>(0.9f, sample.second) /
            static_cast<float>(sample.first);
      }
      recordLeafSelectivity(handleString, sampledSelectivity.value(), false);

      bool trace = (options.traceFlags & OptimizerOptions::kSample) != 0;
      if (trace) {
        std::cout << "Sampled scan " << handleString << "= "
                  << sampledSelectivity.value() << " time= "
                  << velox::succinctMicros(velox::getCurrentTimeMicro() - start)
                  << std::endl;
      }
    }
  }

  // Step 3: Always call conjunctsSelectivity on all filters and update values
  auto allFilters = table.allFilters();
  float conjunctsSelectivityValue = 1.0f;

  if (!allFilters.empty()) {
    PlanState state(*queryCtx()->optimization(), nullptr);
    ConstraintMap constraints;

    auto selectivity =
        conjunctsSelectivity(state, allFilters, true, constraints);
    conjunctsSelectivityValue = selectivity.trueFraction;

    // Update column values based on constraints from
    // filter analysis
    setBaseTableValues(constraints, table);
  }

  // Step 4: Use sampling selectivity if available, otherwise use
  // conjunctsSelectivity
  if (sampledSelectivity.has_value()) {
    table.filterSelectivity = sampledSelectivity.value();
  } else {
    table.filterSelectivity = conjunctsSelectivityValue;
  }
}

void VeloxHistory::setLeafSelectivity(
    BaseTable& table,
    const velox::RowTypePtr& scanType) {
  // Try to handle discrete predicate columns first
  if (setLeafSelectivityDiscrete(table, scanType)) {
    return;
  }

  // Handle non-discrete case
  setLeafSelectivityNoDiscrete(table, scanType);
}

void VeloxHistory::guessFanouts(std::span<JoinEdge*> edges) {
  // Process edges in a single pass: check cache, and divide uncached edges
  // into uniqueTablePairs and remaining based on sampleKey uniqueness
  std::vector<JoinEdge*> uniqueTablePairs;
  std::vector<JoinEdge*> remaining;
  folly::F14FastSet<std::string> seenUncachedKeys;

  {
    std::lock_guard<std::recursive_mutex> l(mutex_);
    for (auto* edge : edges) {
      auto keyPair = edge->sampleKey();

      if (!keyPair.first.empty() && joinSamples_.contains(keyPair.first)) {
        // This edge is covered by the cache, call guessFanout directly
        edge->guessFanout();
      } else if (!keyPair.first.empty()) {
        // Uncached edge with a valid sampleKey
        if (!seenUncachedKeys.contains(keyPair.first)) {
          // First occurrence of this uncached key
          seenUncachedKeys.insert(keyPair.first);
          uniqueTablePairs.push_back(edge);
        } else {
          // Duplicate key
          remaining.push_back(edge);
        }
      } else {
        // No valid sampleKey, add to remaining
        remaining.push_back(edge);
      }
    }
  }
  // Mutex is released here

  // Process uniqueTablePairs in parallel using AsyncSource
  if (!uniqueTablePairs.empty()) {
    using ErrorPtr = std::exception_ptr;
    std::vector<std::shared_ptr<velox::AsyncSource<ErrorPtr>>> asyncSources;
    asyncSources.reserve(uniqueTablePairs.size());

    // Enable thread-safe mode before async operations
    {
      queryCtx()->setThreadSafe(true);
      SCOPE_EXIT {
        queryCtx()->setThreadSafe(false);
      };

      for (auto* edge : uniqueTablePairs) {
        // Capture the current queryCtx to use in the async operation
        auto* capturedCtx = queryCtx();
        auto asyncSource = std::make_shared<velox::AsyncSource<ErrorPtr>>(
            [edge, capturedCtx]() -> std::unique_ptr<ErrorPtr> {
              try {
                // Save the current thread's queryCtx
                auto* savedCtx = queryCtx();
                // Set to the captured context for the duration of this
                // operation
                queryCtx() = capturedCtx;
                // Restore the original queryCtx on exit
                SCOPE_EXIT {
                  queryCtx() = savedCtx;
                };

                edge->guessFanout();
                return nullptr;
              } catch (...) {
                return std::make_unique<ErrorPtr>(std::current_exception());
              }
            });
        asyncSources.push_back(asyncSource);
      }

      // Enqueue prepare() calls on the executor
      auto* optimization = queryCtx()->optimization();
      if (auto executor = optimization->veloxQueryCtx()->executor()) {
        for (auto& asyncSource : asyncSources) {
          executor->add([asyncSource]() { asyncSource->prepare(); });
        }
      }
      // Call move() on all AsyncSources and collect errors
      std::unique_ptr<ErrorPtr> firstError = nullptr;
      for (auto& asyncSource : asyncSources) {
        auto error = asyncSource->move();
        if (error && !firstError) {
          firstError = std::move(error);
        }
      }

      // Rethrow the first error if any
      if (firstError) {
        std::rethrow_exception(*firstError);
      }
    } // End of thread-safe block
  }

  // Finally, call guessFanout for edges in the remaining list
  for (auto* edge : remaining) {
    edge->guessFanout();
  }
}

namespace {

const velox::core::TableScanNode* findScan(
    const velox::core::PlanNodeId& id,
    const runner::MultiFragmentPlanPtr& plan) {
  for (const auto& fragment : plan->fragments()) {
    if (auto node = velox::core::PlanNode::findFirstNode(
            fragment.fragment.planNode.get(),
            [&](const auto* node) { return node->id() == id; })) {
      return dynamic_cast<const velox::core::TableScanNode*>(node);
    }
  }

  return nullptr;
}

void logPrediction(std::string_view message) {
  if (FLAGS_cardinality_warning_threshold != 0) {
    LOG(WARNING) << message;
  }
}

void predictionWarnings(
    const PlanAndStats& plan,
    const velox::core::PlanNodeId& id,
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
    logPrediction(
        fmt::format(
            "Node {} actual={} predicted={} key={}",
            id,
            actualRows,
            predictedRows,
            historyKey));
  } else {
    auto ratio = static_cast<float>(actualRows) / predictedRows;
    auto threshold = FLAGS_cardinality_warning_threshold;
    if (ratio < 1 / threshold || ratio > threshold) {
      logPrediction(
          fmt::format(
              "Node {} actual={} predicted={} key={}",
              id,
              actualRows,
              predictedRows,
              historyKey));
    }
  }
}

} // namespace

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
        uint64_t actualRows{};
        {
          std::lock_guard<std::recursive_mutex> l(mutex_);
          actualRows = op.outputPositions;
          planHistory_[keyIt->second] =
              NodePrediction{.cardinality = static_cast<float>(actualRows)};
        }
        if (op.operatorType == "TableScanOperator") {
          if (const auto* scan = findScan(op.planNodeId, plan.plan)) {
            std::string handle = scan->tableHandle()->toString();
            recordLeafSelectivity(
                handle,
                static_cast<float>(actualRows) /
                    std::max(1.F, static_cast<float>(op.rawInputPositions)),
                true);
          }
        }
        if (it != plan.prediction.end()) {
          auto predictedRows = it->second.cardinality;
          predictionWarnings(
              plan,
              op.planNodeId,
              static_cast<int64_t>(actualRows),
              predictedRows);
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
  auto toFloat = [](const folly::dynamic& v) {
    // TODO Don't use atof.
    return static_cast<float>(atof(v.asString().c_str()));
  };
  for (auto& pair : serialized["leaves"]) {
    leafSelectivities_[pair["key"].asString()] = toFloat(pair["value"]);
  }
  for (auto& pair : serialized["joins"]) {
    joinSamples_[pair["key"].asString()] =
        std::make_pair<float, float>(toFloat(pair["lr"]), toFloat(pair["rl"]));
  }
  for (auto& pair : serialized["plans"]) {
    planHistory_[pair["key"].asString()] =
        NodePrediction{.cardinality = toFloat(pair["card"])};
  }
}

} // namespace facebook::axiom::optimizer
