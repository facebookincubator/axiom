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
#include "axiom/optimizer/ToVelox.h"
#include "velox/common/base/AsyncSource.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/type/Subfield.h"

#include <algorithm>
#include <iostream>
#include <variant>

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

  // Set size from avgLength if available
  if (stats.avgLength.has_value()) {
    const_cast<float&>(result.size) =
        static_cast<float>(stats.avgLength.value());
  }

  // Handle complex types with children
  if (!stats.children.empty()) {
    auto typeKind = value.type->kind();

    if (typeKind == velox::TypeKind::ARRAY) {
      // Array has a single child for elements
      if (stats.children.size() >= 1) {
        auto* childValues = make<ChildValues>();
        childValues->values.reserve(1);

        auto& arrayType = value.type->as<velox::TypeKind::ARRAY>();
        Value elementValue(arrayType.elementType().get(), 1.0f);
        childValues->values.push_back(toValue(stats.children[0], elementValue));

        result.children = childValues;
      }
    } else if (typeKind == velox::TypeKind::MAP) {
      // Map has two children: keys and values
      if (stats.children.size() >= 2) {
        auto* childValues = make<ChildValues>();
        childValues->values.reserve(2);

        auto& mapType = value.type->as<velox::TypeKind::MAP>();
        Value keyValue(mapType.keyType().get(), 1.0f);
        Value valueValue(mapType.valueType().get(), 1.0f);

        childValues->values.push_back(toValue(stats.children[0], keyValue));
        childValues->values.push_back(toValue(stats.children[1], valueValue));

        result.children = childValues;
      }
    } else if (typeKind == velox::TypeKind::ROW) {
      // Struct has named children
      auto* childValues = make<ChildValues>();
      auto& rowType = value.type->as<velox::TypeKind::ROW>();

      childValues->names.reserve(stats.children.size());
      childValues->values.reserve(stats.children.size());

      for (size_t i = 0; i < stats.children.size(); ++i) {
        const auto& childStats = stats.children[i];

        // Use the name from the child statistics
        childValues->names.push_back(toName(childStats.name));

        // Find the corresponding child type from the row type
        const velox::Type* childType = nullptr;
        if (i < rowType.size()) {
          childType = rowType.childAt(i).get();
        } else {
          // Fallback: try to find by name
          for (size_t j = 0; j < rowType.size(); ++j) {
            if (rowType.nameOf(j) == childStats.name) {
              childType = rowType.childAt(j).get();
              break;
            }
          }
        }

        if (childType) {
          Value childValue(childType, 1.0f);
          childValues->values.push_back(toValue(childStats, childValue));
        } else {
          // If we can't find the type, create a placeholder Value
          // This shouldn't normally happen but provides a fallback
          Value childValue(velox::UNKNOWN().get(), 1.0f);
          childValues->values.push_back(childValue);
        }
      }

      result.children = childValues;
    }
  }

  return result;
}

// Overload of toValue that constructs a Value with only the subfields contained
// in the paths of the path set. Follows the paths down the ColumnStatistics and
// makes intermediate and leaf values. When we're at the end of a path from the
// path set and the ColumnStatistics has more structure, include that using
// toValue() without the path set.
// The level parameter indicates the current depth in path navigation (0 = top
// level).
Value toValue(
    const connector::ColumnStatistics& stats,
    const Value& value,
    const PathSet& paths,
    size_t level = 0) {
  Value result = value;

  // Set top-level statistics from stats
  if (stats.numDistinct.has_value()) {
    const_cast<float&>(result.cardinality) =
        static_cast<float>(stats.numDistinct.value());
  }

  const_cast<const velox::Variant*&>(result.min) =
      registerOptionalVariantWithType(stats.min, value.type->kind());
  const_cast<const velox::Variant*&>(result.max) =
      registerOptionalVariantWithType(stats.max, value.type->kind());

  const_cast<float&>(result.nullFraction) = stats.nullPct / 100.0f;

  if (stats.avgLength.has_value()) {
    const_cast<float&>(result.size) =
        static_cast<float>(stats.avgLength.value());
  }

  // Handle complex types with children based on paths
  if (!stats.children.empty()) {
    auto typeKind = value.type->kind();

    if (typeKind == velox::TypeKind::ARRAY) {
      // For arrays, check if we have paths that go deeper
      bool hasChildPaths = false;
      paths.forEachPath([&](PathCP path) {
        if (path->steps().size() > level &&
            path->steps()[level].kind == StepKind::kSubscript) {
          hasChildPaths = true;
        }
      });

      if (hasChildPaths && stats.children.size() >= 1) {
        auto* childValues = make<ChildValues>();
        childValues->values.reserve(1);

        auto& arrayType = value.type->as<velox::TypeKind::ARRAY>();
        Value elementValue(arrayType.elementType().get(), 1.0f);

        // Check if any path continues beyond this level
        bool pathsContinue = false;
        paths.forEachPath([&](PathCP path) {
          const auto& steps = path->steps();
          if (steps.size() > level &&
              steps[level].kind == StepKind::kSubscript &&
              steps.size() > level + 1) {
            pathsContinue = true;
          }
        });

        if (pathsContinue) {
          childValues->values.push_back(
              toValue(stats.children[0], elementValue, paths, level + 1));
        } else {
          // Reached the end of a path, include full structure below
          childValues->values.push_back(
              toValue(stats.children[0], elementValue));
        }

        result.children = childValues;
      }
    } else if (typeKind == velox::TypeKind::MAP) {
      // For maps, check if we have paths that go deeper
      bool hasChildPaths = false;
      paths.forEachPath([&](PathCP path) {
        if (path->steps().size() > level &&
            path->steps()[level].kind == StepKind::kSubscript) {
          hasChildPaths = true;
        }
      });

      if (hasChildPaths && stats.children.size() >= 2) {
        auto* childValues = make<ChildValues>();
        childValues->values.reserve(2);

        auto& mapType = value.type->as<velox::TypeKind::MAP>();
        Value keyValue(mapType.keyType().get(), 1.0f);
        Value valueValue(mapType.valueType().get(), 1.0f);

        // Keys are always included as-is
        childValues->values.push_back(toValue(stats.children[0], keyValue));

        // Check if any path continues beyond this level for values
        bool pathsContinue = false;
        paths.forEachPath([&](PathCP path) {
          const auto& steps = path->steps();
          if (steps.size() > level &&
              steps[level].kind == StepKind::kSubscript &&
              steps.size() > level + 1) {
            pathsContinue = true;
          }
        });

        // Values follow the paths
        if (pathsContinue) {
          childValues->values.push_back(
              toValue(stats.children[1], valueValue, paths, level + 1));
        } else {
          // Reached the end of a path, include full structure below
          childValues->values.push_back(toValue(stats.children[1], valueValue));
        }

        result.children = childValues;
      }
    } else if (typeKind == velox::TypeKind::ROW) {
      // For structs, collect paths for each field
      auto* childValues = make<ChildValues>();
      auto& rowType = value.type->as<velox::TypeKind::ROW>();

      childValues->names.reserve(stats.children.size());
      childValues->values.reserve(stats.children.size());

      for (size_t i = 0; i < stats.children.size(); ++i) {
        const auto& childStats = stats.children[i];
        Name fieldName = toName(childStats.name);

        // Check if this field is accessed in any path at this level
        bool fieldAccessed = false;
        bool pathsContinue = false;

        paths.forEachPath([&](PathCP path) {
          const auto& steps = path->steps();
          if (steps.size() > level && steps[level].kind == StepKind::kField &&
              steps[level].field == fieldName) {
            fieldAccessed = true;
            if (steps.size() > level + 1) {
              pathsContinue = true;
            }
          }
        });

        // Only include fields that are accessed
        if (fieldAccessed) {
          childValues->names.push_back(fieldName);

          // Find the corresponding child type from the row type
          const velox::Type* childType = nullptr;
          if (i < rowType.size()) {
            childType = rowType.childAt(i).get();
          } else {
            // Fallback: try to find by name
            for (size_t j = 0; j < rowType.size(); ++j) {
              if (rowType.nameOf(j) == childStats.name) {
                childType = rowType.childAt(j).get();
                break;
              }
            }
          }

          if (childType) {
            Value childValue(childType, 1.0f);
            if (pathsContinue) {
              childValues->values.push_back(
                  toValue(childStats, childValue, paths, level + 1));
            } else {
              // Reached the end of a path, include full structure below
              childValues->values.push_back(toValue(childStats, childValue));
            }
          } else {
            // Fallback for unknown type
            Value childValue(velox::UNKNOWN().get(), 1.0f);
            childValues->values.push_back(childValue);
          }
        }
      }

      if (!childValues->names.empty()) {
        result.children = childValues;
      }
    }
  }

  return result;
}

// Overload of toValue for map-as-struct columns. Takes a RowType representing
// the skyline struct and a PathSet containing the paths for this column.
// Loops over paths to get distinct first steps at the current level, finds
// child ColumnStatistics by matching step names (comparing field name string
// for named steps, or converting integer key to string for numeric steps), and
// recursively calls toValue with incremented level. The level parameter
// indicates the current depth in path navigation (0 = top level).
Value toValueMapAsStruct(
    const connector::ColumnStatistics& stats,
    const velox::RowTypePtr& type,
    const PathSet& paths,
    size_t level = 0) {
  Value result(type.get(), 1.0f);

  // Set top-level statistics from stats
  if (stats.numDistinct.has_value()) {
    const_cast<float&>(result.cardinality) =
        static_cast<float>(stats.numDistinct.value());
  }

  const_cast<const velox::Variant*&>(result.min) =
      registerOptionalVariantWithType(stats.min, type->kind());
  const_cast<const velox::Variant*&>(result.max) =
      registerOptionalVariantWithType(stats.max, type->kind());

  const_cast<float&>(result.nullFraction) = stats.nullPct / 100.0f;

  if (stats.avgLength.has_value()) {
    const_cast<float&>(result.size) =
        static_cast<float>(stats.avgLength.value());
  }

  // Handle children based on paths
  if (!stats.children.empty() && !paths.empty()) {
    // Collect distinct first steps at the current level from all paths
    folly::F14FastSet<std::string> distinctFirstSteps;
    paths.forEachPath([&](PathCP path) {
      if (path->steps().size() > level) {
        const auto& step = path->steps()[level];
        std::string stepName;
        if (step.field != nullptr) {
          // Field name step
          stepName = std::string(step.field);
        } else {
          // Integer key step - convert to string
          stepName = fmt::format("{}", step.id);
        }
        distinctFirstSteps.insert(stepName);
      }
    });

    // For each distinct step at this level, find corresponding child statistics
    auto* childValues = make<ChildValues>();
    childValues->names.reserve(distinctFirstSteps.size());
    childValues->values.reserve(distinctFirstSteps.size());

    for (const auto& stepName : distinctFirstSteps) {
      // Find child statistics by comparing name
      const connector::ColumnStatistics* childStats = nullptr;
      for (const auto& child : stats.children) {
        if (child.name == stepName) {
          childStats = &child;
          break;
        }
      }

      if (!childStats) {
        continue; // Skip if not found
      }

      // Check if any path continues beyond this level for this step
      bool pathsContinue = false;
      paths.forEachPath([&](PathCP path) {
        const auto& steps = path->steps();
        if (steps.size() > level) {
          const auto& step = steps[level];
          std::string currentStepName;
          if (step.field != nullptr) {
            currentStepName = std::string(step.field);
          } else {
            currentStepName = fmt::format("{}", step.id);
          }

          if (currentStepName == stepName && steps.size() > level + 1) {
            pathsContinue = true;
          }
        }
      });

      // Add name using toName from the child statistics
      childValues->names.push_back(toName(childStats->name));

      // Find the corresponding child type from the row type by name
      const velox::Type* childType = nullptr;
      for (size_t j = 0; j < type->size(); ++j) {
        if (type->nameOf(j) == childStats->name) {
          childType = type->childAt(j).get();
          break;
        }
      }

      if (childType) {
        Value childValue(childType, 1.0f);
        if (pathsContinue) {
          // Recursively call toValue with incremented level
          childValues->values.push_back(
              toValue(*childStats, childValue, paths, level + 1));
        } else {
          // No more paths, use toValue without PathSet
          childValues->values.push_back(toValue(*childStats, childValue));
        }
      } else {
        // Fallback for unknown type
        Value childValue(velox::UNKNOWN().get(), 1.0f);
        childValues->values.push_back(childValue);
      }
    }

    if (!childValues->names.empty()) {
      result.children = childValues;
    }
  }

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
        // Update value fields directly (most are mutable)
        value.cardinality = constrainedValue.cardinality;
        value.min = constrainedValue.min;
        value.max = constrainedValue.max;
        value.trueFraction = constrainedValue.trueFraction;
        value.nullFraction = constrainedValue.nullFraction;
        value.nullable = constrainedValue.nullable;
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
      // Skip complex type columns if complex type sampling is enabled
      if (options.sampleComplexTypes) {
        auto typeKind = column->value().type->kind();
        bool isComplexType =
            (typeKind == velox::TypeKind::ARRAY ||
             typeKind == velox::TypeKind::MAP ||
             typeKind == velox::TypeKind::ROW);
        if (isComplexType) {
          continue;
        }
      }
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
  // Handle complex type statistics if enabled
  auto options = queryCtx()->optimization()->options();
  if (options.sampleComplexTypes) {
    handleComplexTypeStats(table, scanType);
  }

  // Try to handle discrete predicate columns first
  if (setLeafSelectivityDiscrete(table, scanType)) {
    return;
  }

  // Handle non-discrete case
  setLeafSelectivityNoDiscrete(table, scanType);
}

namespace {
// Helper function to compute a commutative hash from a sorted set of field
// names Used as cache key for map-as-struct columns where the skyline struct
// field order doesn't matter
size_t makeMapAsStructCacheKey(
    const std::string& tableName,
    const std::string& columnName,
    const std::vector<std::string>& fieldNames) {
  // Create a sorted copy of field names for commutative hashing
  std::vector<std::string> sortedNames = fieldNames;
  std::sort(sortedNames.begin(), sortedNames.end());

  // Combine table name, column name, and sorted field names into a hash
  size_t hash = std::hash<std::string>{}(tableName);
  hash ^= std::hash<std::string>{}(columnName) + 0x9e3779b9 + (hash << 6) +
      (hash >> 2);

  for (const auto& name : sortedNames) {
    hash ^=
        std::hash<std::string>{}(name) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
  }

  return hash;
}

// Adapts a Subfield for map-as-struct columns by converting the path element
// at index 1 (if it's a subscript) to a nested field.
// For map-as-struct columns, the map is represented as a struct, so subscripts
// need to be converted to struct field accesses.
velox::common::Subfield adaptSubfieldForMapAsStruct(
    velox::common::Subfield subfield) {
  auto& path = const_cast<
      std::vector<std::unique_ptr<velox::common::Subfield::PathElement>>&>(
      subfield.path());

  // Need at least 2 elements: [0] = top-level column name, [1] = map key
  if (path.size() < 2) {
    return subfield;
  }

  auto* pathElement = path[1].get();

  // Check if element at index 1 is a subscript (not already a nested field)
  if (auto* stringSubscript =
          dynamic_cast<velox::common::Subfield::StringSubscript*>(
              pathElement)) {
    // Convert StringSubscript to NestedField - replace in place
    path[1] = std::make_unique<velox::common::Subfield::NestedField>(
        stringSubscript->index());
  } else if (
      auto* longSubscript =
          dynamic_cast<velox::common::Subfield::LongSubscript*>(pathElement)) {
    // Convert LongSubscript to NestedField (cast number to string)
    path[1] = std::make_unique<velox::common::Subfield::NestedField>(
        fmt::format("{}", longSubscript->index()));
  }

  // Already a NestedField or other type - no change needed
  return subfield;
}

/// Normalizes a PathSet by checking for "all fields" subfields and removing
/// all subfields that have the same prefix as the one with all fields.
/// The all fields subfield is also removed.
/// If a column has a subfield with "all fields", the column is treated as if
/// it had no subfields.
PathSet normalizedSubfields(const PathSet& paths) {
  PathSet result;
  PathSet allFieldsPrefixes;

  // First pass: identify all paths that contain an "all fields" step
  paths.forEachPath([&](PathCP path) {
    const auto& steps = path->steps();
    for (size_t i = 0; i < steps.size(); ++i) {
      if (steps[i].allFields) {
        // Create a prefix path up to (but not including) the all fields step
        std::vector<Step> prefixSteps(steps.begin(), steps.begin() + i);
        auto* prefixPath = make<Path>(prefixSteps, std::false_type{});
        auto* internedPrefix = queryCtx()->toPath(prefixPath);
        allFieldsPrefixes.add(internedPrefix->id());
        break;
      }
    }
  });

  // If there are no "all fields" prefixes, return the original paths
  if (allFieldsPrefixes.empty()) {
    return paths;
  }

  // Second pass: filter out paths that start with an "all fields" prefix
  // or are themselves "all fields" paths
  paths.forEachPath([&](PathCP path) {
    const auto& steps = path->steps();
    bool shouldInclude = true;

    // Check if this path contains an "all fields" step - exclude it
    for (const auto& step : steps) {
      if (step.allFields) {
        shouldInclude = false;
        break;
      }
    }

    if (!shouldInclude) {
      return;
    }

    // Check if this path is a prefix or extension of any "all fields" prefix
    allFieldsPrefixes.forEachPath([&](PathCP prefixPath) {
      const auto& prefixSteps = prefixPath->steps();

      // Check if this path starts with the prefix
      if (steps.size() >= prefixSteps.size()) {
        bool isMatch = true;
        for (size_t i = 0; i < prefixSteps.size(); ++i) {
          if (steps[i].kind != prefixSteps[i].kind ||
              steps[i].field != prefixSteps[i].field ||
              steps[i].id != prefixSteps[i].id) {
            isMatch = false;
            break;
          }
        }
        if (isMatch) {
          shouldInclude = false;
        }
      }
    });

    if (shouldInclude) {
      result.add(path->id());
    }
  });

  return result;
}

// Helper function to build a skyline struct type from a map column and its
// accessed subfields Returns the struct type and the field names
std::pair<velox::RowTypePtr, std::vector<std::string>> buildSkylineStruct(
    BaseTable& table,
    ColumnCP column) {
  PathSet allFields = normalizedSubfields(table.columnSubfields(column->id()));

  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(allFields.size());
  types.reserve(allFields.size());

  auto valueType = column->value().type->childAt(1);
  allFields.forEachPath([&](PathCP path) {
    const auto& first = path->steps()[0];
    auto name =
        first.field ? std::string{first.field} : fmt::format("{}", first.id);
    names.push_back(name);
    types.push_back(valueType);
  });

  // Make a copy of names before moving them into ROW
  auto namesCopy = names;
  return {ROW(std::move(names), std::move(types)), std::move(namesCopy)};
}
} // namespace

void VeloxHistory::handleComplexTypeStats(
    BaseTable& table,
    const velox::RowTypePtr& scanType) {
  auto* runnerTable = table.schemaTable->connectorTable;
  if (!runnerTable) {
    return;
  }

  auto [tableHandle, filters] =
      queryCtx()->optimization()->leafHandle(table.id());

  // Create a new table handle without filters for complex type statistics
  // sampling. The leaf handle contains filters, but we need unfiltered
  // statistics for complex types.
  const auto& layouts = runnerTable->layouts();
  if (layouts.empty()) {
    return;
  }

  // Get connector session and create column handles for createTableHandle
  auto connector = layouts[0]->connector();
  auto connectorSession =
      queryCtx()->optimization()->session()->toConnectorSession(
          connector->connectorId());

  // Build a minimal set of column handles - just need any valid column to
  // create the handle. The actual columns for sampling will be specified in the
  // samplingScanType later.
  std::vector<velox::connector::ColumnHandlePtr> columns;
  if (!table.columns.empty()) {
    // Use the first column as a placeholder
    auto* firstCol = table.columns[0];
    std::vector<velox::common::Subfield> emptySubfields;
    columns.push_back(
        layouts[0]->createColumnHandle(
            connectorSession,
            std::string(firstCol->name()),
            std::move(emptySubfields)));
  }

  // Create table handle with empty filters
  std::vector<velox::core::TypedExprPtr> emptyFilterConjuncts;
  std::vector<velox::core::TypedExprPtr> rejectedFilters;
  auto unfilteredHandle = layouts[0]->createTableHandle(
      connectorSession,
      columns,
      *queryCtx()->optimization()->evaluator(),
      std::move(emptyFilterConjuncts),
      rejectedFilters);

  const auto& tableName = table.schemaTable->name();

  // Extract distinct top-level complex type columns
  // Columns with topColumn() set represent subfields, so we skip them and only
  // process their top-level parent columns
  folly::F14FastSet<ColumnCP> topLevelComplexColumns;
  for (auto* column : table.columns) {
    // If this column has a topColumn set, it's a subfield - use the top column
    // instead
    auto* topCol = column->topColumn() ? column->topColumn() : column;

    const auto* valueType = topCol->value().type;
    if (!valueType) {
      continue;
    }

    // Check if this is a complex type (array, map, or struct)
    auto typeKind = valueType->kind();
    if (typeKind == velox::TypeKind::ARRAY ||
        typeKind == velox::TypeKind::MAP || typeKind == velox::TypeKind::ROW) {
      topLevelComplexColumns.insert(topCol);
    }
  }

  if (topLevelComplexColumns.empty()) {
    return;
  }

  // Check cache and set values for cached statistics
  std::vector<std::string> missingColumns;
  std::vector<velox::common::Subfield> missingSubfields;
  std::vector<velox::RowTypePtr> missingScanTypes;
  folly::F14FastMap<ColumnCP, PathSet> topColumnPaths;
  folly::F14FastMap<
      ColumnCP,
      std::variant<std::pair<std::string, std::string>, size_t>>
      columnCacheKeys;
  folly::F14FastMap<ColumnCP, bool> isMapAsStructColumn;

  {
    std::lock_guard<std::recursive_mutex> l(mutex_);

    // For each top-level column, get all accessed subfields
    for (auto* topColumn : topLevelComplexColumns) {
      auto columnName = std::string(topColumn->name());

      // Get the PathSet for this column's subfields
      PathSet paths =
          normalizedSubfields(table.columnSubfields(topColumn->id()));
      topColumnPaths[topColumn] = paths;

      // Check if this is a map-as-struct column
      // Note: If the path set is empty, the column cannot be a map-as-struct
      bool isMapAsStructCol = !paths.empty() &&
          topColumn->value().type->kind() == velox::TypeKind::MAP &&
          queryCtx()->optimization()->options().isMapAsStruct(
              tableName, columnName);

      isMapAsStructColumn[topColumn] = isMapAsStructCol;

      std::variant<std::pair<std::string, std::string>, size_t> key;

      if (isMapAsStructCol) {
        // For map-as-struct, build skyline struct and use commutative hash as
        // key
        auto [skylineType, fieldNames] = buildSkylineStruct(table, topColumn);
        key = makeMapAsStructCacheKey(tableName, columnName, fieldNames);
      } else {
        // For regular complex columns, use {tableName, columnName} as key
        key = std::make_pair(tableName, columnName);
      }

      columnCacheKeys[topColumn] = key;

      auto it = complexTypeStats_.find(key);
      if (it != complexTypeStats_.end()) {
        // Stats found in cache - set the value using appropriate method
        auto& value = const_cast<Value&>(topColumn->value());
        if (isMapAsStructCol) {
          // Use setChildrenUnchecked for map-as-struct columns since the
          // stats type (MAP) differs from the Value type (ROW/skyline struct)
          auto [skylineType, fieldNames] = buildSkylineStruct(table, topColumn);
          Value tempValue = toValueMapAsStruct(*it->second, skylineType, paths);
          value.setChildrenUnchecked(tempValue);
        } else {
          // Use regular assignment for other complex types
          value = toValue(*it->second, value, paths);
        }
      } else {
        // Stats not in cache - add to missing list
        missingColumns.push_back(columnName);

        // Use columnSubfields to convert PathSet to Subfield list
        auto subfields = columnSubfields(&table, topColumn->id());
        for (auto& subfield : subfields) {
          // Adapt subfields for map-as-struct columns
          if (isMapAsStructCol) {
            missingSubfields.push_back(
                adaptSubfieldForMapAsStruct(std::move(subfield)));
          } else {
            missingSubfields.push_back(std::move(subfield));
          }
        }

        // For sampling, use the appropriate scan type
        if (isMapAsStructCol) {
          auto [skylineType, fieldNames] = buildSkylineStruct(table, topColumn);
          missingScanTypes.push_back(skylineType);
        } else {
          missingScanTypes.push_back(nullptr);
        }
      }
    }
  }

  // If all stats are cached, we're done with top columns
  // Now set values for subfield columns
  if (missingColumns.empty()) {
    setSubfieldColumnValues(table);
    return;
  }

  // Sample outside of the mutex for missing columns
  // Build scan type for sampling with skyline structs for map-as-struct columns
  std::vector<std::string> scanNames;
  std::vector<velox::TypePtr> scanTypes;

  for (size_t i = 0; i < missingColumns.size(); ++i) {
    scanNames.push_back(missingColumns[i]);
    if (missingScanTypes[i] != nullptr) {
      // Map-as-struct: use skyline struct type
      scanTypes.push_back(missingScanTypes[i]);
    } else {
      // Regular column: use original type from scanType
      // Find the column in scanType
      for (size_t j = 0; j < scanType->size(); ++j) {
        if (scanType->nameOf(j) == missingColumns[i]) {
          scanTypes.push_back(scanType->childAt(j));
          break;
        }
      }
    }
  }

  auto samplingScanType = ROW(std::move(scanNames), std::move(scanTypes));

  // Create a HashStringAllocator using the leaf memory pool from the expression
  // evaluator
  auto* pool = queryCtx()->optimization()->evaluator()->pool();
  velox::HashStringAllocator allocator(pool);

  std::vector<connector::ColumnStatistics> statistics;
  // Sample with no filters to get base statistics for complex type columns
  std::vector<velox::core::TypedExprPtr> emptyFilters;
  layouts[0]->sample(
      unfilteredHandle,
      1.0f, // 1% sampling
      emptyFilters,
      samplingScanType,
      missingSubfields,
      &allocator,
      &statistics);

  // Acquire mutex and update cache and values
  {
    std::lock_guard<std::recursive_mutex> l(mutex_);

    // Process statistics by top-level column name
    for (const auto& stat : statistics) {
      // Find the top-level column that matches this statistics name
      for (auto* topColumn : topLevelComplexColumns) {
        if (std::string(topColumn->name()) == stat.name) {
          auto keyIt = columnCacheKeys.find(topColumn);
          if (keyIt == columnCacheKeys.end()) {
            continue;
          }

          const auto& key = keyIt->second;

          // Check if stats are still missing (another thread may have added
          // them)
          if (complexTypeStats_.find(key) == complexTypeStats_.end()) {
            // Create a shared_ptr for the statistics before adding to cache
            auto statsPtr =
                std::make_shared<const connector::ColumnStatistics>(stat);

            // Add to cache
            complexTypeStats_[key] = statsPtr;
          }

          // Set the value using appropriate method from the cache
          auto it = complexTypeStats_.find(key);
          if (it != complexTypeStats_.end()) {
            auto& value = const_cast<Value&>(topColumn->value());
            auto pathsIt = topColumnPaths.find(topColumn);
            auto isMapAsStructIt = isMapAsStructColumn.find(topColumn);

            if (isMapAsStructIt != isMapAsStructColumn.end() &&
                isMapAsStructIt->second) {
              // Map-as-struct column: use setChildrenUnchecked since types
              // differ
              auto [skylineType, fieldNames] =
                  buildSkylineStruct(table, topColumn);
              if (pathsIt != topColumnPaths.end()) {
                Value tempValue = toValueMapAsStruct(
                    *it->second, skylineType, pathsIt->second);
                value.setChildrenUnchecked(tempValue);
              } else {
                PathSet emptyPaths;
                Value tempValue =
                    toValueMapAsStruct(*it->second, skylineType, emptyPaths);
                value.setChildrenUnchecked(tempValue);
              }
            } else {
              // Regular complex type: use assignment operator
              if (pathsIt != topColumnPaths.end()) {
                value = toValue(*it->second, value, pathsIt->second);
              } else {
                value = toValue(*it->second, value);
              }
            }
          }
          break;
        }
      }
    }
  }

  // Set values for subfield columns by navigating Value children
  setSubfieldColumnValues(table);
}

// Helper function to set values for subfield columns by navigating Value
// children
void VeloxHistory::setSubfieldColumnValues(BaseTable& table) {
  // Process all columns that are subfields (have a topColumn set)
  for (auto* column : table.columns) {
    if (!column->topColumn()) {
      continue; // Skip top-level columns
    }

    auto* topColumn = column->topColumn();
    auto* path = column->path();

    if (!path || path->empty()) {
      continue;
    }

    // Navigate the Value children of the top column using the path
    const Value* currentValue = &topColumn->value();

    for (const auto& step : path->steps()) {
      if (!currentValue->children) {
        // No children to navigate
        currentValue = nullptr;
        break;
      }

      if (step.kind == StepKind::kField) {
        // For struct fields, find child by name
        bool found = false;
        for (size_t i = 0; i < currentValue->children->names.size(); ++i) {
          if (currentValue->children->names[i] == step.field) {
            if (i < currentValue->children->values.size()) {
              currentValue = &currentValue->children->values[i];
              found = true;
              break;
            }
          }
        }
        if (!found) {
          currentValue = nullptr;
          break;
        }
      } else if (step.kind == StepKind::kSubscript) {
        // For arrays and maps
        if (step.field != nullptr) {
          // Map subscript with key name - navigate to values child
          if (currentValue->children->values.size() >= 2) {
            currentValue = &currentValue->children->values[1];
          } else {
            currentValue = nullptr;
            break;
          }
        } else {
          // Array subscript or numeric map subscript
          if (!currentValue->children->values.empty()) {
            // For maps, values are at index 1; for arrays, elements are at
            // index 0
            size_t childIndex =
                currentValue->children->values.size() == 1 ? 0 : 1;
            if (childIndex < currentValue->children->values.size()) {
              currentValue = &currentValue->children->values[childIndex];
            } else {
              currentValue = nullptr;
              break;
            }
          } else {
            currentValue = nullptr;
            break;
          }
        }
      } else {
        // kCardinality or unknown step kind
        currentValue = nullptr;
        break;
      }
    }

    // Set the subfield column's value
    if (currentValue) {
      auto& subfieldValue = const_cast<Value&>(column->value());
      subfieldValue = *currentValue;
    }
  }
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

namespace {

// Helper function to convert a Variant to folly::dynamic
folly::dynamic variantToJson(const velox::Variant& variant) {
  folly::dynamic obj = folly::dynamic::object();
  obj["kind"] = static_cast<int>(variant.kind());

  switch (variant.kind()) {
    case velox::TypeKind::BOOLEAN:
      obj["value"] = variant.value<bool>();
      break;
    case velox::TypeKind::TINYINT:
      obj["value"] = variant.value<int8_t>();
      break;
    case velox::TypeKind::SMALLINT:
      obj["value"] = variant.value<int16_t>();
      break;
    case velox::TypeKind::INTEGER:
      obj["value"] = variant.value<int32_t>();
      break;
    case velox::TypeKind::BIGINT:
      obj["value"] = variant.value<int64_t>();
      break;
    case velox::TypeKind::REAL:
      obj["value"] = variant.value<float>();
      break;
    case velox::TypeKind::DOUBLE:
      obj["value"] = variant.value<double>();
      break;
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY: {
      auto stringView = variant.value<velox::StringView>();
      obj["value"] = std::string(stringView.data(), stringView.size());
      break;
    }
    case velox::TypeKind::TIMESTAMP:
      obj["value"] = variant.value<velox::Timestamp>().toMillis();
      break;
    default:
      // For unsupported types, skip serialization
      break;
  }
  return obj;
}

// Helper function to convert folly::dynamic to Variant
std::optional<velox::Variant> jsonToVariant(const folly::dynamic& json) {
  if (!json.isObject() || !json.count("kind")) {
    return std::nullopt;
  }

  auto kind = static_cast<velox::TypeKind>(json["kind"].asInt());

  switch (kind) {
    case velox::TypeKind::BOOLEAN:
      return velox::Variant(json["value"].asBool());
    case velox::TypeKind::TINYINT:
      return velox::Variant(static_cast<int8_t>(json["value"].asInt()));
    case velox::TypeKind::SMALLINT:
      return velox::Variant(static_cast<int16_t>(json["value"].asInt()));
    case velox::TypeKind::INTEGER:
      return velox::Variant(static_cast<int32_t>(json["value"].asInt()));
    case velox::TypeKind::BIGINT:
      return velox::Variant(json["value"].asInt());
    case velox::TypeKind::REAL:
      return velox::Variant(static_cast<float>(json["value"].asDouble()));
    case velox::TypeKind::DOUBLE:
      return velox::Variant(json["value"].asDouble());
    case velox::TypeKind::VARCHAR:
      return velox::Variant::create<velox::TypeKind::VARCHAR>(
          json["value"].asString());
    case velox::TypeKind::VARBINARY:
      return velox::Variant::create<velox::TypeKind::VARBINARY>(
          json["value"].asString());
    case velox::TypeKind::TIMESTAMP:
      return velox::Variant(
          velox::Timestamp::fromMillis(json["value"].asInt()));
    default:
      return std::nullopt;
  }
}

// Helper function to convert ColumnStatistics to folly::dynamic (recursive)
folly::dynamic columnStatsToJson(const connector::ColumnStatistics& stats) {
  folly::dynamic obj = folly::dynamic::object();

  obj["name"] = stats.name;
  obj["nonNull"] = stats.nonNull;
  obj["nullPct"] = stats.nullPct;

  if (stats.min.has_value()) {
    obj["min"] = variantToJson(stats.min.value());
  }

  if (stats.max.has_value()) {
    obj["max"] = variantToJson(stats.max.value());
  }

  if (stats.maxLength.has_value()) {
    obj["maxLength"] = stats.maxLength.value();
  }

  if (stats.ascendingPct.has_value()) {
    obj["ascendingPct"] = stats.ascendingPct.value();
  }

  if (stats.descendingPct.has_value()) {
    obj["descendingPct"] = stats.descendingPct.value();
  }

  if (stats.avgLength.has_value()) {
    obj["avgLength"] = stats.avgLength.value();
  }

  if (stats.numDistinct.has_value()) {
    obj["numDistinct"] = stats.numDistinct.value();
  }

  obj["numValues"] = stats.numValues;

  // Recursively serialize children
  if (!stats.children.empty()) {
    auto childrenArray = folly::dynamic::array();
    for (const auto& child : stats.children) {
      childrenArray.push_back(columnStatsToJson(child));
    }
    obj["children"] = childrenArray;
  }

  return obj;
}

// Helper function to convert folly::dynamic to ColumnStatistics (recursive)
connector::ColumnStatistics jsonToColumnStats(const folly::dynamic& json) {
  connector::ColumnStatistics stats;

  if (json.count("name")) {
    stats.name = json["name"].asString();
  }

  if (json.count("nonNull")) {
    stats.nonNull = json["nonNull"].asBool();
  }

  if (json.count("nullPct")) {
    stats.nullPct = static_cast<float>(json["nullPct"].asDouble());
  }

  if (json.count("min")) {
    stats.min = jsonToVariant(json["min"]);
  }

  if (json.count("max")) {
    stats.max = jsonToVariant(json["max"]);
  }

  if (json.count("maxLength")) {
    stats.maxLength = static_cast<int32_t>(json["maxLength"].asInt());
  }

  if (json.count("ascendingPct")) {
    stats.ascendingPct = static_cast<float>(json["ascendingPct"].asDouble());
  }

  if (json.count("descendingPct")) {
    stats.descendingPct = static_cast<float>(json["descendingPct"].asDouble());
  }

  if (json.count("avgLength")) {
    stats.avgLength = static_cast<int32_t>(json["avgLength"].asInt());
  }

  if (json.count("numDistinct")) {
    stats.numDistinct = json["numDistinct"].asInt();
  }

  if (json.count("numValues")) {
    stats.numValues = json["numValues"].asInt();
  }

  // Recursively deserialize children
  if (json.count("children")) {
    for (const auto& childJson : json["children"]) {
      stats.children.push_back(jsonToColumnStats(childJson));
    }
  }

  return stats;
}

} // namespace

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

  // Serialize complex type column statistics cache
  auto complexStatsArray = folly::dynamic::array();
  {
    std::lock_guard<std::recursive_mutex> l(mutex_);
    for (auto& pair : complexTypeStats_) {
      folly::dynamic statsEntry = folly::dynamic::object();

      // Handle both key types in the variant
      if (std::holds_alternative<std::pair<std::string, std::string>>(
              pair.first)) {
        const auto& stringKey =
            std::get<std::pair<std::string, std::string>>(pair.first);
        statsEntry["table"] = stringKey.first;
        statsEntry["column"] = stringKey.second;
        statsEntry["keyType"] = "column";
      } else {
        const auto& hashKey = std::get<size_t>(pair.first);
        statsEntry["hash"] = static_cast<uint64_t>(hashKey);
        statsEntry["keyType"] = "mapAsStruct";
      }

      statsEntry["stats"] = columnStatsToJson(*pair.second);
      complexStatsArray.push_back(statsEntry);
    }
  }
  obj["complexStats"] = complexStatsArray;

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

  // Deserialize complex type column statistics cache
  if (serialized.count("complexStats")) {
    std::lock_guard<std::recursive_mutex> l(mutex_);
    for (auto& entry : serialized["complexStats"]) {
      auto stats = std::make_shared<connector::ColumnStatistics>(
          jsonToColumnStats(entry["stats"]));

      // Handle both key types
      if (entry.count("keyType") &&
          entry["keyType"].asString() == "mapAsStruct") {
        // Map-as-struct key type: use hash
        auto hash = static_cast<size_t>(entry["hash"].asInt());
        complexTypeStats_[hash] = stats;
      } else {
        // Regular column key type: use {table, column} pair
        auto tableName = entry["table"].asString();
        auto columnName = entry["column"].asString();
        complexTypeStats_[std::make_pair(tableName, columnName)] = stats;
      }
    }
  }
}

} // namespace facebook::axiom::optimizer
