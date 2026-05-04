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

#include "axiom/optimizer/MultiFragmentPlan.h"
#include "velox/core/TableWriteTraits.h"

namespace facebook::axiom::optimizer {

namespace {

// Returns elements [start, end) of a container as a std::vector.
template <typename Container>
auto sliceToVector(const Container& container, size_t start, size_t end) {
  return std::vector<typename std::decay_t<decltype(*container.begin())>>(
      container.begin() + start, container.begin() + end);
}

velox::vector_size_t countRows(
    const std::vector<velox::RowVectorPtr>& vectors) {
  velox::vector_size_t total{0};
  for (const auto& vector : vectors) {
    total += vector->size();
  }
  return total;
}

// Splits write results into stats and data rows. Stats rows are identified
// via TableWriteTraits::isStatisticsRow. Data rows are projected to only
// include the first 'numDataColumns' columns (stripping appended stats
// channels). Uses dictionary wrapping to avoid copying when the batch
// contains a mix of row types.
struct SplitWriteResults {
  std::vector<velox::RowVectorPtr> statsRows;
  std::vector<velox::RowVectorPtr> dataRows;
};

SplitWriteResults splitWriteResults(
    const std::vector<velox::RowVectorPtr>& writeResults,
    size_t numDataColumns,
    const velox::RowTypePtr& dataType) {
  SplitWriteResults split;
  for (const auto& result : writeResults) {
    auto statsIndices = velox::allocateIndices(result->size(), result->pool());
    auto dataIndices = velox::allocateIndices(result->size(), result->pool());
    auto* rawStatsIndices = statsIndices->asMutable<velox::vector_size_t>();
    auto* rawDataIndices = dataIndices->asMutable<velox::vector_size_t>();
    velox::vector_size_t numStats{0};
    velox::vector_size_t numData{0};

    for (velox::vector_size_t i = 0; i < result->size(); ++i) {
      if (velox::core::TableWriteTraits::isStatisticsRow(result, i)) {
        rawStatsIndices[numStats++] = i;
      } else {
        rawDataIndices[numData++] = i;
      }
    }

    if (numStats > 0) {
      if (numData == 0) {
        split.statsRows.push_back(result);
      } else {
        std::vector<velox::VectorPtr> children(result->childrenSize());
        for (size_t col = 0; col < result->childrenSize(); ++col) {
          children[col] = velox::BaseVector::wrapInDictionary(
              nullptr, statsIndices, numStats, result->childAt(col));
        }
        split.statsRows.push_back(
            std::make_shared<velox::RowVector>(
                result->pool(),
                result->type(),
                nullptr,
                numStats,
                std::move(children)));
      }
    }

    if (numData > 0) {
      if (numStats == 0 && result->childrenSize() == numDataColumns) {
        split.dataRows.push_back(result);
      } else {
        std::vector<velox::VectorPtr> children(numDataColumns);
        for (size_t col = 0; col < numDataColumns; ++col) {
          children[col] = velox::BaseVector::wrapInDictionary(
              nullptr, dataIndices, numData, result->childAt(col));
        }
        split.dataRows.push_back(
            std::make_shared<velox::RowVector>(
                result->pool(),
                dataType,
                nullptr,
                numData,
                std::move(children)));
      }
    }
  }
  return split;
}

// Extracts stat field values from 'statsVector' for all rows into
// 'stats[startRow + i][column]'. Casts the vector once and iterates all rows.
// Null values are skipped — they occur for min/max/approx_distinct when all
// input values are null.
void extractStatField(
    ColumnStatField field,
    const velox::VectorPtr& statsVector,
    std::vector<std::vector<connector::ColumnStatistics>>& stats,
    velox::vector_size_t startRow,
    size_t column) {
  const auto numRows = statsVector->size();
  switch (field) {
    case ColumnStatField::kCount: {
      auto* values = statsVector->asUnchecked<velox::SimpleVector<int64_t>>();
      for (auto i = 0; i < numRows; ++i) {
        if (!values->isNullAt(i)) {
          stats[startRow + i][column].numValues = values->valueAt(i);
        }
      }
      break;
    }
    case ColumnStatField::kMin:
      for (auto i = 0; i < numRows; ++i) {
        if (!statsVector->isNullAt(i)) {
          stats[startRow + i][column].min = statsVector->variantAt(i);
        }
      }
      break;
    case ColumnStatField::kMax:
      for (auto i = 0; i < numRows; ++i) {
        if (!statsVector->isNullAt(i)) {
          stats[startRow + i][column].max = statsVector->variantAt(i);
        }
      }
      break;
    case ColumnStatField::kApproxDistinct: {
      auto* values = statsVector->asUnchecked<velox::SimpleVector<int64_t>>();
      for (auto i = 0; i < numRows; ++i) {
        if (!values->isNullAt(i)) {
          stats[startRow + i][column].numDistinct = values->valueAt(i);
        }
      }
      break;
    }
    case ColumnStatField::kCountIf: {
      auto* values = statsVector->asUnchecked<velox::SimpleVector<int64_t>>();
      for (auto i = 0; i < numRows; ++i) {
        if (!values->isNullAt(i)) {
          auto countTrue = values->valueAt(i);
          auto& entry = stats[startRow + i][column];
          auto countFalse = entry.numValues - countTrue;
          entry.numDistinct =
              (countTrue > 0 ? 1 : 0) + (countFalse > 0 ? 1 : 0);
        }
      }
      break;
    }
  }
}

// Extracts per-group column statistics from stats rows. Each row represents
// one group (e.g. one partition for partitioned tables, or one row for
// unpartitioned tables).
std::vector<std::vector<connector::ColumnStatistics>> extractPerGroupStats(
    const WriteStatsMapping& statsMapping,
    const std::vector<velox::RowVectorPtr>& statsRows,
    int32_t firstStatsChannel) {
  const auto totalRows = countRows(statsRows);

  std::vector<std::vector<connector::ColumnStatistics>> result(totalRows);
  for (auto& groupStats : result) {
    groupStats.resize(statsMapping.columns.size());
  }

  // Fill column names.
  for (size_t column = 0; column < statsMapping.columns.size(); ++column) {
    for (auto& groupStats : result) {
      groupStats[column].name = statsMapping.columns[column].columnName;
    }
  }

  // Extract stats column by column across all batches. Each stat field
  // corresponds to one channel in the stats output.
  auto channel = firstStatsChannel;
  for (size_t column = 0; column < statsMapping.columns.size(); ++column) {
    for (auto field : statsMapping.columns[column].fields) {
      velox::vector_size_t resultRow{0};
      for (const auto& statsRow : statsRows) {
        VELOX_CHECK_LT(channel, statsRow->childrenSize());
        extractStatField(
            field, statsRow->childAt(channel), result, resultRow, column);
        resultRow += statsRow->size();
      }
      ++channel;
    }
  }

  return result;
}

// Builds a grouping keys RowVector by projecting and concatenating grouping
// key columns from all stats rows. Uses 'tableColumnNames' for the output
// column names (the stats output may use write-input names like "expr_0",
// but finishWrite needs table column names like "pk").
velox::RowVectorPtr extractGroupingKeys(
    const std::vector<velox::RowVectorPtr>& statsRows,
    uint32_t numGroupingKeys,
    const std::vector<std::string>& tableColumnNames) {
  VELOX_CHECK(!statsRows.empty());

  constexpr auto kFirstChannel = velox::core::TableWriteTraits::kStatsChannel;
  const auto lastChannel = kFirstChannel + numGroupingKeys;

  auto resultType = velox::ROW(
      tableColumnNames,
      sliceToVector(
          statsRows[0]->rowType()->children(), kFirstChannel, lastChannel));

  // Fast path: single vector, just project columns without copying data.
  if (statsRows.size() == 1) {
    return std::make_shared<velox::RowVector>(
        statsRows[0]->pool(),
        resultType,
        /*nulls=*/nullptr,
        statsRows[0]->size(),
        sliceToVector(statsRows[0]->children(), kFirstChannel, lastChannel));
  }

  // Multiple vectors: allocate and copy.
  auto result = velox::BaseVector::create<velox::RowVector>(
      resultType, countRows(statsRows), statsRows[0]->pool());

  velox::vector_size_t offset = 0;
  for (const auto& statsRow : statsRows) {
    for (auto channel = kFirstChannel; channel < lastChannel; ++channel) {
      result->childAt(channel - kFirstChannel)
          ->copy(statsRow->childAt(channel).get(), offset, 0, statsRow->size());
    }
    offset += statsRow->size();
  }

  return result;
}

std::pair<
    velox::RowVectorPtr,
    std::vector<std::vector<connector::ColumnStatistics>>>
extractStats(
    const WriteStatsMapping& statsMapping,
    const std::vector<velox::RowVectorPtr>& statsRows) {
  VELOX_CHECK(!statsRows.empty());

  // The stats output layout is:
  //   [row_count, fragment, context, groupingKey0, ..., groupingKeyN,
  //    [$row_count,] stat0, stat1, ...]
  // When there are grouping keys, a count(*) aggregate occupies the first
  // stats channel (right after grouping keys) providing exact per-group
  // row counts. This channel is included in groupingKeys (not in
  // per-column stats).
  const auto firstGroupingKeyChannel =
      velox::core::TableWriteTraits::kStatsChannel;
  // Number of channels to include in groupingKeys: partition keys + row count.
  const auto numGroupingKeyChannels =
      statsMapping.numGroupingKeys + (statsMapping.numGroupingKeys > 0 ? 1 : 0);
  const auto firstStatsChannel =
      firstGroupingKeyChannel + static_cast<int32_t>(numGroupingKeyChannels);

  if (statsMapping.numGroupingKeys == 0) {
    return {
        nullptr,
        extractPerGroupStats(statsMapping, statsRows, firstStatsChannel)};
  }

  auto groupingKeyNames = statsMapping.groupingKeyNames;
  groupingKeyNames.push_back("$row_count");

  return {
      extractGroupingKeys(statsRows, numGroupingKeyChannels, groupingKeyNames),
      extractPerGroupStats(statsMapping, statsRows, firstStatsChannel)};
}

const auto& columnStatFieldNames() {
  static const folly::F14FastMap<ColumnStatField, std::string_view> kNames = {
      {ColumnStatField::kCount, "COUNT"},
      {ColumnStatField::kCountIf, "COUNT_IF"},
      {ColumnStatField::kMin, "MIN"},
      {ColumnStatField::kMax, "MAX"},
      {ColumnStatField::kApproxDistinct, "APPROX_DISTINCT"},
  };
  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(ColumnStatField, columnStatFieldNames);

namespace {

const auto& fragmentTypeNames() {
  static const folly::F14FastMap<FragmentType, std::string_view> kNames = {
      {FragmentType::kSource, "SOURCE"},
      {FragmentType::kFixed, "FIXED"},
      {FragmentType::kSingle, "SINGLE"},
      {FragmentType::kCoordinator, "COORDINATOR"},
  };
  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(FragmentType, fragmentTypeNames);

FinishWrite::FinishWrite(
    std::shared_ptr<connector::ConnectorMetadata> metadata,
    connector::ConnectorSessionPtr session,
    connector::ConnectorWriteHandlePtr handle,
    WriteStatsMapping statsMapping)
    : metadata_{std::move(metadata)},
      session_{std::move(session)},
      handle_{std::move(handle)},
      statsMapping_{std::move(statsMapping)} {
  VELOX_CHECK_NOT_NULL(metadata_);
  VELOX_CHECK_NOT_NULL(session_);
  VELOX_CHECK_NOT_NULL(handle_);
}

FinishWrite::~FinishWrite() {
  if (*this) {
    std::ignore = std::move(*this).abort();
  }
}

connector::RowsFuture FinishWrite::commit(
    const std::vector<velox::RowVectorPtr>& writeResults) && {
  VELOX_CHECK(*this);
  SCOPE_EXIT {
    *this = {};
  };

  if (statsMapping_.columns.empty()) {
    return metadata_->finishWrite(session_, handle_, writeResults, nullptr, {});
  }

  auto [statsRows, dataRows] = splitWriteResults(
      writeResults, handle_->resultType()->size(), handle_->resultType());

  if (statsRows.empty()) {
    return metadata_->finishWrite(session_, handle_, dataRows, nullptr, {});
  }

  auto [partitionKeys, partitionStats] = extractStats(statsMapping_, statsRows);
  return metadata_->finishWrite(
      session_,
      handle_,
      dataRows,
      std::move(partitionKeys),
      std::move(partitionStats));
}

velox::ContinueFuture FinishWrite::abort() && noexcept {
  VELOX_CHECK(*this);
  SCOPE_EXIT {
    *this = {};
  };
  return metadata_->abortWrite(session_, handle_);
}

std::string ExecutableFragment::toHeader(int32_t index) const {
  std::string bucketedSuffix;
  if (!splitToWorkerFns.empty()) {
    bucketedSuffix =
        fmt::format(" bucketed(scans={})", splitToWorkerFns.size());
  }
  return fmt::format(
      "Fragment {}: {} {}{}{}:",
      index,
      taskPrefix,
      FragmentTypeName::toName(type),
      width.has_value() ? fmt::format(" numWorkers={}", width.value()) : "",
      bucketedSuffix);
}

std::string MultiFragmentPlan::toString(
    bool detailed,
    const std::function<void(
        const velox::core::PlanNodeId& nodeId,
        std::string_view indentation,
        std::ostream& out)>& addContext) const {
  // Map task prefix to fragment index.
  folly::F14FastMap<std::string, int32_t> taskPrefixToIndex;
  for (auto i = 0; i < fragments_.size(); ++i) {
    taskPrefixToIndex[fragments_[i].taskPrefix] = i;
  }

  // Map plan node to indices of the input fragment.
  folly::F14FastMap<velox::core::PlanNodeId, std::vector<int32_t>>
      planNodeToIndices;
  for (const auto& fragment : fragments_) {
    for (const auto& input : fragment.inputStages) {
      planNodeToIndices[input.consumerNodeId].emplace_back(
          taskPrefixToIndex[input.producerTaskPrefix]);
    }
  }

  std::stringstream out;
  for (auto i = 0; i < fragments_.size(); ++i) {
    const auto& fragment = fragments_[i];
    out << fragment.toHeader(i) << std::endl;

    out << fragment.fragment.planNode->toString(
               detailed,
               true,
               [&](const velox::core::PlanNodeId& planNodeId,
                   std::string_view indentation,
                   std::ostream& stream) {
                 if (addContext != nullptr) {
                   addContext(planNodeId, indentation, stream);
                 }
                 auto it = planNodeToIndices.find(planNodeId);
                 if (it != planNodeToIndices.end()) {
                   if (it->second.size() == 1) {
                     stream << indentation << "Input Fragment "
                            << it->second.front() << std::endl;
                   } else {
                     stream << indentation << "Input Fragments "
                            << folly::join(", ", it->second) << std::endl;
                   }
                 }
               })
        << std::endl;
  }
  return out.str();
}

std::string MultiFragmentPlan::toSummaryString(
    velox::core::PlanSummaryOptions options) const {
  std::stringstream out;
  for (auto i = 0; i < fragments_.size(); ++i) {
    const auto& fragment = fragments_[i];
    out << fragment.toHeader(i) << std::endl;
    out << fragment.fragment.planNode->toSummaryString(options) << std::endl;
    if (!fragment.inputStages.empty()) {
      out << "Inputs: ";
      for (const auto& input : fragment.inputStages) {
        out << fmt::format(
            " {} <- {} ", input.consumerNodeId, input.producerTaskPrefix);
      }
      out << std::endl;
    }
  }
  return out.str();
}

} // namespace facebook::axiom::optimizer
