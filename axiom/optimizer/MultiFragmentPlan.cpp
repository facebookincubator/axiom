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
#include "velox/core/FixedPointPlanNodes.h"
#include "velox/core/PlanNode.h"
#include "velox/core/TableWriteTraits.h"

#include <folly/Conv.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <utility>

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
// A null value means all input values are null. 'rowCounts' holds the number
// of rows in each group.
void extractStatField(
    ColumnStatField field,
    const velox::VectorPtr& statsVector,
    const std::vector<int64_t>& rowCounts,
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
    case ColumnStatField::kVectorCount: {
      // A group whose indexes are all NULL holds no embeddings.
      auto* values = statsVector->asUnchecked<velox::SimpleVector<int64_t>>();
      for (auto i = 0; i < numRows; ++i) {
        stats[startRow + i][column].numEmbeddings =
            values->isNullAt(i) ? 0 : values->valueAt(i);
      }
      break;
    }
    case ColumnStatField::kSumDataSize: {
      auto* values = statsVector->asUnchecked<velox::SimpleVector<int64_t>>();
      for (auto i = 0; i < numRows; ++i) {
        const auto numGroupRows = rowCounts[startRow + i];
        if (numGroupRows == 0) {
          continue;
        }
        const int64_t totalSize = values->isNullAt(i) ? 0 : values->valueAt(i);
        stats[startRow + i][column].avgSizeInBytes = static_cast<int32_t>(
            std::llround(static_cast<double>(totalSize) / numGroupRows));
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
// unpartitioned tables). 'rowCounts' holds the number of rows in each group.
std::vector<std::vector<connector::ColumnStatistics>> extractPerGroupStats(
    const WriteStatsMapping& statsMapping,
    const std::vector<velox::RowVectorPtr>& statsRows,
    int32_t firstStatsChannel,
    const std::vector<int64_t>& rowCounts) {
  const auto totalRows = countRows(statsRows);
  VELOX_CHECK_EQ(rowCounts.size(), totalRows);

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
            field,
            statsRow->childAt(channel),
            rowCounts,
            result,
            resultRow,
            column);
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

// Returns the number of rows written, summed over the row counts the writers
// report in 'dataRows'.
int64_t countWrittenRows(const std::vector<velox::RowVectorPtr>& dataRows) {
  int64_t numRows{0};
  for (const auto& dataRow : dataRows) {
    auto* values =
        dataRow->childAt(velox::core::TableWriteTraits::kRowCountChannel)
            ->asUnchecked<velox::SimpleVector<int64_t>>();
    for (auto i = 0; i < dataRow->size(); ++i) {
      if (!values->isNullAt(i)) {
        numRows += values->valueAt(i);
      }
    }
  }
  return numRows;
}

// Returns the values of 'channel' across all 'statsRows'.
std::vector<int64_t> readRowCounts(
    const std::vector<velox::RowVectorPtr>& statsRows,
    int32_t channel) {
  std::vector<int64_t> rowCounts;
  rowCounts.reserve(countRows(statsRows));
  for (const auto& statsRow : statsRows) {
    VELOX_CHECK_LT(channel, statsRow->childrenSize());
    auto* values =
        statsRow->childAt(channel)->asUnchecked<velox::SimpleVector<int64_t>>();
    for (auto i = 0; i < statsRow->size(); ++i) {
      rowCounts.push_back(values->valueAt(i));
    }
  }
  return rowCounts;
}

std::pair<
    velox::RowVectorPtr,
    std::vector<std::vector<connector::ColumnStatistics>>>
extractStats(
    const WriteStatsMapping& statsMapping,
    const std::vector<velox::RowVectorPtr>& statsRows,
    const std::vector<velox::RowVectorPtr>& dataRows) {
  VELOX_CHECK(!statsRows.empty());

  // The stats output layout is:
  //   [row_count, fragment, context, groupingKey0, ..., groupingKeyN,
  //    [$row_count,] stat0, stat1, ...]
  // When there are grouping keys, a count(*) aggregate occupies the first
  // stats channel (right after grouping keys) providing exact per-group
  // row counts. This channel is included in groupingKeys (not in
  // per-column stats). Without grouping keys there is a single group, whose
  // row count is the number of rows written.
  constexpr auto kStatsChannel = velox::core::TableWriteTraits::kStatsChannel;

  if (statsMapping.numGroupingKeys == 0) {
    VELOX_CHECK_EQ(countRows(statsRows), 1);
    return {
        nullptr,
        extractPerGroupStats(
            statsMapping,
            statsRows,
            kStatsChannel,
            {countWrittenRows(dataRows)})};
  }

  const auto rowCountChannel =
      kStatsChannel + static_cast<int32_t>(statsMapping.numGroupingKeys);

  auto groupingKeyNames = statsMapping.groupingKeyNames;
  groupingKeyNames.push_back("$row_count");

  return {
      extractGroupingKeys(
          statsRows, statsMapping.numGroupingKeys + 1, groupingKeyNames),
      extractPerGroupStats(
          statsMapping,
          statsRows,
          rowCountChannel + 1,
          readRowCounts(statsRows, rowCountChannel))};
}

const auto& columnStatFieldNames() {
  static const folly::F14FastMap<ColumnStatField, std::string_view> kNames = {
      {ColumnStatField::kCount, "COUNT"},
      {ColumnStatField::kCountIf, "COUNT_IF"},
      {ColumnStatField::kMin, "MIN"},
      {ColumnStatField::kMax, "MAX"},
      {ColumnStatField::kApproxDistinct, "APPROX_DISTINCT"},
      {ColumnStatField::kSumDataSize, "SUM_DATA_SIZE"},
      {ColumnStatField::kVectorCount, "VECTOR_COUNT"},
  };
  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(ColumnStatField, columnStatFieldNames);

FinishWrite::FinishWrite(FinishWrite&& other) noexcept
    : metadata_{std::move(other.metadata_)},
      connectorId_{std::move(other.connectorId_)},
      session_{std::move(other.session_)},
      operation_{std::exchange(other.operation_, std::nullopt)} {}

FinishWrite& FinishWrite::operator=(FinishWrite&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  if (*this) {
    std::ignore = std::move(*this).abort();
  }
  metadata_ = std::move(other.metadata_);
  connectorId_ = std::move(other.connectorId_);
  session_ = std::move(other.session_);
  operation_ = std::exchange(other.operation_, std::nullopt);
  return *this;
}

FinishWrite::FinishWrite(
    std::shared_ptr<connector::ConnectorMetadata> metadata,
    std::string connectorId,
    connector::ConnectorSessionPtr session,
    connector::ConnectorWriteHandlePtr handle,
    WriteStatsMapping statsMapping)
    : metadata_{std::move(metadata)},
      connectorId_{std::move(connectorId)},
      session_{std::move(session)},
      operation_{WriteOperation{
          .handle = std::move(handle),
          .statsMapping = std::move(statsMapping)}} {
  VELOX_CHECK_NOT_NULL(metadata_);
  VELOX_CHECK_NOT_NULL(session_);
  VELOX_CHECK_NOT_NULL(std::get<WriteOperation>(*operation_).handle);
}

FinishWrite::FinishWrite(
    std::shared_ptr<connector::ConnectorMetadata> metadata,
    std::string connectorId,
    connector::ConnectorSessionPtr session,
    connector::ConnectorDeleteHandlePtr handle)
    : metadata_{std::move(metadata)},
      connectorId_{std::move(connectorId)},
      session_{std::move(session)},
      operation_{DeleteOperation{.handle = std::move(handle)}} {
  VELOX_CHECK_NOT_NULL(metadata_);
  VELOX_CHECK_NOT_NULL(session_);
  VELOX_CHECK_NOT_NULL(std::get<DeleteOperation>(*operation_).handle);
}

std::string FinishWrite::toString() const {
  if (!*this) {
    return "";
  }
  return fmt::format(
      "Metadata write via connector '{}': {}",
      connectorId_,
      std::visit(
          [](const auto& operation) { return operation.handle->toString(); },
          *operation_));
}

FinishWrite::~FinishWrite() {
  if (*this) {
    std::ignore = std::move(*this).abort();
  }
}

// static
velox::ContinueFuture FinishWrite::abortOperation(
    connector::ConnectorMetadata& metadata,
    const connector::ConnectorSessionPtr& session,
    const Operation& operation) noexcept {
  if (const auto* deletion = std::get_if<DeleteOperation>(&operation)) {
    return metadata.abortDelete(session, deletion->handle);
  }
  return metadata.abortWrite(
      session, std::get<WriteOperation>(operation).handle);
}

connector::RowsFuture FinishWrite::finish(
    const std::vector<velox::RowVectorPtr>& writeResults) const {
  if (auto* operation = std::get_if<DeleteOperation>(&*operation_)) {
    return metadata_->finishDelete(session_, operation->handle, writeResults);
  }

  auto& operation = std::get<WriteOperation>(*operation_);
  if (operation.statsMapping.columns.empty()) {
    return metadata_->finishWrite(
        session_, operation.handle, writeResults, nullptr, {});
  }

  auto split = splitWriteResults(
      writeResults,
      operation.handle->resultType()->size(),
      operation.handle->resultType());
  velox::RowVectorPtr partitionKeys;
  std::vector<std::vector<connector::ColumnStatistics>> partitionStats;
  if (!split.statsRows.empty()) {
    std::tie(partitionKeys, partitionStats) =
        extractStats(operation.statsMapping, split.statsRows, split.dataRows);
  }
  return metadata_->finishWrite(
      session_,
      operation.handle,
      split.dataRows,
      std::move(partitionKeys),
      std::move(partitionStats));
}

connector::RowsFuture FinishWrite::commit(
    const std::vector<velox::RowVectorPtr>& writeResults) && {
  VELOX_CHECK(*this);
  auto finished =
      folly::makeSemiFutureWith([&] { return finish(writeResults); });

  // The continuation owns what an abort needs, so the write is aborted only
  // when the finish call fails.
  auto operation = std::move(*operation_);
  operation_.reset();
  return std::move(finished).defer(
      [metadata = metadata_,
       session = session_,
       operation =
           std::move(operation)](folly::Try<std::optional<int64_t>>&& result)
          -> connector::RowsFuture {
        if (result.hasValue()) {
          return folly::makeSemiFuture(std::move(result));
        }
        return abortOperation(*metadata, session, operation)
            .defer(
                [failure = std::move(result).exception()](
                    folly::Try<folly::Unit>&& aborted)
                    -> std::optional<int64_t> {
                  if (aborted.hasException()) {
                    LOG(ERROR) << "Failed to abort write: "
                               << aborted.exception().what();
                  }
                  failure.throw_exception();
                });
      });
}

velox::ContinueFuture FinishWrite::abort() && noexcept {
  VELOX_CHECK(*this);
  SCOPE_EXIT {
    operation_.reset();
  };
  return abortOperation(*metadata_, session_, *operation_);
}

namespace {

std::optional<velox::core::MultiStagePlan> makeStagePlan(
    std::vector<velox::core::Stage> stages) {
  if (stages.empty()) {
    return std::nullopt;
  }
  return velox::core::MultiStagePlan(std::move(stages));
}

const FragmentScans& noScans() {
  static const FragmentScans kNoScans;
  return kNoScans;
}

std::string formatFragmentHeader(
    const velox::core::Stage& stage,
    const FragmentScans& scans) {
  std::string bucketedSuffix;
  if (!scans.groupedNodes.empty()) {
    bucketedSuffix =
        fmt::format(" bucketed(leaves={})", scans.groupedNodes.size());
  }
  return fmt::format(
      "Fragment {}: {}{}{}:",
      stage.id,
      stage.type,
      stage.numRemotePartitions.has_value()
          ? fmt::format(
                " numRemotePartitions={}", stage.numRemotePartitions.value())
          : "",
      bucketedSuffix);
}

// Checks that no stage runs more tasks than the plan's options allow.
void checkMaxRemotePartitions(
    const std::vector<velox::core::Stage>& stages,
    int32_t maxRemotePartitions) {
  for (const auto& stage : stages) {
    if (stage.numRemotePartitions.has_value()) {
      VELOX_CHECK_LE(
          stage.numRemotePartitions.value(),
          maxRemotePartitions,
          "Fragment numRemotePartitions exceeds maxRemotePartitions: {}",
          stage.id);
    }
  }
}

void checkGroupedNodes(
    const velox::core::Stage& stage,
    const FragmentScans& scans) {
  if (scans.groupedNodes.empty()) {
    return;
  }

  folly::F14FastSet<velox::core::PlanNodeId> leafIds;
  std::vector<const velox::core::PlanNode*> stack;
  stack.push_back(stage.plan.get());
  while (!stack.empty()) {
    const auto* node = stack.back();
    stack.pop_back();
    if (dynamic_cast<const velox::core::TableScanNode*>(node) != nullptr ||
        dynamic_cast<const velox::core::ExchangeNode*>(node) != nullptr) {
      leafIds.insert(node->id());
    }
    for (const auto& child : node->sources()) {
      stack.push_back(child.get());
    }
  }

  std::optional<int32_t> commonNumPartitions;
  for (const auto& [planNodeId, partitionType] : scans.groupedNodes) {
    VELOX_CHECK(
        leafIds.contains(planNodeId),
        "groupedNodes references a PlanNodeId that is not a leaf TableScan or ExchangeNode in fragment '{}': {}",
        stage.id,
        planNodeId);
    if (partitionType == nullptr) {
      continue;
    }
    if (commonNumPartitions.has_value()) {
      VELOX_CHECK_EQ(
          commonNumPartitions.value(),
          partitionType->numPartitions(),
          "All non-null groupedNodes entries in a fragment must share numPartitions(): {}",
          stage.id);
    } else {
      commonNumPartitions = partitionType->numPartitions();
    }
  }
}

// Checks that the root stage has a type compatible with local result
// consumption.
void checkRootStage(
    const velox::core::Stage& root,
    int32_t maxRemotePartitions) {
  VELOX_CHECK(
      root.type == velox::core::StageType::kSingle ||
          root.type == velox::core::StageType::kCoordinator ||
          (root.type == velox::core::StageType::kSource &&
           maxRemotePartitions == 1),
      "Root fragment must be kSingle or kCoordinator "
      "when remoteOutput is false (kSource allowed only with "
      "maxRemotePartitions == 1): {}",
      root.id);
}

// Checks that each entry of 'scans' names one of the plan's 'numStages'
// stages.
void checkScansNameStages(
    const folly::F14FastMap<int32_t, FragmentScans>& scans,
    size_t numStages) {
  for (const auto& [stageId, _] : scans) {
    VELOX_CHECK_GE(
        stageId, 1, "Scans name a stage that is not in the plan: {}", stageId);
    VELOX_CHECK_LE(
        stageId,
        static_cast<int32_t>(numStages),
        "Scans name a stage that is not in the plan: {}",
        stageId);
  }
}

} // namespace

MultiFragmentPlan::MultiFragmentPlan(
    std::vector<velox::core::Stage> stages,
    folly::F14FastMap<int32_t, FragmentScans> scans,
    Options options)
    : stagePlan_{makeStagePlan(std::move(stages))},
      scans_{std::move(scans)},
      options_{std::move(options)} {
  checkScansNameStages(scans_, this->stages().size());
  if (empty()) {
    return;
  }
  checkMaxRemotePartitions(stagePlan_->stages(), options_.maxRemotePartitions);
  for (const auto& stage : stagePlan_->stages()) {
    checkGroupedNodes(stage, this->scans(stage.id));
  }
  if (!options_.remoteOutput) {
    checkRootStage(stagePlan_->root(), options_.maxRemotePartitions);
  }
}

const velox::core::MultiStagePlan& MultiFragmentPlan::stagePlan() const {
  VELOX_CHECK(stagePlan_.has_value(), "Plan has no stages");
  return stagePlan_.value();
}

const std::vector<velox::core::Stage>& MultiFragmentPlan::stages() const {
  static const std::vector<velox::core::Stage> kNoStages;
  return stagePlan_.has_value() ? stagePlan_->stages() : kNoStages;
}

const FragmentScans& MultiFragmentPlan::scans(int32_t stageId) const {
  const auto it = scans_.find(stageId);
  return it == scans_.end() ? noScans() : it->second;
}

namespace {

// Returns the largest id of 'node' and the nodes under it, including those of
// a fixed point's nested plans. The optimizer gives plan nodes integer ids.
int32_t maxNodeId(const velox::core::PlanNode& node) {
  auto result = folly::to<int32_t>(node.id());
  for (const auto& source : node.sources()) {
    result = std::max(result, maxNodeId(*source));
  }
  if (const auto* fixedPoint = node.as<velox::core::FixedPointNode>()) {
    for (const auto& nested : fixedPoint->nestedPlans()) {
      result = std::max(result, maxNodeId(*nested));
    }
  }
  return result;
}

} // namespace

int32_t MultiFragmentPlan::maxPlanNodeId() const {
  auto result = options_.firstPlanNodeId - 1;
  for (const auto& stage : stages()) {
    result = std::max(result, maxNodeId(*stage.plan));
  }
  return result;
}

std::string MultiFragmentPlan::toString(
    bool detailed,
    const std::function<void(
        const velox::core::PlanNodeId& nodeId,
        std::string_view indentation,
        std::ostream& out)>& addContext) const {
  // Map each Exchange node to the stages producing its input.
  folly::F14FastMap<velox::core::PlanNodeId, std::vector<int32_t>>
      planNodeToProducers;
  for (const auto& stage : stages()) {
    for (const auto& input : stage.inputs) {
      planNodeToProducers[input.exchangeId].emplace_back(input.producerStageId);
    }
  }

  std::stringstream out;
  for (const auto& stage : stages()) {
    const auto& stageScans = scans(stage.id);
    out << formatFragmentHeader(stage, stageScans) << std::endl;

    out << stage.plan->toString(
               detailed,
               true,
               [&](const velox::core::PlanNodeId& planNodeId,
                   std::string_view indentation,
                   std::ostream& stream) {
                 if (addContext != nullptr) {
                   addContext(planNodeId, indentation, stream);
                 }
                 if (auto sampled = stageScans.sampledScans.find(planNodeId);
                     sampled != stageScans.sampledScans.end()) {
                   stream << indentation << "sample: " << sampled->second << "%"
                          << std::endl;
                 }
                 auto it = planNodeToProducers.find(planNodeId);
                 if (it != planNodeToProducers.end()) {
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
  for (const auto& stage : stages()) {
    out << formatFragmentHeader(stage, scans(stage.id)) << std::endl;
    out << stage.plan->toSummaryString(options) << std::endl;
    if (!stage.inputs.empty()) {
      out << "Inputs: ";
      for (const auto& input : stage.inputs) {
        out << fmt::format(
            " {} <- {} ", input.exchangeId, input.producerStageId);
      }
      out << std::endl;
    }
  }
  return out.str();
}

} // namespace facebook::axiom::optimizer
