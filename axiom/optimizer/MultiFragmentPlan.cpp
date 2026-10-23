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

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include <cmath>

namespace facebook::axiom::optimizer {

namespace {

// Returns elements [start, end) of a container as a std::vector.
template <typename Container>
auto sliceToVector(const Container& container, size_t start, size_t end) {
  return std::vector<typename std::decay_t<decltype(*container.begin())>>(
      container.begin() + start, container.begin() + end);
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

} // namespace

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
    std::string connectorId,
    connector::ConnectorSessionPtr session,
    connector::ConnectorWriteHandlePtr handle,
    WriteStatsMapping statsMapping)
    : metadata_{std::move(metadata)},
      connectorId_{std::move(connectorId)},
      session_{std::move(session)},
      handle_{std::move(handle)},
      statsMapping_{std::move(statsMapping)} {
  VELOX_CHECK_NOT_NULL(metadata_);
  VELOX_CHECK_NOT_NULL(session_);
  VELOX_CHECK_NOT_NULL(handle_);
}

std::string FinishWrite::toString() const {
  if (handle_ == nullptr) {
    return "";
  }
  return fmt::format(
      "Metadata write via connector '{}': {}",
      connectorId_,
      handle_->toString());
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

  if (statsMapping_.request.statistics.empty()) {
    return metadata_->finishWrite(session_, handle_, writeResults, {});
  }

  auto [statsRows, dataRows] = splitWriteResults(
      writeResults, handle_->resultType()->size(), handle_->resultType());

  if (statsRows.empty()) {
    return metadata_->finishWrite(session_, handle_, dataRows, {});
  }

  auto results = connector::StatisticsResults::fromAggregates(
      statsMapping_.request,
      statsMapping_.collected,
      statsRows,
      statsRows[0]->pool(),
      velox::core::TableWriteTraits::kStatsChannel);
  return metadata_->finishWrite(
      session_, handle_, dataRows, std::move(results));
}

velox::ContinueFuture FinishWrite::abort() && noexcept {
  VELOX_CHECK(*this);
  SCOPE_EXIT {
    *this = {};
  };
  return metadata_->abortWrite(session_, handle_);
}

namespace {
std::string formatFragmentHeader(const ExecutableFragment& fragment) {
  std::string bucketedSuffix;
  if (!fragment.groupedNodes.empty()) {
    bucketedSuffix =
        fmt::format(" bucketed(leaves={})", fragment.groupedNodes.size());
  }
  return fmt::format(
      "Fragment {}: {}{}{}:",
      fragment.fragmentId,
      FragmentTypeName::toName(fragment.type),
      fragment.numRemotePartitions.has_value()
          ? fmt::format(
                " numRemotePartitions={}", fragment.numRemotePartitions.value())
          : "",
      bucketedSuffix);
}
} // namespace

std::string MultiFragmentPlan::toString(
    bool detailed,
    const std::function<void(
        const velox::core::PlanNodeId& nodeId,
        std::string_view indentation,
        std::ostream& out)>& addContext) const {
  // Map each Exchange node to the fragments producing its input.
  folly::F14FastMap<velox::core::PlanNodeId, std::vector<int32_t>>
      planNodeToProducers;
  for (const auto& fragment : fragments_) {
    for (const auto& input : fragment.inputStages) {
      planNodeToProducers[input.consumerNodeId].emplace_back(
          input.producerFragmentId);
    }
  }

  std::stringstream out;
  for (const auto& fragment : fragments_) {
    out << formatFragmentHeader(fragment) << std::endl;

    out << fragment.fragment.planNode->toString(
               detailed,
               true,
               [&](const velox::core::PlanNodeId& planNodeId,
                   std::string_view indentation,
                   std::ostream& stream) {
                 if (addContext != nullptr) {
                   addContext(planNodeId, indentation, stream);
                 }
                 if (auto sampled = fragment.sampledScans.find(planNodeId);
                     sampled != fragment.sampledScans.end()) {
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
  for (const auto& fragment : fragments_) {
    out << formatFragmentHeader(fragment) << std::endl;
    out << fragment.fragment.planNode->toSummaryString(options) << std::endl;
    if (!fragment.inputStages.empty()) {
      out << "Inputs: ";
      for (const auto& input : fragment.inputStages) {
        out << fmt::format(
            " {} <- {} ", input.consumerNodeId, input.producerFragmentId);
      }
      out << std::endl;
    }
  }
  return out.str();
}

namespace {

// Checks that each fragment's type is consistent with its numRemotePartitions
// and that numRemotePartitions does not exceed maxRemotePartitions.
void checkFragmentTypes(
    const std::vector<ExecutableFragment>& fragments,
    int32_t maxRemotePartitions) {
  for (const auto& fragment : fragments) {
    const auto& numRemotePartitions = fragment.numRemotePartitions;
    const auto fragmentId = fragment.fragmentId;

    switch (fragment.type) {
      case FragmentType::kFixed:
        VELOX_CHECK(
            numRemotePartitions.has_value(),
            "kFixed fragment must have numRemotePartitions set: {}",
            fragmentId);
        break;
      case FragmentType::kSingle:
      case FragmentType::kCoordinator:
        VELOX_CHECK(
            !numRemotePartitions.has_value(),
            "{} fragment must not have numRemotePartitions set: {}",
            FragmentTypeName::toName(fragment.type),
            fragmentId);
        break;
      case FragmentType::kSource:
        break;
    }

    if (numRemotePartitions.has_value()) {
      VELOX_CHECK_GT(
          numRemotePartitions.value(),
          0,
          "Fragment numRemotePartitions must be positive: {}",
          fragmentId);
      VELOX_CHECK_LE(
          numRemotePartitions.value(),
          maxRemotePartitions,
          "Fragment numRemotePartitions exceeds maxRemotePartitions: {}",
          fragmentId);
    }
  }
}

// Returns the node with 'id' in the tree under 'node', including the initial
// plans of a fixed point, which read their exchanges once before the loop.
const velox::core::PlanNode* findConsumerNode(
    const velox::core::PlanNode* node,
    const velox::core::PlanNodeId& id) {
  if (node->id() == id) {
    return node;
  }
  if (const auto* fixedPoint = node->as<velox::core::FixedPointNode>()) {
    for (const auto& declaration : fixedPoint->stateDeclarations()) {
      if (declaration->initialPlan() != nullptr) {
        if (const auto* found =
                findConsumerNode(declaration->initialPlan().get(), id)) {
          return found;
        }
      }
    }
  }
  for (const auto& source : node->sources()) {
    if (const auto* found = findConsumerNode(source.get(), id)) {
      return found;
    }
  }
  return nullptr;
}

// Checks producer-consumer linkage: fragment ids are unique,
// each InputStage references a valid ExchangeNode and an existing producer
// whose root is a PartitionedOutputNode with matching partition count. Also
// checks for self-links, that the last fragment is not a producer, that each
// producer has a single consumer, and that every non-last fragment is
// referenced.
void checkProducerConsumerLinkage(
    const std::vector<ExecutableFragment>& fragments) {
  folly::F14FastMap<int32_t, size_t> fragmentIndices;
  for (size_t i = 0; i < fragments.size(); ++i) {
    const auto fragmentId = fragments[i].fragmentId;
    VELOX_CHECK_GE(fragmentId, 1, "Fragment id must be positive");
    VELOX_CHECK_LE(
        fragmentId,
        static_cast<int32_t>(fragments.size()),
        "Fragment id exceeds the number of fragments");
    auto [_, inserted] = fragmentIndices.emplace(fragmentId, i);
    VELOX_CHECK(inserted, "Duplicate fragment id: {}", fragmentId);
  }

  folly::F14FastSet<size_t> referencedProducers;

  for (const auto& consumer : fragments) {
    for (const auto& inputStage : consumer.inputStages) {
      const auto* consumerNode = findConsumerNode(
          consumer.fragment.planNode.get(), inputStage.consumerNodeId);
      VELOX_CHECK_NOT_NULL(
          consumerNode,
          "Consumer node not found: {}, fragment: {}",
          inputStage.consumerNodeId,
          consumer.fragmentId);
      VELOX_CHECK_NOT_NULL(
          dynamic_cast<const velox::core::ExchangeNode*>(consumerNode),
          "Consumer node must be an ExchangeNode: {}, fragment: {}",
          inputStage.consumerNodeId,
          consumer.fragmentId);

      auto it = fragmentIndices.find(inputStage.producerFragmentId);
      VELOX_CHECK(
          it != fragmentIndices.end(),
          "Producer fragment not found: {}, consumer: {}",
          inputStage.producerFragmentId,
          consumer.fragmentId);

      VELOX_CHECK_NE(
          it->second,
          fragments.size() - 1,
          "Last fragment cannot be a producer: {}",
          inputStage.producerFragmentId);

      VELOX_CHECK_NE(
          inputStage.producerFragmentId,
          consumer.fragmentId,
          "Fragment cannot be its own producer: {}",
          consumer.fragmentId);

      auto [_, isNew] = referencedProducers.insert(it->second);
      VELOX_CHECK(
          isNew,
          "Producer fragment referenced by multiple consumers: {}",
          inputStage.producerFragmentId);

      const auto& producer = fragments[it->second];
      const auto* partitionedOutput =
          dynamic_cast<const velox::core::PartitionedOutputNode*>(
              producer.fragment.planNode.get());
      VELOX_CHECK_NOT_NULL(
          partitionedOutput,
          "Expected PartitionedOutputNode at root of producer fragment: {}",
          producer.fragmentId);

      if (partitionedOutput->isBroadcast() ||
          partitionedOutput->isArbitrary()) {
        continue;
      }

      if (consumer.type == FragmentType::kSource) {
        continue;
      }

      VELOX_CHECK_EQ(
          partitionedOutput->numPartitions(),
          consumer.numRemotePartitions.value_or(1),
          "Partition count mismatch between producer {} and consumer {}",
          producer.fragmentId,
          consumer.fragmentId);
    }
  }

  for (size_t i = 0; i < fragments.size() - 1; ++i) {
    VELOX_CHECK(
        referencedProducers.contains(i),
        "Non-last fragment must be referenced by exactly one consumer: {}",
        fragments[i].fragmentId);
  }
}

void checkGroupedNodes(const ExecutableFragment& fragment) {
  if (fragment.groupedNodes.empty()) {
    return;
  }

  folly::F14FastSet<velox::core::PlanNodeId> leafIds;
  std::vector<const velox::core::PlanNode*> stack;
  stack.push_back(fragment.fragment.planNode.get());
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
  for (const auto& [planNodeId, partitionType] : fragment.groupedNodes) {
    VELOX_CHECK(
        leafIds.contains(planNodeId),
        "groupedNodes references a PlanNodeId that is not a leaf TableScan or ExchangeNode in fragment '{}': {}",
        fragment.fragmentId,
        planNodeId);
    if (partitionType == nullptr) {
      continue;
    }
    if (commonNumPartitions.has_value()) {
      VELOX_CHECK_EQ(
          commonNumPartitions.value(),
          partitionType->numPartitions(),
          "All non-null groupedNodes entries in a fragment must share numPartitions(): {}",
          fragment.fragmentId);
    } else {
      commonNumPartitions = partitionType->numPartitions();
    }
  }
}

// Checks that the last fragment has a type compatible with local result
// consumption.
void checkLastFragment(
    const ExecutableFragment& last,
    int32_t maxRemotePartitions) {
  VELOX_CHECK(
      last.type == FragmentType::kSingle ||
          last.type == FragmentType::kCoordinator ||
          (last.type == FragmentType::kSource && maxRemotePartitions == 1),
      "Last fragment must be kSingle or kCoordinator "
      "when remoteOutput is false (kSource allowed only with "
      "maxRemotePartitions == 1): {}",
      last.fragmentId);
}

} // namespace

void MultiFragmentPlan::checkConsistency(bool mayBeEmpty) const {
  if (fragments_.empty()) {
    VELOX_CHECK(mayBeEmpty, "Plan must have at least one fragment");
    return;
  }

  checkFragmentTypes(fragments_, options_.maxRemotePartitions);

  checkProducerConsumerLinkage(fragments_);

  for (const auto& fragment : fragments_) {
    checkGroupedNodes(fragment);
  }

  if (!options_.remoteOutput) {
    checkLastFragment(fragments_.back(), options_.maxRemotePartitions);
  }
}

} // namespace facebook::axiom::optimizer
