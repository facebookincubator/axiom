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

#include "axiom/connectors/ConnectorSplitManager.h"

#include <utility>

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector {

SplitSchedulingInfo SplitSchedulingInfo::noPreference() {
  return SplitSchedulingInfo{
      SplitNodeSelectionStrategy::kNoPreference, std::nullopt};
}

SplitSchedulingInfo SplitSchedulingInfo::softAffinity(std::string affinityKey) {
  VELOX_CHECK(!affinityKey.empty(), "Split affinity key cannot be empty");
  return SplitSchedulingInfo{
      SplitNodeSelectionStrategy::kSoftAffinity, std::move(affinityKey)};
}

SplitSchedulingInfo::SplitSchedulingInfo(
    SplitNodeSelectionStrategy nodeSelectionStrategy,
    std::optional<std::string> affinityKey)
    : nodeSelectionStrategy_(nodeSelectionStrategy),
      affinityKey_(std::move(affinityKey)) {}

void SplitBatch::addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) {
  addSplit(std::move(split), SplitSchedulingInfo::noPreference());
}

void SplitBatch::addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split,
    SplitSchedulingInfo info) {
  VELOX_CHECK_NOT_NULL(split);
  splits.push_back(std::move(split));
  schedulingInfo.push_back(std::move(info));
}

void SplitBatch::addSplit(ConnectorSplitWithScheduling split) {
  addSplit(std::move(split.connectorSplit), std::move(split.schedulingInfo));
}

const SplitSchedulingInfo& SplitBatch::schedulingInfoAt(size_t index) const {
  VELOX_CHECK_LT(index, splits.size());
  if (schedulingInfo.empty()) {
    static const SplitSchedulingInfo kNoPreference =
        SplitSchedulingInfo::noPreference();
    return kNoPreference;
  }
  VELOX_CHECK_EQ(
      schedulingInfo.size(),
      splits.size(),
      "Split scheduling metadata must match split count");
  return schedulingInfo[index];
}

} // namespace facebook::axiom::connector
