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

#include "axiom/optimizer/connectors/ConnectorSplitSource.h"

namespace facebook::velox::connector {

namespace {

/// A runner::SplitSource that encapsulates a connector::SplitSource.
/// runner::SplitSource does not depend on ConnectorMetadata.h, thus we have a
/// proxy between the two.
class ConnectorSplitSource : public axiom::runner::SplitSource {
 public:
  ConnectorSplitSource(std::shared_ptr<connector::SplitSource> source)
      : source_(std::move(source)) {}

  std::vector<SplitAndGroup> getSplits(uint64_t targetBytes) override;

 private:
  std::shared_ptr<connector::SplitSource> source_;
};

std::vector<axiom::runner::SplitSource::SplitAndGroup>
ConnectorSplitSource::getSplits(uint64_t targetBytes) {
  auto splits = source_->getSplits(targetBytes);

  std::vector<axiom::runner::SplitSource::SplitAndGroup> runnerSplits;
  runnerSplits.reserve(splits.size());

  // Convert the connector::SplitSource::SplitAndGroup to
  // runner::SplitSource::SplitAndGroup.
  for (const auto& split : splits) {
    runnerSplits.emplace_back(
        axiom::runner::SplitSource::SplitAndGroup{split.split, split.group});
  }
  return runnerSplits;
}

} // namespace

std::shared_ptr<axiom::runner::SplitSource>
ConnectorSplitSourceFactory::splitSourceForScan(
    const core::TableScanNode& scan) {
  const auto& handle = scan.tableHandle();
  auto connector = connector::getConnector(handle->connectorId());
  auto partitions =
      connector->metadata()->splitManager()->listPartitions(handle);

  return std::make_shared<ConnectorSplitSource>(
      connector->metadata()->splitManager()->getSplitSource(
          handle, partitions, options_));
}

} // namespace facebook::velox::connector
