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

#include "axiom/optimizer/v2/GroupedRead.h"

#include <algorithm>
#include <vector>

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/NodeRewriter.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

// The partition type a scan is read grouped by: its table's bucketing coarsened
// to 'numWorkers', so two scans that scale alike stay one node. Null when the
// table has no bucketing.
const connector::PartitionType* groupedPartitionType(
    const Scan* scan,
    int32_t numWorkers) {
  const auto* partitionType = scan->storageBucketing().partitionType;
  if (partitionType == nullptr) {
    return nullptr;
  }
  return queryCtx()->scaledPartitionType(partitionType, numWorkers);
}

// True when regrouping the scans under 'node' can make it bucketed. Only a scan
// of a bucketed table contributes, and a global exchange ends the search.
bool hasRegroupableScan(NodeCP node) {
  if (node->is(NodeType::kExchange) &&
      node->as<Exchange>()->partitioning().scope == PropertyScope::kGlobal) {
    return false;
  }
  if (node->is(NodeType::kScan)) {
    return node->as<Scan>()->storageBucketing().partitionType != nullptr;
  }
  if (node->is(NodeType::kUnionAll)) {
    // A union is bucketed only when every input is.
    return std::ranges::all_of(node->inputs(), hasRegroupableScan);
  }
  return std::ranges::any_of(node->inputs(), hasRegroupableScan);
}

// Rewrites a subtree so every scan of a bucketed table is read one bucket-group
// at a time. Nodes above are rebuilt by the base rewriter, which re-derives
// their partitioning from the new inputs.
class GroupedScanRewriter : public NodeRewriter<> {
 public:
  GroupedScanRewriter(Builder& builder, int32_t numWorkers)
      : NodeRewriter<>(builder), numWorkers_{numWorkers} {}

 protected:
  // A global exchange ends grouped-read propagation. Rebuild a driver exchange
  // because it preserves task placement.
  NodeCP rewriteExchange(const Exchange* node, NoContext& context) override {
    if (node->partitioning().scope == PropertyScope::kGlobal) {
      return node;
    }
    return NodeRewriter<>::rewriteExchange(node, context);
  }

  // A union is bucketed only when every input is, so one ungroupable input
  // forfeits it for all of them.
  NodeCP rewriteUnionAll(const UnionAll* node, NoContext& context) override {
    for (NodeCP input : node->inputs()) {
      if (!hasRegroupableScan(input)) {
        return node;
      }
    }
    return NodeRewriter<>::rewriteUnionAll(node, context);
  }

  // Whether this bucketing is any use to the consumer is not decided here: the
  // keys it asked for may belong to another input or another side of a join. A
  // table with no bucketing is left alone.
  NodeCP rewriteScan(const Scan* node, NoContext& /*context*/) override {
    const auto* partitionType = groupedPartitionType(node, numWorkers_);
    if (partitionType == nullptr) {
      return node;
    }
    return builder().make<Scan>(
        {.baseTable = node->baseTable(),
         .outputColumns = node->outputColumns(),
         .scanHandle = node->scanHandle(),
         .groupedPartitionType = partitionType});
  }

 private:
  const int32_t numWorkers_;
};

} // namespace

Partitioning
GroupedRead::partitioning(NodeCP node, int32_t numWorkers, Builder& builder) {
  switch (node->nodeType()) {
    case NodeType::kExchange:
      return node->as<Exchange>()->partitioning().scope ==
              PropertyScope::kGlobal
          ? node->physicalProperties().globalPartition
          : partitioning(node->inputs().front(), numWorkers, builder)
                .dropOrder();
    case NodeType::kScan: {
      const auto* scan = node->as<Scan>();
      const auto* partitionType = groupedPartitionType(scan, numWorkers);
      return partitionType == nullptr
          ? scan->physicalProperties().globalPartition
          : scan->groupedPartition(partitionType);
    }
    case NodeType::kUnionAll:
      if (!hasRegroupableScan(node)) {
        return node->physicalProperties().globalPartition;
      }
      break;
    default:
      break;
  }
  std::vector<Partitioning> inputPartitions;
  inputPartitions.reserve(node->inputs().size());
  for (NodeCP input : node->inputs()) {
    inputPartitions.push_back(partitioning(input, numWorkers, builder));
  }
  return node->globalPartition(inputPartitions, builder);
}

NodeCP GroupedRead::rewrite(NodeCP node, int32_t numWorkers, Builder& builder) {
  GroupedScanRewriter rewriter{builder, numWorkers};
  NoContext context;
  NodeCP grouped = rewriter.rewrite(node, context);
  VELOX_DCHECK(
      grouped->physicalProperties().globalPartition ==
          partitioning(node, numWorkers, builder),
      "A grouped read must have the partitioning it was predicted to have");
  return grouped;
}

} // namespace facebook::axiom::optimizer::v2
