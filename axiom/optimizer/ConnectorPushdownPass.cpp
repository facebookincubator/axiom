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
#include "axiom/optimizer/ConnectorPushdownPass.h"

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "folly/container/F14Map.h"
#include "folly/container/F14Set.h"

namespace facebook::axiom::optimizer {
namespace {

namespace lp = facebook::axiom::logical_plan;
using connector::ConnectorMetadataRegistry;
using connector::PushdownRoot;

using MaximalRootsByConnector =
    folly::F14FastMap<std::string, std::vector<const lp::LogicalPlanNode*>>;

// Per-node result of the bottom-up walk. Every maximal root references exactly
// one connector. Boundaries are recorded at multi-connector cut points;
// single-connector subtrees pass through to the parent, and the top-level
// finalize handles plans with no cut point.
struct NodeInfo {
  folly::F14FastSet<std::string> connectors;
  MaximalRootsByConnector maximalRoots;
};

NodeInfo collect(const lp::LogicalPlanNode& node) {
  NodeInfo info;
  if (node.kind() == lp::NodeKind::kTableScan) {
    info.connectors.insert(node.as<lp::TableScanNode>()->connectorId());
    return info;
  }

  const auto& inputs = node.inputs();
  std::vector<NodeInfo> childInfos;
  childInfos.reserve(inputs.size());
  for (const auto& input : inputs) {
    auto childInfo = collect(*input);
    info.connectors.insert(
        childInfo.connectors.begin(), childInfo.connectors.end());
    childInfos.push_back(std::move(childInfo));
  }

  if (info.connectors.size() <= 1) {
    // Passthrough condition, no cut point.
    return info;
  }

  // Multi-connector cut point: each single-connector child is a
  // maximal root for its connector.
  for (size_t i = 0; i < inputs.size(); ++i) {
    const auto& childInfo = childInfos.at(i);
    if (childInfo.connectors.size() == 1) {
      const auto& connectorId = *childInfo.connectors.begin();
      info.maximalRoots[connectorId].push_back(inputs[i].get());
    } else {
      for (const auto& [connectorId, roots] : childInfo.maximalRoots) {
        auto& destination = info.maximalRoots[connectorId];
        destination.insert(destination.end(), roots.begin(), roots.end());
      }
    }
  }
  return info;
}

bool contains(
    const lp::LogicalPlanNode& haystack,
    const lp::LogicalPlanNode* needle) {
  for (const auto& input : haystack.inputs()) {
    if (input.get() == needle) {
      return true;
    }
    if (contains(*input, needle)) {
      return true;
    }
  }
  return false;
}

// Checks one connector's `co_pushdownPlan` return: non-null fields,
// every root within `subtree`, no bare `TableScanNode` roots, and pairwise
// disjoint within the call. Cross-connector disjointness is structural —
// connectors only see non-overlapping subtrees.
void checkPushdownRoots(
    const std::vector<PushdownRoot>& roots,
    const lp::LogicalPlanNode& subtree) {
  for (const auto& root : roots) {
    VELOX_CHECK_NOT_NULL(root.root, "Connector returned null root");
    VELOX_CHECK_NOT_NULL(root.table, "Connector returned null table");
    VELOX_CHECK(
        root.root == &subtree || contains(subtree, root.root),
        "Connector returned pushdown root outside the subtree it was given");
    VELOX_CHECK(
        root.root->kind() != lp::NodeKind::kTableScan,
        "Connector returned a bare TableScanNode as a pushdown root; "
        "the scan already runs on the connector");
  }
  for (size_t i = 0; i < roots.size(); ++i) {
    for (size_t j = i + 1; j < roots.size(); ++j) {
      VELOX_CHECK(
          roots[i].root != roots[j].root,
          "Connector returned the same pushdown root twice");
      VELOX_CHECK(
          !contains(*roots[i].root, roots[j].root) &&
              !contains(*roots[j].root, roots[i].root),
          "Connector returned overlapping pushdown roots");
    }
  }
}

} // namespace

folly::coro::Task<std::vector<PushdownRoot>> collectConnectorPushdownRoots(
    const lp::LogicalPlanNode& plan) {
  auto info = collect(plan);

  // If no connector referenced by the plan opts into pushdown, skip the rest.
  bool anyOptedIn = false;
  for (const auto& connectorId : info.connectors) {
    auto metadata = ConnectorMetadataRegistry::tryGet(connectorId);
    if (metadata && metadata->isPushdownSupported()) {
      anyOptedIn = true;
      break;
    }
  }
  if (!anyOptedIn) {
    co_return std::vector<PushdownRoot>{};
  }

  MaximalRootsByConnector maximalRoots;
  if (info.connectors.size() == 1) {
    // Whole plan belongs to one connector — the plan itself is the
    // maximal subtree.
    maximalRoots[*info.connectors.begin()].push_back(&plan);
  } else {
    maximalRoots = std::move(info.maximalRoots);
  }

  std::vector<PushdownRoot> pushdownRoots;
  // Iterate connectors in deterministic id order (F14FastMap iteration
  // is non-deterministic).
  std::vector<std::string> connectorIds;
  connectorIds.reserve(maximalRoots.size());
  for (const auto& [connectorId, _] : maximalRoots) {
    connectorIds.push_back(connectorId);
  }
  std::sort(connectorIds.begin(), connectorIds.end());
  // TODO: Fan out across connectors / subtrees with
  // `folly::coro::collectAllRange` so per-connector RPCs run
  // concurrently, then validate each result per its subtree. Sequential
  // for now; cheap to revisit while the API is EXPERIMENTAL.
  for (const auto& connectorId : connectorIds) {
    const auto& subtrees = maximalRoots.at(connectorId);
    auto metadata = ConnectorMetadataRegistry::tryGet(connectorId);
    if (!metadata) {
      continue;
    }
    if (!metadata->isPushdownSupported()) {
      continue;
    }
    for (const auto* subtree : subtrees) {
      auto roots = co_await metadata->co_pushdownPlan(*subtree);
      checkPushdownRoots(roots, *subtree);
      for (auto& root : roots) {
        pushdownRoots.push_back(std::move(root));
      }
    }
  }
  co_return pushdownRoots;
}

} // namespace facebook::axiom::optimizer
