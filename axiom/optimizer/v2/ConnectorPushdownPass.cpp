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

#include "axiom/optimizer/v2/ConnectorPushdownPass.h"

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/ConnectorPushdown.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/Schema.h"
#include "folly/container/F14Map.h"
#include "folly/container/F14Set.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

using connector::ConnectorMetadataRegistry;
using connector::ConnectorPushdown;
using connector::PushdownRoot;

using ConnectorSet = folly::F14FastSet<std::string>;
using NodeSet = folly::F14FastSet<NodeCP>;
using SubstitutionMap = folly::F14FastMap<NodeCP, NodeCP>;

// Nodes strictly below `node`, dedup'd for the hash-consed DAG. Backs
// the O(1) descendant checks in `checkPushdownRoots`.
NodeSet collectDescendants(NodeCP node) {
  NodeSet visited;
  std::vector<NodeCP> stack{node->inputs().begin(), node->inputs().end()};
  while (!stack.empty()) {
    NodeCP current = stack.back();
    stack.pop_back();
    if (!visited.insert(current).second) {
      continue;
    }
    for (NodeCP input : current->inputs()) {
      stack.push_back(input);
    }
  }
  return visited;
}

// Enforces one connector's `co_pushdown` response contract: every returned
// root sits inside the offered `subtree`, no returned root is a bare `Scan`
// (already runs on the connector), and roots within one response are
// pairwise disjoint. Cross-connector disjointness is structural — each
// connector only sees its own maximal single-connector subtree.
void checkPushdownRoots(
    const std::vector<PushdownRoot>& pushdowns,
    NodeCP subtree,
    std::string_view connectorId) {
  const auto subtreeDescendants = collectDescendants(subtree);
  for (const auto& pushdown : pushdowns) {
    VELOX_CHECK(
        pushdown.root == subtree || subtreeDescendants.contains(pushdown.root),
        "Connector {} returned pushdown root of type {} outside the offered subtree",
        connectorId,
        pushdown.root->nodeType());
    VELOX_CHECK(
        pushdown.root->nodeType() != NodeType::kScan,
        "Connector {} returned a bare Scan node as a pushdown root; "
        "the scan already runs on the connector",
        connectorId);
  }
  // Precompute per-root descendant sets so the pairwise disjointness
  // check is O(N^2) hash lookups instead of O(N^2) subtree walks.
  std::vector<NodeSet> descendantsPerRoot;
  descendantsPerRoot.reserve(pushdowns.size());
  for (const auto& pushdown : pushdowns) {
    descendantsPerRoot.push_back(collectDescendants(pushdown.root));
  }
  for (size_t i = 0; i < pushdowns.size(); ++i) {
    for (size_t j = i + 1; j < pushdowns.size(); ++j) {
      VELOX_CHECK(
          pushdowns[i].root != pushdowns[j].root,
          "Connector {} returned the same pushdown root of type {} twice",
          connectorId,
          pushdowns[i].root->nodeType());
      VELOX_CHECK(
          !descendantsPerRoot[i].contains(pushdowns[j].root) &&
              !descendantsPerRoot[j].contains(pushdowns[i].root),
          "Connector {} returned overlapping pushdown roots: {} and {}",
          connectorId,
          pushdowns[i].root->nodeType(),
          pushdowns[j].root->nodeType());
    }
  }
}

// Builds the synthetic `Scan` over `pushdownRoot.table` that replaces
// `pushdownRoot.root`. Enforces the consumer-column contract on
// `connector::PushdownRoot` (name + equivalent type per consumer column).
NodeCP buildSyntheticScan(
    const PushdownRoot& pushdownRoot,
    Builder& builder,
    const Schema& schema) {
  const auto* schemaTable = schema.adoptConnectorTable(pushdownRoot.table);
  const auto& outputColumns = pushdownRoot.root->outputColumns();

  auto* baseTable = make<BaseTable>();
  baseTable->cname = queryCtx()->newName("t");
  baseTable->schemaTable = schemaTable;
  baseTable->filteredCardinality = schemaTable->cardinality;
  // `baseTable->columns` is left empty on purpose: the Scan's outputs are
  // reused from the pushdown root so consumers above resolve unchanged.

  for (ColumnCP column : outputColumns) {
    const auto* schemaColumn = schemaTable->findColumn(column->name());
    VELOX_CHECK_NOT_NULL(
        schemaColumn,
        "Pushdown virtual table missing column expected by consumer: {}",
        column->name());
    VELOX_CHECK(
        column->value().type->equivalent(*schemaColumn->value().type),
        "Pushdown virtual table column type mismatch for {}: consumer {} vs table {}",
        column->name(),
        column->value().type->toString(),
        schemaColumn->value().type->toString());
  }

  return builder.make<Scan>({baseTable, outputColumns, /*filters=*/{}});
}

// One (subtree, connector) pair to be presented to a connector. A subtree
// is offered if it is the maximal single-connector region rooted at either
// the plan root or a multi-connector cut point.
struct PushdownOffer {
  NodeCP subtree;
  std::string connectorId;
};

// Bottom-up walk that computes each subtree's connector reachability and
// collects a `PushdownOffer` for every maximal single-connector subtree —
// at each multi-connector cut point, and at the plan root when the whole
// plan reaches one connector. Bare-`Scan` subtrees are skipped: any
// non-empty response would fail `checkPushdownRoots`.
//
// Offers are pairwise disjoint by construction: a maximal single-connector
// subtree cannot enclose another (its descendants share its connector, so
// no cut point exists within it), and offers at the same cut point are
// structurally-disjoint sibling children. Substitutions accumulated from
// every connector's response therefore compose without a cross-connector
// disjointness pass.
class PushdownOfferCollector {
 public:
  void collect(NodeCP root) {
    walk(root, /*isRoot=*/true);
  }

  std::vector<PushdownOffer> release() && {
    return std::move(offers_);
  }

 private:
  ConnectorSet walk(NodeCP node, bool isRoot) {
    if (node->is(NodeType::kScan)) {
      const auto* schemaTable = node->as<Scan>()->baseTable()->schemaTable;
      VELOX_CHECK_NOT_NULL(schemaTable);
      return {std::string(schemaTable->connectorId())};
    }

    auto inputs = node->inputs();
    std::vector<ConnectorSet> connectors;
    connectors.reserve(inputs.size());
    ConnectorSet merged;
    for (NodeCP input : inputs) {
      connectors.push_back(walk(input, /*isRoot=*/false));
      merged.insert(connectors.back().begin(), connectors.back().end());
    }

    if (merged.size() > 1) {
      // Multi-connector cut point: each single-connector child has just
      // become the top of a maximal subtree — record it as an offer.
      for (size_t i = 0; i < inputs.size(); ++i) {
        if (connectors.at(i).size() == 1) {
          maybeAdd(inputs[i], *connectors.at(i).begin());
        }
      }
    } else if (isRoot && merged.size() == 1) {
      // Single-connector plan root: offer the whole plan.
      maybeAdd(node, *merged.begin());
    }
    return merged;
  }

  // A bare Scan already runs on its connector: the connector cannot return
  // it as a root (`checkPushdownRoots` rejects that) and returning anything
  // else would fall outside the subtree, so skip the round-trip.
  void maybeAdd(NodeCP subtree, const std::string& connectorId) {
    if (subtree->is(NodeType::kScan)) {
      return;
    }
    offers_.push_back(PushdownOffer{subtree, connectorId});
  }

  std::vector<PushdownOffer> offers_;
};

} // namespace

folly::coro::Task<NodeCP> ConnectorPushdownPass::run(
    NodeCP root,
    Builder& builder,
    const Schema& schema) {
  PushdownOfferCollector collector;
  collector.collect(root);
  auto offers = std::move(collector).release();

  // TODO: Fan out `co_pushdown` calls with `folly::coro::collectAllRange` so
  // per-connector RPCs run concurrently. Sequential for now; cheap to revisit
  // while the API is EXPERIMENTAL.
  SubstitutionMap substitutions;
  for (const auto& offer : offers) {
    auto metadata = ConnectorMetadataRegistry::tryGet(offer.connectorId);
    VELOX_CHECK_NOT_NULL(
        metadata,
        "Connector referenced by plan is not registered: {}",
        offer.connectorId);
    auto* pushdown = dynamic_cast<const ConnectorPushdown*>(metadata.get());
    if (pushdown == nullptr) {
      continue;
    }
    auto pushdowns = co_await pushdown->co_pushdown(*offer.subtree);
    checkPushdownRoots(pushdowns, offer.subtree, offer.connectorId);
    for (const auto& pushdownRoot : pushdowns) {
      substitutions.emplace(
          pushdownRoot.root, buildSyntheticScan(pushdownRoot, builder, schema));
    }
  }
  if (substitutions.empty()) {
    co_return root;
  }
  co_return root->rewrite(builder, [&](NodeCP node) {
    auto it = substitutions.find(node);
    return it != substitutions.end() ? it->second : node;
  });
}

} // namespace facebook::axiom::optimizer::v2
