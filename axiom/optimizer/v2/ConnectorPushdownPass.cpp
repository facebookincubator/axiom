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
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/v2/NodeRewriter.h"
#include "axiom/optimizer/v2/ScanHandle.h"
#include "folly/container/F14Map.h"
#include "folly/container/F14Set.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Collect.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

using connector::PushdownRoot;

using ConnectorIdSet = folly::F14FastSet<std::string>;
using NodeSet = folly::F14FastSet<NodeCP>;
using ReplacementByRoot = folly::F14FastMap<NodeCP, NodeCP>;

// Replaces selected nodes while rebuilding only their ancestor paths.
class NodeSubstitutionRewriter : public NodeRewriter<> {
 public:
  NodeSubstitutionRewriter(
      Builder& builder,
      const ReplacementByRoot& replacementByRoot)
      : NodeRewriter<>{builder}, replacementByRoot_{replacementByRoot} {}

  using NodeRewriter<>::rewrite;

  NodeCP rewrite(NodeCP node, NoContext& context) override {
    auto it = replacementByRoot_.find(node);
    return it != replacementByRoot_.end()
        ? it->second
        : NodeRewriter<>::rewrite(node, context);
  }

 private:
  const ReplacementByRoot& replacementByRoot_;
};

// Returns `node` and all nodes below it.
NodeSet collectSubtreeNodes(NodeCP node) {
  NodeSet visited;
  std::vector<NodeCP> stack{node};
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

// Validates that accepted roots belong to the offer, are eligible for
// replacement, and do not overlap one another.
void validateReturnedRoots(
    const std::vector<PushdownRoot>& acceptedRoots,
    NodeCP offeredSubtree,
    std::string_view connectorId) {
  const auto offeredNodes = collectSubtreeNodes(offeredSubtree);
  NodeSet acceptedRootSet;
  for (const auto& acceptedRoot : acceptedRoots) {
    VELOX_CHECK_NOT_NULL(
        acceptedRoot.root, "Pushdown root is null: connector {}", connectorId);
    VELOX_CHECK_NOT_NULL(
        acceptedRoot.table,
        "Pushdown virtual table is null: connector {}, node type {}",
        connectorId,
        acceptedRoot.root->nodeType());
    VELOX_CHECK(
        offeredNodes.contains(acceptedRoot.root),
        "Pushdown root is outside the offered subtree: connector {}, node type {}",
        connectorId,
        acceptedRoot.root->nodeType());
    VELOX_CHECK(
        acceptedRoot.root->nodeType() != NodeType::kScan,
        "Pushdown root cannot be a Scan: connector {}",
        connectorId);
    VELOX_CHECK(
        acceptedRoot.root->requiredStates().empty(),
        "Pushdown root cannot depend on unbound recursive state: connector {}, node type {}",
        connectorId,
        acceptedRoot.root->nodeType());
    VELOX_CHECK(
        acceptedRootSet.insert(acceptedRoot.root).second,
        "Pushdown root was returned twice: connector {}, node type {}",
        connectorId,
        acceptedRoot.root->nodeType());
  }

  std::vector<std::pair<NodeCP, NodeCP>> stack{{offeredSubtree, nullptr}};
  while (!stack.empty()) {
    auto [node, acceptedAncestor] = stack.back();
    stack.pop_back();
    if (acceptedRootSet.contains(node)) {
      if (acceptedAncestor != nullptr) {
        VELOX_FAIL(
            "Pushdown roots cannot be nested: connector {}, node types {} and {}",
            connectorId,
            acceptedAncestor->nodeType(),
            node->nodeType());
      }
      acceptedAncestor = node;
    }
    for (NodeCP input : node->inputs()) {
      stack.emplace_back(input, acceptedAncestor);
    }
  }
}

// Builds a scan of the connector's virtual table and restores the output
// column identities expected by consumers of the replaced root.
NodeCP makeReplacementScan(
    const PushdownRoot& pushdownRoot,
    Builder& builder,
    const Schema& schema,
    std::string_view connectorId,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator) {
  const Node* root = pushdownRoot.root;
  VELOX_CHECK_NOT_NULL(root);
  VELOX_CHECK_NOT_NULL(pushdownRoot.table);
  const auto& consumerColumns = root->outputColumns();
  const auto& tableType = pushdownRoot.table->type();
  VELOX_CHECK_EQ(
      tableType->size(),
      consumerColumns.size(),
      "Pushdown virtual table must have one visible column per root output: connector {}, node type {}",
      connectorId,
      root->nodeType());
  for (uint32_t i = 0; i < consumerColumns.size(); ++i) {
    VELOX_CHECK(
        consumerColumns[i]->value().type->equivalent(*tableType->childAt(i)),
        "Pushdown virtual table column type does not match root output: connector {}, position {}, root {}, table {}",
        connectorId,
        i,
        consumerColumns[i]->value().type->toString(),
        tableType->childAt(i)->toString());
  }
  const auto& layouts = pushdownRoot.table->layouts();
  VELOX_CHECK_EQ(
      layouts.size(),
      1,
      "Pushdown virtual table must have exactly one layout: connector {}",
      connectorId);
  const connector::TableLayout* scanLayout = layouts.front();
  VELOX_CHECK_NOT_NULL(
      scanLayout,
      "Pushdown virtual table has a null layout: connector {}",
      connectorId);
  VELOX_CHECK(
      scanLayout->supportsScan(),
      "Pushdown virtual table layout does not support scans: connector {}, layout {}",
      connectorId,
      scanLayout->label());
  folly::F14FastSet<std::string_view> readColumnNames;
  for (const connector::Column* column : scanLayout->columns()) {
    VELOX_CHECK_NOT_NULL(column);
    readColumnNames.insert(column->name());
  }
  for (uint32_t i = 0; i < tableType->size(); ++i) {
    VELOX_CHECK(
        readColumnNames.contains(tableType->nameOf(i)),
        "Pushdown virtual table layout is missing a visible column: connector {}, layout {}, column {}",
        connectorId,
        scanLayout->label(),
        tableType->nameOf(i));
  }
  VELOX_CHECK_EQ(
      scanLayout->connectorId(),
      connectorId,
      "Pushdown virtual table belongs to a different connector: expected {}",
      connectorId);

  const auto* schemaTable = schema.adoptConnectorTable(pushdownRoot.table);
  auto* baseTable = make<BaseTable>(queryCtx()->newName("t"), schemaTable);
  baseTable->filteredCardinality = schemaTable->cardinality;
  baseTable->numRawInputRows = pushdownRoot.table->numRows();

  ColumnVector scanColumns;
  scanColumns.reserve(tableType->size());
  for (uint32_t i = 0; i < tableType->size(); ++i) {
    const Name nameInTable = toName(tableType->nameOf(i));
    const ColumnCP schemaColumn = schemaTable->findColumn(nameInTable);
    VELOX_CHECK_NOT_NULL(schemaColumn);
    auto* column = make<Column>(
        nameInTable,
        baseTable,
        schemaColumn->value(),
        queryCtx()->newName("c"),
        schemaColumn->name());
    baseTable->columns.push_back(column);
    scanColumns.push_back(column);
  }

  ExprVector rejected;
  const ScanHandle* scanHandle = builder.takeScanHandle(
      ScanHandle::build(
          *baseTable,
          scanColumns,
          /*filters=*/{},
          [](ColumnCP) { return std::vector<velox::common::Subfield>{}; },
          session,
          evaluator,
          rejected));
  VELOX_CHECK(
      rejected.empty(),
      "Connector rejected an empty pushdown filter set: connector {}",
      connectorId);
  const auto* scan = builder.make<Scan>(Scan::Key{
      .baseTable = baseTable,
      .outputColumns = scanColumns,
      .scanHandle = scanHandle,
  });

  ExprVector expressions{scanColumns.begin(), scanColumns.end()};
  return builder.make<Project>(Project::Key{
      .input = scan,
      .exprs = std::move(expressions),
      .outputColumns = consumerColumns,
  });
}

struct ConnectorSubtreeOffer {
  NodeCP subtree;
  std::string metadataId;
  std::string connectorId;
};

struct ConnectorPushdownResponse {
  ConnectorSubtreeOffer offer;
  std::vector<PushdownRoot> acceptedRoots;
};

struct ConnectorPushdownRequest {
  std::shared_ptr<connector::ConnectorMetadata> metadata;
  connector::ConnectorSessionPtr session;
  ConnectorSubtreeOffer offer;
};

folly::coro::Task<ConnectorPushdownResponse> requestConnectorPushdown(
    ConnectorPushdownRequest request) {
  auto acceptedRoots = co_await request.metadata->co_pushdown(
      std::move(request.session), *request.offer.subtree);
  co_return ConnectorPushdownResponse{
      .offer = std::move(request.offer),
      .acceptedRoots = std::move(acceptedRoots),
  };
}

struct NodeConnectorInfo {
  ConnectorIdSet metadataIds;
  ConnectorIdSet connectorIds;
  bool pushdownSupported{false};
  std::vector<ConnectorSubtreeOffer> maximalOffers;
};

// Collects offers bottom-up. An eligible node subsumes its inputs' offers;
// otherwise, their maximal offers propagate toward the root.
NodeConnectorInfo collectConnectorInfo(
    NodeCP node,
    const connector::SchemaResolver& schemaResolver) {
  NodeConnectorInfo info;
  if (node->is(NodeType::kScan)) {
    const auto* schemaTable = node->as<Scan>()->baseTable()->schemaTable;
    VELOX_CHECK_NOT_NULL(schemaTable);
    info.metadataIds.insert(schemaTable->metadataId());
    info.connectorIds.insert(std::string(schemaTable->connectorId()));
    info.pushdownSupported =
        schemaResolver.findMetadata(schemaTable->metadataId())
            ->isPushdownSupported();
    return info;
  }

  for (NodeCP input : node->inputs()) {
    auto inputInfo = collectConnectorInfo(input, schemaResolver);
    info.metadataIds.insert(
        inputInfo.metadataIds.begin(), inputInfo.metadataIds.end());
    info.connectorIds.insert(
        inputInfo.connectorIds.begin(), inputInfo.connectorIds.end());
    info.pushdownSupported =
        info.pushdownSupported || inputInfo.pushdownSupported;
    for (auto& offer : inputInfo.maximalOffers) {
      info.maximalOffers.push_back(std::move(offer));
    }
  }

  if (info.metadataIds.size() == 1 && info.connectorIds.size() == 1 &&
      info.pushdownSupported && node->requiredStates().empty()) {
    info.maximalOffers = {ConnectorSubtreeOffer{
        node, *info.metadataIds.begin(), *info.connectorIds.begin()}};
  }
  return info;
}

} // namespace

NodeCP ConnectorPushdownPass::run(
    NodeCP root,
    Builder& builder,
    const Schema& schema,
    const connector::SchemaResolver& schemaResolver,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator) {
  // A TableWrite is a root sink, so only its input can be offered.
  const NodeCP offerRoot =
      root->is(NodeType::kTableWrite) ? root->as<TableWrite>()->input() : root;

  // Partition the plan into maximal connector subtrees. For one connector:
  //
  //  Project [P]
  //    Join
  //      Filter [L]
  //        Scan(c1.t)
  //      Filter [R]
  //        Scan(c1.u)
  //
  // offers = {P}: the whole tree is one maximal connector subtree.
  auto offers =
      std::move(collectConnectorInfo(offerRoot, schemaResolver).maximalOffers);

  // A connector returns a list because one offer can yield disjoint accepted
  // roots. For P, acceptedRoots may be {L, R}, leaving Project and Join.
  std::vector<folly::coro::Task<ConnectorPushdownResponse>> tasks;
  tasks.reserve(offers.size());
  for (auto& offer : offers) {
    auto metadata = schemaResolver.findMetadata(offer.metadataId);
    tasks.push_back(requestConnectorPushdown(
        ConnectorPushdownRequest{
            .metadata = std::move(metadata),
            .session = session.context()->sessionFor(offer.metadataId),
            .offer = std::move(offer),
        }));
  }

  auto responses =
      folly::coro::blockingWait(folly::coro::collectAllRange(std::move(tasks)));

  ReplacementByRoot replacements;
  for (const auto& response : responses) {
    if (response.acceptedRoots.empty()) {
      continue;
    }
    // Enforce optimizer guarantees: each root belongs to its offer, is not a
    // bare Scan, and is disjoint from the other accepted roots.
    validateReturnedRoots(
        response.acceptedRoots,
        response.offer.subtree,
        response.offer.connectorId);
    for (const auto& acceptedRoot : response.acceptedRoots) {
      replacements.emplace(
          acceptedRoot.root,
          makeReplacementScan(
              acceptedRoot,
              builder,
              schema,
              response.offer.connectorId,
              session,
              evaluator));
    }
  }
  if (replacements.empty()) {
    return root;
  }
  // Replace accepted roots with projections over virtual-table scans. The
  // acceptedRoots = {L, R} response above produces:
  //
  //  Project
  //    Join
  //      Project
  //        Scan(c1.virt_t)
  //      Project
  //        Scan(c1.virt_u)
  NodeSubstitutionRewriter rewriter{builder, replacements};
  return rewriter.rewrite(root);
}

} // namespace facebook::axiom::optimizer::v2
