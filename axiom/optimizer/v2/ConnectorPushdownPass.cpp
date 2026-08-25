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

#include <algorithm>

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
using NodeClosure = folly::F14FastSet<NodeCP>;
using ReplacementByRoot = folly::F14FastMap<NodeCP, NodeCP>;

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

NodeClosure collectStrictDescendants(NodeCP node) {
  NodeClosure visited;
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

NodeClosure collectInclusiveClosure(NodeCP node) {
  auto closure = collectStrictDescendants(node);
  closure.insert(node);
  return closure;
}

void validateReturnedRoots(
    const std::vector<PushdownRoot>& acceptedRoots,
    NodeCP offeredSubtree,
    std::string_view connectorId) {
  const auto offeredDescendants = collectStrictDescendants(offeredSubtree);
  for (const auto& acceptedRoot : acceptedRoots) {
    VELOX_CHECK_NOT_NULL(
        acceptedRoot.root, "Pushdown root is null: connector {}", connectorId);
    VELOX_CHECK_NOT_NULL(
        acceptedRoot.table,
        "Pushdown virtual table is null: connector {}, node type {}",
        connectorId,
        acceptedRoot.root->nodeType());
    VELOX_CHECK(
        acceptedRoot.root == offeredSubtree ||
            offeredDescendants.contains(acceptedRoot.root),
        "Pushdown root is outside the offered subtree: connector {}, node type {}",
        connectorId,
        acceptedRoot.root->nodeType());
    VELOX_CHECK(
        acceptedRoot.root->nodeType() != NodeType::kScan,
        "Pushdown root cannot be a Scan: connector {}",
        connectorId);
    VELOX_CHECK(
        acceptedRoot.root->nodeType() != NodeType::kTableWrite,
        "Pushdown root cannot be a write: connector {}",
        connectorId);
    VELOX_CHECK(
        acceptedRoot.root->requiredStates().empty(),
        "Pushdown root cannot depend on unbound recursive state: connector {}, node type {}",
        connectorId,
        acceptedRoot.root->nodeType());
  }
  std::vector<NodeClosure> closuresPerRoot;
  closuresPerRoot.reserve(acceptedRoots.size());
  for (const auto& acceptedRoot : acceptedRoots) {
    closuresPerRoot.push_back(collectInclusiveClosure(acceptedRoot.root));
  }
  for (size_t i = 0; i < acceptedRoots.size(); ++i) {
    for (size_t j = i + 1; j < acceptedRoots.size(); ++j) {
      VELOX_CHECK(
          acceptedRoots[i].root != acceptedRoots[j].root,
          "Pushdown root was returned twice: connector {}, node type {}",
          connectorId,
          acceptedRoots[i].root->nodeType());
      VELOX_CHECK(
          !closuresPerRoot[i].contains(acceptedRoots[j].root) &&
              !closuresPerRoot[j].contains(acceptedRoots[i].root),
          "Pushdown roots cannot be nested: connector {}, node types {} and {}",
          connectorId,
          acceptedRoots[i].root->nodeType(),
          acceptedRoots[j].root->nodeType());
    }
  }
}

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
  VELOX_CHECK(
      !layouts.empty(),
      "Pushdown virtual table has no layouts: connector {}",
      connectorId);
  const connector::TableLayout* readLayout = layouts.front();
  VELOX_CHECK_NOT_NULL(
      readLayout,
      "Pushdown virtual table has a null first layout: connector {}",
      connectorId);
  VELOX_CHECK(
      readLayout->supportsScan(),
      "Pushdown virtual table first layout does not support scans: connector {}, layout {}",
      connectorId,
      readLayout->label());
  folly::F14FastSet<std::string_view> readColumnNames;
  for (const connector::Column* column : readLayout->columns()) {
    VELOX_CHECK_NOT_NULL(column);
    readColumnNames.insert(column->name());
  }
  for (uint32_t i = 0; i < tableType->size(); ++i) {
    VELOX_CHECK(
        readColumnNames.contains(tableType->nameOf(i)),
        "Pushdown virtual table first layout is missing a visible column: connector {}, layout {}, column {}",
        connectorId,
        readLayout->label(),
        tableType->nameOf(i));
  }
  VELOX_CHECK(
      std::ranges::all_of(
          layouts,
          [&](const connector::TableLayout* layout) {
            return layout != nullptr && layout->connectorId() == connectorId;
          }),
      "Pushdown virtual table belongs to a different connector: expected {}",
      connectorId);

  const auto* schemaTable = schema.adoptConnectorTable(pushdownRoot.table);
  auto* baseTable = make<BaseTable>();
  baseTable->cname = queryCtx()->newName("t");
  baseTable->schemaTable = schemaTable;
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
  ConnectorIdSet connectorIds;
  bool containsWrite{false};
  std::vector<ConnectorSubtreeOffer> maximalOffers;
};

// Collects offers bottom-up. An eligible node subsumes its inputs' offers;
// otherwise, their maximal offers propagate toward the root.
NodeConnectorInfo collectConnectorInfo(NodeCP node) {
  NodeConnectorInfo info;
  if (node->is(NodeType::kScan)) {
    const auto* schemaTable = node->as<Scan>()->baseTable()->schemaTable;
    VELOX_CHECK_NOT_NULL(schemaTable);
    info.connectorIds.insert(std::string(schemaTable->connectorId()));
    return info;
  }

  info.containsWrite = node->is(NodeType::kTableWrite);
  for (NodeCP input : node->inputs()) {
    auto inputInfo = collectConnectorInfo(input);
    info.connectorIds.insert(
        inputInfo.connectorIds.begin(), inputInfo.connectorIds.end());
    info.containsWrite = info.containsWrite || inputInfo.containsWrite;
    for (auto& offer : inputInfo.maximalOffers) {
      info.maximalOffers.push_back(std::move(offer));
    }
  }

  if (!info.containsWrite && info.connectorIds.size() == 1 &&
      node->requiredStates().empty()) {
    info.maximalOffers = {
        ConnectorSubtreeOffer{node, *info.connectorIds.begin()}};
  }
  return info;
}

std::vector<ConnectorSubtreeOffer> collectOffers(NodeCP root) {
  auto info = collectConnectorInfo(root);
  std::vector<ConnectorSubtreeOffer> offers;
  folly::F14FastSet<NodeCP> offeredRoots;
  for (auto& offer : info.maximalOffers) {
    if (offeredRoots.insert(offer.subtree).second) {
      offers.push_back(std::move(offer));
    }
  }
  return offers;
}

} // namespace

NodeCP ConnectorPushdownPass::run(
    NodeCP root,
    Builder& builder,
    const Schema& schema,
    const connector::SchemaResolver& schemaResolver,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator) {
  auto offers = collectOffers(root);

  std::vector<folly::coro::Task<ConnectorPushdownResponse>> tasks;
  tasks.reserve(offers.size());
  for (auto& offer : offers) {
    auto metadata = schemaResolver.findMetadata(offer.connectorId);
    tasks.push_back(requestConnectorPushdown(
        ConnectorPushdownRequest{
            .metadata = std::move(metadata),
            .session = session.toConnectorSession(offer.connectorId),
            .offer = std::move(offer),
        }));
  }

  auto responses =
      folly::coro::blockingWait(folly::coro::collectAllRange(std::move(tasks)));

  ReplacementByRoot replacementByOffer;
  for (const auto& response : responses) {
    if (response.acceptedRoots.empty()) {
      continue;
    }
    validateReturnedRoots(
        response.acceptedRoots,
        response.offer.subtree,
        response.offer.connectorId);
    ReplacementByRoot localReplacements;
    for (const auto& acceptedRoot : response.acceptedRoots) {
      localReplacements.emplace(
          acceptedRoot.root,
          makeReplacementScan(
              acceptedRoot,
              builder,
              schema,
              response.offer.connectorId,
              session,
              evaluator));
    }
    // Confine strict-descendant replacements to the subtree the connector saw.
    NodeSubstitutionRewriter localRewriter{builder, localReplacements};
    replacementByOffer.emplace(
        response.offer.subtree, localRewriter.rewrite(response.offer.subtree));
  }
  if (replacementByOffer.empty()) {
    return root;
  }
  NodeSubstitutionRewriter rewriter{builder, replacementByOffer};
  return rewriter.rewrite(root);
}

} // namespace facebook::axiom::optimizer::v2
