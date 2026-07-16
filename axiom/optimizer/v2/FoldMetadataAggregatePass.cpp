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

#include "axiom/optimizer/v2/FoldMetadataAggregatePass.h"

#include "folly/container/F14Map.h"
#include "folly/container/F14Set.h"
#include "folly/coro/BlockingWait.h"

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/v2/NodeRewriter.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

using logical_plan::SpecialAggregateKind;

class Folder : public NodeRewriter<NoContext> {
 public:
  Folder(
      Builder& builder,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator,
      ScanHandleCache& scanHandles)
      : NodeRewriter(builder),
        session_(session),
        evaluator_(evaluator),
        scanHandles_(scanHandles) {}

 protected:
  NodeCP rewriteAggregate(AggregateCP node, NoContext& context) override {
    bool anyMetadata = false;
    bool allMetadata = true;
    for (const optimizer::Aggregate* aggregate : node->aggregates()) {
      if (aggregate->specialKind().has_value()) {
        anyMetadata = true;
      } else {
        allMetadata = false;
      }
    }
    if (!anyMetadata) {
      return NodeRewriter::rewriteAggregate(node, context);
    }

    // The fold produces one Values row per group, so it applies only when the
    // whole aggregation is metadata aggregates directly over a Scan.
    if (allMetadata && node->input()->is(NodeType::kScan)) {
      if (NodeCP folded = tryFold(node)) {
        return folded;
      }
    }
    return replaceWithFallbacks(node, context);
  }

 private:
  // Folds an aggregation of only metadata aggregates directly over a Scan into
  // a Values node. The caller guarantees that shape. Returns nullptr if the
  // grouping keys are not plain columns or the connector declines.
  NodeCP tryFold(AggregateCP node) {
    const auto* scan = node->input()->as<Scan>();
    const auto* layout =
        scan->baseTable()->schemaTable->columnGroups[0]->layout;

    // Grouping keys and null-count arguments must be base-table columns so they
    // can be named for the connector (via their SchemaTable column); the
    // connector decides (via a nullopt result) whether it can group by them.
    std::vector<std::string> groupingColumns;
    groupingColumns.reserve(node->groupingKeys().size());
    folly::F14FastSet<std::string_view> seen;
    for (ExprCP key : node->groupingKeys()) {
      if (!key->isColumn()) {
        return nullptr;
      }
      const auto* column = key->as<Column>();
      ColumnCP schemaColumn = column->schemaColumn();
      VELOX_CHECK_NOT_NULL(
          schemaColumn,
          "Metadata aggregate grouping key has no schema column: {}",
          column->name());
      VELOX_CHECK(
          seen.insert(schemaColumn->name()).second,
          "Duplicate grouping column in a metadata aggregate: {}",
          schemaColumn->name());
      groupingColumns.emplace_back(schemaColumn->name());
    }

    // Distinct columns whose null counts are needed, and, per aggregate, the
    // index into that list (-1 for a row count, which needs no column). Two
    // aggregates over the same column share one entry.
    std::vector<std::string> nullCountColumns;
    // Keyed by a view into the schema columns' own names (stable for the
    // schema's lifetime), not into nullCountColumns below.
    folly::F14FastMap<std::string_view, int32_t> nullCountColumnIndex;
    std::vector<int32_t> nullCountIndex;
    nullCountIndex.reserve(node->aggregates().size());
    for (const optimizer::Aggregate* aggregate : node->aggregates()) {
      if (*aggregate->specialKind() ==
          SpecialAggregateKind::kMetadataRowCount) {
        nullCountIndex.push_back(-1);
        continue;
      }
      // Reached only for null / non-null count, which take exactly one input.
      if (!aggregate->args()[0]->isColumn()) {
        return nullptr;
      }
      const auto* argColumn = aggregate->args()[0]->as<Column>();
      ColumnCP schemaColumn = argColumn->schemaColumn();
      VELOX_CHECK_NOT_NULL(
          schemaColumn,
          "Metadata aggregate argument has no schema column: {}",
          argColumn->name());
      auto [it, inserted] = nullCountColumnIndex.try_emplace(
          schemaColumn->name(), static_cast<int32_t>(nullCountColumns.size()));
      if (inserted) {
        nullCountColumns.emplace_back(schemaColumn->name());
      }
      nullCountIndex.push_back(it->second);
    }

    const ScanHandle& handle =
        scanHandles_.getOrBuild(*scan, session_, evaluator_);
    auto connectorSession =
        session_.toConnectorSession(layout->connector()->connectorId());
    auto result = folly::coro::blockingWait(layout->co_metadataCounts(
        std::move(connectorSession),
        handle.tableHandle,
        std::move(groupingColumns),
        std::move(nullCountColumns),
        handle.filterConjuncts));
    if (!result.has_value()) {
      return nullptr;
    }

    return makeValues(node, nullCountIndex, *result);
  }

  // Builds the Values node holding one row per group: the grouping-key values
  // followed by each aggregate's count.
  NodeCP makeValues(
      AggregateCP node,
      const std::vector<int32_t>& nullCountIndex,
      const std::vector<connector::MetadataCountGroup>& groups) {
    std::vector<velox::Variant> rows;
    rows.reserve(groups.size());
    for (const auto& group : groups) {
      VELOX_CHECK_EQ(group.key.size(), node->groupingKeys().size());
      group.checkConsistency();
      std::vector<velox::Variant> row;
      row.reserve(node->outputColumns().size());
      for (const auto& keyValue : group.key) {
        row.push_back(keyValue);
      }
      for (size_t i = 0; i < node->aggregates().size(); ++i) {
        row.push_back(
            velox::Variant(count(
                *node->aggregates()[i]->specialKind(),
                nullCountIndex[i],
                group)));
      }
      rows.push_back(velox::Variant::row(std::move(row)));
    }

    // A global (ungrouped) aggregate must return exactly one row even when no
    // partitions match: count(*) over an empty input is 0, not zero rows.
    if (rows.empty() && node->groupingKeys().empty()) {
      std::vector<velox::Variant> row;
      row.reserve(node->aggregates().size());
      for (size_t i = 0; i < node->aggregates().size(); ++i) {
        row.push_back(velox::Variant(int64_t{0}));
      }
      rows.push_back(velox::Variant::row(std::move(row)));
    }

    return builder().make<Values>(Values::Key{
        .source = nullptr,
        .rows = registerVariant(velox::Variant::array(std::move(rows))),
        .outputColumns = node->outputColumns()});
  }

  static int64_t count(
      SpecialAggregateKind kind,
      int32_t nullCountIndex,
      const connector::MetadataCountGroup& group) {
    switch (kind) {
      case SpecialAggregateKind::kMetadataRowCount:
        return group.numRows;
      case SpecialAggregateKind::kMetadataNonNullCount:
        return group.numRows - group.numNulls.at(nullCountIndex);
      case SpecialAggregateKind::kMetadataNullCount:
        return group.numNulls.at(nullCountIndex);
    }
    VELOX_UNREACHABLE(
        "Unhandled SpecialAggregateKind: {}", static_cast<int>(kind));
  }

  // Rebuilds the aggregation with each metadata aggregate replaced by its
  // fallback.
  NodeCP replaceWithFallbacks(AggregateCP node, NoContext& context) {
    NodeCP newInput = rewrite(node->input(), context);
    AggregateCallVector aggregates;
    aggregates.reserve(node->aggregates().size());
    for (const optimizer::Aggregate* aggregate : node->aggregates()) {
      if (!aggregate->specialKind().has_value()) {
        aggregates.push_back(aggregate);
        continue;
      }
      VELOX_USER_CHECK_NOT_NULL(
          aggregate->fallback(),
          "Aggregate cannot be answered from metadata and has no fallback: {}",
          aggregate->name());
      aggregates.push_back(aggregate->fallback());
    }

    return builder().make<Aggregate>(Aggregate::Key{
        .input = newInput,
        .groupingKeys = node->groupingKeys(),
        .aggregates = std::move(aggregates),
        .outputColumns = node->outputColumns(),
        .step = node->step(),
        .groupId = node->groupId(),
        .globalGroupingSets = node->globalGroupingSets()});
  }

  const OptimizerSession& session_;
  velox::core::ExpressionEvaluator& evaluator_;
  ScanHandleCache& scanHandles_;
};

} // namespace

NodeCP FoldMetadataAggregatePass::run(
    NodeCP root,
    Builder& builder,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator,
    ScanHandleCache& scanHandles) {
  Folder folder(builder, session, evaluator, scanHandles);
  return folder.rewrite(root);
}

} // namespace facebook::axiom::optimizer::v2
