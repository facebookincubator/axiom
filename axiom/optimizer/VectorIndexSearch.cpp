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
#include "axiom/optimizer/VectorIndexSearch.h"

#include "axiom/optimizer/AppendAll.h"
#include "axiom/optimizer/ExprFactory.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/TableAccessHandle.h"
#include "folly/String.h"
#include "folly/coro/BlockingWait.h"

namespace facebook::axiom::optimizer {
namespace {

// A scan of a storage table of a vector index, with its columns by name.
struct StorageScan {
  NodeCP node{nullptr};
  folly::F14FastMap<std::string_view, ColumnCP> columns;
};

// Returns 'columns' and the partition columns of 'index'.
std::vector<std::string> withPartitions(
    const connector::VectorIndex& index,
    std::vector<std::string> columns) {
  const auto& partitionColumns = index.partitionColumns->names();
  columns.insert(
      columns.end(), partitionColumns.begin(), partitionColumns.end());
  return columns;
}

class Planner {
 public:
  Planner(
      Builder& builder,
      const Schema& schema,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator)
      : builder_{builder},
        exprs_{builder},
        schema_{schema},
        session_{session},
        evaluator_{evaluator} {}

  // Appends to 'sources' each partition column of 'table' that a column of
  // 'scan', a storage table of 'index', stores, and that column to 'targets'.
  static void mapColumns(
      const BaseTable& table,
      const connector::VectorIndex& index,
      const StorageScan& scan,
      ColumnVector& sources,
      ExprVector& targets) {
    for (ColumnCP column : table.columns) {
      const std::string_view name = column->schemaColumn()->name();
      if (!index.partitionColumns->containsChild(name)) {
        continue;
      }
      const auto it = scan.columns.find(name);
      if (it != scan.columns.end()) {
        sources.push_back(column);
        targets.push_back(it->second);
      }
    }
  }

  // Returns a scan of 'storageTable', a storage table of 'index', of
  // 'columns'. 'filters', over the columns of 'table', the table of 'index',
  // apply to the scan's columns that store them (see mapColumns).
  StorageScan scanStorage(
      const BaseTable& table,
      const connector::VectorIndex& index,
      const SchemaTableName& storageTable,
      const std::vector<std::string>& columns,
      const ExprVector& filters) {
    const auto& connectorId = table.schemaTable->connectorId();
    const auto* schemaTable = schema_.findTable(connectorId, storageTable);
    VELOX_CHECK_NOT_NULL(
        schemaTable, "Vector index storage table not found: {}", storageTable);
    auto* baseTable = make<BaseTable>(queryCtx()->newName("t"), schemaTable);

    StorageScan scan;
    ColumnVector outputColumns;
    for (const auto& name : columns) {
      const Name nameInTable = toName(name);
      ColumnCP column = baseTable->addColumn(
          nameInTable, queryCtx()->newName(nameInTable), /*alias=*/nullptr);
      outputColumns.push_back(column);
      scan.columns.emplace(nameInTable, column);
    }
    scan.node = builder_.make<Scan>(
        Scan::Key{.baseTable = baseTable, .outputColumns = outputColumns});
    if (filters.empty()) {
      return scan;
    }

    ColumnVector sources;
    ExprVector targets;
    mapColumns(table, index, scan, sources, targets);
    const auto predicates = exprs_.substitute(filters, sources, targets);
    for (ExprCP predicate : predicates) {
      // The filters apply to each storage table, so a filter must select the
      // same partitions each time.
      VELOX_USER_CHECK(
          !predicate->containsNonDeterministic(),
          "A vector search may filter its table only by deterministic conditions: {}",
          table.schemaTable->name());
      predicate->columns().forEach<Column>([&](ColumnCP column) {
        VELOX_USER_CHECK(
            column->relation() != &table,
            "A vector search may filter its table only on partition columns: {}",
            table.schemaTable->name());
      });
    }
    scan.node = builder_.make<Filter>({scan.node, predicates});
    return scan;
  }

  // Returns a scan of the codecs of 'index', of each codec, its id and its
  // partition, with 'filters' applied.
  StorageScan scanCodecs(
      const BaseTable& table,
      const connector::VectorIndex& index,
      const ExprVector& filters) {
    return scanStorage(
        table,
        index,
        index.codecTable,
        withPartitions(
            index,
            {std::string(connector::VectorIndex::kCodec),
             std::string(connector::VectorIndex::kCodecId)}),
        filters);
  }

  // See VectorIndexSearch::plan.
  NodeCP planVectorIndexSearch(const VectorSearch* node, NodeCP probe) {
    // Each column the build side produces, and the filters on its rows, as
    // expressions over the columns of 'build', walking down from the search to
    // the scan.
    folly::F14FastMap<ColumnCP, ExprCP> projected;
    for (ColumnCP column : node->build()->outputColumns()) {
      projected.emplace(column, column);
    }
    ExprVector filters;
    NodeCP build = node->build();
    while (build->is(NodeType::kProject) || build->is(NodeType::kFilter)) {
      if (build->is(NodeType::kFilter)) {
        const auto* filter = build->as<Filter>();
        appendAll(filters, filter->predicates());
        build = filter->input();
        continue;
      }
      const auto* project = build->as<Project>();
      ColumnVector sources = project->outputColumns();
      const ExprVector& targets = project->exprs();
      for (auto& [column, expr] : projected) {
        expr = exprs_.substitute(expr, sources, targets);
      }
      filters = exprs_.substitute(filters, sources, targets);
      build = project->input();
    }
    VELOX_USER_CHECK(
        build->is(NodeType::kScan),
        "A vector search must search a table, optionally filtered on its partition columns");
    const auto& table = *build->as<Scan>()->baseTable();

    const auto resolve = [&](ColumnCP column) -> ExprCP {
      const auto it = projected.find(column);
      VELOX_CHECK(
          it != projected.end(),
          "Not a column of the vector search's build side: {}",
          column->toString());
      return it->second;
    };
    ExprCP embedding = resolve(node->buildIndex());
    VELOX_USER_CHECK(
        embedding->isColumn() && embedding->as<Column>()->relation() == &table,
        "A vector search must search a column of its table: {}",
        table.schemaTable->name());
    const auto index = findIndex(
        table,
        embedding->as<Column>()->schemaColumn()->name(),
        node->metric(),
        node->index());
    return planSearch(node, probe, table, index, filters, resolve);
  }

  // Returns the vector index of 'table' that a search of 'column' by 'metric'
  // reads: the one 'hint' names, or else the only one on 'column' with
  // 'metric'. Fails if the hinted index is missing or is on another column or
  // with another metric, and without a hint, if no index or more than one
  // matches.
  static connector::VectorIndexPtr findIndex(
      const BaseTable& table,
      std::string_view column,
      velox::common::vectorindex::VectorIndexMetric metric,
      std::optional<Name> hint) {
    const auto metricName =
        velox::common::vectorindex::VectorIndexMetricName::toName(metric);
    if (hint.has_value()) {
      auto index =
          table.schemaTable->connectorTable->findVectorIndex(hint.value());
      VELOX_USER_CHECK_NOT_NULL(
          index,
          "Vector index hint names no index of the table: {} on {}",
          hint.value(),
          table.schemaTable->name());
      VELOX_USER_CHECK_EQ(
          index->embeddingColumn,
          column,
          "Vector index hint must name an index on the searched column: {} on {}",
          hint.value(),
          table.schemaTable->name());
      VELOX_USER_CHECK_EQ(
          velox::common::vectorindex::VectorIndexMetricName::toName(
              index->properties.metric),
          metricName,
          "Vector index hint must name an index with the search's metric: {} on {}",
          hint.value(),
          table.schemaTable->name());
      return index;
    }

    std::vector<connector::VectorIndexPtr> found;
    std::vector<std::string_view> names;
    for (const auto& index :
         table.schemaTable->connectorTable->vectorIndexes()) {
      if (index->embeddingColumn == column &&
          index->properties.metric == metric) {
        found.push_back(index);
        names.push_back(index->name);
      }
    }
    VELOX_USER_CHECK(
        !found.empty(),
        "Vector search requires a vector index on {}.{} using {}",
        table.schemaTable->name(),
        column,
        metricName);
    VELOX_USER_CHECK_EQ(
        found.size(),
        1,
        "Found multiple matching vector indexes. Use the index hint to specify which one to use: {}.{} using {}, indexes: {}",
        table.schemaTable->name(),
        column,
        metricName,
        folly::join(", ", names));
    return found.front();
  }

  // Fails unless 'index' covers every partition of 'table' that 'filters'
  // select (see connector::TableLayout::co_checkVectorIndexCoverage()).
  void checkCoverage(
      const BaseTable& table,
      const connector::VectorIndexPtr& index,
      const ExprVector& filters) {
    // A filter the connector rejects widens the partitions checked, which only
    // makes the check stricter.
    ExprVector rejected;
    const auto handle = TableAccessHandle::buildScan(
        table,
        /*outputColumns=*/{},
        filters,
        [](ColumnCP) { return std::vector<velox::common::Subfield>{}; },
        session_,
        evaluator_,
        rejected);
    const auto* layout = table.schemaTable->connectorTable->primaryLayout();
    folly::coro::blockingWait(layout->co_checkVectorIndexCoverage(
        session_.context()->sessionFor(layout->connectorId()),
        handle.tableHandle,
        evaluator_,
        index));
  }

  // Returns 'expr', over the columns of 'table', with 'sources' replaced by
  // 'targets'. Fails if it still reads a column of 'table': one that a search
  // through 'index' does not return.
  ExprCP substituteIndexColumns(
      ExprCP expr,
      const BaseTable& table,
      const connector::VectorIndex& index,
      const ColumnVector& sources,
      const ExprVector& targets) {
    ExprCP result = exprs_.substitute(expr, sources, targets);
    result->columns().forEach<Column>([&](ColumnCP column) {
      VELOX_USER_CHECK(
          column->relation() != &table,
          "Reading a column a vector index does not return is not supported yet: {}.{} through index {}",
          table.schemaTable->name(),
          column->schemaColumn()->name(),
          index.name);
    });
    return result;
  }

  // Returns 'node' with its outputs computed from 'plan', the plan of its
  // search: the probe's columns pass through, the score is 'score', and
  // 'buildColumn' computes each column of the build side.
  NodeCP projectSearchOutputs(
      const VectorSearch* node,
      NodeCP plan,
      ColumnCP score,
      const std::function<ExprCP(ColumnCP)>& buildColumn) {
    const PlanObjectSet probeColumns =
        PlanObjectSet::fromObjects(node->probe()->outputColumns());
    ExprVector exprs;
    exprs.reserve(node->outputColumns().size());
    for (ColumnCP source : node->sourceColumns()) {
      if (source == node->score()) {
        exprs.push_back(score);
      } else if (probeColumns.contains(source)) {
        exprs.push_back(source);
      } else {
        exprs.push_back(buildColumn(source));
      }
    }
    return builder_.make<Project>(
        {plan, std::move(exprs), node->outputColumns()});
  }

  // Returns the 'k' rows of 'searched' with the highest 'score' for each
  // 'queryId'. A larger score is nearer for every metric.
  NodeCP
  keepNearest(NodeCP searched, ColumnCP queryId, ColumnCP score, int64_t k) {
    ColumnCP rank = Column::create("__rank", Value(toType(velox::BIGINT())));
    ColumnVector windowOutputs = searched->outputColumns();
    windowOutputs.push_back(rank);
    NodeCP ranked = builder_.make<Window>({
        .input = searched,
        .functions = {Window::Function{
            exprs_.makeRowNumber(),
            Frame::toCurrentRow(),
            /*ignoreNulls=*/false}},
        .partitionKeys = {queryId},
        .orderKeys = {score},
        .orderTypes = {OrderType::kDescNullsLast},
        .outputColumns = std::move(windowOutputs),
    });
    return builder_.make<Filter>(
        {ranked,
         {exprs_.makeLessThanOrEqual(
             rank,
             exprs_.makeLiteral(velox::Variant(k), toType(velox::BIGINT())))}});
  }

  // Plans a search of 'table' through 'index': each probe row is assigned
  // to the 'numProbes' clusters nearest to it of the codec of every partition
  // the filters select, those clusters' local indexes are searched for its
  // 'k' nearest rows, and the 'k' nearest of all are kept:
  //
  //   Filter: row_number <= k, by query_id ordered by score DESC
  //     VectorSearch: (partitions, codec_id, cluster_id) =, local_index, k
  //       VectorSearch: codec, k = numProbes
  //         AssignUniqueId query_id
  //           <probe>
  //         Filter: <filters> / Scan <codec table>
  //       Filter: <filters> / Scan <index table>
  NodeCP planSearch(
      const VectorSearch* node,
      NodeCP probe,
      const BaseTable& table,
      const connector::VectorIndexPtr& index,
      const ExprVector& filters,
      const std::function<ExprCP(ColumnCP)>& resolve) {
    VELOX_USER_CHECK(
        node->joinType() == velox::core::JoinType::kInner,
        "A vector search through a vector index supports only an INNER join for now");
    VELOX_USER_CHECK(
        node->probeKeys().empty(),
        "A vector search through a vector index does not support keys yet");
    // The default of the num_probes hint (see logical_plan::VectorSearchNode).
    const int64_t numProbes =
        node->numProbes().value_or((index->properties.numClusters + 9) / 10);

    const std::string codecIdName{connector::VectorIndex::kCodecId};
    const std::string clusterIdName{connector::VectorIndex::kClusterId};
    const std::string localIndexName{connector::VectorIndex::kLocalIndex};
    StorageScan codecs = scanCodecs(table, *index, filters);
    StorageScan localIndexes = scanStorage(
        table,
        *index,
        index->indexTable,
        withPartitions(*index, {codecIdName, clusterIdName, localIndexName}),
        filters);
    checkCoverage(table, index, filters);

    const Value bigint{toType(velox::BIGINT())};
    const Value real{toType(velox::REAL())};
    ColumnCP queryId = Column::create("__query_id", bigint);
    NodeCP queries = builder_.make<AssignUniqueId>({probe, queryId});

    // Each query is assigned to clusters of the codec of every partition.
    ColumnCP codec = codecs.columns.at(connector::VectorIndex::kCodec);
    ColumnCP clusterId = Column::create("__cluster_id", bigint);
    ColumnCP clusterScore = Column::create("__cluster_score", real);
    ColumnVector assignOutputs = queries->outputColumns();
    for (ColumnCP column : codecs.node->outputColumns()) {
      if (column != codec) {
        assignOutputs.push_back(column);
      }
    }
    assignOutputs.push_back(clusterId);
    assignOutputs.push_back(clusterScore);
    NodeCP assigned = builder_.make<VectorSearch>({
        .probe = queries,
        .build = codecs.node,
        .joinType = velox::core::JoinType::kInner,
        .probeEmbedding = node->probeEmbedding(),
        .buildIndex = codec,
        .metric = index->properties.metric,
        .k = numProbes,
        .id = clusterId,
        .score = clusterScore,
        .outputColumns = assignOutputs,
        .sourceColumns = assignOutputs,
    });

    // Each assignment searches the local indexes of its cluster.
    ExprVector probeKeys;
    ExprVector buildKeys;
    for (const auto& name : index->partitionColumns->names()) {
      probeKeys.push_back(codecs.columns.at(name));
      buildKeys.push_back(localIndexes.columns.at(name));
    }
    probeKeys.push_back(codecs.columns.at(codecIdName));
    buildKeys.push_back(localIndexes.columns.at(codecIdName));
    probeKeys.push_back(clusterId);
    buildKeys.push_back(localIndexes.columns.at(clusterIdName));
    ColumnCP matchId = Column::create("__match_id", bigint);
    ColumnCP score = Column::create("__score", real);
    ColumnVector searchOutputs = assigned->outputColumns();
    searchOutputs.push_back(matchId);
    searchOutputs.push_back(score);
    NodeCP searched = builder_.make<VectorSearch>({
        .probe = assigned,
        .build = localIndexes.node,
        .joinType = velox::core::JoinType::kInner,
        .probeKeys = std::move(probeKeys),
        .buildKeys = std::move(buildKeys),
        .probeEmbedding = node->probeEmbedding(),
        .buildIndex = localIndexes.columns.at(localIndexName),
        .metric = index->properties.metric,
        .k = node->k(),
        .searchOptions = node->searchOptions(),
        .id = matchId,
        .score = score,
        .outputColumns = searchOutputs,
        .sourceColumns = searchOutputs,
    });

    NodeCP nearest = keepNearest(searched, queryId, score, node->k());

    // The table's key is the match's id; its partition columns are the
    // codecs'.
    ColumnVector sources;
    ExprVector targets;
    mapColumns(table, *index, codecs, sources, targets);
    for (ColumnCP column : table.columns) {
      if (column->schemaColumn()->name() == index->keyColumn) {
        sources.push_back(column);
        targets.push_back(matchId);
      }
    }
    return projectSearchOutputs(node, nearest, score, [&](ColumnCP column) {
      return substituteIndexColumns(
          resolve(column), table, *index, sources, targets);
    });
  }

 private:
  Builder& builder_;
  ExprFactory exprs_;
  const Schema& schema_;
  const OptimizerSession& session_;
  velox::core::ExpressionEvaluator& evaluator_;
};

} // namespace

// static
NodeCP VectorIndexSearch::plan(
    const VectorSearch& search,
    NodeCP probe,
    Builder& builder,
    const Schema& schema,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator) {
  return Planner{builder, schema, session, evaluator}.planVectorIndexSearch(
      &search, probe);
}

} // namespace facebook::axiom::optimizer
