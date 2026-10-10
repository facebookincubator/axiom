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

#include "axiom/optimizer/v2/EmitPass.h"
#include "axiom/optimizer/v2/TableAccessHandle.h"

#include <folly/ScopeGuard.h>
#include <folly/container/F14Set.h>
#include <limits>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/WriteStatsBuilder.h"
#include "axiom/optimizer/v2/EstimateProvider.h"
#include "axiom/optimizer/v2/ExprEmitter.h"
#include "axiom/optimizer/v2/PhysicalProperties.h"
#include "velox/core/FixedPointPlanNodes.h"
#include "velox/core/PlanConsistencyChecker.h"
#include "velox/core/TableWriteTraits.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/expression/ExprConstants.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// Size in bytes of a row of 'columns', or nullopt when the size of a
// variable-width column is unknown.
std::optional<float> rowSize(const ColumnVector& columns) {
  float size{0};
  for (ColumnCP column : columns) {
    const auto& value = column->value();
    if (!value.type->isFixedWidth() && !value.avgSizeInBytes.has_value()) {
      return std::nullopt;
    }
    size += value.byteSize();
  }
  return size;
}

// True when `channels` selects every data column in order (no pruning).
bool isIdentityChannels(
    const QGVector<velox::column_index_t>& channels,
    size_t dataSize) {
  if (channels.size() != dataSize) {
    return false;
  }
  for (size_t i = 0; i < channels.size(); ++i) {
    if (channels[i] != i) {
      return false;
    }
  }
  return true;
}

// Wraps the `channels` children of `full` in a new RowVector of `rowType`,
// so pruned data columns are never propagated downstream.
velox::RowVectorPtr selectChannels(
    const velox::RowVectorPtr& full,
    const velox::RowTypePtr& rowType,
    const QGVector<velox::column_index_t>& channels) {
  std::vector<velox::VectorPtr> children;
  children.reserve(channels.size());
  for (auto channel : channels) {
    children.push_back(full->childAt(channel));
  }
  return std::make_shared<velox::RowVector>(
      full->pool(), rowType, full->nulls(), full->size(), std::move(children));
}

velox::core::FieldAccessTypedExprPtr toFieldAccess(
    const velox::TypePtr& type,
    std::string_view name) {
  return std::make_shared<velox::core::FieldAccessTypedExpr>(
      type, std::string{name});
}

velox::core::FieldAccessTypedExprPtr toFieldAccess(ColumnCP column) {
  return toFieldAccess(toTypePtr(column->value().type), column->outputName());
}

// Emits 'expr' as a `FieldAccessTypedExpr`, failing with a role-specific
// error when it isn't a column reference. Velox `AggregationNode`,
// `WindowNode`, and `OrderByNode` require key / FILTER / aggregate-arg
// positions to be plain column references; non-column expressions must be
// pre-projected.
velox::core::FieldAccessTypedExprPtr toFieldAccess(
    ExprCP expr,
    std::string_view role) {
  VELOX_USER_CHECK(
      expr->is(PlanType::kColumnExpr), "{} must be a column reference", role);
  return toFieldAccess(expr->as<Column>());
}

std::vector<velox::core::FieldAccessTypedExprPtr> toFieldAccessList(
    const ColumnVector& columns) {
  std::vector<velox::core::FieldAccessTypedExprPtr> out;
  out.reserve(columns.size());
  for (ColumnCP column : columns) {
    out.push_back(toFieldAccess(column));
  }
  return out;
}

// Output names of 'columns', in order.
std::vector<std::string> namesOf(const ColumnVector& columns) {
  std::vector<std::string> names;
  names.reserve(columns.size());
  for (ColumnCP column : columns) {
    names.push_back(column->outputName());
  }
  return names;
}

std::vector<velox::core::FieldAccessTypedExprPtr> toFieldAccessList(
    const ExprVector& exprs,
    std::string_view role) {
  std::vector<velox::core::FieldAccessTypedExprPtr> out;
  out.reserve(exprs.size());
  for (ExprCP expr : exprs) {
    out.push_back(toFieldAccess(expr, role));
  }
  return out;
}

// Decodes our `OrderType` into Velox's `(ascending, nullsFirst)` pair.
velox::core::SortOrder toSortOrder(OrderType type) {
  const bool ascending =
      type == OrderType::kAscNullsFirst || type == OrderType::kAscNullsLast;
  const bool nullsFirst =
      type == OrderType::kAscNullsFirst || type == OrderType::kDescNullsFirst;
  return velox::core::SortOrder{ascending, nullsFirst};
}

// Converts IR order keys / types into Velox sorting keys and orders. Shared by
// Sort and TopN emission.
std::pair<
    std::vector<velox::core::FieldAccessTypedExprPtr>,
    std::vector<velox::core::SortOrder>>
toSortingKeys(
    const ExprVector& orderKeys,
    const OrderTypeVector& orderTypes,
    std::string_view role) {
  std::vector<velox::core::SortOrder> sortingOrders;
  sortingOrders.reserve(orderTypes.size());
  for (OrderType type : orderTypes) {
    sortingOrders.push_back(toSortOrder(type));
  }
  return {toFieldAccessList(orderKeys, role), std::move(sortingOrders)};
}

// Column indices in 'rowType' of the columns named by 'named' — each element
// exposes '->name()', so this serves both field-access exprs and connector
// Columns.
template <typename Named>
std::vector<velox::column_index_t> toChannels(
    const velox::RowTypePtr& rowType,
    const std::vector<Named>& named) {
  std::vector<velox::column_index_t> channels;
  channels.reserve(named.size());
  for (const auto& element : named) {
    channels.push_back(rowType->getChildIdx(element->name()));
  }
  return channels;
}

// Partition function spec for connector partitioning 'partitionType' over the
// 'rowType' columns named by 'named'. 'isLocal' selects the within-worker
// (LocalPartition) vs cross-worker (PartitionedOutput) variant.
template <typename Named>
velox::core::PartitionFunctionSpecPtr connectorPartitionSpec(
    const connector::PartitionType& partitionType,
    const velox::RowTypePtr& rowType,
    const std::vector<Named>& named,
    bool isLocal) {
  return partitionType.makeSpec(
      toChannels(rowType, named), /*constants=*/{}, isLocal);
}

// Builds a hash partition function spec over 'outputType' hashing the columns
// named by 'keys'. Shared by remote (Exchange) and local (LocalPartition)
// repartitioning.
std::shared_ptr<velox::exec::HashPartitionFunctionSpec> makeHashPartitionSpec(
    const velox::RowTypePtr& outputType,
    const std::vector<velox::core::FieldAccessTypedExprPtr>& keys) {
  return std::make_shared<velox::exec::HashPartitionFunctionSpec>(
      outputType, toChannels(outputType, keys));
}

// Walks the single-input chain down from 'node' and returns true if any node on
// it requires single-threaded execution (a Values source, a gather). Returns
// false at the first multi-input node (a join), which begins a separate
// pipeline.
bool isSingleThreadedPipeline(const velox::core::PlanNodePtr& node) {
  for (auto current = node;;) {
    if (current->requiresSingleThread()) {
      return true;
    }
    if (current->sources().size() != 1) {
      return false;
    }
    current = current->sources()[0];
  }
}

// Returns the connector partitioning shared by the grouped scans in the
// fragment rooted at 'node'. Exchanges are fragment leaves.
const connector::PartitionType* commonFragmentPartitionType(NodeCP node);

class Emitter {
 public:
  Emitter(
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator,
      const MultiFragmentPlan::Options& options)
      : session_{session},
        evaluator_{evaluator},
        exprEmitter_{evaluator.pool()},
        options_{options} {}

  int32_t hashPartitionCount() const {
    return session_.options().hashStageTasks(options_.maxRemotePartitions);
  }

  velox::core::PlanNodePtr emit(NodeCP node) {
    auto plan = emitNode(node);
    recordPrediction(node, plan);
    return plan;
  }

  velox::core::PlanNodePtr emitNode(NodeCP node) {
    switch (node->nodeType()) {
      case NodeType::kScan:
        return emitScan(*node->as<Scan>());
      case NodeType::kFilter:
        return emitFilter(*node->as<Filter>());
      case NodeType::kProject:
        return emitProject(*node->as<Project>());
      case NodeType::kAggregate:
        return emitAggregation(*node->as<Aggregate>());
      case NodeType::kGroupId:
        return emitGroupId(*node->as<GroupId>());
      case NodeType::kMarkDistinct:
        return emitMarkDistinct(*node->as<MarkDistinct>());
      case NodeType::kJoin:
        return emitJoin(*node->as<Join>());
      case NodeType::kIndexLookupJoin:
        return emitIndexLookupJoin(*node->as<IndexLookupJoin>());
      case NodeType::kSort:
        return emitSort(*node->as<Sort>());
      case NodeType::kTopN:
        return emitTopN(*node->as<TopN>());
      case NodeType::kLimit:
        return emitLimit(*node->as<Limit>());
      case NodeType::kValues:
        return emitValues(*node->as<Values>());
      case NodeType::kWindow:
        return emitWindow(*node->as<Window>());
      case NodeType::kInference:
        return emitInference(*node->as<Inference>());
      case NodeType::kRowNumber:
        return emitRowNumber(*node->as<RowNumber>());
      case NodeType::kTopNRowNumber:
        return emitTopNRowNumber(*node->as<TopNRowNumber>());
      case NodeType::kUnnest:
        return emitUnnest(*node->as<Unnest>());
      case NodeType::kUnionAll:
        return emitUnionAll(*node->as<UnionAll>());
      case NodeType::kEnforceSingleRow:
        return emitEnforceSingleRow(*node->as<EnforceSingleRow>());
      case NodeType::kAssignUniqueId:
        return emitAssignUniqueId(*node->as<AssignUniqueId>());
      case NodeType::kEnforceDistinct:
        return emitEnforceDistinct(*node->as<EnforceDistinct>());
      case NodeType::kApply:
        VELOX_NYI("Emit not implemented for {}", node->toString());
      case NodeType::kTableWrite:
        return emitTableWrite(*node->as<TableWrite>());
      case NodeType::kExchange:
        return emitExchange(*node->as<Exchange>());
      case NodeType::kFixedPoint:
        return emitFixedPoint(*node->as<FixedPoint>());
      case NodeType::kWorkingTable:
        return emitWorkingTable(*node->as<WorkingTable>());
    }
    VELOX_UNREACHABLE();
  }

  std::string nextId() {
    return std::to_string(nextNodeId_++);
  }

  // Records the estimated cardinality of IR 'node' against the emitted Velox
  // 'plan's top node id, for EXPLAIN. Skips unknown cardinalities.
  void recordPrediction(NodeCP node, const velox::core::PlanNodePtr& plan) {
    if (plan == nullptr) {
      return;
    }
    const auto& estimate = estimateProvider_.estimate(node);
    if (!estimate.cardinality.has_value()) {
      return;
    }

    std::optional<uint64_t> numRawInputRows;
    std::optional<float> numRawInputBytesPerRow;
    std::optional<uint64_t> numSplits;
    if (node->is(NodeType::kScan)) {
      const auto* baseTable = node->as<Scan>()->baseTable();
      numRawInputRows = baseTable->numRawInputRows;
      numRawInputBytesPerRow = baseTable->numRawInputBytesPerRow;
      numSplits = baseTable->numSplits;
    }

    prediction_[plan->id()] = NodePrediction{
        .cardinality = *estimate.cardinality,
        .numRawInputRows = numRawInputRows,
        .numRawInputBytesPerRow = numRawInputBytesPerRow,
        .numSplits = numSplits,
        .numOutputBytesPerRow = rowSize(node->outputColumns())};
  }

  // Returns `input` wrapped in a local exchange that brings all rows agreeing
  // on `keys` to one driver: a hash repartition on `keys`, or a gather to a
  // single driver when there are none.
  velox::core::PlanNodePtr addLocalPartition(
      velox::core::PlanNodePtr input,
      const std::vector<velox::core::FieldAccessTypedExprPtr>& keys) {
    if (keys.empty()) {
      return velox::core::LocalPartitionNode::gather(
          nextId(), {std::move(input)});
    }

    auto spec = makeHashPartitionSpec(input->outputType(), keys);
    return std::make_shared<velox::core::LocalPartitionNode>(
        nextId(),
        velox::core::LocalPartitionNode::Type::kRepartition,
        /*scaleWriter=*/false,
        std::move(spec),
        std::vector<velox::core::PlanNodePtr>{std::move(input)});
  }

  // Emits `node` as the plan root with output `outputColumns`
  // aliased to `outputNames`.
  velox::core::PlanNodePtr emitRoot(
      NodeCP node,
      const ColumnVector& outputColumns,
      const std::vector<std::string>& outputNames);

 private:
  // Emits a Project root directly with the requested output layout,
  // picking matching expressions from `project.exprs()`.
  velox::core::PlanNodePtr emitRootProject(
      const Project& project,
      const ColumnVector& outputColumns,
      const std::vector<std::string>& outputNames);

  // Applies a Project root's output layout over an already-emitted `input`
  // (e.g. a remote gather), instead of re-emitting `project.input()`.
  velox::core::PlanNodePtr emitRootProjectOver(
      velox::core::PlanNodePtr input,
      const Project& project,
      const ColumnVector& outputColumns,
      const std::vector<std::string>& outputNames);

  // Emits a Project, materializing large shared literals below it. Current
  // Velox plan serialization expands shared expression nodes, and expression
  // evaluation does not reuse constants across Project expressions. This lower
  // Project can go away when both preserve that sharing.
  velox::core::PlanNodePtr emitProjectOver(
      velox::core::PlanNodePtr input,
      const ExprVector& exprs,
      const std::vector<std::string>& outputNames);

  // Returns 'input's 'columns' projected as 'outputNames', reusing 'input'
  // when it is a projection those columns can be read out of.
  velox::core::PlanNodePtr projectAs(
      velox::core::PlanNodePtr input,
      const ColumnVector& columns,
      const std::vector<std::string>& outputNames);

  // Wraps `input` in a Project that selects `outputColumns` aliased
  // to `outputNames`.
  velox::core::PlanNodePtr wrapWithRenameProject(
      velox::core::PlanNodePtr input,
      const ColumnVector& outputColumns,
      const std::vector<std::string>& outputNames);

  velox::core::PlanNodePtr emitScan(const Scan& scan);
  velox::core::PlanNodePtr emitFilter(const Filter& filter);
  velox::core::PlanNodePtr emitProject(const Project& project);
  velox::core::PlanNodePtr emitAggregation(const Aggregate& aggregate);
  velox::core::PlanNodePtr emitSingleAggregation(const Aggregate& aggregate);
  velox::core::PlanNodePtr emitPartialAggregation(const Aggregate& aggregate);
  velox::core::PlanNodePtr emitFinalAggregation(const Aggregate& aggregate);
  // Builds an AggregationNode, carrying the grouping-set default-row info
  // (`globalGroupingSets`/`groupId`) from `aggregate` when present.
  velox::core::PlanNodePtr makeAggregationNode(
      velox::core::AggregationNode::Step step,
      const Aggregate& aggregate,
      std::vector<velox::core::FieldAccessTypedExprPtr> groupingKeys,
      std::vector<velox::core::FieldAccessTypedExprPtr> preGroupedKeys,
      std::vector<std::string> names,
      std::vector<velox::core::AggregationNode::Aggregate> aggregates,
      velox::core::PlanNodePtr input);
  velox::core::PlanNodePtr emitGroupId(const GroupId& groupId);
  velox::core::PlanNodePtr emitMarkDistinct(const MarkDistinct& markDistinct);
  velox::core::PlanNodePtr emitJoin(const Join& join);
  velox::core::PlanNodePtr emitIndexLookupJoin(const IndexLookupJoin& join);
  velox::core::PlanNodePtr emitSort(const Sort& sort);
  velox::core::PlanNodePtr emitTopN(const TopN& topN);
  velox::core::PlanNodePtr emitLimit(const Limit& limit);
  velox::core::PlanNodePtr emitValues(const Values& values);
  velox::core::PlanNodePtr emitWindow(const Window& window);
  velox::core::PlanNodePtr emitInference(const Inference& node);
  velox::core::PlanNodePtr emitRowNumber(const RowNumber& node);
  velox::core::PlanNodePtr emitTopNRowNumber(const TopNRowNumber& node);
  velox::core::PlanNodePtr emitUnnest(const Unnest& unnest);
  velox::core::PlanNodePtr emitUnionAll(const UnionAll& unionNode);
  velox::core::PlanNodePtr emitEnforceSingleRow(const EnforceSingleRow& node);
  velox::core::PlanNodePtr emitAssignUniqueId(const AssignUniqueId& node);
  velox::core::PlanNodePtr emitEnforceDistinct(const EnforceDistinct& node);

  // Lowers `Join(kAnti | kLeftSemiFilter)` with no equi-keys to
  // `NestedLoopJoin(kLeftSemiProject) + Filter + Project`. See
  // https://github.com/facebookincubator/velox/issues/17650.
  velox::core::PlanNodePtr emitSemiAntiViaLeftSemiProject(
      const Join& join,
      velox::core::PlanNodePtr left,
      velox::core::PlanNodePtr right,
      velox::core::TypedExprPtr filter);

  // Wraps 'input' in a `ProjectNode` that keeps only 'keepColumns'.
  velox::core::PlanNodePtr projectToColumns(
      const ColumnVector& keepColumns,
      velox::core::PlanNodePtr input);

  velox::RowTypePtr makeRowType(const ColumnVector& columns) const;

  const std::string& chooseExchangeSerdeKind(
      const velox::RowType& outputType) const {
    return velox::serializer::presto::PrestoVectorSerde::
                estimateColumnarChannels(outputType) >=
            session_.options().minColumnarChannelsForCompactRow
        ? velox::serializer::CompactRowVectorSerde::name()
        : velox::serializer::presto::PrestoVectorSerde::name();
  }

  // Emits 'exchange's input as a producer fragment ending in a
  // `PartitionedOutput`, and returns the consumer-side `ExchangeNode` for the
  // current fragment, wiring the `InputStage` between them.
  velox::core::PlanNodePtr emitExchange(const Exchange& exchange);
  velox::core::PlanNodePtr emitTableWrite(const TableWrite& tableWrite);
  velox::core::PlanNodePtr emitFixedPoint(const FixedPoint& fixedPoint);
  velox::core::PlanNodePtr emitWorkingTable(const WorkingTable& workingTable);

  // Returns the handle of the scan whose rows 'tableWrite' deletes. Fails if
  // that scan's handle does not describe exactly the rows to remove.
  velox::connector::ConnectorTableHandlePtr deletedRowsHandle(
      const TableWrite& tableWrite);

  // Builds the producer-side `PartitionedOutput` capping 'sourcePlan' for
  // 'partitioning'. A connector-bucketed partitioning that scales to a single
  // destination degrades to a single (gather) output.
  velox::core::PlanNodePtr makeExchangeProducer(
      const Partitioning& partitioning,
      const velox::RowTypePtr& outputType,
      const velox::core::PlanNodePtr& sourcePlan);

  // Builds a producer-side `PartitionedOutput` that gathers 'sourcePlan' to a
  // single destination.
  velox::core::PlanNodePtr makeSingleOutput(
      const velox::RowTypePtr& outputType,
      const velox::core::PlanNodePtr& sourcePlan,
      const std::string& serdeKind) {
    return velox::core::PartitionedOutputNode::single(
        nextId(), outputType, serdeKind, sourcePlan);
  }

  // Builds the query result boundary in the client-compatible wire format.
  velox::core::PlanNodePtr makeClientOutput(
      const velox::core::PlanNodePtr& sourcePlan) {
    return makeSingleOutput(
        sourcePlan->outputType(),
        sourcePlan,
        velox::serializer::presto::PrestoVectorSerde::name());
  }

  // Builds the consumer-side exchange node for 'partitioning' — a
  // `MergeExchange` for an order-preserving gather, else a plain `Exchange`.
  velox::core::PlanNodePtr makeExchangeConsumer(
      const Partitioning& partitioning,
      const velox::RowTypePtr& outputType);

  // Turns the accumulated grouped leaves into `fragment.groupedNodes` using
  // the fragment's common connector partitioning. A fragment with no bucketed
  // scan is left non-grouped.
  void finalizeGroupedLeaves(ExecutableFragment& fragment);

  // Runs 'emitFn' with 'fragment' as the current fragment and its common
  // connector partitioning available to every exchange. Finalizes its grouped
  // leaves, then restores the enclosing fragment's state. For a table-write
  // fragment that is finalized after further building, does this inline.
  template <typename EmitFn>
  velox::core::PlanNodePtr
  emitInFragment(NodeCP root, ExecutableFragment& fragment, EmitFn&& emitFn) {
    auto outerState = std::move(fragmentState_);
    SCOPE_EXIT {
      fragmentState_ = std::move(outerState);
    };
    fragmentState_ = {
        .fragment = &fragment,
        .groupedPartitionType = commonFragmentPartitionType(root),
    };
    velox::core::PlanNodePtr plan = std::forward<EmitFn>(emitFn)();
    finalizeGroupedLeaves(fragment);
    return plan;
  }

  // Emits 'node' as the root of child fragment 'fragment'.
  velox::core::PlanNodePtr emitChildFragment(
      NodeCP node,
      ExecutableFragment& fragment);

  // Emits 'root' below a gather that collects the distributed output onto the
  // single-task output fragment 'top'. The output-column layout applies above
  // the gather when the output names repeat (a shuffle needs unique names),
  // otherwise below it so the gather itself is the plan root.
  void emitGatheredOutput(
      NodeCP root,
      const ColumnVector& outputColumns,
      const std::vector<std::string>& outputNames,
      ExecutableFragment& top);

  // A fresh producer/consumer fragment with a unique id.
  ExecutableFragment newFragment() {
    return ExecutableFragment{.fragmentId = ++fragmentCounter_};
  }

  const OptimizerSession& session_;
  velox::core::ExpressionEvaluator& evaluator_;
  ExprEmitter exprEmitter_;
  // A copy, not a reference: emitting a fixed point lowers
  // `maxLocalPartitions` for the recursive subtree and restores it afterwards.
  MultiFragmentPlan::Options options_;
  int32_t nextNodeId_{0};

  // Producer fragments collected as `ir.Exchange`es are lowered; the root
  // fragment is appended last by `emitFragments`.
  std::vector<ExecutableFragment> stages_;
  int32_t fragmentCounter_{0};

  // A grouped leaf of the fragment currently being built: a source that runs
  // per bucket-group under grouped execution. `partitionType` is the
  // partitioning planning chose for a bucketed scan, or null for a
  // hash-partitioned consumer exchange that must route by group. Saved and
  // restored around each child-fragment build so leaves don't leak across
  // fragments.
  struct PendingGroupedLeaf {
    velox::core::PlanNodeId nodeId;
    const connector::PartitionType* partitionType{nullptr};
  };

  // State accumulated while emitting one fragment. It is replaced as emission
  // descends into a producer fragment and restored on return.
  struct FragmentState {
    void addGroupedLeaf(
        const velox::core::PlanNodeId& nodeId,
        const connector::PartitionType* partitionType) {
      groupedLeaves.push_back({nodeId, partitionType});
    }

    void addInputStage(
        const velox::core::PlanNodeId& nodeId,
        int32_t fragmentId) {
      fragment->inputStages.emplace_back(nodeId, fragmentId);
    }

    void addSampledScan(
        const velox::core::PlanNodeId& nodeId,
        double sampledPercentage) {
      fragment->sampledScans.emplace(nodeId, sampledPercentage);
    }

    ExecutableFragment* fragment{nullptr};
    std::vector<PendingGroupedLeaf> groupedLeaves;
    const connector::PartitionType* groupedPartitionType{nullptr};
  };
  FragmentState fragmentState_;

  // Set by emitTableWrite; moved out via takeFinishWrite so the runner can
  // commit or abort the write. Empty (bool false) for read-only queries.
  FinishWrite finishWrite_;

  // Estimates IR-node cardinality for EXPLAIN, memoized over the emit-time IR.
  EstimateProvider estimateProvider_;

  // Per-node estimated cardinality keyed by emitted PlanNodeId; moved out via
  // takePrediction.
  NodePredictionMap prediction_;

 public:
  // Lowers 'root' (plus the output rename to 'outputNames') into fragments,
  // returning them with the root fragment last.
  std::vector<ExecutableFragment> emitFragments(
      NodeCP root,
      const ColumnVector& outputColumns,
      const std::vector<std::string>& outputNames);

  // Transfers the write-finalization handle produced by a TableWrite emit, if
  // any, to the caller. Empty for read-only queries.
  FinishWrite takeFinishWrite() {
    return std::move(finishWrite_);
  }

  // Transfers the per-node cardinality predictions collected during emit.
  NodePredictionMap takePrediction() {
    return std::move(prediction_);
  }
};

velox::core::PlanNodePtr Emitter::projectToColumns(
    const ColumnVector& keepColumns,
    velox::core::PlanNodePtr input) {
  // Select `keepColumns` under their own output names: a rename projection onto
  // themselves.
  return wrapWithRenameProject(
      std::move(input), keepColumns, namesOf(keepColumns));
}

velox::RowTypePtr Emitter::makeRowType(const ColumnVector& columns) const {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(columns.size());
  types.reserve(columns.size());
  for (ColumnCP column : columns) {
    names.push_back(column->outputName());
    types.push_back(toTypePtr(column->value().type));
  }
  return velox::ROW(std::move(names), std::move(types));
}

velox::core::PlanNodePtr Emitter::emitScan(const Scan& scan) {
  VELOX_CHECK_NOT_NULL(
      scan.scanHandle(), "Scan reaches emit without a connector handle");
  const TableAccessHandle& handle = *scan.scanHandle();
  const ColumnVector& consumerColumns = scan.outputColumns();
  const auto& columnHandles = handle.columnHandles;
  const auto& tableHandle = handle.tableHandle;

  // Scan row type is the consumer columns, named by outputName. A column read
  // only by an accepted filter reaches the connector through the table handle,
  // so it is neither projected nor assigned here.
  std::vector<std::string> scanOutputNames;
  std::vector<velox::TypePtr> scanOutputTypes;
  velox::connector::ColumnHandleMap assignments;
  scanOutputNames.reserve(consumerColumns.size());
  scanOutputTypes.reserve(consumerColumns.size());
  for (ColumnCP column : consumerColumns) {
    std::string outputName{column->outputName()};
    scanOutputTypes.push_back(toTypePtr(column->value().type));
    const auto handleIt = columnHandles.find(column);
    VELOX_CHECK(
        handleIt != columnHandles.end(),
        "Scan reads a column the connector handle was not built for: {}",
        column->name());
    assignments[outputName] = handleIt->second;
    scanOutputNames.push_back(std::move(outputName));
  }

  auto scanNode = std::make_shared<velox::core::TableScanNode>(
      nextId(),
      velox::ROW(std::move(scanOutputNames), std::move(scanOutputTypes)),
      tableHandle,
      std::move(assignments));

  // A grouped scan makes its fragment bucketed.
  if (const auto* partitionType = scan.groupedPartitionType()) {
    fragmentState_.addGroupedLeaf(scanNode->id(), partitionType);
  }

  // TABLESAMPLE SYSTEM: the split source emits each split with this
  // probability. Recorded per scan so split generation can sample.
  if (scan.baseTable()->sampledPercentage.has_value()) {
    fragmentState_.addSampledScan(
        scanNode->id(), *scan.baseTable()->sampledPercentage);
  }

  velox::core::PlanNodePtr result = std::move(scanNode);

  return result;
}

velox::core::PlanNodePtr Emitter::emitFilter(const Filter& filter) {
  velox::core::PlanNodePtr input = emit(filter.input());
  return std::make_shared<velox::core::FilterNode>(
      nextId(), exprEmitter_.makeAnd(filter.predicates()), std::move(input));
}

namespace {

template <typename Visitor>
void forEachExprChild(ExprCP expr, Visitor&& visitor) {
  if (expr->is(PlanType::kCallExpr)) {
    for (ExprCP arg : expr->as<Call>()->args()) {
      visitor(arg);
    }
  } else if (expr->is(PlanType::kLambdaExpr)) {
    visitor(expr->as<Lambda>()->body());
  }
}

void addSaturated(uint64_t increment, uint64_t& value) {
  const uint64_t max = std::numeric_limits<uint64_t>::max();
  value = increment > max - value ? max : value + increment;
}

// Counts serialized occurrences by propagating root-to-node path counts
// through the expression DAG in topological order.
template <typename ToLiteral>
std::vector<std::pair<LiteralCP, uint64_t>> countLiteralReferences(
    const ExprVector& exprs,
    ToLiteral&& toLiteral) {
  folly::F14FastSet<ExprCP> visited;
  std::vector<ExprCP> postorder;
  std::vector<LiteralCP> literals;
  std::function<void(ExprCP)> visit = [&](ExprCP expr) {
    if (!visited.insert(expr).second) {
      return;
    }
    if (LiteralCP literal = toLiteral(expr)) {
      literals.push_back(literal);
    }
    forEachExprChild(expr, visit);
    postorder.push_back(expr);
  };
  for (ExprCP expr : exprs) {
    visit(expr);
  }

  folly::F14FastMap<ExprCP, uint64_t> references;
  for (ExprCP expr : exprs) {
    addSaturated(1, references[expr]);
  }
  for (auto it = postorder.rbegin(); it != postorder.rend(); ++it) {
    const uint64_t numReferences = references.at(*it);
    forEachExprChild(*it, [&](ExprCP child) {
      addSaturated(numReferences, references[child]);
    });
  }

  std::vector<std::pair<LiteralCP, uint64_t>> result;
  result.reserve(literals.size());
  for (LiteralCP literal : literals) {
    result.emplace_back(literal, references.at(literal));
  }
  return result;
}

struct SharedLiteral {
  LiteralCP literal;
  velox::core::TypedExprPtr typedExpr;
};

// Returns an emitted literal when its duplicate copies exceed the configured
// limit. Returns nullopt otherwise.
std::optional<SharedLiteral> tryMakeSharedLiteral(
    LiteralCP literal,
    uint64_t numReferences,
    uint64_t maxDuplicatedBytes,
    ExprEmitter& exprEmitter) {
  if (numReferences < 2) {
    return std::nullopt;
  }

  const uint64_t numDuplicateCopies = numReferences - 1;
  const uint64_t maxLiteralBytes = maxDuplicatedBytes / numDuplicateCopies;
  if (literal->literal().estimateValueSize() <= maxLiteralBytes) {
    return std::nullopt;
  }
  return SharedLiteral{literal, exprEmitter.toTypedExpr(literal)};
}

std::vector<SharedLiteral> findSharedLiterals(
    const ExprVector& exprs,
    uint64_t maxDuplicatedBytes,
    ExprEmitter& exprEmitter) {
  std::vector<SharedLiteral> literals;
  for (const auto& [literal, numReferences] :
       countLiteralReferences(exprs, [](ExprCP expr) {
         if (expr->is(PlanType::kLiteralExpr) &&
             !expr->value().type->isFixedWidth()) {
           return expr->as<Literal>();
         }
         return LiteralCP{nullptr};
       })) {
    if (auto sharedLiteral = tryMakeSharedLiteral(
            literal, numReferences, maxDuplicatedBytes, exprEmitter)) {
      literals.push_back(std::move(*sharedLiteral));
    }
  }
  return literals;
}

} // namespace

velox::core::PlanNodePtr Emitter::emitProjectOver(
    velox::core::PlanNodePtr input,
    const ExprVector& exprs,
    const std::vector<std::string>& outputNames) {
  auto sharedLiterals = findSharedLiterals(
      exprs, session_.options().maxDuplicatedLiteralBytes, exprEmitter_);

  if (sharedLiterals.empty()) {
    return std::make_shared<velox::core::ProjectNode>(
        nextId(),
        outputNames,
        exprEmitter_.toTypedExprs(exprs),
        std::move(input));
  }

  const auto& inputType = input->outputType();
  std::vector<std::string> literalNames = inputType->names();
  std::vector<velox::core::TypedExprPtr> literalExprs;
  literalExprs.reserve(inputType->size() + sharedLiterals.size());
  for (size_t i = 0; i < inputType->size(); ++i) {
    literalExprs.push_back(
        toFieldAccess(inputType->childAt(i), inputType->nameOf(i)));
  }

  ExprEmitter::ReplacementMap replacements;
  for (auto& sharedLiteral : sharedLiterals) {
    std::string name{queryCtx()->newName("__shared_literal_")};
    literalNames.push_back(name);
    literalExprs.push_back(std::move(sharedLiteral.typedExpr));
    replacements.emplace(
        sharedLiteral.literal,
        toFieldAccess(toTypePtr(sharedLiteral.literal->value().type), name));
  }

  input = std::make_shared<velox::core::ProjectNode>(
      nextId(),
      std::move(literalNames),
      std::move(literalExprs),
      std::move(input));
  return std::make_shared<velox::core::ProjectNode>(
      nextId(),
      outputNames,
      exprEmitter_.toTypedExprs(exprs, replacements),
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitProject(const Project& project) {
  if (project.isConstantRow()) {
    std::vector<velox::Variant> row;
    row.reserve(project.exprs().size());
    for (ExprCP expr : project.exprs()) {
      row.push_back(expr->as<Literal>()->literal());
    }
    std::vector<velox::RowVectorPtr> rowVectors{
        std::static_pointer_cast<velox::RowVector>(
            velox::BaseVector::createFromVariants(
                makeRowType(project.outputColumns()),
                {velox::Variant::row(std::move(row))},
                evaluator_.pool()))};
    return std::make_shared<velox::core::ValuesNode>(
        nextId(), std::move(rowVectors));
  }
  return emitProjectOver(
      emit(project.input()), project.exprs(), namesOf(project.outputColumns()));
}

velox::core::PlanNodePtr Emitter::emitRootProjectOver(
    velox::core::PlanNodePtr input,
    const Project& project,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames) {
  const auto& projectColumns = project.outputColumns();
  const auto& projectExprs = project.exprs();
  ExprVector exprs;
  exprs.reserve(outputColumns.size());
  for (ColumnCP column : outputColumns) {
    const auto it =
        std::find(projectColumns.begin(), projectColumns.end(), column);
    VELOX_CHECK(
        it != projectColumns.end(),
        "Output column not produced by root Project: {}",
        column->outputName());
    const size_t index = std::distance(projectColumns.begin(), it);
    exprs.push_back(projectExprs[index]);
  }
  return emitProjectOver(std::move(input), exprs, outputNames);
}

velox::core::PlanNodePtr Emitter::emitRootProject(
    const Project& project,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames) {
  return emitRootProjectOver(
      emit(project.input()), project, outputColumns, outputNames);
}

velox::core::PlanNodePtr Emitter::wrapWithRenameProject(
    velox::core::PlanNodePtr input,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames) {
  std::vector<velox::core::TypedExprPtr> projections;
  projections.reserve(outputColumns.size());
  for (ColumnCP column : outputColumns) {
    projections.push_back(toFieldAccess(column));
  }
  return std::make_shared<velox::core::ProjectNode>(
      nextId(),
      std::vector<std::string>{outputNames},
      std::move(projections),
      std::move(input));
}

namespace {

bool isIdentityLayout(
    const Node& root,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames) {
  const auto& rootColumns = root.outputColumns();
  if (outputColumns.size() != rootColumns.size()) {
    return false;
  }
  for (size_t i = 0; i < outputColumns.size(); ++i) {
    if (outputColumns[i] != rootColumns[i] ||
        outputNames[i] != outputColumns[i]->outputName()) {
      return false;
    }
  }
  return true;
}

// True when 'names' contains the same name more than once.
bool hasDuplicateNames(const std::vector<std::string>& names) {
  folly::F14FastSet<std::string_view> seen;
  seen.reserve(names.size());
  for (const auto& name : names) {
    if (!seen.insert(name).second) {
      return true;
    }
  }
  return false;
}

} // namespace

velox::core::PlanNodePtr Emitter::emitRoot(
    NodeCP node,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames) {
  velox::core::PlanNodePtr result;
  if (isIdentityLayout(*node, outputColumns, outputNames)) {
    result = emit(node);
    // Velox's Unnest produces a column per unnested value, while the node may
    // output only some of them, so the emitted node can be the wider of the
    // two; trim it so the extra columns do not leak into the query output.
    if (result->outputType()->size() > outputColumns.size()) {
      result = projectToColumns(outputColumns, std::move(result));
    }
  } else if (node->is(NodeType::kProject)) {
    result = emitRootProject(*node->as<Project>(), outputColumns, outputNames);
    recordPrediction(node, result);
  } else {
    result = wrapWithRenameProject(emit(node), outputColumns, outputNames);
  }
  return result;
}

namespace {

// The types of an aggregate's raw (pre-accumulation) argument columns, which
// Velox needs at every aggregation step.
std::vector<velox::TypePtr> rawInputTypesOf(
    const optimizer::Aggregate& aggregateExpr) {
  std::vector<velox::TypePtr> types;
  types.reserve(aggregateExpr.args().size());
  for (ExprCP arg : aggregateExpr.args()) {
    types.push_back(toTypePtr(arg->value().type));
  }
  return types;
}

// Lowers `aggregateExpr` to a Velox `Aggregate` whose call return
// type is `resultType` (intermediate for kPartial; final otherwise).
velox::core::AggregationNode::Aggregate toVeloxAggregate(
    const optimizer::Aggregate& aggregateExpr,
    const velox::TypePtr& resultType,
    ExprEmitter& exprEmitter) {
  std::vector<velox::core::TypedExprPtr> inputs;
  inputs.reserve(aggregateExpr.args().size());
  for (ExprCP arg : aggregateExpr.args()) {
    VELOX_CHECK(
        arg->is(PlanType::kColumnExpr) || arg->is(PlanType::kLiteralExpr) ||
            arg->is(PlanType::kLambdaExpr),
        "Aggregate argument must be a column, literal or lambda");
    inputs.push_back(exprEmitter.toTypedExpr(arg));
  }

  velox::core::AggregationNode::Aggregate out{};
  out.call = std::make_shared<velox::core::CallTypedExpr>(
      resultType, std::move(inputs), std::string{aggregateExpr.name()});
  out.rawInputTypes = rawInputTypesOf(aggregateExpr);
  out.distinct = aggregateExpr.isDistinct();
  if (aggregateExpr.condition() != nullptr) {
    out.mask = toFieldAccess(aggregateExpr.condition(), "Aggregate FILTER");
  }
  std::tie(out.sortingKeys, out.sortingOrders) = toSortingKeys(
      aggregateExpr.orderKeys(),
      aggregateExpr.orderTypes(),
      "Aggregate ORDER BY key");
  return out;
}

// Builds the kFinal-step `Aggregate` that consumes the intermediate
// accumulator emitted by the Partial under `intermediateName`. mask, ORDER BY,
// and distinct dedup were applied at the Partial; don't repeat them. Lambda
// arguments (e.g. `reduce_agg`'s combiners) must be forwarded because Velox
// binds them at every step. `rawInputTypes` are the Partial's raw inputs,
// recomputed here from the expression's arguments (the Partial that produced
// the intermediate may live in another fragment, so its Velox `Aggregate` is
// not in hand).
velox::core::AggregationNode::Aggregate toVeloxFinalAggregate(
    const optimizer::Aggregate& aggregateExpr,
    std::string_view intermediateName,
    ExprEmitter& exprEmitter) {
  std::vector<velox::core::TypedExprPtr> inputs;
  inputs.push_back(
      std::make_shared<velox::core::FieldAccessTypedExpr>(
          toTypePtr(aggregateExpr.intermediateType()),
          std::string{intermediateName}));
  for (ExprCP arg : aggregateExpr.args()) {
    if (arg->is(PlanType::kLambdaExpr)) {
      inputs.push_back(exprEmitter.toTypedExpr(arg));
    }
  }
  velox::core::AggregationNode::Aggregate out{};
  out.call = std::make_shared<velox::core::CallTypedExpr>(
      toTypePtr(aggregateExpr.value().type),
      std::move(inputs),
      std::string{aggregateExpr.name()});
  out.rawInputTypes = rawInputTypesOf(aggregateExpr);
  return out;
}

// Subset of `groupingKeys` that `local` guarantees are pre-grouped (equal-key
// rows contiguous), so an aggregation over them can stream rather than build a
// full hash table. The larger of: the leading `Sorted` run that are grouping
// keys, and a `Grouped(S)` whose columns are all grouping keys (the whole `S`,
// since rows are contiguous on the set jointly, not on a bare subset). Empty
// when nothing is pre-grouped.
ExprVector computePreGroupedKeys(
    const LocalPropertyVector& local,
    const ExprVector& groupingKeys) {
  if (groupingKeys.empty()) {
    return {};
  }

  const auto isGroupingKey = [&](ColumnCP column) {
    for (ExprCP key : groupingKeys) {
      if (column->sameOrEqual(*key)) {
        return true;
      }
    }
    return false;
  };

  ExprVector best;
  for (const LocalProperty& property : local) {
    ExprVector candidate;
    if (property.kind == LocalPropertyKind::kSorted) {
      // Sorted on c1..cn implies grouped on any leading prefix; take the
      // leading run of sort keys that are grouping keys.
      for (ColumnCP column : property.columns) {
        if (!isGroupingKey(column)) {
          break;
        }
        candidate.push_back(column);
      }
    } else if (std::ranges::all_of(property.columns, isGroupingKey)) {
      // Grouped on the whole set jointly; usable only if every member is a
      // grouping key.
      for (ColumnCP column : property.columns) {
        candidate.push_back(column);
      }
    }
    if (candidate.size() > best.size()) {
      best = std::move(candidate);
    }
  }

  return best;
}

// Builds the (output name, Velox Aggregate) lists for an AggregationNode. The
// per-step call construction differs (single/partial output the result vs the
// intermediate type; final reads its input accumulator column), so the caller
// supplies 'makeAggregate' for aggregate i; the output name is always the i-th
// non-key output column.
std::pair<
    std::vector<std::string>,
    std::vector<velox::core::AggregationNode::Aggregate>>
buildAggregates(
    const Aggregate& aggregate,
    const std::function<velox::core::AggregationNode::Aggregate(size_t)>&
        makeAggregate) {
  const auto& outputColumns = aggregate.outputColumns();
  const size_t numKeys = aggregate.groupingKeys().size();
  std::vector<std::string> names;
  std::vector<velox::core::AggregationNode::Aggregate> aggregates;
  names.reserve(aggregate.aggregates().size());
  aggregates.reserve(aggregate.aggregates().size());
  for (size_t i = 0; i < aggregate.aggregates().size(); ++i) {
    aggregates.push_back(makeAggregate(i));
    names.push_back(outputColumns[numKeys + i]->outputName());
  }
  return {std::move(names), std::move(aggregates)};
}

} // namespace

velox::core::PlanNodePtr Emitter::emitAggregation(const Aggregate& aggregate) {
  switch (aggregate.step()) {
    case AggregateStep::kPartial:
      return emitPartialAggregation(aggregate);
    case AggregateStep::kFinal:
      return emitFinalAggregation(aggregate);
    case AggregateStep::kSingle:
      return emitSingleAggregation(aggregate);
    case AggregateStep::kIntermediate:
      break;
  }
  VELOX_UNREACHABLE();
}

velox::core::PlanNodePtr Emitter::makeAggregationNode(
    velox::core::AggregationNode::Step step,
    const Aggregate& aggregate,
    std::vector<velox::core::FieldAccessTypedExprPtr> groupingKeys,
    std::vector<velox::core::FieldAccessTypedExprPtr> preGroupedKeys,
    std::vector<std::string> names,
    std::vector<velox::core::AggregationNode::Aggregate> aggregates,
    velox::core::PlanNodePtr input) {
  // A Velox AggregationNode names its output after its grouping-key fields, so
  // a key must be published under the column the Aggregate outputs for it. A
  // pass that materializes a key under a fresh name has to reuse that column;
  // otherwise consumers read a column the node does not output.
  for (size_t i = 0; i < groupingKeys.size(); ++i) {
    VELOX_CHECK_EQ(
        groupingKeys[i]->name(),
        aggregate.outputColumns()[i]->outputName(),
        "Grouping key must be published under the aggregate's output column");
  }
  std::vector<velox::vector_size_t> globalGroupingSets(
      aggregate.globalGroupingSets().begin(),
      aggregate.globalGroupingSets().end());
  std::optional<velox::core::FieldAccessTypedExprPtr> groupId;
  if (aggregate.groupId() != nullptr) {
    groupId = toFieldAccess(aggregate.groupId());
  }
  return std::make_shared<velox::core::AggregationNode>(
      nextId(),
      step,
      std::move(groupingKeys),
      std::move(preGroupedKeys),
      std::move(names),
      std::move(aggregates),
      std::move(globalGroupingSets),
      std::move(groupId),
      /*ignoreNullKeys=*/false,
      /*noGroupsSpanBatches=*/false,
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitSingleAggregation(
    const Aggregate& aggregate) {
  velox::core::PlanNodePtr input = emit(aggregate.input());

  auto groupingKeys =
      toFieldAccessList(aggregate.groupingKeys(), "Grouping key");

  // An aggregation over input already grouped on (a prefix of) the keys streams
  // instead of building a full hash table.
  auto preGroupedKeys = toFieldAccessList(
      computePreGroupedKeys(
          aggregate.input()->physicalProperties().local,
          aggregate.groupingKeys()),
      "Pre-grouped key");

  auto [names, aggregates] = buildAggregates(aggregate, [&](size_t i) {
    const auto* aggregateExpr = aggregate.aggregates()[i];
    return toVeloxAggregate(
        *aggregateExpr, toTypePtr(aggregateExpr->value().type), exprEmitter_);
  });

  return makeAggregationNode(
      velox::core::AggregationNode::Step::kSingle,
      aggregate,
      std::move(groupingKeys),
      std::move(preGroupedKeys),
      std::move(names),
      std::move(aggregates),
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitPartialAggregation(
    const Aggregate& aggregate) {
  velox::core::PlanNodePtr input = emit(aggregate.input());
  auto groupingKeys =
      toFieldAccessList(aggregate.groupingKeys(), "Grouping key");

  // The Partial emits the intermediate accumulator, not the final result.
  auto [names, aggregates] = buildAggregates(aggregate, [&](size_t i) {
    const auto* aggregateExpr = aggregate.aggregates()[i];
    return toVeloxAggregate(
        *aggregateExpr,
        toTypePtr(aggregateExpr->intermediateType()),
        exprEmitter_);
  });

  return makeAggregationNode(
      velox::core::AggregationNode::Step::kPartial,
      aggregate,
      std::move(groupingKeys),
      /*preGroupedKeys=*/{},
      std::move(names),
      std::move(aggregates),
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitFinalAggregation(
    const Aggregate& aggregate) {
  velox::core::PlanNodePtr input = emit(aggregate.input());
  auto groupingKeys =
      toFieldAccessList(aggregate.groupingKeys(), "Grouping key");
  // The Final's input is the (exchanged) Partial output: grouping keys followed
  // by one intermediate accumulator per aggregate, so the i-th accumulator is
  // at input column `numKeys + i`.
  const auto& inputColumns = aggregate.input()->outputColumns();
  const size_t numKeys = aggregate.groupingKeys().size();
  auto [names, aggregates] = buildAggregates(aggregate, [&](size_t i) {
    return toVeloxFinalAggregate(
        *aggregate.aggregates()[i],
        inputColumns[numKeys + i]->outputName(),
        exprEmitter_);
  });

  return makeAggregationNode(
      velox::core::AggregationNode::Step::kFinal,
      aggregate,
      std::move(groupingKeys),
      /*preGroupedKeys=*/{},
      std::move(names),
      std::move(aggregates),
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitGroupId(const GroupId& groupId) {
  velox::core::PlanNodePtr input = emit(groupId.input());

  std::vector<velox::core::GroupIdNode::GroupingKeyInfo> groupingKeyInfos;
  groupingKeyInfos.reserve(groupId.groupingKeyColumns().size());
  for (size_t i = 0; i < groupId.groupingKeyColumns().size(); ++i) {
    groupingKeyInfos.push_back({
        .output = std::string{groupId.groupingKeyColumns()[i]->outputName()},
        .input = toFieldAccess(groupId.groupingKeys()[i], "GroupId key"),
    });
  }

  std::vector<std::vector<std::string>> groupingSets;
  groupingSets.reserve(groupId.groupingSets().size());
  for (const auto& set : groupId.groupingSets()) {
    std::vector<std::string> names;
    names.reserve(set.size());
    for (auto keyIndex : set) {
      VELOX_CHECK_LT(
          keyIndex,
          groupingKeyInfos.size(),
          "GroupId grouping-set index out of range");
      names.emplace_back(groupingKeyInfos[keyIndex].output);
    }
    groupingSets.emplace_back(std::move(names));
  }

  std::vector<velox::core::FieldAccessTypedExprPtr> aggregationInputs =
      toFieldAccessList(
          groupId.aggregationInputs(), "GroupId aggregationInput");

  return std::make_shared<velox::core::GroupIdNode>(
      nextId(),
      std::move(groupingSets),
      std::move(groupingKeyInfos),
      std::move(aggregationInputs),
      std::string{groupId.groupId()->outputName()},
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitMarkDistinct(
    const MarkDistinct& markDistinct) {
  velox::core::PlanNodePtr input = emit(markDistinct.input());

  auto markerNames = namesOf(markDistinct.markers());

  std::vector<velox::core::FieldAccessTypedExprPtr> distinctKeys =
      toFieldAccessList(markDistinct.distinctKeys(), "MarkDistinct key");

  return std::make_shared<velox::core::MarkDistinctNode>(
      nextId(),
      std::move(markerNames),
      std::move(distinctKeys),
      toFieldAccessList(markDistinct.masks()),
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitJoin(const Join& join) {
  velox::core::PlanNodePtr left = emit(join.left());
  velox::core::PlanNodePtr right = emit(join.right());

  const auto sourceNames = namesOf(join.sourceColumns());
  const auto outputNames = namesOf(join.outputColumns());
  const auto finishJoin = [&](velox::core::PlanNodePtr plan) {
    return sourceNames == outputNames
        ? std::move(plan)
        : wrapWithRenameProject(
              std::move(plan), join.sourceColumns(), outputNames);
  };

  // PrecomputeProjections guarantees each key is a FieldAccess-shaped
  // Column ref by this point.
  velox::core::TypedExprPtr filter;
  if (!join.filter().empty()) {
    filter = exprEmitter_.makeAnd(join.filter());
  }

  if (!join.leftKeys().empty()) {
    auto leftKeys = toFieldAccessList(join.leftKeys(), "Join leftKey");
    auto rightKeys = toFieldAccessList(join.rightKeys(), "Join rightKey");

    return finishJoin(
        std::make_shared<velox::core::HashJoinNode>(
            nextId(),
            join.joinType(),
            join.nullAware(),
            std::move(leftKeys),
            std::move(rightKeys),
            filter,
            std::move(left),
            std::move(right),
            makeRowType(join.sourceColumns()),
            /*useHashTableCache=*/false,
            join.nullAsValue()));
  }

  const auto joinType = join.joinType();
  if (joinType == velox::core::JoinType::kAnti ||
      joinType == velox::core::JoinType::kLeftSemiFilter) {
    // Velox NestedLoopJoinNode supports kLeftSemiProject but not kAnti
    // or kLeftSemiFilter; lower via a kLeftSemiProject mark column plus
    // a Filter on the mark, then project it away. Remove once Velox
    // closes https://github.com/facebookincubator/velox/issues/17650.
    VELOX_CHECK(
        !join.nullAware(),
        "Null-aware kAnti/kLeftSemiFilter without equi-keys is not supported");
    return finishJoin(emitSemiAntiViaLeftSemiProject(
        join, std::move(left), std::move(right), filter));
  }

  return finishJoin(
      std::make_shared<velox::core::NestedLoopJoinNode>(
          nextId(),
          joinType,
          filter,
          std::move(left),
          std::move(right),
          makeRowType(join.sourceColumns())));
}

velox::core::PlanNodePtr Emitter::emitIndexLookupJoin(
    const IndexLookupJoin& join) {
  velox::core::PlanNodePtr probe = emit(join.probe());
  const auto& handle = *join.lookupHandle();

  velox::connector::ColumnHandleMap assignments;
  for (ColumnCP column : join.lookupOutputColumns()) {
    assignments.emplace(column->outputName(), handle.columnHandles.at(column));
  }
  auto lookupSource = std::make_shared<velox::core::TableScanNode>(
      nextId(),
      makeRowType(join.lookupOutputColumns()),
      handle.tableHandle,
      std::move(assignments));

  velox::core::TypedExprPtr filter;
  if (!join.filter().empty()) {
    filter = exprEmitter_.makeAnd(join.filter());
  }

  auto result = std::make_shared<velox::core::IndexLookupJoinNode>(
      nextId(),
      join.joinType(),
      toFieldAccessList(join.probeKeys(), "IndexLookupJoin probeKey"),
      toFieldAccessList(join.lookupKeys()),
      /*joinConditions=*/std::vector<velox::core::IndexLookupConditionPtr>{},
      std::move(filter),
      /*hasMarker=*/false,
      /*splitOutput=*/std::nullopt,
      std::move(probe),
      std::move(lookupSource),
      makeRowType(join.sourceColumns()));

  return join.sourceColumns() == join.outputColumns()
      ? std::move(result)
      : wrapWithRenameProject(
            std::move(result),
            join.sourceColumns(),
            namesOf(join.outputColumns()));
}

velox::core::PlanNodePtr Emitter::emitSemiAntiViaLeftSemiProject(
    const Join& join,
    velox::core::PlanNodePtr left,
    velox::core::PlanNodePtr right,
    velox::core::TypedExprPtr filter) {
  const std::string markName = fmt::format("__nlj_mark_{}", nextNodeId_);
  const auto& outputColumns = join.outputColumns();

  auto outputType = [&]() {
    std::vector<std::string> innerNames;
    std::vector<velox::TypePtr> innerTypes;
    innerNames.reserve(outputColumns.size() + 1);
    innerTypes.reserve(outputColumns.size() + 1);
    for (ColumnCP column : outputColumns) {
      innerNames.emplace_back(column->outputName());
      innerTypes.push_back(toTypePtr(column->value().type));
    }
    innerNames.push_back(markName);
    innerTypes.push_back(velox::BOOLEAN());
    return velox::ROW(std::move(innerNames), std::move(innerTypes));
  }();

  auto joinNode = std::make_shared<velox::core::NestedLoopJoinNode>(
      nextId(),
      velox::core::JoinType::kLeftSemiProject,
      filter,
      std::move(left),
      std::move(right),
      std::move(outputType));

  auto markRef = std::make_shared<velox::core::FieldAccessTypedExpr>(
      velox::BOOLEAN(), markName);
  velox::core::TypedExprPtr markPredicate = markRef;
  if (join.joinType() == velox::core::JoinType::kAnti) {
    markPredicate = std::make_shared<velox::core::CallTypedExpr>(
        velox::BOOLEAN(),
        std::vector<velox::core::TypedExprPtr>{markRef},
        std::string{queryCtx()->functionNames().negation});
  }

  auto filtered = std::make_shared<velox::core::FilterNode>(
      nextId(), std::move(markPredicate), std::move(joinNode));

  return projectToColumns(outputColumns, std::move(filtered));
}

velox::core::PlanNodePtr Emitter::emitSort(const Sort& sort) {
  velox::core::PlanNodePtr input = emit(sort.input());
  auto [sortingKeys, sortingOrders] =
      toSortingKeys(sort.orderKeys(), sort.orderTypes(), "Sort key");
  return std::make_shared<velox::core::OrderByNode>(
      nextId(), sortingKeys, sortingOrders, sort.isPartial(), std::move(input));
}

velox::core::PlanNodePtr Emitter::emitTopN(const TopN& topN) {
  velox::core::PlanNodePtr input = emit(topN.input());
  auto [sortingKeys, sortingOrders] =
      toSortingKeys(topN.orderKeys(), topN.orderTypes(), "TopN key");
  // The TopN keeps offset + count rows; a trailing Limit applies the offset.
  const int64_t topNCount = topN.offset() + topN.count();
  VELOX_CHECK_LE(
      topNCount,
      std::numeric_limits<int32_t>::max(),
      "TopN offset + count exceeds the Velox TopNNode count limit");
  if (topN.isPartial()) {
    return std::make_shared<velox::core::TopNNode>(
        nextId(),
        sortingKeys,
        sortingOrders,
        static_cast<int32_t>(topNCount),
        /*isPartial=*/true,
        std::move(input));
  }

  velox::core::PlanNodePtr result = std::make_shared<velox::core::TopNNode>(
      nextId(),
      sortingKeys,
      sortingOrders,
      static_cast<int32_t>(topNCount),
      /*isPartial=*/false,
      std::move(input));
  if (topN.offset() > 0) {
    result = std::make_shared<velox::core::LimitNode>(
        nextId(),
        topN.offset(),
        topN.count(),
        /*isPartial=*/false,
        std::move(result));
  }
  return result;
}

velox::core::PlanNodePtr Emitter::emitLimit(const Limit& limit) {
  velox::core::PlanNodePtr input = emit(limit.input());
  return std::make_shared<velox::core::LimitNode>(
      nextId(),
      limit.offset(),
      limit.count(),
      limit.isPartial(),
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitValues(const Values& values) {
  auto* pool = evaluator_.pool();
  const auto& channels = values.channels();
  const auto rowType = makeRowType(values.outputColumns());
  std::vector<velox::RowVectorPtr> rowVectors;

  if (values.rows() != nullptr) {
    const auto& fullRows = values.rows()->array();
    // Keep only the selected fields of each row variant, so a pruned data
    // column is never built.
    std::vector<velox::Variant> prunedRows;
    const std::vector<velox::Variant>* rows = &fullRows;
    if (!fullRows.empty() &&
        !isIdentityChannels(channels, fullRows.front().row().size())) {
      prunedRows.reserve(fullRows.size());
      for (const auto& row : fullRows) {
        const auto& fields = row.row();
        std::vector<velox::Variant> kept;
        kept.reserve(channels.size());
        for (auto channel : channels) {
          kept.push_back(fields[channel]);
        }
        prunedRows.push_back(velox::Variant::row(std::move(kept)));
      }
      rows = &prunedRows;
    }
    rowVectors.emplace_back(
        std::static_pointer_cast<velox::RowVector>(
            velox::BaseVector::createFromVariants(rowType, *rows, pool)));
    return std::make_shared<velox::core::ValuesNode>(
        nextId(), std::move(rowVectors));
  }

  const auto* source = values.source();
  if (source == nullptr) {
    rowVectors.emplace_back(
        velox::BaseVector::create<velox::RowVector>(rowType, /*size=*/0, pool));
    return std::make_shared<velox::core::ValuesNode>(
        nextId(), std::move(rowVectors));
  }

  const auto& sourceRowType = source->outputType();
  const bool identity = isIdentityChannels(channels, sourceRowType->size());
  const auto& data = source->data();
  if (const auto* variants =
          std::get_if<logical_plan::ValuesNode::Variants>(&data)) {
    auto full = std::static_pointer_cast<velox::RowVector>(
        velox::BaseVector::createFromVariants(sourceRowType, *variants, pool));
    rowVectors.emplace_back(
        identity ? full : selectChannels(full, rowType, channels));
  } else if (
      const auto* vectors =
          std::get_if<logical_plan::ValuesNode::Vectors>(&data)) {
    if (identity) {
      rowVectors = *vectors;
    } else {
      rowVectors.reserve(vectors->size());
      for (const auto& vector : *vectors) {
        rowVectors.push_back(selectChannels(vector, rowType, channels));
      }
    }
  } else {
    VELOX_NYI(
        "ValuesNode::Exprs emit not yet supported (needs constant folding from normalize)");
  }
  return std::make_shared<velox::core::ValuesNode>(
      nextId(), std::move(rowVectors));
}

velox::core::PlanNodePtr Emitter::emitWindow(const Window& window) {
  velox::core::PlanNodePtr input = emit(window.input());

  auto partitionKeys =
      toFieldAccessList(window.partitionKeys(), "Window partition key");

  std::vector<velox::core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<velox::core::SortOrder> sortingOrders;
  sortingKeys.reserve(window.orderKeys().size());
  sortingOrders.reserve(window.orderTypes().size());
  for (size_t i = 0; i < window.orderKeys().size(); ++i) {
    sortingKeys.push_back(
        toFieldAccess(window.orderKeys()[i], "Window sort key"));
    sortingOrders.push_back(toSortOrder(window.orderTypes()[i]));
  }

  const auto& functions = window.functions();
  const auto& outputColumns = window.outputColumns();
  const size_t numInputColumns = window.input()->outputColumns().size();
  std::vector<std::string> windowColumnNames;
  std::vector<velox::core::WindowNode::Function> windowFunctions;
  windowColumnNames.reserve(functions.size());
  windowFunctions.reserve(functions.size());
  for (size_t i = 0; i < functions.size(); ++i) {
    auto typed = exprEmitter_.toTypedExpr(functions[i].call);
    auto call =
        std::dynamic_pointer_cast<const velox::core::CallTypedExpr>(typed);
    VELOX_USER_CHECK_NOT_NULL(call, "Window function must be a call");
    const auto& frame = functions[i].frame;
    velox::core::WindowNode::Frame veloxFrame{
        .type = static_cast<velox::core::WindowNode::WindowType>(frame.type),
        .startType =
            static_cast<velox::core::WindowNode::BoundType>(frame.startType),
        .startValue = frame.startValue == nullptr
            ? nullptr
            : exprEmitter_.toTypedExpr(frame.startValue),
        .endType =
            static_cast<velox::core::WindowNode::BoundType>(frame.endType),
        .endValue = frame.endValue == nullptr
            ? nullptr
            : exprEmitter_.toTypedExpr(frame.endValue),
    };
    windowFunctions.push_back({call, veloxFrame, functions[i].ignoreNulls});
    windowColumnNames.push_back(
        outputColumns[numInputColumns + i]->outputName());
  }

  return std::make_shared<velox::core::WindowNode>(
      nextId(),
      std::move(partitionKeys),
      std::move(sortingKeys),
      std::move(sortingOrders),
      std::move(windowColumnNames),
      std::move(windowFunctions),
      /*inputsSorted=*/false,
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitInference(const Inference& node) {
  velox::core::PlanNodePtr input = emit(node.input());

  velox::core::TypedExprPtr typed = exprEmitter_.toTypedExpr(node.call());
  auto call =
      std::dynamic_pointer_cast<const velox::core::CallTypedExpr>(typed);
  VELOX_CHECK_NOT_NULL(
      call, "Inference node must hold a call: {}", typed->toString());

  return std::make_shared<velox::core::RPCNode>(
      nextId(),
      std::move(input),
      std::move(call),
      node.result()->outputName(),
      makeRowType(node.outputColumns()));
}

velox::core::PlanNodePtr Emitter::emitRowNumber(const RowNumber& node) {
  velox::core::PlanNodePtr input = emit(node.input());
  auto partitionKeys =
      toFieldAccessList(node.partitionKeys(), "RowNumber partition key");
  std::optional<std::string> rowNumberColumnName;
  if (node.rankColumn() != nullptr) {
    rowNumberColumnName = node.rankColumn()->outputName();
  }
  return std::make_shared<velox::core::RowNumberNode>(
      nextId(),
      std::move(partitionKeys),
      rowNumberColumnName,
      node.limit(),
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitTopNRowNumber(const TopNRowNumber& node) {
  velox::core::PlanNodePtr input = emit(node.input());
  auto partitionKeys =
      toFieldAccessList(node.partitionKeys(), "TopNRowNumber partition key");
  auto [sortingKeys, sortingOrders] = toSortingKeys(
      node.orderKeys(), node.orderTypes(), "TopNRowNumber order key");
  std::optional<std::string> rowNumberColumnName;
  if (node.rankColumn() != nullptr) {
    rowNumberColumnName = node.rankColumn()->outputName();
  }
  return std::make_shared<velox::core::TopNRowNumberNode>(
      nextId(),
      node.rankFunction(),
      std::move(partitionKeys),
      std::move(sortingKeys),
      std::move(sortingOrders),
      rowNumberColumnName,
      node.limit(),
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitUnnest(const Unnest& unnest) {
  velox::core::PlanNodePtr input = emit(unnest.input());

  std::vector<velox::core::FieldAccessTypedExprPtr> replicateVariables =
      toFieldAccessList(unnest.replicatedColumns());

  std::vector<velox::core::FieldAccessTypedExprPtr> unnestVariables =
      toFieldAccessList(unnest.unnestExpressions(), "Unnest expression");

  std::vector<std::optional<std::string>> unnestNames;
  for (const auto& perExpr : unnest.unnestColumns()) {
    for (ColumnCP column : perExpr) {
      unnestNames.emplace_back(column->outputName());
    }
  }

  std::optional<std::string> ordinalityName;
  if (unnest.ordinalityColumn() != nullptr) {
    ordinalityName = unnest.ordinalityColumn()->outputName();
  }

  std::optional<std::string> markerName;
  if (unnest.isOuter()) {
    markerName = unnest.markerColumn()->outputName();
  }

  return std::make_shared<velox::core::UnnestNode>(
      nextId(),
      std::move(replicateVariables),
      std::move(unnestVariables),
      std::move(unnestNames),
      ordinalityName,
      markerName,
      std::move(input));
}

namespace {

// Returns the index in 'type' of each of 'names'.
std::vector<velox::column_index_t> childIndices(
    const velox::RowType& type,
    const std::vector<std::string>& names) {
  std::vector<velox::column_index_t> indices;
  indices.reserve(names.size());
  for (const auto& name : names) {
    const auto index = type.getChildIdxIfExists(name);
    VELOX_CHECK(index.has_value(), "Column not found in input: {}", name);
    indices.push_back(*index);
  }
  return indices;
}

// Returns the rows of 'values' with the columns at 'indices', named
// 'outputNames'.
std::vector<velox::RowVectorPtr> selectColumns(
    const velox::core::ValuesNode& values,
    const std::vector<velox::column_index_t>& indices,
    const std::vector<std::string>& outputNames) {
  std::vector<velox::TypePtr> types;
  types.reserve(indices.size());
  for (auto index : indices) {
    types.push_back(values.outputType()->childAt(index));
  }
  auto rowType =
      velox::ROW(std::vector<std::string>{outputNames}, std::move(types));
  std::vector<velox::RowVectorPtr> rows;
  rows.reserve(values.values().size());
  for (const auto& vector : values.values()) {
    std::vector<velox::VectorPtr> children;
    children.reserve(indices.size());
    for (auto index : indices) {
      children.push_back(vector->childAt(index));
    }
    rows.push_back(
        std::make_shared<velox::RowVector>(
            vector->pool(),
            rowType,
            nullptr,
            vector->size(),
            std::move(children)));
  }
  return rows;
}

} // namespace

velox::core::PlanNodePtr Emitter::projectAs(
    velox::core::PlanNodePtr input,
    const ColumnVector& columns,
    const std::vector<std::string>& outputNames) {
  const auto sourceNames = namesOf(columns);
  // Re-emitting a projection's expressions under other names is the same
  // projection, while each column is read once. A column at two positions
  // needs a copy of its expression per position.
  if (const auto* project = input->as<velox::core::ProjectNode>();
      project != nullptr && !hasDuplicateNames(sourceNames)) {
    std::vector<velox::core::TypedExprPtr> projections;
    projections.reserve(sourceNames.size());
    for (auto index : childIndices(*project->outputType(), sourceNames)) {
      projections.push_back(project->projections()[index]);
    }
    return std::make_shared<velox::core::ProjectNode>(
        nextId(),
        std::vector<std::string>{outputNames},
        std::move(projections),
        project->sources()[0]);
  }
  // Selecting and renaming columns of a Values is the same rows under other
  // names.
  if (const auto* values = input->as<velox::core::ValuesNode>()) {
    return std::make_shared<velox::core::ValuesNode>(
        nextId(),
        selectColumns(
            *values,
            childIndices(*values->outputType(), sourceNames),
            outputNames));
  }
  return wrapWithRenameProject(std::move(input), columns, outputNames);
}

velox::core::PlanNodePtr Emitter::emitUnionAll(const UnionAll& unionNode) {
  // Velox's LocalPartition requires all sources to share one output type,
  // field names included, so each leg selects the columns feeding the union's
  // positions and takes the union's names for them.
  const auto names = namesOf(unionNode.outputColumns());
  std::vector<velox::core::PlanNodePtr> sources;
  sources.reserve(unionNode.inputs().size());
  for (size_t i = 0; i < unionNode.inputs().size(); ++i) {
    NodeCP input = unionNode.inputs()[i];
    const ColumnVector& legColumns = unionNode.legColumns()[i];
    velox::core::PlanNodePtr emitted = emit(input);
    // Velox's Unnest produces a column per unnested value, while the node may
    // output only some of them, so the emitted leg can be wider than its node.
    if (emitted->outputType()->size() == legColumns.size() &&
        isIdentityLayout(*input, legColumns, names)) {
      sources.push_back(std::move(emitted));
      continue;
    }
    sources.push_back(projectAs(std::move(emitted), legColumns, names));
  }
  return velox::core::LocalPartitionNode::gather(nextId(), std::move(sources));
}

velox::core::PlanNodePtr Emitter::emitEnforceSingleRow(
    const EnforceSingleRow& node) {
  velox::core::PlanNodePtr input = emit(node.input());
  return std::make_shared<velox::core::EnforceSingleRowNode>(
      nextId(), std::move(input));
}

velox::core::PlanNodePtr Emitter::emitAssignUniqueId(
    const AssignUniqueId& node) {
  return std::make_shared<velox::core::AssignUniqueIdNode>(
      nextId(), node.idColumn()->outputName(), emit(node.input()));
}

velox::core::PlanNodePtr Emitter::emitEnforceDistinct(
    const EnforceDistinct& node) {
  velox::core::PlanNodePtr input = emit(node.input());
  auto distinctKeys =
      toFieldAccessList(node.distinctKeys(), "EnforceDistinct distinct key");
  // Stream (no hash table) over input already grouped on a prefix of the
  // distinct keys, mirroring how the aggregation derives its pre-grouped keys.
  auto preGroupedKeys = toFieldAccessList(
      computePreGroupedKeys(
          node.input()->physicalProperties().local, node.distinctKeys()),
      "EnforceDistinct pre-grouped key");
  return std::make_shared<velox::core::EnforceDistinctNode>(
      nextId(),
      std::move(distinctKeys),
      std::move(preGroupedKeys),
      node.errorMessage(),
      std::move(input));
}

// Merges two fragment-type contributions: a kSource and a kFixed coexist as
// kFixed (split-driven scans feeding a fixed-width stage); a kCoordinator
// pooled with single-task work stays kCoordinator (coordinator dominates
// single); equal types merge to themselves; nullopt is "no constraint". A
// kCoordinator with a parallel source (kSource/kFixed) is an invariant
// violation.
std::optional<FragmentType> mergeFragmentTypes(
    std::optional<FragmentType> lhs,
    std::optional<FragmentType> rhs) {
  if (!lhs.has_value()) {
    return rhs;
  }
  if (!rhs.has_value()) {
    return lhs;
  }
  if (*lhs == *rhs) {
    return lhs;
  }
  if ((*lhs == FragmentType::kSource && *rhs == FragmentType::kFixed) ||
      (*lhs == FragmentType::kFixed && *rhs == FragmentType::kSource)) {
    return FragmentType::kFixed;
  }
  if ((*lhs == FragmentType::kCoordinator && *rhs == FragmentType::kSingle) ||
      (*lhs == FragmentType::kSingle && *rhs == FragmentType::kCoordinator)) {
    return FragmentType::kCoordinator;
  }
  VELOX_FAIL(
      "Incompatible fragment-type contributions in one fragment: {} + {}",
      *lhs,
      *rhs);
}

// The fragment type the subtree at 'node' contributes, walking down to (not
// across) inner exchanges. An exchange yields its consumer-side type
// (partitioned → kFixed, gather → kSingle, broadcast/arbitrary → no
// constraint); a scan yields kSource, or kCoordinator when it reads a
// coordinator-only layout.
std::optional<FragmentType> fragmentTypeContribution(NodeCP node) {
  if (node->is(NodeType::kExchange)) {
    const auto& partitioning = node->as<Exchange>()->partitioning();
    if (partitioning.scope == PropertyScope::kDriver) {
      return fragmentTypeContribution(node->as<Exchange>()->input());
    }
    switch (partitioning.kind) {
      case PartitionKind::kPartitioned:
        return FragmentType::kFixed;
      case PartitionKind::kGather:
        return FragmentType::kSingle;
      case PartitionKind::kBroadcast:
      case PartitionKind::kArbitrary:
      case PartitionKind::kUnspecified:
        return std::nullopt;
    }
  }
  switch (node->nodeType()) {
    case NodeType::kScan:
      return node->as<Scan>()->baseTable()->layout()->runsOnCoordinator()
          ? FragmentType::kCoordinator
          : FragmentType::kSource;
    case NodeType::kValues:
    case NodeType::kFixedPoint:
      return FragmentType::kSingle;
    default: {
      std::optional<FragmentType> result;
      for (NodeCP input : node->inputs()) {
        result = mergeFragmentTypes(result, fragmentTypeContribution(input));
      }
      return result;
    }
  }
}

// Sets 'fragment.type' (and 'numRemotePartitions' for kFixed) from an
// already-computed root 'contribution'. At maxRemotePartitions == 1 all
// parallelism collapses to one task. A nullopt contribution (no split source
// below -- no scan, only exchanges and/or values) is single-task regardless of
// output partitioning: kSource requires connector splits to drive its task
// count, so the fallback is kSingle. A kFixed fragment reads a hash exchange
// and runs 'hashPartitionCount' tasks; a grouped fragment's bucket count
// replaces it later.
void decideFragmentType(
    std::optional<FragmentType> contribution,
    int32_t maxRemotePartitions,
    int32_t hashPartitionCount,
    ExecutableFragment& fragment) {
  if (maxRemotePartitions == 1) {
    fragment.type = FragmentType::kSingle;
    return;
  }
  fragment.type = contribution.value_or(FragmentType::kSingle);
  if (fragment.type == FragmentType::kFixed) {
    fragment.numRemotePartitions = hashPartitionCount;
  }
}

// Computes 'node's contribution with fragmentTypeContribution -- walking down
// to, not across, inner exchanges -- and sets the fragment type from it.
void decideFragmentType(
    NodeCP node,
    int32_t maxRemotePartitions,
    int32_t hashPartitionCount,
    ExecutableFragment& fragment) {
  decideFragmentType(
      maxRemotePartitions == 1 ? std::nullopt : fragmentTypeContribution(node),
      maxRemotePartitions,
      hashPartitionCount,
      fragment);
}

const connector::PartitionType* commonFragmentPartitionType(NodeCP node) {
  if (node->is(NodeType::kExchange)) {
    const auto* exchange = node->as<Exchange>();
    return exchange->partitioning().scope == PropertyScope::kDriver
        ? commonFragmentPartitionType(exchange->input())
        : nullptr;
  }

  const connector::PartitionType* result = node->is(NodeType::kScan)
      ? node->as<Scan>()->groupedPartitionType()
      : nullptr;
  for (NodeCP input : node->inputs()) {
    const auto* inputType = commonFragmentPartitionType(input);
    if (inputType == nullptr) {
      continue;
    }
    if (result == nullptr) {
      result = inputType;
      continue;
    }
    auto common = result->copartition(*inputType);
    VELOX_CHECK_NOT_NULL(
        common, "Co-fragmented bucketed scans must be copartitionable");
    result = queryCtx()->registerPartitionType(std::move(common));
  }
  return result;
}

void Emitter::finalizeGroupedLeaves(ExecutableFragment& fragment) {
  if (fragmentState_.groupedPartitionType == nullptr) {
    return;
  }

  // The plan outlives this optimization, so it takes the owning pointer.
  auto owned =
      queryCtx()->sharedPartitionType(fragmentState_.groupedPartitionType);
  VELOX_CHECK_NOT_NULL(
      owned, "A grouped leaf's PartitionType must be owned by the context");
  for (const auto& leaf : fragmentState_.groupedLeaves) {
    fragment.groupedNodes.emplace(
        leaf.nodeId,
        leaf.partitionType != nullptr
            ? std::const_pointer_cast<connector::PartitionType>(owned)
            : nullptr);
  }
  fragment.type = FragmentType::kFixed;
  fragment.numRemotePartitions =
      fragmentState_.groupedPartitionType->numPartitions();
}

velox::core::PlanNodePtr Emitter::emitChildFragment(
    NodeCP node,
    ExecutableFragment& fragment) {
  return emitInFragment(node, fragment, [&] { return emit(node); });
}

void Emitter::emitGatheredOutput(
    NodeCP root,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames,
    ExecutableFragment& top) {
  const bool layoutAboveGather = hasDuplicateNames(outputNames);
  const Project* rootProject =
      root->is(NodeType::kProject) ? root->as<Project>() : nullptr;
  NodeCP belowRoot =
      layoutAboveGather && rootProject != nullptr ? rootProject->input() : root;

  ExecutableFragment source = newFragment();
  decideFragmentType(
      belowRoot, options_.maxRemotePartitions, hashPartitionCount(), source);
  velox::core::PlanNodePtr sourcePlan = layoutAboveGather
      ? emitChildFragment(belowRoot, source)
      : emitInFragment(root, source, [&] {
          return emitRoot(root, outputColumns, outputNames);
        });
  fragmentState_.fragment = &top;

  const auto& serdeKind = chooseExchangeSerdeKind(*sourcePlan->outputType());
  source.fragment.planNode =
      makeSingleOutput(sourcePlan->outputType(), sourcePlan, serdeKind);
  auto gather = std::make_shared<velox::core::ExchangeNode>(
      nextId(), sourcePlan->outputType(), serdeKind);
  top.inputStages.emplace_back(gather->id(), source.fragmentId);
  stages_.push_back(std::move(source));

  if (!layoutAboveGather) {
    top.fragment.planNode = gather;
  } else if (rootProject != nullptr) {
    top.fragment.planNode =
        emitRootProjectOver(gather, *rootProject, outputColumns, outputNames);
  } else {
    top.fragment.planNode =
        wrapWithRenameProject(gather, outputColumns, outputNames);
  }
}

velox::core::PlanNodePtr Emitter::makeExchangeProducer(
    const Partitioning& partitioning,
    const velox::RowTypePtr& outputType,
    const velox::core::PlanNodePtr& sourcePlan) {
  const auto& serdeKind = chooseExchangeSerdeKind(*outputType);
  switch (partitioning.kind) {
    case PartitionKind::kBroadcast:
      return velox::core::PartitionedOutputNode::broadcast(
          nextId(),
          /*numPartitions=*/1,
          outputType,
          serdeKind,
          sourcePlan);
    case PartitionKind::kGather:
      return makeSingleOutput(outputType, sourcePlan, serdeKind);
    case PartitionKind::kArbitrary:
      return velox::core::PartitionedOutputNode::arbitrary(
          nextId(), outputType, serdeKind, sourcePlan);
    case PartitionKind::kPartitioned: {
      auto fields =
          toFieldAccessList(partitioning.keys, "Exchange partition key");

      // Every exchange entering a bucketed fragment uses that fragment's
      // common connector partitioning. Otherwise an exchange may carry its own
      // connector partitioning; with neither, use standard Velox hash.
      int32_t numPartitions = hashPartitionCount();
      velox::core::PartitionFunctionSpecPtr spec;
      const auto* partitionType = fragmentState_.groupedPartitionType != nullptr
          ? fragmentState_.groupedPartitionType
          : partitioning.partitionType;
      if (partitionType != nullptr) {
        numPartitions = partitionType->numPartitions();
        spec = connectorPartitionSpec(
            *partitionType,
            outputType,
            fields,
            /*isLocal=*/false);
      } else {
        spec = makeHashPartitionSpec(outputType, fields);
      }

      if (numPartitions == 1) {
        // A connector partitioning that scales to one destination (e.g. a
        // single-bucket table) needs no partition function: gather to one task.
        return makeSingleOutput(outputType, sourcePlan, serdeKind);
      }

      std::vector<velox::core::TypedExprPtr> keys(fields.begin(), fields.end());
      return std::make_shared<velox::core::PartitionedOutputNode>(
          nextId(),
          velox::core::PartitionedOutputNode::Kind::kPartitioned,
          keys,
          numPartitions,
          partitioning.replicateNullsAndAny,
          std::move(spec),
          outputType,
          serdeKind,
          sourcePlan);
    }
    case PartitionKind::kUnspecified:
      VELOX_UNREACHABLE("Exchange has no concrete partitioning to lower");
  }
}

velox::core::PlanNodePtr Emitter::makeExchangeConsumer(
    const Partitioning& partitioning,
    const velox::RowTypePtr& outputType) {
  const auto& serdeKind = chooseExchangeSerdeKind(*outputType);
  if (partitioning.kind == PartitionKind::kGather &&
      !partitioning.orderKeys.empty()) {
    // Order-preserving gather: the consumer merges the sorted per-task streams.
    auto [sortingKeys, sortingOrders] = toSortingKeys(
        partitioning.orderKeys, partitioning.orderTypes, "Merge exchange key");
    return std::make_shared<velox::core::MergeExchangeNode>(
        nextId(), outputType, sortingKeys, sortingOrders, serdeKind);
  }
  return std::make_shared<velox::core::ExchangeNode>(
      nextId(), outputType, serdeKind);
}

velox::core::PlanNodePtr Emitter::emitExchange(const Exchange& exchange) {
  const auto& partitioning = exchange.partitioning();

  if (partitioning.scope == PropertyScope::kDriver) {
    velox::core::PlanNodePtr input = emit(exchange.input());
    switch (partitioning.kind) {
      case PartitionKind::kGather:
        if (!partitioning.orderKeys.empty()) {
          auto [sortingKeys, sortingOrders] = toSortingKeys(
              partitioning.orderKeys,
              partitioning.orderTypes,
              "Driver merge key");
          return std::make_shared<velox::core::LocalMergeNode>(
              nextId(),
              std::move(sortingKeys),
              std::move(sortingOrders),
              std::vector<velox::core::PlanNodePtr>{std::move(input)});
        }
        return addLocalPartition(std::move(input), {});
      case PartitionKind::kPartitioned: {
        auto keys =
            toFieldAccessList(partitioning.keys, "Driver partition key");
        if (partitioning.partitionType == nullptr) {
          return addLocalPartition(std::move(input), keys);
        }
        const auto outputType = input->outputType();
        return std::make_shared<velox::core::LocalPartitionNode>(
            nextId(),
            velox::core::LocalPartitionNode::Type::kRepartition,
            /*scaleWriter=*/false,
            connectorPartitionSpec(
                *partitioning.partitionType,
                outputType,
                keys,
                /*isLocal=*/true),
            std::vector<velox::core::PlanNodePtr>{std::move(input)});
      }
      case PartitionKind::kBroadcast:
      case PartitionKind::kArbitrary:
      case PartitionKind::kUnspecified:
        VELOX_UNREACHABLE("Unsupported driver exchange kind");
    }
  }

  // Producer fragment: the exchange's input, capped with a PartitionedOutput.
  ExecutableFragment source = newFragment();
  decideFragmentType(
      exchange.input(),
      options_.maxRemotePartitions,
      hashPartitionCount(),
      source);
  velox::core::PlanNodePtr sourcePlan =
      emitChildFragment(exchange.input(), source);
  const auto outputType = sourcePlan->outputType();

  source.fragment.planNode =
      makeExchangeProducer(partitioning, outputType, sourcePlan);

  velox::core::PlanNodePtr consumer =
      makeExchangeConsumer(partitioning, outputType);
  fragmentState_.addInputStage(consumer->id(), source.fragmentId);

  // A partitioned exchange feeding the outer fragment is a group-routed leaf
  // there: if that fragment turns out bucketed, the exchange delivers per
  // group. Recorded as a null-typed leaf, since the fragment's bucketed scans
  // are what fix its width; `finalizeGroupedLeaves` drops it when there are
  // none.
  if (partitioning.kind == PartitionKind::kPartitioned) {
    fragmentState_.addGroupedLeaf(consumer->id(), nullptr);
  }

  stages_.push_back(std::move(source));
  return consumer;
}

velox::connector::ConnectorTableHandlePtr Emitter::deletedRowsHandle(
    const TableWrite& tableWrite) {
  // The scan handle is the only description of the rows to remove, so the rows
  // reaching the write must be the rows the scan reads.
  NodeCP input = tableWrite.input();
  if (input->is(NodeType::kFilter) &&
      input->as<Filter>()->input()->is(NodeType::kScan)) {
    VELOX_USER_FAIL(
        "Connector cannot apply this WHERE clause for DELETE: {}",
        input->as<Filter>()->predicates().front()->toString());
  }
  VELOX_USER_CHECK(
      input->is(NodeType::kScan),
      "DELETE requires a scan with an optional filter");

  const auto& scan = *input->as<Scan>();
  // The connector reads the filters as constraints on the table it is about to
  // change, so the two must be the same table.
  VELOX_USER_CHECK(
      scan.baseTable()->schemaTable->connectorTable == tableWrite.table(),
      "DELETE scans the wrong table: deletes {}, scans {}",
      tableWrite.table()->name().toString(),
      scan.baseTable()->schemaTable->connectorTable->name().toString());

  VELOX_CHECK_NOT_NULL(
      scan.scanHandle(), "Scan reaches emit without a connector handle");
  return scan.scanHandle()->tableHandle;
}

velox::core::PlanNodePtr Emitter::emitTableWrite(const TableWrite& tableWrite) {
  const bool isDelete = tableWrite.kind() == connector::WriteKind::kDelete;

  // A DELETE over an empty input removes no rows. It reports a zero count
  // without starting a write.
  if (isDelete && tableWrite.input()->is(NodeType::kValues) &&
      tableWrite.input()->as<Values>()->cardinality() == 0) {
    return std::make_shared<velox::core::ValuesNode>(
        nextId(),
        std::vector<velox::RowVectorPtr>{
            std::static_pointer_cast<velox::RowVector>(
                velox::BaseVector::createFromVariants(
                    makeRowType(tableWrite.outputColumns()),
                    {velox::Variant::row({velox::Variant(int64_t{0})})},
                    evaluator_.pool()))});
  }

  const auto& table = *tableWrite.table();
  auto* layout = table.layouts().front();
  const auto& connectorId = layout->connector()->connectorId();
  auto metadata = connector::ConnectorMetadataRegistry::get(connectorId);
  auto connectorSession = session_.context()->sessionFor(connectorId);

  auto handle = metadata->beginWrite(
      connectorSession,
      table.shared_from_this(),
      tableWrite.kind(),
      isDelete ? deletedRowsHandle(tableWrite) : nullptr,
      session_.options().explain);

  if (isDelete) {
    if (handle->veloxHandle() != nullptr) {
      VELOX_NYI(
          "Row-level delete is not supported: {}", table.name().toString());
    }

    VELOX_CHECK(!finishWrite_, "Only one TableWrite per query is supported");
    finishWrite_ = FinishWrite{
        metadata, connectorId, std::move(connectorSession), std::move(handle)};
    return nullptr;
  }

  // Parallelize writers only when the input is already distributed; gathering
  // an already-single-task input would add a redundant exchange.
  const bool distributed = options_.maxRemotePartitions > 1 &&
      fragmentTypeContribution(tableWrite.input()) != FragmentType::kSingle;

  std::optional<FragmentState> rootState;
  ExecutableFragment writerFragment;
  if (distributed) {
    writerFragment = newFragment();
    decideFragmentType(
        tableWrite.input(),
        options_.maxRemotePartitions,
        hashPartitionCount(),
        writerFragment);
    rootState.emplace(std::move(fragmentState_));
    fragmentState_ = {
        .fragment = &writerFragment,
        .groupedPartitionType = commonFragmentPartitionType(tableWrite.input()),
    };
  } else {
    fragmentState_.groupedPartitionType =
        commonFragmentPartitionType(tableWrite.input());
  }

  velox::core::PlanNodePtr input = emit(tableWrite.input());

  // A bucketed write reads a bucket exchange (physical planning inserts one
  // unless the source is already co-bucketed), and its writer task count must
  // equal that exchange's partition count: one task per bucket group, each
  // producing one bucket file. The fragment runs no grouped scan, so this is a
  // task count, not grouped execution.
  if (distributed && !layout->partitionColumns().empty() &&
      tableWrite.input()->is(NodeType::kExchange)) {
    const auto& exchangeType =
        tableWrite.input()->physicalProperties().globalPartition.partitionType;
    if (exchangeType != nullptr) {
      writerFragment.type = FragmentType::kFixed;
      writerFragment.numRemotePartitions = exchangeType->numPartitions();
    }
  }

  const auto& inputType = input->outputType();

  // A single-fragment write over a single-threaded pipeline (e.g. a Values
  // source) has one writer producing all rows, so the per-driver stats need no
  // merge. Reporting one driver keeps the plan a bare TableWrite.
  const int32_t writerNumDrivers =
      !distributed && isSingleThreadedPipeline(input)
      ? 1
      : options_.maxLocalPartitions;
  WriteStatsBuilder statsBuilder(
      table,
      inputType,
      *handle,
      writerNumDrivers,
      distributed ? options_.maxRemotePartitions : 1);
  std::optional<velox::core::ColumnStatsSpec> writeStatsSpec;
  if (statsBuilder.hasStats()) {
    writeStatsSpec = statsBuilder.writeSpec();
  }
  auto writeOutputType = writeStatsSpec.has_value()
      ? velox::core::TableWriteTraits::outputType(writeStatsSpec)
      : handle->resultType();

  VELOX_CHECK(!finishWrite_, "Only one TableWrite per query is supported");
  auto insertTableHandle =
      std::make_shared<const velox::core::InsertTableHandle>(
          connectorId,
          handle->veloxHandle(),
          /*notNullColumns=*/folly::F14FastSet<std::string>{});
  finishWrite_ = FinishWrite{
      metadata,
      connectorId,
      std::move(connectorSession),
      std::move(handle),
      statsBuilder.statsMapping()};

  velox::core::PlanNodePtr result =
      std::make_shared<velox::core::TableWriteNode>(
          nextId(),
          inputType,
          table.type()->names(),
          std::move(writeStatsSpec),
          std::move(insertTableHandle),
          /*hasPartitioningScheme=*/false,
          std::move(writeOutputType),
          velox::connector::CommitStrategy::kNoCommit,
          std::move(input));

  // Combine the per-driver write stats within a worker.
  if (statsBuilder.needsMerge()) {
    result = velox::core::LocalPartitionNode::gather(
        nextId(), std::vector<velox::core::PlanNodePtr>{result});
    auto localMergeSpec = statsBuilder.localMergeSpec(result->outputType());
    auto localMergeOutputType =
        velox::core::TableWriteTraits::outputType(localMergeSpec);
    result = std::make_shared<velox::core::TableWriteMergeNode>(
        nextId(),
        std::move(localMergeOutputType),
        std::move(localMergeSpec),
        std::move(result));
  }

  if (!distributed) {
    return result;
  }

  // Gather each worker's intermediate stats to the single root fragment.
  finalizeGroupedLeaves(writerFragment);
  const auto& serdeKind = chooseExchangeSerdeKind(*result->outputType());
  writerFragment.fragment.planNode =
      makeSingleOutput(result->outputType(), result, serdeKind);
  fragmentState_ = std::move(*rootState);
  auto gather = std::make_shared<velox::core::ExchangeNode>(
      nextId(), result->outputType(), serdeKind);
  fragmentState_.addInputStage(gather->id(), writerFragment.fragmentId);
  stages_.push_back(std::move(writerFragment));

  if (!statsBuilder.needsFinalMerge()) {
    return gather;
  }

  // The coordinator merges the per-worker intermediates into final scalar
  // stats.
  auto finalMergeSpec = statsBuilder.finalMergeSpec(gather->outputType());
  auto finalMergeOutputType =
      velox::core::TableWriteTraits::outputType(finalMergeSpec);
  return std::make_shared<velox::core::TableWriteMergeNode>(
      nextId(),
      std::move(finalMergeOutputType),
      std::move(finalMergeSpec),
      std::move(gather));
}

velox::core::PlanNodePtr Emitter::emitFixedPoint(const FixedPoint& fixedPoint) {
  if (options_.maxRemotePartitions > 1) {
    VELOX_NYI("Distributed FixedPoint execution is not yet implemented");
  }

  auto anchorPlan = emit(fixedPoint.anchor());

  VELOX_CHECK_EQ(
      fixedPoint.recursiveNumDrivers().value_or(0),
      1,
      "FixedPoint must be physically planned for one recursive driver before emission");
  const int32_t previousMaxLocalPartitions = std::exchange(
      options_.maxLocalPartitions, *fixedPoint.recursiveNumDrivers());
  SCOPE_EXIT {
    options_.maxLocalPartitions = previousMaxLocalPartitions;
  };
  auto stepPlan = emit(fixedPoint.step());

  auto schema = makeRowType(fixedPoint.outputColumns());
  const std::string stateName{fixedPoint.name()};

  auto convergence = velox::core::ConvergenceConfig::converging(
      emit(fixedPoint.convergence()), fixedPoint.maxIterations());

  std::vector<velox::core::StateDeclarationPtr> stateDeclarations;
  stateDeclarations.push_back(
      std::make_shared<velox::core::VectorStateDeclaration>(
          stateName, schema, std::move(anchorPlan), /*append=*/true));
  std::vector<velox::core::PlanNodePtr> plans;
  plans.push_back(std::move(stepPlan));

  return std::make_shared<velox::core::FixedPointNode>(
      nextId(),
      std::move(stateDeclarations),
      std::move(plans),
      std::move(convergence),
      stateName);
}

velox::core::PlanNodePtr Emitter::emitWorkingTable(
    const WorkingTable& workingTable) {
  auto schema = makeRowType(workingTable.outputColumns());
  const bool delta =
      workingTable.readMode() == WorkingTableReadMode::kLatestDelta;
  return std::make_shared<velox::core::StateSourceNode>(
      nextId(), std::string{workingTable.name()}, schema, delta);
}

namespace {

// Rejects fragments that would require one task to orchestrate multiple
// independent fixed points. Fixed points isolated in separate fragments remain
// valid.
void validateFixedPointLeaves(const ExecutableFragment& fragment) {
  const auto& root = fragment.fragment.planNode;
  int32_t numFixedPoints{0};
  for (const auto& leafId : root->leafPlanNodeIds()) {
    const auto* leaf = velox::core::PlanNode::findNodeById(root.get(), leafId);
    VELOX_CHECK_NOT_NULL(leaf);
    if (leaf->is<velox::core::FixedPointNode>()) {
      ++numFixedPoints;
    }
  }
  if (numFixedPoints > 1) {
    VELOX_NYI(
        "Multiple FixedPoint nodes in one execution fragment are not yet supported: {}",
        numFixedPoints);
  }
}

} // namespace

std::vector<ExecutableFragment> Emitter::emitFragments(
    NodeCP root,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames) {
  ExecutableFragment top = newFragment();
  top.type = FragmentType::kSingle;

  // The output fragment's root for the consistency check, whose output names
  // (the query's) may repeat for user aliases and so are validated as the plan
  // root. For remote output this is the input of the final PartitionedOutput;
  // for the other cases it defaults to the output fragment root below.
  velox::core::PlanNodePtr outputProjection;

  if (root->is(NodeType::kTableWrite)) {
    // A table write is a root sink: emit it (and its gathered input) as the
    // single root fragment. Its output is the write-stats row the runner
    // interprets via FinishWrite, not a user column layout, so it takes no
    // output-rename projection. Like the other roots, cap the output fragment
    // with a single PartitionedOutput when results are consumed remotely, so
    // the runner reads the stats rows from a PartitionedOutput as it does for
    // every other fragment; otherwise emit the write directly.
    fragmentState_ = {.fragment = &top};
    velox::core::PlanNodePtr writePlan = emit(root);
    if (writePlan == nullptr) {
      // The connector carries out the write itself, so there is nothing to
      // execute.
      fragmentState_ = {};
      return std::move(stages_);
    }
    if (options_.remoteOutput) {
      outputProjection = writePlan;
      top.fragment.planNode = makeClientOutput(writePlan);
    } else {
      top.fragment.planNode = writePlan;
    }
  } else {
    // A root whose subtree already runs on one task needs no output gather and
    // is emitted directly, like the single-node case: below a global ORDER BY
    // or aggregate (kSingle), or a coordinator-only scan (kCoordinator). Only a
    // multi-task root is gathered to a single task for the query output.
    // The root's fragment-type contribution drives both the output-gather
    // decision and the root fragment's type; compute it once and reuse it.
    std::optional<FragmentType> rootType;
    if (options_.maxRemotePartitions > 1) {
      rootType = fragmentTypeContribution(root);
    }
    const bool gatherForOutput = options_.maxRemotePartitions > 1 &&
        rootType != FragmentType::kSingle &&
        rootType != FragmentType::kCoordinator;

    if (options_.remoteOutput) {
      // Results stay distributed: emit the root as a single fragment capped by
      // a PartitionedOutput for remote consumption, with no gather consumer
      // fragment. The fragment type follows the root's contents (kFixed for a
      // multi-worker source, kCoordinator for a coordinator-only scan, kSingle
      // at maxRemotePartitions == 1 or when there is no split source below).
      fragmentState_ = {
          .fragment = &top,
          .groupedPartitionType = commonFragmentPartitionType(root),
      };
      decideFragmentType(
          rootType, options_.maxRemotePartitions, hashPartitionCount(), top);
      outputProjection = emitRoot(root, outputColumns, outputNames);
      top.fragment.planNode = makeClientOutput(outputProjection);
    } else if (gatherForOutput) {
      fragmentState_ = {.fragment = &top};
      emitGatheredOutput(root, outputColumns, outputNames, top);
    } else {
      fragmentState_ = {
          .fragment = &top,
          .groupedPartitionType = commonFragmentPartitionType(root),
      };
      decideFragmentType(
          rootType, options_.maxRemotePartitions, hashPartitionCount(), top);
      top.fragment.planNode = emitRoot(root, outputColumns, outputNames);
    }
  }

  finalizeGroupedLeaves(top);
  fragmentState_ = {};

  // Validate the producer fragments and the output fragment. The output
  // fragment is validated at 'outputProjection' so its output names, which may
  // repeat for user aliases, are allowed as the plan root.
  if (outputProjection == nullptr) {
    outputProjection = top.fragment.planNode;
  }

  for (const auto& fragment : stages_) {
    validateFixedPointLeaves(fragment);
    velox::core::PlanConsistencyChecker::check(fragment.fragment.planNode);
  }
  validateFixedPointLeaves(top);
  velox::core::PlanConsistencyChecker::check(outputProjection);

  stages_.push_back(std::move(top));
  return std::move(stages_);
}

} // namespace

EmitPass::Result EmitPass::run(
    NodeCP root,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator,
    const MultiFragmentPlan::Options& options) {
  VELOX_CHECK_EQ(outputColumns.size(), outputNames.size());
  Emitter emitter(session, evaluator, options);
  auto fragments = emitter.emitFragments(root, outputColumns, outputNames);
  return Result{
      std::move(fragments),
      emitter.takeFinishWrite(),
      emitter.takePrediction()};
}

} // namespace facebook::axiom::optimizer::v2
