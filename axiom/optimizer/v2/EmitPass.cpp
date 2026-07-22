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
#include "velox/core/PlanConsistencyChecker.h"
#include "velox/core/TableWriteTraits.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/expression/ExprConstants.h"
#include "velox/serializers/PrestoSerializer.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

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

velox::core::FieldAccessTypedExprPtr toFieldAccess(ColumnCP column) {
  return std::make_shared<velox::core::FieldAccessTypedExpr>(
      toTypePtr(column->value().type), column->outputName());
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

// True when 'values' pass 'input' through unchanged — each value is input
// column i in order — so the table write can read 'input' directly. The write
// node names its output columns itself, so a rename-only projection is
// redundant: this lets the write reuse a source projection (e.g. the SELECT of
// a CTAS, whose columns may be named `expr`/`expr_0`) instead of stacking one.
bool writeInputIsIdentity(
    const velox::RowTypePtr& inputType,
    const std::vector<velox::core::TypedExprPtr>& values) {
  if (values.size() != inputType->size()) {
    return false;
  }
  for (size_t i = 0; i < values.size(); ++i) {
    if (!values[i]->isFieldAccessKind()) {
      return false;
    }
    const auto* field =
        values[i]->asUnchecked<velox::core::FieldAccessTypedExpr>();
    if (!field->isInputColumn() || field->name() != inputType->nameOf(i)) {
      return false;
    }
  }
  return true;
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

class Emitter {
 public:
  Emitter(
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator,
      ScanHandleCache& scanHandles,
      const MultiFragmentPlan::Options& options)
      : session_{session},
        evaluator_{evaluator},
        exprEmitter_{evaluator.pool()},
        scanHandles_{scanHandles},
        options_{options} {}

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
    if (estimate.cardinality.has_value()) {
      prediction_[plan->id()] =
          NodePrediction{.cardinality = *estimate.cardinality};
    }
  }

  // Local exchange inserted below a final/single aggregation at numDrivers > 1
  // so every row of a group is processed by one driver: a repartition on the
  // grouping keys, or a gather to one driver when there are none.
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
  velox::core::PlanNodePtr emitSort(const Sort& sort);
  velox::core::PlanNodePtr emitTopN(const TopN& topN);
  velox::core::PlanNodePtr emitLimit(const Limit& limit);
  velox::core::PlanNodePtr emitValues(const Values& values);
  velox::core::PlanNodePtr emitWindow(const Window& window);
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

  // Emits 'exchange's input as a producer fragment ending in a
  // `PartitionedOutput`, and returns the consumer-side `ExchangeNode` for the
  // current fragment, wiring the `InputStage` between them.
  velox::core::PlanNodePtr emitExchange(const Exchange& exchange);
  velox::core::PlanNodePtr emitTableWrite(const TableWrite& tableWrite);

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
      const velox::core::PlanNodePtr& sourcePlan) {
    return velox::core::PartitionedOutputNode::single(
        nextId(),
        outputType,
        exchangeSerdeKind_,
        std::string{velox::core::TransportKind::kInMemory},
        sourcePlan);
  }

  // Builds the consumer-side exchange node for 'partitioning' — a
  // `MergeExchange` for an order-preserving gather, else a plain `Exchange`.
  velox::core::PlanNodePtr makeExchangeConsumer(
      const Partitioning& partitioning,
      const velox::RowTypePtr& outputType);

  // Runs 'emitFn' with 'fragment' as the current fragment, isolating its
  // grouped leaves and finalizing them onto 'fragment', then restores the
  // enclosing fragment and its leaves. Returns the plan 'emitFn' produced. For
  // a child fragment whose grouped leaves must be finalized after further
  // building (a table write), do the save/restore inline instead.
  template <typename EmitFn>
  velox::core::PlanNodePtr emitInFragment(
      ExecutableFragment& fragment,
      EmitFn&& emitFn) {
    ExecutableFragment* outer = currentFragment_;
    auto outerGroupedLeaves = std::move(groupedLeaves_);
    SCOPE_EXIT {
      currentFragment_ = outer;
      groupedLeaves_ = std::move(outerGroupedLeaves);
    };
    groupedLeaves_.clear();
    currentFragment_ = &fragment;
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

  // A fresh producer/consumer fragment with a unique task prefix.
  ExecutableFragment newFragment() {
    return ExecutableFragment{
        .taskPrefix = fmt::format("fragment{}", ++fragmentCounter_)};
  }

  const OptimizerSession& session_;
  velox::core::ExpressionEvaluator& evaluator_;
  ExprEmitter exprEmitter_;
  ScanHandleCache& scanHandles_;
  const MultiFragmentPlan::Options& options_;
  int32_t nextNodeId_{0};

  // The exchange serialization format for remote
  // `PartitionedOutput`/`Exchange`.
  const std::string exchangeSerdeKind_{
      velox::serializer::presto::PrestoVectorSerde::name()};

  // Producer fragments collected as `ir.Exchange`es are lowered; the root
  // fragment is appended last by `emitFragments`.
  std::vector<ExecutableFragment> stages_;
  // The fragment currently being built; `emitExchange` records its input
  // stages here. Set during the fragment-aware walk.
  ExecutableFragment* currentFragment_{nullptr};
  int32_t fragmentCounter_{0};

  // A grouped leaf of the fragment currently being built: a source that runs
  // per bucket-group under grouped execution. `partitionType` is the
  // connector's (unscaled) partitioning for a bucketed scan, or null for a
  // hash-partitioned consumer exchange that must route by group. Saved and
  // restored around each child-fragment build so leaves don't leak across
  // fragments.
  struct PendingGroupedLeaf {
    velox::core::PlanNodeId nodeId;
    const connector::PartitionType* partitionType{nullptr};
  };
  std::vector<PendingGroupedLeaf> groupedLeaves_;

  // Turns the accumulated `groupedLeaves_` into `fragment.groupedNodes` and,
  // when any bucketed scan is present, makes the fragment `kFixed` at the
  // folded bucket count. A fragment with no bucketed scan is left non-grouped.
  void finalizeGroupedLeaves(ExecutableFragment& fragment);

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
  std::vector<std::string> names;
  std::vector<velox::core::TypedExprPtr> exprs;
  names.reserve(keepColumns.size());
  exprs.reserve(keepColumns.size());
  for (ColumnCP column : keepColumns) {
    names.emplace_back(column->outputName());
    exprs.push_back(toFieldAccess(column));
  }
  return std::make_shared<velox::core::ProjectNode>(
      nextId(), std::move(names), std::move(exprs), std::move(input));
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

// Collects names of every input-column `FieldAccessTypedExpr` reachable
// from `expr`, excluding references bound by an enclosing lambda. Used
// to compute the columns a rejected filter reads, so the scan's
// projected schema can include them.
//
// `LambdaTypedExpr` stores its body outside `inputs()`; descend into it
// explicitly so outer-column captures inside higher-order filters
// (`array_filter`, `any_match`, ...) are picked up. The lambda's
// signature parameters are removed before recursing so a field access
// to a bound name (e.g. `x` in `array_filter(arr, x -> x > 0)`) isn't
// mistaken for a scan-column reference.
void collectInputColumnNamesImpl(
    const velox::core::TypedExprPtr& expr,
    folly::F14FastSet<std::string>& names,
    const folly::F14FastSet<std::string>& boundNames) {
  if (expr->isFieldAccessKind()) {
    const auto* field = expr->asUnchecked<velox::core::FieldAccessTypedExpr>();
    if (field->isInputColumn() && !boundNames.contains(field->name())) {
      names.insert(field->name());
    }
  } else if (expr->isLambdaKind()) {
    const auto* lambda = expr->asUnchecked<velox::core::LambdaTypedExpr>();
    folly::F14FastSet<std::string> childBound = boundNames;
    for (const auto& paramName : lambda->signature()->names()) {
      childBound.insert(paramName);
    }
    collectInputColumnNamesImpl(lambda->body(), names, childBound);
  }
  for (const auto& input : expr->inputs()) {
    collectInputColumnNamesImpl(input, names, boundNames);
  }
}

void collectInputColumnNames(
    const velox::core::TypedExprPtr& expr,
    folly::F14FastSet<std::string>& names) {
  collectInputColumnNamesImpl(expr, names, /*boundNames=*/{});
}

// The connector partitioning of a scan's table, or null when the table is not
// bucketed. A property of the table layout, independent of which columns the
// scan projects: a bucketed table runs grouped even when its bucket column is
// pruned from the output.
const connector::PartitionType* tablePartitionType(const Scan& scan) {
  const auto* schemaTable = scan.baseTable()->schemaTable;
  if (schemaTable == nullptr || schemaTable->columnGroups.empty()) {
    return nullptr;
  }
  const connector::TableLayout* layout = schemaTable->columnGroups[0]->layout;
  return layout != nullptr ? layout->partitionType().get() : nullptr;
}

velox::core::PlanNodePtr Emitter::emitScan(const Scan& scan) {
  // The read schema is the consumer output columns followed by the filter-only
  // columns, with columnHandles aligned to that order. Consumer columns keep
  // their `outputName` (alias) in the scan's row type; the rename happens
  // inside the scan via `assignments[outputName] = handle`. Filter-only
  // columns keep their schema name.
  const ScanHandle* cached = scanHandles_.find(scan);
  ScanHandle built;
  if (cached == nullptr) {
    built = ScanHandle::build(scan, session_, evaluator_);
  }
  const ScanHandle& handle = cached != nullptr ? *cached : built;
  const ColumnVector& consumerColumns = scan.outputColumns();
  const ColumnVector& filterOnlyColumns = handle.filterOnlyColumns;
  const auto& columnHandles = handle.columnHandles;
  const auto& tableHandle = handle.tableHandle;
  std::vector<velox::core::TypedExprPtr> rejectedFilters =
      handle.rejectedFilters;

  // Scan row type keeps consumer columns (named by outputName) plus
  // any filter-only column actually referenced by a rejected filter.
  // Columns referenced only by accepted filters were read by the
  // connector but stay off the projected row.
  folly::F14FastSet<std::string> rejectedFilterRefs;
  for (const auto& rejected : rejectedFilters) {
    collectInputColumnNames(rejected, rejectedFilterRefs);
  }

  std::vector<std::string> scanOutputNames;
  std::vector<velox::TypePtr> scanOutputTypes;
  velox::connector::ColumnHandleMap assignments;
  scanOutputNames.reserve(consumerColumns.size() + filterOnlyColumns.size());
  scanOutputTypes.reserve(consumerColumns.size() + filterOnlyColumns.size());
  for (size_t i = 0; i < consumerColumns.size(); ++i) {
    ColumnCP column = consumerColumns[i];
    std::string outputName{column->outputName()};
    scanOutputTypes.push_back(toTypePtr(column->value().type));
    assignments[outputName] = columnHandles[i];
    scanOutputNames.push_back(std::move(outputName));
  }
  for (size_t i = 0; i < filterOnlyColumns.size(); ++i) {
    ColumnCP column = filterOnlyColumns[i];
    std::string schemaName{column->name()};
    if (!rejectedFilterRefs.contains(schemaName)) {
      continue;
    }
    scanOutputTypes.push_back(toTypePtr(column->value().type));
    assignments[schemaName] = columnHandles[consumerColumns.size() + i];
    scanOutputNames.push_back(std::move(schemaName));
  }

  auto scanNode = std::make_shared<velox::core::TableScanNode>(
      nextId(),
      velox::ROW(std::move(scanOutputNames), std::move(scanOutputTypes)),
      tableHandle,
      std::move(assignments));

  // A scan of a bucketed table is a grouped leaf: it runs per bucket-group
  // under grouped execution, tagging the fragment as bucketed.
  if (const auto* partitionType = tablePartitionType(scan)) {
    groupedLeaves_.push_back({scanNode->id(), partitionType});
  }

  // TABLESAMPLE SYSTEM: the split source emits each split with this
  // probability. Recorded per scan so split generation can sample.
  if (scan.baseTable()->sampledPercentage.has_value()) {
    currentFragment_->sampledScans.emplace(
        scanNode->id(), *scan.baseTable()->sampledPercentage);
  }

  velox::core::PlanNodePtr result = std::move(scanNode);

  if (!rejectedFilters.empty()) {
    velox::core::TypedExprPtr predicate = rejectedFilters.size() == 1
        ? rejectedFilters.front()
        : std::make_shared<velox::core::CallTypedExpr>(
              velox::BOOLEAN(),
              std::move(rejectedFilters),
              velox::expression::kAnd);

    std::unordered_map<std::string, velox::core::TypedExprPtr> rejectedRename;
    for (ColumnCP column : consumerColumns) {
      if (column->name() != column->outputName()) {
        rejectedRename.emplace(
            std::string{column->name()}, toFieldAccess(column));
      }
    }

    if (!rejectedRename.empty()) {
      predicate = predicate->rewriteInputNames(rejectedRename);
    }
    result = std::make_shared<velox::core::FilterNode>(
        nextId(), std::move(predicate), std::move(result));
  }

  return result;
}

velox::core::PlanNodePtr Emitter::emitFilter(const Filter& filter) {
  velox::core::PlanNodePtr input = emit(filter.input());
  return std::make_shared<velox::core::FilterNode>(
      nextId(), exprEmitter_.makeAnd(filter.predicates()), std::move(input));
}

velox::core::PlanNodePtr Emitter::emitProject(const Project& project) {
  velox::core::PlanNodePtr input = emit(project.input());
  const auto& columns = project.outputColumns();
  std::vector<std::string> names;
  names.reserve(columns.size());
  for (ColumnCP column : columns) {
    names.push_back(column->outputName());
  }
  auto typedExprs = exprEmitter_.toTypedExprs(project.exprs());
  return std::make_shared<velox::core::ProjectNode>(
      nextId(), std::move(names), std::move(typedExprs), std::move(input));
}

velox::core::PlanNodePtr Emitter::emitRootProjectOver(
    velox::core::PlanNodePtr input,
    const Project& project,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames) {
  const auto& projectColumns = project.outputColumns();
  const auto& projectExprs = project.exprs();
  std::vector<velox::core::TypedExprPtr> typedExprs;
  typedExprs.reserve(outputColumns.size());
  for (ColumnCP column : outputColumns) {
    const auto it =
        std::find(projectColumns.begin(), projectColumns.end(), column);
    VELOX_CHECK(
        it != projectColumns.end(),
        "Output column not produced by root Project: {}",
        column->outputName());
    const size_t index = std::distance(projectColumns.begin(), it);
    typedExprs.push_back(exprEmitter_.toTypedExpr(projectExprs[index]));
  }
  return std::make_shared<velox::core::ProjectNode>(
      nextId(),
      std::vector<std::string>{outputNames},
      std::move(typedExprs),
      std::move(input));
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
  // Emit via emitNode, not emit, so the root's estimate is recorded once below
  // against the outermost node (the trim/rename projection when one wraps it),
  // rather than also against the inner node emit() would key it to.
  velox::core::PlanNodePtr result;
  if (isIdentityLayout(*node, outputColumns, outputNames)) {
    result = emitNode(node);
    // A scan with a rejected filter emits the filter's columns for the
    // FilterNode; trim them so they do not leak into the query output.
    if (result->outputType()->size() > outputColumns.size()) {
      result = projectToColumns(outputColumns, std::move(result));
    }
  } else if (node->is(NodeType::kProject)) {
    result = emitRootProject(*node->as<Project>(), outputColumns, outputNames);
  } else {
    result = wrapWithRenameProject(emitNode(node), outputColumns, outputNames);
  }

  // A trim or rename projection is cardinality-neutral, so the root's estimate
  // annotates whichever node ends up outermost, so EXPLAIN shows it on top.
  recordPrediction(node, result);
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

  // At numDrivers > 1 a group's rows must all reach one driver. Skip the local
  // exchange when the input is pre-grouped on the keys: a local grouping
  // guarantees driver-confinement by contract (see `LocalProperty` in
  // PhysicalProperties.h), so the aggregation streams correctly per driver.
  if (options_.numDrivers > 1 && preGroupedKeys.empty()) {
    input = addLocalPartition(std::move(input), groupingKeys);
  }

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

  // At numDrivers > 1 the Final's input rows are spread across drivers (read
  // round-robin from a remote exchange, or straight from the Partial in a
  // local-only split), so a local exchange co-partitions each group onto one
  // driver before the merge.
  if (options_.numDrivers > 1) {
    input = addLocalPartition(std::move(input), groupingKeys);
  }

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

  std::vector<std::string> markerNames;
  markerNames.reserve(markDistinct.markers().size());
  for (ColumnCP marker : markDistinct.markers()) {
    markerNames.emplace_back(marker->outputName());
  }

  std::vector<velox::core::FieldAccessTypedExprPtr> distinctKeys =
      toFieldAccessList(markDistinct.distinctKeys(), "MarkDistinct key");

  // At numDrivers > 1 every row of a distinct-key tuple must reach one driver,
  // else each driver marks the tuple as first-seen and the distinct count
  // over-counts. Co-partition the input on the distinct keys.
  if (options_.numDrivers > 1) {
    input = addLocalPartition(std::move(input), distinctKeys);
  }

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

  // PrecomputeProjections guarantees each key is a FieldAccess-shaped
  // Column ref by this point.
  velox::core::TypedExprPtr filter;
  if (!join.filter().empty()) {
    filter = exprEmitter_.makeAnd(join.filter());
  }

  if (!join.leftKeys().empty()) {
    auto leftKeys = toFieldAccessList(join.leftKeys(), "Join leftKey");
    auto rightKeys = toFieldAccessList(join.rightKeys(), "Join rightKey");

    return std::make_shared<velox::core::HashJoinNode>(
        nextId(),
        join.joinType(),
        join.nullAware(),
        std::move(leftKeys),
        std::move(rightKeys),
        filter,
        std::move(left),
        std::move(right),
        makeRowType(join.outputColumns()),
        /*useHashTableCache=*/false,
        join.nullAsValue());
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
    return emitSemiAntiViaLeftSemiProject(
        join, std::move(left), std::move(right), filter);
  }

  return std::make_shared<velox::core::NestedLoopJoinNode>(
      nextId(),
      joinType,
      filter,
      std::move(left),
      std::move(right),
      makeRowType(join.outputColumns()));
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
  // With multiple drivers each driver sorts its rows (a partial sort) and a
  // LocalMerge combines the per-driver runs into one sorted stream. With a
  // single driver a plain final sort suffices.
  const bool multiDriver = options_.numDrivers > 1;
  auto ordered = std::make_shared<velox::core::OrderByNode>(
      nextId(),
      sortingKeys,
      sortingOrders,
      /*isPartial=*/multiDriver,
      std::move(input));
  if (!multiDriver) {
    return ordered;
  }
  return std::make_shared<velox::core::LocalMergeNode>(
      nextId(),
      sortingKeys,
      sortingOrders,
      std::vector<velox::core::PlanNodePtr>{std::move(ordered)});
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
  // At numDrivers > 1 each driver keeps its own top rows (a partial top-n); a
  // gather to one driver then re-applies the top-n across the merged outputs.
  if (options_.numDrivers > 1) {
    input = std::make_shared<velox::core::TopNNode>(
        nextId(),
        sortingKeys,
        sortingOrders,
        static_cast<int32_t>(topNCount),
        /*isPartial=*/true,
        std::move(input));
    input =
        velox::core::LocalPartitionNode::gather(nextId(), {std::move(input)});
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
  // At numDrivers > 1 each driver keeps its own top offset+count rows (a
  // partial limit); a gather to one driver then applies the final offset/count
  // so the limit is enforced across the task, not per driver.
  if (options_.numDrivers > 1) {
    input = std::make_shared<velox::core::LimitNode>(
        nextId(),
        /*offset=*/0,
        limit.offset() + limit.count(),
        /*isPartial=*/true,
        std::move(input));
    input =
        velox::core::LocalPartitionNode::gather(nextId(), {std::move(input)});
  }
  return std::make_shared<velox::core::LimitNode>(
      nextId(),
      limit.offset(),
      limit.count(),
      /*isPartial=*/false,
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

  // At numDrivers > 1 each partition must be complete in one driver:
  // repartition on the PARTITION BY keys, or gather when the window spans the
  // whole input.
  if (options_.numDrivers > 1) {
    input = addLocalPartition(std::move(input), partitionKeys);
  }

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

velox::core::PlanNodePtr Emitter::emitTopNRowNumber(const TopNRowNumber& node) {
  velox::core::PlanNodePtr input = emit(node.input());
  auto partitionKeys =
      toFieldAccessList(node.partitionKeys(), "TopNRowNumber partition key");
  // At numDrivers > 1 each partition must be complete in one driver:
  // repartition on the partition keys, or gather when there are none.
  if (options_.numDrivers > 1) {
    input = addLocalPartition(std::move(input), partitionKeys);
  }
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

  std::vector<std::string> unnestNames;
  for (const auto& perExpr : unnest.unnestColumns()) {
    for (ColumnCP column : perExpr) {
      unnestNames.push_back(column->outputName());
    }
  }

  std::optional<std::string> ordinalityName;
  if (unnest.ordinalityColumn() != nullptr) {
    ordinalityName = unnest.ordinalityColumn()->outputName();
  }

  return std::make_shared<velox::core::UnnestNode>(
      nextId(),
      std::move(replicateVariables),
      std::move(unnestVariables),
      std::move(unnestNames),
      ordinalityName,
      /*markerName=*/std::nullopt,
      std::move(input));
}

velox::core::PlanNodePtr Emitter::emitUnionAll(const UnionAll& unionNode) {
  // `LocalPartitionNode` requires every source to share an identical output
  // type. Each leg's IR `outputColumns` may be narrower than the union's
  // width (legs collapse dup outputs independently), so always wrap each leg
  // in a per-leg ProjectNode that selects via `legColumns[i][j]` (a Column*
  // in the leg's outputColumns) and renames to the union's output names.
  const auto& outputColumns = unionNode.outputColumns();
  const auto& legColumns = unionNode.legColumns();
  const auto numOutputs = outputColumns.size();
  std::vector<velox::core::PlanNodePtr> sources;
  sources.reserve(unionNode.inputs().size());
  for (size_t legIdx = 0; legIdx < unionNode.inputs().size(); ++legIdx) {
    NodeCP input = unionNode.inputs()[legIdx];
    velox::core::PlanNodePtr emittedInput = emit(input);
    const auto& legCols = legColumns[legIdx];
    std::vector<std::string> names;
    std::vector<velox::core::TypedExprPtr> exprs;
    names.reserve(numOutputs);
    exprs.reserve(numOutputs);
    for (size_t j = 0; j < numOutputs; ++j) {
      names.push_back(outputColumns[j]->outputName());
      exprs.push_back(toFieldAccess(legCols[j]));
    }
    sources.push_back(
        std::make_shared<velox::core::ProjectNode>(
            nextId(),
            std::move(names),
            std::move(exprs),
            std::move(emittedInput)));
  }
  return velox::core::LocalPartitionNode::gather(nextId(), std::move(sources));
}

velox::core::PlanNodePtr Emitter::emitEnforceSingleRow(
    const EnforceSingleRow& node) {
  velox::core::PlanNodePtr input = emit(node.input());
  // Asserting at most one row is a global check; at numDrivers > 1 gather to
  // one driver first so the count is across the whole task, not per driver.
  if (options_.numDrivers > 1) {
    input =
        velox::core::LocalPartitionNode::gather(nextId(), {std::move(input)});
  }
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
  // At numDrivers > 1 each distinct-key group must be complete in one driver.
  // Skip when the input is pre-grouped on the keys: a local grouping guarantees
  // driver-confinement by contract (see `LocalProperty`).
  if (options_.numDrivers > 1 && preGroupedKeys.empty()) {
    input = addLocalPartition(std::move(input), distinctKeys);
  }
  return std::make_shared<velox::core::EnforceDistinctNode>(
      nextId(),
      std::move(distinctKeys),
      std::move(preGroupedKeys),
      node.errorMessage(),
      std::move(input));
}

// Merges two fragment-type contributions: a kSource and a kFixed coexist as
// kFixed (split-driven scans feeding a fixed-width stage); equal types merge to
// themselves; nullopt is "no constraint".
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
  VELOX_FAIL(
      "Incompatible fragment-type contributions in one fragment: {} + {}",
      *lhs,
      *rhs);
}

// The fragment type the subtree at 'node' contributes, walking down to (not
// across) inner exchanges. An exchange yields its consumer-side type
// (partitioned → kFixed, gather → kSingle, broadcast/arbitrary → no
// constraint); a scan yields kSource.
std::optional<FragmentType> fragmentTypeContribution(NodeCP node) {
  if (node->is(NodeType::kExchange)) {
    switch (node->as<Exchange>()->partitioning().kind) {
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
      return FragmentType::kSource;
    case NodeType::kValues:
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

// Sets 'fragment.type' (and 'width' for kFixed) from the contents at 'node'. At
// numWorkers == 1 all parallelism collapses to one task.
void decideFragmentType(
    NodeCP node,
    int32_t numWorkers,
    ExecutableFragment& fragment) {
  if (numWorkers == 1) {
    fragment.type = FragmentType::kSingle;
    return;
  }
  fragment.type =
      fragmentTypeContribution(node).value_or(FragmentType::kSource);
  if (fragment.type == FragmentType::kFixed) {
    fragment.width = numWorkers;
  }
}

void Emitter::finalizeGroupedLeaves(ExecutableFragment& fragment) {
  // Fold the bucketed scans' partitionings into one compatible type. `folded`
  // owns the running copartition result so `base`, which points into it (or the
  // first scan's layout-owned type before any fold), never dangles.
  const connector::PartitionType* base = nullptr;
  std::shared_ptr<connector::PartitionType> folded;
  for (const auto& leaf : groupedLeaves_) {
    if (leaf.partitionType == nullptr) {
      continue;
    }
    if (base == nullptr) {
      base = leaf.partitionType;
      continue;
    }
    // Scans share a fragment without an intervening exchange only because the
    // memo found them co-partitionable, so `copartition` must succeed.
    folded = base->copartition(*leaf.partitionType);
    VELOX_CHECK_NOT_NULL(
        folded, "Co-fragmented bucketed scans must be copartitionable");
    base = folded.get();
  }

  if (base == nullptr) {
    return;
  }

  std::shared_ptr<connector::PartitionType> scaled =
      base->scaleDown(options_.numWorkers);
  for (const auto& leaf : groupedLeaves_) {
    fragment.groupedNodes.emplace(
        leaf.nodeId, leaf.partitionType != nullptr ? scaled : nullptr);
  }
  fragment.type = FragmentType::kFixed;
  fragment.width = scaled->numPartitions();
}

velox::core::PlanNodePtr Emitter::emitChildFragment(
    NodeCP node,
    ExecutableFragment& fragment) {
  return emitInFragment(fragment, [&] { return emit(node); });
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
  decideFragmentType(belowRoot, options_.numWorkers, source);
  velox::core::PlanNodePtr sourcePlan = layoutAboveGather
      ? emitChildFragment(belowRoot, source)
      : emitInFragment(
            source, [&] { return emitRoot(root, outputColumns, outputNames); });
  currentFragment_ = &top;

  source.fragment.planNode =
      makeSingleOutput(sourcePlan->outputType(), sourcePlan);
  auto gather = std::make_shared<velox::core::ExchangeNode>(
      nextId(), sourcePlan->outputType(), exchangeSerdeKind_);
  top.inputStages.emplace_back(gather->id(), source.taskPrefix);
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
  switch (partitioning.kind) {
    case PartitionKind::kBroadcast:
      return velox::core::PartitionedOutputNode::broadcast(
          nextId(),
          /*numPartitions=*/1,
          outputType,
          exchangeSerdeKind_,
          std::string{velox::core::TransportKind::kInMemory},
          sourcePlan);
    case PartitionKind::kGather:
      return makeSingleOutput(outputType, sourcePlan);
    case PartitionKind::kArbitrary:
      return velox::core::PartitionedOutputNode::arbitrary(
          nextId(),
          outputType,
          exchangeSerdeKind_,
          std::string{velox::core::TransportKind::kInMemory},
          sourcePlan);
    case PartitionKind::kPartitioned: {
      auto fields =
          toFieldAccessList(partitioning.keys, "Exchange partition key");

      // A partitionType aligns this shuffle to a bucketed side: use the
      // connector's partition function (scaled to the worker count) so rows
      // land in the same groups as the bucketed scan, matching the grouped
      // fragment's width. Otherwise standard Velox hash over numWorkers
      // partitions.
      int32_t numPartitions = options_.numWorkers;
      velox::core::PartitionFunctionSpecPtr spec;
      if (partitioning.partitionType != nullptr) {
        auto scaled =
            partitioning.partitionType->scaleDown(options_.numWorkers);
        numPartitions = scaled->numPartitions();
        spec = connectorPartitionSpec(
            *scaled, outputType, fields, /*isLocal=*/false);
      } else {
        spec = makeHashPartitionSpec(outputType, fields);
      }

      if (numPartitions == 1) {
        // A connector partitioning that scales to one destination (e.g. a
        // single-bucket table) needs no partition function: gather to one task.
        return makeSingleOutput(outputType, sourcePlan);
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
          exchangeSerdeKind_,
          std::string{velox::core::TransportKind::kInMemory},
          sourcePlan);
    }
    case PartitionKind::kUnspecified:
      VELOX_UNREACHABLE("Exchange has no concrete partitioning to lower");
  }
}

velox::core::PlanNodePtr Emitter::makeExchangeConsumer(
    const Partitioning& partitioning,
    const velox::RowTypePtr& outputType) {
  if (partitioning.kind == PartitionKind::kGather &&
      !partitioning.orderKeys.empty()) {
    // Order-preserving gather: the consumer merges the sorted per-task streams.
    auto [sortingKeys, sortingOrders] = toSortingKeys(
        partitioning.orderKeys, partitioning.orderTypes, "Merge exchange key");
    return std::make_shared<velox::core::MergeExchangeNode>(
        nextId(), outputType, sortingKeys, sortingOrders, exchangeSerdeKind_);
  }
  return std::make_shared<velox::core::ExchangeNode>(
      nextId(), outputType, exchangeSerdeKind_);
}

velox::core::PlanNodePtr Emitter::emitExchange(const Exchange& exchange) {
  const auto& partitioning = exchange.partitioning();

  // Producer fragment: the exchange's input, capped with a PartitionedOutput.
  ExecutableFragment source = newFragment();
  decideFragmentType(exchange.input(), options_.numWorkers, source);
  velox::core::PlanNodePtr sourcePlan =
      emitChildFragment(exchange.input(), source);
  const auto outputType = sourcePlan->outputType();

  source.fragment.planNode =
      makeExchangeProducer(partitioning, outputType, sourcePlan);

  velox::core::PlanNodePtr consumer =
      makeExchangeConsumer(partitioning, outputType);
  currentFragment_->inputStages.emplace_back(consumer->id(), source.taskPrefix);

  // A partitioned exchange feeding the outer fragment is a group-routed leaf
  // there: if that fragment turns out bucketed, the exchange delivers per
  // group. Recorded as a null-typed leaf (the fragment's bucketed scan, if any,
  // anchors the width); `finalizeGroupedLeaves` drops it when the fragment has
  // none. A bucketed write, whose fragment has no scan, anchors this leaf
  // explicitly — see emitTableWrite.
  if (partitioning.kind == PartitionKind::kPartitioned) {
    groupedLeaves_.push_back({consumer->id(), nullptr});
  }

  stages_.push_back(std::move(source));
  return consumer;
}

velox::core::PlanNodePtr Emitter::emitTableWrite(const TableWrite& tableWrite) {
  const auto& table = *tableWrite.table();
  auto* layout = table.layouts().front();
  const auto& connectorId = layout->connector()->connectorId();
  auto metadata = connector::ConnectorMetadataRegistry::get(connectorId);
  auto connectorSession = session_.toConnectorSession(connectorId);
  auto handle = metadata->beginWrite(
      connectorSession,
      table.shared_from_this(),
      tableWrite.kind(),
      session_.options().explain);

  // Parallelize writers only when the input is already distributed; gathering
  // an already-single-task input would add a redundant exchange.
  const bool distributed = options_.numWorkers > 1 &&
      fragmentTypeContribution(tableWrite.input()) != FragmentType::kSingle;

  ExecutableFragment* rootFragment = currentFragment_;
  ExecutableFragment writerFragment;
  std::vector<PendingGroupedLeaf> rootGroupedLeaves;
  if (distributed) {
    writerFragment = newFragment();
    decideFragmentType(tableWrite.input(), options_.numWorkers, writerFragment);
    rootGroupedLeaves = std::move(groupedLeaves_);
    groupedLeaves_.clear();
    currentFragment_ = &writerFragment;
  }

  velox::core::PlanNodePtr input = emit(tableWrite.input());

  // A bucketed write's input is the remote bucket exchange (physical planning
  // inserts it unless the source is already co-bucketed). The writer fragment
  // has no bucketed scan to anchor grouped execution, so anchor it on that
  // exchange: each of its bucket partitions becomes one writer group producing
  // one bucket file, matching the exchange's partition count.
  if (distributed && !layout->partitionColumns().empty() &&
      tableWrite.input()->is(NodeType::kExchange)) {
    for (auto& leaf : groupedLeaves_) {
      if (leaf.nodeId == input->id()) {
        leaf.partitionType = layout->partitionType().get();
        break;
      }
    }
  }

  // Velox's TableWriteNode writes its input's columns, so materialize the write
  // value expressions as a Project producing the target columns in schema
  // order. Skip the projection when the input already produces exactly those
  // columns — otherwise it would shadow a source projection and, for a bucketed
  // write, land after the bucket exchange instead of before it.
  auto values = exprEmitter_.toTypedExprs(tableWrite.columnExprs());
  if (!writeInputIsIdentity(input->outputType(), values)) {
    input = std::make_shared<velox::core::ProjectNode>(
        nextId(), table.type()->names(), std::move(values), std::move(input));
  }
  const auto& inputType = input->outputType();

  // A write to a bucketed/partitioned layout repartitions its input on the
  // target's partition (bucket) columns so each partition's rows go to a single
  // writer driver. The remote bucket exchange added in physical planning has
  // already confined each partition to one worker; this is the within-worker
  // split (only meaningful at numDrivers > 1).
  const auto& partitionColumns = layout->partitionColumns();
  if (options_.numDrivers > 1 && !partitionColumns.empty()) {
    input = std::make_shared<velox::core::LocalPartitionNode>(
        nextId(),
        velox::core::LocalPartitionNode::Type::kRepartition,
        /*scaleWriter=*/false,
        connectorPartitionSpec(
            *layout->partitionType(),
            // 'partitionColumns' name the target schema. 'input' corresponds to
            // the schema positionally but may carry source names when the write
            // reuses a source projection, so resolve channels via the schema.
            table.type(),
            partitionColumns,
            /*isLocal=*/true),
        std::vector<velox::core::PlanNodePtr>{std::move(input)});
  }

  // A single-fragment write over a single-threaded pipeline (e.g. a Values
  // source) has one writer producing all rows, so the per-driver stats need no
  // merge. Reporting one driver keeps the plan a bare TableWrite.
  const int32_t numDrivers =
      !distributed && isSingleThreadedPipeline(input) ? 1 : options_.numDrivers;
  WriteStatsBuilder statsBuilder(
      table,
      inputType,
      *handle,
      numDrivers,
      distributed ? options_.numWorkers : 1);
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
          connectorId, handle->veloxHandle());
  finishWrite_ = FinishWrite{
      metadata,
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
  writerFragment.fragment.planNode =
      makeSingleOutput(result->outputType(), result);
  currentFragment_ = rootFragment;
  groupedLeaves_ = std::move(rootGroupedLeaves);
  auto gather = std::make_shared<velox::core::ExchangeNode>(
      nextId(), result->outputType(), exchangeSerdeKind_);
  rootFragment->inputStages.emplace_back(
      gather->id(), writerFragment.taskPrefix);
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
    currentFragment_ = &top;
    velox::core::PlanNodePtr writePlan = emit(root);
    if (options_.remoteOutput) {
      outputProjection = writePlan;
      top.fragment.planNode =
          makeSingleOutput(writePlan->outputType(), writePlan);
    } else {
      top.fragment.planNode = writePlan;
    }
  } else {
    // A root whose subtree already runs on one task (e.g. below a global ORDER
    // BY or aggregate) needs no output gather and is emitted directly, like the
    // single-node case. Only a multi-task root is gathered to a single task for
    // the query output.
    const bool gatherForOutput = options_.numWorkers > 1 &&
        fragmentTypeContribution(root) != FragmentType::kSingle;

    if (options_.remoteOutput) {
      // Results stay distributed: emit the root as a single fragment capped by
      // a PartitionedOutput for remote consumption, with no gather consumer
      // fragment. The fragment type follows the root's contents (kFixed for a
      // multi-worker source, kSingle at numWorkers == 1).
      currentFragment_ = &top;
      decideFragmentType(root, options_.numWorkers, top);
      outputProjection = emitRoot(root, outputColumns, outputNames);
      top.fragment.planNode =
          makeSingleOutput(outputProjection->outputType(), outputProjection);
    } else if (gatherForOutput) {
      emitGatheredOutput(root, outputColumns, outputNames, top);
    } else {
      currentFragment_ = &top;
      top.fragment.planNode = emitRoot(root, outputColumns, outputNames);
    }
  }

  finalizeGroupedLeaves(top);
  currentFragment_ = nullptr;

  // Validate the producer fragments and the output fragment. The output
  // fragment is validated at 'outputProjection' so its output names, which may
  // repeat for user aliases, are allowed as the plan root.
  if (outputProjection == nullptr) {
    outputProjection = top.fragment.planNode;
  }

  for (const auto& fragment : stages_) {
    velox::core::PlanConsistencyChecker::check(fragment.fragment.planNode);
  }
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
    ScanHandleCache& scanHandles,
    const MultiFragmentPlan::Options& options) {
  VELOX_CHECK_EQ(outputColumns.size(), outputNames.size());
  Emitter emitter(session, evaluator, scanHandles, options);
  auto fragments = emitter.emitFragments(root, outputColumns, outputNames);
  return Result{
      std::move(fragments),
      emitter.takeFinishWrite(),
      emitter.takePrediction()};
}

} // namespace facebook::axiom::optimizer::v2
