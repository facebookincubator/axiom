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

#include "axiom/optimizer/v2/Node.h"

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/KeyHash.h"
#include "axiom/optimizer/v2/NodePrinter.h"
#include "axiom/optimizer/v2/NodeVisitor.h"
#include "axiom/optimizer/v2/TableAccessHandle.h"

namespace facebook::axiom::optimizer::v2 {

namespace {
const auto& nodeTypeNames() {
  static const folly::F14FastMap<NodeType, std::string_view> kNames = {
      {NodeType::kScan, "Scan"},
      {NodeType::kFilter, "Filter"},
      {NodeType::kProject, "Project"},
      {NodeType::kLimit, "Limit"},
      {NodeType::kSort, "Sort"},
      {NodeType::kTopN, "TopN"},
      {NodeType::kAggregate, "Aggregate"},
      {NodeType::kGroupId, "GroupId"},
      {NodeType::kMarkDistinct, "MarkDistinct"},
      {NodeType::kValues, "Values"},
      {NodeType::kUnnest, "Unnest"},
      {NodeType::kUnionAll, "UnionAll"},
      {NodeType::kJoin, "Join"},
      {NodeType::kIndexLookupJoin, "IndexLookupJoin"},
      {NodeType::kWindow, "Window"},
      {NodeType::kInference, "Inference"},
      {NodeType::kRowNumber, "RowNumber"},
      {NodeType::kTopNRowNumber, "TopNRowNumber"},
      {NodeType::kApply, "Apply"},
      {NodeType::kEnforceSingleRow, "EnforceSingleRow"},
      {NodeType::kAssignUniqueId, "AssignUniqueId"},
      {NodeType::kEnforceDistinct, "EnforceDistinct"},
      {NodeType::kExchange, "Exchange"},
      {NodeType::kTableWrite, "TableWrite"},
      {NodeType::kFixedPoint, "FixedPoint"},
      {NodeType::kWorkingTable, "WorkingTable"},
      {NodeType::kRepeat, "Repeat"},
  };
  return kNames;
}

const auto& workingTableReadModeNames() {
  static const folly::F14FastMap<WorkingTableReadMode, std::string_view>
      kNames = {
          {WorkingTableReadMode::kLatestDelta, "latestDelta"},
          {WorkingTableReadMode::kAccumulated, "accumulated"},
      };
  return kNames;
}

const auto& placementNames() {
  static const folly::F14FastMap<Placement, std::string_view> kNames = {
      {Placement::kUnplaced, "unplaced"},
      {Placement::kDistributed, "distributed"},
      {Placement::kSingleTask, "singleTask"},
      {Placement::kOneDriverPerTask, "oneDriverPerTask"},
      {Placement::kSingleThread, "singleThread"},
  };
  return kNames;
}

const auto& fixedPointModeNames() {
  static const folly::F14FastMap<FixedPointMode, std::string_view> kNames = {
      {FixedPointMode::kAppend, "append"},
      {FixedPointMode::kReplace, "replace"},
  };
  return kNames;
}
} // namespace

AXIOM_DEFINE_ENUM_NAME(NodeType, nodeTypeNames);
AXIOM_DEFINE_ENUM_NAME(WorkingTableReadMode, workingTableReadModeNames);
AXIOM_DEFINE_ENUM_NAME(FixedPointMode, fixedPointModeNames);
AXIOM_DEFINE_ENUM_NAME(Placement, placementNames);

std::vector<Partitioning> Node::globalPartitions(
    std::span<const NodeCP> nodes) {
  std::vector<Partitioning> partitions;
  partitions.reserve(nodes.size());
  for (NodeCP node : nodes) {
    partitions.push_back(node->physicalProperties().globalPartition);
  }
  return partitions;
}

Node::Node(NodeType nodeType, ColumnVector outputColumns, Properties properties)
    : PlanObject(PlanType::kV2Node),
      nodeType_(nodeType),
      outputColumns_(std::move(outputColumns)),
      properties_(std::move(properties)) {
  const auto columns = PlanObjectSet::fromObjects(outputColumns_);
  std::visit(
      [&](const auto& nodeProperties) {
        nodeProperties.checkReferencesColumns(columns);
      },
      properties_);
}

const PhysicalProperties& Node::physicalProperties() const {
  VELOX_CHECK(placed(), "Node is not placed yet: {}", nodeTypeName());
  return std::get<PhysicalProperties>(properties_);
}

const RelationalProperties& Node::relationalProperties() const {
  VELOX_CHECK(!placed(), "Node is already placed: {}", nodeTypeName());
  return std::get<RelationalProperties>(properties_);
}

std::string Node::toString() const {
  return NodePrinter::toText(this);
}

namespace {

// True when the inputs are placed. Skips absent inputs.
bool placedInputs(std::span<const NodeCP> inputs) {
  std::optional<bool> placed;
  for (NodeCP input : inputs) {
    if (input == nullptr) {
      continue;
    }
    VELOX_CHECK(
        !placed.has_value() || *placed == input->placed(),
        "Node inputs disagree on placement");
    placed = input->placed();
  }
  VELOX_CHECK(placed.has_value(), "Node has no inputs");
  return *placed;
}

// Properties of a node over 'inputs': 'physical()' when the inputs are placed,
// 'relational()' otherwise.
template <typename Physical, typename Relational>
Node::Properties derivedProperties(
    std::initializer_list<NodeCP> inputs,
    Physical&& physical,
    Relational&& relational) {
  if (placedInputs({inputs.begin(), inputs.size()})) {
    return physical();
  }
  return relational();
}

// Properties of a node that exists only in a placed plan.
template <typename Physical>
Node::Properties placedProperties(
    std::initializer_list<NodeCP> inputs,
    Physical&& physical) {
  VELOX_CHECK(
      placedInputs({inputs.begin(), inputs.size()}),
      "Node requires placed inputs");
  return physical();
}

// The relational properties of a node that emits a subset of 'input''s rows,
// with its columns or more.
RelationalProperties subsetRelational(NodeCP input) {
  return {.unique = input->relationalProperties().unique};
}

// The relational properties of a node that emits each of 'input''s rows once,
// with its columns or more.
RelationalProperties sameRowsRelational(NodeCP input) {
  return input->relationalProperties();
}

// A relation of exactly one row, which is unique on the empty set.
RelationalProperties singleRowRelational() {
  return {.unique = {PlanObjectSet{}}, .singleRow = true};
}

bool onOneTask(Placement placement) {
  return placement == Placement::kSingleTask ||
      placement == Placement::kSingleThread;
}

bool onOneDriverPerTask(Placement placement) {
  return placement == Placement::kOneDriverPerTask ||
      placement == Placement::kSingleThread;
}

// The properties of a leaf: 'physical()' when PlanPhysical placed it, with
// the partitioning one task or one driver per task implies; none otherwise.
template <typename Physical>
Node::Properties leafProperties(Placement placement, Physical&& physical) {
  if (placement == Placement::kUnplaced) {
    return RelationalProperties{};
  }
  PhysicalProperties properties = physical();
  if (onOneTask(placement)) {
    properties.globalPartition = Partitioning::gather(PropertyScope::kGlobal);
  }
  if (onOneDriverPerTask(placement)) {
    properties.driverPartition = Partitioning::gather(PropertyScope::kDriver);
  }
  return properties;
}

} // namespace

NodeCP FOLLY_NULLABLE
Node::findFirstNode(NodeCP root, const std::function<bool(NodeCP)>& predicate) {
  VELOX_CHECK_NOT_NULL(root);
  if (predicate(root)) {
    return root;
  }
  for (NodeCP input : root->inputs()) {
    if (NodeCP found = findFirstNode(input, predicate)) {
      return found;
    }
  }
  return nullptr;
}

RequiredStates Node::deriveRequiredStates() const {
  RequiredStates states;
  for (NodeCP input : inputs()) {
    for (const auto& [name, read] : input->requiredStates()) {
      auto [it, inserted] = states.emplace(name, read);
      VELOX_CHECK(
          inserted || it->second == read,
          "WorkingTable reads of state {} disagree on columns or partitioning",
          name);
    }
  }
  return states;
}

namespace {

// Per-operator derivation of driver order. Each node computes
// these in its constructor from its already-constructed inputs; distribution
// and relation-level properties are added with their consumers.

// Keeps the driver order expressible over `columns`: a `kGrouped` entry
// survives only if all its columns do; a `kSorted` entry is truncated to its
// leading run of columns in `columns`. A dropped or truncated entry ends the
// list — nothing nested under a broken outer property still holds.
DriverOrderVector projectDriverOrder(
    const DriverOrderVector& driverOrder,
    const PlanObjectSet& columns) {
  DriverOrderVector result;
  for (const DriverOrder& property : driverOrder) {
    if (property.kind == DriverOrderKind::kGrouped) {
      if (!columns.containsAll(property.columns)) {
        break;
      }
      result.push_back(property);
      continue;
    }
    DriverOrder truncated{.kind = DriverOrderKind::kSorted};
    for (size_t i = 0; i < property.columns.size(); ++i) {
      if (!columns.contains(property.columns[i])) {
        break;
      }
      truncated.columns.push_back(property.columns[i]);
      truncated.orders.push_back(property.orders[i]);
    }
    if (truncated.columns.empty()) {
      break;
    }
    const bool full = truncated.columns.size() == property.columns.size();
    result.push_back(std::move(truncated));
    if (!full) {
      break;
    }
  }
  return result;
}

// Keeps an input's driver order expressible over `columns` (see
// `projectDriverOrder`), for a node that emits only a subset of its input.
DriverOrderVector retainedDriverOrder(
    NodeCP input,
    const ColumnVector& columns) {
  return projectDriverOrder(
      input->physicalProperties().driverOrder,
      PlanObjectSet::fromObjects(columns));
}

// Returns no mapping when any key is not projected as a bare column.
std::optional<PlanObjectSet> remapKeySet(
    const PlanObjectSet& keys,
    const ExprVector& sourceExprs,
    const ColumnVector& outputColumns) {
  ColumnVector mapped;
  mapped.reserve(keys.size());
  for (ColumnCP key : keys.toObjects<Column>()) {
    const auto it = std::ranges::find_if(
        sourceExprs, [&](ExprCP expr) { return expr->sameOrEqual(*key); });
    if (it == sourceExprs.end()) {
      return std::nullopt;
    }
    mapped.push_back(outputColumns[it - sourceExprs.begin()]);
  }
  return PlanObjectSet::fromObjects(mapped);
}

// Re-expresses 'keys' on 'outputColumns', which project 'sourceExprs', dropping
// keys that are not projected as bare columns.
QGVector<PlanObjectSet> remapKeySets(
    const QGVector<PlanObjectSet>& keys,
    const ExprVector& sourceExprs,
    const ColumnVector& outputColumns) {
  QGVector<PlanObjectSet> result;
  for (const PlanObjectSet& key : keys) {
    if (auto mapped = remapKeySet(key, sourceExprs, outputColumns)) {
      result.push_back(std::move(*mapped));
    }
  }
  return result;
}

// A projection emits each input row once: it keeps the input's single row and
// the unique keys it projects as bare columns.
RelationalProperties projectRelational(
    NodeCP input,
    const ExprVector& exprs,
    const ColumnVector& outputColumns) {
  const RelationalProperties& inputProperties = input->relationalProperties();
  return {
      .unique = remapKeySets(inputProperties.unique, exprs, outputColumns),
      .singleRow = inputProperties.singleRow,
  };
}

// Keeps an input's partitioning when a node emits only a subset of its input:
// the rows stay on the task they arrived on, so the distribution survives as
// long as every partition key does. A dropped key leaves the output with no
// expressible partitioning.
Partitioning retainedPartition(
    const Partitioning& inputPartition,
    const ColumnVector& columns) {
  Partitioning partition = inputPartition.dropOrder();
  const auto columnSet = PlanObjectSet::fromObjects(columns);
  for (ExprCP key : partition.keys) {
    if (!key->columns().isSubset(columnSet)) {
      return Partitioning::unspecified(inputPartition.scope);
    }
  }
  return partition;
}

// A single `Sorted` property over the leading run of `keys` that are columns,
// paired with their orders. A computed sort key is not an output column, so it
// (and everything after it) is dropped; empty when the first key is not a
// column.
DriverOrderVector sortedDriverOrder(
    const ExprVector& keys,
    const OrderTypeVector& orderTypes) {
  ColumnVector columns;
  OrderTypeVector orders;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (!keys[i]->isColumn()) {
      break;
    }
    columns.push_back(keys[i]->as<Column>());
    orders.push_back(orderTypes[i]);
  }
  if (columns.empty()) {
    return {};
  }
  DriverOrderVector result;
  result.push_back(
      {.kind = DriverOrderKind::kSorted,
       .columns = std::move(columns),
       .orders = std::move(orders)});
  return result;
}

// Local ordering an exchange's output carries: an order-preserving gather keeps
// the merged sort order; every other exchange interleaves rows and keeps none.
DriverOrderVector exchangeDriverOrder(const Partitioning& partitioning) {
  if (partitioning.orderKeys.empty()) {
    return {};
  }
  return sortedDriverOrder(partitioning.orderKeys, partitioning.orderTypes);
}

// A single `Grouped` property over `columns` (empty when there are none).
DriverOrderVector groupedDriverOrder(const ColumnVector& columns) {
  if (columns.empty()) {
    return {};
  }
  DriverOrderVector result;
  result.push_back({.kind = DriverOrderKind::kGrouped, .columns = columns});
  return result;
}

// A single `Grouped` property over one column.
DriverOrderVector groupedDriverOrder(ColumnCP column) {
  ColumnVector columns;
  columns.push_back(column);
  return groupedDriverOrder(columns);
}

// A hash join emits each probe row's matches contiguously, preserving grouping
// by every surviving probe unique key, at any scope. A spilling probe may break
// this.
void appendProbeUniqueGroupings(
    NodeCP left,
    const PlanObjectSet& outputColumns,
    DriverOrderVector& driverOrder) {
  const PhysicalProperties& properties = left->physicalProperties();
  for (const auto* keys :
       {&properties.globalUnique, &properties.driverUnique}) {
    for (const PlanObjectSet& key : *keys) {
      if (key.isSubset(outputColumns)) {
        driverOrder.push_back(
            DriverOrder{
                .kind = DriverOrderKind::kGrouped,
                .columns = key.template toObjects<Column>()});
      }
    }
  }
}

// The probe (left) side's driver order survives when the join preserves
// probe rows. Build-emitting and cross joins drop them.
// TODO: order through a spillable join is not guaranteed once spilling is
// enabled; revisit when spilling is modeled.
DriverOrderVector joinDriverOrder(
    velox::core::JoinType joinType,
    NodeCP left,
    const ColumnVector& outputColumns) {
  if (Join::preservedSides(joinType).left) {
    DriverOrderVector driverOrder = retainedDriverOrder(left, outputColumns);
    appendProbeUniqueGroupings(
        left, PlanObjectSet::fromObjects(outputColumns), driverOrder);
    return driverOrder;
  }
  return {};
}

// Preserves placement at its existing scope while dropping merge order, which
// belongs only to the producing gather.
Partitioning inheritedPartition(const Partitioning& inputPartition) {
  return inputPartition.dropOrder();
}

// Driver placement of a node that keeps rows on their driver. The input's
// co-location on 'keys' survives any reordering, so it becomes placement on
// 'keys' unless the input's placement already proves it. That replaces an input
// partitioning on keys outside 'keys'.
Partitioning keptDriverPartition(
    const PhysicalProperties& props,
    const ExprVector& keys) {
  if (keys.empty() || !props.coLocates(PropertyScope::kDriver, keys) ||
      props.driverPartition.is(PartitionKind::kGather) ||
      props.driverPartition.coLocates(keys)) {
    return inheritedPartition(props.driverPartition);
  }
  return Partitioning::hash(PropertyScope::kDriver, keys);
}

// Preserves task and driver placement, driver order, and uniqueness for an
// operator that neither moves nor reshapes rows.
// Adds 'columns' as a unique key of 'properties' for a node that sees together
// the rows of 'input' with equal 'keys': across the relation when 'input'
// brings them to one task and one driver, and only within each driver
// otherwise.
void addUniqueKey(
    PhysicalProperties& properties,
    PlanObjectSet columns,
    const PhysicalProperties& input,
    const ExprVector& keys) {
  if (input.coLocates(PropertyScope::kGlobal, keys) &&
      input.coLocates(PropertyScope::kDriver, keys)) {
    properties.globalUnique.push_back(std::move(columns));
  } else {
    properties.driverUnique.push_back(std::move(columns));
  }
}

PhysicalProperties passThroughPhysical(NodeCP input) {
  const PhysicalProperties& props = input->physicalProperties();
  return PhysicalProperties{
      .globalPartition = inheritedPartition(props.globalPartition),
      .driverPartition = inheritedPartition(props.driverPartition),
      .driverOrder = props.driverOrder,
      .globalUnique = props.globalUnique,
      .driverUnique = props.driverUnique};
}

// An exact Limit runs on one driver. It preserves the input's global
// partitioning, driver order, and uniqueness, while gathering its output
// at driver scope.
PhysicalProperties limitPhysical(NodeCP input) {
  PhysicalProperties properties = passThroughPhysical(input);
  properties.driverPartition = Partitioning::gather(PropertyScope::kDriver);
  return properties;
}

// A ranking or inference node keeps each row it emits on its task and driver,
// emits no row twice, and does not keep input order: placement, co-location on
// 'partitionKeys', and uniqueness survive.
PhysicalProperties rankedPhysical(
    NodeCP input,
    const ExprVector& partitionKeys) {
  const PhysicalProperties& props = input->physicalProperties();
  return PhysicalProperties{
      .globalPartition = inheritedPartition(props.globalPartition),
      .driverPartition = keptDriverPartition(props, partitionKeys),
      .globalUnique = props.globalUnique,
      .driverUnique = props.driverUnique};
}

// A window keeps rows on their task and driver and emits each partition
// contiguously in an order unrelated to its input: placement, co-location on
// the partition keys, and uniqueness survive, and the output is grouped on the
// partition keys only.
PhysicalProperties windowPhysical(
    NodeCP input,
    const ExprVector& partitionKeys) {
  const PhysicalProperties& props = input->physicalProperties();
  ColumnVector partitionColumns;
  for (ExprCP key : partitionKeys) {
    partitionColumns.push_back(key->as<Column>());
  }
  return PhysicalProperties{
      .globalPartition = inheritedPartition(props.globalPartition),
      .driverPartition = keptDriverPartition(props, partitionKeys),
      .driverOrder = groupedDriverOrder(partitionColumns),
      .globalUnique = props.globalUnique,
      .driverUnique = props.driverUnique};
}

// Sorting preserves placement and uniqueness while establishing local order.
PhysicalProperties sortedPhysical(
    NodeCP input,
    const ExprVector& orderKeys,
    const OrderTypeVector& orderTypes) {
  const PhysicalProperties& props = input->physicalProperties();
  return PhysicalProperties{
      .globalPartition = inheritedPartition(props.globalPartition),
      .driverPartition = inheritedPartition(props.driverPartition),
      .driverOrder = sortedDriverOrder(orderKeys, orderTypes),
      .globalUnique = props.globalUnique,
      .driverUnique = props.driverUnique};
}

// Remaps the input partitioning through a Project's column map: each partition
// key (an input column) is re-expressed on the output column that projects it,
// whether identity or rename (`exprs[i]` is a bare reference to the key, so its
// output column `outputColumns[i]` carries the same values, hence the same hash
// partition). A key not projected as a bare column — dropped, or appearing only
// inside a computed expression — drops the partition to unspecified. Gather and
// unspecified pass through unchanged.
Partitioning projectPartition(
    const Partitioning& partitioning,
    const ExprVector& exprs,
    const ColumnVector& outputColumns) {
  if (partitioning.kind != PartitionKind::kPartitioned) {
    return partitioning.dropOrder();
  }
  ExprVector keys;
  keys.reserve(partitioning.keys.size());
  for (ExprCP key : partitioning.keys) {
    ExprCP projected = nullptr;
    for (size_t i = 0; i < exprs.size(); ++i) {
      if (exprs[i]->sameOrEqual(*key)) {
        projected = outputColumns[i];
        break;
      }
    }
    if (projected == nullptr) {
      return Partitioning::unspecified(partitioning.scope);
    }
    keys.push_back(projected);
  }
  Partitioning result = partitioning;
  result.keys = std::move(keys);
  return result;
}

// An output column equal to 'key', or null. Inner-join equalities merge the
// equated columns into one equivalence class (see Column::equals); every member
// holds the same value on every output row, so any surviving class member
// partitions the output exactly as 'key' does. Only columns form equivalence
// classes, so a non-column key has no substitute.
ExprCP survivingEquiKey(ExprCP key, const PlanObjectSet& outputColumns) {
  if (!key->is(PlanType::kColumnExpr)) {
    return nullptr;
  }

  if (const auto* equivalence = key->as<Column>()->equivalence()) {
    for (ColumnCP column : equivalence->columns) {
      if (outputColumns.contains(column)) {
        return column;
      }
    }
  }

  return nullptr;
}

// The input's partitioning re-expressed on the aggregate's output. An aggregate
// streams over its input without moving rows, so it preserves the input's
// partitioning — but only when that partitioning's keys are all grouping keys
// (the only input values the aggregate keeps): then those keys are a
// co-location of the groups and survive in the output, re-expressed on the
// grouping output columns they map to. An input partitioned on anything else
// does not co-locate the groups (it should not feed the aggregate at all) and
// could not be expressed on the output, so the partitioning is unknown. A
// grouping-set aggregate reads its GroupId, which has no known partitioning, so
// it falls into the unknown case. A non-hash distribution (gather / broadcast /
// arbitrary) is inherited by kind only, with its merge order dropped (see
// `Partitioning::dropOrder`), as the aggregate reorders rows and so does not
// preserve one. `groupingColumns` are the output columns holding the grouping
// values, aligned with `groupingKeys`.
Partitioning aggregateInputPartition(
    const Partitioning& inputPartition,
    const ExprVector& groupingKeys,
    const ColumnVector& groupingColumns) {
  if (inputPartition.kind != PartitionKind::kPartitioned) {
    return inputPartition.dropOrder();
  }
  ExprVector keys;
  keys.reserve(inputPartition.keys.size());
  for (ExprCP key : inputPartition.keys) {
    const auto it = std::ranges::find(groupingKeys, key);
    if (it == groupingKeys.end()) {
      return Partitioning::unspecified(inputPartition.scope);
    }
    keys.push_back(groupingColumns[it - groupingKeys.begin()]);
  }
  Partitioning result = inputPartition;
  result.keys = std::move(keys);
  return result;
}

// Output partitioning of an aggregation. A partial keeps its input's
// partitioning re-expressed on the output (see `aggregateInputPartition`), even
// with no grouping keys. A complete (single / final) global aggregate gathers
// to one task or driver; a grouped one re-expresses its input's partitioning
// like a partial. 'groupingColumns' are the leading output columns, aligned
// with 'groupingKeys'.
Partitioning aggregatePartition(
    AggregateStep step,
    const Partitioning& inputPartition,
    const ExprVector& groupingKeys,
    const ColumnVector& groupingColumns) {
  if (step != AggregateStep::kPartial && groupingKeys.empty()) {
    return Partitioning::gather(inputPartition.scope);
  }
  return aggregateInputPartition(inputPartition, groupingKeys, groupingColumns);
}

// The leading output columns of an aggregation, which hold the grouping values
// aligned one-to-one with its grouping keys.
ColumnVector aggregateGroupingColumns(
    const ColumnVector& outputColumns,
    size_t numGroupingKeys) {
  return ColumnVector{
      outputColumns.begin(),
      outputColumns.begin() + static_cast<ptrdiff_t>(numGroupingKeys)};
}

// Partitioning of 'partitionType' expressed over 'outputColumns': the
// connector's partition function on the columns matching 'layout's partition
// columns. Unspecified when a partition column is not among 'outputColumns',
// so the partitioning cannot be stated over them.
Partitioning bucketPartition(
    const connector::TableLayout* layout,
    const ColumnVector& outputColumns,
    const connector::PartitionType* partitionType) {
  const auto& partitionColumns = layout->partitionColumns();
  if (partitionType == nullptr || partitionColumns.empty()) {
    return {};
  }

  // A partitioning into one partition puts every row in the same partition, so
  // it tells a consumer nothing about where a row is: no partitioning at all.
  if (partitionType->numPartitions() == 1) {
    return {};
  }

  ExprVector keys;
  keys.reserve(partitionColumns.size());
  for (const auto* partitionColumn : partitionColumns) {
    ColumnCP match = nullptr;
    for (ColumnCP column : outputColumns) {
      if (column->name() == partitionColumn->name()) {
        match = column;
        break;
      }
    }
    if (match == nullptr) {
      return {};
    }
    keys.push_back(match);
  }
  return Partitioning{
      .kind = PartitionKind::kPartitioned,
      .partitionType = partitionType,
      .keys = std::move(keys),
      .scope = PropertyScope::kGlobal};
}

// Output partitioning of a scan: what the plan chose to read it by, not what
// its table affords. Unspecified for an ordinary read, so nothing downstream
// inherits a partitioning the plan does not rely on.
Partitioning scanGlobalPartition(
    BaseTableCP baseTable,
    const ColumnVector& outputColumns,
    const connector::PartitionType* groupedPartitionType) {
  return bucketPartition(
      baseTable->layout(), outputColumns, groupedPartitionType);
}

} // namespace

Partitioning Scan::globalPartition(
    std::span<const Partitioning> /*inputPartitions*/,
    Builder& /*builder*/) const {
  return physicalProperties().globalPartition;
}

Scan::Scan(Key key)
    : Node(
          NodeType::kScan,
          ColumnVector{key.outputColumns},
          leafProperties(
              key.placement,
              [&] {
                return PhysicalProperties{
                    .globalPartition = scanGlobalPartition(
                        key.baseTable,
                        key.outputColumns,
                        key.groupedPartitionType)};
              })),
      baseTable_(key.baseTable),
      scanHandle_(key.scanHandle),
      groupedPartitionType_(key.groupedPartitionType),
      placement_(key.placement) {
  VELOX_CHECK_NOT_NULL(baseTable_);
  VELOX_CHECK(
      placement_ == Placement::kUnplaced || onOneTask(placement_) ||
          !baseTable_->layout()->runsOnCoordinator(),
      "Scan of a coordinator-only table must run on one task");
  folly::F14FastSet<std::string_view> schemaNames;
  folly::F14FastSet<std::string> outputNames;
  schemaNames.reserve(this->outputColumns().size());
  outputNames.reserve(this->outputColumns().size());
  for (ColumnCP column : this->outputColumns()) {
    VELOX_CHECK(
        schemaNames.insert(column->name()).second,
        "Scan outputColumns contains duplicate underlying column name: {}",
        column->name());
    VELOX_CHECK(
        outputNames.insert(column->outputName()).second,
        "Scan outputColumns contains duplicate output name: {}",
        column->outputName());
  }
}

size_t Scan::KeyHash::operator()(const Scan* node) const {
  return hashOf(
      node->baseTable(),
      node->outputColumns(),
      node->scanHandle(),
      node->groupedPartitionType(),
      node->placement());
}

Partitioning Scan::groupedPartition(
    const connector::PartitionType* partitionType) const {
  return scanGlobalPartition(baseTable_, outputColumns(), partitionType);
}

Partitioning Scan::storageBucketing() const {
  const connector::TableLayout* layout = baseTable_->layout();
  return bucketPartition(
      layout, outputColumns(), layout->partitionType().get());
}

size_t Scan::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.baseTable,
      key.outputColumns,
      key.scanHandle,
      key.groupedPartitionType,
      key.placement);
}

bool Scan::KeyEq::operator()(const Scan* left, const Scan* right) const {
  return left->baseTable() == right->baseTable() &&
      left->outputColumns() == right->outputColumns() &&
      left->scanHandle() == right->scanHandle() &&
      left->groupedPartitionType() == right->groupedPartitionType() &&
      left->placement() == right->placement();
}

bool Scan::KeyEq::operator()(const Key& key, const Scan* node) const {
  return key.baseTable == node->baseTable() &&
      key.outputColumns == node->outputColumns() &&
      key.scanHandle == node->scanHandle() &&
      key.groupedPartitionType == node->groupedPartitionType() &&
      key.placement == node->placement();
}

bool Scan::KeyEq::operator()(const Scan* node, const Key& key) const {
  return (*this)(key, node);
}

Partitioning Filter::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return inheritedPartition(inputPartitions[0]);
}

Filter::Filter(Key key)
    : Node(
          NodeType::kFilter,
          ColumnVector{key.input->outputColumns()},
          derivedProperties(
              {key.input},
              [&] { return passThroughPhysical(key.input); },
              [&] { return subsetRelational(key.input); })),
      input_(key.input),
      predicates_(std::move(key.predicates)) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(!predicates_.empty(), "Filter must have at least one predicate");
}

size_t Filter::KeyHash::operator()(const Filter* filter) const {
  return hashOf(filter->input(), filter->predicates());
}

size_t Filter::KeyHash::operator()(const Key& key) const {
  return hashOf(key.input, key.predicates);
}

bool Filter::KeyEq::operator()(const Filter* left, const Filter* right) const {
  return left->input() == right->input() &&
      left->predicates() == right->predicates();
}

bool Filter::KeyEq::operator()(const Key& key, const Filter* filter) const {
  return key.input == filter->input() && key.predicates == filter->predicates();
}

bool Filter::KeyEq::operator()(const Filter* filter, const Key& key) const {
  return (*this)(key, filter);
}

Partitioning Project::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return projectPartition(inputPartitions[0], exprs_, outputColumns());
}

Project::Project(Key key)
    : Node(
          NodeType::kProject,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.input},
              [&] {
                return PhysicalProperties{
                    .globalPartition = projectPartition(
                        key.input->physicalProperties().globalPartition,
                        key.exprs,
                        key.outputColumns),
                    .driverPartition = projectPartition(
                        key.input->physicalProperties().driverPartition,
                        key.exprs,
                        key.outputColumns),
                    .driverOrder =
                        retainedDriverOrder(key.input, key.outputColumns),
                    .globalUnique = remapKeySets(
                        key.input->physicalProperties().globalUnique,
                        key.exprs,
                        key.outputColumns),
                    .driverUnique = remapKeySets(
                        key.input->physicalProperties().driverUnique,
                        key.exprs,
                        key.outputColumns)};
              },
              [&] {
                return projectRelational(
                    key.input, key.exprs, key.outputColumns);
              })),
      input_(key.input),
      exprs_(std::move(key.exprs)) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_EQ(this->outputColumns().size(), exprs_.size());
}

bool Project::isConstantRow() const {
  return input()->is(NodeType::kValues) &&
      input()->as<Values>()->cardinality() == 1 &&
      std::ranges::all_of(exprs_, [](ExprCP expr) {
           return expr->is(PlanType::kLiteralExpr);
         });
}

bool Project::isDeterministic() const {
  for (ExprCP expr : exprs_) {
    if (expr->containsNonDeterministic()) {
      return false;
    }
  }
  return true;
}

size_t Project::KeyHash::operator()(const Project* node) const {
  return hashOf(node->input(), node->outputColumns(), node->exprs());
}

size_t Project::KeyHash::operator()(const Key& key) const {
  return hashOf(key.input, key.outputColumns, key.exprs);
}

bool Project::KeyEq::operator()(const Project* left, const Project* right)
    const {
  return left->input() == right->input() && left->exprs() == right->exprs() &&
      left->outputColumns() == right->outputColumns();
}

bool Project::KeyEq::operator()(const Key& key, const Project* node) const {
  return key.input == node->input() && key.exprs == node->exprs() &&
      key.outputColumns == node->outputColumns();
}

bool Project::KeyEq::operator()(const Project* node, const Key& key) const {
  return (*this)(key, node);
}

Partitioning Limit::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return inheritedPartition(inputPartitions[0]);
}

Limit::Limit(Key key)
    : Node(
          NodeType::kLimit,
          ColumnVector{key.input->outputColumns()},
          derivedProperties(
              {key.input},
              [&] {
                return key.partial ? passThroughPhysical(key.input)
                                   : limitPhysical(key.input);
              },
              [&] { return subsetRelational(key.input); })),
      input_(key.input),
      offset_(key.offset),
      count_(key.count),
      partial_(key.partial) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_GE(offset_, 0);
  VELOX_CHECK_GE(count_, 0);
}

size_t Limit::KeyHash::operator()(const Limit* node) const {
  return hashOf(
      node->input(), node->offset(), node->count(), node->isPartial());
}

size_t Limit::KeyHash::operator()(const Key& key) const {
  return hashOf(key.input, key.offset, key.count, key.partial);
}

bool Limit::KeyEq::operator()(const Limit* left, const Limit* right) const {
  return left->input() == right->input() && left->offset() == right->offset() &&
      left->count() == right->count() &&
      left->isPartial() == right->isPartial();
}

bool Limit::KeyEq::operator()(const Key& key, const Limit* node) const {
  return key.input == node->input() && key.offset == node->offset() &&
      key.count == node->count() && key.partial == node->isPartial();
}

bool Limit::KeyEq::operator()(const Limit* node, const Key& key) const {
  return (*this)(key, node);
}

namespace {

// Checks a position Velox reads as a channel of the input. 'role' names it in
// the error.
void checkColumns(const ExprVector& exprs, std::string_view role) {
  for (ExprCP expr : exprs) {
    VELOX_CHECK(
        expr->is(PlanType::kColumnExpr),
        "{} must be a column: {}",
        role,
        expr->toString());
  }
}

// Checks that each partition key appears once, and each order key once and
// never as a partition key. 'node' names the node in the error.
void checkDistinctKeys(
    const ExprVector& partitionKeys,
    const ExprVector& orderKeys,
    std::string_view node) {
  folly::F14FastSet<ExprCP> seen;
  for (ExprCP key : partitionKeys) {
    VELOX_CHECK(
        seen.insert(key).second,
        "{} partition key repeats: {}",
        node,
        key->toString());
  }
  for (ExprCP key : orderKeys) {
    VELOX_CHECK(
        seen.insert(key).second,
        "{} order key repeats a key: {}",
        node,
        key->toString());
  }
}

} // namespace

ColumnVector Node::outputsAmong(const ColumnVector& columns) const {
  const auto produced = PlanObjectSet::fromObjects(outputColumns_);
  ColumnVector result;
  for (ColumnCP column : columns) {
    if (produced.contains(column)) {
      result.push_back(column);
    }
  }
  return result;
}

bool Node::emitsInputColumns() const {
  switch (nodeType_) {
    case NodeType::kSort:
    case NodeType::kTopN:
    case NodeType::kWindow:
    case NodeType::kInference:
    case NodeType::kRowNumber:
    case NodeType::kTopNRowNumber:
    case NodeType::kEnforceDistinct:
    case NodeType::kMarkDistinct:
    case NodeType::kLimit:
    case NodeType::kExchange:
      return true;
    default:
      return false;
  }
}

Partitioning Sort::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return inheritedPartition(inputPartitions[0]);
}

Sort::Sort(Key key)
    : Node(
          NodeType::kSort,
          ColumnVector{key.input->outputColumns()},
          derivedProperties(
              {key.input},
              [&] {
                return sortedPhysical(key.input, key.orderKeys, key.orderTypes);
              },
              [&] { return sameRowsRelational(key.input); })),
      input_(key.input),
      orderKeys_(std::move(key.orderKeys)),
      orderTypes_(std::move(key.orderTypes)),
      partial_(key.partial) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(!orderKeys_.empty(), "Sort must have at least one order key");
  VELOX_CHECK_EQ(orderKeys_.size(), orderTypes_.size());
  checkColumns(orderKeys_, "Sort order key");
}

size_t Sort::KeyHash::operator()(const Sort* node) const {
  return hashOf(
      node->input(), node->orderKeys(), node->orderTypes(), node->isPartial());
}

size_t Sort::KeyHash::operator()(const Key& key) const {
  return hashOf(key.input, key.orderKeys, key.orderTypes, key.partial);
}

bool Sort::KeyEq::operator()(const Sort* left, const Sort* right) const {
  return left->input() == right->input() &&
      left->orderKeys() == right->orderKeys() &&
      left->orderTypes() == right->orderTypes() &&
      left->isPartial() == right->isPartial();
}

bool Sort::KeyEq::operator()(const Key& key, const Sort* node) const {
  return key.input == node->input() && key.orderKeys == node->orderKeys() &&
      key.orderTypes == node->orderTypes() && key.partial == node->isPartial();
}

bool Sort::KeyEq::operator()(const Sort* node, const Key& key) const {
  return (*this)(key, node);
}

Partitioning TopN::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return inheritedPartition(inputPartitions[0]);
}

TopN::TopN(Key key)
    : Node(
          NodeType::kTopN,
          ColumnVector{key.input->outputColumns()},
          derivedProperties(
              {key.input},
              [&] {
                return sortedPhysical(key.input, key.orderKeys, key.orderTypes);
              },
              [&] { return subsetRelational(key.input); })),
      input_(key.input),
      orderKeys_(std::move(key.orderKeys)),
      orderTypes_(std::move(key.orderTypes)),
      offset_(key.offset),
      count_(key.count),
      partial_(key.partial) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(!orderKeys_.empty(), "TopN must have at least one order key");
  VELOX_CHECK_EQ(orderKeys_.size(), orderTypes_.size());
  checkColumns(orderKeys_, "TopN order key");
  VELOX_CHECK_GE(offset_, 0);
  VELOX_CHECK_GE(count_, 0);
}

size_t TopN::KeyHash::operator()(const TopN* node) const {
  return hashOf(
      node->input(),
      node->orderKeys(),
      node->orderTypes(),
      node->offset(),
      node->count(),
      node->isPartial());
}

size_t TopN::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.input,
      key.orderKeys,
      key.orderTypes,
      key.offset,
      key.count,
      key.partial);
}

bool TopN::KeyEq::operator()(const TopN* left, const TopN* right) const {
  return left->input() == right->input() &&
      left->orderKeys() == right->orderKeys() &&
      left->orderTypes() == right->orderTypes() &&
      left->offset() == right->offset() && left->count() == right->count() &&
      left->isPartial() == right->isPartial();
}

bool TopN::KeyEq::operator()(const Key& key, const TopN* node) const {
  return key.input == node->input() && key.orderKeys == node->orderKeys() &&
      key.orderTypes == node->orderTypes() && key.offset == node->offset() &&
      key.count == node->count() && key.partial == node->isPartial();
}

bool TopN::KeyEq::operator()(const TopN* node, const Key& key) const {
  return (*this)(key, node);
}

namespace {

// Physical properties of an aggregate's output, by step. A `kPartial`
// pre-aggregates per task without moving rows or gathering (even for an empty
// grouping): it keeps the input's partitioning re-expressed on its output, but
// its keys are not globally unique (the same key recurs across tasks).
// `kSingle` / `kFinal` emit each group once per driver, and partitioned on the
// grouping keys when the input is (the `kFinal`'s input is the exchange that
// partitioned the partials).
PhysicalProperties aggregatePhysical(const Aggregate::Key& key) {
  // The grouping values are materialized as the leading output columns, aligned
  // one-to-one with `groupingKeys`; properties are expressed over those output
  // columns, not the (possibly computed) grouping-key expressions.
  const ColumnVector groupingColumns =
      aggregateGroupingColumns(key.outputColumns, key.groupingKeys.size());
  const PhysicalProperties& input = key.input->physicalProperties();
  const Partitioning globalPartition = aggregatePartition(
      key.step, input.globalPartition, key.groupingKeys, groupingColumns);
  const Partitioning driverPartition = aggregatePartition(
      key.step, input.driverPartition, key.groupingKeys, groupingColumns);
  if (key.step == AggregateStep::kPartial) {
    // A group may be emitted more than once, so this is not a Grouped local
    // property.
    return PhysicalProperties{
        .globalPartition = globalPartition, .driverPartition = driverPartition};
  }
  PhysicalProperties properties{
      .globalPartition = globalPartition,
      .driverPartition = driverPartition,
      .driverOrder = groupedDriverOrder(groupingColumns)};
  // Without grouping keys the key is the empty set: one row per driver, and
  // one in all when the input is gathered. A grouping-set aggregate's group-id
  // is one of the keys, so distinct key sets stay distinct.
  addUniqueKey(
      properties,
      PlanObjectSet::fromObjects(groupingColumns),
      input,
      key.groupingKeys);
  return properties;
}

// A single-step aggregation emits each group once, so it is unique on its
// grouping columns. Without grouping keys it emits exactly one row.
RelationalProperties aggregateRelational(const Aggregate::Key& key) {
  VELOX_CHECK(
      key.step == AggregateStep::kSingle,
      "A partial or final aggregation requires placed inputs");
  if (key.groupingKeys.empty()) {
    return singleRowRelational();
  }
  return {
      .unique = {PlanObjectSet::fromObjects(aggregateGroupingColumns(
          key.outputColumns, key.groupingKeys.size()))},
  };
}

} // namespace

Partitioning Aggregate::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return aggregatePartition(
      step_,
      inputPartitions[0],
      groupingKeys_,
      aggregateGroupingColumns(outputColumns(), groupingKeys_.size()));
}

Aggregate::Aggregate(Key key)
    : Node(
          NodeType::kAggregate,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.input},
              [&] { return aggregatePhysical(key); },
              [&] { return aggregateRelational(key); })),
      input_(key.input),
      groupingKeys_(std::move(key.groupingKeys)),
      aggregates_(std::move(key.aggregates)),
      step_(key.step),
      groupId_(key.groupId),
      globalGroupingSets_(std::move(key.globalGroupingSets)) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_EQ(
      this->outputColumns().size(), groupingKeys_.size() + aggregates_.size());
  // Velox couples the two: a group-id key is meaningful for empty-input default
  // rows only alongside the global sets it labels.
  VELOX_CHECK_EQ(groupId_ != nullptr, !globalGroupingSets_.empty());
  if (groupId_ != nullptr) {
    VELOX_CHECK(
        std::ranges::find(groupingKeys_, groupId_) != groupingKeys_.end(),
        "groupId must be one of the grouping keys");
  }
}

size_t Aggregate::KeyHash::operator()(const Aggregate* node) const {
  size_t hash = hashOf(
      node->input(),
      node->groupingKeys(),
      node->aggregates(),
      node->outputColumns());
  hash = velox::bits::hashMix(hash, hashOf(node->groupId()));
  for (auto idx : node->globalGroupingSets()) {
    hash = velox::bits::hashMix(hash, static_cast<size_t>(idx));
  }
  return velox::bits::hashMix(hash, static_cast<size_t>(node->step()));
}

size_t Aggregate::KeyHash::operator()(const Key& key) const {
  size_t hash =
      hashOf(key.input, key.groupingKeys, key.aggregates, key.outputColumns);
  hash = velox::bits::hashMix(hash, hashOf(key.groupId));
  for (auto idx : key.globalGroupingSets) {
    hash = velox::bits::hashMix(hash, static_cast<size_t>(idx));
  }
  return velox::bits::hashMix(hash, static_cast<size_t>(key.step));
}

bool Aggregate::KeyEq::operator()(const Aggregate* left, const Aggregate* right)
    const {
  return left->input() == right->input() &&
      left->groupingKeys() == right->groupingKeys() &&
      left->aggregates() == right->aggregates() &&
      left->outputColumns() == right->outputColumns() &&
      left->step() == right->step() && left->groupId() == right->groupId() &&
      left->globalGroupingSets() == right->globalGroupingSets();
}

bool Aggregate::KeyEq::operator()(const Key& key, const Aggregate* node) const {
  return key.input == node->input() &&
      key.groupingKeys == node->groupingKeys() &&
      key.aggregates == node->aggregates() &&
      key.outputColumns == node->outputColumns() && key.step == node->step() &&
      key.groupId == node->groupId() &&
      key.globalGroupingSets == node->globalGroupingSets();
}

bool Aggregate::KeyEq::operator()(const Aggregate* node, const Key& key) const {
  return (*this)(key, node);
}

Partitioning GroupId::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return retainedPartition(inputPartitions[0], outputColumns());
}

GroupId::GroupId(Key key)
    : Node(
          NodeType::kGroupId,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.input},
              [&] {
                return PhysicalProperties{
                    .globalPartition = retainedPartition(
                        key.input->physicalProperties().globalPartition,
                        key.outputColumns),
                    .driverPartition = retainedPartition(
                        key.input->physicalProperties().driverPartition,
                        key.outputColumns)};
              },
              [] { return RelationalProperties{}; })),
      input_(key.input),
      groupingKeys_(std::move(key.groupingKeys)),
      aggregationInputs_(std::move(key.aggregationInputs)),
      groupingSets_(std::move(key.groupingSets)),
      groupingKeyColumns_(std::move(key.groupingKeyColumns)),
      groupId_(key.groupId) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_NOT_NULL(groupId_);
  VELOX_CHECK_EQ(groupId_->value().type->kind(), velox::TypeKind::BIGINT);
  VELOX_CHECK_EQ(groupingKeys_.size(), groupingKeyColumns_.size());
  VELOX_CHECK(!groupingSets_.empty());
  VELOX_CHECK_EQ(
      this->outputColumns().size(),
      groupingKeyColumns_.size() + aggregationInputs_.size() + 1);
}

size_t GroupId::KeyHash::operator()(const GroupId* node) const {
  size_t hash = hashOf(
      node->input(),
      node->groupingKeys(),
      node->aggregationInputs(),
      node->groupingKeyColumns(),
      node->groupId(),
      node->outputColumns());
  const auto& groupingSets = node->groupingSets();
  for (size_t setIdx = 0; setIdx < groupingSets.size(); ++setIdx) {
    // Mix setIdx and set size to break symmetry across index permutations
    // (e.g., {{1,2},{3}} vs {{1},{2,3}}).
    hash = velox::bits::hashMix(hash, setIdx);
    hash = velox::bits::hashMix(hash, groupingSets[setIdx].size());
    for (auto index : groupingSets[setIdx]) {
      hash = velox::bits::hashMix(hash, static_cast<size_t>(index));
    }
  }
  return hash;
}

size_t GroupId::KeyHash::operator()(const Key& key) const {
  size_t hash = hashOf(
      key.input,
      key.groupingKeys,
      key.aggregationInputs,
      key.groupingKeyColumns,
      key.groupId,
      key.outputColumns);
  const auto& groupingSets = key.groupingSets;
  for (size_t setIdx = 0; setIdx < groupingSets.size(); ++setIdx) {
    // Mix setIdx and set size to break symmetry across index permutations
    // (e.g., {{1,2},{3}} vs {{1},{2,3}}).
    hash = velox::bits::hashMix(hash, setIdx);
    hash = velox::bits::hashMix(hash, groupingSets[setIdx].size());
    for (auto index : groupingSets[setIdx]) {
      hash = velox::bits::hashMix(hash, static_cast<size_t>(index));
    }
  }
  return hash;
}

bool GroupId::KeyEq::operator()(const GroupId* left, const GroupId* right)
    const {
  return left->input() == right->input() &&
      left->groupingKeys() == right->groupingKeys() &&
      left->aggregationInputs() == right->aggregationInputs() &&
      left->groupingSets() == right->groupingSets() &&
      left->groupingKeyColumns() == right->groupingKeyColumns() &&
      left->groupId() == right->groupId() &&
      left->outputColumns() == right->outputColumns();
}

bool GroupId::KeyEq::operator()(const Key& key, const GroupId* node) const {
  return key.input == node->input() &&
      key.groupingKeys == node->groupingKeys() &&
      key.aggregationInputs == node->aggregationInputs() &&
      key.groupingSets == node->groupingSets() &&
      key.groupingKeyColumns == node->groupingKeyColumns() &&
      key.groupId == node->groupId() &&
      key.outputColumns == node->outputColumns();
}

bool GroupId::KeyEq::operator()(const GroupId* node, const Key& key) const {
  return (*this)(key, node);
}

Partitioning MarkDistinct::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return inheritedPartition(inputPartitions[0]);
}

MarkDistinct::MarkDistinct(Key key)
    : Node(
          NodeType::kMarkDistinct,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.input},
              [&] { return passThroughPhysical(key.input); },
              [&] { return sameRowsRelational(key.input); })),
      input_(key.input),
      markers_(std::move(key.markers)),
      distinctKeys_(std::move(key.distinctKeys)),
      masks_(std::move(key.masks)) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(!markers_.empty());
  const size_t expectedMarkers = masks_.empty() ? 1 : masks_.size() + 1;
  VELOX_CHECK_EQ(markers_.size(), expectedMarkers);
  for (ColumnCP marker : markers_) {
    VELOX_CHECK_EQ(marker->value().type->kind(), velox::TypeKind::BOOLEAN);
  }
  for (ColumnCP mask : masks_) {
    VELOX_CHECK_EQ(mask->value().type->kind(), velox::TypeKind::BOOLEAN);
  }
  VELOX_CHECK_EQ(
      this->outputColumns().size(),
      input_->outputColumns().size() + markers_.size());
  VELOX_CHECK(
      !distinctKeys_.empty(),
      "MarkDistinct must have at least one distinct key");
  checkColumns(distinctKeys_, "MarkDistinct distinct key");
}

size_t MarkDistinct::KeyHash::operator()(const MarkDistinct* node) const {
  return hashOf(
      node->input(),
      node->markers(),
      node->distinctKeys(),
      node->masks(),
      node->outputColumns());
}

size_t MarkDistinct::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.input, key.markers, key.distinctKeys, key.masks, key.outputColumns);
}

bool MarkDistinct::KeyEq::operator()(
    const MarkDistinct* left,
    const MarkDistinct* right) const {
  return left->input() == right->input() &&
      left->markers() == right->markers() &&
      left->distinctKeys() == right->distinctKeys() &&
      left->masks() == right->masks() &&
      left->outputColumns() == right->outputColumns();
}

bool MarkDistinct::KeyEq::operator()(const Key& key, const MarkDistinct* node)
    const {
  return key.input == node->input() && key.markers == node->markers() &&
      key.distinctKeys == node->distinctKeys() && key.masks == node->masks() &&
      key.outputColumns == node->outputColumns();
}

bool MarkDistinct::KeyEq::operator()(const MarkDistinct* node, const Key& key)
    const {
  return (*this)(key, node);
}

Partitioning Values::globalPartition(
    std::span<const Partitioning> /*inputPartitions*/,
    Builder& /*builder*/) const {
  return physicalProperties().globalPartition;
}

Values::Values(Key key)
    : Node(
          NodeType::kValues,
          ColumnVector{key.outputColumns},
          leafProperties(key.placement, [] { return PhysicalProperties{}; })),
      source_(key.source),
      rows_(key.rows),
      channels_(std::move(key.channels)),
      placement_(key.placement) {
  VELOX_CHECK(
      placement_ == Placement::kUnplaced || onOneTask(placement_),
      "Values must run on one task");
  VELOX_CHECK(
      source_ == nullptr || rows_ == nullptr,
      "Values cannot carry both a passthrough source and folded rows");

  const auto numColumns = this->outputColumns().size();
  VELOX_CHECK_EQ(
      channels_.size(),
      numColumns,
      "Values channels must have one entry per output column");

  if (source_ != nullptr) {
    const auto& rowType = source_->outputType();
    for (size_t i = 0; i < numColumns; ++i) {
      VELOX_CHECK_LT(channels_[i], rowType->size());
      VELOX_CHECK(
          rowType->childAt(channels_[i])
              ->equivalent(*this->outputColumns()[i]->value().type),
          "Values source column {} type must match outputColumns",
          i);
    }
  } else if (rows_ != nullptr) {
    VELOX_CHECK_EQ(
        rows_->kind(),
        velox::TypeKind::ARRAY,
        "Values folded rows must be an array of row variants");
    for (const auto& row : rows_->array()) {
      VELOX_CHECK_EQ(
          row.kind(),
          velox::TypeKind::ROW,
          "Values folded rows must be row variants");
      for (auto channel : channels_) {
        VELOX_CHECK_LT(channel, row.row().size());
      }
    }
  }
}

const velox::Variant& Values::valueAt(size_t row, size_t column) const {
  VELOX_CHECK_NOT_NULL(rows_);
  return rows_->array()[row].row()[channels_[column]];
}

size_t Values::cardinality() const {
  if (rows_ != nullptr) {
    return rows_->array().size();
  }
  return source_ != nullptr ? source_->cardinality() : 0;
}

bool Values::isEmpty(NodeCP node) {
  return node->is(NodeType::kValues) && node->as<Values>()->cardinality() == 0;
}

bool Values::isSingleRowNoColumns(NodeCP node) {
  return node->is(NodeType::kValues) && node->outputColumns().empty() &&
      node->as<Values>()->cardinality() == 1;
}

size_t Values::KeyHash::operator()(const Values* node) const {
  return hashOf(
      node->source(),
      node->rows(),
      node->outputColumns(),
      node->channels(),
      node->placement());
}

size_t Values::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.source, key.rows, key.outputColumns, key.channels, key.placement);
}

bool Values::KeyEq::operator()(const Values* left, const Values* right) const {
  return left->source() == right->source() && left->rows() == right->rows() &&
      left->outputColumns() == right->outputColumns() &&
      left->channels() == right->channels() &&
      left->placement() == right->placement();
}

bool Values::KeyEq::operator()(const Key& key, const Values* node) const {
  return key.source == node->source() && key.rows == node->rows() &&
      key.outputColumns == node->outputColumns() &&
      key.channels == node->channels() && key.placement == node->placement();
}

bool Values::KeyEq::operator()(const Values* node, const Key& key) const {
  return (*this)(key, node);
}

Partitioning Unnest::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return retainedPartition(inputPartitions[0], replicatedColumns());
}

PlanObjectSet Unnest::generatedColumns() const {
  PlanObjectSet columns;
  for (const auto& perExpression : unnestColumns_) {
    columns.unionObjects(perExpression);
  }
  if (ordinalityColumn_ != nullptr) {
    columns.add(ordinalityColumn_);
  }
  if (markerColumn_ != nullptr) {
    columns.add(markerColumn_);
  }
  return columns;
}

Unnest::Unnest(Key key)
    : Node(
          NodeType::kUnnest,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.input},
              [&] {
                return PhysicalProperties{
                    .globalPartition = retainedPartition(
                        key.input->physicalProperties().globalPartition,
                        key.replicatedColumns),
                    .driverPartition = retainedPartition(
                        key.input->physicalProperties().driverPartition,
                        key.replicatedColumns),
                    .driverOrder =
                        retainedDriverOrder(key.input, key.replicatedColumns)};
              },
              [] { return RelationalProperties{}; })),
      input_(key.input),
      unnestExpressions_(std::move(key.unnestExpressions)),
      replicatedColumns_(std::move(key.replicatedColumns)),
      unnestColumns_(std::move(key.unnestColumns)),
      ordinalityColumn_(key.ordinalityColumn),
      markerColumn_(key.markerColumn) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(
      !unnestExpressions_.empty(), "Unnest must have at least one expression");
  VELOX_CHECK_EQ(
      unnestColumns_.size(),
      unnestExpressions_.size(),
      "Unnest unnestColumns must align with unnestExpressions");
  // Each expression unnests into one result column for an ARRAY, two (key,
  // value) for a MAP.
  for (size_t i = 0; i < unnestExpressions_.size(); ++i) {
    const auto kind = unnestExpressions_[i]->value().type->kind();
    if (kind == velox::TypeKind::ARRAY) {
      VELOX_CHECK_EQ(
          unnestColumns_[i].size(),
          1,
          "Unnest of an ARRAY expression produces one result column");
    } else if (kind == velox::TypeKind::MAP) {
      VELOX_CHECK_EQ(
          unnestColumns_[i].size(),
          2,
          "Unnest of a MAP expression produces two result columns (key, value)");
    } else {
      VELOX_FAIL(
          "Unnest expression must be ARRAY or MAP, got {}",
          unnestExpressions_[i]->value().type->toString());
    }
  }
  // Every outputColumns entry must be a Column the structured fields produce: a
  // replicated input column, a per-expression result, or the ordinality column.
  // Only per-expression results may be pruned from the output; replicated
  // columns and the ordinality column must all appear.
  PlanObjectSet produced;
  for (ColumnCP column : replicatedColumns_) {
    produced.add(column);
  }
  for (const auto& perExpr : unnestColumns_) {
    for (ColumnCP column : perExpr) {
      produced.add(column);
    }
  }
  if (ordinalityColumn_ != nullptr) {
    produced.add(ordinalityColumn_);
  }
  if (markerColumn_ != nullptr) {
    VELOX_CHECK_EQ(
        markerColumn_->value().type->kind(),
        velox::TypeKind::BOOLEAN,
        "Unnest markerColumn must be BOOLEAN");
    produced.add(markerColumn_);
  }

  for (ColumnCP column : this->outputColumns()) {
    VELOX_CHECK(
        produced.contains(column),
        "Unnest outputColumns references a Column not produced by structured fields");
  }

  const auto outputSet = PlanObjectSet::fromObjects(this->outputColumns());
  for (ColumnCP column : replicatedColumns_) {
    VELOX_CHECK(
        outputSet.contains(column),
        "Unnest replicatedColumns must all appear in outputColumns");
  }
  VELOX_CHECK(
      ordinalityColumn_ == nullptr || outputSet.contains(ordinalityColumn_),
      "Unnest ordinalityColumn must appear in outputColumns");
  VELOX_CHECK(
      markerColumn_ == nullptr || outputSet.contains(markerColumn_),
      "Unnest markerColumn must appear in outputColumns");
}

size_t Unnest::KeyHash::operator()(const Unnest* node) const {
  size_t hash = hashOf(
      node->input(),
      node->unnestExpressions(),
      node->outputColumns(),
      node->replicatedColumns(),
      node->ordinalityColumn(),
      node->markerColumn());
  const auto& unnestColumns = node->unnestColumns();
  for (size_t idx = 0; idx < unnestColumns.size(); ++idx) {
    // Mix idx and inner size to break symmetry across index permutations
    // (e.g., [[a,b],[c]] vs [[a],[b,c]]).
    hash = velox::bits::hashMix(hash, idx);
    hash = velox::bits::hashMix(hash, unnestColumns[idx].size());
    for (const auto* column : unnestColumns[idx]) {
      hash = velox::bits::hashMix(hash, reinterpret_cast<uintptr_t>(column));
    }
  }
  return hash;
}

size_t Unnest::KeyHash::operator()(const Key& key) const {
  size_t hash = hashOf(
      key.input,
      key.unnestExpressions,
      key.outputColumns,
      key.replicatedColumns,
      key.ordinalityColumn,
      key.markerColumn);
  const auto& unnestColumns = key.unnestColumns;
  for (size_t idx = 0; idx < unnestColumns.size(); ++idx) {
    // Mix idx and inner size to break symmetry across index permutations
    // (e.g., [[a,b],[c]] vs [[a],[b,c]]).
    hash = velox::bits::hashMix(hash, idx);
    hash = velox::bits::hashMix(hash, unnestColumns[idx].size());
    for (const auto* column : unnestColumns[idx]) {
      hash = velox::bits::hashMix(hash, reinterpret_cast<uintptr_t>(column));
    }
  }
  return hash;
}

bool Unnest::KeyEq::operator()(const Unnest* left, const Unnest* right) const {
  return left->input() == right->input() &&
      left->unnestExpressions() == right->unnestExpressions() &&
      left->replicatedColumns() == right->replicatedColumns() &&
      left->unnestColumns() == right->unnestColumns() &&
      left->ordinalityColumn() == right->ordinalityColumn() &&
      left->markerColumn() == right->markerColumn() &&
      left->outputColumns() == right->outputColumns();
}

bool Unnest::KeyEq::operator()(const Key& key, const Unnest* node) const {
  return key.input == node->input() &&
      key.unnestExpressions == node->unnestExpressions() &&
      key.replicatedColumns == node->replicatedColumns() &&
      key.unnestColumns == node->unnestColumns() &&
      key.ordinalityColumn == node->ordinalityColumn() &&
      key.markerColumn == node->markerColumn() &&
      key.outputColumns == node->outputColumns();
}

bool Unnest::KeyEq::operator()(const Unnest* node, const Key& key) const {
  return (*this)(key, node);
}

namespace {

// Output global partitioning of a union:
//  - Gather, when every leg is already gathered (single-task): the union then
//    runs as one task, so no isolating exchange is needed.
//  - Otherwise bucketed on the union output columns, when every leg is
//    connector-bucketed on leg columns that map (via legColumns) to those same
//    output columns, with copartitionable partitionings. The representative
//    type is the min-partition-count leg's — its count equals the copartition's
//    — reused as a query-lifetime pointer.
//  - Unspecified otherwise, so the union then distributes its legs
//    independently.
Partitioning unionGlobalPartition(
    std::span<const Partitioning> legPartitions,
    const QGVector<ColumnVector>& allLegColumns,
    const ColumnVector& outputColumns) {
  bool allGathered = true;
  for (const Partitioning& legPartition : legPartitions) {
    if (!legPartition.is(PartitionKind::kGather)) {
      allGathered = false;
      break;
    }
  }
  if (allGathered) {
    return Partitioning::gather(PropertyScope::kGlobal);
  }

  // A partitionType of nullptr is standard hash partitioning; a non-null one is
  // a connector's bucketing. Two legs co-partition on the mapped output keys
  // when both are standard hash (same hash, so a given key value lands on the
  // same partition in every leg) or both are copartitionable connector types.
  // The two kinds never co-partition with each other.
  const connector::PartitionType* representative = nullptr;
  ExprVector outputKeys;
  for (size_t leg = 0; leg < legPartitions.size(); ++leg) {
    const Partitioning& part = legPartitions[leg];
    if (part.kind != PartitionKind::kPartitioned) {
      return {};
    }
    // Map each leg partition key to the union output column at the same index.
    const ColumnVector& legColumns = allLegColumns[leg];
    ExprVector legOutputKeys;
    legOutputKeys.reserve(part.keys.size());
    for (ExprCP legKey : part.keys) {
      ExprCP mapped = nullptr;
      for (size_t i = 0; i < legColumns.size(); ++i) {
        if (legColumns[i] == legKey) {
          mapped = outputColumns[i];
          break;
        }
      }
      if (mapped == nullptr) {
        return {};
      }
      legOutputKeys.push_back(mapped);
    }

    if (leg == 0) {
      outputKeys = std::move(legOutputKeys);
      representative = part.partitionType;
      continue;
    }
    if (legOutputKeys.size() != outputKeys.size()) {
      return {};
    }
    for (size_t i = 0; i < outputKeys.size(); ++i) {
      if (legOutputKeys[i] != outputKeys[i]) {
        return {};
      }
    }
    const auto common = Partitioning::commonUnionPartitionType(
        representative, part.partitionType);
    if (!common.has_value()) {
      return {};
    }
    representative = common->connectorType;
  }
  return Partitioning{
      .kind = PartitionKind::kPartitioned,
      .partitionType = representative,
      .keys = std::move(outputKeys),
      .scope = PropertyScope::kGlobal};
}

// Each input is non-null and presents its leg's columns, which line up with
// the outputs by position.
void checkColumns(const UnionAll::Key& key) {
  VELOX_CHECK_GE(key.inputs.size(), 2, "UnionAll requires at least two inputs");
  for (auto* input : key.inputs) {
    VELOX_CHECK_NOT_NULL(input);
  }
  VELOX_CHECK_EQ(
      key.legColumns.size(),
      key.inputs.size(),
      "UnionAll legColumns must have one entry per input");
  for (size_t i = 0; i < key.inputs.size(); ++i) {
    VELOX_CHECK_EQ(
        key.legColumns[i].size(),
        key.outputColumns.size(),
        "UnionAll legColumns[i] must align with outputColumns");
    const auto available =
        PlanObjectSet::fromObjects(key.inputs[i]->outputColumns());
    for (ColumnCP column : key.legColumns[i]) {
      VELOX_CHECK(
          available.contains(column),
          "UnionAll legColumns[{}] references a Column not in inputs[{}]->outputColumns()",
          i,
          i);
    }
  }
}

// Checks the columns before deriving the partitioning, which maps them by
// position.
Node::Properties unionProperties(const UnionAll::Key& key) {
  checkColumns(key);
  if (!placedInputs(key.inputs)) {
    return RelationalProperties{};
  }
  return PhysicalProperties{
      .globalPartition = unionGlobalPartition(
          Node::globalPartitions(key.inputs),
          key.legColumns,
          key.outputColumns),
      // UnionAll deals input batches round-robin across drivers, so no driver
      // placement is guaranteed.
      .driverPartition = Partitioning::arbitrary(PropertyScope::kDriver)};
}

} // namespace

Partitioning UnionAll::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return unionGlobalPartition(inputPartitions, legColumns_, outputColumns());
}

UnionAll::UnionAll(Key key)
    : Node(
          NodeType::kUnionAll,
          ColumnVector{key.outputColumns},
          unionProperties(key)),
      inputs_(std::move(key.inputs)),
      legColumns_(std::move(key.legColumns)) {}

size_t UnionAll::KeyHash::operator()(const UnionAll* node) const {
  const auto& legColumns = node->legColumns();
  size_t hash = hashOf(node->inputs(), node->outputColumns());
  for (size_t i = 0; i < legColumns.size(); ++i) {
    // Mix the leg index and size to break symmetry across pointer permutations
    // (e.g., [[a,b],[c,d]] vs [[a,d],[c,b]]).
    hash = velox::bits::hashMix(hash, i);
    hash = velox::bits::hashMix(hash, legColumns[i].size());
    for (const auto* column : legColumns[i]) {
      hash = velox::bits::hashMix(hash, reinterpret_cast<uintptr_t>(column));
    }
  }
  return hash;
}

size_t UnionAll::KeyHash::operator()(const Key& key) const {
  size_t hash = hashOf(key.inputs, key.outputColumns);
  for (size_t i = 0; i < key.legColumns.size(); ++i) {
    // Mix the leg index and size to break symmetry across pointer permutations
    // (e.g., [[a,b],[c,d]] vs [[a,d],[c,b]]).
    hash = velox::bits::hashMix(hash, i);
    hash = velox::bits::hashMix(hash, key.legColumns[i].size());
    for (const auto* column : key.legColumns[i]) {
      hash = velox::bits::hashMix(hash, reinterpret_cast<uintptr_t>(column));
    }
  }
  return hash;
}

bool UnionAll::KeyEq::operator()(const UnionAll* left, const UnionAll* right)
    const {
  return std::ranges::equal(left->inputs(), right->inputs()) &&
      left->legColumns() == right->legColumns() &&
      left->outputColumns() == right->outputColumns();
}

bool UnionAll::KeyEq::operator()(const Key& key, const UnionAll* node) const {
  return std::ranges::equal(key.inputs, node->inputs()) &&
      key.legColumns == node->legColumns() &&
      key.outputColumns == node->outputColumns();
}

bool UnionAll::KeyEq::operator()(const UnionAll* node, const Key& key) const {
  return (*this)(key, node);
}

bool Join::projectsMark(velox::core::JoinType joinType) {
  return joinType == velox::core::JoinType::kLeftSemiProject ||
      joinType == velox::core::JoinType::kRightSemiProject;
}

bool Join::preservesSource(
    velox::core::JoinType joinType,
    ColumnCP source,
    const PlanObjectSet& leftColumns,
    const PlanObjectSet& rightColumns) {
  const auto preserved = preservedSides(joinType);
  return (preserved.left && leftColumns.contains(source)) ||
      (preserved.right && rightColumns.contains(source));
}

Partitioning Join::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& builder) const {
  return outputPartitioning(
      joinType_,
      inputPartitions[0],
      inputPartitions[1],
      leftKeys_,
      rightKeys_,
      PlanObjectSet::fromObjects(outputColumns()),
      outputColumns(),
      sourceColumns_,
      builder);
}

namespace {

Partitioning joinDriverPartitioning(const Join::Key& key, Builder& builder) {
  // Probe placement survives only when the join preserves probe rows. Two
  // gathered inputs remain gathered for every join type.
  const Partitioning& left = key.left->physicalProperties().driverPartition;
  const Partitioning& right = key.right->physicalProperties().driverPartition;
  const bool preservesProbe = Join::preservedSides(key.joinType).left;
  if (!preservesProbe &&
      !(left.is(PartitionKind::kGather) && right.is(PartitionKind::kGather))) {
    return Partitioning::unspecified(PropertyScope::kDriver);
  }
  const auto outputColumns = PlanObjectSet::fromObjects(key.outputColumns);
  Partitioning result = Join::outputPartitioning(
      key.joinType,
      left,
      right,
      key.leftKeys,
      key.rightKeys,
      outputColumns,
      key.outputColumns,
      key.effectiveSourceColumns(),
      builder);
  if (!preservesProbe || result.is(PartitionKind::kPartitioned) ||
      result.is(PartitionKind::kGather)) {
    return result;
  }
  // A globally unique probe key names one probe row, and that row's output
  // stays on its driver.
  for (const PlanObjectSet& unique :
       key.left->physicalProperties().globalUnique) {
    if (unique.isSubset(outputColumns)) {
      return Partitioning::hash(
          PropertyScope::kDriver, unique.toObjects<Expr>());
    }
  }
  return result;
}

// The unique keys of each side the join emits each row of at most once hold
// for the output, re-expressed on the output columns. 'sideKeys' returns a
// side's unique keys.
template <typename SideKeys>
QGVector<PlanObjectSet> joinUniqueKeys(
    const Join::Key& key,
    const SideKeys& sideKeys) {
  ExprVector sources;
  for (ColumnCP source : key.effectiveSourceColumns()) {
    sources.push_back(source);
  }
  QGVector<PlanObjectSet> result;
  const auto keepUniqueOf = [&](NodeCP side) {
    for (PlanObjectSet& unique :
         remapKeySets(sideKeys(side), sources, key.outputColumns)) {
      result.push_back(std::move(unique));
    }
  };
  const Join::Sides emittedOnce = Join::emittedOnceSides(
      key.joinType, key.left, key.right, key.leftKeys, key.rightKeys);
  if (emittedOnce.left) {
    keepUniqueOf(key.left);
  }
  if (emittedOnce.right) {
    keepUniqueOf(key.right);
  }
  return result;
}

RelationalProperties joinRelational(const Join::Key& key) {
  return {.unique = joinUniqueKeys(key, [](NodeCP side) {
            return side->relationalProperties().unique;
          })};
}

} // namespace

Join::Join(Key key, Builder& builder)
    : Node(
          NodeType::kJoin,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.left, key.right},
              [&] {
                return PhysicalProperties{
                    .globalPartition = outputPartitioning(
                        key.joinType,
                        key.left->physicalProperties().globalPartition,
                        key.right->physicalProperties().globalPartition,
                        key.leftKeys,
                        key.rightKeys,
                        PlanObjectSet::fromObjects(key.outputColumns),
                        key.outputColumns,
                        key.effectiveSourceColumns(),
                        builder),
                    .driverPartition = joinDriverPartitioning(key, builder),
                    .driverOrder = joinDriverOrder(
                        key.joinType, key.left, key.outputColumns),
                    // A side's driver-scoped keys need not survive the
                    // join's redistribution of rows across drivers.
                    .globalUnique = joinUniqueKeys(key, [](NodeCP side) {
                      return side->physicalProperties().globalUnique;
                    })};
              },
              [&] { return joinRelational(key); })),
      inputs_{key.left, key.right},
      joinType_(key.joinType),
      leftKeys_(std::move(key.leftKeys)),
      rightKeys_(std::move(key.rightKeys)),
      filter_(std::move(key.filter)),
      nullAware_(key.nullAware),
      nullAsValue_(key.nullAsValue),
      buildIsLoopInvariant_(key.buildIsLoopInvariant),
      sourceColumns_(key.effectiveSourceColumns()) {
  VELOX_CHECK_NOT_NULL(inputs_[0]);
  VELOX_CHECK_NOT_NULL(inputs_[1]);
  VELOX_CHECK_EQ(leftKeys_.size(), rightKeys_.size());
  VELOX_CHECK(
      !buildIsLoopInvariant_ ||
          canBuildLoopInvariant(
              joinType_, rightKeys_, nullAsValue_, inputs_[0], inputs_[1]),
      "Join build cannot be loop-invariant");
  VELOX_CHECK_EQ(sourceColumns_.size(), outputColumns().size());
  const PlanObjectSet leftColumns =
      PlanObjectSet::fromObjects(inputs_[0]->outputColumns());
  const PlanObjectSet rightColumns =
      PlanObjectSet::fromObjects(inputs_[1]->outputColumns());
  PlanObjectSet available = leftColumns;
  available.unionSet(rightColumns);
  for (size_t i = 0; i < sourceColumns_.size(); ++i) {
    if ((joinType_ == velox::core::JoinType::kLeftSemiProject ||
         joinType_ == velox::core::JoinType::kRightSemiProject) &&
        i + 1 == sourceColumns_.size()) {
      VELOX_CHECK(
          sourceColumns_[i] == outputColumns()[i],
          "Join mark output must supply itself");
      continue;
    }
    VELOX_CHECK(
        available.contains(sourceColumns_[i]),
        "Join source column must come from an input. Output: {}. Source: {}",
        outputColumns()[i]->toString(),
        sourceColumns_[i]->toString());
    const bool sourcePreserved = preservesSource(
        joinType_, sourceColumns_[i], leftColumns, rightColumns);
    VELOX_CHECK(
        sourcePreserved || outputColumns()[i] != sourceColumns_[i],
        "A null-extended Join output must have a fresh identity: {}",
        outputColumns()[i]->toString());
  }
  if (nullAware_) {
    VELOX_CHECK(
        joinType_ == velox::core::JoinType::kAnti ||
            joinType_ == velox::core::JoinType::kLeftSemiProject ||
            joinType_ == velox::core::JoinType::kRightSemiProject,
        "nullAware only valid for kAnti / kLeftSemiProject / kRightSemiProject, got: {}",
        joinType_);
    // Velox forbids a null-aware right semi project that also carries an
    // internal filter (velox PlanNode.h). The negated-mark filter of a
    // reversed antijoin is a separate FilterNode, not this internal filter.
    VELOX_CHECK(
        joinType_ != velox::core::JoinType::kRightSemiProject ||
            filter_.empty(),
        "Null-aware kRightSemiProject does not support an internal filter");
  }
  VELOX_CHECK(
      !(nullAware_ && nullAsValue_),
      "nullAware and nullAsValue are mutually exclusive");

  if (projectsMark(joinType_)) {
    VELOX_CHECK(
        !outputColumns().empty(),
        "A semi-project join must output its mark: {}",
        joinTypeName());
    VELOX_CHECK_EQ(
        outputColumns().back()->value().type->kind(),
        velox::TypeKind::BOOLEAN,
        "A semi-project join must output its mark last: {}",
        joinTypeName());
  }
}

Join::Sides Join::preservedSides(velox::core::JoinType joinType) {
  using JoinType = velox::core::JoinType;
  switch (joinType) {
    case JoinType::kInner:
      return {.left = true, .right = true};
    case JoinType::kLeft:
    case JoinType::kLeftSemiFilter:
    case JoinType::kCountingLeftSemiFilter:
    case JoinType::kLeftSemiProject:
    case JoinType::kAnti:
    case JoinType::kCountingAnti:
      return {.left = true};
    case JoinType::kRight:
    case JoinType::kRightSemiFilter:
    case JoinType::kRightSemiProject:
    case JoinType::kRightAnti:
      return {.right = true};
    case JoinType::kFull:
      return {};
    case JoinType::kNumJoinTypes:
      break;
  }
  VELOX_UNREACHABLE();
}

namespace {
// True when some unique key of 'node' satisfies 'matches'. Reads the relational
// unique keys before placement and the global ones after.
template <typename Predicate>
bool anyUniqueKey(NodeCP node, Predicate matches) {
  if (node->placed()) {
    return std::ranges::any_of(
        node->physicalProperties().globalUnique, matches);
  }
  return std::ranges::any_of(node->relationalProperties().unique, matches);
}

// True when 'node' has at most one row: a unique key with no columns.
bool atMostOneRow(NodeCP node) {
  return anyUniqueKey(
      node, [](const PlanObjectSet& unique) { return unique.empty(); });
}

// True when 'node' has at most one row for each value of 'keys': some unique
// key is a subset of the column keys. Keys that are not columns only refine
// the values, so they are ignored.
bool uniqueOn(NodeCP node, const ExprVector& keys) {
  VELOX_CHECK_GT(keys.size(), 0, "Unique key check must have keys");
  PlanObjectSet columns;
  for (ExprCP key : keys) {
    if (key->is(PlanType::kColumnExpr)) {
      columns.add(key);
    }
  }
  return anyUniqueKey(node, [&](const PlanObjectSet& unique) {
    return unique.isSubset(columns);
  });
}
} // namespace

Join::Sides Join::emittedOnceSides(
    velox::core::JoinType joinType,
    NodeCP left,
    NodeCP right,
    const ExprVector& leftKeys,
    const ExprVector& rightKeys) {
  using JoinType = velox::core::JoinType;
  // True when 'side' has at most one row for each value of 'keys'. A cross
  // join has no keys and matches every row of 'side'.
  const auto matchesAtMostOne = [](NodeCP side, const ExprVector& keys) {
    return keys.empty() ? atMostOneRow(side) : uniqueOn(side, keys);
  };
  switch (joinType) {
    case JoinType::kInner:
      return {
          .left = matchesAtMostOne(right, rightKeys),
          .right = matchesAtMostOne(left, leftKeys)};
    case JoinType::kLeft:
      return {.left = matchesAtMostOne(right, rightKeys)};
    case JoinType::kRight:
      return {.right = matchesAtMostOne(left, leftKeys)};
    case JoinType::kLeftSemiFilter:
    case JoinType::kLeftSemiProject:
    case JoinType::kAnti:
      return {.left = true};
    case JoinType::kRightSemiFilter:
    case JoinType::kRightSemiProject:
    case JoinType::kRightAnti:
      return {.right = true};
    case JoinType::kFull:
    case JoinType::kCountingLeftSemiFilter:
    case JoinType::kCountingAnti:
      return {};
    case JoinType::kNumJoinTypes:
      break;
  }
  VELOX_UNREACHABLE();
}

Partitioning IndexLookupJoin::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return retainedPartition(inputPartitions[0], outputColumns());
}

IndexLookupJoin::IndexLookupJoin(Key key)
    : Node(
          NodeType::kIndexLookupJoin,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.probe},
              [&] {
                return PhysicalProperties{
                    .globalPartition = retainedPartition(
                        key.probe->physicalProperties().globalPartition,
                        key.outputColumns),
                    .driverPartition = retainedPartition(
                        key.probe->physicalProperties().driverPartition,
                        key.outputColumns)};
              },
              [] { return RelationalProperties{}; })),
      probe_(key.probe),
      lookupTable_(key.lookupTable),
      index_(key.index),
      lookupOutputColumns_(std::move(key.lookupOutputColumns)),
      lookupHandle_(key.lookupHandle),
      joinType_(key.joinType),
      probeKeys_(std::move(key.probeKeys)),
      lookupKeys_(std::move(key.lookupKeys)),
      filter_(std::move(key.filter)),
      sourceColumns_(std::move(key.sourceColumns)) {
  VELOX_CHECK_NOT_NULL(probe_);
  VELOX_CHECK_NOT_NULL(lookupTable_);
  VELOX_CHECK_NOT_NULL(index_);
  VELOX_CHECK_NOT_NULL(lookupHandle_);
  VELOX_CHECK(
      joinType_ == velox::core::JoinType::kInner ||
          joinType_ == velox::core::JoinType::kLeft,
      "Index lookup supports only inner and left joins");
  VELOX_CHECK(!probeKeys_.empty());
  VELOX_CHECK_EQ(probeKeys_.size(), lookupKeys_.size());
  VELOX_CHECK_EQ(sourceColumns_.size(), outputColumns().size());
  VELOX_CHECK(index_->distribution.partitionKeys().empty());

  const auto probeColumns = PlanObjectSet::fromObjects(probe_->outputColumns());
  const auto lookupColumns = PlanObjectSet::fromObjects(lookupOutputColumns_);
  for (ColumnCP column : lookupOutputColumns_) {
    VELOX_CHECK(
        lookupHandle_->columnHandles.contains(column),
        "Index lookup reads a column the connector handle was not built for: {}",
        column->name());
  }
  PlanObjectSet available{probeColumns};
  available.unionSet(lookupColumns);
  for (ExprCP keyExpr : probeKeys_) {
    VELOX_CHECK(keyExpr->columns().isSubset(probeColumns));
  }
  const auto& layoutKeys = index_->layout->lookupKeys();
  VELOX_CHECK_LE(lookupKeys_.size(), layoutKeys.size());
  for (size_t i = 0; i < lookupKeys_.size(); ++i) {
    VELOX_CHECK(lookupKeys_[i]->relation() == lookupTable_);
    VELOX_CHECK_EQ(lookupKeys_[i]->name(), layoutKeys[i]->name());
  }
  for (ExprCP conjunct : filter_) {
    VELOX_CHECK(conjunct->columns().isSubset(available));
    PlanObjectSet lookupFilterColumns = conjunct->columns();
    lookupFilterColumns.intersect(lookupColumns);
    VELOX_CHECK(
        lookupFilterColumns.isSubset(
            PlanObjectSet::fromObjects(sourceColumns_)),
        "Index lookup filter columns from the lookup table must be join outputs");
  }
  for (ColumnCP source : sourceColumns_) {
    VELOX_CHECK(available.contains(source));
  }
}

size_t IndexLookupJoin::KeyHash::operator()(const IndexLookupJoin* node) const {
  return hashOf(
      node->probe(),
      node->lookupTable(),
      node->index(),
      node->lookupOutputColumns(),
      node->lookupHandle(),
      node->joinType(),
      node->probeKeys(),
      node->lookupKeys(),
      node->filter(),
      node->outputColumns(),
      node->sourceColumns());
}

size_t IndexLookupJoin::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.probe,
      key.lookupTable,
      key.index,
      key.lookupOutputColumns,
      key.lookupHandle,
      key.joinType,
      key.probeKeys,
      key.lookupKeys,
      key.filter,
      key.outputColumns,
      key.sourceColumns);
}

bool IndexLookupJoin::KeyEq::operator()(
    const IndexLookupJoin* left,
    const IndexLookupJoin* right) const {
  return left->probe() == right->probe() &&
      left->lookupTable() == right->lookupTable() &&
      left->index() == right->index() &&
      left->lookupOutputColumns() == right->lookupOutputColumns() &&
      left->lookupHandle() == right->lookupHandle() &&
      left->joinType() == right->joinType() &&
      left->probeKeys() == right->probeKeys() &&
      left->lookupKeys() == right->lookupKeys() &&
      left->filter() == right->filter() &&
      left->outputColumns() == right->outputColumns() &&
      left->sourceColumns() == right->sourceColumns();
}

bool IndexLookupJoin::KeyEq::operator()(
    const Key& key,
    const IndexLookupJoin* node) const {
  return key.probe == node->probe() && key.lookupTable == node->lookupTable() &&
      key.index == node->index() &&
      key.lookupOutputColumns == node->lookupOutputColumns() &&
      key.lookupHandle == node->lookupHandle() &&
      key.joinType == node->joinType() && key.probeKeys == node->probeKeys() &&
      key.lookupKeys == node->lookupKeys() && key.filter == node->filter() &&
      key.outputColumns == node->outputColumns() &&
      key.sourceColumns == node->sourceColumns();
}

bool IndexLookupJoin::KeyEq::operator()(
    const IndexLookupJoin* node,
    const Key& key) const {
  return (*this)(key, node);
}

// static
bool Join::canSwapInputs(velox::core::JoinType joinType) {
  return joinType == velox::core::JoinType::kInner ||
      joinType == velox::core::JoinType::kLeft ||
      joinType == velox::core::JoinType::kRight ||
      joinType == velox::core::JoinType::kFull;
}

// static
Join::Sides Join::loopInvariantSide(
    velox::core::JoinType joinType,
    bool hasKeys,
    bool nullAsValue,
    Sides readsState) {
  if (joinType == velox::core::JoinType::kInner && hasKeys && !nullAsValue &&
      readsState.left != readsState.right) {
    return {.left = !readsState.left, .right = !readsState.right};
  }
  return {};
}

// static
bool Join::canBuildLoopInvariant(
    velox::core::JoinType joinType,
    const ExprVector& rightKeys,
    bool nullAsValue,
    NodeCP left,
    NodeCP right) {
  if (!loopInvariantSide(
           joinType,
           !rightKeys.empty(),
           nullAsValue,
           {.left = !left->requiredStates().empty(),
            .right = !right->requiredStates().empty()})
           .right) {
    return false;
  }
  folly::F14FastSet<ExprCP> distinct;
  return std::ranges::all_of(rightKeys, [&](ExprCP key) {
    return key->is(PlanType::kColumnExpr) && distinct.insert(key).second;
  });
}

void Join::Key::swapInputs() {
  VELOX_CHECK(
      Join::canSwapInputs(joinType),
      "Join inputs cannot be exchanged while preserving output: {}",
      velox::core::JoinTypeName::toName(joinType));
  std::swap(left, right);
  std::swap(leftKeys, rightKeys);
  joinType = Join::swapType(joinType);
}

velox::core::JoinType Join::swapType(velox::core::JoinType joinType) {
  using JoinType = velox::core::JoinType;
  switch (joinType) {
    case JoinType::kInner:
    case JoinType::kFull:
    // Multiset intersection: exchanging the operands yields the same operation.
    case JoinType::kCountingLeftSemiFilter:
      return joinType;
    case JoinType::kLeft:
      return JoinType::kRight;
    case JoinType::kRight:
      return JoinType::kLeft;
    case JoinType::kLeftSemiFilter:
      return JoinType::kRightSemiFilter;
    case JoinType::kRightSemiFilter:
      return JoinType::kLeftSemiFilter;
    case JoinType::kLeftSemiProject:
      return JoinType::kRightSemiProject;
    case JoinType::kRightSemiProject:
      return JoinType::kLeftSemiProject;
    default:
      VELOX_UNREACHABLE(
          "Join type has no swapped representation: {}",
          velox::core::JoinTypeName::toName(joinType));
  }
}

bool Join::canBroadcastBuild(velox::core::JoinType joinType) {
  using JoinType = velox::core::JoinType;
  return joinType == JoinType::kInner || joinType == JoinType::kLeft ||
      joinType == JoinType::kLeftSemiFilter ||
      joinType == JoinType::kLeftSemiProject || joinType == JoinType::kAnti;
}

bool Join::isKnownEmpty(
    velox::core::JoinType joinType,
    bool leftIsKnownEmpty,
    bool rightIsKnownEmpty) {
  using JoinType = velox::core::JoinType;
  switch (joinType) {
    case JoinType::kInner:
    case JoinType::kLeftSemiFilter:
    case JoinType::kCountingLeftSemiFilter:
    case JoinType::kRightSemiFilter:
      return leftIsKnownEmpty || rightIsKnownEmpty;
    case JoinType::kLeft:
    case JoinType::kLeftSemiProject:
    case JoinType::kAnti:
    case JoinType::kCountingAnti:
      return leftIsKnownEmpty;
    case JoinType::kRight:
    case JoinType::kRightSemiProject:
    case JoinType::kRightAnti:
      return rightIsKnownEmpty;
    case JoinType::kFull:
      return leftIsKnownEmpty && rightIsKnownEmpty;
    case JoinType::kNumJoinTypes:
      break;
  }
  VELOX_UNREACHABLE();
}

namespace {

// Whether values of 'type' that compare equal are also indistinguishable, so
// that either operand order of a coalesce over them yields the same value.
// REAL and DOUBLE compare -0.0 equal to 0.0 while hashing differently, and a
// custom comparison can equate any two representations.
bool supportsCoalesceKey(TypeCP type) {
  if (type->kind() == velox::TypeKind::REAL ||
      type->kind() == velox::TypeKind::DOUBLE ||
      type->providesCustomComparison()) {
    return false;
  }
  for (size_t i = 0; i < type->size(); ++i) {
    if (!supportsCoalesceKey(type->childAt(i).get())) {
      return false;
    }
  }
  return true;
}

// The partitioning a full join retains. Both inputs must be co-partitioned on
// the pairs they join on, so that the coalesce of a pair holds the value that
// placed every output row.
Partitioning fullJoinPartitioning(
    const Partitioning& leftPartitioning,
    const Partitioning& rightPartitioning,
    const ExprVector& leftKeys,
    const ExprVector& rightKeys,
    const PlanObjectSet& outputColumns,
    const ColumnVector& joinOutputColumns,
    const ColumnVector& joinSourceColumns,
    Builder& builder) {
  // A keyless full join has no pair to coalesce, so nothing places its rows.
  if (leftKeys.empty()) {
    return Partitioning::unspecified(leftPartitioning.scope);
  }
  if (leftPartitioning.kind != PartitionKind::kPartitioned ||
      rightPartitioning.kind != PartitionKind::kPartitioned ||
      leftPartitioning.replicateNullsAndAny ||
      rightPartitioning.replicateNullsAndAny ||
      leftPartitioning.keys.size() != rightPartitioning.keys.size()) {
    return Partitioning::unspecified(leftPartitioning.scope);
  }
  const auto partitionType = Partitioning::commonJoinPartitionType(
      leftPartitioning.partitionType, rightPartitioning.partitionType);
  if (!partitionType.has_value()) {
    return Partitioning::unspecified(leftPartitioning.scope);
  }

  ExprVector keys;
  keys.reserve(leftPartitioning.keys.size());
  ExprFactory exprs{builder};
  ExprVector joinOutputExpressions;
  appendAll(joinOutputExpressions, joinOutputColumns);
  // Map each pair of co-partitioned keys to the same join equality.
  for (size_t i = 0; i < leftPartitioning.keys.size(); ++i) {
    std::optional<size_t> joinKeyIndex;
    for (size_t j = 0; j < leftKeys.size(); ++j) {
      if (leftPartitioning.keys[i]->sameOrEqual(*leftKeys[j]) &&
          rightPartitioning.keys[i]->sameOrEqual(*rightKeys[j])) {
        joinKeyIndex = j;
        break;
      }
    }
    if (!joinKeyIndex.has_value()) {
      return Partitioning::unspecified(leftPartitioning.scope);
    }
    const ExprCP leftKey = exprs.substitute(
        leftKeys[*joinKeyIndex], joinSourceColumns, joinOutputExpressions);
    const ExprCP rightKey = exprs.substitute(
        rightKeys[*joinKeyIndex], joinSourceColumns, joinOutputExpressions);
    // Null padding must make the missing side's key NULL so that the coalesce
    // selects the key from the row's surviving side.
    if (leftKey->containsNonDefaultNullBehavior() ||
        rightKey->containsNonDefaultNullBehavior() ||
        leftKey->value().type != rightKey->value().type ||
        !supportsCoalesceKey(leftKey->value().type)) {
      return Partitioning::unspecified(leftPartitioning.scope);
    }
    const ExprCP key = builder.canonicalizeCoalesce(leftKey, rightKey);
    if (!outputColumns.containsColumns(key)) {
      return Partitioning::unspecified(leftPartitioning.scope);
    }
    keys.push_back(key);
  }

  return partitionType->connectorType == nullptr
      ? Partitioning::hash(leftPartitioning.scope, keys)
      : Partitioning::connectorHash(
            leftPartitioning.scope, keys, partitionType->connectorType);
}

} // namespace

Partitioning Join::outputPartitioning(
    velox::core::JoinType joinType,
    const Partitioning& leftPartitioning,
    const Partitioning& rightPartitioning,
    const ExprVector& leftKeys,
    const ExprVector& rightKeys,
    const PlanObjectSet& outputColumns,
    const ColumnVector& joinOutputColumns,
    const ColumnVector& joinSourceColumns,
    Builder& builder) {
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());

  // Both inputs sit on one task, so the join runs there and leaves its output
  // there. Holds for every join type, including the ones that preserve neither
  // side.
  if (leftPartitioning.is(PartitionKind::kGather) &&
      rightPartitioning.is(PartitionKind::kGather)) {
    return Partitioning::gather(leftPartitioning.scope);
  }

  if (joinType == velox::core::JoinType::kFull) {
    return fullJoinPartitioning(
        leftPartitioning,
        rightPartitioning,
        leftKeys,
        rightKeys,
        outputColumns,
        joinOutputColumns,
        joinSourceColumns,
        builder);
  }

  const auto preserved = preservedSides(joinType);
  if (!preserved.left && !preserved.right) {
    return Partitioning::unspecified(leftPartitioning.scope);
  }
  const Partitioning& source =
      preserved.left ? leftPartitioning : rightPartitioning;
  const Partitioning& other =
      preserved.left ? rightPartitioning : leftPartitioning;
  Partitioning output = source.dropOrder();

  if (output.kind != PartitionKind::kPartitioned) {
    return output;
  }

  if (output.partitionType != nullptr && other.partitionType != nullptr) {
    const auto commonType = Partitioning::commonJoinPartitionType(
        output.partitionType, other.partitionType);
    if (!commonType.has_value()) {
      return Partitioning::unspecified(source.scope);
    }
    output.partitionType = commonType->connectorType;
  }

  if (outputColumns.containsAll(output.keys)) {
    return output;
  }

  if (joinType != velox::core::JoinType::kInner) {
    return Partitioning::unspecified(source.scope);
  }

  ExprVector outputKeys;
  outputKeys.reserve(output.keys.size());
  for (ExprCP key : output.keys) {
    if (outputColumns.containsColumns(key)) {
      outputKeys.push_back(key);
      continue;
    }
    ExprCP survivingKey = survivingEquiKey(key, outputColumns);
    if (survivingKey == nullptr) {
      return Partitioning::unspecified(source.scope);
    }
    outputKeys.push_back(survivingKey);
  }
  output.keys = std::move(outputKeys);
  return output;
}

ColumnCP Join::markColumn() const {
  VELOX_CHECK(
      projectsMark(joinType_),
      "Only a semi-project join projects a mark: {}",
      joinTypeName());
  return outputColumns().back();
}

size_t Join::KeyHash::operator()(const Join* node) const {
  return hashOf(
      node->left(),
      node->right(),
      node->joinType(),
      node->leftKeys(),
      node->rightKeys(),
      node->filter(),
      node->outputColumns(),
      node->sourceColumns(),
      node->nullAware(),
      node->nullAsValue(),
      node->buildIsLoopInvariant());
}

size_t Join::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.left,
      key.right,
      key.joinType,
      key.leftKeys,
      key.rightKeys,
      key.filter,
      key.outputColumns,
      key.effectiveSourceColumns(),
      key.nullAware,
      key.nullAsValue,
      key.buildIsLoopInvariant);
}

bool Join::KeyEq::operator()(const Join* left, const Join* right) const {
  return left->left() == right->left() && left->right() == right->right() &&
      left->joinType() == right->joinType() &&
      left->leftKeys() == right->leftKeys() &&
      left->rightKeys() == right->rightKeys() &&
      left->filter() == right->filter() &&
      left->nullAware() == right->nullAware() &&
      left->nullAsValue() == right->nullAsValue() &&
      left->buildIsLoopInvariant() == right->buildIsLoopInvariant() &&
      left->outputColumns() == right->outputColumns() &&
      left->sourceColumns() == right->sourceColumns();
}

bool Join::KeyEq::operator()(const Key& key, const Join* node) const {
  return key.left == node->left() && key.right == node->right() &&
      key.joinType == node->joinType() && key.leftKeys == node->leftKeys() &&
      key.rightKeys == node->rightKeys() && key.filter == node->filter() &&
      key.nullAware == node->nullAware() &&
      key.nullAsValue == node->nullAsValue() &&
      key.buildIsLoopInvariant == node->buildIsLoopInvariant() &&
      key.outputColumns == node->outputColumns() &&
      key.effectiveSourceColumns() == node->sourceColumns();
}

bool Join::KeyEq::operator()(const Join* node, const Key& key) const {
  return (*this)(key, node);
}

Partitioning Inference::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return inheritedPartition(inputPartitions[0]);
}

Inference::Inference(Key key)
    : Node(
          NodeType::kInference,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.input},
              [&] { return rankedPhysical(key.input, /*partitionKeys=*/{}); },
              [&] { return sameRowsRelational(key.input); })),
      input_(key.input),
      call_(key.call),
      result_(key.result) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_NOT_NULL(call_);
  VELOX_CHECK_NOT_NULL(result_);
  VELOX_CHECK_EQ(
      this->outputColumns().size(), input_->outputColumns().size() + 1);
}

size_t Inference::KeyHash::operator()(const Inference* node) const {
  return hashOf(
      node->input(), node->call(), node->result(), node->outputColumns());
}

size_t Inference::KeyHash::operator()(const Key& key) const {
  return hashOf(key.input, key.call, key.result, key.outputColumns);
}

bool Inference::KeyEq::operator()(const Inference* left, const Inference* right)
    const {
  return left->input() == right->input() && left->call() == right->call() &&
      left->result() == right->result() &&
      left->outputColumns() == right->outputColumns();
}

bool Inference::KeyEq::operator()(const Key& key, const Inference* node) const {
  return key.input == node->input() && key.call == node->call() &&
      key.result == node->result() &&
      key.outputColumns == node->outputColumns();
}

bool Inference::KeyEq::operator()(const Inference* node, const Key& key) const {
  return (*this)(key, node);
}

Partitioning Window::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return inheritedPartition(inputPartitions[0]);
}

Window::Window(Key key)
    : Node(
          NodeType::kWindow,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.input},
              [&] { return windowPhysical(key.input, key.partitionKeys); },
              [&] { return sameRowsRelational(key.input); })),
      input_(key.input),
      functions_(std::move(key.functions)),
      partitionKeys_(std::move(key.partitionKeys)),
      orderKeys_(std::move(key.orderKeys)),
      orderTypes_(std::move(key.orderTypes)) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(!functions_.empty(), "Window must have at least one function");
  VELOX_CHECK_EQ(
      this->outputColumns().size(),
      input_->outputColumns().size() + functions_.size());
  VELOX_CHECK_EQ(orderKeys_.size(), orderTypes_.size());
  checkColumns(partitionKeys_, "Window partition key");
  checkColumns(orderKeys_, "Window order key");
  checkDistinctKeys(partitionKeys_, orderKeys_, "Window");
  for (const auto& function : functions_) {
    // A ROWS bound is an offset in rows, which Velox reads as a constant.
    if (function.frame.type != logical_plan::WindowExpr::WindowType::kRange) {
      continue;
    }
    // A RANGE offset bound is compared against the one ORDER BY key.
    if (function.frame.startValue != nullptr ||
        function.frame.endValue != nullptr) {
      VELOX_CHECK_EQ(
          orderKeys_.size(),
          1,
          "Window RANGE frame with an offset bound requires exactly one order key");
    }
    for (ExprCP bound : {function.frame.startValue, function.frame.endValue}) {
      if (bound != nullptr) {
        checkColumns({bound}, "Window RANGE frame bound");
      }
    }
  }
}

size_t Window::KeyHash::operator()(const Window* node) const {
  size_t hash = hashOf(
      node->input(),
      node->partitionKeys(),
      node->orderKeys(),
      node->orderTypes(),
      node->outputColumns());
  for (const auto& function : node->functions()) {
    hash = velox::bits::hashMix(
        hash, hashOf(function.call, function.frame, function.ignoreNulls));
  }
  return hash;
}

size_t Window::KeyHash::operator()(const Key& key) const {
  size_t hash = hashOf(
      key.input,
      key.partitionKeys,
      key.orderKeys,
      key.orderTypes,
      key.outputColumns);
  for (const auto& function : key.functions) {
    hash = velox::bits::hashMix(
        hash, hashOf(function.call, function.frame, function.ignoreNulls));
  }
  return hash;
}

bool Window::KeyEq::operator()(const Window* left, const Window* right) const {
  return left->input() == right->input() &&
      left->functions() == right->functions() &&
      left->partitionKeys() == right->partitionKeys() &&
      left->orderKeys() == right->orderKeys() &&
      left->orderTypes() == right->orderTypes() &&
      left->outputColumns() == right->outputColumns();
}

bool Window::KeyEq::operator()(const Key& key, const Window* node) const {
  return key.input == node->input() && key.functions == node->functions() &&
      key.partitionKeys == node->partitionKeys() &&
      key.orderKeys == node->orderKeys() &&
      key.orderTypes == node->orderTypes() &&
      key.outputColumns == node->outputColumns();
}

bool Window::KeyEq::operator()(const Window* node, const Key& key) const {
  return (*this)(key, node);
}

Partitioning RowNumber::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return inheritedPartition(inputPartitions[0]);
}

RowNumber::RowNumber(Key key)
    : Node(
          NodeType::kRowNumber,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.input},
              [&] { return rankedPhysical(key.input, key.partitionKeys); },
              [&] {
                return key.limit.has_value() ? subsetRelational(key.input)
                                             : sameRowsRelational(key.input);
              })),
      input_(key.input),
      partitionKeys_(std::move(key.partitionKeys)),
      limit_(key.limit),
      rankColumn_(key.rankColumn) {
  VELOX_CHECK_NOT_NULL(input_);
  if (limit_.has_value()) {
    VELOX_CHECK_GT(limit_.value(), 0, "RowNumber limit must be positive");
  }
  VELOX_CHECK_EQ(
      this->outputColumns().size(),
      input_->outputColumns().size() + (rankColumn_ != nullptr ? 1 : 0));
  checkColumns(partitionKeys_, "RowNumber partition key");
  checkDistinctKeys(partitionKeys_, /*orderKeys=*/{}, "RowNumber");
}

size_t RowNumber::KeyHash::operator()(const RowNumber* node) const {
  return hashOf(
      node->input(),
      node->partitionKeys(),
      node->limit(),
      node->rankColumn(),
      node->outputColumns());
}

size_t RowNumber::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.input,
      key.partitionKeys,
      key.limit,
      key.rankColumn,
      key.outputColumns);
}

bool RowNumber::KeyEq::operator()(const RowNumber* left, const RowNumber* right)
    const {
  return left->input() == right->input() &&
      left->partitionKeys() == right->partitionKeys() &&
      left->limit() == right->limit() &&
      left->rankColumn() == right->rankColumn() &&
      left->outputColumns() == right->outputColumns();
}

bool RowNumber::KeyEq::operator()(const Key& key, const RowNumber* node) const {
  return key.input == node->input() &&
      key.partitionKeys == node->partitionKeys() &&
      key.limit == node->limit() && key.rankColumn == node->rankColumn() &&
      key.outputColumns == node->outputColumns();
}

bool RowNumber::KeyEq::operator()(const RowNumber* node, const Key& key) const {
  return (*this)(key, node);
}

Partitioning TopNRowNumber::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return inheritedPartition(inputPartitions[0]);
}

TopNRowNumber::TopNRowNumber(Key key)
    : Node(
          NodeType::kTopNRowNumber,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.input},
              [&] { return rankedPhysical(key.input, key.partitionKeys); },
              [&] { return subsetRelational(key.input); })),
      input_(key.input),
      rankFunction_(key.rankFunction),
      partitionKeys_(std::move(key.partitionKeys)),
      orderKeys_(std::move(key.orderKeys)),
      orderTypes_(std::move(key.orderTypes)),
      limit_(key.limit),
      rankColumn_(key.rankColumn) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(!orderKeys_.empty(), "TopNRowNumber must have order keys");
  checkColumns(partitionKeys_, "TopNRowNumber partition key");
  checkColumns(orderKeys_, "TopNRowNumber order key");
  checkDistinctKeys(partitionKeys_, orderKeys_, "TopNRowNumber");
  VELOX_CHECK_EQ(
      this->outputColumns().size(),
      input_->outputColumns().size() + (rankColumn_ != nullptr ? 1 : 0));
  VELOX_CHECK_EQ(orderKeys_.size(), orderTypes_.size());
  VELOX_CHECK_GT(limit_, 0);
  VELOX_CHECK_EQ(
      this->outputColumns().size(),
      input_->outputColumns().size() + (rankColumn_ != nullptr ? 1 : 0));
}

size_t TopNRowNumber::KeyHash::operator()(const TopNRowNumber* node) const {
  return hashOf(
      node->input(),
      static_cast<int>(node->rankFunction()),
      node->partitionKeys(),
      node->orderKeys(),
      node->orderTypes(),
      node->limit(),
      node->rankColumn(),
      node->outputColumns());
}

size_t TopNRowNumber::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.input,
      static_cast<int>(key.rankFunction),
      key.partitionKeys,
      key.orderKeys,
      key.orderTypes,
      key.limit,
      key.rankColumn,
      key.outputColumns);
}

bool TopNRowNumber::KeyEq::operator()(
    const TopNRowNumber* left,
    const TopNRowNumber* right) const {
  return left->input() == right->input() &&
      left->rankFunction() == right->rankFunction() &&
      left->partitionKeys() == right->partitionKeys() &&
      left->orderKeys() == right->orderKeys() &&
      left->orderTypes() == right->orderTypes() &&
      left->limit() == right->limit() &&
      left->rankColumn() == right->rankColumn() &&
      left->outputColumns() == right->outputColumns();
}

bool TopNRowNumber::KeyEq::operator()(const Key& key, const TopNRowNumber* node)
    const {
  return key.input == node->input() &&
      key.rankFunction == node->rankFunction() &&
      key.partitionKeys == node->partitionKeys() &&
      key.orderKeys == node->orderKeys() &&
      key.orderTypes == node->orderTypes() && key.limit == node->limit() &&
      key.rankColumn == node->rankColumn() &&
      key.outputColumns == node->outputColumns();
}

bool TopNRowNumber::KeyEq::operator()(const TopNRowNumber* node, const Key& key)
    const {
  return (*this)(key, node);
}

Partitioning Apply::globalPartition(
    std::span<const Partitioning> /*inputPartitions*/,
    Builder& /*builder*/) const {
  return {};
}

Apply::Apply(Key key)
    : Node(
          NodeType::kApply,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.input, key.body},
              [] { return PhysicalProperties{}; },
              [] { return RelationalProperties{}; })),
      inputs_{key.input, key.body},
      correlationColumns_(std::move(key.correlationColumns)),
      kind_(key.kind),
      filter_(std::move(key.filter)),
      enforceSingleRow_(key.enforceSingleRow),
      markColumn_(key.markColumn),
      inLhs_(key.inLhs),
      inBodyKey_(key.inBodyKey),
      includeMarker_(key.includeMarker),
      sourceColumns_(key.effectiveSourceColumns()) {
  VELOX_CHECK_NOT_NULL(inputs_[0]);
  VELOX_CHECK_NOT_NULL(inputs_[1]);
  VELOX_CHECK_EQ(sourceColumns_.size(), outputColumns().size());

  // Allowed kinds at Apply: correlated scalar / LEFT lateral (kLeft),
  // CROSS/INNER lateral (kInner), and EXISTS/IN (kLeftSemiProject).
  // Uncorrelated scalars are lowered directly to cross-join +
  // EnforceSingleRow at translate time — no Apply. kAnti / kLeftSemiFilter
  // emerge from the post-decorrelate peephole, never at Apply.
  const bool isLeft = kind_ == velox::core::JoinType::kLeft;
  const bool isInner = kind_ == velox::core::JoinType::kInner;
  VELOX_CHECK(
      isLeft || isInner || kind_ == velox::core::JoinType::kLeftSemiProject,
      "Apply.kind must be kInner / kLeft / kLeftSemiProject; got: {}",
      kind_);

  if (isLeft || isInner) {
    // Scalar / lateral: markColumn and the IN pair are semi/IN-only.
    VELOX_CHECK_NULL(
        markColumn_,
        "markColumn is kLeftSemiProject-only; kLeft/kInner leave it null");
    VELOX_CHECK_NULL(inLhs_, "inLhs is IN-only; non-semi Apply leaves it null");
    VELOX_CHECK_NULL(
        inBodyKey_, "inBodyKey is IN-only; non-semi Apply leaves it null");
    if (isLeft) {
      VELOX_CHECK_NOT_NULL(
          includeMarker_, "kLeft Apply requires an includeMarker");
      VELOX_CHECK_EQ(
          includeMarker_->value().type->kind(),
          velox::TypeKind::BOOLEAN,
          "Apply.includeMarker must be BOOLEAN");
    } else {
      // kInner has no NULL-pad rows, so no includeMarker, and (as a
      // table-valued lateral) no cardinality assertion.
      VELOX_CHECK_NULL(
          includeMarker_, "kInner Apply must not have an includeMarker");
      VELOX_CHECK(
          !enforceSingleRow_,
          "kInner Apply must have enforceSingleRow == false");
    }
  } else {
    VELOX_CHECK_NULL(
        includeMarker_, "includeMarker is kLeft-only; kSemi leaves it null");
    VELOX_CHECK_NOT_NULL(
        markColumn_, "kLeftSemiProject Apply requires a markColumn");
    VELOX_CHECK_EQ(
        markColumn_->value().type->kind(),
        velox::TypeKind::BOOLEAN,
        "kLeftSemiProject markColumn must be BOOLEAN");
    VELOX_CHECK(
        !enforceSingleRow_,
        "kLeftSemiProject Apply must have enforceSingleRow == false "
        "(semi/anti has no cardinality assertion)");
  }
  // The IN pair is set together: both null (scalar / EXISTS) or both
  // non-null (IN). Forbid the half-set state.
  VELOX_CHECK_EQ(
      inLhs_ == nullptr,
      inBodyKey_ == nullptr,
      "Apply.inLhs and Apply.inBodyKey must be set together (both null "
      "for scalar / EXISTS; both non-null for IN)");

  // outputColumns shape check, per kind:
  //   kLeft             : input.outputColumns
  //                       ++ unique(body.outputColumns) ++ includeMarker
  //   kLeftSemiProject  : input.outputColumns ++ markColumn
  //
  // A body column that already appears in input.outputColumns by
  // Column* identity occupies a single output slot.
  const auto& inputCols = inputs_[0]->outputColumns();
  VELOX_CHECK_GE(
      outputColumns().size(),
      inputCols.size(),
      "Apply.outputColumns shorter than input.outputColumns");
  for (size_t i = 0; i < inputCols.size(); ++i) {
    VELOX_CHECK(
        outputColumns()[i] == inputCols[i],
        "Apply.outputColumns prefix must match input.outputColumns");
    VELOX_CHECK(
        sourceColumns_[i] == inputCols[i],
        "Apply.sourceColumns prefix must match input.outputColumns");
  }
  if (isLeft || isInner) {
    // kLeft ends with the includeMarker slot; kInner has no marker.
    const size_t markerSlots = isLeft ? 1 : 0;
    VELOX_CHECK_GE(
        outputColumns().size(),
        inputCols.size() + markerSlots,
        "kLeft/kInner Apply.outputColumns shorter than input (+ includeMarker)");
    const size_t bodySlots =
        outputColumns().size() - inputCols.size() - markerSlots;
    VELOX_CHECK_LE(
        bodySlots,
        inputs_[1]->outputColumns().size(),
        "Apply body-col slot count exceeds body's outputColumns");
    PlanObjectSet seen = PlanObjectSet::fromObjects(inputCols);
    PlanObjectSet seenSources = seen;
    ColumnVector bodySources;
    for (ColumnCP source : inputs_[1]->outputColumns()) {
      if (!seenSources.contains(source)) {
        bodySources.push_back(source);
        seenSources.add(source);
      }
    }
    VELOX_CHECK_EQ(
        bodySlots,
        bodySources.size(),
        "Apply body-col slots must cover unique body.outputColumns");
    for (size_t i = 0; i < bodySlots; ++i) {
      ColumnCP column = outputColumns()[inputCols.size() + i];
      VELOX_CHECK(
          !seen.contains(column),
          "Apply.outputColumns body cols must be unique vs input and each other");
      seen.add(column);
      VELOX_CHECK(
          sourceColumns_[inputCols.size() + i] == bodySources[i],
          "Apply.sourceColumns body cols must match body.outputColumns");
      VELOX_CHECK(
          !isLeft || column != bodySources[i],
          "A null-extended Apply output must have a fresh identity: {}",
          column->toString());
    }
    if (isLeft) {
      VELOX_CHECK(
          outputColumns().back() == includeMarker_,
          "Apply.outputColumns must end with includeMarker for kLeft");
      VELOX_CHECK(
          sourceColumns_.back() == includeMarker_,
          "Apply.sourceColumns must end with includeMarker for kLeft");
    }
  } else {
    VELOX_CHECK_EQ(
        outputColumns().size(),
        inputCols.size() + 1,
        "kLeftSemiProject Apply.outputColumns size mismatch");
    VELOX_CHECK(
        outputColumns().back() == markColumn_,
        "Apply.outputColumns must end with markColumn for kLeftSemiProject");
    VELOX_CHECK(
        sourceColumns_.back() == markColumn_,
        "Apply.sourceColumns must end with markColumn for kLeftSemiProject");
  }
}

ColumnCP Apply::outputForSource(ColumnCP source) const {
  const auto it = std::ranges::find(sourceColumns_, source);
  VELOX_CHECK(
      it != sourceColumns_.end(),
      "Apply source column is absent: {}",
      source->toString());
  return outputColumns()[it - sourceColumns_.begin()];
}

size_t Apply::KeyHash::operator()(const Apply* node) const {
  return hashOf(
      node->input(),
      node->body(),
      node->correlationColumns(),
      node->kind(),
      node->filter(),
      node->enforceSingleRow(),
      node->markColumn(),
      node->inLhs(),
      node->inBodyKey(),
      node->includeMarker(),
      node->outputColumns(),
      node->sourceColumns());
}

size_t Apply::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.input,
      key.body,
      key.correlationColumns,
      key.kind,
      key.filter,
      key.enforceSingleRow,
      key.markColumn,
      key.inLhs,
      key.inBodyKey,
      key.includeMarker,
      key.outputColumns,
      key.effectiveSourceColumns());
}

bool Apply::KeyEq::operator()(const Apply* left, const Apply* right) const {
  return left->input() == right->input() && left->body() == right->body() &&
      left->correlationColumns() == right->correlationColumns() &&
      left->kind() == right->kind() && left->filter() == right->filter() &&
      left->enforceSingleRow() == right->enforceSingleRow() &&
      left->markColumn() == right->markColumn() &&
      left->inLhs() == right->inLhs() &&
      left->inBodyKey() == right->inBodyKey() &&
      left->includeMarker() == right->includeMarker() &&
      left->outputColumns() == right->outputColumns() &&
      left->sourceColumns() == right->sourceColumns();
}

bool Apply::KeyEq::operator()(const Key& key, const Apply* node) const {
  return key.input == node->input() && key.body == node->body() &&
      key.correlationColumns == node->correlationColumns() &&
      key.kind == node->kind() && key.filter == node->filter() &&
      key.enforceSingleRow == node->enforceSingleRow() &&
      key.markColumn == node->markColumn() && key.inLhs == node->inLhs() &&
      key.inBodyKey == node->inBodyKey() &&
      key.includeMarker == node->includeMarker() &&
      key.outputColumns == node->outputColumns() &&
      key.effectiveSourceColumns() == node->sourceColumns();
}

bool Apply::KeyEq::operator()(const Apply* node, const Key& key) const {
  return (*this)(key, node);
}

namespace {
ColumnVector withIdColumn(const ColumnVector& columns, ColumnCP idColumn) {
  ColumnVector result;
  result.reserve(columns.size() + 1);
  for (ColumnCP column : columns) {
    result.push_back(column);
  }
  result.push_back(idColumn);
  return result;
}
} // namespace

Partitioning AssignUniqueId::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return inheritedPartition(inputPartitions[0]);
}

AssignUniqueId::AssignUniqueId(Key key)
    : Node(
          NodeType::kAssignUniqueId,
          withIdColumn(key.input->outputColumns(), key.idColumn),
          derivedProperties(
              {key.input},
              [&] {
                PhysicalProperties properties{
                    .globalPartition = inheritedPartition(
                        key.input->physicalProperties().globalPartition),
                    .driverPartition = inheritedPartition(
                        key.input->physicalProperties().driverPartition),
                    .driverOrder = groupedDriverOrder(key.idColumn),
                    .globalUnique =
                        key.input->physicalProperties().globalUnique,
                    .driverUnique =
                        key.input->physicalProperties().driverUnique};
                properties.globalUnique.push_back(
                    PlanObjectSet::single(key.idColumn));
                return properties;
              },
              [&] {
                RelationalProperties properties = sameRowsRelational(key.input);
                properties.unique.push_back(
                    PlanObjectSet::single(key.idColumn));
                return properties;
              })),
      input_(key.input),
      idColumn_(key.idColumn) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_NOT_NULL(idColumn_);
  VELOX_CHECK_EQ(idColumn_->value().type->kind(), velox::TypeKind::BIGINT);
}

size_t AssignUniqueId::KeyHash::operator()(const AssignUniqueId* node) const {
  return hashOf(node->input(), node->idColumn());
}

size_t AssignUniqueId::KeyHash::operator()(const Key& key) const {
  return hashOf(key.input, key.idColumn);
}

bool AssignUniqueId::KeyEq::operator()(
    const AssignUniqueId* left,
    const AssignUniqueId* right) const {
  return left->input() == right->input() &&
      left->idColumn() == right->idColumn();
}

bool AssignUniqueId::KeyEq::operator()(
    const Key& key,
    const AssignUniqueId* node) const {
  return key.input == node->input() && key.idColumn == node->idColumn();
}

bool AssignUniqueId::KeyEq::operator()(
    const AssignUniqueId* node,
    const Key& key) const {
  return (*this)(key, node);
}

Partitioning EnforceSingleRow::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return projectPartition(
      inputPartitions[0],
      ExprVector{
          input_->outputColumns().begin(), input_->outputColumns().end()},
      outputColumns());
}

EnforceSingleRow::EnforceSingleRow(Key key)
    : Node(
          NodeType::kEnforceSingleRow,
          ColumnVector{key.outputColumns},
          derivedProperties(
              {key.input},
              [&] {
                const ExprVector inputColumns{
                    key.input->outputColumns().begin(),
                    key.input->outputColumns().end()};
                PhysicalProperties properties{
                    .globalPartition = projectPartition(
                        key.input->physicalProperties().globalPartition,
                        inputColumns,
                        key.outputColumns),
                    .driverPartition = projectPartition(
                        key.input->physicalProperties().driverPartition,
                        inputColumns,
                        key.outputColumns)};
                addUniqueKey(
                    properties,
                    PlanObjectSet{},
                    key.input->physicalProperties(),
                    /*keys=*/{});
                return properties;
              },
              [] { return singleRowRelational(); })),
      input_(key.input) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_EQ(outputColumns().size(), input_->outputColumns().size());
  for (size_t i = 0; i < outputColumns().size(); ++i) {
    VELOX_CHECK(
        outputColumns()[i] != input_->outputColumns()[i],
        "EnforceSingleRow outputs must have fresh identities");
  }
}

size_t EnforceSingleRow::KeyHash::operator()(
    const EnforceSingleRow* node) const {
  return hashOf(node->input(), node->outputColumns());
}

size_t EnforceSingleRow::KeyHash::operator()(const Key& key) const {
  return hashOf(key.input, key.outputColumns);
}

bool EnforceSingleRow::KeyEq::operator()(
    const EnforceSingleRow* left,
    const EnforceSingleRow* right) const {
  return left->input() == right->input() &&
      left->outputColumns() == right->outputColumns();
}

bool EnforceSingleRow::KeyEq::operator()(
    const Key& key,
    const EnforceSingleRow* node) const {
  return key.input == node->input() &&
      key.outputColumns == node->outputColumns();
}

bool EnforceSingleRow::KeyEq::operator()(
    const EnforceSingleRow* node,
    const Key& key) const {
  return (*this)(key, node);
}

namespace {
// The columns of 'keys' when every key is a column.
std::optional<PlanObjectSet> columnSet(const ExprVector& keys) {
  PlanObjectSet columns;
  for (ExprCP key : keys) {
    if (!key->is(PlanType::kColumnExpr)) {
      return std::nullopt;
    }
    columns.add(key);
  }
  return columns;
}

} // namespace

Partitioning EnforceDistinct::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return inheritedPartition(inputPartitions[0]);
}

EnforceDistinct::EnforceDistinct(Key key)
    : Node(
          NodeType::kEnforceDistinct,
          ColumnVector{key.input->outputColumns()},
          derivedProperties(
              {key.input},
              [&] {
                PhysicalProperties properties = passThroughPhysical(key.input);
                if (std::optional<PlanObjectSet> distinctColumns =
                        columnSet(key.distinctKeys)) {
                  addUniqueKey(
                      properties,
                      std::move(*distinctColumns),
                      key.input->physicalProperties(),
                      key.distinctKeys);
                }
                return properties;
              },
              [&] {
                RelationalProperties properties = sameRowsRelational(key.input);
                if (std::optional<PlanObjectSet> distinctColumns =
                        columnSet(key.distinctKeys)) {
                  properties.unique.push_back(std::move(*distinctColumns));
                }
                return properties;
              })),
      input_(key.input),
      distinctKeys_(std::move(key.distinctKeys)),
      errorMessage_(key.errorMessage) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(
      !distinctKeys_.empty(),
      "EnforceDistinct must have at least one distinct key");
  VELOX_CHECK_NOT_NULL(errorMessage_);
  checkColumns(distinctKeys_, "EnforceDistinct distinct key");
}

size_t EnforceDistinct::KeyHash::operator()(const EnforceDistinct* node) const {
  return hashOf(node->input(), node->distinctKeys(), node->errorMessage());
}

size_t EnforceDistinct::KeyHash::operator()(const Key& key) const {
  return hashOf(key.input, key.distinctKeys, key.errorMessage);
}

bool EnforceDistinct::KeyEq::operator()(
    const EnforceDistinct* left,
    const EnforceDistinct* right) const {
  return left->input() == right->input() &&
      left->distinctKeys() == right->distinctKeys() &&
      left->errorMessage() == right->errorMessage();
}

bool EnforceDistinct::KeyEq::operator()(
    const Key& key,
    const EnforceDistinct* node) const {
  return key.input == node->input() &&
      key.distinctKeys == node->distinctKeys() &&
      key.errorMessage == node->errorMessage();
}

bool EnforceDistinct::KeyEq::operator()(
    const EnforceDistinct* node,
    const Key& key) const {
  return (*this)(key, node);
}

Partitioning Exchange::globalPartition(
    std::span<const Partitioning> /*inputPartitions*/,
    Builder& /*builder*/) const {
  return physicalProperties().globalPartition;
}

Placement Exchange::placementAfter(
    NodeCP input,
    const Partitioning& partitioning,
    bool oneDriverPerTask) {
  const bool global = partitioning.isGlobal();
  VELOX_CHECK(
      global || !oneDriverPerTask,
      "Driver exchange must run in a stage with several drivers per task");
  const bool oneTask = global
      ? partitioning.is(PartitionKind::kGather)
      : input->physicalProperties().globalPartition.is(PartitionKind::kGather);
  const bool oneDriver =
      global ? oneDriverPerTask : partitioning.is(PartitionKind::kGather);
  if (oneTask) {
    return oneDriver ? Placement::kSingleThread : Placement::kSingleTask;
  }
  return oneDriver ? Placement::kOneDriverPerTask : Placement::kDistributed;
}

Exchange::Exchange(Key key)
    : Node(
          NodeType::kExchange,
          ColumnVector{key.input->outputColumns()},
          placedProperties(
              {key.input},
              [&] {
                // An exchange keeps the global unique keys, a broadcast too:
                // each task holds a full copy of the relation. It drops the
                // driver-scoped ones, whose duplicates it may bring together.
                const PhysicalProperties& input =
                    key.input->physicalProperties();
                return key.partitioning.isGlobal()
                ? PhysicalProperties{
                      .globalPartition = key.partitioning,
                      .driverPartition = onOneDriverPerTask(key.placement)
                          ? Partitioning::gather(PropertyScope::kDriver)
                          : Partitioning{.scope = PropertyScope::kDriver},
                      .driverOrder = exchangeDriverOrder(key.partitioning),
                      .globalUnique = input.globalUnique}
                : PhysicalProperties{
                      .globalPartition = input.globalPartition.dropOrder(),
                      .driverPartition = key.partitioning,
                      .driverOrder = exchangeDriverOrder(key.partitioning),
                      .globalUnique = input.globalUnique};
              })),
      input_(key.input),
      partitioning_(std::move(key.partitioning)),
      placement_(key.placement) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_NE(
      partitioning_.kind,
      PartitionKind::kUnspecified,
      "Exchange requires concrete partitioning");
  // A global exchange's drivers come from its stage, which only its placement
  // records; a driver exchange decides them from its partitioning.
  VELOX_CHECK_EQ(
      placement_,
      placementAfter(
          input_,
          partitioning_,
          partitioning_.isGlobal() && onOneDriverPerTask(placement_)),
      "Exchange placement disagrees with its partitioning");
  const PhysicalProperties& inputProperties = input_->physicalProperties();
  VELOX_CHECK(
      !partitioning_.isGlobal() ||
          !inputProperties.globalPartition.is(PartitionKind::kBroadcast),
      "Global exchange over a broadcast input");
  VELOX_CHECK(
      !partitioning_.is(PartitionKind::kGather) ||
          !(partitioning_.isGlobal() ? inputProperties.globalPartition
                                     : inputProperties.driverPartition)
               .is(PartitionKind::kGather),
      "Gather exchange over an input already gathered at its scope");
  VELOX_CHECK_EQ(
      partitioning_.keys.empty(),
      partitioning_.kind != PartitionKind::kPartitioned,
      "Only a partitioned exchange has partition keys");
  VELOX_CHECK_EQ(
      partitioning_.orderKeys.size(), partitioning_.orderTypes.size());
  VELOX_CHECK(
      partitioning_.orderKeys.empty() ||
          partitioning_.kind == PartitionKind::kGather,
      "Only a gather exchange has merge order");
  VELOX_CHECK(
      partitioning_.partitionType == nullptr ||
          partitioning_.kind == PartitionKind::kPartitioned,
      "Only a partitioned exchange has a partition function");
  VELOX_CHECK(
      !partitioning_.replicateNullsAndAny ||
          (partitioning_.isGlobal() &&
           partitioning_.kind == PartitionKind::kPartitioned),
      "Replicating nulls and any row requires global partitioning");
  VELOX_CHECK(
      partitioning_.isGlobal() ||
          partitioning_.kind == PartitionKind::kPartitioned ||
          partitioning_.kind == PartitionKind::kGather,
      "A driver exchange must partition or gather");

  // A partition or merge-order key must be a column the input produces:
  // whoever places the shuffle materializes an expression key first, so the
  // value is computed once and the consumer above reads that same column.
  checkColumns(partitioning_.keys, "Exchange partitioning key");
  checkColumns(partitioning_.orderKeys, "Exchange merge order key");
}

size_t Exchange::KeyHash::operator()(const Exchange* node) const {
  const auto& partitioning = node->partitioning();
  return hashOf(
      node->input(),
      static_cast<uint8_t>(partitioning.kind),
      partitioning.partitionType,
      partitioning.keys,
      partitioning.orderKeys,
      partitioning.orderTypes,
      static_cast<uint8_t>(partitioning.scope),
      partitioning.replicateNullsAndAny,
      node->placement());
}

size_t Exchange::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.input,
      static_cast<uint8_t>(key.partitioning.kind),
      key.partitioning.partitionType,
      key.partitioning.keys,
      key.partitioning.orderKeys,
      key.partitioning.orderTypes,
      static_cast<uint8_t>(key.partitioning.scope),
      key.partitioning.replicateNullsAndAny,
      key.placement);
}

bool Exchange::KeyEq::operator()(const Exchange* left, const Exchange* right)
    const {
  return left->input() == right->input() &&
      left->partitioning() == right->partitioning() &&
      left->placement() == right->placement();
}

bool Exchange::KeyEq::operator()(const Key& key, const Exchange* node) const {
  return key.input == node->input() &&
      key.partitioning == node->partitioning() &&
      key.placement == node->placement();
}

bool Exchange::KeyEq::operator()(const Exchange* node, const Key& key) const {
  return (*this)(key, node);
}

Partitioning TableWrite::globalPartition(
    std::span<const Partitioning> /*inputPartitions*/,
    Builder& /*builder*/) const {
  return {};
}

TableWrite::TableWrite(Key key)
    : Node(
          NodeType::kTableWrite,
          ColumnVector{Column::create("rows", Value(toType(velox::BIGINT())))},
          derivedProperties(
              {key.input},
              [] { return PhysicalProperties{}; },
              [] { return RelationalProperties{}; })),
      input_(key.input),
      table_(key.table),
      kind_(key.kind),
      columnExprs_(std::move(key.columnExprs)) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_NOT_NULL(table_);
  if (kind_ == connector::WriteKind::kDelete) {
    VELOX_CHECK(columnExprs_.empty(), "Delete writes no columns");
  } else {
    VELOX_CHECK(
        !columnExprs_.empty(), "TableWrite must write at least one column");
    VELOX_CHECK_EQ(columnExprs_.size(), table_->type()->size());
    checkColumns(columnExprs_, "TableWrite column");
    // Velox reads the input positionally, so a reordering or widening of the
    // input silently writes the wrong columns.
    VELOX_CHECK(
        std::equal(
            columnExprs_.begin(),
            columnExprs_.end(),
            input_->outputColumns().begin(),
            input_->outputColumns().end()),
        "TableWrite input does not produce exactly the written columns");
  }
}

size_t TableWrite::KeyHash::operator()(const TableWrite* node) const {
  return hashOf(
      node->input(),
      node->table(),
      static_cast<uint8_t>(node->kind()),
      node->columnExprs());
}

size_t TableWrite::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.input, key.table, static_cast<uint8_t>(key.kind), key.columnExprs);
}

bool TableWrite::KeyEq::operator()(
    const TableWrite* left,
    const TableWrite* right) const {
  return left->input() == right->input() && left->table() == right->table() &&
      left->kind() == right->kind() &&
      left->columnExprs() == right->columnExprs();
}

bool TableWrite::KeyEq::operator()(const Key& key, const TableWrite* node)
    const {
  return key.input == node->input() && key.table == node->table() &&
      key.kind == node->kind() && key.columnExprs == node->columnExprs();
}

bool TableWrite::KeyEq::operator()(const TableWrite* node, const Key& key)
    const {
  return (*this)(key, node);
}

namespace {

// Returns the columns of 'branchColumns' at the positions 'columns' have in
// 'outputColumns': state columns as a branch whose columns line up with the
// state's presents them.
ColumnVector alignedColumns(
    const ColumnVector& columns,
    const ColumnVector& outputColumns,
    const ColumnVector& branchColumns) {
  ColumnVector result;
  result.reserve(columns.size());
  for (ColumnCP column : columns) {
    const auto it = std::ranges::find(outputColumns, column);
    VELOX_CHECK(it != outputColumns.end());
    result.push_back(branchColumns[it - outputColumns.begin()]);
  }
  return result;
}

// Returns 'partitioning', whose keys are among 'stateColumns', over the read's
// 'outputColumns' at the same positions.
Partitioning readPartitioning(
    const Partitioning& partitioning,
    const ColumnVector& stateColumns,
    const ColumnVector& outputColumns) {
  ColumnVector keys;
  keys.reserve(partitioning.keys.size());
  for (ExprCP key : partitioning.keys) {
    VELOX_CHECK(
        key->is(PlanType::kColumnExpr),
        "WorkingTable partition keys must be among its state columns: {}",
        key->toString());
    keys.push_back(key->as<Column>());
  }
  Partitioning result = partitioning;
  result.keys = toExprs(alignedColumns(keys, stateColumns, outputColumns));
  return result;
}

// Checks that the read presents every state column under a fresh one, then
// returns where it is placed.
Partitioning readPartitioning(const WorkingTable::Key& key) {
  VELOX_CHECK_EQ(
      key.outputColumns.size(),
      key.stateColumns.size(),
      "WorkingTable must present every state column");
  for (size_t i = 0; i < key.outputColumns.size(); ++i) {
    VELOX_CHECK(
        key.outputColumns[i] != key.stateColumns[i],
        "WorkingTable output column must be fresh: {}",
        key.outputColumns[i]->toString());
    VELOX_CHECK(
        key.outputColumns[i]->value().type->equivalent(
            *key.stateColumns[i]->value().type),
        "WorkingTable output column type must match its state column: {}",
        key.outputColumns[i]->toString());
  }
  return readPartitioning(
      key.partitioning, key.stateColumns, key.outputColumns);
}

} // namespace

Partitioning WorkingTable::globalPartition(
    std::span<const Partitioning> /*inputPartitions*/,
    Builder& /*builder*/) const {
  return physicalProperties().globalPartition;
}

WorkingTable::WorkingTable(const Key& key)
    : Node(
          NodeType::kWorkingTable,
          ColumnVector{key.outputColumns},
          leafProperties(
              key.placement,
              [&] {
                return PhysicalProperties{
                    .globalPartition = readPartitioning(key)};
              })),
      name_(key.name),
      stateColumns_(key.stateColumns),
      readMode_(key.readMode),
      partitioning_(key.partitioning),
      placement_(key.placement) {
  VELOX_CHECK_NOT_NULL(name_);
  VELOX_CHECK(
      placement_ == Placement::kUnplaced || onOneTask(placement_) ||
          !partitioning_.is(PartitionKind::kGather),
      "Read of gathered state must run on one task");
}

RequiredStates WorkingTable::deriveRequiredStates() const {
  return {{name_, {.columns = stateColumns_, .partitioning = partitioning_}}};
}

// static
void WorkingTable::forEachRead(
    NodeCP node,
    Name name,
    folly::FunctionRef<void(const WorkingTable&)> visit) {
  if (!node->requiredStates().contains(name)) {
    return;
  }
  if (node->is(NodeType::kWorkingTable)) {
    visit(*node->as<WorkingTable>());
    return;
  }
  for (NodeCP input : node->inputs()) {
    forEachRead(input, name, visit);
  }
}

size_t WorkingTable::KeyHash::operator()(const WorkingTable* node) const {
  return hashOf(
      node->name(),
      node->stateColumns(),
      node->outputColumns(),
      node->readMode(),
      node->partitioning().kind,
      node->partitioning().keys,
      node->placement());
}

size_t WorkingTable::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.name,
      key.stateColumns,
      key.outputColumns,
      key.readMode,
      key.partitioning.kind,
      key.partitioning.keys,
      key.placement);
}

bool WorkingTable::KeyEq::operator()(
    const WorkingTable* left,
    const WorkingTable* right) const {
  return left->name() == right->name() &&
      left->stateColumns() == right->stateColumns() &&
      left->outputColumns() == right->outputColumns() &&
      left->readMode() == right->readMode() &&
      left->partitioning() == right->partitioning() &&
      left->placement() == right->placement();
}

bool WorkingTable::KeyEq::operator()(const Key& key, const WorkingTable* node)
    const {
  return key.name == node->name() && key.stateColumns == node->stateColumns() &&
      key.outputColumns == node->outputColumns() &&
      key.readMode == node->readMode() &&
      key.partitioning == node->partitioning() &&
      key.placement == node->placement();
}

bool WorkingTable::KeyEq::operator()(const WorkingTable* node, const Key& key)
    const {
  return (*this)(key, node);
}

namespace {

// The branch reads the fixed point's state, presents the columns the fixed
// point outputs, and reads no other state.
// Returns where the state the branch reads lives.
const Partitioning& checkBranchReadsState(
    NodeCP branch,
    Name name,
    const ColumnVector& stateColumns,
    std::string_view branchName) {
  const auto& states = branch->requiredStates();
  auto it = states.find(name);
  VELOX_CHECK(
      it != states.end(),
      "FixedPoint {} must read its working table",
      branchName);
  VELOX_CHECK(
      it->second.columns == stateColumns,
      "FixedPoint {} WorkingTable state columns must match the fixed-point output by pointer identity",
      branchName);
  VELOX_CHECK_EQ(
      states.size(),
      1,
      "FixedPoint {} must read only the state this fixed point binds, but reads {} states",
      branchName,
      states.size());
  return it->second.partitioning;
}

// Returns how many WorkingTable reads of state 'name' 'node' holds.
int32_t countStateReads(NodeCP node, Name name) {
  int32_t count = 0;
  WorkingTable::forEachRead(
      node, name, [&](const WorkingTable& /*read*/) { ++count; });
  return count;
}

// The anchor, the step, and the convergence when set.
NodeVector fixedPointInputs(const FixedPoint::Key& key) {
  NodeVector inputs{key.anchor, key.step};
  if (key.convergence != nullptr) {
    inputs.push_back(key.convergence);
  }
  return inputs;
}

// The loop's output is the anchor's rows and the step's, so it is partitioned
// as their union is.
Partitioning fixedPointPartition(
    const Partitioning& anchor,
    const Partitioning& step,
    const ColumnVector& sourceColumns,
    const ColumnVector& stepColumns,
    const ColumnVector& outputColumns) {
  const std::array<Partitioning, 2> legPartitions{anchor, step};
  return unionGlobalPartition(
      legPartitions, {sourceColumns, stepColumns}, outputColumns);
}

// 'branch', whose columns align with the state's, is placed the way 'state'
// says the state is: gathered, or hash-partitioned on the aligned keys. The
// next iteration then finds the state where it expects it.
void checkPlacedLikeState(
    NodeCP branch,
    const ColumnVector& branchColumns,
    const ColumnVector& stateColumns,
    const Partitioning& state,
    std::string_view branchName) {
  const auto& placement = branch->physicalProperties().globalPartition;
  if (state.is(PartitionKind::kGather)) {
    VELOX_CHECK(
        placement.is(PartitionKind::kGather),
        "FixedPoint {} must be gathered like its state",
        branchName);
  } else if (state.is(PartitionKind::kPartitioned)) {
    ColumnVector keys;
    for (ExprCP key : state.keys) {
      keys.push_back(key->as<Column>());
    }
    VELOX_CHECK(
        placement.sameClassAs(
            Partitioning::hash(
                PropertyScope::kGlobal,
                toExprs(alignedColumns(keys, stateColumns, branchColumns)))),
        "FixedPoint {} must be partitioned like its state",
        branchName);
  }
}

// The anchor, the step and the fixed point present the same columns by
// position: the anchor's are 'sourceColumns', and the outputs are fresh.
void checkColumns(const FixedPoint::Key& key) {
  VELOX_CHECK_NOT_NULL(key.anchor);
  VELOX_CHECK_NOT_NULL(key.step);
  VELOX_CHECK(
      std::ranges::equal(key.sourceColumns, key.anchor->outputColumns()),
      "FixedPoint source columns must match anchor by pointer identity");
  VELOX_CHECK_EQ(key.sourceColumns.size(), key.outputColumns.size());
  for (size_t i = 0; i < key.outputColumns.size(); ++i) {
    ColumnCP outputColumn = key.outputColumns[i];
    ColumnCP sourceColumn = key.sourceColumns[i];
    VELOX_CHECK(
        outputColumn != sourceColumn,
        "FixedPoint output must have a fresh identity: {}",
        outputColumn->toString());
    VELOX_CHECK(
        outputColumn->value().type->equivalent(*sourceColumn->value().type),
        "FixedPoint output column type must match its source");
  }

  const auto& anchorColumns = key.anchor->outputColumns();
  const auto& stepColumns = key.step->outputColumns();
  VELOX_CHECK_EQ(
      stepColumns.size(),
      anchorColumns.size(),
      "FixedPoint step must produce the same column count as anchor");
  for (size_t i = 0; i < stepColumns.size(); ++i) {
    const auto& stepType = stepColumns[i]->value().type;
    const auto& anchorType = anchorColumns[i]->value().type;
    VELOX_CHECK(
        stepType->equivalent(*anchorType),
        "FixedPoint step output column {} type does not match anchor: step={}, anchor={}",
        i,
        stepType->toString(),
        anchorType->toString());
  }
}

Partitioning fixedPointPartition(const FixedPoint::Key& key) {
  return fixedPointPartition(
      key.anchor->physicalProperties().globalPartition,
      key.step->physicalProperties().globalPartition,
      key.sourceColumns,
      key.step->outputColumns(),
      key.outputColumns);
}

// Checks the columns before deriving the partitioning, which maps them by
// position.
Node::Properties fixedPointProperties(const FixedPoint::Key& key) {
  checkColumns(key);
  return derivedProperties(
      {key.anchor, key.step, key.convergence},
      [&] {
        return PhysicalProperties{.globalPartition = fixedPointPartition(key)};
      },
      [] { return RelationalProperties{}; });
}

} // namespace

Partitioning FixedPoint::globalPartition(
    std::span<const Partitioning> inputPartitions,
    Builder& /*builder*/) const {
  return fixedPointPartition(
      inputPartitions[0],
      inputPartitions[1],
      sourceColumns_,
      step()->outputColumns(),
      outputColumns());
}

FixedPoint::FixedPoint(const Key& key)
    : Node(
          NodeType::kFixedPoint,
          ColumnVector{key.outputColumns},
          fixedPointProperties(key)),
      inputs_{fixedPointInputs(key)},
      name_(key.name),
      sourceColumns_(key.sourceColumns),
      maxIterations_(key.maxIterations),
      failAtBound_(key.failAtBound),
      mode_(key.mode),
      recursiveNumDrivers_(key.recursiveNumDrivers) {
  VELOX_CHECK_NOT_NULL(name_);
  VELOX_CHECK(
      !outputColumns().empty(), "FixedPoint must have an output column");
  VELOX_CHECK_GE(
      maxIterations_, 1, "FixedPoint maxIterations must be at least one");
  VELOX_CHECK(
      !recursiveNumDrivers_.has_value() || *recursiveNumDrivers_ == 1,
      "FixedPoint recursiveNumDrivers must be one when set");
  VELOX_CHECK(
      inputs_[0]->requiredStates().empty(),
      "FixedPoint anchor must not read its working table");
  const auto& state =
      checkBranchReadsState(inputs_[1], name_, outputColumns(), "step");
  if (mode_ == FixedPointMode::kAppend) {
    VELOX_CHECK_EQ(
        countStateReads(inputs_[1], name_),
        1,
        "FixedPoint step must read its state through one WorkingTable");
  }
  if (placed()) {
    checkPlacedLikeState(
        inputs_[0], sourceColumns_, outputColumns(), state, "anchor");
    checkPlacedLikeState(
        inputs_[1],
        inputs_[1]->outputColumns(),
        outputColumns(),
        state,
        "step");
  }

  if (convergence() != nullptr) {
    checkBranchReadsState(convergence(), name_, outputColumns(), "convergence");
    if (mode_ == FixedPointMode::kAppend) {
      VELOX_CHECK_EQ(
          countStateReads(convergence(), name_),
          1,
          "FixedPoint convergence must read its state through one WorkingTable");
    }
    VELOX_CHECK(
        convergence()->requiredStates().at(name_) ==
            inputs_[1]->requiredStates().at(name_),
        "FixedPoint convergence must read the state as the step does");
    const auto& verdict = convergence()->outputColumns();
    VELOX_CHECK_EQ(
        verdict.size(), 1, "FixedPoint convergence must output one column");
    VELOX_CHECK(
        verdict[0]->value().type->kind() == velox::TypeKind::BOOLEAN,
        "FixedPoint convergence must output a BOOLEAN");
  }
  VELOX_CHECK(
      convergence() != nullptr || !movesState(inputs_[1]),
      "FixedPoint whose step repartitions the state needs a convergence");
}

// static
bool FixedPoint::movesState(NodeCP node) {
  if (node->requiredStates().empty()) {
    return false;
  }
  return node->is(NodeType::kExchange) ||
      std::ranges::any_of(node->inputs(), movesState);
}

RequiredStates FixedPoint::deriveRequiredStates() const {
  RequiredStates states = Node::deriveRequiredStates();
  states.erase(name_);
  return states;
}

size_t FixedPoint::KeyHash::operator()(const FixedPoint* node) const {
  return hashOf(
      node->anchor(),
      node->step(),
      node->name(),
      node->outputColumns(),
      node->sourceColumns(),
      node->maxIterations(),
      node->failAtBound(),
      node->mode(),
      node->recursiveNumDrivers().has_value(),
      node->recursiveNumDrivers().value_or(0),
      node->convergence());
}

size_t FixedPoint::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.anchor,
      key.step,
      key.name,
      key.outputColumns,
      key.sourceColumns,
      key.maxIterations,
      key.failAtBound,
      key.mode,
      key.recursiveNumDrivers.has_value(),
      key.recursiveNumDrivers.value_or(0),
      key.convergence);
}

bool FixedPoint::KeyEq::operator()(
    const FixedPoint* left,
    const FixedPoint* right) const {
  return left->anchor() == right->anchor() && left->step() == right->step() &&
      left->name() == right->name() &&
      left->outputColumns() == right->outputColumns() &&
      left->sourceColumns() == right->sourceColumns() &&
      left->maxIterations() == right->maxIterations() &&
      left->failAtBound() == right->failAtBound() &&
      left->mode() == right->mode() &&
      left->recursiveNumDrivers() == right->recursiveNumDrivers() &&
      left->convergence() == right->convergence();
}

bool FixedPoint::KeyEq::operator()(const Key& key, const FixedPoint* node)
    const {
  return key.anchor == node->anchor() && key.step == node->step() &&
      key.name == node->name() && key.outputColumns == node->outputColumns() &&
      key.sourceColumns == node->sourceColumns() &&
      key.maxIterations == node->maxIterations() &&
      key.failAtBound == node->failAtBound() && key.mode == node->mode() &&
      key.recursiveNumDrivers == node->recursiveNumDrivers() &&
      key.convergence == node->convergence();
}

bool FixedPoint::KeyEq::operator()(const FixedPoint* node, const Key& key)
    const {
  return (*this)(key, node);
}

namespace {

// The keys read only 'input's columns.
void checkKeysRead(
    const ExprVector& keys,
    NodeCP input,
    std::string_view role) {
  const auto columns = PlanObjectSet::fromObjects(input->outputColumns());
  for (ExprCP key : keys) {
    VELOX_CHECK(
        key->columns().isSubset(columns),
        "Repeat {} must read only its input: {}",
        role,
        key->toString());
  }
}

NodeVector repeatInputs(const Repeat::Key& key) {
  NodeVector inputs{key.start, key.hop};
  if (key.end != nullptr) {
    inputs.push_back(key.end);
  }
  return inputs;
}

} // namespace

Partitioning Repeat::globalPartition(
    std::span<const Partitioning> /*inputPartitions*/,
    Builder& /*builder*/) const {
  return {};
}

Repeat::Repeat(const Key& key)
    : Node(
          NodeType::kRepeat,
          ColumnVector{key.outputColumns},
          RelationalProperties{}),
      inputs_(repeatInputs(key)),
      startKeys_(key.startKeys),
      hopSourceKeys_(key.hopSourceKeys),
      hopDestinationKeys_(key.hopDestinationKeys),
      endKeys_(key.endKeys),
      minHops_(key.minHops),
      maxHops_(key.maxHops),
      sourceColumns_(key.sourceColumns),
      hopCount_(key.hopCount) {
  VELOX_CHECK_NOT_NULL(start());
  VELOX_CHECK_NOT_NULL(hop());
  VELOX_CHECK(!startKeys_.empty(), "Repeat needs an identity");
  VELOX_CHECK_EQ(startKeys_.size(), hopSourceKeys_.size());
  VELOX_CHECK_EQ(startKeys_.size(), hopDestinationKeys_.size());
  VELOX_CHECK_EQ(startKeys_.size(), endKeys_.size());
  VELOX_CHECK_GE(minHops_, 0, "Repeat minHops must not be negative");
  VELOX_CHECK_LE(minHops_, maxHops_, "Repeat minHops must not exceed maxHops");
  VELOX_CHECK(
      minHops_ != 1 || maxHops_ != 1, "Repeat of exactly one hop is a join");
  checkKeysRead(startKeys_, start(), "start key");
  checkKeysRead(hopSourceKeys_, hop(), "hop source key");
  checkKeysRead(hopDestinationKeys_, hop(), "hop destination key");
  checkKeysRead(endKeys_, end() != nullptr ? end() : start(), "end key");

  auto available = PlanObjectSet::fromObjects(start()->outputColumns());
  if (end() != nullptr) {
    available.unionObjects(end()->outputColumns());
  }
  const auto hopColumns = PlanObjectSet::fromObjects(hop()->outputColumns());
  const auto& sources = effectiveSourceColumns();
  VELOX_CHECK_EQ(sources.size(), outputColumns().size());
  for (size_t i = 0; i < sources.size(); ++i) {
    ColumnCP output = outputColumns()[i];
    if (output == hopCount_) {
      VELOX_CHECK(
          sources[i] == output,
          "Repeat hop count must supply itself: {}",
          output->toString());
      VELOX_CHECK_EQ(
          output->value().type->kind(),
          velox::TypeKind::BIGINT,
          "Repeat hop count must be a BIGINT");
      continue;
    }
    if (available.contains(sources[i])) {
      VELOX_CHECK(
          output == sources[i],
          "Repeat output from start or end must keep its identity: {}",
          output->toString());
      continue;
    }
    VELOX_CHECK(
        hopColumns.contains(sources[i]),
        "Repeat output must come from start, end or hop: {}",
        sources[i]->toString());
    VELOX_CHECK(output != sources[i], "Repeat output from hop must be fresh");
    const auto& type = *output->value().type;
    const auto& sourceType = *sources[i]->value().type;
    if (type.kind() == velox::TypeKind::ARRAY &&
        type.childAt(0)->equivalent(sourceType)) {
      continue;
    }
    VELOX_CHECK(
        type.equivalent(sourceType),
        "Repeat output from hop must be its type or an array of it: {}",
        output->toString());
    VELOX_CHECK_LE(
        maxHops_, 1, "Repeat outputs a hop column's value only for one hop");
  }
  if (hopCount_ != nullptr) {
    VELOX_CHECK(
        std::find(outputColumns().begin(), outputColumns().end(), hopCount_) !=
            outputColumns().end(),
        "Repeat hop count must be an output: {}",
        hopCount_->toString());
  }
}

size_t Repeat::KeyHash::operator()(const Repeat* node) const {
  return hashOf(
      node->start(),
      node->hop(),
      node->end(),
      node->startKeys(),
      node->hopSourceKeys(),
      node->hopDestinationKeys(),
      node->endKeys(),
      node->minHops(),
      node->maxHops(),
      node->outputColumns(),
      node->sourceColumns(),
      node->hopCount());
}

size_t Repeat::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.start,
      key.hop,
      key.end,
      key.startKeys,
      key.hopSourceKeys,
      key.hopDestinationKeys,
      key.endKeys,
      key.minHops,
      key.maxHops,
      key.outputColumns,
      key.sourceColumns,
      key.hopCount);
}

bool Repeat::KeyEq::operator()(const Repeat* left, const Repeat* right) const {
  return left->start() == right->start() && left->hop() == right->hop() &&
      left->end() == right->end() && left->startKeys() == right->startKeys() &&
      left->hopSourceKeys() == right->hopSourceKeys() &&
      left->hopDestinationKeys() == right->hopDestinationKeys() &&
      left->endKeys() == right->endKeys() &&
      left->minHops() == right->minHops() &&
      left->maxHops() == right->maxHops() &&
      left->outputColumns() == right->outputColumns() &&
      left->sourceColumns() == right->sourceColumns() &&
      left->hopCount() == right->hopCount();
}

bool Repeat::KeyEq::operator()(const Key& key, const Repeat* node) const {
  return key.start == node->start() && key.hop == node->hop() &&
      key.end == node->end() && key.startKeys == node->startKeys() &&
      key.hopSourceKeys == node->hopSourceKeys() &&
      key.hopDestinationKeys == node->hopDestinationKeys() &&
      key.endKeys == node->endKeys() && key.minHops == node->minHops() &&
      key.maxHops == node->maxHops() &&
      key.outputColumns == node->outputColumns() &&
      key.sourceColumns == node->sourceColumns() &&
      key.hopCount == node->hopCount();
}

bool Repeat::KeyEq::operator()(const Repeat* node, const Key& key) const {
  return (*this)(key, node);
}

#define V2_DEFINE_ACCEPT(NodeT)                                               \
  void NodeT::accept(const NodeVisitor& visitor, NodeVisitorContext& context) \
      const {                                                                 \
    visitor.visit(*this, context);                                            \
  }

V2_DEFINE_ACCEPT(Scan)
V2_DEFINE_ACCEPT(Filter)
V2_DEFINE_ACCEPT(Project)
V2_DEFINE_ACCEPT(Limit)
V2_DEFINE_ACCEPT(Sort)
V2_DEFINE_ACCEPT(TopN)
V2_DEFINE_ACCEPT(Aggregate)
V2_DEFINE_ACCEPT(GroupId)
V2_DEFINE_ACCEPT(MarkDistinct)
V2_DEFINE_ACCEPT(Values)
V2_DEFINE_ACCEPT(Unnest)
V2_DEFINE_ACCEPT(UnionAll)
V2_DEFINE_ACCEPT(Join)
V2_DEFINE_ACCEPT(IndexLookupJoin)
V2_DEFINE_ACCEPT(Window)
V2_DEFINE_ACCEPT(Inference)
V2_DEFINE_ACCEPT(RowNumber)
V2_DEFINE_ACCEPT(TopNRowNumber)
V2_DEFINE_ACCEPT(Apply)
V2_DEFINE_ACCEPT(EnforceSingleRow)
V2_DEFINE_ACCEPT(AssignUniqueId)
V2_DEFINE_ACCEPT(EnforceDistinct)
V2_DEFINE_ACCEPT(Exchange)
V2_DEFINE_ACCEPT(TableWrite)
V2_DEFINE_ACCEPT(WorkingTable)
V2_DEFINE_ACCEPT(FixedPoint)
V2_DEFINE_ACCEPT(Repeat)

#undef V2_DEFINE_ACCEPT

} // namespace facebook::axiom::optimizer::v2
