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
#include "axiom/optimizer/v2/KeyHash.h"
#include "axiom/optimizer/v2/NodePrinter.h"
#include "axiom/optimizer/v2/NodeVisitor.h"

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
      {NodeType::kWindow, "Window"},
      {NodeType::kTopNRowNumber, "TopNRowNumber"},
      {NodeType::kApply, "Apply"},
      {NodeType::kEnforceSingleRow, "EnforceSingleRow"},
      {NodeType::kAssignUniqueId, "AssignUniqueId"},
      {NodeType::kEnforceDistinct, "EnforceDistinct"},
      {NodeType::kExchange, "Exchange"},
      {NodeType::kTableWrite, "TableWrite"},
  };
  return kNames;
}
} // namespace

AXIOM_DEFINE_ENUM_NAME(NodeType, nodeTypeNames);

std::string Node::toString() const {
  return NodePrinter::toText(this);
}

namespace {

// Per-operator derivation of per-driver local properties. Each node computes
// these in its constructor from its already-constructed inputs; distribution
// and relation-level properties are added with their consumers.

// Keeps the local properties expressible over `columns`: a `kGrouped` entry
// survives only if all its columns do; a `kSorted` entry is truncated to its
// leading run of columns in `columns`. A dropped or truncated entry ends the
// list — nothing nested under a broken outer property still holds.
LocalPropertyVector projectLocal(
    const LocalPropertyVector& local,
    const PlanObjectSet& columns) {
  LocalPropertyVector result;
  for (const LocalProperty& property : local) {
    if (property.kind == LocalPropertyKind::kGrouped) {
      if (!columns.containsAll(property.columns)) {
        break;
      }
      result.push_back(property);
      continue;
    }
    LocalProperty truncated{.kind = LocalPropertyKind::kSorted};
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

// Keeps an input's local properties expressible over `columns` (see
// `projectLocal`), for a node that emits only a subset of its input.
LocalPropertyVector retainedLocal(NodeCP input, const ColumnVector& columns) {
  return projectLocal(
      input->physicalProperties().local, PlanObjectSet::fromObjects(columns));
}

// Keeps an input's unique key-sets whose columns all survive in `columns`, for
// a node that emits only a subset of its input. A key-set with a dropped member
// is no longer a key of the output, so it is removed whole.
UniqueKeySetVector retainedUnique(NodeCP input, const ColumnVector& columns) {
  const auto columnSet = PlanObjectSet::fromObjects(columns);
  UniqueKeySetVector result;
  for (const UniqueKeySet& key : input->physicalProperties().unique) {
    if (key.columns.isSubset(columnSet)) {
      result.push_back(key);
    }
  }
  return result;
}

// A single `Sorted` property over the leading run of `keys` that are columns,
// paired with their orders. A computed sort key is not an output column, so it
// (and everything after it) is dropped; empty when the first key is not a
// column.
LocalPropertyVector sortedLocal(
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
  LocalPropertyVector result;
  result.push_back(
      {.kind = LocalPropertyKind::kSorted,
       .columns = std::move(columns),
       .orders = std::move(orders)});
  return result;
}

// Local ordering an exchange's output carries: an order-preserving gather keeps
// the merged sort order; every other exchange interleaves rows and keeps none.
LocalPropertyVector exchangeLocal(const Partitioning& partitioning) {
  if (partitioning.orderKeys.empty()) {
    return {};
  }
  return sortedLocal(partitioning.orderKeys, partitioning.orderTypes);
}

// A single `Grouped` property over `columns` (empty when there are none).
LocalPropertyVector groupedLocal(const ColumnVector& columns) {
  if (columns.empty()) {
    return {};
  }
  LocalPropertyVector result;
  result.push_back({.kind = LocalPropertyKind::kGrouped, .columns = columns});
  return result;
}

// A single `Grouped` property over one column.
LocalPropertyVector groupedLocal(ColumnCP column) {
  ColumnVector columns;
  columns.push_back(column);
  return groupedLocal(columns);
}

// A single global unique key over 'column' (e.g. AssignUniqueId's id).
UniqueKeySetVector globalUniqueKey(ColumnCP column) {
  UniqueKeySetVector result;
  result.push_back(
      UniqueKeySet{
          .columns = PlanObjectSet::single(column),
          .scope = PropertyScope::kGlobal});
  return result;
}

// Uniqueness that survives a remote exchange. A repartition/gather moves rows
// without duplicating them, so global uniqueness holds; but it can gather
// cross-driver duplicates onto one driver, so driver-scope uniqueness does not
// survive. A broadcast replicates rows and destroys even global uniqueness.
UniqueKeySetVector uniqueAcrossExchange(
    const UniqueKeySetVector& unique,
    const Partitioning& partitioning) {
  if (partitioning.is(PartitionKind::kBroadcast)) {
    return {};
  }
  UniqueKeySetVector result;
  for (const UniqueKeySet& key : unique) {
    if (key.scope == PropertyScope::kGlobal) {
      result.push_back(key);
    }
  }
  return result;
}

// Local grouping an inner/left join gains from probe uniqueness: a hash join
// emits each probe row's matches as one contiguous run, so the output is
// grouped by any key unique on the probe whose columns all survive —
// independent of the probe's input order, so this holds even after an exchange
// erased the probe's own local grouping. Caveat: a spilling probe or a
// driver-scrambling local exchange breaks it; valid at single-fragment,
// numDrivers=1.
void appendProbeUniqueGroupings(
    NodeCP left,
    const PlanObjectSet& outputColumns,
    LocalPropertyVector& local) {
  for (const UniqueKeySet& key : left->physicalProperties().unique) {
    if (key.columns.isSubset(outputColumns)) {
      local.push_back(
          LocalProperty{
              .kind = LocalPropertyKind::kGrouped,
              .columns = key.columns.template toObjects<Column>()});
    }
  }
}

// The probe (left) side's local properties survive an inner / left join within
// a driver; build-emitting and cross joins drop them.
// TODO: order through a spillable join is not guaranteed once spilling is
// enabled; revisit when spilling is modeled.
LocalPropertyVector joinLocal(
    velox::core::JoinType joinType,
    NodeCP left,
    const ColumnVector& outputColumns) {
  if (joinType == velox::core::JoinType::kInner ||
      joinType == velox::core::JoinType::kLeft) {
    LocalPropertyVector local = retainedLocal(left, outputColumns);
    appendProbeUniqueGroupings(
        left, PlanObjectSet::fromObjects(outputColumns), local);
    return local;
  }
  return {};
}

// Properties an operator inherits when it neither repartitions nor changes
// which rows exist in a way that breaks them: the input's distribution (minus
// its merge order, which belonged to the gather it came from — see
// `Partitioning::dropOrder`), local properties, and uniqueness. Such an
// operator keeps all of the input's columns, so every inherited property
// already references output columns.
PhysicalProperties passThroughProperties(NodeCP input) {
  const PhysicalProperties& props = input->physicalProperties();
  return PhysicalProperties{
      .globalPartition = props.globalPartition.dropOrder(),
      .local = props.local,
      .unique = props.unique};
}

// Properties of an operator that sorts its input on `orderKeys` without moving
// rows across tasks: it keeps the input's distribution and uniqueness and adds
// the per-driver sort order. A distributed sort is a per-task sort under a
// separate gather-merge exchange, and a single-stage sort runs over an
// already-gathered input — so the sort node itself inherits the input's
// partitioning either way (it is not the gather).
PhysicalProperties sortedProperties(
    NodeCP input,
    const ExprVector& orderKeys,
    const OrderTypeVector& orderTypes) {
  const PhysicalProperties& props = input->physicalProperties();
  return PhysicalProperties{
      .globalPartition = props.globalPartition.dropOrder(),
      .local = sortedLocal(orderKeys, orderTypes),
      .unique = props.unique};
}

// Remaps the input partitioning through a Project's column map: each partition
// key (an input column) is re-expressed on the output column that projects it,
// whether identity or rename (`exprs[i]` is a bare reference to the key, so its
// output column `outputColumns[i]` carries the same values, hence the same hash
// partition). A key not projected as a bare column — dropped, or appearing only
// inside a computed expression — drops the partition to unspecified. Gather and
// unspecified pass through unchanged.
Partitioning projectGlobalPartition(
    NodeCP input,
    const ExprVector& exprs,
    const ColumnVector& outputColumns) {
  const Partitioning& partitioning =
      input->physicalProperties().globalPartition;
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
      return {};
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

// Output global partitioning of a join. Only an inner or left join keeps the
// probe (left) rows in their partitions, so only those preserve a probe
// partition; other join types drop it. If every probe key is still an output
// column, the output is partitioned exactly as the probe was. Otherwise an
// inner join recovers each remaining key: it keeps a key whose columns all
// survive (a join projects columns unchanged, so a surviving column is
// identity-projected, and an expression over surviving columns still partitions
// the output), else substitutes an equal output column from the key's
// inner-join equivalence class. A left join can't recover a key from its
// null-padded build, so a dropped key makes its partition unspecified. A
// non-hash distribution (gather / broadcast / arbitrary) carries no keys and is
// inherited by kind, minus any merge order (see `Partitioning::dropOrder`).
Partitioning joinGlobalPartition(
    velox::core::JoinType joinType,
    NodeCP left,
    const ColumnVector& outputColumns) {
  if (joinType != velox::core::JoinType::kInner &&
      joinType != velox::core::JoinType::kLeft) {
    return {};
  }

  const Partitioning& probe = left->physicalProperties().globalPartition;
  if (probe.kind != PartitionKind::kPartitioned) {
    return probe.dropOrder();
  }

  const auto outputSet = PlanObjectSet::fromObjects(outputColumns);
  if (outputSet.containsAll(probe.keys)) {
    return probe;
  }

  if (joinType != velox::core::JoinType::kInner) {
    return {};
  }

  ExprVector keys;
  keys.reserve(probe.keys.size());
  for (ExprCP key : probe.keys) {
    if (outputSet.containsColumns(key)) {
      keys.push_back(key);
      continue;
    }
    ExprCP equiKey = survivingEquiKey(key, outputSet);
    if (equiKey == nullptr) {
      return {};
    }
    keys.push_back(equiKey);
  }

  Partitioning result = probe;
  result.keys = std::move(keys);
  return result;
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
    NodeCP input,
    const ExprVector& groupingKeys,
    const ColumnVector& groupingColumns) {
  const Partitioning& inputPartition =
      input->physicalProperties().globalPartition;
  if (inputPartition.kind != PartitionKind::kPartitioned) {
    return inputPartition.dropOrder();
  }
  ExprVector keys;
  keys.reserve(inputPartition.keys.size());
  for (ExprCP key : inputPartition.keys) {
    const auto it = std::ranges::find(groupingKeys, key);
    if (it == groupingKeys.end()) {
      return {};
    }
    keys.push_back(groupingColumns[it - groupingKeys.begin()]);
  }
  Partitioning result = inputPartition;
  result.keys = std::move(keys);
  return result;
}

// Output partitioning of a complete (single / final) aggregation: a global
// aggregate gathers to one task; otherwise the input's partitioning
// re-expressed on the output (see `aggregateInputPartition`).
Partitioning aggregateGlobalPartition(
    NodeCP input,
    const ExprVector& groupingKeys,
    const ColumnVector& groupingColumns) {
  if (groupingKeys.empty()) {
    return Partitioning::globalGather();
  }
  return aggregateInputPartition(input, groupingKeys, groupingColumns);
}

// Output partitioning of a scan of a bucketed (connector-partitioned) table:
// the connector's partition function on the output columns matching the
// layout's partition columns. Unspecified when the table is not partitioned or
// a partition column is not in the scan's output (so the partitioning can't be
// expressed over the output).
Partitioning scanGlobalPartition(const Scan::Key& key) {
  const auto* schemaTable = key.baseTable->schemaTable;
  if (schemaTable == nullptr || schemaTable->columnGroups.empty()) {
    return {};
  }
  const connector::TableLayout* layout = schemaTable->columnGroups[0]->layout;
  if (layout == nullptr) {
    return {};
  }
  const auto partitionType = layout->partitionType();
  const auto& partitionColumns = layout->partitionColumns();
  if (partitionType == nullptr || partitionColumns.empty()) {
    return {};
  }

  ExprVector keys;
  keys.reserve(partitionColumns.size());
  for (const auto* partitionColumn : partitionColumns) {
    ColumnCP match = nullptr;
    for (ColumnCP column : key.outputColumns) {
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
      .partitionType = partitionType.get(),
      .keys = std::move(keys),
      .scope = PropertyScope::kGlobal};
}

} // namespace

Scan::Scan(Key key)
    : Node(
          NodeType::kScan,
          ColumnVector{key.outputColumns},
          PhysicalProperties{.globalPartition = scanGlobalPartition(key)}),
      baseTable_(key.baseTable),
      filters_(std::move(key.filters)) {
  VELOX_CHECK_NOT_NULL(baseTable_);
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
  return hashOf(node->baseTable(), node->outputColumns(), node->filters());
}

size_t Scan::KeyHash::operator()(const Key& key) const {
  return hashOf(key.baseTable, key.outputColumns, key.filters);
}

bool Scan::KeyEq::operator()(const Scan* left, const Scan* right) const {
  return left->baseTable() == right->baseTable() &&
      left->outputColumns() == right->outputColumns() &&
      left->filters() == right->filters();
}

bool Scan::KeyEq::operator()(const Key& key, const Scan* node) const {
  return key.baseTable == node->baseTable() &&
      key.outputColumns == node->outputColumns() &&
      key.filters == node->filters();
}

bool Scan::KeyEq::operator()(const Scan* node, const Key& key) const {
  return (*this)(key, node);
}

Filter::Filter(Key key)
    : Node(
          NodeType::kFilter,
          ColumnVector{key.input->outputColumns()},
          passThroughProperties(key.input)),
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

Project::Project(Key key)
    : Node(
          NodeType::kProject,
          ColumnVector{key.outputColumns},
          PhysicalProperties{
              .globalPartition = projectGlobalPartition(
                  key.input,
                  key.exprs,
                  key.outputColumns),
              .local = retainedLocal(key.input, key.outputColumns),
              .unique = retainedUnique(key.input, key.outputColumns)}),
      input_(key.input),
      exprs_(std::move(key.exprs)) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_EQ(this->outputColumns().size(), exprs_.size());
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

Limit::Limit(Key key)
    : Node(
          NodeType::kLimit,
          ColumnVector{key.input->outputColumns()},
          passThroughProperties(key.input)),
      input_(key.input),
      offset_(key.offset),
      count_(key.count) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_GE(offset_, 0);
  VELOX_CHECK_GE(count_, 0);
}

size_t Limit::KeyHash::operator()(const Limit* node) const {
  return hashOf(node->input(), node->offset(), node->count());
}

size_t Limit::KeyHash::operator()(const Key& key) const {
  return hashOf(key.input, key.offset, key.count);
}

bool Limit::KeyEq::operator()(const Limit* left, const Limit* right) const {
  return left->input() == right->input() && left->offset() == right->offset() &&
      left->count() == right->count();
}

bool Limit::KeyEq::operator()(const Key& key, const Limit* node) const {
  return key.input == node->input() && key.offset == node->offset() &&
      key.count == node->count();
}

bool Limit::KeyEq::operator()(const Limit* node, const Key& key) const {
  return (*this)(key, node);
}

Sort::Sort(Key key)
    : Node(
          NodeType::kSort,
          ColumnVector{key.input->outputColumns()},
          sortedProperties(key.input, key.orderKeys, key.orderTypes)),
      input_(key.input),
      orderKeys_(std::move(key.orderKeys)),
      orderTypes_(std::move(key.orderTypes)) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(!orderKeys_.empty(), "Sort must have at least one order key");
  VELOX_CHECK_EQ(orderKeys_.size(), orderTypes_.size());
}

size_t Sort::KeyHash::operator()(const Sort* node) const {
  return hashOf(node->input(), node->orderKeys(), node->orderTypes());
}

size_t Sort::KeyHash::operator()(const Key& key) const {
  return hashOf(key.input, key.orderKeys, key.orderTypes);
}

bool Sort::KeyEq::operator()(const Sort* left, const Sort* right) const {
  return left->input() == right->input() &&
      left->orderKeys() == right->orderKeys() &&
      left->orderTypes() == right->orderTypes();
}

bool Sort::KeyEq::operator()(const Key& key, const Sort* node) const {
  return key.input == node->input() && key.orderKeys == node->orderKeys() &&
      key.orderTypes == node->orderTypes();
}

bool Sort::KeyEq::operator()(const Sort* node, const Key& key) const {
  return (*this)(key, node);
}

TopN::TopN(Key key)
    : Node(
          NodeType::kTopN,
          ColumnVector{key.input->outputColumns()},
          sortedProperties(key.input, key.orderKeys, key.orderTypes)),
      input_(key.input),
      orderKeys_(std::move(key.orderKeys)),
      orderTypes_(std::move(key.orderTypes)),
      offset_(key.offset),
      count_(key.count) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(!orderKeys_.empty(), "TopN must have at least one order key");
  VELOX_CHECK_EQ(orderKeys_.size(), orderTypes_.size());
  VELOX_CHECK_GE(offset_, 0);
  VELOX_CHECK_GE(count_, 0);
}

size_t TopN::KeyHash::operator()(const TopN* node) const {
  return hashOf(
      node->input(),
      node->orderKeys(),
      node->orderTypes(),
      node->offset(),
      node->count());
}

size_t TopN::KeyHash::operator()(const Key& key) const {
  return hashOf(
      key.input, key.orderKeys, key.orderTypes, key.offset, key.count);
}

bool TopN::KeyEq::operator()(const TopN* left, const TopN* right) const {
  return left->input() == right->input() &&
      left->orderKeys() == right->orderKeys() &&
      left->orderTypes() == right->orderTypes() &&
      left->offset() == right->offset() && left->count() == right->count();
}

bool TopN::KeyEq::operator()(const Key& key, const TopN* node) const {
  return key.input == node->input() && key.orderKeys == node->orderKeys() &&
      key.orderTypes == node->orderTypes() && key.offset == node->offset() &&
      key.count == node->count();
}

bool TopN::KeyEq::operator()(const TopN* node, const Key& key) const {
  return (*this)(key, node);
}

namespace {

// Physical properties of an aggregate's output, by step. A `kPartial`
// pre-aggregates per task without moving rows or gathering (even for an empty
// grouping): it keeps the input's partitioning re-expressed on its output, but
// its keys are not globally unique (the same key recurs across tasks).
// `kSingle` / `kFinal` produce the complete result — globally unique on the
// grouping keys, and partitioned on them when the input is (the `kFinal`'s
// input is the exchange that partitioned the partials).
PhysicalProperties aggregateProperties(const Aggregate::Key& key) {
  // The grouping values are materialized as the leading output columns, aligned
  // one-to-one with `groupingKeys`; properties are expressed over those output
  // columns, not the (possibly computed) grouping-key expressions.
  ColumnVector groupingColumns;
  groupingColumns.reserve(key.groupingKeys.size());
  for (size_t i = 0; i < key.groupingKeys.size(); ++i) {
    groupingColumns.push_back(key.outputColumns[i]);
  }
  if (key.step == AggregateStep::kPartial) {
    return PhysicalProperties{
        .globalPartition = aggregateInputPartition(
            key.input, key.groupingKeys, groupingColumns),
        .local = groupedLocal(groupingColumns)};
  }
  // An aggregate emits one row per distinct grouping-key combination, so the
  // grouping output columns are a global unique key (the empty grouping is the
  // single global row, unique on the empty set; a grouping-set aggregate's
  // group-id is one of the keys, so distinct key sets stay distinct).
  return PhysicalProperties{
      .globalPartition = aggregateGlobalPartition(
          key.input, key.groupingKeys, groupingColumns),
      .local = groupedLocal(groupingColumns),
      .unique = {UniqueKeySet{
          .columns = PlanObjectSet::fromObjects(groupingColumns),
          .scope = PropertyScope::kGlobal}}};
}

} // namespace

Aggregate::Aggregate(Key key)
    : Node(
          NodeType::kAggregate,
          ColumnVector{key.outputColumns},
          aggregateProperties(key)),
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

GroupId::GroupId(Key key)
    : Node(NodeType::kGroupId, ColumnVector{key.outputColumns}, {}),
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

MarkDistinct::MarkDistinct(Key key)
    : Node(
          NodeType::kMarkDistinct,
          ColumnVector{key.outputColumns},
          passThroughProperties(key.input)),
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

Values::Values(Key key)
    : Node(NodeType::kValues, ColumnVector{key.outputColumns}, {}),
      source_(key.source),
      rows_(key.rows) {
  VELOX_CHECK(
      source_ == nullptr || rows_ == nullptr,
      "Values cannot carry both a passthrough source and folded rows");

  const auto numColumns = this->outputColumns().size();
  if (source_ != nullptr) {
    const auto& rowType = source_->outputType();
    VELOX_CHECK_EQ(
        rowType->size(),
        numColumns,
        "Values source schema must match outputColumns count");
    for (size_t i = 0; i < numColumns; ++i) {
      VELOX_CHECK(
          rowType->childAt(i)->equivalent(
              *this->outputColumns()[i]->value().type),
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
      VELOX_CHECK_EQ(
          row.row().size(),
          numColumns,
          "Values folded row field count must match outputColumns");
    }
  }
}

size_t Values::KeyHash::operator()(const Values* node) const {
  return hashOf(node->source(), node->rows(), node->outputColumns());
}

size_t Values::KeyHash::operator()(const Key& key) const {
  return hashOf(key.source, key.rows, key.outputColumns);
}

bool Values::KeyEq::operator()(const Values* left, const Values* right) const {
  return left->source() == right->source() && left->rows() == right->rows() &&
      left->outputColumns() == right->outputColumns();
}

bool Values::KeyEq::operator()(const Key& key, const Values* node) const {
  return key.source == node->source() && key.rows == node->rows() &&
      key.outputColumns == node->outputColumns();
}

bool Values::KeyEq::operator()(const Values* node, const Key& key) const {
  return (*this)(key, node);
}

Unnest::Unnest(Key key)
    : Node(
          NodeType::kUnnest,
          ColumnVector{key.outputColumns},
          PhysicalProperties{
              .local = retainedLocal(key.input, key.replicatedColumns)}),
      input_(key.input),
      unnestExpressions_(std::move(key.unnestExpressions)),
      replicatedColumns_(std::move(key.replicatedColumns)),
      unnestColumns_(std::move(key.unnestColumns)),
      ordinalityColumn_(key.ordinalityColumn) {
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
}

size_t Unnest::KeyHash::operator()(const Unnest* node) const {
  size_t hash = hashOf(
      node->input(),
      node->unnestExpressions(),
      node->outputColumns(),
      node->replicatedColumns(),
      node->ordinalityColumn());
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
      key.ordinalityColumn);
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
      left->outputColumns() == right->outputColumns();
}

bool Unnest::KeyEq::operator()(const Key& key, const Unnest* node) const {
  return key.input == node->input() &&
      key.unnestExpressions == node->unnestExpressions() &&
      key.replicatedColumns == node->replicatedColumns() &&
      key.unnestColumns == node->unnestColumns() &&
      key.ordinalityColumn == node->ordinalityColumn() &&
      key.outputColumns == node->outputColumns();
}

bool Unnest::KeyEq::operator()(const Unnest* node, const Key& key) const {
  return (*this)(key, node);
}

namespace {

// Output global partitioning of a union: bucketed on the union output columns
// when every leg is connector-bucketed on leg columns that map (via legColumns)
// to those same output columns, with copartitionable partitionings. The
// representative type is the min-partition-count leg's — its count equals the
// copartition's — reused as a query-lifetime pointer. Unspecified otherwise, so
// the union then distributes its legs independently.
Partitioning unionGlobalPartition(const UnionAll::Key& key) {
  const connector::PartitionType* representative = nullptr;
  ExprVector outputKeys;
  for (size_t leg = 0; leg < key.inputs.size(); ++leg) {
    const Partitioning& part =
        key.inputs[leg]->physicalProperties().globalPartition;
    if (part.kind != PartitionKind::kPartitioned ||
        part.partitionType == nullptr) {
      return {};
    }
    // Map each leg partition key to the union output column at the same index.
    const ColumnVector& legColumns = key.legColumns[leg];
    ExprVector legOutputKeys;
    legOutputKeys.reserve(part.keys.size());
    for (ExprCP legKey : part.keys) {
      ExprCP mapped = nullptr;
      for (size_t i = 0; i < legColumns.size(); ++i) {
        if (legColumns[i] == legKey) {
          mapped = key.outputColumns[i];
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
    // copartition is only a feasibility check; the output reuses the
    // min-partition-count leg's layout-owned type, whose count equals
    // copartition's (a fresh shared_ptr we can't retain as a raw pointer).
    const auto compatible = representative->copartition(*part.partitionType);
    if (compatible == nullptr) {
      return {};
    }
    if (part.partitionType->numPartitions() < representative->numPartitions()) {
      representative = part.partitionType;
    }
    VELOX_DCHECK_EQ(
        representative->numPartitions(),
        compatible->numPartitions(),
        "Co-bucketed union output must reuse a layout type matching copartition's count");
  }
  return Partitioning{
      .kind = PartitionKind::kPartitioned,
      .partitionType = representative,
      .keys = std::move(outputKeys),
      .scope = PropertyScope::kGlobal};
}

} // namespace

UnionAll::UnionAll(Key key)
    : Node(
          NodeType::kUnionAll,
          ColumnVector{key.outputColumns},
          PhysicalProperties{.globalPartition = unionGlobalPartition(key)}),
      inputs_(std::move(key.inputs)),
      legColumns_(std::move(key.legColumns)) {
  VELOX_CHECK_GE(inputs_.size(), 2, "UnionAll requires at least two inputs");
  for (auto* input : inputs_) {
    VELOX_CHECK_NOT_NULL(input);
  }
  VELOX_CHECK_EQ(
      legColumns_.size(),
      inputs_.size(),
      "UnionAll legColumns must have one entry per input");
  const auto numOutputs = this->outputColumns().size();
  for (size_t i = 0; i < inputs_.size(); ++i) {
    VELOX_CHECK_EQ(
        legColumns_[i].size(),
        numOutputs,
        "UnionAll legColumns[i] must align with outputColumns");
    const auto available =
        PlanObjectSet::fromObjects(inputs_[i]->outputColumns());
    for (ColumnCP column : legColumns_[i]) {
      VELOX_CHECK(
          available.contains(column),
          "UnionAll legColumns[{}] references a Column not in inputs[{}]->outputColumns()",
          i,
          i);
    }
  }
}

size_t UnionAll::KeyHash::operator()(const UnionAll* node) const {
  const auto& legColumns = node->legColumns();
  size_t hash = hashOf(node->inputs(), node->outputColumns());
  for (size_t legIdx = 0; legIdx < legColumns.size(); ++legIdx) {
    // Mix legIdx and leg size to break symmetry across pointer permutations
    // (e.g., [[a,b],[c,d]] vs [[a,d],[c,b]]).
    hash = velox::bits::hashMix(hash, legIdx);
    hash = velox::bits::hashMix(hash, legColumns[legIdx].size());
    for (const auto* column : legColumns[legIdx]) {
      hash = velox::bits::hashMix(hash, reinterpret_cast<uintptr_t>(column));
    }
  }
  return hash;
}

size_t UnionAll::KeyHash::operator()(const Key& key) const {
  size_t hash = hashOf(key.inputs, key.outputColumns);
  for (size_t legIdx = 0; legIdx < key.legColumns.size(); ++legIdx) {
    // Mix legIdx and leg size to break symmetry across pointer permutations
    // (e.g., [[a,b],[c,d]] vs [[a,d],[c,b]]).
    hash = velox::bits::hashMix(hash, legIdx);
    hash = velox::bits::hashMix(hash, key.legColumns[legIdx].size());
    for (const auto* column : key.legColumns[legIdx]) {
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

Join::Join(Key key)
    : Node(
          NodeType::kJoin,
          ColumnVector{key.outputColumns},
          PhysicalProperties{
              .globalPartition = joinGlobalPartition(
                  key.joinType,
                  key.left,
                  key.outputColumns),
              .local = joinLocal(key.joinType, key.left, key.outputColumns)}),
      inputs_{key.left, key.right},
      joinType_(key.joinType),
      leftKeys_(std::move(key.leftKeys)),
      rightKeys_(std::move(key.rightKeys)),
      filter_(std::move(key.filter)),
      nullAware_(key.nullAware),
      nullAsValue_(key.nullAsValue) {
  VELOX_CHECK_NOT_NULL(inputs_[0]);
  VELOX_CHECK_NOT_NULL(inputs_[1]);
  VELOX_CHECK_EQ(leftKeys_.size(), rightKeys_.size());
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
      node->nullAware(),
      node->nullAsValue());
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
      key.nullAware,
      key.nullAsValue);
}

bool Join::KeyEq::operator()(const Join* left, const Join* right) const {
  return left->left() == right->left() && left->right() == right->right() &&
      left->joinType() == right->joinType() &&
      left->leftKeys() == right->leftKeys() &&
      left->rightKeys() == right->rightKeys() &&
      left->filter() == right->filter() &&
      left->nullAware() == right->nullAware() &&
      left->nullAsValue() == right->nullAsValue() &&
      left->outputColumns() == right->outputColumns();
}

bool Join::KeyEq::operator()(const Key& key, const Join* node) const {
  return key.left == node->left() && key.right == node->right() &&
      key.joinType == node->joinType() && key.leftKeys == node->leftKeys() &&
      key.rightKeys == node->rightKeys() && key.filter == node->filter() &&
      key.nullAware == node->nullAware() &&
      key.nullAsValue == node->nullAsValue() &&
      key.outputColumns == node->outputColumns();
}

bool Join::KeyEq::operator()(const Join* node, const Key& key) const {
  return (*this)(key, node);
}

Window::Window(Key key)
    : Node(
          NodeType::kWindow,
          ColumnVector{key.outputColumns},
          passThroughProperties(key.input)),
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

TopNRowNumber::TopNRowNumber(Key key)
    : Node(NodeType::kTopNRowNumber, ColumnVector{key.outputColumns}, {}),
      input_(key.input),
      rankFunction_(key.rankFunction),
      partitionKeys_(std::move(key.partitionKeys)),
      orderKeys_(std::move(key.orderKeys)),
      orderTypes_(std::move(key.orderTypes)),
      limit_(key.limit),
      rankColumn_(key.rankColumn) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(!orderKeys_.empty(), "TopNRowNumber must have order keys");
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

Apply::Apply(Key key)
    : Node(NodeType::kApply, ColumnVector{key.outputColumns}, {}),
      inputs_{key.input, key.body},
      correlationColumns_(std::move(key.correlationColumns)),
      kind_(key.kind),
      filter_(std::move(key.filter)),
      enforceSingleRow_(key.enforceSingleRow),
      markColumn_(key.markColumn),
      inLhs_(key.inLhs),
      inBodyKey_(key.inBodyKey),
      includeMarker_(key.includeMarker) {
  VELOX_CHECK_NOT_NULL(inputs_[0]);
  VELOX_CHECK_NOT_NULL(inputs_[1]);

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
  for (size_t i = 0; i < inputCols.size(); ++i) {
    VELOX_CHECK(
        outputColumns()[i] == inputCols[i],
        "Apply.outputColumns prefix must match input.outputColumns");
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
    for (size_t i = 0; i < bodySlots; ++i) {
      ColumnCP column = outputColumns()[inputCols.size() + i];
      VELOX_CHECK(
          !seen.contains(column),
          "Apply.outputColumns body cols must be unique vs input and each other");
      seen.add(column);
    }
    if (isLeft) {
      VELOX_CHECK(
          outputColumns().back() == includeMarker_,
          "Apply.outputColumns must end with includeMarker for kLeft");
    }
  } else {
    VELOX_CHECK_EQ(
        outputColumns().size(),
        inputCols.size() + 1,
        "kLeftSemiProject Apply.outputColumns size mismatch");
    VELOX_CHECK(
        outputColumns().back() == markColumn_,
        "Apply.outputColumns must end with markColumn for kLeftSemiProject");
  }
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
      node->outputColumns());
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
      key.outputColumns);
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
      left->outputColumns() == right->outputColumns();
}

bool Apply::KeyEq::operator()(const Key& key, const Apply* node) const {
  return key.input == node->input() && key.body == node->body() &&
      key.correlationColumns == node->correlationColumns() &&
      key.kind == node->kind() && key.filter == node->filter() &&
      key.enforceSingleRow == node->enforceSingleRow() &&
      key.markColumn == node->markColumn() && key.inLhs == node->inLhs() &&
      key.inBodyKey == node->inBodyKey() &&
      key.includeMarker == node->includeMarker() &&
      key.outputColumns == node->outputColumns();
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

AssignUniqueId::AssignUniqueId(Key key)
    : Node(
          NodeType::kAssignUniqueId,
          withIdColumn(key.input->outputColumns(), key.idColumn),
          PhysicalProperties{
              .local = groupedLocal(key.idColumn),
              .unique = globalUniqueKey(key.idColumn)}),
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

EnforceSingleRow::EnforceSingleRow(Key key)
    : Node(
          NodeType::kEnforceSingleRow,
          ColumnVector{key.input->outputColumns()},
          passThroughProperties(key.input)),
      input_(key.input) {
  VELOX_CHECK_NOT_NULL(input_);
}

size_t EnforceSingleRow::KeyHash::operator()(
    const EnforceSingleRow* node) const {
  return hashOf(node->input());
}

size_t EnforceSingleRow::KeyHash::operator()(const Key& key) const {
  return hashOf(key.input);
}

bool EnforceSingleRow::KeyEq::operator()(
    const EnforceSingleRow* left,
    const EnforceSingleRow* right) const {
  return left->input() == right->input();
}

bool EnforceSingleRow::KeyEq::operator()(
    const Key& key,
    const EnforceSingleRow* node) const {
  return key.input == node->input();
}

bool EnforceSingleRow::KeyEq::operator()(
    const EnforceSingleRow* node,
    const Key& key) const {
  return (*this)(key, node);
}

EnforceDistinct::EnforceDistinct(Key key)
    : Node(
          NodeType::kEnforceDistinct,
          ColumnVector{key.input->outputColumns()},
          passThroughProperties(key.input)),
      input_(key.input),
      distinctKeys_(std::move(key.distinctKeys)),
      errorMessage_(key.errorMessage) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(
      !distinctKeys_.empty(),
      "EnforceDistinct must have at least one distinct key");
  VELOX_CHECK_NOT_NULL(errorMessage_);
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

Exchange::Exchange(Key key)
    : Node(
          NodeType::kExchange,
          ColumnVector{key.input->outputColumns()},
          // A remote exchange drops per-driver local order/grouping (rows
          // interleave) — except an order-preserving gather, which keeps the
          // merged sort order. Uniqueness survives per uniqueAcrossExchange
          // (global-scope only, none under a broadcast).
          PhysicalProperties{
              .globalPartition = key.partitioning,
              .local = exchangeLocal(key.partitioning),
              .unique = uniqueAcrossExchange(
                  key.input->physicalProperties().unique,
                  key.partitioning)}),
      input_(key.input),
      partitioning_(std::move(key.partitioning)) {
  VELOX_CHECK_NOT_NULL(input_);
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
      partitioning.replicateNullsAndAny);
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
      key.partitioning.replicateNullsAndAny);
}

bool Exchange::KeyEq::operator()(const Exchange* left, const Exchange* right)
    const {
  return left->input() == right->input() &&
      left->partitioning() == right->partitioning();
}

bool Exchange::KeyEq::operator()(const Key& key, const Exchange* node) const {
  return key.input == node->input() && key.partitioning == node->partitioning();
}

bool Exchange::KeyEq::operator()(const Exchange* node, const Key& key) const {
  return (*this)(key, node);
}

TableWrite::TableWrite(Key key)
    : Node(
          NodeType::kTableWrite,
          ColumnVector{Column::create("rows", Value(toType(velox::BIGINT())))},
          {}),
      input_(key.input),
      table_(key.table),
      kind_(key.kind),
      columnExprs_(std::move(key.columnExprs)) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK_NOT_NULL(table_);
  VELOX_CHECK(
      !columnExprs_.empty(), "TableWrite must write at least one column");
  VELOX_CHECK_EQ(columnExprs_.size(), table_->type()->size());
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
V2_DEFINE_ACCEPT(Window)
V2_DEFINE_ACCEPT(TopNRowNumber)
V2_DEFINE_ACCEPT(Apply)
V2_DEFINE_ACCEPT(EnforceSingleRow)
V2_DEFINE_ACCEPT(AssignUniqueId)
V2_DEFINE_ACCEPT(EnforceDistinct)
V2_DEFINE_ACCEPT(Exchange)
V2_DEFINE_ACCEPT(TableWrite)

#undef V2_DEFINE_ACCEPT

} // namespace facebook::axiom::optimizer::v2
