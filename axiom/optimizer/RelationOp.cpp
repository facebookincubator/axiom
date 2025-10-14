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

#include <algorithm>

#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/expression/ScopedVarSetter.h"

namespace facebook::axiom::optimizer {

void Cost::add(const Cost& other) {
  inputCardinality += other.inputCardinality;
  fanout += other.fanout;
  setupCost += other.setupCost;
}

namespace {

const auto& relTypeNames() {
  static const folly::F14FastMap<RelType, std::string_view> kNames = {
      {RelType::kTableScan, "TableScan"},
      {RelType::kRepartition, "Repartition"},
      {RelType::kFilter, "Filter"},
      {RelType::kProject, "Project"},
      {RelType::kJoin, "Join"},
      {RelType::kHashBuild, "HashBuild"},
      {RelType::kAggregation, "Aggregation"},
      {RelType::kOrderBy, "OrderBy"},
      {RelType::kUnionAll, "UnionAll"},
      {RelType::kLimit, "Limit"},
      {RelType::kValues, "Values"},
  };

  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(RelType, relTypeNames)

const Value& RelationOp::value(ExprCP expr) const {
  // Compute new Value by applying restrictions from operators
  // between the place Expr is first defined and the output of
  // 'this'. Memoize the result in 'this'.
  return expr->value();
}

namespace {
template <typename T>
std::string itemsToString(const T* items, size_t n) {
  std::stringstream out;
  for (size_t i = 0; i < n; ++i) {
    out << items[i]->toString();
    if (i < n - 1) {
      out << ", ";
    }
  }
  return out.str();
}

// For leaf nodes, the fanout represents the cardinality, and the unitCost is
// the total cost.
// For non-leaf nodes, the fanout represents the change in cardinality (output
// cardinality / input cardinality), and the unitCost is the per-row cost.
void updateLeafCost(
    float cardinality,
    const ColumnVector& columns,
    Cost& cost) {
  cost.fanout = cardinality;

  const auto size = byteSize(columns);
  const auto numColumns = static_cast<float>(columns.size());
  const auto rowCost = numColumns * Costs::kColumnRowCost +
      std::max<float>(0, size - 8 * numColumns) * Costs::kColumnByteCost;
  cost.unitCost += cost.fanout * rowCost;
}

float orderPrefixDistance(
    const RelationOpPtr& input,
    ColumnGroupCP index,
    const ExprVector& keys) {
  const auto& orderKeys = index->distribution.orderKeys;
  float selection = 1;
  for (int32_t i = 0; i < input->distribution().orderKeys.size() &&
       i < orderKeys.size() && i < keys.size();
       ++i) {
    if (input->distribution().orderKeys[i]->sameOrEqual(*keys[i])) {
      selection *= orderKeys[i]->value().cardinality;
    }
  }
  return selection;
}

} // namespace

TableScan::TableScan(
    RelationOpPtr input,
    Distribution distribution,
    const BaseTable* table,
    ColumnGroupCP index,
    float fanout,
    ColumnVector columns,
    ExprVector lookupKeys,
    velox::core::JoinType joinType,
    ExprVector joinFilter)
    : RelationOp(
          RelType::kTableScan,
          std::move(input),
          std::move(distribution),
          std::move(columns)),
      baseTable(table),
      index(index),
      keys(std::move(lookupKeys)),
      joinType(joinType),
      joinFilter(std::move(joinFilter)) {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = fanout;

  if (!keys.empty()) {
    float lookupRange(index->table->cardinality);
    float orderSelectivity = orderPrefixDistance(input_, index, keys);
    auto distance = lookupRange / std::max<float>(1, orderSelectivity);
    float batchSize = std::min<float>(cost_.inputCardinality, 10000);
    if (orderSelectivity == 1) {
      // The data does not come in key order.
      float batchCost = index->lookupCost(lookupRange) +
          index->lookupCost(lookupRange / batchSize) *
              std::max<float>(1, batchSize);
      cost_.unitCost = batchCost / batchSize;
    } else {
      float batchCost = index->lookupCost(lookupRange) +
          index->lookupCost(distance) * std::max<float>(1, batchSize);
      cost_.unitCost = batchCost / batchSize;
    }
    return;
  }
  const auto cardinality =
      index->table->cardinality * baseTable->filterSelectivity;
  updateLeafCost(cardinality, columns_, cost_);
}

// static
Distribution TableScan::outputDistribution(
    const BaseTable* baseTable,
    ColumnGroupCP index,
    const ColumnVector& columns) {
  auto schemaColumns = transform<ColumnVector>(
      columns, [](auto& column) { return column->schemaColumn(); });

  const auto& distribution = index->distribution;

  ExprVector partition;
  ExprVector orderKeys;
  OrderTypeVector orderTypes;
  // if all partitioning columns are projected, the output is partitioned.
  if (isSubset(distribution.partition, schemaColumns)) {
    partition = distribution.partition;
    replace(partition, schemaColumns, columns.data());
  }

  auto numPrefix = prefixSize(distribution.orderKeys, schemaColumns);
  if (numPrefix > 0) {
    orderKeys = distribution.orderKeys;
    orderKeys.resize(numPrefix);
    orderTypes = distribution.orderTypes;
    orderTypes.resize(numPrefix);
    replace(orderKeys, schemaColumns, columns.data());
  }
  return {
      distribution.distributionType,
      std::move(partition),
      std::move(orderKeys),
      std::move(orderTypes),
      distribution.numKeysUnique <= numPrefix ? distribution.numKeysUnique : 0,
      1.0F / baseTable->filterSelectivity,
  };
}

std::string Cost::toString(bool /*detail*/, bool isUnit) const {
  std::stringstream out;
  float multiplier = isUnit ? 1 : inputCardinality;
  out << succinctNumber(fanout * multiplier) << " rows "
      << succinctNumber(unitCost * multiplier) << "CU";
  if (setupCost > 0) {
    out << ", setup " << succinctNumber(setupCost) << "CU";
  }
  if (totalBytes > 0) {
    out << " build= "
        << velox::succinctBytes(static_cast<uint64_t>(totalBytes));
  }
  if (transferBytes > 0) {
    out << " network= "
        << velox::succinctBytes(static_cast<uint64_t>(transferBytes));
  }
  return out.str();
}

void RelationOp::printCost(bool detail, std::stringstream& out) const {
  auto ctx = queryCtx();
  if (ctx && ctx->contextPlan()) {
    auto plan = ctx->contextPlan();
    auto totalCost = plan->cost.unitCost + plan->cost.setupCost;
    auto pct = 100 * cost_.inputCardinality * cost_.unitCost / totalCost;
    out << " " << std::fixed << std::setprecision(2) << pct << "% ";
  }
  if (detail) {
    out << " " << cost_.toString(detail, false) << std::endl;
  }
}

namespace {

const char* joinTypeLabel(velox::core::JoinType type) {
  switch (type) {
    case velox::core::JoinType::kLeft:
      return "left";
    case velox::core::JoinType::kRight:
      return "right";
    case velox::core::JoinType::kRightSemiFilter:
      return "right exists";
    case velox::core::JoinType::kRightSemiProject:
      return "right exists-flag";
    case velox::core::JoinType::kLeftSemiFilter:
      return "exists";
    case velox::core::JoinType::kLeftSemiProject:
      return "exists-flag";
    case velox::core::JoinType::kAnti:
      return "not exists";
    default:
      return "";
  }
}

QGString sanitizeHistoryKey(std::string in) {
  for (auto i = 0; i < in.size(); ++i) {
    unsigned char c = in[i];
    if (c < 32 || c > 127 || c == '{' || c == '}' || c == '"') {
      in[i] = '?';
    }
  }
  return QGString(in);
}

} // namespace

const QGString& TableScan::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  std::stringstream out;
  out << "scan " << baseTable->schemaTable->name << "(";
  auto* opt = queryCtx()->optimization();
  velox::ScopedVarSetter cnames(&opt->cnamesInExpr(), false);
  for (auto& key : keys) {
    out << "lookup " << key->toString() << ", ";
  }
  std::vector<std::string> filters;
  for (auto& f : baseTable->columnFilters) {
    filters.push_back(f->toString());
  }
  for (auto& f : baseTable->filter) {
    filters.push_back(f->toString());
  }
  std::ranges::sort(filters);
  for (auto& f : filters) {
    out << "f: " << f << ", ";
  }
  out << ")";
  key_ = sanitizeHistoryKey(out.str());
  return key_;
}

std::string TableScan::toString(bool /*recursive*/, bool detail) const {
  std::stringstream out;
  if (input()) {
    out << input()->toString(true, detail);
    out << " *I " << joinTypeLabel(joinType);
  }
  out << baseTable->schemaTable->name << " " << baseTable->cname;
  if (detail) {
    printCost(detail, out);
    if (!input()) {
      out << distribution_.toString() << std::endl;
    }
  }
  return out.str();
}

Values::Values(const ValuesTable& valuesTable, ColumnVector columns)
    : RelationOp{RelType::kValues, nullptr, Distribution::gather(), std::move(columns)},
      valuesTable{valuesTable} {
  cost_.inputCardinality = 1;

  const auto cardinality = valuesTable.cardinality();
  updateLeafCost(cardinality, columns_, cost_);
}

const QGString& Values::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  std::stringstream out;
  out << "values " << valuesTable.values.id();
  key_ = sanitizeHistoryKey(out.str());
  return key_;
}

std::string Values::toString(bool /*recursive*/, bool detail) const {
  VELOX_DCHECK(!input());
  std::stringstream out;
  out << valuesTable.values.id() << " " << valuesTable.cname;
  if (detail) {
    printCost(detail, out);
    out << distribution_.toString() << std::endl;
  }
  return out.str();
}

Join::Join(
    JoinMethod method,
    velox::core::JoinType joinType,
    RelationOpPtr lhs,
    RelationOpPtr rhs,
    ExprVector lhsKeys,
    ExprVector rhsKeys,
    ExprVector filterExprs,
    float fanout,
    ColumnVector columns)
    : RelationOp{RelType::kJoin, std::move(lhs), std::move(columns)},
      method{method},
      joinType{joinType},
      right{std::move(rhs)},
      leftKeys{std::move(lhsKeys)},
      rightKeys{std::move(rhsKeys)},
      filter{std::move(filterExprs)} {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = fanout;

  const float buildSize = right->cost().inputCardinality;
  const auto numRightColumns =
      static_cast<float>(right->input()->columns().size());
  auto rowCost = numRightColumns * Costs::kHashExtractColumnCost;
  const auto numLeftKeys = static_cast<float>(leftKeys.size());
  cost_.unitCost = Costs::hashProbeCost(buildSize) + cost_.fanout * rowCost +
      numLeftKeys * Costs::kHashColumnCost;
}

namespace {
std::pair<std::string, std::string> joinKeysString(
    const ExprVector& left,
    const ExprVector& right) {
  std::vector<int32_t> indices(left.size());
  std::iota(indices.begin(), indices.end(), 0);
  auto* opt = queryCtx()->optimization();
  velox::ScopedVarSetter cname(&opt->cnamesInExpr(), false);
  std::vector<std::string> strings;
  for (auto& k : left) {
    strings.push_back(k->toString());
  }
  std::ranges::sort(
      indices, [&](int32_t l, int32_t r) { return strings[l] < strings[r]; });
  std::stringstream leftStream;
  std::stringstream rightStream;
  for (auto i : indices) {
    leftStream << left[i]->toString() << ", ";
    rightStream << right[i]->toString() << ", ";
  }
  return std::make_pair(leftStream.str(), rightStream.str());
}
} // namespace

const QGString& Join::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  auto& leftTree = input_->historyKey();
  auto& rightTree = right->historyKey();
  std::stringstream out;
  auto [leftText, rightText] = joinKeysString(leftKeys, rightKeys);
  if (leftTree < rightTree || joinType != velox::core::JoinType::kInner) {
    out << "join " << joinTypeLabel(joinType) << "(" << leftTree << " keys "
        << leftText << " = " << rightText << rightTree << ")";
  } else {
    out << "join " << joinTypeLabel(reverseJoinType(joinType)) << "("
        << rightTree << " keys " << rightText << " = " << leftText << leftTree
        << ")";
  }
  key_ = sanitizeHistoryKey(out.str());
  return key_;
}

Join* Join::makeCrossJoin(
    RelationOpPtr input,
    RelationOpPtr right,
    ColumnVector columns) {
  float fanout = right->resultCardinality();
  return make<Join>(
      JoinMethod::kCross,
      velox::core::JoinType::kInner,
      std::move(input),
      std::move(right),
      ExprVector{},
      ExprVector{},
      ExprVector{},
      fanout,
      std::move(columns));
}

std::string Join::toString(bool recursive, bool detail) const {
  std::stringstream out;
  if (recursive) {
    out << input()->toString(true, detail);
  }
  out << "*" << (method == JoinMethod::kHash ? "H" : "M") << " "
      << joinTypeLabel(joinType);
  printCost(detail, out);
  if (detail) {
    out << "columns: " << itemsToString(columns().data(), columns().size())
        << std::endl;
  }
  if (detail && buildCost.unitCost > 0) {
    out << "{ build=" << buildCost.toString(detail, true) << "}";
  }
  if (recursive) {
    out << " (" << right->toString(true, detail) << ")";
    if (detail) {
      out << std::endl;
    }
  }
  return out.str();
}

Repartition::Repartition(
    RelationOpPtr input,
    Distribution distribution,
    ColumnVector columns)
    : RelationOp(
          RelType::kRepartition,
          std::move(input),
          std::move(distribution),
          std::move(columns)) {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;

  auto size = shuffleCost(columns_);

  cost_.unitCost = size;
  cost_.transferBytes =
      cost_.inputCardinality * size * Costs::byteShuffleCost();
}

std::string Repartition::toString(bool recursive, bool detail) const {
  std::stringstream out;
  if (recursive) {
    out << input()->toString(true, detail) << " ";
  }
  out << (distribution().isBroadcast ? "broadcast" : "shuffle") << " ";
  if (detail && !distribution().isBroadcast) {
    out << distribution().toString();
    printCost(detail, out);
  } else if (detail) {
    printCost(detail, out);
  }
  return out.str();
}

namespace {
ColumnVector concatColumns(const ExprVector& lhs, const ColumnVector& rhs) {
  ColumnVector result;
  result.reserve(lhs.size() + rhs.size());
  for (const auto& expr : lhs) {
    result.push_back(expr->as<Column>());
  }
  result.insert(result.end(), rhs.begin(), rhs.end());
  return result;
}
} // namespace

Unnest::Unnest(
    RelationOpPtr input,
    ExprVector replicateColumns,
    ExprVector unnestExprs,
    ColumnVector unnestedColumns)
    : RelationOp{RelType::kUnnest, std::move(input), concatColumns(replicateColumns, unnestedColumns)},
      replicateColumns{std::move(replicateColumns)},
      unnestExprs{std::move(unnestExprs)},
      unnestedColumns{std::move(unnestedColumns)} {
  cost_.inputCardinality = inputCardinality();
}

Aggregation::Aggregation(
    RelationOpPtr input,
    ExprVector groupingKeysVector,
    AggregateVector aggregatesVector,
    velox::core::AggregationNode::Step step,
    ColumnVector columns)
    : RelationOp{RelType::kAggregation, std::move(input), std::move(columns)},
      groupingKeys{std::move(groupingKeysVector)},
      aggregates{std::move(aggregatesVector)},
      step{step} {
  cost_.inputCardinality = inputCardinality();

  float cardinality = 1;
  for (auto key : groupingKeys) {
    cardinality *= key->value().cardinality;
  }

  // The estimated output is input minus the times an input is a
  // duplicate of a key already in the input. The cardinality of the
  // result is (d - d * 1 - (1 / d))^n. where d is the number of
  // potentially distinct keys and n is the number of elements in the
  // input. This approaches d as n goes to infinity. The chance of one in d
  // being unique after n values is 1 - (1/d)^n.
  auto nOut = cardinality -
      cardinality *
          std::pow(1.0F - (1.0F / cardinality), input_->resultCardinality());

  cost_.fanout = nOut / cost_.inputCardinality;
  const auto numGrouppingKeys = static_cast<float>(groupingKeys.size());
  cost_.unitCost = numGrouppingKeys * Costs::hashProbeCost(nOut);

  float rowBytes = byteSize(groupingKeys) + byteSize(aggregates);
  cost_.totalBytes = nOut * rowBytes;
}

std::string Unnest::toString(bool recursive, bool detail) const {
  std::stringstream out;
  if (recursive) {
    out << input()->toString(true, detail) << " ";
  }
  out << "unnest ";
  printCost(detail, out);
  if (detail) {
    out << "replicate columns: "
        << itemsToString(replicateColumns.data(), replicateColumns.size());
    out << ", unnest exprs: "
        << itemsToString(unnestExprs.data(), unnestExprs.size());
    out << ", unnested columns: "
        << itemsToString(unnestedColumns.data(), unnestedColumns.size())
        << std::endl;
  }
  return out.str();
}

const QGString& Aggregation::historyKey() const {
  using velox::core::AggregationNode;
  if (step == AggregationNode::Step::kPartial ||
      step == AggregationNode::Step::kIntermediate) {
    return RelationOp::historyKey();
  }
  if (!key_.empty()) {
    return key_;
  }
  std::stringstream out;
  out << input_->historyKey();
  out << " group by ";
  auto* opt = queryCtx()->optimization();
  velox::ScopedVarSetter cnames(&opt->cnamesInExpr(), false);
  std::vector<std::string> strings;
  for (auto& key : groupingKeys) {
    strings.push_back(key->toString());
  }
  std::ranges::sort(strings);
  for (auto& s : strings) {
    out << s << ", ";
  }
  key_ = sanitizeHistoryKey(out.str());
  return key_;
}

std::string Aggregation::toString(bool recursive, bool detail) const {
  std::stringstream out;
  if (recursive) {
    out << input()->toString(true, detail) << " ";
  }
  out << velox::core::AggregationNode::toName(step) << " agg";
  printCost(detail, out);
  if (detail) {
    if (groupingKeys.empty()) {
      out << "global";
    } else {
      out << itemsToString(groupingKeys.data(), groupingKeys.size());
    }
    out << aggregates.size() << " aggregates" << std::endl;
  }
  return out.str();
}

HashBuild::HashBuild(
    RelationOpPtr input,
    int32_t id,
    ExprVector keysVector,
    PlanP plan)
    : RelationOp{RelType::kHashBuild, std::move(input)},
      buildId{id},
      keys{std::move(keysVector)},
      plan{plan} {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;

  const auto numKeys = static_cast<float>(keys.size());
  const auto numColumns = static_cast<float>(columns().size());
  cost_.unitCost = numKeys * Costs::kHashColumnCost +
      Costs::hashProbeCost(cost_.inputCardinality) +
      numColumns * Costs::kHashExtractColumnCost * 2;
  cost_.totalBytes = cost_.inputCardinality * byteSize(columns());
}

std::string HashBuild::toString(bool recursive, bool detail) const {
  std::stringstream out;
  if (recursive) {
    out << input()->toString(true, detail) << " ";
  }
  out << " Build ";
  printCost(detail, out);
  return out.str();
}

Filter::Filter(RelationOpPtr input, ExprVector exprs)
    : RelationOp{RelType::kFilter, std::move(input)}, exprs_{std::move(exprs)} {
  cost_.inputCardinality = inputCardinality();
  const auto numExprs = static_cast<float>(exprs_.size());
  cost_.unitCost = Costs::kMinimumFilterCost * numExprs;

  // We assume each filter selects 4/5. Small effect makes it so
  // join and scan selectivities that are better known have more
  // influence on plan cardinality. To be filled in from history.
  cost_.fanout = std::pow(0.8F, numExprs);
}

const QGString& Filter::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  std::stringstream out;
  auto* opt = queryCtx()->optimization();
  velox::ScopedVarSetter cname(&opt->cnamesInExpr(), false);
  out << input_->historyKey() << " filter " << "(";
  std::vector<std::string> strings;
  for (auto& e : exprs_) {
    strings.push_back(e->toString());
  }
  std::ranges::sort(strings);
  for (auto& s : strings) {
    out << s << ", ";
  }
  out << ")";
  key_ = sanitizeHistoryKey(out.str());
  return key_;
}

std::string Filter::toString(bool recursive, bool detail) const {
  std::stringstream out;
  if (recursive) {
    out << input()->toString(true, detail) << " ";
  }
  if (detail) {
    out << "Filter (";
    for (auto i = 0; i < exprs_.size(); ++i) {
      out << exprs_[i]->toString();
      if (i < exprs_.size() - 1) {
        out << " and ";
      }
    }
    out << ")\n";
  } else {
    out << "filter " << exprs_.size() << " exprs ";
  }
  return out.str();
}

Project::Project(
    const RelationOpPtr& input,
    ExprVector exprs,
    const ColumnVector& columns,
    bool redundant)
    : RelationOp{RelType::kProject, input, input->distribution().rename(exprs, columns), columns},
      exprs_{std::move(exprs)},
      redundant_{redundant} {
  VELOX_CHECK_EQ(
      exprs_.size(), columns_.size(), "Projection names and exprs must match");

  if (redundant) {
    for (const auto& expr : exprs_) {
      VELOX_CHECK(
          expr->is(PlanType::kColumnExpr),
          "Redundant Project must not contain expressions: {}",
          expr->toString());
    }
  }

  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;

  // TODO Fill in cost_.unitCost and others.
}

std::string Project::toString(bool recursive, bool detail) const {
  std::stringstream out;
  if (recursive) {
    out << input()->toString(true, detail) << " ";
  }
  if (detail) {
    out << "Project (";
    for (auto i = 0; i < exprs_.size(); ++i) {
      out << columns_[i]->toString() << " = " << exprs_[i]->toString();
      if (i < exprs_.size() - 1) {
        out << ", ";
      }
    }
    out << ")\n";
  } else {
    out << "project " << exprs_.size() << " columns ";
  }
  return out.str();
}

namespace {
Distribution makeOrderByDistribution(
    const RelationOpPtr& input,
    ExprVector orderKeys,
    OrderTypeVector orderTypes) {
  Distribution distribution = input->distribution();

  distribution.distributionType = DistributionType::gather();
  distribution.partition.clear();
  distribution.orderKeys = std::move(orderKeys);
  distribution.orderTypes = std::move(orderTypes);
  VELOX_DCHECK_EQ(
      distribution.orderKeys.size(), distribution.orderTypes.size());

  return distribution;
}
} // namespace

OrderBy::OrderBy(
    const RelationOpPtr& input,
    ExprVector orderKeys,
    OrderTypeVector orderTypes,
    int64_t limit,
    int64_t offset)
    : RelationOp{RelType::kOrderBy, input, makeOrderByDistribution(input, std::move(orderKeys), std::move(orderTypes))},
      limit{limit},
      offset{offset} {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;

  // TODO Fill in cost_.unitCost and others.
}

std::string OrderBy::toString(bool recursive, bool detail) const {
  std::stringstream out;
  if (recursive) {
    out << input()->toString(true, detail) << " ";
  }

  if (detail) {
    out << "OrderBy (" << distribution_.toString() << ")\n";
  } else {
    out << "order by " << distribution_.orderKeys.size() << " columns ";
  }
  return out.str();
}

Limit::Limit(RelationOpPtr input, int64_t limit, int64_t offset)
    : RelationOp{RelType::kLimit, std::move(input), Distribution::gather()},
      limit{limit},
      offset{offset} {
  cost_.inputCardinality = inputCardinality();
  cost_.unitCost = 0.01;
  const auto cardinality = static_cast<float>(limit);
  if (cost_.inputCardinality <= cardinality) {
    // Input cardinality does not exceed the limit. The limit is no-op. Doesn't
    // change cardinality.
    cost_.fanout = 1;
  } else {
    // Input cardinality exceeds the limit. Calculate fanout to ensure that
    // fanout * limit = input-cardinality.
    cost_.fanout = cardinality / cost_.inputCardinality;
  }
}

std::string Limit::toString(bool recursive, bool detail) const {
  std::stringstream out;
  if (recursive) {
    out << input()->toString(true, detail) << " ";
  }

  if (detail) {
    out << "Limit (" << offset << ", " << limit << ")\n";
  } else {
    out << "offset " << offset << " limit " << limit << " ";
  }
  return out.str();
}

UnionAll::UnionAll(RelationOpPtrVector inputsVector)
    : RelationOp{RelType::kUnionAll, nullptr, Distribution{}, inputsVector[0]->columns()},
      inputs{std::move(inputsVector)} {
  for (auto& input : inputs) {
    cost_.inputCardinality +=
        input->cost().inputCardinality * input->cost().fanout;
  }

  cost_.fanout = 1;

  // TODO Fill in cost_.unitCost and others.
}

const QGString& UnionAll::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  std::vector<QGString> keys;
  for (const auto& in : inputs) {
    keys.push_back(in->historyKey());
  }
  std::ranges::sort(keys);
  std::stringstream out;
  out << "unionall(";
  for (const auto& key : keys) {
    out << key << ", ";
  }
  out << ")";
  key_ = sanitizeHistoryKey(out.str());
  return key_;
}

std::string UnionAll::toString(bool recursive, bool detail) const {
  std::stringstream out;
  out << "(";
  for (auto i = 0; i < inputs.size(); ++i) {
    out << inputs[i]->toString(recursive, detail);
    if (i < inputs.size() - 1) {
      if (detail) {
        out << std::endl;
      }
      out << " union all ";
      if (detail) {
        out << std::endl;
      }
    }
  }
  out << ")";
  return out.str();
}

TableWrite::TableWrite(
    RelationOpPtr input,
    ExprVector inputColumns,
    const WritePlan* write)
    : RelationOp{RelType::kTableWrite, std::move(input), Distribution::gather(), {}},
      inputColumns{std::move(inputColumns)},
      write{write} {
  cost_.inputCardinality = inputCardinality();
  cost_.unitCost = 0.01;
  VELOX_DCHECK_EQ(
      this->inputColumns.size(), this->write->table().type()->size());
}

std::string TableWrite::toString(bool recursive, bool detail) const {
  std::stringstream out;
  if (recursive) {
    out << input()->toString(true, detail) << " ";
  }

  const auto& table = write->table();
  const auto& type = *table.type();
  if (detail) {
    out << fmt::format(
        "TableWrite to {} ({} columns)", table.name(), type.size());

    out << " columns:";
    VELOX_DCHECK_LT(0, type.size(), "Table must have at least one column");
    for (uint32_t i = 0; i < type.size(); ++i) {
      out << " " << type.nameOf(i) << "=" << inputColumns[i]->toString();
      if (i < inputColumns.size() - 1) {
        out << ", ";
      }
    }

    printCost(detail, out);
  } else {
    out << fmt::format(
        "TableWrite {} columns to {}", type.size(), table.name());
  }

  return out.str();
}

} // namespace facebook::axiom::optimizer
