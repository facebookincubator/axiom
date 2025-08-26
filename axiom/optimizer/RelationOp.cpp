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

#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/expression/ScopedVarSetter.h"

namespace facebook::velox::optimizer {

void Cost::add(const Cost& other) {
  inputCardinality += other.inputCardinality;
  fanout += other.fanout;
  setupCost += other.setupCost;
}

const Value& RelationOp::value(ExprCP expr) const {
  // Compute new Value by applying restrictions from operators
  // between the place Expr is first defined and the output of
  // 'this'. Memoize the result in 'this'.
  return expr->value();
}

namespace {
template <typename T>
std::string itemsToString(const T* items, int32_t n) {
  std::stringstream out;
  for (auto i = 0; i < n; ++i) {
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
  const auto numColumns = columns.size();
  const auto rowCost = numColumns * Costs::kColumnRowCost +
      std::max<float>(0, size - 8 * numColumns) * Costs::kColumnByteCost;
  cost.unitCost += cost.fanout * rowCost;
}

float orderPrefixDistance(
    const RelationOpPtr& input,
    ColumnGroupP index,
    const ExprVector& keys) {
  float selection = 1;
  for (int32_t i = 0; i < input->distribution().orderKeys.size() &&
       i < index->distribution().orderKeys.size() && i < keys.size();
       ++i) {
    if (input->distribution().orderKeys[i]->sameOrEqual(*keys[i])) {
      selection *= index->distribution().orderKeys[i]->value().cardinality;
    }
  }
  return selection;
}

} // namespace

TableScan::TableScan(
    RelationOpPtr input,
    Distribution _distribution,
    const BaseTable* table,
    ColumnGroupP _index,
    float fanout,
    ColumnVector columns,
    ExprVector lookupKeys,
    velox::core::JoinType joinType,
    ExprVector joinFilter)
    : RelationOp(
          RelType::kTableScan,
          std::move(input),
          std::move(_distribution),
          std::move(columns)),
      baseTable(table),
      index(_index),
      keys(std::move(lookupKeys)),
      joinType(joinType),
      joinFilter(std::move(joinFilter)) {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = fanout;

  if (!keys.empty()) {
    float lookupRange(index->table->cardinality);
    float orderSelectivity = orderPrefixDistance(this->input(), index, keys);
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
    ColumnGroupP index,
    const ColumnVector& columns) {
  auto schemaColumns = transform<ColumnVector>(
      columns, [](auto& column) { return column->schemaColumn(); });

  ExprVector partition;
  ExprVector orderKeys;
  OrderTypeVector orderTypes;
  // if all partitioning columns are projected, the output is partitioned.
  if (isSubset(index->distribution().partition, schemaColumns)) {
    partition = index->distribution().partition;
    replace(partition, schemaColumns, columns.data());
  }

  auto numPrefix = prefixSize(index->distribution().orderKeys, schemaColumns);
  if (numPrefix > 0) {
    orderKeys = index->distribution().orderKeys;
    orderKeys.resize(numPrefix);
    orderTypes = index->distribution().orderTypes;
    orderTypes.resize(numPrefix);
    replace(orderKeys, schemaColumns, columns.data());
  }
  return {
      index->distribution().distributionType,
      std::move(partition),
      std::move(orderKeys),
      std::move(orderTypes),
      index->distribution().numKeysUnique <= numPrefix
          ? index->distribution().numKeysUnique
          : 0,
      1.0F / baseTable->filterSelectivity,
  };
}

// static
PlanObjectSet TableScan::availableColumns(
    const BaseTable* baseTable,
    ColumnGroupP index) {
  // The columns of base table that exist in 'index'.
  PlanObjectSet result;
  for (auto column : index->columns()) {
    for (auto baseColumn : baseTable->columns) {
      if (baseColumn->name() == column->name()) {
        result.add(baseColumn);
        break;
      }
    }
  }
  return result;
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
    out << " build= " << velox::succinctBytes(totalBytes);
  }
  if (transferBytes > 0) {
    out << " network= " << velox::succinctBytes(transferBytes);
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

QGstring sanitizeHistoryKey(std::string in) {
  for (auto i = 0; i < in.size(); ++i) {
    unsigned char c = in[i];
    if (c < 32 || c > 127 || c == '{' || c == '}' || c == '"') {
      in[i] = '?';
    }
  }
  return QGstring(in);
}

const QGstring& TableScan::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  std::stringstream out;
  out << "scan " << baseTable->schemaTable->name << "(";
  auto* opt = queryCtx()->optimization();
  ScopedVarSetter cnames(&opt->cnamesInExpr(), false);
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
  std::sort(filters.begin(), filters.end());
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

const QGstring& Values::historyKey() const {
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
    JoinMethod _method,
    velox::core::JoinType _joinType,
    RelationOpPtr input,
    RelationOpPtr _right,
    ExprVector _leftKeys,
    ExprVector rightKeys,
    ExprVector filter,
    float fanout,
    ColumnVector columns)
    : RelationOp(
          RelType::kJoin,
          input,
          input->distribution(),
          std::move(columns)),
      method(_method),
      joinType(_joinType),
      right(std::move(_right)),
      leftKeys(std::move(_leftKeys)),
      rightKeys(std::move(rightKeys)),
      filter(std::move(filter)) {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = fanout;

  float buildSize = right->cost().inputCardinality;
  auto rowCost =
      right->input()->columns().size() * Costs::kHashExtractColumnCost;
  cost_.unitCost = Costs::hashProbeCost(buildSize) + cost_.fanout * rowCost +
      leftKeys.size() * Costs::kHashColumnCost;
}

namespace {
std::pair<std::string, std::string> joinKeysString(
    const ExprVector& left,
    const ExprVector& right) {
  std::vector<int32_t> indices(left.size());
  std::iota(indices.begin(), indices.end(), 0);
  auto* opt = queryCtx()->optimization();
  ScopedVarSetter cname(&opt->cnamesInExpr(), false);
  std::vector<std::string> strings;
  for (auto& k : left) {
    strings.push_back(k->toString());
  }
  std::sort(indices.begin(), indices.end(), [&](int32_t l, int32_t r) {
    return strings[l] < strings[r];
  });
  std::stringstream leftStream;
  std::stringstream rightStream;
  for (auto i : indices) {
    leftStream << left[i]->toString() << ", ";
    rightStream << right[i]->toString() << ", ";
  }
  return std::make_pair(leftStream.str(), rightStream.str());
}
} // namespace

const QGstring& Join::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  auto& leftTree = input_->historyKey();
  auto& rightTree = right->historyKey();
  std::stringstream out;
  auto [leftText, rightText] = joinKeysString(leftKeys, rightKeys);
  if (leftTree < rightTree || joinType != core::JoinType::kInner) {
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

Aggregation::Aggregation(
    RelationOpPtr input,
    ExprVector _groupingKeys,
    AggregateVector _aggregates,
    velox::core::AggregationNode::Step step,
    ColumnVector columns)
    : RelationOp(
          RelType::kAggregation,
          input,
          input->distribution(),
          std::move(columns)),
      groupingKeys(std::move(_groupingKeys)),
      aggregates(std::move(_aggregates)),
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
      cardinality * pow(1.0 - (1.0 / cardinality), input->resultCardinality());

  cost_.fanout = nOut / cost_.inputCardinality;
  cost_.unitCost = groupingKeys.size() * Costs::hashProbeCost(nOut);

  float rowBytes = byteSize(groupingKeys) + byteSize(aggregates);
  cost_.totalBytes = nOut * rowBytes;
}

const QGstring& Aggregation::historyKey() const {
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
  ScopedVarSetter cnames(&opt->cnamesInExpr(), false);
  std::vector<std::string> strings;
  for (auto& key : groupingKeys) {
    strings.push_back(key->toString());
  }
  std::sort(strings.begin(), strings.end());
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
    ExprVector _keys,
    PlanP plan)
    : RelationOp(
          RelType::kHashBuild,
          input,
          input->distribution(),
          input->columns()),
      buildId(id),
      keys(std::move(_keys)),
      plan(plan) {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;

  cost_.unitCost = keys.size() * Costs::kHashColumnCost +
      Costs::hashProbeCost(cost_.inputCardinality) +
      this->input()->columns().size() * Costs::kHashExtractColumnCost * 2;
  cost_.totalBytes =
      cost_.inputCardinality * byteSize(this->input()->columns());
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
    : RelationOp(
          RelType::kFilter,
          input,
          input->distribution(),
          input->columns()),
      exprs_(std::move(exprs)) {
  cost_.inputCardinality = inputCardinality();
  cost_.unitCost = Costs::kMinimumFilterCost * exprs_.size();

  // We assume each filter selects 4/5. Small effect makes it so
  // join and scan selectivities that are better known have more
  // influence on plan cardinality. To be filled in from history.
  cost_.fanout = pow(0.8, exprs_.size());
}

const QGstring& Filter::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  std::stringstream out;
  auto* opt = queryCtx()->optimization();
  ScopedVarSetter cname(&opt->cnamesInExpr(), false);
  out << input_->historyKey() << " filter " << "(";
  std::vector<std::string> strings;
  for (auto& e : exprs_) {
    strings.push_back(e->toString());
  }
  std::sort(strings.begin(), strings.end());
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

Project::Project(RelationOpPtr input, ExprVector exprs, ColumnVector columns)
    : RelationOp(
          RelType::kProject,
          input,
          input->distribution().rename(exprs, columns),
          columns),
      exprs_(std::move(exprs)) {
  VELOX_CHECK_EQ(
      exprs_.size(), columns_.size(), "Projection names and exprs must match");

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
    RelationOpPtr input,
    ExprVector orderKeys,
    OrderTypeVector orderTypes,
    int64_t limit,
    int64_t offset)
    : RelationOp(
          RelType::kOrderBy,
          input,
          makeOrderByDistribution(
              input,
              std::move(orderKeys),
              std::move(orderTypes))),
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
    : RelationOp(
          RelType::kLimit,
          input,
          Distribution::gather(),
          input->columns()),
      limit{limit},
      offset{offset} {
  cost_.inputCardinality = inputCardinality();
  cost_.unitCost = 0.01;
  if (cost_.inputCardinality <= limit) {
    // Input cardinality does not exceed the limit. The limit is no-op. Doesn't
    // change cardinality.
    cost_.fanout = 1;
  } else {
    // Input cardinality exceeds the limit. Calculate fanout to ensure that
    // fanout * limit = input-cardinality.
    cost_.fanout = limit / cost_.inputCardinality;
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

UnionAll::UnionAll(RelationOpPtrVector _inputs)
    : RelationOp(
          RelType::kUnionAll,
          nullptr,
          Distribution{},
          _inputs[0]->columns()),
      inputs(std::move(_inputs)) {
  for (auto& input : inputs) {
    cost_.inputCardinality +=
        input->cost().inputCardinality * input->cost().fanout;
  }

  cost_.fanout = 1;

  // TODO Fill in cost_.unitCost and others.
}

const QGstring& UnionAll::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  std::vector<QGstring> keys;
  for (auto in : inputs) {
    keys.push_back(in->historyKey());
  }
  std::sort(keys.begin(), keys.end());
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

} // namespace facebook::velox::optimizer
