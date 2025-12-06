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
#include "axiom/optimizer/RelationOpPrinter.h"
#include "axiom/optimizer/RelationOpVisitor.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/expression/ScopedVarSetter.h"

namespace facebook::axiom::optimizer {

void PlanCost::add(RelationOp& op) {
  cost += op.cost().totalCost();
  cardinality = op.cost().resultCardinality();
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
      {RelType::kUnnest, "Unnest"},
      {RelType::kTableWrite, "TableWrite"},
  };

  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(RelType, relTypeNames)

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
    BaseTableCP table,
    ColumnGroupCP index,
    const ColumnVector& columns)
    : TableScan(
          /*input=*/nullptr,
          TableScan::outputDistribution(table, index, columns),
          table,
          index,
          /*fanout=*/index->table->cardinality * table->filterSelectivity,
          columns,
          /*lookupKeys=*/{},
          velox::core::JoinType::kInner,
          /*joinFilter=*/{}) {}

TableScan::TableScan(
    RelationOpPtr input,
    Distribution distribution,
    BaseTableCP table,
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
      1.0f / baseTable->filterSelectivity,
  };
}

std::string Cost::toString(bool /*detail*/, bool isUnit) const {
  std::stringstream out;
  float multiplier = isUnit ? 1 : inputCardinality;
  out << succinctNumber(fanout * multiplier) << " rows "
      << succinctNumber(unitCost * multiplier) << "CU";

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

void RelationOp::checkInputCardinality() const {
  if (input_ != nullptr) {
    const auto inputCardinality = input_->resultCardinality();
    VELOX_CHECK(std::isfinite(inputCardinality));

    // TODO Assert that inputCardinality > 0.
    VELOX_CHECK_GE(inputCardinality, 0);
  }
}

std::string RelationOp::toString() const {
  return RelationOpPrinter::toText(*this);
}

std::string RelationOp::toOneline() const {
  return RelationOpPrinter::toOneline(*this);
}

void RelationOp::printCost(bool detail, std::stringstream& out) const {
  auto ctx = queryCtx();
  if (ctx && ctx->contextPlan()) {
    auto planCost = ctx->contextPlan()->cost.cost;
    auto pct = 100 * cost_.totalCost() / planCost;
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
  out << "scan " << baseTable->schemaTable->name() << "(";
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
  out << baseTable->schemaTable->name() << " " << baseTable->cname;
  if (detail) {
    printCost(detail, out);
    if (!input()) {
      out << distribution_.toString() << std::endl;
    }
  }
  return out.str();
}

void TableScan::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
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
  key_ = sanitizeHistoryKey("values");
  return key_;
}

std::string Values::toString(bool /*recursive*/, bool detail) const {
  VELOX_DCHECK(!input());
  std::stringstream out;
  out << valuesTable.cname;
  if (detail) {
    printCost(detail, out);
    out << distribution_.toString() << std::endl;
  }
  return out.str();
}

void Values::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

namespace {

const auto& joinMethodNames() {
  static const folly::F14FastMap<JoinMethod, std::string_view> kNames = {
      {JoinMethod::kHash, "Hash"},
      {JoinMethod::kMerge, "Merge"},
      {JoinMethod::kCross, "Cross"},
  };

  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(JoinMethod, joinMethodNames);

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

  const float buildSize = right->resultCardinality();
  const auto numKeys = leftKeys.size();
  const auto probeCost = Costs::hashTableCost(buildSize) +
      // Multiply by min(fanout, 1) because most misses will not compare and if
      // fanout > 1, there is still only one compare.
      (Costs::kKeyCompareCost * numKeys * std::min<float>(1, fanout)) +
      numKeys * Costs::kHashColumnCost;

  const auto rowBytes = byteSize(right->input()->columns());
  const auto rowCost = Costs::hashRowCost(buildSize, rowBytes);

  cost_.unitCost = probeCost + cost_.fanout * rowCost;
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
  if (recursive) {
    out << " (" << right->toString(true, detail) << ")";
    if (detail) {
      out << std::endl;
    }
  }
  return out.str();
}

void Join::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
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

  if (distribution().isBroadcast) {
    out << "broadcast ";
  } else if (distribution().isGather()) {
    out << "gather ";
  } else {
    out << "repartition ";
    if (detail) {
      out << distribution().toString() << " ";
    }
  }
  if (detail) {
    printCost(detail, out);
  }
  return out.str();
}

void Repartition::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
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

namespace {
double partialFlushInterval(
    double totalInput,
    double numDistinct,
    double maxDistinct) {
  // Handle edge cases.
  if (maxDistinct >= numDistinct) {
    return totalInput;
  }
  VELOX_CHECK_GT(maxDistinct, 0);

  // The expected number of samples to see k out of n distinct values
  // follows from the coupon collector problem:
  // E[k] = n * (1/n + 1/(n-1) + ... + 1/(n-k+1))

  const auto n = numDistinct;
  const auto k = maxDistinct;

  // Approximate the partial harmonic sum using logarithms (constant time):
  // H(n,k) = Σ(i=0 to k-1) 1/(n-i) ≈ ln(n) - ln(n-k) = ln(n/(n-k))
  // This uses the integral approximation: ∫_{n-k}^n 1/x dx
  const double harmonicSum = std::log(n / (n - k));

  // Expected number of samples in a uniform distribution.
  const double expectedSamples = n * harmonicSum;

  // Scale by the ratio of total input to distinct values to account for
  // non-uniform distribution.
  const double scalingFactor = totalInput / numDistinct;

  return expectedSamples * scalingFactor;
}

// Predicts the number of distinct values expected after sampling numRows items
// from a population with numDistinct distinct values.
// @param numRows The count of samples that are initially seen.
// @param numDistinct The count of distinct values in the full population.
// @return predicted number of distinct values seen after numRows inputs
double expectedNumDistincts(double numRows, double numDistinct) {
  if (numDistinct <= 0 || numRows <= 0) {
    // Avoid /0 downstream. Less than one distinct value does not make sense for
    // aggregation.
    return 1.0f;
  }

  // Using the coupon collector formula.
  // Expected distinct values = d * (1 - (1 - 1/d)^n)
  // , where d is total distinct values and n is number of samples. The
  // number of distinct rows cannot be more than the number of input rows.
  return numDistinct * (1.0 - std::pow(1.0 - (1.0 / numDistinct), numRows));
}

// Computes a saturating product using rational saturation function. The result
// behaves like multiplication when far from max, but asymptotically approaches
// max as the product increases.
//
// Formula: max * P / (max + P), where P is the product of all numbers.
double saturatingProduct(double max, std::span<double> numbers) {
  // Compute the product of all numbers.
  double product = 1.0;
  for (auto n : numbers) {
    product *= n;
  }

  // Apply rational saturation function: max * P / (max + P).
  return max * product / (max + product);
}

// Returns a table with max cardinality > 0. Returns nullptr if all tables have
// cardinality of zero.
PlanObjectCP largestTable(const PlanObjectSet& tables) {
  PlanObjectCP largestTable = nullptr;
  double maxCardinality = 0.0;

  tables.forEach([&](const auto& table) {
    double cardinality = tableCardinality(table);
    if (cardinality > maxCardinality) {
      maxCardinality = cardinality;
      largestTable = table;
    }
  });

  return largestTable;
}

// Computes the maximum cardinality estimate for aggregation grouping keys
// using saturating product to avoid overflow. When multiple keys come from the
// same table, cap the maximum cardinality estimate at table's cardinality.
double maxGroups(const ExprVector& groupingKeys) {
  if (groupingKeys.empty()) {
    return 1.0;
  }

  // Map from table to the keys that originate from that table.
  folly::F14FastMap<PlanObjectCP, std::vector<ExprCP>> tableToKeys;

  // For each grouping key, find its largest table of origin.
  for (auto* key : groupingKeys) {
    const auto allTables = key->allTables();
    if (allTables.empty()) {
      // Key doesn't depend on any table (e.g., constant), skip.
      continue;
    }

    if (auto table = largestTable(allTables)) {
      tableToKeys[table].push_back(key);
    }
  }

  // Calculate cardinality estimate for each table group.
  std::vector<double> groupCardinalities;
  groupCardinalities.reserve(tableToKeys.size());

  double maxTableCardinality = 0.0;

  for (const auto& [table, keys] : tableToKeys) {
    const double maxCardinality = tableCardinality(table);

    maxTableCardinality = std::max(maxTableCardinality, maxCardinality);

    double groupCardinality;
    if (keys.size() == 1) {
      // Single key per table: use min of key cardinality and table cardinality.
      groupCardinality =
          std::min<double>(keys[0]->value().cardinality, maxCardinality);
    } else {
      // Multiple keys: collect cardinalities and use saturatingProduct.
      std::vector<double> keyCardinalities;
      keyCardinalities.reserve(keys.size());
      for (auto key : keys) {
        keyCardinalities.push_back(key->value().cardinality);
      }
      groupCardinality = saturatingProduct(maxCardinality, keyCardinalities);
    }

    groupCardinalities.push_back(groupCardinality);
  }

  if (groupCardinalities.size() == 1) {
    return groupCardinalities[0];
  }

  // Combine cardinalities from multiple tables using saturatingProduct.
  double combinedMax = std::max<double>(3.0 * maxTableCardinality, 1e10);
  return saturatingProduct(combinedMax, groupCardinalities);
}
} // namespace

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

  const auto numKeys = groupingKeys.size();
  if (numKeys > 0) {
    // Input cardinality before the partial aggregation.
    float inputBeforePartial;
    if (step == velox::core::AggregationNode::Step::kFinal &&
        input_->is(RelType::kRepartition) &&
        input_->input()->is(RelType::kAggregation)) {
      auto partial = input_->input().get();

      VELOX_CHECK(!std::isnan(partial->cost().inputCardinality));
      VELOX_CHECK(std::isfinite(partial->cost().inputCardinality));

      inputBeforePartial = partial->inputCardinality();
    } else {
      inputBeforePartial = cost_.inputCardinality;
    }

    setCostWithGroups(inputBeforePartial);
  } else {
    // Global aggregation (no grouping keys).
    cost_.unitCost = aggregates.size() * Costs::kSimpleAggregateCost;

    // Avoid division by zero.
    cost_.fanout = 1.0f / cost_.inputCardinality;
  }

  VELOX_CHECK_LE(cost_.fanout, 1.0f);
}

namespace {
// The cost includes:
// - Compute the hash of the grouping keys := Costs::kHashColumnCost * numKeys
// - Lookup the hash in the hash table := Costs::hashTableCost(nOut)
// - Compare the keys := kKeyCompareCost * numKeys
// - Access the row of the hash table (twice) := 2 * Costs::hashRowCost(nOut,
// rowBytes)
// - Update accumulators := aggregates.size() * Costs::kSimpleAggregateCost
float aggregationCost(
    size_t numKeys,
    size_t numAggregates,
    float rowBytes,
    float numGroups) {
  return Costs::kHashColumnCost * numKeys + Costs::hashTableCost(numGroups) +
      Costs::kKeyCompareCost * numKeys +
      numAggregates * Costs::kSimpleAggregateCost +
      2 * Costs::hashRowCost(numGroups, rowBytes);
}
} // namespace

void Aggregation::setCostWithGroups(int64_t inputBeforePartial) {
  auto* optimization = queryCtx()->optimization();
  const auto& runnerOptions = optimization->runnerOptions();

  const auto maxCardinality = std::max<double>(1, maxGroups(groupingKeys));

  const auto numGroups =
      expectedNumDistincts(inputBeforePartial, maxCardinality);

  const auto numKeys = groupingKeys.size();
  const auto rowBytes =
      byteSize(groupingKeys) + byteSize(aggregates) + Costs::kHashRowBytes;

  if (step != velox::core::AggregationNode::Step::kPartial) {
    float localExchangeCost = 0;
    if (runnerOptions.numDrivers > 1) {
      // If more than one driver per fragment, a non-partial group by needs a
      // local exchange. Estimated to be 1/3 of a remote shuffle.
      localExchangeCost = shuffleCost(input_->columns()) / 3;
    }

    // Aggregation in one step, no estimate of reduction from partial.
    cost_.unitCost =
        aggregationCost(numKeys, aggregates.size(), rowBytes, numGroups) +
        localExchangeCost;

    // numGroups can be > inputCardinality since this is calculated against the
    // input before partial and inputCardinality is scaled down by partial
    // reduction.
    cost_.fanout =
        std::min<double>(inputCardinality(), numGroups) / inputCardinality();
    cost_.totalBytes = numGroups * rowBytes;
    return;
  }

  const auto& veloxQueryConfig = optimization->veloxQueryCtx()->queryConfig();
  const float maxPartialMemory =
      veloxQueryConfig.maxPartialAggregationMemoryUsage();
  const float abandonPartialMinRows =
      veloxQueryConfig.abandonPartialAggregationMinRows();
  const float abandonPartialMinFraction =
      veloxQueryConfig.abandonPartialAggregationMinPct() / 100.0;

  const auto partialCapacity =
      std::min<double>(numGroups, maxPartialMemory / rowBytes);

  // The number of distinct keys we expect to see in the initial sample before
  // we consider abandoning partial aggregation.
  const auto initialDistincts =
      expectedNumDistincts(abandonPartialMinRows, maxCardinality);

  // The number of input rows expected for each flush of partial aggregation.
  const auto partialInputBetweenFlushes =
      partialFlushInterval(inputBeforePartial, partialCapacity, maxCardinality);

  // Partial cannot reduce more than the expected total reduction. Partial
  // reduction can be overestimated when input is a fraction of possible values
  // and partial capacity is set to be no greater than input.
  auto partialFanout = std::max<double>(
      numGroups / inputBeforePartial,
      partialCapacity / partialInputBetweenFlushes);

  const auto width = runnerOptions.numWorkers * runnerOptions.numDrivers;
  if ((inputBeforePartial > abandonPartialMinRows * width &&
       initialDistincts > abandonPartialMinRows * abandonPartialMinFraction) ||
      (inputBeforePartial > numGroups * 5 &&
       partialFanout > abandonPartialMinFraction)) {
    // Partial agg does not reduce.
    partialFanout = 1;
  }

  cost_.fanout = partialFanout;
  if (partialFanout == 1) {
    cost_.unitCost = 0.1 * rowBytes;
    cost_.totalBytes = initialDistincts * rowBytes;
  } else {
    cost_.unitCost =
        aggregationCost(numKeys, aggregates.size(), rowBytes, partialCapacity);
    cost_.totalBytes = partialCapacity * rowBytes;
  }
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

void Unnest::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
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

void Aggregation::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

HashBuild::HashBuild(RelationOpPtr input, ExprVector keysVector)
    : RelationOp{RelType::kHashBuild, std::move(input)},
      keys{std::move(keysVector)} {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;

  const auto numKeys = static_cast<float>(keys.size());
  const auto rowBytes = byteSize(columns());
  const auto numColumns = static_cast<float>(columns().size());
  // Per row cost calculates the column hashes twice, once to partition and a
  // second time to insert.
  cost_.unitCost = (numKeys * 2 * Costs::kHashColumnCost) +
      Costs::hashBuildCost(cost_.inputCardinality, rowBytes) +
      numKeys * Costs::kKeyCompareCost +
      numColumns * Costs::kHashExtractColumnCost * 2;

  cost_.totalBytes = cost_.inputCardinality * rowBytes;
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

void HashBuild::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
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

void Filter::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
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

void Project::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
bool Project::isRedundant(
    const RelationOpPtr& input,
    const ExprVector& exprs,
    const ColumnVector& columns) {
  const auto& inputColumns = input->columns();

  if (inputColumns.size() != exprs.size()) {
    return false;
  }

  for (auto i = 0; i < inputColumns.size(); ++i) {
    if (inputColumns[i] != exprs[i]) {
      return false;
    }

    if (inputColumns[i]->outputName() != columns[i]->outputName()) {
      return false;
    }
  }

  return true;
}

OrderBy::OrderBy(
    RelationOpPtr input,
    ExprVector orderKeys,
    OrderTypeVector orderTypes,
    int64_t limit,
    int64_t offset)
    : RelationOp{RelType::kOrderBy, std::move(input), Distribution::gather(std::move(orderKeys), std::move(orderTypes))},
      limit{limit},
      offset{offset} {
  cost_.inputCardinality = inputCardinality();
  if (limit == -1) {
    cost_.fanout = 1;
  } else {
    const auto cardinality = static_cast<float>(limit);
    if (cost_.inputCardinality <= cardinality) {
      // Input cardinality does not exceed the limit. The limit is no-op.
      // Doesn't change cardinality.
      cost_.fanout = 1;
    } else {
      // Input cardinality exceeds the limit. Calculate fanout to ensure that
      // fanout * limit = input-cardinality.
      cost_.fanout = cardinality / cost_.inputCardinality;
    }
  }

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

void OrderBy::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
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

void Limit::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
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

void UnionAll::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

// TODO Figure out a cleaner solution to setting 'distribution' and 'columns'.
TableWrite::TableWrite(
    RelationOpPtr input,
    ExprVector inputColumns,
    const WritePlan* write)
    : RelationOp{RelType::kTableWrite, input, input->distribution().isGather() ? Distribution::gather() : Distribution(), {}},
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

void TableWrite::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

} // namespace facebook::axiom::optimizer
