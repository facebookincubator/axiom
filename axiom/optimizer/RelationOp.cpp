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

#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/JoinConstraints.h"
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
  cost = optimizer::add(cost, op.cost().totalCost());
  cardinality = op.resultCardinality();
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
      {RelType::kEnforceSingleRow, "EnforceSingleRow"},
      {RelType::kAssignUniqueId, "AssignUniqueId"},
      {RelType::kEnforceDistinct, "EnforceDistinct"},
      {RelType::kWindow, "Window"},
      {RelType::kRowNumber, "RowNumber"},
      {RelType::kTopNRowNumber, "TopNRowNumber"},
      {RelType::kMarkDistinct, "MarkDistinct"},
      {RelType::kGroupId, "GroupId"},
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
    std::optional<float> cardinality,
    const ColumnVector& columns,
    Cost& cost) {
  cost.fanout = cardinality;

  const auto size = byteSize(columns);
  const auto numColumns = static_cast<float>(columns.size());
  const auto rowCost = numColumns * Costs::kColumnRowCost +
      std::max<float>(0, size - 8 * numColumns) * Costs::kColumnByteCost;
  // An unknown cardinality makes the unit cost unknown.
  cost.unitCost = mul(cost.fanout, rowCost);
}

std::optional<float> orderPrefixDistance(
    const RelationOpPtr& input,
    ColumnGroupCP index,
    const ExprVector& keys) {
  const auto& orderKeys = index->distribution.orderKeys();
  std::optional<float> selection = 1;
  for (int32_t i = 0; i < input->distribution().orderKeys().size() &&
       i < orderKeys.size() && i < keys.size();
       ++i) {
    if (input->distribution().orderKeys()[i]->sameOrEqual(*keys[i])) {
      selection = mul(selection, orderKeys[i]->value().cardinality);
    }
  }
  return selection;
}

bool isCoordinatorOnlyScan(BaseTableCP baseTable) {
  auto* optimization = queryCtx()->optimization();
  if (optimization == nullptr) {
    return false;
  }

  const auto& systemConnectorId = optimization->options().systemConnectorId;
  return !systemConnectorId.empty() &&
      baseTable->schemaTable->connectorId() == systemConnectorId;
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
          /*fanout=*/table->filteredCardinality,
          columns,
          /*lookupKeys=*/{},
          velox::core::JoinType::kInner,
          /*joinFilter=*/{}) {}

TableScan::TableScan(
    RelationOpPtr input,
    Distribution distribution,
    BaseTableCP table,
    ColumnGroupCP index,
    std::optional<float> fanout,
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

  // Initialize constraints from base table columns. If the table has filters,
  // column values already reflect narrowed cardinality and min/max from filter
  // selectivity computation (see setBaseTableValues in VeloxHistory.cpp).
  for (auto* column : columns_) {
    constraints_.emplace(column->id(), column->value());
  }

  if (!keys.empty()) {
    const auto orderSelectivityOpt = orderPrefixDistance(input_, index, keys);
    // The lookup unit cost is unknown if the input cardinality (batch size),
    // the order selectivity, or the table cardinality could not be estimated.
    if (cost_.inputCardinality.has_value() && orderSelectivityOpt.has_value() &&
        index->table->cardinality.has_value()) {
      float lookupRange(*index->table->cardinality);
      float orderSelectivity = *orderSelectivityOpt;
      auto distance = lookupRange / std::max<float>(1, orderSelectivity);
      float batchSize = std::min<float>(*cost_.inputCardinality, 10000);
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
    }
    return;
  }

  const auto cardinality = baseTable->filteredCardinality;
  updateLeafCost(cardinality, columns_, cost_);

  // Cap column cardinalities to the output row count. Skip when the output
  // cardinality is unknown so a known NDV is not wiped out.
  const std::optional<float> outputCardinality = resultCardinality();
  if (outputCardinality.has_value()) {
    for (auto& [id, constraint] : constraints_) {
      constraint.cardinality =
          minOf(constraint.cardinality, *outputCardinality);
    }
  }
}

// static
Distribution TableScan::outputDistribution(
    const BaseTable* baseTable,
    ColumnGroupCP index,
    const ColumnVector& columns) {
  if (isCoordinatorOnlyScan(baseTable)) {
    return Distribution::gather();
  }

  // Re-express the index's distribution over the scan's output columns. At scan
  // time no equivalence classes exist, so rename matches schema columns to
  // output columns by identity.
  auto schemaColumns = transform<ExprVector>(
      columns, [](auto& column) { return column->schemaColumn(); });

  return index->distribution.rename(schemaColumns, columns);
}

namespace {
std::string formatNumber(std::optional<float> value) {
  return value.has_value() ? succinctNumber(*value) : "?";
}

std::string formatBytes(std::optional<float> value) {
  return value.has_value() ? velox::succinctBytes(static_cast<uint64_t>(*value))
                           : "?";
}
} // namespace

std::string Cost::toString(bool /*detail*/, bool isUnit) const {
  std::stringstream out;
  const std::optional<float> multiplier =
      isUnit ? std::optional<float>(1) : inputCardinality;
  out << formatNumber(mul(fanout, multiplier)) << " rows "
      << formatNumber(mul(unitCost, multiplier)) << "CU";

  // Show byte volumes when unknown or known-positive; skip a known zero.
  if (!totalBytes.has_value() || *totalBytes > 0) {
    out << " build= " << formatBytes(totalBytes);
  }
  if (!transferBytes.has_value() || *transferBytes > 0) {
    out << " network= " << formatBytes(transferBytes);
  }
  return out.str();
}

std::string PlanCost::toString() const {
  const auto format = [](std::optional<float> value) -> std::string {
    return value.has_value()
        ? fmt::format(std::locale("en_US.UTF-8"), "{:L}", *value)
        : "?";
  };
  return fmt::format(
      "cost: {}, cardinality: {}", format(cost), format(cardinality));
}

void RelationOp::checkInputCardinality() const {
  if (input_ != nullptr) {
    const auto inputCardinality = input_->resultCardinality();
    // Only validate when the cardinality is known; an unknown estimate
    // legitimately propagates as nullopt.
    if (inputCardinality.has_value()) {
      VELOX_CHECK(std::isfinite(*inputCardinality));

      // TODO Assert that inputCardinality > 0.
      VELOX_CHECK_GE(*inputCardinality, 0);
    }
  }
}

void RelationOp::checkDistribution() const {
  const auto kind = distribution_.kind();
  if (relType_ == RelType::kRepartition) {
    // A Repartition must specify what kind of exchange it emits.
    VELOX_CHECK_NE(
        kind,
        Distribution::Kind::kUnspecified,
        "Repartition requires a specific distribution kind");
  } else {
    // Broadcast and arbitrary describe consumer-side exchange semantics; they
    // only exist on a Repartition. Distribution::rename drops them when
    // producing a Distribution for a non-Repartition consumer.
    VELOX_CHECK(
        kind != Distribution::Kind::kBroadcast &&
            kind != Distribution::Kind::kArbitrary,
        "Distribution kind {} is only valid on Repartition, got {}",
        kind,
        relTypeName());
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
    const auto planCost = ctx->contextPlan()->cost.cost;
    const auto pct = divide(mul(cost_.totalCost(), 100), planCost);
    out << " " << formatNumber(pct) << "% ";
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

  // Initialize constraints from column values.
  for (auto* column : columns_) {
    constraints_.emplace(column->id(), column->value());
  }
}

const QGString& Values::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  key_ = sanitizeHistoryKey("values");
  return key_;
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

namespace {
// Adjusts raw fanout for join-type semantics.
// Join edge: a -- b. Left is a. Right is b.
// @param fanout For each row in 'a' there are so many matches in 'b'.
// @param rlFanout For each row in 'b' there are so many matches in 'a'.
// @param rightToLeftRatio |b| / |a|
float adjustFanoutForJoinType(
    velox::core::JoinType joinType,
    float fanout,
    float rlFanout,
    float rightToLeftRatio) {
  switch (joinType) {
    case velox::core::JoinType::kInner:
      return fanout;
    case velox::core::JoinType::kLeft:
      return std::max<float>(1, fanout);
    case velox::core::JoinType::kRight:
      return std::max<float>(1, rlFanout) * rightToLeftRatio;
    case velox::core::JoinType::kFull:
      return std::max<float>(std::max<float>(1, fanout), rightToLeftRatio);
    case velox::core::JoinType::kLeftSemiProject:
      return 1;
    case velox::core::JoinType::kLeftSemiFilter:
      [[fallthrough]];
    case velox::core::JoinType::kCountingLeftSemiFilter:
      return std::min<float>(1, fanout);
    case velox::core::JoinType::kRightSemiProject:
      return rightToLeftRatio;
    case velox::core::JoinType::kRightSemiFilter:
      return std::min<float>(1, rlFanout) * rightToLeftRatio;
    case velox::core::JoinType::kAnti:
      [[fallthrough]];
    case velox::core::JoinType::kCountingAnti:
      return std::max<float>(0, 1 - fanout);
    default:
      VELOX_UNREACHABLE();
  }
}
} // namespace

Join::Join(
    JoinMethod method,
    velox::core::JoinType joinType,
    bool nullAware,
    bool nullAsValue,
    RelationOpPtr lhs,
    RelationOpPtr rhs,
    ExprVector lhsKeys,
    ExprVector rhsKeys,
    ExprVector filterExprs,
    std::optional<float> fanout,
    std::optional<float> rlFanout,
    ColumnVector columns)
    : RelationOp{RelType::kJoin, std::move(lhs), std::move(columns)},
      method{method},
      joinType{joinType},
      nullAware{nullAware},
      nullAsValue{nullAsValue},
      right{std::move(rhs)},
      leftKeys{std::move(lhsKeys)},
      rightKeys{std::move(rhsKeys)},
      filter{std::move(filterExprs)} {
  VELOX_DCHECK_EQ(leftKeys.size(), rightKeys.size());
#ifndef NDEBUG
  for (const auto* key : leftKeys) {
    VELOX_DCHECK(key->is(PlanType::kColumnExpr));
  }
  for (const auto* key : rightKeys) {
    VELOX_DCHECK(key->is(PlanType::kColumnExpr));
  }
#endif
  std::optional<float> filterSelectivity = computeFilterSelectivity();
  initConstraints(fanout, rlFanout, filterSelectivity);
  initCost(fanout, rlFanout, filterSelectivity);
}

std::optional<float> Join::computeFilterSelectivity() const {
  if (filter.empty()) {
    return 1.0f;
  }
  ConstraintMap inputConstraints = input_->constraints();
  for (const auto& [columnId, constraint] : right->constraints()) {
    inputConstraints.emplace(columnId, constraint);
  }
  const auto selectivity =
      conjunctsSelectivity(inputConstraints, filter, false);
  if (!selectivity.has_value()) {
    return std::nullopt;
  }
  return selectivity->trueFraction;
}

void Join::initConstraints(
    std::optional<float> fanout,
    std::optional<float> rlFanout,
    std::optional<float> filterSelectivity) {
  // Add constraints for output columns only.
  auto outputColumnIds = PlanObjectSet::fromObjects(columns_);
  for (const auto& [columnId, constraint] : input_->constraints()) {
    if (outputColumnIds.contains(columnId)) {
      constraints_.emplace(columnId, constraint);
    }
  }
  for (const auto& [columnId, constraint] : right->constraints()) {
    if (outputColumnIds.contains(columnId)) {
      constraints_.emplace(columnId, constraint);
    }
  }

  if (joinType == velox::core::JoinType::kAnti ||
      joinType == velox::core::JoinType::kCountingAnti) {
    // Without a known fanout or filter selectivity the anti-join selectivity is
    // unknown, so key/payload NDV refinements are skipped and constraints flow
    // through.
    if (fanout.has_value() && filterSelectivity.has_value()) {
      const float antiSelectivity =
          std::max(0.0f, 1.0f - *fanout * *filterSelectivity);
      JoinConstraints::updateAntiKeys(
          leftKeys, rightKeys, antiSelectivity, constraints_);
      JoinConstraints::scalePayloadCardinality(
          input_->columns(),
          leftKeys,
          antiSelectivity,
          inputCardinality(),
          constraints_);
    }
    return;
  }

  auto [leftOptional, rightOptional] = JoinConstraints::optionality(joinType);

  JoinConstraints::updateKeys(
      leftKeys, rightKeys, leftOptional, rightOptional, constraints_);

  if (joinType == velox::core::JoinType::kLeftSemiProject) {
    JoinConstraints::addMark(columns_, fanout, filterSelectivity, constraints_);
  }

  // Update constraints for optional-side payload columns.
  if (leftOptional) {
    const std::optional<float> leftNullFraction =
        JoinConstraints::computeNullFraction(leftKeys, rightKeys, constraints_);
    JoinConstraints::updatePayload(
        input_->columns(), leftKeys, leftNullFraction, constraints_);
  }
  if (rightOptional) {
    const std::optional<float> rightNullFraction =
        JoinConstraints::computeNullFraction(rightKeys, leftKeys, constraints_);
    JoinConstraints::updatePayload(
        right->columns(), rightKeys, rightNullFraction, constraints_);
  }

  // Scale non-key payload cardinalities when the join eliminates rows.
  // The preserved side of outer joins keeps all rows, so its NDV is unchanged.
  // An unknown filter selectivity leaves payload NDVs unscaled.
  const bool scaleLeft = leftOptional || isInnerJoin(joinType) ||
      isLeftSemiFilterJoin(joinType) || isCountingLeftSemiFilterJoin(joinType);
  if (scaleLeft && fanout.has_value() && filterSelectivity.has_value()) {
    const float leftSelectivity = std::min(1.0f, *fanout) * *filterSelectivity;
    JoinConstraints::scalePayloadCardinality(
        input_->columns(),
        leftKeys,
        leftSelectivity,
        inputCardinality(),
        constraints_);
  }

  const bool scaleRight =
      rightOptional || isInnerJoin(joinType) || isRightSemiFilterJoin(joinType);
  if (scaleRight && rlFanout.has_value() && filterSelectivity.has_value()) {
    const float rightSelectivity =
        std::min(1.0f, *rlFanout) * *filterSelectivity;
    JoinConstraints::scalePayloadCardinality(
        right->columns(),
        rightKeys,
        rightSelectivity,
        right->resultCardinality(),
        constraints_);
  }
}

void Join::initCost(
    std::optional<float> fanout,
    std::optional<float> rlFanout,
    std::optional<float> filterSelectivity) {
  cost_.inputCardinality = inputCardinality();
  const auto buildSizeOpt = right->resultCardinality();
  // The right-to-left ratio is unknown if either the build cardinality or the
  // input (probe) cardinality is unknown.
  const auto rightToLeftRatio = divide(buildSizeOpt, cost_.inputCardinality);

  // The output fanout is unknown if the raw fanouts, filter selectivity, or the
  // right-to-left ratio is unknown.
  const auto adjustedFanout = mul(fanout, filterSelectivity);
  const auto adjustedRlFanout = mul(rlFanout, filterSelectivity);
  if (adjustedFanout.has_value() && adjustedRlFanout.has_value() &&
      rightToLeftRatio.has_value()) {
    cost_.fanout = adjustFanoutForJoinType(
        joinType, *adjustedFanout, *adjustedRlFanout, *rightToLeftRatio);
  }

  const auto numKeys = leftKeys.size();
  const auto rowBytes = byteSize(right->columns());

  // The unit cost depends on the build cardinality and the output fanout; it is
  // unknown if either is unknown.
  if (buildSizeOpt.has_value() && cost_.fanout.has_value()) {
    const float buildSize = *buildSizeOpt;
    const float fanoutValue = *cost_.fanout;
    const auto rowCost = Costs::hashRowCost(buildSize, rowBytes);

    // For NLJ (kCross), there's no hash table to probe, so the
    // hashTableCost term — which models per-left-row bucket lookup — must
    // not be charged.
    const float probeCost = method == JoinMethod::kCross
        ? 0.0f
        : Costs::hashTableCost(buildSize) +
            // Multiply by min(fanout, 1) because most misses will not compare
            // and if fanout > 1, there is still only one compare.
            (Costs::kKeyCompareCost * numKeys *
             std::min<float>(1, fanoutValue)) +
            numKeys * Costs::kHashColumnCost;

    cost_.unitCost = probeCost + fanoutValue * rowCost;
  }
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
    velox::core::JoinType joinType,
    ExprVector filter,
    ColumnVector columns) {
  std::optional<float> fanout = right->resultCardinality();
  std::optional<float> rlFanout = input->resultCardinality();
  return make<Join>(
      JoinMethod::kCross,
      joinType,
      /*nullAware=*/false,
      /*nullAsValue=*/false,
      std::move(input),
      std::move(right),
      ExprVector{},
      ExprVector{},
      std::move(filter),
      fanout,
      rlFanout,
      std::move(columns));
}

void Join::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

Repartition::Repartition(
    RelationOpPtr input,
    Distribution distribution,
    ColumnVector columns,
    bool replicateNullsAndAny)
    : RelationOp(
          RelType::kRepartition,
          std::move(input),
          std::move(distribution),
          std::move(columns)),
      replicateNullsAndAny_{replicateNullsAndAny} {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;

  auto unitCost = shuffleCost(columns_);
  auto rowBytes = byteSize(columns_);

  // A broadcast replicates the full input to every worker, so its shuffle CPU
  // and transfer costs scale with the number of workers. 'this->' disambiguates
  // the accessor from the constructor parameter 'distribution'.
  if (this->distribution().isBroadcast()) {
    const auto numWorkers =
        queryCtx()->optimization()->runnerOptions().numWorkers;
    unitCost *= numWorkers;
    rowBytes *= numWorkers;
  }

  cost_.unitCost = unitCost;
  cost_.transferBytes = mul(cost_.inputCardinality, rowBytes);

  // Repartition projects all input columns.
  constraints_ = input_->constraints();
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
    UnnestTableCP unnestTable)
    : RelationOp{RelType::kUnnest, std::move(input), concatColumns(replicateColumns, unnestTable->columns)},
      replicateColumns{std::move(replicateColumns)},
      unnestExprs{std::move(unnestExprs)},
      unnestTable{unnestTable} {
  cost_.inputCardinality = inputCardinality();

  // Use a heuristic for average array/map size.
  // TODO Compute fanout from unnest expression array size statistics when
  // available.
  cost_.fanout = 10;
  cost_.unitCost = 0;

  initConstraints();
}

void Unnest::initConstraints() {
  const auto& inputConstraints = input_->constraints();

  // Add constraints for replicate columns.
  // Cardinality remains the same (same distinct values, just repeated).
  for (const auto& expr : replicateColumns) {
    auto* column = expr->as<Column>();
    auto it = inputConstraints.find(column->id());
    if (it != inputConstraints.end()) {
      constraints_.emplace(column->id(), it->second);
    }
  }

  // Add constraints for the unnest table's output columns. Unnested values
  // come from array/map elements (no detailed stats — use the column's
  // default), and the ordinality column is non-null with fanout as a
  // cardinality approximation.
  for (auto* column : unnestTable->columns) {
    if (column == unnestTable->ordinalityColumn) {
      // Unnest always sets a fanout.
      Value ordValue(column->value().type, cost_.fanout.value());
      ordValue.nullable = false;
      constraints_.emplace(column->id(), ordValue);
    } else {
      constraints_.emplace(column->id(), column->value());
    }
  }
}

void Unnest::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
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

// Predicts the number of distinct values expected after keeping a fraction of
// rows. Uses the coupon collector formula via expectedNumDistincts.
double sampledNdv(double ndv, double numRows, double fraction) {
  if (fraction >= 1.0) {
    return ndv;
  }
  return expectedNumDistincts(numRows * fraction, ndv);
}

// Scales NDV of all columns in 'constraints' using the coupon collector
// formula. Models the effect of keeping a fraction of rows on the number of
// distinct values seen.
void scaleCardinalities(
    ConstraintMap& constraints,
    std::optional<float> inputCardinality,
    std::optional<float> fanout) {
  // Without a known input cardinality and fanout the scaled NDV cannot be
  // estimated; leave cardinalities unchanged.
  if (!inputCardinality.has_value() || !fanout.has_value() || *fanout >= 1.0) {
    return;
  }
  for (auto& [columnId, constraint] : constraints) {
    // Per-column NDV that is itself unknown stays unknown.
    if (constraint.cardinality.has_value()) {
      constraint.cardinality = std::max(
          1.0, sampledNdv(*constraint.cardinality, *inputCardinality, *fanout));
    }
  }
}

// Computes a saturating product using rational saturation function. The
// result behaves like multiplication when far from max, but asymptotically
// approaches max as the product increases.
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

// Returns a table with max cardinality > 0. Returns nullptr if all tables
// have cardinality of zero.
PlanObjectCP largestTable(const PlanObjectSet& tables) {
  PlanObjectCP largestTable = nullptr;
  double maxCardinality = 0.0;

  tables.forEach([&](const auto& table) {
    const auto cardinality = table->template as<TableObject>()->cardinality();
    if (cardinality.has_value() && *cardinality > maxCardinality) {
      maxCardinality = *cardinality;
      largestTable = table;
    }
  });

  return largestTable;
}

// Computes the maximum cardinality estimate for aggregation grouping keys
// using saturating product to avoid overflow. When multiple keys come from
// the same table, cap the maximum cardinality estimate at table's
// cardinality.
std::optional<double> maxGroups(
    const ExprVector& groupingKeys,
    const ConstraintMap& constraints) {
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
    const auto maxCardinalityOpt = table->as<TableObject>()->cardinality();
    // An unknown table cardinality makes the group-count estimate unknown.
    if (!maxCardinalityOpt.has_value()) {
      return std::nullopt;
    }
    const double maxCardinality = *maxCardinalityOpt;

    maxTableCardinality = std::max(maxTableCardinality, maxCardinality);

    double groupCardinality;
    if (keys.size() == 1) {
      // Single key per table: use min of key cardinality and table
      // cardinality.
      const auto keyNdv = constraints.at(keys[0]->id()).cardinality;
      if (!keyNdv.has_value()) {
        return std::nullopt;
      }
      groupCardinality = std::min<double>(*keyNdv, maxCardinality);
    } else {
      // Multiple keys: collect cardinalities and use saturatingProduct.
      std::vector<double> keyCardinalities;
      keyCardinalities.reserve(keys.size());
      for (auto key : keys) {
        const auto keyNdv = constraints.at(key->id()).cardinality;
        if (!keyNdv.has_value()) {
          return std::nullopt;
        }
        keyCardinalities.push_back(*keyNdv);
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
    ExprVector _groupingKeys,
    ExprVector _preGroupedKeys,
    AggregateVector _aggregates,
    velox::core::AggregationNode::Step step,
    ColumnVector columns,
    QGVector<int32_t> _globalGroupingSets,
    ColumnCP _groupId)
    : RelationOp{RelType::kAggregation, std::move(input), std::move(columns)},
      groupingKeys{std::move(_groupingKeys)},
      aggregates{std::move(_aggregates)},
      step{step},
      preGroupedKeys{std::move(_preGroupedKeys)},
      globalGroupingSets{std::move(_globalGroupingSets)},
      groupId{_groupId} {
  if (!globalGroupingSets.empty()) {
    VELOX_CHECK_NOT_NULL(
        groupId, "groupId must be set when globalGroupingSets is non-empty");
  }
  if (groupId != nullptr) {
    VELOX_CHECK(
        std::find(groupingKeys.begin(), groupingKeys.end(), groupId) !=
            groupingKeys.end(),
        "groupId must appear in groupingKeys");
  }
#ifndef NDEBUG
  VELOX_DCHECK_EQ(
      columns_.size(),
      groupingKeys.size() + aggregates.size(),
      "Output columns must be groupingKeys followed by aggregate results.");

  VELOX_DCHECK_LE(preGroupedKeys.size(), groupingKeys.size());

  for (const auto* key : groupingKeys) {
    VELOX_DCHECK(key->is(PlanType::kColumnExpr));
  }
#endif

  cost_.inputCardinality = inputCardinality();

  const auto numKeys = groupingKeys.size();
  if (numKeys > 0) {
    // Input cardinality before the partial aggregation.
    std::optional<float> inputBeforePartial;
    if (step == velox::core::AggregationNode::Step::kFinal &&
        input_->is(RelType::kRepartition) &&
        input_->input()->is(RelType::kAggregation)) {
      auto partial = input_->input().get();

      const auto partialInputCardinality = partial->cost().inputCardinality;
      if (partialInputCardinality.has_value()) {
        VELOX_CHECK(!std::isnan(*partialInputCardinality));
        VELOX_CHECK(std::isfinite(*partialInputCardinality));
      }

      inputBeforePartial = partial->inputCardinality();
    } else {
      inputBeforePartial = cost_.inputCardinality;
    }

    setCostWithGroups(inputBeforePartial);
  } else {
    // Global aggregation (no grouping keys).
    cost_.unitCost = aggregates.size() * Costs::kSimpleAggregateCost;

    // Avoid division by zero. Unknown input cardinality => unknown fanout.
    cost_.fanout = divide(1.0f, cost_.inputCardinality);
  }

  // cost_.fanout may be unknown; only check when it is known.
  if (cost_.fanout.has_value()) {
    VELOX_CHECK_LE(*cost_.fanout, 1.0f);
  }

  initConstraints();
}

void Aggregation::initConstraints() {
  const std::optional<float> outputCardinality = resultCardinality();
  const auto numKeys = groupingKeys.size();

  for (size_t i = 0; i < columns_.size(); ++i) {
    auto* column = columns_[i];

    Value constraint = [&]() {
      if (i < numKeys) {
        auto it = input()->constraints().find(groupingKeys[i]->id());
        VELOX_DCHECK(
            it != input()->constraints().end(),
            "Missing constraint for grouping key: {}",
            column->toString());
        return it->second;
      } else {
        return column->value();
      }
    }();

    // Cap the column NDV at the group count. Skip when the output cardinality
    // is unknown so a known NDV is not wiped out.
    if (outputCardinality.has_value()) {
      constraint.cardinality =
          minOf(constraint.cardinality, *outputCardinality);
    }

    constraints_.emplace(column->id(), constraint);
  }
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

// Per-row cost of sorting 'cardinality' rows by 'numKeys' keys.
// Models O(n log n) comparisons amortized per row.
float sortCost(size_t numKeys, float cardinality) {
  if (numKeys == 0 || cardinality <= 1) {
    return 0;
  }
  return Costs::kKeyCompareCost * numKeys * std::log2(cardinality);
}
} // namespace

void Aggregation::setCostWithGroups(
    std::optional<float> inputBeforePartialOpt) {
  auto* optimization = queryCtx()->optimization();
  const auto& runnerOptions = optimization->runnerOptions();

  const auto maxGroupsOpt = maxGroups(groupingKeys, input_->constraints());
  // The grouping cost is unknown when the input row count or the maximum
  // number of distinct groups could not be estimated. Leave cost_ fanout,
  // unitCost, and totalBytes as nullopt so the unknown propagates.
  if (!inputBeforePartialOpt.has_value() || !maxGroupsOpt.has_value()) {
    return;
  }
  const float inputBeforePartial = *inputBeforePartialOpt;

  const auto maxCardinality = std::max<double>(1, *maxGroupsOpt);

  // The grouping key NDV already reflects upstream filtering, so the distinct
  // group count is min(NDV, input rows). Re-applying the coupon-collector
  // formula here would discount a second time and invent a reduction even for a
  // unique key.
  const auto numGroups = std::min<double>(maxCardinality, inputBeforePartial);

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

    // numGroups can be > inputCardinality since this is calculated against
    // the input before partial and inputCardinality is scaled down by partial
    // reduction. Unknown input cardinality => unknown fanout.
    const auto inputCard = inputCardinality();
    if (inputCard.has_value() && *inputCard != 0) {
      cost_.fanout = std::min<double>(*inputCard, numGroups) / *inputCard;
    }
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
  // reduction can be overestimated when input is a fraction of possible
  // values and partial capacity is set to be no greater than input.
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

void Aggregation::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

HashBuild::HashBuild(RelationOpPtr input, ExprVector keysVector, PlanP plan)
    : RelationOp{RelType::kHashBuild, std::move(input)},
      keys{std::move(keysVector)},
      plan{plan} {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;

  const auto numKeys = static_cast<float>(keys.size());
  const auto rowBytes = byteSize(columns());
  const auto numColumns = static_cast<float>(columns().size());
  // Per row cost calculates the column hashes twice, once to partition and a
  // second time to insert. The hashBuildCost term depends on the input
  // cardinality, so the unit cost is unknown when that is unknown.
  if (cost_.inputCardinality.has_value()) {
    cost_.unitCost = (numKeys * 2 * Costs::kHashColumnCost) +
        Costs::hashBuildCost(*cost_.inputCardinality, rowBytes) +
        numKeys * Costs::kKeyCompareCost +
        numColumns * Costs::kHashExtractColumnCost * 2;
  }

  cost_.totalBytes = mul(cost_.inputCardinality, rowBytes);

  // HashBuild projects all input columns.
  constraints_ = input_->constraints();
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

  // Start with input constraints
  constraints_ = input_->constraints();

  // Compute selectivity using filter analysis, updating constraints_. An
  // unknown selectivity leaves the fanout unknown.
  auto selectivity = conjunctsSelectivity(constraints_, exprs_, true);
  if (selectivity.has_value()) {
    cost_.fanout = static_cast<float>(selectivity->trueFraction);
  }

  // Scale NDV for all columns using the coupon collector formula.
  // conjunctsSelectivity narrows the value space for filtered columns;
  // scaleCardinalities narrows the expected count based on the combined row
  // reduction. See FilterSelectivity.md for why these compose correctly.
  scaleCardinalities(constraints_, cost_.inputCardinality, cost_.fanout);
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

  // Derive and propagate constraints from input expressions to output
  // columns. Only output columns are added to constraints_.
  ConstraintMap inputConstraints = input_->constraints();
  for (size_t i = 0; i < exprs_.size(); ++i) {
    Value exprValue = exprConstraint(exprs_[i], inputConstraints);
    constraints_.emplace(columns_[i]->id(), exprValue);
  }

  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;
  // A projection (in particular a redundant rename) has no per-row cost.
  // TODO: Model the per-expression evaluation cost.
  cost_.unitCost = 0;
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
    // An unknown input cardinality leaves the fanout unknown.
    if (cost_.inputCardinality.has_value()) {
      if (*cost_.inputCardinality <= cardinality) {
        // Input cardinality does not exceed the limit. The limit is no-op.
        // Doesn't change cardinality.
        cost_.fanout = 1;
      } else {
        // Input cardinality exceeds the limit. Calculate fanout to ensure that
        // fanout * limit = input-cardinality.
        cost_.fanout = cardinality / *cost_.inputCardinality;
      }
    }
  }

  // Modeled with zero per-row cost.
  // TODO: Model the sort cost.
  cost_.unitCost = 0;

  // OrderBy projects all input columns. When there's a LIMIT, keeping N of M
  // rows is equivalent to sampling with fraction s = N/M. Use the coupon
  // collector formula to estimate the number of distinct values surviving the
  // sampling.
  constraints_ = input_->constraints();
  scaleCardinalities(constraints_, cost_.inputCardinality, cost_.fanout);
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
  cost_.unitCost = Costs::kMinimalUnitCost;
  const auto cardinality = static_cast<float>(limit);
  // An unknown input cardinality leaves the fanout unknown.
  if (cost_.inputCardinality.has_value()) {
    if (*cost_.inputCardinality <= cardinality) {
      // Input cardinality does not exceed the limit. The limit is no-op.
      // Doesn't change cardinality.
      cost_.fanout = 1;
    } else {
      // Input cardinality exceeds the limit. Calculate fanout to ensure that
      // fanout * limit = input-cardinality.
      cost_.fanout = cardinality / *cost_.inputCardinality;
    }
  }

  // Limit projects all input columns. Keeping N of M rows is equivalent to
  // sampling with fraction s = N/M. Use the coupon collector formula to
  // estimate the number of distinct values surviving the sampling.
  constraints_ = input_->constraints();
  scaleCardinalities(constraints_, cost_.inputCardinality, cost_.fanout);
}

void Limit::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

namespace {

// Returns a Distribution describing the partitioning shared by every input
// when their partitions match per Distribution::isSamePartition; otherwise
// returns an empty Distribution. Only partition-related fields (kind,
// partition type, partition keys) are propagated — UNION ALL preserves how
// rows map to partitions but does not preserve per-input ordering,
// uniqueness, or clustering.
Distribution commonInputDistribution(const RelationOpPtrVector& inputs) {
  VELOX_CHECK_GE(inputs.size(), 2, "UnionAll requires at least 2 inputs");
  const auto& first = inputs[0]->distribution();
  for (size_t i = 1; i < inputs.size(); ++i) {
    if (!first.isSamePartition(inputs[i]->distribution())) {
      return Distribution{};
    }
  }
  return Distribution{
      first.kind(),
      first.partitionType(),
      first.partitionKeys(),
  };
}

} // namespace

UnionAll::UnionAll(RelationOpPtrVector inputsVector)
    : RelationOp{
          RelType::kUnionAll,
          /*input=*/nullptr,
          commonInputDistribution(inputsVector),
          inputsVector[0]->columns()},
      inputs{std::move(inputsVector)} {
  cost_.inputCardinality = 0;
  for (auto& input : inputs) {
    cost_.inputCardinality =
        add(cost_.inputCardinality,
            mul(input->cost().inputCardinality, input->cost().fanout));
  }

  cost_.fanout = 1;
  // A union-all has no per-row cost.
  cost_.unitCost = 0;

  initConstraints();
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

void UnionAll::initConstraints() {
  // Output columns are from the first input.
  const auto& outputColumns = columns_;

  for (size_t i = 0; i < outputColumns.size(); ++i) {
    auto* outputColumn = outputColumns[i];

    // Reads constraint for the i-th column of the given input operator from
    // the operator's constraint map.
    auto inputConstraint = [&](size_t inputIndex) -> const Value& {
      VELOX_CHECK_LT(inputIndex, inputs.size());
      VELOX_CHECK_LT(i, inputs[inputIndex]->columns().size());
      const auto* column = inputs[inputIndex]->columns()[i];
      auto it = inputs[inputIndex]->constraints().find(column->id());
      VELOX_CHECK(
          it != inputs[inputIndex]->constraints().end(),
          "Missing constraint for column: {}",
          column->toString());
      return it->second;
    };

    // Get constraint from first input as starting point.
    Value combined = inputConstraint(0);
    // Row counts and the weighted fraction accumulators are all unknown if any
    // leg's cardinality or the corresponding fraction is unknown; the helpers
    // propagate nullopt.
    std::optional<float> totalRows = inputs[0]->resultCardinality();
    std::optional<float> weightedNullFraction =
        mul(combined.nullFraction, totalRows);
    std::optional<float> weightedTrueFraction =
        mul(combined.trueFraction, totalRows);

    // Derive min and max across all legs. All-NULL legs contribute no
    // bound. A leg with unknown bound wipes the combined bound — no later
    // leg can recover it.
    auto deriveBound = [&](VariantCP Value::* bound,
                           const auto& compare) -> VariantCP {
      VariantCP result = nullptr;
      for (size_t j = 0; j < inputs.size(); ++j) {
        const Value& inputValue = inputConstraint(j);
        if (inputValue.nullFraction == 1.0) {
          continue;
        }
        VariantCP candidate = inputValue.*bound;
        if (candidate == nullptr) {
          return nullptr;
        }
        if (result == nullptr || compare(*candidate, *result)) {
          result = candidate;
        }
      }
      return result;
    };
    combined.min = deriveBound(
        &Value::min, [](const auto& a, const auto& b) { return a < b; });
    combined.max = deriveBound(
        &Value::max, [](const auto& a, const auto& b) { return b < a; });

    // Combine remaining fields from inputs beyond the first.
    for (size_t j = 1; j < inputs.size(); ++j) {
      const Value& inputValue = inputConstraint(j);
      const std::optional<float> inputRows = inputs[j]->resultCardinality();

      totalRows = add(totalRows, inputRows);
      weightedNullFraction =
          add(weightedNullFraction, mul(inputValue.nullFraction, inputRows));
      weightedTrueFraction =
          add(weightedTrueFraction, mul(inputValue.trueFraction, inputRows));
      combined.nullable = combined.nullable || inputValue.nullable;
    }

    // Estimate the union NDV by inclusion-exclusion over the legs' ranges. Fold
    // legs left to right; for each leg subtract the values expected to be
    // shared with the running range, assuming uniform density within each range
    // and independence across legs. This keeps the summed NDV for disjoint legs
    // and discounts the overlap otherwise. A leg without an integer range
    // contributes its full NDV (the disjoint assumption) and clears the running
    // range so later legs cannot estimate overlap against it.
    std::optional<float> unionNdv = inputConstraint(0).cardinality;
    VariantCP runMin = inputConstraint(0).min;
    VariantCP runMax = inputConstraint(0).max;
    for (size_t j = 1; j < inputs.size() && unionNdv.has_value(); ++j) {
      const Value& leg = inputConstraint(j);
      if (!leg.cardinality.has_value()) {
        unionNdv = std::nullopt;
        break;
      }
      const auto runRange = rangeCardinality(combined.type, runMin, runMax);
      const auto legRange = rangeCardinality(combined.type, leg.min, leg.max);
      float shared = 0;
      if (runRange.has_value() && legRange.has_value()) {
        // All four bounds are set here (rangeCardinality returns a value only
        // when both of its bounds are). Overlap = [max(mins), min(maxs)].
        VariantCP overlapMin = *runMin < *leg.min ? leg.min : runMin;
        VariantCP overlapMax = *leg.max < *runMax ? leg.max : runMax;
        if (!(*overlapMax < *overlapMin)) {
          if (const auto overlap =
                  rangeCardinality(combined.type, overlapMin, overlapMax)) {
            shared = *overlap * (*unionNdv / *runRange) *
                (*leg.cardinality / *legRange);
          }
        }
        runMin = *leg.min < *runMin ? leg.min : runMin;
        runMax = *runMax < *leg.max ? leg.max : runMax;
      } else {
        // A leg without an integer range clears the running range so later legs
        // fall back to the disjoint (summed) assumption.
        runMin = nullptr;
        runMax = nullptr;
      }
      unionNdv = std::max(
          std::max(*unionNdv, *leg.cardinality),
          *unionNdv + *leg.cardinality - shared);
    }
    combined.cardinality = unionNdv;
    // Weighted averages are unknown if the accumulators or row count are
    // unknown; divide also yields nullopt when totalRows is 0.
    combined.nullFraction = divide(weightedNullFraction, totalRows);
    combined.trueFraction = divide(weightedTrueFraction, totalRows);

    constraints_.emplace(outputColumn->id(), combined);
  }
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
  cost_.fanout = 1;
  cost_.unitCost = Costs::kMinimalUnitCost;
  VELOX_DCHECK_EQ(
      this->inputColumns.size(), this->write->table().type()->size());
}

void TableWrite::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

EnforceSingleRow::EnforceSingleRow(RelationOpPtr input)
    : RelationOp(
          RelType::kEnforceSingleRow,
          input,
          input->distribution(),
          input->columns()) {
  // Cardinality neutral: passes through exactly 1 row or fails at runtime.
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;
  cost_.unitCost = 0;

  // EnforceSingleRow projects all input columns.
  constraints_ = input_->constraints();
}

void EnforceSingleRow::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

namespace {

// Returns the Value for a ranking column (row_number, rank, dense_rank) with
// cardinality set to min(limit, inputCardinality) and nullable set to false.
Value rankingColumnValue(
    ColumnCP outputColumn,
    std::optional<float> inputCardinality,
    std::optional<int32_t> limit) {
  auto value = outputColumn->value();
  // An unknown input cardinality leaves the ranking column's NDV unknown.
  value.cardinality = limit.has_value()
      ? minOf(static_cast<float>(limit.value()), inputCardinality)
      : inputCardinality;
  value.nullable = false;
  value.nullFraction = 0;
  return value;
}

ColumnVector appendColumn(const ColumnVector& columns, ColumnCP column) {
  ColumnVector result = columns;
  result.push_back(column);
  return result;
}
} // namespace

AssignUniqueId::AssignUniqueId(RelationOpPtr input, ColumnCP uniqueIdColumn)
    : RelationOp(
          RelType::kAssignUniqueId,
          input,
          Distribution(
              input->distribution().kind(),
              input->distribution().partitionType(),
              input->distribution().partitionKeys(),
              input->distribution().orderKeys(),
              input->distribution().orderTypes(),
              input->distribution().numKeysUnique(),
              ExprVector{uniqueIdColumn}),
          appendColumn(input->columns(), uniqueIdColumn)),
      uniqueIdColumn_(uniqueIdColumn) {
  // Fanout is 1 (cardinality neutral).
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;
  cost_.unitCost = Costs::kMinimalUnitCost;

  // Copy all input constraints (AssignUniqueId projects all input columns).
  constraints_ = input_->constraints();

  // Add constraint for the uniqueId column: one unique value per row,
  // non-null. The NDV equals the row count, which is unknown if the result
  // cardinality is unknown.
  Value uniqueIdValue(uniqueIdColumn_->value().type, resultCardinality());
  uniqueIdValue.nullable = false;
  constraints_.emplace(uniqueIdColumn_->id(), uniqueIdValue);
}

void AssignUniqueId::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

EnforceDistinct::EnforceDistinct(
    RelationOpPtr input,
    ExprVector distinctKeys,
    ExprVector preGroupedKeys,
    Name errorMessage)
    : RelationOp(RelType::kEnforceDistinct, std::move(input)),
      distinctKeys_(std::move(distinctKeys)),
      preGroupedKeys_(std::move(preGroupedKeys)),
      errorMessage_(errorMessage) {
  // Fanout is 1 (cardinality neutral).
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;
  // TODO: Review cost for EnforceDistinct.
  cost_.unitCost = 0.01;

  // EnforceDistinct projects all input columns.
  constraints_ = input_->constraints();
}

void EnforceDistinct::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

Window::Window(
    RelationOpPtr input,
    ExprVector _partitionKeys,
    ExprVector _orderKeys,
    OrderTypeVector _orderTypes,
    WindowFunctionVector _windowFunctions,
    bool _inputsSorted,
    ColumnVector columns)
    : RelationOp{RelType::kWindow, std::move(input), std::move(columns)},
      partitionKeys{std::move(_partitionKeys)},
      orderKeys{std::move(_orderKeys)},
      orderTypes{std::move(_orderTypes)},
      windowFunctions{std::move(_windowFunctions)},
      inputsSorted{_inputsSorted} {
  VELOX_CHECK_EQ(orderKeys.size(), orderTypes.size());

  cost_.inputCardinality = inputCardinality();
  // Window functions are cardinality-neutral.
  cost_.fanout = 1;

  const auto numKeys = partitionKeys.size() + orderKeys.size();
  const float windowFunctionCost =
      windowFunctions.size() * Costs::kSimpleAggregateCost;
  if (inputsSorted) {
    cost_.unitCost = windowFunctionCost;
  } else if (cost_.inputCardinality.has_value()) {
    // Sorting cost depends on the input cardinality; unknown when it is.
    cost_.unitCost =
        sortCost(numKeys, *cost_.inputCardinality) + windowFunctionCost;
  }

  initConstraints();
}

void Window::initConstraints() {
  // Window passes through all input constraints and adds new columns for
  // window function results.
  constraints_ = input_->constraints();
  const auto numInputColumns = columns_.size() - windowFunctions.size();
  for (size_t i = numInputColumns; i < columns_.size(); ++i) {
    auto* column = columns_[i];
    constraints_.emplace(column->id(), column->value());
  }
}

const QGString& Window::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  std::stringstream out;
  out << input_->historyKey();
  out << " window";
  auto* opt = queryCtx()->optimization();
  velox::ScopedVarSetter cnames(&opt->cnamesInExpr(), false);
  if (!partitionKeys.empty()) {
    out << " partition by ";
    std::vector<std::string> strings;
    for (auto& key : partitionKeys) {
      strings.push_back(key->toString());
    }
    std::ranges::sort(strings);
    for (auto& s : strings) {
      out << s << ", ";
    }
  }
  key_ = sanitizeHistoryKey(out.str());
  return key_;
}

void Window::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

RowNumber::RowNumber(
    RelationOpPtr input,
    ExprVector _partitionKeys,
    std::optional<int32_t> _limit,
    ColumnCP _outputColumn,
    ColumnVector columns)
    : RelationOp{RelType::kRowNumber, std::move(input), std::move(columns)},
      partitionKeys{std::move(_partitionKeys)},
      limit{_limit},
      outputColumn{_outputColumn} {
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;
  // No sorting, just hashing by partition keys. The hashTableCost term depends
  // on the input cardinality, so the unit cost is unknown when it is.
  if (cost_.inputCardinality.has_value()) {
    cost_.unitCost = Costs::kHashColumnCost * partitionKeys.size() +
        Costs::hashTableCost(*cost_.inputCardinality);
  }

  // Pass through all input constraints and add a new column for the row number.
  constraints_ = input_->constraints();
  constraints_.emplace(
      outputColumn->id(),
      rankingColumnValue(outputColumn, cost_.inputCardinality, limit));
}

const QGString& RowNumber::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  std::stringstream out;
  out << input_->historyKey();
  out << " row_number";
  auto* opt = queryCtx()->optimization();
  velox::ScopedVarSetter cnames(&opt->cnamesInExpr(), false);
  if (!partitionKeys.empty()) {
    out << " partition by ";
    std::vector<std::string> strings;
    for (auto& key : partitionKeys) {
      strings.push_back(key->toString());
    }
    std::ranges::sort(strings);
    for (auto& s : strings) {
      out << s << ", ";
    }
  }
  key_ = sanitizeHistoryKey(out.str());
  return key_;
}

void RowNumber::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

TopNRowNumber::TopNRowNumber(
    RelationOpPtr input,
    ExprVector _partitionKeys,
    ExprVector _orderKeys,
    OrderTypeVector _orderTypes,
    velox::core::TopNRowNumberNode::RankFunction _rankFunction,
    int32_t _limit,
    ColumnCP _outputColumn,
    ColumnVector columns)
    : RelationOp{RelType::kTopNRowNumber, std::move(input), std::move(columns)},
      partitionKeys{std::move(_partitionKeys)},
      orderKeys{std::move(_orderKeys)},
      orderTypes{std::move(_orderTypes)},
      rankFunction{_rankFunction},
      limit{_limit},
      outputColumn{_outputColumn} {
  VELOX_CHECK_EQ(orderKeys.size(), orderTypes.size());
  VELOX_CHECK(!orderKeys.empty());

  cost_.inputCardinality = inputCardinality();
  cost_.fanout = 1;
  // Hash by partition keys + partial sort by order keys with limit-based
  // pruning. Cheaper than a full Window sort because only top N rows are kept.
  // The sort term depends on the input cardinality, so the unit cost is
  // unknown when it is.
  const auto numKeys = partitionKeys.size() + orderKeys.size();
  if (cost_.inputCardinality.has_value()) {
    cost_.unitCost = Costs::kHashColumnCost * partitionKeys.size() +
        sortCost(numKeys, *cost_.inputCardinality);
  }

  // Pass through all input constraints and add a new column for the ranking
  // result.
  constraints_ = input_->constraints();
  constraints_.emplace(
      outputColumn->id(),
      rankingColumnValue(outputColumn, cost_.inputCardinality, limit));
}

const QGString& TopNRowNumber::historyKey() const {
  if (!key_.empty()) {
    return key_;
  }
  std::stringstream out;
  out << input_->historyKey();
  out << " topn_row_number";
  auto* opt = queryCtx()->optimization();
  velox::ScopedVarSetter cnames(&opt->cnamesInExpr(), false);
  if (!partitionKeys.empty()) {
    out << " partition by ";
    std::vector<std::string> strings;
    for (auto& key : partitionKeys) {
      strings.push_back(key->toString());
    }
    std::ranges::sort(strings);
    for (auto& s : strings) {
      out << s << ", ";
    }
  }
  out << " order by ";
  for (size_t i = 0; i < orderKeys.size(); ++i) {
    out << orderKeys[i]->toString() << " "
        << OrderTypeName::toName(orderTypes[i]) << ", ";
  }
  out << " limit " << limit;
  key_ = sanitizeHistoryKey(out.str());
  return key_;
}

void TopNRowNumber::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

MarkDistinct::MarkDistinct(
    RelationOpPtr input,
    ColumnVector markers,
    ExprVector keys,
    ColumnVector masks)
    : RelationOp(
          RelType::kMarkDistinct,
          input,
          [&]() {
            ColumnVector cols = input->columns();
            for (const auto* marker : markers) {
              cols.push_back(marker);
            }
            return cols;
          }()),
      markers_(std::move(markers)),
      keys_(std::move(keys)),
      masks_(std::move(masks)) {
  VELOX_CHECK_EQ(
      markers_.size(),
      masks_.size() + 1,
      "MarkDistinct must have one marker for the no-mask channel plus one per mask");
  VELOX_CHECK(!keys_.empty());
  auto* optimization = queryCtx()->optimization();
  const auto& runnerOptions = optimization->runnerOptions();

  cost_.inputCardinality = inputCardinality();
  // MarkDistinct doesn't reduce rows.
  cost_.fanout = 1;

  const auto maxGroupsOpt = maxGroups(keys_, input_->constraints());
  const auto maskBitmapBytes = (masks_.size() + 7) / 8;
  const auto rowBytes =
      byteSize(keys_) + Costs::kHashRowBytes + maskBitmapBytes;

  // The group count drives both the memory footprint and the unit cost; both
  // are unknown when the input cardinality or the max group count is unknown.
  if (cost_.inputCardinality.has_value() && maxGroupsOpt.has_value()) {
    const auto maxCardinality = std::max<double>(1, *maxGroupsOpt);
    // min(NDV, input rows); see the note in Aggregation::setCostWithGroups.
    const auto numGroups =
        std::min<double>(maxCardinality, *cost_.inputCardinality);
    cost_.totalBytes = numGroups * rowBytes;

    // Cost is similar to a hash aggregation for tracking distinct values. Each
    // input row probes the hash table to check if its key combination has been
    // seen, and inserts a new entry if not. Estimated localExchangeCost to be
    // 1/3 of a remote shuffle, following the same cost modeling of Aggregation.
    float localExchangeCost = 0;
    if (runnerOptions.numDrivers > 1) {
      localExchangeCost = shuffleCost(input_->columns()) / 3;
    }
    auto markDistinctCost = Costs::kHashColumnCost * keys_.size() +
        Costs::hashTableCost(numGroups) +
        Costs::kKeyCompareCost * keys_.size() +
        Costs::hashRowCost(numGroups, rowBytes) +
        masks_.size() * Costs::kSimpleAggregateCost;
    cost_.unitCost = localExchangeCost + markDistinctCost;
  }

  constraints_ = input->constraints();
  for (const auto* marker : markers_) {
    constraints_.emplace(marker->id(), marker->value());
  }
}

void MarkDistinct::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

namespace {

// Builds the output columns for GroupId from grouping keys, aggregation
// inputs, and the group ID column.
ColumnVector makeGroupIdColumns(
    const ColumnVector& groupingKeyColumns,
    const ExprVector& aggregationInputs,
    ColumnCP groupId) {
  ColumnVector columns;
  columns.reserve(groupingKeyColumns.size() + aggregationInputs.size() + 1);
  for (auto* column : groupingKeyColumns) {
    columns.push_back(column);
  }
  for (auto* expr : aggregationInputs) {
    VELOX_CHECK(
        expr->is(PlanType::kColumnExpr),
        "GroupId aggregation input must be a Column");
    columns.push_back(expr->as<Column>());
  }
  columns.push_back(groupId);
  return columns;
}

} // namespace

GroupId::GroupId(
    RelationOpPtr input,
    ColumnVector groupingKeys,
    ExprVector aggregationInputs,
    GroupingSets groupingSets,
    ColumnVector groupingKeyColumns,
    ColumnCP groupId)
    : RelationOp(
          RelType::kGroupId,
          std::move(input),
          makeGroupIdColumns(groupingKeyColumns, aggregationInputs, groupId)),
      groupingKeys_(std::move(groupingKeys)),
      aggregationInputs_(std::move(aggregationInputs)),
      groupingSets_(std::move(groupingSets)),
      groupingKeyColumns_(std::move(groupingKeyColumns)),
      groupId_(groupId) {
  VELOX_CHECK_GT(
      groupingSets_.size(),
      1,
      "GroupId requires multiple grouping sets. "
      "A single grouping set is a regular GROUP BY.");

  // Each input row is duplicated once per set.
  cost_.inputCardinality = inputCardinality();
  cost_.fanout = groupingSets_.size();
  cost_.unitCost = Costs::kMinimalUnitCost * groupingSets_.size();

  constraints_ = input_->constraints();

  // Add constraints for the renamed output key columns. GroupId NULLs out
  // keys for non-participating grouping sets, so they are always nullable.
  const auto numSets = groupingSets_.size();
  std::vector<size_t> setsPerKey(groupingKeyColumns_.size(), 0);
  for (const auto& set : groupingSets_) {
    for (auto idx : set) {
      VELOX_CHECK_LT(idx, groupingKeyColumns_.size());
      ++setsPerKey[idx];
    }
  }

  for (size_t keyIdx = 0; keyIdx < groupingKeyColumns_.size(); ++keyIdx) {
    auto value = groupingKeyColumns_[keyIdx]->value();
    value.nullable = true;
    value.nullFraction = static_cast<float>(numSets - setsPerKey[keyIdx]) /
        static_cast<float>(numSets);
    constraints_.emplace(groupingKeyColumns_[keyIdx]->id(), value);
  }

  constraints_.emplace(groupId_->id(), groupId_->value());
}

void GroupId::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}
} // namespace facebook::axiom::optimizer
