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
  const auto& orderKeys = index->distribution.orderKeys();
  float selection = 1;
  for (int32_t i = 0; i < input->distribution().orderKeys().size() &&
       i < orderKeys.size() && i < keys.size();
       ++i) {
    if (input->distribution().orderKeys()[i]->sameOrEqual(*keys[i])) {
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

  // Initialize constraints from base table columns. If the table has filters,
  // column values already reflect narrowed cardinality and min/max from filter
  // selectivity computation (see setBaseTableValues in VeloxHistory.cpp).
  for (auto* column : columns_) {
    constraints_.emplace(column->id(), column->value());
  }

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

  // Cap column cardinalities to the output row count.
  const auto outputCardinality = resultCardinality();
  for (auto& [id, constraint] : constraints_) {
    constraint.cardinality =
        std::min(constraint.cardinality, outputCardinality);
  }
}

// static
Distribution TableScan::outputDistribution(
    const BaseTable* baseTable,
    ColumnGroupCP index,
    const ColumnVector& columns) {
  auto schemaColumns = transform<ColumnVector>(
      columns, [](auto& column) { return column->schemaColumn(); });

  const auto& distribution = index->distribution;

  ExprVector partitionKeys;
  ExprVector orderKeys;
  OrderTypeVector orderTypes;
  // if all partitioning columns are projected, the output is partitioned.
  if (isSubset(distribution.partitionKeys(), schemaColumns)) {
    partitionKeys = distribution.partitionKeys();
    replace(partitionKeys, schemaColumns, columns.data());
  }

  auto numPrefix = prefixSize(distribution.orderKeys(), schemaColumns);
  if (numPrefix > 0) {
    orderKeys = distribution.orderKeys();
    orderKeys.resize(numPrefix);
    orderTypes = distribution.orderTypes();
    orderTypes.resize(numPrefix);
    replace(orderKeys, schemaColumns, columns.data());
  }
  return Distribution(
      distribution.distributionType(),
      std::move(partitionKeys),
      std::move(orderKeys),
      std::move(orderTypes),
      distribution.numKeysUnique() <= numPrefix ? distribution.numKeysUnique()
                                                : 0);
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

// Predicts the number of distinct values expected after sampling numRows
// items from a population with numDistinct distinct values.
double expectedNumDistincts(double numRows, double numDistinct) {
  if (numDistinct <= 0 || numRows <= 0) {
    return 1.0;
  }

  // Using the coupon collector formula.
  // Expected distinct values = d * (1 - (1 - 1/d)^n)
  // where d is total distinct values and n is number of samples.

  // For numerical stability, use the identity:
  //   (1 - 1/d)^n = exp(n * log(1 - 1/d))
  // When d is large, log(1 - 1/d) ≈ -1/d (first-order Taylor expansion),
  // so (1 - 1/d)^n ≈ exp(-n/d).
  //
  // We use exp(-n/d) directly when d is large enough (d >= 1e6) to avoid
  // precision loss from computing log(1 - 1/d) with floating point.
  // For smaller d, we use std::log1p(-1/d) which is numerically stable
  // for small arguments.
  double exponent;
  if (numDistinct >= 1e6) {
    // Large d: use exp(-n/d) approximation directly.
    exponent = -numRows / numDistinct;
  } else {
    // std::log1p(x) computes log(1+x) accurately for small x.
    exponent = numRows * std::log1p(-1.0 / numDistinct);
  }

  // Clamp exponent to avoid underflow. exp(-746) ≈ 0 in double precision.
  // For very large negative exponents, (1-1/d)^n → 0, so result → d.
  if (exponent < -700) {
    return numDistinct;
  }

  return numDistinct * (1.0 - std::exp(exponent));
}

// Static methods for populating join constraints.
struct JoinConstraints {
  // Returns {leftOptional, rightOptional} for the given join type.
  // leftOptional is true for RIGHT and FULL joins.
  // rightOptional is true for LEFT and FULL joins.
  static std::pair<bool, bool> optionality(velox::core::JoinType joinType) {
    bool leftOptional =
        (joinType == velox::core::JoinType::kRight ||
         joinType == velox::core::JoinType::kFull);
    bool rightOptional =
        (joinType == velox::core::JoinType::kLeft ||
         joinType == velox::core::JoinType::kFull);
    return {leftOptional, rightOptional};
  }

  // Updates key constraints for inner join (neither side optional).
  // Sets nullable=false, nullFraction=0, and applies
  // columnComparisonSelectivity to both sides.
  static void updateKeyInner(
      ExprCP left,
      ExprCP right,
      bool leftInOutput,
      bool rightInOutput,
      Value leftValue,
      Value rightValue,
      ConstraintMap& constraints) {
    leftValue.nullable = false;
    leftValue.nullFraction = 0.0f;
    rightValue.nullable = false;
    rightValue.nullFraction = 0.0f;

    ConstraintMap tempConstraints;
    columnComparisonSelectivity(
        left,
        right,
        leftValue,
        rightValue,
        queryCtx()->functionNames().equality,
        true,
        tempConstraints);

    if (leftInOutput && tempConstraints.contains(left->id())) {
      constraints.insert_or_assign(left->id(), tempConstraints.at(left->id()));
    }
    if (rightInOutput && tempConstraints.contains(right->id())) {
      constraints.insert_or_assign(
          right->id(), tempConstraints.at(right->id()));
    }
  }

  // Updates key constraints for outer join (one or both sides optional).
  // Computes null fraction for optional-side keys based on the ratio of
  // distinct values. Non-optional side constraints are not modified.
  static void updateKeyOuter(
      ExprCP left,
      ExprCP right,
      bool leftOptional,
      bool rightOptional,
      bool leftInOutput,
      bool rightInOutput,
      Value leftValue,
      Value rightValue,
      ConstraintMap& constraints) {
    // Compute the fraction of rows on the non-optional side that don't find
    // a match on the optional side.
    //
    // For LEFT JOIN: nullFraction = fraction of left rows that don't match
    // any right row = max(0, 1 - ndv(right) / ndv(left)).
    //
    // For RIGHT JOIN: same logic with sides reversed.
    float leftNullFraction = 0.0f;
    float rightNullFraction = 0.0f;

    if (leftOptional && rightValue.cardinality > 0) {
      leftNullFraction =
          std::max(0.0f, 1.0f - leftValue.cardinality / rightValue.cardinality);
    }

    if (rightOptional && leftValue.cardinality > 0) {
      rightNullFraction =
          std::max(0.0f, 1.0f - rightValue.cardinality / leftValue.cardinality);
    }

    if (leftOptional) {
      leftValue.nullable = true;
      leftValue.nullFraction = leftNullFraction;
    }

    if (rightOptional) {
      rightValue.nullable = true;
      rightValue.nullFraction = rightNullFraction;
    }

    // Compute constraints into tempConstraints, then selectively apply only
    // to the optional side(s). We use tempConstraints because
    // columnComparisonSelectivity adds constraints for both sides, but for
    // outer joins we only want constraints on the optional side(s). We also
    // need to preserve the nullable and nullFraction we computed above.
    ConstraintMap tempConstraints;
    columnComparisonSelectivity(
        left,
        right,
        leftValue,
        rightValue,
        queryCtx()->functionNames().equality,
        true,
        tempConstraints);

    if (leftOptional && leftInOutput && tempConstraints.contains(left->id())) {
      Value constraint = tempConstraints.at(left->id());
      constraint.nullable = leftValue.nullable;
      constraint.nullFraction = leftValue.nullFraction;
      constraints.insert_or_assign(left->id(), constraint);
    }
    if (rightOptional && rightInOutput &&
        tempConstraints.contains(right->id())) {
      Value constraint = tempConstraints.at(right->id());
      constraint.nullable = rightValue.nullable;
      constraint.nullFraction = rightValue.nullFraction;
      constraints.insert_or_assign(right->id(), constraint);
    }
  }

  // Updates constraints for a single join key pair.
  // Dispatches to updateKeyInner or updateKeyOuter based on optionality.
  // constraints must already be populated with output columns.
  static void updateKey(
      ExprCP left,
      ExprCP right,
      bool leftOptional,
      bool rightOptional,
      ConstraintMap& constraints) {
    bool leftInOutput = constraints.contains(left->id());
    bool rightInOutput = constraints.contains(right->id());
    if (!leftInOutput && !rightInOutput) {
      return;
    }

    Value leftValue = value(constraints, left);
    Value rightValue = value(constraints, right);

    if (!leftOptional && !rightOptional) {
      updateKeyInner(
          left,
          right,
          leftInOutput,
          rightInOutput,
          leftValue,
          rightValue,
          constraints);
    } else {
      updateKeyOuter(
          left,
          right,
          leftOptional,
          rightOptional,
          leftInOutput,
          rightInOutput,
          leftValue,
          rightValue,
          constraints);
    }
  }

  // Updates constraints for join key columns.
  // constraints must already be populated with output columns.
  static void updateKeys(
      const ExprVector& left,
      const ExprVector& right,
      bool leftOptional,
      bool rightOptional,
      ConstraintMap& constraints) {
    VELOX_CHECK_EQ(
        left.size(), right.size(), "Join key vectors must have same size");

    for (size_t i = 0; i < left.size(); ++i) {
      updateKey(left[i], right[i], leftOptional, rightOptional, constraints);
    }
  }

  // Updates constraints for join payload (non-key) columns from the optional
  // side of an outer join. Sets nullable=true and nullFraction for unmatched
  // rows. constraints must already be populated with output columns.
  static void updatePayload(
      const ColumnVector& columns,
      const ExprVector& keys,
      float nullFraction,
      ConstraintMap& constraints) {
    auto keyIds = PlanObjectSet::fromObjects(keys);

    for (auto* column : columns) {
      if (keyIds.contains(column)) {
        continue;
      }
      auto it = constraints.find(column->id());
      if (it != constraints.end()) {
        it->second.nullable = true;
        it->second.nullFraction = nullFraction;
      }
    }
  }

  // Computes the null fraction for the optional side of an outer join.
  // Returns the fraction of rows on the non-optional side that don't
  // find a match, i.e. max(0, 1 - ndv(optional_key) / ndv(non_optional_key)).
  static float computeNullFraction(
      const ExprVector& optionalKeys,
      const ExprVector& nonOptionalKeys,
      const ConstraintMap& constraints) {
    float nullFraction = 0.0f;
    for (size_t i = 0; i < optionalKeys.size(); ++i) {
      const auto& optionalValue = value(constraints, optionalKeys[i]);
      const auto& nonOptionalValue = value(constraints, nonOptionalKeys[i]);
      if (nonOptionalValue.cardinality > 0) {
        nullFraction = std::max(
            nullFraction,
            std::max(
                0.0f,
                1.0f -
                    optionalValue.cardinality / nonOptionalValue.cardinality));
      }
    }
    return nullFraction;
  }

  // Adds mark column constraint for left semi project (mark join).
  // The mark column is a boolean indicating whether each probe row found a
  // match. Computes trueFraction from raw fanout and filter selectivity.
  static void addMark(
      const ColumnVector& columns,
      float fanout,
      float filterSelectivity,
      ConstraintMap& constraints) {
    VELOX_DCHECK(!columns.empty(), "LeftSemiProject must have columns");

    auto* markColumn = columns.back();
    VELOX_DCHECK(
        markColumn->value().type->isBoolean(), "Mark column must be boolean");

    Value markValue = markColumn->value();
    markValue.trueFraction = std::min<float>(1, fanout) * filterSelectivity;
    markValue.nullable = false;
    markValue.nullFraction = 0;

    constraints.emplace(markColumn->id(), markValue);
  }

  // Scales cardinality of non-key payload columns using the coupon collector
  // formula. When a join eliminates rows (selectivity < 1), the number of
  // distinct values in payload columns decreases non-linearly.
  static void scalePayloadCardinality(
      const ColumnVector& columns,
      const ExprVector& keys,
      float selectivity,
      float numRows,
      ConstraintMap& constraints) {
    if (selectivity >= 1.0f) {
      return;
    }
    auto keyIds = PlanObjectSet::fromObjects(keys);
    for (auto* column : columns) {
      if (keyIds.contains(column)) {
        continue;
      }
      auto it = constraints.find(column->id());
      if (it != constraints.end()) {
        it->second.cardinality = std::max(
            1.0f,
            (float)expectedNumDistincts(
                numRows * selectivity, it->second.cardinality));
      }
    }
  }

  // Updates key constraints for anti join (NOT EXISTS / NOT IN).
  //
  // Anti join deterministically removes rows whose key values appear on the
  // right side. The minNdv overlapping values are removed, leaving
  // ndv(leftKey) - minNdv distinct key values.
  //
  // Key nullFraction increases: all nulls survive (x = NULL is never true
  // in NOT EXISTS), but only a fraction of non-nulls survive.
  // newNullFraction = nullFraction / (nullFraction + (1 - nullFraction) *
  // antiSelectivity).
  //
  // Payload columns are scaled separately via scalePayloadCardinality.
  static void updateAntiKeys(
      const ExprVector& leftKeys,
      const ExprVector& rightKeys,
      float antiSelectivity,
      ConstraintMap& constraints) {
    for (size_t i = 0; i < leftKeys.size(); ++i) {
      auto it = constraints.find(leftKeys[i]->id());
      if (it != constraints.end()) {
        auto& value = it->second;
        const float minNdv =
            std::min(value.cardinality, rightKeys[i]->value().cardinality);
        value.cardinality = std::max(1.0f, value.cardinality - minNdv);

        const float survivingFraction =
            value.nullFraction + (1.0f - value.nullFraction) * antiSelectivity;
        value.nullFraction = survivingFraction > 0
            ? value.nullFraction / survivingFraction
            : 0.0f;
      }
    }
  }
};

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
      return std::min<float>(1, fanout);
    case velox::core::JoinType::kRightSemiProject:
      return rightToLeftRatio;
    case velox::core::JoinType::kRightSemiFilter:
      return std::min<float>(1, rlFanout) * rightToLeftRatio;
    case velox::core::JoinType::kAnti:
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
    RelationOpPtr lhs,
    RelationOpPtr rhs,
    ExprVector lhsKeys,
    ExprVector rhsKeys,
    ExprVector filterExprs,
    float fanout,
    float rlFanout,
    ColumnVector columns)
    : RelationOp{RelType::kJoin, std::move(lhs), std::move(columns)},
      method{method},
      joinType{joinType},
      nullAware{nullAware},
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
  float filterSelectivity = computeFilterSelectivity();
  initConstraints(fanout, rlFanout, filterSelectivity);
  initCost(fanout, rlFanout, filterSelectivity);
}

float Join::computeFilterSelectivity() const {
  if (filter.empty()) {
    return 1.0f;
  }
  ConstraintMap inputConstraints = input_->constraints();
  for (const auto& [columnId, constraint] : right->constraints()) {
    inputConstraints.emplace(columnId, constraint);
  }
  return conjunctsSelectivity(inputConstraints, filter, false).trueFraction;
}

void Join::initConstraints(
    float fanout,
    float rlFanout,
    float filterSelectivity) {
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

  if (joinType == velox::core::JoinType::kAnti) {
    const float antiSelectivity =
        std::max(0.0f, 1.0f - fanout * filterSelectivity);
    JoinConstraints::updateAntiKeys(
        leftKeys, rightKeys, antiSelectivity, constraints_);
    JoinConstraints::scalePayloadCardinality(
        input_->columns(),
        leftKeys,
        antiSelectivity,
        inputCardinality(),
        constraints_);
    return;
  }

  auto [leftOptional, rightOptional] = JoinConstraints::optionality(joinType);

  JoinConstraints::updateKeys(
      leftKeys, rightKeys, leftOptional, rightOptional, constraints_);

  if (joinType == velox::core::JoinType::kLeftSemiProject) {
    JoinConstraints::addMark(columns_, fanout, filterSelectivity, constraints_);
  }

  // Update constraints for optional-side payload columns.
  // This uses the same ndv-based formula as updateKey().
  if (leftOptional) {
    const float leftNullFraction =
        JoinConstraints::computeNullFraction(leftKeys, rightKeys, constraints_);
    JoinConstraints::updatePayload(
        input_->columns(), leftKeys, leftNullFraction, constraints_);
  }
  if (rightOptional) {
    const float rightNullFraction =
        JoinConstraints::computeNullFraction(rightKeys, leftKeys, constraints_);
    JoinConstraints::updatePayload(
        right->columns(), rightKeys, rightNullFraction, constraints_);
  }

  // Scale non-key payload cardinalities when the join eliminates rows.
  // The preserved side of outer joins keeps all rows, so its NDV is unchanged.
  const bool scaleLeft =
      leftOptional || isInnerJoin(joinType) || isLeftSemiFilterJoin(joinType);
  if (scaleLeft) {
    const float leftSelectivity = std::min(1.0f, fanout) * filterSelectivity;
    JoinConstraints::scalePayloadCardinality(
        input_->columns(),
        leftKeys,
        leftSelectivity,
        inputCardinality(),
        constraints_);
  }

  const bool scaleRight =
      rightOptional || isInnerJoin(joinType) || isRightSemiFilterJoin(joinType);
  if (scaleRight) {
    const float rightSelectivity = std::min(1.0f, rlFanout) * filterSelectivity;
    JoinConstraints::scalePayloadCardinality(
        right->columns(),
        rightKeys,
        rightSelectivity,
        right->resultCardinality(),
        constraints_);
  }
}

void Join::initCost(float fanout, float rlFanout, float filterSelectivity) {
  cost_.inputCardinality = inputCardinality();
  const float rightToLeftRatio =
      right->resultCardinality() / cost_.inputCardinality;
  cost_.fanout = adjustFanoutForJoinType(
      joinType,
      fanout * filterSelectivity,
      rlFanout * filterSelectivity,
      rightToLeftRatio);

  const float buildSize = right->resultCardinality();
  const auto numKeys = leftKeys.size();
  const auto probeCost = Costs::hashTableCost(buildSize) +
      // Multiply by min(fanout, 1) because most misses will not compare and
      // if fanout > 1, there is still only one compare.
      (Costs::kKeyCompareCost * numKeys * std::min<float>(1, cost_.fanout)) +
      numKeys * Costs::kHashColumnCost;

  const auto rowBytes = byteSize(right->columns());
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
    velox::core::JoinType joinType,
    ExprVector filter,
    ColumnVector columns) {
  float fanout = right->resultCardinality();
  float rlFanout = input->resultCardinality();
  return make<Join>(
      JoinMethod::kCross,
      joinType,
      /*nullAware=*/false,
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

  // Repartition projects all input columns.
  constraints_ = input_->constraints();
}

void Repartition::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

namespace {
ColumnVector concatColumns(
    const ExprVector& lhs,
    const ColumnVector& rhs,
    ColumnCP ordinalityColumn) {
  ColumnVector result;
  result.reserve(lhs.size() + rhs.size() + (ordinalityColumn ? 1 : 0));
  for (const auto& expr : lhs) {
    result.push_back(expr->as<Column>());
  }
  result.insert(result.end(), rhs.begin(), rhs.end());
  if (ordinalityColumn) {
    result.push_back(ordinalityColumn);
  }
  return result;
}
} // namespace

Unnest::Unnest(
    RelationOpPtr input,
    ExprVector replicateColumns,
    ExprVector unnestExprs,
    ColumnVector unnestedColumns,
    ColumnCP ordinalityColumn)
    : RelationOp{RelType::kUnnest, std::move(input), concatColumns(replicateColumns, unnestedColumns, ordinalityColumn)},
      replicateColumns{std::move(replicateColumns)},
      unnestExprs{std::move(unnestExprs)},
      unnestedColumns{std::move(unnestedColumns)},
      ordinalityColumn{ordinalityColumn} {
  cost_.inputCardinality = inputCardinality();

  // Use a heuristic for average array/map size.
  // TODO Compute fanout from unnest expression array size statistics when
  // available.
  cost_.fanout = 10;

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

  // Add constraints for unnested columns.
  // These come from array/map elements - we don't have detailed info,
  // so use default constraints from the column's value.
  for (auto* column : unnestedColumns) {
    constraints_.emplace(column->id(), column->value());
  }

  // Add constraint for ordinality column if present.
  // Ordinality is non-null. Use fanout as an approximation for cardinality.
  if (ordinalityColumn) {
    Value ordValue(ordinalityColumn->value().type, cost_.fanout);
    ordValue.nullable = false;
    constraints_.emplace(ordinalityColumn->id(), ordValue);
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
    double cardinality = tableCardinality(table);
    if (cardinality > maxCardinality) {
      maxCardinality = cardinality;
      largestTable = table;
    }
  });

  return largestTable;
}

// Computes the maximum cardinality estimate for aggregation grouping keys
// using saturating product to avoid overflow. When multiple keys come from
// the same table, cap the maximum cardinality estimate at table's
// cardinality.
double maxGroups(
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
    const double maxCardinality = tableCardinality(table);

    maxTableCardinality = std::max(maxTableCardinality, maxCardinality);

    double groupCardinality;
    if (keys.size() == 1) {
      // Single key per table: use min of key cardinality and table
      // cardinality.
      groupCardinality = std::min<double>(
          constraints.at(keys[0]->id()).cardinality, maxCardinality);
    } else {
      // Multiple keys: collect cardinalities and use saturatingProduct.
      std::vector<double> keyCardinalities;
      keyCardinalities.reserve(keys.size());
      for (auto key : keys) {
        keyCardinalities.push_back(constraints.at(key->id()).cardinality);
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
    ColumnVector columns)
    : RelationOp{RelType::kAggregation, std::move(input), std::move(columns)},
      groupingKeys{std::move(_groupingKeys)},
      aggregates{std::move(_aggregates)},
      step{step},
      preGroupedKeys{std::move(_preGroupedKeys)} {
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

  initConstraints();
}

void Aggregation::initConstraints() {
  float outputCardinality = resultCardinality();
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

    constraint.cardinality =
        std::min(constraint.cardinality, outputCardinality);

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

void Aggregation::setCostWithGroups(int64_t inputBeforePartial) {
  auto* optimization = queryCtx()->optimization();
  const auto& runnerOptions = optimization->runnerOptions();

  const auto maxCardinality =
      std::max<double>(1, maxGroups(groupingKeys, input_->constraints()));

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

    // numGroups can be > inputCardinality since this is calculated against
    // the input before partial and inputCardinality is scaled down by partial
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
  // second time to insert.
  cost_.unitCost = (numKeys * 2 * Costs::kHashColumnCost) +
      Costs::hashBuildCost(cost_.inputCardinality, rowBytes) +
      numKeys * Costs::kKeyCompareCost +
      numColumns * Costs::kHashExtractColumnCost * 2;

  cost_.totalBytes = cost_.inputCardinality * rowBytes;

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

  // Compute selectivity using filter analysis, updating constraints_
  auto selectivity = conjunctsSelectivity(constraints_, exprs_, true);
  cost_.fanout = selectivity.trueFraction;
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

  // TODO Fill in cost_.unitCost and others.
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

  // OrderBy projects all input columns. When there's a LIMIT, keeping N of M
  // rows is equivalent to sampling with fraction s = N/M. Use the coupon
  // collector formula to estimate the number of distinct values surviving the
  // sampling.
  if (cost_.fanout >= 1.0) {
    constraints_ = input_->constraints();
  } else {
    const auto fraction = cost_.fanout;
    for (const auto& [columnId, constraint] : input_->constraints()) {
      Value adjusted = constraint;
      adjusted.cardinality = std::max(
          1.0,
          sampledNdv(constraint.cardinality, cost_.inputCardinality, fraction));
      constraints_.emplace(columnId, adjusted);
    }
  }
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
    // Input cardinality does not exceed the limit. The limit is no-op.
    // Doesn't change cardinality.
    cost_.fanout = 1;
  } else {
    // Input cardinality exceeds the limit. Calculate fanout to ensure that
    // fanout * limit = input-cardinality.
    cost_.fanout = cardinality / cost_.inputCardinality;
  }

  // Limit projects all input columns. Keeping N of M rows is equivalent to
  // sampling with fraction s = N/M. Use the coupon collector formula to
  // estimate the number of distinct values surviving the sampling.
  const auto fraction = cost_.fanout;
  for (const auto& [columnId, constraint] : input_->constraints()) {
    Value adjusted = constraint;
    adjusted.cardinality = std::max(
        1.0,
        sampledNdv(constraint.cardinality, cost_.inputCardinality, fraction));
    constraints_.emplace(columnId, adjusted);
  }
}

void Limit::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}

UnionAll::UnionAll(RelationOpPtrVector inputsVector)
    : RelationOp{RelType::kUnionAll, nullptr, Distribution{}, inputsVector[0]->columns()},
      inputs{std::move(inputsVector)} {
  cost_.inputCardinality = 0;
  for (auto& input : inputs) {
    cost_.inputCardinality +=
        input->cost().inputCardinality * input->cost().fanout;
  }

  cost_.fanout = 1;

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
    float totalRows = inputs[0]->resultCardinality();
    float weightedNullFraction = combined.nullFraction * totalRows;
    float weightedTrueFraction = combined.trueFraction != Value::kUnknown
        ? combined.trueFraction * totalRows
        : 0;
    bool anyUnknownTrueFraction = combined.trueFraction == Value::kUnknown;

    // Combine constraints from remaining inputs.
    for (size_t j = 1; j < inputs.size(); ++j) {
      const Value& inputValue = inputConstraint(j);
      float inputRows = inputs[j]->resultCardinality();

      totalRows += inputRows;
      // Sum cardinalities as upper bound for distinct count.
      combined.cardinality += inputValue.cardinality;
      weightedNullFraction += inputValue.nullFraction * inputRows;
      if (inputValue.trueFraction == Value::kUnknown) {
        anyUnknownTrueFraction = true;
      } else {
        weightedTrueFraction += inputValue.trueFraction * inputRows;
      }
      combined.nullable = combined.nullable || inputValue.nullable;

      // Combine min/max (use nullptr if any is unknown).
      if (combined.min != nullptr && inputValue.min != nullptr) {
        if (*inputValue.min < *combined.min) {
          combined.min = inputValue.min;
        }
      } else {
        combined.min = nullptr;
      }

      if (combined.max != nullptr && inputValue.max != nullptr) {
        if (*combined.max < *inputValue.max) {
          combined.max = inputValue.max;
        }
      } else {
        combined.max = nullptr;
      }
    }

    combined.nullFraction =
        totalRows > 0 ? weightedNullFraction / totalRows : 0;
    combined.trueFraction = anyUnknownTrueFraction
        ? Value::kUnknown
        : (totalRows > 0 ? weightedTrueFraction / totalRows : 0);

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
  cost_.unitCost = 0.01;
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
    float inputCardinality,
    std::optional<int32_t> limit) {
  auto value = outputColumn->value();
  value.cardinality = limit.has_value()
      ? std::min(static_cast<float>(limit.value()), inputCardinality)
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
              input->distribution().distributionType(),
              input->distribution().partitionKeys(),
              input->distribution().orderKeys(),
              input->distribution().orderTypes(),
              input->distribution().numKeysUnique(),
              ExprVector{uniqueIdColumn}),
          appendColumn(input->columns(), uniqueIdColumn)),
      uniqueIdColumn_(uniqueIdColumn) {
  // Fanout is 1 (cardinality neutral).
  cost_.fanout = 1;
  cost_.unitCost = 0.01; // Minimal cost for generating unique IDs.

  // Copy all input constraints (AssignUniqueId projects all input columns).
  constraints_ = input_->constraints();

  // Add constraint for the uniqueId column: one unique value per row,
  // non-null.
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
  cost_.fanout = 1;
  cost_.unitCost = 0.01; // Minimal cost for enforcing distinctness.

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
  cost_.unitCost =
      (inputsSorted ? 0 : sortCost(numKeys, cost_.inputCardinality)) +
      windowFunctions.size() * Costs::kSimpleAggregateCost;

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
  // No sorting, just hashing by partition keys.
  cost_.unitCost = Costs::kHashColumnCost * partitionKeys.size() +
      Costs::hashTableCost(cost_.inputCardinality);

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
  const auto numKeys = partitionKeys.size() + orderKeys.size();
  cost_.unitCost = Costs::kHashColumnCost * partitionKeys.size() +
      sortCost(numKeys, cost_.inputCardinality);

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

namespace {

// Builds the output columns for GroupId from grouping key infos, aggregation
// inputs, and the group ID column.
ColumnVector makeGroupIdColumns(
    const QGVector<GroupId::GroupingKeyInfo>& groupingKeyInfos,
    const ExprVector& aggregationInputs,
    ColumnCP groupIdColumn) {
  ColumnVector columns;
  for (const auto& keyInfo : groupingKeyInfos) {
    VELOX_CHECK(
        keyInfo.input->is(PlanType::kColumnExpr),
        "GroupId key input must be a Column");
    columns.push_back(keyInfo.input->as<Column>());
  }
  for (auto* expr : aggregationInputs) {
    VELOX_CHECK(
        expr->is(PlanType::kColumnExpr),
        "GroupId aggregation input must be a Column");
    columns.push_back(expr->as<Column>());
  }
  columns.push_back(groupIdColumn);
  return columns;
}

// Returns a Value for the group ID column: non-nullable integer in
// [0, numGroupingSets - 1].
Value makeGroupIdColumnValue(ColumnCP groupIdColumn, size_t numGroupingSets) {
  Value value(groupIdColumn->value().type, numGroupingSets);
  value.nullable = false;
  value.min = registerVariant(int64_t{0});
  value.max = registerVariant(static_cast<int64_t>(numGroupingSets - 1));
  return value;
}

} // namespace

GroupId::GroupId(
    RelationOpPtr input,
    QGVector<QGVector<int32_t>> groupingSets,
    QGVector<GroupingKeyInfo> groupingKeyInfos,
    ExprVector aggregationInputs,
    ColumnCP groupIdColumn)
    : RelationOp(
          RelType::kGroupId,
          std::move(input),
          makeGroupIdColumns(
              groupingKeyInfos,
              aggregationInputs,
              groupIdColumn)),
      groupingSets_(std::move(groupingSets)),
      groupingKeyInfos_(std::move(groupingKeyInfos)),
      aggregationInputs_(std::move(aggregationInputs)),
      groupIdColumn_(groupIdColumn) {
  VELOX_CHECK_GE(
      groupingSets_.size(), 2, "GroupId requires two or more grouping sets.");

  // Fanout equals the number of grouping sets since each input row is
  // duplicated once per set.
  cost_.fanout = groupingSets_.size();
  // Minimal per-row cost scaled by the number of grouping sets.
  cost_.unitCost = 0.01 * groupingSets_.size();

  constraints_ = input_->constraints();
  constraints_.emplace(
      groupIdColumn_->id(),
      makeGroupIdColumnValue(groupIdColumn_, groupingSets_.size()));
}

void GroupId::accept(
    const RelationOpVisitor& visitor,
    RelationOpVisitorContext& context) const {
  visitor.visit(*this, context);
}
} // namespace facebook::axiom::optimizer
