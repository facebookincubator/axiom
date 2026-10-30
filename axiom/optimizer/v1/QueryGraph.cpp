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

#include <ranges>

#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/v1/DerivedTable.h"
#include "axiom/optimizer/v1/Optimization.h"
#include "axiom/optimizer/v1/QueryGraph.h"
#include "velox/expression/ScopedVarSetter.h"

namespace facebook::axiom::optimizer::v1 {

const WindowPlan* WindowPlan::withFunctions(
    QGVector<WindowFunctionCP> functions,
    ColumnVector columns) const {
  auto merged = functions_;
  auto mergedColumns = columns_;
  merged.insert(merged.end(), functions.begin(), functions.end());
  mergedColumns.insert(mergedColumns.end(), columns.begin(), columns.end());
  return make<WindowPlan>(
      std::move(merged), std::move(mergedColumns), rankingLimit_);
}

namespace {
// Verifies that all tables referenced by 'exprs' are in dt.tableSet or 'dt'
// itself.
template <typename T>
void checkTableReferences(
    const DerivedTable& dt,
    std::span<const T> exprs,
    const char* context) {
  for (auto* expr : exprs) {
    expr->allTables().forEach([&](PlanObjectCP table) {
      VELOX_CHECK(
          dt.tableSet.contains(table) || table == &dt,
          "{} references table not in tableSet or 'this': {}, {}",
          context,
          dt.cname,
          expr->toString());
    });
  }
}
} // namespace

AggregationPlan::AggregationPlan(
    ExprVector groupingKeys,
    AggregateVector aggregates,
    ColumnVector columns,
    ColumnVector intermediateColumns,
    GroupingSets groupingSets,
    ColumnCP groupId)
    : PlanObject(PlanType::kAggregationNode),
      groupingKeys_(std::move(groupingKeys)),
      aggregates_(std::move(aggregates)),
      columns_(std::move(columns)),
      intermediateColumns_(std::move(intermediateColumns)),
      groupingSets_(std::move(groupingSets)),
      groupId_(groupId) {
  const auto expectedSize =
      groupingKeys_.size() + aggregates_.size() + (groupId_ != nullptr ? 1 : 0);
  VELOX_CHECK_EQ(expectedSize, columns_.size());
  VELOX_CHECK_EQ(columns_.size(), intermediateColumns_.size());

  VELOX_CHECK_EQ(
      groupingSets_.empty(),
      groupId_ == nullptr,
      "groupingSets and groupId must both be set or both be empty");

  for (const auto& set : groupingSets_) {
    folly::F14FastSet<int32_t> seen;
    for (const auto idx : set) {
      VELOX_CHECK_LT(
          idx,
          groupingKeys_.size(),
          "Grouping set index out of bounds: index={}, numKeys={}",
          idx,
          groupingKeys_.size());
      VELOX_CHECK(
          seen.insert(idx).second, "Duplicate index in grouping set: {}", idx);
    }
  }
}

void AggregationPlan::checkConsistency(const DerivedTable& dt) const {
  checkTableReferences<ExprCP>(dt, groupingKeys_, "Grouping key");
  checkTableReferences<AggregateCP>(dt, aggregates_, "Aggregate");

  // Grouping key columns reference one of dt.tables or 'dt' itself.
  // Aggregate columns must reference the 'dt'.
  const auto numKeys = groupingKeys_.size();
  checkTableReferences<ColumnCP>(
      dt, std::span(columns_).subspan(0, numKeys), "Grouping key");
  for (size_t i = numKeys; i < columns_.size(); ++i) {
    auto* relation = columns_[i]->relation();
    VELOX_CHECK(
        relation == &dt,
        "Aggregate column does not reference DT: {}, {}",
        dt.cname,
        columns_[i]->toString());
  }
}

void WindowPlan::checkConsistency(const DerivedTable& dt) const {
  checkTableReferences<WindowFunctionCP>(dt, functions_, "Window function");

  // Window output columns must reference the 'dt'.
  for (auto* column : columns_) {
    VELOX_CHECK(
        column->relation() == &dt,
        "Window column does not reference DT: {}, {}",
        dt.cname,
        column->toString());
  }
}

std::string ValuesTable::toString() const {
  std::stringstream out;
  out << "{" << PlanObject::toString() << cname << "}";
  return out.str();
}

std::string UnnestTable::toString() const {
  std::stringstream out;
  out << "{" << PlanObject::toString() << cname << "}";
  return out.str();
}

namespace {
// Returns the JoinType for the left side of a join. The left side is never
// semi or anti, so the result is always one of kInner, kLeft, kRight, kFull.
velox::core::JoinType leftSideJoinType(velox::core::JoinType joinType) {
  switch (joinType) {
    case velox::core::JoinType::kLeft:
      return velox::core::JoinType::kRight;
    case velox::core::JoinType::kRight:
      return velox::core::JoinType::kLeft;
    case velox::core::JoinType::kFull:
      return velox::core::JoinType::kFull;
    default:
      return velox::core::JoinType::kInner;
  }
}
} // namespace

JoinSide JoinEdge::sideOf(PlanObjectCP side, bool other) const {
  if ((side == rightTable_ && !other) || (side == leftTable_ && other)) {
    return {
        rightTable_,
        rightKeys_,
        lrFanout_,
        joinType_,
        markColumn_,
        rightUnique_,
        rightColumns_,
        rightExprs_};
  }

  return {
      leftTable_,
      leftKeys_,
      rlFanout_,
      leftSideJoinType(joinType_),
      markColumn_,
      leftUnique_,
      leftColumns_,
      leftExprs_};
}

bool JoinEdge::isBroadcastableType() const {
  // Counting joins cannot use broadcast because each worker gets its own copy
  // of build-side per-key counters. With broadcast, multiple workers decrement
  // independent copies, producing too many output rows.
  return !leftOptional() && !isCounting();
}

void JoinEdge::addEquality(ExprCP left, ExprCP right, bool update) {
  // Drop equivalence-class-redundant keys; sound via attachPredicate's
  // always-attaches invariant.
  for (size_t i = 0; i < leftKeys_.size(); ++i) {
    if (leftKeys_[i]->sameOrEqual(*left) &&
        rightKeys_[i]->sameOrEqual(*right)) {
      return;
    }
  }
  leftKeys_.push_back(left);
  rightKeys_.push_back(right);
  if (update) {
    guessFanout();
  }
}

JoinEdge* JoinEdge::reverse(JoinEdge& join) {
  VELOX_CHECK(join.isInner(), "JoinEdge::reverse only supports inner joins");

  auto* reversed = JoinEdge::makeInner(join.rightTable_, join.leftTable_);

  // Swap the join keys
  for (auto i = 0; i < join.numKeys(); ++i) {
    reversed->addEquality(join.rightKeys_[i], join.leftKeys_[i], false);
  }

  // Swap the fanouts.
  reversed->setFanouts(join.rlFanout_, join.lrFanout_);

  return reversed;
}

QGVector<int32_t> AggregationPlan::globalGroupingSets() const {
  QGVector<int32_t> result;
  for (int32_t i = 0; i < groupingSets_.size(); ++i) {
    if (groupingSets_[i].empty()) {
      result.push_back(i);
    }
  }
  return result;
}

std::pair<std::string, bool> JoinEdge::sampleKey() const {
  if (!leftTable_ || leftTable_->isNot(PlanType::kTableNode) ||
      rightTable_->isNot(PlanType::kTableNode)) {
    return std::make_pair("", false);
  }
  velox::ScopedVarSetter pref(&queryCtx()->cnamesInExpr(), false);
  std::vector<int32_t> indices(leftKeys_.size());
  std::iota(indices.begin(), indices.end(), 0);
  std::vector<std::string> leftString;
  for (auto& k : leftKeys_) {
    leftString.push_back(k->toString());
  }
  std::ranges::sort(indices, [&](int32_t l, int32_t r) {
    return leftString[l] < leftString[r];
  });
  auto left =
      fmt::format("{} ", leftTable_->as<BaseTable>()->schemaTable->name());
  auto right =
      fmt::format("{} ", rightTable_->as<BaseTable>()->schemaTable->name());
  for (auto i : indices) {
    left += leftKeys_[i]->toString() + " ";
    right += rightKeys_[i]->toString() + " ";
  }
  if (left < right) {
    return std::make_pair(left + " " + right, false);
  }
  return std::make_pair(right + " " + left, true);
}

std::string JoinEdge::toString() const {
  std::stringstream out;
  out << "<join " << (leftTable_ ? cname(leftTable_) : " multiple tables ");
  switch (joinType_) {
    case velox::core::JoinType::kFull:
      out << " full outer ";
      break;
    case velox::core::JoinType::kLeftSemiProject:
      out << " exists project ";
      break;
    case velox::core::JoinType::kLeft:
      out << " left ";
      break;
    case velox::core::JoinType::kLeftSemiFilter:
    case velox::core::JoinType::kCountingLeftSemiFilter:
      out << " exists ";
      break;
    case velox::core::JoinType::kAnti:
    case velox::core::JoinType::kCountingAnti:
      out << " not exists ";
      break;
    case velox::core::JoinType::kRight:
      out << " right ";
      break;
    default:
      if (directed_) {
        out << " unnest ";
      } else {
        out << " inner ";
      }
      break;
  }
  out << cname(rightTable_);
  out << " on ";
  for (size_t i = 0; i < leftKeys_.size(); ++i) {
    if (i > 0) {
      out << " and ";
    }
    out << leftKeys_[i]->toString();
    if (i < rightKeys_.size()) {
      out << " = " << rightKeys_[i]->toString();
    }
  }
  if (!filter_.empty()) {
    out << " filter " << conjunctsToString(filter_);
  }
  if (rowNumberColumn_) {
    out << " row# " << rowNumberColumn_->toString();
  }
  out << ">";
  return out.str();
}

PlanObjectSet JoinEdge::allTables() const {
  PlanObjectSet set;

  for (const auto* key : leftKeys_) {
    set.unionSet(key->allTables());
  }

  for (const auto* key : rightKeys_) {
    set.unionSet(key->allTables());
  }

  for (const auto* conjunct : filter_) {
    set.unionSet(conjunct->allTables());
  }

  return set;
}

namespace {

struct JoinFanout {
  std::optional<float> fanout;
  bool unique{false};
};

// Estimates the number of matching rows per equality lookup on 'keys' given
// 'scanCardinality' total rows. For each key pair, divides by
// max(ndv(thisKey), ndv(otherKey)) to account for values on the probe side
// that have no match on the scan side. Returns nullopt (unknown) if the scan
// cardinality or any join-key NDV is unknown.
std::optional<float> estimateFanout(
    std::optional<float> scanCardinality,
    const ExprVector& keys,
    const ExprVector& otherKeys) {
  if (!scanCardinality.has_value()) {
    return std::nullopt;
  }
  if (keys.empty()) {
    return scanCardinality;
  }

  for (size_t i = 0; i < keys.size(); ++i) {
    const auto thisNdv = keys[i]->value().cardinality;
    const auto otherNdv = otherKeys[i]->value().cardinality;
    // An unknown join-key NDV makes the fanout unknown.
    if (!thisNdv.has_value() || !otherNdv.has_value()) {
      return std::nullopt;
    }
    if (*thisNdv == 0 || *otherNdv == 0) {
      return 0;
    }
  }

  auto fanout = *scanCardinality /
      std::max(
          *keys[0]->value().cardinality, *otherKeys[0]->value().cardinality);
  for (size_t i = 1; i < keys.size(); ++i) {
    auto distinctValues = std::max(
        *keys[i]->value().cardinality, *otherKeys[i]->value().cardinality);
    if (distinctValues > fanout) {
      fanout = 1;
    } else {
      fanout /= distinctValues;
    }
  }
  return fanout;
}

// Estimates the number of matching rows per equality lookup on 'keys' for
// 'table'. BaseTable uses column statistics via estimateFanout and checks
// uniqueness via SchemaTable::isUnique(). DerivedTable uses its post-planning
// cardinality and marks unique = true when 'keys' cover all grouping keys.
// Values/Unnest use their cardinality with unique = false.
JoinFanout joinFanout(
    PlanObjectCP table,
    const ExprVector& keys,
    const ExprVector& otherKeys) {
  if (table->is(PlanType::kTableNode)) {
    auto schemaTable = table->as<BaseTable>()->schemaTable;

    auto fanout = estimateFanout(schemaTable->cardinality, keys, otherKeys);

    const bool allColumns =
        std::all_of(keys.begin(), keys.end(), [](ExprCP key) {
          return key->is(PlanType::kColumnExpr);
        });

    bool unique = false;
    if (allColumns) {
      CPSpan<Column> columns(
          reinterpret_cast<ColumnCP const*>(keys.data()), keys.size());
      unique = schemaTable->isUnique(columns);
    }

    return {fanout, unique};
  }

  if (table->is(PlanType::kDerivedTableNode)) {
    const auto* dt = table->as<DerivedTable>();
    return {
        .fanout = estimateFanout(dt->cardinality(), keys, otherKeys),
        .unique = dt->aggregation &&
            keys.size() >= dt->aggregation->groupingKeys().size(),
    };
  }

  VELOX_CHECK(table->isTable());
  const auto cardinality = table->as<TableObject>()->cardinality();
  return {
      .fanout = estimateFanout(cardinality, keys, otherKeys), .unique = false};
}

std::optional<float> baseSelectivity(PlanObjectCP object) {
  if (object->is(PlanType::kTableNode)) {
    auto* baseTable = object->as<BaseTable>();
    const auto baseCardinality = baseTable->schemaTable->cardinality;
    const auto filteredCardinality = baseTable->filteredCardinality;
    // An unknown filtered cardinality makes the selectivity unknown.
    if (!filteredCardinality.has_value()) {
      return std::nullopt;
    }
    // filteredCardinality can exceed baseCardinality when the connector
    // returns inconsistent statistics, e.g. a known `Table::numRows()`
    // disagreeing with the partition row count from `co_estimateStats`.
    if (baseCardinality.has_value() &&
        *baseCardinality > *filteredCardinality) {
      return *filteredCardinality / *baseCardinality;
    }
  }
  return 1;
}
} // namespace

void JoinEdge::guessFanout() {
  if (fanoutsFixed_) {
    return;
  }

  if (leftTable_ == nullptr || leftKeys_.empty()) {
    lrFanout_ = 1.1;
    rlFanout_ = 1;
    return;
  }

  auto* opt = queryCtx()->optimization();
  const auto& options = opt->options();

  auto left = joinFanout(leftTable_, leftKeys_, rightKeys_);
  auto right = joinFanout(rightTable_, rightKeys_, leftKeys_);
  leftUnique_ = left.unique;
  rightUnique_ = right.unique;

  // PK-FK ratio uses raw (pre-filter) cardinality for BaseTable so that the
  // filter is re-applied exactly once via `baseSelectivity`. For other table
  // kinds, only post-filter cardinality exists and `baseSelectivity` returns
  // 1, so the post-filter value is what feeds the ratio.
  auto pkFkRatioCardinality = [](PlanObjectCP table) -> std::optional<float> {
    if (table->is(PlanType::kTableNode)) {
      return table->as<BaseTable>()->schemaTable->cardinality;
    }
    return table->as<TableObject>()->cardinality();
  };

  // If one side has unique join keys, this is a primary key (PK) to foreign
  // key (FK) join. For example, joining orders (PK: orderkey) with lineitem
  // (FK: orderkey), if orders is the left table, then leftUnique_ is true: each
  // lineitem matches at most one order (rlFanout ≤ 1), while each order may
  // match many lineitems (lrFanout = cardLineitem / cardOrders). When both
  // sides are unique (1:1 join), leftUnique takes precedence.
  if (leftUnique_) {
    rlFanout_ = mul(left.fanout, baseSelectivity(leftTable_));
    lrFanout_ =
        mul(divide(
                pkFkRatioCardinality(rightTable_),
                pkFkRatioCardinality(leftTable_)),
            baseSelectivity(rightTable_));
  } else if (rightUnique_) {
    lrFanout_ = mul(right.fanout, baseSelectivity(rightTable_));
    rlFanout_ =
        mul(divide(
                pkFkRatioCardinality(leftTable_),
                pkFkRatioCardinality(rightTable_)),
            baseSelectivity(leftTable_));
  } else {
    auto [sampledLeftFanout, sampledRightFanout] = options.sampleJoins
        ? opt->history().sampleJoin(this)
        : std::pair<float, float>(0, 0);
    if (sampledLeftFanout == 0 && sampledRightFanout == 0) {
      lrFanout_ = mul(right.fanout, baseSelectivity(rightTable_));
      rlFanout_ = mul(left.fanout, baseSelectivity(leftTable_));
    } else {
      lrFanout_ =
          mul(std::optional<float>(sampledRightFanout),
              baseSelectivity(rightTable_));
      rlFanout_ = mul(
          std::optional<float>(sampledLeftFanout), baseSelectivity(leftTable_));
    }
  }
}

} // namespace facebook::axiom::optimizer::v1
