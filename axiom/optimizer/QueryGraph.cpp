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

#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/PlanUtils.h"
#include "velox/expression/ScopedVarSetter.h"

namespace lp = facebook::axiom::logical_plan;

namespace facebook::axiom::optimizer {

// static
const char* SpecialFormCallNames::kAnd = "__and";
// static
const char* SpecialFormCallNames::kOr = "__or";
// static
const char* SpecialFormCallNames::kCast = "__cast";
// static
const char* SpecialFormCallNames::kTryCast = "__try_cast";
// static
const char* SpecialFormCallNames::kTry = "__try";
// static
const char* SpecialFormCallNames::kCoalesce = "__coalesce";
// static
const char* SpecialFormCallNames::kIf = "__if";
// static
const char* SpecialFormCallNames::kSwitch = "__switch";
// static
const char* SpecialFormCallNames::kIn = "__in";

void Column::equals(ColumnCP other) const {
  if (!equivalence_ && !other->equivalence_) {
    auto* equiv = make<Equivalence>();
    equiv->columns.push_back(this);
    equiv->columns.push_back(other);
    equivalence_ = equiv;
    other->equivalence_ = equiv;
    return;
  }
  if (!other->equivalence_) {
    other->equivalence_ = equivalence_;
    equivalence_->columns.push_back(other);
    return;
  }
  if (!equivalence_) {
    other->equals(this);
    return;
  }
  for (auto& column : other->equivalence_->columns) {
    equivalence_->columns.push_back(column);
    column->equivalence_ = equivalence_;
  }
}

Name cname(PlanObjectCP relation) {
  switch (relation->type()) {
    case PlanType::kTableNode:
      return relation->as<BaseTable>()->cname;
    case PlanType::kValuesTableNode:
      return relation->as<ValuesTable>()->cname;
    case PlanType::kUnnestTableNode:
      return relation->as<UnnestTable>()->cname;
    case PlanType::kDerivedTableNode:
      return relation->as<DerivedTable>()->cname;
    default:
      VELOX_UNREACHABLE("Unexpected relation: {}", relation->typeName());
  }
}

float tableCardinality(PlanObjectCP table) {
  if (table->is(PlanType::kTableNode)) {
    return table->as<BaseTable>()->schemaTable->cardinality;
  }
  if (table->is(PlanType::kValuesTableNode)) {
    return table->as<ValuesTable>()->cardinality();
  }

  if (table->is(PlanType::kUnnestTableNode)) {
    return table->as<UnnestTable>()->cardinality();
  }

  VELOX_CHECK(table->is(PlanType::kDerivedTableNode));
  return table->as<DerivedTable>()->cardinality;
}

std::string Column::toString() const {
  const auto* opt = queryCtx()->optimization();
  if (!opt->cnamesInExpr() || relation_ == nullptr) {
    return name_;
  }

  return fmt::format("{}.{}", cname(relation_), name_);
}

Call::Call(
    PlanType type,
    Name name,
    const Value& value,
    ExprVector args,
    FunctionSet functions)
    : Expr(type, value),
      name_(name),
      args_(std::move(args)),
      functions_(functions),
      metadata_(functionMetadata(name_)) {
  for (auto arg : args_) {
    columns_.unionSet(arg->columns());
    subexpressions_.unionSet(arg->subexpressions());
    subexpressions_.add(arg);
  }
}

std::string Call::toString() const {
  std::stringstream out;
  out << name_ << "(";
  for (auto i = 0; i < args_.size(); ++i) {
    out << args_[i]->toString() << (i == args_.size() - 1 ? ")" : ", ");
  }
  return out.str();
}

std::string Aggregate::toString() const {
  std::stringstream out;
  out << name() << "(";

  if (isDistinct_) {
    out << "DISTINCT ";
  }

  for (auto i = 0; i < args().size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << args()[i]->toString();
  }

  if (!orderKeys_.empty()) {
    out << " ORDER BY " << orderByToString(orderKeys_, orderTypes_);
  }

  out << ")";

  if (condition_) {
    out << " FILTER (WHERE " << condition_->toString() << ")";
  }

  return out.str();
}

const Aggregate* Aggregate::dropDistinct() const {
  if (!isDistinct_) {
    return this;
  }
  return make<Aggregate>(
      name(),
      value_,
      args(),
      functions(),
      /*isDistinct=*/false,
      condition_,
      intermediateType_,
      orderKeys_,
      orderTypes_);
}

std::string WindowFunction::toString() const {
  std::stringstream out;
  out << name() << "(";

  for (auto i = 0; i < args().size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << args()[i]->toString();
  }

  out << ") OVER (";

  if (!partitionKeys_.empty()) {
    out << "PARTITION BY ";
    for (auto i = 0; i < partitionKeys_.size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << partitionKeys_[i]->toString();
    }
  }

  if (!orderKeys_.empty()) {
    if (!partitionKeys_.empty()) {
      out << " ";
    }
    out << "ORDER BY " << orderByToString(orderKeys_, orderTypes_);
  }

  out << " " << lp::WindowExpr::toName(frame_.type) << " BETWEEN ";
  if (frame_.startValue) {
    out << frame_.startValue->toString() << " ";
  }
  out << lp::WindowExpr::toName(frame_.startType) << " AND ";
  if (frame_.endValue) {
    out << frame_.endValue->toString() << " ";
  }
  out << lp::WindowExpr::toName(frame_.endType);

  out << ")";
  return out.str();
}

std::string Field::toString() const {
  std::stringstream out;
  out << base_->toString() << ".";
  if (field_) {
    out << field_;
  } else {
    out << fmt::format("{}", index_);
  }
  return out.str();
}

std::optional<PathSet> SubfieldSet::findSubfields(int32_t id) const {
  for (auto i = 0; i < ids.size(); ++i) {
    if (ids[i] == id) {
      return subfields[i];
    }
  }
  return std::nullopt;
}

void BaseTable::addJoinedBy(JoinEdgeP join) {
  pushBackUnique(joinedBy, join);
}

std::optional<int32_t> BaseTable::columnId(Name column) const {
  for (auto i = 0; i < columns.size(); ++i) {
    if (columns[i]->name() == column) {
      return columns[i]->id();
    }
  }
  return std::nullopt;
}

PathSet BaseTable::columnSubfields(int32_t id) const {
  PathSet subfields;
  if (auto maybe = payloadSubfields.findSubfields(id)) {
    subfields = maybe.value();
  }
  if (auto maybe = controlSubfields.findSubfields(id)) {
    subfields.unionSet(maybe.value());
  }

  Path::subfieldSkyline(subfields);
  return subfields;
}

std::string BaseTable::toString() const {
  std::stringstream out;
  out << "{" << PlanObject::toString();
  out << schemaTable->name() << " " << cname << "}";
  return out.str();
}

void ValuesTable::addJoinedBy(JoinEdgeP join) {
  pushBackUnique(joinedBy, join);
}

std::string ValuesTable::toString() const {
  std::stringstream out;
  out << "{" << PlanObject::toString() << cname << "}";
  return out.str();
}

void UnnestTable::addJoinedBy(JoinEdgeP join) {
  pushBackUnique(joinedBy, join);
}

std::string UnnestTable::toString() const {
  std::stringstream out;
  out << "{" << PlanObject::toString() << cname << "}";
  return out.str();
}

JoinSide JoinEdge::sideOf(PlanObjectCP side, bool other) const {
  if ((side == rightTable_ && !other) || (side == leftTable_ && other)) {
    return {
        rightTable_,
        rightKeys_,
        lrFanout_,
        rightOptional_,
        leftOptional_,
        rightExists_,
        rightNotExists_,
        markColumn_,
        rightUnique_,
        rightColumns_,
        rightExprs_};
  }

  return {
      leftTable_,
      leftKeys_,
      rlFanout_,
      leftOptional_,
      rightOptional_,
      false,
      false,
      markColumn_,
      leftUnique_,
      leftColumns_,
      leftExprs_};
}

bool JoinEdge::isBroadcastableType() const {
  return !leftOptional_;
}

void JoinEdge::addEquality(ExprCP left, ExprCP right, bool update) {
  for (auto i = 0; i < leftKeys_.size(); ++i) {
    if (leftKeys_[i] == left && rightKeys_[i] == right) {
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

std::pair<std::string, bool> JoinEdge::sampleKey() const {
  if (!leftTable_ || leftTable_->isNot(PlanType::kTableNode) ||
      rightTable_->isNot(PlanType::kTableNode)) {
    return std::make_pair("", false);
  }
  auto* opt = queryCtx()->optimization();
  velox::ScopedVarSetter pref(&opt->cnamesInExpr(), false);
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
  if (leftOptional_ && rightOptional_) {
    out << " full outer ";
  } else if (markColumn_) {
    out << " exists project ";
  } else if (rightOptional_) {
    out << " left ";
  } else if (rightExists_) {
    out << " exists ";
  } else if (rightNotExists_) {
    out << " not exists ";
  } else if (leftOptional_) {
    out << " right ";
  } else if (directed_) {
    out << " unnest ";
  } else {
    out << " inner ";
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

const FunctionSet& Expr::functions() const {
  static FunctionSet empty;
  return empty;
}

bool Expr::sameOrEqual(const Expr& other) const {
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }
  switch (type()) {
    case PlanType::kColumnExpr:
      return as<Column>()->equivalence() &&
          as<Column>()->equivalence() == other.as<Column>()->equivalence();
    case PlanType::kAggregateExpr: {
      auto a = as<Aggregate>();
      auto b = other.as<Aggregate>();
      if (a->isDistinct() != b->isDistinct() ||
          (a->condition() != b->condition() &&
           (!a->condition() || !b->condition() ||
            !a->condition()->sameOrEqual(*b->condition())))) {
        return false;
      }
    }
      [[fallthrough]];
    case PlanType::kCallExpr: {
      if (as<Call>()->name() != other.as<Call>()->name()) {
        return false;
      }
      auto numArgs = as<Call>()->args().size();
      if (numArgs != other.as<Call>()->args().size()) {
        return false;
      }
      for (auto i = 0; i < numArgs; ++i) {
        if (!as<Call>()->argAt(i)->sameOrEqual(*other.as<Call>()->argAt(i))) {
          return false;
        }
      }
      return true;
    }
    default:
      return false;
  }
}

PlanObjectCP Expr::singleTable() const {
  if (is(PlanType::kColumnExpr)) {
    return as<Column>()->relation();
  }

  PlanObjectCP table = nullptr;
  bool multiple = false;
  columns_.forEach<Column>([&](auto column) {
    if (!table) {
      table = column->relation();
    } else if (table != column->relation()) {
      multiple = true;
    }
  });

  return multiple ? nullptr : table;
}

PlanObjectSet Expr::allTables() const {
  PlanObjectSet set;
  columns_.forEach<Column>([&](auto column) { set.add(column->relation()); });
  return set;
}

Column::Column(
    Name name,
    PlanObjectCP relation,
    const Value& value,
    Name alias,
    Name nameInTable,
    ColumnCP topColumn,
    PathCP path)
    : Expr(PlanType::kColumnExpr, value),
      name_(name),
      relation_(relation),
      alias_(alias),
      topColumn_(topColumn),
      path_(path) {
  columns_.add(this);
  subexpressions_.add(this);
  if (relation_ && relation_->is(PlanType::kTableNode)) {
    if (topColumn_) {
      VELOX_CHECK(topColumn_->relation() == relation_);
      VELOX_CHECK_NULL(topColumn_->topColumn());
      VELOX_CHECK_NULL(topColumn_->path());
      schemaColumn_ = topColumn_->schemaColumn_;
    } else {
      schemaColumn_ = relation->as<BaseTable>()->schemaTable->findColumn(
          nameInTable ? nameInTable : name_);
    }
    VELOX_CHECK(schemaColumn_);
  }
}

void BaseTable::addFilter(ExprCP expr) {
  const auto& columns = expr->columns();

  VELOX_CHECK_GT(columns.size(), 0);

  if (columns.size() == 1) {
    columnFilters.push_back(expr);
  } else {
    filter.push_back(expr);
  }

  queryCtx()->optimization()->filterUpdated(this);
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
  float fanout;
  bool unique;
};

// Estimates the number of matching rows per equality lookup on 'keys' given
// 'scanCardinality' total rows. For each key pair, divides by
// max(ndv(thisKey), ndv(otherKey)) to account for values on the probe side
// that have no match on the scan side.
float estimateFanout(
    float scanCardinality,
    const ExprVector& keys,
    const ExprVector& otherKeys) {
  if (keys.empty()) {
    return scanCardinality;
  }

  auto fanout = scanCardinality /
      std::max(keys[0]->value().cardinality, otherKeys[0]->value().cardinality);
  for (size_t i = 1; i < keys.size(); ++i) {
    auto distinctValues = std::max(
        keys[i]->value().cardinality, otherKeys[i]->value().cardinality);
    if (distinctValues > fanout) {
      fanout = 1;
    } else {
      fanout /= distinctValues;
    }
  }
  return fanout;
}

// Estimates the number of matching rows per equality lookup on 'keys' for
// 'table'. For BaseTable, uses column statistics via estimateFanout and checks
// uniqueness via SchemaTable::isUnique(). For ValuesTable, UnnestTable, and
// DerivedTable, estimates cardinality using column statistics. For
// DerivedTable with an aggregation, sets unique = true when keys cover all
// grouping keys.
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

  if (table->is(PlanType::kValuesTableNode)) {
    const auto cardinality = table->as<ValuesTable>()->cardinality();
    return {
        .fanout = estimateFanout(cardinality, keys, otherKeys),
        .unique = false};
  }

  if (table->is(PlanType::kUnnestTableNode)) {
    const auto cardinality = table->as<UnnestTable>()->cardinality();
    return {
        .fanout = estimateFanout(cardinality, keys, otherKeys),
        .unique = false};
  }

  VELOX_CHECK(table->is(PlanType::kDerivedTableNode));
  const auto* dt = table->as<DerivedTable>();
  return {
      .fanout = estimateFanout(dt->cardinality, keys, otherKeys),
      .unique = dt->aggregation &&
          keys.size() >= dt->aggregation->groupingKeys().size(),
  };
}

float baseSelectivity(PlanObjectCP object) {
  if (object->is(PlanType::kTableNode)) {
    return object->as<BaseTable>()->filterSelectivity;
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

  // If one side has unique join keys, this is a primary key (PK) to foreign
  // key (FK) join. For example, joining orders (PK: orderkey) with lineitem
  // (FK: orderkey), if orders is the left table, then leftUnique_ is true: each
  // lineitem matches at most one order (rlFanout ≤ 1), while each order may
  // match many lineitems (lrFanout = cardLineitem / cardOrders). When both
  // sides are unique (1:1 join), leftUnique takes precedence.
  if (leftUnique_) {
    rlFanout_ = baseSelectivity(leftTable_);
    lrFanout_ = tableCardinality(rightTable_) / tableCardinality(leftTable_) *
        baseSelectivity(rightTable_);
  } else if (rightUnique_) {
    lrFanout_ = baseSelectivity(rightTable_);
    rlFanout_ = tableCardinality(leftTable_) / tableCardinality(rightTable_) *
        baseSelectivity(leftTable_);
  } else {
    auto [sampledLeftFanout, sampledRightFanout] = options.sampleJoins
        ? opt->history().sampleJoin(this)
        : std::pair<float, float>(0, 0);
    if (sampledLeftFanout == 0 && sampledRightFanout == 0) {
      lrFanout_ = right.fanout * baseSelectivity(rightTable_);
      rlFanout_ = left.fanout * baseSelectivity(leftTable_);
    } else {
      lrFanout_ = sampledRightFanout * baseSelectivity(rightTable_);
      rlFanout_ = sampledLeftFanout * baseSelectivity(leftTable_);
    }
  }
}

} // namespace facebook::axiom::optimizer
