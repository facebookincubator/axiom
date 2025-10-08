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

std::string Column::toString() const {
  const auto* opt = queryCtx()->optimization();
  if (!opt->cnamesInExpr()) {
    return name_;
  }

  Name cname = "";
  if (relation_) {
    switch (relation_->type()) {
      case PlanType::kTableNode:
        cname = relation_->as<BaseTable>()->cname;
        break;
      case PlanType::kValuesTableNode:
        cname = relation_->as<ValuesTable>()->cname;
        break;
      case PlanType::kUnnestTableNode:
        cname = relation_->as<UnnestTable>()->cname;
        break;
      case PlanType::kDerivedTableNode:
        cname = relation_->as<DerivedTable>()->cname;
        break;
      default:
        cname = "--";
    }
  }

  return fmt::format("{}.{}", cname, name_);
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

std::optional<BitSet> SubfieldSet::findSubfields(int32_t id) const {
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

BitSet BaseTable::columnSubfields(
    int32_t id,
    bool controlOnly,
    bool payloadOnly) const {
  BitSet subfields;
  if (!controlOnly) {
    if (auto maybe = payloadSubfields.findSubfields(id)) {
      subfields = maybe.value();
    }
  }
  if (!payloadOnly) {
    if (auto maybe = controlSubfields.findSubfields(id)) {
      subfields.unionSet(maybe.value());
    }
  }
  Path::subfieldSkyline(subfields);
  return subfields;
}

std::string BaseTable::toString() const {
  std::stringstream out;
  out << "{" << PlanObject::toString();
  out << schemaTable->name << " " << cname << "}";
  return out.str();
}

void ValuesTable::addJoinedBy(JoinEdgeP join) {
  pushBackUnique(joinedBy, join);
}

std::string ValuesTable::toString() const {
  std::stringstream out;
  out << "{" << PlanObject::toString();
  out << values.id() << " " << cname << "}";
  return out.str();
}

void UnnestTable::addJoinedBy(JoinEdgeP join) {
  pushBackUnique(joinedBy, join);
}

std::string UnnestTable::toString() const {
  std::stringstream out;
  out << "{" << PlanObject::toString();
  out << cname << "}";
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
        rightUnique_};
  }

  return {
      leftTable_,
      leftKeys_,
      rlFanout_,
      leftOptional_,
      rightOptional_,
      false,
      false,
      nullptr,
      leftUnique_};
}

bool JoinEdge::isBroadcastableType() const {
  return !leftOptional_;
}

void JoinEdge::addEquality(ExprCP left, ExprCP right, bool update) {
  leftKeys_.push_back(left);
  rightKeys_.push_back(right);
  if (update) {
    guessFanout();
  }
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
      fmt::format("{} ", leftTable_->as<BaseTable>()->schemaTable->name);
  auto right =
      fmt::format("{} ", rightTable_->as<BaseTable>()->schemaTable->name);
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
  out << "<join "
      << (leftTable_ ? leftTable_->toString() : " multiple tables ");
  if (leftOptional_ && rightOptional_) {
    out << " full outr ";
  } else if (markColumn_) {
    out << " exists project ";
  } else if (rightOptional_) {
    out << " left";
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
  out << rightTable_->toString();
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
      auto a = reinterpret_cast<const Aggregate*>(this);
      auto b = reinterpret_cast<const Aggregate*>(&other);
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
        if (as<Call>()->argAt(i)->sameOrEqual(*other.as<Call>()->argAt(i))) {
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
    ColumnCP top,
    PathCP path)
    : Expr(PlanType::kColumnExpr, value),
      name_(name),
      relation_(relation),
      alias_(alias),
      topColumn_(top),
      path_(path) {
  columns_.add(this);
  subexpressions_.add(this);
  if (relation_ && relation_->is(PlanType::kTableNode)) {
    if (topColumn_) {
      schemaColumn_ = topColumn_->schemaColumn_;
    } else {
      schemaColumn_ = relation->as<BaseTable>()->schemaTable->findColumn(
          nameInTable ? nameInTable : name_);
      VELOX_CHECK(schemaColumn_);
    }
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

namespace {
template <typename U>
inline CPSpan<Column> toRangeCast(const ExprVector& exprs) {
  return CPSpan<Column>(
      reinterpret_cast<const Column* const*>(exprs.data()), exprs.size());
}
} // namespace

void JoinEdge::guessFanout() {
  if (fanoutsFixed_) {
    return;
  }

  if (leftTable_ == nullptr) {
    lrFanout_ = 1.1;
    rlFanout_ = 1;
    return;
  }

  auto* opt = queryCtx()->optimization();
  auto samplePair = opt->history().sampleJoin(this);
  auto left = joinCardinality(leftTable_, toRangeCast<Column>(leftKeys_));
  auto right = joinCardinality(rightTable_, toRangeCast<Column>(rightKeys_));
  leftUnique_ = left.unique;
  rightUnique_ = right.unique;
  if (samplePair.first == 0 && samplePair.second == 0) {
    lrFanout_ = right.joinCardinality * baseSelectivity(rightTable_);
    rlFanout_ = left.joinCardinality * baseSelectivity(leftTable_);
  } else {
    lrFanout_ = samplePair.second * baseSelectivity(rightTable_);
    rlFanout_ = samplePair.first * baseSelectivity(leftTable_);
  }
  // If one side is unique, the other side is a pk to fk join, with fanout =
  // fk-table-card / pk-table-card.
  if (rightUnique_) {
    lrFanout_ = baseSelectivity(rightTable_);
    rlFanout_ = tableCardinality(leftTable_) / tableCardinality(rightTable_) *
        baseSelectivity(leftTable_);
  }
  if (leftUnique_) {
    rlFanout_ = baseSelectivity(leftTable_);
    lrFanout_ = tableCardinality(rightTable_) / tableCardinality(leftTable_) *
        baseSelectivity(rightTable_);
  }
}

} // namespace facebook::axiom::optimizer
