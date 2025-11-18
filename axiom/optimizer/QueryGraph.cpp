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
#include <algorithm>
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
  if (!opt->cnamesInExpr() || relation_ == nullptr) {
    return name_;
  }

  Name cname;
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
      VELOX_UNREACHABLE(
          "Unexpected relation: {}", PlanTypeName::toName(relation_->type()));
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
      markColumn_,
      leftUnique_};
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
  out << "<join "
      << (leftTable_ ? leftTable_->toString() : " multiple tables ");
  if (leftOptional_ && rightOptional_) {
    out << " full outer ";
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
  } else if (unnest_) {
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

namespace {

bool sameOrEqualExpr(ExprCP l, ExprCP r) {
  if (l == r) {
    return true;
  }
  if (!l || !r) {
    return false;
  }
  if (l->type() != r->type()) {
    return false;
  }
  auto sameOrEqualExprs = [&](const auto& l, const auto& r) {
    return std::ranges::equal(l, r, sameOrEqualExpr);
  };
  switch (l->type()) {
    case PlanType::kColumnExpr:
      return l->as<Column>()->equivalence() &&
          l->as<Column>()->equivalence() == r->as<Column>()->equivalence();
    case PlanType::kLiteralExpr:
      return l->as<Literal>()->literal() == r->as<Literal>()->literal();
    case PlanType::kFieldExpr: {
      const auto* a = l->as<Field>();
      const auto* b = r->as<Field>();
      if (a->field() != b->field() || a->index() != b->index()) {
        return false;
      }
      return sameOrEqualExpr(a->base(), b->base());
    }
    case PlanType::kAggregateExpr: {
      const auto* a = l->as<Aggregate>();
      const auto* b = r->as<Aggregate>();
      if (a->isDistinct() != b->isDistinct()) {
        return false;
      }
      if (sameOrEqualExpr(a->condition(), b->condition())) {
        return false;
      }
      if (a->orderTypes() != b->orderTypes()) {
        return false;
      }
      if (!sameOrEqualExprs(a->orderKeys(), b->orderKeys())) {
        return false;
      }
    }
      [[fallthrough]];
    case PlanType::kCallExpr: {
      const auto* a = l->as<Call>();
      const auto* b = r->as<Call>();
      if (a->name() != b->name()) {
        return false;
      }
      return sameOrEqualExprs(a->args(), b->args());
    }
    case PlanType::kWindowExpr: {
      const auto* a = l->as<Window>();
      const auto* b = r->as<Window>();
      if (a->name() != b->name()) {
        return false;
      }
      if (a->ignoreNulls() != b->ignoreNulls()) {
        return false;
      }
      if (a->spec() != b->spec()) {
        return false;
      }
      const auto& f1 = a->frame();
      const auto& f2 = b->frame();
      if (f1.type != f2.type) {
        return false;
      }
      if (f1.startType != f2.startType) {
        return false;
      }
      if (f1.endType != f2.endType) {
        return false;
      }
      if (!sameOrEqualExpr(f1.startValue, f2.startValue)) {
        return false;
      }
      if (!sameOrEqualExpr(f1.endValue, f2.endValue)) {
        return false;
      }
      return sameOrEqualExprs(a->args(), b->args());
    }
    // TODO: handle other expression types: Lambda.
    default:
      return false;
  }
}

} // namespace

bool Expr::sameOrEqual(const Expr& other) const {
  return sameOrEqualExpr(this, &other);
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

inline CPSpan<Column> toRangeCast(const ExprVector& exprs) {
  return {reinterpret_cast<const Column* const*>(exprs.data()), exprs.size()};
}

} // namespace

void JoinEdge::guessFanout() {
  if (fanoutsFixed_) {
    return;
  }

  // TODO: Why fanouts are set to 1.1 and 1 when left table is null?
  if (leftTable_ == nullptr) {
    lrFanout_ = 1.1;
    rlFanout_ = 1;
    return;
  }

  auto* opt = queryCtx()->optimization();
  auto samplePair = opt->history().sampleJoin(this);
  auto left = joinCardinality(leftTable_, toRangeCast(leftKeys_));
  auto right = joinCardinality(rightTable_, toRangeCast(rightKeys_));
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

bool WindowSpec::operator==(const WindowSpec& other) const {
  if (partitionKeys.size() != other.partitionKeys.size() ||
      orderKeys.size() != other.orderKeys.size() ||
      orderTypes.size() != other.orderTypes.size()) {
    return false;
  }

  for (size_t i = 0; i < partitionKeys.size(); ++i) {
    if (!partitionKeys[i]->sameOrEqual(*other.partitionKeys[i])) {
      return false;
    }
  }

  for (size_t i = 0; i < orderKeys.size(); ++i) {
    if (!orderKeys[i]->sameOrEqual(*other.orderKeys[i])) {
      return false;
    }
  }

  return orderTypes == other.orderTypes;
}

size_t WindowSpec::Hasher::operator()(const WindowSpec& spec) const {
  size_t hash = 0;
  for (const auto& key : spec.partitionKeys) {
    hash = velox::bits::hashMix(hash, folly::hasher<ExprCP>()(key));
  }
  for (const auto& key : spec.orderKeys) {
    hash = velox::bits::hashMix(hash, folly::hasher<ExprCP>()(key));
  }
  for (const auto& type : spec.orderTypes) {
    hash = velox::bits::hashMix(hash, folly::hasher<OrderType>()(type));
  }
  return hash;
}

} // namespace facebook::axiom::optimizer
