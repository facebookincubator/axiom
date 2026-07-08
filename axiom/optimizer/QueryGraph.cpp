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

#include "axiom/optimizer/DerivedTable.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "folly/hash/Hash.h"
#include "velox/common/base/BitUtil.h"
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
// static
const char* SpecialFormCallNames::kNullIf = "__nullif";

void Column::rebindAll(const ColumnVector& columns, PlanObjectCP newRelation) {
  for (auto* column : columns) {
    const_cast<Column*>(column)->relation_ = newRelation;
  }
}

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
    out << args_[i]->toString() << (i == args_.size() - 1 ? "" : ", ");
  }
  if (name_ == SpecialFormCallNames::kCast ||
      name_ == SpecialFormCallNames::kTryCast) {
    out << " as " << value().type->toString();
  }
  out << ")";
  return out.str();
}

size_t Literal::KeyHash::operator()(const Literal* literal) const {
  return velox::bits::hashMix(
      folly::hasher<const velox::Type*>()(literal->value().type),
      literal->literal().hash());
}

size_t Literal::KeyHash::operator()(const KeyView& key) const {
  return velox::bits::hashMix(
      folly::hasher<const velox::Type*>()(key.type), key.value->hash());
}

bool Literal::KeyEq::operator()(const Literal* left, const Literal* right)
    const {
  return left->value().type == right->value().type &&
      left->literal() == right->literal();
}

bool Literal::KeyEq::operator()(const KeyView& key, const Literal* literal)
    const {
  return key.type == literal->value().type && *key.value == literal->literal();
}

bool Literal::KeyEq::operator()(const Literal* literal, const KeyView& key)
    const {
  return (*this)(key, literal);
}

namespace {
// Hashes a Call's identity: name, result type, and args by pointer.
template <typename Args>
size_t hashCallIdentity(Name name, const velox::Type* type, const Args& args) {
  size_t hash = folly::hasher<Name>()(name);
  for (const auto* arg : args) {
    hash = velox::bits::hashMix(hash, folly::hasher<ExprCP>()(arg));
  }
  return velox::bits::hashMix(hash, folly::hasher<const velox::Type*>()(type));
}
} // namespace

size_t Call::KeyHash::operator()(const Call* call) const {
  return hashCallIdentity(call->name(), call->value().type, call->args());
}

size_t Call::KeyHash::operator()(const KeyView& key) const {
  return hashCallIdentity(key.name, key.type, key.args);
}

bool Call::KeyEq::operator()(const Call* left, const Call* right) const {
  return left->name() == right->name() &&
      left->value().type == right->value().type &&
      std::ranges::equal(left->args(), right->args());
}

bool Call::KeyEq::operator()(const KeyView& key, const Call* call) const {
  return key.name == call->name() && key.type == call->value().type &&
      std::ranges::equal(key.args, call->args());
}

bool Call::KeyEq::operator()(const Call* call, const KeyView& key) const {
  return (*this)(key, call);
}

namespace {
template <typename Args, typename OrderKeys, typename OrderTypes>
size_t hashAggregateIdentity(
    Name name,
    bool isDistinct,
    ExprCP condition,
    const Args& args,
    const OrderKeys& orderKeys,
    const OrderTypes& orderTypes) {
  size_t hash = folly::hasher<Name>()(name);
  hash = velox::bits::hashMix(hash, folly::hasher<bool>()(isDistinct));
  if (condition != nullptr) {
    hash = velox::bits::hashMix(hash, folly::hasher<ExprCP>()(condition));
  }
  for (const auto* arg : args) {
    hash = velox::bits::hashMix(hash, folly::hasher<ExprCP>()(arg));
  }
  for (const auto* key : orderKeys) {
    hash = velox::bits::hashMix(hash, folly::hasher<ExprCP>()(key));
  }
  for (auto order : orderTypes) {
    hash = velox::bits::hashMix(hash, folly::hasher<OrderType>()(order));
  }
  return hash;
}

bool aggregateIdentityEq(
    const Aggregate::KeyView& key,
    const Aggregate* aggregate) {
  return key.name == aggregate->name() &&
      key.isDistinct == aggregate->isDistinct() &&
      key.condition == aggregate->condition() &&
      std::ranges::equal(key.args, aggregate->args()) &&
      std::ranges::equal(key.orderKeys, aggregate->orderKeys()) &&
      std::ranges::equal(key.orderTypes, aggregate->orderTypes());
}
} // namespace

size_t Aggregate::KeyHash::operator()(const Aggregate* aggregate) const {
  return hashAggregateIdentity(
      aggregate->name(),
      aggregate->isDistinct(),
      aggregate->condition(),
      aggregate->args(),
      aggregate->orderKeys(),
      aggregate->orderTypes());
}

size_t Aggregate::KeyHash::operator()(const KeyView& key) const {
  return hashAggregateIdentity(
      key.name,
      key.isDistinct,
      key.condition,
      key.args,
      key.orderKeys,
      key.orderTypes);
}

bool Aggregate::KeyEq::operator()(const Aggregate* left, const Aggregate* right)
    const {
  return left->name() == right->name() &&
      left->isDistinct() == right->isDistinct() &&
      left->condition() == right->condition() &&
      std::ranges::equal(left->args(), right->args()) &&
      std::ranges::equal(left->orderKeys(), right->orderKeys()) &&
      std::ranges::equal(left->orderTypes(), right->orderTypes());
}

bool Aggregate::KeyEq::operator()(
    const KeyView& key,
    const Aggregate* aggregate) const {
  return aggregateIdentityEq(key, aggregate);
}

bool Aggregate::KeyEq::operator()(
    const Aggregate* aggregate,
    const KeyView& key) const {
  return aggregateIdentityEq(key, aggregate);
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

const Aggregate* Aggregate::replaceDistinctAndFilterByMarker(
    ExprCP marker) const {
  VELOX_CHECK(isDistinct_);
  VELOX_CHECK_NOT_NULL(marker);
  return make<Aggregate>(
      name(),
      value_,
      args(),
      functions(),
      /*isDistinct=*/false,
      marker,
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

bool Expr::hasAnyTableIn(const PlanObjectSet& tables) const {
  return columns_.anyOf<Column>(
      [&](auto column) { return tables.contains(column->relation()); });
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
    // Pointer equality suffices: equality calls are interned by ToGraph,
    // so a duplicate insert resolves to the same Call*.
    for (auto* existing : filter) {
      if (existing == expr) {
        return;
      }
    }
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

} // namespace facebook::axiom::optimizer
