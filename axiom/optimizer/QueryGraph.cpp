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

#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "folly/hash/Hash.h"
#include "velox/common/base/BitUtil.h"

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
// static
const char* SpecialFormCallNames::kDereference = "__dereference";
// static
const char* SpecialFormCallNames::kRowConstructor = "__row_constructor";

// static
ColumnCP Column::createForSymbol(Name symbol, const Value& value) {
  VELOX_DCHECK(
      queryCtx()->isV2(),
      "Column::createForSymbol synthesizes a relation-less column, a v2-only idiom");
  return make<Column>(
      queryCtx()->newName(symbol),
      /*relation=*/nullptr,
      value,
      /*alias=*/symbol);
}

// static
ColumnCP Column::createForNullExtendedValue(ColumnCP source) {
  Value value = source->value();
  value.trueFraction.reset();
  value.nullFraction.reset();
  value.nullable = true;
  const Name outputName = toName(source->outputName());
  return make<Column>(
      queryCtx()->newName(outputName),
      /*relation=*/nullptr,
      value,
      /*alias=*/outputName,
      /*nameInTable=*/nullptr,
      source->topColumn(),
      source->path());
}

// static
ColumnCP Column::createWithUnknownValue(ColumnCP source) {
  const Name outputName = toName(source->outputName());
  return make<Column>(
      queryCtx()->newName(outputName),
      /*relation=*/nullptr,
      Value(source->value().type),
      /*alias=*/outputName,
      /*nameInTable=*/nullptr,
      source->topColumn(),
      source->path());
}

// static
ColumnCP Column::createBoolean(std::string_view prefix) {
  return create(prefix, Value(toType(velox::BOOLEAN()), /*cardinality=*/2));
}

// static
ColumnCP Column::create(std::string_view prefix, const Value& value) {
  VELOX_DCHECK(
      queryCtx()->isV2(),
      "Column::create synthesizes a relation-less column, a v2-only idiom");
  return make<Column>(queryCtx()->newName(prefix), /*relation=*/nullptr, value);
}

bool Column::equals(ColumnCP other) const {
  if (!equivalence_ && !other->equivalence_) {
    auto* equiv = make<Equivalence>();
    equiv->columns.push_back(this);
    equiv->columns.push_back(other);
    equivalence_ = equiv;
    other->equivalence_ = equiv;
    return true;
  }
  if (!other->equivalence_) {
    other->equivalence_ = equivalence_;
    equivalence_->columns.push_back(other);
    return true;
  }
  if (!equivalence_) {
    return other->equals(this);
  }
  // Already in one class: merging would iterate and grow the same vector.
  if (equivalence_ == other->equivalence_) {
    return false;
  }
  for (auto& column : other->equivalence_->columns) {
    equivalence_->columns.push_back(column);
    column->equivalence_ = equivalence_;
  }
  return true;
}

Name cname(PlanObjectCP relation) {
  VELOX_CHECK(
      relation->isTable(), "Unexpected relation: {}", relation->typeName());
  return relation->as<TableObject>()->cname;
}

std::string Column::toString() const {
  if (queryCtx()->isV2()) {
    return name_;
  }

  if (!queryCtx()->cnamesInExpr() || relation_ == nullptr) {
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

bool Call::isRowConstructor() const {
  return name() == SpecialFormCallNames::kRowConstructor;
}

const std::vector<velox::Variant>* Call::constantInList() const {
  if (name() != SpecialFormCallNames::kIn || args().size() != 2 ||
      !args()[1]->is(PlanType::kLiteralExpr)) {
    return nullptr;
  }
  const auto* type = args()[1]->value().type;
  const auto& literal = literalAt(1);
  if (!type->isArray() || literal.isNull() ||
      !type->childAt(0)->equivalent(*args()[0]->value().type)) {
    return nullptr;
  }
  return &literal.array();
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

namespace {
Value literalValue(Value value, const velox::Variant* literal) {
  VELOX_CHECK_NOT_NULL(literal);
  if (literal->isNull()) {
    value.nullFraction = 1;
    return value;
  }
  value.nullFraction = 0;
  value.nullable = false;
  if (value.type->isPrimitiveType()) {
    value.min = literal;
    value.max = literal;
  }
  if (value.type->kind() == velox::TypeKind::BOOLEAN) {
    value.trueFraction = literal->value<bool>() ? 1 : 0;
  }
  return value;
}
} // namespace

Literal::Literal(const Value& value, const velox::Variant* literal)
    : Expr(PlanType::kLiteralExpr, literalValue(value, literal)),
      literal_(literal) {}

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

velox::Variant Aggregate::emptyInputResult() const {
  auto result = queryCtx()->functionRegistry().aggregateResultForEmptyInput(
      name(), rawInputType_);
  return result.isNull() ? velox::Variant::null(value().type->kind()) : result;
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

const connector::TableLayout* BaseTable::layout() const {
  VELOX_CHECK_NOT_NULL(schemaTable);
  VELOX_CHECK(!schemaTable->columnGroups.empty());

  const auto* layout = schemaTable->columnGroups[0]->layout;
  VELOX_CHECK_NOT_NULL(layout);
  return layout;
}

std::string BaseTable::toString() const {
  std::stringstream out;
  out << "{" << PlanObject::toString();
  out << schemaTable->name() << " " << cname << "}";
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

bool BaseTable::addFilter(ExprCP expr) {
  const auto& columns = expr->columns();

  VELOX_CHECK_GT(columns.size(), 0);

  if (columns.size() == 1) {
    columnFilters.push_back(expr);
  } else {
    // Pointer equality suffices: equality calls are interned by ToGraph,
    // so a duplicate insert resolves to the same Call*.
    for (auto* existing : filter) {
      if (existing == expr) {
        return false;
      }
    }
    filter.push_back(expr);
  }

  return true;
}

} // namespace facebook::axiom::optimizer
