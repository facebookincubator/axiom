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

#pragma once

#include "axiom/common/Enums.h"
#include "axiom/optimizer/BitSet.h"

namespace facebook::axiom::optimizer {

/// Enum for types of query graph objects. Used when making a tree into
/// a query graph and later to differentiate between tables, derived
/// tables and different expressions.
enum class PlanType : uint32_t {
  // Expressions.
  kColumnExpr = 0,
  kLiteralExpr,
  kCallExpr,
  kAggregateExpr,
  kFieldExpr,
  kLambdaExpr,
  // Plan nodes.
  kTableNode,
  kValuesTableNode,
  kUnnestTableNode,
  kDerivedTableNode,
  kAggregationNode,
  kWriteNode,
};

AXIOM_DECLARE_ENUM_NAME(PlanType);

/// True if 'type' is an expression with a value.
inline bool isExprType(PlanType type) {
  return type == PlanType::kColumnExpr || type == PlanType::kCallExpr ||
      type == PlanType::kLiteralExpr;
}

/// Common superclass of all vertices of a query graph. This
/// includes tables, columns, expressions, derived tables etc. These
/// all have a unique small integer id. Ids are often to track
/// membership in PlanObjectSets. These stand for e.g. the columns
/// assigned by an operator or the tables in a partial plan.  Joins
/// are edges of the graph but do not have ids, rather, they are
/// identified by the ids of their end points. PlanObjects are created at the
/// start of planning and are arena allocated to be all dropped when the
/// planning is complete.
class PlanObject {
 public:
  explicit PlanObject(PlanType type)
      : type_(type), id_(queryCtx()->newId(this)) {}

  virtual ~PlanObject() = default;

  void operator delete(void* /*ptr*/) {
    LOG(FATAL) << "Plan objects are not deletable";
  }

  int32_t id() const {
    return id_;
  }

  PlanType type() const {
    return type_;
  }

  bool isColumn() const {
    return type_ == PlanType::kColumnExpr;
  }

  bool is(PlanType type) const {
    return type_ == type;
  }

  bool isNot(PlanType type) const {
    return type_ != type;
  }

  /// Caller must ensure this type is correct.
  template <typename T>
  const T* as() const {
    static_assert(std::is_base_of_v<PlanObject, T>);
    VELOX_DCHECK_NOT_NULL(dynamic_cast<const T*>(this));
    return static_cast<const T*>(this);
  }

  /// Caller must ensure this type is correct.
  template <typename T>
  T* as() {
    static_assert(std::is_base_of_v<PlanObject, T>);
    VELOX_DCHECK_NOT_NULL(dynamic_cast<T*>(this));
    return static_cast<T*>(this);
  }

  /// Returns a view on children, e.g. arguments of a function call.
  virtual CPSpan<PlanObject> children() const {
    return {};
  }

  /// Returns true if 'this' is an expression with a value.
  virtual bool isExpr() const {
    return false;
  }

  /// Returns true if this is a table or derived table.
  virtual bool isTable() const {
    return false;
  }

  /// Returns true if 'this' is a function call or aggregate function call.
  virtual bool isFunction() const {
    return false;
  }

  size_t hash() const;

  virtual std::string toString() const {
    return fmt::format("#{}", id_);
  }

 private:
  const PlanType type_;
  const int32_t id_;
};

using PlanObjectP = PlanObject*;
using PlanObjectCP = const PlanObject*;
using PlanObjectVector = QGVector<PlanObjectCP>;

/// Set of PlanObjects. Uses the objects id() as an index into a bitmap.
class PlanObjectSet : public BitSet {
 public:
  /// True if id of 'object' is in 'this'.
  bool contains(PlanObjectCP object) const {
    return object->id() < bits_.size() * 64 &&
        velox::bits::isBitSet(bits_.data(), object->id());
  }

  /// Inserts id of 'object'.
  void add(PlanObjectCP object) {
    auto id = object->id();
    BitSet::add(id);
  }

  /// Erases id of 'object'.
  void erase(PlanObjectCP object) {
    BitSet::erase(object->id());
  }

  /// Adds ids of all columns 'expr' depends on.
  void unionColumns(ExprCP expr);
  void unionColumns(const ExprVector& exprs);

  /// Adds ids of all objects in 'objects'.
  template <typename V>
  void unionObjects(const V& objects) {
    for (auto& object : objects) {
      add(object);
    }
  }

  /// Returns the objects corresponding to ids in 'this' as a vector of const
  /// T*.
  template <typename T = PlanObject>
  QGVector<const T*> toObjects() const {
    QGVector<const T*> objects;
    objects.reserve(size());
    forEach(
        [&](auto object) { objects.emplace_back(object->template as<T>()); });
    return objects;
  }

  /// Returns the only object stored in this set. The caller must ensure the set
  /// contains exactly one object.
  template <typename T = PlanObject>
  const T* onlyObject() const {
    VELOX_DCHECK_EQ(size(), 1);

    auto id = velox::bits::findFirstBit(bits_.data(), 0, bits_.size() * 64);
    return queryCtx()->objectAt(id)->template as<T>();
  }

  /// Applies 'func' to each object in 'this'.
  template <typename Func>
  void forEach(Func func) const {
    forEach<PlanObject, Func>(func);
  }

  template <typename T, typename Func>
  void forEach(Func func) const {
    auto ctx = queryCtx();
    velox::bits::forEachSetBit(bits_.data(), 0, bits_.size() * 64, [&](auto i) {
      func(ctx->objectAt(i)->template as<T>());
    });
  }

  template <typename Func>
  void forEachMutable(Func func) const {
    auto ctx = queryCtx();
    velox::bits::forEachSetBit(bits_.data(), 0, bits_.size() * 64, [&](auto i) {
      func(ctx->mutableObjectAt(i));
    });
  }

  /// Prnts the contents with ids and the string representation of the objects
  /// if 'names' is true.
  std::string toString(bool names) const;
};

} // namespace facebook::axiom::optimizer

namespace std {
template <>
struct hash<::facebook::axiom::optimizer::PlanObjectSet> {
  size_t operator()(
      const ::facebook::axiom::optimizer::PlanObjectSet& set) const {
    return set.hash();
  }
};
} // namespace std
