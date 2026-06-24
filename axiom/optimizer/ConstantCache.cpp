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

#include "axiom/optimizer/ConstantCache.h"

#include <folly/hash/Hash.h>
#include <functional>
#include <typeindex>

#include "velox/common/base/BitUtil.h"
#include "velox/type/FloatingPointUtil.h"

namespace facebook::axiom::optimizer {

namespace {

template <velox::TypeKind Kind>
uint64_t hashScalar(const velox::Variant& value) {
  using NativeType = typename velox::TypeTraits<Kind>::NativeType;
  if constexpr (std::is_floating_point_v<NativeType>) {
    return velox::util::floating_point::NaNAwareHash<NativeType>{}(
        value.value<Kind>());
  } else {
    return folly::Hash{}(value.value<Kind>());
  }
}

template <velox::TypeKind Kind>
bool equalScalar(const velox::Variant& lhs, const velox::Variant& rhs) {
  using NativeType = typename velox::TypeTraits<Kind>::NativeType;
  if constexpr (std::is_floating_point_v<NativeType>) {
    return velox::util::floating_point::NaNAwareEquals<NativeType>{}(
        lhs.value<Kind>(), rhs.value<Kind>());
  } else {
    return lhs.value<Kind>() == rhs.value<Kind>();
  }
}

// Hashes `value` from its stored bytes, recursing into ARRAY / ROW / MAP
// containers. Bypasses any custom comparator the Variant's type may carry
// (Variant::hash() routes through that comparator).
uint64_t structuralHash(const velox::Variant& value) {
  if (value.isNull()) {
    return velox::bits::kNullHash;
  }
  const auto kind = value.kind();
  if (kind == velox::TypeKind::ARRAY) {
    const auto& children = value.value<velox::TypeKind::ARRAY>();
    uint64_t hash = velox::bits::kNullHash;
    for (size_t i = 0; i < children.size(); ++i) {
      hash = velox::bits::hashMix(hash, structuralHash(children[i]));
    }
    return hash;
  }
  if (kind == velox::TypeKind::ROW) {
    const auto& children = value.value<velox::TypeKind::ROW>();
    uint64_t hash = velox::bits::kNullHash;
    for (size_t i = 0; i < children.size(); ++i) {
      hash = velox::bits::hashMix(hash, structuralHash(children[i]));
    }
    return hash;
  }
  if (kind == velox::TypeKind::MAP) {
    const auto& entries = value.value<velox::TypeKind::MAP>();
    uint64_t hash = velox::bits::kNullHash;
    for (const auto& [key, mapped] : entries) {
      const auto entryHash =
          velox::bits::hashMix(structuralHash(key), structuralHash(mapped));
      hash = velox::bits::commutativeHashMix(hash, entryHash);
    }
    return hash;
  }
  if (kind == velox::TypeKind::OPAQUE) {
    const auto& capsule = value.value<velox::TypeKind::OPAQUE>();
    return velox::bits::hashMix(
        std::hash<std::type_index>{}(capsule.type->typeIndex()),
        folly::Hash{}(reinterpret_cast<uintptr_t>(capsule.obj.get())));
  }
  if (kind == velox::TypeKind::UNKNOWN) {
    return velox::bits::kNullHash;
  }
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(hashScalar, kind, value);
}

// Returns true iff `lhs` and `rhs` hold the same stored bytes, recursing
// into ARRAY / ROW / MAP. Bypasses any custom comparator the Variant's
// type may carry (Variant::operator== routes through that comparator).
bool structuralEquals(const velox::Variant& lhs, const velox::Variant& rhs) {
  if (lhs.kind() != rhs.kind()) {
    return false;
  }
  if (lhs.isNull() || rhs.isNull()) {
    return lhs.isNull() && rhs.isNull();
  }
  const auto kind = lhs.kind();
  if (kind == velox::TypeKind::ARRAY) {
    const auto& lhsElements = lhs.value<velox::TypeKind::ARRAY>();
    const auto& rhsElements = rhs.value<velox::TypeKind::ARRAY>();
    if (lhsElements.size() != rhsElements.size()) {
      return false;
    }
    for (size_t i = 0; i < lhsElements.size(); ++i) {
      if (!structuralEquals(lhsElements[i], rhsElements[i])) {
        return false;
      }
    }
    return true;
  }
  if (kind == velox::TypeKind::ROW) {
    const auto& lhsFields = lhs.value<velox::TypeKind::ROW>();
    const auto& rhsFields = rhs.value<velox::TypeKind::ROW>();
    if (lhsFields.size() != rhsFields.size()) {
      return false;
    }
    for (size_t i = 0; i < lhsFields.size(); ++i) {
      if (!structuralEquals(lhsFields[i], rhsFields[i])) {
        return false;
      }
    }
    return true;
  }
  if (kind == velox::TypeKind::MAP) {
    const auto& lhsEntries = lhs.value<velox::TypeKind::MAP>();
    const auto& rhsEntries = rhs.value<velox::TypeKind::MAP>();
    if (lhsEntries.size() != rhsEntries.size()) {
      return false;
    }
    auto lhsIt = lhsEntries.begin();
    auto rhsIt = rhsEntries.begin();
    while (lhsIt != lhsEntries.end()) {
      if (!structuralEquals(lhsIt->first, rhsIt->first) ||
          !structuralEquals(lhsIt->second, rhsIt->second)) {
        return false;
      }
      ++lhsIt;
      ++rhsIt;
    }
    return true;
  }
  if (kind == velox::TypeKind::OPAQUE) {
    const auto& lhsCapsule = lhs.value<velox::TypeKind::OPAQUE>();
    const auto& rhsCapsule = rhs.value<velox::TypeKind::OPAQUE>();
    return lhsCapsule.type->typeIndex() == rhsCapsule.type->typeIndex() &&
        lhsCapsule.obj == rhsCapsule.obj;
  }
  if (kind == velox::TypeKind::UNKNOWN) {
    return true;
  }
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(equalScalar, kind, lhs, rhs);
}

} // namespace

size_t ConstantCache::KeyHash::operator()(const Key& key) const {
  return velox::bits::hashMix(
      std::hash<const velox::Type*>()(key.type), structuralHash(*key.value));
}

bool ConstantCache::KeyEqual::operator()(const Key& lhs, const Key& rhs) const {
  return lhs.type == rhs.type && structuralEquals(*lhs.value, *rhs.value);
}

ExprCP ConstantCache::get(const logical_plan::ConstantExpr& constant) {
  Key key{toType(constant.type()), constant.value()};
  auto it = entries_.find(key);
  if (it != entries_.end()) {
    return it->second;
  }

  Value value(key.type, 1);
  if (key.value->isNull()) {
    value.nullFraction = 1.0;
  } else {
    value.nullFraction = 0.0;
    value.nullable = false;
    if (key.type->isPrimitiveType()) {
      value.min = key.value.get();
      value.max = key.value.get();
    }
  }

  auto* literal = make<Literal>(value, key.value.get());
  entries_[std::move(key)] = literal;
  return literal;
}

} // namespace facebook::axiom::optimizer
