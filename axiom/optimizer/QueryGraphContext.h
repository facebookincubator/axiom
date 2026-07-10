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

#include <fmt/format.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include "axiom/common/Enums.h"
#include "axiom/optimizer/ArenaCache.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/type/Variant.h"

// #define QG_USE_MALLOC
#define QG_CACHE_ARENA

/// Thread local context and utilities for query planning.

namespace facebook::axiom::optimizer {

/// Pointer to an arena allocated interned copy of a null terminated string.
/// Used for identifiers. Allows comparing strings by comparing pointers.
using Name = const char*;

/// Shorthand for a view on an array of T*
template <typename T>
using CPSpan = std::span<const T* const>;

class PlanObject;

using PlanObjectP = PlanObject*;
using PlanObjectCP = const PlanObject*;

struct TypeHasher {
  size_t operator()(const velox::TypePtr& type) const {
    // hash on recursive TypeKind. Structs that differ in field names
    // only or decimals with different precisions will collide, no
    // other collisions expected.
    return type->hashKind();
  }
};

struct TypeComparer {
  bool operator()(const velox::TypePtr& lhs, const velox::TypePtr& rhs) const {
    return *lhs == *rhs;
  }
};

/// Converts std::string to name used in query graph objects. raw pointer to
/// arena allocated const chars.
Name toName(std::string_view string);

struct Plan;
using PlanP = Plan*;
class Optimization;

/// STL compatible allocator that manages std:: containers allocated in the
/// QueryGraphContext arena.
template <class T>
struct QGAllocator {
  using value_type = T;
  QGAllocator() = default;

  template <typename U>
  explicit QGAllocator(QGAllocator<U> /*other*/) {}

  T* allocate(std::size_t n);

  void deallocate(T* p, std::size_t /*n*/) noexcept;

  friend bool operator==(
      const QGAllocator& /*lhs*/,
      const QGAllocator& /*rhs*/) {
    return true;
  }
};

template <typename T>
using QGVector = std::vector<T, QGAllocator<T>>;

/// Aligns 'raw + 4' forward to 'alignment' bytes within
/// [raw, raw + paddedSize), writes the resulting 4-byte offset from raw
/// in the 4 bytes immediately before the aligned address, and returns
/// the aligned address. 'paddedSize' must be at least
/// 'bytes + alignment + 4'.
inline void*
alignWithDelta(char* raw, size_t bytes, size_t alignment, size_t paddedSize) {
  void* aligned = raw + 4;
  paddedSize -= 4;
  aligned = std::align(alignment, bytes, aligned, paddedSize);
  VELOX_DCHECK_NOT_NULL(aligned);
  int32_t delta = static_cast<int32_t>(static_cast<char*>(aligned) - raw - 4);
  *reinterpret_cast<int32_t*>(static_cast<char*>(aligned) - 4) = delta;
  return aligned;
}

/// Returns the raw pointer that backs an aligned pointer produced by
/// alignWithDelta().
inline void* rawFromAligned(void* aligned) {
  int32_t delta = *reinterpret_cast<int32_t*>(static_cast<char*>(aligned) - 4);
  return static_cast<char*>(aligned) - 4 - delta;
}

/// An allocator backed by QueryGraphContext that guarantees a configurable
/// alignment. The alignment must be a power of 2 and not be 0. This allocator
/// can be used with folly F14 containers that require 16-byte alignment.
template <class T, uint8_t Alignment = 16>
struct QGAlignedAllocator {
  using value_type = T;

  static_assert(Alignment != 0, "Alignment cannot be 0.");
  static_assert(
      (Alignment & (Alignment - 1)) == 0,
      "Alignment must be a power of 2.");

  template <class Other>
  struct rebind {
    using other = QGAlignedAllocator<Other, Alignment>;
  };

  QGAlignedAllocator() = default;

  template <class U, uint8_t A>
  explicit QGAlignedAllocator(const QGAlignedAllocator<U, A>& /*other*/) {}

  T* allocate(std::size_t n);

  void deallocate(T* p, std::size_t n) noexcept;

  friend bool operator==(
      const QGAlignedAllocator& /*lhs*/,
      const QGAlignedAllocator& /*rhs*/) {
    return true;
  }

 private:
  // Pad the memory user requested by some padding to facilitate memory
  // alignment later. Memory layout:
  // - padding (length is stored in `delta`)
  // - delta (4 bytes storing the size of padding)
  // - the aligned ptr
  static std::size_t calculatePaddedSize(std::size_t n) {
    return velox::checkedPlus<size_t>(
        Alignment + 4, velox::checkedMultiply(n, sizeof(T)));
  }
};

/// F14FastMap using QGAlignedAllocator for use in QueryGraphContext.
template <
    typename Key,
    typename Value,
    typename Hasher = std::hash<Key>,
    typename KeyEqual = std::equal_to<Key>>
using QGF14FastMap = folly::F14FastMap<
    Key,
    Value,
    Hasher,
    KeyEqual,
    QGAlignedAllocator<std::pair<const Key, Value>>>;

/// F14FastSet using QGAlignedAllocator for use in QueryGraphContext.
template <
    typename Key,
    typename Hasher = std::hash<Key>,
    typename KeyEqual = std::equal_to<Key>>
using QGF14FastSet =
    folly::F14FastSet<Key, Hasher, KeyEqual, QGAlignedAllocator<Key>>;

/// Elements of subfield paths. The QueryGraphContext holds a dedupped
/// collection of distinct paths.
enum class StepKind : uint8_t {
  kField,
  kSubscript,
  kElementAt,
};

AXIOM_DECLARE_ENUM_NAME(StepKind);

struct Step {
  StepKind kind;

  /// Map key if kind is kSubscript. Struct field if kind is kField.
  Name field{nullptr};

  /// Zero-based array index if kind is kSubscript.
  int64_t id{0};

  /// True if all fields/keys are accessed at this level but there is a subset
  /// of fields accessed at a child level.
  bool allFields{false};

  bool operator==(const Step& other) const;

  bool operator<(const Step& other) const;

  bool operator>=(const Step& other) const {
    return !(*this < other);
  }

  bool operator>(const Step& other) const {
    return other < *this;
  }

  size_t hash() const;
};

using StepVector = QGVector<Step>;

class BitSet;

class Path {
 public:
  Path() = default;

  explicit Path(std::span<const Step> steps, std::false_type /*reverse*/)
      : steps_{steps.begin(), steps.end()} {}

  explicit Path(std::span<const Step> steps, std::true_type /*reverse*/)
      : steps_{steps.rbegin(), steps.rend()} {}

  void operator delete(void* ptr);

  /// True if 'prefix' is a prefix of 'this', but doesn't equal to 'this'.
  bool hasPrefix(const Path& prefix) const;

  Path* field(const char* name) {
    VELOX_CHECK(mutable_);
    steps_.push_back(Step{.kind = StepKind::kField, .field = toName(name)});
    return this;
  }

  Path* subscript(const char* name) {
    VELOX_CHECK(mutable_);
    steps_.push_back(Step{.kind = StepKind::kSubscript, .field = toName(name)});
    return this;
  }

  Path* subscript(int64_t id) {
    VELOX_CHECK(mutable_);
    steps_.push_back(Step{.kind = StepKind::kSubscript, .id = id});
    return this;
  }

  const StepVector& steps() const {
    return steps_;
  }

  int32_t id() const {
    return id_;
  }

  bool empty() const {
    return steps_.empty();
  }

  void setId(int32_t id) const {
    VELOX_CHECK(mutable_);
    id_ = id;
  }

  bool operator==(const Path& other) const;

  bool operator<(const Path& other) const;

  bool operator>=(const Path& other) const {
    return !(*this < other);
  }

  bool operator>(const Path& other) const {
    return other < *this;
  }

  size_t hash() const;

  std::string toString() const;

  void makeImmutable() const {
    mutable_ = false;
  }

  /// Removes elements of 'subfields' where the path has a prefix in
  /// 'subfields'.
  static void subfieldSkyline(BitSet& subfields);

 private:
  StepVector steps_;
  mutable int32_t id_{-1};
  mutable bool mutable_{true};
};

using PathCP = const Path*;

struct PathHasher {
  size_t operator()(const PathCP path) const {
    return path->hash();
  }
};

struct PathComparer {
  bool operator()(const PathCP left, const PathCP right) const {
    return *left == *right;
  }
};

/// Holds function names with well-defined semantics that the optimizer can use
/// for expression analysis and transformations. These are looked up from the
/// function registry during query graph construction.
struct FunctionNames {
  /// Scalar functions.
  Name equality{nullptr};
  Name negation{nullptr};
  Name elementAt{nullptr};
  Name subscript{nullptr};
  Name lt{nullptr};
  Name lte{nullptr};
  Name gt{nullptr};
  Name gte{nullptr};
  Name isNull{nullptr};
  Name between{nullptr};
  Name like{nullptr};

  /// Aggregate functions.
  Name arbitrary{nullptr};
  Name count{nullptr};

  /// Window functions.
  Name rowNumber{nullptr};
  Name rank{nullptr};
  Name denseRank{nullptr};

  /// True if 'name' is a ranking window function (row_number, rank,
  /// dense_rank).
  bool isRanking(Name name) const {
    return name == rowNumber || name == rank || name == denseRank;
  }
};

/// Context for making a query plan. Owns all memory associated to
/// planning, except for the input PlanNode tree. The result of
/// planning is also owned by 'this', so the planning result must be
/// copied into a non-owned target specific representation before
/// destroying 'this'. QueryGraphContext is not thread safe and may
/// be accessed from one thread at a time. Memory allocation
/// references this via a thread local through queryCtx().
class QueryGraphContext {
 public:
  static constexpr int32_t kArenaAlignment = 8;

  explicit QueryGraphContext(velox::HashStringAllocator& allocator);

  /// Returns a new unique id to use for 'object' and associates 'object' to
  /// this id. Tagging objects with integers ids is useful for efficiently
  /// representing sets of objects as bitmaps.
  int32_t newId(PlanObject* object) {
    objects_.push_back(object);
    return static_cast<int32_t>(objects_.size() - 1);
  }

  /// Allocates 'size' bytes from the arena of 'this', aligned to
  /// kArenaAlignment. The allocation lives until free() is called on it or
  /// the arena is destroyed.
  void* allocate(int32_t size) {
    // See facebookincubator/axiom#1422: required for libc++ std::hash<T*>
    // correctness under TBAA.
    size_t paddedSize = static_cast<size_t>(size) + kArenaAlignment + 4;
    char* ptr;
#ifdef QG_TEST_USE_MALLOC
    // Benchmark-only. Dropping the arena will not free un-free'd allocs.
    ptr = reinterpret_cast<char*>(::malloc(paddedSize));
#elif defined(QG_CACHE_ARENA)
    ptr = reinterpret_cast<char*>(
        cache_.allocate(static_cast<int32_t>(paddedSize)));
#else
    ptr = reinterpret_cast<char*>(allocator_.allocate(paddedSize)->begin());
#endif
    return alignWithDelta(ptr, size, kArenaAlignment, paddedSize);
  }

  /// Frees ptr, which must have been allocated with allocate() above. Calling
  /// this is not mandatory since objects from the arena get freed at latest
  /// when the arena is destroyed.
  void free(void* ptr) {
    void* raw = rawFromAligned(ptr);
#ifdef QG_TEST_USE_MALLOC
    ::free(raw);
#elif defined(QG_CACHE_ARENA)
    cache_.free(raw);
#else
    allocator_.free(velox::HashStringAllocator::headerOf(raw));
#endif
  }

  /// Returns the object associated to 'id'. See newId()
  PlanObjectCP objectAt(int32_t id) {
    return objects_[id];
  }

  PlanObjectP mutableObjectAt(int32_t id) {
    return objects_[id];
  }

  /// Returns the top level plan being processed when printing operator trees.
  /// If non-null, allows showing percentages.
  PlanP& contextPlan() {
    return contextPlan_;
  }

  /// The top level Optimization instance.
  Optimization*& optimization() {
    return optimization_;
  }

  /// Returns the function names used for expression analysis.
  const FunctionNames& functionNames() const {
    return functionNames_;
  }

  /// Returns the interned representation of 'str', i.e. Returns a
  /// pointer to a canonical null terminated const char* with the same
  /// characters as 'str'. Allows comparing names by comparing
  /// pointers.
  Name toName(std::string_view str);

  // Records the use of a TypePtr in optimization. Returns a canonical
  // representative of the type, allowing pointer equality for exact match.
  // Allows mapping from the Type* back to TypePtr.
  const velox::Type* toType(const velox::TypePtr& type);

  /// Returns the canonical TypePtr corresponding to 'type'. 'type' must have
  /// been previously returned by toType().
  const velox::TypePtr& toTypePtr(const velox::Type* type);

  /// Returns the interned instance of 'path'. 'path' is either
  /// retained if it is not previously known or it is deleted. Must be
  /// allocated from the arena of 'this'.
  PathCP toPath(PathCP path);

  PathCP pathById(uint32_t id) {
    VELOX_DCHECK_LT(id, pathById_.size());
    return pathById_[id];
  }

  /// Takes ownership of a Variant for the duration. Variants are allocated
  /// with new so not in the arena.
  velox::Variant* registerVariant(std::unique_ptr<velox::Variant> value) {
    allVariants_.push_back(std::move(value));
    return allVariants_.back().get();
  }

  /// Returns a globally unique interned name `{prefix}{N}` within this query
  /// context. Example: `newName("a_")` returns `"a_47"`.
  Name newName(std::string_view prefix) {
    return toName(fmt::format("{}{}", prefix, ++nameCounter_));
  }

  /// True when the v2 optimizer is driving this query. v2 runs without an
  /// `Optimization` (a v1 construct), so a null `optimization()` marks v2.
  bool isV2() const {
    return optimization_ == nullptr;
  }

 private:
  void populateFunctionNames();

  velox::TypePtr dedupType(const velox::TypePtr& type);

  velox::HashStringAllocator& allocator_;
  ArenaCache cache_;

  // PlanObjects are stored at the index given by their id.
  std::vector<PlanObjectP> objects_;

  // Set of interned copies of identifiers. insert() into this returns the
  // canonical interned copy of any string. Lifetime is limited to 'allocator_'.
  folly::F14FastSet<std::string_view> names_;

  folly::F14FastSet<velox::TypePtr, TypeHasher, TypeComparer> deduppedTypes_;

  // Maps raw Type* back to shared TypePtr. Used in toType()() and toTypePtr().
  folly::F14FastMap<const velox::Type*, velox::TypePtr> toTypePtr_;

  folly::F14FastSet<PathCP, PathHasher, PathComparer> deduppedPaths_;

  std::vector<PathCP> pathById_;

  PlanP contextPlan_{nullptr};
  Optimization* optimization_{nullptr};
  FunctionNames functionNames_;

  std::vector<std::unique_ptr<velox::Variant>> allVariants_;

  uint32_t nameCounter_{0};
};

/// Returns a mutable reference to the calling thread's QueryGraphContext.
QueryGraphContext*& queryCtx();

template <class T>
T* QGAllocator<T>::allocate(std::size_t n) {
  return reinterpret_cast<T*>(
      queryCtx()->allocate(velox::checkedMultiply(n, sizeof(T))));
}

template <class T>
void QGAllocator<T>::deallocate(T* p, std::size_t /*n*/) noexcept {
  queryCtx()->free(p);
}

template <class T, uint8_t Alignment>
T* QGAlignedAllocator<T, Alignment>::allocate(std::size_t n) {
  auto paddedSize = calculatePaddedSize(n);
  auto* ptr = reinterpret_cast<char*>(queryCtx()->allocate(paddedSize));
  return reinterpret_cast<T*>(
      alignWithDelta(ptr, n * sizeof(T), Alignment, paddedSize));
}

template <class T, uint8_t Alignment>
void QGAlignedAllocator<T, Alignment>::deallocate(
    T* p,
    std::size_t /*n*/) noexcept {
  queryCtx()->free(rawFromAligned(p));
}

template <class T, class... Args>
inline T* make(Args&&... args) {
  static_assert(
      alignof(T) <= QueryGraphContext::kArenaAlignment,
      "Type alignment exceeds arena alignment.");
  return new (queryCtx()->allocate(sizeof(T))) T(std::forward<Args>(args)...);
}

/// Shorthand for toType() in queryCtx().
const velox::Type* toType(const velox::TypePtr& type);

/// Shorthand for toTypePtr() in queryCtx().
const velox::TypePtr& toTypePtr(const velox::Type* type);

/// Shorthand for toPath() in queryCtx().
PathCP toPath(std::span<const Step> steps, bool reverse = false);

inline void Path::operator delete(void* ptr) {
  queryCtx()->free(ptr);
}

/// Shorthand for registerVariant() in queryCtx().
template <typename... Args>
velox::Variant* registerVariant(Args&&... args) {
  return queryCtx()->registerVariant(
      std::make_unique<velox::Variant>(std::forward<Args>(args)...));
}

// Forward declarations of common types and collections.
class Expr;
using ExprCP = const Expr*;
using ExprVector = QGVector<ExprCP>;

class Column;
using ColumnCP = const Column*;
using ColumnVector = QGVector<ColumnCP>;

class Literal;
using LiteralCP = const Literal*;

} // namespace facebook::axiom::optimizer
