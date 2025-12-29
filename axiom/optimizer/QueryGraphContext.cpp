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

#include "axiom/optimizer/QueryGraphContext.h"

#include <algorithm>
#include "axiom/optimizer/BitSet.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer {

QueryGraphContext::QueryGraphContext(velox::HashStringAllocator& allocator)
    : allocator_(allocator),
      cache_(allocator_)
#ifndef NDEBUG
      ,
      creationThreadId_(std::this_thread::get_id())
#endif
{
  auto addName = [&](const char* name) {
    names_.emplace(std::string_view(name, strlen(name)));
  };

  addName(SpecialFormCallNames::kAnd);
  addName(SpecialFormCallNames::kOr);
  addName(SpecialFormCallNames::kCast);
  addName(SpecialFormCallNames::kTryCast);
  addName(SpecialFormCallNames::kTry);
  addName(SpecialFormCallNames::kCoalesce);
  addName(SpecialFormCallNames::kIf);
  addName(SpecialFormCallNames::kSwitch);
  addName(SpecialFormCallNames::kIn);
}

int32_t QueryGraphContext::newId(PlanObject* object) {
  if (threadSafe_) {
    std::lock_guard<std::recursive_mutex> lock(allocateMutex_);
    return newIdLocked(object);
  }
  return newIdLocked(object);
}

int32_t QueryGraphContext::newIdLocked(PlanObject* object) {
  objects_.push_back(object);
  return static_cast<int32_t>(objects_.size() - 1);
}

void* QueryGraphContext::allocate(int32_t size) {
  if (threadSafe_) {
    std::lock_guard<std::recursive_mutex> lock(allocateMutex_);
    return allocateLocked(size);
  }
  return allocateLocked(size);
}

void* QueryGraphContext::allocateLocked(int32_t size) {
#ifndef NDEBUG
  if (!threadSafe_) {
    VELOX_CHECK_EQ(
        std::this_thread::get_id(),
        creationThreadId_,
        "allocate() called on different thread than QueryGraphContext was created on");
  }
#endif
#ifdef QG_TEST_USE_MALLOC
  // Benchmark-only. Dropping the arena will not free un-free'd allocs.
  return ::malloc(size);
#elif defined(QG_CACHE_ARENA)
  return cache_.allocate(size);
#else
  return allocator_.allocate(size)->begin();
#endif
}

void QueryGraphContext::free(void* ptr) {
  if (threadSafe_) {
    std::lock_guard<std::recursive_mutex> lock(allocateMutex_);
    freeLocked(ptr);
    return;
  }
  freeLocked(ptr);
}

void QueryGraphContext::freeLocked(void* ptr) {
#ifndef NDEBUG
  if (!threadSafe_) {
    VELOX_CHECK_EQ(
        std::this_thread::get_id(),
        creationThreadId_,
        "free() called on different thread than QueryGraphContext was created on");
  }
#endif
#ifdef QG_TEST_USE_MALLOC
  ::free(ptr);
#elif defined(QG_CACHE_ARENA)
  cache_.free(ptr);
#else
  allocator_.free(velox::HashStringAllocator::headerOf(ptr));
#endif
}

QueryGraphContext*& queryCtx() {
  static thread_local QueryGraphContext* context;
  return context;
}

bool statsFetched() {
  return queryCtx()->optimization()->statsFetched();
}

const char* QueryGraphContext::toName(std::string_view str) {
  if (threadSafe_) {
    std::lock_guard<std::recursive_mutex> lock(allocateMutex_);
    return toNameLocked(str);
  }
  return toNameLocked(str);
}

const char* QueryGraphContext::toNameLocked(std::string_view str) {
  auto it = names_.find(str);
  if (it != names_.end()) {
    return it->data();
  }

  char* data = allocator_.allocate(str.size() + 1)->begin(); // NOLINT
  memcpy(data, str.data(), str.size());
  data[str.size()] = 0;
  names_.insert(std::string_view(data, str.size()));
  return data;
}

Name toName(std::string_view string) {
  return queryCtx()->toName(string);
}

const velox::Type* QueryGraphContext::toType(const velox::TypePtr& type) {
  if (threadSafe_) {
    std::lock_guard<std::recursive_mutex> lock(allocateMutex_);
    return toTypeLocked(type);
  }
  return toTypeLocked(type);
}

const velox::Type* QueryGraphContext::toTypeLocked(const velox::TypePtr& type) {
  return dedupType(type).get();
}

velox::TypePtr QueryGraphContext::dedupType(const velox::TypePtr& type) {
  auto it = deduppedTypes_.find(type);
  if (it != deduppedTypes_.end()) {
    return *it;
  }
  auto size = type->size();
  if (size == 0) {
    deduppedTypes_.insert(type);
    toTypePtr_[type.get()] = type;
    return type;
  }
  std::vector<velox::TypePtr> children;
  children.reserve(size);
  for (auto i = 0; i < size; ++i) {
    children.push_back(dedupType(type->childAt(i)));
  }
  velox::TypePtr newType;
  switch (type->kind()) {
    case velox::TypeKind::ROW: {
      std::vector<std::string> names;
      names.reserve(size);
      for (auto i = 0; i < size; ++i) {
        names.push_back(type->as<velox::TypeKind::ROW>().nameOf(i));
      }
      newType = ROW(std::move(names), std::move(children));
    } break;
    case velox::TypeKind::ARRAY:
      newType = ARRAY(children[0]);
      break;
    case velox::TypeKind::MAP:
      newType = MAP(children[0], children[1]);
      break;
    case velox::TypeKind::FUNCTION: {
      auto returnType = std::move(children.back());
      children.pop_back();
      newType = FUNCTION(std::move(children), std::move(returnType));
    } break;
    default:
      VELOX_FAIL("Type has size > 0 and is not row/array/map");
  }
  deduppedTypes_.insert(newType);
  toTypePtr_[newType.get()] = newType;
  return newType;
}

const velox::TypePtr& QueryGraphContext::toTypePtr(const velox::Type* type) {
  auto it = toTypePtr_.find(type);
  if (it != toTypePtr_.end()) {
    return it->second;
  }
  VELOX_FAIL("Cannot translate {} back to TypePtr", type->toString());
}

const velox::Type* toType(const velox::TypePtr& type) {
  return queryCtx()->toType(type);
}

const velox::TypePtr& toTypePtr(const velox::Type* type) {
  return queryCtx()->toTypePtr(type);
}

bool Step::operator==(const Step& other) const {
  return kind == other.kind && field == other.field && id == other.id;
}

bool Step::operator<(const Step& other) const {
  if (kind != other.kind) {
    return kind < other.kind;
  }
  if (field != other.field) {
    return field < other.field;
  }
  return id < other.id;
}

size_t Step::hash() const {
  return 1 + static_cast<int32_t>(kind) + reinterpret_cast<size_t>(field) + id;
}

size_t Path::hash() const {
  size_t h = 123;
  for (auto& step : steps_) {
    h = (h + 1921) * step.hash();
  }
  return h;
}

bool Path::operator==(const Path& other) const {
  if (steps_.size() != other.steps_.size()) {
    return false;
  }

  for (size_t i = 0; i < steps_.size(); ++i) {
    if (steps_[i] != other.steps_[i]) {
      return false;
    }
  }
  return true;
}

bool Path::operator<(const Path& other) const {
  for (auto i = 0; i < steps_.size() && i < other.steps_.size(); ++i) {
    if (steps_[i] < other.steps_[i]) {
      return true;
    }
  }
  return steps_.size() < other.steps_.size();
}

bool Path::hasPrefix(const Path& prefix) const {
  if (prefix.steps_.size() >= steps_.size()) {
    return false;
  }

  for (size_t i = 0; i < prefix.steps_.size(); ++i) {
    if (steps_[i] != prefix.steps_[i]) {
      return false;
    }
  }
  return true;
}

std::string Path::toString() const {
  if (steps_.empty()) {
    return "<empty>";
  }

  std::stringstream out;
  for (auto& step : steps_) {
    switch (step.kind) {
      case StepKind::kCardinality:
        out << ".cardinality";
        break;
      case StepKind::kField:
        if (step.field) {
          out << "." << step.field;
          break;
        }
        out << fmt::format("__{}", step.id);
        break;
      case StepKind::kSubscript:
        if (step.field) {
          out << "[" << step.field << "]";
        } else {
          out << "[" << step.id << "]";
        }
        break;
    }
  }
  return out.str();
}

PathCP QueryGraphContext::toPath(PathCP path) {
  if (threadSafe_) {
    std::lock_guard<std::recursive_mutex> lock(allocateMutex_);
    return toPathLocked(path);
  }
  return toPathLocked(path);
}

PathCP QueryGraphContext::toPathLocked(PathCP path) {
  path->setId(static_cast<int32_t>(pathById_.size()));
  path->makeImmutable();
  auto pair = deduppedPaths_.insert(path);
  if (path != *pair.first) {
    delete path;
  } else {
    pathById_.push_back(path);
  }
  return *pair.first;
}

void Path::subfieldSkyline(BitSet& subfields) {
  if (subfields.empty()) {
    return;
  }

  // Expand the ids to fields and remove subfields where there exists a shorter
  // prefix.

  auto ctx = queryCtx();
  bool allFields = false;
  std::vector<std::vector<PathCP>> bySize;
  subfields.forEach([&](auto id) {
    auto path = ctx->pathById(id);
    auto size = path->steps().size();
    if (size == 0) {
      allFields = true;
    }
    if (!allFields) {
      --size;
      if (size >= bySize.size()) {
        bySize.resize(size + 1);
      }
      bySize[size].push_back(path);
    }
  });

  if (allFields) {
    subfields = BitSet();
    return;
  }

  for (auto& set : bySize) {
    std::ranges::sort(
        set, [](PathCP left, PathCP right) { return *left < *right; });
  }

  for (int32_t i = 0; i < bySize.size() - 1; ++i) {
    for (auto path : bySize[i]) {
      // Delete paths where 'path' is a prefix.
      for (int32_t size = i + 1; size < bySize.size(); ++size) {
        ptrdiff_t firstErase = -1;
        auto& paths = bySize[size];
        auto it = std::ranges::lower_bound(paths, path);
        if (it != paths.end() && !(*it)->hasPrefix(*path)) {
          ++it;
        }
        while (it != paths.end() && (*it)->hasPrefix(*path)) {
          if (firstErase < 0) {
            firstErase = it - paths.begin();
          }
          subfields.erase((*it)->id());
          ++it;
        }
        if (firstErase != -1) {
          paths.erase(paths.begin() + firstErase, it);
        }
      }
    }
  }
}

PathCP toPath(std::span<const Step> steps, bool reverse) {
  PathCP path = reverse ? make<Path>(steps, std::true_type{})
                        : make<Path>(steps, std::false_type{});
  return queryCtx()->toPath(path);
}

const velox::Variant* QueryGraphContext::registerVariant(
    std::shared_ptr<const velox::Variant> value) {
  auto pair = variants_.insert(value);
  auto* rawPtr = pair.first->get();
  reverseConstantDedup_[rawPtr] = value;
  return rawPtr;
}

} // namespace facebook::axiom::optimizer
