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
#include "axiom/optimizer/BitSet.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::velox::optimizer {

QueryGraphContext::QueryGraphContext(velox::HashStringAllocator& allocator)
    : allocator_(allocator), cache_(allocator_) {
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

QueryGraphContext*& queryCtx() {
  static thread_local QueryGraphContext* context;
  return context;
}

PlanObjectP QueryGraphContext::dedup(PlanObjectP object) {
  auto pair = deduppedObjects_.insert(object);
  return *pair.first;
}

const char* QueryGraphContext::toName(std::string_view str) {
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

const Type* QueryGraphContext::toType(const TypePtr& type) {
  return dedupType(type).get();
}

TypePtr QueryGraphContext::dedupType(const TypePtr& type) {
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
  std::vector<TypePtr> children;
  children.reserve(size);
  for (auto i = 0; i < size; ++i) {
    children.push_back(dedupType(type->childAt(i)));
  }
  TypePtr newType;
  switch (type->kind()) {
    case TypeKind::ROW: {
      std::vector<std::string> names;
      names.reserve(size);
      for (auto i = 0; i < size; ++i) {
        names.push_back(type->as<TypeKind::ROW>().nameOf(i));
      }
      newType = ROW(std::move(names), std::move(children));
    } break;
    case TypeKind::ARRAY:
      newType = ARRAY(children[0]);
      break;
    case TypeKind::MAP:
      newType = MAP(children[0], children[1]);
      break;
    case TypeKind::FUNCTION: {
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

const TypePtr& QueryGraphContext::toTypePtr(const Type* type) {
  auto it = toTypePtr_.find(type);
  if (it != toTypePtr_.end()) {
    return it->second;
  }
  VELOX_FAIL("Cannot translate {} back to TypePtr", type->toString());
}

const Type* toType(const TypePtr& type) {
  return queryCtx()->toType(type);
}

const TypePtr& toTypePtr(const Type* type) {
  return queryCtx()->toTypePtr(type);
}

const BaseVector* QueryGraphContext::toVector(const VectorPtr& vector) {
  auto it = deduppedVectors_.find(vector.get());
  if (it != deduppedVectors_.end()) {
    return it->second.get();
  }
  deduppedVectors_[vector.get()] = vector;
  return vector.get();
}

VectorPtr QueryGraphContext::toVectorPtr(const BaseVector* vector) {
  auto it = deduppedVectors_.find(vector);
  VELOX_CHECK(it != deduppedVectors_.end());
  return it->second;
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
    std::sort(set.begin(), set.end(), [](PathCP left, PathCP right) {
      return *left < *right;
    });
  }

  for (int32_t i = 0; i < bySize.size() - 1; ++i) {
    for (auto path : bySize[i]) {
      // Delete paths where 'path' is a prefix.
      for (int32_t size = i + 1; size < bySize.size(); ++size) {
        ptrdiff_t firstErase = -1;
        auto& paths = bySize[size];
        auto it = std::lower_bound(paths.begin(), paths.end(), path);
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

} // namespace facebook::velox::optimizer
