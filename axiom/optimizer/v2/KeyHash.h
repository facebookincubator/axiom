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

#include <type_traits>

#include "axiom/optimizer/QueryGraph.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::axiom::optimizer::v2 {

/// Hashes a single value. Pointer / integer / enum / range / Frame are
/// handled generically; pointer-elements of ranges are hashed by address,
/// matching how `Key::operator==` compares them.
template <typename T>
size_t hashOne(const T& value);

// Forward declaration: keeps `hashOf` (template) able to resolve to this
// non-template overload during phase-1 name lookup; otherwise the generic
// `hashOne<Frame>` template hits the catch-all static_assert.
size_t hashOne(const Frame& frame);

/// Hashes a list of fields by combining `hashOne` of each via `hashMix`.
template <typename... Args>
size_t hashOf(const Args&... args) {
  size_t hash = 0;
  ((hash = velox::bits::hashMix(hash, hashOne(args))), ...);
  return hash;
}

inline size_t hashOne(const Frame& frame) {
  return hashOf(
      frame.type,
      frame.startType,
      frame.startValue,
      frame.endType,
      frame.endValue);
}

template <typename T>
size_t hashOne(const T& value) {
  if constexpr (std::is_pointer_v<T>) {
    return reinterpret_cast<uintptr_t>(value);
  } else if constexpr (std::is_enum_v<T>) {
    return static_cast<size_t>(value);
  } else if constexpr (std::is_integral_v<T>) {
    return static_cast<size_t>(value);
  } else if constexpr (requires {
                         value.begin();
                         value.end();
                       }) {
    size_t hash = 0;
    for (const auto& element : value) {
      hash = velox::bits::hashMix(hash, hashOne(element));
    }
    return hash;
  } else {
    static_assert(
        sizeof(T) == 0, "Add a hashOne overload for this type in KeyHash.h");
  }
}

} // namespace facebook::axiom::optimizer::v2
