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

#include <utility>

#include <folly/container/F14Map.h>

namespace facebook::axiom::optimizer {

/// Tracks the transitive closure of equalities over pointer-identity values.
/// Each class of transitively equal values has one representative:
///
///   EquivalenceClosure<ColumnCP> closure;
///   closure.merge(a, b);
///   closure.merge(b, c);
///   closure.find(a) == closure.find(c);  // true
template <typename T>
class EquivalenceClosure {
 public:
  /// Adds 'value' as a class of its own unless it is already in one.
  void add(T value) {
    parents_.try_emplace(value, value);
  }

  /// Returns the representative of the class of 'value', adding 'value' first
  /// if it is new.
  T find(T value) {
    add(value);
    T root = value;
    while (parents_[root] != root) {
      root = parents_[root];
    }
    while (value != root) {
      T next = parents_[value];
      parents_[value] = root;
      value = next;
    }
    return root;
  }

  /// Merges the classes of 'left' and 'right'. The representative of the
  /// class of 'left' represents the merged class.
  void merge(T left, T right) {
    merge(left, right, [](T, T) { return false; });
  }

  /// Merges the classes of 'left' and 'right'. 'better(x, y)' returns true
  /// when representative 'x' is preferred over 'y'; on a tie the
  /// representative of the class of 'left' stays.
  template <typename Better>
  void merge(T left, T right, Better better) {
    left = find(left);
    right = find(right);
    if (left == right) {
      return;
    }
    if (better(right, left)) {
      std::swap(left, right);
    }
    parents_[right] = left;
  }

 private:
  // Maps each value to its parent. A representative is its own parent.
  folly::F14FastMap<T, T> parents_;
};

} // namespace facebook::axiom::optimizer
