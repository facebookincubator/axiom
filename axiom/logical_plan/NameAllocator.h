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

#include <string>
#include <unordered_set>

namespace facebook::axiom::logical_plan {

/// Generate unique names based on user-provided hints.
class NameAllocator {
 public:
  /// Returns 'hint' as is it is unique. Otherwise, return 'hint_N' where N is a
  /// numeric suffix appended to ensure uniqueness. If 'hint' already has a
  /// suffix and is not unique, the suffix is replaced with a new one.
  ///
  /// Example:
  ///
  ///   newName("a") -> "a"
  ///   newName("a") -> "a_0"
  ///   newName("a") -> "a_1"
  ///   newName("a_0") -> "a_2"
  std::string newName(const std::string& hint);

  void reset() {
    names_.clear();
    nextId_ = 0;
  }

 private:
  std::unordered_set<std::string> names_;
  int32_t nextId_{0};
};

} // namespace facebook::axiom::logical_plan
