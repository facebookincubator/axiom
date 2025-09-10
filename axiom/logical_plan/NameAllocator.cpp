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
#include "axiom/logical_plan/NameAllocator.h"
#include <velox/common/base/Exceptions.h>

namespace facebook::axiom::logical_plan {
namespace {

bool isAllDigits(std::string_view str) {
  return std::ranges::all_of(str, isdigit);
}

} // namespace

std::string NameAllocator::newName(const std::string& hint) {
  VELOX_CHECK(!hint.empty(), "Hint cannot be empty");

  // Strip suffix past '_' if all digits.
  std::string prefix = hint;

  auto pos = prefix.rfind('_');
  if (pos != std::string::npos &&
      isAllDigits({prefix.data() + pos + 1, prefix.size() - pos - 1})) {
    prefix = prefix.substr(0, pos);
  }

  std::string name = prefix;
  do {
    if (names_.insert(name).second) {
      return name;
    }
    name = fmt::format("{}_{}", prefix, nextId_++);
  } while (true);
}

} // namespace facebook::axiom::logical_plan
