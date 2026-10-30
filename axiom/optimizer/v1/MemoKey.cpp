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
#include "axiom/optimizer/v1/MemoKey.h"

#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer::v1 {

size_t MemoKey::hash() const {
  size_t hash = tables.hash();
  for (auto& exists : existences) {
    hash = velox::bits::commutativeHashMix(hash, exists.hash());
  }
  return hash;
}

bool MemoKey::operator==(const MemoKey& other) const {
  if (firstTable == other.firstTable && columns == other.columns &&
      tables == other.tables) {
    if (existences.size() != other.existences.size()) {
      return false;
    }
    for (auto& e : existences) {
      for (auto& e2 : other.existences) {
        if (e2 == e) {
          break;
        }
      }
    }
    return true;
  }
  return false;
}

std::string MemoKey::toString() const {
  auto result = fmt::format(
      "MemoKey({}, columns: {}, tables: {}",
      cname(firstTable),
      columns.toString(true),
      tables.toString(true));
  if (!existences.empty()) {
    result += ", existences: ";
    for (auto i = 0; i < existences.size(); ++i) {
      if (i > 0) {
        result += "; ";
      }
      result += existences[i].toString(true);
    }
  }
  result += ")";
  return result;
}

} // namespace facebook::axiom::optimizer::v1
