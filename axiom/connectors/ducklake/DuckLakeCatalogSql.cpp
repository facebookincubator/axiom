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

#include "axiom/connectors/ducklake/DuckLakeCatalogSql.h"

namespace facebook::axiom::connector::ducklake {

std::string quoteDuckLakeCatalogSqlString(std::string_view value) {
  std::string result{"'"};
  result.reserve(value.size() + 2);
  for (const auto c : value) {
    if (c == '\'') {
      result += "''";
    } else {
      result.push_back(c);
    }
  }
  result.push_back('\'');
  return result;
}

} // namespace facebook::axiom::connector::ducklake
