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

#include "axiom/optimizer/SchemaUtils.h"

#include "folly/String.h"

namespace facebook::velox::optimizer {

TableNameParser::TableNameParser(std::string_view name) {
  std::vector<std::string_view> parts;
  folly::split('.', name, parts);

  const bool anyEmpty = std::ranges::any_of(
      parts, [](std::string_view part) { return part.empty(); });
  if (anyEmpty) {
    return;
  }

  valid_ = true;
  switch (parts.size()) {
    case 1:
      table_ = parts[0];
      break;
    case 2:
      schema_ = parts[0];
      table_ = parts[1];
      break;
    case 3:
      catalog_ = parts[0];
      schema_ = parts[1];
      table_ = parts[2];
      break;
    default:
      valid_ = false;
      break;
  }
}

} // namespace facebook::velox::optimizer
