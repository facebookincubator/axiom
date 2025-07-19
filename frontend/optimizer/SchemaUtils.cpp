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

#include "optimizer/SchemaUtils.h" //@manual

#include "folly/String.h"

namespace facebook::velox::optimizer {

TableNameParser::TableNameParser(const std::string& name) {
  std::vector<std::string> parts;
  folly::split('.', name, parts);

  bool anyempty =
      std::any_of(parts.begin(), parts.end(), [](const std::string& part) {
        return part.empty();
      });
  if (anyempty) {
    return;
  }

  valid_ = true;
  switch (parts.size()) {
    case 1:
      table_ = std::move(parts[0]);
      break;
    case 2:
      schema_ = std::move(parts[0]);
      table_ = std::move(parts[1]);
      break;
    case 3:
      catalog_ = std::move(parts[0]);
      schema_ = std::move(parts[1]);
      table_ = std::move(parts[2]);
      break;
    default:
      valid_ = false;
      break;
  }
}

} // namespace facebook::velox::optimizer
