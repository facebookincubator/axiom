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

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer {

/// Parses table names of the following formats:
///    - "tablename"
///    - "schema.tablename"
///    - "catalog.schema.tablename"
/// Including performing validity checks on the input.
/// Catalog and schema are optional, but input without
/// a table name present will be marked as invalid.
class TableNameParser {
 public:
  explicit TableNameParser(std::string_view name);

  /// False if the input name was invalid.
  bool valid() const {
    return valid_;
  }

  /// The catalog of the input name, or nullopt if catalog
  /// was not present.
  const std::optional<std::string>& catalog() const {
    VELOX_CHECK(valid_, "invalid catalog name access");
    return catalog_;
  }

  /// The schema of the input name, or nullopt if schema
  /// was not present.
  const std::optional<std::string>& schema() const {
    VELOX_CHECK(valid_, "invalid schema name access");
    return schema_;
  }

  /// The table portion of the input name.
  const std::string& table() const {
    VELOX_CHECK(valid_, "invalid table name access");
    return table_;
  }

 private:
  bool valid_ = false;
  std::optional<std::string> catalog_;
  std::optional<std::string> schema_;
  std::string table_;
};

} // namespace facebook::axiom::optimizer
