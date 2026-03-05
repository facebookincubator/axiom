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

#include "axiom/connectors/SchemaResolver.h"
#include "axiom/connectors/SchemaUtils.h"

namespace facebook::axiom::connector {

void SchemaResolver::setTargetTable(std::string_view catalog, TablePtr table) {
  VELOX_CHECK_NULL(targetTable_);

  targetCatalog_ = catalog;
  targetTable_ = std::move(table);
}

TablePtr SchemaResolver::findTable(
    std::string_view catalog,
    std::string_view name) const {
  TableNameParser parser(name);
  VELOX_USER_CHECK(parser.valid(), "Invalid table name: '{}'", name);

  if (parser.catalog().has_value()) {
    VELOX_USER_CHECK_EQ(
        catalog,
        parser.catalog().value(),
        "Input catalog must match table catalog specifier");
  }

  std::string lookupName;
  if (parser.schema().has_value()) {
    lookupName = fmt::format("{}.{}", parser.schema().value(), parser.table());
  } else if (!defaultSchema_.empty()) {
    lookupName = fmt::format("{}.{}", defaultSchema_, parser.table());
  } else {
    lookupName = parser.table();
  }

  if (targetCatalog_ == catalog && targetTable_->name() == lookupName) {
    return targetTable_;
  }

  return ConnectorMetadata::metadata(catalog)->findTable(lookupName);
}

} // namespace facebook::axiom::connector
