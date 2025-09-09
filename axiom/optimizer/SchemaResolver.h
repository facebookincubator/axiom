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
#include <utility>

#include "axiom/optimizer/connectors/ConnectorMetadata.h"

namespace facebook::axiom::optimizer {

class SchemaResolver {
 public:
  explicit SchemaResolver(std::string defaultSchema = "")
      : defaultSchema_{std::move(defaultSchema)} {}

  virtual ~SchemaResolver() = default;

  // Converts a table name to a resolved Table, or nullptr if the table doesn't
  // exist. If a connector for the specified catalog doesn't exist, an error
  // will be returned. Input table name can be any of the following formats:
  //   - "tablename"
  //   - "schema.tablename"
  //   - "catalog.schema.tablename"
  // If schema is omitted, defaultSchema will be prepended prior to lookup.
  // If the table name specifies a different catalog than the one specified
  // as a parameter, an error will be thrown.
  virtual velox::connector::TablePtr findTable(
      const std::string& catalog,
      const std::string& name);

 private:
  const std::string defaultSchema_;
};

} // namespace facebook::axiom::optimizer
