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
#include "optimizer/connectors/ConnectorMetadata.h" //@manual
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::optimizer {

class SchemaResolver {
 public:
  SchemaResolver(
      const std::shared_ptr<connector::Connector>& defaultConnector,
      const std::string& defaultSchema)
      : defaultConnector_(defaultConnector), defaultSchema_(defaultSchema) {}

  virtual ~SchemaResolver() = default;

  // Converts a table name to a resolved Table, or nullptr if the table doesn't
  // exist. Input can be any of the following formats:
  //   - "tablename"
  //   - "schema.tablename"
  //   - "catalog.schema.tablename"
  // If catalog is omitted, defaultConnector will be used for the table lookup.
  // If schema is omitted, defaultSchema will be prepended prior to lookup.
  virtual const connector::Table* findTable(const std::string& name);

 private:
  // Connector to use if name does not specify a catalog.
  const std::shared_ptr<connector::Connector> defaultConnector_;
  const std::string defaultSchema_;
};

} // namespace facebook::velox::optimizer
