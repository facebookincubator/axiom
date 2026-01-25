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

#include <memory>
#include <string>
#include <vector>

#include "folly/executors/IOThreadPoolExecutor.h"
#include "velox/connectors/Connector.h"

namespace facebook::axiom {

/**
 * Helper class to register connectors for Axiom and Velox.
 *
 * This class handles the details of registering TPCH connectors
 * and connectors for tables stored in the local filesystem in
 * Parquet or ORC format.
 */
class Connectors {
 public:
  static constexpr const char* kTpchConnectorId = "tpch";

  /// Unregister all connectors with ids in `connectorIds_`.
  virtual ~Connectors();

  /// Initialize file formats and ioExecutor. Must be called before
  /// registering any connectors.
  void initialize();

  /// Registers the TPCH connector under the connector ID "tpch".
  /// This allows queries like "select * from tpch.sf1.lineitem".
  std::shared_ptr<velox::connector::Connector> registerTpchConnector();

  /// Registers the connector for tables stored in the local filesystem under
  /// `dataPath`. Table "foo" will be stored as files in format `dataFormat` in
  /// the directory `${dataPath}/foo`. Allowed formats are "parquet" and "orc".
  ///
  ///  The connector is registered under `connectorId`, so queries can access
  ///  local tables like "INSERT INTO ${connectorId}.write_table SELECT * FROM
  ///  ${connectorId}.read_table".
  std::shared_ptr<velox::connector::Connector> registerLocalHiveConnector(
      const std::string& connectorId,
      const std::string& dataPath,
      const std::string& dataFormat);

 protected:
  folly::Executor* ioExecutor() {
    return ioExecutor_.get();
  }

  // Unregister these on destruction.
  std::vector<std::string> connectorIds_{};

 private:
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
};

} // namespace facebook::axiom
