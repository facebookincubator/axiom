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

#include "axiom/connectors/ConnectorSession.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::axiom::connector::hive {

class HiveMetadataConfig {
 public:
  /// The file system path containing local data to be used for query
  /// planning and execution by LocalHiveConnectorMetadata.
  static constexpr const char* kLocalDataPath = "hive_local_data_path";

  /// The name of the file format to use for processing data at kLocalDataPath.
  /// Supported values: "parquet", "dwrf", "text". Defaults to "parquet" if
  /// not specified.
  static constexpr const char* kLocalFileFormat = "hive_local_file_format";

  /// Session property: the maximum number of partitions a discrete-predicate
  /// listing (see TableLayout::discretePredicates) may return before the
  /// connector fails the query. Unset means unlimited.
  static constexpr const char* kMaxPartitionsPerDiscretePredicates =
      "hive_max_partitions_per_discrete_predicates";

  std::string localDataPath() const;

  std::string localFileFormat() const;

  /// Applies the session override when present, otherwise the connector
  /// setting.
  bool readTimestampPartitionValueAsLocalTime(
      const ConnectorSessionPtr& session) const;

  /// Applies the same precedence to a caller-supplied property map, for
  /// callers holding session properties rather than a session.
  bool readTimestampPartitionValueAsLocalTime(
      const velox::config::ConfigBase* sessionProperties) const;

  /// HiveMetadataConfig may be initialized from a base config which also
  /// contains execution config defined in velox/connectors/hive/HiveConfig.h,
  /// so some additional properties may be present which are not defined here.
  /// 'connectorId' is the catalog name a session may prefix a property with.
  HiveMetadataConfig(
      std::shared_ptr<const velox::config::ConfigBase> config,
      std::string connectorId)
      : config_{std::move(config)}, connectorId_{std::move(connectorId)} {
    VELOX_CHECK_NOT_NULL(
        config_, "Config is null for HiveMetadataConfig initialization");
    VELOX_CHECK(!connectorId_.empty(), "HiveMetadataConfig needs a connector");
  }

 protected:
  const std::shared_ptr<const velox::config::ConfigBase>& config() const {
    return config_;
  }

 private:
  std::shared_ptr<const velox::config::ConfigBase> config_;
  const std::string connectorId_;
};

} // namespace facebook::axiom::connector::hive
