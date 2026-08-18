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

#include "axiom/connectors/hive/HiveMetadataConfig.h"
#include "gtest/gtest.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/hive/HiveConfig.h"

namespace facebook::axiom::connector::hive {
namespace {

ConnectorSessionPtr makeSession(Properties properties) {
  return std::make_shared<ConnectorSession>(
      "query", "user", std::move(properties));
}

TEST(HiveMetadataConfigTest, defaultConfig) {
  HiveMetadataConfig config(
      std::make_shared<velox::config::ConfigBase>(
          std::unordered_map<std::string, std::string>()),
      "hive");
  ASSERT_EQ(config.localDataPath(), "");
  ASSERT_EQ(config.localFileFormat(), "");
  EXPECT_TRUE(config.readTimestampPartitionValueAsLocalTime(nullptr));
}

TEST(HiveMetadataConfigTest, overrideConfig) {
  std::unordered_map<std::string, std::string> properties = {
      {HiveMetadataConfig::kLocalDataPath, "/path/to/data"},
      {HiveMetadataConfig::kLocalFileFormat, "parquet"}};
  HiveMetadataConfig config(
      std::make_shared<velox::config::ConfigBase>(std::move(properties)),
      "hive");
  ASSERT_EQ(config.localDataPath(), "/path/to/data");
  ASSERT_EQ(config.localFileFormat(), "parquet");
}

TEST(HiveMetadataConfigTest, timestampPartitionSetting) {
  HiveMetadataConfig config(
      std::make_shared<velox::config::ConfigBase>(
          std::unordered_map<std::string, std::string>{
              {velox::connector::hive::FileConfig::
                   kReadTimestampPartitionValueAsLocalTime,
               "false"}}),
      "hive");
  EXPECT_FALSE(config.readTimestampPartitionValueAsLocalTime(makeSession({})));
  EXPECT_TRUE(config.readTimestampPartitionValueAsLocalTime(makeSession(
      {{velox::connector::hive::FileConfig::
            kReadTimestampPartitionValueAsLocalTimeSession,
        "true"}})));
  EXPECT_TRUE(config.readTimestampPartitionValueAsLocalTime(makeSession(
      {{std::string("hive.") +
            velox::connector::hive::FileConfig::
                kReadTimestampPartitionValueAsLocalTimeSession,
        "true"}})));
}

} // namespace
} // namespace facebook::axiom::connector::hive
