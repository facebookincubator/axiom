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

namespace facebook::axiom::connector::hive {
namespace {

TEST(HiveMetadataConfigTest, defaultConfig) {
  HiveMetadataConfig config(
      std::make_shared<velox::config::ConfigBase>(
          std::unordered_map<std::string, std::string>()));
  ASSERT_EQ(config.localDataPath(), "");
  ASSERT_EQ(config.localFileFormat(), "");
}

TEST(HiveMetadataConfigTest, overrideConfig) {
  std::unordered_map<std::string, std::string> properties = {
      {HiveMetadataConfig::kLocalDataPath, "/path/to/data"},
      {HiveMetadataConfig::kLocalFileFormat, "parquet"}};
  HiveMetadataConfig config(
      std::make_shared<velox::config::ConfigBase>(std::move(properties)));
  ASSERT_EQ(config.localDataPath(), "/path/to/data");
  ASSERT_EQ(config.localFileFormat(), "parquet");
}

} // namespace
} // namespace facebook::axiom::connector::hive
