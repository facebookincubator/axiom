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

#include "axiom/application/SessionProperties.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/core/QueryConfig.h"
#include "velox/core/QueryConfigProvider.h"
#include "velox/functions/prestosql/PrestoConfigProvider.h"
#include "velox/functions/prestosql/PrestoQueryConfig.h"

namespace facebook::axiom::application {
namespace {

using velox::core::QueryConfig;
using velox::functions::prestosql::PrestoQueryConfig;

std::shared_ptr<const ConfigRegistry> makeRegistry() {
  auto registry = std::make_shared<ConfigRegistry>();
  registry->add(
      "execution", std::make_shared<velox::core::QueryConfigProvider>());
  registry->add(
      PrestoQueryConfig::kPrefix,
      std::make_shared<velox::functions::prestosql::PrestoConfigProvider>());
  registry->add("c", std::make_shared<velox::core::QueryConfigProvider>());
  return registry;
}

TEST(SessionPropertiesTest, queryConfig) {
  SessionConfig session{makeRegistry()};
  session.set("execution", QueryConfig::kSessionTimezone, "Asia/Tokyo");
  session.set(
      PrestoQueryConfig::kPrefix,
      PrestoQueryConfig::kArrayAggIgnoreNulls,
      "true");

  const auto config = SessionProperties::queryConfig(session, "execution");
  EXPECT_EQ(config.at(QueryConfig::kSessionTimezone), "Asia/Tokyo");
  EXPECT_EQ(
      config.at(
          PrestoQueryConfig::qualify(PrestoQueryConfig::kArrayAggIgnoreNulls)),
      "true");
  EXPECT_EQ(QueryConfig{config}.sessionTimezone(), "Asia/Tokyo");
}

TEST(SessionPropertiesTest, connectors) {
  SessionConfig session{makeRegistry()};
  session.set("c", QueryConfig::kSessionTimezone, "Asia/Tokyo");

  const auto properties =
      SessionProperties::connectorProperties(session, {"c", "missing"});
  EXPECT_THAT(properties, testing::SizeIs(1));
  EXPECT_EQ(properties.at("c").at(QueryConfig::kSessionTimezone), "Asia/Tokyo");

  const auto configs =
      SessionProperties::connectorConfigs(session, {"c", "missing"});
  EXPECT_THAT(configs, testing::SizeIs(1));
  EXPECT_EQ(
      configs.at("c")->get<std::string>(QueryConfig::kSessionTimezone),
      "Asia/Tokyo");
}

} // namespace
} // namespace facebook::axiom::application
