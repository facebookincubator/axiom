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

// Tests for ConfigRegistry and SessionConfig.
//
// Uses a TestConfigProvider that declares a few properties of
// different types to exercise:
// - Registry: add, resolve, all, duplicate prefix, unknown property
// - SessionConfig: set, get, reset, getAll, type validation, all()

#include "axiom/common/SessionConfig.h"
#include <gtest/gtest.h>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::axiom;
using namespace facebook::velox::config;

namespace {

class TestConfigProvider : public ConfigProvider {
 public:
  std::vector<ConfigProperty> properties() const override {
    return {
        {"flag_a", ConfigPropertyType::kBoolean, "true", "A boolean flag."},
        {"count", ConfigPropertyType::kInteger, "10", "An integer count."},
        {"ratio", ConfigPropertyType::kDouble, "0.5", "A double ratio."},
        {"label", ConfigPropertyType::kString, std::nullopt, "A string label."},
    };
  }

  std::string normalize(std::string_view name, std::string_view value)
      const override {
    // Normalize label to lowercase (simulates enum-like normalization).
    if (name == "label") {
      auto lower = std::string(value);
      std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
      return lower;
    }
    return std::string(value);
  }
};

class SessionConfigTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto registry = std::make_shared<ConfigRegistry>();
    registry->add("test", std::make_shared<TestConfigProvider>());
    registry_ = registry;
    config_ = std::make_unique<SessionConfig>(registry_);
  }

  // Finds an entry by property name in the all() result.
  SessionConfig::Entry findEntry(std::string_view name) const {
    auto entries = config_->all();
    auto it = std::find_if(entries.begin(), entries.end(), [&](const auto& e) {
      return e.property.name == name;
    });
    VELOX_CHECK(it != entries.end(), "Property not found: {}", name);
    return *it;
  }

  std::shared_ptr<const ConfigRegistry> registry_;
  std::unique_ptr<SessionConfig> config_;
};

TEST_F(SessionConfigTest, getDefault) {
  EXPECT_EQ(config_->effectiveValue("test.flag_a"), "true");
  EXPECT_EQ(config_->effectiveValue("test.count"), "10");
  EXPECT_EQ(config_->effectiveValue("test.ratio"), "0.5");
  EXPECT_EQ(config_->effectiveValue("test.label"), std::nullopt);
}

TEST_F(SessionConfigTest, setAndGet) {
  EXPECT_TRUE(config_->set("test.flag_a", "false"));
  EXPECT_EQ(config_->effectiveValue("test.flag_a"), "false");

  // Setting same value returns false.
  EXPECT_FALSE(config_->set("test.flag_a", "false"));
}

TEST_F(SessionConfigTest, resetToDefault) {
  config_->set("test.flag_a", "false");
  EXPECT_TRUE(config_->reset("test.flag_a"));
  EXPECT_EQ(config_->effectiveValue("test.flag_a"), "true");

  // Reset when not overridden returns false.
  EXPECT_FALSE(config_->reset("test.flag_a"));
}

TEST_F(SessionConfigTest, typeValidation) {
  VELOX_ASSERT_THROW(
      config_->set("test.flag_a", "banana"), "Expected boolean value");
  VELOX_ASSERT_THROW(
      config_->set("test.count", "abc"), "Expected integer value");
  VELOX_ASSERT_THROW(
      config_->set("test.ratio", "xyz"), "Expected double value");

  // String accepts anything.
  EXPECT_NO_THROW(config_->set("test.label", "anything"));
}

TEST_F(SessionConfigTest, unknownProperty) {
  VELOX_ASSERT_THROW(
      config_->effectiveValue("test.nonexistent"), "Unknown session property");
  VELOX_ASSERT_THROW(
      config_->effectiveValue("unknown.flag"),
      "Unknown session property prefix");
  VELOX_ASSERT_THROW(
      config_->effectiveValue("noDotPrefix"),
      "Session property must be prefix-qualified");
}

TEST_F(SessionConfigTest, effective) {
  config_->set("test.flag_a", "false");
  auto props = config_->effectiveValues("test");

  EXPECT_EQ(props["flag_a"], "false"); // overridden
  EXPECT_EQ(props["count"], "10"); // default
  EXPECT_EQ(props["ratio"], "0.5"); // default
  // "label" has no default, not set — absent from map.
  EXPECT_EQ(props.find("label"), props.end());
}

TEST_F(SessionConfigTest, allEntries) {
  config_->set("test.count", "42");
  EXPECT_EQ(config_->all().size(), 4);

  auto entry = findEntry("count");
  EXPECT_EQ(entry.currentValue, "42");
  EXPECT_TRUE(entry.isOverridden);
}

TEST_F(SessionConfigTest, normalize) {
  // Boolean normalization: normalizeType lowercases booleans.
  config_->set("test.flag_a", "FALSE");
  EXPECT_EQ(config_->effectiveValue("test.flag_a"), "false");

  // Setting to the default (after normalization) clears the override.
  config_->set("test.flag_a", "TRUE");
  EXPECT_EQ(config_->effectiveValue("test.flag_a"), "true");

  // Verify the override was actually cleared, not just storing "true".
  EXPECT_FALSE(findEntry("flag_a").isOverridden);

  // Provider normalization: label is lowercased by the provider.
  config_->set("test.label", "Hello");
  EXPECT_EQ(config_->effectiveValue("test.label"), "hello");
}

TEST_F(SessionConfigTest, duplicatePrefix) {
  auto registry = std::make_shared<ConfigRegistry>();
  registry->add("test", std::make_shared<TestConfigProvider>());
  VELOX_ASSERT_THROW(
      registry->add("test", std::make_shared<TestConfigProvider>()),
      "Config prefix already registered");
}

TEST(ConfigRegistryTest, configFileDefaults) {
  auto registry = std::make_shared<ConfigRegistry>();
  registry->add(
      "test",
      std::make_shared<TestConfigProvider>(),
      {{"flag_a", "false"}, {"count", "99"}});

  // Verify overridden defaults via resolve().
  auto flagResolution = registry->resolve("test.flag_a");
  EXPECT_EQ(flagResolution.property.defaultValue, "false");

  auto countResolution = registry->resolve("test.count");
  EXPECT_EQ(countResolution.property.defaultValue, "99");

  // Properties not in the config file retain their code defaults.
  auto ratioResolution = registry->resolve("test.ratio");
  EXPECT_EQ(ratioResolution.property.defaultValue, "0.5");

  // SessionConfig sees the config-file defaults.
  SessionConfig config(registry);
  EXPECT_EQ(config.effectiveValue("test.flag_a"), "false");
  EXPECT_EQ(config.effectiveValue("test.count"), "99");
  EXPECT_EQ(config.effectiveValue("test.ratio"), "0.5");

  // Session override still wins over config-file default.
  config.set("test.flag_a", "true");
  EXPECT_EQ(config.effectiveValue("test.flag_a"), "true");

  // Reset returns to config-file default, not code default.
  config.reset("test.flag_a");
  EXPECT_EQ(config.effectiveValue("test.flag_a"), "false");

  // effectiveValues returns config-file defaults for unset properties.
  auto props = config.effectiveValues("test");
  EXPECT_EQ(props["flag_a"], "false");
  EXPECT_EQ(props["count"], "99");
  EXPECT_EQ(props["ratio"], "0.5");

  // all() shows config-file default as the default value.
  auto entries = config.all();
  for (const auto& entry : entries) {
    if (entry.property.name == "flag_a") {
      EXPECT_EQ(entry.property.defaultValue, "false");
      EXPECT_EQ(entry.currentValue, "false");
      EXPECT_FALSE(entry.isOverridden);
    }
  }
}

} // namespace
