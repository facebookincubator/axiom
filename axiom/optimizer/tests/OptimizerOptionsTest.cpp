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
#include "axiom/optimizer/OptimizerOptions.h"

#include <gtest/gtest.h>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {

namespace {

// Finds a property's default value by name.
std::string getDefault(
    const std::vector<velox::config::ConfigProperty>& props,
    std::string_view name) {
  for (const auto& prop : props) {
    if (prop.name == name) {
      return prop.defaultValue.value_or("");
    }
  }
  VELOX_UNREACHABLE("Property not found: {}", name);
}

} // namespace

TEST(OptimizerOptionsTest, codeDefaults) {
  OptimizerOptions options;
  auto props = options.properties();

  EXPECT_EQ(getDefault(props, OptimizerOptions::kSampleJoins), "false");
  EXPECT_EQ(getDefault(props, OptimizerOptions::kSyntacticJoinOrder), "false");
  EXPECT_EQ(getDefault(props, OptimizerOptions::kParallelProjectWidth), "1");
  EXPECT_EQ(getDefault(props, OptimizerOptions::kTraceFlags), "0");
  EXPECT_EQ(
      getDefault(props, OptimizerOptions::kUseFilteredTableStats), "true");
  EXPECT_EQ(getDefault(props, OptimizerOptions::kBroadcastSizeLimit), "100MB");
}

TEST(OptimizerOptionsTest, configOverrides) {
  OptimizerOptions options(
      {{std::string(OptimizerOptions::kSyntacticJoinOrder), "true"},
       {std::string(OptimizerOptions::kParallelProjectWidth), "4"}});
  auto props = options.properties();

  EXPECT_EQ(getDefault(props, OptimizerOptions::kSyntacticJoinOrder), "true");
  EXPECT_EQ(getDefault(props, OptimizerOptions::kParallelProjectWidth), "4");
  EXPECT_EQ(getDefault(props, OptimizerOptions::kSampleJoins), "false");
}

TEST(OptimizerOptionsTest, from) {
  folly::F14FastMap<std::string, std::string> props = {
      {std::string(OptimizerOptions::kSampleJoins), "true"},
      {std::string(OptimizerOptions::kParallelProjectWidth), "8"},
      {std::string(OptimizerOptions::kTraceFlags), "5"},
      {std::string(OptimizerOptions::kBroadcastSizeLimit), "5GB"},
  };
  auto options = OptimizerOptions::from(props);

  EXPECT_TRUE(options.sampleJoins);
  EXPECT_EQ(options.parallelProjectWidth, 8);
  EXPECT_EQ(options.traceFlags, 5);
  EXPECT_EQ(options.broadcastSizeLimit, 5LL << 30);
  EXPECT_FALSE(options.syntacticJoinOrder);
  EXPECT_FALSE(options.sampleFilters);
}

TEST(OptimizerOptionsTest, broadcastSizeLimitDefaultsAgree) {
  // The string default and the byte default must denote the same size.
  auto options = OptimizerOptions::from(
      {{std::string(OptimizerOptions::kBroadcastSizeLimit),
        std::string(OptimizerOptions::kBroadcastSizeLimitDefault)}});
  EXPECT_EQ(
      options.broadcastSizeLimit,
      OptimizerOptions::kBroadcastSizeLimitDefaultBytes);
}

TEST(OptimizerOptionsTest, normalizeRejectsInvalidValues) {
  OptimizerOptions options;
  VELOX_ASSERT_THROW(
      options.normalize(OptimizerOptions::kParallelProjectWidth, "0"),
      "parallel_project_width must be >= 1");
  VELOX_ASSERT_THROW(
      options.normalize(OptimizerOptions::kBroadcastSizeLimit, "100"),
      "Invalid capacity string");
  VELOX_ASSERT_THROW(
      options.normalize(OptimizerOptions::kFixedPointMaxIterations, "0"),
      "fixed_point_max_iterations must be >= 1");
}

} // namespace facebook::axiom::optimizer
