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

#include "axiom/common/Session.h"

#include <gtest/gtest.h>

namespace facebook::axiom {
namespace {

TEST(SessionTest, toConnectorSessionScopesPropertiesByConnectorId) {
  Session session{
      "query",
      "user",
      {
          {"prism.node_selection_strategy", "SOFT_AFFINITY"},
          {"prism.affinity_scheduling_file_section_size", "64MB"},
          {"hive.node_selection_strategy", "NO_PREFERENCE"},
          {"unscoped_property", "ignored"},
      }};

  auto prismSession = session.toConnectorSession("prism");

  EXPECT_EQ(prismSession->queryId(), "query");
  EXPECT_EQ(prismSession->user(), "user");
  const auto& prismProperties = prismSession->properties();
  EXPECT_EQ(prismProperties.size(), 2);
  EXPECT_EQ(prismProperties.at("node_selection_strategy"), "SOFT_AFFINITY");
  EXPECT_EQ(
      prismProperties.at("affinity_scheduling_file_section_size"), "64MB");
  EXPECT_EQ(prismProperties.count("hive.node_selection_strategy"), 0);
  EXPECT_EQ(prismProperties.count("unscoped_property"), 0);
}

} // namespace
} // namespace facebook::axiom
