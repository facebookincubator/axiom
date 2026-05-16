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

#include "axiom/connectors/ConnectorSplitManager.h"

#include <gtest/gtest.h>

namespace facebook::axiom::connector {
namespace {

TEST(ConnectorSplitManagerTest, splitBatchDefaultsSchedulingInfo) {
  SplitBatch batch;
  batch.splits.push_back(
      std::make_shared<velox::connector::ConnectorSplit>("test"));

  const auto& info = batch.schedulingInfoAt(0);
  EXPECT_EQ(
      info.nodeSelectionStrategy(), SplitNodeSelectionStrategy::kNoPreference);
  EXPECT_FALSE(info.affinityKey().has_value());
}

TEST(ConnectorSplitManagerTest, splitBatchStoresSchedulingInfo) {
  SplitBatch batch;
  batch.addSplit(
      std::make_shared<velox::connector::ConnectorSplit>("test"),
      SplitSchedulingInfo::softAffinity("file-section"));

  ASSERT_EQ(batch.splits.size(), 1);
  const auto& info = batch.schedulingInfoAt(0);
  EXPECT_EQ(
      info.nodeSelectionStrategy(), SplitNodeSelectionStrategy::kSoftAffinity);
  ASSERT_TRUE(info.affinityKey().has_value());
  EXPECT_EQ(info.affinityKey().value(), "file-section");
}

} // namespace
} // namespace facebook::axiom::connector
