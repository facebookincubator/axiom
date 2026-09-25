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

#include "axiom/connectors/ConnectorSession.h"

#include <memory>

#include <gtest/gtest.h>

#include "axiom/connectors/tests/TestConnectorContext.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::connector {
namespace {

class PartitionCache : public ConnectorQueryState {
 public:
  explicit PartitionCache(int32_t partitions) : partitions_{partitions} {}

  int32_t partitions() const {
    return partitions_;
  }

  void setPartitions(int32_t partitions) {
    partitions_ = partitions;
  }

 private:
  int32_t partitions_;
};

class OtherState : public ConnectorQueryState {};

ConnectorSessionPtr makeSession() {
  return std::make_shared<ConnectorSession>(
      "q1", "user", Properties{}, makeTestStatWriter());
}

// A connector that keeps nothing for the query leaves the slot empty.
TEST(ConnectorSessionTest, emptySlot) {
  auto session = makeSession();

  EXPECT_EQ(session->queryState(), nullptr);
}

// Attached state reads back as the connector's own type.
TEST(ConnectorSessionTest, typedRead) {
  auto session = makeSession();

  session->initQueryState(std::make_unique<PartitionCache>(7));

  ASSERT_NE(session->queryState(), nullptr);
  auto& state = session->queryStateAs<PartitionCache>();
  EXPECT_EQ(state.partitions(), 7);
  state.setPartitions(9);
  EXPECT_EQ(session->queryStateAs<PartitionCache>().partitions(), 9);
}

// A second attach fails rather than silently replacing state already in use.
TEST(ConnectorSessionTest, attachTwice) {
  auto session = makeSession();
  session->initQueryState(std::make_unique<PartitionCache>(7));

  VELOX_ASSERT_THROW(
      session->initQueryState(std::make_unique<PartitionCache>(9)),
      "already has query state");
}

// A wrong-type read fails rather than returning a null the caller must test.
TEST(ConnectorSessionTest, wrongType) {
  auto session = makeSession();
  session->initQueryState(std::make_unique<PartitionCache>(7));

  VELOX_ASSERT_THROW(
      session->queryState()->asChecked<OtherState>(), "Failed to cast from");
}

// A typed read of an empty slot fails rather than dereferencing a null.
TEST(ConnectorSessionTest, readEmptySlot) {
  auto session = makeSession();

  VELOX_ASSERT_THROW(
      session->queryStateAs<PartitionCache>(),
      "Connector kept no state for the query");
}

} // namespace
} // namespace facebook::axiom::connector
