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

#include <gtest/gtest.h>

#include "velox/common/base/ConcurrentRuntimeStatWriter.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::connector {
namespace {

class QueryState : public ConnectorQueryState {};
class OtherQueryState : public ConnectorQueryState {};

// Query state distinguishes initialization from whether the connector keeps
// state, and initialization runs only once.
TEST(ConnectorSessionTest, queryStateInitialization) {
  for (bool withState : {false, true}) {
    SCOPED_TRACE(withState);
    auto writer = std::make_shared<velox::ConcurrentRuntimeStatWriter>();
    ConnectorSession session{"q1", "user", {}, writer};

    VELOX_ASSERT_THROW(
        session.queryStateAs<QueryState>(),
        "Connector query state is not initialized");

    session.setQueryState(withState ? std::make_unique<QueryState>() : nullptr);
    EXPECT_EQ(session.queryStateAs<QueryState>() != nullptr, withState);
    VELOX_ASSERT_THROW(
        session.setQueryState(nullptr),
        "Connector query state is already initialized");
  }
}

// Typed state access rejects a different connector-defined state type.
TEST(ConnectorSessionTest, queryStateType) {
  auto writer = std::make_shared<velox::ConcurrentRuntimeStatWriter>();
  ConnectorSession session{"q1", "user", {}, writer};
  session.setQueryState(std::make_unique<QueryState>());

  VELOX_ASSERT_THROW(
      session.queryStateAs<OtherQueryState>(), "Failed to cast from");
}

} // namespace
} // namespace facebook::axiom::connector
