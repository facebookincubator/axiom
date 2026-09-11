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

#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::connector {
namespace {

class KeptState : public ConnectorQueryState {};

class OtherState : public ConnectorQueryState {};

ConnectorSession makeSession() {
  return ConnectorSession{
      "q1",
      "user",
      Properties{},
      std::make_shared<velox::NoopRuntimeStatWriter>()};
}

TEST(ConnectorSessionTest, stateIsAttachedOnce) {
  auto session = makeSession();
  session.initializeQueryState(std::make_unique<KeptState>());

  VELOX_ASSERT_THROW(
      session.initializeQueryState(std::make_unique<KeptState>()),
      "Session already has query state");
}

TEST(ConnectorSessionTest, readingStateThatWasNeverKeptFails) {
  auto session = makeSession();

  EXPECT_EQ(session.queryState(), nullptr);
  VELOX_ASSERT_THROW(
      session.queryStateAs<KeptState>(), "Connector kept no state for query");
}

TEST(ConnectorSessionTest, readingStateAsTheWrongTypeFails) {
  auto session = makeSession();
  session.initializeQueryState(std::make_unique<KeptState>());

  EXPECT_EQ(&session.queryStateAs<KeptState>(), session.queryState());
  VELOX_ASSERT_THROW(session.queryStateAs<OtherState>(), "Failed to cast");
}

} // namespace
} // namespace facebook::axiom::connector
