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

#include "axiom/runner/RunnerSession.h"

#include <gtest/gtest.h>

#include "velox/common/base/ConcurrentRuntimeStatWriter.h"

namespace facebook::axiom::runner {
namespace {

// toConnectorSession() routes a connector to the writer its provider returns,
// carrying the session's identity; the component writer stays separate.
TEST(RunnerSessionTest, connectorSessionUsesProviderWriter) {
  velox::ConcurrentRuntimeStatWriter componentWriter;
  velox::ConcurrentRuntimeStatWriter hiveWriter;
  RunnerSession session{
      "q1",
      "user",
      Properties{},
      connector::ConnectorProperties{},
      componentWriter,
      [&](std::string_view connectorId) -> velox::BaseRuntimeStatWriter& {
        EXPECT_EQ(connectorId, "hive");
        return hiveWriter;
      }};

  EXPECT_EQ(&session.statsWriter(), &componentWriter);

  auto connectorSession = session.toConnectorSession("hive");
  EXPECT_EQ(connectorSession->queryId(), "q1");
  EXPECT_EQ(connectorSession->user(), "user");
  EXPECT_EQ(&connectorSession->statsWriter(), &hiveWriter);
}

} // namespace
} // namespace facebook::axiom::runner
