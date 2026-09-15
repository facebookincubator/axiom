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

// A connector records into the writer its provider returns; the component's
// own writer is separate.
TEST(RunnerSessionTest, connectorSessionUsesProviderWriter) {
  velox::ConcurrentRuntimeStatWriter componentWriter;
  velox::ConcurrentRuntimeStatWriter connectorWriter;

  auto context = std::make_shared<connector::ConnectorContext>(
      "q1",
      "user",
      connector::ConnectorProperties{},
      [&](std::string_view connectorId)
          -> std::shared_ptr<velox::BaseRuntimeStatWriter> {
        EXPECT_EQ(connectorId, "a");
        return std::shared_ptr<velox::BaseRuntimeStatWriter>(
            &connectorWriter, [](auto*) {});
      });

  RunnerSession session{
      context,
      std::shared_ptr<velox::BaseRuntimeStatWriter>(
          &componentWriter, [](auto*) {}),
      Properties{}};

  EXPECT_EQ(&session.statsWriter(), &componentWriter);

  auto connectorSession = session.context()->sessionFor("a");
  EXPECT_EQ(connectorSession->queryId(), "q1");
  EXPECT_EQ(connectorSession->user(), "user");
  EXPECT_EQ(&connectorSession->statsWriter(), &connectorWriter);
}

} // namespace
} // namespace facebook::axiom::runner
