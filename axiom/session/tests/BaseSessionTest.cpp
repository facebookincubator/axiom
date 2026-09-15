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

#include "axiom/session/BaseSession.h"

#include <gtest/gtest.h>

#include "axiom/connectors/tests/TestConnectorContext.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom {
namespace {

// Identity is the query's, not the component's.
TEST(BaseSessionTest, identityComesFromTheContext) {
  auto context = connector::makeTestContext("q1");
  BaseSession session{context, connector::makeTestStatWriter(), {}};

  EXPECT_EQ(session.context(), context);
  EXPECT_EQ(session.queryId(), "q1");
  EXPECT_EQ(session.user(), "test");
}

// A component reads its own slice and sees nothing it was not given.
TEST(BaseSessionTest, propertyReadsThisComponentsSlice) {
  BaseSession session{
      connector::makeTestContext("q1"),
      connector::makeTestStatWriter(),
      connector::Properties{{"max_plan_objects", "100"}}};

  EXPECT_EQ(session.property("max_plan_objects"), "100");
  EXPECT_EQ(session.property("absent"), std::nullopt);
}

TEST(BaseSessionTest, aContextIsRequired) {
  VELOX_ASSERT_THROW(
      BaseSession(nullptr, connector::makeTestStatWriter(), {}),
      "requires a context");
}

TEST(BaseSessionTest, aWriterIsRequired) {
  VELOX_ASSERT_THROW(
      BaseSession(connector::makeTestContext("q1"), nullptr, {}),
      "requires a writer");
}

} // namespace
} // namespace facebook::axiom
