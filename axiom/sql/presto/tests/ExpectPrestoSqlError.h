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

#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/sql/presto/PrestoSqlError.h"

/// Verifies that a statement throws PrestoSqlError with kSyntax kind
/// and a message containing the expected substring.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(statement, expectedMessage)         \
  EXPECT_THAT(                                                               \
      [&]() { statement; },                                                  \
      testing::Throws<axiom::sql::presto::PrestoSqlError>(testing::AllOf(    \
          testing::Property(                                                 \
              &axiom::sql::presto::PrestoSqlError::kind,                     \
              testing::Eq(axiom::sql::presto::PrestoSqlErrorKind::kSyntax)), \
          testing::Property(                                                 \
              &axiom::sql::presto::PrestoSqlError::what,                     \
              testing::HasSubstr(expectedMessage)))))

/// Verifies that a statement throws PrestoSqlError with kSemantic kind
/// and a message containing the expected substring.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(statement, expectedMessage)         \
  EXPECT_THAT(                                                                 \
      [&]() { statement; },                                                    \
      testing::Throws<axiom::sql::presto::PrestoSqlError>(testing::AllOf(      \
          testing::Property(                                                   \
              &axiom::sql::presto::PrestoSqlError::kind,                       \
              testing::Eq(axiom::sql::presto::PrestoSqlErrorKind::kSemantic)), \
          testing::Property(                                                   \
              &axiom::sql::presto::PrestoSqlError::what,                       \
              testing::HasSubstr(expectedMessage)))))
