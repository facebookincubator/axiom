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

#include <gtest/gtest.h>

#include "axiom/connectors/file/FileHandler.h"
#include "axiom/connectors/file/parquet/ParquetFileHandler.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::connector::file {
namespace {

using namespace facebook::velox;

class FileHandlerRegistryTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    registerParquetHandler();
  }
};

TEST_F(FileHandlerRegistryTest, unknownHandlerFails) {
  VELOX_ASSERT_THROW(handler("orc"), "Unsupported file format: orc");
}

TEST_F(FileHandlerRegistryTest, schemasReturnsRegisteredNames) {
  auto names = schemas();
  EXPECT_FALSE(names.empty());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "parquet") != names.end());
}

TEST_F(FileHandlerRegistryTest, hasHandlerForRegistered) {
  EXPECT_TRUE(hasHandler("parquet"));
}

TEST_F(FileHandlerRegistryTest, hasHandlerFalseForUnknown) {
  EXPECT_FALSE(hasHandler("orc"));
  EXPECT_FALSE(hasHandler("default"));
}

TEST_F(FileHandlerRegistryTest, duplicateRegistrationFails) {
  auto& existing = handler("parquet");
  VELOX_ASSERT_THROW(
      registerHandler("parquet", existing),
      "FileHandler already registered for schema: parquet");
}

} // namespace
} // namespace facebook::axiom::connector::file
