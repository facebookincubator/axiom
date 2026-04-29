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

#include "axiom/connectors/ducklake/DuckLakeCatalogClient.h"

#include <fstream>

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TempDirectoryPath.h"

namespace facebook::axiom::connector::ducklake {
namespace {

TEST(DuckLakeCatalogClientTest, reportsUnreadableDuckDbCatalog) {
  auto directory =
      facebook::velox::common::testutil::TempDirectoryPath::create();
  const auto catalogPath = directory->getPath() + "/metadata.ducklake";
  std::ofstream output{catalogPath};
  output << "not a duckdb database";
  output.close();

  VELOX_ASSERT_USER_THROW(
      DuckLakeCatalogClient::create(
          DuckLakeCatalogSpec{DuckLakeCatalogBackend::kDuckDb, catalogPath}),
      "Failed to open DuckLake DuckDB catalog");
}

} // namespace
} // namespace facebook::axiom::connector::ducklake
