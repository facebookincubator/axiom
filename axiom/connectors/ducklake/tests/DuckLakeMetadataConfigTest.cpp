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

#include "axiom/connectors/ducklake/DuckLakeMetadataConfig.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::connector::ducklake {
namespace {

using ::testing::Eq;

TEST(DuckLakeMetadataConfigTest, parsesDuckDbCatalogUrls) {
  auto spec = DuckLakeCatalogSpec::parse("ducklake:metadata.ducklake");

  EXPECT_THAT(spec.backend, Eq(DuckLakeCatalogBackend::kDuckDb));
  EXPECT_THAT(spec.metadataPath, Eq("metadata.ducklake"));

  spec = DuckLakeCatalogSpec::parse(" ducklake:duckdb:/tmp/metadata.ducklake ");

  EXPECT_THAT(spec.backend, Eq(DuckLakeCatalogBackend::kDuckDb));
  EXPECT_THAT(spec.metadataPath, Eq("/tmp/metadata.ducklake"));
}

TEST(DuckLakeMetadataConfigTest, parsesOtherCatalogBackends) {
  auto spec =
      DuckLakeCatalogSpec::parse("ducklake:sqlite:/tmp/metadata.sqlite");

  EXPECT_THAT(spec.backend, Eq(DuckLakeCatalogBackend::kSqlite));
  EXPECT_THAT(spec.metadataPath, Eq("/tmp/metadata.sqlite"));

  spec = DuckLakeCatalogSpec::parse(
      "ducklake:postgres:host=localhost dbname=lake");

  EXPECT_THAT(spec.backend, Eq(DuckLakeCatalogBackend::kPostgres));
  EXPECT_THAT(spec.metadataPath, Eq("host=localhost dbname=lake"));
}

TEST(DuckLakeMetadataConfigTest, rejectsInvalidUrls) {
  VELOX_ASSERT_THROW(
      DuckLakeCatalogSpec::parse(""), "DuckLake catalog URL is empty");
  VELOX_ASSERT_THROW(
      DuckLakeCatalogSpec::parse("metadata.ducklake"),
      "DuckLake catalog URL must start with 'ducklake:'");
  VELOX_ASSERT_THROW(
      DuckLakeCatalogSpec::parse("ducklake:"),
      "DuckLake catalog URL is missing metadata location");
}

} // namespace
} // namespace facebook::axiom::connector::ducklake
