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

TEST(DuckLakeMetadataConfigTest, duckdbBackends) {
  auto spec = DuckLakeCatalogSpec::parse("ducklake:metadata.ducklake");

  ASSERT_EQ(spec.backend, DuckLakeCatalogBackend::kDuckDb);
  ASSERT_EQ(spec.metadataPath, "metadata.ducklake");

  spec = DuckLakeCatalogSpec::parse(" ducklake:duckdb:/tmp/metadata.ducklake ");

  ASSERT_EQ(spec.backend, DuckLakeCatalogBackend::kDuckDb);
  ASSERT_EQ(spec.metadataPath, "/tmp/metadata.ducklake");
}

TEST(DuckLakeMetadataConfigTest, otherBackends) {
  auto spec =
      DuckLakeCatalogSpec::parse("ducklake:sqlite:/tmp/metadata.sqlite");

  ASSERT_EQ(spec.backend, DuckLakeCatalogBackend::kSqlite);
  ASSERT_EQ(spec.metadataPath, "/tmp/metadata.sqlite");

  spec = DuckLakeCatalogSpec::parse(
      "ducklake:postgres:host=localhost dbname=lake");

  ASSERT_EQ(spec.backend, DuckLakeCatalogBackend::kPostgres);
  ASSERT_EQ(spec.metadataPath, "host=localhost dbname=lake");
}

TEST(DuckLakeMetadataConfigTest, invalidUrls) {
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
