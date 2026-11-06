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

#include "axiom/connectors/VectorIndex.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox;

namespace facebook::axiom::connector {
namespace {

using Properties = folly::F14FastMap<std::string, Variant>;

Properties requiredProperties() {
  return {
      {"distance_metric", Variant("cosine")},
      {"index_type", Variant("ivf_flat")},
      {"num_clusters", Variant(int32_t{10})},
      {"dimension", Variant(int32_t{768})},
  };
}

// Returns 'properties' with 'name' set to 'value'.
Properties with(Properties properties, std::string name, Variant value) {
  properties[std::move(name)] = std::move(value);
  return properties;
}

// Calls VectorIndexProperties::extract on a copy of 'properties'.
VectorIndexProperties extract(Properties properties) {
  return VectorIndexProperties::extract(properties);
}

TEST(VectorIndexTest, extract) {
  // The index's own properties are removed; the rest remain as table
  // properties.
  Properties properties{
      {"distance_metric", Variant("cosine")},
      {"index_type", Variant("ivf_flat")},
      {"num_clusters", Variant(int32_t{10})},
      {"dimension", Variant(int32_t{768})},
      {"codec_policy", Variant("shared")},
      {"index_options", Variant::map({{Variant("ivf.nlist"), Variant("4")}})},
      {"partitioned_by", Variant::array({Variant("ds")})},
  };
  const VectorIndexProperties expected{
      .metric = common::vectorindex::VectorIndexMetric::kCosine,
      .indexKind = common::vectorindex::VectorIndexKind::kIvfFlat,
      .numClusters = 10,
      .dimension = 768,
      .codecPolicy = VectorIndexCodecPolicy::kShared,
      .indexOptions = {{"ivf.nlist", "4"}},
  };
  EXPECT_EQ(expected, VectorIndexProperties::extract(properties));
  ASSERT_EQ(1, properties.size());
  EXPECT_TRUE(properties.contains("partitioned_by"));

  // The codec policy defaults to per partition.
  const auto defaults = extract(requiredProperties());
  EXPECT_EQ(VectorIndexCodecPolicy::kPerPartition, defaults.codecPolicy);

  // toProperties() gives back exactly what extract() reads, leaving out
  // 'index_options' when there are none.
  for (const auto& original : {expected, defaults}) {
    auto roundTrip = original.toProperties();
    EXPECT_EQ(original, VectorIndexProperties::extract(roundTrip));
    EXPECT_TRUE(roundTrip.empty());
  }
  EXPECT_FALSE(defaults.toProperties().contains("index_options"));
}

TEST(VectorIndexTest, extractErrors) {
  const auto required = requiredProperties();
  for (const auto* name :
       {"distance_metric", "index_type", "num_clusters", "dimension"}) {
    SCOPED_TRACE(name);
    auto properties = required;
    properties.erase(name);
    VELOX_ASSERT_THROW(
        extract(properties), "Missing required vector index property");
  }

  VELOX_ASSERT_THROW(
      extract(with(required, "distance_metric", Variant("l1"))),
      "Unsupported distance metric: l1");
  VELOX_ASSERT_THROW(
      extract(with(required, "index_type", Variant("lsh"))),
      "Unsupported index type: lsh");
  VELOX_ASSERT_THROW(
      extract(with(required, "num_clusters", Variant(int64_t{0}))),
      "Vector index property must be positive: num_clusters");
  VELOX_ASSERT_THROW(
      extract(with(required, "dimension", Variant(int64_t{0}))),
      "Vector index property must be positive: dimension");
  VELOX_ASSERT_THROW(
      extract(with(required, "num_clusters", Variant("10"))),
      "Vector index property must be an integer: num_clusters");
  VELOX_ASSERT_THROW(
      extract(with(required, "distance_metric", Variant(int32_t{1}))),
      "Vector index property must be a string: distance_metric");
  VELOX_ASSERT_THROW(
      extract(
          with(required, "distance_metric", Variant::null(TypeKind::VARCHAR))),
      "Vector index property must not be null: distance_metric");
  VELOX_ASSERT_THROW(
      extract(with(required, "codec_policy", Variant("daily"))),
      "Unsupported codec policy: daily");
  VELOX_ASSERT_THROW(
      extract(with(required, "index_options", Variant("ivf.nlist=4"))),
      "Vector index property must be a map of strings: index_options");
  VELOX_ASSERT_THROW(
      extract(with(
          required,
          "index_options",
          Variant::map({{Variant("ivf.nlist"), Variant(int32_t{4})}}))),
      "Vector index property must be a string: index_options");
  VELOX_ASSERT_THROW(
      extract(with(
          required,
          "index_options",
          Variant::map({{Variant("hnsw.M"), Variant("8")}}))),
      "hnsw.M");
}

} // namespace
} // namespace facebook::axiom::connector
