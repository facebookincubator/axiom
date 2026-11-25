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

#include "axiom/functions/sparksql/SparkFunctions.h"

#include <concepts>

namespace facebook::axiom::functions::sparksql {

using optimizer::FunctionMetadata;
using optimizer::LambdaArg;
using optimizer::LambdaInfo;

namespace {

using DrawnFrom = FunctionMetadata::DrawnFrom;

// Registers the functions that take lambdas.
void registerLambdaFunctions(optimizer::FunctionRegistry& registry) {
  const auto add = [&](std::string_view name,
                       LambdaInfo info,
                       std::optional<int32_t> subfieldArg,
                       float cost) {
    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->lambdas.push_back(std::move(info));
    metadata->subfieldArg = subfieldArg;
    metadata->cost = cost;
    registry.registerFunction(name, std::move(metadata));
  };

  add("transform",
      {.ordinal = 1, .lambdaArg = {LambdaArg::kElement}, .argOrdinal = {0}},
      0,
      20);
  add("transform_values",
      {.ordinal = 1,
       .lambdaArg = {LambdaArg::kKey, LambdaArg::kValue},
       .argOrdinal = {0, 0}},
      0,
      40);
  add("zip_with",
      {.ordinal = 2,
       .lambdaArg = {LambdaArg::kElement, LambdaArg::kElement},
       .argOrdinal = {0, 1}},
      std::nullopt,
      20);
}

// Registers the functions whose result values are some of an argument's
// values.
void registerDrawnFrom(optimizer::FunctionRegistry& registry) {
  const auto makeMetadata = [](std::same_as<DrawnFrom> auto... drawnFrom) {
    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->drawnFrom = [=](auto) {
      return std::vector<DrawnFrom>{drawnFrom...};
    };
    return metadata;
  };
  const auto add = [&](std::string_view name,
                       std::same_as<DrawnFrom> auto... drawnFrom) {
    registry.registerFunction(name, makeMetadata(drawnFrom...));
  };

  for (std::string_view name : {"array_min", "array_max"}) {
    add(name, DrawnFrom::resultValue().drawnFromElement(0));
  }
  for (std::string_view name :
       {"filter", "array_sort", "array_distinct", "shuffle"}) {
    add(name, DrawnFrom::resultElement().drawnFromElement(0));
  }
  add("map_keys", DrawnFrom::resultElement().drawnFromMapKey(0));
  add("map_values", DrawnFrom::resultElement().drawnFromMapValue(0));

  // Aggregates draw from their arguments across the group's rows.
  auto collectListMetadata =
      makeMetadata(DrawnFrom::resultElement().drawnFromValue(0));
  collectListMetadata->partialAggregationReduces = false;
  registry.registerFunction("collect_list", std::move(collectListMetadata));
  add("collect_set", DrawnFrom::resultElement().drawnFromValue(0));
  for (std::string_view name :
       {"min", "max", "min_by", "max_by", "arbitrary"}) {
    add(name, DrawnFrom::resultValue().drawnFromValue(0));
  }
}

} // namespace

// static
void SparkFunctions::registerAll(optimizer::FunctionRegistry& registry) {
  registerLambdaFunctions(registry);

  registry.registerEquality("equalto");
  registry.registerNegation("not");
  registry.registerElementAt("element_at");
  registry.registerArbitrary("arbitrary");
  registry.registerCount("count");
  registry.registerBoolOr("bool_or");
  registry.registerLessThan("lessthan");
  registry.registerLessThanOrEqual("lessthanorequal");
  registry.registerGreaterThan("greaterthan");
  registry.registerGreaterThanOrEqual("greaterthanorequal");
  registry.registerIsNull("isnull");
  registry.registerBetween("between");
  registry.registerLike("like");
  registry.registerRandom("rand");
  registry.registerFail("raise_error");
  registry.registerRowNumber("row_number");
  registry.registerRank("rank");
  registry.registerDenseRank("dense_rank");

  registry.registerStatsAggregates({
      .min = "min",
      .max = "max",
      .countIf = "count_if",
      .approxDistinct = "approx_distinct",
      .sumDataSize = "sum_data_size_for_stats",
      .vectorCount = "vector_count",
  });

  registry.registerAggregateEmptyResultResolver(
      {"count_if", "approx_distinct"},
      [](std::string_view /*name*/,
         std::span<const velox::Type* const> /*argTypes*/) {
        return velox::Variant::create<velox::TypeKind::BIGINT>(0);
      });

  registerDrawnFrom(registry);

  registry.registerReversibleFunction("equalto");
  registry.registerReversibleFunction("lessthan", "greaterthan");
  registry.registerReversibleFunction("lessthanorequal", "greaterthanorequal");
  registry.registerReversibleFunction("add");
  registry.registerReversibleFunction("multiply");
}

// static
std::shared_ptr<optimizer::FunctionRegistry> SparkFunctions::makeRegistry() {
  auto registry = std::make_shared<optimizer::FunctionRegistry>();
  registerAll(*registry);
  return registry;
}

} // namespace facebook::axiom::functions::sparksql
