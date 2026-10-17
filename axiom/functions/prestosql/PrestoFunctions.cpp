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

#include "axiom/functions/prestosql/PrestoFunctions.h"

#include <fmt/format.h>

#include "velox/expression/ExprConstants.h"

namespace facebook::axiom::functions::prestosql {

using optimizer::FunctionMetadata;
using optimizer::LambdaArg;
using optimizer::LambdaInfo;
namespace lp = facebook::axiom::logical_plan;

// static
void PrestoFunctions::registerAll(
    optimizer::FunctionRegistry& registry,
    std::string_view prefix) {
  auto fullName = [&](std::string_view name) {
    return prefix.empty() ? std::string(name)
                          : fmt::format("{}{}", prefix, name);
  };

  {
    LambdaInfo info{
        .ordinal = 1,
        .lambdaArg = {LambdaArg::kKey, LambdaArg::kValue},
        .argOrdinal = {0, 0},
    };

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->lambdas.push_back(std::move(info));
    metadata->subfieldArg = 0;
    metadata->cost = 40;
    registry.registerFunction(
        fullName("transform_values"), std::move(metadata));
  }

  {
    LambdaInfo info{
        .ordinal = 1,
        .lambdaArg = {LambdaArg::kElement},
        .argOrdinal = {0},
    };

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->lambdas.push_back(std::move(info));
    metadata->subfieldArg = 0;
    metadata->cost = 20;
    registry.registerFunction(fullName("transform"), std::move(metadata));
  }

  {
    LambdaInfo info{
        .ordinal = 2,
        .lambdaArg = {LambdaArg::kElement, LambdaArg::kElement},
        .argOrdinal = {0, 1},
    };

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->lambdas.push_back(std::move(info));
    metadata->cost = 20;
    registry.registerFunction(fullName("zip"), std::move(metadata));
  }

  registry.registerEquality(fullName("eq"));
  registry.registerNegation(fullName("not"));
  registry.registerElementAt(fullName("element_at"));
  registry.registerSubscript(fullName("subscript"));
  registry.registerCardinality(fullName("cardinality"));
  registry.registerArbitrary(fullName("arbitrary"));
  registry.registerCount(fullName("count"));
  registry.registerBoolOr(fullName("bool_or"));
  registry.registerLessThan(fullName("lt"));
  registry.registerLessThanOrEqual(fullName("lte"));
  registry.registerGreaterThan(fullName("gt"));
  registry.registerGreaterThanOrEqual(fullName("gte"));
  registry.registerIsNull(fullName("is_null"));
  registry.registerDistinctFrom(fullName("distinct_from"));
  registry.registerBetween(fullName("between"));
  registry.registerLike(fullName("like"));
  registry.registerRandom(fullName("rand"));
  registry.registerRowNumber(fullName("row_number"));
  registry.registerRank(fullName("rank"));
  registry.registerDenseRank(fullName("dense_rank"));

  registry.registerStatsAggregates({
      .min = fullName("min"),
      .max = fullName("max"),
      .countIf = fullName("count_if"),
      .approxDistinct = fullName("approx_distinct"),
      .sumDataSize = fullName("sum_data_size_for_stats"),
  });

  registry.registerAggregateEmptyResultResolver(
      {fullName("count_if"), fullName("approx_distinct")},
      [](std::string_view /*name*/,
         std::span<const velox::Type* const> /*argTypes*/) {
        return velox::Variant::create<velox::TypeKind::BIGINT>(0);
      });

  registry.registerReversibleFunction(fullName("eq"));
  registry.registerReversibleFunction(fullName("lt"), fullName("gt"));
  registry.registerReversibleFunction(fullName("lte"), fullName("gte"));
  registry.registerReversibleFunction(fullName("plus"));
  registry.registerReversibleFunction(fullName("multiply"));

  // Presto names special forms without a prefix.
  registry.registerSpecialForm(lp::SpecialForm::kAnd, velox::expression::kAnd);
  registry.registerSpecialForm(lp::SpecialForm::kOr, velox::expression::kOr);
  registry.registerSpecialForm(
      lp::SpecialForm::kCast, velox::expression::kCast);
  registry.registerSpecialForm(
      lp::SpecialForm::kTryCast, velox::expression::kTryCast);
  registry.registerSpecialForm(lp::SpecialForm::kTry, velox::expression::kTry);
  registry.registerSpecialForm(lp::SpecialForm::kIf, velox::expression::kIf);
  registry.registerSpecialForm(
      lp::SpecialForm::kCoalesce, velox::expression::kCoalesce);
  registry.registerSpecialForm(
      lp::SpecialForm::kSwitch, velox::expression::kSwitch);
  registry.registerSpecialForm(lp::SpecialForm::kIn, "in");
  registry.registerSpecialForm(
      lp::SpecialForm::kNullIf, velox::expression::kNullIf);
  registry.registerSpecialForm(
      lp::SpecialForm::kRowConstructor, velox::expression::kRowConstructor);
}

// static
std::shared_ptr<optimizer::FunctionRegistry> PrestoFunctions::makeRegistry() {
  auto registry = std::make_shared<optimizer::FunctionRegistry>();
  registerAll(*registry);
  return registry;
}

} // namespace facebook::axiom::functions::prestosql
