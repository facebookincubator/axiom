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

#include "axiom/logical_plan/Expr.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::velox::optimizer {

/// Describes functions accepting lambdas and functions with special treatment
/// of subfields.
struct FunctionMetadata {
  bool processSubfields() const {
    return subfieldArg.has_value() || !fieldIndexForArg.empty() ||
        isArrayConstructor || isMapConstructor || valuePathToArgPath;
  }

  const LambdaInfo* lambdaInfo(int32_t index) const {
    for (const auto& lambda : lambdas) {
      if (lambda.ordinal == index) {
        return &lambda;
      }
    }
    return nullptr;
  }

  std::vector<LambdaInfo> lambdas;

  /// If accessing a subfield on the result means that the same subfield is
  /// required in an argument, this is the ordinal of the argument. This is 1
  /// for transform_values, which means that transform_values(map, <lambda>)[1]
  /// implies that key 1 is accessed in 'map'.
  std::optional<int32_t> subfieldArg;

  /// If true, then access of subscript 'i' in result means that argument 'i' is
  /// accessed.
  bool isArrayConstructor{false};

  /// If key 'k' in result is accessed, then the argument that corresponds to
  /// this key is accessed.
  bool isMapConstructor{false};

  /// If ordinal fieldIndexForArg_[i] is accessed, then argument argOrdinal_[i]
  /// is accessed.
  std::vector<int32_t> fieldIndexForArg;

  /// Ordinal of argument that produces the result subfield in the corresponding
  /// element of 'fieldIndexForArg_'.
  std::vector<int32_t> argOrdinal;

  using ValuePathToArgPath =
      std::function<std::pair<std::vector<Step>, int32_t>(
          const std::vector<Step>&,
          const logical_plan::CallExpr& call)>;

  /// Translates a path over the function result to a path over an argument.
  ValuePathToArgPath valuePathToArgPath;

  /// bits of FunctionSet for the function.
  FunctionSet functionSet;

  /// Static fixed cost for processing one row. use 'costFunc' for non-constant
  /// cost.
  float cost{1};

  /// Function for evaluating the per-row cost when the cost depends on
  /// arguments and their stats.
  std::function<float(const Call*)> costFunc;

  /// Translates a set of paths into path, expression pairs if the complex type
  /// returning function is decomposable into per-path subexpressions. Suppose
  /// the function applies array sort to all arrays in a map. suppose it is used
  /// in [k1][0] and [k2][1]. This could return [k1] = array_sort(arg[k1]) and
  /// k2 = array_sort(arg[k2]. 'arg'  comes from 'call'.
  std::function<std::unordered_map<PathCP, logical_plan::ExprPtr>(
      const logical_plan::CallExpr* call,
      std::vector<PathCP>& paths)>
      explode;
};

using FunctionMetadataCP = const FunctionMetadata*;

class FunctionRegistry {
 public:
  FunctionMetadataCP metadata(std::string_view name) const;

  void registerFunction(
      std::string_view function,
      std::unique_ptr<FunctionMetadata> metadata);

  static FunctionRegistry* instance();

 private:
  folly::F14FastMap<std::string, std::unique_ptr<FunctionMetadata>> metadata_;
};

FunctionMetadataCP functionMetadata(std::string_view name);

} // namespace facebook::velox::optimizer
