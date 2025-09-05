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
#include "axiom/optimizer/QueryGraphContext.h"

namespace facebook::velox::optimizer {

/// A bit set that qualifies an Expr. Represents which functions/kinds
/// of functions are found inside the children of an Expr.
class FunctionSet {
 public:
  /// Indicates an aggregate function in the set.
  static constexpr uint64_t kAggregate = 1;

  /// Indicates a non-determinstic function in the set.
  static constexpr uint64_t kNonDeterministic = 1UL << 1;

  FunctionSet() : set_(0) {}

  explicit FunctionSet(uint64_t set) : set_(set) {}

  /// True if 'item' is in 'this'.
  bool contains(int64_t item) const {
    return 0 != (set_ & item);
  }

  /// Unions 'this' and 'other' and returns the result.
  FunctionSet operator|(const FunctionSet& other) const {
    return FunctionSet(set_ | other.set_);
  }

  /// Unions 'this' and 'other' and returns the result.
  FunctionSet operator|(uint64_t other) const {
    return FunctionSet(set_ | other);
  }

 private:
  uint64_t set_;
};

/// Describes where the args given to a lambda come from.
enum class LambdaArg : int8_t { kKey, kValue, kElement };

// Lambda function process arrays or maps using lambda expressions.
// Example:
//
//    filter(array, x -> x > 0)
//
//    LambdaInfo{.ordinal = 1, .lambdaArg = {kElement}, .argOrdinal = {0}}
//
// , where .ordinal = 1 says that lambda expression is the second argument of
// the function; .lambdaArg = {kElement} together with .argOrdinal = {0} say
// that the lambda expression takes one argument, which is the element of the
// array, which is to be found in the first argument of the function.
//
// clang-format off
//    transform_values(map, (k, v) -> v + 1)
//
//    LambdaInfo{.ordinal = 1, .lambdaArg = {kKey, kValue}, .argOrdinal = {0, 0}}
// clang-format on
//
// , where ordinal = 1 says that lambda expression is the second argument of the
// function; .lambdaArg = {kKey, kValue} together with .argOrdinal = {0, 0} say
// that lambda expression takes two arguments, which are the key and the value
// of the same map, which is to be found in the first argument of the function.
//
// clang-format off
//    zip(a, b, (x, y) -> x + y)
//
//    LambdaInfo{.ordinal = 2, .lambdaArg = {kElement, kElement}, .argOrdinal = {0, 1}}
// clang-format on
//
// , where ordinal = 2 says that lambda expression is the third argument of the
// function; .lambdaArg = {kElement, kElement} together with .argOrdinal = {0,
// 1} say that lambda expression takes two arguments: first is an element of the
// array in the first argument of the function; second is an element of the
// array in the second argument of the function.
//
struct LambdaInfo {
  /// The ordinal of the lambda in the function's args.
  int32_t ordinal;

  /// Getter applied to the collection given in corresponding 'argOrdinal' to
  /// get each argument of the lambda.
  std::vector<LambdaArg> lambdaArg;

  /// The ordinal of the array or map that provides the lambda argument in the
  /// function's args. 1:1 with lambdaArg.
  std::vector<int32_t> argOrdinal;
};

class Call;

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

  /// Bits of FunctionSet for the function.
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
  /// @return metadata for function 'name' or nullptr if 'name' is not
  /// registered.
  FunctionMetadataCP metadata(std::string_view name) const;

  const std::string& equality() const {
    return equality_;
  }

  const std::optional<std::string>& elementAt() const {
    return elementAt_;
  }

  const std::optional<std::string>& subscript() const {
    return subscript_;
  }

  const std::optional<std::string>& cardinality() const {
    return cardinality_;
  }

  std::string specialForm(logical_plan::SpecialForm specialForm) {
    auto it = specialForms_.find(specialForm);
    VELOX_USER_CHECK(it != specialForms_.end());
    return it->second;
  }

  /// @return a mapping of reversible functions.
  const folly::F14FastMap<std::string, std::string>& reversibleFunctions()
      const {
    return reversibleFunctions_;
  }

  /// Registers function 'name' with specified 'metadata' if 'name' is not
  /// already registered.
  /// @return true if registered 'name' successfully, false otherwise.
  bool registerFunction(
      std::string_view name,
      std::unique_ptr<FunctionMetadata> metadata);

  /// Registers function 'name' that has semantics of Presto's 'eq'.
  void registerEquality(std::string_view name);

  /// Registers function 'name' that has semantics of Presto's 'element_at'.
  /// When applied to an array, returns element of the array at the specified
  /// zero-based index. When applies to a map, returns value correspondig to the
  /// specified key. Returns null if array index is out of bounds or map key is
  /// not found.
  /// @return true if successfully registered, false if a different 'element_at'
  /// function is already registered.
  bool registerElementAt(std::string_view name);

  /// Registers function 'name' that has semantics of Presto's 'subfield'.
  /// Similar to 'element_at', but throws if array index is out of bounds or map
  /// key is missing.
  /// @return true if successfully registered, false if a different 'subfield'
  /// function is already registered.
  bool registerSubscript(std::string_view name);

  /// Registers function 'name' that has semantics of Presto's 'cardinality',
  /// i.e. returns the number of entries in an array or map.
  /// @return true if successfully registered, false if a different
  /// 'cardinality' function is already registered.
  bool registerCardinality(std::string_view name);

  bool registerSpecialForm(
      logical_plan::SpecialForm specialForm,
      std::string_view name);

  /// Registers a function that takes 2 arguments whose order can be changed
  /// without affecting the results, i.e. f(x, y) == f(y, x). For example,
  /// plus, multiple, eq.
  /// @return true if registered 'name' successfully, false if function is
  /// already registered.
  bool registerReversibleFunction(std::string_view name);

  /// Registers a pair of functions that take 2 arguments such that name(x, y)
  /// == reverseName(y, x) for any x and y. For example, {lt, gt}, {lte, gte}.
  /// @return true if registered 'name' successfully, false if 'name' is
  /// already registered.s
  bool registerReversibleFunction(
      std::string_view name,
      std::string_view reverseName);

  static FunctionRegistry* instance();

  /// Registers Presto functions transform, transform_values, zip, and
  /// row_constructor along with metadata. Registers reversible Presto functions
  /// eq, lt, gt, lte, gte, plus, multiply, and, or.
  static void registerPrestoFunctions(std::string_view prefix = "");

 private:
  folly::F14FastMap<std::string, std::unique_ptr<FunctionMetadata>> metadata_;
  std::string equality_{"eq"};
  std::optional<std::string> elementAt_;
  std::optional<std::string> subscript_;
  std::optional<std::string> cardinality_;
  folly::F14FastMap<std::string, std::string> reversibleFunctions_;
  folly::F14FastMap<logical_plan::SpecialForm, std::string> specialForms_;
};

/// Shortcut for FunctionRegistry::instance()->metadata(name).
FunctionMetadataCP functionMetadata(std::string_view name);

std::string specialForm(logical_plan::SpecialForm specialForm);

} // namespace facebook::velox::optimizer
