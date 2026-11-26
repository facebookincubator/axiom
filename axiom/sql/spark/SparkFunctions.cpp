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

#include "axiom/sql/spark/SparkFunctions.h"

#include <folly/container/F14Map.h>

#include "velox/common/base/Exceptions.h"

namespace axiom::sql::spark {
namespace {

namespace lp = facebook::axiom::logical_plan;

// Returns the Velox name of a Spark operator or function whose Velox name
// differs. Other names pass through: Velox registers them under their Spark
// names.
std::string toFunctionName(std::string_view name) {
  static const folly::F14FastMap<std::string_view, std::string_view>
      kOperators = {
          {"+", "add"},
          {"-", "subtract"},
          {"*", "multiply"},
          {"/", "divide"},
          {"%", "remainder"},
          {"negative", "unaryminus"},
          {"=", "equalto"},
          {"==", "equalto"},
          {"<=>", "equalnullsafe"},
          {">", "greaterthan"},
          {"<", "lessthan"},
          {">=", "greaterthanorequal"},
          {"<=", "lessthanorequal"},
          {"&", "bitwise_and"},
          {"|", "bitwise_or"},
          {"^", "bitwise_xor"},
          {"~", "bitwise_not"},
          {"||", "concat"},
      };
  auto it = kOperators.find(name);
  return std::string(it == kOperators.end() ? name : it->second);
}

} // namespace

lp::ExprApi SparkFunctions::call(
    std::string_view name,
    std::vector<lp::ExprApi> args) {
  // Spark's analyzer passes size() its legacy flag: a NULL has size -1.
  if (name == "size" && args.size() == 1) {
    args.push_back(lp::Lit(true));
  }

  // when(c1, v1, c2, v2, ..., else) with more than one condition is a switch.
  if (name == "if" || name == "when") {
    VELOX_USER_CHECK_GE(
        args.size(),
        2,
        "if/when requires at least 2 arguments (condition, then_value)");
    return lp::Call(args.size() > 3 ? "switch" : "if", args);
  }

  if (name == "!=" || name == "<>") {
    return lp::Call("not", lp::Call("equalto", args));
  }

  return lp::Call(toFunctionName(name), args);
}

} // namespace axiom::sql::spark
