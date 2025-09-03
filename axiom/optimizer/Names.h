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

#include <string_view>
#include <unordered_set>

namespace facebook::velox::optimizer {

struct Names {
  static const char* kEq;
  static const char* kLt;
  static const char* kLte;
  static const char* kGt;
  static const char* kGte;
  static const char* kPlus;
  static const char* kMultiply;
  static const char* kAnd;
  static const char* kOr;
  static const char* kCast;
  static const char* kTryCast;
  static const char* kTry;
  static const char* kCoalesce;
  static const char* kIf;
  static const char* kSwitch;
  static const char* kIn;
  static const char* kSubscript;

  static std::unordered_set<std::string_view> builtinNames();

  static bool isCanonicalizable(const char* name);
  static const char* reverse(const char* name);
};

} // namespace facebook::velox::optimizer
