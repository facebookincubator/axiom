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

#include "axiom/cli/OutputFormat.h"

namespace axiom::sql {

namespace {
const auto& outputFormatNames() {
  static const folly::F14FastMap<OutputFormat, std::string_view> kNames = {
      {OutputFormat::kAligned, "ALIGNED"},
      {OutputFormat::kJson, "JSON"},
  };
  return kNames;
}
} // namespace

AXIOM_DEFINE_ENUM_NAME(OutputFormat, outputFormatNames);

} // namespace axiom::sql
