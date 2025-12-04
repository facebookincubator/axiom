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

#include "axiom/cli/OutputPrinter.h"

namespace axiom::sql {

/// Prints query results as JSON Lines (one JSON object per row).
class JsonPrinter : public OutputPrinter {
 public:
  JsonPrinter() = default;

  int32_t printResults(
      const std::vector<facebook::velox::RowVectorPtr>& results,
      int32_t maxRows) override;
};

} // namespace axiom::sql
