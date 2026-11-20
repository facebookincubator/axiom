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

#include "axiom/connectors/ConnectorMetadata.h"

namespace facebook::axiom::connector {

/// Estimates every filter at selectivity 1.0.
class NoopFilterSelectivityEstimator : public FilterSelectivityEstimator {
 public:
  FilterEstimate estimate(
      const std::vector<velox::core::TypedExprPtr>&,
      const folly::F14FastMap<std::string, TypedColumnStatistics>&)
      const override {
    return {};
  }

  FilterEstimate estimate(
      const folly::F14FastMap<std::string, const velox::common::Filter*>&,
      const folly::F14FastMap<std::string, TypedColumnStatistics>&)
      const override {
    return {};
  }
};

} // namespace facebook::axiom::connector
