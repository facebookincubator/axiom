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

#include <optional>
#include <string>

#include "velox/type/Filter.h"
#include "velox/type/Type.h"

namespace facebook::axiom::connector::hive {

/// Tests an encoded partition value against a Velox filter.
///
/// Hive-style partition metadata stores values as strings. The column type
/// determines how the string is converted before applying the filter.
bool testPartitionValue(
    const velox::common::Filter& filter,
    const std::optional<std::string>& value,
    const velox::Type& type);

} // namespace facebook::axiom::connector::hive
