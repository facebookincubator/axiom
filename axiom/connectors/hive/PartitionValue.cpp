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

#include "axiom/connectors/hive/PartitionValue.h"

#include <folly/Conv.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector::hive {

bool testPartitionValue(
    const velox::common::Filter& filter,
    const std::optional<std::string>& value,
    const velox::Type& type) {
  if (!value.has_value()) {
    return filter.testNull();
  }

  if (type.isDate()) {
    return filter.testInt64(velox::DATE()->toDays(value.value()));
  }

  switch (type.kind()) {
    case velox::TypeKind::BOOLEAN:
      return filter.testBool(folly::to<bool>(value.value()));
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::BIGINT:
      return filter.testInt64(folly::to<int64_t>(value.value()));
    case velox::TypeKind::VARCHAR:
      return filter.testBytes(
          value.value().c_str(), static_cast<int32_t>(value.value().size()));
    default:
      VELOX_UNREACHABLE(
          "Unsupported partition column type: {}", type.toString());
  }
}

} // namespace facebook::axiom::connector::hive
