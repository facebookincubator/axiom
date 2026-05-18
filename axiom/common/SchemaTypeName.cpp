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

#include "axiom/common/SchemaTypeName.h"

#include <fmt/core.h>
#include "velox/common/base/BitUtil.h"

namespace facebook::axiom {

std::string SchemaTypeName::toString() const {
  return fmt::format("{}", *this);
}

} // namespace facebook::axiom

size_t std::hash<facebook::axiom::SchemaTypeName>::operator()(
    const facebook::axiom::SchemaTypeName& name) const {
  auto hash = folly::hasher<std::string>{}(name.type);
  hash = facebook::velox::bits::hashMix(
      hash, folly::hasher<std::string>{}(name.schema));
  return hash;
}
