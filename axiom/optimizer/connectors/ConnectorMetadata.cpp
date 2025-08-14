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

#include "axiom/optimizer/connectors/ConnectorMetadata.h"

namespace facebook::velox::connector {

namespace {
folly::F14FastMap<TableKind, std::string> tableKindNames() {
  static const folly::F14FastMap<TableKind, std::string> kNames = {
      {TableKind::kTable, "kTable"},
      {TableKind::kTempTable, "kTempTable"},
  };

  return kNames;
}

folly::F14FastMap<WriteKind, std::string> writeKindNames() {
  static const folly::F14FastMap<WriteKind, std::string> kNames = {
      {WriteKind::kInsert, "kInsert"},
      {WriteKind::kUpdate, "kUpdate"},
      {WriteKind::kDelete, "kDelete"},
  };

  return kNames;
}

} // namespace

VELOX_DEFINE_ENUM_NAME(TableKind, tableKindNames);

VELOX_DEFINE_ENUM_NAME(WriteKind, writeKindNames);

} // namespace facebook::velox::connector
