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

#include "axiom/common/Enums.h"

namespace facebook::axiom {

/// Specifies what type of write is intended when initiating or concluding a
/// write operation.
enum class WriteKind {
  /// A write operation to a new table which does not yet exist in the
  /// connector. Covers both creation of an empty table and create as select
  /// operations.
  kCreate = 1,

  /// Rows are added and all columns must be specified for the TableWriter.
  /// Covers insert, Hive partition replacement or any other operation which
  /// adds whole rows.
  kInsert = 2,

  /// Individual rows are deleted. Only row ids as per
  /// ConnectorMetadata::rowIdHandles() are passed to the TableWriter.
  kDelete = 3,

  /// Column values in individual rows are changed. The TableWriter
  /// gets first the row ids as per ConnectorMetadata::rowIdHandles()
  /// and then new values for the columns being changed. The new values
  /// may overlap with row ids if the row id is a set of primary key
  /// columns.
  kUpdate = 4,
};

AXIOM_DECLARE_ENUM_NAME(WriteKind);

} // namespace facebook::axiom

AXIOM_ENUM_FORMATTER(facebook::axiom::WriteKind);
