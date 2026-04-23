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

#include "arrow/api.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace axiom::collagen {

/// Converts a vector of Velox RowVector into an Arrow Array and serializes it
/// using Arrow IPC.
///
/// Returns the Arrow Buffer containing the serialized IPC payload. Throws
/// CollagenException on errors.
std::shared_ptr<arrow::Buffer> toArrowIpc(
    const std::vector<facebook::velox::RowVectorPtr>& vectors,
    uint64_t& rowCount);

/// Convert an Arrow IPC payload into Velox Vectors.
///
/// This is meant to be used for literals embedded into query plans, not large
/// datasets.
std::vector<facebook::velox::RowVectorPtr> fromArrowIpc(
    const std::shared_ptr<arrow::Buffer>& buffer,
    facebook::velox::memory::MemoryPool* pool);

} // namespace axiom::collagen
