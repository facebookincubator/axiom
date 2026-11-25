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

namespace axiom::spark_connect {

/// Converts between Velox vectors and Arrow IPC, the format Spark Connect uses
/// for data in plans and in results.
class ArrowIpc {
 public:
  /// Serializes 'vectors', which share a type, as an Arrow IPC stream. Sets
  /// 'rowCount' to the number of rows.
  static std::shared_ptr<arrow::Buffer> serialize(
      const std::vector<facebook::velox::RowVectorPtr>& vectors,
      uint64_t& rowCount);

  /// Deserializes an Arrow IPC stream into Velox vectors. Meant for data
  /// embedded in plans, not large datasets.
  static std::vector<facebook::velox::RowVectorPtr> deserialize(
      const std::shared_ptr<arrow::Buffer>& buffer,
      facebook::velox::memory::MemoryPool* pool);
};

} // namespace axiom::spark_connect
