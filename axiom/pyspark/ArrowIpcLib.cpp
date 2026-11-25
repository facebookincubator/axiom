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

#include "axiom/pyspark/ArrowIpcLib.h"
#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;

namespace axiom::collagen {

namespace {

void checkOk(const arrow::Status& status) {
  VELOX_CHECK(status.ok(), "{}", status.ToString());
}

template <typename T>
T valueOrThrow(arrow::Result<T> result) {
  checkOk(result.status());
  return std::move(result).ValueUnsafe();
}

} // namespace

std::shared_ptr<arrow::Buffer> toArrowIpc(
    const std::vector<velox::RowVectorPtr>& vectors,
    uint64_t& rowCount) {
  rowCount = 0; // Initialize rowCount to zero

  if (vectors.empty()) {
    // Return empty buffer for empty input
    auto sink = valueOrThrow(arrow::io::BufferOutputStream::Create());
    return valueOrThrow(sink->Finish());
  }

  // PySpark does not support Arrow's run-end encoding. Dictionary flattening
  // avoids null dictionary pointers in complex outputs.
  const ArrowOptions options{
      .flattenDictionary = true,
      .flattenConstant = true,
  };

  // Get schema from the first vector (assuming all vectors have the same
  // schema)
  ArrowSchema arrowSchema;
  velox::exportToArrow(vectors[0], arrowSchema, options);
  auto schema = valueOrThrow(arrow::ImportSchema(&arrowSchema));

  // Create output stream and writer
  auto sink = valueOrThrow(arrow::io::BufferOutputStream::Create());
  auto arrowWriter =
      valueOrThrow(arrow::ipc::MakeStreamWriter(sink.get(), schema));

  // Convert each Velox RowVector to Arrow RecordBatch and write to stream
  for (const auto& vector : vectors) {
    ArrowArray arrowArray;
    velox::exportToArrow(vector, arrowArray, vector->pool(), options);
    auto arrowBatch =
        valueOrThrow(arrow::ImportRecordBatch(&arrowArray, schema));
    checkOk(arrowWriter->WriteRecordBatch(*arrowBatch));

    rowCount += vector->size();
  }

  checkOk(arrowWriter->Close());
  return valueOrThrow(sink->Finish());
}

std::vector<velox::RowVectorPtr> fromArrowIpc(
    const std::shared_ptr<arrow::Buffer>& buffer,
    velox::memory::MemoryPool* pool) {
  if (buffer->size() == 0) {
    return {};
  }

  auto reader = valueOrThrow(
      arrow::ipc::RecordBatchStreamReader::Open(
          std::make_shared<arrow::io::BufferReader>(buffer)));

  ArrowSchema arrowSchema;
  checkOk(arrow::ExportSchema(*reader->schema(), &arrowSchema));

  std::vector<velox::RowVectorPtr> outputVectors;

  // Iterate over each Arrow record batch, convert to Velox vector.
  while (true) {
    auto arrowBatch = valueOrThrow(reader->Next());
    if (!arrowBatch) {
      break;
    }

    ArrowArray arrowArray;
    checkOk(arrow::ExportRecordBatch(*arrowBatch, &arrowArray));

    auto vector = velox::importFromArrowAsViewer(arrowSchema, arrowArray, pool);
    outputVectors.emplace_back(
        std::dynamic_pointer_cast<velox::RowVector>(vector));
  }
  return outputVectors;
}

} // namespace axiom::collagen
