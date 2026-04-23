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
#include "axiom/pyspark/Exception.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;

namespace axiom::collagen {

std::shared_ptr<arrow::Buffer> toArrowIpc(
    const std::vector<velox::RowVectorPtr>& vectors,
    uint64_t& rowCount) {
  rowCount = 0; // Initialize rowCount to zero

  if (vectors.empty()) {
    // Return empty buffer for empty input
    COLLAGEN_ARROW_ASSIGN_OR_THROW(
        auto sink, arrow::io::BufferOutputStream::Create())
    COLLAGEN_ARROW_ASSIGN_OR_THROW(auto outputBuffer, sink->Finish());
    return outputBuffer;
  }

  // Flatten dictionary encoding to avoid null dictionary pointers in
  // ArrowArray when exporting complex types like MAP<BIGINT, ARRAY<BIGINT>>
  // from map_zip_with results.
  const ArrowOptions options{.flattenDictionary = true};

  // Get schema from the first vector (assuming all vectors have the same
  // schema)
  ArrowSchema arrowSchema;
  velox::exportToArrow(vectors[0], arrowSchema, options);
  COLLAGEN_ARROW_ASSIGN_OR_THROW(
      auto schema, arrow::ImportSchema(&arrowSchema));

  // Create output stream and writer
  COLLAGEN_ARROW_ASSIGN_OR_THROW(
      auto sink, arrow::io::BufferOutputStream::Create())

  std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
  COLLAGEN_ARROW_CHECK(
      arrow::ipc::MakeStreamWriter(sink.get(), schema).Value(&arrowWriter));

  // Convert each Velox RowVector to Arrow RecordBatch and write to stream
  for (const auto& vector : vectors) {
    ArrowArray arrowArray;
    velox::exportToArrow(vector, arrowArray, vector->pool(), options);
    COLLAGEN_ARROW_ASSIGN_OR_THROW(
        auto arrowBatch, arrow::ImportRecordBatch(&arrowArray, schema));
    COLLAGEN_ARROW_CHECK(arrowWriter->WriteRecordBatch(*arrowBatch));

    // Update rowCount with the number of rows in the current vector
    LOG(INFO) << "Vector size: " << vector->size();
    rowCount += vector->size();
  }

  COLLAGEN_ARROW_CHECK(arrowWriter->Close());
  COLLAGEN_ARROW_ASSIGN_OR_THROW(auto outputBuffer, sink->Finish());
  return outputBuffer;
}

std::vector<velox::RowVectorPtr> fromArrowIpc(
    const std::shared_ptr<arrow::Buffer>& buffer,
    velox::memory::MemoryPool* pool) {
  if (buffer->size() == 0) {
    return {};
  }

  COLLAGEN_ARROW_ASSIGN_OR_THROW(
      auto reader,
      arrow::ipc::RecordBatchStreamReader::Open(
          std::make_shared<arrow::io::BufferReader>(buffer)));

  ArrowSchema arrowSchema;
  COLLAGEN_ARROW_CHECK(arrow::ExportSchema(*reader->schema(), &arrowSchema));

  std::vector<velox::RowVectorPtr> outputVectors;

  // Iterate over each Arrow record batch, convert to Velox vector.
  while (true) {
    COLLAGEN_ARROW_ASSIGN_OR_THROW(auto arrowBatch, reader->Next());
    if (!arrowBatch) {
      break;
    }

    ArrowArray arrowArray;
    COLLAGEN_ARROW_CHECK(ExportRecordBatch(*arrowBatch, &arrowArray));

    auto vector = velox::importFromArrowAsViewer(arrowSchema, arrowArray, pool);
    outputVectors.emplace_back(
        std::dynamic_pointer_cast<velox::RowVector>(vector));
  }
  return outputVectors;
}

} // namespace axiom::collagen
