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

#include "axiom/connectors/file/parquet/ParquetFileHandler.h"

#include <folly/synchronization/CallOnce.h>

#include "axiom/connectors/file/core/FileDataSources.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/parquet/reader/Metadata.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::axiom::connector::file {

using namespace facebook::velox;

namespace {

constexpr const char* kColumnChunks = "column_chunks";
constexpr const char* kRowGroups = "row_groups";

const RowTypePtr& rowGroupsSchema() {
  static auto kSchema = ROW({
      {"row_group_id", BIGINT()},
      {"num_rows", BIGINT()},
      {"total_byte_size", BIGINT()},
      {"total_compressed_size", BIGINT()},
  });
  return kSchema;
}

const RowTypePtr& columnChunksSchema() {
  static auto kSchema = ROW({
      {"row_group_id", BIGINT()},
      {"column_id", BIGINT()},
      {"name", VARCHAR()},
      {"compression", VARCHAR()},
      {"encodings", VARCHAR()},
      {"compressed_size", BIGINT()},
      {"uncompressed_size", BIGINT()},
      {"num_values", BIGINT()},
      {"min", VARCHAR()},
      {"max", VARCHAR()},
      {"null_count", BIGINT()},
  });
  return kSchema;
}

std::unique_ptr<parquet::ParquetReader> openFile(
    const std::string& filePath,
    memory::MemoryPool* pool) {
  dwio::common::ReaderOptions readerOptions(pool);
  auto fileSystem = filesystems::getFileSystem(filePath, /*config=*/nullptr);
  auto readFile = fileSystem->openFileForRead(filePath);
  auto input =
      std::make_unique<dwio::common::BufferedInput>(std::move(readFile), *pool);
  return std::make_unique<parquet::ParquetReader>(
      std::move(input), readerOptions);
}

class ParquetDataSource : public StreamingDataSource {
 public:
  ParquetDataSource(
      const RowTypePtr& outputType,
      const RowTypePtr& /*fullSchema*/,
      const velox::connector::ColumnHandleMap& columnHandles,
      memory::MemoryPool* pool)
      : StreamingDataSource(outputType, pool) {
    validateOutputColumns(outputType_, columnHandles);
    projectedColumns_ = outputType_->names();
  }

 protected:
  void onSplit(const std::string& filePath) override {
    reader_ = openFile(filePath, pool_);
    dwio::common::RowReaderOptions rowReaderOptions;
    rowReaderOptions.select(
        std::make_shared<dwio::common::ColumnSelector>(
            reader_->rowType(), projectedColumns_));
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
    scanSpec->addAllChildFields(*outputType_);
    rowReaderOptions.setScanSpec(std::move(scanSpec));
    rowReader_ = reader_->createRowReader(rowReaderOptions);
    output_ = BaseVector::create(outputType_, 1, pool_);
  }

  std::optional<RowVectorPtr> readBatch(uint64_t size) override {
    if (!rowReader_) {
      return nullptr;
    }
    auto rowsRead = rowReader_->next(size, output_);
    if (rowsRead == 0) {
      return nullptr;
    }
    auto rowVector = std::dynamic_pointer_cast<RowVector>(output_);
    for (auto i = 0; i < rowVector->childrenSize(); ++i) {
      BaseVector::flattenVector(rowVector->childAt(i));
    }
    return rowVector;
  }

 private:
  std::vector<std::string> projectedColumns_;
  std::unique_ptr<parquet::ParquetReader> reader_;
  std::unique_ptr<dwio::common::RowReader> rowReader_;
  VectorPtr output_;
};

class RowGroupsDataSource : public MetadataDataSource {
 public:
  RowGroupsDataSource(
      const RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      memory::MemoryPool* pool)
      : MetadataDataSource(outputType, rowGroupsSchema(), columnHandles, pool) {
  }

 protected:
  RowVectorPtr build() override {
    auto reader = openFile(split_->filePath, pool_);
    auto fileMetadata = reader->fileMetaData();
    auto numRowGroups = static_cast<vector_size_t>(fileMetadata.numRowGroups());

    auto rowGroupIds =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), numRowGroups, pool_);
    auto numRows =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), numRowGroups, pool_);
    auto totalByteSize =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), numRowGroups, pool_);
    auto totalCompressedSize =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), numRowGroups, pool_);

    for (vector_size_t i = 0; i < numRowGroups; ++i) {
      auto rowGroup = fileMetadata.rowGroup(i);
      rowGroupIds->set(i, static_cast<int64_t>(i));
      numRows->set(i, rowGroup.numRows());
      totalByteSize->set(i, rowGroup.totalByteSize());
      totalCompressedSize->set(i, rowGroup.totalCompressedSize());
    }

    return std::make_shared<RowVector>(
        pool_,
        rowGroupsSchema(),
        nullptr,
        numRowGroups,
        std::vector<VectorPtr>{
            rowGroupIds, numRows, totalByteSize, totalCompressedSize});
  }
};

// Joins a column chunk's page encodings into a comma-separated string of
// Parquet encoding names (e.g. "RLE,PLAIN").
std::string encodingsToString(
    const std::vector<parquet::thrift::Encoding>& encodings) {
  std::string result;
  for (auto encoding : encodings) {
    std::string_view name;
    if (!result.empty()) {
      result += ',';
    }
    if (apache::thrift::TEnumTraits<parquet::thrift::Encoding>::findName(
            encoding, &name)) {
      result.append(name);
    } else {
      result += std::to_string(static_cast<int32_t>(encoding));
    }
  }
  return result;
}

// Column-chunk statistics formatted for display. Empty optionals mean the
// chunk carries no statistics for that field.
struct ColumnChunkStats {
  std::optional<std::string> min;
  std::optional<std::string> max;
  std::optional<int64_t> nullCount;
};

// Reads min/max/null_count from a column chunk's Parquet statistics, formatting
// numeric min/max as decimal strings. Returns empty optionals when the chunk
// has no statistics or 'type' has no comparable min/max representation.
ColumnChunkStats readColumnChunkStats(
    parquet::ColumnChunkMetaDataPtr& chunk,
    const TypePtr& type,
    int64_t numRows) {
  ColumnChunkStats stats;
  if (!chunk.hasStatistics()) {
    return stats;
  }
  stats.nullCount = chunk.getColumnMetadataStatsNullCount();
  if (type == nullptr) {
    return stats;
  }

  auto columnStats = chunk.getColumnStatistics(type, numRows);
  if (auto* intStats = dynamic_cast<dwio::common::IntegerColumnStatistics*>(
          columnStats.get())) {
    if (intStats->getMinimum().has_value()) {
      stats.min = std::to_string(intStats->getMinimum().value());
    }
    if (intStats->getMaximum().has_value()) {
      stats.max = std::to_string(intStats->getMaximum().value());
    }
  } else if (
      auto* doubleStats = dynamic_cast<dwio::common::DoubleColumnStatistics*>(
          columnStats.get())) {
    if (doubleStats->getMinimum().has_value()) {
      stats.min = std::to_string(doubleStats->getMinimum().value());
    }
    if (doubleStats->getMaximum().has_value()) {
      stats.max = std::to_string(doubleStats->getMaximum().value());
    }
  } else if (
      auto* stringStats = dynamic_cast<dwio::common::StringColumnStatistics*>(
          columnStats.get())) {
    stats.min = stringStats->getMinimum();
    stats.max = stringStats->getMaximum();
  }
  return stats;
}

// A flattened leaf column: its dotted path from the file root and its scalar
// type. Column chunks are stored per leaf, so a row group has one chunk per
// entry here, in this order.
struct LeafColumn {
  std::string path;
  TypePtr type;
};

// Appends 'segment' to a dotted leaf path, omitting the separator at the root
// so a top-level field stays unprefixed (no leading '.').
std::string joinPath(const std::string& prefix, std::string_view segment) {
  return prefix.empty() ? std::string(segment)
                        : fmt::format("{}.{}", prefix, segment);
}

// Flattens 'type' into its leaf columns in document order, matching the order
// of column chunks in a Parquet row group. Nested fields are named by their
// dotted path: struct fields join with '.', list elements append ".element",
// and map entries append ".key"/".value" (e.g. "address.city", "tags.element",
// "lookup.key").
void collectLeafColumns(
    const std::string& prefix,
    const TypePtr& type,
    std::vector<LeafColumn>& leaves) {
  if (type->kind() == TypeKind::ROW) {
    const auto& rowType = type->asRow();
    for (auto i = 0; i < rowType.size(); ++i) {
      collectLeafColumns(
          joinPath(prefix, rowType.nameOf(i)), rowType.childAt(i), leaves);
    }
  } else if (type->kind() == TypeKind::ARRAY) {
    collectLeafColumns(joinPath(prefix, "element"), type->childAt(0), leaves);
  } else if (type->kind() == TypeKind::MAP) {
    collectLeafColumns(joinPath(prefix, "key"), type->childAt(0), leaves);
    collectLeafColumns(joinPath(prefix, "value"), type->childAt(1), leaves);
  } else {
    leaves.push_back({prefix, type});
  }
}

class ColumnChunksDataSource : public MetadataDataSource {
 public:
  ColumnChunksDataSource(
      const RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      memory::MemoryPool* pool)
      : MetadataDataSource(
            outputType,
            columnChunksSchema(),
            columnHandles,
            pool) {}

 protected:
  RowVectorPtr build() override {
    auto reader = openFile(split_->filePath, pool_);
    auto fileMetadata = reader->fileMetaData();

    std::vector<LeafColumn> leaves;
    collectLeafColumns("", reader->rowType(), leaves);

    vector_size_t totalRows = 0;
    for (int rg = 0; rg < fileMetadata.numRowGroups(); ++rg) {
      totalRows += fileMetadata.rowGroup(rg).numColumns();
    }

    auto rowGroupIds =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), totalRows, pool_);
    auto columnIds =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), totalRows, pool_);
    auto names =
        BaseVector::create<FlatVector<StringView>>(VARCHAR(), totalRows, pool_);
    auto compressions =
        BaseVector::create<FlatVector<StringView>>(VARCHAR(), totalRows, pool_);
    auto encodings =
        BaseVector::create<FlatVector<StringView>>(VARCHAR(), totalRows, pool_);
    auto compressedSizes =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), totalRows, pool_);
    auto uncompressedSizes =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), totalRows, pool_);
    auto numValues =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), totalRows, pool_);
    auto mins =
        BaseVector::create<FlatVector<StringView>>(VARCHAR(), totalRows, pool_);
    auto maxes =
        BaseVector::create<FlatVector<StringView>>(VARCHAR(), totalRows, pool_);
    auto nullCounts =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), totalRows, pool_);

    vector_size_t row = 0;
    for (int rg = 0; rg < fileMetadata.numRowGroups(); ++rg) {
      auto rowGroup = fileMetadata.rowGroup(rg);
      for (int col = 0; col < rowGroup.numColumns(); ++col) {
        auto chunk = rowGroup.columnChunk(col);
        std::string columnName;
        TypePtr columnType;
        if (static_cast<size_t>(col) < leaves.size()) {
          columnName = leaves.at(col).path;
          columnType = leaves.at(col).type;
        } else {
          columnName = fmt::format("column_{}", col);
        }
        auto compression = common::compressionKindToString(chunk.compression());
        auto encoding = encodingsToString(chunk.encodings());

        auto stats =
            readColumnChunkStats(chunk, columnType, rowGroup.numRows());

        rowGroupIds->set(row, static_cast<int64_t>(rg));
        columnIds->set(row, static_cast<int64_t>(col));
        names->set(row, StringView(columnName));
        compressions->set(row, StringView(compression));
        encodings->set(row, StringView(encoding));
        compressedSizes->set(row, chunk.totalCompressedSize());
        uncompressedSizes->set(row, chunk.totalUncompressedSize());
        numValues->set(row, chunk.numValues());
        setOptionalString(*mins, row, stats.min);
        setOptionalString(*maxes, row, stats.max);
        setOptionalInt(*nullCounts, row, stats.nullCount);
        ++row;
      }
    }

    return std::make_shared<RowVector>(
        pool_,
        columnChunksSchema(),
        nullptr,
        row,
        std::vector<VectorPtr>{
            rowGroupIds,
            columnIds,
            names,
            compressions,
            encodings,
            compressedSizes,
            uncompressedSizes,
            numValues,
            mins,
            maxes,
            nullCounts});
  }

 private:
  static void setOptionalString(
      FlatVector<StringView>& vector,
      vector_size_t row,
      const std::optional<std::string>& value) {
    if (value.has_value()) {
      vector.set(row, StringView(value.value()));
    } else {
      vector.setNull(row, true);
    }
  }

  static void setOptionalInt(
      FlatVector<int64_t>& vector,
      vector_size_t row,
      std::optional<int64_t> value) {
    if (value.has_value()) {
      vector.set(row, value.value());
    } else {
      vector.setNull(row, true);
    }
  }
};

} // namespace

ParquetFileHandler::ParquetFileHandler() {
  addMetadataTable(
      kRowGroups,
      rowGroupsSchema(),
      [](const auto& outputType, const auto& columnHandles, auto* pool) {
        return std::make_unique<RowGroupsDataSource>(
            outputType, columnHandles, pool);
      });
  addMetadataTable(
      kColumnChunks,
      columnChunksSchema(),
      [](const auto& outputType, const auto& columnHandles, auto* pool) {
        return std::make_unique<ColumnChunksDataSource>(
            outputType, columnHandles, pool);
      });
}

void registerParquetHandler() {
  static folly::once_flag once;
  folly::call_once(once, [] {
    static ParquetFileHandler handler;
    registerHandler("parquet", handler);
  });
}

velox::RowTypePtr ParquetFileHandler::resolve(
    const std::string& filePath,
    velox::memory::MemoryPool* pool) const {
  velox::dwio::common::ReaderOptions readerOptions(pool);
  auto fileSystem =
      velox::filesystems::getFileSystem(filePath, /*config=*/nullptr);
  auto readFile = fileSystem->openFileForRead(filePath);
  auto input = std::make_unique<velox::dwio::common::BufferedInput>(
      std::move(readFile), *pool);
  auto reader = std::make_unique<velox::parquet::ParquetReader>(
      std::move(input), readerOptions);
  return reader->rowType();
}

std::unique_ptr<velox::connector::DataSource>
ParquetFileHandler::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::RowTypePtr& fullSchema,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::memory::MemoryPool* pool) const {
  return std::make_unique<ParquetDataSource>(
      outputType, fullSchema, columnHandles, pool);
}

} // namespace facebook::axiom::connector::file
