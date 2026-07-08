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
#include "velox/type/Variant.h"
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
      {"path", ARRAY(VARCHAR())},
      {"compression", VARCHAR()},
      {"encodings", ARRAY(VARCHAR())},
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

// Maps a column chunk's page encodings to their Parquet encoding names (e.g.
// "RLE", "PLAIN"), falling back to the numeric code for unknown encodings.
std::vector<std::string> encodingNames(
    const std::vector<parquet::thrift::Encoding>& encodings) {
  std::vector<std::string> names;
  names.reserve(encodings.size());
  for (auto encoding : encodings) {
    std::string_view name;
    if (apache::thrift::TEnumTraits<parquet::thrift::Encoding>::findName(
            encoding, &name)) {
      names.emplace_back(name);
    } else {
      names.push_back(std::to_string(static_cast<int32_t>(encoding)));
    }
  }
  return names;
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

// Flattens 'type' into its leaf types in schema document order, matching the
// order of the per-leaf column chunks in a Parquet row group. A type with no
// children is a leaf; every other type recurses over its children in order,
// which for ROW, ARRAY and MAP is the same order Parquet lays out the leaf
// column chunks (row fields, array element, then map key followed by value).
// Leaf names are not derived here: they come from each chunk's physical
// path_in_schema metadata (see ColumnChunksDataSource::build), which carries
// the synthetic repeated-group levels ("list"/"key_value") that the logical row
// type omits. Only the leaf types are collected, to decode each chunk's
// statistics.
void collectLeafTypes(const TypePtr& type, std::vector<TypePtr>& leafTypes) {
  if (type->size() == 0) {
    leafTypes.push_back(type);
    return;
  }
  for (auto i = 0; i < type->size(); ++i) {
    collectLeafTypes(type->childAt(i), leafTypes);
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

    std::vector<TypePtr> leafTypes;
    collectLeafTypes(reader->rowType(), leafTypes);

    vector_size_t totalRows = 0;
    for (int rg = 0; rg < fileMetadata.numRowGroups(); ++rg) {
      totalRows += fileMetadata.rowGroup(rg).numColumns();
    }

    auto rowGroupIds =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), totalRows, pool_);
    auto columnIds =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), totalRows, pool_);
    auto compressions =
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

    std::vector<std::vector<std::string>> paths;
    paths.reserve(totalRows);
    std::vector<std::vector<std::string>> encodings;
    encodings.reserve(totalRows);

    vector_size_t row = 0;
    for (int rg = 0; rg < fileMetadata.numRowGroups(); ++rg) {
      auto rowGroup = fileMetadata.rowGroup(rg);
      // Column chunks and 'leafTypes' are both in schema document order (a
      // depth-first walk of the leaves), so column 'col' below and
      // leafTypes[col] refer to the same schema leaf: the chunk supplies the
      // path, the leaf type decodes the statistics. The two orders derive from
      // the same schema, so the only way they can diverge is a leaf-count
      // disagreement; reject that here rather than mislabel the chunks.
      VELOX_USER_CHECK_EQ(
          rowGroup.numColumns(),
          static_cast<int>(leafTypes.size()),
          "Parquet row group column-chunk count does not match the file schema's leaf count: {}",
          split_->filePath);
      for (int col = 0; col < rowGroup.numColumns(); ++col) {
        auto chunk = rowGroup.columnChunk(col);
        const auto& leafType = leafTypes.at(col);
        auto compression = common::compressionKindToString(chunk.compression());

        auto stats = readColumnChunkStats(chunk, leafType, rowGroup.numRows());

        rowGroupIds->set(row, static_cast<int64_t>(rg));
        columnIds->set(row, static_cast<int64_t>(col));
        // Physical path_in_schema as ordered segments, including the
        // "list"/"key_value" group levels Parquet inserts for arrays and maps.
        paths.push_back(chunk.pathInSchema());
        encodings.push_back(encodingNames(chunk.encodings()));
        compressions->set(row, StringView(compression));
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
            makeStringArray(paths),
            compressions,
            makeStringArray(encodings),
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

  // Builds an ARRAY<VARCHAR> vector with one array per row from the per-row
  // string lists (used for both the path segments and the encoding names).
  VectorPtr makeStringArray(
      const std::vector<std::vector<std::string>>& rows) const {
    std::vector<Variant> arrays;
    arrays.reserve(rows.size());
    for (const auto& row : rows) {
      std::vector<Variant> elements;
      elements.reserve(row.size());
      for (const auto& value : row) {
        elements.emplace_back(value);
      }
      arrays.push_back(Variant::array(std::move(elements)));
    }
    return BaseVector::createFromVariants(ARRAY(VARCHAR()), arrays, pool_);
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
