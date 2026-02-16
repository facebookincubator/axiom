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

// CLI for generating TPC-H data in Parquet or DWRF format.
//
// Design guidelines:
//   - All user-facing output goes to stderr (stdout is reserved for data).
//   - Input validation uses early returns with clean error messages (no
//     exceptions).
//   - Runtime errors (e.g. from Velox) are left uncaught to preserve full stack
//     traces for debugging.
//   - glog output is suppressed by default; enable with --debug.

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <chrono>
#include <filesystem>
#include "axiom/cli/Connectors.h"
#include "axiom/optimizer/tests/TpchDataGenerator.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/tpch/gen/TpchGen.h"

DEFINE_string(data_path, "", "Output directory for TPC-H data.");
DEFINE_double(sf, 0.1, "TPC-H scale factor (e.g., 0.1, 1, 10).");
DEFINE_string(data_format, "parquet", "Data format: parquet or dwrf.");
DEFINE_string(
    compression,
    "none",
    "Compression: none, snappy, zlib, zstd, lz4, gzip.");
DEFINE_bool(debug, false, "Enable debug logging.");

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
using facebook::axiom::optimizer::test::TpchDataGenerator;

namespace {

void generateTpchData(
    const std::string& path,
    double scaleFactor,
    dwio::common::FileFormat fileFormat,
    common::CompressionKind compressionKind) {
  for (const auto& table : tpch::tables) {
    const auto tableName = tpch::toTableName(table);

    fmt::print(stderr, "Generating {} ...\n", tableName);

    const auto start = std::chrono::steady_clock::now();

    auto numRows = TpchDataGenerator::createTable(
        table, path, scaleFactor, fileFormat, compressionKind);

    const auto elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start);

    fmt::print(
        stderr,
        "Generated {}: {} rows in {:.2f} seconds.\n",
        tableName,
        numRows,
        elapsed.count());
  }
}

} // namespace

int main(int argc, char** argv) {
  gflags::SetUsageMessage(
      "Generate TPC-H data.\n\n"
      "Usage:\n"
      "  buck run axiom/cli:tpchgen -- \\\n"
      "    --data_path /home/$USER/tpch/sf0.1 --sf 0.1 \\\n"
      "    [--data_format parquet] [--compression snappy]\n");

  folly::Init init(&argc, &argv);

  // Suppress glog output unless --debug is set.
  FLAGS_logtostderr = FLAGS_debug;

  if (FLAGS_data_path.empty()) {
    fmt::print(stderr, "Error: --data_path must be specified.\n");
    return 1;
  }

  if (FLAGS_sf <= 0) {
    fmt::print(stderr, "Error: --sf must be positive.\n");
    return 1;
  }

  const auto fileFormat = dwio::common::toFileFormat(FLAGS_data_format);
  if (fileFormat != dwio::common::FileFormat::PARQUET &&
      fileFormat != dwio::common::FileFormat::DWRF) {
    fmt::print(
        stderr,
        "Error: Unsupported data format: '{}'. Use 'parquet' or 'dwrf'.\n",
        FLAGS_data_format);
    return 1;
  }

  const auto compressionKind =
      common::stringToCompressionKind(FLAGS_compression);

  std::error_code ec;
  std::filesystem::create_directories(FLAGS_data_path, ec);
  if (ec) {
    fmt::print(
        stderr,
        "Error: Cannot create directory '{}': {}\n",
        FLAGS_data_path,
        ec.message());
    return 1;
  }

  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  filesystems::registerLocalFileSystem();

  facebook::axiom::Connectors connectors;
  connectors.registerTpchConnector(
      std::string(PlanBuilder::kTpchDefaultConnectorId));
  connectors.registerLocalHiveConnector(
      FLAGS_data_path,
      FLAGS_data_format,
      std::string(PlanBuilder::kHiveDefaultConnectorId));

  fmt::print(
      stderr,
      "Generating TPC-H data (sf={}, format={}, compression={}) in {}\n",
      FLAGS_sf,
      FLAGS_data_format,
      FLAGS_compression,
      FLAGS_data_path);

  generateTpchData(FLAGS_data_path, FLAGS_sf, fileFormat, compressionKind);

  return 0;
}
