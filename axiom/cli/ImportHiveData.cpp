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

// CLI for importing external data files (Parquet, DWRF, or TEXT) into the Axiom
// Hive connector. Scans each subdirectory under --data_path, infers schema from
// file headers, computes column statistics, and writes .schema and .stats
// metadata files required by LocalHiveConnectorMetadata.
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
#include "axiom/connectors/hive/LocalTableBuilder.h"
#include "axiom/connectors/hive/LocalTableMetadata.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/type/Type.h"

DEFINE_string(data_path, "", "Root directory containing table subdirectories.");
DEFINE_string(data_format, "parquet", "Data format: parquet, dwrf, or text.");
DEFINE_bool(force, false, "Re-import tables that already have metadata.");
DEFINE_bool(debug, false, "Enable debug logging.");

using namespace facebook::velox;

int main(int argc, char** argv) {
  gflags::SetUsageMessage(
      "Import external data files into the Axiom Hive connector.\n\n"
      "Scans each subdirectory under --data_path, infers schema from\n"
      "file headers, computes column statistics, and writes .schema and\n"
      ".stats metadata files required by the Hive connector.\n\n"
      "Usage:\n"
      "  axiom_hive_import --data_path /path/to/data [--data_format parquet]\n\n"
      "Tables that already have metadata are skipped. Use --force to\n"
      "re-import (e.g. after adding new data files to a table).\n");

  folly::Init init(&argc, &argv);

  // Suppress glog output unless --debug is set.
  FLAGS_logtostderr = FLAGS_debug;

  if (FLAGS_data_path.empty()) {
    fmt::print(stderr, "Error: --data_path must be specified.\n");
    return 1;
  }

  if (!std::filesystem::is_directory(FLAGS_data_path)) {
    fmt::print(stderr, "Error: '{}' is not a directory.\n", FLAGS_data_path);
    return 1;
  }

  const auto fileFormat = dwio::common::toFileFormat(FLAGS_data_format);
  if (fileFormat != dwio::common::FileFormat::PARQUET &&
      fileFormat != dwio::common::FileFormat::DWRF &&
      fileFormat != dwio::common::FileFormat::TEXT) {
    fmt::print(
        stderr,
        "Error: Unsupported data format: '{}'. "
        "Use 'parquet', 'dwrf', or 'text'.\n",
        FLAGS_data_format);
    return 1;
  }

  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  filesystems::registerLocalFileSystem();
  Type::registerSerDe();
  auto pool = memory::memoryManager()->addLeafPool("import");

  // Register a Hive connector so LocalTableBuilder can sample for NDV.
  auto connectorRegistry = connector::ConnectorRegistry::create();
  auto metadataRegistry =
      facebook::axiom::connector::ConnectorMetadataRegistry::create();
  facebook::axiom::Connectors connectors{connectorRegistry, metadataRegistry};
  auto connector =
      connectors.registerLocalHiveConnector(FLAGS_data_path, FLAGS_data_format);
  auto* hiveConnector =
      dynamic_cast<connector::hive::HiveConnector*>(connector.get());

  facebook::axiom::connector::hive::LocalTableBuilder builder(
      pool.get(), fileFormat, hiveConnector);

  // Discover table subdirectories.
  std::vector<std::filesystem::path> tableDirs;
  for (const auto& entry :
       std::filesystem::directory_iterator(FLAGS_data_path)) {
    if (entry.is_directory()) {
      tableDirs.push_back(entry.path());
    }
  }

  std::sort(tableDirs.begin(), tableDirs.end());

  if (tableDirs.empty()) {
    fmt::print(
        stderr,
        "No table subdirectories found in '{}'.\n"
        "Each subdirectory is treated as a table. Place data files in\n"
        "subdirectories, e.g. {}/trips/file.parquet\n",
        FLAGS_data_path,
        FLAGS_data_path);
    return 1;
  }

  fmt::print(
      stderr,
      "Importing {} table(s) from '{}' (format: {})\n",
      tableDirs.size(),
      FLAGS_data_path,
      FLAGS_data_format);

  size_t numImported{0};
  for (const auto& tableDir : tableDirs) {
    const auto tableName = tableDir.filename().string();

    // Skip directories that contain no data files (only hidden metadata).
    bool hasDataFiles{false};
    for (const auto& file : std::filesystem::directory_iterator(tableDir)) {
      if (file.is_regular_file() &&
          !file.path().filename().string().starts_with('.')) {
        hasDataFiles = true;
        break;
      }
    }

    if (!hasDataFiles) {
      fmt::print(stderr, "  {} ... skipped (no data files)\n", tableName);
      continue;
    }

    // Skip tables that already have .schema and .stats metadata unless --force.
    if (!FLAGS_force &&
        std::filesystem::exists(
            facebook::axiom::connector::hive::schemaPath(tableDir.string()))) {
      fmt::print(
          stderr,
          "  {} ... skipped (already imported, use --force to re-import)\n",
          tableName);
      continue;
    }

    fmt::print(stderr, "  {} ...", tableName);

    auto start = std::chrono::steady_clock::now();
    builder.build(tableDir.string());
    auto elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start);

    fmt::print(stderr, " done ({:.2f}s)\n", elapsed.count());
    ++numImported;
  }

  fmt::print(stderr, "Imported {} table(s).\n", numImported);
  return 0;
}
