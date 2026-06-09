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

/// A command line tool to generate query graph, logical plan, or
/// distributed plan visualization from a SQL query. Supports TPC-H tables
/// by default, or local tables in Parquet/DWRF/text format when --data_path
/// is specified.
///
/// Generate query graph (default):
///   buck2 run //axiom/cli:graphviz -- --query "SELECT * FROM customer" \
///       --output query.svg
///
/// Generate logical plan:
///   buck2 run //axiom/cli:graphviz -- --mode=logical \
///       --query "SELECT * FROM customer" --output plan.svg
///
/// Generate distributed plan:
///   buck2 run //axiom/cli:graphviz -- --mode=distributed \
///       --query "SELECT * FROM customer" --output plan.svg
///
/// Use local tables instead of TPC-H:
///   buck2 run //axiom/cli:graphviz -- --data_path /path/to/data \
///       --data_format parquet --query "SELECT * FROM mytable" --output
///       plan.svg
///
/// Generate DOT file (no .svg extension):
///   buck2 run //axiom/cli:graphviz -- --query "SELECT * FROM customer" \
///       --output query
///
/// The query can also be passed via stdin:
///   echo "SELECT * FROM customer" | buck2 run //axiom/cli:graphviz -- \
///       --query "" --output query.svg

#include <folly/Subprocess.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include "axiom/cli/Connectors.h"
#include "axiom/cli/SqlQueryRunner.h"
#include "axiom/cli/SystemUser.h"

DEFINE_string(
    query,
    "",
    "SQL query to generate DOT for. If empty, reads query from stdin.");
DEFINE_string(
    output,
    "",
    "Output file path. Use .svg extension to generate SVG.");
DEFINE_string(
    mode,
    "graph",
    "Visualization mode: graph, logical, or distributed.");
DEFINE_string(
    data_path,
    "",
    "Path to directory with local tables. If empty, uses TPC-H tables.");
DEFINE_string(
    data_format,
    "parquet",
    "Format of local tables: parquet, dwrf, or text.");
DEFINE_int32(
    num_workers,
    4,
    "Number of workers (only used by --mode=distributed).");
DEFINE_int32(
    num_drivers,
    4,
    "Number of drivers per worker (only used by --mode=distributed).");

namespace facebook::axiom {
namespace {

// Returns the path to the 'dot' executable.
std::string findDotExecutable() {
  static const std::vector<std::string> kDotPaths = {
      "/usr/bin/dot",
      "/usr/local/bin/dot",
      "/opt/homebrew/bin/dot",
  };

  for (const auto& path : kDotPaths) {
    if (std::filesystem::exists(path)) {
      return path;
    }
  }

  return "dot";
}

// Converts DOT to SVG using the 'dot' command. Returns true on success.
bool dotToSvg(const std::string& dot, const std::string& outputPath) {
  try {
    std::vector<std::string> args = {
        findDotExecutable(), "-Tsvg", "-o", outputPath};
    folly::Subprocess proc(
        args,
        folly::Subprocess::Options().pipeStdin().pipeStderr().closeOtherFds());

    auto result = proc.communicate(dot);
    auto returnCode = proc.wait();

    if (!returnCode.exited() || returnCode.exitStatus() != 0) {
      std::cerr << "Failed to run 'dot' command." << std::endl;
      if (!result.second.empty()) {
        std::cerr << "Error: " << result.second << std::endl;
      }
      return false;
    }
    return true;
  } catch (const std::exception& e) {
    std::cerr << "Failed to run 'dot' command: " << e.what() << std::endl;
    std::cerr << "Make sure Graphviz is installed (e.g., 'sudo apt install "
                 "graphviz')."
              << std::endl;
    return false;
  }
}

} // namespace
} // namespace facebook::axiom

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv, false);

  if (FLAGS_output.empty()) {
    std::cerr << "Usage: " << argv[0]
              << " --query \"SELECT ...\" --output <file>" << std::endl;
    std::cerr << "   or: echo \"SELECT ...\" | " << argv[0]
              << " --query \"\" --output <file.svg>" << std::endl;
    std::cerr << std::endl;
    std::cerr
        << "Generates query graph, logical plan, or distributed plan visualization."
        << std::endl;
    std::cerr << std::endl;
    std::cerr << "Options:" << std::endl;
    std::cerr << "  --mode          graph (default), logical, or distributed"
              << std::endl;
    std::cerr << "  --data_path     Path to local tables (default: TPC-H)"
              << std::endl;
    std::cerr
        << "  --data_format   Format: parquet, dwrf, text (default: parquet)"
        << std::endl;
    std::cerr << std::endl;
    std::cerr << "Use .svg extension to generate SVG (requires 'dot' command)."
              << std::endl;
    std::cerr << "Otherwise, generates DOT file." << std::endl;
    return 1;
  }

  std::string query = FLAGS_query;
  if (query.empty()) {
    std::ostringstream buffer;
    buffer << std::cin.rdbuf();
    query = buffer.str();
  }

  if (query.empty()) {
    std::cerr << "No query provided." << std::endl;
    return 1;
  }

  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});

  facebook::axiom::Connectors connectors;
  axiom::sql::SqlQueryRunner runner{axiom::sql::SystemUser::resolve()};
  runner.initialize([&]() {
    auto defaultConnector = connectors.registerTpchConnector();
    auto defaultSchema = "tiny";
    if (!FLAGS_data_path.empty()) {
      defaultConnector = connectors.registerLocalHiveConnector(
          FLAGS_data_path, FLAGS_data_format);
      defaultSchema = "default";
    }
    return std::make_pair(defaultConnector->connectorId(), defaultSchema);
  });

  std::string dot;
  if (FLAGS_mode == "graph") {
    dot = runner.toQueryGraphDot(query);
  } else if (FLAGS_mode == "logical") {
    dot = runner.toLogicalPlanDot(query);
  } else if (FLAGS_mode == "distributed") {
    dot = runner.toMultiFragmentPlanDot(
        query, FLAGS_num_workers, FLAGS_num_drivers);
  } else {
    std::cerr << "Unknown --mode: " << FLAGS_mode
              << ". Expected graph, logical, or distributed." << std::endl;
    return 1;
  }

  if (FLAGS_output.ends_with(".svg")) {
    if (!facebook::axiom::dotToSvg(dot, FLAGS_output)) {
      return 1;
    }
    std::cerr << "Generated: " << FLAGS_output << std::endl;
  } else {
    std::ofstream out(FLAGS_output);
    if (!out) {
      std::cerr << "Failed to open output file: " << FLAGS_output << std::endl;
      return 1;
    }
    out << dot;
    std::cerr << "Generated: " << FLAGS_output << std::endl;
  }

  return 0;
}
