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

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <sys/resource.h>
#include <chrono>
#include <iostream>

#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/OptimizerOptions.h"
#include "axiom/optimizer/tests/FeatureGen.h"
#include "axiom/optimizer/tests/utils/DfFunctions.h"
#include "axiom/optimizer/tests/utils/Register.h"
#include "axiom/runner/LocalRunner.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryConfig.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/TaskStats.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"

DEFINE_string(data_path, "", "Directory path for the local dataset");
DEFINE_int32(num_float, 10, "Number of distinct float feature keys");
DEFINE_int32(num_id_list, 10, "Number of distinct id_list feature keys");
DEFINE_int32(
    id_list_max_cardinality,
    100,
    "Maximum cardinality for id_list features");
DEFINE_int32(
    num_id_score_list,
    5,
    "Number of distinct id_score_list feature keys");
DEFINE_bool(create, false, "If true, creates a new dataset");
DEFINE_int32(subfield_mode, 2, "Subfield pushdown mode (1, 2, or 3)");
DEFINE_int32(parallel, 4, "Parallel projection width");
DEFINE_int32(num_drivers, 4, "Number of drivers for executing the plan");
DEFINE_int32(num_files, 10, "Number of files to generate");
DEFINE_int32(num_rows_per_file, 10000, "Number of rows per file");
DEFINE_int32(num_repeats, 3, "Number of times to repeat the read test");
DEFINE_int32(batch_size, 0, "Target batch size in rows (0 = use default)");
DEFINE_int64(
    batch_bytes,
    512 << 20,
    "Preferred batch size in bytes (default 512MB)");
DEFINE_bool(
    parallel_lazy,
    true,
    "If true, makeSubfieldProjections creates a LazyDereferenceNode, "
    "otherwise a ProjectNode");

using namespace facebook::velox;
using namespace facebook::axiom;

namespace {

constexpr std::string_view kHiveConnectorId = "hive";

// Helper to print ParallelProjectNode information from the plan
void printPlanSummary(const core::PlanNodePtr& plan) {
  LOG(INFO) << "\n=== Plan Summary (ParallelProjectNodes) ===";

  std::function<void(const core::PlanNodePtr&)> visit =
      [&](const core::PlanNodePtr& node) {
        if (!node) {
          return;
        }

        if (auto* parallelProject =
                dynamic_cast<const core::ParallelProjectNode*>(node.get())) {
          const auto& groups = parallelProject->exprGroups();
          LOG(INFO) << "ParallelProjectNode [" << parallelProject->id()
                    << "]: " << groups.size() << " groups";

          for (size_t i = 0; i < groups.size(); ++i) {
            const auto& group = groups[i];
            std::string firstExpr =
                group.empty() ? "(empty)" : group.front()->toString();
            std::string lastExpr =
                group.empty() ? "(empty)" : group.back()->toString();

            // Truncate long expressions
            if (firstExpr.size() > 80) {
              firstExpr = firstExpr.substr(0, 77) + "...";
            }
            if (lastExpr.size() > 80) {
              lastExpr = lastExpr.substr(0, 77) + "...";
            }

            LOG(INFO) << "  Group " << i << ": " << group.size()
                      << " expressions";
            LOG(INFO) << "    First: " << firstExpr;
            if (group.size() > 1) {
              LOG(INFO) << "    Last:  " << lastExpr;
            }
          }
        }

        for (const auto& source : node->sources()) {
          visit(source);
        }
      };

  visit(plan);
}

// Helper to print runtime stats per operator
void printRuntimeStats(const std::vector<exec::TaskStats>& stats) {
  LOG(INFO) << "\n=== Runtime Stats per PlanNode ===";
  LOG(INFO) << fmt::format(
      "{:<20} {:<15} {:>12} {:>12} {:>12}",
      "NodeType",
      "NodeId",
      "WallMs",
      "CpuMs",
      "PeakMemMB");
  LOG(INFO) << std::string(75, '-');

  for (size_t taskIdx = 0; taskIdx < stats.size(); ++taskIdx) {
    const auto& taskStats = stats[taskIdx];
    for (const auto& pipeline : taskStats.pipelineStats) {
      for (const auto& op : pipeline.operatorStats) {
        // Sum wall and CPU time from addInput, getOutput, and finish timings
        uint64_t wallNanos = op.addInputTiming.wallNanos +
            op.getOutputTiming.wallNanos + op.finishTiming.wallNanos;
        uint64_t cpuNanos = op.addInputTiming.cpuNanos +
            op.getOutputTiming.cpuNanos + op.finishTiming.cpuNanos;

        double wallMs = wallNanos / 1'000'000.0;
        double cpuMs = cpuNanos / 1'000'000.0;
        double peakMemMB =
            op.memoryStats.peakTotalMemoryReservation / (1024.0 * 1024.0);

        LOG(INFO) << fmt::format(
            "{:<20} {:<15} {:>12.2f} {:>12.2f} {:>12.2f}",
            op.operatorType,
            op.planNodeId,
            wallMs,
            cpuMs,
            peakMemMB);
      }
    }
  }
}

// Global state for benchmark
std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor;
std::unique_ptr<folly::CPUThreadPoolExecutor> cpuExecutor;
std::shared_ptr<memory::MemoryPool> rootPool;
std::shared_ptr<memory::MemoryPool> optimizerPool;

// Initialize Velox and register all required components
void initialize() {
  // Initialize memory manager
  memory::MemoryManager::initialize({});

  // Register scalar and aggregate functions
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  optimizer::FunctionRegistry::registerPrestoFunctions();

  // Register filesystems and file formats
  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();

  // Register serializers
  serializer::presto::PrestoVectorSerde::registerVectorSerde();
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }

  // Register exchange source
  exec::ExchangeSource::registerFactory(exec::test::createLocalExchangeSource);

  // Register UDFs
  velox_udf::registerDfMockUdfs();
  optimizer::test::registerDfFunctions();

  // Create executors
  ioExecutor = std::make_unique<folly::IOThreadPoolExecutor>(8);
  cpuExecutor = std::make_unique<folly::CPUThreadPoolExecutor>(4);

  // Create memory pools
  rootPool = memory::memoryManager()->addRootPool("feature_benchmark");
  optimizerPool = rootPool->addLeafChild("optimizer");
}

// Register hive connector with the data path
void registerHiveConnector(const std::string& dataPath) {
  using facebook::axiom::connector::ConnectorMetadata;
  using facebook::axiom::connector::hive::LocalHiveConnectorMetadata;
  using facebook::velox::connector::hive::HiveConfig;
  using facebook::velox::connector::hive::HiveConnector;
  using facebook::velox::connector::hive::HiveConnectorFactory;

  std::unordered_map<std::string, std::string> configs;
  configs[HiveConfig::kLocalDataPath] = dataPath;
  configs[HiveConfig::kLocalFileFormat] =
      dwio::common::toString(dwio::common::FileFormat::DWRF);

  auto hiveConfig = std::make_shared<config::ConfigBase>(std::move(configs));

  HiveConnectorFactory hiveConnectorFactory;
  auto hiveConnector = hiveConnectorFactory.newConnector(
      std::string(kHiveConnectorId), hiveConfig, ioExecutor.get());
  facebook::velox::connector::registerConnector(std::move(hiveConnector));

  // Register LocalHiveConnectorMetadata
  auto connectorPtr =
      facebook::velox::connector::getConnector(std::string(kHiveConnectorId));
  ConnectorMetadata::registerMetadata(
      std::string(kHiveConnectorId),
      std::make_shared<LocalHiveConnectorMetadata>(
          dynamic_cast<HiveConnector*>(connectorPtr.get())));
}

// Unregister hive connector
void unregisterHiveConnector() {
  facebook::axiom::connector::ConnectorMetadata::unregisterMetadata(
      std::string(kHiveConnectorId));
  facebook::velox::connector::unregisterConnector(
      std::string(kHiveConnectorId));
}

// Create QueryCtx for running queries
std::shared_ptr<core::QueryCtx> makeQueryCtx(
    const std::string& queryId,
    const std::unordered_map<std::string, std::string>& config = {}) {
  std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
      connectorConfigs;
  connectorConfigs[std::string(kHiveConnectorId)] =
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>{});

  return core::QueryCtx::create(
      cpuExecutor.get(),
      core::QueryConfig(config),
      std::move(connectorConfigs),
      cache::AsyncDataCache::getInstance(),
      rootPool,
      /*spillExecutor=*/nullptr,
      queryId);
}

// Get feature options from flags
optimizer::test::FeatureOptions getFeatureOptions() {
  optimizer::test::FeatureOptions opts;
  opts.rng.seed(42);
  opts.numFloat = FLAGS_num_float;
  opts.numIdList = FLAGS_num_id_list;
  opts.numIdScoreList = FLAGS_num_id_score_list;
  opts.idListMinCard = 1;
  opts.idListMaxCard = FLAGS_id_list_max_cardinality;
  opts.expCardinalities = true;
  return opts;
}

// Create the dataset
void createDataset() {
  // Use aggregate pool since dwrf::Writer creates child pools internally
  auto pool = rootPool->addAggregateChild("create_dataset");
  auto vectorPool = pool->addLeafChild("vectors");
  auto opts = getFeatureOptions();

  auto fs = filesystems::getFileSystem(FLAGS_data_path, {});
  fs->mkdir(fmt::format("{}/features", FLAGS_data_path));

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set<const std::vector<uint32_t>>(
      dwrf::Config::MAP_FLAT_COLS, {2, 3, 4});

  for (int fileIdx = 0; fileIdx < FLAGS_num_files; ++fileIdx) {
    // Use multiple smaller batches to avoid int32 offset overflow in arrays
    constexpr int32_t kNumBatches = 10;
    auto vectors = optimizer::test::makeFeatures(
        kNumBatches,
        FLAGS_num_rows_per_file / kNumBatches,
        opts,
        vectorPool.get());

    const auto filePath =
        fmt::format("{}/features/features_{}.dwrf", FLAGS_data_path, fileIdx);

    // Write using dwrf writer directly
    dwrf::WriterOptions writerOpts;
    writerOpts.config = config;
    writerOpts.schema = vectors[0]->type();
    writerOpts.memoryPool = pool.get();

    auto sink = std::make_unique<dwio::common::LocalFileSink>(
        filePath, dwio::common::FileSink::Options{});
    auto writer = std::make_unique<dwrf::Writer>(std::move(sink), writerOpts);

    for (const auto& vec : vectors) {
      writer->write(vec);
    }
    writer->close();

    LOG(INFO) << "Created file " << fileIdx + 1 << "/" << FLAGS_num_files
              << ": " << filePath;
  }

  LOG(INFO) << "Dataset creation complete. Total rows: "
            << (FLAGS_num_files * FLAGS_num_rows_per_file);

  // Explicitly destruct pools before returning to ensure they are cleaned up
  // before the memory manager is destroyed at program exit.
  vectorPool.reset();
  pool.reset();
}

// Run the benchmark
void runBenchmark() {
  auto opts = getFeatureOptions();

  // Initialize the struct types in opts by calling makeFeatures for one row.
  // This populates floatStruct, idListStruct, idScoreListStruct which are
  // needed by makeFeaturePipeline.
  auto initPool = rootPool->addLeafChild("init");
  optimizer::test::makeFeatures(1, 1, opts, initPool.get());
  initPool.reset();

  // Reinitialize metadata to pick up newly created tables
  auto* metadata = facebook::axiom::connector::ConnectorMetadata::metadata(
      std::string(kHiveConnectorId));
  auto* hiveMetadata = dynamic_cast<
      facebook::axiom::connector::hive::LocalHiveConnectorMetadata*>(metadata);
  VELOX_CHECK_NOT_NULL(hiveMetadata);
  hiveMetadata->reinitialize();

  // Configure batch size if specified
  std::unordered_map<std::string, std::string> queryConfig;
  // Disable TableScan getOutput time limit to avoid premature yields during
  // benchmarking.
  queryConfig[core::QueryConfig::kTableScanGetOutputTimeLimitMs] = "0";
  queryConfig[core::QueryConfig::kPreferredOutputBatchBytes] =
      std::to_string(FLAGS_batch_bytes);
  if (FLAGS_batch_size > 0) {
    queryConfig[core::QueryConfig::kPreferredOutputBatchRows] =
        std::to_string(FLAGS_batch_size);
  }

  // Create the feature pipeline
  auto logicalPlan =
      optimizer::test::makeFeaturePipeline(opts, kHiveConnectorId);

  LOG(INFO) << "=== Feature Pipeline Benchmark ===";
  LOG(INFO) << "Configuration:";
  LOG(INFO) << "  Subfield mode: " << FLAGS_subfield_mode;
  LOG(INFO) << "  Parallel width: " << FLAGS_parallel;
  LOG(INFO) << "  Num drivers: " << FLAGS_num_drivers;
  LOG(INFO) << "  Num float features: " << FLAGS_num_float;
  LOG(INFO) << "  Num id_list features: " << FLAGS_num_id_list;
  LOG(INFO) << "  Num id_score_list features: " << FLAGS_num_id_score_list;
  if (FLAGS_batch_size > 0) {
    LOG(INFO) << "  Batch size: " << FLAGS_batch_size;
  }

  // Configure optimizer options
  optimizer::OptimizerOptions optimizerOptions;
  switch (FLAGS_subfield_mode) {
    case 1:
      break;
    case 2:
      optimizerOptions.pushdownSubfields = true;
      optimizerOptions.allMapsAsStruct = true;
      break;
    case 3:
      optimizerOptions.pushdownSubfields = true;
      optimizerOptions.mapAsStruct["features"] = {
          "float_features", "id_list_features", "id_score_list_features"};
      break;
    default:
      LOG(FATAL) << "Invalid subfield_mode: " << FLAGS_subfield_mode;
  }
  optimizerOptions.parallelProjectWidth = FLAGS_parallel;
  optimizerOptions.lazySubfieldProject = FLAGS_parallel_lazy;

  // Compile the plan using Optimization::toVeloxPlan
  runner::MultiFragmentPlan::Options runnerOptions;
  runnerOptions.numDrivers = FLAGS_num_drivers;
  runnerOptions.numWorkers = 1;

  auto planAndStats = optimizer::Optimization::toVeloxPlan(
      *logicalPlan, *optimizerPool, optimizerOptions, runnerOptions);

  // Print plan summary before running
  printPlanSummary(planAndStats.plan->fragments().front().fragment.planNode);

  for (int run = 0; run < FLAGS_num_repeats; ++run) {
    LOG(INFO) << "\n=== Run " << (run + 1) << "/" << FLAGS_num_repeats
              << " ===";

    struct rusage startUsage, endUsage;
    getrusage(RUSAGE_SELF, &startUsage);
    auto startWall = std::chrono::steady_clock::now();

    auto queryId = fmt::format("feature_benchmark_run_{}", run);
    auto queryCtx = makeQueryCtx(queryId, queryConfig);

    // Create a new LocalRunner
    auto runner = std::make_shared<runner::LocalRunner>(
        planAndStats.plan,
        runner::FinishWrite{}, // empty finishWrite for read-only queries
        queryCtx,
        std::make_shared<runner::ConnectorSplitSourceFactory>(),
        optimizerPool);

    // Calculate data sizes
    int64_t totalOutputBytes = 0;
    int64_t totalOutputRows = 0;

    while (auto batch = runner->next()) {
      totalOutputRows += batch->size();
      totalOutputBytes += batch->retainedSize();
    }

    auto endWall = std::chrono::steady_clock::now();
    getrusage(RUSAGE_SELF, &endUsage);

    // Calculate wall time
    double wallTimeSec =
        std::chrono::duration<double>(endWall - startWall).count();
    double wallTimeMs = wallTimeSec * 1000.0;

    // Calculate user and system CPU time
    auto toSeconds = [](const timeval& tv) {
      return static_cast<double>(tv.tv_sec) +
          static_cast<double>(tv.tv_usec) / 1'000'000.0;
    };
    double userCpuSec = toSeconds(endUsage.ru_utime) - toSeconds(startUsage.ru_utime);
    double systemCpuSec = toSeconds(endUsage.ru_stime) - toSeconds(startUsage.ru_stime);
    double totalCpuSec = userCpuSec + systemCpuSec;

    // Calculate CPU% (total CPU as percentage of wall time)
    double cpuPercent = (wallTimeSec > 0) ? (totalCpuSec / wallTimeSec) * 100.0 : 0.0;
    // Calculate system% (system CPU as percentage of total CPU)
    double systemPercent = (totalCpuSec > 0) ? (systemCpuSec / totalCpuSec) * 100.0 : 0.0;

    // Calculate rates
    double outputMBps =
        (totalOutputBytes / (1024.0 * 1024.0)) / (wallTimeMs / 1000.0);

    LOG(INFO) << "Results:";
    LOG(INFO) << fmt::format(
        "  Wall time: {:.1f} ms, CPU: {:.0f}%, sys: {:.0f}%",
        wallTimeMs,
        cpuPercent,
        systemPercent);
    LOG(INFO) << "  Output bytes: " << totalOutputBytes << " ("
              << (totalOutputBytes / (1024.0 * 1024.0)) << " MB)";
    LOG(INFO) << "  Output rows: " << totalOutputRows;
    LOG(INFO) << "  Output rate: " << outputMBps << " MB/s";
    LOG(INFO) << "  Peak query pool size: "
              << queryCtx->pool()->peakBytes() / (1024.0 * 1024.0) << " MB";

    // Print runtime stats per operator
    printRuntimeStats(runner->stats());
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  if (FLAGS_data_path.empty()) {
    LOG(ERROR) << "Must specify --data_path";
    return 1;
  }

  // Initialize all components
  initialize();

  // Register hive connector with data path
  registerHiveConnector(FLAGS_data_path);

  if (FLAGS_create) {
    LOG(INFO) << "Creating dataset...";
    createDataset();
    LOG(INFO) << "Dataset creation complete.";
  } else {
    runBenchmark();
  }

  // Unregister connector first - it may hold references to pools.
  unregisterHiveConnector();

  // Shutdown executors before freeing pools to ensure all async work completes.
  cpuExecutor.reset();
  ioExecutor.reset();

  // Explicitly free all memory pools after activity is complete and before
  // the memory manager is destroyed at program exit.
  // Child (leaf) pools first, then aggregate/root pools.
  optimizerPool.reset();
  rootPool.reset();

  return 0;
}
