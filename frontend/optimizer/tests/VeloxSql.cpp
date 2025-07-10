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
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <sys/resource.h>
#include <sys/time.h>

#include "optimizer/connectors/hive/LocalHiveConnectorMetadata.h" //@manual
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/Exchange.h"

#include "optimizer/Plan.h" //@manual
#include "optimizer/SchemaResolver.h" //@manual
#include "optimizer/VeloxHistory.h" //@manual
#include "optimizer/connectors/ConnectorSplitSource.h" //@manual
#include "velox/benchmarks/QueryBenchmarkBase.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/QueryPlanner.h"
#include "velox/parse/TypeResolver.h"
#include "velox/runner/LocalRunner.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/VectorSaver.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::runner;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;

namespace {
static bool notEmpty(const char* /*flagName*/, const std::string& value) {
  return !value.empty();
}

int32_t printResults(const std::vector<RowVectorPtr>& results) {
  int32_t numRows = 0;
  std::cout << "Results:" << std::endl;
  bool printType = true;
  for (const auto& vector : results) {
    // Print RowType only once.
    if (printType) {
      std::cout << vector->type()->asRow().toString() << std::endl;
      printType = false;
    }
    for (vector_size_t i = 0; i < vector->size(); ++i) {
      std::cout << vector->toString(i) << std::endl;
      ++numRows;
    }
  }
  return numRows;
}
} // namespace

DEFINE_string(
    data_path,
    "",
    "Root path of data. Data layout must follow Hive-style partitioning. ");

DECLARE_string(ssd_path);
DECLARE_int32(ssd_cache_gb);
DECLARE_int32(ssd_checkpoint_interval_gb);

DEFINE_int32(optimizer_trace, 0, "Optimizer trace level");

DEFINE_bool(print_plan, false, "Print optimizer results");

DEFINE_bool(print_short_plan, false, "Print one line plan from optimizer.");

DEFINE_bool(print_stats, false, "print statistics");
DECLARE_bool(include_custom_stats);

DEFINE_int32(max_rows, 100, "Max number of printed result rows");

DEFINE_int32(num_workers, 4, "Number of in-process workers");

DECLARE_int32(num_drivers);
DEFINE_int64(split_target_bytes, 16 << 20, "Approx bytes covered by one split");

DECLARE_int32(cache_gb);

DEFINE_bool(use_mmap, false, "Use mmap for buffers and cache");

DEFINE_string(
    query,
    "",
    "Text of query. If empty, reads ';' separated queries from standard input");

DEFINE_string(
    record,
    "",
    "Name of SQL file with a single query. Writes the "
    "output to <name>.ref for use with --check");
DEFINE_string(
    check,
    "",
    "Name of SQL file with a single query. Runs and "
    "compares with <name>.ref, previously recorded with --record");

DEFINE_validator(data_path, &notEmpty);

const char* helpText =
    "Velox Interactive SQL\n"
    "\n"
    "Type SQL and end with ';'.\n"
    "To set a flag, type 'flag <gflag_name> = <valu>;' Leave a space on either side of '='.\n"
    "\n"
    "Useful flags:\n"
    "\n"
    "num_workers - Make a distributed plan for this many workers. Runs it in-process with remote exchanges with serialization and passing data in memory. If num_workers is 1, makes single node plans without remote exchanges.\n"
    "\n"
    "num_drivers - Specifies the parallelism for workers. This many threads per pipeline per worker.\n"
    "\n"
    "print_short_plan - Prints a one line summary of join order.\n"
    "\n"
    "print_plan - Prints optimizer best plan with per operator cardinalities and costs.\n"
    "\n"
    "print_stats - Prints the Velox stats of after execution. Annotates operators with predicted and acttual output cardinality.\n"
    "\n"
    "include_custom_stats - Prints per operator runtime stats.\n";

DEFINE_bool(
    check_test_flag_combinations,
    true,
    "Check that all runs in test_flags_file sweep produce the same result");

class VeloxRunner : public QueryBenchmarkBase {
 public:
  void initialize() {
    if (FLAGS_cache_gb) {
      memory::MemoryManagerOptions options;
      int64_t memoryBytes = FLAGS_cache_gb * (1LL << 30);
      options.useMmapAllocator = FLAGS_use_mmap;
      options.allocatorCapacity = memoryBytes;
      options.useMmapArena = true;
      options.mmapArenaCapacityRatio = 1;
      memory::MemoryManager::testingSetInstance(options);
      std::unique_ptr<cache::SsdCache> ssdCache;
      if (FLAGS_ssd_cache_gb) {
        constexpr int32_t kNumSsdShards = 16;
        cacheExecutor_ =
            std::make_unique<folly::IOThreadPoolExecutor>(kNumSsdShards);
        const cache::SsdCache::Config config(
            FLAGS_ssd_path,
            static_cast<uint64_t>(FLAGS_ssd_cache_gb) << 30,
            kNumSsdShards,
            cacheExecutor_.get(),
            static_cast<uint64_t>(FLAGS_ssd_checkpoint_interval_gb) << 30);
        ssdCache = std::make_unique<cache::SsdCache>(config);
      }

      cache_ = cache::AsyncDataCache::create(
          memory::memoryManager()->allocator(), std::move(ssdCache));
      cache::AsyncDataCache::setInstance(cache_.get());
    } else {
      memory::MemoryManagerOptions options;
      memory::MemoryManager::testingSetInstance(options);
    }

    rootPool_ = memory::memoryManager()->addRootPool("velox_sql");

    optimizerPool_ = rootPool_->addLeafChild("optimizer");
    schemaPool_ = rootPool_->addLeafChild("schema");
    checkPool_ = rootPool_->addLeafChild("check");

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    filesystems::registerLocalFileSystem();
    parquet::registerParquetReaderFactory();
    dwrf::registerDwrfReaderFactory();
    exec::ExchangeSource::registerFactory(
        exec::test::createLocalExchangeSource);
    serializer::presto::PrestoVectorSerde::registerVectorSerde();
    if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
      serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
    }
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(8);
    std::unordered_map<std::string, std::string> connectorConfig;
    connectorConfig[connector::hive::HiveConfig::kLocalDataPath] =
        FLAGS_data_path;
    connectorConfig[connector::hive::HiveConfig::kLocalFileFormat] =
        FLAGS_data_format;
    auto config =
        std::make_shared<config::ConfigBase>(std::move(connectorConfig));
    connector::registerConnectorFactory(
        std::make_shared<connector::hive::HiveConnectorFactory>());
    connector_ =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(kHiveConnectorId, config, ioExecutor_.get());
    connector::registerConnector(connector_);

    std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
        connectorConfigs;
    auto copy = hiveConfig_;
    connectorConfigs[kHiveConnectorId] =
        std::make_shared<config::ConfigBase>(std::move(copy));

    schemaQueryCtx_ = core::QueryCtx::create(
        executor_.get(),
        core::QueryConfig(config_),
        std::move(connectorConfigs),
        cache::AsyncDataCache::getInstance(),
        rootPool_->shared_from_this(),
        spillExecutor_.get(),
        "schema");
    common::SpillConfig spillConfig;
    common::PrefixSortConfig prefixSortConfig;

    schemaRootPool_ = rootPool_->addAggregateChild("schemaRoot");
    connectorQueryCtx_ = std::make_shared<connector::ConnectorQueryCtx>(
        schemaPool_.get(),
        schemaRootPool_.get(),
        schemaQueryCtx_->connectorSessionProperties(kHiveConnectorId),
        &spillConfig,
        prefixSortConfig,
        std::make_unique<exec::SimpleExpressionEvaluator>(
            schemaQueryCtx_.get(), schemaPool_.get()),
        schemaQueryCtx_->cache(),
        "scan_for_schema",
        "schema",
        "N/a",
        0,
        schemaQueryCtx_->queryConfig().sessionTimezone());

    schema_ = std::make_shared<facebook::velox::optimizer::SchemaResolver>(
        connector_, "");

    planner_ = std::make_unique<core::DuckDbQueryPlanner>(optimizerPool_.get());
    auto& tables = dynamic_cast<connector::hive::LocalHiveConnectorMetadata*>(
                       connector_->metadata())
                       ->tables();
    for (auto& pair : tables) {
      planner_->registerTable(pair.first, pair.second->rowType());
    }
    planner_->registerTableScan(
        [this](
            const std::string& id,
            const std::string& name,
            const RowTypePtr& rowType,
            const std::vector<std::string>& columnNames) {
          return toTableScan(id, name, rowType, columnNames);
        });
    history_ = std::make_unique<facebook::velox::optimizer::VeloxHistory>();
    history_->updateFromFile(FLAGS_data_path + "/.history");
    executor_ =
        std::make_shared<folly::CPUThreadPoolExecutor>(std::max<int32_t>(
            std::thread::hardware_concurrency() * 2,
            FLAGS_num_workers * FLAGS_num_drivers * 2 + 2));
    spillExecutor_ = std::make_shared<folly::IOThreadPoolExecutor>(4);
  }

  core::PlanNodePtr toTableScan(
      const std::string& id,
      const std::string& name,
      const RowTypePtr& rowType,
      const std::vector<std::string>& columnNames) {
    using namespace connector::hive;
    auto handle = std::make_shared<HiveTableHandle>(
        kHiveConnectorId, name, true, common::SubfieldFilters{}, nullptr);
    connector::ColumnHandleMap assignments;

    auto table = connector_->metadata()->findTable(name);
    for (auto i = 0; i < rowType->size(); ++i) {
      auto projectedName = rowType->nameOf(i);
      auto& columnName = columnNames[i];
      VELOX_CHECK(
          table->columnMap().find(columnName) != table->columnMap().end(),
          "No column {} in {}",
          columnName,
          name);
      assignments[projectedName] = std::make_shared<HiveColumnHandle>(
          columnName,
          HiveColumnHandle::ColumnType::kRegular,
          rowType->childAt(i),
          rowType->childAt(i));
    }
    return std::make_shared<core::TableScanNode>(
        id, rowType, handle, assignments);
  }

  void runInner(
      LocalRunner& runner,
      std::vector<RowVectorPtr>& result,
      RunStats& stats) {
    uint64_t micros = 0;
    {
      struct rusage start;
      getrusage(RUSAGE_SELF, &start);
      MicrosecondTimer timer(&micros);
      while (auto rows = runner.next()) {
        result.push_back(rows);
      }

      struct rusage final;
      getrusage(RUSAGE_SELF, &final);
      auto tvNanos = [](struct timeval tv) {
        return tv.tv_sec * 1000000000 + tv.tv_usec * 1000;
      };
      stats.userNanos = tvNanos(final.ru_utime) - tvNanos(start.ru_utime);
      stats.systemNanos = tvNanos(final.ru_stime) - tvNanos(start.ru_stime);
    }
    stats.micros = micros;
  }

  /// stores results and plans to 'ref', to be used with --check.
  void setRecordStream(std::ofstream* ref) {
    record_ = ref;
  }

  /// Compares results to data in 'ref'. 'ref' is produced with --record.
  void setCheckStream(std::ifstream* ref) {
    check_ = ref;
  }

  void run(const std::string& sql) {
    if (record_ || check_) {
      std::string error;
      std::string plan;
      std::vector<RowVectorPtr> result;
      runSql(sql, nullptr, nullptr, &error);
      if (error.empty()) {
        runSql(sql, &result, &plan, &error);
      }
      if (record_) {
        if (!error.empty()) {
          writeString(error, *record_);
        } else {
          writeString("", *record_);
          writeString(plan, *record_);
          writeVectors(result, *record_);
        }
      } else if (check_) {
        auto refError = readString(*check_);
        if (refError != error) {
          ++numFailed_;
          std::cerr << "Expected error "
                    << (refError.empty() ? std::string("no error") : refError)
                    << " got "
                    << (error.empty() ? std::string("no error") : error)
                    << std::endl;
          if (!refError.empty()) {
            readString(*check_);
            readVectors(*check_);
          }
          return;
        }
        if (!error.empty()) {
          // errors matched.
          return;
        }
        auto refPlan = readString(*check_);
        auto refResult = readVectors(*check_);
        bool planMiss = false;
        bool resultMiss = false;
        if (plan != refPlan) {
          std::cerr << "Plan mismatch: Expected " << refPlan << std::endl
                    << " got " << plan << std::endl;
          ++numPlanMismatch_;
          planMiss = true;
        }
        if (!assertEqualResults(refResult, result)) {
          ++numResultMismatch_;
          resultMiss = true;
        }
        if (!resultMiss && !planMiss) {
          ++numPassed_;
        } else {
          ++numFailed_;
        }
      }
    } else {
      if (!FLAGS_test_flags_file.empty()) {
        sql_ = sql;
        try {
          parameters_.clear();
          runStats_.clear();
          gflags::FlagSaver save;
          runAllCombinations();
          for (auto& dim : parameters_) {
            modifiedFlags_.insert(dim.flag);
          }
        } catch (const std::exception& e) {
        }
        hasReferenceResult_ = false;
        referenceResult_.clear();
        referenceRunner_.reset();
      } else {
        runSql(sql);
      }
    }
  }

  const OperatorStats* findOperatorStats(
      const TaskStats& taskStats,
      const core::PlanNodeId& id) {
    for (auto& p : taskStats.pipelineStats) {
      for (auto& o : p.operatorStats) {
        if (o.planNodeId == id) {
          return &o;
        }
      }
    }
    return nullptr;
  }

  std::string predictionString(
      const core::PlanNodeId& id,
      const TaskStats& taskStats,
      const optimizer::NodePredictionMap& prediction) {
    auto it = prediction.find(id);
    if (it == prediction.end()) {
      return "";
    }
    auto* operatorStats = findOperatorStats(taskStats, id);
    if (!operatorStats) {
      return fmt::format("*** missing stats for {}", id);
    }
    auto predicted = it->second.cardinality;
    auto actual = operatorStats->outputPositions;
    return fmt::format("predicted={} actual={} ", predicted, actual);
  }

  /// Runs a query and returns the result as a single vector in *resultVector,
  /// the plan text in *planString and the error message in *errorString.
  /// *errorString is not set if no error. Any of these may be nullptr.
  std::shared_ptr<LocalRunner> runSql(
      const std::string& sql,
      std::vector<RowVectorPtr>* resultVector = nullptr,
      std::string* planString = nullptr,
      std::string* errorString = nullptr,
      std::vector<exec::TaskStats>* statsReturn = nullptr,
      RunStats* runStatsReturn = nullptr) {
    std::shared_ptr<LocalRunner> runner;
    std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
        connectorConfigs;
    auto copy = hiveConfig_;
    connectorConfigs[kHiveConnectorId] =
        std::make_shared<config::ConfigBase>(std::move(copy));
    ++queryCounter_;
    auto queryCtx = core::QueryCtx::create(
        executor_.get(),
        core::QueryConfig(config_),
        std::move(connectorConfigs),
        cache::AsyncDataCache::getInstance(),
        rootPool_->shared_from_this(),
        spillExecutor_.get(),
        fmt::format("query_{}", queryCounter_));

    // The default Locus for planning is the system and data of 'connector_'.
    optimizer::Locus locus(connector_->connectorId().c_str(), connector_.get());
    core::PlanNodePtr plan;
    try {
      plan = planner_->plan(sql);
    } catch (std::exception& e) {
      std::cerr << "parse error: " << e.what() << std::endl;
      if (errorString) {
        *errorString = fmt::format("Parse error: {}", e.what());
      }
      return nullptr;
    }
    MultiFragmentPlan::Options opts;
    opts.numWorkers = FLAGS_num_workers;
    opts.numDrivers = FLAGS_num_drivers;
    auto allocator =
        std::make_unique<HashStringAllocator>(optimizerPool_.get());
    auto context =
        std::make_unique<facebook::velox::optimizer::QueryGraphContext>(
            *allocator);
    facebook::velox::optimizer::queryCtx() = context.get();
    exec::SimpleExpressionEvaluator evaluator(
        queryCtx.get(), optimizerPool_.get());
    optimizer::PlanAndStats planAndStats;
    try {
      facebook::velox::optimizer::Schema veraxSchema(
          "test", schema_.get(), &locus);
      optimizer::OptimizerOptions optimizerOpts = {
          .traceFlags = FLAGS_optimizer_trace};
      optimizer::Optimization opt(
          *plan,
          veraxSchema,
          *history_,
          queryCtx,
          evaluator,
          optimizerOpts,
          opts);
      auto best = opt.bestPlan();
      if (planString) {
        *planString = best->op->toString(true, false);
      }
      if (FLAGS_print_short_plan) {
        std::cout << "Plan: " << best->toString(false);
      }
      if (FLAGS_print_plan) {
        std::cout << "Plan: " << best->toString(true);
      }
      planAndStats = opt.toVeloxPlan(best->op, opts);
    } catch (const std::exception& e) {
      facebook::velox::optimizer::queryCtx() = nullptr;
      std::cerr << "optimizer error: " << e.what() << std::endl;
      if (errorString) {
        *errorString = fmt::format("optimizer error: {}", e.what());
      }
      return nullptr;
    }
    facebook::velox::optimizer::queryCtx() = nullptr;
    RunStats runStats;
    try {
      connector::SplitOptions splitOptions{
          .targetSplitCount = FLAGS_num_workers * FLAGS_num_drivers * 2,
          .fileBytesPerSplit = static_cast<uint64_t>(FLAGS_split_target_bytes)};
      runner = std::make_shared<LocalRunner>(
          planAndStats.plan,
          queryCtx,
          std::make_shared<connector::ConnectorSplitSourceFactory>(
              splitOptions));
      std::vector<RowVectorPtr> results;
      runInner(*runner, results, runStats);

      int numRows = printResults(results);
      if (resultVector) {
        *resultVector = results;
      }
      auto stats = runner->stats();
      if (statsReturn) {
        *statsReturn = stats;
      }
      auto& fragments = planAndStats.plan->fragments();
      for (int32_t i = fragments.size() - 1; i >= 0; --i) {
        for (auto& pipeline : stats[i].pipelineStats) {
          auto& first = pipeline.operatorStats[0];
          if (first.operatorType == "TableScan") {
            runStats.rawInputBytes += first.rawInputBytes;
          }
        }
        if (FLAGS_print_stats) {
          std::cout << "Fragment " << i << ":" << std::endl;
          std::cout << printPlanWithStats(
              *fragments[i].fragment.planNode,
              stats[i],
              FLAGS_include_custom_stats,
              [&](auto id) {
                return predictionString(id, stats[i], planAndStats.prediction);
              });
          std::cout << std::endl;
        }
      }
      history_->recordVeloxExecution(planAndStats, stats);
      std::cout << numRows << " rows " << runStats.toString(false) << std::endl;
    } catch (const std::exception& e) {
      std::cerr << "Query terminated with: " << e.what() << std::endl;
      if (errorString) {
        *errorString = fmt::format("Runtime error: {}", e.what());
      }
      waitForCompletion(runner);
      return nullptr;
    }
    waitForCompletion(runner);
    if (runStatsReturn) {
      *runStatsReturn = runStats;
    }
    return runner;
  }

  void runMain(std::ostream& out, RunStats& runStats) override {
    std::vector<RowVectorPtr> result;
    auto runner = runSql(sql_, &result, nullptr, nullptr, nullptr, &runStats);
    if (FLAGS_check_test_flag_combinations) {
      if (hasReferenceResult_) {
        exec::test::assertEqualResults(referenceResult_, result);
        result.clear();
      } else {
        hasReferenceResult_ = true;
        referenceResult_ = std::move(result);
        referenceRunner_ = std::move(runner);
      }
    } else {
      // Must clear before 'runner' goes out of scope.
      result.clear();
    }
  }

  void waitForCompletion(const std::shared_ptr<LocalRunner>& runner) {
    if (runner) {
      try {
        runner->waitForCompletion(500000);
      } catch (const std::exception& /*ignore*/) {
      }
    }
  }

  /// Returns exit status for run. 0 is passed, 1 is plan differences only, 2 is
  /// result differences.
  int32_t checkStatus() {
    std::cerr << numPassed_ << " passed " << numFailed_ << " failed "
              << numPlanMismatch_ << " plan mismatch " << numResultMismatch_
              << " result mismatch" << std::endl;
    if (!numFailed_) {
      return 0;
    }
    return numResultMismatch_ ? 2 : 1;
  }

  std::unordered_map<std::string, std::string>& sessionConfig() {
    return config_;
  }

  std::set<std::string>& modifiedFlags() {
    return modifiedFlags_;
  }

  void saveHistory() {
    history_->saveToFile(FLAGS_data_path + "/.history");
  }

  void clearHistory() {
    history_ = std::make_unique<optimizer::VeloxHistory>();
  }

 private:
  template <typename T>
  static void write(const T& value, std::ostream& out) {
    out.write((char*)&value, sizeof(T));
  }

  template <typename T>
  static T read(std::istream& in) {
    T value;
    in.read((char*)&value, sizeof(T));
    return value;
  }

  static std::string readString(std::istream& in) {
    auto len = read<int32_t>(in);
    std::string result;
    result.resize(len);
    in.read(result.data(), result.size());
    return result;
  }

  static void writeString(const std::string& string, std::ostream& out) {
    write<int32_t>(string.size(), out);
    out.write(string.data(), string.size());
  }

  std::vector<RowVectorPtr> readVectors(std::istream& in) {
    auto size = read<int32_t>(in);
    std::vector<RowVectorPtr> result(size);
    for (auto i = 0; i < size; ++i) {
      result[i] = std::dynamic_pointer_cast<RowVector>(
          restoreVector(in, checkPool_.get()));
    }
    return result;
  }

  void writeVectors(std::vector<RowVectorPtr>& vectors, std::ostream& out) {
    write<int32_t>(vectors.size(), out);
    for (auto& vector : vectors) {
      saveVector(*vector, out);
    }
  }

  int32_t printResults(const std::vector<RowVectorPtr>& results) {
    std::cout << "Results:" << std::endl;
    bool printType = true;
    int32_t numRows = 0;
    for (auto vectorIndex = 0; vectorIndex < results.size(); ++vectorIndex) {
      const auto& vector = results[vectorIndex];
      // Print RowType only once.
      if (printType) {
        std::cout << vector->type()->asRow().toString() << std::endl;
        printType = false;
      }
      for (vector_size_t i = 0; i < vector->size(); ++i) {
        std::cout << vector->toString(i) << std::endl;
        if (++numRows >= FLAGS_max_rows) {
          int32_t numLeft = (vector->size() - (i - 1));
          ++vectorIndex;
          for (; vectorIndex < results.size(); ++vectorIndex) {
            numLeft += results[vectorIndex]->size();
          }
          if (numLeft) {
            std::cout << fmt::format("[Omitted {} more rows.", numLeft)
                      << std::endl;
          }
          return numRows + numLeft;
        }
      }
    }
    return numRows;
  }

  std::shared_ptr<memory::MemoryAllocator> allocator_;
  std::shared_ptr<cache::AsyncDataCache> cache_;
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> optimizerPool_;
  std::shared_ptr<memory::MemoryPool> schemaPool_;
  std::shared_ptr<memory::MemoryPool> schemaRootPool_;
  std::shared_ptr<memory::MemoryPool> checkPool_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> cacheExecutor_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<folly::IOThreadPoolExecutor> spillExecutor_;
  std::shared_ptr<core::QueryCtx> schemaQueryCtx_;
  std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  std::shared_ptr<connector::Connector> connector_;
  std::shared_ptr<optimizer::SchemaResolver> schema_;
  std::unique_ptr<facebook::velox::optimizer::VeloxHistory> history_;
  std::unique_ptr<core::DuckDbQueryPlanner> planner_;
  std::unordered_map<std::string, std::string> hiveConfig_;
  std::ofstream* record_{nullptr};
  std::ifstream* check_{nullptr};
  int32_t numPassed_{0};
  int32_t numFailed_{0};
  int32_t numPlanMismatch_{0};
  int32_t numResultMismatch_{0};
  int32_t queryCounter_{0};
  std::string sql_;
  bool hasReferenceResult_{false};
  // Keeps live 'referenceResult_'.
  std::shared_ptr<runner::LocalRunner> referenceRunner_;
  // Result from first run of flag value sweep.
  std::vector<RowVectorPtr> referenceResult_;
  std::set<std::string> modifiedFlags_;
};

std::string readCommand(std::istream& in, bool& end) {
  std::string line;
  std::stringstream command;
  end = false;
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == ';') {
      command << line.substr(0, line.size() - 1);
      return command.str();
    }
    command << line << std::endl;
  }
  end = true;
  return "";
}

void readCommands(
    VeloxRunner& runner,
    const std::string& prompt,
    std::istream& in) {
  for (;;) {
    std::cout << prompt;
    bool end;
    std::string command = readCommand(in, end);
    if (end) {
      break;
    }
    if (command.empty()) {
      continue;
    }
    auto cstr = command.c_str();
    if (command.substr(0, 4) == "help") {
      std::cout << helpText;
      continue;
    }
    char* flag = nullptr;
    char* value = nullptr;
    if (sscanf(cstr, "flag %ms = %ms", &flag, &value) == 2) {
      auto message = gflags::SetCommandLineOption(flag, value);
      if (!message.empty()) {
        std::cout << message;
        runner.modifiedFlags().insert(std::string(flag));
      } else {
        std::cout << "No flag " << flag;
      }
      free(flag);
      free(value);
      continue;
    }
    if (sscanf(cstr, "clear %ms", &flag) == 1) {
      gflags::CommandLineFlagInfo info;
      if (!gflags::GetCommandLineFlagInfo(flag, &info)) {
        std::cout << "No flag " << flag << std::endl;
        continue;
      }
      auto message =
          gflags::SetCommandLineOption(flag, info.default_value.c_str());
      if (!message.empty()) {
        std::cout << message;
      }
      continue;
    }
    if (command == "flags") {
      std::cout << "Modified flags:\n";
      for (auto& name : runner.modifiedFlags()) {
        std::string value;
        if (gflags::GetCommandLineOption(name.c_str(), &value)) {
          std::cout << name << " = " << value << std::endl;
        }
      }
      continue;
    }
    if (sscanf(cstr, "session %ms = %ms", &flag, &value) == 2) {
      std::cout << "session " << flag << " set to " << value << std::endl;
      runner.sessionConfig()[std::string(flag)] = std::string(value);
      free(flag);
      free(value);
      continue;
    }
    if (command.substr(0, 11) == "savehistory") {
      runner.saveHistory();
      continue;
    }
    if (command.substr(0, 12) == "clearhistory") {
      runner.clearHistory();
      continue;
    }
    runner.run(command);
  }
}

void initCommands(VeloxRunner& runner) {
  auto* home = getenv("HOME");
  std::string homeDir = home ? std::string(home) : std::string(".");
  std::ifstream in(homeDir + "/.vsql");
  readCommands(runner, "", in);
}

void recordQueries(VeloxRunner& runner) {
  std::ifstream in(FLAGS_record);
  std::ofstream ref;
  ref.open(FLAGS_record + ".ref", std::ios_base::out | std::ios_base::trunc);
  runner.setRecordStream(&ref);
  readCommands(runner, "", in);
}

void checkQueries(VeloxRunner& runner) {
  std::ifstream in(FLAGS_check);
  std::ifstream ref(FLAGS_check + ".ref");
  runner.setCheckStream(&ref);
  readCommands(runner, "", in);
  exit(runner.checkStatus());
}

std::string sevenBit(std::string& in) {
  for (auto i = 0; i < in.size(); ++i) {
    if ((uint8_t)in[i] > 127) {
      in[i] = ' ';
    }
  }
  return in;
}

int main(int argc, char** argv) {
  std::string kUsage(
      "Velox local SQL command line. Run 'velox_sql --help' for available options.\n");
  gflags::SetUsageMessage(kUsage);
  folly::Init init(&argc, &argv, false);
  VeloxRunner runner;
  try {
    runner.initialize();
    initCommands(runner);
    if (!FLAGS_query.empty()) {
      runner.run(FLAGS_query);
    } else if (!FLAGS_record.empty()) {
      recordQueries(runner);
    } else if (!FLAGS_check.empty()) {
      checkQueries(runner);
    } else {
      std::cout
          << "Velox SQL. Type statement and end with ;. flag name = value; sets a gflag. help; prints help text."
          << std::endl;
      readCommands(runner, "SQL> ", std::cin);
    }
  } catch (std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    exit(-1);
  }
  return 0;
}
