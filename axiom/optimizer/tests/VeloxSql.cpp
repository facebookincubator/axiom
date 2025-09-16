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
#include <iostream>
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/SchemaResolver.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/optimizer/tests/PrestoParser.h"
#include "axiom/runner/LocalRunner.h"
#include "velox/benchmarks/QueryBenchmarkBase.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/VectorSaver.h"

DEFINE_string(
    data_path,
    "",
    "Root path of data. Data layout must follow Hive-style partitioning. ");

// Defined in velox/benchmarks/QueryBenchmarkBase.cpp
DECLARE_string(ssd_path);
DECLARE_int32(ssd_cache_gb);
DECLARE_int32(ssd_checkpoint_interval_gb);
DECLARE_int32(cache_gb);

DEFINE_bool(use_mmap, false, "Use mmap for buffers and cache");

DEFINE_uint32(optimizer_trace, 0, "Optimizer trace level");

DEFINE_bool(print_logical_plan, false, "Print logical plan (optimizer input)");

DEFINE_bool(print_plan, false, "Print optimizer results");

DEFINE_bool(print_short_plan, false, "Print one line plan from optimizer.");

DEFINE_bool(print_velox_plan, false, "Print executable Velox plan");

DEFINE_bool(
    print_stats,
    false,
    "Print executable Velox plan annotated with runtime statistics");

// Defined in velox/benchmarks/QueryBenchmarkBase.cpp
DECLARE_bool(include_custom_stats);

DEFINE_int32(max_rows, 100, "Max number of printed result rows");

DEFINE_int32(num_workers, 4, "Number of in-process workers");

// Defined in velox/benchmarks/QueryBenchmarkBase.cpp
DECLARE_int32(num_drivers);

DEFINE_int64(split_target_bytes, 16 << 20, "Approx bytes covered by one split");

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

DEFINE_bool(
    check_test_flag_combinations,
    true,
    "Check that all runs in test_flags_file sweep produce the same result");

using namespace facebook::velox;

namespace facebook::axiom {

const char* helpText =
    "Velox Interactive SQL\n"
    "\n"
    "Type SQL and end with ';'.\n"
    "To set a flag, type 'flag <gflag_name> = <value>;' Leave a space on either side of '='.\n"
    "\n"
    "Useful flags:\n"
    "\n"
    "num_workers - Make a distributed plan for this many workers. Runs it in-process with remote exchanges with serialization and passing data in memory. If num_workers is 1, makes single node plans without remote exchanges.\n"
    "\n"
    "num_drivers - Specifies the parallelism for workers. This many threads per pipeline per worker.\n"
    "\n"
    "print_logical_plan - Prints logical plan (input to the optimizer)r.\n"
    "\n"
    "print_short_plan - Prints a one line summary of join order.\n"
    "\n"
    "print_plan - Prints optimizer best plan with per operator cardinalities and costs.\n"
    "\n"
    "print_velox_plan - Prints executable Velox plan.\n"
    "\n"
    "print_stats - Prints the Velox stats of after execution. Annotates operators with predicted and acttual output cardinality.\n"
    "\n"
    "include_custom_stats - Prints per operator runtime stats.\n";

static const std::string kHiveConnectorId = "hive";

class VeloxRunner : public velox::QueryBenchmarkBase {
 public:
  void initialize() override {
    initializeMemoryManager();
    rootPool_ = memory::memoryManager()->addRootPool("velox_sql");

    optimizerPool_ = rootPool_->addLeafChild("optimizer");
    checkPool_ = rootPool_->addLeafChild("check");

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();

    optimizer::FunctionRegistry::registerPrestoFunctions();

    filesystems::registerLocalFileSystem();
    parquet::registerParquetReaderFactory();
    dwrf::registerDwrfReaderFactory();
    exec::ExchangeSource::registerFactory(
        exec::test::createLocalExchangeSource);
    serializer::presto::PrestoVectorSerde::registerVectorSerde();
    if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
      serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
    }

    if (!FLAGS_data_path.empty()) {
      connector_ = registerHiveConnector(FLAGS_data_path);
    } else {
      connector_ = registerTpchConnector();
    }

    schema_ = std::make_shared<optimizer::SchemaResolver>();

    prestoParser_ = std::make_unique<optimizer::test::PrestoParser>(
        connector_->connectorId(), optimizerPool_.get());

    history_ = std::make_unique<optimizer::VeloxHistory>();

    if (!FLAGS_data_path.empty()) {
      history_->updateFromFile(FLAGS_data_path + "/.history");
    }

    executor_ =
        std::make_shared<folly::CPUThreadPoolExecutor>(std::max<int32_t>(
            std::thread::hardware_concurrency() * 2,
            FLAGS_num_workers * FLAGS_num_drivers * 2 + 2));
    spillExecutor_ = std::make_shared<folly::IOThreadPoolExecutor>(4);
  }

  void initializeMemoryManager() {
    if (FLAGS_cache_gb) {
      memory::MemoryManager::Options options;
      int64_t memoryBytes = FLAGS_cache_gb * (1LL << 30);
      options.useMmapAllocator = FLAGS_use_mmap;
      options.allocatorCapacity = memoryBytes;
      options.useMmapArena = true;
      options.mmapArenaCapacityRatio = 1;
      memory::MemoryManager::testingSetInstance(options);

      cache_ = cache::AsyncDataCache::create(
          memory::memoryManager()->allocator(), setupSsdCache());
      cache::AsyncDataCache::setInstance(cache_.get());
    } else {
      memory::MemoryManager::Options options;
      memory::MemoryManager::testingSetInstance(options);
    }
  }

  std::unique_ptr<cache::SsdCache> setupSsdCache() {
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
      return std::make_unique<cache::SsdCache>(config);
    }

    return nullptr;
  }

  std::shared_ptr<velox::connector::Connector> registerTpchConnector() {
    auto emptyConfig = std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});

    velox::connector::tpch::TpchConnectorFactory factory;
    auto connector = factory.newConnector("tpch", emptyConfig);
    velox::connector::registerConnector(connector);

    connector::ConnectorMetadata::registerMetadata(
        connector->connectorId(),
        std::make_shared<connector::tpch::TpchConnectorMetadata>(
            dynamic_cast<velox::connector::tpch::TpchConnector*>(
                connector.get())));

    return connector;
  }

  std::shared_ptr<velox::connector::Connector> registerHiveConnector(
      const std::string& dataPath) {
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(8);

    std::unordered_map<std::string, std::string> connectorConfig = {
        {velox::connector::hive::HiveConfig::kLocalDataPath, dataPath},
        {velox::connector::hive::HiveConfig::kLocalFileFormat,
         FLAGS_data_format},
    };

    auto config =
        std::make_shared<config::ConfigBase>(std::move(connectorConfig));

    velox::connector::hive::HiveConnectorFactory factory;
    auto connector =
        factory.newConnector(kHiveConnectorId, config, ioExecutor_.get());
    velox::connector::registerConnector(connector);

    connector::ConnectorMetadata::registerMetadata(
        kHiveConnectorId,
        std::make_shared<connector::hive::LocalHiveConnectorMetadata>(
            dynamic_cast<velox::connector::hive::HiveConnector*>(
                connector.get())));

    return connector;
  }

  std::vector<RowVectorPtr> runInner(
      facebook::axiom::runner::LocalRunner& runner,
      RunStats& stats) {
    std::vector<RowVectorPtr> results;
    uint64_t micros = 0;
    {
      struct rusage start;
      getrusage(RUSAGE_SELF, &start);
      MicrosecondTimer timer(&micros);

      while (auto rows = runner.next()) {
        results.push_back(rows);
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

    return results;
  }

  // Stores results and plans to 'ref', to be used with --check.
  void setRecordStream(std::ofstream* ref) {
    record_ = ref;
  }

  // Compares results to data in 'ref'. 'ref' is produced with --record.
  void setCheckStream(std::ifstream* ref) {
    check_ = ref;
  }

  void run(std::string_view sql) {
    optimizer::test::SqlStatementPtr sqlStatement;
    try {
      sqlStatement = prestoParser_->parse(sql);
    } catch (std::exception& e) {
      std::cerr << "Failed to parse SQL: " << e.what() << std::endl;
      return;
    }

    if (sqlStatement->isExplain()) {
      auto* explain =
          sqlStatement->asUnchecked<optimizer::test::ExplainStatement>();

      CHECK(explain->statement()->isSelect());
      auto* select =
          explain->statement()->asUnchecked<optimizer::test::SelectStatement>();
      if (explain->isAnalyze()) {
        runExplainAnalyze(*select);
      } else {
        runExplain(*select);
      }
      return;
    }

    CHECK(sqlStatement->isSelect());

    const auto logicalPlan =
        sqlStatement->asUnchecked<optimizer::test::SelectStatement>()->plan();

    if (record_ || check_) {
      std::string error;
      std::string plan;
      std::vector<RowVectorPtr> result;
      runSql(logicalPlan, nullptr, nullptr, &error);
      if (error.empty()) {
        runSql(logicalPlan, &result, &plan, &error);
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
        if (!exec::test::assertEqualResults(refResult, result)) {
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
        logicalPlan_ = logicalPlan;
        try {
          parameters_.clear();
          runStats_.clear();
          gflags::FlagSaver save;
          runAllCombinations();
          for (auto& dim : parameters_) {
            modifiedFlags_.insert(dim.flag);
          }
        } catch (const std::exception&) {
        }
        hasReferenceResult_ = false;
        referenceResult_.clear();
        referenceRunner_.reset();
      } else {
        runSql(logicalPlan);
      }
    }
  }

  std::shared_ptr<core::QueryCtx> newQuery() {
    ++queryCounter_;

    return core::QueryCtx::create(
        executor_.get(),
        core::QueryConfig(config_),
        {},
        cache::AsyncDataCache::getInstance(),
        rootPool_->shared_from_this(),
        spillExecutor_.get(),
        fmt::format("query_{}", queryCounter_));
  }

  void runExplain(const optimizer::test::SelectStatement& statement) {
    auto plan = optimize(statement.plan(), newQuery());
    std::cout << plan.toString() << std::endl;
  }

  void runExplainAnalyze(const optimizer::test::SelectStatement& statement) {
    auto queryCtx = newQuery();
    auto planAndStats = optimize(statement.plan(), queryCtx);

    auto runner = makeRunner(planAndStats, queryCtx);
    SCOPE_EXIT {
      waitForCompletion(runner);
    };

    RunStats unused;
    auto results = runInner(*runner, unused);

    printPlanWithStats(*runner, planAndStats.prediction);

    std::cout << "(" << countResults(results) << " rows in " << results.size()
              << " batches)" << std::endl
              << std::endl;
  }

  optimizer::PlanAndStats optimize(
      const logical_plan::LogicalPlanNodePtr& logicalPlan,
      const std::shared_ptr<core::QueryCtx>& queryCtx,
      const std::function<void(const optimizer::Plan&)>& peakAtBestPlan =
          nullptr) {
    if (FLAGS_print_logical_plan) {
      std::cout << "Logical plan: " << std::endl
                << logical_plan::PlanPrinter::toText(*logicalPlan) << std::endl;
    }

    facebook::axiom::runner::MultiFragmentPlan::Options opts;
    opts.numWorkers = FLAGS_num_workers;
    opts.numDrivers = FLAGS_num_drivers;
    auto allocator =
        std::make_unique<HashStringAllocator>(optimizerPool_.get());
    auto context = std::make_unique<optimizer::QueryGraphContext>(*allocator);

    optimizer::queryCtx() = context.get();
    SCOPE_EXIT {
      optimizer::queryCtx() = nullptr;
    };

    // The default Locus for planning is the system and data of
    // 'connector_'.
    optimizer::Locus locus(connector_->connectorId().c_str(), connector_.get());
    optimizer::Schema veraxSchema("test", schema_.get(), &locus);
    exec::SimpleExpressionEvaluator evaluator(
        queryCtx.get(), optimizerPool_.get());

    optimizer::Optimization optimization(
        *logicalPlan,
        veraxSchema,
        *history_,
        queryCtx,
        evaluator,
        {.traceFlags = FLAGS_optimizer_trace},
        opts);

    auto best = optimization.bestPlan();

    if (peakAtBestPlan) {
      peakAtBestPlan(*best);
    }

    if (FLAGS_print_short_plan) {
      std::cout << "Physical plan (oneline): " << best->toString(false)
                << std::endl;
    }

    if (FLAGS_print_plan) {
      std::cout << "Physical plan: " << std::endl
                << best->toString(true) << std::endl;
    }

    return optimization.toVeloxPlan(best->op);
  }

  static void printPlanWithStats(
      facebook::axiom::runner::LocalRunner& runner,
      const optimizer::NodePredictionMap& estimates) {
    std::cout << runner.printPlanWithStats([&](const core::PlanNodeId& nodeId,
                                               std::string_view indentation,
                                               std::ostream& out) {
      auto it = estimates.find(nodeId);
      if (it != estimates.end()) {
        out << indentation << "Estimate: " << it->second.cardinality
            << " rows, " << succinctBytes(it->second.peakMemory)
            << " peak memory" << std::endl;
      }
    });
  }

  static std::shared_ptr<facebook::axiom::runner::LocalRunner> makeRunner(
      const optimizer::PlanAndStats& planAndStats,
      const std::shared_ptr<core::QueryCtx>& queryCtx) {
    connector::SplitOptions splitOptions{
        .targetSplitCount = FLAGS_num_workers * FLAGS_num_drivers * 2,
        .fileBytesPerSplit = static_cast<uint64_t>(FLAGS_split_target_bytes)};

    return std::make_shared<facebook::axiom::runner::LocalRunner>(
        planAndStats.plan,
        queryCtx,
        std::make_shared<facebook::axiom::runner::ConnectorSplitSourceFactory>(
            splitOptions));
  }

  /// Runs a query and returns the result as a single vector in *resultVector,
  /// the plan text in *planString and the error message in *errorString.
  /// *errorString is not set if no error. Any of these may be nullptr.
  std::shared_ptr<facebook::axiom::runner::LocalRunner> runSql(
      const logical_plan::LogicalPlanNodePtr& logicalPlan,
      std::vector<RowVectorPtr>* resultVector = nullptr,
      std::string* planString = nullptr,
      std::string* errorString = nullptr,
      std::vector<exec::TaskStats>* statsReturn = nullptr,
      RunStats* runStatsReturn = nullptr) {
    auto queryCtx = newQuery();
    optimizer::PlanAndStats planAndStats;
    try {
      planAndStats = optimize(logicalPlan, queryCtx, [&](const auto& best) {
        if (planString) {
          *planString = best.op->toString(true, false);
        }
      });
    } catch (const std::exception& e) {
      std::cerr << "Failed to optimize: " << e.what() << std::endl;
      if (errorString) {
        *errorString = fmt::format("Failed to optimize: {}", e.what());
      }
      return nullptr;
    }

    if (FLAGS_print_velox_plan) {
      std::cout << "Velox executable plan: " << std::endl
                << planAndStats.plan->toString() << std::endl;
    }

    try {
      auto runner = makeRunner(planAndStats, queryCtx);
      SCOPE_EXIT {
        waitForCompletion(runner);
      };

      RunStats runStats;
      auto results = runInner(*runner, runStats);

      if (resultVector) {
        *resultVector = results;
      }

      const auto stats = runner->stats();
      if (statsReturn) {
        *statsReturn = stats;
      }

      const auto& fragments = planAndStats.plan->fragments();
      for (auto i = 0; i < fragments.size(); ++i) {
        auto nodeStats = exec::toPlanStats(stats[i]);
        for (const auto& scan : fragments[i].scans) {
          auto statsIt = nodeStats.find(scan->id());
          if (statsIt != nodeStats.end()) {
            runStats.rawInputBytes += statsIt->second.rawInputBytes;
          }
        }
      }

      if (runStatsReturn) {
        *runStatsReturn = runStats;
      }

      const int numRows = printResults(results);

      if (FLAGS_print_stats) {
        printPlanWithStats(*runner, planAndStats.prediction);
      }

      history_->recordVeloxExecution(planAndStats, stats);
      std::cout << numRows << " rows " << runStats.toString(false) << std::endl;

      return runner;
    } catch (const std::exception& e) {
      std::cerr << "Query terminated with: " << e.what() << std::endl;
      if (errorString) {
        *errorString = fmt::format("Runtime error: {}", e.what());
      }
      return nullptr;
    }
  }

  void runMain(std::ostream& out, RunStats& runStats) override {
    std::vector<RowVectorPtr> result;
    auto runner =
        runSql(logicalPlan_, &result, nullptr, nullptr, nullptr, &runStats);
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

  void waitForCompletion(
      const std::shared_ptr<facebook::axiom::runner::LocalRunner>& runner) {
    if (runner) {
      try {
        runner->waitForCompletion(500000);
      } catch (const std::exception&) {
      }
    }
  }

  /// Returns exit status for run. 0 is passed, 1 is plan differences only, 2
  /// is result differences.
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

  static void writeString(std::string_view string, std::ostream& out) {
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

  static int64_t countResults(const std::vector<RowVectorPtr>& results) {
    int64_t numRows = 0;
    for (const auto& result : results) {
      numRows += result->size();
    }
    return numRows;
  }

  static int32_t printResults(const std::vector<RowVectorPtr>& results) {
    const auto numRows = countResults(results);

    auto printFooter = [&]() {
      std::cout << "(" << numRows << " rows in " << results.size()
                << " batches)" << std::endl
                << std::endl;
    };

    if (numRows == 0) {
      printFooter();
      return 0;
    }

    const auto type = results.front()->rowType();
    std::cout << type->toString() << std::endl;

    const auto numColumns = type->size();

    std::vector<std::vector<std::string>> data;
    std::vector<size_t> widths(numColumns, 0);
    std::vector<bool> alignLeft(numColumns);

    for (auto i = 0; i < numColumns; ++i) {
      widths[i] = type->nameOf(i).size();
      alignLeft[i] = type->childAt(i)->isVarchar();
    }

    auto printSeparator = [&]() {
      std::cout << std::setfill('-');
      for (auto i = 0; i < numColumns; ++i) {
        if (i > 0) {
          std::cout << "-+-";
        }
        std::cout << std::setw(widths[i]) << "";
      }
      std::cout << std::endl;
      std::cout << std::setfill(' ');
    };

    auto printRow = [&](const auto& row) {
      for (auto i = 0; i < numColumns; ++i) {
        if (i > 0) {
          std::cout << " | ";
        }
        std::cout << std::setw(widths[i]);
        if (alignLeft[i]) {
          std::cout << std::left;
        } else {
          std::cout << std::right;
        }
        std::cout << row[i];
      }
      std::cout << std::endl;
    };

    int32_t numPrinted = 0;

    auto doPrint = [&]() {
      printSeparator();
      printRow(type->names());
      printSeparator();

      for (auto row : data) {
        printRow(row);
      }

      if (numPrinted < numRows) {
        std::cout << std::endl;
        std::cout << "..." << (numRows - numPrinted) << " more rows."
                  << std::endl;
      }

      printFooter();
    };

    for (const auto& result : results) {
      for (auto row = 0; row < result->size(); ++row) {
        data.emplace_back();

        auto& rowData = data.back();
        rowData.resize(numColumns);
        for (auto column = 0; column < numColumns; ++column) {
          rowData[column] = result->childAt(column)->toString(row);
          widths[column] = std::max(widths[column], rowData[column].size());
        }

        ++numPrinted;
        if (numPrinted >= FLAGS_max_rows) {
          doPrint();
          return numRows;
        }
      }
    }

    doPrint();

    return numRows;
  }

  std::shared_ptr<cache::AsyncDataCache> cache_;
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> optimizerPool_;
  std::shared_ptr<memory::MemoryPool> checkPool_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> cacheExecutor_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<folly::IOThreadPoolExecutor> spillExecutor_;
  std::shared_ptr<velox::connector::Connector> connector_;
  std::shared_ptr<optimizer::SchemaResolver> schema_;
  std::unique_ptr<optimizer::VeloxHistory> history_;
  std::unique_ptr<optimizer::test::PrestoParser> prestoParser_;
  std::ofstream* record_{nullptr};
  std::ifstream* check_{nullptr};
  int32_t numPassed_{0};
  int32_t numFailed_{0};
  int32_t numPlanMismatch_{0};
  int32_t numResultMismatch_{0};
  int32_t queryCounter_{0};
  logical_plan::LogicalPlanNodePtr logicalPlan_;
  bool hasReferenceResult_{false};
  // Keeps live 'referenceResult_'.
  std::shared_ptr<facebook::axiom::runner::LocalRunner> referenceRunner_;
  // Result from first run of flag value sweep.
  std::vector<RowVectorPtr> referenceResult_;
  std::set<std::string> modifiedFlags_;
};

// Reads multi-line command from 'in' until encounters ';' followed by zero or
// more whitespaces.
// @return Command text with leading and trailing whitespaces as well as
// trailing ';' removed.
std::string readCommand(std::istream& in, bool& end) {
  std::stringstream command;
  end = false;

  bool stripLeadingSpaces = true;

  std::string line;
  while (std::getline(in, line)) {
    int64_t startPos = 0;
    if (stripLeadingSpaces) {
      for (; startPos < line.size(); ++startPos) {
        if (std::isspace(line[startPos])) {
          continue;
        }
        break;
      }
    }

    if (startPos == line.size()) {
      continue;
    }

    // Allow spaces after ';'.
    for (int64_t i = line.size() - 1; i >= startPos; --i) {
      if (std::isspace(line[i])) {
        continue;
      }

      if (line[i] == ';') {
        command << line.substr(startPos, i - startPos);
        return command.str();
      }

      break;
    }

    stripLeadingSpaces = false;
    command << line.substr(startPos) << std::endl;
  }
  end = true;
  return "";
}

void readCommands(
    VeloxRunner& runner,
    std::string_view prompt,
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

    if (command.starts_with("exit") || command.starts_with("quit")) {
      break;
    }

    if (command.starts_with("help")) {
      std::cout << helpText;
      continue;
    }

    char* flag = nullptr;
    char* value = nullptr;
    if (sscanf(command.c_str(), "flag %ms = %ms", &flag, &value) == 2) {
      auto message = gflags::SetCommandLineOption(flag, value);
      if (!message.empty()) {
        std::cout << message;
        runner.modifiedFlags().insert(std::string(flag));
      } else {
        std::cout << "Failed to set flag '" << flag << "' to '" << value << "'"
                  << std::endl;
      }
      free(flag);
      free(value);
      continue;
    }

    if (sscanf(command.c_str(), "clear %ms", &flag) == 1) {
      gflags::CommandLineFlagInfo info;
      if (!gflags::GetCommandLineFlagInfo(flag, &info)) {
        std::cout << "Failed to clear flag '" << flag << "'" << std::endl;
        continue;
      }
      auto message =
          gflags::SetCommandLineOption(flag, info.default_value.c_str());
      if (!message.empty()) {
        std::cout << message;
      }
      continue;
    }

    if (command.starts_with("flags")) {
      const auto& names = runner.modifiedFlags();
      std::cout << "Modified flags (" << names.size() << "):\n";
      for (const auto& name : names) {
        std::string value;
        if (gflags::GetCommandLineOption(name.c_str(), &value)) {
          std::cout << name << " = " << value << std::endl;
        }
      }
      continue;
    }

    if (sscanf(command.c_str(), "session %ms = %ms", &flag, &value) == 2) {
      std::cout << "Session '" << flag << "' set to '" << value << "'"
                << std::endl;
      runner.sessionConfig()[std::string(flag)] = std::string(value);
      free(flag);
      free(value);
      continue;
    }

    if (command.starts_with("savehistory")) {
      runner.saveHistory();
      continue;
    }

    if (command.starts_with("clearhistory")) {
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

} // namespace facebook::axiom

int main(int argc, char** argv) {
  gflags::SetUsageMessage(
      "Velox local SQL command line. "
      "Run 'velox_sql --help' for available options.\n");

  folly::Init init(&argc, &argv, false);

  try {
    facebook::axiom::VeloxRunner runner;
    runner.initialize();

    facebook::axiom::initCommands(runner);

    if (!FLAGS_query.empty()) {
      runner.run(FLAGS_query);
    } else if (!FLAGS_record.empty()) {
      facebook::axiom::recordQueries(runner);
    } else if (!FLAGS_check.empty()) {
      facebook::axiom::checkQueries(runner);
    } else {
      std::cout << "Velox SQL. Type statement and end with ;.\n"
                   "flag name = value; sets a gflag.\n"
                   "help; prints help text."
                << std::endl;
      readCommands(runner, "SQL> ", std::cin);
    }
  } catch (std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    exit(-1);
  }

  return 0;
}
