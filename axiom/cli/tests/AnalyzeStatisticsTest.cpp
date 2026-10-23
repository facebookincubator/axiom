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
#include <folly/json.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"
#include "velox/exec/PlanNodeStats.h"

namespace axiom::sql {
namespace {
using namespace facebook::velox;
using namespace facebook::axiom::connector;

class AnalyzeStatisticsTest : public SqlQueryRunnerTestBase {
 protected:
  static constexpr auto kQuery =
      "SELECT count(*), sum(u.b) FROM t "
      "JOIN u ON t.a = u.a "
      "JOIN v ON t.b = v.a WHERE v.b = 999";

  struct Measurement {
    std::vector<int64_t> answer;
    int64_t buildRows{0};
    int64_t joinOutputRows{0};
    folly::dynamic operators{folly::dynamic::array};
    std::string plan;
    std::string optimized;
  };

  // Creates t with 10,000 rows, v with 1,000 rows and u with 'numRows' rows.
  // The query joins them on t.a = u.a and t.b = v.a.
  void load(int64_t numRows) {
    // Turn off join and filter sampling, so the only new information the
    // optimizer gets is the statistics that ANALYZE computes below.
    run("SET SESSION optimizer.sample_joins = false");
    run("SET SESSION optimizer.sample_filters = false");
    auto t = testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
    t->addData(
        makeRowVector(
            {"a", "b"},
            {makeFlatVector<int64_t>(10000, [](auto row) { return row; }),
             makeFlatVector<int64_t>(
                 10000, [](auto row) { return row % 1000; })}),
        false);
    auto v = testConnector_->addTable("v", ROW({"a", "b"}, BIGINT()));
    v->addData(
        makeRowVector(
            {"a", "b"},
            {makeFlatVector<int64_t>(1000, [](auto row) { return row; }),
             makeFlatVector<int64_t>(1000, [](auto row) { return row; })}),
        false);
    auto u = testConnector_->addTable("u", ROW({"a", "b"}, BIGINT()));
    for (int64_t start = 0; start < numRows; start += 4096) {
      const auto size = std::min<int64_t>(4096, numRows - start);
      u->addData(
          makeRowVector(
              {"a", "b"},
              {makeFlatVector<int64_t>(
                   size,
                   [start](auto row) {
                     const int64_t index = start + row;
                     // 90% of the rows use key 0, which the filter on v
                     // removes. The other keys are spread evenly and are the
                     // same on every run.
                     return index % 10 == 0 ? (index / 10) % 10000 : 0;
                   }),
               makeFlatVector<int64_t>(
                   size,
                   [start](auto row) { return 1 + (start + row) % 17; })}),
          false);
    }
  }

  static SqlQueryRunner::RunOptions options() {
    return {.numWorkers = 1, .numDrivers = 4};
  }

  Measurement measure() {
    auto opts = options();
    auto statement = runner_->parseSingle(kQuery, opts, nullptr);
    // Runs the query the normal way, through an entry point that also returns
    // each operator's final counters.
    auto execution = runner_->executeSelectOrInsert(*statement, opts);
    Measurement result;
    result.optimized =
        *runner_->run(std::string("EXPLAIN (TYPE OPTIMIZED) ") + kQuery, opts)
             .message;
    execution->drain([&](auto batch) {
      VELOX_CHECK_EQ(batch->size(), 1);
      for (const auto& child : batch->children()) {
        result.answer.push_back(child->variantAt(0).template value<int64_t>());
      }
    });
    result.plan = execution->printPlanWithStats(true);
    for (const auto& task : execution->stats()) {
      for (const auto& pipeline : task.pipelineStats) {
        for (const auto& op : pipeline.operatorStats) {
          if (op.operatorType != "HashBuild" &&
              op.operatorType != "HashProbe") {
            continue;
          }
          result.operators.push_back(
              folly::dynamic::object("node", op.planNodeId)(
                  "operator", op.operatorType)("input_rows", op.inputPositions)(
                  "output_rows", op.outputPositions)(
                  "peak_memory_bytes",
                  op.memoryStats.peakTotalMemoryReservation));
          if (op.operatorType == "HashBuild") {
            result.buildRows += op.inputPositions;
          } else {
            result.joinOutputRows += op.outputPositions;
          }
        }
      }
    }
    return result;
  }

  std::vector<uint64_t> timings(int warmups, int repeats) {
    std::vector<uint64_t> values;
    for (int i = -warmups; i < repeats; ++i) {
      auto opts = options();
      uint64_t micros = 0;
      opts.onComplete = [&](const auto& info) { micros = info.timing.execute; };
      runner_->run(kQuery, opts);
      if (i >= 0) {
        values.push_back(micros);
      }
    }
    return values;
  }

  static uint64_t median(std::vector<uint64_t> values) {
    std::sort(values.begin(), values.end());
    return values.at(values.size() / 2);
  }

  void analyze() {
    testConnector_->metadata()->publishAnalyzedStatistics = true;
    for (const auto* name : {"t", "v", "u"}) {
      runner_->run(fmt::format("ANALYZE {}", name), options());
    }
  }

  folly::dynamic statisticsSnapshot() {
    folly::dynamic snapshot = folly::dynamic::object;
    for (const auto* name : {"u", "t", "v"}) {
      const auto table =
          testConnector_->metadata()->findTable({"default", name});
      folly::dynamic columns = folly::dynamic::object;
      for (const auto* column : table->allColumns()) {
        if (column->hidden()) {
          continue;
        }
        const auto* stats = column->stats();
        columns[column->name()] = stats && stats->numDistinct
            ? folly::dynamic(*stats->numDistinct)
            : folly::dynamic(nullptr);
      }
      snapshot[name] =
          folly::dynamic::object("rows", *table->numRows())("ndv", columns);
    }
    return snapshot;
  }

  void experiment(int64_t rows, int repeats, const std::string& directory) {
    load(rows);
    auto old = testConnector_->metadata()->findTable({"default", "u"});
    ASSERT_EQ(old->numRows(), rows);
    EXPECT_TRUE(
        !old->findColumn("a")->stats() ||
        !old->findColumn("a")->stats()->numDistinct);
    const auto beforeStatistics = statisticsSnapshot();
    const auto before = measure();
    // Only the benchmark passes repeats, so only it times the query.
    const auto beforeTimes =
        repeats > 0 ? timings(2, repeats) : std::vector<uint64_t>{};
    uint64_t analyzeMicros = 0;
    const auto start = std::chrono::steady_clock::now();
    analyze();
    analyzeMicros = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - start)
                        .count();
    auto updated = testConnector_->metadata()->findTable({"default", "u"});
    ASSERT_NE(old.get(), updated.get());
    ASSERT_EQ(updated->numRows(), old->numRows());
    EXPECT_EQ(
        std::static_pointer_cast<const TestTable>(old)->data().front().get(),
        std::static_pointer_cast<const TestTable>(updated)
            ->data()
            .front()
            .get());
    EXPECT_TRUE(
        !old->findColumn("a")->stats() ||
        !old->findColumn("a")->stats()->numDistinct);
    ASSERT_TRUE(updated->findColumn("a")->stats()->numDistinct);
    const auto after = measure();
    const auto afterTimes =
        repeats > 0 ? timings(2, repeats) : std::vector<uint64_t>{};
    EXPECT_EQ(before.answer, after.answer);
    // Work out the expected answer from how load() made the data: the matching
    // rows are 9990, 19990, and so on.
    std::vector<int64_t> expected{0, 0};
    for (int64_t index = 9990; index < rows; index += 10000) {
      ++expected[0];
      expected[1] += 1 + index % 17;
    }
    EXPECT_EQ(before.answer, expected);
    EXPECT_LT(after.buildRows, before.buildRows);
    EXPECT_LT(after.joinOutputRows, before.joinOutputRows);
    if (!directory.empty()) {
      std::cout << "BEFORE\n" << before.plan << "AFTER\n" << after.plan;
      std::cout << "build rows " << before.buildRows << " -> "
                << after.buildRows << "; join output rows "
                << before.joinOutputRows << " -> " << after.joinOutputRows
                << "; median execution us " << median(beforeTimes) << " -> "
                << median(afterTimes) << "; analyze us " << analyzeMicros
                << std::endl;
      std::filesystem::create_directories(directory);
      std::ofstream(directory + "/before-plan.txt") << before.plan;
      std::ofstream(directory + "/after-plan.txt") << after.plan;
      std::ofstream(directory + "/before-optimized.txt") << before.optimized;
      std::ofstream(directory + "/after-optimized.txt") << after.optimized;
      folly::dynamic baseline = folly::dynamic::array;
      folly::dynamic analyzed = folly::dynamic::array;
      for (auto value : beforeTimes) {
        baseline.push_back(value);
      }
      for (auto value : afterTimes) {
        analyzed.push_back(value);
      }
      folly::dynamic evidence = folly::dynamic::object("query", kQuery)(
          "u_rows", rows)("t_rows", 10000)("v_rows", 1000)("workers", 1)(
          "drivers", 4)("warmups_per_phase", 2)(
          "before_execution_us", baseline)("after_execution_us", analyzed)(
          "before_median_us", median(beforeTimes))(
          "after_median_us", median(afterTimes))(
          "analyze_wall_us", analyzeMicros)(
          "before_operators", before.operators)(
          "after_operators", after.operators);
      folly::dynamic answer = folly::dynamic::array;
      for (auto value : after.answer) {
        answer.push_back(value);
      }
      evidence["answer"] = answer;
      evidence["before_statistics"] = beforeStatistics;
      evidence["after_statistics"] = statisticsSnapshot();
      std::ofstream(directory + "/results.json")
          << folly::toPrettyJson(evidence);
    }
  }
};

TEST_F(AnalyzeStatisticsTest, statisticsChangeJoinWork) {
  experiment(100000, 0, "");
}

// A benchmark that the normal test run skips. Run it with:
// AXIOM_ANALYZE_EVIDENCE_DIR=/tmp/analyze-benefit axiom_cli_test
//   --gtest_filter=AnalyzeStatisticsTest.DISABLED_benchmark
//   --gtest_also_run_disabled_tests
TEST_F(AnalyzeStatisticsTest, DISABLED_benchmark) {
  const auto* dir = std::getenv("AXIOM_ANALYZE_EVIDENCE_DIR");
  ASSERT_NE(dir, nullptr);
  experiment(2000000, 9, dir);
}
} // namespace
} // namespace axiom::sql
