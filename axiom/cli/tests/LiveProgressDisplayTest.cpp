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

#include "axiom/cli/LiveProgressDisplay.h"

#include <fmt/core.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cctype>
#include <sstream>
#include <string>
#include <vector>

#include "axiom/runner/QueryProgress.h"

using ::testing::ElementsAre;

namespace axiom::cli {

namespace runner = facebook::axiom::runner;

namespace {

// Fixed render width so the laid-out frame is deterministic.
constexpr int kWidth = 80;

// The per-stage grid header, shared by every grid-rendering case.
constexpr const char* kGridHeader =
    "     STAGES   ROWS  ROWS/s  BYTES  BYTES/s  QUEUED    RUN   DONE";

// Removes ANSI CSI escape sequences (ESC '[' ... final-letter) that update()
// emits for cursor moves and line erases, then splits the remaining text into
// lines (dropping the trailing empty piece after the last newline). The result
// is the visible text of the frame, one entry per drawn row.
std::vector<std::string> renderLines(const std::string& frame) {
  std::string text;
  for (size_t i = 0; i < frame.size();) {
    if (frame[i] == '\x1b' && i + 1 < frame.size() && frame[i + 1] == '[') {
      i += 2;
      while (i < frame.size() &&
             std::isalpha(static_cast<unsigned char>(frame[i])) == 0) {
        ++i;
      }
      if (i < frame.size()) {
        ++i; // the final letter
      }
    } else {
      text += frame[i++];
    }
  }

  std::vector<std::string> lines;
  size_t start = 0;
  while (start <= text.size()) {
    const size_t newline = text.find('\n', start);
    if (newline == std::string::npos) {
      if (start < text.size()) {
        lines.push_back(text.substr(start));
      }
      break;
    }
    lines.push_back(text.substr(start, newline - start));
    start = newline + 1;
  }
  return lines;
}

// Renders one frame for `info` at kWidth and returns its visible lines.
std::vector<std::string> render(
    const runner::QueryProgress& info,
    LiveProgressDisplay::Options options = {}) {
  std::ostringstream out;
  LiveProgressDisplay display(out, options);
  display.update(info, kWidth);
  return renderLines(out.str());
}

// Builds a single-stage running snapshot whose only stage mirrors the
// query-level stats.
runner::QueryProgress makeProgress(
    int32_t completedSplits,
    int32_t totalSplits,
    int64_t scanRows,
    int64_t scanBytes,
    int64_t wallMicros) {
  runner::QueryProgress info;
  info.queryId = "q1";
  info.wallTimeMicros = wallMicros;
  info.stats.state = runner::ExecutionState::kRunning;
  info.stats.totalSplits = totalSplits;
  info.stats.completedSplits = completedSplits;
  info.stats.scanRows = scanRows;
  info.stats.scanBytes = scanBytes;
  runner::StageProgress only;
  only.stats = info.stats;
  info.stages.push_back(only);
  return info;
}

TEST(LiveProgressDisplayTest, rendersGridFrame) {
  EXPECT_THAT(
      render(makeProgress(
          /*completedSplits=*/1,
          /*totalSplits=*/4,
          /*scanRows=*/100,
          /*scanBytes=*/0,
          /*wallMicros=*/2'000'000)),
      ElementsAre(
          "",
          "Query q1, RUNNING, 4 splits",
          "0:02 [100 rows, 0B] [50 rows/s, 0B/s] [======                ] 1/4",
          "",
          kGridHeader,
          "0.........R    100      50     0B       0B       0      0      1"));
}

TEST(LiveProgressDisplayTest, formatsSiUnits) {
  EXPECT_THAT(
      render(makeProgress(
          /*completedSplits=*/2,
          /*totalSplits=*/4,
          /*scanRows=*/1'500'000,
          /*scanBytes=*/6'560'000'000,
          /*wallMicros=*/2'000'000)),
      ElementsAre(
          "",
          "Query q1, RUNNING, 4 splits",
          "0:02 [1.5M rows, 6.11GB] [750K rows/s, 3.05GB/s] [===========           ] 2/4",
          "",
          kGridHeader,
          "0.........R   1.5M    750K  6.11G    3.05G       0      0      2"));
}

TEST(LiveProgressDisplayTest, debugAddsSplitAndCpuLines) {
  runner::QueryProgress info = makeProgress(1, 4, 100, 0, 2'000'000);
  info.stats.queuedSplits = 1;
  info.stats.runningSplits = 2;
  info.stats.cpuTimeMicros = 4'000'000;

  EXPECT_THAT(
      render(info, {.showSplitAndCpuDetail = true}),
      ElementsAre(
          "",
          "Query q1, RUNNING, 4 splits",
          "Splits:   1 queued, 2 running, 1 done",
          "CPU Time: 4.0s total, 25 rows/s, 0B/s",
          "0:02 [100 rows, 0B] [50 rows/s, 0B/s] [======>>>>>>>>>>>     ] 1/4",
          "",
          kGridHeader,
          "0.........R    100      50     0B       0B       0      0      1"));
}

TEST(LiveProgressDisplayTest, ordersStagesRootFirstAndIndentsByDepth) {
  // Stage 0 (root) consumes stage 1; the grid lists the root first and indents
  // the deeper stage by two spaces.
  runner::QueryProgress info;
  info.queryId = "q1";
  info.wallTimeMicros = 2'000'000;
  info.stats.state = runner::ExecutionState::kRunning;

  runner::StageProgress root;
  root.producers = {1};
  root.stats.state = runner::ExecutionState::kRunning;
  root.stats.scanRows = 200;
  runner::StageProgress leaf;
  leaf.stats.state = runner::ExecutionState::kFinished;
  leaf.stats.scanRows = 100;
  info.stages = {root, leaf};

  EXPECT_THAT(
      render(info),
      ElementsAre(
          "",
          "Query q1, RUNNING, 0 splits",
          "0:02 [0 rows, 0B] [0 rows/s, 0B/s] [                  <=> ]",
          "",
          kGridHeader,
          "0.........R    200     100     0B       0B       0      0      0",
          "  1.......F    100       0     0B       0B       0      0      0"));
}

TEST(LiveProgressDisplayTest, clearErasesTheDrawnBlock) {
  std::ostringstream out;
  LiveProgressDisplay display(out, /*options=*/{});
  display.update(makeProgress(1, 4, 100, 0, 2'000'000), kWidth);
  const size_t drawnRows = renderLines(out.str()).size();

  out.str("");
  display.clear();

  // clear() moves up over every drawn row and erases to end of screen.
  EXPECT_EQ(out.str(), fmt::format("\x1b[{}A\x1b[0J", drawnRows));
}

} // namespace
} // namespace axiom::cli
