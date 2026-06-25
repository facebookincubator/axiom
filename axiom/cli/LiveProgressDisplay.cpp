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
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>

#include "axiom/runner/QueryProgress.h"
#include "velox/common/base/Exceptions.h"

namespace axiom::cli {

namespace runner = facebook::axiom::runner;

namespace {

// Drops trailing zeros and a now-trailing decimal point, e.g. 1.50 -> "1.5",
// 2.00 -> "2".
std::string trimZeros(std::string text) {
  if (text.find('.') == std::string::npos) {
    return text;
  }
  while (!text.empty() && text.back() == '0') {
    text.pop_back();
  }
  if (!text.empty() && text.back() == '.') {
    text.pop_back();
  }
  return text;
}

// Formats with 3 significant digits: two decimals below 10, one below 100, none
// above.
std::string formatDecimal(double value) {
  if (value < 10) {
    return trimZeros(fmt::format("{:.2f}", value));
  }
  if (value < 100) {
    return trimZeros(fmt::format("{:.1f}", value));
  }
  return fmt::format("{:.0f}", value);
}

// Formats a count with decimal SI-style suffixes (K/M/B/T/Q) on a 1000
// boundary.
std::string formatCount(int64_t count) {
  double fractional = static_cast<double>(count);
  const char* unit = "";
  if (fractional >= 1'000) {
    fractional /= 1'000;
    unit = "K";
  }
  if (fractional >= 1'000) {
    fractional /= 1'000;
    unit = "M";
  }
  if (fractional >= 1'000) {
    fractional /= 1'000;
    unit = "B";
  }
  if (fractional >= 1'000) {
    fractional /= 1'000;
    unit = "T";
  }
  if (fractional >= 1'000) {
    fractional /= 1'000;
    unit = "Q";
  }
  return formatDecimal(fractional) + unit;
}

// Formats a byte size with binary suffixes (K/M/G/T/P) on a 1024 boundary. When
// `longForm`, suffixes carry a trailing 'B' (e.g. "1.97GB").
std::string formatDataSize(int64_t bytes, bool longForm) {
  double fractional = static_cast<double>(bytes);
  std::string unit;
  bool scaled{false};
  for (const char* unitSuffix : {"K", "M", "G", "T", "P"}) {
    if (fractional >= 1'024) {
      fractional /= 1'024;
      unit = unitSuffix;
      scaled = true;
    } else {
      break;
    }
  }
  if (!scaled) {
    unit = "B";
  } else if (longForm) {
    unit += "B";
  }
  return formatDecimal(fractional) + unit;
}

// Formats a per-second count rate.
std::string formatCountRate(double count, double seconds, bool longForm) {
  double rate = count / seconds;
  if (std::isnan(rate) || std::isinf(rate)) {
    rate = 0;
  }
  std::string formatted = formatCount(static_cast<int64_t>(rate));
  if (longForm) {
    formatted += "/s";
  }
  return formatted;
}

// Formats a per-second byte rate.
std::string formatDataRate(int64_t bytes, double seconds, bool longForm) {
  double rate = static_cast<double>(bytes) / seconds;
  if (std::isnan(rate) || std::isinf(rate)) {
    rate = 0;
  }
  std::string formatted = formatDataSize(static_cast<int64_t>(rate), false);
  if (longForm) {
    if (formatted.empty() || formatted.back() != 'B') {
      formatted += "B";
    }
    formatted += "/s";
  }
  return formatted;
}

// Formats a duration: sub-second as "<n>ms", otherwise "<min>:<ss>". Switches
// on the raw microseconds so e.g. 600ms renders as "600ms", not "0:01".
std::string formatTime(int64_t micros) {
  if (micros < 1'000'000) {
    return fmt::format("{}ms", llround(static_cast<double>(micros) / 1'000));
  }
  const int64_t totalSeconds = micros / 1'000'000;
  return fmt::format("{}:{:02d}", totalSeconds / 60, totalSeconds % 60);
}

// Ceiling of integer division. Takes int64_t so callers can multiply split
// counts by the bar width without overflowing int.
int64_t ceilDiv(int64_t dividend, int64_t divisor) {
  return ((dividend + divisor) - 1) / divisor;
}

// Builds a determinate progress bar: split the width across complete ("="),
// running (">"), and pending (" ") segments.
std::string formatProgressBar(int width, int complete, int running, int total) {
  if (total == 0) {
    return std::string(width, ' ');
  }
  const int pending = std::max(0, total - complete - running);

  int completeLength = static_cast<int>(std::min<int64_t>(
      width, ceilDiv(static_cast<int64_t>(complete) * width, total)));
  int pendingLength = static_cast<int>(std::min<int64_t>(
      width, ceilDiv(static_cast<int64_t>(pending) * width, total)));

  const int minRunningLength = (running > 0) ? 1 : 0;
  int runningLength = std::max(
      static_cast<int>(std::min<int64_t>(
          width, ceilDiv(static_cast<int64_t>(running) * width, total))),
      minRunningLength);

  if (((completeLength + runningLength + pendingLength) != width) &&
      (pending > 0)) {
    pendingLength = std::max(0, width - completeLength - runningLength);
  }
  if ((completeLength + runningLength + pendingLength) != width) {
    runningLength =
        std::max(minRunningLength, width - completeLength - pendingLength);
  }
  if (((completeLength + runningLength + pendingLength) > width) &&
      (complete > 0)) {
    completeLength = std::max(0, width - runningLength - pendingLength);
  }

  return std::string(completeLength, '=') + std::string(runningLength, '>') +
      std::string(pendingLength, ' ');
}

// Builds an indeterminate progress bar: a "<=>" marker that bounces back and
// forth as `tick` advances.
std::string formatProgressBar(int width, int tick) {
  constexpr int kMarkerWidth = 3; // "<=>"
  const int range = width - kMarkerWidth;
  if (range <= 0) {
    return std::string(std::max(0, width), ' ');
  }
  int lower = tick % range;
  if (((tick / range) % 2) != 0) {
    lower = range - lower;
  }
  return std::string(lower, ' ') + "<=>" +
      std::string(width - (lower + kMarkerWidth), ' ');
}

std::string pluralize(std::string_view word, int count) {
  return count != 1 ? std::string(word) + "s" : std::string(word);
}

// Total source rows processed: rows scanned from storage plus rows received
// over exchanges. A stage draws from a single source, so one term is zero.
int64_t sourceRows(const runner::ExecutionStats& stats) {
  return stats.scanRows + stats.shuffleRows;
}

// Total source bytes processed; see sourceRows.
int64_t sourceBytes(const runner::ExecutionStats& stats) {
  return stats.scanBytes + stats.shuffleBytes;
}

// Single-character state glyph for the compact stage grid.
char stateChar(runner::ExecutionState state) {
  switch (state) {
    case runner::ExecutionState::kRunning:
      return 'R';
    case runner::ExecutionState::kFinished:
      return 'F';
    case runner::ExecutionState::kPlanned:
      return 'P';
  }
  VELOX_UNREACHABLE();
}

// Returns each stage's depth from the root (0 = root) by following producer
// edges, so the renderer can indent the stage tree. Roots are stages no other
// stage produces for. The stage graph is a tree -- each stage feeds exactly one
// consumer -- so a producer's depth is one past its consumer's. Out-of-range
// producer indexes (a transient mid-poll gap) and already-visited stages (which
// only a malformed cyclic snapshot would yield) are skipped.
std::vector<int32_t> stageDepths(
    const std::vector<runner::StageProgress>& stages) {
  const auto count = static_cast<int32_t>(stages.size());
  const auto valid = [&](int32_t index) { return index >= 0 && index < count; };

  std::vector<bool> isProducer(stages.size(), false);
  for (const auto& stage : stages) {
    for (const int32_t producer : stage.producers) {
      if (valid(producer)) {
        isProducer.at(producer) = true;
      }
    }
  }

  std::vector<int32_t> depths(stages.size(), 0);
  std::vector<bool> visited(stages.size(), false);
  std::vector<int32_t> queue;
  for (int32_t i = 0; i < count; ++i) {
    if (!isProducer.at(i)) {
      visited.at(i) = true;
      queue.push_back(i);
    }
  }
  for (size_t head = 0; head < queue.size(); ++head) {
    const int32_t consumer = queue.at(head);
    for (const int32_t producer : stages.at(consumer).producers) {
      if (valid(producer) && !visited.at(producer)) {
        visited.at(producer) = true;
        depths.at(producer) = depths.at(consumer) + 1;
        queue.push_back(producer);
      }
    }
  }
  return depths;
}

} // namespace

LiveProgressDisplay::LiveProgressDisplay(std::ostream& out, Options options)
    : out_{out}, options_{options} {}

LiveProgressDisplay::~LiveProgressDisplay() {
  // A destructor is implicitly noexcept; swallow any terminal-write failure
  // from clear() so it cannot escape during stack unwinding and call
  // std::terminate.
  try {
    clear();
  } catch (...) {
  }
}

namespace {

// Appends one rendered line to the current frame; the caller's emitter erases
// the line first and counts the rows drawn.
using LineEmitter = std::function<void(std::string_view)>;

// Renders the one-line fallback used when the terminal is too narrow for the
// grid.
void renderNarrowSummary(
    const LineEmitter& emit,
    const runner::QueryProgress& info,
    const std::string& wall) {
  if (info.stats.totalSplits > 0) {
    emit(
        fmt::format(
            "{} {}/{}",
            wall,
            formatCount(info.stats.completedSplits),
            formatCount(info.stats.totalSplits)));
  } else {
    emit(wall);
  }
}

// Renders the query header, the optional split/CPU detail lines, and the
// progress bar.
void renderSummary(
    const LineEmitter& emit,
    const runner::QueryProgress& info,
    int width,
    const std::string& wall,
    double seconds,
    bool showSplitAndCpuDetail) {
  emit("");
  emit(
      fmt::format(
          "Query {}, {}, {} {}",
          info.queryId,
          runner::ExecutionStateName::toName(info.stats.state),
          info.stats.totalSplits,
          pluralize("split", info.stats.totalSplits)));

  if (showSplitAndCpuDetail) {
    emit(
        fmt::format(
            "Splits:   {} queued, {} running, {} done",
            info.stats.queuedSplits,
            info.stats.runningSplits,
            info.stats.completedSplits));
    const double cpuSeconds =
        static_cast<double>(info.stats.cpuTimeMicros) / 1'000'000;
    emit(
        fmt::format(
            "CPU Time: {:.1f}s total, {} rows/s, {}",
            cpuSeconds,
            formatCountRate(sourceRows(info.stats), cpuSeconds, false),
            formatDataRate(sourceBytes(info.stats), cpuSeconds, true)));
  }

  // Progress bar width: terminal width (capped at 100) minus the summary's
  // fixed non-bar characters (~75), plus a 17-char base, then shortened by any
  // time prefix wider than the usual 5 ("MM:SS").
  int progressWidth = (std::min(width, 100) - 75) + 17;
  if (wall.size() > 5) {
    progressWidth -= static_cast<int>(wall.size()) - 5;
  }
  progressWidth = std::max(1, progressWidth);

  if (info.stats.totalSplits > 0) {
    const std::string bar = formatProgressBar(
        progressWidth,
        info.stats.completedSplits,
        std::max(0, info.stats.runningSplits),
        info.stats.totalSplits);
    emit(
        fmt::format(
            "{} [{} rows, {}] [{} rows/s, {}] [{}] {}/{}",
            wall,
            formatCount(sourceRows(info.stats)),
            formatDataSize(sourceBytes(info.stats), true),
            formatCountRate(sourceRows(info.stats), seconds, false),
            formatDataRate(sourceBytes(info.stats), seconds, true),
            bar,
            formatCount(info.stats.completedSplits),
            formatCount(info.stats.totalSplits)));
  } else {
    // No splits scheduled yet: show an indeterminate marker driven by elapsed
    // time at decisecond resolution, so it keeps moving between sub-second
    // polls.
    const std::string bar =
        formatProgressBar(progressWidth, static_cast<int>(seconds * 10));
    emit(
        fmt::format(
            "{} [{} rows, {}] [{} rows/s, {}] [{}]",
            wall,
            formatCount(sourceRows(info.stats)),
            formatDataSize(sourceBytes(info.stats), true),
            formatCountRate(sourceRows(info.stats), seconds, false),
            formatDataRate(sourceBytes(info.stats), seconds, true),
            bar));
  }
}

// Renders the per-stage grid: a header row and one row per stage, ordered
// shallowest (root) first and indented by depth.
void renderStageGrid(
    const LineEmitter& emit,
    const runner::QueryProgress& info,
    double seconds) {
  if (info.stages.empty()) {
    return;
  }
  emit("");
  emit(
      fmt::format(
          "{:>10}{:>1}  {:>5}  {:>6}  {:>5}  {:>7}  {:>6}  {:>5}  {:>5}",
          "STAGE",
          "S",
          "ROWS",
          "ROWS/s",
          "BYTES",
          "BYTES/s",
          "QUEUED",
          "RUN",
          "DONE"));

  // The display label numbers stages in root-first order; the model's index is
  // the raw fragment index.
  const auto depths = stageDepths(info.stages);
  std::vector<size_t> order(info.stages.size());
  std::iota(order.begin(), order.end(), 0);
  std::stable_sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs) {
    return depths[lhs] < depths[rhs];
  });

  int32_t displayLabel{0};
  for (const size_t idx : order) {
    const auto& stage = info.stages[idx];

    // Stage label: depth-indent + sequential label, dot-padded to
    // kStageLabelWidth columns. Deep trees whose indent + label exceed the
    // column are truncated so the fixed-width grid stays aligned.
    constexpr size_t kStageLabelWidth = 10;
    std::string name =
        std::string(static_cast<size_t>(std::max(0, depths[idx])) * 2, ' ') +
        std::to_string(displayLabel);
    if (name.size() < kStageLabelWidth) {
      name += std::string(kStageLabelWidth - name.size(), '.');
    } else if (name.size() > kStageLabelWidth) {
      name.resize(kStageLabelWidth);
    }
    ++displayLabel;

    // Finished stages report a zero rate.
    const bool done = stage.stats.state == runner::ExecutionState::kFinished;
    const std::string rowsPerSecond = done
        ? formatCountRate(0, 0, false)
        : formatCountRate(sourceRows(stage.stats), seconds, false);
    const std::string bytesPerSecond = done
        ? formatDataRate(0, 0, false)
        : formatDataRate(sourceBytes(stage.stats), seconds, false);

    emit(
        fmt::format(
            "{:>10}{:>1}  {:>5}  {:>6}  {:>5}  {:>7}  {:>6}  {:>5}  {:>5}",
            name,
            std::string(1, stateChar(stage.stats.state)),
            formatCount(sourceRows(stage.stats)),
            rowsPerSecond,
            formatDataSize(sourceBytes(stage.stats), false),
            bytesPerSecond,
            stage.stats.queuedSplits,
            stage.stats.runningSplits,
            stage.stats.completedSplits));
  }
}

} // namespace

void LiveProgressDisplay::update(const runner::QueryProgress& info, int width) {
  auto lines = lines_.wlock();

  // Build the whole frame in one buffer and write it once, so a slow or SSH
  // terminal never renders a half-drawn frame and we issue a single flush.
  std::string frame;
  if (*lines > 0) {
    // Move to the top of the previous block (CSI nA) and erase from there to
    // the end of the screen (CSI 0J). Clearing the whole block keeps surplus
    // rows from a taller previous frame from lingering below the new content.
    frame += fmt::format("\x1b[{}A\x1b[0J", *lines);
  }

  int drawn{0};
  // Erases the line (CSI 2K) before each rewrite and counts the rows drawn.
  const LineEmitter emit = [&](std::string_view line) {
    frame += "\x1b[2K";
    frame += line;
    frame += '\n';
    ++drawn;
  };

  const double seconds = static_cast<double>(info.wallTimeMicros) / 1'000'000;
  const std::string wall = formatTime(info.wallTimeMicros);

  // Narrow terminals cannot fit the grid; fall back to a one-line summary.
  if (width < 75) {
    renderNarrowSummary(emit, info, wall);
  } else {
    renderSummary(
        emit, info, width, wall, seconds, options_.showSplitAndCpuDetail);
    renderStageGrid(emit, info, seconds);
  }

  out_ << frame;
  out_.flush();
  *lines = drawn;
}

void LiveProgressDisplay::clear() {
  auto lines = lines_.wlock();
  if (*lines == 0) {
    return;
  }
  // Move back to the top of the block and erase everything below the cursor
  // (CSI 0J), clearing the status display before results are printed.
  out_ << fmt::format("\x1b[{}A\x1b[0J", *lines);
  out_.flush();
  *lines = 0;
}

} // namespace axiom::cli
