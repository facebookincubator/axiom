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
#pragma once

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/vector/ComplexVector.h"

namespace axiom::sql {

/// Timing statistics for query execution.
///
/// Captures wall clock time, user CPU time, and system CPU time for a query.
/// Used to provide performance metrics to users.
struct Timing {
  /// Wall clock time elapsed in microseconds.
  uint64_t micros{0};

  /// User CPU time in nanoseconds.
  /// Time spent executing user-mode code (application logic).
  uint64_t userNanos{0};

  /// System CPU time in nanoseconds.
  /// Time spent in kernel mode (system calls, I/O).
  uint64_t systemNanos{0};

  /// Formats timing information as a human-readable string.
  ///
  /// Example output: "1.5s / 800ms user / 150ms system (63%)"
  /// The percentage indicates CPU utilization relative to wall clock time.
  std::string toString() const {
    double pct = 0;
    if (micros > 0) {
      pct = 100.0 * (userNanos + systemNanos) / (micros * 1000);
    }

    std::stringstream out;
    out << facebook::velox::succinctNanos(micros * 1000) << " / "
        << facebook::velox::succinctNanos(userNanos) << " user / "
        << facebook::velox::succinctNanos(systemNanos) << " system (" << pct
        << "%)";
    return out.str();
  }
};

/// Abstract interface for formatting and printing query results.
///
/// This interface allows the CLI to support multiple output formats (aligned
/// tables, JSON, CSV, etc.) without coupling the query execution logic to the
/// presentation layer. Implementations handle both result formatting and
/// optional timing information display.
///
/// Implementations must be thread-safe for the printResults() call, as they
/// may be invoked from multiple contexts.
class OutputPrinter {
 public:
  virtual ~OutputPrinter() = default;

  /// Prints query results to stdout.
  ///
  /// Formats and outputs the query results according to the specific
  /// implementation (e.g., as aligned tables or JSON). Results are provided
  /// as batches of row vectors, which the printer iterates through.
  ///
  /// @param results Vector of RowVectorPtr batches containing the query output.
  ///                Each batch may contain multiple rows. The printer should
  ///                handle batching transparently to the user.
  /// @param maxRows Maximum number of rows to print. If the total row count
  ///                exceeds this limit, implementations should truncate output
  ///                and optionally indicate that rows were omitted.
  /// @return The total number of rows across all batches (not just printed).
  ///         This allows callers to report statistics even when output is
  ///         truncated.
  virtual int32_t printResults(
      const std::vector<facebook::velox::RowVectorPtr>& results,
      int32_t maxRows) = 0;
};

} // namespace axiom::sql
