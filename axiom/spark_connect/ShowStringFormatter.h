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

#include <string>
#include <vector>

#include "velox/vector/ComplexVector.h"

namespace axiom::spark_connect {

/// Formats rows as the text Spark's DataFrame.show() prints, as Spark's
/// Dataset.showString does.
///
///   SparkToAxiom translates a ShowString relation as its input's first
///   numRows + 1 rows, each column cast to VARCHAR. The server runs that and
///   returns ShowStringFormatter::format(...) as the one cell of the result.
class ShowStringFormatter {
 public:
  /// Returns the text of the table.
  ///
  /// @param type The columns: VARCHAR, or VARBINARY, which prints as hex bytes.
  /// @param rows Up to numRows + 1 rows; one more row than shown adds the
  /// "only showing top" footer.
  /// @param numRows Number of rows to show. A negative value shows none.
  /// @param truncate If more than 0, cells longer than this are cut and all
  /// cells align right. Otherwise cells print in full and align left.
  /// @param vertical If true, prints each row as a block of "name | value"
  /// lines. Otherwise prints a table with a header row and a line per row.
  static std::string format(
      const facebook::velox::RowTypePtr& type,
      const std::vector<facebook::velox::RowVectorPtr>& rows,
      int32_t numRows,
      int32_t truncate,
      bool vertical);
};

} // namespace axiom::spark_connect
