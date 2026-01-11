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

#include "axiom/cli/AlignedTablePrinter.h"
#include <iomanip>
#include <iostream>

using namespace facebook::velox;

namespace axiom::sql {

namespace {

constexpr const char* kColumnSeparator = " | ";
constexpr const char* kSeparatorJunction = "-+-";

int64_t countResults(const std::vector<RowVectorPtr>& results) {
  int64_t numRows = 0;
  for (const auto& result : results) {
    numRows += result->size();
  }
  return numRows;
}

} // namespace

int32_t AlignedTablePrinter::printResults(
    const std::vector<RowVectorPtr>& results,
    int32_t maxRows) {
  const auto numRows = countResults(results);

  auto printFooter = [numRows, numBatches = results.size()]() {
    std::cout << "(" << numRows << " rows in " << numBatches << " batches)"
              << std::endl
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
  std::vector<int> widths(numColumns, 0);
  std::vector<bool> alignLeft(numColumns);

  for (auto i = 0; i < numColumns; ++i) {
    widths[i] = static_cast<int>(type->nameOf(i).size());
    alignLeft[i] = type->childAt(i)->isVarchar();
  }

  auto printTableSeparator = [&widths, numColumns]() {
    std::cout << std::setfill('-');
    for (auto i = 0; i < numColumns; ++i) {
      if (i > 0) {
        std::cout << kSeparatorJunction;
      }
      std::cout << std::setw(widths[i]) << "";
    }
    std::cout << std::endl << std::setfill(' ');
  };

  auto printTableRow =
      [&widths, &alignLeft, numColumns](const std::vector<std::string>& row) {
        for (auto i = 0; i < numColumns; ++i) {
          if (i > 0) {
            std::cout << kColumnSeparator;
          }
          std::cout << std::setw(widths[i]);
          std::cout << (alignLeft[i] ? std::left : std::right);
          std::cout << row[i];
        }
        std::cout << std::endl;
      };

  int32_t numPrinted = 0;

  auto printCompleteTable = [&]() {
    printTableSeparator();
    printTableRow(type->names());
    printTableSeparator();

    for (const auto& row : data) {
      printTableRow(row);
    }

    if (numPrinted < numRows) {
      std::cout << std::endl
                << "..." << (numRows - numPrinted) << " more rows."
                << std::endl;
    }

    printFooter();
  };

  // Iterate through result batches and accumulate data rows.
  for (const auto& result : results) {
    for (auto row = 0; row < result->size(); ++row) {
      data.emplace_back();

      auto& rowData = data.back();
      rowData.resize(numColumns);

      // Convert each column value to string and track maximum width.
      for (auto column = 0; column < numColumns; ++column) {
        rowData[column] = result->childAt(column)->toString(row);
        widths[column] =
            std::max(widths[column], static_cast<int>(rowData[column].size()));
      }

      ++numPrinted;
      if (numPrinted >= maxRows) {
        printCompleteTable();
        return static_cast<int32_t>(numRows);
      }
    }
  }

  printCompleteTable();

  return static_cast<int32_t>(numRows);
}

} // namespace axiom::sql
