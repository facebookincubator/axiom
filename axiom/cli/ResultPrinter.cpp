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

#include "axiom/cli/ResultPrinter.h"
#include <iomanip>
#include <iostream>
#include "velox/functions/prestosql/types/PrestoTypes.h"
#include "velox/vector/DecodedVector.h"

using namespace facebook::velox;

namespace axiom::cli {

namespace {

int64_t countResults(const std::vector<RowVectorPtr>& results) {
  int64_t numRows = 0;
  for (const auto& result : results) {
    numRows += result->size();
  }
  return numRows;
}

} // namespace

int32_t printResults(
    const std::vector<RowVectorPtr>& results,
    int64_t maxRows,
    std::ostream& output) {
  const auto numRows = countResults(results);

  auto printFooter = [&]() {
    output << "(" << numRows << " rows in " << results.size() << " batches)"
           << std::endl
           << std::endl;
  };

  if (numRows == 0) {
    printFooter();
    return 0;
  }

  const auto type = results.front()->rowType();
  const auto numColumns = type->size();

  std::vector<std::vector<std::string>> data;
  std::vector<size_t> widths(numColumns, 0);
  std::vector<bool> alignLeft(numColumns);

  for (auto i = 0; i < numColumns; ++i) {
    widths[i] = type->nameOf(i).size();
    alignLeft[i] = type->childAt(i)->isVarchar();
  }

  auto printSeparator = [&]() {
    output << std::setfill('-');
    for (auto i = 0; i < numColumns; ++i) {
      if (i > 0) {
        output << "-+-";
      }
      output << std::setw(widths[i]) << "";
    }
    output << std::endl;
    output << std::setfill(' ');
  };

  auto printRow = [&](const auto& row) {
    for (auto i = 0; i < numColumns; ++i) {
      if (i > 0) {
        output << " | ";
      }
      if (alignLeft[i]) {
        output << std::left;
        // Skip padding on the last column to avoid trailing whitespace.
        if (i < numColumns - 1) {
          output << std::setw(widths[i]);
        }
      } else {
        output << std::right;
        output << std::setw(widths[i]);
      }
      output << row[i];
    }
    output << std::endl;
  };

  int32_t numPrinted = 0;

  auto doPrint = [&]() {
    printSeparator();
    printRow(type->names());
    printSeparator();

    for (const auto& row : data) {
      printRow(row);
    }

    if (numPrinted < numRows) {
      output << std::endl;
      output << "..." << (numRows - numPrinted) << " more rows." << std::endl;
    }

    printFooter();
  };

  std::vector<DecodedVector> decodedColumns(numColumns);
  for (const auto& result : results) {
    for (auto column = 0; column < numColumns; ++column) {
      decodedColumns[column].decode(*result->childAt(column));
    }

    for (auto row = 0; row < result->size(); ++row) {
      data.emplace_back();

      auto& rowData = data.back();
      rowData.resize(numColumns);
      for (auto column = 0; column < numColumns; ++column) {
        rowData[column] = PrestoTypes::valueToString(
            decodedColumns[column], row, type->childAt(column));
        widths[column] = std::max(widths[column], rowData[column].size());
      }

      ++numPrinted;
      if (numPrinted >= maxRows) {
        doPrint();
        return numRows;
      }
    }
  }

  doPrint();

  return numRows;
}

} // namespace axiom::cli
