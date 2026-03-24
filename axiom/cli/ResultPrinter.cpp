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
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"
#include "velox/functions/prestosql/types/TimeWithTimezoneType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/functions/prestosql/types/UuidType.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SimpleVector.h"

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
    int64_t maxRows) {
  const auto numRows = countResults(results);

  auto printFooter = [&]() {
    std::cout << "(" << numRows << " rows in " << results.size() << " batches)"
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
      if (alignLeft[i]) {
        std::cout << std::left;
        // Skip padding on the last column to avoid trailing whitespace.
        if (i < numColumns - 1) {
          std::cout << std::setw(widths[i]);
        }
      } else {
        std::cout << std::right;
        std::cout << std::setw(widths[i]);
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

    for (const auto& row : data) {
      printRow(row);
    }

    if (numPrinted < numRows) {
      std::cout << std::endl;
      std::cout << "..." << (numRows - numPrinted) << " more rows."
                << std::endl;
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
        auto& decodedColumn = decodedColumns[column];
        if (decodedColumn.isNullAt(row)) {
          rowData[column] = decodedColumn.toString(row);
        } else {
          const auto& columnType = type->childAt(column);
          if (isTimestampWithTimeZoneType(columnType)) {
            rowData[column] = TIMESTAMP_WITH_TIME_ZONE()->valueToString(
                decodedColumn.valueAt<int64_t>(row));
          } else if (columnType->isTime()) {
            char buf[16];
            rowData[column] =
                TIME()->valueToString(decodedColumn.valueAt<int64_t>(row), buf);
          } else if (isTimeWithTimeZone(columnType)) {
            char buf[32];
            rowData[column] = TIME_WITH_TIME_ZONE()->valueToString(
                decodedColumn.valueAt<int64_t>(row), buf);
          } else if (columnType->isIntervalDayTime()) {
            rowData[column] = INTERVAL_DAY_TIME()->valueToString(
                decodedColumn.valueAt<int64_t>(row));
          } else if (columnType->isIntervalYearMonth()) {
            rowData[column] = INTERVAL_YEAR_MONTH()->valueToString(
                decodedColumn.valueAt<int32_t>(row));
          } else if (isIPPrefixType(columnType)) {
            auto* rowVector = decodedColumn.base()->as<RowVector>();
            auto decodedRow = decodedColumn.index(row);
            auto ip =
                rowVector->childAt(0)->as<SimpleVector<int128_t>>()->valueAt(
                    decodedRow);
            auto prefixLength =
                rowVector->childAt(1)->as<SimpleVector<int8_t>>()->valueAt(
                    decodedRow);
            char buf[IPPrefixType::kMaxStringSize];
            rowData[column] = IPPREFIX()->valueToString(ip, prefixLength, buf);
          } else if (isIPAddressType(columnType)) {
            rowData[column] = IPADDRESS()->valueToString(
                decodedColumn.valueAt<int128_t>(row));
          } else if (isUuidType(columnType)) {
            char buf[UuidType::kStringSize];
            rowData[column] = UUID()->valueToString(
                decodedColumn.valueAt<int128_t>(row), buf);
          } else {
            rowData[column] = decodedColumn.toString(row);
          }
        }
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
