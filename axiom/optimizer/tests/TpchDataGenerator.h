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

#include "velox/common/compression/Compression.h"
#include "velox/dwio/common/Options.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::axiom::optimizer::test {

class TpchDataGenerator {
 public:
  /// Generates a single TPC-H table in the specified format.
  ///
  /// @param table TPC-H table to generate.
  /// @param path Directory to write the table to. Must exist.
  /// @param scaleFactor TPC-H scale factor (e.g., 0.1, 1, 10).
  /// @param format File format to use (PARQUET or DWRF).
  /// @param compression Compression codec to use.
  /// @return Number of rows generated.
  static int64_t createTable(
      velox::tpch::Table table,
      std::string_view path,
      double scaleFactor,
      velox::dwio::common::FileFormat format,
      velox::common::CompressionKind compression);

  /// Writes all TPC-H tables to 'path' in the specified format.
  ///
  /// @param path Directory to write the tables to. Must exist.
  /// @param scaleFactor TPC-H scale factor (e.g., 0.1, 1, 10). Controls the
  /// size of the generated data. SF=1 produces ~6M lineitem rows.
  /// @param format File format to use (PARQUET or DWRF).
  static void createTables(
      std::string_view path,
      double scaleFactor = 0.1,
      velox::dwio::common::FileFormat format =
          velox::dwio::common::FileFormat::PARQUET,
      velox::common::CompressionKind compression =
          velox::common::CompressionKind::CompressionKind_NONE);

  static void registerTpchConnector(const std::string& id);

  static void unregisterTpchConnector(const std::string& id);
};

} // namespace facebook::axiom::optimizer::test
