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

#include <gflags/gflags.h>

DECLARE_string(data_path);
DECLARE_bool(create_dataset);

namespace facebook::axiom::optimizer::test {

class ParquetTpchTest {
 public:
  /// Writes TPC-H tables in Parquet format to 'path'.
  ///
  /// @param path Directory to write the tables to. Must exist.
  /// @param scaleFactor TPC-H scale factor (e.g., 0.1, 1, 10). Controls the
  /// size of the generated data. SF=1 produces ~6M lineitem rows.
  static void createTables(std::string_view path, double scaleFactor = 0.1);

  static void registerTpchConnector(const std::string& id);

  static void unregisterTpchConnector(const std::string& id);
};

} // namespace facebook::axiom::optimizer::test
