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

#include "velox/exec/tests/utils/TempDirectoryPath.h"

DECLARE_string(data_path);
DECLARE_bool(create_dataset);

namespace facebook::velox::optimizer::test {

class ParquetTpchTest {
 public:
  /// Writes TPC-H tables in Parquet format to a temp directory. Use --data_path
  /// GFlag to specify an alternative directory. That directory must exist.
  ///
  /// No-op if --data_path is specified, but --create_dataset is false.
  ///
  /// To create tables,
  ///   - registers Hive and TPC-H connectors,
  ///   - for each table, creates and runs Velox plan to read from TPC-H
  ///   connector and
  ///       write to Hive connector.
  /// Unregisters Hive and TPC-H connectors before returning.
  static void createTables();

 private:
  static std::shared_ptr<exec::test::TempDirectoryPath> tempDirectory_;
};

} // namespace facebook::velox::optimizer::test
