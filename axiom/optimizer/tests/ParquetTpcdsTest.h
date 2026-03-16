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

DECLARE_string(tpcds_data_path);
DECLARE_bool(create_tpcds_dataset);
DECLARE_double(tpcds_scale);

namespace facebook::axiom::optimizer::test {

class ParquetTpcdsTest {
 public:
  /// Writes TPC-DS tables in Parquet format to a temp directory. Use
  /// --tpcds_data_path GFlag to specify an alternative directory. That
  /// directory must exist.
  ///
  /// No-op if --tpcds_data_path is specified, but --create_tpcds_dataset is
  /// false.
  ///
  /// To create tables,
  ///   - registers Hive and TPC-DS connectors,
  ///   - for each table, creates and runs Velox plan to read from TPC-DS
  ///   connector and
  ///       write to Hive connector.
  /// Unregisters Hive and TPC-DS connectors before returning.
  static void createTables(std::string_view path);

  static void registerTpcdsConnector(const std::string& id);

  static void unregisterTpcdsConnector(const std::string& id);
};

} // namespace facebook::axiom::optimizer::test
