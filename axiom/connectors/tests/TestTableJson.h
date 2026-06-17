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
#include <string_view>

#include "velox/common/config/Config.h"

namespace facebook::axiom::connector {

class TestConnector;

/// Registers TestConnector tables (schema and statistics, no row data) from
/// JSON. Used to reproduce optimizer scenarios with controlled statistics, e.g.
/// a bad plan observed in production, without copying the underlying data.
///
/// Each JSON document describes one table:
///   {
///     "name": "orders",
///     "numRows": 1500000,
///     "columns": [
///       {"name": "o_orderkey", "type": "BIGINT", "numDistinct": 1500000,
///        "min": 1, "max": 6000000, "nullFraction": 0.0},
///       {"name": "o_orderstatus", "type": "VARCHAR", "numDistinct": 3}
///     ]
///   }
///
/// Per-column fields other than 'name' and 'type' are optional; a missing field
/// leaves that statistic unset, so the optimizer applies the same default it
/// would for a column whose statistic is absent in production.
///
/// The common entry point reads the file list from a connector config property:
///   TestTableJson::loadFromConfig(connector, config);
class TestTableJson {
 public:
  /// Connector config property naming the table JSON files: a comma-separated
  /// list of paths or shell-style globs (e.g. "*.json" or "a.json,b.json").
  /// Relative entries resolve against kConfigDir.
  static constexpr std::string_view kTables = "tables";

  /// Connector config property holding the directory that relative kTables
  /// entries resolve against. The CLI injects it from --etc_dir.
  static constexpr std::string_view kConfigDir = "catalog.dir";

  /// Parses 'json' describing a single table and registers it in 'connector'
  /// via addTable() and setStats(). Throws on malformed input.
  static void load(TestConnector& connector, std::string_view json);

  /// Reads the file at 'path' and load()s its contents.
  static void loadFile(TestConnector& connector, const std::string& path);

  /// Reads kTables from 'config', resolves and glob-expands each entry against
  /// kConfigDir, and loadFile()s every match. A no-op when kTables is unset.
  /// A literal (non-glob) entry that matches no file throws; a glob that
  /// matches nothing is silently empty.
  static void loadFromConfig(
      TestConnector& connector,
      const velox::config::ConfigBase& config);
};

} // namespace facebook::axiom::connector
