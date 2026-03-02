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
#include "axiom/optimizer/tests/TpchQueries.h"
#include <boost/algorithm/string.hpp>
#include <fstream>
#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"

namespace facebook::axiom::optimizer::tests {

std::string readSqlFromFile(const std::string& filePath) {
  auto path = velox::test::getDataFilePath("axiom/optimizer/tests", filePath);
  std::ifstream inputFile(path, std::ifstream::binary);

  VELOX_CHECK(inputFile, "Failed to open SQL file: {}", path);

  auto begin = inputFile.tellg();
  inputFile.seekg(0, std::ios::end);
  auto end = inputFile.tellg();

  const auto fileSize = end - begin;
  VELOX_CHECK_GT(fileSize, 0, "SQL file is empty: {}", path);

  std::string sql;
  sql.resize(fileSize);

  inputFile.seekg(begin);
  inputFile.read(sql.data(), fileSize);
  inputFile.close();

  return sql;
}

std::string readTpchSql(int32_t query) {
  auto sql = readSqlFromFile(fmt::format("tpch/queries/q{}.sql", query));

  boost::trim_right(sql);
  if (!sql.empty() && sql.back() == ';') {
    sql.pop_back();
  }
  return sql;
}

} // namespace facebook::axiom::optimizer::tests
