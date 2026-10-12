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

#include <stdint.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::axiom::runner::test {

struct TableSpec {
  std::string name;
  velox::RowTypePtr columns;
  int32_t rowsPerVector{10'000};
  int32_t numVectorsPerFile{5};
  int32_t numFiles{5};

  /// Function applied to generated RowVectors for the table before writing.
  /// May be used to insert non-random data on top of the random data from
  /// HiveConnectorTestBase::makeVectors.
  std::function<void(const velox::RowVectorPtr& vector)> customizeData;
};

/// Test helper class that manages a TestCase with a set of generated
/// tables and a HiveConnector that exposes the files and their
/// metadata. The lifetime of the test data is the test case consisting
/// of multiple Google unit test cases.
class LocalRunnerTestBase : public velox::exec::test::HiveConnectorTestBase {
 protected:
  static void SetUpTestCase();

  void SetUp() override;

  void TearDown() override;

  static void TearDownTestCase() {
    tempDirectory_.reset();
    HiveConnectorTestBase::TearDownTestCase();
  }

  /// Creates a QueryCtx for the specified query.
  std::shared_ptr<velox::core::QueryCtx> makeQueryCtx(
      const std::string& queryId);

  /// Creates test tables with randomly-generated data in DWRF format.
  /// Creates a temp directory on the first call. Subsequent calls are no-ops.
  void makeTables(const std::vector<TableSpec>& specs);

 private:
  void setupConnector();

  inline static std::shared_ptr<velox::common::testutil::TempDirectoryPath>
      tempDirectory_;
  inline static std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
};

} // namespace facebook::axiom::runner::test
