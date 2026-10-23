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

#include <functional>
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/container/HeterogeneousAccess.h>

#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "velox/connectors/Connector.h"
#include "velox/core/ExpressionEvaluator.h"
#include "velox/core/Expressions.h"
#include "velox/type/Subfield.h"

namespace facebook::axiom::optimizer::v2 {

/// Returns the parts of 'column' a query reads, empty for the whole column.
using SubfieldsOf =
    std::function<std::vector<velox::common::Subfield>(ColumnCP)>;

/// Connector access for reading a table by scan or index lookup.
struct TableAccessHandle {
  /// Offers 'filters' to the connector for reading 'outputColumns' of
  /// 'baseTable' and returns the handle it builds. Calls into the connector.
  /// Appends to 'rejected' the conjuncts the connector rejected, which the
  /// caller must apply itself.
  ///
  /// 'subfieldsOf' gives the parts of one column the query reads, so a complex
  /// column can be read in part. Returning none reads the column whole. It is
  /// asked about every column the read needs, including the ones only a filter
  /// references, which the caller cannot know in advance.
  static TableAccessHandle buildScan(
      const BaseTable& baseTable,
      const ColumnVector& outputColumns,
      const ExprVector& filters,
      const SubfieldsOf& subfieldsOf,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator,
      ExprVector& rejected);

  /// Builds index-lookup access using equality constraints on 'lookupKeys'.
  static TableAccessHandle buildIndexLookup(
      const BaseTable& baseTable,
      const ColumnVector& outputColumns,
      const ExprVector& filters,
      const ColumnVector& lookupKeys,
      const SubfieldsOf& subfieldsOf,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator,
      ExprVector& rejected);

  /// Table handle with the accepted filters pushed into the connector.
  velox::connector::ConnectorTableHandlePtr tableHandle;

  /// Column handle per column of the connector read schema: every column the
  /// `Scan` outputs, plus the ones only the connector's own filters read,
  /// which it reads without projecting.
  folly::F14FastMap<ColumnCP, velox::connector::ColumnHandlePtr> columnHandles;
};

} // namespace facebook::axiom::optimizer::v2
