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

#include <folly/container/F14Map.h>

#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/QueryGraph.h"
#include "velox/connectors/Connector.h"
#include "velox/core/Expressions.h"

namespace facebook::axiom::optimizer::v2 {

/// Returns the parts of 'column' a query reads, empty for the whole column.
using SubfieldsOf =
    std::function<std::vector<velox::common::Subfield>(ColumnCP)>;

/// Connector access for a scanned leaf table: the result of negotiating filter
/// pushdown with the connector. Made once, by the pushdown pass, and pointed
/// at by the `Scan` from there on.
struct ScanHandle {
  /// Offers 'filters' to the connector for reading 'outputColumns' of
  /// 'baseTable' and returns the handle it builds. Calls into the connector.
  /// Appends to 'rejected' the conjuncts the connector rejected, which the
  /// caller must apply itself.
  ///
  /// 'subfieldsOf' gives the parts of one column the query reads, so a complex
  /// column can be read in part. Returning none reads the column whole. It is
  /// asked about every column the read needs, including the ones only a filter
  /// references, which the caller cannot know in advance.
  static ScanHandle build(
      const BaseTable& baseTable,
      const ColumnVector& outputColumns,
      const ExprVector& filters,
      const SubfieldsOf& subfieldsOf,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator,
      ExprVector& rejected);

  /// Table handle with the accepted filters pushed into the connector.
  velox::connector::ConnectorTableHandlePtr tableHandle;

  /// Estimated logical width of the connector read schema, including columns
  /// used only by pushed filters.
  float inputRowBytes{0};

  /// Column handle per column emitted by the `Scan`, including columns needed
  /// by filters the connector rejected. Columns used only by accepted filters
  /// contribute to `inputRowBytes` but are not emitted.
  folly::F14FastMap<ColumnCP, velox::connector::ColumnHandlePtr> columnHandles;
};

} // namespace facebook::axiom::optimizer::v2
