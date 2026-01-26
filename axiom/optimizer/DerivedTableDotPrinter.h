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

#include <ostream>

namespace facebook::axiom::optimizer {

struct DerivedTable;

/// Generates a DOT language representation of a DerivedTable query graph for
/// visualization with Graphviz.
///
/// Usage:
///   std::ofstream file("query_graph.dot");
///   DerivedTableDotPrinter::print(dt, file);
///
/// Then render with: dot -Tsvg query_graph.dot -o query_graph.svg
class DerivedTableDotPrinter {
 public:
  static void print(const DerivedTable& root, std::ostream& out);
};

} // namespace facebook::axiom::optimizer
