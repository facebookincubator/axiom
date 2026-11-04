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
#include <string>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ast/AstNodesAll.h"

namespace axiom::sql::presto {

/// Plans a call of the vector_search table function as a vector search join
/// (see logical_plan::VectorSearchNode): the queries on the left, the corpus
/// on the right.
///
///   SELECT q.name, p.id, similarity
///   FROM TABLE(vector_search(
///       queries => TABLE(SELECT name, embedding, ds FROM new_products) AS q,
///       corpus => TABLE(SELECT * FROM products WHERE ds = '2026-09-30') AS p,
///       embeddings => DESCRIPTOR(embedding),
///       metric => 'cosine',
///       k => 10,
///       score => DESCRIPTOR(similarity),
///       hints => MAP(ARRAY['num_probes'], ARRAY['50'])))
class VectorSearchPlanner {
 public:
  /// Returns the plan of a table argument.
  using TablePlanner = std::function<facebook::axiom::logical_plan::PlanBuilder(
      const RelationPtr& table)>;

  /// Returns the plan of a table of one row with one column, 'name', holding
  /// 'expression'.
  using RowPlanner = std::function<facebook::axiom::logical_plan::PlanBuilder(
      const ExpressionPtr& expression,
      std::string_view name)>;

  /// Returns the plan of 'call'. Its arguments are named:
  /// - 'queries', a table with the embeddings to search for, or 'query', an
  ///   ARRAY(REAL) expression;
  /// - 'corpus': the table to search, TABLE(name), or TABLE(SELECT * FROM name
  ///   WHERE <predicate>) to search only the partitions the predicate selects;
  /// - 'embeddings': the ARRAY(REAL) columns to compare, DESCRIPTOR(query
  ///   column, corpus column). DESCRIPTOR(column) names a column both have, or
  ///   only the corpus's column with 'query';
  /// - 'metric': a VARCHAR literal naming how to compare embeddings;
  /// - 'k': a positive integer literal;
  /// - 'score', optional: DESCRIPTOR(column), adds a column with each match's
  ///   score under that name;
  /// - 'hints', optional: how the search runs, MAP(ARRAY[...], ARRAY[...]) of
  ///   VARCHAR literals: 'index', the name of the corpus's vector index to
  ///   search; 'num_probes', the number of clusters nearest to each query to
  ///   search in each partition of the index; and the settings of the index's
  ///   local indexes. They become the search's hints (see
  ///   logical_plan::VectorSearchNode).
  ///
  /// The result has the score, if requested, then the queries' columns and the
  /// corpus's columns: up to 'k' rows per query. The columns of a table
  /// argument keep its alias, or the table's name for TABLE(name), as their
  /// qualifier, so a name both sides have resolves qualified, as in a join:
  /// q.ds and p.ds. Other columns have no qualifier.
  static facebook::axiom::logical_plan::PlanBuilder plan(
      const TableFunctionInvocation& call,
      const TablePlanner& planTable,
      const RowPlanner& planRow);
};

} // namespace axiom::sql::presto
