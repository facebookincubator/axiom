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

#include "axiom/sql/presto/RecursiveCteValidator.h"

#include "axiom/sql/presto/ExpressionPlanner.h"
#include "axiom/sql/presto/PrestoSqlError.h"
#include "axiom/sql/presto/ast/DefaultTraversalVisitor.h"
#include "velox/exec/Aggregate.h"

namespace axiom::sql::presto {
namespace {

// Detects whether a query body references the table 'cteName' (already
// canonicalized) -- i.e. whether a WITH RECURSIVE CTE actually recurses.
// Descent stops at a nested query that redefines the same name, so an inner
// CTE shadowing the outer name is not mistaken for a self-reference.
class CteSelfReferenceFinder : public DefaultTraversalVisitor {
 public:
  explicit CteSelfReferenceFinder(std::string cteName)
      : cteName_{std::move(cteName)} {}

  bool found() const {
    return found_;
  }

 protected:
  void visitTable(Table* node) override {
    if (canonicalizeName(node->name()->suffix()) == cteName_) {
      found_ = true;
    }
  }

  void visitQuery(Query* node) override {
    if (const auto& with = node->with()) {
      for (const auto& withQuery : with->queries()) {
        if (canonicalizeIdentifier(*withQuery->name()) == cteName_) {
          // The name is shadowed by a nested CTE; references below resolve to
          // that CTE, not the one we are looking for.
          return;
        }
      }
    }
    DefaultTraversalVisitor::visitQuery(node);
  }

 private:
  const std::string cteName_;
  bool found_{false};
};

// Rejects constructs ANSI forbids in a WITH RECURSIVE recursive term:
// DISTINCT, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, window functions
// (FunctionCall with OVER), and aggregate function calls. Stops at
// TableSubquery boundaries -- subqueries have their own iteration scope
// and are unrestricted.
class RecursiveTermValidator : public DefaultTraversalVisitor {
 public:
  explicit RecursiveTermValidator(std::string cteName)
      : cteName_{std::move(cteName)} {}

 protected:
  void visitQuery(Query* node) override {
    rejectOrderLimitOffset(
        node->location(), node->orderBy(), node->limit(), node->offset());
    DefaultTraversalVisitor::visitQuery(node);
  }

  void visitQuerySpecification(QuerySpecification* node) override {
    AXIOM_PRESTO_SYNTAX_CHECK(
        !node->select()->isDistinct(),
        node->location(),
        cteName_,
        "WITH RECURSIVE recursive term does not support DISTINCT");
    AXIOM_PRESTO_SYNTAX_CHECK(
        node->groupBy() == nullptr,
        node->location(),
        cteName_,
        "WITH RECURSIVE recursive term does not support GROUP BY");
    AXIOM_PRESTO_SYNTAX_CHECK(
        node->having() == nullptr,
        node->location(),
        cteName_,
        "WITH RECURSIVE recursive term does not support HAVING");
    rejectOrderLimitOffset(
        node->location(), node->orderBy(), node->limit(), node->offset());
    DefaultTraversalVisitor::visitQuerySpecification(node);
  }

  void visitFunctionCall(FunctionCall* node) override {
    const auto& name = node->name()->suffix();
    AXIOM_PRESTO_SYNTAX_CHECK(
        node->window() == nullptr,
        node->location(),
        cteName_,
        "WITH RECURSIVE recursive term does not support window functions");
    AXIOM_PRESTO_SYNTAX_CHECK(
        facebook::velox::exec::getAggregateFunctionEntry(name) == nullptr,
        node->location(),
        cteName_,
        "WITH RECURSIVE recursive term does not support aggregate functions");
    DefaultTraversalVisitor::visitFunctionCall(node);
  }

  // FROM-clause subqueries have their own scope; the recursive ref inside
  // them reads the working table at the current iteration, which is
  // well-defined. Matches Postgres / DuckDB.
  void visitTableSubquery(TableSubquery* /*node*/) override {}

  // Scalar / IN / EXISTS / quantified subqueries (`SubqueryExpression`)
  // resolve the recursive ref per-row of the outer query, which is not
  // what users mean. Matches Postgres / DuckDB. Stop descent: the
  // subquery's internal constructs are unrestricted -- only the ref's
  // presence is forbidden.
  void visitSubqueryExpression(SubqueryExpression* node) override {
    if (node->query() != nullptr &&
        RecursiveCteValidator::referencesCte(*node->query(), cteName_)) {
      AXIOM_PRESTO_SYNTAX_FAIL(
          node->location(),
          cteName_,
          "WITH RECURSIVE recursive term does not support the recursive reference inside a scalar/IN/EXISTS subquery");
    }
  }

  // Per ANSI / Postgres / DuckDB: the recursive ref may not appear on the
  // nullable side of an outer join -- a NULL-padded row can re-join with
  // itself on the next iteration, producing spurious infinite growth.
  void visitJoin(Join* node) override {
    switch (node->joinType()) {
      case Join::Type::kLeft:
        rejectRefOnRightSide(*node);
        break;
      case Join::Type::kRight:
        rejectRefOnLeftSide(*node);
        break;
      case Join::Type::kFull:
        rejectRefOnLeftSide(*node);
        rejectRefOnRightSide(*node);
        break;
      case Join::Type::kInner:
      case Join::Type::kCross:
      case Join::Type::kImplicit:
        break;
    }
    DefaultTraversalVisitor::visitJoin(node);
  }

 private:
  void rejectRefOnLeftSide(const Join& node) {
    if (node.left() != nullptr &&
        RecursiveCteValidator::referencesCte(*node.left(), cteName_)) {
      AXIOM_PRESTO_SYNTAX_FAIL(
          node.location(),
          cteName_,
          "WITH RECURSIVE recursive reference may not appear on the nullable left side of an outer join");
    }
  }

  void rejectRefOnRightSide(const Join& node) {
    if (node.right() != nullptr &&
        RecursiveCteValidator::referencesCte(*node.right(), cteName_)) {
      AXIOM_PRESTO_SYNTAX_FAIL(
          node.location(),
          cteName_,
          "WITH RECURSIVE recursive reference may not appear on the nullable right side of an outer join");
    }
  }

  void rejectOrderLimitOffset(
      const NodeLocation& location,
      const std::shared_ptr<OrderBy>& orderBy,
      const std::optional<std::string>& limit,
      const std::shared_ptr<Offset>& offset) {
    AXIOM_PRESTO_SYNTAX_CHECK(
        orderBy == nullptr,
        location,
        cteName_,
        "WITH RECURSIVE does not support ORDER BY in the recursive CTE");
    AXIOM_PRESTO_SYNTAX_CHECK(
        !limit.has_value(),
        location,
        cteName_,
        "WITH RECURSIVE does not support LIMIT in the recursive CTE");
    AXIOM_PRESTO_SYNTAX_CHECK(
        offset == nullptr,
        location,
        cteName_,
        "WITH RECURSIVE does not support OFFSET in the recursive CTE");
  }

  const std::string cteName_;
};

void validateRecursiveTerm(const Node& stepBody, const std::string& cteName) {
  RecursiveTermValidator validator{cteName};
  validator.process(const_cast<Node*>(&stepBody));
}

} // namespace

bool RecursiveCteValidator::referencesCte(
    const Node& body,
    const std::string& cteName) {
  CteSelfReferenceFinder finder{cteName};
  finder.process(const_cast<Node*>(&body));
  return finder.found();
}

const Union* RecursiveCteValidator::extractAndValidateRecursiveUnion(
    const WithQuery& withEntry,
    const std::string& cteName) {
  auto* bodyQuery = dynamic_cast<Query*>(withEntry.query().get());
  AXIOM_PRESTO_SYNTAX_CHECK(
      bodyQuery != nullptr,
      withEntry.location(),
      cteName,
      "Recursive CTE body is not a query");
  const auto& queryBody = bodyQuery->queryBody();

  // Reject set operations other than UNION ALL.
  AXIOM_PRESTO_SYNTAX_CHECK(
      queryBody->is(NodeType::kUnion) && !queryBody->as<Union>()->isDistinct(),
      bodyQuery->location(),
      cteName,
      "Recursive CTE '{}' body must be UNION ALL",
      cteName);

  const auto* unionExpr = queryBody->as<Union>();

  // The self-reference must live in the recursive (right) term. A reference
  // in the anchor (left) term -- including the left side of a 3+-way UNION
  // ALL, which parses left-associatively -- is not supported.
  AXIOM_PRESTO_SYNTAX_CHECK(
      !RecursiveCteValidator::referencesCte(*unionExpr->left(), cteName),
      bodyQuery->location(),
      cteName,
      "WITH RECURSIVE self-reference must appear in the recursive term "
      "(right side of UNION ALL), not the anchor term");

  // ORDER BY / LIMIT / OFFSET on the wrapping Query are meaningless in a
  // recursive CTE (executor decides per-iteration order; ANSI forbids).
  AXIOM_PRESTO_SYNTAX_CHECK(
      bodyQuery->orderBy() == nullptr,
      bodyQuery->location(),
      cteName,
      "WITH RECURSIVE does not support ORDER BY in the recursive CTE");
  AXIOM_PRESTO_SYNTAX_CHECK(
      !bodyQuery->limit().has_value(),
      bodyQuery->location(),
      cteName,
      "WITH RECURSIVE does not support LIMIT in the recursive CTE");
  AXIOM_PRESTO_SYNTAX_CHECK(
      bodyQuery->offset() == nullptr,
      bodyQuery->location(),
      cteName,
      "WITH RECURSIVE does not support OFFSET in the recursive CTE");

  validateRecursiveTerm(*unionExpr->right(), cteName);

  return unionExpr;
}

} // namespace axiom::sql::presto
