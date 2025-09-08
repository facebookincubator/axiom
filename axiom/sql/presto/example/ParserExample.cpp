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

#include <folly/init/Init.h>
#include <iostream>
#include "axiom/sql/presto/ast/AstBuilder.h"
#include "axiom/sql/presto/ast/AstPrinter.h"
#include "axiom/sql/presto/ast/UpperCaseInputStream.h"
#include "axiom/sql/presto/grammar/PrestoSqlLexer.h"
#include "axiom/sql/presto/grammar/PrestoSqlParser.h"

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv, false);

  using namespace axiom::sql::presto;

  const std::string sql =
      "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id";

  std::cout << "SQL: " << sql << std::endl;

  UpperCaseInputStream input(sql);
  PrestoSqlLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  PrestoSqlParser parser(&tokens);

  auto* queryContext = parser.query();

  if (parser.getNumberOfSyntaxErrors()) {
    std::cout << "Parse Errors: " << parser.getNumberOfSyntaxErrors()
              << std::endl;
    return 1;
  }

  AstBuilder astBuilder;
  auto query =
      std::any_cast<std::shared_ptr<Query>>(astBuilder.visit(queryContext));

  AstPrinter printer(std::cout);
  query->accept(&printer);

  return 0;
}
